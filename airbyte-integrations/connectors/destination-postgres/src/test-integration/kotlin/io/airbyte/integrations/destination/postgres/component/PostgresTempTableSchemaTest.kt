/*
 * Copyright (c) 2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.postgres.component

import io.airbyte.cdk.load.command.Append
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.command.NamespaceMapper
import io.airbyte.cdk.load.data.FieldType
import io.airbyte.cdk.load.data.ObjectType
import io.airbyte.cdk.load.data.StringType
import io.airbyte.cdk.load.schema.model.ColumnSchema
import io.airbyte.cdk.load.schema.model.StreamTableSchema
import io.airbyte.cdk.load.schema.model.TableName
import io.airbyte.cdk.load.schema.model.TableNames
import io.airbyte.cdk.load.table.ColumnNameMapping
import io.airbyte.integrations.destination.postgres.client.PostgresAirbyteClient
import io.airbyte.integrations.destination.postgres.client.PostgresTempTableRegistry
import io.airbyte.integrations.destination.postgres.spec.PostgresConfiguration
import io.airbyte.integrations.destination.postgres.sql.PostgresColumnUtils
import io.airbyte.integrations.destination.postgres.sql.PostgresDirectLoadSqlGenerator
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import java.util.UUID
import javax.sql.DataSource
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

/**
 * Exercises the dedicated temp table schema against a real Postgres: the cross-schema promotion of
 * a temp table to a final table, and the registry that tracks temp tables through their life.
 */
@MicronautTest(environments = ["component"])
class PostgresTempTableSchemaTest(
    private val dataSource: DataSource,
    private val sqlGenerator: PostgresDirectLoadSqlGenerator,
    private val columnUtils: PostgresColumnUtils,
    private val baseConfiguration: PostgresConfiguration,
) {
    private val suffix = UUID.randomUUID().toString().replace("-", "").take(12)
    private val finalSchema = "final_$suffix"
    private val tempSchema = "tmp_$suffix"
    private val finalTable = TableName(finalSchema, "tickets")
    private val tempTable = TableName(tempSchema, "tickets_tmp_hash")
    private val columnNameMapping = ColumnNameMapping(emptyMap())

    private lateinit var registry: PostgresTempTableRegistry
    private lateinit var client: PostgresAirbyteClient
    private val env = mutableMapOf<String, String>()

    @BeforeEach
    fun setup() {
        val configuration = baseConfiguration.copy(tempTableSchema = tempSchema)
        registry = PostgresTempTableRegistry(dataSource, configuration)
        registry.env = env::get
        client =
            PostgresAirbyteClient(dataSource, sqlGenerator, columnUtils, configuration, registry)
        env.clear()
        env["WORKER_JOB_ID"] = "1001"
        env["WORKER_JOB_ATTEMPT"] = "0"
        env["CONNECTION_ID"] = "conn-a"
        runBlocking {
            client.createNamespace(finalSchema)
            client.createNamespace(tempSchema)
        }
        registry.createRegistryTable()
    }

    @AfterEach
    fun teardown() {
        execute("DROP SCHEMA IF EXISTS \"$finalSchema\" CASCADE")
        execute("DROP SCHEMA IF EXISTS \"$tempSchema\" CASCADE")
    }

    @Test
    fun `creating the registry twice is harmless`() {
        registry.createRegistryTable()
        assertEquals(emptyList<Map<String, String?>>(), registryRows())
    }

    @Test
    fun `a temp table is registered with the job that created it`() = runBlocking {
        client.createTable(stream(), tempTable, columnNameMapping, replace = true)

        assertTrue(client.tableExists(tempTable))
        val row = registryRows().single()
        assertEquals(tempTable.name, row["table_name"])
        assertEquals(finalSchema, row["final_schema"])
        assertEquals("tickets", row["final_table"])
        assertEquals("tickets", row["stream_name"])
        assertEquals("conn-a", row["connection_id"])
        assertEquals("1001", row["created_by_job_id"])
        assertEquals("0", row["created_by_attempt"])
        assertEquals("1001", row["last_used_by_job_id"])
        assertNotNull(row["created_at"])
    }

    @Test
    fun `a later job reusing the temp table keeps its creation and moves its last use`() =
        runBlocking {
            client.createTable(stream(), tempTable, columnNameMapping, replace = true)
            val createdAt = registryRows().single()["created_at"]

            env["WORKER_JOB_ID"] = "1002"
            client.createTable(stream(), tempTable, columnNameMapping, replace = false)

            val row = registryRows().single()
            assertEquals(createdAt, row["created_at"])
            assertEquals("1001", row["created_by_job_id"])
            assertEquals("1002", row["last_used_by_job_id"])
        }

    @Test
    fun `a later job replacing the temp table becomes its creator`() = runBlocking {
        client.createTable(stream(), tempTable, columnNameMapping, replace = true)

        env["WORKER_JOB_ID"] = "1002"
        client.createTable(stream(), tempTable, columnNameMapping, replace = true)

        assertEquals("1002", registryRows().single()["created_by_job_id"])
    }

    @Test
    fun `dropping a temp table removes it from the registry`() = runBlocking {
        client.createTable(stream(), tempTable, columnNameMapping, replace = true)

        client.dropTable(tempTable)

        assertFalse(client.tableExists(tempTable))
        assertEquals(emptyList<Map<String, String?>>(), registryRows())
    }

    @Test
    fun `promoting a temp table moves it into the final schema with its data and privileges`() =
        runBlocking {
            // An existing final table that the promotion must replace.
            client.createTable(stream(), finalTable, columnNameMapping, replace = true)
            client.createTable(stream(), tempTable, columnNameMapping, replace = true)
            execute(
                "INSERT INTO \"$tempSchema\".\"${tempTable.name}\" " +
                    "(_airbyte_raw_id, _airbyte_extracted_at, _airbyte_meta, _airbyte_generation_id, test_key) " +
                    "VALUES ('r1', now(), '{}', 0, 'from-temp')"
            )
            val reader = "reader_$suffix"
            execute("CREATE ROLE \"$reader\"")
            try {
                execute(
                    "GRANT SELECT ON \"$tempSchema\".\"${tempTable.name}\" TO \"$reader\""
                )

                client.overwriteTable(tempTable, finalTable)

                assertFalse(client.tableExists(tempTable))
                assertTrue(client.tableExists(finalTable))
                assertEquals(1L, client.countTable(finalTable))
                assertEquals(
                    "from-temp",
                    queryString("SELECT test_key FROM \"$finalSchema\".\"tickets\"")
                )
                // The grant made in the temp schema travelled with the table.
                assertEquals(
                    "t",
                    queryString(
                        "SELECT has_table_privilege('$reader', '\"$finalSchema\".\"tickets\"', 'SELECT')::text"
                    ).take(1)
                )
                // Nothing but the registry is left in the temp schema.
                assertEquals(
                    PostgresTempTableRegistry.REGISTRY_TABLE_NAME,
                    queryString(
                        "SELECT string_agg(table_name, ',') FROM information_schema.tables " +
                            "WHERE table_schema = '$tempSchema'"
                    )
                )
                assertEquals(emptyList<Map<String, String?>>(), registryRows())
            } finally {
                execute("DROP OWNED BY \"$reader\"")
                execute("DROP ROLE \"$reader\"")
            }
        }

    private fun stream(): DestinationStream {
        val schema = ObjectType(linkedMapOf("test_key" to FieldType(StringType, nullable = true)))
        return DestinationStream(
            unmappedNamespace = finalSchema,
            unmappedName = "tickets",
            importType = Append,
            schema = schema,
            generationId = 3L,
            minimumGenerationId = 0L,
            syncId = 99L,
            namespaceMapper = NamespaceMapper(),
            tableSchema =
                StreamTableSchema(
                    tableNames = TableNames(finalTableName = finalTable, tempTableName = tempTable),
                    columnSchema =
                        ColumnSchema(
                            inputSchema = schema.properties,
                            inputToFinalColumnNames = mapOf("test_key" to "test_key"),
                            finalSchema = emptyMap(),
                        ),
                    importType = Append,
                ),
        )
    }

    private fun registryRows(): List<Map<String, String?>> =
        dataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement
                    .executeQuery(
                        "SELECT * FROM \"$tempSchema\".\"${PostgresTempTableRegistry.REGISTRY_TABLE_NAME}\""
                    )
                    .use { rs ->
                        val columns = (1..rs.metaData.columnCount).map { rs.metaData.getColumnName(it) }
                        val rows = mutableListOf<Map<String, String?>>()
                        while (rs.next()) {
                            rows.add(columns.associateWith { rs.getString(it) })
                        }
                        rows
                    }
            }
        }

    private fun queryString(sql: String): String =
        dataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.executeQuery(sql).use { rs ->
                    rs.next()
                    rs.getString(1)
                }
            }
        }

    private fun execute(sql: String) {
        dataSource.connection.use { connection ->
            connection.createStatement().use { it.execute(sql) }
        }
    }
}
