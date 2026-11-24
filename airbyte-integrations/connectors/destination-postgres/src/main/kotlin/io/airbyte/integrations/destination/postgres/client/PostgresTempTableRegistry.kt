/*
 * Copyright (c) 2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.postgres.client

import com.google.common.annotations.VisibleForTesting
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import io.airbyte.cdk.ConfigErrorException
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.schema.model.TableName
import io.airbyte.integrations.destination.postgres.spec.PostgresConfiguration
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.sql.PreparedStatement
import java.sql.Types
import javax.sql.DataSource

private val log = KotlinLogging.logger {}

/**
 * Bookkeeping for the dedicated temp table schema.
 *
 * Temp table names are opaque hashes, so nothing in the database says which stream a temp table
 * belongs to or which job left it behind. When a temp table schema is configured, this keeps one
 * row per temp table in [REGISTRY_TABLE_NAME], in that same schema: what it stages data for, which
 * job created it and when, and which job touched it last. A row is removed when its table is
 * dropped or promoted to a final table, so the registry mirrors the temp tables that exist -- and
 * a row with an old `last_used_at` is an orphan.
 *
 * The registry is bookkeeping, never a correctness requirement. Once a sync is running, a failed
 * registry write is logged and the sync continues; only [createRegistryTable], which runs before
 * any data moves, is allowed to fail the sync.
 *
 * With no temp table schema configured, [schema] is null, [tracks] is always false and nothing
 * here touches the database.
 */
@Singleton
@SuppressFBWarnings(
    value =
        [
            "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE",
            "SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING"
        ],
    justification =
        "Only the schema identifier, which comes from the connector configuration, is interpolated. Every value is bound as a parameter."
)
class PostgresTempTableRegistry(
    private val dataSource: DataSource,
    postgresConfiguration: PostgresConfiguration,
) {
    /** The dedicated temp table schema, or null when the feature is off. */
    val schema: String? = postgresConfiguration.tempTableSchema

    /** Replaceable in tests; the identity of the running job only exists as env vars. */
    @VisibleForTesting var env: (String) -> String? = System::getenv

    companion object {
        const val REGISTRY_TABLE_NAME = "_airbyte_temp_tables"

        private const val CONNECTION_ID_ENV = "CONNECTION_ID"
        private const val AIRBYTE_CONNECTION_ID_ENV = "AIRBYTE_CONNECTION_ID"
        private const val JOB_ID_ENV = "WORKER_JOB_ID"
        private const val ATTEMPT_ENV = "WORKER_JOB_ATTEMPT"
    }

    /** True when [tableName] is a temp table this registry keeps a row for. */
    fun tracks(tableName: TableName): Boolean =
        schema != null && tableName.namespace == schema && tableName.name != REGISTRY_TABLE_NAME

    /**
     * Rejects a temp table schema that a final table also uses. Sharing it would put final tables
     * back among the temp tables -- and, worse, make them look like temp tables to anything that
     * cleans the schema up.
     */
    fun requireDedicatedSchema(finalTableNamespaces: Collection<String>) {
        val tempTableSchema = schema ?: return
        if (tempTableSchema in finalTableNamespaces) {
            throw ConfigErrorException(
                "The temporary tables schema \"$tempTableSchema\" is also the schema of a final " +
                    "table. Choose a schema that no stream writes to, so that it only ever " +
                    "holds temporary tables."
            )
        }
    }

    /** Creates the registry table if it is missing. The schema must already exist. */
    fun createRegistryTable() {
        val sql = createRegistryTableSql() ?: return
        log.info { sql.trimIndent() }
        try {
            dataSource.connection.use { connection ->
                connection.createStatement().use { it.execute(sql) }
            }
        } catch (e: org.postgresql.util.PSQLException) {
            // Concurrent syncs share the registry and can race to create it; IF NOT EXISTS does
            // not protect against two sessions inserting the same catalog rows.
            if (
                e.message?.contains("pg_type_typname_nsp_index") == true ||
                    e.message?.contains("duplicate key value") == true ||
                    e.message?.contains("already exists") == true
            ) {
                log.debug(e) { "Temp table registry already exists (race condition), ignoring" }
            } else {
                throw e
            }
        }
    }

    /**
     * Records that the running job created or reused [tableName] to stage data for [stream].
     *
     * @param newlyCreated false when the table already existed and was kept, in which case the
     * original creation time and creating job are preserved and only the last-use columns move.
     */
    fun recordUse(stream: DestinationStream, tableName: TableName, newlyCreated: Boolean) {
        val sql = recordUseSql(newlyCreated) ?: return
        val finalTableName = stream.tableSchema.tableNames.finalTableName
        val jobId = env(JOB_ID_ENV)?.takeIf { it.isNotBlank() }
        val attempt = env(ATTEMPT_ENV)?.takeIf { it.isNotBlank() }
        val connectionId =
            (env(CONNECTION_ID_ENV) ?: env(AIRBYTE_CONNECTION_ID_ENV))?.takeIf { it.isNotBlank() }
        bestEffort("record temp table ${tableName.namespace}.${tableName.name}") {
            dataSource.connection.use { connection ->
                connection.prepareStatement(sql).use { statement ->
                    statement.setString(1, tableName.name)
                    statement.setNullableString(2, finalTableName?.namespace)
                    statement.setNullableString(3, finalTableName?.name)
                    statement.setNullableString(4, stream.unmappedNamespace)
                    statement.setString(5, stream.unmappedName)
                    statement.setNullableString(6, connectionId)
                    statement.setLong(7, stream.syncId)
                    statement.setLong(8, stream.generationId)
                    statement.setNullableString(9, jobId)
                    statement.setNullableString(10, attempt)
                    statement.setNullableString(11, jobId)
                    statement.setNullableString(12, attempt)
                    statement.executeUpdate()
                }
            }
        }
    }

    /** Records that [tableName] no longer exists as a temp table. */
    fun recordRemoval(tableName: TableName) {
        val sql = recordRemovalSql() ?: return
        bestEffort("remove temp table ${tableName.namespace}.${tableName.name}") {
            dataSource.connection.use { connection ->
                connection.prepareStatement(sql).use { statement ->
                    statement.setString(1, tableName.name)
                    statement.executeUpdate()
                }
            }
        }
    }

    internal fun createRegistryTableSql(): String? =
        registryTable()?.let {
            """
            CREATE TABLE IF NOT EXISTS $it (
                table_name text PRIMARY KEY,
                final_schema text,
                final_table text,
                stream_namespace text,
                stream_name text NOT NULL,
                connection_id text,
                sync_id bigint,
                generation_id bigint,
                created_at timestamptz NOT NULL DEFAULT now(),
                created_by_job_id text,
                created_by_attempt text,
                last_used_at timestamptz NOT NULL DEFAULT now(),
                last_used_by_job_id text,
                last_used_by_attempt text
            );
            """
        }

    internal fun recordUseSql(newlyCreated: Boolean): String? =
        registryTable()?.let {
            // A row can outlive its table (a manual DROP, a failed removal), so a newly created
            // table always resets the creation columns instead of trusting what is there.
            val creationColumns =
                if (newlyCreated) {
                    """
                    created_at = now(),
                    created_by_job_id = EXCLUDED.created_by_job_id,
                    created_by_attempt = EXCLUDED.created_by_attempt,"""
                } else {
                    ""
                }
            """
            INSERT INTO $it (
                table_name, final_schema, final_table, stream_namespace, stream_name,
                connection_id, sync_id, generation_id,
                created_by_job_id, created_by_attempt,
                last_used_by_job_id, last_used_by_attempt
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (table_name) DO UPDATE SET
                final_schema = EXCLUDED.final_schema,
                final_table = EXCLUDED.final_table,
                stream_namespace = EXCLUDED.stream_namespace,
                stream_name = EXCLUDED.stream_name,
                connection_id = EXCLUDED.connection_id,
                sync_id = EXCLUDED.sync_id,
                generation_id = EXCLUDED.generation_id,$creationColumns
                last_used_at = now(),
                last_used_by_job_id = EXCLUDED.last_used_by_job_id,
                last_used_by_attempt = EXCLUDED.last_used_by_attempt;
            """
        }

    internal fun recordRemovalSql(): String? =
        registryTable()?.let { "DELETE FROM $it WHERE table_name = ?;" }

    private fun registryTable(): String? = schema?.let { "\"$it\".\"$REGISTRY_TABLE_NAME\"" }

    private fun bestEffort(action: String, block: () -> Unit) {
        try {
            block()
        } catch (e: Exception) {
            log.warn(e) {
                "Could not $action in the temp table registry. The sync continues; the registry " +
                    "may be out of date for this table."
            }
        }
    }

    private fun PreparedStatement.setNullableString(index: Int, value: String?) {
        if (value == null) setNull(index, Types.VARCHAR) else setString(index, value)
    }
}
