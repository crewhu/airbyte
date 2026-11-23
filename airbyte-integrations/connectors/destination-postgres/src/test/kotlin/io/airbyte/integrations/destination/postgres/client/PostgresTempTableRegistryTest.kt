/*
 * Copyright (c) 2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.postgres.client

import io.airbyte.cdk.ConfigErrorException
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.schema.model.TableName
import io.airbyte.integrations.destination.postgres.spec.PostgresConfiguration
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.SQLException
import java.sql.Types
import javax.sql.DataSource
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

internal class PostgresTempTableRegistryTest {

    private val dataSource = mockk<DataSource>()

    private fun registry(tempTableSchema: String?): PostgresTempTableRegistry {
        val configuration =
            mockk<PostgresConfiguration> { every { this@mockk.tempTableSchema } returns tempTableSchema }
        return PostgresTempTableRegistry(dataSource, configuration)
    }

    private fun stream(): DestinationStream = mockk {
        every { tableSchema.tableNames.finalTableName } returns TableName("airbyte_etl", "tickets")
        every { unmappedNamespace } returns null
        every { unmappedName } returns "tickets"
        every { syncId } returns 42L
        every { generationId } returns 7L
    }

    @Test
    fun `tracks only tables in the temp schema, and never the registry itself`() {
        val registry = registry("airbyte_tmp")

        assertTrue(registry.tracks(TableName("airbyte_tmp", "abc123")))
        assertFalse(registry.tracks(TableName("airbyte_etl", "tickets")))
        assertFalse(
            registry.tracks(TableName("airbyte_tmp", PostgresTempTableRegistry.REGISTRY_TABLE_NAME))
        )
    }

    @Test
    fun `does nothing when no temp schema is configured`() {
        val registry = registry(null)

        assertFalse(registry.tracks(TableName("airbyte_etl", "abc123")))
        assertNull(registry.createRegistryTableSql())
        registry.requireDedicatedSchema(listOf("airbyte_etl"))
        registry.createRegistryTable()
        registry.recordUse(stream(), TableName("airbyte_etl", "abc123"), newlyCreated = true)
        registry.recordRemoval(TableName("airbyte_etl", "abc123"))

        verify(exactly = 0) { dataSource.connection }
    }

    @Test
    fun `rejects a temp schema that a final table also uses`() {
        val registry = registry("airbyte_etl")

        assertThrows(ConfigErrorException::class.java) {
            registry.requireDedicatedSchema(listOf("public", "airbyte_etl"))
        }
        assertDoesNotThrow { registry.requireDedicatedSchema(listOf("public")) }
    }

    @Test
    fun `a newly created table resets the creation columns, a reused one keeps them`() {
        val registry = registry("airbyte_tmp")

        val created = registry.recordUseSql(newlyCreated = true)!!
        val reused = registry.recordUseSql(newlyCreated = false)!!

        assertTrue(created.contains("INSERT INTO \"airbyte_tmp\".\"_airbyte_temp_tables\""))
        assertTrue(created.contains("created_at = now()"))
        assertTrue(created.contains("created_by_job_id = EXCLUDED.created_by_job_id"))
        assertFalse(reused.contains("created_at = now()"))
        assertFalse(reused.contains("created_by_job_id = EXCLUDED.created_by_job_id"))
        assertTrue(reused.contains("last_used_at = now()"))
        assertTrue(reused.contains("last_used_by_job_id = EXCLUDED.last_used_by_job_id"))
    }

    @Test
    fun `recordUse binds the table, the stream and the running job`() {
        val statement = mockk<PreparedStatement>(relaxed = true)
        every { dataSource.connection } returns
            mockk<Connection> {
                every { prepareStatement(any()) } returns statement
                every { close() } just Runs
            }
        val registry = registry("airbyte_tmp")
        registry.env =
            mapOf(
                "WORKER_JOB_ID" to "475338",
                "WORKER_JOB_ATTEMPT" to "2",
                "CONNECTION_ID" to "conn-1",
            )::get

        registry.recordUse(stream(), TableName("airbyte_tmp", "abc123"), newlyCreated = true)

        verify {
            statement.setString(1, "abc123")
            statement.setString(2, "airbyte_etl")
            statement.setString(3, "tickets")
            statement.setNull(4, Types.VARCHAR)
            statement.setString(5, "tickets")
            statement.setString(6, "conn-1")
            statement.setLong(7, 42L)
            statement.setLong(8, 7L)
            statement.setString(9, "475338")
            statement.setString(10, "2")
            statement.setString(11, "475338")
            statement.setString(12, "2")
            statement.executeUpdate()
        }
    }

    @Test
    fun `a job with no identity in its environment is recorded with nulls`() {
        val statement = mockk<PreparedStatement>(relaxed = true)
        every { dataSource.connection } returns
            mockk<Connection> {
                every { prepareStatement(any()) } returns statement
                every { close() } just Runs
            }
        val registry = registry("airbyte_tmp")
        registry.env = { null }

        registry.recordUse(stream(), TableName("airbyte_tmp", "abc123"), newlyCreated = true)

        verify {
            statement.setNull(6, Types.VARCHAR)
            statement.setNull(9, Types.VARCHAR)
            statement.setNull(10, Types.VARCHAR)
        }
    }

    @Test
    fun `a failed registry write never fails the sync`() {
        every { dataSource.connection } throws SQLException("registry unavailable")
        val registry = registry("airbyte_tmp")
        registry.env = { null }

        assertDoesNotThrow {
            registry.recordUse(stream(), TableName("airbyte_tmp", "abc123"), newlyCreated = true)
            registry.recordRemoval(TableName("airbyte_tmp", "abc123"))
        }
    }

    @Test
    fun `a failure to create the registry table is not swallowed`() {
        every { dataSource.connection } throws SQLException("permission denied")
        val registry = registry("airbyte_tmp")

        assertThrows(SQLException::class.java) { registry.createRegistryTable() }
    }
}
