/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.command

import io.airbyte.cdk.load.data.FieldType
import io.airbyte.cdk.load.data.ObjectType
import io.airbyte.cdk.load.data.ObjectTypeWithEmptySchema
import io.airbyte.cdk.load.data.ObjectTypeWithoutSchema
import io.airbyte.cdk.load.data.json.JsonSchemaToAirbyteType
import io.airbyte.cdk.load.schema.TableSchemaFactory
import io.airbyte.cdk.load.schema.model.TableName
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream
import io.airbyte.protocol.models.v0.DestinationSyncMode
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.util.UUID

private val log = KotlinLogging.logger {}

@Singleton
class DestinationStreamFactory(
    private val jsonSchemaToAirbyteType: JsonSchemaToAirbyteType,
    private val namespaceMapper: NamespaceMapper,
    private val schemaFactory: TableSchemaFactory,
) {
    // Per-worker fallback id. Concurrent jobs running in separate worker pods get
    // different ids, which keeps temp table names isolated even when no stable
    // identifier (CONNECTION_ID env var or non-zero syncId) is available.
    private val workerFallbackUniqueId: String by lazy {
        UUID.randomUUID().toString().take(8)
    }

    fun make(stream: ConfiguredAirbyteStream, resolvedTableName: TableName): DestinationStream {
        val airbyteSchemaType = jsonSchemaToAirbyteType.convert(stream.stream.jsonSchema)
        val airbyteSchema: Map<String, FieldType> =
            when (airbyteSchemaType) {
                is ObjectType -> airbyteSchemaType.properties
                is ObjectTypeWithEmptySchema,
                is ObjectTypeWithoutSchema -> emptyMap()
                else -> throw IllegalStateException("")
            }
        val importType =
            when (stream.destinationSyncMode) {
                null -> throw IllegalArgumentException("Destination sync mode was null")
                DestinationSyncMode.APPEND -> Append
                DestinationSyncMode.OVERWRITE -> Overwrite
                DestinationSyncMode.APPEND_DEDUP ->
                    Dedupe(primaryKey = stream.primaryKey, cursor = stream.cursorField)
                DestinationSyncMode.UPDATE -> Update
                DestinationSyncMode.SOFT_DELETE -> SoftDelete
            }
        val syncId = stream.syncId ?: 0L
        val tempTableUniqueId =
            System.getenv("CONNECTION_ID")
                ?: System.getenv("AIRBYTE_CONNECTION_ID")
                ?: syncId.takeIf { it != 0L }?.toString()
                ?: workerFallbackUniqueId
        val tableSchema =
            schemaFactory.make(
                resolvedTableName,
                airbyteSchema,
                importType,
                uniqueId = tempTableUniqueId,
            )

        return DestinationStream(
            unmappedNamespace = stream.stream.namespace,
            unmappedName = stream.stream.name,
            namespaceMapper = namespaceMapper,
            importType = importType,
            generationId = stream.generationId ?: 0L,
            minimumGenerationId = stream.minimumGenerationId ?: 0L,
            syncId = syncId,
            schema = airbyteSchemaType,
            isFileBased = stream.stream.isFileBased ?: false,
            includeFiles = stream.includeFiles ?: false,
            destinationObjectName = stream.destinationObjectName,
            matchingKey =
                stream.destinationObjectName?.let {
                    fromCompositeNestedKeyToCompositeKey(stream.primaryKey)
                },
            tableSchema = tableSchema,
        )
    }

    private fun fromCompositeNestedKeyToCompositeKey(
        compositeNestedKey: List<List<String>>
    ): List<String> {
        if (compositeNestedKey.any { it.size > 1 }) {
            throw IllegalArgumentException(
                "Nested keys are not supported for matching keys. Key was $compositeNestedKey",
            )
        }
        if (compositeNestedKey.any { it.isEmpty() }) {
            throw IllegalArgumentException(
                "Parts of the composite key need to have at least one element. Key was $compositeNestedKey",
            )
        }

        return compositeNestedKey.map { it[0] }.toList()
    }
}
