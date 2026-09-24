/*
 * Copyright (c) 2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.dataflow.finalization

import io.airbyte.cdk.load.command.DestinationCatalog
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.message.DestinationRecordStreamComplete
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.util.concurrent.ConcurrentHashMap

private val log = KotlinLogging.logger {}

/** Tracks whether we've received stream complete messages for all streams in the catalog. */
@Singleton
class StreamCompletionTracker(
    catalog: DestinationCatalog,
) {
    private val expectedStreams: Set<DestinationStream.Descriptor> =
        catalog.streams.map { it.mappedDescriptor }.toSet()

    private val completedStreams: MutableSet<DestinationStream.Descriptor> =
        ConcurrentHashMap.newKeySet()

    fun accept(msg: DestinationRecordStreamComplete) {
        val descriptor = msg.stream.mappedDescriptor
        completedStreams.add(descriptor)
        log.info {
            "crewhu fork | stream-completion | accepted complete for ${descriptor.toPrettyString()} " +
                "(${completedStreams.size}/${expectedStreams.size})"
        }
    }

    fun allStreamsComplete(): Boolean {
        val complete = completedStreams.containsAll(expectedStreams)
        if (!complete) {
            // Diagnostics: a stream that never registers as complete makes the destination discard
            // every temp table without upserting, so name which side is empty or mismatched.
            val missing = expectedStreams - completedStreams
            val unexpected = completedStreams - expectedStreams
            log.warn {
                "crewhu fork | stream-completion | incomplete: " +
                    "expected=${expectedStreams.map { it.toPrettyString() }} " +
                    "completed=${completedStreams.map { it.toPrettyString() }} " +
                    "missing=${missing.map { it.toPrettyString() }} " +
                    "unexpected=${unexpected.map { it.toPrettyString() }}"
            }
        }
        return complete
    }
}
