/*
 * Copyright (c) 2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.dataflow

import io.airbyte.cdk.load.command.DestinationCatalog
import io.airbyte.cdk.load.dataflow.finalization.StreamCompletionTracker
import io.airbyte.cdk.load.dataflow.pipeline.PipelineRunner
import io.airbyte.cdk.load.write.DestinationWriter
import io.airbyte.cdk.load.write.StreamLoader
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking

@Singleton
class DestinationLifecycle(
    private val destinationInitializer: DestinationWriter,
    private val destinationCatalog: DestinationCatalog,
    private val pipeline: PipelineRunner,
    private val completionTracker: StreamCompletionTracker,
    @Named("streamInitDispatcher") private val streamInitDispatcher: CoroutineDispatcher,
    @Named("streamFinalizeDispatcher") private val streamFinalizeDispatcher: CoroutineDispatcher,
) {
    private val log = KotlinLogging.logger {}

    fun run() {
        // Initialize the destination to make sure that it is ready for the data ingestion
        initializeDestination()

        // Create prepare individual streams for the data ingestion. E.g. create tables and
        // propagate the schema updates
        val streamLoaders = initializeIndividualStreams()

        try {
            // Move data
            runBlocking { pipeline.run() }
        } catch (e: Throwable) {
            // A pipeline failure used to propagate straight out of run(), so no stream loader
            // ever got closed and every temp table the job had created was left behind — one
            // orphan per stream per failed attempt, and nothing reclaims them since temp table
            // names are unique per job. Finalize anyway: with streams incomplete, each loader's
            // close() receives the failure, discards the load and drops its temp table.
            try {
                finalizeIndividualStreams(streamLoaders)
                teardownDestination()
            } catch (cleanupError: Exception) {
                // Logged and swallowed so a cleanup failure (e.g. the DB connection died with
                // the pipeline) cannot mask the pipeline failure being rethrown.
                log.error(cleanupError) {
                    "Stream finalization after pipeline failure also failed; temp tables may be left behind"
                }
            }
            throw e
        }

        // The pipeline drained the input and returned without throwing, so every stream the source
        // sent arrived in full. Treat that as completion: the platform does not deliver
        // STREAM_STATUS traces here, and without this the tracker stays empty and the finalization
        // below discards every temp table instead of upserting it.
        completionTracker.acceptEndOfInput()

        finalizeIndividualStreams(streamLoaders)

        teardownDestination()
    }

    private fun initializeDestination() {
        // The run blocking is not needed
        runBlocking {
            log.info { "Initializing the destination" }
            destinationInitializer.setup()
            log.info { "Destination initialized" }
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    private fun initializeIndividualStreams(): List<StreamLoader> {
        return runBlocking {
            val result =
                destinationCatalog.streams
                    .map {
                        async(streamInitDispatcher) {
                            log.info {
                                "Starting stream loader for stream ${it.mappedDescriptor.namespace}:${it.mappedDescriptor.name}"
                            }
                            val streamLoader = destinationInitializer.createStreamLoader(it)
                            streamLoader.start()
                            log.info {
                                "Stream loader for stream ${it.mappedDescriptor.namespace}:${it.mappedDescriptor.name} started"
                            }
                            streamLoader
                        }
                    }
                    .awaitAll()

            return@runBlocking result
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    private fun finalizeIndividualStreams(streamLoaders: List<StreamLoader>) {
        if (!completionTracker.allStreamsComplete()) {
            log.warn {
                "One or more streams did not complete. Skipping destructive finalization operations..."
            }
        }

        runBlocking {
            streamLoaders
                .map {
                    async(streamFinalizeDispatcher) {
                        log.info {
                            "Finalizing stream ${it.stream.mappedDescriptor.namespace}:${it.stream.mappedDescriptor.name}"
                        }
                        it.teardown(completionTracker.allStreamsComplete())
                        log.info {
                            "Finalized stream ${it.stream.mappedDescriptor.namespace}:${it.stream.mappedDescriptor.name}"
                        }
                    }
                }
                .awaitAll()
        }
    }

    private fun teardownDestination() {
        runBlocking {
            log.info { "Tearing down the destination" }
            destinationInitializer.teardown()
            log.info { "Destination torn down" }
        }
    }
}
