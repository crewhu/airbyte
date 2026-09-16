/*
 * Copyright (c) 2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.integrations.destination.postgres

import io.airbyte.cdk.AirbyteDestinationRunner
import io.github.oshai.kotlinlogging.KotlinLogging

private val log = KotlinLogging.logger {}

/**
 * Build marker for this fork's image.
 *
 * Baked in at build time by build-and-push.sh (see BUILD_TAG in that script). It is logged on
 * every startup so a job's logs say unambiguously which image ran — the Airbyte UI shows the tag
 * that is *configured*, which is not proof of what the pod actually pulled when a tag has been
 * reused or a layer cached.
 */
private const val BUILD_TAG = "@BUILD_TAG@"

fun main(args: Array<String>) {
    log.info { "crewhu fork | destination-postgres | build=$BUILD_TAG" }
    AirbyteDestinationRunner.run(*args)
}
