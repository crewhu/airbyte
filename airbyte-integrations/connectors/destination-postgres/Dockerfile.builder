# Build environment for the destination-postgres connector.
#
# Pins the JDK and Gradle that the Airbyte build needs (Java 21 — the Gradle
# plugins refuse anything older, and Gradle 8.14 cannot read class files from
# Java 26+). Building inside this image means the host only needs Docker: no
# JDK, no Gradle, no SDKMAN, and no risk of a machine-specific Java version
# breaking the build.
#
# This image compiles the connector and produces build/distributions/airbyte-app.tar.
# It deliberately does NOT build the connector's own Docker image — that would
# require Docker-in-Docker. The host's docker buildx assembles the final image
# from the tar, which is also what lets us cross-build for linux/amd64.

FROM eclipse-temurin:21-jdk

# Gradle needs a writable home; keep it on a volume-friendly path so the
# caller can cache it between runs (see build-and-push.sh).
ENV GRADLE_USER_HOME=/gradle-cache

RUN apt-get update \
    && apt-get install -y --no-install-recommends git unzip \
    && rm -rf /var/lib/apt/lists/*

# The repo's gradle wrapper downloads the pinned Gradle distribution itself,
# so there is nothing else to install here.

WORKDIR /workspace

ENTRYPOINT ["/bin/bash", "-c"]
