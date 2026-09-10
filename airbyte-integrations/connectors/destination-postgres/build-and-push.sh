#!/usr/bin/env bash
#
# Build and (optionally) push a destination-postgres connector image.
#
# The only host requirement is a working Docker engine. The JDK and Gradle live
# inside a builder image (Dockerfile.builder), so the result does not depend on
# what Java happens to be installed locally.
#
# Two stages:
#   1. Compile in the builder container  -> build/distributions/airbyte-app.tar
#   2. Assemble the connector image on the host with docker buildx (linux/amd64)
#
# Splitting them avoids Docker-in-Docker: the container never needs a daemon.
#
# Usage:
#   ./build-and-push.sh --tag v1.2.3
#   ./build-and-push.sh --tag v1.2.3 --push
#   ./build-and-push.sh --tag test --skip-tests
#
# Run --help for all options.

set -euo pipefail

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

CONNECTOR_NAME="destination-postgres"
DEFAULT_REGISTRY_REPO="vsantos98/destination-postgres"
BUILDER_IMAGE="airbyte-connector-builder:jdk21"
PLATFORM="linux/amd64"

TAG=""
REGISTRY_REPO="$DEFAULT_REGISTRY_REPO"
DO_PUSH=false
RUN_TESTS=true
REBUILD_BUILDER=false

# Resolve paths independently of the caller's working directory.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
CONNECTOR_DIR="$SCRIPT_DIR"
GRADLE_PROJECT=":airbyte-integrations:connectors:${CONNECTOR_NAME}"

# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

if [[ -t 1 ]]; then
    BOLD=$'\033[1m'; RED=$'\033[31m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'; RESET=$'\033[0m'
else
    BOLD=""; RED=""; GREEN=""; YELLOW=""; RESET=""
fi

step() { echo; echo "${BOLD}==> $*${RESET}"; }
info() { echo "    $*"; }
warn() { echo "${YELLOW}    warning: $*${RESET}"; }
die()  { echo "${RED}error: $*${RESET}" >&2; exit 1; }

usage() {
    cat <<EOF
${BOLD}build-and-push.sh${RESET} — build the ${CONNECTOR_NAME} connector image

${BOLD}Options${RESET}
  --tag TAG          Image tag to produce. Required.
                     Use a distinct tag per build so Airbyte does not serve a
                     cached layer and so you can roll back.
  --repo REPO        Target repository. Default: ${DEFAULT_REGISTRY_REPO}
  --push             Push to the registry after building. Requires docker login.
                     Without this flag the image is only loaded locally.
  --skip-tests       Skip the unit tests. Faster, but unverified.
  --rebuild-builder  Force a rebuild of the JDK builder image.
  --platform PLAT    Target platform. Default: ${PLATFORM}
  -h, --help         Show this help.

${BOLD}Examples${RESET}
  # Local build, tests run, nothing published
  ./build-and-push.sh --tag temp-table-fix

  # Build and publish
  ./build-and-push.sh --tag temp-table-fix --push
EOF
}

# ---------------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------------

while [[ $# -gt 0 ]]; do
    case "$1" in
        --tag)              TAG="${2:-}";           shift 2 ;;
        --repo)             REGISTRY_REPO="${2:-}"; shift 2 ;;
        --platform)         PLATFORM="${2:-}";      shift 2 ;;
        --push)             DO_PUSH=true;           shift ;;
        --skip-tests)       RUN_TESTS=false;        shift ;;
        --rebuild-builder)  REBUILD_BUILDER=true;   shift ;;
        -h|--help)          usage; exit 0 ;;
        *)                  die "unknown option: $1 (try --help)" ;;
    esac
done

[[ -n "$TAG" ]] || { usage >&2; echo >&2; die "--tag is required"; }

LOCAL_IMAGE="${CONNECTOR_NAME}-local:${TAG}"
REMOTE_IMAGE="${REGISTRY_REPO}:${TAG}"

# Short commit, with a marker when the tree has uncommitted changes — so a
# stamped image can always be traced back to source.
GIT_SHA="$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo 'nogit')"
if ! git -C "$REPO_ROOT" diff --quiet HEAD -- "$CONNECTOR_DIR" 2>/dev/null; then
    GIT_SHA="${GIT_SHA}-dirty"
fi

# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------

step "Checking Docker engine"
if ! docker version >/dev/null 2>&1; then
    die "Docker is not responding. Start OrbStack or Docker Desktop and retry.
       OrbStack: open -a OrbStack"
fi
info "engine: $(docker version --format '{{.Server.Os}}/{{.Server.Arch}}')"

# Refuse to overwrite a tag that already exists remotely, unless it is a
# throwaway. Overwriting a published tag makes rollback impossible and lets
# Airbyte serve a stale cached image.
if [[ "$DO_PUSH" == true ]]; then
    if docker manifest inspect "$REMOTE_IMAGE" >/dev/null 2>&1; then
        warn "$REMOTE_IMAGE already exists in the registry and will be overwritten."
        read -r -p "    Continue? [y/N] " reply
        [[ "$reply" =~ ^[Yy]$ ]] || die "aborted"
    fi
fi

# ---------------------------------------------------------------------------
# 1. Builder image
# ---------------------------------------------------------------------------

step "Preparing builder image ($BUILDER_IMAGE)"
if [[ "$REBUILD_BUILDER" == true ]] || ! docker image inspect "$BUILDER_IMAGE" >/dev/null 2>&1; then
    info "building..."
    docker build \
        --file "$CONNECTOR_DIR/Dockerfile.builder" \
        --tag "$BUILDER_IMAGE" \
        "$CONNECTOR_DIR"
else
    info "already present (use --rebuild-builder to refresh)"
fi

# ---------------------------------------------------------------------------
# 2. Compile inside the container
# ---------------------------------------------------------------------------

# A named volume keeps the Gradle cache across runs; without it every build
# re-downloads the whole dependency tree.
GRADLE_CACHE_VOLUME="airbyte-connector-gradle-cache"
docker volume create "$GRADLE_CACHE_VOLUME" >/dev/null

GRADLE_TASKS="$GRADLE_PROJECT:distTar"
if [[ "$RUN_TESTS" == true ]]; then
    GRADLE_TASKS="$GRADLE_PROJECT:test $GRADLE_TASKS"
else
    warn "tests skipped (--skip-tests)"
fi

# Stamp the build tag into the source so the connector can log which image it
# is. The Airbyte UI only shows the tag that is *configured*; that is not proof
# of what a pod actually pulled. The placeholder is restored on exit, including
# on failure, so the working tree is never left modified.
VERSION_FILE="$CONNECTOR_DIR/src/main/kotlin/io/airbyte/integrations/destination/postgres/PostgresDestinationV2.kt"
BUILD_STAMP="${TAG} (${GIT_SHA})"

restore_version_file() {
    if [[ -n "${VERSION_FILE_BACKUP:-}" && -f "$VERSION_FILE_BACKUP" ]]; then
        mv "$VERSION_FILE_BACKUP" "$VERSION_FILE"
    fi
}
trap restore_version_file EXIT

VERSION_FILE_BACKUP="$(mktemp)"
cp "$VERSION_FILE" "$VERSION_FILE_BACKUP"
# The stamp contains no '|', so it is safe as a sed delimiter here.
sed -i.bak "s|@BUILD_TAG@|${BUILD_STAMP}|" "$VERSION_FILE" && rm -f "${VERSION_FILE}.bak"

grep -q "$BUILD_STAMP" "$VERSION_FILE" \
    || die "failed to stamp the build tag into $(basename "$VERSION_FILE")"

step "Compiling connector in container"
info "tasks: $GRADLE_TASKS"
info "build stamp: $BUILD_STAMP"

# --no-daemon: the container is discarded anyway, and a lingering daemon would
# hold the cache volume open.
if ! docker run --rm \
    --volume "$REPO_ROOT:/workspace" \
    --volume "$GRADLE_CACHE_VOLUME:/gradle-cache" \
    --workdir /workspace \
    "$BUILDER_IMAGE" \
    "./gradlew $GRADLE_TASKS --no-daemon"; then
    die "compilation failed — see the Gradle output above"
fi

DIST_TAR="$CONNECTOR_DIR/build/distributions/airbyte-app.tar"
[[ -f "$DIST_TAR" ]] || die "expected artifact not found: $DIST_TAR"
info "artifact: $(du -h "$DIST_TAR" | cut -f1) $DIST_TAR"

# ---------------------------------------------------------------------------
# 3. Assemble the connector image
# ---------------------------------------------------------------------------

# The base image is pinned in metadata.yaml; read it from there so this script
# does not drift from the connector's declared base.
BASE_IMAGE="$(
    grep -A5 'connectorBuildOptions:' "$CONNECTOR_DIR/metadata.yaml" \
        | grep 'baseImage:' \
        | head -1 \
        | sed 's/.*baseImage: *//' \
        | tr -d '"'
)"
[[ -n "$BASE_IMAGE" ]] || die "could not read baseImage from metadata.yaml"

step "Assembling connector image"
info "base:     $BASE_IMAGE"
info "platform: $PLATFORM"
info "tag:      $LOCAL_IMAGE"

# Stage the build context the way the Gradle docker task would.
DOCKER_CONTEXT="$CONNECTOR_DIR/build/airbyte/docker"
mkdir -p "$DOCKER_CONTEXT"
cp "$DIST_TAR" "$DOCKER_CONTEXT/airbyte-app.tar"
cp "$REPO_ROOT/docker-images/Dockerfile.java-connector-non-airbyte-ci" "$DOCKER_CONTEXT/Dockerfile"

docker buildx build \
    --platform "$PLATFORM" \
    --build-arg "BASE_IMAGE=$BASE_IMAGE" \
    --build-arg "CONNECTOR_NAME=$CONNECTOR_NAME" \
    --label "io.airbyte.app=$CONNECTOR_NAME" \
    --label "io.airbyte.version=$TAG" \
    --tag "$LOCAL_IMAGE" \
    --file "$DOCKER_CONTEXT/Dockerfile" \
    --output type=docker \
    "$DOCKER_CONTEXT"

# ---------------------------------------------------------------------------
# 4. Verify
# ---------------------------------------------------------------------------

step "Verifying image"
ACTUAL_ARCH="$(docker image inspect "$LOCAL_IMAGE" --format '{{.Os}}/{{.Architecture}}')"
if [[ "$ACTUAL_ARCH" != "$PLATFORM" ]]; then
    die "architecture mismatch: built $ACTUAL_ARCH, expected $PLATFORM"
fi
info "architecture: $ACTUAL_ARCH"

# `spec` exercises the connector end to end without touching a database.
info "running connector spec..."
if docker run --rm --platform "$PLATFORM" "$LOCAL_IMAGE" spec >/dev/null 2>&1; then
    info "${GREEN}spec ok${RESET}"
else
    die "the connector failed to run 'spec' — the image is broken, not publishing"
fi

# ---------------------------------------------------------------------------
# 5. Push
# ---------------------------------------------------------------------------

if [[ "$DO_PUSH" == true ]]; then
    step "Pushing to $REMOTE_IMAGE"
    docker tag "$LOCAL_IMAGE" "$REMOTE_IMAGE"
    docker push "$REMOTE_IMAGE"

    DIGEST="$(docker buildx imagetools inspect "$REMOTE_IMAGE" --format '{{.Manifest.Digest}}' 2>/dev/null || echo 'unknown')"
    step "${GREEN}Published${RESET}"
    info "image:  $REMOTE_IMAGE"
    info "digest: $DIGEST"
    info "arch:   $ACTUAL_ARCH"
    echo
    info "In Airbyte, set the destination's custom image to:"
    info "  ${BOLD}$REMOTE_IMAGE${RESET}"
else
    step "${GREEN}Built${RESET} (not pushed)"
    info "local image: $LOCAL_IMAGE"
    echo
    info "To publish:  $0 --tag $TAG --push"
fi
