variable "SERVER_VERSION" {
  default = "1.29.1"
}

variable "CLI_VERSION" {
  default = "1.5.1"
}

variable "IMAGE_REPO" {
  default = "temporaliotest"
}

variable "IMAGE_SHA_TAG" {
  default = ""
}

variable "IMAGE_BRANCH_TAG" {
  default = ""
}

variable "SAFE_IMAGE_BRANCH_TAG" {
  default = join("-", [for c in regexall("[a-z0-9]+", lower(IMAGE_BRANCH_TAG)) : c])
}

variable "TEMPORAL_SHA" {
  default = ""
}

variable "TAG_LATEST" {
  default = false
}

variable "ENABLE_CACHE" {
  default = false
}

# Legacy targets (legacy-admin-tools, legacy-server) are for building images with server versions
# older than v1.27.0 (3 minor versions behind v1.30.0). Once support for pre-1.27.0 versions is
# no longer needed, these legacy targets can be removed and only the standard targets should be used.

target "admin-tools" {
  context = "docker"
  dockerfile = "targets/admin-tools.Dockerfile"
  tags = compact([
    "${IMAGE_REPO}/admin-tools:${IMAGE_SHA_TAG}",
    "${IMAGE_REPO}/admin-tools:${SAFE_IMAGE_BRANCH_TAG}",
    TAG_LATEST ? "${IMAGE_REPO}/admin-tools:latest" : "",
  ])
  platforms = ["linux/amd64", "linux/arm64"]
  cache-from = ENABLE_CACHE ? ["type=registry,ref=${IMAGE_REPO}/admin-tools:buildcache"] : []
  cache-to = ENABLE_CACHE ? ["type=registry,ref=${IMAGE_REPO}/admin-tools:buildcache,mode=max"] : []
  labels = {
    "org.opencontainers.image.title" = "admin-tools"
    "org.opencontainers.image.description" = "Temporal admin tools"
    "org.opencontainers.image.url" = "https://github.com/temporalio/temporal"
    "org.opencontainers.image.source" = "https://github.com/temporalio/temporal"
    "org.opencontainers.image.licenses" = "MIT"
    "org.opencontainers.image.version" = "${SERVER_VERSION}"
    "org.opencontainers.image.revision" = "${TEMPORAL_SHA}"
    "org.opencontainers.image.created" = timestamp()
    "com.temporal.server.version" = "${SERVER_VERSION}"
    "com.temporal.cli.version" = "${CLI_VERSION}"
  }
}

target "server" {
  context = "docker"
  dockerfile = "targets/server.Dockerfile"
  tags = compact([
    "${IMAGE_REPO}/server:${IMAGE_SHA_TAG}",
    "${IMAGE_REPO}/server:${SAFE_IMAGE_BRANCH_TAG}",
    TAG_LATEST ? "${IMAGE_REPO}/server:latest" : "",
  ])
  platforms = ["linux/amd64", "linux/arm64"]
  cache-from = ENABLE_CACHE ? ["type=registry,ref=${IMAGE_REPO}/server:buildcache"] : []
  cache-to = ENABLE_CACHE ? ["type=registry,ref=${IMAGE_REPO}/server:buildcache,mode=max"] : []
  labels = {
    "org.opencontainers.image.title" = "server"
    "org.opencontainers.image.description" = "Temporal Server"
    "org.opencontainers.image.url" = "https://github.com/temporalio/temporal"
    "org.opencontainers.image.source" = "https://github.com/temporalio/temporal"
    "org.opencontainers.image.licenses" = "MIT"
    "org.opencontainers.image.version" = "${SERVER_VERSION}"
    "org.opencontainers.image.revision" = "${TEMPORAL_SHA}"
    "org.opencontainers.image.created" = timestamp()
    "com.temporal.server.version" = "${SERVER_VERSION}"
  }
}
