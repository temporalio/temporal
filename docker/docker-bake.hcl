variable "SERVER_VERSION" {
  default = "unknown"
}

variable "CLI_VERSION" {
  default = "unknown"
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

# IMPORTANT: When updating ALPINE_TAG, also update the default value in:
# - docker/targets/admin-tools.Dockerfile
# - docker/targets/server.Dockerfile
variable "ALPINE_TAG" {
  default = "3.23@sha256:c78ded0fee4493809c8ca71d4a6057a46237763d952fae15ea418f6d14137f2d"
}

target "admin-tools" {
  context = "docker"
  dockerfile = "targets/admin-tools.Dockerfile"
  args = {
    ALPINE_TAG = "${ALPINE_TAG}"
  }
  tags = compact([
    "${IMAGE_REPO}/admin-tools:${IMAGE_SHA_TAG}",
    "${IMAGE_REPO}/admin-tools:${SAFE_IMAGE_BRANCH_TAG}",
    TAG_LATEST ? "${IMAGE_REPO}/admin-tools:latest" : "",
  ])
  platforms = ["linux/amd64", "linux/arm64"]
  labels = {
    "org.opencontainers.image.title" = "admin-tools"
    "org.opencontainers.image.description" = "Temporal admin tools"
    "org.opencontainers.image.url" = "https://github.com/temporalio/temporal"
    "org.opencontainers.image.source" = "https://github.com/temporalio/temporal"
    "org.opencontainers.image.licenses" = "MIT"
    "org.opencontainers.image.revision" = "${TEMPORAL_SHA}"
    "org.opencontainers.image.created" = timestamp()
    "com.temporal.server.version" = "${SERVER_VERSION}"
    "com.temporal.cli.version" = "${CLI_VERSION}"
  }
}

target "server" {
  context = "docker"
  dockerfile = "targets/server.Dockerfile"
  args = {
    ALPINE_TAG = "${ALPINE_TAG}"
  }
  tags = compact([
    "${IMAGE_REPO}/server:${IMAGE_SHA_TAG}",
    "${IMAGE_REPO}/server:${SAFE_IMAGE_BRANCH_TAG}",
    TAG_LATEST ? "${IMAGE_REPO}/server:latest" : "",
  ])
  platforms = ["linux/amd64", "linux/arm64"]
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
