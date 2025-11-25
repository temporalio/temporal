variable "SERVER_VERSION" {
  default = "1.29.1"
}
variable "ALPINE_IMAGE" {
  default = "alpine:3.22@sha256:4b7ce07002c69e8f3d704a9c5d6fd3053be500b7f1c69fc0d80990c2ad8dd412"
}


variable "CLI_VERSION" {
  default = "1.5.0"
}

variable "TCTL_VERSION" {
  default = "1.18.4"
}

variable "IMAGE_REPO" {
  default = "temporaliotest"
}

variable "IMAGE_SHA_TAG" {}

variable "IMAGE_BRANCH_TAG" {}

variable "SAFE_IMAGE_BRANCH_TAG" {
  default = join("-", [for c in regexall("[a-z0-9]+", lower(IMAGE_BRANCH_TAG)) : c])
}

variable "TEMPORAL_SHA" {
  default = ""
}

variable "TAG_LATEST" {
  default = false
}

# Legacy targets (legacy-admin-tools, legacy-server) are for building images with server versions
# older than v1.27.0 (3 minor versions behind v1.30.0). Once support for pre-1.27.0 versions is
# no longer needed, these legacy targets can be removed and only the standard targets should be used.

target "admin-tools" {
  dockerfile = "targets/admin-tools.Dockerfile"
  target = "temporal-admin-tools"
  tags = compact([
    "${IMAGE_REPO}/admin-tools:${IMAGE_SHA_TAG}",
    "${IMAGE_REPO}/admin-tools:${SAFE_IMAGE_BRANCH_TAG}",
    TAG_LATEST ? "${IMAGE_REPO}/admin-tools:latest" : "",
  ])
  platforms = ["linux/amd64", "linux/arm64"]
  args = {
    ALPINE_IMAGE = "${ALPINE_IMAGE}"
  }
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

target "legacy-admin-tools" {
  dockerfile = "targets/legacy-admin-tools.Dockerfile"
  target = "temporal-admin-tools"
  tags = compact([
    "${IMAGE_REPO}/admin-tools:${IMAGE_SHA_TAG}",
    "${IMAGE_REPO}/admin-tools:${SAFE_IMAGE_BRANCH_TAG}",
    TAG_LATEST ? "${IMAGE_REPO}/admin-tools:latest" : "",
  ])
  platforms = ["linux/amd64", "linux/arm64"]
  args = {
    ALPINE_IMAGE = "${ALPINE_IMAGE}"
  }
  labels = {
    "org.opencontainers.image.title" = "admin-tools"
    "org.opencontainers.image.description" = "Temporal admin tools with database clients"
    "org.opencontainers.image.url" = "https://github.com/temporalio/temporal"
    "org.opencontainers.image.source" = "https://github.com/temporalio/temporal"
    "org.opencontainers.image.licenses" = "MIT"
    "org.opencontainers.image.version" = "${SERVER_VERSION}"
    "org.opencontainers.image.revision" = "${TEMPORAL_SHA}"
    "org.opencontainers.image.created" = timestamp()
    "com.temporal.server.version" = "${SERVER_VERSION}"
    "com.temporal.cli.version" = "${CLI_VERSION}"
    "com.temporal.tctl.version" = "${TCTL_VERSION}"
  }
}

target "server" {
  dockerfile = "targets/server.Dockerfile"
  tags = compact([
    "${IMAGE_REPO}/server:${IMAGE_SHA_TAG}",
    "${IMAGE_REPO}/server:${SAFE_IMAGE_BRANCH_TAG}",
    TAG_LATEST ? "${IMAGE_REPO}/server:latest" : "",
  ])
  platforms = ["linux/amd64", "linux/arm64"]
  args = {
    ALPINE_IMAGE = "${ALPINE_IMAGE}"
  }
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

target "legacy-server" {
  dockerfile = "targets/legacy-server.Dockerfile"
  tags = compact([
    "${IMAGE_REPO}/server:${IMAGE_SHA_TAG}",
    "${IMAGE_REPO}/server:${SAFE_IMAGE_BRANCH_TAG}",
    TAG_LATEST ? "${IMAGE_REPO}/server:latest" : "",
  ])
  platforms = ["linux/amd64", "linux/arm64"]
  args = {
    ALPINE_IMAGE = "${ALPINE_IMAGE}"
    TEMPORAL_VERSION = "${SERVER_VERSION}"
    TEMPORAL_SHA = "${TEMPORAL_SHA}"
  }
  labels = {
    "org.opencontainers.image.title" = "server"
    "org.opencontainers.image.description" = "Temporal Server with tctl"
    "org.opencontainers.image.url" = "https://github.com/temporalio/temporal"
    "org.opencontainers.image.source" = "https://github.com/temporalio/temporal"
    "org.opencontainers.image.licenses" = "MIT"
    "org.opencontainers.image.version" = "${SERVER_VERSION}"
    "org.opencontainers.image.revision" = "${TEMPORAL_SHA}"
    "org.opencontainers.image.created" = timestamp()
    "com.temporal.server.version" = "${SERVER_VERSION}"
    "com.temporal.cli.version" = "${CLI_VERSION}"
    "com.temporal.tctl.version" = "${TCTL_VERSION}"
  }
}
