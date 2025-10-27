variable "platforms" {
  default = ["linux/amd64", "linux/arm64"]
}

variable "IMAGE_REPO" {
  default = "ghcr.io/chaptersix"
}

variable "IMAGE_SHA_TAG" {}

variable "IMAGE_BRANCH_TAG" {}

variable "SAFE_IMAGE_BRANCH_TAG" {
  default = join("-", [for c in regexall("[a-z0-9]+", lower(IMAGE_BRANCH_TAG)) : c])
}

variable "TEMPORAL_SHA" {}

variable "TAG_LATEST" {
  default = false
}

group "default" {
  targets = [
    "server",
    "admin-tools",
  ]
}

target "server" {
  dockerfile = ".docker/server.Dockerfile"
  context = "."
  tags = [
    "${IMAGE_REPO}/server:${IMAGE_SHA_TAG}",
    "${IMAGE_REPO}/server:${SAFE_IMAGE_BRANCH_TAG}",
    TAG_LATEST ? "${IMAGE_REPO}/server:latest" : ""
  ]
  platforms = platforms
  args = {
    TEMPORAL_SHA = "${TEMPORAL_SHA}"
  }
  labels = {
    "org.opencontainers.image.title" = "server"
    "org.opencontainers.image.description" = "Temporal Server"
    "org.opencontainers.image.url" = "https://github.com/temporalio/temporal"
    "org.opencontainers.image.source" = "https://github.com/temporalio/temporal"
    "org.opencontainers.image.licenses" = "MIT"
  }
}

target "admin-tools" {
  dockerfile = ".docker/admin-tools.Dockerfile"
  context = "."
  tags = [
    "${IMAGE_REPO}/admin-tools:${IMAGE_SHA_TAG}",
    "${IMAGE_REPO}/admin-tools:${SAFE_IMAGE_BRANCH_TAG}",
    TAG_LATEST ? "${IMAGE_REPO}/admin-tools:latest" : ""
  ]
  platforms = platforms
  labels = {
    "org.opencontainers.image.title" = "admin-tools"
    "org.opencontainers.image.description" = "Temporal Admin Tools"
    "org.opencontainers.image.url" = "https://github.com/temporalio/temporal"
    "org.opencontainers.image.source" = "https://github.com/temporalio/temporal"
    "org.opencontainers.image.licenses" = "MIT"
  }
}
