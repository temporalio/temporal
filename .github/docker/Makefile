CLI_VERSION ?= 1.5.0
IMAGE_REPO ?= temporaliotest
TAG_LATEST ?= false

TEMPORAL_SHA ?= 07b75b062d194720bf65775e656942439f6b93a9
IMAGE_SHA_TAG := $(shell git rev-parse --short HEAD)
IMAGE_BRANCH_TAG := $(shell git rev-parse --abbrev-ref HEAD)

BAKE_ENV := CLI_VERSION=$(CLI_VERSION) \
	IMAGE_REPO=$(IMAGE_REPO) \
	IMAGE_SHA_TAG=$(IMAGE_SHA_TAG) \
	IMAGE_BRANCH_TAG=$(IMAGE_BRANCH_TAG) \
	TEMPORAL_SHA=$(TEMPORAL_SHA) \
	TAG_LATEST=$(TAG_LATEST) \
	ALPINE_IMAGE=$(ALPINE_IMAGE)

.DEFAULT_GOAL := build-all

.PHONY: build-all
build-all: build-server build-admin-tools

.PHONY: build-admin-tools
build-admin-tools:
	go run build-from-source.go --cli-version=$(CLI_VERSION) --arch=amd64 --temporal-sha=$(TEMPORAL_SHA)
	go run build-from-source.go --cli-version=$(CLI_VERSION) --arch=arm64 --temporal-sha=$(TEMPORAL_SHA)
	$(BAKE_ENV) docker buildx bake admin-tools

.PHONY: build-server
build-server:
	go run build-from-source.go --cli-version=$(CLI_VERSION) --arch=amd64 --temporal-sha=$(TEMPORAL_SHA)
	go run build-from-source.go --cli-version=$(CLI_VERSION) --arch=arm64 --temporal-sha=$(TEMPORAL_SHA)
	$(BAKE_ENV) docker buildx bake server

.PHONY: clean
clean:
	rm -rf ./build
