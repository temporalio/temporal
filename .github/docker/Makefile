CLI_VERSION ?= 1.5.0
IMAGE_REPO ?= temporaliotest
TAG_LATEST ?= false
TEMPORAL_SHA ?= 07b75b062d194720bf65775e656942439f6b93a9

.DEFAULT_GOAL := build-all

.PHONY: build-all
build-all:
	go run build-from-source.go \
		--cli-version=$(CLI_VERSION) \
		--temporal-sha=$(TEMPORAL_SHA) \
		--image-repo=$(IMAGE_REPO) \
		--tag-latest=$(TAG_LATEST)

.PHONY: build-admin-tools
build-admin-tools:
	go run build-from-source.go \
		--cli-version=$(CLI_VERSION) \
		--temporal-sha=$(TEMPORAL_SHA) \
		--image-repo=$(IMAGE_REPO) \
		--tag-latest=$(TAG_LATEST) \
		--target=admin-tools

.PHONY: build-server
build-server:
	go run build-from-source.go \
		--cli-version=$(CLI_VERSION) \
		--temporal-sha=$(TEMPORAL_SHA) \
		--image-repo=$(IMAGE_REPO) \
		--tag-latest=$(TAG_LATEST) \
		--target=server

.PHONY: clean
clean:
	rm -rf ./build
