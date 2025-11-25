SERVER_VERSION ?= 1.29.1
CLI_VERSION ?= 1.5.0
TCTL_VERSION ?= 1.18.4
IMAGE_REPO ?= temporaliotest
TAG_LATEST ?= false

TEMPORAL_SHA := $(shell git rev-parse HEAD)
IMAGE_SHA_TAG := $(shell git rev-parse --short HEAD)
IMAGE_BRANCH_TAG := $(shell git rev-parse --abbrev-ref HEAD)

BAKE_ENV := SERVER_VERSION=$(SERVER_VERSION) \
	CLI_VERSION=$(CLI_VERSION) \
	TCTL_VERSION=$(TCTL_VERSION) \
	IMAGE_REPO=$(IMAGE_REPO) \
	IMAGE_SHA_TAG=$(IMAGE_SHA_TAG) \
	IMAGE_BRANCH_TAG=$(IMAGE_BRANCH_TAG) \
	TEMPORAL_SHA=$(TEMPORAL_SHA) \
	TAG_LATEST=$(TAG_LATEST)

.PHONY: build-tools
build-admin-tools:
	./download-binaries.sh $(SERVER_VERSION) $(CLI_VERSION) $(TCTL_VERSION) amd64
	./download-binaries.sh $(SERVER_VERSION) $(CLI_VERSION) $(TCTL_VERSION) arm64
	./copy-schema.sh
	$(BAKE_ENV) docker buildx bake admin-tools

.PHONY: build-admin-tools-legacy
build-admin-tools-legacy:
	./download-binaries.sh $(SERVER_VERSION) $(CLI_VERSION) $(TCTL_VERSION) amd64
	./download-binaries.sh $(SERVER_VERSION) $(CLI_VERSION) $(TCTL_VERSION) arm64
	./copy-schema.sh
	$(BAKE_ENV) docker buildx bake legacy-admin-tools

.PHONY: build-server
build-server:
	./download-binaries.sh $(SERVER_VERSION) $(CLI_VERSION) $(TCTL_VERSION) amd64
	./download-binaries.sh $(SERVER_VERSION) $(CLI_VERSION) $(TCTL_VERSION) arm64
	$(BAKE_ENV) docker buildx bake server

.PHONY: build-server-legacy
build-server-legacy:
	./download-binaries.sh $(SERVER_VERSION) $(CLI_VERSION) $(TCTL_VERSION) amd64
	./download-binaries.sh $(SERVER_VERSION) $(CLI_VERSION) $(TCTL_VERSION) arm64
	$(BAKE_ENV) docker buildx bake legacy-server

.PHONY: push
push:
	./download-binaries.sh $(SERVER_VERSION) $(CLI_VERSION) $(TCTL_VERSION) amd64
	./download-binaries.sh $(SERVER_VERSION) $(CLI_VERSION) $(TCTL_VERSION) arm64
	./copy-schema.sh
	$(BAKE_ENV) docker buildx bake --push

.PHONY: clean
clean:
	rm -rf ./build
