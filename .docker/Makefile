CLI_VERSION := $(shell cat .cli-version)
TEMPORAL_SHA := $(shell cd .. && git rev-parse HEAD)
IMAGE_SHA_TAG := sha-$(shell cd .. && git rev-parse --short HEAD)
IMAGE_BRANCH_TAG := branch-$(shell cd .. && git rev-parse --abbrev-ref HEAD)
IMAGE_REPO ?= ghcr.io/chaptersix

DOCKER := docker buildx
BAKE := cd .. && IMAGE_SHA_TAG=$(IMAGE_SHA_TAG) \
		IMAGE_BRANCH_TAG=$(IMAGE_BRANCH_TAG) \
		TEMPORAL_SHA=$(TEMPORAL_SHA) \
		IMAGE_REPO=$(IMAGE_REPO) \
		$(DOCKER) bake -f .docker/docker-bake.hcl
NATIVE_ARCH := $(shell go env GOARCH)
BAKE_OUTPUT ?= docker

.PHONY: all
all: prepare-bins build-native

.PHONY: prepare-bins
prepare-bins: goreleaser-bins download-cli organize-bins

.PHONY: goreleaser-bins
goreleaser-bins:
	@echo "Building binaries with goreleaser..."
	@cd .. && goreleaser build --snapshot --clean \
		--id temporal-server \
		--id temporal-cassandra-tool \
		--id temporal-sql-tool \
		--id temporal-elasticsearch-tool \
		--id tdbg

.PHONY: download-cli
download-cli:
	@echo "Downloading temporal CLI ${CLI_VERSION}..."
	@mkdir -p dist/tmp
	@for arch in amd64 arm64; do \
		echo "Downloading for linux/$$arch..."; \
		VERSION=$$(echo ${CLI_VERSION} | sed 's/^v//'); \
		curl -fsSL "https://github.com/temporalio/cli/releases/download/${CLI_VERSION}/temporal_cli_$${VERSION}_linux_$$arch.tar.gz" | \
			tar -xzf - -C dist/tmp && \
			mkdir -p dist/$$arch && \
			mv dist/tmp/temporal dist/$$arch/temporal; \
	done
	@rm -rf dist/tmp

.PHONY: organize-bins
organize-bins:
	@echo "Organizing binaries for Docker..."
	@mkdir -p dist/amd64 dist/arm64
	@echo "Copying amd64 binaries..."
	@for binary in temporal-server temporal-cassandra-tool temporal-sql-tool temporal-elasticsearch-tool tdbg; do \
		find ../dist -type f -name "$$binary" | grep linux_amd64 | xargs -I {} cp -v {} dist/amd64/ ; \
	done
	@echo "Copying arm64 binaries..."
	@for binary in temporal-server temporal-cassandra-tool temporal-sql-tool temporal-elasticsearch-tool tdbg; do \
		find ../dist -type f -name "$$binary" | grep linux_arm64 | xargs -I {} cp -v {} dist/arm64/ ; \
	done
	@echo "Verifying binaries..."
	@ls -lh dist/amd64/ || echo "Warning: No amd64 binaries found"
	@ls -lh dist/arm64/ || echo "Warning: No arm64 binaries found"

.PHONY: build-native
build-native: prepare-bins
	@echo "Building native Docker images..."
	$(BAKE) --set "*.platform=linux/$(NATIVE_ARCH)" --load

.PHONY: build
build: prepare-bins
	@echo "Building multi-arch Docker images..."
	$(BAKE) --set="*.output=type=$(BAKE_OUTPUT)"

.PHONY: test
test:
	@echo "Testing Docker images..."
	IMAGE_SHA_TAG=$(IMAGE_SHA_TAG) IMAGE_REPO=$(IMAGE_REPO) ./scripts/test.sh

.PHONY: clean
clean:
	@echo "Cleaning build artifacts..."
	rm -rf dist/
	rm -rf ../dist/

.PHONY: help
help:
	@echo "Temporal Docker Build Makefile"
	@echo ""
	@echo "Targets:"
	@echo "  all              - Build binaries and native Docker images (default)"
	@echo "  prepare-bins     - Build binaries with goreleaser and download CLI"
	@echo "  goreleaser-bins  - Build Temporal binaries with goreleaser"
	@echo "  download-cli     - Download Temporal CLI from GitHub releases"
	@echo "  organize-bins    - Organize binaries for Docker build"
	@echo "  build-native     - Build Docker images for native architecture"
	@echo "  build            - Build multi-arch Docker images"
	@echo "  test             - Test Docker images"
	@echo "  clean            - Clean build artifacts"
	@echo ""
	@echo "Environment Variables:"
	@echo "  IMAGE_REPO       - Docker image repository (default: ghcr.io/chaptersix)"
	@echo "  BAKE_OUTPUT      - Docker output type: docker or registry (default: docker)"
