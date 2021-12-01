DOCKER_IMAGE_TAG ?= latest
# Pass "registry" to automatically push image to the docker hub.
DOCKER_BUILDX_OUTPUT ?= image

##### docker build #####
base-admin-tools:
	docker build -f base-admin-tools.Dockerfile -t temporalio/base-admin-tools:$(DOCKER_IMAGE_TAG) .
	docker push temporalio/base-admin-tools:$(DOCKER_IMAGE_TAG)

base-builder:
	docker build -f base-builder.Dockerfile -t temporalio/base-builder:$(DOCKER_IMAGE_TAG) .
	docker push temporalio/base-builder:$(DOCKER_IMAGE_TAG)

base-server:
	docker build -f base-server.Dockerfile -t temporalio/base-server:$(DOCKER_IMAGE_TAG) .
	docker push temporalio/base-server:$(DOCKER_IMAGE_TAG)

base-ci-builder:
	docker build -f base-ci-builder.Dockerfile -t temporalio/base-ci-builder:$(DOCKER_IMAGE_TAG) .
	docker push temporalio/base-ci-builder:$(DOCKER_IMAGE_TAG)

##### docker buildx #####
docker-buildx-container:
	docker buildx create --name builder-x --driver docker-container --use

base-admin-tools-x:
	docker buildx build . -f base-admin-tools.Dockerfile -t temporalio/base-admin-tools:$(DOCKER_IMAGE_TAG) --platform linux/amd64,linux/arm64 --output type=$(DOCKER_BUILDX_OUTPUT)

base-builder-x:
	docker buildx build . -f base-builder.Dockerfile -t temporalio/base-builder:$(DOCKER_IMAGE_TAG) --platform linux/amd64,linux/arm64 --output type=$(DOCKER_BUILDX_OUTPUT)

base-server-x:
	docker buildx build . -f base-server.Dockerfile -t temporalio/base-server:$(DOCKER_IMAGE_TAG) --platform linux/amd64,linux/arm64 --output type=$(DOCKER_BUILDX_OUTPUT)

base-ci-builder-x:
	@echo CI builder is not supported because \"shellcheck\" and \"fossa\" are not available on \"linux/arm64\".

