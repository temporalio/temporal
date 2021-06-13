ifndef DOCKER_IMAGE_TAG
DOCKER_IMAGE_TAG := "latest"
endif

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
buildx-docker-container:
	docker buildx create --driver docker-container --use

base-admin-tools-x:
	docker buildx build -f base-admin-tools.Dockerfile -t temporalio/base-admin-tools:$(DOCKER_IMAGE_TAG) --platform linux/amd64,linux/arm64 --output type=registry .

base-builder-x:
	docker buildx build -f base-builder.Dockerfile -t temporalio/base-builder:$(DOCKER_IMAGE_TAG) --platform linux/amd64,linux/arm64 --output type=registry .

base-server-x:
	docker buildx build -f base-server.Dockerfile -t temporalio/base-server:$(DOCKER_IMAGE_TAG) --platform linux/amd64,linux/arm64 --output type=registry .

base-ci-builder-x:
	@echo CI builder is not supported because \"shellcheck\" and \"fossa\" are not available on \"linux/arm64\".

