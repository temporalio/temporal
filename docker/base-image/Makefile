ifndef DOCKER_IMAGE_TAG
DOCKER_IMAGE_TAG := "latest"
endif

base-images:
	docker build -f base-admin-tools.Dockerfile -t temporalio/base-admin-tools:$(DOCKER_IMAGE_TAG) .
	docker build -f base-admin-builder.Dockerfile -t temporalio/base-builder:$(DOCKER_IMAGE_TAG) .
	docker build -f base-admin-server.Dockerfile -t temporalio/base-server:$(DOCKER_IMAGE_TAG) .

	docker push temporalio/base-admin-tools:$(DOCKER_IMAGE_TAG)
	docker push temporalio/base-builder:$(DOCKER_IMAGE_TAG)
	docker push temporalio/base-server:$(DOCKER_IMAGE_TAG)
