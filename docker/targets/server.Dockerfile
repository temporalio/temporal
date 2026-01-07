# IMPORTANT: When updating ALPINE_TAG, also update the default value in:
# - docker/docker-bake.hcl (variable "ALPINE_TAG")
# - docker/targets/admin-tools.Dockerfile (ARG ALPINE_TAG)
# NOTE: We use just the tag without a digest pin because digest-pinned manifest lists
# cause platform resolution issues in multi-arch buildx builds (InvalidBaseImagePlatform warnings).
ARG ALPINE_TAG=3.23

FROM --platform=$TARGETPLATFORM alpine:${ALPINE_TAG}

ARG TARGETARCH

RUN apk add --no-cache \
    ca-certificates \
    tzdata && addgroup -g 1000 temporal && \
    adduser -u 1000 -G temporal -D temporal

COPY --chmod=755 ./build/${TARGETARCH}/temporal-server /usr/local/bin/
COPY --chmod=755 ./scripts/sh/entrypoint.sh /etc/temporal/entrypoint.sh

WORKDIR /etc/temporal
USER temporal

CMD [ "/etc/temporal/entrypoint.sh" ]
