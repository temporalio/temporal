# IMPORTANT: When updating ALPINE_TAG, also update the default value in:
# - docker/docker-bake.hcl (variable "ALPINE_TAG")
# - docker/targets/server.Dockerfile (ARG ALPINE_TAG)
# NOTE: We use just the tag without a digest pin because digest-pinned manifest lists
# cause platform resolution issues in multi-arch buildx builds (InvalidBaseImagePlatform warnings).
ARG ALPINE_TAG=3.23

FROM --platform=$TARGETPLATFORM alpine:${ALPINE_TAG}

ARG TARGETARCH

RUN apk add --no-cache \
    ca-certificates \
    tzdata && addgroup -g 1000 temporal && \
    adduser -u 1000 -G temporal -D temporal

# Copy all admin tool binaries:
# - temporal (CLI)
# - temporal-cassandra-tool
# - temporal-sql-tool
# - temporal-elasticsearch-tool
# - tdbg
COPY --chmod=755 \
    ./build/${TARGETARCH}/temporal \
    ./build/${TARGETARCH}/temporal-cassandra-tool \
    ./build/${TARGETARCH}/temporal-sql-tool \
    ./build/${TARGETARCH}/temporal-elasticsearch-tool \
    ./build/${TARGETARCH}/tdbg \
    /usr/local/bin/

COPY ./build/temporal/schema /etc/temporal/schema

USER temporal

CMD ["sh", "-c", "trap exit INT HUP TERM; sleep infinity"]
