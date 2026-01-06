# IMPORTANT: When updating ALPINE_TAG, also update the default value in:
# - docker/docker-bake.hcl (variable "ALPINE_TAG")
# - docker/targets/server.Dockerfile (ARG ALPINE_TAG)
ARG ALPINE_TAG=3.23.2@sha256:865b95f46d98cf867a156fe4a135ad3fe50d2056aa3f25ed31662dff6da4eb62

FROM alpine:${ALPINE_TAG}

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
