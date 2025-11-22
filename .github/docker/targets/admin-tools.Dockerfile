ARG ALPINE_IMAGE=alpine:3.22@sha256:4b7ce07002c69e8f3d704a9c5d6fd3053be500b7f1c69fc0d80990c2ad8dd412

FROM ${BASE_IMAGE} AS temporal-admin-tools

ARG TARGETARCH
ARG TEMPORAL_VERSION
ARG TEMPORAL_SHA
ARG CLI_VERSION

RUN apk add --no-cache \
    ca-certificates \
    tzdata \
    tini

# Copy binaries
COPY --chmod=755 ./build/${TARGETARCH}/temporal /usr/local/bin/
COPY --chmod=755 ./build/${TARGETARCH}/tdbg /usr/local/bin/
COPY --chmod=755 ./build/${TARGETARCH}/temporal-cassandra-tool /usr/local/bin/
COPY --chmod=755 ./build/${TARGETARCH}/temporal-sql-tool /usr/local/bin/
COPY ./build/${TARGETARCH}/ /tmp/binaries/
RUN if [ -f /tmp/binaries/temporal-elasticsearch-tool ]; then \
    cp /tmp/binaries/temporal-elasticsearch-tool /usr/local/bin/ && \
    chmod 755 /usr/local/bin/temporal-elasticsearch-tool; \
    fi && \
    rm -rf /tmp/binaries
COPY ./build/temporal/schema /etc/temporal/schema

RUN addgroup -g 1000 temporal && \
    adduser -u 1000 -G temporal -D temporal

USER temporal
WORKDIR /etc/temporal

ENTRYPOINT ["tini", "--", "sleep", "infinity"]
