ARG BASE_IMAGE=alpine:3.22@sha256:beefdbd8a1da6d2915566fde36db9db0b524eb737fc57cd1367effd16dc0d06d

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
COPY ./build/temporal/schema /etc/temporal/schema

RUN addgroup -g 1000 temporal && \
    adduser -u 1000 -G temporal -D temporal

USER temporal
WORKDIR /etc/temporal

ENTRYPOINT ["tini", "--", "sleep", "infinity"]
