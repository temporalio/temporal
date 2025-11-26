
ARG DOCKERIZE_VERSION=v0.9.2
FROM golang:1.25-alpine sha256:154a3d4ba5a80316771e042c706e5e0f84e01e80d3ccba6bc7df6ea93702e93c AS builder
# Using this method to task advantage of some of the supply chain protections go provides
# https://go.dev/blog/supply-chain
RUN go install github.com/jwilder/dockerize@${DOCKERIZE_VERSION}
RUN cp $(which dockerize) /usr/local/bin/dockerize

ARG ALPINE_IMAGE
FROM ${ALPINE_IMAGE}
ARG TARGETARCH
ARG TEMPORAL_SHA=unknown
ARG TEMPORAL_VERSION=dev




# Install runtime dependencies and dockerize
RUN apk update --no-cache \
    && apk add --no-cache wget openssl ca-certificates tzdata bash curl 

# Use bash for subsequent RUN commands
SHELL ["/bin/bash", "-c"]

# Setup temporal user and directories
RUN addgroup -g 1000 temporal && \
    adduser -u 1000 -G temporal -D temporal && \
    mkdir -p /etc/temporal/config/dynamicconfig && \
    touch /etc/temporal/config/dynamicconfig/docker.yaml && \
    chown -R temporal:temporal /etc/temporal/config

WORKDIR /etc/temporal

ENV TEMPORAL_HOME=/etc/temporal
ENV TEMPORAL_SHA=${TEMPORAL_SHA}

EXPOSE 6933 6934 6935 6939 7233 7234 7235 7239

# Copy binaries
COPY --chmod=755 ./build/${TARGETARCH}/tctl /usr/local/bin/
COPY --chmod=755 ./build/${TARGETARCH}/tctl-authorization-plugin /usr/local/bin/
COPY --chmod=755 ./build/${TARGETARCH}/temporal-server /usr/local/bin/
COPY --chmod=755 ./build/${TARGETARCH}/temporal /usr/local/bin/
COPY --from=builder /usr/local/bin/dockerize /usr/local/bin
# Copy configs
COPY ./build/config_template.yaml /etc/temporal/config/config_template.yaml

# Copy scripts
COPY --chmod=755 ./scripts/bash/entrypoint.sh /etc/temporal/entrypoint.sh
COPY --chmod=755 ./scripts/bash/start-temporal.sh /etc/temporal/start-temporal.sh

USER temporal

ENTRYPOINT ["/etc/temporal/entrypoint.sh"]
