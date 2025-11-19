ARG ALPINE_IMAGE=alpine:3.22@sha256:4b7ce07002c69e8f3d704a9c5d6fd3053be500b7f1c69fc0d80990c2ad8dd412

FROM ${ALPINE_IMAGE}
ARG TARGETARCH
ARG TEMPORAL_SHA=unknown
ARG DOCKERIZE_VERSION=v0.9.2
ARG TEMPORAL_VERSION=dev

# Install runtime dependencies and dockerize
RUN apk update --no-cache \
    && apk add --no-cache wget openssl ca-certificates tzdata bash curl \
    && set -eux; \
    case "${TARGETARCH}" in \
    amd64) DOCKERIZE_PLATFORM="alpine-linux"; DOCKERIZE_ARCH="amd64" ;; \
    arm64) DOCKERIZE_PLATFORM="linux"; DOCKERIZE_ARCH="arm64" ;; \
    arm) DOCKERIZE_PLATFORM="linux"; DOCKERIZE_ARCH="armhf" ;; \
    *) echo "unsupported TARGETARCH ${TARGETARCH}"; exit 1 ;; \
    esac; \
    wget -O - "https://github.com/jwilder/dockerize/releases/download/${DOCKERIZE_VERSION}/dockerize-${DOCKERIZE_PLATFORM}-${DOCKERIZE_ARCH}-${DOCKERIZE_VERSION}.tar.gz" | tar xzf - -C /usr/local/bin \
    && chmod +x /usr/local/bin/dockerize \
    && apk del wget

# Setup temporal user and directories
RUN addgroup -g 1000 temporal && \
    adduser -u 1000 -G temporal -D temporal && \
    mkdir -p /etc/temporal/config && \
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

# Copy configs
COPY ./build/config_docker.yaml /etc/temporal/config/dynamicconfig/docker.yaml
COPY ./build/config_template.yaml /etc/temporal/config/config_template.yaml

# Copy scripts
COPY --chmod=755 ./scripts/entrypoint.sh /etc/temporal/entrypoint.sh
COPY --chmod=755 ./scripts/start-temporal.sh /etc/temporal/start-temporal.sh

USER temporal

ENTRYPOINT ["/etc/temporal/entrypoint.sh"]
