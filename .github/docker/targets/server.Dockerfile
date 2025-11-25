ARG ALPINE_IMAGE=alpine:3.22@sha256:4b7ce07002c69e8f3d704a9c5d6fd3053be500b7f1c69fc0d80990c2ad8dd412

FROM ${ALPINE_IMAGE}

ARG TARGETARCH

RUN apk add --no-cache \
    ca-certificates \
    tzdata


RUN addgroup -g 1000 temporal && \
    adduser -u 1000 -G temporal -D temporal && \
    mkdir -p /etc/temporal/config/dynamicconfig && \
    touch /etc/temporal/config/dynamicconfig/docker.yaml && \
    chown -R temporal:temporal /etc/temporal/config

WORKDIR /etc/temporal

COPY --chmod=755 ./build/${TARGETARCH}/temporal-server /usr/local/bin/

USER temporal
ENTRYPOINT [ "temporal-server", "start" ]
