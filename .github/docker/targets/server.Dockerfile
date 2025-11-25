ARG ALPINE_IMAGE

FROM ${ALPINE_IMAGE}

ARG TARGETARCH

RUN apk add --no-cache \
    ca-certificates \
    tzdata && addgroup -g 1000 temporal && \
    adduser -u 1000 -G temporal -D temporal

COPY --chmod=755 ./build/${TARGETARCH}/temporal-server /usr/local/bin/

WORKDIR /etc/temporal
USER temporal

ENTRYPOINT [ "temporal-server", "start" ]
