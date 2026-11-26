ARG ALPINE_TAG

FROM alpine:${ALPINE_TAG}

ARG TARGETARCH

RUN apk add --no-cache \
    ca-certificates \
    tzdata && \
    apk upgrade --no-cache zlib && \
    addgroup -g 1000 temporal && \
    adduser -u 1000 -G temporal -D temporal

COPY --chmod=755 ./build/${TARGETARCH}/temporal-server /usr/local/bin/
COPY --chmod=755 ./scripts/sh/entrypoint.sh /etc/temporal/entrypoint.sh

WORKDIR /etc/temporal
USER temporal

HEALTHCHECK --interval=10s --timeout=5s --start-period=30s \
    CMD ["/etc/temporal/entrypoint.sh", "healthcheck"]

CMD [ "/etc/temporal/entrypoint.sh" ]
