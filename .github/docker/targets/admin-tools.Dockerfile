FROM alpine:3.22@sha256:4b7ce07002c69e8f3d704a9c5d6fd3053be500b7f1c69fc0d80990c2ad8dd412

ARG TARGETARCH

RUN apk add --no-cache \
    ca-certificates \
    tzdata && addgroup -g 1000 temporal && \
    adduser -u 1000 -G temporal -D temporal

COPY --chmod=755 ./build/${TARGETARCH}/temporal /usr/local/bin/
COPY --chmod=755 ./build/${TARGETARCH}/tdbg /usr/local/bin/
COPY --chmod=755 ./build/${TARGETARCH}/temporal-cassandra-tool /usr/local/bin/
COPY --chmod=755 ./build/${TARGETARCH}/temporal-sql-tool /usr/local/bin/
COPY --chmod=755 ./build/${TARGETARCH}/temporal-elasticsearch-tool /usr/local/bin/

COPY ./build/temporal/schema /etc/temporal/schema

USER temporal

CMD ["sh", "-c", "trap exit INT HUP TERM; sleep infinity"]
