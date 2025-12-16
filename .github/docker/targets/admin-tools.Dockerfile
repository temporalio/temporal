FROM alpine:3.23@sha256:1ff80fba9ebbc89b4e7c0e01f6c5304d39bdf29efd37cfec3f99c11dc66f81f3

ARG TARGETARCH

RUN apk add --no-cache \
    ca-certificates \
    tzdata && addgroup -g 1000 temporal && \
    adduser -u 1000 -G temporal -D temporal

# Copy all admin tool binaries:
# - temporal (CLI)
# - temporal-server
# - temporal-cassandra-tool
# - temporal-sql-tool
# - temporal-elasticsearch-tool
# - tdbg
COPY --chmod=755 ./build/${TARGETARCH}/* /usr/local/bin/

COPY ./build/temporal/schema /etc/temporal/schema

USER temporal

CMD ["sh", "-c", "trap exit INT HUP TERM; sleep infinity"]
