ARG ALPINE_IMAGE

FROM ${ALPINE_IMAGE} AS temporal-admin-tools

ARG TARGETARCH

RUN apk add --no-cache \
    ca-certificates \
    tzdata \
    tini

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
