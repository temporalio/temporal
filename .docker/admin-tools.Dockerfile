ARG BASE_ADMIN_TOOLS_IMAGE=temporalio/base-admin-tools:1.12.12

FROM ${BASE_ADMIN_TOOLS_IMAGE} as temporal-admin-tools
ARG TARGETARCH

# Essential binaries only
COPY ./.docker/dist/${TARGETARCH}/temporal /usr/local/bin/
COPY ./.docker/dist/${TARGETARCH}/tdbg /usr/local/bin/
COPY ./.docker/dist/${TARGETARCH}/temporal-cassandra-tool /usr/local/bin/
COPY ./.docker/dist/${TARGETARCH}/temporal-sql-tool /usr/local/bin/
COPY ./.docker/dist/${TARGETARCH}/temporal-elasticsearch-tool /usr/local/bin/
COPY ./schema /etc/temporal/schema

# Setup bash completion
RUN apk add bash-completion && \
    temporal completion bash > /etc/bash/temporal-completion.sh && \
    addgroup -g 1000 temporal && \
    adduser -u 1000 -G temporal -D temporal

USER temporal
WORKDIR /etc/temporal

ENTRYPOINT ["tini", "--", "sleep", "infinity"]
