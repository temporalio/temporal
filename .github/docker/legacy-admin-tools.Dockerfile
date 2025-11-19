ARG BASE_IMAGE=alpine:3.22@sha256:beefdbd8a1da6d2915566fde36db9db0b524eb737fc57cd1367effd16dc0d06d

FROM ${BASE_IMAGE} AS builder

# These are necessary to install cqlsh
RUN apk add --update --no-cache \
    python3-dev \
    musl-dev \
    libev-dev \
    gcc \
    pipx

RUN pipx install --global cqlsh

FROM ${BASE_IMAGE} AS base-admin-tools

RUN apk upgrade --no-cache
RUN apk add --no-cache \
    python3 \
    libev \
    ca-certificates \
    tzdata \
    bash \
    curl \
    jq \
    yq \
    mysql-client \
    postgresql-client \
    expat \
    tini

COPY --from=builder /opt/pipx/venvs/cqlsh /opt/pipx/venvs/cqlsh
RUN ln -s /opt/pipx/venvs/cqlsh/bin/cqlsh /usr/local/bin/cqlsh

# validate cqlsh installation
RUN cqlsh --version

SHELL ["/bin/bash", "-c"]

##### Temporal admin tools #####
FROM base-admin-tools as temporal-admin-tools
ARG TARGETARCH
ARG TEMPORAL_VERSION
ARG TEMPORAL_SHA
ARG CLI_VERSION
ARG TCTL_VERSION

COPY ./build/${TARGETARCH}/tctl /usr/local/bin
COPY ./build/${TARGETARCH}/tctl-authorization-plugin /usr/local/bin
COPY ./build/${TARGETARCH}/temporal /usr/local/bin
COPY ./build/${TARGETARCH}/temporal-cassandra-tool /usr/local/bin
COPY ./build/${TARGETARCH}/temporal-sql-tool /usr/local/bin
COPY ./build/${TARGETARCH}/tdbg /usr/local/bin
COPY ./build/temporal/schema /etc/temporal/schema

# Alpine has a /etc/bash/bashrc that sources all files named /etc/bash/*.sh for
# interactive shells, so we can add completion logic in /etc/bash/temporal-completion.sh
# Completion for temporal depends on the bash-completion package.
RUN apk add bash-completion && \
    temporal completion bash > /etc/bash/temporal-completion.sh && \
    addgroup -g 1000 temporal && \
    adduser -u 1000 -G temporal -D temporal
USER temporal
WORKDIR /etc/temporal

# Keep the container running.
ENTRYPOINT ["tini", "--", "sleep", "infinity"]
