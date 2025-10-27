ARG BASE_IMAGE=alpine:3.22

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
