# Alpine base image
FROM alpine:3.12 AS alpine

RUN apk add --update --no-cache \
    ca-certificates \
    tzdata \
    bash \
    curl \
    vim

# set up nsswitch.conf for Go's "netgo" implementation
# https://github.com/gliderlabs/docker-alpine/issues/367#issuecomment-424546457
RUN test ! -e /etc/nsswitch.conf && echo 'hosts: files dns' > /etc/nsswitch.conf

SHELL ["/bin/bash", "-c"]

# All temporal tool binaries
FROM alpine AS base-admin-tools

RUN apk add --update --no-cache \
    jq \
    mysql-client \
    postgresql-client \
    python2 \
    && curl https://bootstrap.pypa.io/2.7/get-pip.py | python \
    && pip install cqlsh
