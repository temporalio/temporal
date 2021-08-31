FROM alpine:3.14 AS base-admin-tools

RUN apk add --update --no-cache \
    ca-certificates \
    tzdata \
    bash \
    curl \
    vim \
    jq \
    mysql-client \
    postgresql-client \
    python2 \
    && curl https://bootstrap.pypa.io/pip/2.7/get-pip.py | python \
    && pip install cqlsh

# set up nsswitch.conf for Go's "netgo" implementation
# https://github.com/gliderlabs/docker-alpine/issues/367#issuecomment-424546457
RUN test ! -e /etc/nsswitch.conf && echo 'hosts: files dns' > /etc/nsswitch.conf

SHELL ["/bin/bash", "-c"]
