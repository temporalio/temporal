##### dockerize builder: built from source to support arm & x86 #####
# alpine3.14 requires docker 20.10: https://gitlab.alpinelinux.org/alpine/aports/-/issues/12396
FROM golang:1.17.3-alpine3.13 AS dockerize-builder

RUN apk add --update --no-cache \
    git

RUN mkdir -p /xsrc && \
    git clone https://github.com/jwilder/dockerize.git && \
    cd dockerize && \
    go mod init github.com/jwilder/dockerize && \
    go mod tidy && \
    go build -o /usr/local/bin/dockerize . && \
    rm -rf /xsrc

##### base-server target #####
# alpine3.14 requires docker 20.10: https://gitlab.alpinelinux.org/alpine/aports/-/issues/12396
FROM alpine:3.13 AS base-server

RUN apk add --update --no-cache \
    ca-certificates \
    tzdata \
    bash \
    curl \
    vim

COPY --from=dockerize-builder /usr/local/bin/dockerize /usr/local/bin/dockerize
# set up nsswitch.conf for Go's "netgo" implementation
# https://github.com/gliderlabs/docker-alpine/issues/367#issuecomment-424546457
RUN test ! -e /etc/nsswitch.conf && echo 'hosts: files dns' > /etc/nsswitch.conf

SHELL ["/bin/bash", "-c"]
