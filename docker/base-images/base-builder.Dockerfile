FROM golang:1.16-alpine AS base-builder

RUN apk add --update --no-cache \
    make \
    git \
    protobuf \
    build-base

RUN wget -O- https://raw.githubusercontent.com/fossas/fossa-cli/master/install.sh | sh

