FROM golang:1.16-alpine3.14 AS base-ci-builder

RUN apk add --update --no-cache \
    make \
    git \
    protobuf \
    build-base \
    shellcheck

RUN wget -O- https://raw.githubusercontent.com/fossas/fossa-cli/master/install.sh | sh
