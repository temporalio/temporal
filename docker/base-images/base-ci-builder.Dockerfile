FROM golang:1.17-alpine AS base-ci-builder

RUN apk add --update --no-cache \
    make \
    git \
    protobuf \
    build-base \
    shellcheck

RUN wget -O- https://raw.githubusercontent.com/fossas/spectrometer/master/install.sh | sh
