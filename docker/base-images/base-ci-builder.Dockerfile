# alpine3.14 doesn work due to https://gitlab.alpinelinux.org/alpine/aports/-/issues/12396
# Buildkite elastic stack needs to be upgraded to the version which has docker 20.10.0 (at least).
FROM golang:1.17-alpine3.13 AS base-ci-builder

RUN apk add --update --no-cache \
    make \
    git \
    protobuf \
    build-base \
    shellcheck

RUN wget -O- https://raw.githubusercontent.com/fossas/spectrometer/master/install.sh | sh
