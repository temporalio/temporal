# alpine3.14 requires docker 20.10: https://gitlab.alpinelinux.org/alpine/aports/-/issues/12396
FROM golang:1.17.3-alpine3.13 AS base-builder

RUN apk add --update --no-cache \
    make \
    git
