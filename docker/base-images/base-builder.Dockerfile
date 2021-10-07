# alpine3.14 requires docker 20.10: https://gitlab.alpinelinux.org/alpine/aports/-/issues/12396
FROM golang:1.17-alpine3.14 AS base-builder

#RUN apk -U upgrade
RUN apk add --update --no-cache \
    make \
    git \
    git-lfs
