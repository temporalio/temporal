FROM golang:1.16-alpine AS base-builder

RUN apk add --update --no-cache \
    make \
    git \
    protobuf
