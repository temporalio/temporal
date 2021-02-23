FROM golang:1.15-alpine AS temporalio/builder:1.0.0

RUN apk add --update --no-cache \
    make \
    git
