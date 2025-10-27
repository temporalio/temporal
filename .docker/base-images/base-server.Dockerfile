ARG BASE_IMAGE=alpine:3.22

FROM golang:1.24-alpine3.22 AS builder

ARG DOCKERIZE_VERSION=v0.9.2
RUN go install github.com/jwilder/dockerize@${DOCKERIZE_VERSION}
RUN cp $(which dockerize) /usr/local/bin/dockerize

##### base-server target #####
FROM ${BASE_IMAGE} AS base-server

RUN apk upgrade --no-cache
RUN apk add --no-cache \
    ca-certificates \
    tzdata \
    bash \
    curl

COPY --from=builder /usr/local/bin/dockerize /usr/local/bin

SHELL ["/bin/bash", "-c"]
