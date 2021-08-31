FROM golang:1.17-alpine AS base-builder

RUN apk add --update --no-cache \
    make \
    git
