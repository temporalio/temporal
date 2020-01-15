FROM golang:1.13.3-alpine

RUN apk add --update --no-cache ca-certificates make curl git mercurial bzr openssh protobuf

RUN git config --global url."git@github.com:".insteadOf "https://github.com/"

# Making sure that dependency is not touched
ENV GOFLAGS="-mod=readonly"

# https://github.com/docker-library/golang/blob/c1baf037d71331eb0b8d4c70cff4c29cf124c5e0/1.4/Dockerfile
RUN mkdir -p /temporal
WORKDIR /temporal
