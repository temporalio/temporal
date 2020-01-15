FROM golang:1.13.3-alpine

RUN apk add --update --no-cache ca-certificates make curl git mercurial bzr openssh protobuf

# Making sure that dependency is not touched
ENV GOFLAGS="-mod=readonly"

ENV SSH_AUTH_SOCK=/ssh-agent
ENV GOPRIVATE=go.temporal.io/temporal

WORKDIR /temporal