FROM golang:1.13.3-alpine

RUN apk add --update --no-cache ca-certificates make curl git mercurial bzr openssh protobuf

# make sure domain is accepted
RUN touch /root/.ssh/known_hosts
RUN ssh-keyscan github.com >> /root/.ssh/known_hosts

RUN git config --global url."git@github.com:".insteadOf "https://github.com/"

# Making sure that dependency is not touched
ENV GOFLAGS="-mod=readonly"

ENV SSH_AUTH_SOCK=/ssh-agent
ENV GOPRIVATE=go.temporal.io/temporal

WORKDIR /temporal