FROM golang:1.13.3-alpine

RUN apk add --update --no-cache ca-certificates make curl git mercurial bzr openssh protobuf

RUN git config --global url."git@github.com:".insteadOf "https://github.com/"

# Making sure that dependency is not touched
ENV GOFLAGS="-mod=readonly"

WORKDIR /temporal

# Copy go mod dependencies and build cache
COPY go.mod ./
COPY go.sum ./

RUN go mod download

COPY . .
