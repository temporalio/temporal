# Build Temporal binaries
FROM golang:1.15-alpine AS base-builder

RUN apk add --update --no-cache \
    make \
    git

# Making sure that dependency is not touched
ENV GOFLAGS="-mod=readonly"

WORKDIR /temporal

# Copy go mod dependencies first and build docker cache
COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 make bins
