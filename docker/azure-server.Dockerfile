# Azure Pipeline Dockerfile for Temporal Server
# Multi-stage build: compiles Go binaries and creates final image

# Global build arguments (must be before any FROM)
ARG ALPINE_TAG=3.23.2

# Stage 1: Build the server binary
FROM golang:1.25-alpine AS builder

RUN apk add --no-cache \
    make \
    git \
    curl \
    ca-certificates

WORKDIR /temporal

# Copy go mod files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source
COPY . ./

# Build temporal-server for the target architecture
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -o /temporal-server ./cmd/server

# Stage 2: Create the final image
ARG ALPINE_TAG
FROM alpine:${ALPINE_TAG}

RUN apk add --no-cache \
    ca-certificates \
    tzdata && \
    addgroup -g 1000 temporal && \
    adduser -u 1000 -G temporal -D temporal

# Copy server binary from builder
COPY --chmod=755 --from=builder /temporal-server /usr/local/bin/temporal-server

# Copy entrypoint script
COPY --chmod=755 docker/scripts/sh/entrypoint.sh /etc/temporal/entrypoint.sh

# Copy config template
COPY docker/config_template.yaml /etc/temporal/config/config_template.yaml

WORKDIR /etc/temporal
USER temporal

EXPOSE 7233 7234 7235 7239

CMD [ "/etc/temporal/entrypoint.sh" ]
