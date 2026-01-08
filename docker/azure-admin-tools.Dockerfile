# Azure Pipeline Dockerfile for Temporal Admin Tools
# Multi-stage build: compiles Go binaries and creates final image

# Global build arguments (must be before any FROM)
ARG ALPINE_TAG=3.23.2

# Stage 1: Build all admin tool binaries
FROM golang:1.24-alpine AS builder

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

# Build all admin tool binaries for the target architecture
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -o /temporal-cassandra-tool ./cmd/tools/cassandra
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -o /temporal-sql-tool ./cmd/tools/sql
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -o /temporal-elasticsearch-tool ./cmd/tools/elasticsearch
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -o /tdbg ./cmd/tools/tdbg

# Stage 2: Create the final image
ARG ALPINE_TAG
FROM alpine:${ALPINE_TAG}

RUN apk add --no-cache \
    ca-certificates \
    tzdata && \
    addgroup -g 1000 temporal && \
    adduser -u 1000 -G temporal -D temporal

# Copy all admin tool binaries from builder
COPY --chmod=755 --from=builder /temporal-cassandra-tool /usr/local/bin/temporal-cassandra-tool
COPY --chmod=755 --from=builder /temporal-sql-tool /usr/local/bin/temporal-sql-tool
COPY --chmod=755 --from=builder /temporal-elasticsearch-tool /usr/local/bin/temporal-elasticsearch-tool
COPY --chmod=755 --from=builder /tdbg /usr/local/bin/tdbg

# Copy schema files
COPY schema /etc/temporal/schema

USER temporal

CMD ["sh", "-c", "trap exit INT HUP TERM; sleep infinity"]
