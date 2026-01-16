# Temporal Server with MongoDB Plugin
# Multi-stage build for optimized image size

# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /temporal

# Install build dependencies
RUN apk add --no-cache git make

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the server
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o temporal-server ./cmd/server

# Build tools
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o temporal-cassandra-tool ./cmd/tools/cassandra
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o temporal-sql-tool ./cmd/tools/sql

# Runtime stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata bash curl

WORKDIR /etc/temporal

# Copy binaries from builder
COPY --from=builder /temporal/temporal-server /usr/local/bin/
COPY --from=builder /temporal/temporal-cassandra-tool /usr/local/bin/
COPY --from=builder /temporal/temporal-sql-tool /usr/local/bin/

# Copy config files
COPY config /etc/temporal/config

# Expose ports
# 7233 - Frontend gRPC
# 7234 - History gRPC
# 7235 - Matching gRPC
# 7236 - Worker gRPC
EXPOSE 7233 7234 7235 7236

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD curl -f http://localhost:7233/health || exit 1

# Default command
ENTRYPOINT ["temporal-server"]
CMD ["start"]
