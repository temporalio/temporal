# Build Stage
FROM golang:1.21-alpine AS builder

# Set the Current Working Directory inside the container
WORKDIR /app

# Copy go.mod and go.sum files.
# These files are expected to be at the root of the build context (samples/go/payment-retry/)
# and will be copied into /app/ in the container.
COPY go.mod go.sum ./

# Download dependencies.
# This step leverages Docker layer caching. If go.mod and go.sum haven't changed,
# this layer will be cached, speeding up subsequent builds.
RUN go mod download

# Copy the rest of the application's source code from the build context.
# The build context will be samples/go/payment-retry/, so this copies everything
# from samples/go/payment-retry/ into /app/ in the container.
COPY . .

# Build the Go application
# CGO_ENABLED=0 disables Cgo, producing a statically linked binary.
# GOOS=linux specifies the target operating system for the build.
# -v for verbose output.
# -o /app/worker specifies the output file path for the compiled binary.
# ./worker/main.go is the path to the main package of the worker application,
# relative to the WORKDIR /app.
RUN CGO_ENABLED=0 GOOS=linux go build -v -o /app/worker ./worker/main.go

# Runtime Stage
FROM alpine:latest

# Set the Current Working Directory inside the container
WORKDIR /app

# Copy the built executable from the builder stage
COPY --from=builder /app/worker /app/worker

# Expose port 9090 for Prometheus metrics
EXPOSE 9090

# Set the entrypoint for the container
ENTRYPOINT ["/app/worker"]
