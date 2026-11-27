package main

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestCheckServing(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("ready", healthpb.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus("starting", healthpb.HealthCheckResponse_NOT_SERVING)
	server := grpc.NewServer()
	healthpb.RegisterHealthServer(server, healthServer)
	t.Cleanup(func() { _ = listener.Close() })
	t.Cleanup(server.Stop)
	go func() { _ = server.Serve(listener) }()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	address := listener.Addr().String()
	require.NoError(t, checkServing(ctx, address, "ready", insecure.NewCredentials()))
	require.ErrorContains(t, checkServing(ctx, address, "starting", insecure.NewCredentials()), "NOT_SERVING")
	require.ErrorContains(t, checkServing(ctx, address, "missing", insecure.NewCredentials()), "unknown service")

	server.Stop()
	require.Error(t, checkServing(ctx, address, "ready", insecure.NewCredentials()))
}
