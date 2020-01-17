package replicator

import (
	"context"
	"time"

	"google.golang.org/grpc/metadata"

	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/client"
)

var (
	// call header to cadence server
	headers = metadata.New(map[string]string{
		common.LibraryVersionHeaderName: "1.0.0",
		common.FeatureVersionHeaderName: client.GoWorkerConsistentQueryVersion,
		common.ClientImplHeaderName:     client.GoSDK,
		// TODO: remove these headers when server is vanilla gRPC (not YARPC)
		"rpc-caller":   "temporal-replicator",
		"rpc-service":  "cadence-frontend",
		"rpc-encoding": "proto",
	})
)

func createContextWithCancel(timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx := metadata.NewOutgoingContext(context.Background(), headers)
	ctx, cancel := context.WithTimeout(ctx, timeout)

	return ctx, cancel
}
