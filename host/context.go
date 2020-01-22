package host

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
		"rpc-caller":   "temporal-onebox",
		"rpc-service":  "cadence-frontend",
		"rpc-encoding": "proto",
	})
)

func createContext() context.Context {
	ctx, _ := createContextWithCancel(90 * time.Second)
	return ctx
}

func createContextWithCancel(timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx := metadata.NewOutgoingContext(context.Background(), headers)
	ctx, cancel := context.WithTimeout(ctx, timeout)

	return ctx, cancel
}
