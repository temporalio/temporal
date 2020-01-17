package history

import (
	"context"
	"time"
)

//var (
//	// call header to cadence server
//	headers = metadata.New(map[string]string{
//		common.LibraryVersionHeaderName: "1.0.0",
//		common.FeatureVersionHeaderName: client.GoWorkerConsistentQueryVersion,
//		common.ClientImplHeaderName:     client.GoSDK,
//		// TODO: remove these headers when server is vanilla gRPC (not YARPC)
//		"rpc-caller":   "temporal-history",
//		"rpc-service":  "cadence-frontend",
//		"rpc-encoding": "proto",
//	})
//)

func createContextWithCancel(timeout time.Duration) (context.Context, context.CancelFunc) {
	//	ctx := metadata.NewOutgoingContext(context.Background(), headers)
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, timeout)

	return ctx, cancel
}
