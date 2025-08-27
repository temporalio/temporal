package interceptor

import (
	"context"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	// ResourceExhaustedCauseHeader is added to rpc response if request returns ResourceExhausted error.
	ResourceExhaustedCauseHeader = "X-Resource-Exhausted-Cause"

	// ResourceExhaustedScopeHeader is added to rpc response if request returns ResourceExhausted error.
	ResourceExhaustedScopeHeader = "X-Resource-Exhausted-Scope"
)

// NewFrontendServiceErrorInterceptor returns a gRPC interceptor that has two responsibilities:
//  1. Mask certain internal service error details.
//  2. Propagate resource exhaustion details via gRPC headers.
func NewFrontendServiceErrorInterceptor(
	logger log.Logger,
) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		resp, err := handler(ctx, req)
		if err == nil {
			return resp, nil
		}

		switch serviceErr := err.(type) {
		case *serviceerrors.ShardOwnershipLost:
			err = serviceerror.NewUnavailable("shard unavailable, please backoff and retry")
		case *serviceerror.DataLoss:
			err = serviceerror.NewUnavailable("internal history service error")
		case *serviceerror.ResourceExhausted:
			if headerErr := grpc.SetHeader(ctx, metadata.Pairs(
				ResourceExhaustedCauseHeader, serviceErr.Cause.String(),
				ResourceExhaustedScopeHeader, serviceErr.Scope.String(),
			)); headerErr != nil {
				// So while this is *not* a user-facing error or problem in itself,
				// it indicates that there might be larger connection issues at play.
				logger.Error("Failed to add Resource-Exhausted headers to response",
					tag.Operation(api.MethodName(info.FullMethod)),
					tag.Error(headerErr))
			}
		}

		return resp, err
	}
}
