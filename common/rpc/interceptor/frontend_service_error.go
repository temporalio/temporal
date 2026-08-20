package interceptor

import (
	"context"
	"errors"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/rpc/interceptor/nexus"
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

type FrontendServiceErrorInterceptor struct {
	logger log.Logger
}

// NewFrontendServiceErrorInterceptorWrapper returns interceptors that have two responsibilities:
//  1. Mask certain internal service error details.
//  2. Propagate resource exhaustion details via gRPC headers.
func NewFrontendServiceErrorInterceptorWrapper(logger log.Logger) *FrontendServiceErrorInterceptor {
	return &FrontendServiceErrorInterceptor{
		logger: logger,
	}
}

func NewFrontendServiceErrorInterceptor(logger log.Logger) grpc.UnaryServerInterceptor {
	t := NewFrontendServiceErrorInterceptorWrapper(logger)
	return t.Intercept
}

func (f *FrontendServiceErrorInterceptor) Intercept(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	resp, err := handler(ctx, req)

	return resp, f.transformError(ctx, info.FullMethod, err, true)
}

func (f *FrontendServiceErrorInterceptor) InterceptNexus(
	ctx context.Context,
	in nexus.InterceptorInput,
	next nexus.HandlerFunc,
) (any, error) {
	resp, err := next(ctx, in)
	if ie, ok := errors.AsType[*nexus.InterceptorError](err); ok {
		ie.Err = f.transformError(ctx, in.APIName(), ie.Err, false)
		return resp, ie
	}
	return resp, f.transformError(ctx, in.APIName(), err, false)
}

func (f *FrontendServiceErrorInterceptor) transformError(ctx context.Context, method string, err error, isGRPC bool) error {
	if err == nil {
		return nil
	}
	method = api.MethodName(method)

	switch serviceErr := err.(type) {
	case *serviceerrors.ShardOwnershipLost:
		err = serviceerror.NewUnavailable("shard unavailable, please backoff and retry")
	case *serviceerror.DataLoss:
		err = serviceerror.NewUnavailable("internal history service error")
	case *serviceerror.ResourceExhausted:
		if !isGRPC {
			break
		}
		if headerErr := grpc.SetHeader(ctx, metadata.Pairs(
			ResourceExhaustedCauseHeader, serviceErr.Cause.String(),
			ResourceExhaustedScopeHeader, serviceErr.Scope.String(),
		)); headerErr != nil {
			// So while this is *not* a user-facing error or problem in itself,
			// it indicates that there might be larger connection issues at play.
			f.logger.Error("Failed to add Resource-Exhausted headers to response",
				tag.Operation(method),
				tag.Error(headerErr))
		}
	default:
	}
	return err
}
