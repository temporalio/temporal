package interceptor

import (
	"context"
	"errors"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
)

type RetryDelayFloorInterceptor struct {
	namespaceRegistry namespace.Registry
	floor             dynamicconfig.DurationPropertyFnWithNamespaceFilter
	metricsHandler    metrics.Handler
	errs              []*serviceerror.ResourceExhausted
}

func NewRetryDelayFloorInterceptor(
	namespaceRegistry namespace.Registry,
	floor dynamicconfig.DurationPropertyFnWithNamespaceFilter,
	metricsHandler metrics.Handler,
	errs ...*serviceerror.ResourceExhausted,
) *RetryDelayFloorInterceptor {
	return &RetryDelayFloorInterceptor{
		namespaceRegistry: namespaceRegistry,
		floor:             floor,
		metricsHandler:    metricsHandler,
		errs:              errs,
	}
}

func (i *RetryDelayFloorInterceptor) Intercept(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	resp, err := handler(ctx, req)
	for _, sentinel := range i.errs {
		if errors.Is(err, sentinel) {
			return resp, withResourceExhaustedRetryDelayFloor(
				sentinel,
				MustGetNamespaceName(i.namespaceRegistry, req),
				i.floor,
				i.metricsHandler,
				api.MethodName(info.FullMethod),
			)
		}
	}
	return resp, err
}

func withResourceExhaustedRetryDelayFloor(
	base *serviceerror.ResourceExhausted,
	namespaceName namespace.Name,
	floorFn dynamicconfig.DurationPropertyFnWithNamespaceFilter,
	metricsHandler metrics.Handler,
	operation string,
) *serviceerror.ResourceExhausted {
	if base == nil || floorFn == nil {
		return base
	}

	delay := floorFn(namespaceName.String())
	if delay <= 0 {
		return base
	}

	// Append RetryInfo last so clients that only decode details[0] remain unaffected.
	st, err := base.Status().WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(delay)})
	if err != nil {
		return base
	}
	withDelay, ok := serviceerror.FromStatus(st).(*serviceerror.ResourceExhausted)
	if !ok {
		return base
	}
	metrics.ResourceExhaustedRetryDelayFloorEmitted.With(metricsHandler).Record(
		1,
		metrics.NamespaceTag(namespaceName.String()),
		metrics.OperationTag(operation),
	)
	return withDelay
}
