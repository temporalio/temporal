package interceptor

import (
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/types/known/durationpb"
)

// LimitInterceptorOption configures a namespace or concurrent request limit interceptor.
type LimitInterceptorOption func(*limitInterceptorOptions)

type limitInterceptorOptions struct {
	retryDelayFloor dynamicconfig.DurationPropertyFnWithNamespaceFilter
}

// WithResourceExhaustedRetryDelayFloor configures the minimum retry delay attached to frontend limiter rejections.
func WithResourceExhaustedRetryDelayFloor(
	floor dynamicconfig.DurationPropertyFnWithNamespaceFilter,
) LimitInterceptorOption {
	return func(options *limitInterceptorOptions) {
		options.retryDelayFloor = floor
	}
}

func newLimitInterceptorOptions(options ...LimitInterceptorOption) limitInterceptorOptions {
	var interceptorOptions limitInterceptorOptions
	for _, option := range options {
		option(&interceptorOptions)
	}
	return interceptorOptions
}

// withResourceExhaustedRetryDelayFloor attaches the floor on the frontend gRPC interceptor path. History and matching
// providers pass no options, and Nexus handlers that call Allow directly do not attach a retry hint.
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

	// TODO: We might need a upper limit for the floor. It's possible for misconfigured dynamic config to set a floor that is too high (e.g. 5m instead of 5s).
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
