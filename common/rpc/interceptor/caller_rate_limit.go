package interceptor

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/namespace"
)

var ErrCallerRateLimitExceeded = &serviceerror.ResourceExhausted{
	Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_CALLER_RPS_LIMIT,
	Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
	Message: "caller rate limit exceeded",
}

type CallerRateLimitInterceptor interface {
	Allow(handlerNamespace *namespace.Namespace, apiName string, headerGetter headers.HeaderGetter) error
}

type noopCallerRateLimitInterceptor struct{}

func NewNoopCallerRateLimitInterceptor() CallerRateLimitInterceptor {
	return noopCallerRateLimitInterceptor{}
}

func (noopCallerRateLimitInterceptor) Allow(*namespace.Namespace, string, headers.HeaderGetter) error {
	return nil
}
