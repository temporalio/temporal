package interceptor

import (
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/namespace"
)

type CallerRateLimitInterceptor interface {
	Allow(handlerNamespace namespace.Name, apiName string, headerGetter headers.HeaderGetter) error
}

type noopCallerRateLimitInterceptor struct{}

func NewNoopCallerRateLimitInterceptor() CallerRateLimitInterceptor {
	return noopCallerRateLimitInterceptor{}
}

func (noopCallerRateLimitInterceptor) Allow(namespace.Name, string, headers.HeaderGetter) error {
	return nil
}
