package interceptor

import (
	"context"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/service/frontend/configs"
	"google.golang.org/grpc"
)

const (
	NamespaceRateLimitDefaultToken = 1
)

var (
	ErrNamespaceRateLimitServerBusy = &serviceerror.ResourceExhausted{
		Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
		Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
		Message: "namespace rate limit exceeded",
	}
)

type (
	NamespaceRateLimitInterceptor interface {
		Intercept(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error)
		Allow(namespaceName namespace.Name, methodName string, headerGetter headers.HeaderGetter) error
	}

	NamespaceRateLimitInterceptorImpl struct {
		namespaceRegistry                 namespace.Registry
		rateLimiter                       quotas.RequestRateLimiter
		tokens                            map[string]int
		reducePollWorkflowHistoryPriority dynamicconfig.BoolPropertyFn
	}
)

var _ grpc.UnaryServerInterceptor = (*NamespaceRateLimitInterceptorImpl)(nil).Intercept
var _ NamespaceRateLimitInterceptor = (*NamespaceRateLimitInterceptorImpl)(nil)

func NewNamespaceRateLimitInterceptor(
	namespaceRegistry namespace.Registry,
	rateLimiter quotas.RequestRateLimiter,
	tokens map[string]int,
) NamespaceRateLimitInterceptor {
	return &NamespaceRateLimitInterceptorImpl{
		namespaceRegistry: namespaceRegistry,
		rateLimiter:       rateLimiter,
		tokens:            tokens,
	}
}

func (ni *NamespaceRateLimitInterceptorImpl) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	if ns := MustGetNamespaceName(ni.namespaceRegistry, req); ns != namespace.EmptyName {
		method := info.FullMethod
		if IsLongPollGetHistoryRequest(req) {
			method = configs.PollWorkflowHistoryAPIName
		}
		if err := ni.Allow(ns, method, headers.NewGRPCHeaderGetter(ctx)); err != nil {
			return nil, err
		}
	}

	return handler(ctx, req)
}

func (ni *NamespaceRateLimitInterceptorImpl) Allow(namespaceName namespace.Name, methodName string, headerGetter headers.HeaderGetter) error {
	token, ok := ni.tokens[methodName]
	if !ok {
		token = NamespaceRateLimitDefaultToken
	}

	if !ni.rateLimiter.Allow(time.Now().UTC(), quotas.NewRequest(
		methodName,
		token,
		namespaceName.String(),
		headerGetter.Get(headers.CallerTypeHeaderName),
		0,  // this interceptor layer does not throttle based on caller segment
		"", // this interceptor layer does not throttle based on call initiation
	)) {
		return ErrNamespaceRateLimitServerBusy
	}
	return nil
}

func IsLongPollGetHistoryRequest(
	req interface{},
) bool {
	switch request := req.(type) {
	case *workflowservice.GetWorkflowExecutionHistoryRequest:
		return request.GetWaitNewEvent()
	}
	return false
}
