package interceptor

import (
	"context"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
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
		Wait(ctx context.Context, namespaceName namespace.Name, methodName string, headerGetter headers.HeaderGetter) error
	}

	NamespaceRateLimitInterceptorImpl struct {
		namespaceRegistry                 namespace.Registry
		rateLimiter                       quotas.RequestRateLimiter
		tokens                            map[string]int
		reducePollWorkflowHistoryPriority dynamicconfig.BoolPropertyFn
		pollMethods                       map[string]struct{}
		pollWaitForToken                  dynamicconfig.BoolPropertyFnWithNamespaceFilter
		logger                            log.Logger
	}
)

var _ grpc.UnaryServerInterceptor = (*NamespaceRateLimitInterceptorImpl)(nil).Intercept
var _ NamespaceRateLimitInterceptor = (*NamespaceRateLimitInterceptorImpl)(nil)

func NewNamespaceRateLimitInterceptor(
	namespaceRegistry namespace.Registry,
	rateLimiter quotas.RequestRateLimiter,
	tokens map[string]int,
	pollMethods map[string]struct{},
	pollWaitForToken dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	logger log.Logger,
) NamespaceRateLimitInterceptor {
	return &NamespaceRateLimitInterceptorImpl{
		namespaceRegistry: namespaceRegistry,
		rateLimiter:       rateLimiter,
		tokens:            tokens,
		pollMethods:       pollMethods,
		pollWaitForToken:  pollWaitForToken,
		logger:            logger,
	}
}

func (ni *NamespaceRateLimitInterceptorImpl) Intercept(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	if ns := MustGetNamespaceName(ni.namespaceRegistry, req); ns != namespace.EmptyName {
		method := info.FullMethod
		if IsLongPollGetWorkflowExecutionHistoryRequest(req) {
			method = configs.PollWorkflowHistoryAPIName
		} else if IsLongPollDescribeActivityExecutionRequest(req) {
			method = configs.PollActivityExecutionAPIName
		}
		if ni.pollWaitForToken(ns.String()) {
			if _, ok := ni.pollMethods[info.FullMethod]; ok {
				if err := ni.Wait(ctx, ns, method, headers.NewGRPCHeaderGetter(ctx)); err != nil {
					return nil, err
				}
				return handler(ctx, req)
			}
		}
		if err := ni.Allow(ns, method, headers.NewGRPCHeaderGetter(ctx)); err != nil {
			return nil, err
		}
	}

	return handler(ctx, req)
}

func (ni *NamespaceRateLimitInterceptorImpl) Wait(ctx context.Context, namespaceName namespace.Name, methodName string, headerGetter headers.HeaderGetter) error {
	token, ok := ni.tokens[methodName]
	if !ok {
		token = NamespaceRateLimitDefaultToken
	}
	request := quotas.NewRequest(
		methodName,
		token,
		namespaceName.String(),
		headerGetter.Get(headers.CallerTypeHeaderName),
		0,  // this interceptor layer does not throttle based on caller segment
		"", // this interceptor layer does not throttle based on call initiation
	)

	if ni.rateLimiter.Allow(time.Now().UTC(), request) {
		return nil
	}

	waitCtx := ctx
	var cancel context.CancelFunc = func() {}
	if deadline, ok := ctx.Deadline(); ok {
		waitCtx, cancel = context.WithDeadline(ctx, deadline.Add(-common.CriticalLongPollTimeout))
	}
	defer cancel()

	mh := GetMetricsHandlerFromContext(ctx, ni.logger).WithTags(
		metrics.NamespaceTag(namespaceName.String()),
		metrics.OperationTag(methodName),
	)
	start := time.Now()
	err := ni.rateLimiter.Wait(waitCtx, request)
	metrics.NamespaceRateLimitWaitLatency.With(mh).Record(time.Since(start))

	if err != nil && ctx.Err() == nil {
		return ErrNamespaceRateLimitServerBusy
	}
	return ctx.Err()
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

func IsLongPollGetWorkflowExecutionHistoryRequest(
	req any,
) bool {
	switch request := req.(type) {
	case *workflowservice.GetWorkflowExecutionHistoryRequest:
		return request.GetWaitNewEvent()
	}
	return false
}

func IsLongPollDescribeActivityExecutionRequest(
	req any,
) bool {
	switch request := req.(type) {
	case *workflowservice.DescribeActivityExecutionRequest:
		return len(request.GetLongPollToken()) > 0
	}
	return false
}
