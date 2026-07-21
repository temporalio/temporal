package interceptor

import (
	"context"
	"sync"
	"sync/atomic"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/quotas/calculator"
	"google.golang.org/grpc"
)

type (
	// ConcurrentRequestLimitInterceptor intercepts requests to the server and enforces a limit on the number of
	// requests that can be in-flight at any given time, according to the configured quotas.
	ConcurrentRequestLimitInterceptor struct {
		namespaceRegistry namespace.Registry
		logger            log.Logger
		quotaCalculator   calculator.NamespaceCalculator
		// tokens is a map of method name to the number of tokens that should be consumed for that method. If there is
		// no entry for a method, then no tokens will be consumed, so the method will not be limited.
		tokens map[string]int

		sync.Mutex
		activeTokensCount     map[string]*int32
		pendingRequestMetrics map[string]*pendingRequestCounter
	}

	pendingRequestCounter struct {
		sync.Mutex
		count int32
	}
)

var (
	_ grpc.UnaryServerInterceptor = (*ConcurrentRequestLimitInterceptor)(nil).Intercept

	ErrNamespaceCountLimitServerBusy = &serviceerror.ResourceExhausted{
		Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
		Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
		Message: "namespace concurrent poller limit exceeded",
	}
)

func NewConcurrentRequestLimitInterceptor(
	namespaceRegistry namespace.Registry,
	memberCounter calculator.MemberCounter,
	logger log.Logger,
	perInstanceQuota func(ns string) int,
	globalQuota func(ns string) int,
	tokens map[string]int,
) *ConcurrentRequestLimitInterceptor {
	return &ConcurrentRequestLimitInterceptor{
		namespaceRegistry: namespaceRegistry,
		logger:            logger,
		quotaCalculator: calculator.NewLoggedNamespaceCalculator(
			calculator.ClusterAwareNamespaceQuotaCalculator{
				MemberCounter:    memberCounter,
				PerInstanceQuota: perInstanceQuota,
				GlobalQuota:      globalQuota,
			},
			log.With(logger, tag.ComponentLongPollHandler, tag.ScopeNamespace),
		),
		tokens:                tokens,
		activeTokensCount:     make(map[string]*int32),
		pendingRequestMetrics: make(map[string]*pendingRequestCounter),
	}
}

func (ni *ConcurrentRequestLimitInterceptor) Intercept(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	nsName := MustGetNamespaceName(ni.namespaceRegistry, req)
	mh := GetMetricsHandlerFromContext(ctx, ni.logger)
	cleanup, err := ni.Allow(nsName, info.FullMethod, mh, req)
	defer cleanup()
	if err != nil {
		return nil, err
	}

	return handler(ctx, req)
}

func (ni *ConcurrentRequestLimitInterceptor) Allow(
	namespaceName namespace.Name,
	methodName string,
	mh metrics.Handler,
	req any,
) (func(), error) {
	return ni.AllowWithMetricKey(
		namespaceName,
		methodName,
		telemetryUnaryOverrideOperationTag(methodName, api.MethodName(methodName), req),
		mh,
		req,
	)
}

// AllowWithMetricKey applies quota accounting by methodName while recording
// pending requests under the operation-tag identity in metricKey.
func (ni *ConcurrentRequestLimitInterceptor) AllowWithMetricKey(
	namespaceName namespace.Name,
	methodName string,
	metricKey string,
	mh metrics.Handler,
	req any,
) (func(), error) {
	// token will default to 0
	token := ni.tokens[methodName]

	if token == 0 {
		return func() {}, nil
	}
	// for GetWorkflowExecutionHistoryRequest, we only care about long poll requests
	longPollReq, ok := req.(*workflowservice.GetWorkflowExecutionHistoryRequest)
	if ok && !longPollReq.WaitNewEvent {
		// ignore non-long-poll GetHistory calls.
		return func() {}, nil
	}

	tokenCounter := ni.counter(namespaceName, methodName)
	count := atomic.AddInt32(tokenCounter, int32(token))
	metricCounter := ni.metricCounter(namespaceName, metricKey)
	metricCounter.add(token, mh)
	cleanup := func() {
		atomic.AddInt32(tokenCounter, -int32(token))
		metricCounter.add(-token, mh)
	}

	// frontend.namespaceCount is applied per poller type temporarily to prevent
	// one poller type to take all token waiting in the long poll.
	if float64(count) > ni.quotaCalculator.GetQuota(namespaceName.String()) {
		return cleanup, ErrNamespaceCountLimitServerBusy
	}
	return cleanup, nil
}

func (ni *ConcurrentRequestLimitInterceptor) counter(
	namespace namespace.Name,
	methodName string,
) *int32 {
	key := ni.getCounterKey(namespace, methodName)

	ni.Lock()
	defer ni.Unlock()

	counter, ok := ni.activeTokensCount[key]
	if !ok {
		counter = new(int32)
		ni.activeTokensCount[key] = counter
	}
	return counter
}

func (ni *ConcurrentRequestLimitInterceptor) metricCounter(
	namespace namespace.Name,
	metricKey string,
) *pendingRequestCounter {
	key := ni.getCounterKey(namespace, metricKey)

	ni.Lock()
	defer ni.Unlock()

	counter, ok := ni.pendingRequestMetrics[key]
	if !ok {
		counter = new(pendingRequestCounter)
		ni.pendingRequestMetrics[key] = counter
	}
	return counter
}

func (c *pendingRequestCounter) add(token int, mh metrics.Handler) int32 {
	c.Lock()
	defer c.Unlock()

	// Keep the transition and recording ordered so concurrent updates cannot
	// emit a stale value last.
	c.count += int32(token)
	mh.Gauge(metrics.ServicePendingRequests.Name()).Record(float64(c.count))
	return c.count
}

func (ni *ConcurrentRequestLimitInterceptor) getCounterKey(
	namespaceName namespace.Name,
	key string,
) string {
	return namespaceName.String() + "/" + key
}
