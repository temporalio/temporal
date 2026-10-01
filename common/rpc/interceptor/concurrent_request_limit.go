package interceptor

import (
	"context"
	"sync"
	"sync/atomic"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/quotas/calculator"
	"google.golang.org/grpc"
)

type (
	// ConcurrentRequestLimitInterceptor intercepts requests to the server and enforces a limit on the number of
	// requests that can be in-flight at any given time, according to the configured quotas.
	ConcurrentRequestLimitInterceptor struct {
		namespaceRegistry            namespace.Registry
		logger                       log.Logger
		quotaCalculator              calculator.NamespaceCalculator
		internalPerNSQuotaCalculator calculator.NamespaceCalculator
		// tokens is a map of method name to the number of tokens that should be consumed for that method. If there is
		// no entry for a method, then no tokens will be consumed, so the method will not be limited.
		tokens map[string]int

		sync.Mutex
		activeTokensCount map[concurrentRequestCounterKey]*int32
	}

	concurrentRequestCounterKey struct {
		namespace namespace.Name
		method    string
		internal  bool
	}

	// ConcurrentRequestQuotas is the per-instance and cluster-wide cap for one population of
	// long-running requests. A global value of 0 means the per-instance cap is used.
	ConcurrentRequestQuotas struct {
		PerInstance func(namespace string) int
		Global      func(namespace string) int
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
	customer ConcurrentRequestQuotas,
	internalPerNS ConcurrentRequestQuotas,
	tokens map[string]int,
) *ConcurrentRequestLimitInterceptor {
	return &ConcurrentRequestLimitInterceptor{
		namespaceRegistry:            namespaceRegistry,
		logger:                       logger,
		quotaCalculator:              newNamespaceCountQuotaCalculator(memberCounter, logger, customer),
		internalPerNSQuotaCalculator: newNamespaceCountQuotaCalculator(memberCounter, logger, internalPerNS),
		tokens:                       tokens,
		activeTokensCount:            make(map[concurrentRequestCounterKey]*int32),
	}
}

func newNamespaceCountQuotaCalculator(
	memberCounter calculator.MemberCounter,
	logger log.Logger,
	quotas ConcurrentRequestQuotas,
) calculator.NamespaceCalculator {
	return calculator.NewLoggedNamespaceCalculator(
		calculator.ClusterAwareNamespaceQuotaCalculator{
			MemberCounter:    memberCounter,
			PerInstanceQuota: quotas.PerInstance,
			GlobalQuota:      quotas.Global,
		},
		log.With(logger, tag.ComponentLongPollHandler, tag.ScopeNamespace),
	)
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

	// Task-queue polls on an internal per-namespace queue use a separate budget so customer
	// pollers cannot exhaust the slots used by per-namespace system workers. Other long-running
	// RPCs stay on the customer budget. Each budget is still applied per API method.
	internal := isInternalPerNSPoll(req)
	quotaCalculator := ni.quotaCalculator
	if internal {
		quotaCalculator = ni.internalPerNSQuotaCalculator
	}

	counter := ni.counter(namespaceName, methodName, internal)
	count := atomic.AddInt32(counter, int32(token))
	cleanup := func() { atomic.AddInt32(counter, -int32(token)) }

	scope := "namespace"
	if internal {
		scope = "internal_per_ns"
	}
	mh.WithTags(metrics.StringTag("poller_limit_scope", scope)).
		Gauge(metrics.ServicePendingRequests.Name()).
		Record(float64(count))

	if float64(count) > quotaCalculator.GetQuota(namespaceName.String()) {
		return cleanup, ErrNamespaceCountLimitServerBusy
	}
	return cleanup, nil
}

func isInternalPerNSPoll(req any) bool {
	switch r := req.(type) {
	case *workflowservice.PollWorkflowTaskQueueRequest:
		return primitives.IsInternalPerNsTaskQueue(effectiveTaskQueueName(r.GetTaskQueue()))
	case *workflowservice.PollActivityTaskQueueRequest:
		return primitives.IsInternalPerNsTaskQueue(effectiveTaskQueueName(r.GetTaskQueue()))
	case *workflowservice.PollNexusTaskQueueRequest:
		return primitives.IsInternalPerNsTaskQueue(effectiveTaskQueueName(r.GetTaskQueue()))
	default:
		return false
	}
}

// Sticky workflow polls carry a generated queue name. NormalName is the queue the worker is registered on.
func effectiveTaskQueueName(tq *taskqueuepb.TaskQueue) string {
	if tq.GetKind() == enumspb.TASK_QUEUE_KIND_STICKY && tq.GetNormalName() != "" {
		return tq.GetNormalName()
	}
	return tq.GetName()
}

func (ni *ConcurrentRequestLimitInterceptor) counter(
	namespace namespace.Name,
	methodName string,
	internal bool,
) *int32 {
	key := concurrentRequestCounterKey{
		namespace: namespace,
		method:    methodName,
		internal:  internal,
	}

	ni.Lock()
	defer ni.Unlock()

	counter, ok := ni.activeTokensCount[key]
	if !ok {
		counter = new(int32)
		ni.activeTokensCount[key] = counter
	}
	return counter
}
