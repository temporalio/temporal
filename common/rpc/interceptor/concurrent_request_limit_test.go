package interceptor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/quotas/calculator"
	"go.temporal.io/server/common/quotas/quotastest"
	interceptornexus "go.temporal.io/server/common/rpc/interceptor/nexus"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

type nsCountLimitTestCase struct {
	// name of the test case
	name string
	// request to be intercepted by the ConcurrentRequestLimitInterceptor
	request any
	// numBlockedRequests is the number of pending requests that will be blocked including the final request.
	numBlockedRequests int
	// memberCounter returns the number of members in the namespace.
	memberCounter calculator.MemberCounter
	// perInstanceLimit is the limit on the number of pending requests per-instance.
	perInstanceLimit int
	// globalLimit is the limit on the number of pending requests across all instances.
	globalLimit int
	// methodName is the fully-qualified name of the gRPC method being intercepted.
	methodName string
	// tokens is a map of method slugs (e.g. just the part of the method name after the final slash) to the number of
	// tokens that will be consumed by that method.
	tokens map[string]int
	// expectRateLimit is true if the interceptor should respond with a rate limit error.
	expectRateLimit bool
}

// TestNamespaceCountLimitInterceptor_Intercept verifies that the ConcurrentRequestLimitInterceptor responds with a rate
// limit error when requests would exceed the concurrent poller limit for a namespace.
func TestNamespaceCountLimitInterceptor_Intercept(t *testing.T) {
	t.Parallel()
	for _, tc := range []nsCountLimitTestCase{
		{
			name:               "no limit exceeded",
			request:            nil,
			numBlockedRequests: 2,
			perInstanceLimit:   2,
			globalLimit:        4,
			memberCounter:      quotastest.NewFakeMemberCounter(2),
			methodName:         "/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace",
			tokens: map[string]int{
				"/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace": 1,
			},
			expectRateLimit: false,
		},
		{
			name:               "per-instance limit exceeded",
			request:            nil,
			numBlockedRequests: 3,
			perInstanceLimit:   2,
			globalLimit:        4,
			memberCounter:      quotastest.NewFakeMemberCounter(2),
			methodName:         "/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace",
			tokens: map[string]int{
				"/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace": 1,
			},
			expectRateLimit: true,
		},
		{
			name:               "global limit exceeded",
			request:            nil,
			numBlockedRequests: 3,
			perInstanceLimit:   3,
			globalLimit:        4,
			memberCounter:      quotastest.NewFakeMemberCounter(2),
			methodName:         "/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace",
			tokens: map[string]int{
				"/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace": 1,
			},
			expectRateLimit: true,
		},
		{
			name:               "global limit zero",
			request:            nil,
			numBlockedRequests: 3,
			perInstanceLimit:   3,
			globalLimit:        0,
			memberCounter:      quotastest.NewFakeMemberCounter(2),
			methodName:         "/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace",
			tokens: map[string]int{
				"/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace": 1,
			},
			expectRateLimit: false,
		},
		{
			name:               "method name does not consume token",
			request:            nil,
			numBlockedRequests: 3,
			perInstanceLimit:   2,
			globalLimit:        4,
			memberCounter:      quotastest.NewFakeMemberCounter(2),
			methodName:         "/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace",
			tokens:             map[string]int{},
			expectRateLimit:    false,
		},
		{
			name:               "long poll request",
			request:            &workflowservice.GetWorkflowExecutionHistoryRequest{WaitNewEvent: true},
			numBlockedRequests: 3,
			perInstanceLimit:   2,
			globalLimit:        4,
			memberCounter:      quotastest.NewFakeMemberCounter(2),
			methodName:         "/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecutionHistory",
			tokens: map[string]int{
				"/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecutionHistory": 1,
			},
			expectRateLimit: true,
		},
		{
			name:               "non-long poll request",
			request:            &workflowservice.GetWorkflowExecutionHistoryRequest{WaitNewEvent: false},
			numBlockedRequests: 3,
			perInstanceLimit:   2,
			globalLimit:        4,
			memberCounter:      quotastest.NewFakeMemberCounter(2),
			methodName:         "/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecutionHistory",
			tokens: map[string]int{
				"/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecutionHistory": 1,
			},
			expectRateLimit: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func TestConcurrentRequestLimitInterceptor_InterceptNexus(t *testing.T) {
	interceptor := NewConcurrentRequestLimitInterceptor(
		nil,
		quotastest.NewFakeMemberCounter(1),
		log.NewNoopLogger(),
		ConcurrentRequestQuotas{
			PerInstance: dynamicconfig.GetIntPropertyFnFilteredByNamespace(1),
			Global:      dynamicconfig.GetIntPropertyFnFilteredByNamespace(1),
		},
		ConcurrentRequestQuotas{
			PerInstance: dynamicconfig.GetIntPropertyFnFilteredByNamespace(0),
			Global:      dynamicconfig.GetIntPropertyFnFilteredByNamespace(0),
		},
		map[string]int{"NexusAPI": 1},
	)
	input := interceptornexus.NewStartOpInput("s",
		"o",
		time.Now(),
		nexus.StartOperationOptions{},
		nil,
		interceptornexus.ForwardingInfo{},
		interceptornexus.RequestMetadata{NamespaceEntry: namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Name: testNamespace}, nil, ""), APIName: "NexusAPI"})

	ctx := context.Background()

	blockUntilFirstReqStarted := make(chan struct{})
	unblockFirstRequest := make(chan struct{})
	firstReqErrorCh := make(chan error, 1)

	go func() {
		_, err := interceptor.InterceptNexus(
			ctx,
			input,
			func(context.Context, interceptornexus.InterceptorInput) (any, error) {
				close(blockUntilFirstReqStarted)
				<-unblockFirstRequest
				return nil, nil
			},
		)
		firstReqErrorCh <- err
	}()
	<-blockUntilFirstReqStarted
	// second req should never proceed to calling next
	_, err := interceptor.InterceptNexus(
		ctx,
		input,
		func(context.Context, interceptornexus.InterceptorInput) (any, error) {
			t.Fatal("second request reached handler")
			return nil, errors.New("throttled request reached")
		},
	)
	var interceptorErr *interceptornexus.InterceptorError
	require.ErrorAs(t, err, &interceptorErr)
	require.Equal(t, "namespace_concurrency_limited", interceptorErr.Outcome)

	close(unblockFirstRequest)
	require.NoError(t, <-firstReqErrorCh)
}

// run the test case by simulating a bunch of blocked pollers, sending a final request, and verifying that it is either
// rate limited or not.
func (tc *nsCountLimitTestCase) run(t *testing.T) {
	ctrl := gomock.NewController(t)
	handler := tc.createRequestHandler()
	interceptor := tc.createInterceptor(ctrl)
	// Spawn a bunch of blocked requests in the background.
	tc.spawnBlockedRequests(handler, interceptor)

	// With all the blocked requests in flight, send the final request and verify whether it is rate limited or not.
	_, err := interceptor.Intercept(context.Background(), tc.request, &grpc.UnaryServerInfo{
		FullMethod: tc.methodName,
	}, noopHandler)

	if tc.expectRateLimit {
		assert.ErrorContains(t, err, "namespace concurrent poller limit exceeded")
	} else {
		assert.NoError(t, err)
	}

	// Clean up by unblocking all the requests.
	handler.Unblock()

	for i := 0; i < tc.numBlockedRequests-1; i++ {
		assert.NoError(t, <-handler.errs)
	}
}

func (tc *nsCountLimitTestCase) createRequestHandler() *testRequestHandler {
	return &testRequestHandler{
		started: make(chan struct{}),
		respond: make(chan struct{}),
		errs:    make(chan error, tc.numBlockedRequests-1),
	}
}

// spawnBlockedRequests sends a bunch of requests to the interceptor which will block until signaled.
func (tc *nsCountLimitTestCase) spawnBlockedRequests(
	handler *testRequestHandler,
	interceptor *ConcurrentRequestLimitInterceptor,
) {
	for i := 0; i < tc.numBlockedRequests-1; i++ {
		go func() {
			_, err := interceptor.Intercept(context.Background(), tc.request, &grpc.UnaryServerInfo{
				FullMethod: tc.methodName,
			}, handler.Handle)
			handler.errs <- err
		}()
	}

	for i := 0; i < tc.numBlockedRequests-1; i++ {
		<-handler.started
	}
}

func (tc *nsCountLimitTestCase) createInterceptor(ctrl *gomock.Controller) *ConcurrentRequestLimitInterceptor {
	registry := namespace.NewMockRegistry(ctrl)
	registry.EXPECT().GetNamespace(gomock.Any()).Return(&namespace.Namespace{}, nil).AnyTimes()

	interceptor := NewConcurrentRequestLimitInterceptor(
		registry,
		tc.memberCounter,
		log.NewNoopLogger(),
		ConcurrentRequestQuotas{
			PerInstance: dynamicconfig.GetIntPropertyFnFilteredByNamespace(tc.perInstanceLimit),
			Global:      dynamicconfig.GetIntPropertyFnFilteredByNamespace(tc.globalLimit),
		},
		ConcurrentRequestQuotas{
			PerInstance: dynamicconfig.GetIntPropertyFnFilteredByNamespace(0),
			Global:      dynamicconfig.GetIntPropertyFnFilteredByNamespace(0),
		},
		tc.tokens,
	)

	return interceptor
}

// noopHandler is a grpc.UnaryHandler which does nothing.
func noopHandler(context.Context, any) (any, error) {
	return nil, nil
}

// testRequestHandler provides a grpc.UnaryHandler which signals when it starts and does not respond until signaled.
type testRequestHandler struct {
	started chan struct{}
	respond chan struct{}
	errs    chan error
}

func (h testRequestHandler) Unblock() {
	close(h.respond)
}

// Handle signals that the request has started and then blocks until signaled to respond.
func (h testRequestHandler) Handle(context.Context, any) (any, error) {
	h.started <- struct{}{}
	<-h.respond

	return nil, nil
}

func TestNamespaceCountLimitInterceptorPollerClassification(t *testing.T) {
	t.Parallel()

	const (
		workflowMethod = "/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowTaskQueue"
		activityMethod = "/temporal.api.workflowservice.v1.WorkflowService/PollActivityTaskQueue"
		nexusMethod    = "/temporal.api.workflowservice.v1.WorkflowService/PollNexusTaskQueue"
	)
	for _, tc := range []struct {
		name     string
		method   string
		request  any
		internal bool
	}{
		{
			name:     "workflow internal queue",
			method:   workflowMethod,
			request:  &workflowservice.PollWorkflowTaskQueueRequest{TaskQueue: &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue}},
			internal: true,
		},
		{
			name:     "activity internal queue",
			method:   activityMethod,
			request:  &workflowservice.PollActivityTaskQueueRequest{TaskQueue: &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue}},
			internal: true,
		},
		{
			name:     "nexus internal queue",
			method:   nexusMethod,
			request:  &workflowservice.PollNexusTaskQueueRequest{TaskQueue: &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue}},
			internal: true,
		},
		{
			name:    "activity regular queue",
			method:  activityMethod,
			request: &workflowservice.PollActivityTaskQueueRequest{TaskQueue: &taskqueuepb.TaskQueue{Name: "regular-tq"}},
		},
		{
			name:    "nexus regular queue",
			method:  nexusMethod,
			request: &workflowservice.PollNexusTaskQueueRequest{TaskQueue: &taskqueuepb.TaskQueue{Name: "regular-tq"}},
		},
		{
			name:     "worker controller internal queue",
			method:   activityMethod,
			request:  &workflowservice.PollActivityTaskQueueRequest{TaskQueue: &taskqueuepb.TaskQueue{Name: primitives.WorkerControllerPerNSWorkerTaskQueue}},
			internal: true,
		},
		{
			name:     "prefixed internal queue",
			method:   nexusMethod,
			request:  &workflowservice.PollNexusTaskQueueRequest{TaskQueue: &taskqueuepb.TaskQueue{Name: "temporal-sys-per-ns-custom-tq"}},
			internal: true,
		},
		{
			name:   "sticky internal normal name",
			method: workflowMethod,
			request: &workflowservice.PollWorkflowTaskQueueRequest{TaskQueue: &taskqueuepb.TaskQueue{
				Name: "regular-sticky-tq", Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: primitives.PerNSWorkerTaskQueue,
			}},
			internal: true,
		},
		{
			name:   "sticky regular normal name",
			method: workflowMethod,
			request: &workflowservice.PollWorkflowTaskQueueRequest{TaskQueue: &taskqueuepb.TaskQueue{
				Name: primitives.PerNSWorkerTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: "regular-tq",
			}},
		},
		{
			name:   "sticky without normal name",
			method: workflowMethod,
			request: &workflowservice.PollWorkflowTaskQueueRequest{TaskQueue: &taskqueuepb.TaskQueue{
				Name: primitives.PerNSWorkerTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_STICKY,
			}},
			internal: true,
		},
		{
			name:   "normal queue ignores normal name",
			method: workflowMethod,
			request: &workflowservice.PollWorkflowTaskQueueRequest{TaskQueue: &taskqueuepb.TaskQueue{
				Name: "regular-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL, NormalName: primitives.PerNSWorkerTaskQueue,
			}},
		},
		{
			name:    "nil queue uses default quota",
			method:  workflowMethod,
			request: &workflowservice.PollWorkflowTaskQueueRequest{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			interceptor := NewConcurrentRequestLimitInterceptor(
				nil,
				quotastest.NewFakeMemberCounter(1),
				log.NewNoopLogger(),
				ConcurrentRequestQuotas{
					PerInstance: dynamicconfig.GetIntPropertyFnFilteredByNamespace(1),
					Global:      dynamicconfig.GetIntPropertyFnFilteredByNamespace(0),
				},
				ConcurrentRequestQuotas{
					PerInstance: dynamicconfig.GetIntPropertyFnFilteredByNamespace(0),
					Global:      dynamicconfig.GetIntPropertyFnFilteredByNamespace(0),
				},
				map[string]int{tc.method: 1},
			)
			mh := metricstest.NewCaptureHandler()
			capture := mh.StartCapture()
			defer mh.StopCapture(capture)

			cleanup, err := interceptor.Allow("test-namespace", tc.method, mh, tc.request)
			defer cleanup()
			wantLimitGroup := "default"
			if tc.internal {
				require.ErrorIs(t, err, ErrNamespaceCountLimitServerBusy)
				wantLimitGroup = "internal_per_ns"
			} else {
				require.NoError(t, err)
			}
			recordings := capture.SnapshotMetric(metrics.ServicePendingRequests.Name())
			require.Len(t, recordings, 1)
			require.Equal(t, wantLimitGroup, recordings[0].Tags["concurrency_limit_group"])
		})
	}
}

func TestNamespaceCountLimitInterceptorIndependentPollerQuotas(t *testing.T) {
	t.Parallel()

	const (
		method         = "/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowTaskQueue"
		activityMethod = "/temporal.api.workflowservice.v1.WorkflowService/PollActivityTaskQueue"
	)
	for _, tc := range []struct {
		name                string
		defaultPerInstance  int
		defaultGlobal       int
		internalPerInstance int
		internalGlobal      int
		defaultLimit        int
		internalLimit       int
	}{
		{
			name: "per instance", defaultPerInstance: 1, internalPerInstance: 2,
			defaultLimit: 1, internalLimit: 2,
		},
		{
			name: "global divided by members", defaultPerInstance: 5, defaultGlobal: 4,
			internalPerInstance: 5, internalGlobal: 2, defaultLimit: 2, internalLimit: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			interceptor := NewConcurrentRequestLimitInterceptor(
				nil,
				quotastest.NewFakeMemberCounter(2),
				log.NewNoopLogger(),
				ConcurrentRequestQuotas{
					PerInstance: dynamicconfig.GetIntPropertyFnFilteredByNamespace(tc.defaultPerInstance),
					Global:      dynamicconfig.GetIntPropertyFnFilteredByNamespace(tc.defaultGlobal),
				},
				ConcurrentRequestQuotas{
					PerInstance: dynamicconfig.GetIntPropertyFnFilteredByNamespace(tc.internalPerInstance),
					Global:      dynamicconfig.GetIntPropertyFnFilteredByNamespace(tc.internalGlobal),
				},
				map[string]int{method: 1, activityMethod: 1},
			)
			for _, pool := range []struct {
				queue string
				limit int
			}{
				{queue: "regular-tq", limit: tc.defaultLimit},
				{queue: primitives.PerNSWorkerTaskQueue, limit: tc.internalLimit},
			} {
				request := &workflowservice.PollWorkflowTaskQueueRequest{TaskQueue: &taskqueuepb.TaskQueue{Name: pool.queue}}
				for range pool.limit {
					cleanup, err := interceptor.Allow("test-namespace", method, metrics.NoopMetricsHandler, request)
					t.Cleanup(cleanup)
					require.NoError(t, err)
				}
				cleanup, err := interceptor.Allow("test-namespace", method, metrics.NoopMetricsHandler, request)
				cleanup()
				require.ErrorIs(t, err, ErrNamespaceCountLimitServerBusy)

				cleanup, err = interceptor.Allow("test-namespace", activityMethod, metrics.NoopMetricsHandler,
					&workflowservice.PollActivityTaskQueueRequest{TaskQueue: request.TaskQueue})
				t.Cleanup(cleanup)
				require.NoError(t, err)

				cleanup, err = interceptor.Allow("other-namespace", method, metrics.NoopMetricsHandler, request)
				t.Cleanup(cleanup)
				require.NoError(t, err)
			}
		})
	}
}

func TestNamespaceCountLimitInterceptorReleasesPollerQuota(t *testing.T) {
	t.Parallel()

	const method = "/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowTaskQueue"
	handlerErr := errors.New("handler failed")
	for _, tc := range []struct {
		name       string
		queue      string
		handlerErr error
	}{
		{name: "regular handler success", queue: "regular-tq"},
		{name: "regular handler error", queue: "regular-tq", handlerErr: handlerErr},
		{name: "internal handler success", queue: primitives.PerNSWorkerTaskQueue},
		{name: "internal handler error", queue: primitives.PerNSWorkerTaskQueue, handlerErr: handlerErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			registry := namespace.NewMockRegistry(gomock.NewController(t))
			registry.EXPECT().GetNamespace(gomock.Any()).Return(&namespace.Namespace{}, nil).AnyTimes()
			quotas := ConcurrentRequestQuotas{
				PerInstance: dynamicconfig.GetIntPropertyFnFilteredByNamespace(1),
				Global:      dynamicconfig.GetIntPropertyFnFilteredByNamespace(0),
			}
			interceptor := NewConcurrentRequestLimitInterceptor(
				registry, quotastest.NewFakeMemberCounter(1), log.NewNoopLogger(), quotas, quotas, map[string]int{method: 1},
			)
			request := &workflowservice.PollWorkflowTaskQueueRequest{
				Namespace: "test-namespace", TaskQueue: &taskqueuepb.TaskQueue{Name: tc.queue},
			}
			info := &grpc.UnaryServerInfo{FullMethod: method}
			_, err := interceptor.Intercept(context.Background(), request, info, func(ctx context.Context, req any) (any, error) {
				_, err := interceptor.Intercept(ctx, req, info, noopHandler)
				require.ErrorIs(t, err, ErrNamespaceCountLimitServerBusy)
				return nil, tc.handlerErr
			})
			if tc.handlerErr != nil {
				require.ErrorIs(t, err, tc.handlerErr)
			} else {
				require.NoError(t, err)
			}

			_, err = interceptor.Intercept(context.Background(), request, info, noopHandler)
			require.NoError(t, err)
		})
	}
}
