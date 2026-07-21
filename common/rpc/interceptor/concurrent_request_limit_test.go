package interceptor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/quotas/calculator"
	"go.temporal.io/server/common/quotas/quotastest"
	"go.temporal.io/server/common/testing/await"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

const pendingRequestLimitTestMethodName = "/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowTaskQueue"

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

func TestConcurrentRequestLimitInterceptor_AllowRecordsPendingRequestsOnCleanup(t *testing.T) {
	t.Parallel()

	t.Run("admitted request", func(t *testing.T) {
		t.Parallel()

		metricsHandler := metricstest.NewCaptureHandler()
		capture := metricsHandler.StartCapture()
		defer metricsHandler.StopCapture(capture)

		cleanup, err := newConcurrentRequestLimitInterceptorForTest(1).Allow(
			namespace.Name("test-namespace"),
			pendingRequestLimitTestMethodName,
			metricsHandler,
			nil,
		)
		require.NoError(t, err)

		cleanup()

		requirePendingRequestRecordings(t, capture, 1, 0)
	})

	t.Run("rejected request", func(t *testing.T) {
		t.Parallel()

		metricsHandler := metricstest.NewCaptureHandler()
		capture := metricsHandler.StartCapture()
		defer metricsHandler.StopCapture(capture)
		interceptor := newConcurrentRequestLimitInterceptorForTest(1)

		admittedCleanup, err := interceptor.Allow(
			namespace.Name("test-namespace"),
			pendingRequestLimitTestMethodName,
			metricsHandler,
			nil,
		)
		require.NoError(t, err)

		rejectedCleanup, err := interceptor.Allow(
			namespace.Name("test-namespace"),
			pendingRequestLimitTestMethodName,
			metricsHandler,
			nil,
		)
		require.ErrorIs(t, err, ErrNamespaceCountLimitServerBusy)

		rejectedCleanup()
		requirePendingRequestRecordings(t, capture, 1, 2, 1)

		admittedCleanup()
		requirePendingRequestRecordings(t, capture, 1, 2, 1, 0)
	})
}

func TestPendingRequestCounter_AddSerializesRecordings(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name         string
		initialCount int32
		token        int
		expected     []float64
	}{
		{name: "admissions", token: 1, expected: []float64{1, 2}},
		{name: "cleanups", initialCount: 2, token: -1, expected: []float64{1, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			captureHandler := metricstest.NewCaptureHandler()
			capture := captureHandler.StartCapture()
			defer captureHandler.StopCapture(capture)
			metricsHandler := &blockingMetricsHandler{Handler: captureHandler}
			counter := pendingRequestCounter{count: tc.initialCount}

			blocked, release := metricsHandler.blockNextPendingRequestRecording()
			defer release()

			firstDone := runAsync(func() { counter.add(tc.token, metricsHandler) })
			<-blocked
			secondDone := runAsync(func() { counter.add(tc.token, metricsHandler) })

			require.Never(t, func() bool {
				return isClosed(secondDone)
			}, 100*time.Millisecond, 10*time.Millisecond)

			release()
			await.RequireTrue(t, func() bool {
				return isClosed(firstDone) && isClosed(secondDone)
			}, time.Second, 10*time.Millisecond)

			requirePendingRequestRecordings(t, capture, tc.expected...)
		})
	}
}

func newConcurrentRequestLimitInterceptorForTest(limit int) *ConcurrentRequestLimitInterceptor {
	return NewConcurrentRequestLimitInterceptor(
		nil,
		quotastest.NewFakeMemberCounter(1),
		log.NewNoopLogger(),
		dynamicconfig.GetIntPropertyFnFilteredByNamespace(limit),
		dynamicconfig.GetIntPropertyFnFilteredByNamespace(0),
		map[string]int{pendingRequestLimitTestMethodName: 1},
	)
}

func requirePendingRequestRecordings(t *testing.T, capture *metricstest.Capture, expected ...float64) {
	t.Helper()

	recordings := capture.Snapshot()[metrics.ServicePendingRequests.Name()]
	require.Len(t, recordings, len(expected))
	for i, expectedValue := range expected {
		require.InDelta(t, expectedValue, recordings[i].Value, 0)
	}
}

type blockingMetricsHandler struct {
	metrics.Handler

	mu        sync.Mutex
	blockNext bool
	blocked   chan struct{}
	release   chan struct{}
}

func (h *blockingMetricsHandler) blockNextPendingRequestRecording() (<-chan struct{}, func()) {
	h.mu.Lock()
	defer h.mu.Unlock()

	blocked := make(chan struct{})
	release := make(chan struct{})
	h.blockNext = true
	h.blocked = blocked
	h.release = release

	return blocked, sync.OnceFunc(func() { close(release) })
}

func (h *blockingMetricsHandler) Gauge(name string) metrics.GaugeIface {
	gauge := h.Handler.Gauge(name)
	if name != metrics.ServicePendingRequests.Name() {
		return gauge
	}

	return metrics.GaugeFunc(func(value float64, tags ...metrics.Tag) {
		h.mu.Lock()
		block := h.blockNext
		blocked := h.blocked
		release := h.release
		h.blockNext = false
		h.mu.Unlock()

		if block {
			close(blocked)
			<-release
		}
		gauge.Record(value, tags...)
	})
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func runAsync(f func()) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()
	return done
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
		dynamicconfig.GetIntPropertyFnFilteredByNamespace(tc.perInstanceLimit),
		dynamicconfig.GetIntPropertyFnFilteredByNamespace(tc.globalLimit),
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
