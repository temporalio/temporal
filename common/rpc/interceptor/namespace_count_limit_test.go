package interceptor

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/quotas/quotastest"
	"google.golang.org/grpc"
)

type testCase struct {
	// name of the test case
	name string
	// request to be intercepted
	request interface{}
	// numBlockedRequests is the number of pending requests that will be blocked before the final request is sent.
	numBlockedRequests int
	// perInstanceLimit is the limit on the number of pending requests per instance. It is ignored if globalCountLimit
	// is used.
	perInstanceLimit int
	// instances is the number of instances that this host will think are running, including itself.
	instances int
	// globalCountLimit is the limit on the number of pending requests across all instances.
	globalCountLimit int
	// methodName is the name of the method that will be called on the request. Different methods may consume different
	// numbers of tokens.
	methodName string
	// tokens is a map of method names to the number of tokens that will be consumed by that method.
	tokens map[string]int
	// expectRateLimit is true if the interceptor should respond with a rate limit error.
	expectRateLimit bool
}

// TestNamespaceCountLimitInterceptor_Intercept verifies that the NamespaceCountLimitInterceptor responds with a rate
// limit error when requests would exceed the concurrent poller limit for a namespace.
func TestNamespaceCountLimitInterceptor_Intercept(t *testing.T) {
	t.Parallel()
	for _, tc := range []testCase{
		{
			name:               "no limit hit",
			request:            nil,
			numBlockedRequests: 2,
			perInstanceLimit:   3,
			instances:          4,
			globalCountLimit:   12,
			methodName:         "/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace",
			tokens: map[string]int{
				"DescribeNamespace": 1,
			},
			expectRateLimit: false,
		},
		{
			name:               "per-instance limit hit",
			request:            nil,
			numBlockedRequests: 2,
			perInstanceLimit:   2,
			instances:          4,
			globalCountLimit:   0,
			methodName:         "/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace",
			tokens: map[string]int{
				"DescribeNamespace": 1,
			},
			expectRateLimit: true,
		},
		{
			name:               "global limit hit",
			request:            nil,
			numBlockedRequests: 2,
			perInstanceLimit:   3,
			instances:          4,
			globalCountLimit:   11,
			methodName:         "/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace",
			tokens: map[string]int{
				"DescribeNamespace": 1,
			},
			expectRateLimit: true,
		},
		{
			name:               "method name does not consume token",
			request:            nil,
			numBlockedRequests: 2,
			perInstanceLimit:   3,
			instances:          4,
			globalCountLimit:   11,
			methodName:         "/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace",
			tokens:             map[string]int{},
			expectRateLimit:    false,
		},
		{
			name:               "long poll request",
			request:            &workflowservice.GetWorkflowExecutionHistoryRequest{WaitNewEvent: true},
			numBlockedRequests: 2,
			perInstanceLimit:   3,
			instances:          4,
			globalCountLimit:   11,
			methodName:         "/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecutionHistory",
			tokens: map[string]int{
				"GetWorkflowExecutionHistory": 1,
			},
			expectRateLimit: true,
		},
		{
			name:               "non-long poll request",
			request:            &workflowservice.GetWorkflowExecutionHistoryRequest{WaitNewEvent: false},
			numBlockedRequests: 2,
			perInstanceLimit:   3,
			instances:          4,
			globalCountLimit:   11,
			methodName:         "/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecutionHistory",
			tokens: map[string]int{
				"GetWorkflowExecutionHistory": 1,
			},
			expectRateLimit: false,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// run the test case by simulating a bunch of blocked pollers, sending a final request, and verifying that it is either
// rate limited or not.
func (tc *testCase) run(t *testing.T) {
	ctrl := gomock.NewController(t)
	interceptor := tc.createInterceptor(ctrl)
	handler, errs := tc.createBlockedPollRequests(interceptor)

	_, err := interceptor.Intercept(context.Background(), tc.request, &grpc.UnaryServerInfo{
		FullMethod: tc.methodName,
	}, noOpHandler)

	if tc.expectRateLimit {
		assert.ErrorContains(t, err, "namespace concurrent poller limit exceeded")
	} else {
		assert.NoError(t, err)
	}
	handler.Unblock()
	for i := 0; i < tc.numBlockedRequests; i++ {
		assert.NoError(t, <-errs)
	}
}

func (tc *testCase) createBlockedPollRequests(interceptor *NamespaceCountLimitInterceptor) (testRequestHandler, chan error) {
	handler := testRequestHandler{
		started: make(chan struct{}),
		respond: make(chan struct{}),
	}
	errs := make(chan error, tc.numBlockedRequests)
	for i := 0; i < tc.numBlockedRequests; i++ {
		go func() {
			_, err := interceptor.Intercept(context.Background(), tc.request, &grpc.UnaryServerInfo{
				FullMethod: tc.methodName,
			}, handler.Handle)
			errs <- err
		}()
	}
	for i := 0; i < tc.numBlockedRequests; i++ {
		<-handler.started
	}
	return handler, errs
}

func (tc *testCase) createInterceptor(ctrl *gomock.Controller) *NamespaceCountLimitInterceptor {
	registry := namespace.NewMockRegistry(ctrl)
	registry.EXPECT().GetNamespace(gomock.Any()).Return(&namespace.Namespace{}, nil).AnyTimes()
	logger := log.NewNoopLogger()
	instanceCounter := quotastest.NewFakeInstanceCounter(tc.instances)
	perInstanceCountLimit := func(namespace string) int {
		return tc.perInstanceLimit
	}
	globalCountLimit := func(namespace string) int {
		return tc.globalCountLimit
	}
	interceptor := NewNamespaceCountLimitInterceptor(
		registry,
		logger,
		instanceCounter,
		perInstanceCountLimit,
		globalCountLimit,
		tc.tokens,
	)
	return interceptor
}

// noOpHandler is a grpc.UnaryHandler which does nothing.
func noOpHandler(context.Context, interface{}) (interface{}, error) {
	return nil, nil
}

// testRequestHandler provides a grpc.UnaryHandler which signals when it starts and does not respond until signaled.
type testRequestHandler struct {
	started chan struct{}
	respond chan struct{}
}

func (h testRequestHandler) Unblock() {
	close(h.respond)
}

// Handle signals that the request has started and then blocks until signaled to respond.
func (h testRequestHandler) Handle(context.Context, interface{}) (interface{}, error) {
	h.started <- struct{}{}
	<-h.respond
	return nil, nil
}
