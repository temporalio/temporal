package interceptor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/quotas"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

const (
	pollWorkflowTaskQueueMethod = "/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowTaskQueue"
	otherMethod                 = "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution"
	testNamespace               = "test-namespace"
)

type namespaceRateLimitInterceptorSuite struct {
	suite.Suite
	*require.Assertions

	controller      *gomock.Controller
	mockRateLimiter *quotas.MockRequestRateLimiter
	mockRegistry    *namespace.MockRegistry
}

func TestNamespaceRateLimitInterceptorSuite(t *testing.T) {
	suite.Run(t, &namespaceRateLimitInterceptorSuite{})
}

func (s *namespaceRateLimitInterceptorSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.mockRateLimiter = quotas.NewMockRequestRateLimiter(s.controller)
	s.mockRegistry = namespace.NewMockRegistry(s.controller)
}

func (s *namespaceRateLimitInterceptorSuite) newImpl(pollWaitForToken bool) *NamespaceRateLimitInterceptorImpl {
	return &NamespaceRateLimitInterceptorImpl{
		namespaceRegistry: s.mockRegistry,
		rateLimiter:       s.mockRateLimiter,
		tokens:            map[string]int{},
		pollMethods: map[string]struct{}{
			pollWorkflowTaskQueueMethod: {},
		},
		pollWaitForToken: func(_ string) bool { return pollWaitForToken },
		metricsHandler:   metrics.NoopMetricsHandler,
	}
}

// Wait() tests

func (s *namespaceRateLimitInterceptorSuite) TestWait_TokenImmediatelyAvailable() {
	s.mockRateLimiter.EXPECT().Allow(gomock.Any(), gomock.Any()).Return(true)
	s.mockRateLimiter.EXPECT().Wait(gomock.Any(), gomock.Any()).Times(0)

	ni := s.newImpl(true)
	err := ni.Wait(context.Background(), testNamespace, pollWorkflowTaskQueueMethod, noopHeaderGetter{})
	s.NoError(err)
}

func (s *namespaceRateLimitInterceptorSuite) TestWait_WaitSucceeds() {
	s.mockRateLimiter.EXPECT().Allow(gomock.Any(), gomock.Any()).Return(false)
	s.mockRateLimiter.EXPECT().Wait(gomock.Any(), gomock.Any()).Return(nil)

	ni := s.newImpl(true)
	err := ni.Wait(context.Background(), testNamespace, pollWorkflowTaskQueueMethod, noopHeaderGetter{})
	s.NoError(err)
}

func (s *namespaceRateLimitInterceptorSuite) TestWait_NoDeadlineOnCtx() {
	// No deadline → waitCtx == ctx; should not panic and should succeed.
	s.mockRateLimiter.EXPECT().Allow(gomock.Any(), gomock.Any()).Return(false)
	s.mockRateLimiter.EXPECT().Wait(gomock.Any(), gomock.Any()).Return(nil)

	ni := s.newImpl(true)
	err := ni.Wait(context.Background(), testNamespace, pollWorkflowTaskQueueMethod, noopHeaderGetter{})
	s.NoError(err)
}

func (s *namespaceRateLimitInterceptorSuite) TestWait_ShortenedDeadlineExpires_OriginalCtxValid() {
	// Outer ctx has deadline = now + CriticalLongPollTimeout + 2s.
	// Shortened waitCtx deadline = now + 2s → expires quickly.
	// Original ctx is still alive → expect ErrNamespaceRateLimitServerBusy.
	s.mockRateLimiter.EXPECT().Allow(gomock.Any(), gomock.Any()).Return(false)
	s.mockRateLimiter.EXPECT().Wait(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ quotas.Request) error {
			<-ctx.Done()
			return ctx.Err()
		})

	outerDeadline := time.Now().Add(common.CriticalLongPollTimeout + 2*time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), outerDeadline)
	defer cancel()

	ni := s.newImpl(true)
	err := ni.Wait(ctx, testNamespace, pollWorkflowTaskQueueMethod, noopHeaderGetter{})
	s.ErrorIs(err, ErrNamespaceRateLimitServerBusy)
}

func (s *namespaceRateLimitInterceptorSuite) TestWait_DeadlineTooShortToWait() {
	// Outer ctx has deadline <= now + CriticalLongPollTimeout → no time to wait.
	// Expect immediate ErrNamespaceRateLimitServerBusy without calling rateLimiter.Wait().
	s.mockRateLimiter.EXPECT().Allow(gomock.Any(), gomock.Any()).Return(false)
	s.mockRateLimiter.EXPECT().Wait(gomock.Any(), gomock.Any()).Times(0)

	outerDeadline := time.Now().Add(common.CriticalLongPollTimeout - time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), outerDeadline)
	defer cancel()

	ni := s.newImpl(true)
	err := ni.Wait(ctx, testNamespace, pollWorkflowTaskQueueMethod, noopHeaderGetter{})
	s.ErrorIs(err, ErrNamespaceRateLimitServerBusy)
}

func (s *namespaceRateLimitInterceptorSuite) TestWait_OriginalCtxCancelled() {
	// When the original context is cancelled, Wait() should propagate ctx.Err().
	ctx, cancel := context.WithCancel(context.Background())
	s.mockRateLimiter.EXPECT().Allow(gomock.Any(), gomock.Any()).Return(false)
	s.mockRateLimiter.EXPECT().Wait(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ quotas.Request) error {
			cancel()
			<-ctx.Done()
			return ctx.Err()
		})

	ni := s.newImpl(true)
	err := ni.Wait(ctx, testNamespace, pollWorkflowTaskQueueMethod, noopHeaderGetter{})
	s.ErrorIs(err, context.Canceled)
}

// Intercept() routing tests

func (s *namespaceRateLimitInterceptorSuite) TestIntercept_PollMethod_WaitForTokenEnabled() {
	// Poll method + pollWaitForToken=true → calls Wait(), handler invoked.
	s.mockRegistry.EXPECT().GetNamespace(namespace.Name(testNamespace)).Return(nil, nil)
	s.mockRateLimiter.EXPECT().Allow(gomock.Any(), gomock.Any()).Return(true) // Wait() fast path

	handlerCalled := false
	handler := func(ctx context.Context, req any) (any, error) {
		handlerCalled = true
		return nil, nil
	}

	ni := s.newImpl(true)
	req := &workflowservice.PollWorkflowTaskQueueRequest{Namespace: testNamespace}
	_, err := ni.Intercept(context.Background(), req, &grpc.UnaryServerInfo{FullMethod: pollWorkflowTaskQueueMethod}, handler)
	s.NoError(err)
	s.True(handlerCalled)
}

func (s *namespaceRateLimitInterceptorSuite) TestIntercept_PollMethod_WaitDisabled() {
	// Poll method + pollWaitForToken=false → falls through to Allow().
	s.mockRegistry.EXPECT().GetNamespace(namespace.Name(testNamespace)).Return(nil, nil)
	s.mockRateLimiter.EXPECT().Allow(gomock.Any(), gomock.Any()).Return(true)

	handlerCalled := false
	handler := func(ctx context.Context, req any) (any, error) {
		handlerCalled = true
		return nil, nil
	}

	ni := s.newImpl(false)
	req := &workflowservice.PollWorkflowTaskQueueRequest{Namespace: testNamespace}
	_, err := ni.Intercept(context.Background(), req, &grpc.UnaryServerInfo{FullMethod: pollWorkflowTaskQueueMethod}, handler)
	s.NoError(err)
	s.True(handlerCalled)
}

func (s *namespaceRateLimitInterceptorSuite) TestIntercept_NonPollMethod_WaitEnabled() {
	// Non-poll method + pollWaitForToken=true → uses Allow(), not Wait().
	s.mockRegistry.EXPECT().GetNamespace(namespace.Name(testNamespace)).Return(nil, nil)
	s.mockRateLimiter.EXPECT().Allow(gomock.Any(), gomock.Any()).Return(true)

	handlerCalled := false
	handler := func(ctx context.Context, req any) (any, error) {
		handlerCalled = true
		return nil, nil
	}

	ni := s.newImpl(true)
	req := &workflowservice.StartWorkflowExecutionRequest{Namespace: testNamespace}
	_, err := ni.Intercept(context.Background(), req, &grpc.UnaryServerInfo{FullMethod: otherMethod}, handler)
	s.NoError(err)
	s.True(handlerCalled)
}

func (s *namespaceRateLimitInterceptorSuite) TestIntercept_PollMethod_WaitEnabled_RateLimited() {
	// Poll method + pollWaitForToken=true, rate limited → error returned, handler not called.
	s.mockRegistry.EXPECT().GetNamespace(namespace.Name(testNamespace)).Return(nil, nil)
	s.mockRateLimiter.EXPECT().Allow(gomock.Any(), gomock.Any()).Return(false) // Wait() slow path
	s.mockRateLimiter.EXPECT().Wait(gomock.Any(), gomock.Any()).Return(nil)    // token granted

	handlerCalled := false
	handler := func(ctx context.Context, req any) (any, error) {
		handlerCalled = true
		return nil, nil
	}

	ni := s.newImpl(true)
	req := &workflowservice.PollWorkflowTaskQueueRequest{Namespace: testNamespace}
	_, err := ni.Intercept(context.Background(), req, &grpc.UnaryServerInfo{FullMethod: pollWorkflowTaskQueueMethod}, handler)
	s.NoError(err)
	s.True(handlerCalled)
}

func (s *namespaceRateLimitInterceptorSuite) TestIntercept_PollMethod_WaitEnabled_ContextExpired() {
	// Poll method + pollWaitForToken=true, shortened deadline fires → ErrNamespaceRateLimitServerBusy.
	s.mockRegistry.EXPECT().GetNamespace(namespace.Name(testNamespace)).Return(nil, nil)
	s.mockRateLimiter.EXPECT().Allow(gomock.Any(), gomock.Any()).Return(false)
	s.mockRateLimiter.EXPECT().Wait(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ quotas.Request) error {
			<-ctx.Done()
			return ctx.Err()
		})

	outerDeadline := time.Now().Add(common.CriticalLongPollTimeout + 2*time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), outerDeadline)
	defer cancel()

	ni := s.newImpl(true)
	req := &workflowservice.PollWorkflowTaskQueueRequest{Namespace: testNamespace}
	_, err := ni.Intercept(ctx, req, &grpc.UnaryServerInfo{FullMethod: pollWorkflowTaskQueueMethod}, func(_ context.Context, _ any) (any, error) {
		return nil, nil
	})
	s.ErrorIs(err, ErrNamespaceRateLimitServerBusy)
}

// noopHeaderGetter implements headers.HeaderGetter with empty values.
type noopHeaderGetter struct{}

func (noopHeaderGetter) Get(_ string) string { return "" }
