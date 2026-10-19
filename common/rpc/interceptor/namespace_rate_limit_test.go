package interceptor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/service/frontend/configs"
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
		pollMethods: map[string]struct{}{
			pollWorkflowTaskQueueMethod: {},
		},
		pollWaitForToken: func(_ string) bool { return pollWaitForToken },
		metricsHandler:   metrics.NoopMetricsHandler,
	}
}

func (s *namespaceRateLimitInterceptorSuite) newImplWithRetryFloor(
	pollWaitForToken bool,
	metricsHandler metrics.Handler,
) NamespaceRateLimitInterceptor {
	return NewNamespaceRateLimitInterceptor(
		s.mockRegistry,
		s.mockRateLimiter,
		map[string]struct{}{pollWorkflowTaskQueueMethod: {}},
		func(string) bool { return pollWaitForToken },
		metricsHandler,
		WithResourceExhaustedRetryDelayFloor(func(string) time.Duration { return 2 * time.Second }),
	)
}

func (s *namespaceRateLimitInterceptorSuite) TestInterceptN_AllowRejectionGetsRetryDelayFloor() {
	s.mockRegistry.EXPECT().GetNamespace(namespace.Name(testNamespace)).Return(nil, nil)
	s.mockRateLimiter.EXPECT().Allow(gomock.Any(), gomock.Any()).Return(false)

	mh := metricstest.NewCaptureHandler()
	capture := mh.StartCapture()
	defer mh.StopCapture(capture)
	ni := s.newImplWithRetryFloor(false, mh)
	handlerCalled := false
	_, err := ni.InterceptN(
		context.Background(),
		&workflowservice.StartWorkflowExecutionRequest{Namespace: testNamespace},
		&grpc.UnaryServerInfo{FullMethod: otherMethod},
		func(context.Context, any) (any, error) {
			handlerCalled = true
			return nil, nil
		},
		3,
	)

	retryInfo := retryInfoDetail(err)
	s.Require().NotNil(retryInfo)
	s.Equal(2*time.Second, retryInfo.GetRetryDelay().AsDuration())
	s.False(handlerCalled)
	recordings := capture.SnapshotMetric(metrics.ResourceExhaustedRetryDelayFloorEmitted.Name())
	s.Require().Len(recordings, 1)
	s.Equal(map[string]string{
		"namespace": testNamespace,
		"operation": "StartWorkflowExecution",
	}, recordings[0].Tags)
}

func (s *namespaceRateLimitInterceptorSuite) TestIntercept_WaitDeadlineTooShortGetsRetryDelayFloor() {
	s.mockRegistry.EXPECT().GetNamespace(namespace.Name(testNamespace)).Return(nil, nil)
	s.mockRateLimiter.EXPECT().Allow(gomock.Any(), gomock.Any()).Return(false)
	s.mockRateLimiter.EXPECT().Wait(gomock.Any(), gomock.Any()).Times(0)

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(common.CriticalLongPollTimeout-time.Millisecond))
	defer cancel()
	ni := s.newImplWithRetryFloor(true, metrics.NoopMetricsHandler)
	_, err := ni.Intercept(ctx, &workflowservice.PollWorkflowTaskQueueRequest{Namespace: testNamespace},
		&grpc.UnaryServerInfo{FullMethod: pollWorkflowTaskQueueMethod}, noopHandler)

	retryInfo := retryInfoDetail(err)
	s.Require().NotNil(retryInfo)
	s.Equal(2*time.Second, retryInfo.GetRetryDelay().AsDuration())
}

func (s *namespaceRateLimitInterceptorSuite) TestIntercept_WaitFailureGetsRetryDelayFloor() {
	s.mockRegistry.EXPECT().GetNamespace(namespace.Name(testNamespace)).Return(nil, nil)
	s.mockRateLimiter.EXPECT().Allow(gomock.Any(), gomock.Any()).Return(false)
	s.mockRateLimiter.EXPECT().Wait(gomock.Any(), gomock.Any()).Return(errors.New("wait failed"))

	ni := s.newImplWithRetryFloor(true, metrics.NoopMetricsHandler)
	_, err := ni.Intercept(context.Background(), &workflowservice.PollWorkflowTaskQueueRequest{Namespace: testNamespace},
		&grpc.UnaryServerInfo{FullMethod: pollWorkflowTaskQueueMethod}, noopHandler)

	retryInfo := retryInfoDetail(err)
	s.Require().NotNil(retryInfo)
	s.Equal(2*time.Second, retryInfo.GetRetryDelay().AsDuration())
}

func (s *namespaceRateLimitInterceptorSuite) TestAllowAndWaitKeepPlainSentinel() {
	s.mockRateLimiter.EXPECT().Allow(gomock.Any(), gomock.Any()).Return(false).Times(2)
	s.mockRateLimiter.EXPECT().Wait(gomock.Any(), gomock.Any()).Return(errors.New("wait failed"))
	mh := metricstest.NewCaptureHandler()
	capture := mh.StartCapture()
	defer mh.StopCapture(capture)
	ni := s.newImplWithRetryFloor(true, mh)

	s.Same(ErrNamespaceRateLimitServerBusy, ni.Allow(context.Background(), testNamespace, otherMethod, noopHeaderGetter{}))
	s.Same(ErrNamespaceRateLimitServerBusy, ni.Wait(context.Background(), testNamespace, otherMethod, noopHeaderGetter{}))
	s.Empty(capture.SnapshotMetric(metrics.ResourceExhaustedRetryDelayFloorEmitted.Name()))
}

func (s *namespaceRateLimitInterceptorSuite) TestIntercept_RateLimitedWithoutConfiguredFloorKeepsSentinel() {
	for _, tc := range []struct {
		name    string
		options []LimitInterceptorOption
	}{
		{name: "unset"},
		{
			name: "zero",
			options: []LimitInterceptorOption{
				WithResourceExhaustedRetryDelayFloor(func(string) time.Duration { return 0 }),
			},
		},
	} {
		s.Run(tc.name, func() {
			assertions := require.New(s.T())
			s.mockRegistry.EXPECT().GetNamespace(namespace.Name(testNamespace)).Return(nil, nil)
			s.mockRateLimiter.EXPECT().Allow(gomock.Any(), gomock.Any()).Return(false)
			mh := metricstest.NewCaptureHandler()
			capture := mh.StartCapture()
			defer mh.StopCapture(capture)
			ni := NewNamespaceRateLimitInterceptor(
				s.mockRegistry,
				s.mockRateLimiter,
				map[string]struct{}{},
				func(string) bool { return false },
				mh,
				tc.options...,
			)

			_, err := ni.Intercept(context.Background(), &workflowservice.StartWorkflowExecutionRequest{Namespace: testNamespace},
				&grpc.UnaryServerInfo{FullMethod: otherMethod}, noopHandler)
			assertions.Same(ErrNamespaceRateLimitServerBusy, err)
			assertions.ErrorIs(err, ErrNamespaceRateLimitServerBusy)
			assertions.Len(serviceerror.ToStatus(err).Proto().GetDetails(), 1)
			assertions.Nil(retryInfoDetail(err))
			assertions.Empty(capture.SnapshotMetric(metrics.ResourceExhaustedRetryDelayFloorEmitted.Name()))
		})
	}
}

func (s *namespaceRateLimitInterceptorSuite) TestIntercept_FrontendRetryDelaySettingDoesNotAffectNoOptionConstructor() {
	dynamicConfigClient := dynamicconfig.NewMemoryClient()
	removeOverride := dynamicConfigClient.OverrideSetting(dynamicconfig.FrontendResourceExhaustedRetryDelayFloor, 2*time.Second)
	defer removeOverride()
	dynamicConfig := dynamicconfig.NewCollection(dynamicConfigClient, log.NewNoopLogger())
	floor := dynamicconfig.FrontendResourceExhaustedRetryDelayFloor.Get(dynamicConfig)
	s.Equal(2*time.Second, floor(testNamespace))

	for _, tc := range []struct {
		name        string
		pollMethods map[string]struct{}
	}{
		{name: "history", pollMethods: map[string]struct{}{}},
		{name: "matching", pollMethods: configs.PollTaskAPISet},
	} {
		s.Run(tc.name, func() {
			assertions := require.New(s.T())
			s.mockRegistry.EXPECT().GetNamespace(namespace.Name(testNamespace)).Return(nil, nil)
			s.mockRateLimiter.EXPECT().Allow(gomock.Any(), gomock.Any()).Return(false)
			mh := metricstest.NewCaptureHandler()
			capture := mh.StartCapture()
			defer mh.StopCapture(capture)
			// History and matching construct the namespace limiter without frontend retry delay options.
			ni := NewNamespaceRateLimitInterceptor(
				s.mockRegistry,
				s.mockRateLimiter,
				tc.pollMethods,
				func(string) bool { return false },
				mh,
			)
			_, err := ni.Intercept(context.Background(), &workflowservice.PollWorkflowTaskQueueRequest{Namespace: testNamespace},
				&grpc.UnaryServerInfo{FullMethod: pollWorkflowTaskQueueMethod}, noopHandler)
			assertions.Same(ErrNamespaceRateLimitServerBusy, err)
			assertions.ErrorIs(err, ErrNamespaceRateLimitServerBusy)
			assertions.Len(serviceerror.ToStatus(err).Proto().GetDetails(), 1)
			assertions.Nil(retryInfoDetail(err))
			assertions.Empty(capture.SnapshotMetric(metrics.ResourceExhaustedRetryDelayFloorEmitted.Name()))
		})
	}
}

func (s *namespaceRateLimitInterceptorSuite) TestIntercept_HandlerErrorIsNotDecorated() {
	s.mockRegistry.EXPECT().GetNamespace(namespace.Name(testNamespace)).Return(nil, nil)
	s.mockRateLimiter.EXPECT().Allow(gomock.Any(), gomock.Any()).Return(true)
	mh := metricstest.NewCaptureHandler()
	capture := mh.StartCapture()
	defer mh.StopCapture(capture)
	ni := s.newImplWithRetryFloor(false, mh)

	_, err := ni.Intercept(context.Background(), &workflowservice.StartWorkflowExecutionRequest{Namespace: testNamespace},
		&grpc.UnaryServerInfo{FullMethod: otherMethod}, func(context.Context, any) (any, error) {
			return nil, ErrNamespaceRateLimitServerBusy
		})
	s.Same(ErrNamespaceRateLimitServerBusy, err)
	s.Nil(retryInfoDetail(err))
	s.Empty(capture.SnapshotMetric(metrics.ResourceExhaustedRetryDelayFloorEmitted.Name()))
}

func (s *namespaceRateLimitInterceptorSuite) TestIntercept_WaitCancellationIsNotDecorated() {
	s.mockRegistry.EXPECT().GetNamespace(namespace.Name(testNamespace)).Return(nil, nil)
	s.mockRateLimiter.EXPECT().Allow(gomock.Any(), gomock.Any()).Return(false)
	s.mockRateLimiter.EXPECT().Wait(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ quotas.Request) error {
			return ctx.Err()
		})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancel()
	mh := metricstest.NewCaptureHandler()
	capture := mh.StartCapture()
	defer mh.StopCapture(capture)
	ni := s.newImplWithRetryFloor(true, mh)

	_, err := ni.Intercept(ctx, &workflowservice.PollWorkflowTaskQueueRequest{Namespace: testNamespace},
		&grpc.UnaryServerInfo{FullMethod: pollWorkflowTaskQueueMethod}, noopHandler)
	s.ErrorIs(err, context.Canceled)
	s.Nil(retryInfoDetail(err))
	s.Empty(capture.SnapshotMetric(metrics.ResourceExhaustedRetryDelayFloorEmitted.Name()))
}

func (s *namespaceRateLimitInterceptorSuite) TestIntercept_RetryDelayFloorIsNamespaceScoped() {
	const firstNamespace = "first-namespace"
	const secondNamespace = "second-namespace"
	var floorNamespaces []string
	s.mockRegistry.EXPECT().GetNamespace(namespace.Name(firstNamespace)).Return(nil, nil)
	s.mockRegistry.EXPECT().GetNamespace(namespace.Name(secondNamespace)).Return(nil, nil)
	s.mockRateLimiter.EXPECT().Allow(gomock.Any(), gomock.Any()).Return(false).Times(2)
	mh := metricstest.NewCaptureHandler()
	capture := mh.StartCapture()
	defer mh.StopCapture(capture)
	ni := NewNamespaceRateLimitInterceptor(
		s.mockRegistry,
		s.mockRateLimiter,
		map[string]struct{}{},
		func(string) bool { return false },
		mh,
		WithResourceExhaustedRetryDelayFloor(func(namespace string) time.Duration {
			floorNamespaces = append(floorNamespaces, namespace)
			if namespace == firstNamespace {
				return 2 * time.Second
			}
			return 5 * time.Second
		}),
	)

	for _, tc := range []struct {
		namespace string
		wantDelay time.Duration
	}{
		{namespace: firstNamespace, wantDelay: 2 * time.Second},
		{namespace: secondNamespace, wantDelay: 5 * time.Second},
	} {
		_, err := ni.Intercept(context.Background(), &workflowservice.StartWorkflowExecutionRequest{Namespace: tc.namespace},
			&grpc.UnaryServerInfo{FullMethod: otherMethod}, noopHandler)
		retryInfo := retryInfoDetail(err)
		s.Require().NotNil(retryInfo)
		s.Equal(tc.wantDelay, retryInfo.GetRetryDelay().AsDuration())
	}
	recordings := capture.SnapshotMetric(metrics.ResourceExhaustedRetryDelayFloorEmitted.Name())
	s.Require().Len(recordings, 2)
	s.Equal(firstNamespace, recordings[0].Tags["namespace"])
	s.Equal(secondNamespace, recordings[1].Tags["namespace"])
	s.Equal([]string{firstNamespace, secondNamespace}, floorNamespaces)
}

// test namespace level override
func (s *namespaceRateLimitInterceptorSuite) TestIntercept_RetryDelayFloorNamespaceOverride() {
	const overrideNamespace = "override-namespace"
	const otherNamespace = "other-namespace"
	const lowerNamespace = "lower-namespace"
	dynamicConfigClient := dynamicconfig.NewMemoryClient()
	removeOverride := dynamicConfigClient.OverrideSetting(dynamicconfig.FrontendResourceExhaustedRetryDelayFloor, []dynamicconfig.ConstrainedValue{
		// The cell-wide value is listed first on purpose: dynamic config matches by constraint, not list order.
		{Value: time.Second},
		{
			Constraints: dynamicconfig.Constraints{Namespace: overrideNamespace},
			Value:       5 * time.Second,
		},
		{Constraints: dynamicconfig.Constraints{Namespace: lowerNamespace}, Value: 500 * time.Millisecond},
	})
	defer removeOverride()
	dynamicConfig := dynamicconfig.NewCollection(dynamicConfigClient, log.NewNoopLogger())
	floor := dynamicconfig.FrontendResourceExhaustedRetryDelayFloor.Get(dynamicConfig)

	s.mockRegistry.EXPECT().GetNamespace(namespace.Name(overrideNamespace)).Return(nil, nil)
	s.mockRegistry.EXPECT().GetNamespace(namespace.Name(otherNamespace)).Return(nil, nil)
	s.mockRegistry.EXPECT().GetNamespace(namespace.Name(lowerNamespace)).Return(nil, nil)
	s.mockRateLimiter.EXPECT().Allow(gomock.Any(), gomock.Any()).Return(false).Times(3)
	ni := NewNamespaceRateLimitInterceptor(
		s.mockRegistry,
		s.mockRateLimiter,
		map[string]struct{}{},
		func(string) bool { return false },
		metrics.NoopMetricsHandler,
		WithResourceExhaustedRetryDelayFloor(floor),
	)

	for _, tc := range []struct {
		namespace string
		wantDelay time.Duration
	}{
		{namespace: overrideNamespace, wantDelay: 5 * time.Second},
		{namespace: otherNamespace, wantDelay: time.Second},
		{namespace: lowerNamespace, wantDelay: 500 * time.Millisecond},
	} {
		_, err := ni.Intercept(context.Background(), &workflowservice.StartWorkflowExecutionRequest{Namespace: tc.namespace},
			&grpc.UnaryServerInfo{FullMethod: otherMethod}, noopHandler)
		retryInfo := retryInfoDetail(err)
		s.Require().NotNil(retryInfo)
		s.Equal(tc.wantDelay, retryInfo.GetRetryDelay().AsDuration())
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
