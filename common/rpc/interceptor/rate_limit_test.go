package interceptor

import (
	"context"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/quotas"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

type (
	// rateLimitInterceptorSuite struct {
	rateLimitInterceptorSuite struct {
		suite.Suite
		*require.Assertions

		controller      *gomock.Controller
		mockRateLimiter *quotas.MockRequestRateLimiter
	}
)

func TestRateLimitInterceptorSuite(t *testing.T) {
	suite.Run(t, &rateLimitInterceptorSuite{})
}

func (s *rateLimitInterceptorSuite) TestInterceptNexus() {
	for _, tc := range []struct {
		name            string
		apiName         string
		input           NexusInterceptorInput
		allow           *bool
		nextCalled      bool
		expectedOutcome string
	}{
		{name: "allowed", apiName: "NexusOperation", input: NewStartNexusOpInput("service", "operation", testNamespace, nexus.StartOperationOptions{}, nil), allow: new(true), nextCalled: true},
		{name: "rate limited", apiName: "NexusOperation", input: NewStartNexusOpInput("service", "operation", testNamespace, nexus.StartOperationOptions{}, nil), allow: new(false), expectedOutcome: "global_rate_limited"},
		{name: "missing API name", expectedOutcome: "interceptor_failed"},
		{name: "missing request header", apiName: "NexusOperation", input: NewCompleteNexusOpInput(testNamespace, nil), expectedOutcome: "interceptor_failed"},
	} {
		s.Run(tc.name, func() {
			ctx := context.Background()
			interceptor := NewRateLimitInterceptor(s.mockRateLimiter, nil)
			if tc.apiName != "" {
				ctx = WithNexusAPIName(ctx, tc.apiName)
			}
			if tc.allow != nil {
				s.mockRateLimiter.EXPECT().Allow(gomock.Any(), gomock.Any()).Return(*tc.allow)
			}
			input := tc.input
			if input == nil {
				input = NewStartNexusOpInput("service", "operation", testNamespace, nexus.StartOperationOptions{}, nil)
			}
			nextCalled := false
			_, err := interceptor.InterceptNexus(
				ctx,
				input,
				func(context.Context, NexusInterceptorInput) (any, error) {
					nextCalled = true
					return nil, nil
				},
			)
			if tc.expectedOutcome != "" {
				var interceptorErr *InterceptorError
				s.ErrorAs(err, &interceptorErr)
				s.Equal(tc.expectedOutcome, interceptorErr.Outcome)
			} else {
				s.NoError(err)
			}
			s.Equal(tc.nextCalled, nextCalled)
		})
	}
}

func (s *rateLimitInterceptorSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.mockRateLimiter = quotas.NewMockRequestRateLimiter(s.controller)
}

func (s *rateLimitInterceptorSuite) TestInterceptWithTokenConfig() {
	methodName := "TEST/METHOD"
	interceptor := NewRateLimitInterceptor(s.mockRateLimiter, map[string]int{methodName: 0})
	// mock rate limiter should not be called.
	s.mockRateLimiter.EXPECT().Allow(gomock.Any(), gomock.Any()).MaxTimes(0).Return(false)

	handlerCalled := false
	handler := func(ctx context.Context, req any) (any, error) {
		handlerCalled = true
		return nil, nil
	}
	_, err := interceptor.Intercept(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: methodName}, handler)
	s.NoError(err)
	s.True(handlerCalled)
}

func (s *rateLimitInterceptorSuite) TestInterceptWithNoTokenConfig() {
	interceptor := NewRateLimitInterceptor(s.mockRateLimiter, nil)
	// mock rate limiter is set to blocking.
	s.mockRateLimiter.EXPECT().Allow(gomock.Any(), gomock.Any()).MaxTimes(1).Return(false)

	handlerCalled := false
	handler := func(ctx context.Context, req any) (any, error) {
		handlerCalled = true
		return nil, nil
	}
	_, err := interceptor.Intercept(context.Background(), nil, &grpc.UnaryServerInfo{}, handler)
	s.Error(err)
	s.False(handlerCalled)
}

func (s *rateLimitInterceptorSuite) TestInterceptWithNonZeroTokenConfig() {
	methodName := "TEST/METHOD"
	interceptor := NewRateLimitInterceptor(s.mockRateLimiter, map[string]int{methodName: 100})
	// mock rate limiter is set to non-blocking.
	s.mockRateLimiter.EXPECT().Allow(gomock.Any(), gomock.Any()).MaxTimes(1).Return(true)

	handlerCalled := false
	handler := func(ctx context.Context, req any) (any, error) {
		handlerCalled = true
		return nil, nil
	}
	_, err := interceptor.Intercept(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: methodName}, handler)
	s.NoError(err)
	s.True(handlerCalled)
}
