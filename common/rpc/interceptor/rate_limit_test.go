package interceptor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/headers"
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

// Caller-name forwarding is required by per-namespace priority functions
// such as the fairness limiter.
func (s *rateLimitInterceptorSuite) TestInterceptPropagatesCallerName() {
	methodName := "TEST/METHOD"
	interceptor := NewRateLimitInterceptor(s.mockRateLimiter, nil)

	var captured quotas.Request
	s.mockRateLimiter.EXPECT().
		Allow(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ time.Time, req quotas.Request) bool {
			captured = req
			return true
		})

	ctx := headers.SetCallerType(
		headers.SetCallerName(context.Background(), "ns-a"),
		headers.CallerTypeAPI,
	)
	handler := func(ctx context.Context, req any) (any, error) { return nil, nil }
	_, err := interceptor.Intercept(ctx, nil, &grpc.UnaryServerInfo{FullMethod: methodName}, handler)
	s.NoError(err)
	s.Equal("ns-a", captured.Caller)
	s.Equal(headers.CallerTypeAPI, captured.CallerType)
	s.Equal(methodName, captured.API)
}
