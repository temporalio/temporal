package interceptor

import (
	"context"
	"testing"

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
