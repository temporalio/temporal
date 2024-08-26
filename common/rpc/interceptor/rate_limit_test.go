// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package interceptor

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/quotas"
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
