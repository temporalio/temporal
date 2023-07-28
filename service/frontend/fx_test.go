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

package frontend

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/internal/nettest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type testCase struct {
	// name of the test case
	name string
	// t is the test object
	t *testing.T
	// globalRPSLimit is the global RPS limit for all frontend hosts
	globalRPSLimit int
	// perInstanceRPSLimit is the RPS limit for each frontend host
	perInstanceRPSLimit int
	// operatorRPSRatio is the ratio of the global RPS limit that is reserved for operator requests
	operatorRPSRatio float64
	// expectRateLimit is true if the interceptor should return a rate limit error
	expectRateLimit bool
	// numRequests is the number of requests to send to the interceptor
	numRequests int
	// serviceResolver is used to determine the number of frontend hosts for the global rate limiter
	serviceResolver membership.ServiceResolver
	// configure is a function that can be used to override the default test case values
	configure func(tc *testCase)
}

func TestRateLimitInterceptorProvider(t *testing.T) {
	t.Parallel()

	// The burst limit is 2 * the rps limit, so this is 8, which is < the number of requests. The interceptor should
	// rate limit one of the last two requests because we exceeded the burst limit, and there is no delay between the
	// last two requests.
	lowPerInstanceRPSLimit := 4
	// The burst limit is 2 * the rps limit, so this is 10, which is >= the number of requests. The interceptor should
	// not rate limit any of the requests because we never exceed the burst limit.
	highPerInstanceRPSLimit := 5
	// The number of hosts is 10, so if 4 is too low of an RPS limit per-instance, then 40 is too low of a global RPS
	// limit.
	numHosts := 10
	lowGlobalRPSLimit := lowPerInstanceRPSLimit * numHosts
	highGlobalRPSLimit := highPerInstanceRPSLimit * numHosts
	operatorRPSRatio := 0.2

	testCases := []testCase{
		{
			name: "both rate limits hit",
			configure: func(tc *testCase) {
				tc.globalRPSLimit = lowGlobalRPSLimit
				tc.perInstanceRPSLimit = lowPerInstanceRPSLimit
				tc.operatorRPSRatio = operatorRPSRatio
				tc.expectRateLimit = true
			},
		},
		{
			name: "global rate limit hit",
			configure: func(tc *testCase) {
				tc.globalRPSLimit = lowGlobalRPSLimit
				tc.perInstanceRPSLimit = highPerInstanceRPSLimit
				tc.operatorRPSRatio = operatorRPSRatio
				tc.expectRateLimit = true
			},
		},
		{
			name: "per instance rate limit hit but ignored because global rate limit is not hit",
			configure: func(tc *testCase) {
				tc.globalRPSLimit = highGlobalRPSLimit
				tc.perInstanceRPSLimit = lowPerInstanceRPSLimit
				tc.operatorRPSRatio = operatorRPSRatio
				tc.expectRateLimit = false
			},
		},
		{
			name: "neither rate limit hit",
			configure: func(tc *testCase) {
				tc.globalRPSLimit = highGlobalRPSLimit
				tc.perInstanceRPSLimit = highPerInstanceRPSLimit
				tc.operatorRPSRatio = operatorRPSRatio
				tc.expectRateLimit = false
			},
		},
		{
			name: "global rate limit not configured and per instance rate limit not hit",
			configure: func(tc *testCase) {
				tc.globalRPSLimit = 0
				tc.perInstanceRPSLimit = highPerInstanceRPSLimit
				tc.operatorRPSRatio = operatorRPSRatio
				tc.expectRateLimit = false
			},
		},
		{
			name: "global rate limit not configured and per instance rate limit is hit",
			configure: func(tc *testCase) {
				tc.globalRPSLimit = 0
				tc.perInstanceRPSLimit = lowPerInstanceRPSLimit
				tc.operatorRPSRatio = operatorRPSRatio
				tc.expectRateLimit = true
			},
		},
		{
			name: "global rate limit not configured and zero per-instance rate limit",
			configure: func(tc *testCase) {
				tc.globalRPSLimit = 0
				tc.perInstanceRPSLimit = 0
				tc.operatorRPSRatio = operatorRPSRatio
				tc.expectRateLimit = true
			},
		},
		{
			name: "nil service resolver causes global RPS limit to be ignored",
			configure: func(tc *testCase) {
				tc.globalRPSLimit = lowPerInstanceRPSLimit
				tc.perInstanceRPSLimit = highPerInstanceRPSLimit
				tc.operatorRPSRatio = operatorRPSRatio
				tc.expectRateLimit = false
				tc.serviceResolver = nil
			},
		},
		{
			name: "no hosts causes global RPS limit to be ignored",
			configure: func(tc *testCase) {
				tc.globalRPSLimit = lowPerInstanceRPSLimit
				tc.perInstanceRPSLimit = highPerInstanceRPSLimit
				tc.operatorRPSRatio = operatorRPSRatio
				tc.expectRateLimit = false
				serviceResolver := membership.NewMockServiceResolver(gomock.NewController(tc.t))
				serviceResolver.EXPECT().MemberCount().Return(0).AnyTimes()
				tc.serviceResolver = serviceResolver
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tc.numRequests = 10
			tc.t = t
			{
				// Create a mock service resolver which returns the number of frontend hosts.
				// This may be overridden by the test case.
				ctrl := gomock.NewController(t)
				serviceResolver := membership.NewMockServiceResolver(ctrl)
				serviceResolver.EXPECT().MemberCount().Return(numHosts).AnyTimes()
				tc.serviceResolver = serviceResolver
			}
			tc.configure(&tc)

			// Create a rate limit interceptor which uses the per-instance and global RPS limits from the test case.
			rateLimitInterceptor := RateLimitInterceptorProvider(&Config{
				RPS: func() int {
					return tc.perInstanceRPSLimit
				},
				GlobalRPS: func() int {
					return tc.globalRPSLimit
				},
				NamespaceReplicationInducingAPIsRPS: func() int {
					// this is not used in this test
					return 0
				},
				OperatorRPSRatio: func() float64 {
					return tc.operatorRPSRatio
				},
			}, tc.serviceResolver)

			// Create a gRPC server for the fake workflow service.
			svc := &testSvc{}
			server := grpc.NewServer(grpc.UnaryInterceptor(rateLimitInterceptor.Intercept))
			workflowservice.RegisterWorkflowServiceServer(server, svc)

			pipe := nettest.NewPipe()

			var wg sync.WaitGroup
			defer wg.Wait()
			wg.Add(1)

			listener := nettest.NewListener(pipe)
			go func() {
				defer wg.Done()

				_ = server.Serve(listener)
			}()

			// Create a gRPC client to the fake workflow service.
			dialer := grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return pipe.Connect(ctx.Done())
			})
			transportCredentials := grpc.WithTransportCredentials(insecure.NewCredentials())
			conn, err := grpc.DialContext(context.Background(), "fake", dialer, transportCredentials)
			require.NoError(t, err)

			defer server.Stop()

			client := workflowservice.NewWorkflowServiceClient(conn)

			// Generate load by sending a number of requests to the server.
			for i := 0; i < tc.numRequests; i++ {
				_, err = client.StartWorkflowExecution(
					context.Background(),
					&workflowservice.StartWorkflowExecutionRequest{},
				)
				if err != nil {
					break
				}
			}

			// Check if the rate limit is hit.
			if tc.expectRateLimit {
				assert.ErrorContains(t, err, "rate limit exceeded")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// testSvc is a fake workflow service.
type testSvc struct {
	workflowservice.UnimplementedWorkflowServiceServer
}

// StartWorkflowExecution is a fake implementation of the StartWorkflowExecution gRPC method which does nothing.
func (t *testSvc) StartWorkflowExecution(
	context.Context,
	*workflowservice.StartWorkflowExecutionRequest,
) (*workflowservice.StartWorkflowExecutionResponse, error) {
	return &workflowservice.StartWorkflowExecutionResponse{}, nil
}
