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
	"errors"
	"net"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/internal/nettest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type rateLimitInterceptorTestCase struct {
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
	configure func(tc *rateLimitInterceptorTestCase)
}

type namespaceRateLimitInterceptorTestCase struct {
	// name of the test case
	name string
	// numRequests is the number of requests to send to the interceptor
	numRequests int
	// numVisibility is the number of visibility requests to send to the interceptor
	numVisibilityRequests int
	// numReplicationInducingRequests is the number of replication inducing requests to send to the interceptor
	numReplicationInducingRequests int
	// expectRateLimit is true if the interceptor should return a rate limit error
	expectRateLimit bool
	// frontendServiceCount is the number of frontend services returned by ServiceResolver to rate limiter
	frontendServiceCount int
	// Rate limiter config values
	globalNamespaceRPS                                                int
	maxNamespaceRPSPerInstance                                        int
	maxNamespaceBurstRatioPerInstance                                 float64
	globalNamespaceVisibilityRPS                                      int
	maxNamespaceVisibilityRPSPerInstance                              int
	maxNamespaceVisibilityBurstRatioPerInstance                       float64
	globalNamespaceNamespaceReplicationInducingAPIsRPS                int
	maxNamespaceNamespaceReplicationInducingAPIsRPSPerInstance        int
	maxNamespaceNamespaceReplicationInducingAPIsBurstRatioPerInstance float64
}

type rateLimitMetricsTestcase struct {
	// name of the test case
	name string
	// frontendServiceCount is the number of frontend services returned by ServiceResolver to rate limiter
	frontendServiceCount int
	// Rate limiter config values
	rps               int
	globalRPS         int
	expectedHostLimit int
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

	testCases := []rateLimitInterceptorTestCase{
		{
			name: "both rate limits hit",
			configure: func(tc *rateLimitInterceptorTestCase) {
				tc.globalRPSLimit = lowGlobalRPSLimit
				tc.perInstanceRPSLimit = lowPerInstanceRPSLimit
				tc.operatorRPSRatio = operatorRPSRatio
				tc.expectRateLimit = true
			},
		},
		{
			name: "global rate limit hit",
			configure: func(tc *rateLimitInterceptorTestCase) {
				tc.globalRPSLimit = lowGlobalRPSLimit
				tc.perInstanceRPSLimit = highPerInstanceRPSLimit
				tc.operatorRPSRatio = operatorRPSRatio
				tc.expectRateLimit = true
			},
		},
		{
			name: "per instance rate limit hit but ignored because global rate limit is not hit",
			configure: func(tc *rateLimitInterceptorTestCase) {
				tc.globalRPSLimit = highGlobalRPSLimit
				tc.perInstanceRPSLimit = lowPerInstanceRPSLimit
				tc.operatorRPSRatio = operatorRPSRatio
				tc.expectRateLimit = false
			},
		},
		{
			name: "neither rate limit hit",
			configure: func(tc *rateLimitInterceptorTestCase) {
				tc.globalRPSLimit = highGlobalRPSLimit
				tc.perInstanceRPSLimit = highPerInstanceRPSLimit
				tc.operatorRPSRatio = operatorRPSRatio
				tc.expectRateLimit = false
			},
		},
		{
			name: "global rate limit not configured and per instance rate limit not hit",
			configure: func(tc *rateLimitInterceptorTestCase) {
				tc.globalRPSLimit = 0
				tc.perInstanceRPSLimit = highPerInstanceRPSLimit
				tc.operatorRPSRatio = operatorRPSRatio
				tc.expectRateLimit = false
			},
		},
		{
			name: "global rate limit not configured and per instance rate limit is hit",
			configure: func(tc *rateLimitInterceptorTestCase) {
				tc.globalRPSLimit = 0
				tc.perInstanceRPSLimit = lowPerInstanceRPSLimit
				tc.operatorRPSRatio = operatorRPSRatio
				tc.expectRateLimit = true
			},
		},
		{
			name: "global rate limit not configured and zero per-instance rate limit",
			configure: func(tc *rateLimitInterceptorTestCase) {
				tc.globalRPSLimit = 0
				tc.perInstanceRPSLimit = 0
				tc.operatorRPSRatio = operatorRPSRatio
				tc.expectRateLimit = true
			},
		},
		{
			name: "nil service resolver causes global RPS limit to be ignored",
			configure: func(tc *rateLimitInterceptorTestCase) {
				tc.globalRPSLimit = lowPerInstanceRPSLimit
				tc.perInstanceRPSLimit = highPerInstanceRPSLimit
				tc.operatorRPSRatio = operatorRPSRatio
				tc.expectRateLimit = false
				tc.serviceResolver = nil
			},
		},
		{
			name: "no hosts causes global RPS limit to be ignored",
			configure: func(tc *rateLimitInterceptorTestCase) {
				tc.globalRPSLimit = lowPerInstanceRPSLimit
				tc.perInstanceRPSLimit = highPerInstanceRPSLimit
				tc.operatorRPSRatio = operatorRPSRatio
				tc.expectRateLimit = false
				serviceResolver := membership.NewMockServiceResolver(gomock.NewController(tc.t))
				serviceResolver.EXPECT().AvailableMemberCount().Return(0).AnyTimes()
				tc.serviceResolver = serviceResolver
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tc.numRequests = 10
			tc.t = t
			{
				// Create a mock service resolver which returns the number of frontend hosts.
				// This may be overridden by the test case.
				ctrl := gomock.NewController(t)
				serviceResolver := membership.NewMockServiceResolver(ctrl)
				serviceResolver.EXPECT().AvailableMemberCount().Return(numHosts).AnyTimes()
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
			}, tc.serviceResolver, metrics.NoopMetricsHandler, log.NewTestLogger())

			// Create a gRPC server for the fake workflow service.
			svc := &testSvc{}
			server := grpc.NewServer(grpc.ChainUnaryInterceptor(rpc.NewServiceErrorInterceptor(log.NewTestLogger()), rateLimitInterceptor.Intercept))
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
			var header metadata.MD

			// Generate load by sending a number of requests to the server.
			for i := 0; i < tc.numRequests; i++ {
				_, err = client.StartWorkflowExecution(
					context.Background(),
					&workflowservice.StartWorkflowExecutionRequest{},
					grpc.Header(&header),
				)
				if err != nil {
					break
				}
			}

			// Check if the rate limit is hit.
			if tc.expectRateLimit {
				assert.ErrorContains(t, err, "rate limit exceeded")
				s := status.Convert(err)
				var resourceExhausted *serviceerror.ResourceExhausted
				errors.As(serviceerror.FromStatus(s), &resourceExhausted)
				assert.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT, resourceExhausted.Cause)
				assert.Equal(t, enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM, resourceExhausted.Scope)

				assert.Len(t, header.Get(rpc.ResourceExhaustedCauseHeader), 1)
				assert.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT.String(), header.Get(rpc.ResourceExhaustedCauseHeader)[0])
				assert.Len(t, header.Get(rpc.ResourceExhaustedScopeHeader), 1)
				assert.Equal(t, enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM.String(), header.Get(rpc.ResourceExhaustedScopeHeader)[0])
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

// ListWorkflowExecution is a fake implementation of the ListWorkflowExecution gRPC method which does nothing.
func (t *testSvc) ListWorkflowExecutions(
	context.Context,
	*workflowservice.ListWorkflowExecutionsRequest,
) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	return &workflowservice.ListWorkflowExecutionsResponse{}, nil
}

// RegisterNamespace is a fake implementation of the RegisterNamespace gRPC method which does nothing.
func (t *testSvc) RegisterNamespace(
	context.Context,
	*workflowservice.RegisterNamespaceRequest,
) (*workflowservice.RegisterNamespaceResponse, error) {
	return &workflowservice.RegisterNamespaceResponse{}, nil
}

func TestNamespaceRateLimitInterceptorProvider(t *testing.T) {
	t.Parallel()
	testCases := []namespaceRateLimitInterceptorTestCase{
		{
			name:                              "namespace rate allow when burst ratio is 1",
			maxNamespaceRPSPerInstance:        10,
			maxNamespaceBurstRatioPerInstance: 1,
			numRequests:                       10,
			expectRateLimit:                   false,
		},
		{
			name:                              "namespace rate hit when burst ratio is 1",
			maxNamespaceRPSPerInstance:        10,
			maxNamespaceBurstRatioPerInstance: 1,
			numRequests:                       11,
			expectRateLimit:                   true,
		},
		{
			name:                              "namespace burst allow when burst ratio is 1.5",
			maxNamespaceRPSPerInstance:        10,
			maxNamespaceBurstRatioPerInstance: 1.5,
			numRequests:                       15,
			expectRateLimit:                   false,
		},
		{
			name:                              "namespace burst hit when burst ratio is 1.5",
			maxNamespaceRPSPerInstance:        10,
			maxNamespaceBurstRatioPerInstance: 1.5,
			numRequests:                       16,
			expectRateLimit:                   true,
		},
		{
			name:                              "namespace burst allow when burst ratio is 0.5",
			maxNamespaceRPSPerInstance:        10,
			maxNamespaceBurstRatioPerInstance: 0.5,
			numRequests:                       5,
			expectRateLimit:                   false,
		},
		{
			name:                              "namespace burst ratio does not apply for values < 1",
			maxNamespaceRPSPerInstance:        10,
			maxNamespaceBurstRatioPerInstance: 0.5,
			numRequests:                       10,
			expectRateLimit:                   false,
		},
		{
			name:                              "namespace burst allow when burst ratio is 1 and global limit is set",
			frontendServiceCount:              1,
			globalNamespaceRPS:                10,
			maxNamespaceRPSPerInstance:        5,
			maxNamespaceBurstRatioPerInstance: 1,
			numRequests:                       10,
			expectRateLimit:                   false,
		},
		{
			name:                              "namespace burst hit when burst ratio is 1 and global limit is set",
			frontendServiceCount:              1,
			globalNamespaceRPS:                5,
			maxNamespaceRPSPerInstance:        10,
			maxNamespaceBurstRatioPerInstance: 1,
			numRequests:                       6,
			expectRateLimit:                   true,
		},
		{
			name:                              "namespace burst allow when burst ratio is 1.5 and global limit is set",
			frontendServiceCount:              1,
			globalNamespaceRPS:                10,
			maxNamespaceRPSPerInstance:        5,
			maxNamespaceBurstRatioPerInstance: 1.5,
			numRequests:                       15,
			expectRateLimit:                   false,
		},
		{
			name:                              "namespace burst hit when burst ratio is 1.5 and global limit is set",
			frontendServiceCount:              1,
			globalNamespaceRPS:                5,
			maxNamespaceRPSPerInstance:        10,
			maxNamespaceBurstRatioPerInstance: 1.5,
			numRequests:                       8,
			expectRateLimit:                   true,
		},
		{
			name:                                 "visibility rate allow when burst ratio is 1",
			maxNamespaceVisibilityRPSPerInstance: 10,
			maxNamespaceVisibilityBurstRatioPerInstance: 1,
			numVisibilityRequests:                       10,
			expectRateLimit:                             false,
		},
		{
			name:                                 "visibility rate hit when burst ratio is 1",
			maxNamespaceVisibilityRPSPerInstance: 10,
			maxNamespaceVisibilityBurstRatioPerInstance: 1,
			numVisibilityRequests:                       11,
			expectRateLimit:                             true,
		},
		{
			name:                                 "visibility burst allow when burst ratio is 1.5",
			maxNamespaceVisibilityRPSPerInstance: 10,
			maxNamespaceVisibilityBurstRatioPerInstance: 1.5,
			numVisibilityRequests:                       15,
			expectRateLimit:                             false,
		},
		{
			name:                                 "visibility burst hit when burst ratio is 1.5",
			maxNamespaceVisibilityRPSPerInstance: 10,
			maxNamespaceVisibilityBurstRatioPerInstance: 1.5,
			numVisibilityRequests:                       16,
			expectRateLimit:                             true,
		},
		{
			name:                                 "visibility burst allow when burst ratio is 0.5",
			maxNamespaceVisibilityRPSPerInstance: 10,
			maxNamespaceVisibilityBurstRatioPerInstance: 0.5,
			numVisibilityRequests:                       5,
			expectRateLimit:                             false,
		},
		{
			name:                                 "visibility burst ratio does not apply for values < 1",
			maxNamespaceVisibilityRPSPerInstance: 10,
			maxNamespaceVisibilityBurstRatioPerInstance: 0.5,
			numVisibilityRequests:                       10,
			expectRateLimit:                             false,
		},
		{
			name:                                 "visibility burst allow when burst ratio is 1 and global limit is set",
			frontendServiceCount:                 1,
			globalNamespaceVisibilityRPS:         10,
			maxNamespaceVisibilityRPSPerInstance: 5,
			maxNamespaceVisibilityBurstRatioPerInstance: 1,
			numVisibilityRequests:                       10,
			expectRateLimit:                             false,
		},
		{
			name:                                 "visibility burst hit when burst ratio is 1 and global limit is set",
			frontendServiceCount:                 1,
			globalNamespaceVisibilityRPS:         5,
			maxNamespaceVisibilityRPSPerInstance: 10,
			maxNamespaceVisibilityBurstRatioPerInstance: 1,
			numVisibilityRequests:                       6,
			expectRateLimit:                             true,
		},
		{
			name:                                 "visibility burst allow when burst ratio is 1.5 and global limit is set",
			frontendServiceCount:                 1,
			globalNamespaceVisibilityRPS:         10,
			maxNamespaceVisibilityRPSPerInstance: 5,
			maxNamespaceVisibilityBurstRatioPerInstance: 1.5,
			numVisibilityRequests:                       15,
			expectRateLimit:                             false,
		},
		{
			name:                                 "visibility burst hit when burst ratio is 1.5 and global limit is set",
			frontendServiceCount:                 1,
			globalNamespaceVisibilityRPS:         5,
			maxNamespaceVisibilityRPSPerInstance: 10,
			maxNamespaceVisibilityBurstRatioPerInstance: 1.5,
			numVisibilityRequests:                       8,
			expectRateLimit:                             true,
		},
		{
			name: "replication inducing op rate allow when burst ratio is 1",
			maxNamespaceNamespaceReplicationInducingAPIsRPSPerInstance:        10,
			maxNamespaceNamespaceReplicationInducingAPIsBurstRatioPerInstance: 1,
			numReplicationInducingRequests:                                    10,
			expectRateLimit:                                                   false,
		},
		{
			name: "replication inducing op rate hit when burst ratio is 1",
			maxNamespaceNamespaceReplicationInducingAPIsRPSPerInstance:        10,
			maxNamespaceNamespaceReplicationInducingAPIsBurstRatioPerInstance: 1,
			numReplicationInducingRequests:                                    11,
			expectRateLimit:                                                   true,
		},
		{
			name: "replication inducing op burst allow when burst ratio is 1.5",
			maxNamespaceNamespaceReplicationInducingAPIsRPSPerInstance:        10,
			maxNamespaceNamespaceReplicationInducingAPIsBurstRatioPerInstance: 1.5,
			numReplicationInducingRequests:                                    15,
			expectRateLimit:                                                   false,
		},
		{
			name: "replication inducing op burst hit when burst ratio is 1.5",
			maxNamespaceNamespaceReplicationInducingAPIsRPSPerInstance:        10,
			maxNamespaceNamespaceReplicationInducingAPIsBurstRatioPerInstance: 1.5,
			numReplicationInducingRequests:                                    16,
			expectRateLimit:                                                   true,
		},
		{
			name: "replication inducing op burst allow when burst ratio is 0.5",
			maxNamespaceNamespaceReplicationInducingAPIsRPSPerInstance:        10,
			maxNamespaceNamespaceReplicationInducingAPIsBurstRatioPerInstance: 0.5,
			numReplicationInducingRequests:                                    5,
			expectRateLimit:                                                   false,
		},
		{
			name: "replication inducing op burst ratio does not apply for values < 1",
			maxNamespaceNamespaceReplicationInducingAPIsRPSPerInstance:        10,
			maxNamespaceNamespaceReplicationInducingAPIsBurstRatioPerInstance: 0.5,
			numReplicationInducingRequests:                                    10,
			expectRateLimit:                                                   false,
		},
		{
			name:                 "replication inducing op burst allow when burst ratio is 1 and global limit is set",
			frontendServiceCount: 1,
			globalNamespaceNamespaceReplicationInducingAPIsRPS:                10,
			maxNamespaceNamespaceReplicationInducingAPIsRPSPerInstance:        5,
			maxNamespaceNamespaceReplicationInducingAPIsBurstRatioPerInstance: 1,
			numReplicationInducingRequests:                                    10,
			expectRateLimit:                                                   false,
		},
		{
			name:                 "replication inducing op burst hit when burst ratio is 1 and global limit is set",
			frontendServiceCount: 1,
			globalNamespaceNamespaceReplicationInducingAPIsRPS:                5,
			maxNamespaceNamespaceReplicationInducingAPIsRPSPerInstance:        10,
			maxNamespaceNamespaceReplicationInducingAPIsBurstRatioPerInstance: 1,
			numReplicationInducingRequests:                                    6,
			expectRateLimit:                                                   true,
		},
		{
			name:                 "replication inducing op burst allow when burst ratio is 1.5 and global limit is set",
			frontendServiceCount: 1,
			globalNamespaceNamespaceReplicationInducingAPIsRPS:                10,
			maxNamespaceNamespaceReplicationInducingAPIsRPSPerInstance:        5,
			maxNamespaceNamespaceReplicationInducingAPIsBurstRatioPerInstance: 1.5,
			numReplicationInducingRequests:                                    15,
			expectRateLimit:                                                   false,
		},
		{
			name:                 "replication inducing op burst hit when burst ratio is 1.5 and global limit is set",
			frontendServiceCount: 1,
			globalNamespaceNamespaceReplicationInducingAPIsRPS:                5,
			maxNamespaceNamespaceReplicationInducingAPIsRPSPerInstance:        10,
			maxNamespaceNamespaceReplicationInducingAPIsBurstRatioPerInstance: 1.5,
			numReplicationInducingRequests:                                    8,
			expectRateLimit:                                                   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			namespaceName := "test-namespace"
			mockRegistry := namespace.NewMockRegistry(gomock.NewController(t))
			mockRegistry.EXPECT().GetNamespace(namespace.Name(namespaceName)).Return(&namespace.Namespace{}, nil).AnyTimes()
			serviceResolver := membership.NewMockServiceResolver(gomock.NewController(t))
			serviceResolver.EXPECT().AvailableMemberCount().Return(tc.frontendServiceCount).AnyTimes()

			config := getTestConfig(tc)

			// Create a rate limit interceptor.
			rateLimitInterceptor := NamespaceRateLimitInterceptorProvider(
				primitives.FrontendService,
				&config,
				mockRegistry,
				serviceResolver,
				log.NewTestLogger(),
			)

			// Create a gRPC server for the fake workflow service.
			svc := &testSvc{}
			server := grpc.NewServer(grpc.ChainUnaryInterceptor(rpc.NewServiceErrorInterceptor(log.NewTestLogger()), rateLimitInterceptor.Intercept))
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
			var header metadata.MD

			defer func() {
				// Check if the rate limit is hit.
				if tc.expectRateLimit {
					assert.ErrorContains(t, err, "rate limit exceeded")
					s := status.Convert(err)
					var resourceExhausted *serviceerror.ResourceExhausted
					errors.As(serviceerror.FromStatus(s), &resourceExhausted)
					assert.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT, resourceExhausted.Cause)
					assert.Equal(t, enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE, resourceExhausted.Scope)

					assert.Len(t, header.Get(rpc.ResourceExhaustedCauseHeader), 1)
					assert.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT.String(), header.Get(rpc.ResourceExhaustedCauseHeader)[0])
					assert.Len(t, header.Get(rpc.ResourceExhaustedScopeHeader), 1)
					assert.Equal(t, enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE.String(), header.Get(rpc.ResourceExhaustedScopeHeader)[0])
				} else {
					assert.NoError(t, err)
				}
			}()

			// Generate load by sending a number of requests to the server.
			for i := 0; i < tc.numRequests; i++ {
				_, err = client.StartWorkflowExecution(
					context.Background(),
					&workflowservice.StartWorkflowExecutionRequest{
						Namespace: namespaceName,
					},
					grpc.Header(&header),
				)
				if err != nil {
					return
				}
			}

			for i := 0; i < tc.numVisibilityRequests; i++ {
				_, err = client.ListWorkflowExecutions(
					context.Background(),
					&workflowservice.ListWorkflowExecutionsRequest{
						Namespace: namespaceName,
					},
					grpc.Header(&header),
				)
				if err != nil {
					return
				}
			}

			for i := 0; i < tc.numReplicationInducingRequests; i++ {
				_, err = client.RegisterNamespace(
					context.Background(),
					&workflowservice.RegisterNamespaceRequest{
						Namespace: namespaceName,
					},
					grpc.Header(&header),
				)
				if err != nil {
					return
				}
			}
		})
	}
}

func getTestConfig(tc namespaceRateLimitInterceptorTestCase) Config {
	return Config{
		OperatorRPSRatio: func() float64 {
			return 0.20
		},
		GlobalNamespaceRPS: func(namespace string) int {
			return getOrDefaultLimit(tc.globalNamespaceRPS)
		},
		GlobalNamespaceVisibilityRPS: func(namespace string) int {
			return getOrDefaultLimit(tc.globalNamespaceVisibilityRPS)
		},
		GlobalNamespaceNamespaceReplicationInducingAPIsRPS: func(namespace string) int {
			return getOrDefaultLimit(tc.globalNamespaceNamespaceReplicationInducingAPIsRPS)
		},
		MaxNamespaceRPSPerInstance: func(namespace string) int {
			return getOrDefaultLimit(tc.maxNamespaceRPSPerInstance)
		},
		MaxNamespaceBurstRatioPerInstance: func(namespace string) float64 {
			return tc.maxNamespaceBurstRatioPerInstance
		},
		MaxNamespaceVisibilityRPSPerInstance: func(namespace string) int {
			return getOrDefaultLimit(tc.maxNamespaceVisibilityRPSPerInstance)
		},
		MaxNamespaceVisibilityBurstRatioPerInstance: func(namespace string) float64 {
			return getOrDefaultLimit(tc.maxNamespaceVisibilityBurstRatioPerInstance)
		},
		MaxNamespaceNamespaceReplicationInducingAPIsRPSPerInstance: func(namespace string) int {
			return getOrDefaultLimit(tc.maxNamespaceNamespaceReplicationInducingAPIsRPSPerInstance)
		},
		MaxNamespaceNamespaceReplicationInducingAPIsBurstRatioPerInstance: func(namespace string) float64 {
			return getOrDefaultLimit(tc.maxNamespaceNamespaceReplicationInducingAPIsBurstRatioPerInstance)
		},
	}
}

func TestNamespaceRateLimitMetrics(t *testing.T) {
	testCases := []rateLimitMetricsTestcase{
		{
			name:                 "global limit is emitted when set",
			rps:                  100,
			globalRPS:            10,
			frontendServiceCount: 5,
			expectedHostLimit:    2, // This should be globalRPS/frontendServiceCount
		},
		{
			name:                 "host limit emitted when global limit is not set",
			rps:                  10,
			globalRPS:            -1,
			frontendServiceCount: 0,
			expectedHostLimit:    10,
		},
		{
			name:                 "host limit emitted when frontend service count is not set",
			rps:                  10,
			globalRPS:            100,
			frontendServiceCount: 0,
			expectedHostLimit:    10,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			testNS := "test_namespace"
			mockRegistry := namespace.NewMockRegistry(gomock.NewController(t))
			mockRegistry.EXPECT().GetNamespace(namespace.Name(testNS)).Return(&namespace.Namespace{}, nil).AnyTimes()
			serviceResolver := membership.NewMockServiceResolver(gomock.NewController(t))
			serviceResolver.EXPECT().AvailableMemberCount().Return(tc.frontendServiceCount).AnyTimes()
			metricsHandler := metricstest.NewCaptureHandler()
			capture := metricsHandler.StartCapture()

			config := &Config{
				RPS: func() int {
					return getOrDefaultLimit(tc.rps)
				},
				GlobalRPS: func() int {
					return getOrDefaultLimit(tc.globalRPS)
				},
				// Values below this are not used in this test. But they are required to initialize rate limiter.
				OperatorRPSRatio: func() float64 {
					return 0.2
				},
				NamespaceReplicationInducingAPIsRPS: func() int {
					return 1
				},
			}

			// Create a rate limit interceptor which uses the per-instance and global RPS limits from the test case.
			rateLimitInterceptor := RateLimitInterceptorProvider(config, serviceResolver, metricsHandler, log.NewTestLogger())

			// Create a gRPC server for the fake workflow service.
			svc := &testSvc{}
			server := grpc.NewServer(
				grpc.ChainUnaryInterceptor(
					rpc.NewServiceErrorInterceptor(log.NewTestLogger()),
					rateLimitInterceptor.Intercept,
				),
			)
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

			_, _ = client.StartWorkflowExecution(
				context.Background(),
				&workflowservice.StartWorkflowExecutionRequest{
					Namespace: testNS,
				},
			)

			// Check if limits are emitted by metrics handler
			snapshot := capture.Snapshot()
			for _, limit := range snapshot[metrics.HostRPSLimit.Name()] {
				assert.Equal(t, float64(tc.expectedHostLimit), limit.Value)
			}
		})
	}
}

func getOrDefaultLimit[T int | float64](limit T) T {
	if limit == 0 {
		return 100
	}
	return limit
}
