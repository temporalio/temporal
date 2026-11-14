package frontend

import (
	"context"
	"errors"
	"net"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/draft"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/testing/nettest"
	"go.temporal.io/server/common/testing/testhooks"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type rateLimitInterceptorTestCase struct {
	// name of the test case
	name string
	// t is the test object
	t *testing.T
	// ctrl is the mock controller
	ctrl *gomock.Controller
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
				serviceResolver := membership.NewMockServiceResolver(tc.ctrl)
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
			tc.ctrl = gomock.NewController(t)
			{
				// Create a mock service resolver which returns the number of frontend hosts.
				// This may be overridden by the test case.
				serviceResolver := membership.NewMockServiceResolver(tc.ctrl)
				serviceResolver.EXPECT().AvailableMemberCount().Return(numHosts).AnyTimes()
				tc.serviceResolver = serviceResolver
			}
			tc.configure(&tc)

			serviceErrorInterceptor := interceptor.NewServiceErrorInterceptor(
				dynamicconfig.GetIntPropertyFn(4000),
				metrics.NewMockHandler(tc.ctrl),
				log.NewTestLogger(),
			)

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
			server := grpc.NewServer(grpc.ChainUnaryInterceptor(
				serviceErrorInterceptor.Intercept,
				interceptor.NewFrontendServiceErrorInterceptor(log.NewTestLogger()),
				rateLimitInterceptor.Intercept,
			))
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
			conn, err := grpc.NewClient("localhost", dialer, transportCredentials)
			require.NoError(t, err)
			conn.Connect()
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

				assert.Len(t, header.Get(interceptor.ResourceExhaustedCauseHeader), 1)
				assert.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT.String(), header.Get(interceptor.ResourceExhaustedCauseHeader)[0])
				assert.Len(t, header.Get(interceptor.ResourceExhaustedScopeHeader), 1)
				assert.Equal(t, enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM.String(), header.Get(interceptor.ResourceExhaustedScopeHeader)[0])
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
			name:                              "namespace burst hit when burst ratio is 0.5",
			maxNamespaceRPSPerInstance:        10,
			maxNamespaceBurstRatioPerInstance: 0.5,
			numRequests:                       6,
			expectRateLimit:                   true,
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
			numRequests:                       9,
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
			name:                                 "visibility burst hit when burst ratio is 0.5",
			maxNamespaceVisibilityRPSPerInstance: 10,
			maxNamespaceVisibilityBurstRatioPerInstance: 0.5,
			numVisibilityRequests:                       6,
			expectRateLimit:                             true,
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
			numVisibilityRequests:                       9,
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
			name: "replication inducing op burst hit when burst ratio is 0.5",
			maxNamespaceNamespaceReplicationInducingAPIsRPSPerInstance:        10,
			maxNamespaceNamespaceReplicationInducingAPIsBurstRatioPerInstance: 0.5,
			numReplicationInducingRequests:                                    6,
			expectRateLimit:                                                   true,
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
			numReplicationInducingRequests:                                    9,
			expectRateLimit:                                                   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)

			namespaceName := "test-namespace"
			mockRegistry := namespace.NewMockRegistry(ctrl)
			mockRegistry.EXPECT().GetNamespace(namespace.Name(namespaceName)).Return(&namespace.Namespace{}, nil).AnyTimes()
			serviceResolver := membership.NewMockServiceResolver(ctrl)
			serviceResolver.EXPECT().AvailableMemberCount().Return(tc.frontendServiceCount).AnyTimes()

			config := getTestConfig(tc)

			serviceErrorInterceptor := interceptor.NewServiceErrorInterceptor(
				dynamicconfig.GetIntPropertyFn(4000),
				metrics.NewMockHandler(ctrl),
				log.NewTestLogger(),
			)

			// Create a rate limit interceptor.
			rateLimiters := NamespaceRateLimitersProvider(
				primitives.FrontendService,
				&config,
				serviceResolver,
				log.NewTestLogger(),
			)
			rateLimitInterceptor := NamespaceRateLimitInterceptorProvider(
				mockRegistry,
				rateLimiters,
				&config,
				metrics.NoopMetricsHandler,
			)

			// Create a gRPC server for the fake workflow service.
			svc := &testSvc{}
			server := grpc.NewServer(grpc.ChainUnaryInterceptor(
				serviceErrorInterceptor.Intercept,
				interceptor.NewFrontendServiceErrorInterceptor(log.NewTestLogger()),
				rateLimitInterceptor.Intercept,
			))
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
			conn, err := grpc.NewClient("localhost", dialer, transportCredentials)
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

					assert.Len(t, header.Get(interceptor.ResourceExhaustedCauseHeader), 1)
					assert.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT.String(), header.Get(interceptor.ResourceExhaustedCauseHeader)[0])
					assert.Len(t, header.Get(interceptor.ResourceExhaustedScopeHeader), 1)
					assert.Equal(t, enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE.String(), header.Get(interceptor.ResourceExhaustedScopeHeader)[0])
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
		PollWaitForNamespaceRateLimitToken: func(_ string) bool { return false },
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
			ctrl := gomock.NewController(t)

			testNS := "test_namespace"
			mockRegistry := namespace.NewMockRegistry(ctrl)
			mockRegistry.EXPECT().GetNamespace(namespace.Name(testNS)).Return(&namespace.Namespace{}, nil).AnyTimes()
			serviceResolver := membership.NewMockServiceResolver(ctrl)
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

			serviceErrorInterceptor := interceptor.NewServiceErrorInterceptor(
				dynamicconfig.GetIntPropertyFn(4000),
				metrics.NewMockHandler(ctrl),
				log.NewTestLogger(),
			)

			// Create a rate limit interceptor which uses the per-instance and global RPS limits from the test case.
			rateLimitInterceptor := RateLimitInterceptorProvider(config, serviceResolver, metricsHandler, log.NewTestLogger())

			// Create a gRPC server for the fake workflow service.
			svc := &testSvc{}
			server := grpc.NewServer(grpc.ChainUnaryInterceptor(
				serviceErrorInterceptor.Intercept,
				interceptor.NewFrontendServiceErrorInterceptor(log.NewTestLogger()),
				rateLimitInterceptor.Intercept,
			))
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
			conn, err := grpc.NewClient("localhost", dialer, transportCredentials)
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

// The remaining interceptors are independently tested; keep the provider's actual
// boundary wrappers and early-return positions while bypassing unrelated services.
func draftBoundaryOptions(
	t *testing.T,
	dc *dynamicconfig.Collection,
	registry namespace.Registry,
	redirection *interceptor.Redirection,
	custom grpc.UnaryServerInterceptor,
	omitters ...func(any, *draft.Selection) (bool, error),
) GrpcServerOptions {
	t.Helper()
	logger := log.NewNoopLogger()
	rpcFactory := common.NewMockRPCFactory(gomock.NewController(t))
	rpcFactory.EXPECT().GetFrontendGRPCServerOptions().Return(nil, nil)
	serviceConfig := &Config{
		KeepAliveMinTime:               dynamicconfig.GetDurationPropertyFn(time.Second),
		KeepAlivePermitWithoutStream:   dynamicconfig.GetBoolPropertyFn(false),
		KeepAliveMaxConnectionIdle:     dynamicconfig.GetDurationPropertyFn(time.Second),
		KeepAliveMaxConnectionAge:      dynamicconfig.GetDurationPropertyFn(time.Second),
		KeepAliveMaxConnectionAgeGrace: dynamicconfig.GetDurationPropertyFn(time.Second),
		KeepAliveTime:                  dynamicconfig.GetDurationPropertyFn(time.Second),
		KeepAliveTimeout:               dynamicconfig.GetDurationPropertyFn(time.Second),
	}
	omit := draft.Omit
	if len(omitters) > 0 {
		omit = omitters[0]
	}
	options := GrpcServerOptionsProvider(
		logger, nil, serviceConfig, primitives.FrontendService, rpcFactory,
		interceptor.NewServiceErrorInterceptor(dynamicconfig.GetIntPropertyFn(4000), metrics.NoopMetricsHandler, logger),
		interceptor.NewDraftFieldsInterceptor(dynamicconfig.FrontendEnabledDraftFeatureAPIs.Get(dc), registry, logger, omit),
		nil, (*interceptor.NamespaceRateLimitInterceptorImpl)(nil), nil, interceptor.NewNamespaceValidatorInterceptor(registry, dynamicconfig.GetBoolPropertyFn(false), dynamicconfig.GetIntPropertyFn(100), nil), nil, nil, redirection,
		nil, nil, nil, nil, nil, nil, nil, nil, nil,
		interceptor.NewMaskInternalErrorDetailsInterceptor(dynamicconfig.FrontendMaskInternalErrorDetails.Get(dc), registry, logger),
		nil, nil, nil, []grpc.UnaryServerInterceptor{custom}, nil, metrics.NoopMetricsHandler, testhooks.TestHooks{},
	)
	wantOuter := []string{"DraftFieldsInterceptor).OutgoingIntercept", "MaskInternalErrorDetailsInterceptor).Intercept", "ServiceErrorInterceptor).Intercept", "NewFrontendServiceErrorInterceptor"}
	for idx, name := range wantOuter {
		require.Contains(t, runtime.FuncForPC(reflect.ValueOf(options.UnaryInterceptors[idx]).Pointer()).Name(), name)
	}
	require.Contains(t, runtime.FuncForPC(reflect.ValueOf(options.UnaryInterceptors[5]).Pointer()).Name(), "NamespaceValidatorInterceptor).NamespaceValidateIntercept")
	require.Contains(t, runtime.FuncForPC(reflect.ValueOf(options.UnaryInterceptors[6]).Pointer()).Name(), "DraftFieldsInterceptor).IncomingIntercept")
	for idx := 4; idx < len(options.UnaryInterceptors); idx++ {
		name := runtime.FuncForPC(reflect.ValueOf(options.UnaryInterceptors[idx]).Pointer()).Name()
		if strings.Contains(name, "NamespaceValidatorInterceptor).NamespaceValidateIntercept") || strings.Contains(name, "DraftFieldsInterceptor).IncomingIntercept") || (redirection != nil && strings.Contains(name, "Redirection).Intercept")) || reflect.ValueOf(options.UnaryInterceptors[idx]).Pointer() == reflect.ValueOf(custom).Pointer() {
			continue
		}
		options.UnaryInterceptors[idx] = func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			return handler(ctx, req)
		}
	}
	return options
}

const (
	draftUpdateFeature = "update-user-tagging"
	draftHistoryMethod = "/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecutionHistory"
	draftRWTCMethod    = "/temporal.api.workflowservice.v1.WorkflowService/RespondWorkflowTaskCompleted"
	draftUpdateMethod  = "/temporal.api.workflowservice.v1.WorkflowService/UpdateWorkflowExecution"
)

// The unconstrained value enables the feature; the "tenant" and "namespace"
// overrides disable it.
func draftUpdateFeatureCollection() *dynamicconfig.Collection {
	return dynamicconfig.NewCollection(dynamicconfig.StaticClient{
		dynamicconfig.FrontendEnabledDraftFeatureAPIs.Key(): []dynamicconfig.ConstrainedValue{
			{Constraints: dynamicconfig.Constraints{Namespace: "tenant"}, Value: []any{}},
			{Constraints: dynamicconfig.Constraints{Namespace: "namespace"}, Value: []any{}},
			{Value: []any{draftUpdateFeature}},
		},
	}, log.NewNoopLogger())
}

// draftUpdateHistory returns a history whose update requests carry the draft
// user field.
func draftUpdateHistory(user string) *historypb.History {
	return &historypb.History{Events: []*historypb.HistoryEvent{
		{EventId: 1, Attributes: &historypb.HistoryEvent_WorkflowExecutionUpdateAcceptedEventAttributes{
			WorkflowExecutionUpdateAcceptedEventAttributes: &historypb.WorkflowExecutionUpdateAcceptedEventAttributes{
				AcceptedRequest: &updatepb.Request{User: user, Meta: &updatepb.Meta{UpdateId: "u"}},
			},
		}},
		{EventId: 2, Attributes: &historypb.HistoryEvent_WorkflowExecutionUpdateAdmittedEventAttributes{
			WorkflowExecutionUpdateAdmittedEventAttributes: &historypb.WorkflowExecutionUpdateAdmittedEventAttributes{
				Request: &updatepb.Request{User: user},
			},
		}},
	}}
}

func draftUpdateUsers(h *historypb.History) []string {
	return []string{
		h.Events[0].GetWorkflowExecutionUpdateAcceptedEventAttributes().GetAcceptedRequest().GetUser(),
		h.Events[1].GetWorkflowExecutionUpdateAdmittedEventAttributes().GetRequest().GetUser(),
	}
}

// An inner custom interceptor returns an affected response early. The outgoing
// stage must still omit it under the request's namespace.
func TestDraftFieldsProvider_CustomEarlyReturn(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry := namespace.NewMockRegistry(ctrl)
	registry.EXPECT().GetNamespaceByID(namespace.ID("tenant-id")).Return(
		namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Id: "tenant-id", Name: "tenant"}, nil, "local"), nil).AnyTimes()
	token, err := tasktoken.NewSerializer().Serialize(&tokenspb.Task{NamespaceId: "tenant-id"})
	require.NoError(t, err)

	for _, tc := range []struct {
		name   string
		method string
		req    any
		resp   func() (any, *historypb.History)
		want   []string
	}{
		{"history-by-name", draftHistoryMethod, &workflowservice.GetWorkflowExecutionHistoryRequest{Namespace: "tenant"}, func() (any, *historypb.History) {
			h := draftUpdateHistory("bob")
			return &workflowservice.GetWorkflowExecutionHistoryResponse{History: h}, h
		}, []string{"", ""}},
		{"history-enabled-namespace", draftHistoryMethod, &workflowservice.GetWorkflowExecutionHistoryRequest{Namespace: "other"}, func() (any, *historypb.History) {
			h := draftUpdateHistory("bob")
			return &workflowservice.GetWorkflowExecutionHistoryResponse{History: h}, h
		}, []string{"bob", "bob"}},
		{"rwtc-token-only", draftRWTCMethod, &workflowservice.RespondWorkflowTaskCompletedRequest{TaskToken: token}, func() (any, *historypb.History) {
			h := draftUpdateHistory("bob")
			return &workflowservice.RespondWorkflowTaskCompletedResponse{WorkflowTask: &workflowservice.PollWorkflowTaskQueueResponse{History: h}}, h
		}, []string{"", ""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, h := tc.resp()
			custom := func(context.Context, any, *grpc.UnaryServerInfo, grpc.UnaryHandler) (any, error) { return resp, nil }
			options := draftBoundaryOptions(t, draftUpdateFeatureCollection(), registry, nil, custom)
			chain := chainUnaryServerInterceptors(options.UnaryInterceptors)
			out, err := chain(t.Context(), tc.req, &grpc.UnaryServerInfo{FullMethod: tc.method}, func(context.Context, any) (any, error) {
				t.Fatal("custom interceptor must return early")
				return nil, nil
			})
			require.NoError(t, err)
			require.Same(t, resp, out)
			require.Equal(t, tc.want, draftUpdateUsers(h))
			require.Equal(t, int64(1), h.Events[0].GetEventId())
			require.Equal(t, "u", h.Events[0].GetWorkflowExecutionUpdateAcceptedEventAttributes().GetAcceptedRequest().GetMeta().GetUpdateId())
		})
	}
}

// DC redirection returns a remote response for an affected method. The
// redirected request is omitted before it is forwarded, and the remote
// response is omitted before it is returned.
func TestDraftFieldsProvider_RedirectionEarlyReturn(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry := namespace.NewMockRegistry(ctrl)
	entry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: "namespace-id", Name: "namespace"}, nil,
		&persistencespb.NamespaceReplicationConfig{ActiveClusterName: "remote", Clusters: []string{"local", "remote"}}, 1,
	)
	registry.EXPECT().GetNamespace(namespace.Name("namespace")).Return(entry, nil).AnyTimes()
	clusterMetadata := cluster.NewMockMetadata(ctrl)
	clusterMetadata.EXPECT().GetCurrentClusterName().Return("local").AnyTimes()
	bean := client.NewMockBean(ctrl)
	var forwarded *workflowservice.UpdateWorkflowExecutionRequest
	conn := &draftRemoteConn{invoke: func(_ context.Context, method string, args, reply any, _ ...grpc.CallOption) error {
		switch method {
		case draftHistoryMethod:
			proto.Merge(reply.(proto.Message), &workflowservice.GetWorkflowExecutionHistoryResponse{History: draftUpdateHistory("bob")})
		case draftUpdateMethod:
			forwarded = args.(*workflowservice.UpdateWorkflowExecutionRequest)
		default:
			t.Fatalf("unexpected redirected method %s", method)
		}
		return nil
	}}
	bean.EXPECT().GetRemoteFrontendClient("remote").Return(conn, nil, nil).AnyTimes()
	redirect := interceptor.NewRedirection(
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true), dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false), registry,
		config.DCRedirectionPolicy{Policy: interceptor.DCRedirectionPolicyAllAPIsForwarding}, log.NewNoopLogger(), bean, metrics.NoopMetricsHandler, clock.NewRealTimeSource(), clusterMetadata,
	)
	custom := func(context.Context, any, *grpc.UnaryServerInfo, grpc.UnaryHandler) (any, error) {
		t.Fatal("redirection must return early")
		return nil, nil
	}
	options := draftBoundaryOptions(t, draftUpdateFeatureCollection(), registry, redirect, custom)
	chain := chainUnaryServerInterceptors(options.UnaryInterceptors)
	out, err := chain(t.Context(), &workflowservice.GetWorkflowExecutionHistoryRequest{Namespace: "namespace"}, &grpc.UnaryServerInfo{FullMethod: draftHistoryMethod}, func(context.Context, any) (any, error) {
		t.Fatal("redirection must return early")
		return nil, nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"", ""}, draftUpdateUsers(out.(*workflowservice.GetWorkflowExecutionHistoryResponse).GetHistory()))

	req := &workflowservice.UpdateWorkflowExecutionRequest{Namespace: "namespace", Request: &updatepb.Request{User: "bob"}}
	_, err = chain(t.Context(), req, &grpc.UnaryServerInfo{FullMethod: draftUpdateMethod}, func(context.Context, any) (any, error) {
		t.Fatal("redirection must return early")
		return nil, nil
	})
	require.NoError(t, err)
	require.NotNil(t, forwarded)
	require.Empty(t, forwarded.GetRequest().GetUser(), "forwarded request must be omitted before redirection")
}

func TestDraftFieldsProvider_ErrorWrapperOrder(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry := namespace.NewMockRegistry(ctrl)
	registry.EXPECT().GetNamespace(namespace.Name("namespace")).Return(namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Name: "namespace"}, nil, "local"), nil).AnyTimes()
	options := draftBoundaryOptions(t, dynamicconfig.NewNoopCollection(), registry, nil, func(context.Context, any, *grpc.UnaryServerInfo, grpc.UnaryHandler) (any, error) { return nil, nil })
	chain := chainUnaryServerInterceptors(options.UnaryInterceptors[:4])
	for _, tc := range []struct {
		err     error
		code    codes.Code
		message string
	}{
		{serviceerror.NewDataLoss("internal details"), codes.Unavailable, "internal history service error"},
		{serviceerror.NewInternal("internal details"), codes.Internal, "something went wrong, please retry"},
		{serviceerror.NewNotFound("stable details"), codes.NotFound, "stable details"},
	} {
		resp, err := chain(t.Context(), &workflowservice.StartWorkflowExecutionRequest{Namespace: "namespace"}, &grpc.UnaryServerInfo{FullMethod: draftFrontendMethod}, func(context.Context, any) (any, error) { return nil, tc.err })
		require.Nil(t, resp)
		require.Equal(t, tc.code, status.Code(err))
		require.Contains(t, status.Convert(err).Message(), tc.message)
		if tc.code == codes.Internal {
			require.NotContains(t, err.Error(), "internal details")
		}
	}
}

const draftFrontendMethod = "/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo"

type draftRemoteConn struct {
	grpc.ClientConnInterface
	invoke func(context.Context, string, any, any, ...grpc.CallOption) error
}

func (c *draftRemoteConn) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	return c.invoke(ctx, method, args, reply, opts...)
}
