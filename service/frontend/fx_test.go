package frontend

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/draft"
	"go.temporal.io/server/common/draft/testfixture"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/nettest"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testhooks"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
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
	// namespaceReplicationInducingAPIsRPS is the RPS limit for namespace-replication-inducing APIs, pod-wide
	namespaceReplicationInducingAPIsRPS int
	// numReplicationInducingRequests is the number of namespace-replication-inducing requests to send to the interceptor
	numReplicationInducingRequests int
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
		{
			name: "replication inducing rate limit hit",
			configure: func(tc *rateLimitInterceptorTestCase) {
				tc.globalRPSLimit = highGlobalRPSLimit
				tc.perInstanceRPSLimit = highPerInstanceRPSLimit
				tc.operatorRPSRatio = operatorRPSRatio
				tc.namespaceReplicationInducingAPIsRPS = lowPerInstanceRPSLimit
				tc.numReplicationInducingRequests = 10
				tc.expectRateLimit = true
			},
		},
		{
			name: "replication inducing rate limit not hit",
			configure: func(tc *rateLimitInterceptorTestCase) {
				tc.globalRPSLimit = highGlobalRPSLimit
				tc.perInstanceRPSLimit = highPerInstanceRPSLimit
				tc.operatorRPSRatio = operatorRPSRatio
				tc.namespaceReplicationInducingAPIsRPS = highPerInstanceRPSLimit
				tc.numReplicationInducingRequests = 10
				tc.expectRateLimit = false
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
			config := &Config{
				RPS: func() int {
					return tc.perInstanceRPSLimit
				},
				GlobalRPS: func() int {
					return tc.globalRPSLimit
				},
				NamespaceReplicationInducingAPIsRPS: func() int {
					return tc.namespaceReplicationInducingAPIsRPS
				},
				OperatorRPSRatio: func() float64 {
					return tc.operatorRPSRatio
				},
				EnableDescribeMutableStateRateLimit: func() bool {
					return false
				},
			}
			rateLimiters := RateLimitersProvider(config, tc.serviceResolver, metrics.NoopMetricsHandler, log.NewTestLogger())
			rateLimitInterceptor := RateLimitInterceptorProvider(config, rateLimiters)

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

			defer func() {
				// Check if the rate limit is hit.
				if tc.expectRateLimit {
					require.ErrorContains(t, err, "rate limit exceeded")
					s := status.Convert(err)
					var resourceExhausted *serviceerror.ResourceExhausted
					errors.As(serviceerror.FromStatus(s), &resourceExhausted)
					require.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT, resourceExhausted.Cause)
					require.Equal(t, enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM, resourceExhausted.Scope)

					require.Len(t, header.Get(interceptor.ResourceExhaustedCauseHeader), 1)
					require.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT.String(), header.Get(interceptor.ResourceExhaustedCauseHeader)[0])
					require.Len(t, header.Get(interceptor.ResourceExhaustedScopeHeader), 1)
					require.Equal(t, enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM.String(), header.Get(interceptor.ResourceExhaustedScopeHeader)[0])
				} else {
					require.NoError(t, err)
				}
			}()

			// Generate load by sending a number of requests to the server.
			for i := 0; i < tc.numRequests; i++ {
				_, err = client.StartWorkflowExecution(
					context.Background(),
					&workflowservice.StartWorkflowExecutionRequest{},
					grpc.Header(&header),
				)
				if err != nil {
					return
				}
			}

			for i := 0; i < tc.numReplicationInducingRequests; i++ {
				_, err = client.RegisterNamespace(
					context.Background(),
					&workflowservice.RegisterNamespaceRequest{},
					grpc.Header(&header),
				)
				if err != nil {
					return
				}
			}
		})
	}
}

// TestRateLimitInterceptorProviderPodOnlyAPIs verifies that PodOnlyAPIToPriority entries (e.g.
// DescribeMutableState) are routed to the pod-level Execution rate limiter, the same way
// APIToPriority entries are, but only while EnableDescribeMutableStateRateLimit is enabled.
func TestRateLimitInterceptorProviderPodOnlyAPIs(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name              string
		executionRPSLimit int
		rateLimitEnabled  bool
		expectRateLimit   bool
	}{
		{name: "admin API rate limit hit", executionRPSLimit: 0, rateLimitEnabled: true, expectRateLimit: true},
		{name: "admin API rate limit not hit", executionRPSLimit: 100, rateLimitEnabled: true, expectRateLimit: false},
		{name: "admin API rate limit disabled by flag", executionRPSLimit: 0, rateLimitEnabled: false, expectRateLimit: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			serviceResolver := membership.NewMockServiceResolver(ctrl)
			serviceResolver.EXPECT().AvailableMemberCount().Return(1).AnyTimes()

			config := &Config{
				RPS: func() int {
					return tc.executionRPSLimit
				},
				GlobalRPS: func() int {
					return tc.executionRPSLimit
				},
				NamespaceReplicationInducingAPIsRPS: func() int {
					return 100
				},
				OperatorRPSRatio: func() float64 {
					return 0.2
				},
				EnableDescribeMutableStateRateLimit: func() bool {
					return tc.rateLimitEnabled
				},
			}
			rateLimiters := RateLimitersProvider(config, serviceResolver, metrics.NoopMetricsHandler, log.NewTestLogger())
			rateLimitInterceptor := RateLimitInterceptorProvider(config, rateLimiters)

			err := rateLimitInterceptor.Allow(
				adminservice.AdminService_DescribeMutableState_FullMethodName,
				headers.NewGRPCHeaderGetter(context.Background()),
			)
			if tc.expectRateLimit {
				require.Error(t, err)
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
				EnableDescribeMutableStateRateLimit: func() bool {
					return false
				},
			}

			serviceErrorInterceptor := interceptor.NewServiceErrorInterceptor(
				dynamicconfig.GetIntPropertyFn(4000),
				metrics.NewMockHandler(ctrl),
				log.NewTestLogger(),
			)

			// Create a rate limit interceptor which uses the per-instance and global RPS limits from the test case.
			rateLimiters := RateLimitersProvider(config, serviceResolver, metricsHandler, log.NewTestLogger())
			rateLimitInterceptor := RateLimitInterceptorProvider(config, rateLimiters)

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
	custom grpc.UnaryServerInterceptor,
	overrides ...fx.Option,
) GrpcServerOptions {
	t.Helper()
	ctrl := gomock.NewController(t)
	rpcFactory := common.NewMockRPCFactory(ctrl)
	rpcFactory.EXPECT().GetFrontendGRPCServerOptions().Return(nil, nil)
	clusterMetadata := cluster.NewMockMetadata(ctrl)
	clusterMetadata.EXPECT().GetCurrentClusterName().Return("local").AnyTimes()

	health := interceptor.NewHealthInterceptor()
	health.SetHealthy(true)
	var customInterceptors []grpc.UnaryServerInterceptor
	if custom != nil {
		customInterceptors = []grpc.UnaryServerInterceptor{custom}
	}

	namespaceRateLimiters := NamespaceRateLimiters{
		Execution:                    quotas.NoopRequestRateLimiter,
		Visibility:                   quotas.NoopRequestRateLimiter,
		NamespaceReplicationInducing: quotas.NoopRequestRateLimiter,
	}
	var options GrpcServerOptions
	app := fx.New(
		fx.NopLogger,
		fx.Supply(
			dc, health, primitives.FrontendService, &config.Config{}, config.Persistence{}, config.DCRedirectionPolicy{},
			testhooks.TestHooks{}, customInterceptors, []grpc.StreamServerInterceptor{},
			fx.Annotate(rpcFactory, fx.As(new(common.RPCFactory))),
			fx.Annotate(registry, fx.As(new(namespace.Registry))),
			fx.Annotate(clusterMetadata, fx.As(new(cluster.Metadata))),
			fx.Annotate(log.NewNoopLogger(), fx.As(new(log.Logger))),
			fx.Annotate(metrics.NoopMetricsHandler, fx.As(new(metrics.Handler))),
			fx.Annotate(clock.NewRealTimeSource(), fx.As(new(clock.TimeSource))),
			namespaceRateLimiters, RateLimiters(namespaceRateLimiters),
		),
		fx.Provide(
			ConfigProvider,
			resource.DefaultSnTaggedLoggerProvider,
			GrpcServerOptionsProvider,
			ServiceErrorInterceptorProvider,
			DraftFieldsInterceptorProvider,
			NamespaceValidatorInterceptorProvider,
			MaskInternalErrorDetailsInterceptorProvider,
			NamespaceLogInterceptorProvider,
			interceptor.NewRoutingKeyExtractor,
			BusinessIDInterceptorProvider,
			NamespaceHandoverInterceptorProvider,
			RedirectionInterceptorProvider,
			RetryDelayFloorInterceptorProvider,
			ErrorHandlerProvider,
			TelemetryInterceptorProvider,
			RetryableInterceptorProvider,
			NamespaceCountLimitInterceptorProvider,
			NamespaceRateLimitInterceptorProvider,
			RateLimitInterceptorProvider,
			SDKVersionInterceptorProvider,
			CallerInfoInterceptorProvider,
			SlowRequestLoggerInterceptorProvider,
			ContextMetadataInterceptorProvider,
			metrics.NewServerStatsHandler,
			func() telemetry.ServerStatsHandler { return nil },
			func() resource.NamespaceLogger { return nil },
			func() membership.ServiceResolver { return nil },
			func() client.Bean { return nil },
			func() *authorization.Interceptor { return &authorization.Interceptor{} },
			func() *chasm.ChasmVisibilityInterceptor { return chasm.ChasmVisibilityInterceptorProvider(nil) },
		),
		fx.Options(overrides...),
		fx.Populate(&options),
	)
	require.NoError(t, app.Err())
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
	tenant := namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{
		Id: "tenant-id", Name: "tenant", State: enumspb.NAMESPACE_STATE_REGISTERED,
	}, nil, "local")
	registry.EXPECT().GetNamespaceByID(namespace.ID("tenant-id")).Return(tenant, nil).AnyTimes()
	registry.EXPECT().GetNamespace(namespace.Name("tenant")).Return(tenant, nil).AnyTimes()
	registry.EXPECT().GetNamespace(namespace.Name("other")).Return(namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: "other", State: enumspb.NAMESPACE_STATE_REGISTERED}, nil, "local",
	), nil).AnyTimes()
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
			options := draftBoundaryOptions(t, draftUpdateFeatureCollection(), registry, custom)
			chain := chainUnaryServerInterceptors(options.UnaryInterceptors)
			out, err := chain(t.Context(), tc.req, &grpc.UnaryServerInfo{FullMethod: tc.method}, func(context.Context, any) (any, error) {
				t.Fatal("custom interceptor must return early")
				return nil, nil
			})
			require.NoError(t, err)
			require.Equal(t, []string{"bob", "bob"}, draftUpdateUsers(h))
			var returnedHistory *historypb.History
			switch response := out.(type) {
			case *workflowservice.GetWorkflowExecutionHistoryResponse:
				returnedHistory = response.GetHistory()
			case *workflowservice.RespondWorkflowTaskCompletedResponse:
				returnedHistory = response.GetWorkflowTask().GetHistory()
			default:
				t.Fatalf("unexpected response type %T", out)
			}
			require.NotNil(t, returnedHistory)
			require.Equal(t, tc.want, draftUpdateUsers(returnedHistory), "response filtering must use the validated namespace")
			require.Equal(t, int64(1), returnedHistory.Events[0].GetEventId())
			require.Equal(t, "u", returnedHistory.Events[0].GetWorkflowExecutionUpdateAcceptedEventAttributes().GetAcceptedRequest().GetMeta().GetUpdateId())
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
	custom := func(context.Context, any, *grpc.UnaryServerInfo, grpc.UnaryHandler) (any, error) {
		t.Fatal("redirection must return early")
		return nil, nil
	}
	options := draftBoundaryOptions(t, draftUpdateFeatureCollection(), registry, custom,
		fx.Replace(
			config.DCRedirectionPolicy{Policy: interceptor.DCRedirectionPolicyAllAPIsForwarding},
			fx.Annotate(bean, fx.As(new(client.Bean))),
		),
	)
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

func TestDraftFieldsProvider_OutgoingIsOutermost(t *testing.T) {
	registry := namespace.NewMockRegistry(gomock.NewController(t))
	options := draftBoundaryOptions(t, draftUpdateFeatureCollection(), registry, nil)
	require.NotEmpty(t, options.UnaryInterceptors)

	req := &workflowservice.GetWorkflowExecutionHistoryRequest{Namespace: "tenant"}
	source := &workflowservice.GetWorkflowExecutionHistoryResponse{History: draftUpdateHistory("bob")}
	out, err := options.UnaryInterceptors[0](t.Context(), req, &grpc.UnaryServerInfo{FullMethod: draftHistoryMethod}, func(context.Context, any) (any, error) {
		return source, nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"", ""}, draftUpdateUsers(out.(*workflowservice.GetWorkflowExecutionHistoryResponse).GetHistory()))
	require.Equal(t, []string{"bob", "bob"}, draftUpdateUsers(source.GetHistory()))
}

func TestDraftFieldsProvider_ErrorWrapperOrder(t *testing.T) {
	registry := namespace.NewMockRegistry(gomock.NewController(t))
	registry.EXPECT().GetNamespace(namespace.Name("namespace")).Return(namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: "namespace", State: enumspb.NAMESPACE_STATE_REGISTERED}, nil, "local",
	), nil).AnyTimes()
	const maskedMessage = "something went wrong, please retry"
	dc := dynamicconfig.NewCollection(dynamicconfig.StaticClient{
		dynamicconfig.MaxServiceErrorMessageLength.Key(): len(maskedMessage) - 1,
	}, log.NewNoopLogger())
	for _, tc := range []struct {
		err     error
		code    codes.Code
		message string
	}{
		{serviceerror.NewDataLoss("internal details"), codes.Unavailable, "internal history service error"},
		{serviceerror.NewInternal("internal details"), codes.Internal, maskedMessage},
		{serviceerror.NewNotFound("stable details"), codes.NotFound, "stable details"},
	} {
		t.Run(tc.code.String(), func(t *testing.T) {
			custom := func(context.Context, any, *grpc.UnaryServerInfo, grpc.UnaryHandler) (any, error) {
				return nil, tc.err
			}
			options := draftBoundaryOptions(t, dc, registry, custom)
			chain := chainUnaryServerInterceptors(options.UnaryInterceptors)
			req := &workflowservice.StartWorkflowExecutionRequest{Namespace: "namespace"}
			info := &grpc.UnaryServerInfo{FullMethod: workflowservice.WorkflowService_StartWorkflowExecution_FullMethodName}
			resp, err := chain(t.Context(), req, info, func(context.Context, any) (any, error) {
				t.Fatal("custom interceptor must return early")
				return nil, nil
			})
			require.Nil(t, resp)
			require.Equal(t, tc.code, status.Code(err))
			require.Contains(t, status.Convert(err).Message(), tc.message)
			if tc.code == codes.Internal {
				require.NotContains(t, err.Error(), "internal details")
			}
		})
	}
}

type draftRemoteConn struct {
	grpc.ClientConnInterface
	invoke func(context.Context, string, any, any, ...grpc.CallOption) error
}

func (c *draftRemoteConn) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	return c.invoke(ctx, method, args, reply, opts...)
}

func TestDraftRPCGatePanicIsGeneric(t *testing.T) {
	t.Parallel()
	for _, source := range []string{"config", "namespace registry"} {
		t.Run(source, func(t *testing.T) {
			t.Parallel()
			registry := namespace.NewMockRegistry(gomock.NewController(t))
			request := &testfixture.MethodRequest{}
			var req any = request
			if source == "namespace registry" {
				request.NamespaceId = "tenant-id"
				req = &draftNamespaceIDRequest{draftProtoRequest: request, NamespaceIDGetter: request}
				registry.EXPECT().GetNamespaceName(namespace.ID("tenant-id")).DoAndReturn(func(namespace.ID) (namespace.Name, error) {
					panic("private draft registry error")
				})
			}
			dc := dynamicconfig.NewCollection(dynamicconfig.StaticClient{
				dynamicconfig.FrontendMaskInternalErrorDetails.Key(): false,
			}, log.NewNoopLogger())
			options := draftBoundaryOptions(t, dc, registry, nil, fx.Decorate(func(logger log.Logger) *interceptor.DraftFieldsInterceptor {
				return interceptor.NewDraftFieldsInterceptor(func(name string) []string {
					require.Equal(t, "config", source)
					require.Empty(t, name)
					panic("private draft config error")
				}, registry, logger, draft.Omit, draftFixtureService("FixtureService"))
			}))
			chain := chainUnaryServerInterceptors(options.UnaryInterceptors)
			info := &grpc.UnaryServerInfo{FullMethod: testfixture.FixtureService_Preview_FullMethodName}
			out, err := chain(t.Context(), req, info, func(context.Context, any) (any, error) {
				t.Fatal("draft RPC was dispatched after a gate failure")
				return nil, nil
			})
			require.Nil(t, out)
			protorequire.ProtoEqual(t, status.New(codes.Internal, "draft field omission failed").Proto(), status.Convert(err).Proto())
		})
	}
}

type draftProtoRequest interface {
	proto.Message
	OmitDraftFields(func(string) bool) bool
}

type draftNamespaceIDRequest struct {
	draftProtoRequest
	interceptor.NamespaceIDGetter
}

func TestDraftRPCGateRunsAfterNormalizationAndBeforeRedirection(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		service protoreflect.Name
		method  string
		feature string
	}{
		{"FixtureService", testfixture.FixtureService_Preview_FullMethodName, "method"},
		{"DraftFixtureService", testfixture.DraftFixtureService_Preview_FullMethodName, "service"},
	} {
		t.Run(tc.feature, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			registry := namespace.NewMockRegistry(ctrl)
			entry := namespace.NewGlobalNamespaceForTest(
				&persistencespb.NamespaceInfo{Id: "tenant-id", Name: "tenant"}, nil,
				&persistencespb.NamespaceReplicationConfig{ActiveClusterName: "remote", Clusters: []string{"local", "remote"}}, 1,
			)
			registry.EXPECT().GetNamespaceByID(namespace.ID("tenant-id")).Return(entry, nil)
			// Use a request supported by the production task-token namespace normalizer.
			token, err := tasktoken.NewSerializer().Serialize(&tokenspb.Task{NamespaceId: "tenant-id"})
			require.NoError(t, err)
			req := &workflowservice.RespondWorkflowTaskCompletedRequest{TaskToken: token}
			dc := dynamicconfig.NewCollection(dynamicconfig.StaticClient{
				dynamicconfig.EnableNamespaceNotActiveAutoForwarding.Key(): true,
				dynamicconfig.FrontendEnabledDraftFeatureAPIs.Key(): []dynamicconfig.ConstrainedValue{
					{Constraints: dynamicconfig.Constraints{Namespace: "tenant"}, Value: []any{}},
					{Value: []any{tc.feature}},
				},
			}, log.NewNoopLogger())
			options := draftBoundaryOptions(t, dc, registry, nil,
				fx.Replace(
					config.DCRedirectionPolicy{Policy: interceptor.DCRedirectionPolicyAllAPIsForwarding},
					fx.Annotate(client.NewMockBean(ctrl), fx.As(new(client.Bean))),
				),
				fx.Decorate(func(cfg *Config, logger log.Logger) *interceptor.DraftFieldsInterceptor {
					return interceptor.NewDraftFieldsInterceptor(cfg.EnabledDraftFeatureAPIs, registry, logger, draft.Omit, draftFixtureService(tc.service))
				}),
				fx.Decorate(func(redirection *interceptor.Redirection) *interceptor.Redirection {
					return redirection.WithRedirectResponses(map[string]func() any{
						tc.method: func() any { return &testfixture.MethodResponse{} },
					})
				}),
			)
			chain := chainUnaryServerInterceptors(options.UnaryInterceptors)
			out, err := chain(t.Context(), req, &grpc.UnaryServerInfo{FullMethod: tc.method}, func(context.Context, any) (any, error) {
				t.Fatal("disabled draft RPC was dispatched")
				return nil, nil
			})
			require.Nil(t, out)
			require.Equal(t, "tenant", req.Namespace)
			protorequire.ProtoEqual(t, status.New(codes.Unimplemented, "method is not available").Proto(), status.Convert(err).Proto())
		})
	}
}

func draftFixtureService(name protoreflect.Name) protoreflect.ServiceDescriptor {
	return testfixture.File_common_draft_testfixture_filter_proto.Services().ByName(name)
}

func TestDraftFieldsRejectIDOnlyRequestsWithoutMaskLookup(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		method  string
		message draftProtoRequest
		want    *status.Status
	}{
		{
			name:    "disabled method",
			method:  testfixture.FixtureService_Preview_FullMethodName,
			message: &testfixture.MethodRequest{},
			want:    status.New(codes.Unimplemented, "method is not available"),
		},
		{
			name:    "disabled enum value",
			method:  testfixture.FixtureService_CheckEnums_FullMethodName,
			message: &testfixture.EnumRequest{Value: testfixture.StableEnum_STABLE_ENUM_PREVIEW},
			want:    status.New(codes.InvalidArgument, "request contains a disabled draft enum value"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			registry := namespace.NewMockRegistry(gomock.NewController(t))
			registry.EXPECT().GetNamespaceName(namespace.ID("tenant-id")).Return(namespace.Name("tenant"), nil)
			req := &draftNamespaceIDRequest{
				draftProtoRequest: tc.message,
				NamespaceIDGetter: &testfixture.MethodRequest{NamespaceId: "tenant-id"},
			}
			options := draftBoundaryOptions(t, dynamicconfig.NewNoopCollection(), registry, nil,
				fx.Decorate(func(cfg *Config, logger log.Logger) *interceptor.DraftFieldsInterceptor {
					return interceptor.NewDraftFieldsInterceptor(cfg.EnabledDraftFeatureAPIs, registry, logger, draft.Omit, draftFixtureService("FixtureService"))
				}),
			)
			chain := chainUnaryServerInterceptors(options.UnaryInterceptors)
			out, err := chain(t.Context(), req, &grpc.UnaryServerInfo{FullMethod: tc.method}, func(context.Context, any) (any, error) {
				t.Fatal("rejected request was dispatched")
				return nil, nil
			})
			require.Nil(t, out)
			protorequire.ProtoEqual(t, tc.want.Proto(), status.Convert(err).Proto())
		})
	}
}
