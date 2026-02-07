package configs

import (
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/quotas/quotastest"
	"go.temporal.io/server/common/testing/temporalapi"
)

var (
	testRateBurstFn        = quotas.NewDefaultIncomingRateBurst(func() float64 { return 5 })
	testOperatorRPSRatioFn = func() float64 { return 0.2 }
)

type (
	quotasSuite struct {
		suite.Suite
		*require.Assertions
	}
)

func TestQuotasSuite(t *testing.T) {
	s := new(quotasSuite)
	suite.Run(t, s)
}

func (s *quotasSuite) SetupSuite() {
}

func (s *quotasSuite) TearDownSuite() {
}

func (s *quotasSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *quotasSuite) TearDownTest() {
}

func (s *quotasSuite) TestExecutionAPIToPriorityMapping() {
	for _, priority := range APIToPriority {
		index := slices.Index(ExecutionAPIPrioritiesOrdered, priority)
		s.NotEqual(-1, index)
	}
}

func (s *quotasSuite) TestVisibilityAPIToPriorityMapping() {
	for _, priority := range VisibilityAPIToPriority {
		index := slices.Index(VisibilityAPIPrioritiesOrdered, priority)
		s.NotEqual(-1, index)
	}
}

func (s *quotasSuite) TestNamespaceReplicationInducingAPIToPriorityMapping() {
	for _, priority := range NamespaceReplicationInducingAPIToPriority {
		index := slices.Index(NamespaceReplicationInducingAPIPrioritiesOrdered, priority)
		s.NotEqual(-1, index)
	}
}

func (s *quotasSuite) TestExecutionAPIPrioritiesOrdered() {
	for idx := range ExecutionAPIPrioritiesOrdered[1:] {
		s.True(ExecutionAPIPrioritiesOrdered[idx] < ExecutionAPIPrioritiesOrdered[idx+1])
	}
}

func (s *quotasSuite) TestVisibilityAPIPrioritiesOrdered() {
	for idx := range VisibilityAPIPrioritiesOrdered[1:] {
		s.True(VisibilityAPIPrioritiesOrdered[idx] < VisibilityAPIPrioritiesOrdered[idx+1])
	}
}

func (s *quotasSuite) TestNamespaceReplicationInducingAPIPrioritiesOrdered() {
	for idx := range NamespaceReplicationInducingAPIPrioritiesOrdered[1:] {
		s.True(NamespaceReplicationInducingAPIPrioritiesOrdered[idx] < NamespaceReplicationInducingAPIPrioritiesOrdered[idx+1])
	}
}

func (s *quotasSuite) TestVisibilityAPIs() {
	apis := map[string]struct{}{
		"/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecution":           {},
		"/temporal.api.workflowservice.v1.WorkflowService/CountWorkflowExecutions":        {},
		"/temporal.api.workflowservice.v1.WorkflowService/ScanWorkflowExecutions":         {},
		"/temporal.api.workflowservice.v1.WorkflowService/ListOpenWorkflowExecutions":     {},
		"/temporal.api.workflowservice.v1.WorkflowService/ListClosedWorkflowExecutions":   {},
		"/temporal.api.workflowservice.v1.WorkflowService/ListWorkflowExecutions":         {},
		"/temporal.api.workflowservice.v1.WorkflowService/ListArchivedWorkflowExecutions": {},
		"/temporal.api.workflowservice.v1.WorkflowService/ListWorkers":                    {},
		"/temporal.api.workflowservice.v1.WorkflowService/DescribeWorker":                 {},

		"/temporal.api.workflowservice.v1.WorkflowService/GetWorkerTaskReachability":         {},
		"/temporal.api.workflowservice.v1.WorkflowService/ListSchedules":                     {},
		"/temporal.api.workflowservice.v1.WorkflowService/CountSchedules":                    {},
		"/temporal.api.workflowservice.v1.WorkflowService/ListBatchOperations":               {},
		"/temporal.api.workflowservice.v1.WorkflowService/DescribeTaskQueueWithReachability": {},
		"/temporal.api.workflowservice.v1.WorkflowService/ListDeployments":                   {},
		"/temporal.api.workflowservice.v1.WorkflowService/GetDeploymentReachability":         {},
		"/temporal.api.workflowservice.v1.WorkflowService/ListWorkerDeployments":             {},

		"/temporal.api.workflowservice.v1.WorkflowService/CountActivityExecutions": {},
		"/temporal.api.workflowservice.v1.WorkflowService/ListActivityExecutions":  {},
	}

	var service workflowservice.WorkflowServiceServer
	t := reflect.TypeOf(&service).Elem()
	apiToPriority := make(map[string]int, t.NumMethod())
	for i := 0; i < t.NumMethod(); i++ {
		apiName := "/temporal.api.workflowservice.v1.WorkflowService/" + t.Method(i).Name
		if t.Method(i).Name == "DescribeTaskQueue" {
			apiName += "WithReachability"
		}
		if _, ok := apis[apiName]; ok {
			apiToPriority[apiName] = VisibilityAPIToPriority[apiName]
		}
	}
	s.Equal(apiToPriority, VisibilityAPIToPriority)
}

func (s *quotasSuite) TestNamespaceReplicationInducingAPIs() {
	apis := map[string]struct{}{
		"/temporal.api.workflowservice.v1.WorkflowService/RegisterNamespace":                {},
		"/temporal.api.workflowservice.v1.WorkflowService/UpdateNamespace":                  {},
		"/temporal.api.workflowservice.v1.WorkflowService/UpdateWorkerBuildIdCompatibility": {},
		"/temporal.api.workflowservice.v1.WorkflowService/UpdateWorkerVersioningRules":      {},
	}

	var service workflowservice.WorkflowServiceServer
	t := reflect.TypeOf(&service).Elem()
	apiToPriority := make(map[string]int, t.NumMethod())
	for i := 0; i < t.NumMethod(); i++ {
		apiName := "/temporal.api.workflowservice.v1.WorkflowService/" + t.Method(i).Name
		if _, ok := apis[apiName]; ok {
			apiToPriority[apiName] = NamespaceReplicationInducingAPIToPriority[apiName]
		}
	}
	s.Equal(apiToPriority, NamespaceReplicationInducingAPIToPriority)
}

func (s *quotasSuite) TestAllAPIs() {
	apisWithPriority := make(map[string]struct{})
	for api := range APIToPriority {
		apisWithPriority[api] = struct{}{}
	}
	for api := range VisibilityAPIToPriority {
		apisWithPriority[api] = struct{}{}
	}
	for api := range NamespaceReplicationInducingAPIToPriority {
		apisWithPriority[api] = struct{}{}
	}
	var service workflowservice.WorkflowServiceServer
	temporalapi.WalkExportedMethods(&service, func(m reflect.Method) {
		_, ok := apisWithPriority["/temporal.api.workflowservice.v1.WorkflowService/"+m.Name]
		s.True(ok, "missing priority for API: %v", m.Name)
	})
	_, ok := apisWithPriority[DispatchNexusTaskByNamespaceAndTaskQueueAPIName]
	s.Truef(ok, "missing priority for API: %q", DispatchNexusTaskByNamespaceAndTaskQueueAPIName)
	_, ok = apisWithPriority[DispatchNexusTaskByEndpointAPIName]
	s.Truef(ok, "missing priority for API: %q", DispatchNexusTaskByEndpointAPIName)
	_, ok = apisWithPriority[CompleteNexusOperation]
	s.Truef(ok, "missing priority for API: %q", CompleteNexusOperation)
}

func (s *quotasSuite) TestOperatorPriority_Execution() {
	limiter := NewExecutionPriorityRateLimiter(testRateBurstFn, testOperatorRPSRatioFn)
	s.testOperatorPrioritized(limiter, "DescribeWorkflowExecution")
}

func (s *quotasSuite) TestOperatorPriority_Visibility() {
	limiter := NewVisibilityPriorityRateLimiter(testRateBurstFn, testOperatorRPSRatioFn)
	s.testOperatorPrioritized(limiter, "ListOpenWorkflowExecutions")
}

func (s *quotasSuite) TestOperatorPriority_NamespaceReplicationInducing() {
	limiter := NewNamespaceReplicationInducingAPIPriorityRateLimiter(testRateBurstFn, testOperatorRPSRatioFn)
	s.testOperatorPrioritized(limiter, "RegisterNamespace")
}

func (s *quotasSuite) TestNexusEndpointRateLimiter_PerCallerPerEndpoint() {
	// 1 frontend instance, 1 RPS per (caller, endpoint). Burst ratio is 2x, so burst=2.
	limiter := NewNexusEndpointRateLimiter(
		quotastest.NewFakeMemberCounter(1),
		dynamicconfig.TypedPropertyFnWithDestinationFilter[int](func(namespace, destination string) int {
			return 1
		}),
		func(string, string) float64 { return 2.0 },
	)

	now := time.Now()

	// namespace-a → endpoint-1: exhaust burst (2 tokens).
	reqA1 := quotas.Request{API: "api", Token: 1, Caller: "namespace-a", Destination: "endpoint-1"}
	s.True(limiter.Allow(now, reqA1))
	s.True(limiter.Allow(now, reqA1))

	// namespace-b → endpoint-1: allowed (separate bucket).
	reqB1 := quotas.Request{API: "api", Token: 1, Caller: "namespace-b", Destination: "endpoint-1"}
	s.True(limiter.Allow(now, reqB1))

	// namespace-a → endpoint-1: denied (burst exhausted).
	s.False(limiter.Allow(now, reqA1))

	// namespace-a → endpoint-2: allowed (separate bucket).
	reqA2 := quotas.Request{API: "api", Token: 1, Caller: "namespace-a", Destination: "endpoint-2"}
	s.True(limiter.Allow(now, reqA2))
}

func (s *quotasSuite) TestNexusEndpointRateLimiter_ZeroRPS() {
	// 0 means unlimited.
	limiter := NewNexusEndpointRateLimiter(
		quotastest.NewFakeMemberCounter(1),
		dynamicconfig.TypedPropertyFnWithDestinationFilter[int](func(namespace, destination string) int {
			return 0
		}),
		func(string, string) float64 { return 2.0 },
	)

	now := time.Now()
	req := quotas.Request{API: "api", Token: 1, Caller: "ns", Destination: "ep"}
	for range 100 {
		s.True(limiter.Allow(now, req))
	}
}

func (s *quotasSuite) TestNexusEndpointRateLimiter_DividedByMemberCount() {
	// Cluster limit 10, 5 instances → 2 RPS per instance, burst ratio 2x → burst=4.
	limiter := NewNexusEndpointRateLimiter(
		quotastest.NewFakeMemberCounter(5),
		dynamicconfig.TypedPropertyFnWithDestinationFilter[int](func(namespace, destination string) int {
			return 10
		}),
		func(string, string) float64 { return 2.0 },
	)

	now := time.Now()
	req := quotas.Request{API: "api", Token: 1, Caller: "ns", Destination: "ep"}
	allowed := 0
	for range 10 {
		if limiter.Allow(now, req) {
			allowed++
		}
	}
	// Rate=2, burst ratio=2x → burst=4 tokens available instantly.
	s.Equal(4, allowed, "expected burst tokens")
}

func (s *quotasSuite) testOperatorPrioritized(limiter quotas.RequestRateLimiter, api string) {
	operatorRequest := quotas.NewRequest(
		api,
		1,
		"test-namespace",
		headers.CallerTypeOperator,
		-1,
		"")

	apiRequest := quotas.NewRequest(
		api,
		1,
		"test-namespace",
		headers.CallerTypeAPI,
		-1,
		"")

	requestTime := time.Now()
	limitCount := 0

	for range 12 {
		if !limiter.Allow(requestTime, apiRequest) {
			limitCount++
			s.True(limiter.Allow(requestTime, operatorRequest))
		}
	}
	s.Equal(2, limitCount)
}
