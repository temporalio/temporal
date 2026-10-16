package configs

import (
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
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

func (s *quotasSuite) TestPodOnlyAPIToPriorityMapping() {
	for _, priority := range PodOnlyAPIToPriority {
		index := slices.Index(ExecutionAPIPrioritiesOrdered, priority)
		s.NotEqual(-1, index)
	}
}

// temporary test while DescribeMutableState API rate limiting is rolled out.
func (s *quotasSuite) TestDescribeMutableStateAPIToPriorityMapping() {
	for _, priority := range DescribeMutableStateAPIToPriority {
		index := slices.Index(ExecutionAPIPrioritiesOrdered, priority)
		s.NotEqual(-1, index)
	}
}

func (s *quotasSuite) TestExecutionAPIPrioritiesOrdered() {
	for idx := range ExecutionAPIPrioritiesOrdered[1:] {
		s.Less(ExecutionAPIPrioritiesOrdered[idx], ExecutionAPIPrioritiesOrdered[idx+1])
	}
}

func (s *quotasSuite) TestVisibilityAPIPrioritiesOrdered() {
	for idx := range VisibilityAPIPrioritiesOrdered[1:] {
		s.Less(VisibilityAPIPrioritiesOrdered[idx], VisibilityAPIPrioritiesOrdered[idx+1])
	}
}

func (s *quotasSuite) TestNamespaceReplicationInducingAPIPrioritiesOrdered() {
	for idx := range NamespaceReplicationInducingAPIPrioritiesOrdered[1:] {
		s.Less(NamespaceReplicationInducingAPIPrioritiesOrdered[idx], NamespaceReplicationInducingAPIPrioritiesOrdered[idx+1])
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

		"/temporal.api.workflowservice.v1.WorkflowService/GetWorkerTaskReachability":         {},
		"/temporal.api.workflowservice.v1.WorkflowService/ListSchedules":                     {},
		"/temporal.api.workflowservice.v1.WorkflowService/CountSchedules":                    {},
		"/temporal.api.workflowservice.v1.WorkflowService/ListBatchOperations":               {},
		"/temporal.api.workflowservice.v1.WorkflowService/DescribeTaskQueueWithReachability": {},
		"/temporal.api.workflowservice.v1.WorkflowService/ListDeployments":                   {},
		"/temporal.api.workflowservice.v1.WorkflowService/GetDeploymentReachability":         {},
		"/temporal.api.workflowservice.v1.WorkflowService/ListWorkerDeployments":             {},

		"/temporal.api.workflowservice.v1.WorkflowService/CountActivityExecutions":       {},
		"/temporal.api.workflowservice.v1.WorkflowService/ListActivityExecutions":        {},
		"/temporal.api.workflowservice.v1.WorkflowService/CountNexusOperationExecutions": {},
		"/temporal.api.workflowservice.v1.WorkflowService/ListNexusOperationExecutions":  {},
	}

	t := reflect.TypeFor[workflowservice.WorkflowServiceServer]()
	apiToPriority := make(map[string]int, t.NumMethod())
	for method := range t.Methods() {
		apiName := "/temporal.api.workflowservice.v1.WorkflowService/" + method.Name
		if method.Name == "DescribeTaskQueue" {
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
		"/temporal.api.workflowservice.v1.WorkflowService/RegisterNamespace":                 {},
		"/temporal.api.workflowservice.v1.WorkflowService/UpdateNamespace":                   {},
		"/temporal.api.workflowservice.v1.WorkflowService/UpdateWorkerBuildIdCompatibility":  {},
		"/temporal.api.workflowservice.v1.WorkflowService/UpdateWorkerVersioningRules":       {},
		"/temporal.api.workflowservice.v1.WorkflowService/SetWorkerDeploymentCurrentVersion": {},
		"/temporal.api.workflowservice.v1.WorkflowService/SetWorkerDeploymentRampingVersion": {},
		"/temporal.api.workflowservice.v1.WorkflowService/DeleteWorkerDeploymentVersion":     {},
		"/temporal.api.workflowservice.v1.WorkflowService/UpdateTaskQueueConfig":             {},
	}

	t := reflect.TypeFor[workflowservice.WorkflowServiceServer]()
	apiToPriority := make(map[string]int, t.NumMethod())
	for method := range t.Methods() {
		apiName := "/temporal.api.workflowservice.v1.WorkflowService/" + method.Name
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

func (s *quotasSuite) TestGlobalNamespaceRateLimiterUsesConfiguredBurstRatio() {
	limiter := NewGlobalNamespaceRateLimiter(
		quotastest.NewFakeMemberCounter(2),
		func(namespace string) int {
			s.Equal("test-namespace", namespace)
			return 10
		},
		func(namespace string) float64 {
			s.Equal("test-namespace", namespace)
			return 3
		},
		log.NewNoopLogger(),
	)
	request := quotas.NewRequest(
		"DescribeWorkerDeployment",
		1,
		"test-namespace",
		headers.CallerTypeAPI,
		-1,
		"",
	)
	requestTime := time.Now()

	for range 15 {
		s.True(limiter.Allow(requestTime, request))
	}
	s.False(limiter.Allow(requestTime, request))
}

// TestRecordWorkerHeartbeatP3SurvivesP0StartupBurst is a regression for #12564.
//
// When workers start concurrently, each WorkerFactory fires DescribeNamespace (P0).
// In PriorityRateLimiterImpl.Allow, a P0 request decides on the P0 bucket and then
// calls Reserve on every lower-priority bucket (P1 through P5). This means a startup
// burst of 21 concurrent P0 calls drains 21 tokens from the P3 bucket.
//
// The test uses rate=30 (burst=60) so the numbers are tractable:
//   - 21 P0 reserves leave the P3 bucket at 60-21 = 39 tokens.
//   - The same 21 P0 reserves also drain 21 tokens from P4; then 39 concurrent
//     PollWorkflowTaskQueue (P4) calls exhaust the remaining 39, leaving P4 at 0.
//   - RecordWorkerHeartbeat is now P3: Allow decides on P3 (39 ≥ 1) → true.
//     With the old P4 mapping it would decide on P4 (0 < 1) → false (RESOURCE_EXHAUSTED).
func (s *quotasSuite) TestRecordWorkerHeartbeatP3SurvivesP0StartupBurst() {
	rateBurstFn := quotas.NewDefaultIncomingRateBurst(func() float64 { return 30 })
	limiter := NewExecutionPriorityRateLimiter(rateBurstFn, testOperatorRPSRatioFn)
	now := time.Now()

	makeReq := func(api string) quotas.Request {
		return quotas.NewRequest(api, 1, "test-namespace", headers.CallerTypeAPI, -1, "")
	}

	// 21 concurrent P0 DescribeNamespace calls (simulates 21 WorkerFactory.start() firing
	// in parallel). Each Allow also charges the P3 and P4 buckets via consumeRateLimiters.
	describeNamespace := "/temporal.api.workflowservice.v1.WorkflowService/DescribeNamespace"
	for range 21 {
		s.True(limiter.Allow(now, makeReq(describeNamespace)), "P0 DescribeNamespace must be allowed")
	}
	// P3 bucket: burst(60) - 21 P0 charges = 39 tokens remaining.
	// P4 bucket: burst(60) - 21 P0 charges = 39 tokens remaining.

	// Saturate P4 with concurrent long-poll calls (the other P4 residents that share the bucket).
	pollWorkflow := "/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowTaskQueue"
	for range 39 {
		limiter.Allow(now, makeReq(pollWorkflow))
	}
	// P4 bucket: 39 - 39 = 0 tokens remaining.

	// RecordWorkerHeartbeat is P3: P3 has 39 tokens → succeeds.
	heartbeat := "/temporal.api.workflowservice.v1.WorkflowService/RecordWorkerHeartbeat"
	s.True(limiter.Allow(now, makeReq(heartbeat)),
		"RecordWorkerHeartbeat (P3) must succeed after P0 startup burst: "+
			"P3 retains 39 tokens and is not affected by P4 long-poll saturation")

	// Confirm P4 is fully exhausted — a PollWorkflowTaskQueue at this point is denied.
	// This shows what would have happened to RecordWorkerHeartbeat under the old P4 mapping.
	s.False(limiter.Allow(now, makeReq(pollWorkflow)),
		"P4 is exhausted: old RecordWorkerHeartbeat (P4) mapping would have failed here")
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
