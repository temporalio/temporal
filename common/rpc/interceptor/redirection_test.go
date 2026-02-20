package interceptor

import (
	"context"
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/testing/temporalapi"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type (
	redirectionInterceptorSuite struct {
		suite.Suite
		*require.Assertions

		controller      *gomock.Controller
		namespaceCache  *namespace.MockRegistry
		clientBean      *client.MockBean
		clusterMetadata *cluster.MockMetadata

		redirector *Redirection
	}
)

func TestRedirectionInterceptorSuite(t *testing.T) {
	s := new(redirectionInterceptorSuite)
	suite.Run(t, s)
}

func (s *redirectionInterceptorSuite) SetupSuite() {
}

func (s *redirectionInterceptorSuite) TearDownSuite() {
}

func (s *redirectionInterceptorSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.namespaceCache = namespace.NewMockRegistry(s.controller)
	s.clientBean = client.NewMockBean(s.controller)
	s.clusterMetadata = cluster.NewMockMetadata(s.controller)

	s.clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	s.redirector = NewRedirection(
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		s.namespaceCache,
		config.DCRedirectionPolicy{
			Policy: DCRedirectionPolicyAllAPIsForwarding,
		},
		log.NewNoopLogger(),
		s.clientBean,
		metrics.NoopMetricsHandler,
		clock.NewRealTimeSource(),
		s.clusterMetadata,
	)
}

func (s *redirectionInterceptorSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *redirectionInterceptorSuite) TestLocalAPI() {
	apis := make(map[string]struct{})
	for api := range localAPIResponses {
		apis[api] = struct{}{}
	}
	s.Equal(map[string]struct{}{
		"DeprecateNamespace": {},
		"DescribeNamespace":  {},
		"ListNamespaces":     {},
		"RegisterNamespace":  {},
		"UpdateNamespace":    {},

		"GetSearchAttributes": {},
		"GetClusterInfo":      {},
		"GetSystemInfo":       {},
	}, apis)
}

func (s *redirectionInterceptorSuite) TestGlobalAPI() {
	apis := make(map[string]struct{})
	for api := range globalAPIResponses {
		apis[api] = struct{}{}
	}
	s.Equal(map[string]struct{}{
		"DescribeTaskQueue":                  {},
		"DescribeWorkflowExecution":          {},
		"GetWorkflowExecutionHistory":        {},
		"GetWorkflowExecutionHistoryReverse": {},
		"ListArchivedWorkflowExecutions":     {},
		"ListClosedWorkflowExecutions":       {},
		"ListOpenWorkflowExecutions":         {},
		"ListWorkflowExecutions":             {},
		"ScanWorkflowExecutions":             {},
		"CountWorkflowExecutions":            {},
		"PollActivityTaskQueue":              {},
		"PollWorkflowTaskQueue":              {},
		"PollNexusTaskQueue":                 {},
		"QueryWorkflow":                      {},
		"RecordActivityTaskHeartbeat":        {},
		"RecordActivityTaskHeartbeatById":    {},
		"RequestCancelWorkflowExecution":     {},
		"ResetStickyTaskQueue":               {},
		"ShutdownWorker":                     {},
		"ResetWorkflowExecution":             {},
		"RespondActivityTaskCanceled":        {},
		"RespondActivityTaskCanceledById":    {},
		"RespondActivityTaskCompleted":       {},
		"RespondActivityTaskCompletedById":   {},
		"RespondActivityTaskFailed":          {},
		"RespondActivityTaskFailedById":      {},
		"RespondWorkflowTaskCompleted":       {},
		"RespondWorkflowTaskFailed":          {},
		"RespondQueryTaskCompleted":          {},
		"RespondNexusTaskCompleted":          {},
		"RespondNexusTaskFailed":             {},
		"SignalWithStartWorkflowExecution":   {},
		"SignalWorkflowExecution":            {},
		"StartWorkflowExecution":             {},
		"ExecuteMultiOperation":              {},
		"UpdateWorkflowExecution":            {},
		"PollWorkflowExecutionUpdate":        {},
		"TerminateWorkflowExecution":         {},
		"DeleteWorkflowExecution":            {},
		"ListTaskQueuePartitions":            {},
		"PauseWorkflowExecution":             {},
		"UnpauseWorkflowExecution":           {},

		"CreateSchedule":                   {},
		"DescribeSchedule":                 {},
		"UpdateSchedule":                   {},
		"PatchSchedule":                    {},
		"DeleteSchedule":                   {},
		"ListSchedules":                    {},
		"CountSchedules":                   {},
		"ListScheduleMatchingTimes":        {},
		"UpdateWorkerBuildIdCompatibility": {},
		"GetWorkerBuildIdCompatibility":    {},
		"UpdateWorkerVersioningRules":      {},
		"GetWorkerVersioningRules":         {},
		"GetWorkerTaskReachability":        {},

		"StartBatchOperation":                   {},
		"StopBatchOperation":                    {},
		"DescribeBatchOperation":                {},
		"ListBatchOperations":                   {},
		"UpdateActivityOptions":                 {},
		"PauseActivity":                         {},
		"UnpauseActivity":                       {},
		"ResetActivity":                         {},
		"UpdateWorkflowExecutionOptions":        {},
		"DescribeDeployment":                    {},
		"ListDeployments":                       {},
		"GetDeploymentReachability":             {},
		"GetCurrentDeployment":                  {},
		"SetCurrentDeployment":                  {},
		"DescribeWorkerDeploymentVersion":       {},
		"SetWorkerDeploymentCurrentVersion":     {},
		"SetWorkerDeploymentRampingVersion":     {},
		"SetWorkerDeploymentManager":            {},
		"DescribeWorkerDeployment":              {},
		"ListWorkerDeployments":                 {},
		"DeleteWorkerDeployment":                {},
		"DeleteWorkerDeploymentVersion":         {},
		"UpdateWorkerDeploymentVersionMetadata": {},
		"CreateWorkflowRule":                    {},
		"DescribeWorkflowRule":                  {},
		"DeleteWorkflowRule":                    {},
		"ListWorkflowRules":                     {},
		"TriggerWorkflowRule":                   {},
		"RecordWorkerHeartbeat":                 {},
		"ListWorkers":                           {},
		"DescribeWorker":                        {},
		"UpdateTaskQueueConfig":                 {},
		"FetchWorkerConfig":                     {},
		"UpdateWorkerConfig":                    {},

		"StartActivityExecution":         {},
		"CountActivityExecutions":        {},
		"ListActivityExecutions":         {},
		"DescribeActivityExecution":      {},
		"PollActivityExecution":          {},
		"RequestCancelActivityExecution": {},
		"TerminateActivityExecution":     {},
		"DeleteActivityExecution":        {},
	}, apis)
}

func (s *redirectionInterceptorSuite) TestAPIResultMapping() {
	var service workflowservice.WorkflowServiceServer
	t := reflect.TypeOf(&service).Elem()
	expectedAPIs := make(map[string]any, t.NumMethod())
	temporalapi.WalkExportedMethods(&service, func(m reflect.Method) {
		expectedAPIs[m.Name] = m.Type.Out(0)
	})

	actualAPIs := make(map[string]any)
	for api, respAllocFn := range localAPIResponses {
		actualAPIs[api] = reflect.TypeOf(respAllocFn())
	}
	for api, respAllocFn := range globalAPIResponses {
		actualAPIs[api] = reflect.TypeOf(respAllocFn())
	}

	s.Equal(expectedAPIs, actualAPIs)
}

func (s *redirectionInterceptorSuite) TestHandleLocalAPIInvocation() {
	ctx := context.Background()
	req := &workflowservice.RegisterNamespaceRequest{}
	functionInvoked := false
	handler := func(ctx context.Context, req any) (any, error) {
		functionInvoked = true
		return &workflowservice.RegisterNamespaceResponse{}, nil
	}
	methodName := "RegisterNamespace"

	resp, err := s.redirector.handleLocalAPIInvocation(
		ctx,
		req,
		handler,
		methodName,
	)
	s.NoError(err)
	s.IsType(&workflowservice.RegisterNamespaceResponse{}, resp)
	s.True(functionInvoked)
}

func (s *redirectionInterceptorSuite) TestHandleGlobalAPIInvocation_Local() {
	ctx := context.Background()
	req := &workflowservice.SignalWithStartWorkflowExecutionRequest{}
	info := &grpc.UnaryServerInfo{}
	functionInvoked := false
	handler := func(ctx context.Context, req any) (any, error) {
		functionInvoked = true
		return &workflowservice.SignalWithStartWorkflowExecutionResponse{}, nil
	}
	namespaceName := namespace.Name("(╯°Д°)╯ ┻━┻")
	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: uuid.NewString(), Name: namespaceName.String()},
		&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1,
	)
	s.namespaceCache.EXPECT().GetNamespace(namespaceName).Return(namespaceEntry, nil).AnyTimes()
	methodName := "SignalWithStartWorkflowExecution"

	resp, err := s.redirector.handleRedirectAPIInvocation(
		ctx,
		req,
		info,
		handler,
		methodName,
		globalAPIResponses[methodName],
		namespaceName,
	)
	s.NoError(err)
	s.IsType(&workflowservice.SignalWithStartWorkflowExecutionResponse{}, resp)
	s.True(functionInvoked)
}

func (s *redirectionInterceptorSuite) TestHandleGlobalAPIInvocation_Redirect() {
	ctx := context.Background()
	req := &workflowservice.SignalWithStartWorkflowExecutionRequest{}
	info := &grpc.UnaryServerInfo{
		FullMethod: "/temporal.api.workflowservice.v1.WorkflowService/SignalWithStartWorkflowExecution",
	}
	namespaceName := namespace.Name("(╯°Д°)╯ ┻━┻")
	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: uuid.NewString(), Name: namespaceName.String()},
		&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1,
	)
	s.namespaceCache.EXPECT().GetNamespace(namespaceName).Return(namespaceEntry, nil).AnyTimes()
	methodName := "SignalWithStartWorkflowExecution"

	grpcConn := &mockClientConnInterface{
		Suite:          &s.Suite,
		targetMethod:   info.FullMethod,
		targetResponse: &workflowservice.SignalWithStartWorkflowExecutionResponse{},
	}
	s.clientBean.EXPECT().GetRemoteFrontendClient(cluster.TestAlternativeClusterName).Return(grpcConn, nil, nil).Times(1)

	resp, err := s.redirector.handleRedirectAPIInvocation(
		ctx,
		req,
		info,
		nil,
		methodName,
		globalAPIResponses[methodName],
		namespaceName,
	)
	s.NoError(err)
	s.IsType(&workflowservice.SignalWithStartWorkflowExecutionResponse{}, resp)
}

func (s *redirectionInterceptorSuite) TestHandleGlobalAPIInvocation_NamespaceNotFound() {
	ctx := context.Background()
	req := &workflowservice.PollWorkflowTaskQueueRequest{}
	info := &grpc.UnaryServerInfo{
		FullMethod: "/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowTaskQueue",
	}

	namespaceName := namespace.Name("unknown_namespace")
	s.namespaceCache.EXPECT().GetNamespace(namespaceName).Return(nil, &serviceerror.NamespaceNotFound{}).AnyTimes()
	methodName := "PollWorkflowTaskQueue"

	resp, err := s.redirector.handleRedirectAPIInvocation(
		ctx,
		req,
		info,
		nil,
		methodName,
		globalAPIResponses[methodName],
		namespaceName,
	)
	s.Nil(resp)
	s.IsType(&serviceerror.NamespaceNotFound{}, err)
}

func (s *redirectionInterceptorSuite) TestRedirectionAllowed_Empty() {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{}))
	allowed := s.redirector.RedirectionAllowed(ctx)
	s.True(allowed)
}

func (s *redirectionInterceptorSuite) TestRedirectionAllowed_Error() {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{
		DCRedirectionContextHeaderName: "?",
	}))
	allowed := s.redirector.RedirectionAllowed(ctx)
	s.True(allowed)
}

func (s *redirectionInterceptorSuite) TestRedirectionAllowed_True() {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{
		DCRedirectionContextHeaderName: "t",
	}))
	allowed := s.redirector.RedirectionAllowed(ctx)
	s.True(allowed)
}

func (s *redirectionInterceptorSuite) TestRedirectionAllowed_False() {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{
		DCRedirectionContextHeaderName: "f",
	}))
	allowed := s.redirector.RedirectionAllowed(ctx)
	s.False(allowed)
}

type (
	mockClientConnInterface struct {
		*suite.Suite
		targetMethod   string
		targetResponse any
	}
)

var _ grpc.ClientConnInterface = (*mockClientConnInterface)(nil)

func (s *mockClientConnInterface) Invoke(
	_ context.Context,
	method string,
	_ any,
	reply any,
	_ ...grpc.CallOption,
) error {
	s.Equal(s.targetMethod, method)
	s.Equal(s.targetResponse, reply)
	return nil
}

func (s *mockClientConnInterface) NewStream(
	_ context.Context,
	_ *grpc.StreamDesc,
	_ string,
	_ ...grpc.CallOption,
) (grpc.ClientStream, error) {
	panic("implement me")
}

// Test that local API invocations do not emit redirection metrics
func (s *redirectionInterceptorSuite) TestHandleLocalAPIInvocation_NoRedirectionMetrics() {
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	redirector := NewRedirection(
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		s.namespaceCache,
		config.DCRedirectionPolicy{Policy: DCRedirectionPolicyAllAPIsForwarding},
		log.NewNoopLogger(),
		s.clientBean,
		metricsHandler,
		clock.NewRealTimeSource(),
		s.clusterMetadata,
	)

	ctx := context.Background()
	req := &workflowservice.RegisterNamespaceRequest{}
	handler := func(ctx context.Context, req any) (any, error) {
		return &workflowservice.RegisterNamespaceResponse{}, nil
	}
	methodName := "RegisterNamespace"

	_, err := redirector.handleLocalAPIInvocation(ctx, req, handler, methodName)
	s.NoError(err)

	// Verify no redirection metrics were emitted
	snapshot := capture.Snapshot()
	s.Empty(snapshot[metrics.ClientRedirectionRequests.Name()], "ClientRedirectionRequests should not be emitted for local API")
	s.Empty(snapshot[metrics.ClientRedirectionLatency.Name()], "ClientRedirectionLatency should not be emitted for local API")
	s.Empty(snapshot[metrics.ClientRedirectionFailures.Name()], "ClientRedirectionFailures should not be emitted for local API")
}

// Test that global API with local routing does not emit redirection metrics
func (s *redirectionInterceptorSuite) TestHandleGlobalAPIInvocation_LocalRouting_NoRedirectionMetrics() {
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	redirector := NewRedirection(
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		s.namespaceCache,
		config.DCRedirectionPolicy{Policy: DCRedirectionPolicyAllAPIsForwarding},
		log.NewNoopLogger(),
		s.clientBean,
		metricsHandler,
		clock.NewRealTimeSource(),
		s.clusterMetadata,
	)

	ctx := context.Background()
	req := &workflowservice.SignalWithStartWorkflowExecutionRequest{}
	info := &grpc.UnaryServerInfo{}
	handler := func(ctx context.Context, req any) (any, error) {
		return &workflowservice.SignalWithStartWorkflowExecutionResponse{}, nil
	}
	namespaceName := namespace.Name("test-namespace")
	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: uuid.NewString(), Name: namespaceName.String()},
		&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName, // Active in current cluster
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1,
	)
	s.namespaceCache.EXPECT().GetNamespace(namespaceName).Return(namespaceEntry, nil).AnyTimes()
	methodName := "SignalWithStartWorkflowExecution"

	_, err := redirector.handleRedirectAPIInvocation(
		ctx, req, info, handler, methodName,
		globalAPIResponses[methodName], namespaceName,
	)
	s.NoError(err)

	// Verify no redirection metrics were emitted
	snapshot := capture.Snapshot()
	s.Empty(snapshot[metrics.ClientRedirectionRequests.Name()], "ClientRedirectionRequests should not be emitted for local routing")
	s.Empty(snapshot[metrics.ClientRedirectionLatency.Name()], "ClientRedirectionLatency should not be emitted for local routing")
	s.Empty(snapshot[metrics.ClientRedirectionFailures.Name()], "ClientRedirectionFailures should not be emitted for local routing")
}

// Test that global API with remote routing emits redirection metrics
func (s *redirectionInterceptorSuite) TestHandleGlobalAPIInvocation_RemoteRouting_EmitsRedirectionMetrics() {
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	redirector := NewRedirection(
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		s.namespaceCache,
		config.DCRedirectionPolicy{Policy: DCRedirectionPolicyAllAPIsForwarding},
		log.NewNoopLogger(),
		s.clientBean,
		metricsHandler,
		clock.NewRealTimeSource(),
		s.clusterMetadata,
	)

	ctx := context.Background()
	req := &workflowservice.SignalWithStartWorkflowExecutionRequest{}
	info := &grpc.UnaryServerInfo{
		FullMethod: "/temporal.api.workflowservice.v1.WorkflowService/SignalWithStartWorkflowExecution",
	}
	namespaceName := namespace.Name("test-namespace")
	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: uuid.NewString(), Name: namespaceName.String()},
		&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName, // Active in remote cluster
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1,
	)
	s.namespaceCache.EXPECT().GetNamespace(namespaceName).Return(namespaceEntry, nil).AnyTimes()
	methodName := "SignalWithStartWorkflowExecution"

	grpcConn := &mockClientConnInterface{
		Suite:          &s.Suite,
		targetMethod:   info.FullMethod,
		targetResponse: &workflowservice.SignalWithStartWorkflowExecutionResponse{},
	}
	s.clientBean.EXPECT().GetRemoteFrontendClient(cluster.TestAlternativeClusterName).Return(grpcConn, nil, nil).Times(1)

	_, err := redirector.handleRedirectAPIInvocation(
		ctx, req, info, nil, methodName,
		globalAPIResponses[methodName], namespaceName,
	)
	s.NoError(err)

	// Verify redirection metrics WERE emitted
	snapshot := capture.Snapshot()
	s.NotEmpty(snapshot[metrics.ClientRedirectionRequests.Name()], "ClientRedirectionRequests should be emitted for remote routing")
	s.NotEmpty(snapshot[metrics.ClientRedirectionLatency.Name()], "ClientRedirectionLatency should be emitted for remote routing")

	// Verify the request count is 1
	requestMetrics := snapshot[metrics.ClientRedirectionRequests.Name()]
	s.Len(requestMetrics, 1)
	s.Equal(int64(1), requestMetrics[0].Value)
}

// Test that local routing with error does not emit failure metrics
func (s *redirectionInterceptorSuite) TestHandleGlobalAPIInvocation_LocalRoutingWithError_NoFailureMetrics() {
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	redirector := NewRedirection(
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		s.namespaceCache,
		config.DCRedirectionPolicy{Policy: DCRedirectionPolicyAllAPIsForwarding},
		log.NewNoopLogger(),
		s.clientBean,
		metricsHandler,
		clock.NewRealTimeSource(),
		s.clusterMetadata,
	)

	ctx := context.Background()
	req := &workflowservice.SignalWithStartWorkflowExecutionRequest{}
	info := &grpc.UnaryServerInfo{}
	expectedError := serviceerror.NewInternal("local processing error")
	handler := func(ctx context.Context, req any) (any, error) {
		return nil, expectedError
	}
	namespaceName := namespace.Name("test-namespace")
	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: uuid.NewString(), Name: namespaceName.String()},
		&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName, // Active in current cluster
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1,
	)
	s.namespaceCache.EXPECT().GetNamespace(namespaceName).Return(namespaceEntry, nil).AnyTimes()
	methodName := "SignalWithStartWorkflowExecution"

	_, err := redirector.handleRedirectAPIInvocation(
		ctx, req, info, handler, methodName,
		globalAPIResponses[methodName], namespaceName,
	)
	s.Error(err)
	s.Equal(expectedError, err)

	// Verify NO redirection metrics were emitted (including failures)
	snapshot := capture.Snapshot()
	s.Empty(snapshot[metrics.ClientRedirectionRequests.Name()], "ClientRedirectionRequests should not be emitted for local routing")
	s.Empty(snapshot[metrics.ClientRedirectionLatency.Name()], "ClientRedirectionLatency should not be emitted for local routing")
	s.Empty(snapshot[metrics.ClientRedirectionFailures.Name()], "ClientRedirectionFailures should not be emitted for local routing even with error")
}

// Test that remote routing with error emits failure metrics
func (s *redirectionInterceptorSuite) TestHandleGlobalAPIInvocation_RemoteRoutingWithError_EmitsFailureMetrics() {
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	redirector := NewRedirection(
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		s.namespaceCache,
		config.DCRedirectionPolicy{Policy: DCRedirectionPolicyAllAPIsForwarding},
		log.NewNoopLogger(),
		s.clientBean,
		metricsHandler,
		clock.NewRealTimeSource(),
		s.clusterMetadata,
	)

	ctx := context.Background()
	req := &workflowservice.SignalWithStartWorkflowExecutionRequest{}
	info := &grpc.UnaryServerInfo{
		FullMethod: "/temporal.api.workflowservice.v1.WorkflowService/SignalWithStartWorkflowExecution",
	}
	namespaceName := namespace.Name("test-namespace")
	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: uuid.NewString(), Name: namespaceName.String()},
		&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName, // Active in remote cluster
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1,
	)
	s.namespaceCache.EXPECT().GetNamespace(namespaceName).Return(namespaceEntry, nil).AnyTimes()
	methodName := "SignalWithStartWorkflowExecution"

	// Setup mock remote client to return error
	expectedError := serviceerror.NewUnavailable("remote cluster unavailable")
	s.clientBean.EXPECT().GetRemoteFrontendClient(cluster.TestAlternativeClusterName).Return(nil, nil, expectedError).Times(1)

	_, err := redirector.handleRedirectAPIInvocation(
		ctx, req, info, nil, methodName,
		globalAPIResponses[methodName], namespaceName,
	)
	s.Error(err)

	// Verify all redirection metrics including failures were emitted
	snapshot := capture.Snapshot()
	s.NotEmpty(snapshot[metrics.ClientRedirectionRequests.Name()], "ClientRedirectionRequests should be emitted for remote routing")
	s.NotEmpty(snapshot[metrics.ClientRedirectionLatency.Name()], "ClientRedirectionLatency should be emitted for remote routing")
	s.NotEmpty(snapshot[metrics.ClientRedirectionFailures.Name()], "ClientRedirectionFailures should be emitted for remote routing with error")

	// Verify the failure count is 1
	failureMetrics := snapshot[metrics.ClientRedirectionFailures.Name()]
	s.Len(failureMetrics, 1)
	s.Equal(int64(1), failureMetrics[0].Value)
}
