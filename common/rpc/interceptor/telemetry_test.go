package interceptor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	startWorkflow   = "StartWorkflowExecution"
	executeMultiOps = "ExecuteMultiOperation"
)

func TestEmitActionMetric(t *testing.T) {
	controller := gomock.NewController(t)
	register := namespace.NewMockRegistry(controller)
	metricsHandler := metrics.NewMockHandler(controller)
	logger := log.NewNoopLogger()
	logAllReqErrors := dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false)
	requestErrorHandler := NewMockErrorHandler(controller)
	telemetry := NewTelemetryInterceptor(register,
		metricsHandler,
		logger,
		logAllReqErrors,
		requestErrorHandler)

	testCases := []struct {
		methodName        string
		fullName          string
		expectEmitMetrics bool
		req               any
		resp              any
	}{
		{
			methodName: startWorkflow,
			fullName:   api.WorkflowServicePrefix + startWorkflow,
			resp:       workflowservice.StartWorkflowExecutionResponse_builder{Started: false}.Build(),
		},
		{
			methodName:        startWorkflow,
			fullName:          api.WorkflowServicePrefix + startWorkflow,
			resp:              workflowservice.StartWorkflowExecutionResponse_builder{Started: true}.Build(),
			expectEmitMetrics: true,
		},
		{
			methodName: startWorkflow,
			fullName:   api.WorkflowServicePrefix + startWorkflow,
			req: workflowservice.StartWorkflowExecutionRequest_builder{
				Namespace:                "test-namespace",
				OnConflictOptions:        &workflowpb.OnConflictOptions{},
				WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
			}.Build(),
			resp:              workflowservice.StartWorkflowExecutionResponse_builder{Started: false}.Build(),
			expectEmitMetrics: true,
		},
		{
			methodName: executeMultiOps,
			fullName:   api.WorkflowServicePrefix + executeMultiOps,
			resp: workflowservice.ExecuteMultiOperationResponse_builder{
				Responses: []*workflowservice.ExecuteMultiOperationResponse_Response{
					workflowservice.ExecuteMultiOperationResponse_Response_builder{
						StartWorkflow: workflowservice.StartWorkflowExecutionResponse_builder{
							Started: false,
						}.Build(),
					}.Build(),
					workflowservice.ExecuteMultiOperationResponse_Response_builder{
						UpdateWorkflow: &workflowservice.UpdateWorkflowExecutionResponse{},
					}.Build(),
				},
			}.Build(),
		},
		{
			methodName: executeMultiOps,
			fullName:   api.WorkflowServicePrefix + executeMultiOps,
			resp: workflowservice.ExecuteMultiOperationResponse_builder{
				Responses: []*workflowservice.ExecuteMultiOperationResponse_Response{
					workflowservice.ExecuteMultiOperationResponse_Response_builder{
						StartWorkflow: workflowservice.StartWorkflowExecutionResponse_builder{
							Started: true,
						}.Build(),
					}.Build(),
					workflowservice.ExecuteMultiOperationResponse_Response_builder{
						UpdateWorkflow: &workflowservice.UpdateWorkflowExecutionResponse{},
					}.Build(),
				},
			}.Build(),
			expectEmitMetrics: true,
		},
		{
			methodName: executeMultiOps,
			fullName:   api.WorkflowServicePrefix + executeMultiOps,
			resp: workflowservice.ExecuteMultiOperationResponse_builder{
				Responses: []*workflowservice.ExecuteMultiOperationResponse_Response{
					workflowservice.ExecuteMultiOperationResponse_Response_builder{
						StartWorkflow: workflowservice.StartWorkflowExecutionResponse_builder{
							Started: false,
						}.Build(),
					}.Build(),
				},
			}.Build(),
			req: workflowservice.ExecuteMultiOperationRequest_builder{
				Namespace: "test-namespace",
				Operations: []*workflowservice.ExecuteMultiOperationRequest_Operation{
					workflowservice.ExecuteMultiOperationRequest_Operation_builder{
						StartWorkflow: workflowservice.StartWorkflowExecutionRequest_builder{
							Namespace:                "test-namespace",
							OnConflictOptions:        &workflowpb.OnConflictOptions{},
							WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
						}.Build(),
					}.Build(),
				},
			}.Build(),
			expectEmitMetrics: true,
		},
		{
			methodName: executeMultiOps,
			fullName:   api.WorkflowServicePrefix + executeMultiOps,
			resp: workflowservice.ExecuteMultiOperationResponse_builder{
				Responses: []*workflowservice.ExecuteMultiOperationResponse_Response{
					// missing start response
					workflowservice.ExecuteMultiOperationResponse_Response_builder{
						UpdateWorkflow: &workflowservice.UpdateWorkflowExecutionResponse{},
					}.Build(),
				},
			}.Build(),
		},
		{
			methodName: executeMultiOps,
			fullName:   api.WorkflowServicePrefix + executeMultiOps,
			resp: workflowservice.ExecuteMultiOperationResponse_builder{
				Responses: []*workflowservice.ExecuteMultiOperationResponse_Response{
					// no responses
				},
			}.Build(),
		},
		{
			methodName: queryWorkflow,
			fullName:   api.AdminServicePrefix + queryWorkflow,
		},
		{
			methodName: metrics.MatchingClientAddWorkflowTaskScope,
			fullName:   api.WorkflowServicePrefix + queryWorkflow,
		},
		{
			methodName: "UpdateWorkflowExecution",
			fullName:   api.WorkflowServicePrefix + queryWorkflow,
		}, {
			methodName: metrics.HistoryRespondWorkflowTaskCompletedScope,
			fullName:   api.WorkflowServicePrefix + "RespondWorkflowTaskCompleted",
			req: workflowservice.RespondWorkflowTaskCompletedRequest_builder{
				Messages: []*protocolpb.Message{
					protocolpb.Message_builder{
						Id:   "MESSAGE_ID",
						Body: &updateAcceptanceMessageBody,
					}.Build(),
				},
			}.Build(),
			expectEmitMetrics: true,
		},
		{
			methodName: metrics.HistoryRespondWorkflowTaskCompletedScope,
			fullName:   api.WorkflowServicePrefix + "RespondWorkflowTaskCompleted",
			req: workflowservice.RespondWorkflowTaskCompletedRequest_builder{
				Commands: []*commandpb.Command{
					commandpb.Command_builder{
						CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
					}.Build(),
				},
			}.Build(),
			expectEmitMetrics: true,
		},
		{
			methodName: metrics.HistoryRespondWorkflowTaskCompletedScope,
			fullName:   api.WorkflowServicePrefix + "RespondWorkflowTaskCompleted",
			req: workflowservice.RespondWorkflowTaskCompletedRequest_builder{
				Messages: []*protocolpb.Message{
					protocolpb.Message_builder{
						Id:   "MESSAGE_ID",
						Body: &updateRejectionMessageBody,
					}.Build(),
				},
			}.Build(),
			expectEmitMetrics: true,
		},
		{
			methodName: metrics.HistoryRespondWorkflowTaskCompletedScope,
			fullName:   api.WorkflowServicePrefix + "RespondWorkflowTaskCompleted",
			req: workflowservice.RespondWorkflowTaskCompletedRequest_builder{
				Messages: []*protocolpb.Message{
					protocolpb.Message_builder{
						Id:   "MESSAGE_ID",
						Body: &updateResponseMessageBody,
					}.Build(),
				},
			}.Build(),
		},
		{
			methodName: queryWorkflow,
			fullName:   api.WorkflowServicePrefix + queryWorkflow,
			req: workflowservice.QueryWorkflowRequest_builder{
				Query: querypb.WorkflowQuery_builder{
					QueryType: "some_type",
				}.Build(),
			}.Build(),
			expectEmitMetrics: true,
		},
		{
			methodName: updateWorkflowExecutionOptions,
			fullName:   api.WorkflowServicePrefix + updateWorkflowExecutionOptions,
			req: workflowservice.UpdateWorkflowExecutionOptionsRequest_builder{
				Namespace: "test-namespace",
				WorkflowExecution: commonpb.WorkflowExecution_builder{
					WorkflowId: "test-workflow-id",
					RunId:      "test-run-id",
				}.Build(),
				WorkflowExecutionOptions: workflowpb.WorkflowExecutionOptions_builder{
					VersioningOverride: workflowpb.VersioningOverride_builder{
						Behavior:      enumspb.VERSIONING_BEHAVIOR_PINNED,
						PinnedVersion: "fake-version",
					}.Build(),
				}.Build(),
			}.Build(),
			expectEmitMetrics: true,
		},
		{
			methodName: queryWorkflow,
			fullName:   api.WorkflowServicePrefix + queryWorkflow,
			req: workflowservice.QueryWorkflowRequest_builder{
				Query: querypb.WorkflowQuery_builder{
					QueryType: "__temporal_workflow_metadata",
				}.Build(),
			}.Build(),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.methodName, func(t *testing.T) {
			if tt.expectEmitMetrics {
				metricsHandler.EXPECT().Counter(metrics.ActionCounter.Name()).Return(metrics.NoopCounterMetricFunc).Times(1)
			} else {
				metricsHandler.EXPECT().Counter(metrics.ActionCounter.Name()).Return(metrics.NoopCounterMetricFunc).Times(0)
			}
			telemetry.emitActionMetric(tt.methodName, tt.fullName, tt.req, metricsHandler, tt.resp)
		})
	}
}

func TestHandleError(t *testing.T) {
	controller := gomock.NewController(t)
	mockLogger := log.NewMockLogger(controller)
	metricsHandler := metrics.NewMockHandler(controller)

	testCases := []struct {
		name                      string
		err                       error
		expectLogging             bool
		ServiceFailuresCount      int
		ServiceErrorWithTypeCount int
		ResourceExhaustedCount    int
		logAllErrors              dynamicconfig.BoolPropertyFnWithNamespaceFilter
	}{
		{
			name:                      "serviceerror-invalid-argument",
			err:                       serviceerror.NewInvalidArgument("invalid argument"),
			expectLogging:             false,
			ServiceFailuresCount:      0,
			ResourceExhaustedCount:    0,
			ServiceErrorWithTypeCount: 1,
			logAllErrors:              dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		},
		{
			name:                      "serviceerror-invalid-argument-log-all",
			err:                       serviceerror.NewInvalidArgument("invalid argument"),
			expectLogging:             true,
			ServiceFailuresCount:      0,
			ResourceExhaustedCount:    0,
			ServiceErrorWithTypeCount: 1,
			logAllErrors:              dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		},
		{
			name:                      "statuserror-invalid-argument",
			err:                       status.Error(codes.InvalidArgument, "invalid argument"),
			expectLogging:             false,
			ServiceFailuresCount:      0,
			ResourceExhaustedCount:    0,
			ServiceErrorWithTypeCount: 1,
			logAllErrors:              dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		},
		{
			name:                      "statuserror-invalid-argument-log-all",
			err:                       status.Error(codes.InvalidArgument, "invalid argument"),
			expectLogging:             true,
			ServiceFailuresCount:      0,
			ResourceExhaustedCount:    0,
			ServiceErrorWithTypeCount: 1,
			logAllErrors:              dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		},
		{
			name:                      "serviceerror-internal",
			err:                       serviceerror.NewInternal("internal"),
			expectLogging:             true,
			ServiceFailuresCount:      1,
			ResourceExhaustedCount:    0,
			ServiceErrorWithTypeCount: 1,
			logAllErrors:              dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		},
		{
			name:                      "serviceerror-internal-log-all",
			err:                       serviceerror.NewInternal("internal"),
			expectLogging:             true,
			ServiceFailuresCount:      1,
			ResourceExhaustedCount:    0,
			ServiceErrorWithTypeCount: 1,
			logAllErrors:              dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		},
		{
			name:                      "statuserror-internal",
			err:                       status.Error(codes.Internal, "internal"),
			expectLogging:             true,
			ServiceFailuresCount:      1,
			ResourceExhaustedCount:    0,
			ServiceErrorWithTypeCount: 1,
			logAllErrors:              dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		},
		{
			name:                      "statuserror-internal-log-all",
			err:                       status.Error(codes.Internal, "internal"),
			expectLogging:             true,
			ServiceFailuresCount:      1,
			ResourceExhaustedCount:    0,
			ServiceErrorWithTypeCount: 1,
			logAllErrors:              dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		},
		{
			name: "resource-exhausted-system",
			err: &serviceerror.ResourceExhausted{
				Message: "resource exhausted",
				Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_UNSPECIFIED,
				Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
			},
			expectLogging:             false,
			ServiceFailuresCount:      1,
			ResourceExhaustedCount:    1,
			ServiceErrorWithTypeCount: 1,
			logAllErrors:              dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		},
		{
			name: "resource-exhausted-namespace",
			err: &serviceerror.ResourceExhausted{
				Message: "resource exhausted",
				Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_UNSPECIFIED,
				Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
			},
			expectLogging:             true,
			ServiceFailuresCount:      0,
			ResourceExhaustedCount:    1,
			ServiceErrorWithTypeCount: 1,
			logAllErrors:              dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		},
		{
			name:                      "canceled",
			err:                       context.Canceled,
			expectLogging:             false,
			ServiceFailuresCount:      0,
			ResourceExhaustedCount:    0,
			ServiceErrorWithTypeCount: 1,
			logAllErrors:              dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		},
		{
			name:                      "deadline-exceeded",
			err:                       context.DeadlineExceeded,
			expectLogging:             true,
			ServiceFailuresCount:      1,
			ResourceExhaustedCount:    0,
			ServiceErrorWithTypeCount: 1,
			logAllErrors:              dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		},
		{
			name:                      "shard-ownership-lost",
			err:                       serviceerrors.NewShardOwnershipLost("shard ownership lost", "hostname"),
			expectLogging:             true,
			ServiceFailuresCount:      1,
			ResourceExhaustedCount:    0,
			ServiceErrorWithTypeCount: 1,
			logAllErrors:              dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			metricsHandler.EXPECT().Counter(metrics.ServiceFailures.Name()).Return(metrics.NoopCounterMetricFunc).Times(tt.ServiceFailuresCount)
			metricsHandler.EXPECT().Counter(metrics.ServiceErrorWithType.Name()).Return(metrics.NoopCounterMetricFunc).Times(tt.ServiceErrorWithTypeCount)
			metricsHandler.EXPECT().Counter(metrics.ServiceErrResourceExhaustedCounter.Name()).Return(metrics.NoopCounterMetricFunc).Times(tt.ResourceExhaustedCount)

			requestErrorHandler := NewRequestErrorHandler(mockLogger, tt.logAllErrors)

			if tt.expectLogging {
				mockLogger.EXPECT().Error(gomock.Eq("service failures"), gomock.Any()).Times(1)
			} else {
				mockLogger.EXPECT().Error(gomock.Eq("service failures"), gomock.Any()).Times(0)
			}

			requestErrorHandler.HandleError(nil,
				"",
				metricsHandler,
				[]tag.Tag{},
				tt.err,
				"test")
		})
	}
}

func TestOperationOverwrite(t *testing.T) {
	testCases := []struct {
		methodName        string
		fullName          string
		expectedOperation string
	}{
		{
			"DeleteWorkflowExecution",
			api.AdminServicePrefix + "DeleteWorkflowExecution",
			"AdminDeleteWorkflowExecution",
		},
		{
			"DeleteNamespace",
			api.OperatorServicePrefix + "DeleteNamespace",
			"OperatorDeleteNamespace",
		},
		{
			startWorkflow,
			api.WorkflowServicePrefix + startWorkflow,
			startWorkflow,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.methodName, func(t *testing.T) {
			operation := telemetryOverrideOperationTag(tt.fullName, tt.methodName)
			assert.Equal(t, tt.expectedOperation, operation)
		})
	}
}

func TestOperationOverride(t *testing.T) {
	wid := "test_workflow_id"
	rid := "test_run_id"

	testCases := []struct {
		methodName        string
		fullName          string
		req               interface{}
		expectedOperation string
	}{
		{
			"GetWorkflowExecutionHistory",
			api.WorkflowServicePrefix + "GetWorkflowExecutionHistory",
			workflowservice.GetWorkflowExecutionHistoryRequest_builder{
				Execution: commonpb.WorkflowExecution_builder{
					WorkflowId: wid,
					RunId:      rid,
				}.Build(),
				WaitNewEvent: false,
			}.Build(),
			"GetWorkflowExecutionHistory",
		},
		{
			"GetWorkflowExecutionHistory",
			api.WorkflowServicePrefix + "GetWorkflowExecutionHistory",
			workflowservice.GetWorkflowExecutionHistoryRequest_builder{
				Execution: commonpb.WorkflowExecution_builder{
					WorkflowId: wid,
					RunId:      rid,
				}.Build(),
				WaitNewEvent: true,
			}.Build(),
			"PollWorkflowExecutionHistory",
		},
		{
			"GetWorkflowExecutionHistory",
			api.HistoryServicePrefix + "GetWorkflowExecutionHistory",
			historyservice.GetWorkflowExecutionHistoryRequest_builder{
				Request: workflowservice.GetWorkflowExecutionHistoryRequest_builder{
					Execution: commonpb.WorkflowExecution_builder{
						WorkflowId: wid,
						RunId:      rid,
					}.Build(),
					WaitNewEvent: false,
				}.Build(),
			}.Build(),
			"GetWorkflowExecutionHistory",
		},
		{
			"GetWorkflowExecutionHistory",
			api.HistoryServicePrefix + "GetWorkflowExecutionHistory",
			historyservice.GetWorkflowExecutionHistoryRequest_builder{
				Request: workflowservice.GetWorkflowExecutionHistoryRequest_builder{
					Execution: commonpb.WorkflowExecution_builder{
						WorkflowId: wid,
						RunId:      rid,
					}.Build(),
					WaitNewEvent: true,
				}.Build(),
			}.Build(),
			"PollWorkflowExecutionHistory",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.methodName, func(t *testing.T) {
			operation := telemetryUnaryOverrideOperationTag(tt.fullName, tt.methodName, tt.req)
			assert.Equal(t, tt.expectedOperation, operation)
		})
	}
}
