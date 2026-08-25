package interceptor

import (
	"context"
	"errors"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	interceptornexus "go.temporal.io/server/common/rpc/interceptor/nexus"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestTelemetryInterceptNexusOutermost(t *testing.T) {
	extraTag := metrics.StringTag("configured", "tag")
	input := interceptornexus.NewStartOpInput(
		"s", "o", testNamespace, nexus.StartOperationOptions{}, nil,
		interceptornexus.ForwardingInfo{},
		interceptornexus.RequestMetadata{MetricTags: []metrics.Tag{extraTag}},
	)
	for _, tc := range []struct {
		name            string
		handlerOut      any
		handlerErr      error
		setOverride     string
		expectedOutcome string
		expectedErrors  int
	}{
		{
			name:            "sync success is derived from the result type",
			handlerOut:      &nexus.HandlerStartOperationResultSync[any]{},
			expectedOutcome: "sync_success",
		},
		{
			name:            "async success is derived from the result type",
			handlerOut:      &nexus.HandlerStartOperationResultAsync{},
			expectedOutcome: "async_success",
		},
		{
			name:            "an interceptor's outcome rides on its error",
			handlerErr:      &interceptornexus.InterceptorError{Err: errors.New("rejected"), Outcome: "rejected"},
			expectedOutcome: "rejected",
			expectedErrors:  1,
		},
		{
			name:            "an unclassified error counts as internal",
			handlerErr:      errors.New("boom"),
			expectedOutcome: "internal_error",
			expectedErrors:  1,
		},
		{
			name:            "a short-circuiting interceptor overrides the success outcome",
			handlerOut:      &nexus.HandlerStartOperationResultSync[any]{},
			setOverride:     interceptornexus.OutcomeRequestForwarded,
			expectedOutcome: "request_forwarded",
		},
		{
			name:            "an error outcome wins over the override",
			handlerErr:      &interceptornexus.InterceptorError{Err: errors.New("forward failed"), Outcome: "forwarded_request_error"},
			setOverride:     interceptornexus.OutcomeRequestForwarded,
			expectedOutcome: "forwarded_request_error",
			expectedErrors:  1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metricsHandler := metricstest.NewCaptureHandler()
			capture := metricsHandler.StartCapture()
			defer metricsHandler.StopCapture(capture)

			telemetry := NewTelemetryInterceptor(nil, metricsHandler, log.NewNoopLogger(), nil, nil)
			nextCalled := false
			out, err := telemetry.InterceptNexusOutermost(
				context.Background(),
				input,
				func(ctx context.Context, _ interceptornexus.InterceptorInput) (any, error) {
					nextCalled = true
					// Downstream interceptors read the published handler from the context.
					require.NotNil(t, GetMetricsHandlerFromContext(ctx, log.NewNoopLogger()))
					if tc.setOverride != "" {
						interceptornexus.SetOutcomeOverride(ctx, tc.setOverride)
					}
					return tc.handlerOut, tc.handlerErr
				},
			)
			require.True(t, nextCalled)
			require.Equal(t, tc.handlerOut, out)
			require.Equal(t, tc.handlerErr, err)

			snapshot := capture.Snapshot()
			namespaceTag := metrics.NamespaceTag(testNamespace)

			outcomeTag := metrics.OutcomeTag(tc.expectedOutcome)
			methodTag := metrics.NexusMethodTag("StartNexusOperation")
			nexusRequests := snapshot[metrics.NexusRequests.Name()]
			require.Len(t, nexusRequests, 1)
			require.Equal(t, outcomeTag.Value, nexusRequests[0].Tags[outcomeTag.Key])
			require.Equal(t, methodTag.Value, nexusRequests[0].Tags[methodTag.Key])
			require.Equal(t, namespaceTag.Value, nexusRequests[0].Tags[namespaceTag.Key])
			require.Equal(t, extraTag.Value, nexusRequests[0].Tags[extraTag.Key])
			require.Len(t, snapshot[metrics.NexusLatency.Name()], 1)
			require.Len(t, snapshot[metrics.NexusRequestErrors.Name()], tc.expectedErrors)

			requests := snapshot[metrics.ServiceRequests.Name()]
			require.Len(t, requests, 1)
			require.Equal(t, "StartNexusOperation", requests[0].Tags[metrics.OperationTagName])
			require.Equal(t, namespaceTag.Value, requests[0].Tags[namespaceTag.Key])
			require.Len(t, snapshot[metrics.ServiceLatency.Name()], 1)
		})
	}
}

// The shared chain position records nothing; InterceptNexusOutermost is the only recorder.
func TestTelemetryInterceptNexusRecordsNothing(t *testing.T) {
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	telemetry := NewTelemetryInterceptor(nil, metricsHandler, log.NewNoopLogger(), nil, nil)
	nextCalled := false
	_, err := telemetry.InterceptNexus(
		context.Background(),
		interceptornexus.NewStartOpInput("s", "o", testNamespace, nexus.StartOperationOptions{}, nil, interceptornexus.ForwardingInfo{}, interceptornexus.RequestMetadata{}),
		func(context.Context, interceptornexus.InterceptorInput) (any, error) {
			nextCalled = true
			return nil, nil
		},
	)
	require.NoError(t, err)
	require.True(t, nextCalled)
	require.Empty(t, capture.Snapshot())
}

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
			resp:       &workflowservice.StartWorkflowExecutionResponse{Started: false},
		},
		{
			methodName:        startWorkflow,
			fullName:          api.WorkflowServicePrefix + startWorkflow,
			resp:              &workflowservice.StartWorkflowExecutionResponse{Started: true},
			expectEmitMetrics: true,
		},
		{
			methodName: startWorkflow,
			fullName:   api.WorkflowServicePrefix + startWorkflow,
			req: &workflowservice.StartWorkflowExecutionRequest{
				Namespace:                "test-namespace",
				OnConflictOptions:        &workflowpb.OnConflictOptions{},
				WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
			},
			resp:              &workflowservice.StartWorkflowExecutionResponse{Started: false},
			expectEmitMetrics: true,
		},
		{
			methodName: executeMultiOps,
			fullName:   api.WorkflowServicePrefix + executeMultiOps,
			resp: &workflowservice.ExecuteMultiOperationResponse{
				Responses: []*workflowservice.ExecuteMultiOperationResponse_Response{
					{
						Response: &workflowservice.ExecuteMultiOperationResponse_Response_StartWorkflow{
							StartWorkflow: &workflowservice.StartWorkflowExecutionResponse{
								Started: false,
							},
						},
					},
					{
						Response: &workflowservice.ExecuteMultiOperationResponse_Response_UpdateWorkflow{
							UpdateWorkflow: &workflowservice.UpdateWorkflowExecutionResponse{},
						},
					},
				},
			},
		},
		{
			methodName: executeMultiOps,
			fullName:   api.WorkflowServicePrefix + executeMultiOps,
			resp: &workflowservice.ExecuteMultiOperationResponse{
				Responses: []*workflowservice.ExecuteMultiOperationResponse_Response{
					{
						Response: &workflowservice.ExecuteMultiOperationResponse_Response_StartWorkflow{
							StartWorkflow: &workflowservice.StartWorkflowExecutionResponse{
								Started: true,
							},
						},
					},
					{
						Response: &workflowservice.ExecuteMultiOperationResponse_Response_UpdateWorkflow{
							UpdateWorkflow: &workflowservice.UpdateWorkflowExecutionResponse{},
						},
					},
				},
			},
			expectEmitMetrics: true,
		},
		{
			methodName: executeMultiOps,
			fullName:   api.WorkflowServicePrefix + executeMultiOps,
			resp: &workflowservice.ExecuteMultiOperationResponse{
				Responses: []*workflowservice.ExecuteMultiOperationResponse_Response{
					{
						Response: &workflowservice.ExecuteMultiOperationResponse_Response_StartWorkflow{
							StartWorkflow: &workflowservice.StartWorkflowExecutionResponse{
								Started: false,
							},
						},
					},
				},
			},
			req: &workflowservice.ExecuteMultiOperationRequest{
				Namespace: "test-namespace",
				Operations: []*workflowservice.ExecuteMultiOperationRequest_Operation{
					{
						Operation: &workflowservice.ExecuteMultiOperationRequest_Operation_StartWorkflow{
							StartWorkflow: &workflowservice.StartWorkflowExecutionRequest{
								Namespace:                "test-namespace",
								OnConflictOptions:        &workflowpb.OnConflictOptions{},
								WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
							},
						},
					},
				},
			},
			expectEmitMetrics: true,
		},
		{
			methodName: executeMultiOps,
			fullName:   api.WorkflowServicePrefix + executeMultiOps,
			resp: &workflowservice.ExecuteMultiOperationResponse{
				Responses: []*workflowservice.ExecuteMultiOperationResponse_Response{
					// missing start response
					{
						Response: &workflowservice.ExecuteMultiOperationResponse_Response_UpdateWorkflow{
							UpdateWorkflow: &workflowservice.UpdateWorkflowExecutionResponse{},
						},
					},
				},
			},
		},
		{
			methodName: executeMultiOps,
			fullName:   api.WorkflowServicePrefix + executeMultiOps,
			resp: &workflowservice.ExecuteMultiOperationResponse{
				Responses: []*workflowservice.ExecuteMultiOperationResponse_Response{
					// no responses
				},
			},
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
			req: &workflowservice.RespondWorkflowTaskCompletedRequest{
				Messages: []*protocolpb.Message{
					{
						Id:   "MESSAGE_ID",
						Body: &updateAcceptanceMessageBody,
					},
				},
			},
			expectEmitMetrics: true,
		},
		{
			methodName: metrics.HistoryRespondWorkflowTaskCompletedScope,
			fullName:   api.WorkflowServicePrefix + "RespondWorkflowTaskCompleted",
			req: &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{
					{
						CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
					},
				},
			},
			expectEmitMetrics: true,
		},
		{
			methodName: metrics.HistoryRespondWorkflowTaskCompletedScope,
			fullName:   api.WorkflowServicePrefix + "RespondWorkflowTaskCompleted",
			req: &workflowservice.RespondWorkflowTaskCompletedRequest{
				Messages: []*protocolpb.Message{
					{
						Id:   "MESSAGE_ID",
						Body: &updateRejectionMessageBody,
					},
				},
			},
			expectEmitMetrics: true,
		},
		{
			methodName: metrics.HistoryRespondWorkflowTaskCompletedScope,
			fullName:   api.WorkflowServicePrefix + "RespondWorkflowTaskCompleted",
			req: &workflowservice.RespondWorkflowTaskCompletedRequest{
				Messages: []*protocolpb.Message{
					{
						Id:   "MESSAGE_ID",
						Body: &updateResponseMessageBody,
					},
				},
			},
		},
		{
			methodName: queryWorkflow,
			fullName:   api.WorkflowServicePrefix + queryWorkflow,
			req: &workflowservice.QueryWorkflowRequest{
				Query: &querypb.WorkflowQuery{
					QueryType: "some_type",
				},
			},
			expectEmitMetrics: true,
		},
		{
			methodName: updateWorkflowExecutionOptions,
			fullName:   api.WorkflowServicePrefix + updateWorkflowExecutionOptions,
			req: &workflowservice.UpdateWorkflowExecutionOptionsRequest{
				Namespace: "test-namespace",
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: "test-workflow-id",
					RunId:      "test-run-id",
				},
				WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{
					VersioningOverride: &workflowpb.VersioningOverride{
						Behavior:      enumspb.VERSIONING_BEHAVIOR_PINNED,
						PinnedVersion: "fake-version",
					},
				},
			},
			expectEmitMetrics: true,
		},
		{
			methodName: queryWorkflow,
			fullName:   api.WorkflowServicePrefix + queryWorkflow,
			req: &workflowservice.QueryWorkflowRequest{
				Query: &querypb.WorkflowQuery{
					QueryType: "__temporal_workflow_metadata",
				},
			},
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
		req               any
		expectedOperation string
	}{
		{
			"GetWorkflowExecutionHistory",
			api.WorkflowServicePrefix + "GetWorkflowExecutionHistory",
			&workflowservice.GetWorkflowExecutionHistoryRequest{
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: wid,
					RunId:      rid,
				},
				WaitNewEvent: false,
			},
			"GetWorkflowExecutionHistory",
		},
		{
			"GetWorkflowExecutionHistory",
			api.WorkflowServicePrefix + "GetWorkflowExecutionHistory",
			&workflowservice.GetWorkflowExecutionHistoryRequest{
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: wid,
					RunId:      rid,
				},
				WaitNewEvent: true,
			},
			"PollWorkflowExecutionHistory",
		},
		{
			"GetWorkflowExecutionHistory",
			api.HistoryServicePrefix + "GetWorkflowExecutionHistory",
			&historyservice.GetWorkflowExecutionHistoryRequest{
				Request: &workflowservice.GetWorkflowExecutionHistoryRequest{
					Execution: &commonpb.WorkflowExecution{
						WorkflowId: wid,
						RunId:      rid,
					},
					WaitNewEvent: false,
				},
			},
			"GetWorkflowExecutionHistory",
		},
		{
			"GetWorkflowExecutionHistory",
			api.HistoryServicePrefix + "GetWorkflowExecutionHistory",
			&historyservice.GetWorkflowExecutionHistoryRequest{
				Request: &workflowservice.GetWorkflowExecutionHistoryRequest{
					Execution: &commonpb.WorkflowExecution{
						WorkflowId: wid,
						RunId:      rid,
					},
					WaitNewEvent: true,
				},
			},
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
