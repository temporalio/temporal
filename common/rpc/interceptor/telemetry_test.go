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
	"testing"

	"github.com/stretchr/testify/assert"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	"go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
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
	telemetry := NewTelemetryInterceptor(register,
		metricsHandler,
		log.NewNoopLogger(),
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false))

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
				Query: &query.WorkflowQuery{
					QueryType: "some_type",
				},
			},
			expectEmitMetrics: true,
		},
		{
			methodName: queryWorkflow,
			fullName:   api.WorkflowServicePrefix + queryWorkflow,
			req: &workflowservice.QueryWorkflowRequest{
				Query: &query.WorkflowQuery{
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
	registry := namespace.NewMockRegistry(controller)
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
			name:                      "resource-exhausted",
			err:                       serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_UNSPECIFIED, "resource exhausted"),
			expectLogging:             false,
			ServiceFailuresCount:      0,
			ResourceExhaustedCount:    1,
			ServiceErrorWithTypeCount: 1,
			logAllErrors:              dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		},
		{
			name:                      "resource-exhausted",
			err:                       serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_UNSPECIFIED, "resource exhausted"),
			expectLogging:             true,
			ServiceFailuresCount:      0,
			ResourceExhaustedCount:    1,
			ServiceErrorWithTypeCount: 1,
			logAllErrors:              dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			metricsHandler.EXPECT().Counter(metrics.ServiceFailures.Name()).Return(metrics.NoopCounterMetricFunc).Times(tt.ServiceFailuresCount)
			metricsHandler.EXPECT().Counter(metrics.ServiceErrorWithType.Name()).Return(metrics.NoopCounterMetricFunc).Times(tt.ServiceErrorWithTypeCount)
			metricsHandler.EXPECT().Counter(metrics.ServiceErrResourceExhaustedCounter.Name()).Return(metrics.NoopCounterMetricFunc).Times(tt.ResourceExhaustedCount)

			telemetry := NewTelemetryInterceptor(registry,
				metricsHandler,
				mockLogger,
				tt.logAllErrors)

			if tt.expectLogging {
				mockLogger.EXPECT().Error(gomock.Eq("service failures"), gomock.Any()).Times(1)
			} else {
				mockLogger.EXPECT().Error(gomock.Eq("service failures"), gomock.Any()).Times(0)
			}

			telemetry.HandleError(nil,
				"",
				metricsHandler,
				[]tag.Tag{},
				tt.err,
				"test")
		})
	}
}

func TestOperationOverwrite(t *testing.T) {
	controller := gomock.NewController(t)
	register := namespace.NewMockRegistry(controller)
	metricsHandler := metrics.NewMockHandler(controller)
	telemetry := NewTelemetryInterceptor(register,
		metricsHandler,
		log.NewNoopLogger(),
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false))

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
			operation := telemetry.overrideOperationTag(tt.fullName, tt.methodName)
			assert.Equal(t, tt.expectedOperation, operation)
		})
	}
}

func TestOperationOverride(t *testing.T) {
	controller := gomock.NewController(t)
	register := namespace.NewMockRegistry(controller)
	metricsHandler := metrics.NewMockHandler(controller)
	telemetry := NewTelemetryInterceptor(register, metricsHandler,
		log.NewNoopLogger(),
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false))

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
			operation := telemetry.unaryOverrideOperationTag(tt.fullName, tt.methodName, tt.req)
			assert.Equal(t, tt.expectedOperation, operation)
		})
	}
}
