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
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
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
	startWorkflow = "StartWorkflowExecution"
	queryWorkflow = "QueryWorkflow"
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
			methodName:        queryWorkflow,
			fullName:          api.WorkflowServicePrefix + queryWorkflow,
			expectEmitMetrics: true,
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
			expectLogging:             true,
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

func TestGetWorkflowTags(t *testing.T) {
	controller := gomock.NewController(t)
	registry := namespace.NewMockRegistry(controller)
	metricsHandler := metrics.NewMockHandler(controller)
	serializer := common.NewProtoTaskTokenSerializer()
	telemetry := NewTelemetryInterceptor(registry,
		metricsHandler,
		log.NewTestLogger(),
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false))

	wid := "test_workflow_id"
	rid := "test_run_id"
	taskToken := token.Task{
		WorkflowId: wid,
		RunId:      rid,
	}
	taskTokenBytes, err := serializer.Serialize(&taskToken)
	assert.NoError(t, err)

	testCases := []struct {
		name       string
		req        interface{}
		workflowID string
		runID      string
	}{
		{
			name:       "Request with only workflowID",
			req:        &workflowservice.StartWorkflowExecutionRequest{WorkflowId: wid},
			workflowID: wid,
		},
		{
			name:       "Request with workflowID and runID",
			req:        &workflowservice.RecordActivityTaskHeartbeatByIdRequest{WorkflowId: wid, RunId: rid},
			workflowID: wid,
			runID:      rid,
		},
		{
			name: "Request with execution",
			req: &workflowservice.GetWorkflowExecutionHistoryRequest{
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: wid,
					RunId:      rid,
				},
			},
			workflowID: wid,
			runID:      rid,
		},
		{
			name: "Request with workflow_execution",
			req: &workflowservice.RequestCancelWorkflowExecutionRequest{
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: wid,
					RunId:      rid,
				},
			},
			workflowID: wid,
			runID:      rid,
		},
		{
			name: "Request with task_token",
			req: &workflowservice.RespondActivityTaskCompletedRequest{
				TaskToken: taskTokenBytes,
			},
			workflowID: wid,
			runID:      rid,
		},
		{
			name: "Nil request",
			req:  nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			tags := telemetry.getWorkflowTags(tt.req)
			if len(tt.workflowID) > 0 {
				assert.Contains(t, tags, tag.WorkflowID(tt.workflowID))
			}
			if len(tt.runID) > 0 {
				assert.Contains(t, tags, tag.WorkflowRunID(tt.runID))
			}
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
