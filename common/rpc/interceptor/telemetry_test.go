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

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	commonpb "go.temporal.io/api/common/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
)

const (
	startWorkflow = "StartWorkflowExecution"
	queryWorkflow = "QueryWorkflow"
)

func TestEmitActionMetric(t *testing.T) {
	controller := gomock.NewController(t)
	register := namespace.NewMockRegistry(controller)
	metricsHandler := metrics.NewMockHandler(controller)
	telemetry := NewTelemetryInterceptor(register, metricsHandler, log.NewNoopLogger())

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
				metricsHandler.EXPECT().Counter(gomock.Any()).Return(metrics.NoopCounterMetricFunc).Times(1)
			} else {
				metricsHandler.EXPECT().Counter(gomock.Any()).Return(metrics.NoopCounterMetricFunc).Times(0)
			}
			telemetry.emitActionMetric(tt.methodName, tt.fullName, tt.req, metricsHandler, tt.resp)
		})
	}
}

func TestHandleError(t *testing.T) {
	controller := gomock.NewController(t)
	registry := namespace.NewMockRegistry(controller)
	metricsHandler := metrics.NewMockHandler(controller)
	telemetry := NewTelemetryInterceptor(registry, metricsHandler, log.NewNoopLogger())

	testCases := []struct {
		name                 string
		err                  error
		expectServiceFailure bool
	}{
		{
			name:                 "serviceerror-invalid-argument",
			err:                  serviceerror.NewInvalidArgument("invalid argument"),
			expectServiceFailure: false,
		},
		{
			name:                 "statuserror-invalid-argument",
			err:                  status.Error(codes.InvalidArgument, "invalid argument"),
			expectServiceFailure: false,
		},
		{
			name:                 "serviceerror-internal",
			err:                  serviceerror.NewInternal("internal"),
			expectServiceFailure: true,
		},
		{
			name:                 "statuserror-internal",
			err:                  status.Error(codes.Internal, "internal"),
			expectServiceFailure: true,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			metricsHandler.EXPECT().Counter(metrics.ServiceErrorWithType.Name()).Return(metrics.NoopCounterMetricFunc).Times(1)
			times := 0
			if tt.expectServiceFailure {
				times = 1
			}
			metricsHandler.EXPECT().Counter(metrics.ServiceFailures.Name()).Return(metrics.NoopCounterMetricFunc).Times(times)
			telemetry.HandleError(nil, metricsHandler, []tag.Tag{}, tt.err)
		})
	}
}

func TestOperationOverwrite(t *testing.T) {
	controller := gomock.NewController(t)
	register := namespace.NewMockRegistry(controller)
	metricsHandler := metrics.NewMockHandler(controller)
	telemetry := NewTelemetryInterceptor(register, metricsHandler, log.NewNoopLogger())

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
	telemetry := NewTelemetryInterceptor(registry, metricsHandler, log.NewTestLogger())

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
			name:       "Frontend StartWorkflowExecutionRequest with only workflowID",
			req:        &workflowservice.StartWorkflowExecutionRequest{WorkflowId: wid},
			workflowID: wid,
		},
		{
			name:       "Frontend RecordActivityTaskHeartbeatByIdRequest with workflowID and runID",
			req:        &workflowservice.RecordActivityTaskHeartbeatByIdRequest{WorkflowId: wid, RunId: rid},
			workflowID: wid,
			runID:      rid,
		},
		{
			name: "Frontend GetWorkflowExecutionHistoryRequest with execution",
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
			name: "Frontend RequestCancelWorkflowExecutionRequest with workflow_execution",
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
			name: "Frontend RespondActivityTaskCompletedRequest with task_token",
			req: &workflowservice.RespondActivityTaskCompletedRequest{
				TaskToken: taskTokenBytes,
			},
			workflowID: wid,
			runID:      rid,
		},
		{
			name: "Frontend RespondQueryTaskCompletedRequest (task_token is ignored)",
			req: &workflowservice.RespondQueryTaskCompletedRequest{
				TaskToken: taskTokenBytes,
			},
		},
		{
			name: "History DescribeWorkflowExecutionRequest",
			req: &historyservice.DescribeWorkflowExecutionRequest{
				Request: &workflowservice.DescribeWorkflowExecutionRequest{
					Execution: &commonpb.WorkflowExecution{
						WorkflowId: wid,
						RunId:      rid,
					},
				},
			},
			workflowID: wid,
			runID:      rid,
		},
		{
			name: "History RespondWorkflowTaskCompletedRequest",
			req: &historyservice.RespondWorkflowTaskCompletedRequest{
				CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
					TaskToken: taskTokenBytes,
				},
			},
			workflowID: wid,
			runID:      rid,
		},
		{
			name: "Matching QueryWorkflowRequest",
			req: &matchingservice.QueryWorkflowRequest{
				QueryRequest: &workflowservice.QueryWorkflowRequest{
					Execution: &commonpb.WorkflowExecution{
						WorkflowId: wid,
						RunId:      rid,
					},
				},
			},
			workflowID: wid,
			runID:      rid,
		},
		{
			name: "Matching RespondWorkflowTaskCompletedRequest",
			req: &matchingservice.RespondQueryTaskCompletedRequest{
				CompletedRequest: &workflowservice.RespondQueryTaskCompletedRequest{
					TaskToken: taskTokenBytes,
				},
			},
		},
		{
			name: "Nil request",
			req:  nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			tags := telemetry.getWorkflowTags(tt.req)
			var (
				workflowIDTag tag.Tag
				runIDTag      tag.Tag
			)
			for _, tg := range tags {
				if tg.Key() == tag.WorkflowID("").Key() {
					workflowIDTag = tg
				}
				if tg.Key() == tag.WorkflowRunID("").Key() {
					runIDTag = tg
				}
			}

			if tt.workflowID != "" {
				assert.NotNil(t, workflowIDTag)
				assert.Equal(t, workflowIDTag.Value(), tt.workflowID)
			} else {
				assert.Nil(t, workflowIDTag)
			}
			if tt.runID != "" {
				assert.NotNil(t, runIDTag)
				assert.Equal(t, runIDTag.Value(), tt.runID)
			} else {
				assert.Nil(t, runIDTag)
			}
		})
	}
}

func TestOperationOverride(t *testing.T) {
	controller := gomock.NewController(t)
	register := namespace.NewMockRegistry(controller)
	metricsHandler := metrics.NewMockHandler(controller)
	telemetry := NewTelemetryInterceptor(register, metricsHandler, log.NewNoopLogger())

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
