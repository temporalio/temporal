package main

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
)

func TestWorkflowTagGetters(t *testing.T) {
	testCases := []struct {
		name             string
		reqT             reflect.Type
		workflowIDGetter string
		runIDGetter      string
		taskTokenGetter  string
	}{
		{
			name:             "Request with only workflowID",
			reqT:             reflect.TypeOf(&workflowservice.StartWorkflowExecutionRequest{}),
			workflowIDGetter: "GetWorkflowId()",
		},
		{
			name:             "Request with workflowID and runID",
			reqT:             reflect.TypeOf(&workflowservice.RecordActivityTaskHeartbeatByIdRequest{}),
			workflowIDGetter: "GetWorkflowId()",
			runIDGetter:      "GetRunId()",
		},
		{
			name:             "Request with execution",
			reqT:             reflect.TypeOf(&workflowservice.GetWorkflowExecutionHistoryRequest{}),
			workflowIDGetter: "GetExecution().GetWorkflowId()",
			runIDGetter:      "GetExecution().GetRunId()",
		},
		{
			name:             "Request with workflow_execution",
			reqT:             reflect.TypeOf(&workflowservice.RequestCancelWorkflowExecutionRequest{}),
			workflowIDGetter: "GetWorkflowExecution().GetWorkflowId()",
			runIDGetter:      "GetWorkflowExecution().GetRunId()",
		},
		{
			name:            "Request with task_token",
			reqT:            reflect.TypeOf(&workflowservice.RespondActivityTaskCompletedRequest{}),
			taskTokenGetter: "GetTaskToken()",
		},
		{
			name: "Special handling for RespondQueryTaskCompletedRequest",
			reqT: reflect.TypeOf(&workflowservice.RespondQueryTaskCompletedRequest{}),
		},
		{
			name:             "Matching request",
			reqT:             reflect.TypeOf(&matchingservice.QueryWorkflowRequest{}),
			workflowIDGetter: "GetQueryRequest().GetExecution().GetWorkflowId()",
			runIDGetter:      "GetQueryRequest().GetExecution().GetRunId()",
		},
		{
			name:             "History request",
			reqT:             reflect.TypeOf(&historyservice.SignalWorkflowExecutionRequest{}),
			workflowIDGetter: "GetSignalRequest().GetWorkflowExecution().GetWorkflowId()",
			runIDGetter:      "GetSignalRequest().GetWorkflowExecution().GetRunId()",
		},
		{
			name:             "History request overrides",
			reqT:             reflect.TypeOf(&historyservice.ReplicateWorkflowStateRequest{}),
			workflowIDGetter: "GetWorkflowState().GetExecutionInfo().GetWorkflowId()",
			runIDGetter:      "GetWorkflowState().GetExecutionState().GetRunId()",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			rd := workflowTagGetters(tt.reqT, 0)
			if tt.workflowIDGetter != "" {
				assert.Equal(t, tt.workflowIDGetter, rd.WorkflowIdGetter)
			}
			if tt.runIDGetter != "" {
				assert.Equal(t, tt.runIDGetter, rd.RunIdGetter)
			}
			if tt.taskTokenGetter != "" {
				assert.Equal(t, tt.taskTokenGetter, rd.TaskTokenGetter)
			}
		})
	}
}
