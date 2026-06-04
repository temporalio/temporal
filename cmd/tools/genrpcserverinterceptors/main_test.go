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
		name              string
		reqT              reflect.Type
		workflowIDGetter  string
		runIDGetter       string
		taskTokenGetter   string
		activityIDGetter  string
		operationIDGetter string
		chasmRunIDGetter  string
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
			activityIDGetter: "GetActivityId()",
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
		{
			name:             "Chasm activity request with activity_id and run_id",
			reqT:             reflect.TypeOf(&workflowservice.DescribeActivityExecutionRequest{}),
			activityIDGetter: "GetActivityId()",
			chasmRunIDGetter: "GetRunId()",
		},
		{
			name:             "Chasm activity request with only activity_id",
			reqT:             reflect.TypeOf(&workflowservice.StartActivityExecutionRequest{}),
			activityIDGetter: "GetActivityId()",
		},
		{
			name:              "History request with nested operation_id",
			reqT:              reflect.TypeOf(&historyservice.CancelNexusOperationRequest{}),
			operationIDGetter: "GetRequest().GetOperationId()",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			rd := workflowTagGetters(tt.reqT, 0)
			assert.Equal(t, tt.workflowIDGetter, rd.WorkflowIDGetter, "WorkflowIDGetter")
			assert.Equal(t, tt.runIDGetter, rd.RunIDGetter, "RunIDGetter")
			assert.Equal(t, tt.taskTokenGetter, rd.TaskTokenGetter, "TaskTokenGetter")
			assert.Equal(t, tt.activityIDGetter, rd.ActivityIDGetter, "ActivityIDGetter")
			assert.Equal(t, tt.operationIDGetter, rd.OperationIDGetter, "OperationIDGetter")
			assert.Equal(t, tt.chasmRunIDGetter, rd.ChasmRunIDGetter, "ChasmRunIDGetter")
		})
	}
}
