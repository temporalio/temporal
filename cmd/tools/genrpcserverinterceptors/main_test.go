package main

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
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
			reqT:             reflect.TypeFor[*workflowservice.StartWorkflowExecutionRequest](),
			workflowIDGetter: "GetWorkflowId()",
		},
		{
			name:             "Request with workflowID and runID",
			reqT:             reflect.TypeFor[*workflowservice.RecordActivityTaskHeartbeatByIdRequest](),
			workflowIDGetter: "GetWorkflowId()",
			runIDGetter:      "GetRunId()",
			activityIDGetter: "GetActivityId()",
		},
		{
			name:             "Request with execution",
			reqT:             reflect.TypeFor[*workflowservice.GetWorkflowExecutionHistoryRequest](),
			workflowIDGetter: "GetExecution().GetWorkflowId()",
			runIDGetter:      "GetExecution().GetRunId()",
		},
		{
			name:             "Request with workflow_execution",
			reqT:             reflect.TypeFor[*workflowservice.RequestCancelWorkflowExecutionRequest](),
			workflowIDGetter: "GetWorkflowExecution().GetWorkflowId()",
			runIDGetter:      "GetWorkflowExecution().GetRunId()",
		},
		{
			name:            "Request with task_token",
			reqT:            reflect.TypeFor[*workflowservice.RespondActivityTaskCompletedRequest](),
			taskTokenGetter: "GetTaskToken()",
		},
		{
			name: "Special handling for RespondQueryTaskCompletedRequest",
			reqT: reflect.TypeFor[*workflowservice.RespondQueryTaskCompletedRequest](),
		},
		{
			name: "Special handling for PollNexusTaskQueueResponse",
			reqT: reflect.TypeFor[*workflowservice.PollNexusTaskQueueResponse](),
		},
		{
			name:             "Matching request",
			reqT:             reflect.TypeFor[*matchingservice.QueryWorkflowRequest](),
			workflowIDGetter: "GetQueryRequest().GetExecution().GetWorkflowId()",
			runIDGetter:      "GetQueryRequest().GetExecution().GetRunId()",
		},
		{
			name:             "History request",
			reqT:             reflect.TypeFor[*historyservice.SignalWorkflowExecutionRequest](),
			workflowIDGetter: "GetSignalRequest().GetWorkflowExecution().GetWorkflowId()",
			runIDGetter:      "GetSignalRequest().GetWorkflowExecution().GetRunId()",
		},
		{
			name:             "History request overrides",
			reqT:             reflect.TypeFor[*historyservice.ReplicateWorkflowStateRequest](),
			workflowIDGetter: "GetWorkflowState().GetExecutionInfo().GetWorkflowId()",
			runIDGetter:      "GetWorkflowState().GetExecutionState().GetRunId()",
		},
		{
			name:             "Chasm activity request with activity_id and run_id",
			reqT:             reflect.TypeFor[*workflowservice.DescribeActivityExecutionRequest](),
			activityIDGetter: "GetActivityId()",
			chasmRunIDGetter: "GetRunId()",
		},
		{
			name:             "Chasm activity request with only activity_id",
			reqT:             reflect.TypeFor[*workflowservice.StartActivityExecutionRequest](),
			activityIDGetter: "GetActivityId()",
		},
		{
			name:              "History request with nested operation_id",
			reqT:              reflect.TypeFor[*historyservice.CancelNexusOperationRequest](),
			operationIDGetter: "GetRequest().GetOperationId()",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			rd := workflowTagGetters(tt.reqT, 0)
			require.Equal(t, tt.workflowIDGetter, rd.WorkflowIDGetter, "WorkflowIDGetter")
			require.Equal(t, tt.runIDGetter, rd.RunIDGetter, "RunIDGetter")
			require.Equal(t, tt.taskTokenGetter, rd.TaskTokenGetter, "TaskTokenGetter")
			require.Equal(t, tt.activityIDGetter, rd.ActivityIDGetter, "ActivityIDGetter")
			require.Equal(t, tt.operationIDGetter, rd.OperationIDGetter, "OperationIDGetter")
			require.Equal(t, tt.chasmRunIDGetter, rd.ChasmRunIDGetter, "ChasmRunIDGetter")
		})
	}
}
