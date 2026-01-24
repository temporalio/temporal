package logtags_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/rpc/interceptor/logtags"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/testing/testvars"
)

func TestExtract(t *testing.T) {
	serializer := tasktoken.NewSerializer()

	wt := logtags.NewWorkflowTags(
		serializer,
		log.NewTestLogger(),
	)

	tv := testvars.New(t)
	tv = tv.WithRunID(tv.Any().RunID())
	taskToken := tokenspb.Task_builder{
		WorkflowId: tv.WorkflowID(),
		RunId:      tv.RunID(),
	}.Build()
	taskTokenBytes, err := serializer.Serialize(taskToken)
	assert.NoError(t, err)

	testCases := []struct {
		name       string
		req        interface{}
		fullMethod string
		workflowID string
		runID      string
	}{
		{
			name:       "Frontend StartWorkflowExecutionRequest with only workflowID",
			req:        workflowservice.StartWorkflowExecutionRequest_builder{WorkflowId: tv.WorkflowID()}.Build(),
			fullMethod: "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution",
			workflowID: tv.WorkflowID(),
		},
		{
			name:       "Frontend RecordActivityTaskHeartbeatByIdRequest with workflowID and runID",
			req:        workflowservice.RecordActivityTaskHeartbeatByIdRequest_builder{WorkflowId: tv.WorkflowID(), RunId: tv.RunID()}.Build(),
			fullMethod: "/temporal.api.workflowservice.v1.WorkflowService/RecordActivityTaskHeartbeatById",
			workflowID: tv.WorkflowID(),
			runID:      tv.RunID(),
		},
		{
			name: "Frontend GetWorkflowExecutionHistoryRequest with execution",
			req: workflowservice.GetWorkflowExecutionHistoryRequest_builder{
				Execution: commonpb.WorkflowExecution_builder{
					WorkflowId: tv.WorkflowID(),
					RunId:      tv.RunID(),
				}.Build(),
			}.Build(),
			fullMethod: "/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecutionHistory",
			workflowID: tv.WorkflowID(),
			runID:      tv.RunID(),
		},
		{
			name: "Frontend RequestCancelWorkflowExecutionRequest with workflow_execution",
			req: workflowservice.RequestCancelWorkflowExecutionRequest_builder{
				WorkflowExecution: commonpb.WorkflowExecution_builder{
					WorkflowId: tv.WorkflowID(),
					RunId:      tv.RunID(),
				}.Build(),
			}.Build(),
			fullMethod: "/temporal.api.workflowservice.v1.WorkflowService/RequestCancelWorkflowExecution",
			workflowID: tv.WorkflowID(),
			runID:      tv.RunID(),
		},
		{
			name: "Frontend RespondActivityTaskCompletedRequest with task_token",
			req: workflowservice.RespondActivityTaskCompletedRequest_builder{
				TaskToken: taskTokenBytes,
			}.Build(),
			fullMethod: "/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCompleted",
			workflowID: tv.WorkflowID(),
			runID:      tv.RunID(),
		},
		{
			name: "Frontend RespondQueryTaskCompletedRequest (task_token is ignored)",
			req: workflowservice.RespondQueryTaskCompletedRequest_builder{
				TaskToken: taskTokenBytes,
			}.Build(),
			fullMethod: "/temporal.api.workflowservice.v1.WorkflowService/RespondQueryTaskCompleted",
		},
		{
			name: "History DescribeWorkflowExecutionRequest",
			req: historyservice.DescribeWorkflowExecutionRequest_builder{
				Request: workflowservice.DescribeWorkflowExecutionRequest_builder{
					Execution: commonpb.WorkflowExecution_builder{
						WorkflowId: tv.WorkflowID(),
						RunId:      tv.RunID(),
					}.Build(),
				}.Build(),
			}.Build(),
			fullMethod: "/temporal.server.api.historyservice.v1.HistoryService/DescribeWorkflowExecution",
			workflowID: tv.WorkflowID(),
			runID:      tv.RunID(),
		},
		{
			name: "History RespondWorkflowTaskCompletedRequest",
			req: historyservice.RespondWorkflowTaskCompletedRequest_builder{
				CompleteRequest: workflowservice.RespondWorkflowTaskCompletedRequest_builder{
					TaskToken: taskTokenBytes,
				}.Build(),
			}.Build(),
			fullMethod: "/temporal.server.api.historyservice.v1.HistoryService/RespondWorkflowTaskCompleted",
			workflowID: tv.WorkflowID(),
			runID:      tv.RunID(),
		},
		{
			name: "Matching QueryWorkflowRequest",
			req: matchingservice.QueryWorkflowRequest_builder{
				QueryRequest: workflowservice.QueryWorkflowRequest_builder{
					Execution: commonpb.WorkflowExecution_builder{
						WorkflowId: tv.WorkflowID(),
						RunId:      tv.RunID(),
					}.Build(),
				}.Build(),
			}.Build(),
			fullMethod: "/temporal.server.api.matchingservice.v1.MatchingService/QueryWorkflow",
			workflowID: tv.WorkflowID(),
			runID:      tv.RunID(),
		},
		{
			name: "Matching RespondWorkflowTaskCompletedRequest",
			req: matchingservice.RespondQueryTaskCompletedRequest_builder{
				CompletedRequest: workflowservice.RespondQueryTaskCompletedRequest_builder{
					TaskToken: taskTokenBytes,
				}.Build(),
			}.Build(),
			fullMethod: "/temporal.server.api.matchingservice.v1.MatchingService/RespondQueryTaskCompleted",
		},
		{
			name: "Nil request",
			req:  nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			tags := wt.Extract(tt.req, tt.fullMethod)
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
