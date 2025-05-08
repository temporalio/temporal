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
	taskToken := tokenspb.Task{
		WorkflowId: tv.WorkflowID(),
		RunId:      tv.RunID(),
	}
	taskTokenBytes, err := serializer.Serialize(&taskToken)
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
			req:        &workflowservice.StartWorkflowExecutionRequest{WorkflowId: tv.WorkflowID()},
			fullMethod: "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution",
			workflowID: tv.WorkflowID(),
		},
		{
			name:       "Frontend RecordActivityTaskHeartbeatByIdRequest with workflowID and runID",
			req:        &workflowservice.RecordActivityTaskHeartbeatByIdRequest{WorkflowId: tv.WorkflowID(), RunId: tv.RunID()},
			fullMethod: "/temporal.api.workflowservice.v1.WorkflowService/RecordActivityTaskHeartbeatById",
			workflowID: tv.WorkflowID(),
			runID:      tv.RunID(),
		},
		{
			name: "Frontend GetWorkflowExecutionHistoryRequest with execution",
			req: &workflowservice.GetWorkflowExecutionHistoryRequest{
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: tv.WorkflowID(),
					RunId:      tv.RunID(),
				},
			},
			fullMethod: "/temporal.api.workflowservice.v1.WorkflowService/GetWorkflowExecutionHistory",
			workflowID: tv.WorkflowID(),
			runID:      tv.RunID(),
		},
		{
			name: "Frontend RequestCancelWorkflowExecutionRequest with workflow_execution",
			req: &workflowservice.RequestCancelWorkflowExecutionRequest{
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: tv.WorkflowID(),
					RunId:      tv.RunID(),
				},
			},
			fullMethod: "/temporal.api.workflowservice.v1.WorkflowService/RequestCancelWorkflowExecution",
			workflowID: tv.WorkflowID(),
			runID:      tv.RunID(),
		},
		{
			name: "Frontend RespondActivityTaskCompletedRequest with task_token",
			req: &workflowservice.RespondActivityTaskCompletedRequest{
				TaskToken: taskTokenBytes,
			},
			fullMethod: "/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCompleted",
			workflowID: tv.WorkflowID(),
			runID:      tv.RunID(),
		},
		{
			name: "Frontend RespondQueryTaskCompletedRequest (task_token is ignored)",
			req: &workflowservice.RespondQueryTaskCompletedRequest{
				TaskToken: taskTokenBytes,
			},
			fullMethod: "/temporal.api.workflowservice.v1.WorkflowService/RespondQueryTaskCompleted",
		},
		{
			name: "History DescribeWorkflowExecutionRequest",
			req: &historyservice.DescribeWorkflowExecutionRequest{
				Request: &workflowservice.DescribeWorkflowExecutionRequest{
					Execution: &commonpb.WorkflowExecution{
						WorkflowId: tv.WorkflowID(),
						RunId:      tv.RunID(),
					},
				},
			},
			fullMethod: "/temporal.server.api.historyservice.v1.HistoryService/DescribeWorkflowExecution",
			workflowID: tv.WorkflowID(),
			runID:      tv.RunID(),
		},
		{
			name: "History RespondWorkflowTaskCompletedRequest",
			req: &historyservice.RespondWorkflowTaskCompletedRequest{
				CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
					TaskToken: taskTokenBytes,
				},
			},
			fullMethod: "/temporal.server.api.historyservice.v1.HistoryService/RespondWorkflowTaskCompleted",
			workflowID: tv.WorkflowID(),
			runID:      tv.RunID(),
		},
		{
			name: "Matching QueryWorkflowRequest",
			req: &matchingservice.QueryWorkflowRequest{
				QueryRequest: &workflowservice.QueryWorkflowRequest{
					Execution: &commonpb.WorkflowExecution{
						WorkflowId: tv.WorkflowID(),
						RunId:      tv.RunID(),
					},
				},
			},
			fullMethod: "/temporal.server.api.matchingservice.v1.MatchingService/QueryWorkflow",
			workflowID: tv.WorkflowID(),
			runID:      tv.RunID(),
		},
		{
			name: "Matching RespondWorkflowTaskCompletedRequest",
			req: &matchingservice.RespondQueryTaskCompletedRequest{
				CompletedRequest: &workflowservice.RespondQueryTaskCompletedRequest{
					TaskToken: taskTokenBytes,
				},
			},
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
