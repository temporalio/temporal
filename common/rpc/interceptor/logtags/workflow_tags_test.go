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
	mustSerialize := func(token *tokenspb.Task) []byte {
		t.Helper()
		taskTokenBytes, err := serializer.Serialize(token)
		assert.NoError(t, err)
		return taskTokenBytes
	}

	wt := logtags.NewWorkflowTags(
		serializer,
		log.NewTestLogger(),
	)

	tv := testvars.New(t)
	tv = tv.WithRunID(tv.Any().RunID())
	taskTokenBytes := mustSerialize(&tokenspb.Task{
		WorkflowId: tv.WorkflowID(),
		RunId:      tv.RunID(),
	})

	type expectation struct {
		workflowID string
		activityID string
		runID      string
	}

	testCases := []struct {
		name       string
		req        any
		fullMethod string
		expect     expectation
	}{
		{
			name:       "Frontend StartWorkflowExecutionRequest with only workflowID",
			req:        &workflowservice.StartWorkflowExecutionRequest{WorkflowId: tv.WorkflowID()},
			fullMethod: "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution",
			expect:     expectation{workflowID: tv.WorkflowID()},
		},
		{
			name:       "Frontend RecordActivityTaskHeartbeatByIdRequest with workflowID and runID",
			req:        &workflowservice.RecordActivityTaskHeartbeatByIdRequest{WorkflowId: tv.WorkflowID(), RunId: tv.RunID()},
			fullMethod: "/temporal.api.workflowservice.v1.WorkflowService/RecordActivityTaskHeartbeatById",
			expect:     expectation{workflowID: tv.WorkflowID(), runID: tv.RunID()},
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
			expect:     expectation{workflowID: tv.WorkflowID(), runID: tv.RunID()},
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
			expect:     expectation{workflowID: tv.WorkflowID(), runID: tv.RunID()},
		},
		{
			name: "Frontend RespondActivityTaskCompletedRequest with task_token",
			req: &workflowservice.RespondActivityTaskCompletedRequest{
				TaskToken: taskTokenBytes,
			},
			fullMethod: "/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCompleted",
			expect:     expectation{workflowID: tv.WorkflowID(), runID: tv.RunID()},
		},
		{
			name: "Frontend RespondActivityTaskCompletedRequest with workflow activity task_token",
			req: &workflowservice.RespondActivityTaskCompletedRequest{
				TaskToken: mustSerialize(&tokenspb.Task{
					WorkflowId: tv.WorkflowID(),
					RunId:      tv.RunID(),
					ActivityId: "workflow-activity-id",
				}),
			},
			fullMethod: "/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCompleted",
			expect:     expectation{workflowID: tv.WorkflowID(), runID: tv.RunID()},
		},
		{
			name: "Frontend RespondActivityTaskCompletedRequest with CHASM task_token",
			req: &workflowservice.RespondActivityTaskCompletedRequest{
				TaskToken: mustSerialize(&tokenspb.Task{
					RunId:        tv.RunID(),
					ActivityId:   "activity-id",
					ComponentRef: []byte("component-ref"),
				}),
			},
			fullMethod: "/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCompleted",
			expect:     expectation{activityID: "activity-id", runID: tv.RunID()},
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
			expect:     expectation{workflowID: tv.WorkflowID(), runID: tv.RunID()},
		},
		{
			name: "History RespondWorkflowTaskCompletedRequest",
			req: &historyservice.RespondWorkflowTaskCompletedRequest{
				CompleteRequest: &workflowservice.RespondWorkflowTaskCompletedRequest{
					TaskToken: taskTokenBytes,
				},
			},
			fullMethod: "/temporal.server.api.historyservice.v1.HistoryService/RespondWorkflowTaskCompleted",
			expect:     expectation{workflowID: tv.WorkflowID(), runID: tv.RunID()},
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
			expect:     expectation{workflowID: tv.WorkflowID(), runID: tv.RunID()},
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
			var got expectation
			for _, tg := range wt.Extract(tt.req, tt.fullMethod) {
				switch tg.Key() {
				case tag.WorkflowID("").Key():
					got.workflowID = tg.Value().(string)
				case tag.ActivityID("").Key():
					got.activityID = tg.Value().(string)
				case tag.WorkflowRunID("").Key():
					got.runID = tg.Value().(string)
				default:
				}
			}
			assert.Equal(t, tt.expect, got)
		})
	}
}
