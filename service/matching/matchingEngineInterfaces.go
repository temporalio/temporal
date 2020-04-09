package matching

import (
	"context"

	"github.com/temporalio/temporal/.gen/proto/matchingservice"
)

type (
	// Engine exposes interfaces for clients to poll for activity and decision tasks.
	Engine interface {
		Stop()
		AddDecisionTask(ctx context.Context, addRequest *matchingservice.AddDecisionTaskRequest) (syncMatch bool, err error)
		AddActivityTask(ctx context.Context, addRequest *matchingservice.AddActivityTaskRequest) (syncMatch bool, err error)
		PollForDecisionTask(ctx context.Context, request *matchingservice.PollForDecisionTaskRequest) (*matchingservice.PollForDecisionTaskResponse, error)
		PollForActivityTask(ctx context.Context, request *matchingservice.PollForActivityTaskRequest) (*matchingservice.PollForActivityTaskResponse, error)
		QueryWorkflow(ctx context.Context, request *matchingservice.QueryWorkflowRequest) (*matchingservice.QueryWorkflowResponse, error)
		RespondQueryTaskCompleted(ctx context.Context, request *matchingservice.RespondQueryTaskCompletedRequest) error
		CancelOutstandingPoll(ctx context.Context, request *matchingservice.CancelOutstandingPollRequest) error
		DescribeTaskList(ctx context.Context, request *matchingservice.DescribeTaskListRequest) (*matchingservice.DescribeTaskListResponse, error)
		ListTaskListPartitions(ctx context.Context, request *matchingservice.ListTaskListPartitionsRequest) (*matchingservice.ListTaskListPartitionsResponse, error)
	}
)
