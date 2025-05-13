package queryworkflow

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
)

func IsWorkersPolling(
	ctx context.Context,
	namespaceID string,
	taskQueue string,
	matchingClient matchingservice.MatchingServiceClient,
) bool {
	describeRequest := &matchingservice.DescribeTaskQueueRequest{
		NamespaceId: namespaceID,
		DescRequest: &workflowservice.DescribeTaskQueueRequest{
			TaskQueue: &taskqueue.TaskQueue{
				Name: taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
		},
	}
	describeResponse, err := matchingClient.DescribeTaskQueue(ctx, describeRequest)
	if err != nil {
		return false
	}
	return len(describeResponse.DescResponse.GetPollers()) != 0
}
