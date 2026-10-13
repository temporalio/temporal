// Package eagerdispatch contains shared client-side helpers for eager dispatch admission.
package eagerdispatch

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
)

// Grant asks Matching whether the activity task queue may eagerly dispatch each item.
func Grant(
	ctx context.Context,
	client matchingservice.MatchingServiceClient,
	namespaceID string,
	taskQueue string,
	items []*matchingservice.GrantEagerDispatchRequest_Item,
) (*matchingservice.GrantEagerDispatchResponse, error) {
	return client.GrantEagerDispatch(ctx, &matchingservice.GrantEagerDispatchRequest{
		NamespaceId: namespaceID,
		TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
			TaskQueue:     taskQueue,
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY,
		},
		Items: items,
	})
}
