package deletedlqtasks

import (
	"context"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/tasks"
)

func Invoke(
	ctx context.Context,
	historyTaskQueueManager persistence.HistoryTaskQueueManager,
	req *historyservice.DeleteDLQTasksRequest,
	registry tasks.TaskCategoryRegistry,
) (*historyservice.DeleteDLQTasksResponse, error) {
	category, err := api.GetTaskCategory(int(req.DlqKey.TaskCategory), registry)
	if err != nil {
		return nil, err
	}

	if req.InclusiveMaxTaskMetadata == nil {
		return nil, serviceerror.NewInvalidArgument("must supply inclusive_max_task_metadata")
	}

	resp, err := historyTaskQueueManager.DeleteTasks(ctx, &persistence.DeleteTasksRequest{
		QueueKey: persistence.QueueKey{
			QueueType:     persistence.QueueTypeHistoryDLQ,
			Category:      category,
			SourceCluster: req.DlqKey.SourceCluster,
			TargetCluster: req.DlqKey.TargetCluster,
		},
		InclusiveMaxMessageMetadata: persistence.MessageMetadata{
			ID: req.InclusiveMaxTaskMetadata.MessageId,
		},
	})
	if err != nil {
		return nil, err
	}

	return &historyservice.DeleteDLQTasksResponse{MessagesDeleted: resp.MessagesDeleted}, nil
}
