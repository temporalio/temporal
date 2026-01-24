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
	category, err := api.GetTaskCategory(int(req.GetDlqKey().GetTaskCategory()), registry)
	if err != nil {
		return nil, err
	}

	if !req.HasInclusiveMaxTaskMetadata() {
		return nil, serviceerror.NewInvalidArgument("must supply inclusive_max_task_metadata")
	}

	resp, err := historyTaskQueueManager.DeleteTasks(ctx, &persistence.DeleteTasksRequest{
		QueueKey: persistence.QueueKey{
			QueueType:     persistence.QueueTypeHistoryDLQ,
			Category:      category,
			SourceCluster: req.GetDlqKey().GetSourceCluster(),
			TargetCluster: req.GetDlqKey().GetTargetCluster(),
		},
		InclusiveMaxMessageMetadata: persistence.MessageMetadata{
			ID: req.GetInclusiveMaxTaskMetadata().GetMessageId(),
		},
	})
	if err != nil {
		return nil, err
	}

	return historyservice.DeleteDLQTasksResponse_builder{MessagesDeleted: resp.MessagesDeleted}.Build(), nil
}
