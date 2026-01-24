// Package getdlqtasks contains the logic to implement the [historyservice.HistoryServiceServer.GetDLQTasks] API.
package getdlqtasks

import (
	"context"
	"errors"

	"go.temporal.io/api/serviceerror"
	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/tasks"
)

// Invoke the GetDLQTasks API. All errors returned from this function are already translated into the appropriate type
// from the [serviceerror] package.
func Invoke(
	ctx context.Context,
	historyTaskQueueManager persistence.HistoryTaskQueueManager,
	taskCategoryRegistry tasks.TaskCategoryRegistry,
	req *historyservice.GetDLQTasksRequest,
) (*historyservice.GetDLQTasksResponse, error) {
	category, err := api.GetTaskCategory(int(req.GetDlqKey().GetTaskCategory()), taskCategoryRegistry)
	if err != nil {
		return nil, err
	}

	response, err := historyTaskQueueManager.ReadRawTasks(ctx, &persistence.ReadTasksRequest{
		QueueKey: persistence.QueueKey{
			QueueType:     persistence.QueueTypeHistoryDLQ,
			Category:      category,
			SourceCluster: req.GetDlqKey().GetSourceCluster(),
			TargetCluster: req.GetDlqKey().GetTargetCluster(),
		},
		PageSize:      int(req.GetPageSize()),
		NextPageToken: req.GetNextPageToken(),
	})
	if err != nil {
		if errors.Is(err, persistence.ErrReadTasksNonPositivePageSize) {
			return nil, consts.ErrInvalidPageSize
		}

		return nil, serviceerror.NewUnavailablef("GetDLQTasks failed. Error: %v", err)
	}

	dlqTasks := make([]*commonspb.HistoryDLQTask, len(response.Tasks))
	for i, task := range response.Tasks {
		dlqTasks[i] = commonspb.HistoryDLQTask_builder{
			Metadata: commonspb.HistoryDLQTaskMetadata_builder{
				MessageId: task.MessageMetadata.ID,
			}.Build(),
			Payload: commonspb.HistoryTask_builder{
				ShardId: task.Payload.GetShardId(),
				Blob:    task.Payload.GetBlob(),
			}.Build(),
		}.Build()
	}

	return historyservice.GetDLQTasksResponse_builder{
		DlqTasks:      dlqTasks,
		NextPageToken: response.NextPageToken,
	}.Build(), nil
}
