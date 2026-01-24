package listtasks

import (
	"context"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func Invoke(
	ctx context.Context,
	taskCategoryRegistry tasks.TaskCategoryRegistry,
	executionManager persistence.ExecutionManager,
	request *historyservice.ListTasksRequest,
) (*historyservice.ListTasksResponse, error) {
	adminRequest := request.GetRequest()
	taskCategory, ok := taskCategoryRegistry.GetCategoryByID(int(adminRequest.GetCategory()))
	if !ok {
		return nil, &serviceerror.InvalidArgument{
			Message: fmt.Sprintf("unknown task category: %v", adminRequest.GetCategory()),
		}
	}

	taskRange := adminRequest.GetTaskRange()
	var minTaskKey, maxTaskKey tasks.Key
	if taskRange.HasInclusiveMinTaskKey() {
		minTaskKey = tasks.NewKey(
			timestamp.TimeValue(taskRange.GetInclusiveMinTaskKey().GetFireTime()),
			taskRange.GetInclusiveMinTaskKey().GetTaskId(),
		)
		if err := tasks.ValidateKey(minTaskKey); err != nil {
			return nil, &serviceerror.InvalidArgument{
				Message: fmt.Sprintf("invalid minTaskKey: %v", err.Error()),
			}
		}
	}
	if taskRange.HasExclusiveMaxTaskKey() {
		maxTaskKey = tasks.NewKey(
			timestamp.TimeValue(taskRange.GetExclusiveMaxTaskKey().GetFireTime()),
			taskRange.GetExclusiveMaxTaskKey().GetTaskId(),
		)
		if err := tasks.ValidateKey(maxTaskKey); err != nil {
			return nil, &serviceerror.InvalidArgument{
				Message: fmt.Sprintf("invalid maxTaskKey: %v", err.Error()),
			}
		}
	}

	resp, err := executionManager.GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
		ShardID:             adminRequest.GetShardId(),
		TaskCategory:        taskCategory,
		InclusiveMinTaskKey: minTaskKey,
		ExclusiveMaxTaskKey: maxTaskKey,
		BatchSize:           int(adminRequest.GetBatchSize()),
		NextPageToken:       adminRequest.GetNextPageToken(),
	})
	if err != nil {
		return nil, err
	}

	return historyservice.ListTasksResponse_builder{
		Response: adminservice.ListHistoryTasksResponse_builder{
			Tasks:         toAdminTask(resp.Tasks),
			NextPageToken: resp.NextPageToken,
		}.Build(),
	}.Build(), nil
}

func toAdminTask(historyTasks []tasks.Task) []*adminservice.Task {
	var adminTasks []*adminservice.Task
	for _, historyTask := range historyTasks {
		historyTaskVersion := common.EmptyVersion
		if taskWithVersion, ok := historyTask.(tasks.HasVersion); ok {
			historyTaskVersion = taskWithVersion.GetVersion()
		}

		adminTasks = append(adminTasks, adminservice.Task_builder{
			NamespaceId: historyTask.GetNamespaceID(),
			WorkflowId:  historyTask.GetWorkflowID(),
			RunId:       historyTask.GetRunID(),
			TaskId:      historyTask.GetTaskID(),
			TaskType:    historyTask.GetType(),
			FireTime:    timestamppb.New(historyTask.GetKey().FireTime),
			Version:     historyTaskVersion,
		}.Build())
	}
	return adminTasks
}
