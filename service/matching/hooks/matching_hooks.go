package hooks

import (
	"context"

	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/common/namespace"
)

type (
	TaskMatchHookDetails struct {
		Namespace               *namespace.Namespace
		TaskQueueName           string
		TaskQueueType           enumspb.TaskQueueType
		DeploymentVersion       *deploymentpb.WorkerDeploymentVersion
		IsSyncMatch             bool
		TaskQueueStatsRetriever func() map[int32]*taskqueuepb.TaskQueueStats
	}
	TaskMatchHook interface {
		ProcessTaskMatch(ctx context.Context, event *TaskMatchHookDetails)
	}
)
