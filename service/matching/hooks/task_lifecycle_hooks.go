package hooks

import (
	"context"

	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tqid"
)

type (
	// TaskQueuePartition is a simplified version of tqid.Partition that removes details
	// the hooks should not concern themselves with
	TaskQueuePartition interface {
		NamespaceId() string
		TaskQueue() *tqid.TaskQueue
		TaskType() enumspb.TaskQueueType
		Kind() enumspb.TaskQueueKind
	}

	TaskHookFactoryCreateDetails struct {
		Namespace *namespace.Namespace
		Partition TaskQueuePartition
	}
	TaskAddHookDetails struct {
		DeploymentVersion *deploymentpb.WorkerDeploymentVersion
		IsSyncMatch       bool
	}

	TaskHookFactory interface {
		// Create returns a TaskHook instance that will be leveraged as part
		// of the specific task queue partition (as specified in the details).
		// This might also return nil, if no hooking into that task queue
		// partition is desired.
		Create(details *TaskHookFactoryCreateDetails) TaskHook
	}
	TaskHook interface {
		// Start is called when the task queue partition manager for the hooks partition is started
		Start()
		// Stop is called when the task queue partition manager for the hooks partition is stopped
		Stop()
		// ProcessTaskAdd is called for each Task addition (whether sync or async matching)
		ProcessTaskAdd(ctx context.Context, event *TaskAddHookDetails)
	}
)
