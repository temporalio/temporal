//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination physical_task_queue_manager_mock.go

package matching

import (
	"context"
	"time"

	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
)

type (
	physicalTaskQueueManager interface {
		Start()
		Stop(unloadCause)
		WaitUntilInitialized(context.Context) error
		SetupDraining()
		// PollTask blocks waiting for a task Returns error when context deadline is exceeded
		// maxDispatchPerSecond is the max rate at which tasks are allowed to be dispatched
		// from this task queue to pollers
		PollTask(ctx context.Context, pollMetadata *pollMetadata) (*internalTask, error)
		// MarkAlive updates the liveness timer to keep this physicalTaskQueueManager alive.
		MarkAlive()
		// TrySyncMatch tries to match task to a local or remote poller. If not possible, returns false.
		TrySyncMatch(ctx context.Context, task *internalTask) (bool, error)
		// SpoolTask spools a task to persistence to be matched asynchronously when a poller is available.
		SpoolTask(taskInfo *persistencespb.TaskInfo) error
		// TODO(pri): old matcher cleanup
		ProcessSpooledTask(ctx context.Context, task *internalTask) error
		// DispatchSpooledTask dispatches a task to a poller. When there are no pollers to pick
		// up the task, this method will return error. Task will not be persisted to db
		// TODO(pri): old matcher cleanup
		DispatchSpooledTask(ctx context.Context, task *internalTask, userDataChanged <-chan struct{}) error
		AddSpooledTask(task *internalTask) error
		AddSpooledTaskToMatcher(task *internalTask)
		UserDataChanged()
		// DispatchQueryTask will dispatch query to local or remote poller. If forwarded then result or error is returned,
		// if dispatched to local poller then nil and nil is returned.
		DispatchQueryTask(ctx context.Context, taskId string, request *matchingservice.QueryWorkflowRequest) (*matchingservice.QueryWorkflowResponse, error)
		// DispatchNexusTask dispatches a nexus task to a local or remote poller. If forwarded then result or
		// error is returned, if dispatched to local poller then nil and nil is returned.
		DispatchNexusTask(ctx context.Context, taskId string, request *matchingservice.DispatchNexusTaskRequest) (*matchingservice.DispatchNexusTaskResponse, error)
		UpdatePollerInfo(pollerIdentity, *pollMetadata)
		GetAllPollerInfo() []*taskqueuepb.PollerInfo
		HasPollerAfter(accessTime time.Time) bool
		// LegacyDescribeTaskQueue returns pollers info and legacy TaskQueueStatus for this physical queue
		LegacyDescribeTaskQueue(includeTaskQueueStatus bool) *matchingservice.DescribeTaskQueueResponse
		GetStatsByPriority() map[int32]*taskqueuepb.TaskQueueStats
		GetInternalTaskQueueStatus() []*taskqueuespb.InternalTaskQueueStatus
		UnloadFromPartitionManager(unloadCause)
		QueueKey() *PhysicalTaskQueueKey
		// MakePollerScalingDecision makes a decision on whether to scale pollers up or down based on the current state
		// of the task queue and the task about to be returned.
		MakePollerScalingDecision(ctx context.Context, pollStartTime time.Time) *taskqueuepb.PollerScalingDecision
		// GetFairnessWeightOverrides returns current fairness weight overrides for this queue.
		GetFairnessWeightOverrides() fairnessWeightOverrides
	}
)
