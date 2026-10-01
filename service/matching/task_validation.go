//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination task_validation_mock.go
package matching

import (
	"context"
	"slices"
	"sync"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
)

const (
	taskReaderOfferTimeout        = 60 * time.Second // TODO(pri): old matcher cleanup
	taskReaderValidationThreshold = 600 * time.Second
)

type (
	taskValidator interface {
		// maybeValidate checks if a task has expired / is valid
		// if return false, then task is invalid and should be discarded
		// if return true, then task is *maybe-valid*, and should be dispatched
		//
		// a task is invalid if this task is already failed; timeout; completed, etc.
		// a task is *not invalid* if this task can be started, or caller cannot verify the validity
		maybeValidate(
			task *persistencespb.AllocatedTaskInfo,
			taskType enumspb.TaskQueueType,
		) bool
	}

	taskValidationInfo struct {
		taskID         int64
		validationTime time.Time
	}

	taskValidatorImpl struct {
		tqCtx             context.Context
		clusterMetadata   cluster.Metadata
		namespaceRegistry namespace.Registry
		historyClient     historyservice.HistoryServiceClient

		lock sync.Mutex
		// recentTasks is a ring of the most recently tracked tasks, with one slot per concurrent
		// caller so each caller's task stays tracked while it's put back and matched again.
		recentTasks []taskValidationInfo
		nextSlot    int
	}
)

func newTaskValidator(
	tqCtx context.Context,
	clusterMetadata cluster.Metadata,
	namespaceRegistry namespace.Registry,
	historyClient historyservice.HistoryServiceClient,
	concurrency int,
) *taskValidatorImpl {
	return &taskValidatorImpl{
		tqCtx:             tqCtx,
		clusterMetadata:   clusterMetadata,
		namespaceRegistry: namespaceRegistry,
		historyClient:     historyClient,
		recentTasks:       make([]taskValidationInfo, max(concurrency, 1)),
	}
}

func (v *taskValidatorImpl) maybeValidate(
	task *persistencespb.AllocatedTaskInfo,
	taskType enumspb.TaskQueueType,
) bool {
	if IsTaskExpired(task) {
		return false
	}
	if !v.preValidate(task) {
		return true
	}
	valid, err := v.isTaskValid(task, taskType)
	if err != nil {
		return true
	}
	v.postValidate(task)
	return valid
}

// preValidate track a task and return if validation should be done
func (v *taskValidatorImpl) preValidate(
	task *persistencespb.AllocatedTaskInfo,
) bool {
	namespaceID := task.Data.NamespaceId
	namespaceEntry, err := v.namespaceRegistry.GetNamespaceByID(namespace.ID(namespaceID))
	if err != nil {
		// if cannot find the namespace entry, treat task as active
		return v.preValidateActive(task)
	}
	// CONSIDER(fretz12): preValidate passes task.Data.WorkflowId as the routing key. The current namespace
	// resolver (defaultReplicationResolver) ignores routing keys, so standalone activities (with an empty
	// key) correctly use the namespace active cluster. If a future namespace resolver uses routing keys,
	// deserialize component_ref and use its business_id as the routing key; otherwise Matching can dispatch
	// a task from a passive cluster, causing duplicate activity execution and external side effects.
	if v.clusterMetadata.GetCurrentClusterName() == namespaceEntry.ActiveClusterName(namespace.RoutingKey{ID: task.Data.WorkflowId}) {
		return v.preValidateActive(task)
	}
	return v.preValidatePassive(task)
}

// preValidateActive track a task and return if validation should be done, if namespace is active
func (v *taskValidatorImpl) preValidateActive(
	task *persistencespb.AllocatedTaskInfo,
) bool {
	v.lock.Lock()
	defer v.lock.Unlock()

	info := v.findLocked(task.TaskId)
	if info == nil {
		// first time seen the task, caller should try to dispatch first
		if task.Data.CreateTime != nil {
			v.trackLocked(taskValidationInfo{
				taskID:         task.TaskId,
				validationTime: task.Data.CreateTime.AsTime(), // task is valid when created
			})
		} else {
			v.trackLocked(taskValidationInfo{
				taskID:         task.TaskId,
				validationTime: time.Now().UTC(), // if no creation time specified, use now
			})
		}
		return false
	}

	// this task has been validated before
	return time.Since(info.validationTime) > taskReaderValidationThreshold
}

// preValidatePassive track a task and return if validation should be done, if namespace is passive
func (v *taskValidatorImpl) preValidatePassive(
	task *persistencespb.AllocatedTaskInfo,
) bool {
	v.lock.Lock()
	defer v.lock.Unlock()

	info := v.findLocked(task.TaskId)
	if info == nil {
		// first time seen the task, make a decision based on task creation time
		if task.Data.CreateTime != nil {
			info = v.trackLocked(taskValidationInfo{
				taskID:         task.TaskId,
				validationTime: task.Data.CreateTime.AsTime(), // task is valid when created
			})
		} else {
			info = v.trackLocked(taskValidationInfo{
				taskID:         task.TaskId,
				validationTime: time.Now().UTC(), // if no creation time specified, use now
			})
		}
	}

	// this task has been validated before
	return time.Since(info.validationTime) > taskReaderValidationThreshold
}

// postValidate update tracked task info
func (v *taskValidatorImpl) postValidate(
	task *persistencespb.AllocatedTaskInfo,
) {
	v.lock.Lock()
	defer v.lock.Unlock()

	v.trackLocked(taskValidationInfo{
		taskID:         task.TaskId,
		validationTime: time.Now().UTC(),
	})
}

// call with lock held
func (v *taskValidatorImpl) findLocked(taskID int64) *taskValidationInfo {
	i := slices.IndexFunc(v.recentTasks, func(info taskValidationInfo) bool { return info.taskID == taskID })
	if i < 0 {
		return nil
	}
	return &v.recentTasks[i]
}

// trackLocked updates the task's slot, or replaces the oldest tracked task if it has none.
// Evicting a task only makes the next call treat it as unseen, which delays validation but
// never drops a task. call with lock held
func (v *taskValidatorImpl) trackLocked(info taskValidationInfo) *taskValidationInfo {
	slot := v.findLocked(info.taskID)
	if slot == nil {
		slot = &v.recentTasks[v.nextSlot]
		v.nextSlot = (v.nextSlot + 1) % len(v.recentTasks)
	}
	*slot = info
	return slot
}

func (v *taskValidatorImpl) isTaskValid(
	task *persistencespb.AllocatedTaskInfo,
	taskType enumspb.TaskQueueType,
) (bool, error) {
	ctx, cancel := context.WithTimeout(v.tqCtx, ioTimeout)
	defer cancel()

	namespaceID := task.Data.NamespaceId
	workflowID := task.Data.WorkflowId
	runID := task.Data.RunId

	switch taskType {
	case enumspb.TASK_QUEUE_TYPE_ACTIVITY:
		resp, err := v.historyClient.IsActivityTaskValid(ctx, &historyservice.IsActivityTaskValidRequest{
			NamespaceId: namespaceID,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			Clock:            task.Data.Clock,
			ScheduledEventId: task.Data.ScheduledEventId,
			Stamp:            task.Data.GetStamp(),
			ComponentRef:     task.Data.GetComponentRef(),
		})
		switch err.(type) {
		case nil:
			return resp.IsValid, nil
		case *serviceerror.NotFound:
			return false, nil
		default:
			return false, err
		}
	case enumspb.TASK_QUEUE_TYPE_WORKFLOW:
		resp, err := v.historyClient.IsWorkflowTaskValid(ctx, &historyservice.IsWorkflowTaskValidRequest{
			NamespaceId: namespaceID,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			Clock:            task.Data.Clock,
			ScheduledEventId: task.Data.ScheduledEventId,
			Stamp:            task.Data.GetStamp(),
		})
		switch err.(type) {
		case nil:
			return resp.IsValid, nil
		case *serviceerror.NotFound:
			return false, nil
		default:
			return false, err
		}
	default:
		return true, nil
	}
}

// TODO https://github.com/temporalio/temporal/issues/1021
//
//	there should be more validation logic here
//	1. if task has valid TTL -> TTL reached -> delete
//	2. if task has 0 TTL / no TTL -> logic need to additionally check if corresponding workflow still exists
func IsTaskExpired(t *persistencespb.AllocatedTaskInfo) bool {
	expiry := timestamp.TimeValue(t.GetData().GetExpiryTime())
	return expiry.Unix() > 0 && expiry.Before(time.Now())
}
