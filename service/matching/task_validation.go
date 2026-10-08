//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination task_validation_mock.go
package matching

import (
	"context"
	"sync"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
)

const (
	taskReaderOfferTimeout    = 60 * time.Second // TODO(pri): old matcher cleanup
	taskValidatorCacheMaxSize = 1024
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
		config            *taskQueueConfig
		clusterMetadata   cluster.Metadata
		namespaceRegistry namespace.Registry
		historyClient     historyservice.HistoryServiceClient

		mu    sync.Mutex
		cache cache.Cache
	}
)

func newTaskValidator(
	tqCtx context.Context,
	config *taskQueueConfig,
	clusterMetadata cluster.Metadata,
	namespaceRegistry namespace.Registry,
	historyClient historyservice.HistoryServiceClient,
) *taskValidatorImpl {
	return &taskValidatorImpl{
		tqCtx:             tqCtx,
		config:            config,
		clusterMetadata:   clusterMetadata,
		namespaceRegistry: namespaceRegistry,
		historyClient:     historyClient,
		cache:             cache.New(taskValidatorCacheMaxSize, nil),
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

func (v *taskValidatorImpl) lookupOrInit(task *persistencespb.AllocatedTaskInfo) (info taskValidationInfo, existed bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	// Redirected tasks from different physical queues can share the same task ID.
	if info, ok := v.cache.Get(task).(taskValidationInfo); ok {
		return info, true
	}
	validationTime := time.Now().UTC()
	if task.Data.CreateTime != nil {
		validationTime = task.Data.CreateTime.AsTime()
	}
	info = taskValidationInfo{taskID: task.TaskId, validationTime: validationTime}
	v.cache.Put(task, info)
	return info, false
}

// preValidateActive track a task and return if validation should be done, if namespace is active
func (v *taskValidatorImpl) preValidateActive(
	task *persistencespb.AllocatedTaskInfo,
) bool {
	info, existed := v.lookupOrInit(task)
	if !existed {
		return false
	}
	return time.Since(info.validationTime) > v.config.ValidatorValidationThreshold()
}

// preValidatePassive track a task and return if validation should be done, if namespace is passive
func (v *taskValidatorImpl) preValidatePassive(
	task *persistencespb.AllocatedTaskInfo,
) bool {
	info, _ := v.lookupOrInit(task)
	return time.Since(info.validationTime) > v.config.ValidatorValidationThreshold()
}

// postValidate update tracked task info
func (v *taskValidatorImpl) postValidate(
	task *persistencespb.AllocatedTaskInfo,
) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.cache.Put(task, taskValidationInfo{
		taskID:         task.TaskId,
		validationTime: time.Now().UTC(),
	})
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
