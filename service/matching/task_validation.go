//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination task_validation_mock.go
package matching

import (
	"context"
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

const taskValidatorCacheMaxSize = 100

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

	taskValidatorImpl struct {
		tqCtx             context.Context
		config            *taskQueueConfig
		clusterMetadata   cluster.Metadata
		namespaceRegistry namespace.Registry
		historyClient     historyservice.HistoryServiceClient

		cache cache.Cache // *AllocatedTaskInfo -> int64 (unix nanos)
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
		v.cache.Delete(task)
		return false
	}
	if !v.preValidate(task) {
		return true
	}
	valid, err := v.isTaskValid(task, taskType)
	if err != nil {
		return true
	}
	if valid {
		v.postValidate(task)
	} else {
		v.cache.Delete(task)
	}
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

func (v *taskValidatorImpl) lookupOrInit(task *persistencespb.AllocatedTaskInfo) (validationTimeNs int64, existed bool) {
	// Redirected tasks from different physical queues can share the same task ID,
	// so use *persistencespb.AllocatedTaskInfo as the cache key.
	if validationTimeNs, ok := v.cache.Get(task).(int64); ok {
		return validationTimeNs, true
	}
	validationTimeNs = time.Now().UnixNano()
	if task.Data.CreateTime != nil {
		validationTimeNs = task.Data.CreateTime.AsTime().UnixNano()
	}
	v.cache.Put(task, validationTimeNs)
	return validationTimeNs, false
}

// preValidateActive track a task and return if validation should be done, if namespace is active
func (v *taskValidatorImpl) preValidateActive(
	task *persistencespb.AllocatedTaskInfo,
) bool {
	validationTimeNs, existed := v.lookupOrInit(task)
	if !existed {
		return false
	}
	return time.Since(time.Unix(0, validationTimeNs)) > v.config.ValidatorValidationThreshold()
}

// preValidatePassive track a task and return if validation should be done, if namespace is passive
func (v *taskValidatorImpl) preValidatePassive(
	task *persistencespb.AllocatedTaskInfo,
) bool {
	validationTimeNs, _ := v.lookupOrInit(task)
	return time.Since(time.Unix(0, validationTimeNs)) > v.config.ValidatorValidationThreshold()
}

// postValidate update tracked task info
func (v *taskValidatorImpl) postValidate(
	task *persistencespb.AllocatedTaskInfo,
) {
	v.cache.Put(task, time.Now().UnixNano())
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
