package activityoptions

import (
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/util"
)

// MergeActivityOptions applies the fields specified in updateFields from mergeFrom into mergeInto in-place.
// updateFields is a map of camelCase JSON field paths, as returned by util.ParseFieldMask
// Returns an error if a required source parent message is nil in mergeFrom
// when a sub-field of that parent is listed in updateFields.
//
//nolint:revive // cyclomatic: field-mask application requires explicit handling of each supported field
func MergeActivityOptions(mergeInto, mergeFrom *activitypb.ActivityOptions, updateFields map[string]struct{}) error {
	if _, ok := updateFields["taskQueue.name"]; ok {
		if mergeFrom.GetTaskQueue() == nil {
			return serviceerror.NewInvalidArgument("TaskQueue is not provided")
		}
		if mergeInto.TaskQueue == nil {
			mergeInto.TaskQueue = mergeFrom.GetTaskQueue()
		} else {
			mergeInto.TaskQueue.Name = mergeFrom.GetTaskQueue().GetName()
		}
	}

	if _, ok := updateFields["scheduleToCloseTimeout"]; ok {
		mergeInto.ScheduleToCloseTimeout = mergeFrom.GetScheduleToCloseTimeout()
	}

	if _, ok := updateFields["scheduleToStartTimeout"]; ok {
		mergeInto.ScheduleToStartTimeout = mergeFrom.GetScheduleToStartTimeout()
	}

	if _, ok := updateFields["startToCloseTimeout"]; ok {
		mergeInto.StartToCloseTimeout = mergeFrom.GetStartToCloseTimeout()
	}

	if _, ok := updateFields["heartbeatTimeout"]; ok {
		mergeInto.HeartbeatTimeout = mergeFrom.GetHeartbeatTimeout()
	}

	if _, ok := updateFields["priority"]; ok {
		mergeInto.Priority = mergeFrom.GetPriority()
	}

	if _, ok := updateFields["priority.priorityKey"]; ok {
		if mergeFrom.GetPriority() == nil {
			return serviceerror.NewInvalidArgument("Priority is not provided")
		}
		if mergeInto.Priority == nil {
			mergeInto.Priority = &commonpb.Priority{}
		}
		mergeInto.Priority.PriorityKey = mergeFrom.GetPriority().GetPriorityKey()
	}

	if _, ok := updateFields["priority.fairnessKey"]; ok {
		if mergeFrom.GetPriority() == nil {
			return serviceerror.NewInvalidArgument("Priority is not provided")
		}
		if mergeInto.Priority == nil {
			mergeInto.Priority = &commonpb.Priority{}
		}
		mergeInto.Priority.FairnessKey = mergeFrom.GetPriority().GetFairnessKey()
	}

	if _, ok := updateFields["priority.fairnessWeight"]; ok {
		if mergeFrom.GetPriority() == nil {
			return serviceerror.NewInvalidArgument("Priority is not provided")
		}
		if mergeInto.Priority == nil {
			mergeInto.Priority = &commonpb.Priority{}
		}
		mergeInto.Priority.FairnessWeight = mergeFrom.GetPriority().GetFairnessWeight()
	}

	if _, ok := updateFields["retryPolicy"]; ok {
		if mergeFrom.GetRetryPolicy() == nil {
			return serviceerror.NewInvalidArgument("RetryPolicy is not provided")
		}
		mergeInto.RetryPolicy = mergeFrom.GetRetryPolicy()
	}

	if _, ok := updateFields["retryPolicy.initialInterval"]; ok {
		if mergeFrom.GetRetryPolicy() == nil {
			return serviceerror.NewInvalidArgument("RetryPolicy is not provided")
		}
		if mergeInto.RetryPolicy == nil {
			mergeInto.RetryPolicy = &commonpb.RetryPolicy{}
		}
		mergeInto.RetryPolicy.InitialInterval = mergeFrom.GetRetryPolicy().GetInitialInterval()
	}

	if _, ok := updateFields["retryPolicy.backoffCoefficient"]; ok {
		if mergeFrom.GetRetryPolicy() == nil {
			return serviceerror.NewInvalidArgument("RetryPolicy is not provided")
		}
		if mergeInto.RetryPolicy == nil {
			mergeInto.RetryPolicy = &commonpb.RetryPolicy{}
		}
		mergeInto.RetryPolicy.BackoffCoefficient = mergeFrom.GetRetryPolicy().GetBackoffCoefficient()
	}

	if _, ok := updateFields["retryPolicy.maximumInterval"]; ok {
		if mergeFrom.GetRetryPolicy() == nil {
			return serviceerror.NewInvalidArgument("RetryPolicy is not provided")
		}
		if mergeInto.RetryPolicy == nil {
			mergeInto.RetryPolicy = &commonpb.RetryPolicy{}
		}
		mergeInto.RetryPolicy.MaximumInterval = mergeFrom.GetRetryPolicy().GetMaximumInterval()
	}

	if _, ok := updateFields["retryPolicy.maximumAttempts"]; ok {
		if mergeFrom.GetRetryPolicy() == nil {
			return serviceerror.NewInvalidArgument("RetryPolicy is not provided")
		}
		if mergeInto.RetryPolicy == nil {
			mergeInto.RetryPolicy = &commonpb.RetryPolicy{}
		}
		mergeInto.RetryPolicy.MaximumAttempts = mergeFrom.GetRetryPolicy().GetMaximumAttempts()
	}

	if util.FieldMaskHasSubPath(updateFields, "asyncHandoffOptions") && mergeFrom.GetAsyncHandoffOptions() == nil {
		return serviceerror.NewInvalidArgument("AsyncHandoffOptions is not provided")
	}

	if util.FieldMaskHasPathOrSubPath(updateFields, "asyncHandoffOptions.retryPolicy") &&
		mergeFrom.GetAsyncHandoffOptions().GetRetryPolicy() == nil {
		return serviceerror.NewInvalidArgument("AsyncHandoffOptions.RetryPolicy is not provided")
	}

	if _, ok := updateFields["asyncHandoffOptions"]; ok {
		mergeInto.AsyncHandoffOptions = mergeFrom.GetAsyncHandoffOptions()
	}

	if util.FieldMaskHasSubPath(updateFields, "asyncHandoffOptions") {
		if mergeInto.AsyncHandoffOptions == nil {
			mergeInto.AsyncHandoffOptions = &activitypb.AsyncHandoffOptions{}
		}

		into, from := mergeInto.AsyncHandoffOptions, mergeFrom.GetAsyncHandoffOptions()
		if _, ok := updateFields["asyncHandoffOptions.startToHandoffTimeout"]; ok {
			into.StartToHandoffTimeout = from.GetStartToHandoffTimeout()
		}

		if _, ok := updateFields["asyncHandoffOptions.handoffToCloseTimeout"]; ok {
			into.HandoffToCloseTimeout = from.GetHandoffToCloseTimeout()
		}

		if _, ok := updateFields["asyncHandoffOptions.heartbeatTimeout"]; ok {
			into.HeartbeatTimeout = from.GetHeartbeatTimeout()
		}

		if _, ok := updateFields["asyncHandoffOptions.retryPolicy"]; ok {
			into.RetryPolicy = from.GetRetryPolicy()
		}

		if util.FieldMaskHasSubPath(updateFields, "asyncHandoffOptions.retryPolicy") {
			if into.RetryPolicy == nil {
				into.RetryPolicy = &commonpb.RetryPolicy{}
			}

			if _, ok := updateFields["asyncHandoffOptions.retryPolicy.initialInterval"]; ok {
				into.RetryPolicy.InitialInterval = from.GetRetryPolicy().GetInitialInterval()
			}

			if _, ok := updateFields["asyncHandoffOptions.retryPolicy.backoffCoefficient"]; ok {
				into.RetryPolicy.BackoffCoefficient = from.GetRetryPolicy().GetBackoffCoefficient()
			}

			if _, ok := updateFields["asyncHandoffOptions.retryPolicy.maximumInterval"]; ok {
				into.RetryPolicy.MaximumInterval = from.GetRetryPolicy().GetMaximumInterval()
			}

			if _, ok := updateFields["asyncHandoffOptions.retryPolicy.maximumAttempts"]; ok {
				into.RetryPolicy.MaximumAttempts = from.GetRetryPolicy().GetMaximumAttempts()
			}
		}
	}

	if _, ok := updateFields["startDelay"]; ok {
		mergeInto.StartDelay = mergeFrom.GetStartDelay()
	}

	return nil
}
