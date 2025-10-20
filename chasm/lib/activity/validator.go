package activity

import (
	"fmt"

	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/priorities"
	"go.temporal.io/server/common/retrypolicy"
	"go.temporal.io/server/common/tqid"
	"google.golang.org/protobuf/types/known/durationpb"
)

// RequestAttributesValidator validates and adjusts activity request attributes.
type RequestAttributesValidator struct {
	activityID                      string
	activityType                    string
	getDefaultActivityRetrySettings dynamicconfig.TypedPropertyFnWithNamespaceFilter[retrypolicy.DefaultRetrySettings]
	maxIDLengthLimit                int
	namespaceID                     namespace.ID
	options                         *activitypb.ActivityOptions
	priority                        *commonpb.Priority
}

func NewRequestAttributesValidator(
	activityID string,
	activityType string,
	getDefaultActivityRetrySettings dynamicconfig.TypedPropertyFnWithNamespaceFilter[retrypolicy.DefaultRetrySettings],
	maxIDLengthLimit int,
	namespaceID namespace.ID,
	options *activitypb.ActivityOptions,
	priority *commonpb.Priority,
) RequestAttributesValidator {
	return RequestAttributesValidator{
		activityID:                      activityID,
		activityType:                    activityType,
		getDefaultActivityRetrySettings: getDefaultActivityRetrySettings,
		maxIDLengthLimit:                maxIDLengthLimit,
		namespaceID:                     namespaceID,
		options:                         options,
		priority:                        priority,
	}
}

func (v *RequestAttributesValidator) GetActivityOptions() *activitypb.ActivityOptions {
	return v.options
}

// ValidateAndAdjustTimeouts validates the activity request attributes and adjusts the ActivityOptions timeout based on
// the following rules.
// runTimeout is the workflow run timeout. Set to durationpb.New(0) if not applicable.
// 1. If ScheduleToClose is set, fill in missing ScheduleToStart and StartToClose from ScheduleToClose
// 2. If StartToClose is set but ScheduleToClose is not set, set ScheduleToClose to runTimeout, and fill in missing ScheduleToStart from runTimeout
// 3. If neither ScheduleToClose nor StartToClose is set, return error
// 4. Ensure all timeouts do not exceed runTimeout if runTimeout is set (>0)
// 5. Ensure HeartbeatTimeout does not exceed StartToClose
func (v *RequestAttributesValidator) ValidateAndAdjustTimeouts(runTimeout *durationpb.Duration) error {
	if err := tqid.NormalizeAndValidate(v.options.TaskQueue, "", v.maxIDLengthLimit); err != nil {
		return fmt.Errorf("invalid TaskQueue: %w. ActivityId=%s ActivityType=%s", err, v.activityID, v.activityType)
	}

	if v.activityID == "" {
		return serviceerror.NewInvalidArgumentf("ActivityId is not set. ActivityType=%s", v.activityType)
	}
	if v.activityType == "" {
		return serviceerror.NewInvalidArgumentf("ActivityType is not set. ActivityID=%s", v.activityID)
	}

	if err := v.validateActivityRetryPolicy(v.namespaceID, v.options.RetryPolicy); err != nil {
		return fmt.Errorf("invalid ActivityRetryPolicy: %w. ActivityId=%s ActivityType=%s", err, v.activityID, v.activityType)
	}

	if len(v.activityID) > v.maxIDLengthLimit {
		return serviceerror.NewInvalidArgumentf("ActivityId exceeds length limit. ActivityId=%s ActivityType=%s Length=%d Limit=%d",
			v.activityID, v.activityType, len(v.activityID), v.maxIDLengthLimit)
	}
	if len(v.activityType) > v.maxIDLengthLimit {
		return serviceerror.NewInvalidArgumentf("ActivityType exceeds length limit. ActivityId=%s ActivityType=%s Length=%d Limit=%d",
			v.activityID, v.activityType, len(v.activityType), v.maxIDLengthLimit)
	}

	// Only attempt to deduce and fill in unspecified timeouts only when all timeouts are non-negative.
	if err := timestamp.ValidateAndCapProtoDuration(v.options.GetScheduleToCloseTimeout()); err != nil {
		return serviceerror.NewInvalidArgumentf("Invalid ScheduleToCloseTimeout: %v. ActivityId=%s ActivityType=%s",
			err, v.activityID, v.activityType)
	}
	if err := timestamp.ValidateAndCapProtoDuration(v.options.GetScheduleToStartTimeout()); err != nil {
		return serviceerror.NewInvalidArgumentf("Invalid ScheduleToStartTimeout: %v. ActivityId=%s ActivityType=%s",
			err, v.activityID, v.activityType)
	}
	if err := timestamp.ValidateAndCapProtoDuration(v.options.GetStartToCloseTimeout()); err != nil {
		return serviceerror.NewInvalidArgumentf("Invalid StartToCloseTimeout: %v. ActivityId=%s ActivityType=%s",
			err, v.activityID, v.activityType)
	}
	if err := timestamp.ValidateAndCapProtoDuration(v.options.GetHeartbeatTimeout()); err != nil {
		return serviceerror.NewInvalidArgumentf("Invalid HeartbeatTimeout: %v. ActivityId=%s ActivityType=%s",
			err, v.activityID, v.activityType)
	}

	if err := priorities.Validate(v.priority); err != nil {
		return serviceerror.NewInvalidArgumentf("Invalid Priorities: %v. ActivityId=%s ActivityType=%s",
			err, v.activityID, v.activityType)
	}

	return v.adjustActivityTimeouts(runTimeout)
}

func (v *RequestAttributesValidator) validateActivityRetryPolicy(
	namespaceID namespace.ID,
	retryPolicy *commonpb.RetryPolicy,
) error {
	if retryPolicy == nil {
		return nil
	}
	// TODO: this is a namespace setting, not a namespace id setting
	defaultActivityRetrySettings := v.getDefaultActivityRetrySettings(namespaceID.String())
	retrypolicy.EnsureDefaults(retryPolicy, defaultActivityRetrySettings)
	return retrypolicy.Validate(retryPolicy)
}

func (v *RequestAttributesValidator) adjustActivityTimeouts(runTimeout *durationpb.Duration) error {
	ScheduleToCloseSet := v.options.GetScheduleToCloseTimeout().AsDuration() > 0
	ScheduleToStartSet := v.options.GetScheduleToStartTimeout().AsDuration() > 0
	StartToCloseSet := v.options.GetStartToCloseTimeout().AsDuration() > 0

	if ScheduleToCloseSet {
		if ScheduleToStartSet {
			v.options.ScheduleToStartTimeout = timestamp.MinDurationPtr(v.options.GetScheduleToStartTimeout(),
				v.options.GetScheduleToCloseTimeout())
		} else {
			v.options.ScheduleToStartTimeout = v.options.GetScheduleToCloseTimeout()
		}
		if StartToCloseSet {
			v.options.StartToCloseTimeout = timestamp.MinDurationPtr(v.options.GetStartToCloseTimeout(),
				v.options.GetScheduleToCloseTimeout())
		} else {
			v.options.StartToCloseTimeout = v.options.GetScheduleToCloseTimeout()
		}
	} else if StartToCloseSet {
		// We are in !validScheduleToClose due to the first if above
		v.options.ScheduleToCloseTimeout = runTimeout
		if !ScheduleToStartSet {
			v.options.ScheduleToStartTimeout = runTimeout
		}
	} else {
		// Deduction failed as there's not enough information to fill in missing timeouts.
		return serviceerror.NewInvalidArgumentf("A valid StartToClose or ScheduleToCloseTimeout is not set on ScheduleActivityTaskCommand. ActivityId=%s ActivityType=%s",
			v.activityID, v.activityType)
	}
	// ensure activity timeout never larger than workflow timeout
	if runTimeout.AsDuration() > 0 {
		runTimeoutDur := runTimeout.AsDuration()
		if v.options.GetScheduleToCloseTimeout().AsDuration() > runTimeoutDur {
			v.options.ScheduleToCloseTimeout = runTimeout
		}
		if v.options.GetScheduleToStartTimeout().AsDuration() > runTimeoutDur {
			v.options.ScheduleToStartTimeout = runTimeout
		}
		if v.options.GetStartToCloseTimeout().AsDuration() > runTimeoutDur {
			v.options.StartToCloseTimeout = runTimeout
		}
		if v.options.GetHeartbeatTimeout().AsDuration() > runTimeoutDur {
			v.options.HeartbeatTimeout = runTimeout
		}
	}

	v.options.HeartbeatTimeout = timestamp.MinDurationPtr(v.options.GetHeartbeatTimeout(), v.options.GetStartToCloseTimeout())

	return nil
}
