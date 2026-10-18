package workflow

import (
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/dynamicconfig"
)

var (
	ErrWorkflowTimeSkippingNotEnabled  = serviceerror.NewUnimplemented("Workflow time skipping is not enabled for the namespace.")
	ErrTimeSkippingFastForwardIDNotSet = serviceerror.NewInvalidArgument("Time skipping config invalid: fast_forward_id must be set")
)

func (c Config) validateAndPopulateWorkflowTimeSkippingConfig(tsc *commonpb.TimeSkippingConfig, namespace string) error {
	return ValidateAndPopulateWorkflowTimeSkippingConfig(
		tsc,
		namespace,
		c.workflowTimeSkippingEnabled,
		c.timeSkippingMaxSkipPerSession,
		c.timeSkippingMaxAccumulatedDuration,
	)
}

func ValidateAndPopulateWorkflowTimeSkippingConfig(
	tsc *commonpb.TimeSkippingConfig,
	namespace string,
	workflowEnabled dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	maxSkipPerSession dynamicconfig.IntPropertyFnWithNamespaceFilter,
	maxAccumulatedDuration dynamicconfig.DurationPropertyFn,
) error {
	if tsc == nil {
		return nil
	}
	if !workflowEnabled(namespace) {
		return ErrWorkflowTimeSkippingNotEnabled
	}
	return ValidateAndPopulateTimeSkippingConfig(tsc, namespace, maxSkipPerSession, maxAccumulatedDuration)
}

func ValidateAndPopulateTimeSkippingConfig(
	tsc *commonpb.TimeSkippingConfig,
	namespace string,
	maxSkipPerSession dynamicconfig.IntPropertyFnWithNamespaceFilter,
	maxAccumulatedDuration dynamicconfig.DurationPropertyFn,
) error {
	if tsc == nil {
		return nil
	}
	if tsc.GetMaxSessionSkipCount() <= 0 {
		tsc.MaxSessionSkipCount = max(1, int32(maxSkipPerSession(namespace)))
	}
	if !tsc.GetEnabled() && tsc.GetFastForwardConfig() != nil {
		return serviceerror.NewInvalidArgument("time_skipping_config: cannot set fast_forward when enabled is false")
	}
	if ff := tsc.GetFastForwardConfig(); ff != nil {
		if ff.GetDuration().AsDuration() <= 0 {
			return serviceerror.NewInvalidArgument("Time skipping config invalid: fast_forward duration must be positive")
		}
		maxDuration := maxAccumulatedDuration()
		if ff.GetDuration().AsDuration() >= maxDuration {
			return serviceerror.NewInvalidArgumentf("Time skipping config invalid: fast_forward duration exceeds max value (%.1f hours) allowed", maxDuration.Hours())
		}
		if strings.TrimSpace(ff.GetId()) == "" {
			return ErrTimeSkippingFastForwardIDNotSet
		}
	}
	return nil
}
