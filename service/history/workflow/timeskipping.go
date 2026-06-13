package workflow

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/types/known/durationpb"
)

// propagateTimeSkippingToNextRun propagates both time skipping config and state to the next run in
// the chain (CaN, retry, cron). The config is deep-cloned so the next run can mutate it without
// affecting the source.
func propagateTimeSkippingToNextRun(
	source *persistencespb.WorkflowExecutionInfo,
) (*commonpb.TimeSkippingConfig, *commonpb.TimeSkippingStatePropagation) {
	previousTSC := source.GetTimeSkippingInfo().GetConfig()

	// if disabled, we just return nil for the new TSC
	var newTSC *commonpb.TimeSkippingConfig
	if previousTSC.GetEnabled() {
		newTSC = common.CloneProto(previousTSC)
	}

	stateProp := &commonpb.TimeSkippingStatePropagation{
		InitialSkippedDuration: durationpb.New(accumulatedSkippedDuration(source)),
	}
	if ff := source.GetTimeSkippingInfo().GetFastForwardInfo(); ff != nil && !ff.GetHasReached() {
		stateProp.FastForwardTargetTime = ff.GetTargetTime()
	}
	return newTSC, stateProp
}

// propagateTimeSkippingToChild makes sure the start time of the child workflow execution
// is shifted forward by the accumulated skipped duration.
// FastForward is never propagated to children.
func propagateTimeSkippingToChild(
	source *persistencespb.WorkflowExecutionInfo,
) (*commonpb.TimeSkippingConfig, *commonpb.TimeSkippingStatePropagation) {
	accum := accumulatedSkippedDuration(source)
	var stateProp *commonpb.TimeSkippingStatePropagation
	if accum > 0 {
		stateProp = &commonpb.TimeSkippingStatePropagation{
			InitialSkippedDuration: durationpb.New(accum),
		}
	}

	enabled := source.GetTimeSkippingInfo().GetConfig().GetEnabled()
	disableChildPropagation := source.GetTimeSkippingInfo().GetConfig().GetDisableChildPropagation()
	if !enabled || disableChildPropagation {
		return nil, stateProp
	}

	return &commonpb.TimeSkippingConfig{
		Enabled: enabled,
	}, stateProp
}

func accumulatedSkippedDuration(source *persistencespb.WorkflowExecutionInfo) time.Duration {
	return source.GetTimeSkippingInfo().GetAccumulatedSkippedDuration().AsDuration()
}
