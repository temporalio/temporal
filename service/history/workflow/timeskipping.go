package workflow

import (
	"time"

	workflowpb "go.temporal.io/api/workflow/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/types/known/durationpb"
)

// propagateTimeSkippingToNextRun propagates both time skipping config and state to the next run in
// the chain (CaN, retry, cron). The config is deep-cloned so the next run can mutate it without
// affecting the source.
func propagateTimeSkippingToNextRun(
	source *persistencespb.WorkflowExecutionInfo,
) (*workflowpb.TimeSkippingConfig, *workflowpb.TimeSkippingStatePropagation) {
	var tsc *workflowpb.TimeSkippingConfig
	if cfg := source.GetTimeSkippingInfo().GetConfig(); cfg != nil {
		tsc = common.CloneProto(cfg)
	}
	stateProp := &workflowpb.TimeSkippingStatePropagation{
		InitialSkippedDuration: durationpb.New(accumulatedSkippedDuration(source)),
	}
	if ff := source.GetTimeSkippingInfo().GetFastForwardInfo(); ff != nil && !ff.GetHasReached() {
		stateProp.FastForwardTargetTime = ff.GetTargetTime()
	}
	return tsc, stateProp
}

// propagateTimeSkippingToChild makes sure the start time of the child workflow execution
// is shifted forward by the accumulated skipped duration.
// FastForward is never propagated to children.
func propagateTimeSkippingToChild(
	source *persistencespb.WorkflowExecutionInfo,
) (*workflowpb.TimeSkippingConfig, *workflowpb.TimeSkippingStatePropagation) {
	accum := accumulatedSkippedDuration(source)
	var stateProp *workflowpb.TimeSkippingStatePropagation
	if accum > 0 {
		stateProp = &workflowpb.TimeSkippingStatePropagation{
			InitialSkippedDuration: durationpb.New(accum),
		}
	}

	enabled := source.GetTimeSkippingInfo().GetConfig().GetEnabled()
	if !enabled {
		return nil, stateProp
	}
	return &workflowpb.TimeSkippingConfig{
		Enabled: enabled,
	}, stateProp
}

func accumulatedSkippedDuration(source *persistencespb.WorkflowExecutionInfo) time.Duration {
	return source.GetTimeSkippingInfo().GetAccumulatedSkippedDuration().AsDuration()
}
