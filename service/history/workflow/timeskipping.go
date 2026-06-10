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
) (*workflowpb.TimeSkippingConfig, *durationpb.Duration) {
	var tsc *workflowpb.TimeSkippingConfig
	if cfg := source.GetTimeSkippingInfo().GetConfig(); cfg != nil {
		tsc = common.CloneProto(cfg)
	}
	return tsc, durationpb.New(accumulatedSkippedDuration(source))
}

// propagateTimeSkippingToChild makes sure the start time of the child workflow execution
// is shifted forward by the accumulated skipped duration.
// MaxElapsedDuration/FastForward is never propagated to children.
func propagateTimeSkippingToChild(
	source *persistencespb.WorkflowExecutionInfo,
) (*workflowpb.TimeSkippingConfig, *durationpb.Duration) {
	initialSkip := durationpb.New(accumulatedSkippedDuration(source))
	enabled := source.GetTimeSkippingInfo().GetConfig().GetEnabled()
	if !enabled {
		return nil, initialSkip
	}
	return &workflowpb.TimeSkippingConfig{
		Enabled: enabled,
	}, initialSkip
}

func accumulatedSkippedDuration(source *persistencespb.WorkflowExecutionInfo) time.Duration {
	return source.GetTimeSkippingInfo().GetAccumulatedSkippedDuration().AsDuration()
}
