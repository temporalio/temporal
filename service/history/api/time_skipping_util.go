package api

import (
	"time"

	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	historyi "go.temporal.io/server/service/history/interfaces"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// DefaultAutoSkipMaxFirings is the default max_firings value when the caller
// sets 0 (or omits it) in AutoSkipConfig.
const DefaultAutoSkipMaxFirings = 10

// EffectiveAutoSkipMaxFirings returns the effective max_firings for an auto-skip config,
// applying the default when the value is 0.
func EffectiveAutoSkipMaxFirings(autoSkip *workflowpb.TimeSkippingConfig_AutoSkipConfig) int32 {
	if autoSkip == nil {
		return 0
	}
	if autoSkip.GetMaxFirings() == 0 {
		return DefaultAutoSkipMaxFirings
	}
	return autoSkip.GetMaxFirings()
}

// ValidateAndApplyTimeSkippingConfig validates the new time-skipping config and applies it
// to the mutable state's executionInfo. VirtualTimeOffset starts at 0 (the default)
// when time-skipping is first enabled — no explicit initialization needed.
func ValidateAndApplyTimeSkippingConfig(ms historyi.MutableState, newConfig *workflowpb.TimeSkippingConfig, now time.Time) error {
	executionInfo := ms.GetExecutionInfo()
	currentConfig := executionInfo.GetTimeSkippingConfig()

	// Cannot disable once enabled.
	if currentConfig.GetEnabled() && !newConfig.GetEnabled() {
		return serviceerror.NewFailedPrecondition("time skipping cannot be disabled once enabled")
	}

	executionInfo.TimeSkippingConfig = newConfig
	ResetAutoSkipState(executionInfo, now)
	return nil
}

// ResetAutoSkipState resolves the auto-skip deadline from the config and resets
// firings_used. Call this whenever auto-skip config is set or updated.
func ResetAutoSkipState(executionInfo *persistencespb.WorkflowExecutionInfo, now time.Time) {
	autoSkip := executionInfo.GetTimeSkippingConfig().GetAutoSkip()
	if autoSkip == nil {
		// Auto-skip cleared — reset state.
		executionInfo.AutoSkipDeadline = nil
		executionInfo.AutoSkipFiringsUsed = 0
		return
	}

	// Reset firings counter on every config update.
	executionInfo.AutoSkipFiringsUsed = 0

	// Resolve deadline from config.
	switch bound := autoSkip.GetBound().(type) {
	case *workflowpb.TimeSkippingConfig_AutoSkipConfig_UntilTime:
		executionInfo.AutoSkipDeadline = bound.UntilTime
	case *workflowpb.TimeSkippingConfig_AutoSkipConfig_UntilDuration:
		// deadline = currentVirtualTime + duration
		// currentVirtualTime = now + virtualTimeOffset
		virtualNow := now.Add(executionInfo.GetVirtualTimeOffset().AsDuration())
		executionInfo.AutoSkipDeadline = timestamppb.New(virtualNow.Add(bound.UntilDuration.AsDuration()))
	default:
		// No bound set — unbounded auto-skip (no deadline).
		executionInfo.AutoSkipDeadline = nil
	}
}
