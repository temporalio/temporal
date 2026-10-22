package chasm

import "go.temporal.io/server/common/metrics"

const (
	ExecutionForceTerminationReasonDeleteExecution              metrics.ReasonString = "delete_execution"
	ExecutionForceTerminationReasonHistorySizeExceedsLimit      metrics.ReasonString = "history_size_limit"
	ExecutionForceTerminationReasonHistoryCountExceedsLimit     metrics.ReasonString = "history_count_limit"
	ExecutionForceTerminationReasonMutableStateSizeExceedsLimit metrics.ReasonString = "mutable_state_size_limit"
	ExecutionForceTerminationReasonEventBatchSizeExceedsLimit   metrics.ReasonString = "event_batch_size_limit"
	ExecutionForceTerminationReasonVersionConflict              metrics.ReasonString = "version_conflict"
)
