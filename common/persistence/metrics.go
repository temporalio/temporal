package persistence

import (
	"errors"

	"go.temporal.io/server/common/metrics"
)

const (
	WorkflowIDTagName = "workflow_id"
	RunIDTagName      = "run_id"
	ErrorTagName      = "error"
)

// EmitDataLossMetric emits a data loss metric for DataLoss errors
func EmitDataLossMetric(
	handler metrics.Handler,
	namespaceName, workflowID, runID, source string,
	err error,
) {
	metrics.DataLossCounter.With(handler).Record(1,
		metrics.NamespaceTag(namespaceName),
		metrics.StringTag(WorkflowIDTagName, workflowID),
		metrics.StringTag(RunIDTagName, runID),
		metrics.OperationTag(source),
		metrics.StringTag(ErrorTagName, err.Error()),
	)
}

// IsMissingCurrentRecordError returns true when err is a
// CurrentWorkflowConditionFailedError with an empty RunID, which indicates the
// current execution record is missing from the database (corruption).
// A non-empty RunID means a different run owns the current record (normal conflict).
func IsMissingCurrentRecordError(err error) bool {
	var condErr *CurrentWorkflowConditionFailedError
	if errors.As(err, &condErr) && condErr.RunID == "" {
		return true
	}
	return false
}

// EmitCurrentRecordMissingMetric emits a metric when a missing current
// execution record is detected during a workflow update.
func EmitCurrentRecordMissingMetric(
	handler metrics.Handler,
	namespaceName, workflowID, runID, source string,
	err error,
) {
	metrics.CurrentRecordMissingCounter.With(handler).Record(1,
		metrics.NamespaceTag(namespaceName),
		metrics.StringTag(WorkflowIDTagName, workflowID),
		metrics.StringTag(RunIDTagName, runID),
		metrics.OperationTag(source),
		metrics.StringTag(ErrorTagName, err.Error()),
	)
}
