package persistence

import (
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
