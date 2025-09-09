package persistence

import (
	"errors"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/metrics"
)

const (
	WorkflowIDTagName = "workflow_id"
	RunIDTagName      = "run_id"
	ErrorTagName      = "error"
)

// EmitDataLossMetric emits a data loss metric if the error is a DataLoss error
func EmitDataLossMetric(
	handler metrics.Handler,
	enabled bool,
	namespaceID, workflowID, runID, source, errorMessage string,
	err error,
) {
	if enabled && err != nil {
		var dataLossErr *serviceerror.DataLoss
		if errors.As(err, &dataLossErr) {
			metrics.DataLossCounter.With(handler).Record(1,
				metrics.NamespaceIDTag(namespaceID),
				metrics.StringTag(WorkflowIDTagName, workflowID),
				metrics.StringTag(RunIDTagName, runID),
				metrics.OperationTag(source),
				metrics.StringTag(ErrorTagName, errorMessage),
			)
		}
	}
}
