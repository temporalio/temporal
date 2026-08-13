package model

import (
	"time"

	"go.temporal.io/server/common/telemetry"
)

const factOutcomeCompleted = telemetry.WorkflowCloseOutcomeCompleted

func workflowCloseTransition(outcome string) string {
	switch outcome {
	case telemetry.WorkflowCloseOutcomeCompleted:
		return WorkflowComplete
	case telemetry.WorkflowCloseOutcomeFailed:
		return WorkflowFail
	case telemetry.WorkflowCloseOutcomeCanceled:
		return WorkflowCancel
	case telemetry.WorkflowCloseOutcomeTerminated:
		return WorkflowTerminate
	case telemetry.WorkflowCloseOutcomeTimedOut:
		return WorkflowTimeout
	case telemetry.WorkflowCloseOutcomeContinuedAsNew:
		return WorkflowRunContinueAsNew
	default:
		return outcome
	}
}

func eventTimeOrNow(eventTime time.Time) time.Time {
	if eventTime.IsZero() {
		return time.Now()
	}
	return eventTime
}
