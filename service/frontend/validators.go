package frontend

import (
	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
)

func validateExecution(w *commonpb.WorkflowExecution) error {
	if w == nil {
		return errExecutionNotSet
	}
	if w.GetWorkflowId() == "" {
		return errWorkflowIDNotSet
	}
	if w.GetRunId() != "" && uuid.Parse(w.GetRunId()) == nil {
		return errInvalidRunID
	}
	return nil
}

func validateRateLimit(update *workflowservice.UpdateTaskQueueConfigRequest_RateLimitUpdate, label string) error {
	if update == nil || update.RateLimit == nil {
		return nil
	}
	if update.RateLimit.GetRequestsPerSecond() < 0 {
		return serviceerror.NewInvalidArgumentf("RequestsPerSecond for %s rate limit must be non-negative.", label)
	}
	return validateStringField(label+".Reason", update.GetReason(), maxReasonLength, false)
}

func validateStringField(fieldName string, value string, maxLen int, required bool) error {
	if required && len(value) == 0 {
		return serviceerror.NewInvalidArgumentf("%s field must be non-empty.", fieldName)
	}
	if len(value) > maxLen {
		return serviceerror.NewInvalidArgumentf("%s field too long (max %d characters).", fieldName, maxLen)
	}
	return nil
}
