package frontend

import (
	"strings"

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

func validateFairnessWeightUpdates(updates map[string]float32, maxConfigLimit int) error {
	if maxConfigLimit > 0 && len(updates) > maxConfigLimit {
		return serviceerror.NewInvalidArgumentf(
			"too many overrides in request: got %d, maximum %d",
			len(updates), maxConfigLimit,
		)
	}

	for k, w := range updates {
		if strings.TrimSpace(k) == "" {
			return serviceerror.NewInvalidArgument("override key must not be empty")
		}
		// NaN check: (w != w) is true only for NaN
		if w != w {
			return serviceerror.NewInvalidArgumentf(
				"invalid weight for key %q: value is not a number", k,
			)
		}
		if w < 0 && w != -1 {
			return serviceerror.NewInvalidArgumentf(
				"invalid weight for key %q: must be >= 0 or -1 (delete)", k,
			)
		}
	}

	return nil
}
