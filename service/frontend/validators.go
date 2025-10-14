package frontend

import (
	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/priorities"
)

var (
	errFairnessKeyEmpty = serviceerror.NewInvalidArgument("fairness weight override key must not be empty")
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

func validateFairnessWeightUpdate(
	set map[string]float32,
	unset []string,
	maxConfigLimit int,
) error {
	total := len(set) + len(unset)
	if total > maxConfigLimit {
		return serviceerror.NewInvalidArgumentf(
			"too many fairness weight overrides in request: got %d, maximum %d",
			total, maxConfigLimit,
		)
	}

	for k, w := range set {
		if k == "" {
			return errFairnessKeyEmpty
		}
		if err := priorities.ValidateFairnessKey(k); err != nil {
			return err
		}
		if err := priorities.ValidateFairnessWeight(w); err != nil {
			return serviceerror.NewInvalidArgumentf(
				"invalid fairness weight weight for key %q: %v", k, err,
			)
		}
	}

	for _, k := range unset {
		if k == "" {
			return errFairnessKeyEmpty
		}
		if err := priorities.ValidateFairnessKey(k); err != nil {
			return err
		}
	}

	for k := range set {
		for _, u := range unset {
			if k == u {
				return serviceerror.NewInvalidArgumentf(
					"fairness weight override key %q present in both set and unset lists", k)
			}
		}
	}

	return nil
}
