package executions

import (
	"context"

	persistencespb "go.temporal.io/server/api/persistence/v1"
)

type (
	MutableState struct {
		*persistencespb.WorkflowMutableState
	}

	MutableStateValidationResult struct {
		// type tag used for metrics
		failureType string
		// failure details used for logging
		failureDetails string
	}

	Validator interface {
		Validate(ctx context.Context, mutableState *MutableState) ([]MutableStateValidationResult, error)
	}
)
