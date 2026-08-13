package nexusoperation

//go:generate go run ../../../cmd/tools/genvalidationcoverage -messages-file validator_gen.messages -out validator_gen.go

import (
	"github.com/google/uuid"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/validation"
)

var ValidatorModule = validation.Module(
	"chasm.lib.nexusoperation.validators",
	newDeleteNexusOperationExecutionRequestValidator,
)

func newDeleteNexusOperationExecutionRequestValidator(
	config *Config,
) deleteNexusOperationExecutionRequestFieldValidators {
	return deleteNexusOperationExecutionRequestFieldValidators{
		Namespace:   validation.NoOp[workflowservice.DeleteNexusOperationExecutionRequest, string](),
		OperationId: validation.Field[workflowservice.DeleteNexusOperationExecutionRequest](requiredID(config.MaxIDLengthLimit())),
		RunId:       validation.Field[workflowservice.DeleteNexusOperationExecutionRequest](validateOptionalRunID),
	}
}

func requiredID(limit int) func(string, string) error {
	return func(fieldName string, value string) error {
		if value == "" {
			return serviceerror.NewInvalidArgumentf("%s is required", fieldName)
		}
		return validateIDLength(fieldName, value, limit)
	}
}

func validateIDLength(fieldName, value string, limit int) error {
	if len(value) > limit {
		return serviceerror.NewInvalidArgumentf("%s exceeds length limit. Length=%d Limit=%d", fieldName, len(value), limit)
	}
	return nil
}

func validateOptionalRunID(fieldName string, runID string) error {
	if runID == "" {
		return nil
	}
	if err := uuid.Validate(runID); err != nil {
		return serviceerror.NewInvalidArgumentf("%s is not a valid UUID", fieldName)
	}
	return nil
}
