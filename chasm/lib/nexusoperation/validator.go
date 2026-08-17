package nexusoperation

//go:generate go run ../../../cmd/tools/genvalidationcoverage -messages-file validator_gen.messages -out validator_gen.go

import (
	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/validation"
)

var ValidatorModule = validation.Module(
	"chasm.lib.nexusoperation.validators",
	newDeleteNexusOperationExecutionValidator,
	newDescribeNexusOperationExecutionValidator,
	newPollNexusOperationExecutionValidator,
	newRequestCancelNexusOperationExecutionValidator,
)

func newDeleteNexusOperationExecutionValidator(config *Config) deleteNexusOperationExecutionValidator {
	type req = workflowservice.DeleteNexusOperationExecutionRequest
	return deleteNexusOperationExecutionValidator{
		Request: deleteNexusOperationExecutionRequestFieldValidators{
			Namespace:   validation.NoOp[req, string](),
			OperationId: validation.Field[req](requiredID(config.MaxIDLengthLimit())),
			RunId:       validation.Field[req](validateOptionalRunID),
		},
		Response: deleteNexusOperationExecutionResponseFieldValidators{},
	}
}

func newDescribeNexusOperationExecutionValidator(config *Config) describeNexusOperationExecutionValidator {
	type req = workflowservice.DescribeNexusOperationExecutionRequest
	type resp = workflowservice.DescribeNexusOperationExecutionResponse
	return describeNexusOperationExecutionValidator{
		Request: describeNexusOperationExecutionRequestFieldValidators{
			Namespace:      validation.NoOp[req, string](),
			OperationId:    validation.Field[req](requiredID(config.MaxIDLengthLimit())),
			RunId:          validation.Field[req](validateOptionalRunID),
			IncludeInput:   validation.NoOp[req, bool](),
			IncludeOutcome: validation.NoOp[req, bool](),
			LongPollToken:  validation.NoOp[req, []byte](),
		},
		Response: describeNexusOperationExecutionResponseFieldValidators{
			RunId:         validation.NoOp[resp, string](),
			Info:          validation.NoOp[resp, *nexuspb.NexusOperationExecutionInfo](),
			Input:         validation.NoOp[resp, *commonpb.Payload](),
			LongPollToken: validation.NoOp[resp, []byte](),
		},
	}
}

func newRequestCancelNexusOperationExecutionValidator(config *Config) requestCancelNexusOperationExecutionValidator {
	type req = workflowservice.RequestCancelNexusOperationExecutionRequest
	return requestCancelNexusOperationExecutionValidator{
		Request: requestCancelNexusOperationExecutionRequestFieldValidators{
			Namespace:   validation.NoOp[req, string](),
			OperationId: validation.Field[req](requiredID(config.MaxIDLengthLimit())),
			RunId:       validation.Field[req](validateOptionalRunID),
			Identity:    validation.NoOp[req, string](),
			RequestId:   validation.Field[req](validateOptionalRunID),
			Reason:      validation.NoOp[req, string](),
		},
		Response: requestCancelNexusOperationExecutionResponseFieldValidators{},
	}
}

func newPollNexusOperationExecutionValidator(config *Config) pollNexusOperationExecutionValidator {
	type req = workflowservice.PollNexusOperationExecutionRequest
	type resp = workflowservice.PollNexusOperationExecutionResponse
	return pollNexusOperationExecutionValidator{
		Request: pollNexusOperationExecutionRequestFieldValidators{
			Namespace:   validation.NoOp[req, string](),
			OperationId: validation.Field[req](requiredID(config.MaxIDLengthLimit())),
			RunId:       validation.Field[req](validateOptionalRunID),
			WaitStage:   validation.Field[req](validateWaitStage),
		},
		Response: pollNexusOperationExecutionResponseFieldValidators{
			RunId:          validation.NoOp[resp, string](),
			WaitStage:      validation.NoOp[resp, enumspb.NexusOperationWaitStage](),
			OperationToken: validation.NoOp[resp, string](),
		},
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

func validateWaitStage(fieldName string, stage enumspb.NexusOperationWaitStage) error {
	if stage == enumspb.NEXUS_OPERATION_WAIT_STAGE_UNSPECIFIED {
		return serviceerror.NewInvalidArgumentf("%s must be specified", fieldName)
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
