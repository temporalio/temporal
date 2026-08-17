package nexusoperation

//go:generate go run ../../../cmd/tools/genvalidationcoverage -messages-file validator_gen.messages -out validator_gen.go

import (
	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/validation"
	"google.golang.org/protobuf/types/known/durationpb"
)

var ValidatorModule = validation.Module(
	"chasm.lib.nexusoperation.validators",
	newDeleteNexusOperationExecutionValidator,
	newDescribeNexusOperationExecutionValidator,
	newPollNexusOperationExecutionValidator,
	newRequestCancelNexusOperationExecutionValidator,
	newTerminateNexusOperationExecutionValidator,
	newListNexusOperationExecutionsValidator,
	newCountNexusOperationExecutionsValidator,
	newStartNexusOperationExecutionValidator,
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

func newStartNexusOperationExecutionValidator(config *Config) startNexusOperationExecutionValidator {
	type req = workflowservice.StartNexusOperationExecutionRequest
	type resp = workflowservice.StartNexusOperationExecutionResponse
	return startNexusOperationExecutionValidator{
		Request: startNexusOperationExecutionRequestFieldValidators{
			Namespace:              validation.NoOp[req, string](),
			Identity:               validation.NoOp[req, string](),
			RequestId:              validation.Field[req](validateOptionalRunID),
			OperationId:            validation.Field[req](validateIDLength0(config.MaxIDLengthLimit())),
			Endpoint:               validation.Field[req](requiredString),
			Service:                validation.Field[req](requiredString),
			Operation:              validation.Field[req](requiredString),
			ScheduleToCloseTimeout: validation.NoOp[req, *durationpb.Duration](),
			ScheduleToStartTimeout: validation.NoOp[req, *durationpb.Duration](),
			StartToCloseTimeout:    validation.NoOp[req, *durationpb.Duration](),
			Input:                  validation.NoOp[req, *commonpb.Payload](),
			IdReusePolicy:          validation.NoOp[req, enumspb.NexusOperationIdReusePolicy](),
			IdConflictPolicy:       validation.NoOp[req, enumspb.NexusOperationIdConflictPolicy](),
			SearchAttributes:       validation.NoOp[req, *commonpb.SearchAttributes](),
			NexusHeader:            validation.NoOp[req, map[string]string](),
			UserMetadata:           validation.NoOp[req, *sdkpb.UserMetadata](),
		},
		Response: startNexusOperationExecutionResponseFieldValidators{
			RunId:   validation.NoOp[resp, string](),
			Started: validation.NoOp[resp, bool](),
		},
	}
}

func newCountNexusOperationExecutionsValidator() countNexusOperationExecutionsValidator {
	type req = workflowservice.CountNexusOperationExecutionsRequest
	type resp = workflowservice.CountNexusOperationExecutionsResponse
	return countNexusOperationExecutionsValidator{
		Request: countNexusOperationExecutionsRequestFieldValidators{
			Namespace: validation.NoOp[req, string](),
			Query:     validation.NoOp[req, string](),
		},
		Response: countNexusOperationExecutionsResponseFieldValidators{
			Count:  validation.NoOp[resp, int64](),
			Groups: validation.NoOp[resp, []*workflowservice.CountNexusOperationExecutionsResponse_AggregationGroup](),
		},
	}
}

func newListNexusOperationExecutionsValidator() listNexusOperationExecutionsValidator {
	type req = workflowservice.ListNexusOperationExecutionsRequest
	type resp = workflowservice.ListNexusOperationExecutionsResponse
	return listNexusOperationExecutionsValidator{
		Request: listNexusOperationExecutionsRequestFieldValidators{
			Namespace:     validation.NoOp[req, string](),
			PageSize:      validation.NoOp[req, int32](),
			NextPageToken: validation.NoOp[req, []byte](),
			Query:         validation.NoOp[req, string](),
		},
		Response: listNexusOperationExecutionsResponseFieldValidators{
			Operations:    validation.NoOp[resp, []*nexuspb.NexusOperationExecutionListInfo](),
			NextPageToken: validation.NoOp[resp, []byte](),
		},
	}
}

func newTerminateNexusOperationExecutionValidator(config *Config) terminateNexusOperationExecutionValidator {
	type req = workflowservice.TerminateNexusOperationExecutionRequest
	return terminateNexusOperationExecutionValidator{
		Request: terminateNexusOperationExecutionRequestFieldValidators{
			Namespace:   validation.NoOp[req, string](),
			OperationId: validation.Field[req](requiredID(config.MaxIDLengthLimit())),
			RunId:       validation.Field[req](validateOptionalRunID),
			Identity:    validation.NoOp[req, string](),
			RequestId:   validation.Field[req](validateOptionalRunID),
			Reason:      validation.NoOp[req, string](),
		},
		Response: terminateNexusOperationExecutionResponseFieldValidators{},
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

func requiredString(fieldName, value string) error {
	if value == "" {
		return serviceerror.NewInvalidArgumentf("%s is required", fieldName)
	}
	return nil
}

// validateIDLength0 validates an optional ID: empty is allowed but if provided it must be within the length limit.
func validateIDLength0(limit int) func(string, string) error {
	return func(fieldName, value string) error {
		if value == "" {
			return nil
		}
		return validateIDLength(fieldName, value, limit)
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
