package nexusoperation

//go:generate go run ../../../cmd/tools/genvalidationcoverage -messages-file validator_gen.messages -out validator_gen.go

import (
	"slices"
	"strings"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/validation"
	"google.golang.org/protobuf/types/known/durationpb"
)

var ValidatorModule = validation.Module(
	"chasm.lib.nexusoperation.validators",
	newStartNexusOperationExecutionRequestValidator,
	newPollNexusOperationExecutionRequestValidator,
	newRequestCancelNexusOperationExecutionRequestValidator,
	newTerminateNexusOperationExecutionRequestValidator,
	newDeleteNexusOperationExecutionRequestValidator,
)

func newUserMetadataValidator(config *Config) userMetadataFieldValidators {
	return userMetadataFieldValidators{
		Summary: func(ns string, _ *sdkpb.UserMetadata, fieldName string, summary *commonpb.Payload) error {
			if summary.Size() > config.MaxUserMetadataSummarySize(ns) {
				return serviceerror.NewInvalidArgumentf("%s exceeds size limit", fieldName)
			}
			return nil
		},
		Details: func(ns string, _ *sdkpb.UserMetadata, fieldName string, details *commonpb.Payload) error {
			if details.Size() > config.MaxUserMetadataDetailsSize(ns) {
				return serviceerror.NewInvalidArgumentf("%s exceeds size limit", fieldName)
			}
			return nil
		},
	}
}

func newStartNexusOperationExecutionRequestValidator(
	config *Config,
	logger log.Logger,
	saMapperProvider searchattribute.MapperProvider,
	saValidator *searchattribute.Validator,
) startNexusOperationExecutionRequestFieldValidators {
	userMetadataValidators := newUserMetadataValidator(config)
	return startNexusOperationExecutionRequestFieldValidators{
		Namespace: func(*workflowservice.StartNexusOperationExecutionRequest, string, string) error {
			return nil
		},
		Identity: validation.Field[workflowservice.StartNexusOperationExecutionRequest](maxStringLength(config.MaxIDLengthLimit())),
		RequestId: func(req *workflowservice.StartNexusOperationExecutionRequest, fieldName string, requestID string) error {
			if requestID == "" {
				req.RequestId = uuid.NewString()
				return nil
			}
			return validateStringMaxLength(fieldName, requestID, config.MaxIDLengthLimit())
		},
		OperationId: validation.Field[workflowservice.StartNexusOperationExecutionRequest](requiredID(config.MaxIDLengthLimit())),
		Endpoint: func(_ *workflowservice.StartNexusOperationExecutionRequest, fieldName string, endpoint string) error {
			if endpoint == "" {
				return serviceerror.NewInvalidArgumentf("%s is required", fieldName)
			}
			return nil
		},
		Service: func(req *workflowservice.StartNexusOperationExecutionRequest, fieldName string, service string) error {
			if service == "" {
				return serviceerror.NewInvalidArgumentf("%s is required", fieldName)
			}
			limit := config.MaxServiceNameLength(req.GetNamespace())
			if len(service) > limit {
				return serviceerror.NewInvalidArgumentf("%s exceeds length limit. Length=%d Limit=%d", fieldName, len(service), limit)
			}
			return nil
		},
		Operation: func(req *workflowservice.StartNexusOperationExecutionRequest, fieldName string, operation string) error {
			if operation == "" {
				return serviceerror.NewInvalidArgumentf("%s is required", fieldName)
			}
			limit := config.MaxOperationNameLength(req.GetNamespace())
			if len(operation) > limit {
				return serviceerror.NewInvalidArgumentf("%s exceeds length limit. Length=%d Limit=%d", fieldName, len(operation), limit)
			}
			return nil
		},
		ScheduleToCloseTimeout: startNexusValidateScheduleToCloseTimeout(config),
		ScheduleToStartTimeout: startNexusValidateScheduleToStartTimeout(),
		StartToCloseTimeout:    startNexusValidateStartToCloseTimeout(),
		Input:                  startNexusValidateInput(config, logger),
		IdReusePolicy: func(req *workflowservice.StartNexusOperationExecutionRequest, _ string, idReusePolicy enumspb.NexusOperationIdReusePolicy) error {
			if idReusePolicy == enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_UNSPECIFIED {
				req.IdReusePolicy = enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_ALLOW_DUPLICATE
			}
			return nil
		},
		IdConflictPolicy: func(req *workflowservice.StartNexusOperationExecutionRequest, _ string, idConflictPolicy enumspb.NexusOperationIdConflictPolicy) error {
			if idConflictPolicy == enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_UNSPECIFIED {
				req.IdConflictPolicy = enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_FAIL
			}
			return nil
		},
		SearchAttributes: startNexusValidateSearchAttributes(saMapperProvider, saValidator),
		NexusHeader: startNexusValidateNexusHeader(config),
		UserMetadata: func(req *workflowservice.StartNexusOperationExecutionRequest, _ string, userMetadata *sdkpb.UserMetadata) error {
			return userMetadataValidators.ValidateAndNormalize(req.GetNamespace(), userMetadata)
		},
	}
}

func startNexusValidateScheduleToCloseTimeout(config *Config) func(*workflowservice.StartNexusOperationExecutionRequest, string, *durationpb.Duration) error {
	return func(req *workflowservice.StartNexusOperationExecutionRequest, fieldName string, scheduleToCloseTimeout *durationpb.Duration) error {
		if err := timestamp.ValidateAndCapProtoDuration(scheduleToCloseTimeout); err != nil {
			return serviceerror.NewInvalidArgumentf("%s is invalid: %v", fieldName, err)
		}
		duration := scheduleToCloseTimeout.AsDuration()
		maxTimeout := config.MaxOperationScheduleToCloseTimeout(req.GetNamespace())
		if maxTimeout > 0 && (duration == 0 || duration > maxTimeout) {
			req.ScheduleToCloseTimeout = durationpb.New(maxTimeout)
		}
		return nil
	}
}

func startNexusValidateScheduleToStartTimeout() func(*workflowservice.StartNexusOperationExecutionRequest, string, *durationpb.Duration) error {
	return func(req *workflowservice.StartNexusOperationExecutionRequest, fieldName string, scheduleToStartTimeout *durationpb.Duration) error {
		if err := timestamp.ValidateAndCapProtoDuration(scheduleToStartTimeout); err != nil {
			return serviceerror.NewInvalidArgumentf("%s is invalid: %v", fieldName, err)
		}
		scheduleToCloseTimeout := req.GetScheduleToCloseTimeout().AsDuration()
		if scheduleToCloseTimeout > 0 && scheduleToStartTimeout.AsDuration() > scheduleToCloseTimeout {
			req.ScheduleToStartTimeout = req.GetScheduleToCloseTimeout()
		}
		return nil
	}
}

func startNexusValidateStartToCloseTimeout() func(*workflowservice.StartNexusOperationExecutionRequest, string, *durationpb.Duration) error {
	return func(req *workflowservice.StartNexusOperationExecutionRequest, fieldName string, startToCloseTimeout *durationpb.Duration) error {
		if err := timestamp.ValidateAndCapProtoDuration(startToCloseTimeout); err != nil {
			return serviceerror.NewInvalidArgumentf("%s is invalid: %v", fieldName, err)
		}
		scheduleToCloseTimeout := req.GetScheduleToCloseTimeout().AsDuration()
		if scheduleToCloseTimeout > 0 && startToCloseTimeout.AsDuration() > scheduleToCloseTimeout {
			req.StartToCloseTimeout = req.GetScheduleToCloseTimeout()
		}
		return nil
	}
}


func startNexusValidateInput(config *Config, logger log.Logger) func(*workflowservice.StartNexusOperationExecutionRequest, string, *commonpb.Payload) error {
	return func(req *workflowservice.StartNexusOperationExecutionRequest, fieldName string, input *commonpb.Payload) error {
		ns := req.GetNamespace()
		inputSize := input.Size()
		if inputSize > config.PayloadSizeLimitWarn(ns) {
			logger.Warn("Nexus Start Operation input size exceeds the warning limit.",
				tag.WorkflowNamespace(ns),
				tag.OperationID(req.GetOperationId()),
				tag.BlobSize(int64(inputSize)),
				tag.BlobSizeViolationOperation("StartNexusOperationExecution"))
		}
		if inputSize > config.PayloadSizeLimit(ns) {
			return serviceerror.NewInvalidArgumentf("%s exceeds size limit. Length=%d Limit=%d", fieldName, inputSize, config.PayloadSizeLimit(ns))
		}
		return nil
	}
}

func startNexusValidateSearchAttributes(saMapperProvider searchattribute.MapperProvider, saValidator *searchattribute.Validator) func(*workflowservice.StartNexusOperationExecutionRequest, string, *commonpb.SearchAttributes) error {
	return func(req *workflowservice.StartNexusOperationExecutionRequest, _ string, _ *commonpb.SearchAttributes) error {
		namespaceName := req.GetNamespace()
		searchAttributes := req.SearchAttributes
		if saMapperProvider != nil && searchAttributes != nil {
			var err error
			searchAttributes, err = searchattribute.UnaliasFields(saMapperProvider, searchAttributes, namespaceName)
			if err != nil {
				return err
			}
		}
		if err := saValidator.Validate(searchAttributes, namespaceName); err != nil {
			return err
		}
		return saValidator.ValidateSize(searchAttributes, namespaceName)
	}
}

func startNexusValidateNexusHeader(config *Config) func(*workflowservice.StartNexusOperationExecutionRequest, string, map[string]string) error {
	return func(req *workflowservice.StartNexusOperationExecutionRequest, fieldName string, nexusHeader map[string]string) error {
		headerLength := 0
		loweredHeaders := make(map[string]string, len(nexusHeader))
		disallowedHeaders := config.DisallowedOperationHeaders()
		for key, value := range nexusHeader {
			loweredKey := strings.ToLower(key)
			headerLength += len(loweredKey) + len(value)
			if slices.Contains(disallowedHeaders, loweredKey) {
				return serviceerror.NewInvalidArgumentf("%s contains a disallowed key: %q", fieldName, key)
			}
			loweredHeaders[loweredKey] = value
		}
		if headerLength > config.MaxOperationHeaderSize(req.GetNamespace()) {
			return serviceerror.NewInvalidArgumentf("%s exceeds size limit", fieldName)
		}
		req.NexusHeader = loweredHeaders
		return nil
	}
}

func newDescribeNexusOperationExecutionRequestValidator(
	config *Config,
	namespaceID string,
) describeNexusOperationExecutionRequestFieldValidators {
	return describeNexusOperationExecutionRequestFieldValidators{
		Namespace: func(*workflowservice.DescribeNexusOperationExecutionRequest, string, string) error {
			return nil
		},
		OperationId: validation.Field[workflowservice.DescribeNexusOperationExecutionRequest](requiredID(config.MaxIDLengthLimit())),
		RunId: func(_ *workflowservice.DescribeNexusOperationExecutionRequest, fieldName string, runID string) error {
			if runID != "" {
				if err := uuid.Validate(runID); err != nil {
					return serviceerror.NewInvalidArgumentf("%s is not a valid UUID", fieldName)
				}
			}
			return nil
		},
		IncludeInput: func(*workflowservice.DescribeNexusOperationExecutionRequest, string, bool) error {
			return nil
		},
		IncludeOutcome: func(*workflowservice.DescribeNexusOperationExecutionRequest, string, bool) error {
			return nil
		},
		LongPollToken: func(req *workflowservice.DescribeNexusOperationExecutionRequest, _ string, longPollToken []byte) error {
			if len(longPollToken) > 0 && req.GetRunId() == "" {
				return serviceerror.NewInvalidArgument("run_id is required when long_poll_token is provided")
			}
			if len(longPollToken) > 0 {
				ref, err := chasm.DeserializeComponentRef(longPollToken)
				if err != nil {
					return serviceerror.NewInvalidArgument("invalid long poll token")
				}
				if ref.NamespaceID != namespaceID {
					return serviceerror.NewInvalidArgument("long poll token does not match execution")
				}
			}
			return nil
		},
	}
}

func newPollNexusOperationExecutionRequestValidator(
	config *Config,
) pollNexusOperationExecutionRequestFieldValidators {
	return pollNexusOperationExecutionRequestFieldValidators{
		Namespace: func(*workflowservice.PollNexusOperationExecutionRequest, string, string) error {
			return nil
		},
		OperationId: validation.Field[workflowservice.PollNexusOperationExecutionRequest](requiredID(config.MaxIDLengthLimit())),
		RunId:       validation.Field[workflowservice.PollNexusOperationExecutionRequest](validateOptionalRunID),
		WaitStage: func(req *workflowservice.PollNexusOperationExecutionRequest, fieldName string, waitStage enumspb.NexusOperationWaitStage) error {
			if waitStage == enumspb.NEXUS_OPERATION_WAIT_STAGE_UNSPECIFIED {
				req.WaitStage = enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED
				return nil
			}
			switch waitStage {
			case enumspb.NEXUS_OPERATION_WAIT_STAGE_STARTED, enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED:
				return nil
			default:
				return serviceerror.NewInvalidArgumentf("unsupported %s: %s", fieldName, waitStage)
			}
		},
	}
}

func newRequestCancelNexusOperationExecutionRequestValidator(
	config *Config,
) requestCancelNexusOperationExecutionRequestFieldValidators {
	return requestCancelNexusOperationExecutionRequestFieldValidators{
		Namespace: func(*workflowservice.RequestCancelNexusOperationExecutionRequest, string, string) error {
			return nil
		},
		OperationId: validation.Field[workflowservice.RequestCancelNexusOperationExecutionRequest](requiredID(config.MaxIDLengthLimit())),
		RunId:       validation.Field[workflowservice.RequestCancelNexusOperationExecutionRequest](validateOptionalRunID),
		Identity:    validation.Field[workflowservice.RequestCancelNexusOperationExecutionRequest](maxStringLength(config.MaxIDLengthLimit())),
		RequestId: func(req *workflowservice.RequestCancelNexusOperationExecutionRequest, fieldName string, requestID string) error {
			if requestID == "" {
				req.RequestId = uuid.NewString()
				return nil
			}
			return validateStringMaxLength(fieldName, requestID, config.MaxIDLengthLimit())
		},
		Reason: func(req *workflowservice.RequestCancelNexusOperationExecutionRequest, fieldName string, reason string) error {
			return validateStringMaxLength(fieldName, reason, config.MaxReasonLength(req.GetNamespace()))
		},
	}
}

func newTerminateNexusOperationExecutionRequestValidator(
	config *Config,
) terminateNexusOperationExecutionRequestFieldValidators {
	return terminateNexusOperationExecutionRequestFieldValidators{
		Namespace: func(*workflowservice.TerminateNexusOperationExecutionRequest, string, string) error {
			return nil
		},
		OperationId: validation.Field[workflowservice.TerminateNexusOperationExecutionRequest](requiredID(config.MaxIDLengthLimit())),
		RunId:       validation.Field[workflowservice.TerminateNexusOperationExecutionRequest](validateOptionalRunID),
		Identity:    validation.Field[workflowservice.TerminateNexusOperationExecutionRequest](maxStringLength(config.MaxIDLengthLimit())),
		RequestId: func(req *workflowservice.TerminateNexusOperationExecutionRequest, fieldName string, requestID string) error {
			if requestID == "" {
				req.RequestId = uuid.NewString()
				return nil
			}
			return validateStringMaxLength(fieldName, requestID, config.MaxIDLengthLimit())
		},
		Reason: func(req *workflowservice.TerminateNexusOperationExecutionRequest, fieldName string, reason string) error {
			return validateStringMaxLength(fieldName, reason, config.MaxReasonLength(req.GetNamespace()))
		},
	}
}

func newDeleteNexusOperationExecutionRequestValidator(
	config *Config,
) deleteNexusOperationExecutionRequestFieldValidators {
	return deleteNexusOperationExecutionRequestFieldValidators{
		Namespace: func(*workflowservice.DeleteNexusOperationExecutionRequest, string, string) error {
			return nil
		},
		OperationId: validation.Field[workflowservice.DeleteNexusOperationExecutionRequest](requiredID(config.MaxIDLengthLimit())),
		RunId: func(_ *workflowservice.DeleteNexusOperationExecutionRequest, _ string, runID string) error {
			if runID != "" {
				if err := uuid.Validate(runID); err != nil {
					return serviceerror.NewInvalidArgument("invalid run id: must be a valid UUID")
				}
			}
			return nil
		},
	}
}

func validateStringMaxLength(fieldName string, value string, limit int) error {
	if len(value) > limit {
		return serviceerror.NewInvalidArgumentf("%s exceeds length limit. Length=%d Limit=%d", fieldName, len(value), limit)
	}
	return nil
}

func maxStringLength(limit int) func(string, string) error {
	return func(fieldName string, value string) error {
		return validateStringMaxLength(fieldName, value, limit)
	}
}

func requiredID(limit int) func(string, string) error {
	return func(fieldName string, value string) error {
		if value == "" {
			return serviceerror.NewInvalidArgumentf("%s is required", fieldName)
		}
		return validateStringMaxLength(fieldName, value, limit)
	}
}

func validateOptionalRunID(fieldName string, runID string) error {
	if runID != "" {
		if err := uuid.Validate(runID); err != nil {
			return serviceerror.NewInvalidArgumentf("%s is not a valid UUID", fieldName)
		}
	}
	return nil
}
