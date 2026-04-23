package nexusoperation

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	"google.golang.org/protobuf/types/known/durationpb"
)

// ValidateServiceName checks that the service name does not exceed the configured limit.
func ValidateServiceName(service string, limit int) error {
	if len(service) > limit {
		return fmt.Errorf("service exceeds length limit. Length=%d Limit=%d", len(service), limit)
	}
	return nil
}

// ValidateOperationName checks that the operation name does not exceed the configured limit.
func ValidateOperationName(operation string, limit int) error {
	if len(operation) > limit {
		return fmt.Errorf("operation exceeds length limit. Length=%d Limit=%d", len(operation), limit)
	}
	return nil
}

// ValidateAndLowercaseNexusHeaders validates headers and returns a new map with lower-cased keys.
func ValidateAndLowercaseNexusHeaders(headers map[string]string, disallowed []string, sizeLimit int) (map[string]string, error) {
	headerLength := 0
	lowered := make(map[string]string, len(headers))
	for k, v := range headers {
		lowerK := strings.ToLower(k)
		headerLength += len(lowerK) + len(v)
		if slices.Contains(disallowed, lowerK) {
			return nil, fmt.Errorf("nexus_header contains a disallowed key: %q", k)
		}
		lowered[lowerK] = v
	}
	if headerLength > sizeLimit {
		return nil, errors.New("nexus_header exceeds size limit")
	}
	return lowered, nil
}

// ValidatePayloadSize checks that the payload does not exceed the size limit.
func ValidatePayloadSize(input *commonpb.Payload, limit int) error {
	if input.Size() > limit {
		return errors.New("input exceeds size limit")
	}
	return nil
}

//revive:disable-next-line:cognitive-complexity,cyclomatic
func validateAndNormalizeStartRequest(
	req *workflowservice.StartNexusOperationExecutionRequest,
	config *Config,
	logger log.Logger,
	saMapperProvider searchattribute.MapperProvider,
	saValidator *searchattribute.Validator,
) error {
	ns := req.GetNamespace()
	if req.GetRequestId() == "" {
		req.RequestId = uuid.NewString()
	} else if len(req.GetRequestId()) > config.MaxIDLengthLimit() {
		return serviceerror.NewInvalidArgumentf("request_id exceeds length limit. Length=%d Limit=%d",
			len(req.GetRequestId()), config.MaxIDLengthLimit())
	}
	if req.GetOperationId() == "" {
		return serviceerror.NewInvalidArgument("operation_id is required")
	}
	if len(req.GetOperationId()) > config.MaxIDLengthLimit() {
		return serviceerror.NewInvalidArgumentf("operation_id exceeds length limit. Length=%d Limit=%d",
			len(req.GetOperationId()), config.MaxIDLengthLimit())
	}
	if len(req.GetIdentity()) > config.MaxIDLengthLimit() {
		return serviceerror.NewInvalidArgumentf("identity exceeds length limit. Length=%d Limit=%d",
			len(req.GetIdentity()), config.MaxIDLengthLimit())
	}
	if req.GetEndpoint() == "" {
		return serviceerror.NewInvalidArgument("endpoint is required")
	}
	if req.GetService() == "" {
		return serviceerror.NewInvalidArgument("service is required")
	}
	if err := ValidateServiceName(req.GetService(), config.MaxServiceNameLength(ns)); err != nil {
		return serviceerror.NewInvalidArgument(err.Error())
	}
	if req.GetOperation() == "" {
		return serviceerror.NewInvalidArgument("operation is required")
	}
	if err := ValidateOperationName(req.GetOperation(), config.MaxOperationNameLength(ns)); err != nil {
		return serviceerror.NewInvalidArgument(err.Error())
	}
	if err := timestamp.ValidateAndCapProtoDuration(req.GetScheduleToCloseTimeout()); err != nil {
		return serviceerror.NewInvalidArgumentf("schedule_to_close_timeout is invalid: %v", err)
	}
	if err := timestamp.ValidateAndCapProtoDuration(req.GetScheduleToStartTimeout()); err != nil {
		return serviceerror.NewInvalidArgumentf("schedule_to_start_timeout is invalid: %v", err)
	}
	if err := timestamp.ValidateAndCapProtoDuration(req.GetStartToCloseTimeout()); err != nil {
		return serviceerror.NewInvalidArgumentf("start_to_close_timeout is invalid: %v", err)
	}

	scheduleToCloseTimeout := req.GetScheduleToCloseTimeout().AsDuration()
	maxTimeout := config.MaxOperationScheduleToCloseTimeout(ns)
	if maxTimeout > 0 {
		if scheduleToCloseTimeout == 0 || scheduleToCloseTimeout > maxTimeout {
			// Apply the effective namespace limit to schedule_to_close_timeout before capping the other timeouts.
			req.ScheduleToCloseTimeout = durationpb.New(maxTimeout)
			scheduleToCloseTimeout = maxTimeout
		}
	}

	// Bound schedule_to_start_timeout and start_to_close_timeout to schedule_to_close_timeout.
	if scheduleToCloseTimeout > 0 {
		if req.GetScheduleToStartTimeout().AsDuration() > scheduleToCloseTimeout {
			req.ScheduleToStartTimeout = req.GetScheduleToCloseTimeout()
		}
		if req.GetStartToCloseTimeout().AsDuration() > scheduleToCloseTimeout {
			req.StartToCloseTimeout = req.GetScheduleToCloseTimeout()
		}
	}

	inputSize := req.GetInput().Size()
	if inputSize > config.PayloadSizeLimitWarn(ns) {
		logger.Warn("Nexus Start Operation input size exceeds the warning limit.",
			tag.WorkflowNamespace(ns),
			tag.OperationID(req.GetOperationId()),
			tag.BlobSize(int64(inputSize)),
			tag.BlobSizeViolationOperation("StartNexusOperationExecution"))
	}
	if inputSize > config.PayloadSizeLimit(ns) {
		return serviceerror.NewInvalidArgumentf("input exceeds size limit. Length=%d Limit=%d",
			inputSize, config.PayloadSizeLimit(ns))
	}

	loweredHeaders, err := ValidateAndLowercaseNexusHeaders(req.GetNexusHeader(), config.DisallowedOperationHeaders(), config.MaxOperationHeaderSize(ns))
	if err != nil {
		return serviceerror.NewInvalidArgument(err.Error())
	}
	req.NexusHeader = loweredHeaders
	if err := validateAndNormalizeSearchAttributes(req, saMapperProvider, saValidator); err != nil {
		// SA validator already returns properly typed gRPC status errors; no need to re-wrap.
		return err
	}
	if req.GetIdReusePolicy() == enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_UNSPECIFIED {
		req.IdReusePolicy = enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_ALLOW_DUPLICATE
	}
	if req.GetIdConflictPolicy() == enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_UNSPECIFIED {
		req.IdConflictPolicy = enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_FAIL
	}
	return nil
}

func validateAndNormalizeDeleteRequest(req *workflowservice.DeleteNexusOperationExecutionRequest, config *Config) error {
	if req.GetOperationId() == "" {
		return serviceerror.NewInvalidArgument("operation_id is required")
	}
	if len(req.GetOperationId()) > config.MaxIDLengthLimit() {
		return serviceerror.NewInvalidArgumentf("operation_id exceeds length limit. Length=%d Limit=%d",
			len(req.GetOperationId()), config.MaxIDLengthLimit())
	}
	if req.GetRunId() != "" {
		if err := uuid.Validate(req.GetRunId()); err != nil {
			return serviceerror.NewInvalidArgument("invalid run id: must be a valid UUID")
		}
	}
	return nil
}

func validateAndNormalizeDescribeRequest(
	req *workflowservice.DescribeNexusOperationExecutionRequest,
	namespaceID string,
	config *Config,
) error {
	if req.GetOperationId() == "" {
		return serviceerror.NewInvalidArgument("operation_id is required")
	}
	if len(req.GetOperationId()) > config.MaxIDLengthLimit() {
		return serviceerror.NewInvalidArgumentf("operation_id exceeds length limit. Length=%d Limit=%d",
			len(req.GetOperationId()), config.MaxIDLengthLimit())
	}
	if len(req.GetLongPollToken()) > 0 && req.GetRunId() == "" {
		return serviceerror.NewInvalidArgument("run_id is required when long_poll_token is provided")
	}
	if req.GetRunId() != "" {
		if err := uuid.Validate(req.GetRunId()); err != nil {
			return serviceerror.NewInvalidArgument("run_id is not a valid UUID")
		}
	}
	if len(req.GetLongPollToken()) > 0 {
		ref, err := chasm.DeserializeComponentRef(req.GetLongPollToken())
		if err != nil {
			return serviceerror.NewInvalidArgument("invalid long poll token")
		}
		if ref.NamespaceID != namespaceID {
			return serviceerror.NewInvalidArgument("long poll token does not match execution")
		}
	}
	return nil
}

func validateAndNormalizePollRequest(req *workflowservice.PollNexusOperationExecutionRequest, config *Config) error {
	// Normalize wait stage: UNSPECIFIED defaults to CLOSED.
	if req.GetWaitStage() == enumspb.NEXUS_OPERATION_WAIT_STAGE_UNSPECIFIED {
		req.WaitStage = enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED
	} else {
		switch req.GetWaitStage() {
		case enumspb.NEXUS_OPERATION_WAIT_STAGE_STARTED,
			enumspb.NEXUS_OPERATION_WAIT_STAGE_CLOSED:
		default:
			return serviceerror.NewInvalidArgumentf("unsupported wait_stage: %s", req.GetWaitStage())
		}
	}
	if req.GetOperationId() == "" {
		return serviceerror.NewInvalidArgument("operation_id is required")
	}
	if len(req.GetOperationId()) > config.MaxIDLengthLimit() {
		return serviceerror.NewInvalidArgumentf("operation_id exceeds length limit. Length=%d Limit=%d",
			len(req.GetOperationId()), config.MaxIDLengthLimit())
	}
	if runID := req.GetRunId(); runID != "" {
		if err := uuid.Validate(runID); err != nil {
			return serviceerror.NewInvalidArgument("run_id is not a valid UUID")
		}
	}
	return nil
}

func validateAndNormalizeCancelRequest(req *workflowservice.RequestCancelNexusOperationExecutionRequest, config *Config) error {
	if req.GetRequestId() == "" {
		req.RequestId = uuid.NewString()
	} else if len(req.GetRequestId()) > config.MaxIDLengthLimit() {
		return serviceerror.NewInvalidArgumentf("request_id exceeds length limit. Length=%d Limit=%d",
			len(req.GetRequestId()), config.MaxIDLengthLimit())
	}
	if req.GetOperationId() == "" {
		return serviceerror.NewInvalidArgument("operation_id is required")
	}
	if len(req.GetOperationId()) > config.MaxIDLengthLimit() {
		return serviceerror.NewInvalidArgumentf("operation_id exceeds length limit. Length=%d Limit=%d",
			len(req.GetOperationId()), config.MaxIDLengthLimit())
	}
	if runID := req.GetRunId(); runID != "" {
		if err := uuid.Validate(runID); err != nil {
			return serviceerror.NewInvalidArgument("run_id is not a valid UUID")
		}
	}
	if len(req.GetIdentity()) > config.MaxIDLengthLimit() {
		return serviceerror.NewInvalidArgumentf("identity exceeds length limit. Length=%d Limit=%d",
			len(req.GetIdentity()), config.MaxIDLengthLimit())
	}
	if len(req.GetReason()) > config.MaxReasonLength(req.GetNamespace()) {
		return serviceerror.NewInvalidArgumentf("reason exceeds length limit. Length=%d Limit=%d",
			len(req.GetReason()), config.MaxReasonLength(req.GetNamespace()))
	}

	return nil
}

func validateAndNormalizeTerminateRequest(req *workflowservice.TerminateNexusOperationExecutionRequest, config *Config) error {
	if req.GetRequestId() == "" {
		req.RequestId = uuid.NewString()
	} else if len(req.GetRequestId()) > config.MaxIDLengthLimit() {
		return serviceerror.NewInvalidArgumentf("request_id exceeds length limit. Length=%d Limit=%d",
			len(req.GetRequestId()), config.MaxIDLengthLimit())
	}
	if req.GetOperationId() == "" {
		return serviceerror.NewInvalidArgument("operation_id is required")
	}
	if len(req.GetOperationId()) > config.MaxIDLengthLimit() {
		return serviceerror.NewInvalidArgumentf("operation_id exceeds length limit. Length=%d Limit=%d",
			len(req.GetOperationId()), config.MaxIDLengthLimit())
	}
	if runID := req.GetRunId(); runID != "" {
		if err := uuid.Validate(runID); err != nil {
			return serviceerror.NewInvalidArgument("run_id is not a valid UUID")
		}
	}
	if len(req.GetIdentity()) > config.MaxIDLengthLimit() {
		return serviceerror.NewInvalidArgumentf("identity exceeds length limit. Length=%d Limit=%d",
			len(req.GetIdentity()), config.MaxIDLengthLimit())
	}
	if len(req.GetReason()) > config.MaxReasonLength(req.GetNamespace()) {
		return serviceerror.NewInvalidArgumentf("reason exceeds length limit. Length=%d Limit=%d",
			len(req.GetReason()), config.MaxReasonLength(req.GetNamespace()))
	}

	return nil
}

func validateAndNormalizeSearchAttributes(
	req *workflowservice.StartNexusOperationExecutionRequest,
	saMapperProvider searchattribute.MapperProvider,
	saValidator *searchattribute.Validator,
) error {
	namespaceName := req.GetNamespace()

	// Unalias search attributes for validation.
	saToValidate := req.SearchAttributes
	if saMapperProvider != nil && saToValidate != nil {
		var err error
		saToValidate, err = searchattribute.UnaliasFields(saMapperProvider, saToValidate, namespaceName)
		if err != nil {
			return err
		}
	}

	if err := saValidator.Validate(saToValidate, namespaceName); err != nil {
		return err
	}

	return saValidator.ValidateSize(saToValidate, namespaceName)
}
