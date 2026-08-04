package nexusoperation

import (
	"slices"
	"strings"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	"google.golang.org/protobuf/types/known/durationpb"
)

const errInvalidRunID = "run_id is not a valid UUID"

// cancelOrTerminateRequest is the subset of the cancel and terminate requests that are validated the same way.
type cancelOrTerminateRequest interface {
	GetNamespace() string
	GetOperationId() string
	GetRunId() string
	GetIdentity() string
	GetReason() string
}

// validator validates and normalizes the frontend requests for standalone Nexus operations.
//
// Requests are mutated in place: defaults are applied, values are capped to their configured
// limits, and headers are lower-cased.
type validator struct {
	config           *Config
	logger           log.Logger
	saMapperProvider searchattribute.MapperProvider
	saValidator      *searchattribute.Validator
}

func newValidator(
	config *Config,
	logger log.Logger,
	saMapperProvider searchattribute.MapperProvider,
	saValidator *searchattribute.Validator,
) *validator {
	return &validator{
		config:           config,
		logger:           logger,
		saMapperProvider: saMapperProvider,
		saValidator:      saValidator,
	}
}

func (v *validator) validateAndNormalizeStartRequest(req *workflowservice.StartNexusOperationExecutionRequest) error {
	ns := req.GetNamespace()

	if err := v.normalizeRequestID(&req.RequestId); err != nil {
		return err
	}
	if err := v.validateOperationID(req.GetOperationId()); err != nil {
		return err
	}
	if err := v.validateIDLength("identity", req.GetIdentity()); err != nil {
		return err
	}
	if err := v.validateTarget(req); err != nil {
		return err
	}
	if err := v.validateAndCapTimeouts(req); err != nil {
		return err
	}
	if err := v.validateInput(req); err != nil {
		return err
	}
	if err := v.validateUserMetadata(ns, req.GetUserMetadata()); err != nil {
		return err
	}

	loweredHeaders, err := v.validateAndLowercaseHeaders(ns, req.GetNexusHeader())
	if err != nil {
		return err
	}
	req.NexusHeader = loweredHeaders

	if err := v.validateAndNormalizeSearchAttributes(req); err != nil {
		return err
	}

	v.normalizeIDPolicies(req)
	return nil
}

func (v *validator) validateAndNormalizeDescribeRequest(
	req *workflowservice.DescribeNexusOperationExecutionRequest,
	namespaceID string,
) error {
	if err := v.validateOperationID(req.GetOperationId()); err != nil {
		return err
	}
	if len(req.GetLongPollToken()) > 0 && req.GetRunId() == "" {
		return serviceerror.NewInvalidArgument("run_id is required when long_poll_token is provided")
	}
	if err := v.validateRunID(req.GetRunId()); err != nil {
		return err
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

func (v *validator) validateAndNormalizePollRequest(req *workflowservice.PollNexusOperationExecutionRequest) error {
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
	if err := v.validateOperationID(req.GetOperationId()); err != nil {
		return err
	}
	return v.validateRunID(req.GetRunId())
}

func (v *validator) validateAndNormalizeCancelRequest(req *workflowservice.RequestCancelNexusOperationExecutionRequest) error {
	if err := v.normalizeRequestID(&req.RequestId); err != nil {
		return err
	}
	return v.validateCancelOrTerminateRequest(req)
}

func (v *validator) validateAndNormalizeTerminateRequest(req *workflowservice.TerminateNexusOperationExecutionRequest) error {
	if err := v.normalizeRequestID(&req.RequestId); err != nil {
		return err
	}
	return v.validateCancelOrTerminateRequest(req)
}

func (v *validator) validateAndNormalizeDeleteRequest(req *workflowservice.DeleteNexusOperationExecutionRequest) error {
	if err := v.validateOperationID(req.GetOperationId()); err != nil {
		return err
	}
	return v.validateRunID(req.GetRunId())
}

// validateCancelOrTerminateRequest validates the fields the cancel and terminate requests have in
// common.
func (v *validator) validateCancelOrTerminateRequest(req cancelOrTerminateRequest) error {
	if err := v.validateOperationID(req.GetOperationId()); err != nil {
		return err
	}
	if err := v.validateRunID(req.GetRunId()); err != nil {
		return err
	}
	if err := v.validateIDLength("identity", req.GetIdentity()); err != nil {
		return err
	}
	if limit := v.config.MaxReasonLength(req.GetNamespace()); len(req.GetReason()) > limit {
		return serviceerror.NewInvalidArgumentf("reason exceeds length limit. Length=%d Limit=%d",
			len(req.GetReason()), limit)
	}
	return nil
}

// validateTarget checks the endpoint, service, and operation the request targets.
func (v *validator) validateTarget(req *workflowservice.StartNexusOperationExecutionRequest) error {
	ns := req.GetNamespace()
	if req.GetEndpoint() == "" {
		return serviceerror.NewInvalidArgument("endpoint is required")
	}
	if req.GetService() == "" {
		return serviceerror.NewInvalidArgument("service is required")
	}
	if limit := v.config.MaxServiceNameLength(ns); len(req.GetService()) > limit {
		return serviceerror.NewInvalidArgumentf("service exceeds length limit. Length=%d Limit=%d",
			len(req.GetService()), limit)
	}
	if req.GetOperation() == "" {
		return serviceerror.NewInvalidArgument("operation is required")
	}
	if limit := v.config.MaxOperationNameLength(ns); len(req.GetOperation()) > limit {
		return serviceerror.NewInvalidArgumentf("operation exceeds length limit. Length=%d Limit=%d",
			len(req.GetOperation()), limit)
	}
	return nil
}

// validateAndCapTimeouts validates the request timeouts, capping schedule_to_close_timeout to the
// namespace limit and the remaining timeouts to schedule_to_close_timeout.
func (v *validator) validateAndCapTimeouts(req *workflowservice.StartNexusOperationExecutionRequest) error {
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
	maxTimeout := v.config.MaxOperationScheduleToCloseTimeout(req.GetNamespace())
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
	return nil
}

func (v *validator) validateInput(req *workflowservice.StartNexusOperationExecutionRequest) error {
	ns := req.GetNamespace()
	inputSize := req.GetInput().Size()
	if inputSize > v.config.PayloadSizeLimitWarn(ns) {
		v.logger.Warn("Nexus Start Operation input size exceeds the warning limit.",
			tag.WorkflowNamespace(ns),
			tag.OperationID(req.GetOperationId()),
			tag.BlobSize(int64(inputSize)),
			tag.BlobSizeViolationOperation("StartNexusOperationExecution"))
	}
	if limit := v.config.PayloadSizeLimit(ns); inputSize > limit {
		return serviceerror.NewInvalidArgumentf("input exceeds size limit. Length=%d Limit=%d", inputSize, limit)
	}
	return nil
}

func (v *validator) validateUserMetadata(ns string, metadata *sdkpb.UserMetadata) error {
	summarySize := metadata.GetSummary().Size()
	if limit := v.config.MaxUserMetadataSummarySize(ns); summarySize > limit {
		return serviceerror.NewInvalidArgumentf(
			"user_metadata.summary exceeds size limit. Length=%d Limit=%d", summarySize, limit)
	}
	detailsSize := metadata.GetDetails().Size()
	if limit := v.config.MaxUserMetadataDetailsSize(ns); detailsSize > limit {
		return serviceerror.NewInvalidArgumentf(
			"user_metadata.details exceeds size limit. Length=%d Limit=%d", detailsSize, limit)
	}
	return nil
}

// validateAndLowercaseHeaders validates the Nexus headers and returns a new map with lower-cased
// keys.
func (v *validator) validateAndLowercaseHeaders(ns string, headers map[string]string) (map[string]string, error) {
	disallowed := v.config.DisallowedOperationHeaders()
	headerLength := 0
	lowered := make(map[string]string, len(headers))
	for k, val := range headers {
		lowerK := strings.ToLower(k)
		headerLength += len(lowerK) + len(val)
		if slices.Contains(disallowed, lowerK) {
			return nil, serviceerror.NewInvalidArgumentf("nexus_header contains a disallowed key: %q", k)
		}
		lowered[lowerK] = val
	}
	if headerLength > v.config.MaxOperationHeaderSize(ns) {
		return nil, serviceerror.NewInvalidArgument("nexus_header exceeds size limit")
	}
	return lowered, nil
}

func (v *validator) validateAndNormalizeSearchAttributes(req *workflowservice.StartNexusOperationExecutionRequest) error {
	namespaceName := req.GetNamespace()

	// Unalias search attributes for validation.
	saToValidate := req.SearchAttributes
	if v.saMapperProvider != nil && saToValidate != nil {
		var err error
		saToValidate, err = searchattribute.UnaliasFields(v.saMapperProvider, saToValidate, namespaceName)
		if err != nil {
			return err
		}
	}

	if err := v.saValidator.Validate(saToValidate, namespaceName); err != nil {
		return err
	}

	return v.saValidator.ValidateSize(saToValidate, namespaceName)
}

// normalizeIDPolicies applies the default ID reuse and conflict policies.
func (v *validator) normalizeIDPolicies(req *workflowservice.StartNexusOperationExecutionRequest) {
	if req.GetIdReusePolicy() == enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_UNSPECIFIED {
		req.IdReusePolicy = enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_ALLOW_DUPLICATE
	}
	if req.GetIdConflictPolicy() == enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_UNSPECIFIED {
		req.IdConflictPolicy = enumspb.NEXUS_OPERATION_ID_CONFLICT_POLICY_FAIL
	}
}

// normalizeRequestID validates the request ID, or sets it to a UUID if empty.
func (v *validator) normalizeRequestID(requestID *string) error {
	if *requestID == "" {
		*requestID = uuid.NewString()
		return nil
	}
	return v.validateIDLength("request_id", *requestID)
}

func (v *validator) validateOperationID(operationID string) error {
	if operationID == "" {
		return serviceerror.NewInvalidArgument("operation_id is required")
	}
	return v.validateIDLength("operation_id", operationID)
}

// validateRunID checks that the run ID, if set, is a valid UUID.
func (v *validator) validateRunID(runID string) error {
	if runID == "" {
		return nil
	}
	if err := uuid.Validate(runID); err != nil {
		return serviceerror.NewInvalidArgument(errInvalidRunID)
	}
	return nil
}

// validateIDLength checks the given field against the shared max ID length limit.
func (v *validator) validateIDLength(field, value string) error {
	if limit := v.config.MaxIDLengthLimit(); len(value) > limit {
		return serviceerror.NewInvalidArgumentf("%s exceeds length limit. Length=%d Limit=%d", field, len(value), limit)
	}
	return nil
}
