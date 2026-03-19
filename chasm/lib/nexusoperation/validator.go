package nexusoperation

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
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

// ValidateAndCapScheduleToCloseTimeout validates and caps the schedule-to-close timeout.
// It returns the (possibly capped) duration.
func ValidateAndCapScheduleToCloseTimeout(timeout *durationpb.Duration, maxTimeout time.Duration) (*durationpb.Duration, error) {
	if err := timestamp.ValidateAndCapProtoDuration(timeout); err != nil {
		return timeout, fmt.Errorf("schedule_to_close_timeout is invalid: %v", err)
	}
	if maxTimeout > 0 {
		if t := timeout.AsDuration(); t == 0 || t > maxTimeout {
			timeout = durationpb.New(maxTimeout)
		}
	}
	return timeout, nil
}

// ValidatePayloadSize checks that the payload does not exceed the size limit.
func ValidatePayloadSize(input *commonpb.Payload, limit int) error {
	if input.Size() > limit {
		return errors.New("input exceeds size limit")
	}
	return nil
}

func validateAndNormalizeStartRequest(
	req *workflowservice.StartNexusOperationExecutionRequest,
	config *Config,
	saMapperProvider searchattribute.MapperProvider,
	saValidator *searchattribute.Validator,
) error {
	ns := req.GetNamespace()
	if req.GetOperationId() == "" {
		return serviceerror.NewInvalidArgument("operation_id is required")
	}
	if len(req.GetOperationId()) > config.MaxIDLengthLimit() {
		return serviceerror.NewInvalidArgumentf("operation_id exceeds length limit. Length=%d Limit=%d",
			len(req.GetOperationId()), config.MaxIDLengthLimit())
	}
	if req.GetRequestId() == "" {
		req.RequestId = uuid.NewString()
	}
	if len(req.GetRequestId()) > config.MaxIDLengthLimit() {
		return serviceerror.NewInvalidArgumentf("request_id exceeds length limit. Length=%d Limit=%d",
			len(req.GetRequestId()), config.MaxIDLengthLimit())
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
	var err error
	if req.ScheduleToCloseTimeout, err = ValidateAndCapScheduleToCloseTimeout(
		req.GetScheduleToCloseTimeout(),
		config.MaxOperationScheduleToCloseTimeout(ns),
	); err != nil {
		return serviceerror.NewInvalidArgument(err.Error())
	}
	if err := ValidatePayloadSize(req.GetInput(), config.PayloadSizeLimit(ns)); err != nil {
		return serviceerror.NewInvalidArgument(err.Error())
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

func validateDescribeNexusOperationExecutionRequest(req *workflowservice.DescribeNexusOperationExecutionRequest, config *Config) error {
	if req.GetOperationId() == "" {
		return serviceerror.NewInvalidArgument("operation_id is required")
	}
	if len(req.GetOperationId()) > config.MaxIDLengthLimit() {
		return serviceerror.NewInvalidArgumentf("operation_id exceeds length limit. Length=%d Limit=%d",
			len(req.GetOperationId()), config.MaxIDLengthLimit())
	}
	if len(req.GetRunId()) > config.MaxIDLengthLimit() {
		return serviceerror.NewInvalidArgumentf("run_id exceeds length limit. Length=%d Limit=%d",
			len(req.GetRunId()), config.MaxIDLengthLimit())
	}
	// TODO: Add long-poll validation (run_id required when long_poll_token is set).
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
