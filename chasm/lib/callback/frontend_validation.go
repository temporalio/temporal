package callback

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	callbackpb "go.temporal.io/api/callback/v1"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Returns a serviceerror.InvalidArgument error for a missing required field.
func missingRequiredFieldError(fieldName string) error {
	msg := fmt.Sprintf("%s is required", fieldName)
	return serviceerror.NewInvalidArgument(msg)
}

func verifyFieldLength(fieldName, fieldValue string, maxLen int) error {
	if l := len(fieldValue); l > maxLen {
		return serviceerror.NewInvalidArgumentf("%s exceeds length limit. Length=%d Limit=%d", fieldName, l, maxLen)
	}
	return nil
}

func verifyIsUUID(fieldName, fieldValue string) error {
	if err := uuid.Validate(fieldValue); err != nil {
		return serviceerror.NewInvalidArgumentf("invalid %s: must be a valid UUID", fieldName)
	}
	return nil
}

// requiredStringField is a tuple of a required field name and its value.
// Used instead of a map[string]string to provide deterministic
// errors if multiple fields aren't set.
type requiredStringField struct {
	FieldName string
	Value     string
}

func (rf requiredStringField) Validate() error {
	if rf.Value == "" {
		return missingRequiredFieldError(rf.FieldName)
	}
	return nil
}

type requiredFields []requiredStringField

func (fields requiredFields) Validate() error {
	for _, rf := range fields {
		if err := rf.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// frontendRequestValidator bundles the configuration data for validating an incoming request.
//
// In addition some operations may *mutate* the request object too, e.g. to ensure a valid RunID
// is available if one wasn't supplied by the caller.
type frontendRequestValidator struct {
	config           *Config
	cbValidator      Validator
	logger           log.Logger
	saMapperProvider searchattribute.MapperProvider
	saValidator      *searchattribute.Validator
}

func (rv *frontendRequestValidator) ValidateAndNormalizeStartCallbackExecution(
	ctx context.Context, req *workflowservice.StartCallbackExecutionRequest) error {
	// Set RequestID if missing.
	if req.GetRequestId() == "" {
		req.RequestId = uuid.NewString()
	}

	// Check required fields each have a value.
	requiredFields := requiredFields{
		{"namespace", req.GetNamespace()},
		{"identity", req.GetIdentity()},
		{"callback_id", req.GetCallbackId()},
	}
	if err := requiredFields.Validate(); err != nil {
		return err
	}

	// Field lengths
	maxLen := rv.config.MaxIDLength()
	if err := verifyFieldLength("callback_id", req.GetCallbackId(), maxLen); err != nil {
		return err
	}
	if err := verifyFieldLength("identity", req.GetIdentity(), maxLen); err != nil {
		return err
	}
	if err := verifyFieldLength("request_id", req.GetRequestId(), maxLen); err != nil {
		return err
	}

	// Validate the callback to be invoked and its parameters.
	if err := rv.cbValidator.Validate(ctx, req.GetNamespace(), []*commonpb.Callback{req.GetCallback()}); err != nil {
		return err
	}

	// ScheduleToCloseTimeout
	if schedToCloseTimeout := req.GetScheduleToCloseTimeout(); schedToCloseTimeout != nil {
		if schedToCloseTimeout.AsDuration() <= 0 {
			return serviceerror.NewInvalidArgument("schedule_to_close_timeout must be positive")
		}

		// Clamp the ScheduleToCloseTimeout to the maximum allowed if set.
		maxAllowed := rv.config.MaxCallbackScheduleToCloseTimeout(req.Namespace)
		if maxAllowed > 0 {
			clamped := min(schedToCloseTimeout.AsDuration(), maxAllowed)
			req.ScheduleToCloseTimeout = durationpb.New(clamped)
		}
	}

	// Validate the input data to deliver to the callback URL, currently only one kind is supported (Completion).
	completion := req.GetCompletion()
	if completion == nil {
		return serviceerror.NewInvalidArgument("completion is not set on request")
	}
	if completion.GetSuccess() == nil && completion.GetFailure() == nil {
		return serviceerror.NewInvalidArgument("completion must have either success or failure set")
	}
	if completion.GetSuccess() != nil && completion.GetFailure() != nil {
		return serviceerror.NewInvalidArgument("completion must have exactly one of success or failure set, not both")
	}
	if err := rv.validateCompletionSize(req, completion); err != nil {
		return err
	}

	// Search Attributes
	if searchAttrib := req.GetSearchAttributes(); searchAttrib != nil {
		if err := rv.validateSearchAttributes(req, searchAttrib); err != nil {
			return err
		}
	}

	return nil
}

func (rv *frontendRequestValidator) validateCompletionSize(req namespacer, completion *callbackpb.CallbackExecutionCompletion) error {
	ns := req.GetNamespace()

	sizeWarnLimit := rv.config.BlobSizeLimitWarn(ns)
	sizeErrorLimit := rv.config.BlobSizeLimitError(ns)

	blobSize := proto.Size(completion)
	if blobSize > sizeWarnLimit {
		rv.logger.Warn("completion blob size exceeds the warning limit",
			tag.WorkflowNamespace(ns),
			tag.BlobSize(int64(blobSize)))
	}

	if blobSize > sizeErrorLimit {
		return common.ErrBlobSizeExceedsLimit
	}

	return nil
}

func (rv *frontendRequestValidator) validateSearchAttributes(req namespacer, saToValidate *commonpb.SearchAttributes) error {
	namespaceName := req.GetNamespace()

	// Unalias search attributes for validation.
	if rv.saMapperProvider != nil && saToValidate != nil {
		var err error
		saToValidate, err = searchattribute.UnaliasFields(rv.saMapperProvider, saToValidate, namespaceName)
		if err != nil {
			return err
		}
	}

	if err := rv.saValidator.Validate(saToValidate, namespaceName); err != nil {
		return err
	}

	return rv.saValidator.ValidateSize(saToValidate, namespaceName)
}

func (rv *frontendRequestValidator) ValidateDescribeCallbackExecution(req *workflowservice.DescribeCallbackExecutionRequest, targetNamespaceID namespace.ID) error {
	// Check required fields each have a value.
	requiredFields := requiredFields{
		{"namespace", req.GetNamespace()},
		{"callback_id", req.GetCallbackId()},
	}
	if err := requiredFields.Validate(); err != nil {
		return err
	}

	// RunID (if set)
	if req.GetRunId() != "" {
		if err := verifyIsUUID("run_id", req.GetRunId()); err != nil {
			return err
		}
	}

	// Field lengths
	if err := verifyFieldLength("callback_id", req.GetCallbackId(), rv.config.MaxIDLength()); err != nil {
		return err
	}

	// Long-poll Token
	if len(req.GetLongPollToken()) > 0 {
		if req.GetRunId() == "" {
			return serviceerror.NewInvalidArgument("run_id is required when long_poll_token is provided")
		}

		ref, err := chasm.DeserializeComponentRef(req.GetLongPollToken())
		if err != nil {
			return serviceerror.NewInvalidArgument("invalid long poll token")
		}
		if ref.NamespaceID != targetNamespaceID.String() {
			return serviceerror.NewInvalidArgument("long poll token does not match execution")
		}
	}

	return nil
}

func (rv *frontendRequestValidator) ValidatePollCallbackExecution(req *workflowservice.PollCallbackExecutionRequest) error {
	// Check required fields each have a value.
	requiredFields := requiredFields{
		{"namespace", req.GetNamespace()},
		{"callback_id", req.GetCallbackId()},
	}
	if err := requiredFields.Validate(); err != nil {
		return err
	}

	// RunID (if set)
	if req.GetRunId() != "" {
		if err := verifyIsUUID("run_id", req.GetRunId()); err != nil {
			return err
		}
	}

	// Field lengths
	return verifyFieldLength("callback_id", req.GetCallbackId(), rv.config.MaxIDLength())
}

func (rv *frontendRequestValidator) ValidateAndNormalizeTerminateCallbackExecution(req *workflowservice.TerminateCallbackExecutionRequest) error {
	// Set RequestID if missing.
	if req.GetRequestId() == "" {
		req.RequestId = uuid.NewString()
	}

	// Check required fields each have a value.
	// NOTE: We don't require the Identity or Reason fields to be set,
	// and just set reasonable defaults.
	requiredFields := requiredFields{
		{"namespace", req.GetNamespace()},
		{"callback_id", req.GetCallbackId()},
	}
	if err := requiredFields.Validate(); err != nil {
		return err
	}

	// RunID (if set)
	if req.GetRunId() != "" {
		if err := verifyIsUUID("run_id", req.GetRunId()); err != nil {
			return err
		}
	}

	// Field lengths.
	maxLen := rv.config.MaxIDLength()
	if err := verifyFieldLength("callback_id", req.GetCallbackId(), maxLen); err != nil {
		return err
	}
	if err := verifyFieldLength("identity", req.GetIdentity(), maxLen); err != nil {
		return err
	}
	if err := verifyFieldLength("request_id", req.GetRequestId(), maxLen); err != nil {
		return err
	}

	// Capping reason to "MaxIDLength", despite it not really being an ID.
	return verifyFieldLength("reason", req.GetReason(), maxLen)
}

func (rv *frontendRequestValidator) ValidateDeleteCallbackExecution(req *workflowservice.DeleteCallbackExecutionRequest) error {
	// Check required fields each have a value.
	requiredFields := requiredFields{
		{"namespace", req.GetNamespace()},
		{"callback_id", req.GetCallbackId()},
	}
	if err := requiredFields.Validate(); err != nil {
		return err
	}

	// Field lengths
	return verifyFieldLength("callback_id", req.GetCallbackId(), rv.config.MaxIDLength())
}

func (rv *frontendRequestValidator) ValidateListCallbackExecutions(req *workflowservice.ListCallbackExecutionsRequest) error {
	if req.GetNamespace() == "" {
		return missingRequiredFieldError("namespace")
	}
	return nil
}

func (rv *frontendRequestValidator) ValidateCountCallbackExecutions(req *workflowservice.CountCallbackExecutionsRequest) error {
	if req.GetNamespace() == "" {
		return missingRequiredFieldError("namespace")
	}
	return nil
}
