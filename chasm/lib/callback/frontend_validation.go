package callback

import (
	"fmt"

	"github.com/google/uuid"
	callbackpb "go.temporal.io/api/callback/v1"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/searchattribute"
	"google.golang.org/protobuf/proto"
)

// Returns a serviceerror.InvalidArgument error for a missing required field.
func missingRequiredFieldError(fieldName string) error {
	msg := fmt.Sprintf("%s is not set on request.", fieldName)
	return serviceerror.NewInvalidArgument(msg)
}

type RequestIDer interface {
	GetRequestId() string
}

func verifyRequestIDLength(reqProto RequestIDer, config *Config) error {
	l := len(reqProto.GetRequestId())
	maxLen := config.MaxIDLength()
	if l > maxLen {
		return serviceerror.NewInvalidArgumentf("callback ID exceeds length limit. Length=%d Limit=%d", l, maxLen)
	}
	return nil
}

type CallbackIDer interface {
	GetCallbackId() string
}

func verifyCallbackIDLength(reqProto CallbackIDer, config *Config) error {
	l := len(reqProto.GetCallbackId())
	maxLen := config.MaxIDLength()
	if l > maxLen {
		return serviceerror.NewInvalidArgumentf("callback ID exceeds length limit. Length=%d Limit=%d", l, maxLen)
	}
	return nil
}

// frontendRequestValidator bundles the configuration data for validating an incomming request.
//
// IMPORTANT: Validation methods MAY mutate the incomming request, in order to ensure they all have
// a valid RunID (if one was not specified already).
type frontendRequestValidator struct {
	config           *Config
	cbValidator      Validator
	logger           log.Logger
	saMapperProvider searchattribute.MapperProvider
	saValidator      *searchattribute.Validator
}

func (rv *frontendRequestValidator) ValidateStartCallbackExecution(req *workflowservice.StartCallbackExecutionRequest) error {
	// Set RequestID if missing.
	if req.GetRequestId() == "" {
		req.RequestId = uuid.NewString()
	}

	// Required fields.
	requiredFields := map[string]string{
		"Namespace":  req.GetNamespace(),
		"Identity":   req.GetIdentity(),
		"RequestId":  req.GetRequestId(),
		"CallbackId": req.GetCallbackId(),
	}
	for k, v := range requiredFields {
		if v == "" {
			return missingRequiredFieldError(k)
		}
	}

	// Field lengths
	if err := verifyRequestIDLength(req, rv.config); err != nil {
		return err
	}
	if err := verifyCallbackIDLength(req, rv.config); err != nil {
		return err
	}

	// Validate the callback to be invoked and its parameters.
	if err := rv.cbValidator.Validate(req.GetNamespace(), []*commonpb.Callback{req.Callback}); err != nil {
		return err
	}

	// ScheduleToCloseTimeout
	if req.GetScheduleToCloseTimeout() == nil || req.GetScheduleToCloseTimeout().AsDuration() <= 0 {
		return serviceerror.NewInvalidArgument("ScheduleToCloseTimeout must be set and positive.")
	}

	// Validate the input data to deliver to the callback URL, currently only one kind is supported (Completion).
	completion := req.GetCompletion()
	if completion == nil {
		return serviceerror.NewInvalidArgument("Completion is not set on request.")
	}
	if completion.GetSuccess() == nil && completion.GetFailure() == nil {
		return serviceerror.NewInvalidArgument("Completion must have either success or failure set.")
	}
	if completion.GetSuccess() != nil && completion.GetFailure() != nil {
		return serviceerror.NewInvalidArgument("Completion must have exactly one of success or failure set, not both.")
	}
	// Validate the size of the completion is reasonable.
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

func (rv *frontendRequestValidator) validateCompletionSize(req Namespacer, completion *callbackpb.CallbackExecutionCompletion) error {
	namespace := req.GetNamespace()

	sizeWarnLimit := rv.config.BlobSizeLimitWarn(namespace)
	sizeErrorLimit := rv.config.BlobSizeLimitError(namespace)

	blobSize := proto.Size(completion)
	if blobSize > sizeWarnLimit {
		rv.logger.Warn("Completion blob size exceeds the warning limit.",
			tag.WorkflowNamespace(namespace),
			tag.BlobSize(int64(blobSize)))
	}

	if blobSize > sizeErrorLimit {
		return common.ErrBlobSizeExceedsLimit
	}

	return nil
}

func (rv *frontendRequestValidator) validateSearchAttributes(req Namespacer, saToValidate *commonpb.SearchAttributes) error {
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

func (rv *frontendRequestValidator) ValidateDescribeCallbackExecution(req *workflowservice.DescribeCallbackExecutionRequest) error {
	// Required fields.
	requiredFields := map[string]string{
		"Namespace":  req.GetNamespace(),
		"CallbackId": req.GetCallbackId(),
	}
	for k, v := range requiredFields {
		if v == "" {
			return missingRequiredFieldError(k)
		}
	}

	// Field lengths
	if err := verifyCallbackIDLength(req, rv.config); err != nil {
		return err
	}

	// A long-poll token requires the RunID be set.
	if len(req.GetLongPollToken()) > 0 && req.GetRunId() == "" {
		return serviceerror.NewInvalidArgument("RunID is required when LongPollToken is provided")
	}

	return nil
}

func (rv *frontendRequestValidator) ValidatePollCallbackExecution(req *workflowservice.PollCallbackExecutionRequest) error {
	// Required fields.
	requiredFields := map[string]string{
		"Namespace":  req.GetNamespace(),
		"CallbackId": req.GetCallbackId(),
	}
	for k, v := range requiredFields {
		if v == "" {
			return missingRequiredFieldError(k)
		}
	}

	// Field lengths
	return verifyCallbackIDLength(req, rv.config)
}

func (rv *frontendRequestValidator) ValidateTerminateCallbackExecution(req *workflowservice.TerminateCallbackExecutionRequest) error {
	// Set RequestID if missing.
	if req.GetRequestId() == "" {
		req.RequestId = uuid.NewString()
	}

	// Required fields.
	requiredFields := map[string]string{
		"RequestId":  req.GetRequestId(),
		"Namespace":  req.GetNamespace(),
		"CallbackId": req.GetCallbackId(),

		// NOTE: We don't require the Identity or Reason fields to be set,
		// and just set reasonable defaults.
	}
	for k, v := range requiredFields {
		if v == "" {
			return missingRequiredFieldError(k)
		}
	}

	// Field lengths
	if err := verifyRequestIDLength(req, rv.config); err != nil {
		return err
	}
	return verifyCallbackIDLength(req, rv.config)
}

func (rv *frontendRequestValidator) ValidateDeleteCallbackExecution(req *workflowservice.DeleteCallbackExecutionRequest) error {
	// Required fields.
	requiredFields := map[string]string{
		"Namespace":  req.GetNamespace(),
		"CallbackId": req.GetCallbackId(),
	}
	for k, v := range requiredFields {
		if v == "" {
			return missingRequiredFieldError(k)
		}
	}

	// Field lengths
	return verifyCallbackIDLength(req, rv.config)
}

func (rv *frontendRequestValidator) ValidateListCallbackExecutions(req *workflowservice.ListCallbackExecutionsRequest) error {
	if req.GetNamespace() == "" {
		return missingRequiredFieldError("Namespace")
	}
	return nil
}

func (rv *frontendRequestValidator) ValidateCountCallbackExecutions(req *workflowservice.CountCallbackExecutionsRequest) error {
	if req.GetNamespace() == "" {
		return missingRequiredFieldError("Namespace")
	}
	return nil
}
