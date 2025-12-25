package workflow

import (
	"fmt"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/enums"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/priorities"
	"go.temporal.io/server/common/retrypolicy"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/tqid"
	"google.golang.org/protobuf/types/known/durationpb"
)

var (
	ErrWorkflowIDNotSet                            = serviceerror.NewInvalidArgument("WorkflowId is not set on request.")
	errIncompatibleIDReusePolicyTerminateIfRunning = serviceerror.NewInvalidArgument("Invalid WorkflowIDReusePolicy: WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING cannot be used together with a WorkflowIDConflictPolicy")
	errIncompatibleIDReusePolicyRejectDuplicate    = serviceerror.NewInvalidArgument("Invalid WorkflowIDReusePolicy: WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE cannot be used together with WorkflowIdConflictPolicy WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING")
	errInvalidWorkflowExecutionTimeoutSeconds      = serviceerror.NewInvalidArgument("An invalid WorkflowExecutionTimeoutSeconds is set on request.")
	errInvalidWorkflowRunTimeoutSeconds            = serviceerror.NewInvalidArgument("An invalid WorkflowRunTimeoutSeconds is set on request.")
	errInvalidWorkflowTaskTimeoutSeconds           = serviceerror.NewInvalidArgument("An invalid WorkflowTaskTimeoutSeconds is set on request.")
	ErrCronAndStartDelaySet                        = serviceerror.NewInvalidArgument("CronSchedule and WorkflowStartDelay may not be used together.")
	ErrInvalidWorkflowStartDelaySeconds            = serviceerror.NewInvalidArgument("An invalid WorkflowStartDelaySeconds is set on request.")
)

type Validator struct {
	config           Config
	saMapperProvider searchattribute.MapperProvider
	saValidator      *searchattribute.Validator
}

func NewValidator(
	config Config,
	saMapperProvider searchattribute.MapperProvider,
	saValidator *searchattribute.Validator,
) *Validator {
	return &Validator{
		config:           config,
		saMapperProvider: saMapperProvider,
		saValidator:      saValidator,
	}
}

func (v *Validator) ValidateWorkflowID(
	workflowID string,
) error {
	if workflowID == "" {
		return ErrWorkflowIDNotSet
	}
	if len(workflowID) > v.config.maxIDLengthLimit() {
		return serviceerror.NewInvalidArgumentf("WorkflowId exceeds maximum allowed length (%d/%d)", len(workflowID), v.config.maxIDLengthLimit())
	}
	return nil
}

type StartWorkflowTimeoutLikeRequest interface {
	GetWorkflowExecutionTimeout() *durationpb.Duration
	GetWorkflowRunTimeout() *durationpb.Duration
	GetWorkflowTaskTimeout() *durationpb.Duration
}

func (v *Validator) ValidateWorkflowTimeouts(
	request StartWorkflowTimeoutLikeRequest,
) error {
	if err := timestamp.ValidateAndCapProtoDuration(request.GetWorkflowExecutionTimeout()); err != nil {
		return fmt.Errorf("%w cause: %v", errInvalidWorkflowExecutionTimeoutSeconds, err)
	}

	if err := timestamp.ValidateAndCapProtoDuration(request.GetWorkflowRunTimeout()); err != nil {
		return fmt.Errorf("%w cause: %v", errInvalidWorkflowRunTimeoutSeconds, err)
	}

	if err := timestamp.ValidateAndCapProtoDuration(request.GetWorkflowTaskTimeout()); err != nil {
		return fmt.Errorf("%w cause: %v", errInvalidWorkflowTaskTimeoutSeconds, err)
	}

	return nil
}

func (v *Validator) ValidateRetryPolicy(namespaceName string, retryPolicy *commonpb.RetryPolicy) error {
	if retryPolicy == nil {
		// By default, if the user does not explicitly set a retry policy for a Workflow, do not perform any retries.
		return nil
	}

	retrypolicy.EnsureDefaults(retryPolicy, v.config.defaultWorkflowRetrySettings(namespaceName))
	return retrypolicy.Validate(retryPolicy)
}

func (v *Validator) ValidateWorkflowStartDelay(
	cronSchedule string,
	startDelay *durationpb.Duration,
) error {
	if len(cronSchedule) > 0 && startDelay != nil {
		return ErrCronAndStartDelaySet
	}

	if err := timestamp.ValidateAndCapProtoDuration(startDelay); err != nil {
		return fmt.Errorf("%w cause: %v", ErrInvalidWorkflowStartDelaySeconds, err)
	}

	return nil
}
func (v *Validator) ValidateWorkflowIdReusePolicy(
	reusePolicy enumspb.WorkflowIdReusePolicy,
	conflictPolicy enumspb.WorkflowIdConflictPolicy,
) error {
	if conflictPolicy != enumspb.WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED &&
		reusePolicy == enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING {
		return errIncompatibleIDReusePolicyTerminateIfRunning
	}
	if conflictPolicy == enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING &&
		reusePolicy == enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE {
		return errIncompatibleIDReusePolicyRejectDuplicate
	}
	return nil
}

func (v *Validator) ValidateLinks(
	ns string,
	links []*commonpb.Link,
) error {
	maxAllowedLinks := v.config.maxLinksPerRequest(ns)
	if len(links) > maxAllowedLinks {
		return serviceerror.NewInvalidArgumentf("cannot attach more than %d links per request, got %d", maxAllowedLinks, len(links))
	}

	maxSize := v.config.linkMaxSize(ns)
	for _, l := range links {
		if l.Size() > maxSize {
			return serviceerror.NewInvalidArgumentf("link exceeds allowed size of %d, got %d", maxSize, l.Size())
		}
		switch t := l.Variant.(type) {
		case *commonpb.Link_WorkflowEvent_:
			if t.WorkflowEvent.GetNamespace() == "" {
				return serviceerror.NewInvalidArgument("workflow event link must not have an empty namespace field")
			}
			if t.WorkflowEvent.GetWorkflowId() == "" {
				return serviceerror.NewInvalidArgument("workflow event link must not have an empty workflow ID field")
			}
			if t.WorkflowEvent.GetRunId() == "" {
				return serviceerror.NewInvalidArgument("workflow event link must not have an empty run ID field")
			}
			if t.WorkflowEvent.GetEventRef().GetEventType() == enumspb.EVENT_TYPE_UNSPECIFIED && t.WorkflowEvent.GetEventRef().GetEventId() != 0 {
				return serviceerror.NewInvalidArgument("workflow event link ref cannot have an unspecified event type and a non-zero event ID")
			}
		case *commonpb.Link_BatchJob_:
			if t.BatchJob.GetJobId() == "" {
				return serviceerror.NewInvalidArgument("batch job link must not have an empty job ID")
			}
		default:
			return serviceerror.NewInvalidArgument("unsupported link variant")
		}
	}
	return nil
}

func (v *Validator) UnaliasedSearchAttributesFrom(
	attributes *commonpb.SearchAttributes,
	namespaceName string,
) (*commonpb.SearchAttributes, error) {
	sa, err := searchattribute.UnaliasFields(v.saMapperProvider, attributes, namespaceName)
	if err != nil {
		return nil, err
	}

	if err = v.ValidateSearchAttributes(sa, namespaceName); err != nil {
		return nil, err
	}
	return sa, nil
}

func (v *Validator) ValidateSearchAttributes(searchAttributes *commonpb.SearchAttributes, namespaceName string) error {
	if err := v.saValidator.Validate(searchAttributes, namespaceName); err != nil {
		return err
	}
	return v.saValidator.ValidateSize(searchAttributes, namespaceName)
}

func (v *Validator) ValidateSignalWithStartRequest(request *workflowservice.SignalWithStartWorkflowExecutionRequest) error {
	if request == nil {
		return serviceerror.NewInvalidArgument("request is empty")
	}

	if err := v.ValidateWorkflowID(request.GetWorkflowId()); err != nil {
		return err
	}

	if request.GetSignalName() == "" {
		return serviceerror.NewInvalidArgument("signal not set")
	}

	if len(request.GetSignalName()) > v.config.maxIDLengthLimit() {
		return serviceerror.NewInvalidArgumentf("signal name exceeds maximum allowed length (%d/%d)", len(request.GetSignalName()), v.config.maxIDLengthLimit())
	}

	if request.GetWorkflowType().GetName() == "" {
		return serviceerror.NewInvalidArgument("workflow type not set")
	}

	if len(request.GetWorkflowType().GetName()) > v.config.maxIDLengthLimit() {
		return serviceerror.NewInvalidArgumentf("workflow type name exceeds maximum allowed length (%d/%d)", len(request.GetWorkflowType().GetName()), v.config.maxIDLengthLimit())
	}

	if err := tqid.NormalizeAndValidate(request.TaskQueue, "", v.config.maxIDLengthLimit()); err != nil {
		return err
	}

	if request.RequestId == "" {
		// For easy direct API use, we default the request ID here but expect all
		// SDKs and other auto-retrying clients to set it
		request.RequestId = uuid.NewString()
	} else if len(request.RequestId) > v.config.maxIDLengthLimit() {
		return serviceerror.NewInvalidArgumentf("Request ID exceeds maximum allowed length (%d/%d)", len(request.RequestId), v.config.maxIDLengthLimit())
	}

	if err := v.ValidateWorkflowTimeouts(request); err != nil {
		return err
	}

	if err := v.ValidateRetryPolicy(request.GetNamespace(), request.RetryPolicy); err != nil {
		return err
	}

	if err := v.ValidateWorkflowStartDelay(request.GetCronSchedule(), request.WorkflowStartDelay); err != nil {
		return err
	}

	if err := v.ValidateWorkflowIdReusePolicy(
		request.WorkflowIdReusePolicy,
		request.WorkflowIdConflictPolicy,
	); err != nil {
		return err
	}

	if request.WorkflowIdConflictPolicy == enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL {
		// Signal-with-*Required*-Start is not supported
		name := enumspb.WorkflowIdConflictPolicy_name[int32(request.WorkflowIdConflictPolicy.Number())]
		return serviceerror.NewInvalidArgumentf("Invalid WorkflowIDConflictPolicy: %v is not supported for this operation.", name)
	}

	enums.SetDefaultWorkflowIdReusePolicy(&request.WorkflowIdReusePolicy)
	enums.SetDefaultWorkflowIdConflictPolicy(&request.WorkflowIdConflictPolicy, enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING)

	sa, err := v.UnaliasedSearchAttributesFrom(request.GetSearchAttributes(), request.GetNamespace())
	if err != nil {
		return err
	}
	request.SearchAttributes = sa

	if err := priorities.Validate(request.Priority); err != nil {
		return err
	}

	return v.ValidateLinks(request.GetNamespace(), request.GetLinks())
}
