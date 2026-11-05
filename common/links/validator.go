// Package links validates temporal.api.common.v1.Link values attached to executions.
//
// Validation mirrors common/callbacks and is split in two:
//
//   - [Validator.Validate] checks the contents of a single request: the shape of each link, its size,
//     and the number of links on the request. It is stateless and runs in the frontend.
//   - [Validator.ValidateAdditions] checks per-execution aggregates (e.g. the total number of links on
//     an execution) against what the execution already holds. It runs in history, where that state is
//     available, and assumes Validate has already been called.
//
// Links embedded in completion callbacks ([commonpb.Callback.Links]) are checked by Validate and count
// toward the per-request limit. They are NOT counted toward execution aggregates: a callback's links
// are part of its size, so they are already charged to the callback budget enforced by
// [go.temporal.io/server/common/callbacks.Validator.ValidateAdditions].
//
// Only CHASM components that store caller-attached links (standalone activities and standalone Nexus
// operations) enforce execution aggregates. Workflow links are persisted in history events, so their
// total is bounded by history size limits instead.
package links

import (
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/dynamicconfig"
)

// CurrentLinksInfo describes the caller-attached links already on an execution, so that aggregate
// limits can be enforced. Mirrors callbacks.CurrentCallbacksInfo.
type CurrentLinksInfo struct {
	Count int
}

// CurrentLinksInfoOf computes the [CurrentLinksInfo] for the links already attached to an execution,
// e.g. chasm.Context.Links(component).
func CurrentLinksInfoOf(existing []*commonpb.Link) CurrentLinksInfo {
	return CurrentLinksInfo{Count: len(existing)}
}

// Validator validates links attached to executions (e.g. workflows, standalone activities, and
// standalone Nexus operations).
type Validator interface {
	// Validate rejects a request whose links are malformed, too large, or too numerous. Links embedded
	// in cbs are validated the same way and count toward the per-request limit; pass nil for requests
	// that cannot carry completion callbacks.
	Validate(namespaceName string, links []*commonpb.Link, cbs []*commonpb.Callback) error

	// ValidateAdditions enforces aggregate limits, like the total number of links that can be attached
	// to an execution. It is the cumulative counterpart to Validate, and should be called on both
	// start and attach paths before persisting caller-attached links.
	//
	// newLinks must only contain links attached directly to the request. Callback-embedded links are
	// charged to the callback budget instead (see package docs).
	ValidateAdditions(namespaceName string, newLinks []*commonpb.Link, existing CurrentLinksInfo) error
}

// ValidatorConfig holds the limits a [Validator] enforces.
type ValidatorConfig struct {
	// Per-request limits, enforced by Validate.
	MaxLinksPerRequest dynamicconfig.IntPropertyFnWithNamespaceFilter
	LinkMaxSize        dynamicconfig.IntPropertyFnWithNamespaceFilter

	// Per-execution limits, enforced by ValidateAdditions.
	MaxLinksPerExecution dynamicconfig.IntPropertyFnWithNamespaceFilter
}

func (vc *ValidatorConfig) Validate() error {
	var missingFields []string
	assertGetterIsSet := func(name string, getter dynamicconfig.IntPropertyFnWithNamespaceFilter) {
		if getter == nil {
			missingFields = append(missingFields, name)
		}
	}

	assertGetterIsSet("MaxLinksPerRequest", vc.MaxLinksPerRequest)
	assertGetterIsSet("LinkMaxSize", vc.LinkMaxSize)
	assertGetterIsSet("MaxLinksPerExecution", vc.MaxLinksPerExecution)

	if len(missingFields) != 0 {
		return fmt.Errorf("missing required fields: %v", missingFields)
	}
	return nil
}

type validator struct {
	config ValidatorConfig
}

// NewValidator returns a new Validator.
func NewValidator(config ValidatorConfig) (Validator, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &validator{config: config}, nil
}

// ValidatorProvider creates a link Validator using the production dynamic config keys.
func ValidatorProvider(dc *dynamicconfig.Collection) (Validator, error) {
	return NewValidator(ValidatorConfig{
		MaxLinksPerRequest:   dynamicconfig.FrontendMaxLinksPerRequest.Get(dc),
		LinkMaxSize:          dynamicconfig.FrontendLinkMaxSize.Get(dc),
		MaxLinksPerExecution: dynamicconfig.MaxLinksPerComponent.Get(dc),
	})
}

// Validate checks the count of links on the request (including those embedded in cbs), and the size
// and required fields of each one.
func (v *validator) Validate(namespaceName string, links []*commonpb.Link, cbs []*commonpb.Callback) error {
	count := len(links)
	for _, cb := range cbs {
		count += len(cb.GetLinks())
	}
	maxLinks := v.config.MaxLinksPerRequest(namespaceName)
	if count > maxLinks {
		return serviceerror.NewInvalidArgumentf("cannot attach more than %d links per request, got %d", maxLinks, count)
	}

	maxSize := v.config.LinkMaxSize(namespaceName)
	if err := validateEach(links, maxSize); err != nil {
		return err
	}
	for _, cb := range cbs {
		if err := validateEach(cb.GetLinks(), maxSize); err != nil {
			return err
		}
	}
	return nil
}

// ValidateAdditions checks the count the execution would reach once newLinks are attached. Errors are
// FailedPrecondition rather than InvalidArgument: the request may be perfectly well-formed and only
// fail because of what the execution already holds.
func (v *validator) ValidateAdditions(
	namespaceName string,
	newLinks []*commonpb.Link,
	existing CurrentLinksInfo,
) error {
	if len(newLinks) == 0 {
		return nil
	}
	maxCount := v.config.MaxLinksPerExecution(namespaceName)
	if existing.Count+len(newLinks) > maxCount {
		return serviceerror.NewFailedPreconditionf(
			"cannot attach more than %d links to an execution (%d links already attached)",
			maxCount,
			existing.Count,
		)
	}
	return nil
}

// validateEach checks the size and required fields of each link.
func validateEach(links []*commonpb.Link, maxSize int) error {
	for _, l := range links {
		if l.Size() > maxSize {
			return serviceerror.NewInvalidArgumentf("link exceeds allowed size of %d, got %d", maxSize, l.Size())
		}
		if err := validateFields(l); err != nil {
			return err
		}
	}
	return nil
}

// validateFields confirms that Link has all the required fields.
// nolint:revive // cognitive-complexity is high but justified to keep each case together for readability.
func validateFields(l *commonpb.Link) error {
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
		if requestIDRef := t.WorkflowEvent.GetRequestIdRef(); requestIDRef != nil {
			if requestIDRef.GetRequestId() == "" {
				return serviceerror.NewInvalidArgument("workflow event request ID ref must not have an empty request ID")
			}
			if requestIDRef.GetEventType() == enumspb.EVENT_TYPE_UNSPECIFIED {
				return serviceerror.NewInvalidArgument("workflow event request ID ref must not have an unspecified event type")
			}
		}
	case *commonpb.Link_BatchJob_:
		if t.BatchJob.GetJobId() == "" {
			return serviceerror.NewInvalidArgument("batch job link must not have an empty job ID")
		}
	case *commonpb.Link_Callback_:
		if t.Callback.GetNamespace() == "" {
			return serviceerror.NewInvalidArgument("callback link must have a namespace")
		}
		if t.Callback.GetExecution() == nil {
			return serviceerror.NewInvalidArgument("callback link must have an execution")
		}
		exType := t.Callback.GetExecution().GetType()
		if exType == enumspb.EXECUTION_TYPE_UNSPECIFIED {
			return serviceerror.NewInvalidArgument("callback link execution must have a type")
		}
		if _, ok := enumspb.ExecutionType_name[int32(exType)]; !ok {
			return serviceerror.NewInvalidArgument("callback link execution type is unknown")
		}
		if t.Callback.GetExecution().GetBusinessId() == "" {
			return serviceerror.NewInvalidArgument("callback link execution must have a business ID")
		}
		if t.Callback.GetExecution().GetRunId() == "" {
			return serviceerror.NewInvalidArgument("callback link execution must have a run ID")
		}
		if t.Callback.GetRequestId() == "" {
			return serviceerror.NewInvalidArgument("callback link must have a request ID")
		}
	case *commonpb.Link_NexusOperation_:
		if t.NexusOperation.GetNamespace() == "" {
			return serviceerror.NewInvalidArgument("nexus operation link must not have an empty namespace field")
		}
		if t.NexusOperation.GetOperationId() == "" {
			return serviceerror.NewInvalidArgument("nexus operation link must not have an empty operation ID field")
		}
		if t.NexusOperation.GetRunId() == "" {
			return serviceerror.NewInvalidArgument("nexus operation link must not have an empty run ID field")
		}
	case *commonpb.Link_Activity_:
		if t.Activity.GetNamespace() == "" {
			return serviceerror.NewInvalidArgument("activity link must not have an empty namespace field")
		}
		if t.Activity.GetActivityId() == "" {
			return serviceerror.NewInvalidArgument("activity link must not have an empty activity ID field")
		}
		if t.Activity.GetRunId() == "" {
			return serviceerror.NewInvalidArgument("activity link must not have an empty run ID field")
		}
	case *commonpb.Link_Workflow_:
		if t.Workflow.GetNamespace() == "" {
			return serviceerror.NewInvalidArgument("workflow link must not have an empty namespace field")
		}
		if t.Workflow.GetWorkflowId() == "" {
			return serviceerror.NewInvalidArgument("workflow link must not have an empty workflow ID field")
		}
		if t.Workflow.GetRunId() == "" {
			return serviceerror.NewInvalidArgument("workflow link must not have an empty run ID field")
		}
	default:
		return serviceerror.NewInvalidArgument("unsupported link variant")
	}

	return nil
}
