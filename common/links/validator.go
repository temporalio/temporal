// Package links provides validation helpers for temporal.api.common.v1.Link values.
package links

import (
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/dynamicconfig"
)

// ValidateWithCallbacks validates all links attached directly to a request or embedded in its callbacks.
func ValidateWithCallbacks(
	links []*commonpb.Link,
	callbacks []*commonpb.Callback,
	maxAllowedLinks int,
	maxSize int,
) error {
	allLinks := make([]*commonpb.Link, 0, len(links)+len(callbacks))
	allLinks = append(allLinks, links...)
	for _, callback := range callbacks {
		allLinks = append(allLinks, callback.GetLinks()...)
	}
	return Validate(allLinks, maxAllowedLinks, maxSize)
}

// Validate checks that the given links do not exceed the configured count and
// per-link size limits, and that each link's variant has its required fields
// populated.
func Validate(links []*commonpb.Link, maxAllowedLinks, maxSize int) error {
	if len(links) > maxAllowedLinks {
		return serviceerror.NewInvalidArgumentf("cannot attach more than %d links per request, got %d", maxAllowedLinks, len(links))
	}
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
	case *commonpb.Link_BatchJob_:
		if t.BatchJob.GetJobId() == "" {
			return serviceerror.NewInvalidArgument("batch job link must not have an empty job ID")
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

// Validator validates links attached to executions. It enforces both
// per-request limits (count + size + variant shape) and a cumulative cap.
type Validator struct {
	// resourceName describes the resource the link is attached to in an error message, as a noun
	// phrase including its article. e.g. "an activity", "a nexus operation".
	resourceName string

	maxLinksPerRequest  dynamicconfig.IntPropertyFnWithNamespaceFilter
	maxLinksPerResource dynamicconfig.IntPropertyFnWithNamespaceFilter
	linkMaxSize         dynamicconfig.IntPropertyFnWithNamespaceFilter
}

func NewValidator(
	resourceName string,
	maxLinksPerRequest dynamicconfig.IntPropertyFnWithNamespaceFilter,
	maxLinksPerResource dynamicconfig.IntPropertyFnWithNamespaceFilter,
	linkMaxSize dynamicconfig.IntPropertyFnWithNamespaceFilter,
) *Validator {
	return &Validator{
		resourceName:        resourceName,
		maxLinksPerRequest:  maxLinksPerRequest,
		maxLinksPerResource: maxLinksPerResource,
		linkMaxSize:         linkMaxSize,
	}
}

// ValidateRequest checks count, per-link size, and variant shape for the links
// on a single incoming request.
func (v *Validator) ValidateRequest(namespaceName string, links []*commonpb.Link) error {
	return Validate(links, v.maxLinksPerRequest(namespaceName), v.linkMaxSize(namespaceName))
}

// ValidateRequestWithCallbacks also counts and validates links embedded in completion callbacks.
func (v *Validator) ValidateRequestWithCallbacks(
	namespaceName string,
	links []*commonpb.Link,
	callbacks []*commonpb.Callback,
) error {
	return ValidateWithCallbacks(
		links,
		callbacks,
		v.maxLinksPerRequest(namespaceName),
		v.linkMaxSize(namespaceName),
	)
}

// ValidateTotal checks that adding addingCount links to a resource will not exceed the limit.
func (v *Validator) ValidateTotal(namespaceName string, existingCount, addingCount int) error {
	maxLinks := v.maxLinksPerResource(namespaceName)
	if existingCount+addingCount > maxLinks {
		return serviceerror.NewFailedPreconditionf(
			"cannot attach more than %d links to %s (%d links already attached)",
			maxLinks,
			v.resourceName,
			existingCount,
		)
	}
	return nil
}
