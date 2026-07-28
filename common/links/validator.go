// Package links provides validation helpers for temporal.api.common.v1.Link values.
package links

import (
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
)

// Validate checks that the given links do not exceed the configured count and
// per-link size limits, and that each link's variant has its required fields
// populated.
// nolint:revive // cognitive-complexity is high but justified to keep each case together for readability.
func Validate(links []*commonpb.Link, maxAllowedLinks, maxSize int) error {
	if len(links) > maxAllowedLinks {
		return serviceerror.NewInvalidArgumentf("cannot attach more than %d links per request, got %d", maxAllowedLinks, len(links))
	}
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
		default:
			return serviceerror.NewInvalidArgument("unsupported link variant")
		}
	}
	return nil
}
