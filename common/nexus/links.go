package nexus

import (
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/temporalnexus"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// ConvertNexusLinksToProtoLinks converts a slice of Nexus SDK links into Temporal proto links,
// supporting Link_Workflow, Link_WorkflowEvent, Link_Activity, and other variants. Unsupported
// or malformed entries are skipped with a warning since links are non-essential to execution.
func ConvertNexusLinksToProtoLinks(nexusLinks []nexus.Link, logger log.Logger) []*commonpb.Link {
	logLinkConversionError := func(l nexus.Link, err error) {
		logger.Warn(
			"failed to parse Nexus link",
			tag.Error(err),
			tag.NewStringTag("nexus-link-type", l.Type),
			tag.URL(l.URL.String()),
		)
	}

	var out []*commonpb.Link
	for _, nexusLink := range nexusLinks {
		if nexusLink.URL == nil {
			logger.Warn("Nexus link has no URL", tag.NewStringTag("nexus-link-type", nexusLink.Type))
			continue
		}
		switch nexusLink.Type {
		case string((&commonpb.Link_WorkflowEvent{}).ProtoReflect().Descriptor().FullName()):
			link, err := ConvertNexusLinkToLinkWorkflowEvent(nexusLink)
			if err != nil {
				logLinkConversionError(nexusLink, err)
				continue
			}
			out = append(out, &commonpb.Link{
				Variant: &commonpb.Link_WorkflowEvent_{WorkflowEvent: link},
			})
		case string((&commonpb.Link_Activity{}).ProtoReflect().Descriptor().FullName()):
			link, err := ConvertNexusLinkToLinkActivity(nexusLink)
			if err != nil {
				logLinkConversionError(nexusLink, err)
				continue
			}
			out = append(out, &commonpb.Link{
				Variant: &commonpb.Link_Activity_{Activity: link},
			})
		case string((&commonpb.Link_Callback{}).ProtoReflect().Descriptor().FullName()):
			link, err := ConvertNexusLinkToLinkCallback(nexusLink)
			if err != nil {
				logLinkConversionError(nexusLink, err)
				continue
			}
			out = append(out, &commonpb.Link{
				Variant: &commonpb.Link_Callback_{Callback: link},
			})
		case string((&commonpb.Link_Workflow{}).ProtoReflect().Descriptor().FullName()):
			link, err := temporalnexus.ConvertNexusLinkToLinkWorkflow(nexusLink)
			if err != nil {
				logLinkConversionError(nexusLink, err)
				continue
			}
			out = append(out, &commonpb.Link{
				Variant: &commonpb.Link_Workflow_{Workflow: link},
			})
		case string((&commonpb.Link_NexusOperation{}).ProtoReflect().Descriptor().FullName()):
			link, err := temporalnexus.ConvertNexusLinkToLinkNexusOperation(nexusLink)
			if err != nil {
				logger.Warn(
					"failed to parse link",
					tag.NewStringTag("nexus-link-type", nexusLink.Type),
					tag.URL(nexusLink.URL.String()),
					tag.Error(err),
				)
				continue
			}
			out = append(out, &commonpb.Link{
				Variant: &commonpb.Link_NexusOperation_{NexusOperation: link},
			})
		default:
			logger.Warn("invalid Nexus link data type",
				tag.NewStringTag("nexus-link-type", nexusLink.Type),
			)
		}
	}
	return out
}
