package nexus

import (
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/temporalnexus"
	commonlinks "go.temporal.io/server/common/links"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// ConvertAndSanitizeHandlerLinks converts links returned by a remote Nexus handler or worker into proto
// links, then filters them with [commonlinks.Validator.Sanitize]. Anything dropped is logged; links are
// non-essential to execution, so they never fail the response that carried them.
func ConvertAndSanitizeHandlerLinks(
	nexusLinks []nexus.Link,
	namespaceName string,
	validator commonlinks.Validator,
	logger log.Logger,
) []*commonpb.Link {
	return SanitizeHandlerLinks(ConvertNexusLinksToProtoLinks(nexusLinks, logger), namespaceName, validator, logger)
}

// SanitizeHandlerLinks is [ConvertAndSanitizeHandlerLinks] for links that are already in proto form.
func SanitizeHandlerLinks(
	links []*commonpb.Link,
	namespaceName string,
	validator commonlinks.Validator,
	logger log.Logger,
) []*commonpb.Link {
	if len(links) == 0 {
		return links
	}
	kept, err := validator.Sanitize(namespaceName, links)
	if err != nil {
		logger.Warn(
			"dropped links returned by Nexus handler",
			tag.WorkflowNamespace(namespaceName),
			tag.NewInt("dropped-count", len(links)-len(kept)),
			tag.Error(err),
		)
	}
	return kept
}

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
