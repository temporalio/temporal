package nexus

import (
	"fmt"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// ConvertNexusLinksToProtoLinks converts a slice of Nexus SDK links into Temporal proto links,
// supporting Link_WorkflowEvent and Link_Activity variants. Unsupported or malformed entries
// are skipped with a warning since links are non-essential to execution.
func ConvertNexusLinksToProtoLinks(nexusLinks []nexus.Link, logger log.Logger) []*commonpb.Link {
	var out []*commonpb.Link
	for _, nexusLink := range nexusLinks {
		switch nexusLink.Type {
		case string((&commonpb.Link_WorkflowEvent{}).ProtoReflect().Descriptor().FullName()):
			link, err := ConvertNexusLinkToLinkWorkflowEvent(nexusLink)
			if err != nil {
				logger.Warn(
					fmt.Sprintf("failed to parse link to %q: %s", nexusLink.Type, nexusLink.URL),
					tag.Error(err),
				)
				continue
			}
			out = append(out, &commonpb.Link{
				Variant: &commonpb.Link_WorkflowEvent_{WorkflowEvent: link},
			})
		case string((&commonpb.Link_Activity{}).ProtoReflect().Descriptor().FullName()):
			link, err := ConvertNexusLinkToLinkActivity(nexusLink)
			if err != nil {
				logger.Warn(
					fmt.Sprintf("failed to parse link to %q: %s", nexusLink.Type, nexusLink.URL),
					tag.Error(err),
				)
				continue
			}
			out = append(out, &commonpb.Link{
				Variant: &commonpb.Link_Activity_{Activity: link},
			})
		default:
			logger.Warn(fmt.Sprintf("invalid link data type: %q", nexusLink.Type))
		}
	}
	return out
}
