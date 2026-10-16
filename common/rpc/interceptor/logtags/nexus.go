package logtags

import (
	"strings"

	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/log/tag"
)

// NexusStage returns the lifecycle phase when the request identifies one unambiguously.
func NexusStage(_ any, fullMethod string) []tag.Tag {
	switch fullMethod {
	case api.WorkflowServicePrefix + "StartNexusOperationExecution",
		api.WorkflowServicePrefix + "DescribeNexusOperationExecution",
		api.WorkflowServicePrefix + "PollNexusOperationExecution",
		api.WorkflowServicePrefix + "RequestCancelNexusOperationExecution",
		api.WorkflowServicePrefix + "TerminateNexusOperationExecution",
		api.WorkflowServicePrefix + "DeleteNexusOperationExecution",
		api.WorkflowServicePrefix + "ListNexusOperationExecutions",
		api.WorkflowServicePrefix + "CountNexusOperationExecutions":
		return []tag.Tag{tag.NexusStageCallerFrontend}
	default:
		if strings.HasPrefix(fullMethod, "/temporal.server.chasm.lib.nexusoperation.proto.v1.NexusOperationService/") {
			return []tag.Tag{tag.NexusStageCallerFrontend}
		}
	}
	return nil
}
