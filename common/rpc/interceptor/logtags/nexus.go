package logtags

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/log/tag"
)

// NexusStage returns the lifecycle phase when the request identifies one unambiguously.
func NexusStage(req any, fullMethod string) []tag.Tag {
	switch fullMethod {
	case api.WorkflowServicePrefix + "PollNexusTaskQueue",
		api.WorkflowServicePrefix + "RespondNexusTaskCompleted",
		api.WorkflowServicePrefix + "RespondNexusTaskFailed",
		api.MatchingServicePrefix + "DispatchNexusTask",
		api.MatchingServicePrefix + "PollNexusTaskQueue",
		api.MatchingServicePrefix + "RespondNexusTaskCompleted",
		api.MatchingServicePrefix + "RespondNexusTaskFailed":
		return []tag.Tag{tag.NexusStageHandlerTaskDelivery}
	case api.MatchingServicePrefix + "CancelOutstandingPoll":
		if request, ok := req.(*matchingservice.CancelOutstandingPollRequest); ok && request.GetTaskQueueType() == enumspb.TASK_QUEUE_TYPE_NEXUS {
			return []tag.Tag{tag.NexusStageHandlerTaskDelivery}
		}
	case api.HistoryServicePrefix + "StartNexusOperation",
		api.HistoryServicePrefix + "CancelNexusOperation",
		api.NexusServicePrefix + "DispatchByEndpoint",
		api.NexusServicePrefix + "DispatchByNamespaceAndTaskQueue":
		return []tag.Tag{tag.NexusStageHandlerInbound}
	default:
	}
	return nil
}
