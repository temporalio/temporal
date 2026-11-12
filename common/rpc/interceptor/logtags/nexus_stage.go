package logtags

import (
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/log/tag"
)

// NexusStage returns the lifecycle phase when the request identifies one unambiguously.
func NexusStage(req any, fullMethod string) []tag.Tag {
	switch fullMethod {
	case api.HistoryServicePrefix + "CompleteNexusOperation",
		api.HistoryServicePrefix + "CompleteNexusOperationChasm",
		api.NexusServicePrefix + "CompleteNexusOperation":
		return []tag.Tag{tag.NexusStageCallerInbound}
	case api.HistoryServicePrefix + "StartNexusOperation",
		api.HistoryServicePrefix + "CancelNexusOperation",
		api.NexusServicePrefix + "DispatchByEndpoint",
		api.NexusServicePrefix + "DispatchByNamespaceAndTaskQueue":
		return []tag.Tag{tag.NexusStageHandlerInbound}
	default:
	}
	return nil
}
