// Generates all generated files in this package:
//go:generate go run ../../../../cmd/tools/genrpcserverinterceptors

package logtags

import (
	"strings"

	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/tasktoken"
)

type (
	WorkflowTags struct {
		serializer *tasktoken.Serializer
		logger     log.Logger
	}
)

func NewWorkflowTags(
	serializer *tasktoken.Serializer,
	logger log.Logger,
) *WorkflowTags {
	return &WorkflowTags{
		serializer: serializer,
		logger:     logger,
	}
}

// Extract returns the workflow tags of the request and, for Nexus RPCs, their Nexus lifecycle stage.
func (wt *WorkflowTags) Extract(req any, fullMethod string) []tag.Tag {
	return append(wt.extractWorkflowTags(req, fullMethod), NexusStage(req, fullMethod)...)
}

func (wt *WorkflowTags) extractWorkflowTags(req any, fullMethod string) []tag.Tag {
	if req == nil {
		return nil
	}
	switch {
	case strings.HasPrefix(fullMethod, api.WorkflowServicePrefix):
		return wt.extractFromWorkflowServiceServerMessage(req)
	case strings.HasPrefix(fullMethod, api.OperatorServicePrefix):
		// OperatorService doesn't have a single API with workflow tags.
		return nil
	case strings.HasPrefix(fullMethod, api.AdminServicePrefix):
		return wt.extractFromAdminServiceServerMessage(req)
	case strings.HasPrefix(fullMethod, api.HistoryServicePrefix):
		return wt.extractFromHistoryServiceServerMessage(req)
	case strings.HasPrefix(fullMethod, api.MatchingServicePrefix):
		return wt.extractFromMatchingServiceServerMessage(req)
	default:
		return nil
	}
}

func (wt *WorkflowTags) fromTaskToken(taskTokenBytes []byte) []tag.Tag {
	if len(taskTokenBytes) == 0 {
		return nil
	}
	taskToken, err := wt.serializer.Deserialize(taskTokenBytes)
	if err != nil {
		wt.logger.Warn("unable to deserialize task token while getting workflow tags", tag.Error(err))
		return nil
	}
	return []tag.Tag{tag.WorkflowID(taskToken.WorkflowId), tag.WorkflowRunID(taskToken.RunId)}
}

// NexusStage returns the lifecycle phase when the request identifies one unambiguously.
func NexusStage(req any, fullMethod string) []tag.Tag {
	switch fullMethod {
	case api.HistoryServicePrefix + "StartNexusOperation",
		api.HistoryServicePrefix + "CancelNexusOperation",
		api.NexusServicePrefix + "DispatchByEndpoint",
		api.NexusServicePrefix + "DispatchByNamespaceAndTaskQueue":
		return []tag.Tag{tag.NexusStageHandlerInbound}
	default:
	}
	return nil
}
