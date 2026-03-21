package temporalfs

import (
	"context"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/service/history/deletemanager"
)

// tfsPostDeleteHook implements deletemanager.PostDeleteHook.
// In OSS this is a no-op logger (the pull path handles cleanup).
// SaaS overrides via fx.Decorate with a real implementation that
// queries visibility for TFS executions and calls DetachWorkflow.
type tfsPostDeleteHook struct {
	logger log.Logger
}

var _ deletemanager.PostDeleteHook = (*tfsPostDeleteHook)(nil)

func newTFSPostDeleteHook(logger log.Logger) *tfsPostDeleteHook {
	return &tfsPostDeleteHook{logger: logger}
}

func (h *tfsPostDeleteHook) AfterWorkflowDeletion(ctx context.Context, namespaceID string, workflowID string) {
	// OSS: log and rely on the OwnerCheckTask (pull path) for cleanup.
	// SaaS can override this to query visibility for TFS executions
	// owned by this workflow and call DetachWorkflow for each.
	h.logger.Debug("TFS: workflow deleted, pull path will handle TFS cleanup",
		tag.WorkflowNamespaceID(namespaceID),
		tag.WorkflowID(workflowID),
	)
}
