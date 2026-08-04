package migration

import (
	"context"
	"errors"
	"time"

	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/wideevents"
)

// phaseHandoverIncomplete is the NamespaceLifecycle phase for a handover wait that ended with
// shards still behind. A handover cannot complete until every shard's watermark has been acked by
// the target cluster; the existing "Wait handover not ready" log reports counts and the single
// worst shard, which is not enough to name the shards actually holding it up.
//
// One event per WaitHandover call, emitted on the way out. A handover that completes leaves no
// laggards and emits nothing, so the happy path is silent.
const phaseHandoverIncomplete = "shard_handover_incomplete"

// maxLaggingShardsInSummary caps the per-shard list so a cluster with thousands of shards cannot
// produce an unbounded details blob. The true count is always in not_ready_count.
const maxLaggingShardsInSummary = 64

type laggingShard struct {
	ShardID      int32 `json:"shard_id"`
	LaggingTasks int64 `json:"lagging_tasks"`
}

// handoverLagSnapshot is the most recent poll's view of which shards are behind. checkHandoverOnce
// overwrites it on every poll; WaitHandover emits whatever is left in it when the activity unwinds.
type handoverLagSnapshot struct {
	totalShards int
	readyCount  int
	// notReadyCount is the true count; laggingShards may be truncated.
	notReadyCount int
	// missingHandoverInfoCount is the subset of not-ready shards whose namespace cache has not
	// picked up the handover yet, so they carry no watermark rather than a lagging one.
	missingHandoverInfoCount int
	maxLaggingTasks          int64
	maxLaggingTasksShardID   int32
	laggingShards            []laggingShard
}

// emitHandoverLagSummary reports the shards still behind when the wait ended.
//
// WaitHandover never returns on its own while shards are lagging: StartToClose is capped at
// maximumHandoverTimeoutSeconds with no retry, so the activity is killed from the outside. The
// kill reaches this code because the SDK cancels the activity context off the heartbeat, the next
// GetReplicationStatus fails, and WaitHandover returns — which runs the defer that calls this.
func (a *activities) emitHandoverLagSummary(
	waitRequest waitHandoverRequest,
	snapshot *handoverLagSnapshot,
	elapsed time.Duration,
	exitErr error,
) {
	if snapshot.notReadyCount == 0 {
		return
	}

	details := map[string]any{
		"remote_cluster":              waitRequest.RemoteCluster,
		"total_shards":                snapshot.totalShards,
		"ready_count":                 snapshot.readyCount,
		"not_ready_count":             snapshot.notReadyCount,
		"missing_handover_info_count": snapshot.missingHandoverInfoCount,
		"max_lagging_tasks":           snapshot.maxLaggingTasks,
		"max_lagging_tasks_shard_id":  snapshot.maxLaggingTasksShardID,
		"lagging_shards":              snapshot.laggingShards,
		"lagging_shards_truncated":    snapshot.notReadyCount > len(snapshot.laggingShards),
		"elapsed_seconds":             elapsed.Seconds(),
		"exit_reason":                 handoverExitReason(exitErr),
	}
	if exitErr != nil {
		details["exit_error"] = exitErr.Error()
	}

	wideevents.Emit(a.EventLogger, wideevents.NamespaceLifecyclePayload{
		Phase:       phaseHandoverIncomplete,
		Namespace:   waitRequest.Namespace,
		NamespaceID: a.namespaceIDForEvent(waitRequest.Namespace),
		Details:     details,
	})
}

// handoverExitReason separates the activity being killed while still waiting from a failed
// GetReplicationStatus, since the two point at different problems.
func handoverExitReason(err error) string {
	switch {
	case err == nil:
		return "returned_ready"
	case errors.Is(err, context.Canceled):
		return "context_canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	default:
		return "error"
	}
}

// namespaceIDForEvent resolves the namespace ID so events can be joined on it rather than on the
// name. Called once per activity, off the poll path.
func (a *activities) namespaceIDForEvent(name string) string {
	ns, err := a.NamespaceRegistry.GetNamespace(namespace.Name(name))
	if err != nil {
		return ""
	}
	return ns.ID().String()
}
