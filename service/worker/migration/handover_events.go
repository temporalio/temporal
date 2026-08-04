package migration

import (
	"time"

	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/wideevents"
)

// emitHandoverLagSummary reports the shards still behind when the wait ended. The phase and its
// payload are defined in common/wideevents alongside the rest of the handover events.
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
	wideevents.Emit(a.EventLogger, wideevents.HandoverIncomplete(wideevents.HandoverIncompleteParams{
		Namespace:                waitRequest.Namespace,
		NamespaceID:              a.namespaceIDForEvent(waitRequest.Namespace),
		RemoteCluster:            waitRequest.RemoteCluster,
		TotalShards:              snapshot.totalShards,
		ReadyCount:               snapshot.readyCount,
		NotReadyCount:            snapshot.notReadyCount,
		MissingHandoverInfoCount: snapshot.missingHandoverInfoCount,
		MaxLaggingTasks:          snapshot.maxLaggingTasks,
		MaxLaggingTasksShardID:   snapshot.maxLaggingTasksShardID,
		LaggingShards:            snapshot.laggingShards,
		Elapsed:                  elapsed,
		ExitErr:                  exitErr,
	}))
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
