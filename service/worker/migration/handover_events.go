package migration

import (
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/wideevents"
)

// phaseShardHandoverReadiness is the NamespaceLifecycle phase for one shard's handover-readiness
// transition. A handover cannot complete until every shard's watermark has been acked by the
// target cluster; the existing aggregate log only reports counts and the single worst shard, so
// a handover that stalls on a few specific shards is not attributable from it. These events name
// the shard.
const phaseShardHandoverReadiness = "shard_handover_readiness"

// emitShardHandoverReadiness records a shard entering or leaving the not-ready state.
//
// notReadyShards holds the previous poll's readiness, so a shard that stays not-ready emits once
// rather than once per poll: checkHandoverOnce runs on a 1s loop, and emitting every not-ready
// shard on every poll would be thousands of events a minute on a large cluster — worst exactly
// when the handover is stuck and the events matter. What survives the suppression is the useful
// part: which shards went not-ready, and when each cleared.
//
// A not-ready shard with lagging_tasks == 0 is the missing-handover-info case (the namespace
// cache on that shard has not picked up the handover yet); any other not-ready shard is behind
// by lagging_tasks.
func (a *activities) emitShardHandoverReadiness(
	waitRequest waitHandoverRequest,
	status shardStatus,
	notReadyShards map[int32]bool,
) {
	notReadyNow := !status.isReady
	if notReadyNow == notReadyShards[status.shardID] {
		return
	}
	if notReadyNow {
		notReadyShards[status.shardID] = true
	} else {
		delete(notReadyShards, status.shardID)
	}

	wideevents.Emit(a.EventLogger, wideevents.NamespaceLifecyclePayload{
		Phase:       phaseShardHandoverReadiness,
		Namespace:   waitRequest.Namespace,
		NamespaceID: a.namespaceIDForEvent(waitRequest.Namespace),
		Details: map[string]any{
			"shard_id":       status.shardID,
			"ready":          status.isReady,
			"lagging_tasks":  status.laggingTasks,
			"remote_cluster": waitRequest.RemoteCluster,
		},
	})
}

// namespaceIDForEvent resolves the namespace ID so events can be joined on it rather than on the
// name. Only called on a readiness transition, so the registry lookup is off the poll path.
func (a *activities) namespaceIDForEvent(name string) string {
	ns, err := a.NamespaceRegistry.GetNamespace(namespace.Name(name))
	if err != nil {
		return ""
	}
	return ns.ID().String()
}
