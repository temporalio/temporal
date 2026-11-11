package replication

import (
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/wideevents"
)

type PayloadPhase string

const (
	PayloadReceived PayloadPhase = "received"
	PayloadAcked    PayloadPhase = "acked"
)

type PayloadObservation struct {
	NamespaceID string
	Origin      wideevents.ReplicationTaskOrigin
	Source      ClusterShardKey
	Target      ClusterShardKey
	Phase       PayloadPhase
	At          time.Time
}

// PayloadObserver accounts for tracked HIGH payloads independently of event emission.
// Calls may be concurrent and must return from memory without blocking or I/O.
// Received excludes tracker duplicates; Acked means execution succeeded, not that
// an ACK has reached the sender. Cancellation and failure do not count as success.
type PayloadObserver interface {
	ObservePayload(PayloadObservation)
}

type routedPayloadObserver struct {
	observer PayloadObserver
	source   ClusterShardKey
	target   ClusterShardKey
}

func payloadObserverForRoute(observer PayloadObserver, source, target ClusterShardKey) PayloadObserver {
	if observer == nil {
		return nil
	}
	return routedPayloadObserver{observer: observer, source: source, target: target}
}

func (o routedPayloadObserver) ObservePayload(value PayloadObservation) {
	value.Source, value.Target = o.source, o.target
	o.observer.ObservePayload(value)
}

func observePayload(
	observer PayloadObserver,
	task *replicationspb.ReplicationTask,
	cluster string,
	shard int32,
	phase PayloadPhase,
	at time.Time,
) {
	if observer == nil || task.GetPriority() != enumsspb.TASK_PRIORITY_HIGH {
		return
	}
	executable := ExecutableTaskImpl{replicationTask: task}
	namespaceID, _, _ := executable.workflowKeyFromTask()
	if namespaceID == "" {
		return
	}
	observer.ObservePayload(PayloadObservation{
		NamespaceID: namespaceID,
		Origin:      wideevents.ReplicationTaskOrigin{ClusterName: cluster, ShardID: shard, TaskID: task.GetSourceTaskId()},
		Phase:       phase,
		At:          at,
	})
}
