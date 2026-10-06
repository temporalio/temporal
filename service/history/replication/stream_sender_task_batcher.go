package replication

import (
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/common/persistence/versionhistory"
)

type streamSenderTaskBatcher struct {
	enabled        bool
	pending        *convertedReplicationTask
	metricsHandler metrics.Handler
}

func newStreamSenderTaskBatcher(
	enabled bool,
	metricsHandler metrics.Handler,
	fromClusterID int32,
	toClusterID int32,
	priority enumsspb.TaskPriority,
) *streamSenderTaskBatcher {
	return &streamSenderTaskBatcher{
		enabled: enabled,
		metricsHandler: metricsHandler.WithTags(
			metrics.FromClusterIDTag(fromClusterID),
			metrics.ToClusterIDTag(toClusterID),
			metrics.ReplicationTaskPriorityTag(priority),
		),
	}
}

// Batch returns every task whose order is now fixed. It may return no tasks while retaining a
// verify task, or two tasks when a retained verify and the next task must both be sent.
func (b *streamSenderTaskBatcher) Batch(
	next convertedReplicationTask,
) (ready []convertedReplicationTask) {
	if !b.enabled {
		return []convertedReplicationTask{next}
	}

	if b.pending == nil {
		if isCoalescableVerifyTask(next.task) {
			b.pending = &next
			return nil
		}
		return []convertedReplicationTask{next}
	}

	pending := b.pending
	b.pending = nil
	if canCoalesceVerifyTasks(pending.task, next.task) {
		metrics.ReplicationTaskVerifyCoalesced.With(b.metricsHandler).Record(
			1,
			metrics.OperationTag(TaskOperationTagFromTask(pending.sourceTask.GetType())),
		)
	} else {
		ready = append(ready, *pending)
	}

	if isCoalescableVerifyTask(next.task) {
		b.pending = &next
	} else {
		ready = append(ready, next)
	}
	return ready
}

// Flush releases the final retained task before the stream sender advances its watermark.
func (b *streamSenderTaskBatcher) Flush() []convertedReplicationTask {
	if b.pending == nil {
		return nil
	}
	pending := *b.pending
	b.pending = nil
	return []convertedReplicationTask{pending}
}

func isCoalescableVerifyTask(task *replicationspb.ReplicationTask) bool {
	attr := task.GetVerifyVersionedTransitionTaskAttributes()
	return attr != nil && attr.GetNewRunId() == ""
}

func canCoalesceVerifyTasks(prev *replicationspb.ReplicationTask, next *replicationspb.ReplicationTask) bool {
	prevAttr := prev.GetVerifyVersionedTransitionTaskAttributes()
	nextAttr := next.GetVerifyVersionedTransitionTaskAttributes()
	if prevAttr == nil || nextAttr == nil || prevAttr.GetNewRunId() != "" {
		return false
	}
	if prevAttr.GetNamespaceId() != nextAttr.GetNamespaceId() ||
		prevAttr.GetWorkflowId() != nextAttr.GetWorkflowId() ||
		prevAttr.GetRunId() != nextAttr.GetRunId() ||
		prevAttr.GetArchetypeId() != nextAttr.GetArchetypeId() {
		return false
	}
	if transitionhistory.Compare(prev.GetVersionedTransition(), next.GetVersionedTransition()) > 0 {
		return false
	}
	prevItems := prevAttr.GetEventVersionHistory()
	if len(prevItems) == 0 {
		return true
	}
	return versionhistory.ContainsVersionHistoryItem(
		versionhistory.NewVersionHistory(nil, nextAttr.GetEventVersionHistory()),
		prevItems[len(prevItems)-1],
	)
}
