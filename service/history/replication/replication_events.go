package replication

import (
	"go.temporal.io/server/common/definition"
	commonevents "go.temporal.io/server/common/events"
	"go.temporal.io/server/common/namespace"
)

// emitReplicationExecuting emits a best-effort "executing" ReplicationLifecycle event when an
// executable replication task is picked up to execute on the target cluster. It never affects
// control flow: a nil handler / unresolved shard is a no-op and namespace resolution failures
// fall back to "".
//
// The event handler and shard id are taken from the target shard (resolved by namespace+workflow),
// because the replication ProcessToolBox does not carry the event handler in all wirings.
func emitReplicationExecuting(
	toolBox ProcessToolBox,
	key definition.WorkflowKey,
	taskType string,
	attempt int32,
) {
	if !toolBox.Config.EmitReplicationLifecycleEvents() {
		return
	}
	shardContext, err := toolBox.ShardController.GetShardByNamespaceWorkflow(namespace.ID(key.NamespaceID), key.WorkflowID)
	if err != nil {
		return
	}
	handler := shardContext.GetEventHandler()
	if handler == nil {
		return
	}

	var nsName string
	if name, err := toolBox.NamespaceCache.GetNamespaceName(namespace.ID(key.NamespaceID)); err == nil {
		nsName = name.String()
	}

	commonevents.EmitReplicationLifecycle(handler, commonevents.ReplicationLifecyclePayload{
		Phase:       commonevents.ReplicationExecuting,
		TaskType:    taskType,
		Shard:       shardContext.GetShardID(),
		Namespace:   nsName,
		NamespaceID: key.NamespaceID,
		WorkflowID:  key.WorkflowID,
		RunID:       key.RunID,
		Attempt:     attempt,
	})
}
