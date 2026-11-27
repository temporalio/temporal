//go:build !test_dep

package matching

import (
	"context"
	"testing"

	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testhooks"
)

// The original hook key was removed when matching switched to the failpoint registry.
var benchmarkDisableSyncMatchHook testhooks.Key[bool, namespace.ID]

// BenchmarkSkipSyncMatchTestHooks measures the previous testhooks stub, which only exists without
// test_dep. Under test_dep, testhooks.Get is a sync.Map lookup and would not be comparable. The
// stub returns constants and is inlined, so this row is expected to cost about as much as the
// empty-loop NoCheck row of BenchmarkSkipSyncMatch.
func BenchmarkSkipSyncMatchTestHooks(b *testing.B) {
	ns := benchmarkSyncMatchNamespace()
	eng := &matchingEngineImpl{}
	for b.Loop() {
		_, _ = testhooks.Get(eng.testHooks, benchmarkDisableSyncMatchHook, ns.ID())
	}
}

// BenchmarkTrySyncMatchTestHooks measures TrySyncMatch as it was before the registry switch.
func BenchmarkTrySyncMatchTestHooks(b *testing.B) {
	benchmarkTrySyncMatch(b, nil, (*physicalTaskQueueManagerImpl).benchmarkTrySyncMatchWithTestHooks)
}

// benchmarkTrySyncMatchWithTestHooks mirrors TrySyncMatch before the registry switch.
func (c *physicalTaskQueueManagerImpl) benchmarkTrySyncMatchWithTestHooks(ctx context.Context, task *internalTask) (syncMatchOutcome, error) {
	if !task.isForwarded() {
		c.liveness.markAlive()
		c.incTaskTracker(c.tasksAdded, priorityKey(task.getPriority().GetPriorityKey()), 1)
		if disable, _ := testhooks.Get(c.partitionMgr.engine.testHooks, benchmarkDisableSyncMatchHook, c.partitionMgr.ns.ID()); disable {
			return syncMatchNoPoller, nil
		}
	}

	return c.matcher.Offer(ctx, task)
}
