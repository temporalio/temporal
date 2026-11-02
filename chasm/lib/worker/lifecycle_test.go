package worker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	workerpb "go.temporal.io/api/worker/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/protorequire"
)

// TestWorkerGroupLifecycle verifies that membership and the lease survive a reload,
// stale expiry work is ignored, and detached workers remain accessible after expiry.
func TestWorkerGroupLifecycle(t *testing.T) {
	t.Parallel()
	env := newLifecycleTestEnvironment(t)
	root := env.root

	// Start two workers with a shared lease.
	ctx := chasm.NewMutableContext(t.Context(), root)
	heartbeats := []*workerpb.WorkerHeartbeat{
		workerHeartbeat("worker-1", "group-1"),
		workerHeartbeat("worker-2", "group-1"),
	}
	group, err := newWorkerGroup(ctx, "group-1", heartbeats, time.Minute)
	require.NoError(t, err)
	require.NoError(t, root.SetRootComponent(group))
	_, err = root.CloseTransaction(t.Context(), true)
	require.NoError(t, err)
	oldDeadline := group.GetLeaseExpirationTime().AsTime()
	initialNodes := root.Snapshot(nil).Nodes

	// Renew the lease and add a worker without rewriting existing worker components.
	env.timeSource.Advance(30 * time.Second)
	ctx = chasm.NewMutableContext(t.Context(), root)
	component, err := root.Component(ctx, chasm.ComponentRef{})
	require.NoError(t, err)
	group = component.(*WorkerGroup)
	heartbeats = append(heartbeats, workerHeartbeat("worker-3", "group-1"))
	require.NoError(t, group.recordHeartbeat(ctx, heartbeats, time.Minute))
	mutation, err := root.CloseTransaction(t.Context(), true)
	require.NoError(t, err)
	snapshot := root.Snapshot(nil)
	workerTypeID := chasm.GenerateTypeID(chasm.FullyQualifiedName(libraryName, workerComponentName))
	for path, node := range initialNodes {
		if node.GetMetadata().GetComponentAttributes().GetTypeId() == workerTypeID {
			require.NotContains(t, mutation.UpdatedNodes, path, "existing workers should not be rewritten")
		}
	}
	logicalTasks := snapshot.Nodes[""].GetMetadata().GetComponentAttributes().GetPureTasks()
	require.Len(t, logicalTasks, 1, "CHASM should remove the obsolete logical expiry task")
	require.Equal(t, group.GetLeaseExpirationTime().AsTime(), logicalTasks[0].GetScheduledTime().AsTime())

	// Reload the persisted execution and deliver stale expiry work.
	reloaded := env.reloadTree(t, snapshot.Nodes)
	component, err = reloaded.Component(chasm.NewContext(t.Context(), reloaded), chasm.ComponentRef{})
	require.NoError(t, err)
	protorequire.ProtoEqual(t, group.WorkerGroupState, component.(*WorkerGroup).WorkerGroupState)
	group = component.(*WorkerGroup)
	require.Len(t, group.Workers, 3)
	readCtx := chasm.NewContext(t.Context(), reloaded)
	worker := group.Workers["worker-1"].Get(readCtx)
	serializedRef, err := readCtx.Ref(worker)
	require.NoError(t, err)
	workerRef, err := chasm.DeserializeComponentRef(serializedRef)
	require.NoError(t, err)
	env.timeSource.Update(oldDeadline)
	executed, err := reloaded.ExecutePureTask(t.Context(), chasm.TaskAttributes{ScheduledTime: oldDeadline}, &workerstatepb.LeaseExpiryTask{})
	require.NoError(t, err)
	require.False(t, executed)

	// The current deadline closes the run and removes its remaining expiry task.
	deadline := group.GetLeaseExpirationTime().AsTime()
	env.timeSource.Update(deadline)
	executed, err = reloaded.ExecutePureTask(t.Context(), chasm.TaskAttributes{ScheduledTime: deadline}, &workerstatepb.LeaseExpiryTask{})
	require.NoError(t, err)
	require.True(t, executed)
	_, err = reloaded.CloseTransaction(t.Context(), true)
	require.NoError(t, err)
	component, err = reloaded.Component(chasm.NewContext(t.Context(), reloaded), chasm.NewComponentRef[*WorkerGroup](chasm.ExecutionKey{}))
	require.NoError(t, err)
	require.Equal(t, chasm.LifecycleStateCompleted, component.LifecycleState(chasm.NewContext(t.Context(), reloaded)))
	closedSnapshot := reloaded.Snapshot(nil)
	require.Empty(t, closedSnapshot.Nodes[""].GetMetadata().GetComponentAttributes().GetPureTasks())

	// Reload the closed group and access a worker with progress intent. This would
	// fail for an ordinary child, even though read-only access would still work.
	closedTree := env.reloadTree(t, closedSnapshot.Nodes)
	progressCtx := chasm.NewMutableContext(chasm.NewContextWithOperationIntent(t.Context(), chasm.OperationIntentProgress), closedTree)
	component, err = closedTree.Component(progressCtx, workerRef)
	require.NoError(t, err)
	require.IsType(t, &Worker{}, component)
	require.Equal(t, chasm.LifecycleStateRunning, component.LifecycleState(progressCtx))
}

// TestWorkerGroupOfflineRead verifies that persisted worker state and expiry task
// types can be decoded without runtime handlers.
func TestWorkerGroupOfflineRead(t *testing.T) {
	t.Parallel()
	env := newLifecycleTestEnvironment(t)
	ctx := chasm.NewMutableContext(t.Context(), env.root)
	heartbeats := []*workerpb.WorkerHeartbeat{
		workerHeartbeat("worker-1", "group-1"),
		workerHeartbeat("worker-2", "group-1"),
	}
	group, err := newWorkerGroup(ctx, "group-1", heartbeats, time.Minute)
	require.NoError(t, err)
	require.NoError(t, env.root.SetRootComponent(group))
	_, err = env.root.CloseTransaction(t.Context(), true)
	require.NoError(t, err)
	snapshot := env.root.Snapshot(nil)

	// Decode with registration-only libraries, as an offline reader would.
	nilRegistry := chasm.NewRegistry(log.NewTestLogger())
	require.NoError(t, nilRegistry.Register(NewNilLibrary()))
	decoded, readCtx, err := chasm.NewDetachedExecution[*WorkerGroup](t.Context(), env.executionRecord(snapshot.Nodes), nilRegistry)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, group.WorkerGroupState, decoded.WorkerGroupState)
	require.Len(t, decoded.Workers, len(heartbeats))
	for _, heartbeat := range heartbeats {
		worker := decoded.Workers[heartbeat.GetWorkerInstanceKey()].Get(readCtx)
		protorequire.ProtoEqual(t, &workerstatepb.WorkerState{}, worker.WorkerState)
	}
	logicalTasks := snapshot.Nodes[""].GetMetadata().GetComponentAttributes().GetPureTasks()
	require.Len(t, logicalTasks, 1)
	_, ok := nilRegistry.TaskByID(logicalTasks[0].GetTypeId())
	require.True(t, ok, "offline readers must retain expiry task registration")
}

type lifecycleTestEnvironment struct {
	root       *chasm.Node
	registry   *chasm.Registry
	backend    *chasm.MockNodeBackend
	timeSource *clock.EventTimeSource
}

func newLifecycleTestEnvironment(t *testing.T) *lifecycleTestEnvironment {
	t.Helper()
	logger := log.NewTestLogger()
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(newLibrary(nil)))
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Unix(1000, 0).UTC())
	backend := &chasm.MockNodeBackend{
		HandleGetWorkflowKey: func() definition.WorkflowKey {
			return definition.NewWorkflowKey("namespace-1", "group-1", "run-1")
		},
		HandleGetExecutionInfo: func() *persistencespb.WorkflowExecutionInfo {
			return &persistencespb.WorkflowExecutionInfo{}
		},
		HandleNextTransitionCount: func() int64 { return 1 },
		HandleGetCurrentVersion:   func() int64 { return 1 },
		HandleNow:                 timeSource.Now,
	}
	root := chasm.NewEmptyTree(registry, backend, chasm.DefaultPathEncoder, logger, metrics.NoopMetricsHandler)
	return &lifecycleTestEnvironment{
		root:       root,
		registry:   registry,
		backend:    backend,
		timeSource: timeSource,
	}
}

func (e *lifecycleTestEnvironment) reloadTree(t *testing.T, nodes map[string]*persistencespb.ChasmNode) *chasm.Node {
	t.Helper()
	root, err := chasm.NewTreeFromDB(nodes, e.registry, e.backend, chasm.DefaultPathEncoder, log.NewTestLogger(), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	return root
}

func (e *lifecycleTestEnvironment) executionRecord(nodes map[string]*persistencespb.ChasmNode) *persistencespb.WorkflowMutableState {
	key := e.backend.GetWorkflowKey()
	return &persistencespb.WorkflowMutableState{
		ChasmNodes: nodes,
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: key.NamespaceID,
			WorkflowId:  key.WorkflowID,
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{RunId: key.RunID},
	}
}
