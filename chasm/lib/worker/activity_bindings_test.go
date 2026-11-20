package worker

import (
	"context"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

// testTree drives a WorkerGroup through real CHASM transactions, so that state is serialized
// into persisted nodes and can be reloaded from them.
type testTree struct {
	t        *testing.T
	registry *chasm.Registry
	backend  *chasm.MockNodeBackend
	root     *chasm.Node
}

func newTestTree(t *testing.T) *testTree {
	t.Helper()
	registry := chasm.NewRegistry(log.NewTestLogger())
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(NewNilLibrary()))
	backend := &chasm.MockNodeBackend{
		HandleGetWorkflowKey: func() definition.WorkflowKey {
			return definition.NewWorkflowKey("ns-id", "grouping-key", "run-id")
		},
		HandleNextTransitionCount: func() int64 { return 1 },
		HandleGetCurrentVersion:   func() int64 { return 1 },
		HandleNow:                 func() time.Time { return time.Unix(1000, 0).UTC() },
	}
	tree := &testTree{t: t, registry: registry, backend: backend}
	tree.root = chasm.NewEmptyTree(registry, backend, chasm.DefaultPathEncoder, log.NewTestLogger(), metrics.NoopMetricsHandler)
	require.NoError(t, tree.root.SetRootComponent(newWorkerGroup()))
	tree.closeTransaction()
	return tree
}

// update applies the mutations in one transaction and returns the paths of the nodes it wrote.
func (tt *testTree) update(byWorker map[string]Mutation[string]) []string {
	tt.t.Helper()
	ctx := chasm.NewMutableContext(context.Background(), tt.root)
	component, err := tt.root.Component(ctx, chasm.ComponentRef{})
	require.NoError(tt.t, err)
	require.NoError(tt.t, component.(*WorkerGroup).applyActivityBindings(ctx, byWorker))
	return tt.closeTransaction()
}

func (tt *testTree) closeTransaction() []string {
	tt.t.Helper()
	mutation, err := tt.root.CloseTransaction(context.Background(), true)
	require.NoError(tt.t, err)
	return slices.Sorted(maps.Keys(mutation.UpdatedNodes))
}

// read returns each worker's activity bindings, read through the Set interface.
func (tt *testTree) read() map[string][]string {
	tt.t.Helper()
	ctx := chasm.NewContext(context.Background(), tt.root)
	component, err := tt.root.Component(ctx, chasm.ComponentRef{})
	require.NoError(tt.t, err)
	result := make(map[string][]string)
	for key, field := range component.(*WorkerGroup).Workers {
		bindings, err := field.Get(ctx).ActivityBindings.Get(ctx).Read(ctx)
		require.NoError(tt.t, err)
		result[key] = bindings
	}
	return result
}

// reload replaces the in-memory tree with one decoded from the persisted nodes.
func (tt *testTree) reload() {
	tt.t.Helper()
	root, err := chasm.NewTreeFromDB(
		tt.root.Snapshot(nil).Nodes,
		tt.registry,
		tt.backend,
		chasm.DefaultPathEncoder,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
	)
	require.NoError(tt.t, err)
	tt.root = root
}

func TestApplyActivityBindings(t *testing.T) {
	t.Parallel()
	tree := newTestTree(t)

	// A worker is created on its first non-empty mutation; an empty mutation creates nothing.
	tree.update(map[string]Mutation[string]{
		"worker-1": {Add: []string{"token-b", "token-a"}},
		"worker-2": {},
	})
	require.Equal(t, map[string][]string{"worker-1": {"token-a", "token-b"}}, tree.read())

	// Workers are independent: updating one leaves the other unchanged.
	tree.update(map[string]Mutation[string]{
		"worker-1": {Remove: []string{"token-a"}},
		"worker-2": {Add: []string{"token-a"}},
	})
	require.Equal(t, map[string][]string{
		"worker-1": {"token-b"},
		"worker-2": {"token-a"},
	}, tree.read())

	// An empty mutation for an existing worker writes none of its nodes; only the root,
	// which every update transaction writes, is persisted.
	require.Equal(t, []string{""}, tree.update(map[string]Mutation[string]{"worker-1": {}}))
	require.Equal(t, []string{""}, tree.update(nil))
}

// TestApplyActivityBindingsAfterReload covers decoding the interface-typed
// Field[Set[string]] from persisted nodes, then reading and updating through it.
func TestApplyActivityBindingsAfterReload(t *testing.T) {
	t.Parallel()
	tree := newTestTree(t)
	tree.update(map[string]Mutation[string]{
		"worker-1": {Add: []string{"token-a", "token-b"}},
		"worker-2": {Add: []string{"token-c"}},
	})

	tree.reload()
	require.Equal(t, map[string][]string{
		"worker-1": {"token-a", "token-b"},
		"worker-2": {"token-c"},
	}, tree.read())

	tree.update(map[string]Mutation[string]{"worker-1": {Add: []string{"token-d"}, Remove: []string{"token-a"}}})
	tree.reload()
	require.Equal(t, map[string][]string{
		"worker-1": {"token-b", "token-d"},
		"worker-2": {"token-c"},
	}, tree.read())
}
