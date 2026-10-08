package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// newWorkflowTestTree loads a tree from serializedNodes, or returns an empty tree if there are none.
func newWorkflowTestTree(
	t *testing.T,
	backend chasm.NodeBackend,
	serializedNodes map[string]*persistencespb.ChasmNode,
) *chasm.Node {
	t.Helper()

	logger := log.NewTestLogger()
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(NewLibrary(NewRegistry())))
	require.NoError(t, registry.Register(callback.NewNilLibrary()))

	root, err := chasm.NewTreeFromDB(
		serializedNodes, registry, backend, chasm.DefaultPathEncoder, logger, metrics.NoopMetricsHandler,
	)
	require.NoError(t, err)
	return root
}

// persistWorkflowWithCallback writes a workflow with a single completion callback ("req-1") the way
// the current code does, and returns the nodes that would be persisted.
func persistWorkflowWithCallback(
	t *testing.T,
	backend chasm.NodeBackend,
) map[string]*persistencespb.ChasmNode {
	t.Helper()

	root := newWorkflowTestTree(t, backend, nil)
	mutableCtx := chasm.NewMutableContext(t.Context(), root)
	require.NoError(t, root.SetRootComponent(NewWorkflow(mutableCtx, chasm.NewMSPointer(backend))))

	// Go through the tree so the root node is marked dirty, as the write path does.
	component, err := root.ComponentByPath(mutableCtx, nil)
	require.NoError(t, err)
	require.NoError(t, component.(*Workflow).AddCompletionCallbacks(
		mutableCtx,
		timestamppb.Now(),
		"req-1",
		[]*commonpb.Callback{nexusCallback("http://cb-1")},
	))

	mutation, err := root.CloseTransaction()
	require.NoError(t, err)
	require.Contains(t, mutation.UpdatedNodes, "")
	return mutation.UpdatedNodes
}

func loadWorkflow(
	t *testing.T,
	backend chasm.NodeBackend,
	serializedNodes map[string]*persistencespb.ChasmNode,
) *Workflow {
	t.Helper()

	root := newWorkflowTestTree(t, backend, serializedNodes)
	component, err := root.ComponentByPath(chasm.NewContext(t.Context(), root), nil)
	require.NoError(t, err)
	wf, ok := component.(*Workflow)
	require.True(t, ok)
	return wf
}

func cloneNodes(nodes map[string]*persistencespb.ChasmNode) map[string]*persistencespb.ChasmNode {
	cloned := make(map[string]*persistencespb.ChasmNode, len(nodes))
	for path, node := range nodes {
		cloned[path] = proto.Clone(node).(*persistencespb.ChasmNode)
	}
	return cloned
}

// TestWorkflowStateReplacesEmptyState pins the on-disk compatibility of swapping the workflow
// root component's state proto from emptypb.Empty to WorkflowState. Executions written by an
// older server persisted either no data blob at all (NewWorkflow left Empty nil) or the zero
// encoding of Empty, and both must load as a zero-valued WorkflowState with their callbacks
// intact.
func TestWorkflowStateReplacesEmptyState(t *testing.T) {
	t.Parallel()

	backend := &chasm.MockNodeBackend{
		HandleGetCurrentVersion:   func() int64 { return 1 },
		HandleNextTransitionCount: func() int64 { return 1 },
	}
	persisted := persistWorkflowWithCallback(t, backend)

	emptyEncoded, err := proto.Marshal(&emptypb.Empty{})
	require.NoError(t, err)

	for _, tc := range []struct {
		name string
		// rewriteRoot, if set, rewrites the root node to what a pre-WorkflowState server would
		// have stored.
		rewriteRoot func(root *persistencespb.ChasmNode)
		wantCBCount int64
	}{
		{
			// NewWorkflow never set Empty, so serializeComponentNode wrote no blob at all.
			name:        "NoDataBlob",
			rewriteRoot: func(root *persistencespb.ChasmNode) { root.Data = nil },
			wantCBCount: 0,
		},
		{
			// Belt and braces: an explicitly encoded Empty is also zero bytes.
			name: "EncodedEmpty",
			rewriteRoot: func(root *persistencespb.ChasmNode) {
				root.Data = &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: emptyEncoded}
			},
			wantCBCount: 0,
		},
		{
			// Guards the two cases above against passing vacuously: the blob this server
			// writes must come back with its counters intact.
			name:        "CurrentEncoding",
			wantCBCount: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			serializedNodes := cloneNodes(persisted)
			if tc.rewriteRoot != nil {
				tc.rewriteRoot(serializedNodes[""])
			}

			wf := loadWorkflow(t, backend, serializedNodes)
			require.NotNil(t, wf.WorkflowState)
			require.Equal(t, tc.wantCBCount, wf.GetCallbackMetadata().GetTotalCallbacksCount())

			// The rest of the tree is unaffected by the state swap.
			require.Len(t, wf.Callbacks, 1)
			require.Contains(t, wf.Callbacks, "req-1-0")
		})
	}
}
