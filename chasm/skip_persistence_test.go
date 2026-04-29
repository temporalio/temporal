package chasm

import (
	"context"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

// minimalTestComponent returns a TestComponent with just enough fields set for
// CloseTransaction to succeed. It has no sub-components so the root node is the
// only thing we need to reason about in these tests.
func (s *nodeSuite) minimalTestComponent() *TestComponent {
	return &TestComponent{
		ComponentData: &persistencespb.WorkflowExecutionState{RunId: "initial-run-id"},
		MSPointer:     NewMSPointer(s.nodeBackend),
	}
}

// TestSkipPersistenceIfClean_NewNode verifies that a brand-new component node
// (never written to storage) is always persisted, even though skip-if-clean is
// the default behavior. The guard is initialStatePersisted == false.
func (s *nodeSuite) TestSkipPersistenceIfClean_NewNode() {
	s.nodeBackend.HandleNextTransitionCount = func() int64 { return 1 }
	s.nodeBackend.HandleGetCurrentVersion = func() int64 { return 1 }

	rootNode := NewEmptyTree(s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger, s.metricsHandler)
	s.NoError(rootNode.SetRootComponent(s.minimalTestComponent()))

	mutation, err := rootNode.CloseTransaction()
	s.NoError(err)

	s.Contains(mutation.UpdatedNodes, "", "brand-new node must always be persisted")
	s.NotNil(mutation.UpdatedNodes[""].GetData(), "data blob must be present")
}

// TestSkipPersistenceIfClean_LoadedUnmodified verifies the core optimization:
// a node loaded from storage and accessed via MutableContext without any data
// mutation is NOT added to UpdatedNodes and its LastUpdateVersionedTransition is
// not bumped.
func (s *nodeSuite) TestSkipPersistenceIfClean_LoadedUnmodified() {
	s.nodeBackend.HandleNextTransitionCount = func() int64 { return 1 }
	s.nodeBackend.HandleGetCurrentVersion = func() int64 { return 1 }

	// First transaction: create and persist.
	rootNode := NewEmptyTree(s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger, s.metricsHandler)
	s.NoError(rootNode.SetRootComponent(s.minimalTestComponent()))

	firstMutation, err := rootNode.CloseTransaction()
	s.NoError(err)
	s.Contains(firstMutation.UpdatedNodes, "", "new node must be in UpdatedNodes")

	persistedNodes := common.CloneProtoMap(firstMutation.UpdatedNodes)
	originalTransition := proto.Clone(
		persistedNodes[""].GetMetadata().GetLastUpdateVersionedTransition(),
	).(*persistencespb.VersionedTransition)

	// Second transaction: load from storage, touch via MutableContext, no mutation.
	s.nodeBackend.HandleNextTransitionCount = func() int64 { return 2 }
	s.nodeBackend.HandleGetCurrentVersion = func() int64 { return 1 }

	rootNode2, err := NewTreeFromDB(persistedNodes, s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger, s.metricsHandler)
	s.NoError(err)

	ctx := NewMutableContext(context.Background(), rootNode2)
	_, err = rootNode2.Component(ctx, ComponentRef{})
	s.NoError(err)

	secondMutation, err := rootNode2.CloseTransaction()
	s.NoError(err)

	s.NotContains(secondMutation.UpdatedNodes, "", "unchanged node must not be in UpdatedNodes")
	s.Equal(
		originalTransition.TransitionCount,
		rootNode2.serializedNode.GetMetadata().GetLastUpdateVersionedTransition().GetTransitionCount(),
		"LastUpdateVersionedTransition must not be bumped for an unchanged node",
	)
}

// TestSkipPersistenceIfClean_LoadedModified verifies that a node whose data was
// actually mutated IS added to UpdatedNodes and its transition IS bumped.
func (s *nodeSuite) TestSkipPersistenceIfClean_LoadedModified() {
	s.nodeBackend.HandleNextTransitionCount = func() int64 { return 1 }
	s.nodeBackend.HandleGetCurrentVersion = func() int64 { return 1 }

	rootNode := NewEmptyTree(s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger, s.metricsHandler)
	s.NoError(rootNode.SetRootComponent(s.minimalTestComponent()))

	firstMutation, err := rootNode.CloseTransaction()
	s.NoError(err)
	persistedNodes := common.CloneProtoMap(firstMutation.UpdatedNodes)

	s.nodeBackend.HandleNextTransitionCount = func() int64 { return 2 }
	s.nodeBackend.HandleGetCurrentVersion = func() int64 { return 1 }

	rootNode2, err := NewTreeFromDB(persistedNodes, s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger, s.metricsHandler)
	s.NoError(err)

	ctx := NewMutableContext(context.Background(), rootNode2)
	component, err := rootNode2.Component(ctx, ComponentRef{})
	s.NoError(err)

	// Mutate a field so the serialized bytes change.
	component.(*TestComponent).ComponentData.RunId = "modified-run-id"

	secondMutation, err := rootNode2.CloseTransaction()
	s.NoError(err)

	s.Contains(secondMutation.UpdatedNodes, "", "modified node must be in UpdatedNodes")
	s.Equal(
		int64(2),
		secondMutation.UpdatedNodes[""].GetMetadata().GetLastUpdateVersionedTransition().GetTransitionCount(),
		"LastUpdateVersionedTransition must be bumped for a modified node",
	)
}

// TestSkipPersistenceIfClean_WithNewTask verifies that a node with unchanged data
// is still added to UpdatedNodes when a new task is scheduled on it, so the task
// is written to storage and its versioned transition is bumped for replication.
func (s *nodeSuite) TestSkipPersistenceIfClean_WithNewTask() {
	s.nodeBackend.HandleNextTransitionCount = func() int64 { return 1 }
	s.nodeBackend.HandleGetCurrentVersion = func() int64 { return 1 }

	rootNode := NewEmptyTree(s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger, s.metricsHandler)
	s.NoError(rootNode.SetRootComponent(s.minimalTestComponent()))

	firstMutation, err := rootNode.CloseTransaction()
	s.NoError(err)
	persistedNodes := common.CloneProtoMap(firstMutation.UpdatedNodes)

	s.nodeBackend.HandleNextTransitionCount = func() int64 { return 2 }
	s.nodeBackend.HandleGetCurrentVersion = func() int64 { return 1 }

	rootNode2, err := NewTreeFromDB(persistedNodes, s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger, s.metricsHandler)
	s.NoError(err)

	ctx := NewMutableContext(context.Background(), rootNode2)
	component, err := rootNode2.Component(ctx, ComponentRef{})
	s.NoError(err)

	// Data is unchanged — only a side-effect task is added.
	s.testLibrary.mockSideEffectTaskHandler.EXPECT().
		Validate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).Times(1)
	ctx.AddTask(component, TaskAttributes{}, &TestSideEffectTask{Data: []byte("task-payload")})

	secondMutation, err := rootNode2.CloseTransaction()
	s.NoError(err)

	s.Contains(secondMutation.UpdatedNodes, "", "node with a new task must be in UpdatedNodes even if data is unchanged")
	componentAttr := secondMutation.UpdatedNodes[""].GetMetadata().GetComponentAttributes()
	s.Len(componentAttr.SideEffectTasks, 1, "side-effect task must be written to the persisted node")
}
