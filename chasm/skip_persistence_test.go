package chasm

import (
	"context"
	"reflect"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

func (s *nodeSuite) TestSkipPersistenceIfClean_NewNode() {
	s.nodeBackend.HandleNextTransitionCount = func() int64 { return 1 }
	s.nodeBackend.HandleGetCurrentVersion = func() int64 { return 1 }

	rootNode := NewEmptyTree(s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger, s.metricsHandler)
	rootComponent := &TestSkipIfCleanComponent{
		Data: &persistencespb.WorkflowExecutionState{
			RunId: "initial-run-id",
		},
	}
	s.NoError(rootNode.SetRootComponent(rootComponent))

	mutation, err := rootNode.CloseTransaction()
	s.NoError(err)

	// Brand-new node must always be persisted.
	s.Contains(mutation.UpdatedNodes, "", "root node must be in UpdatedNodes for new component")
	s.NotNil(mutation.UpdatedNodes[""].GetData(), "data must be serialized")
}
func (s *nodeSuite) TestSkipPersistenceIfClean_LoadedUnmodified() {
	s.nodeBackend.HandleNextTransitionCount = func() int64 { return 1 }
	s.nodeBackend.HandleGetCurrentVersion = func() int64 { return 1 }

	// ---- First transaction: create and persist the component. ----
	rootNode := NewEmptyTree(s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger, s.metricsHandler)
	rootComponent := &TestSkipIfCleanComponent{
		Data: &persistencespb.WorkflowExecutionState{
			RunId: "some-run-id",
		},
	}
	s.NoError(rootNode.SetRootComponent(rootComponent))

	firstMutation, err := rootNode.CloseTransaction()
	s.NoError(err)
	s.Contains(firstMutation.UpdatedNodes, "", "new node must be in UpdatedNodes")

	persistedNodes := common.CloneProtoMap(firstMutation.UpdatedNodes)
	originalTransition := proto.Clone(
		persistedNodes[""].GetMetadata().GetLastUpdateVersionedTransition(),
	).(*persistencespb.VersionedTransition)

	// ---- Second transaction: load from storage, access via MutableContext without mutation. ----
	s.nodeBackend.HandleNextTransitionCount = func() int64 { return 2 }
	s.nodeBackend.HandleGetCurrentVersion = func() int64 { return 1 }

	rootNode2, err := NewTreeFromDB(persistedNodes, s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger, s.metricsHandler)
	s.NoError(err)

	// Access via MutableContext — marks the node dirty without mutating data.
	ctx := NewMutableContext(context.Background(), rootNode2)
	component, err := rootNode2.Component(ctx, ComponentRef{})
	s.NoError(err)
	s.IsType(&TestSkipIfCleanComponent{}, component, "should deserialize as TestSkipIfCleanComponent")

	secondMutation, err := rootNode2.CloseTransaction()
	s.NoError(err)

	// Node data is unchanged, does not appear in UpdatedNodes.
	s.NotContains(secondMutation.UpdatedNodes, "", "unchanged node must not be in UpdatedNodes")

	// LastUpdateVersionedTransition must not have been bumped.
	s.Equal(
		originalTransition.TransitionCount,
		rootNode2.serializedNode.GetMetadata().GetLastUpdateVersionedTransition().GetTransitionCount(),
		"LastUpdateVersionedTransition must not be bumped for unchanged node",
	)
}

func (s *nodeSuite) TestSkipPersistenceIfClean_LoadedModified() {
	s.nodeBackend.HandleNextTransitionCount = func() int64 { return 1 }
	s.nodeBackend.HandleGetCurrentVersion = func() int64 { return 1 }

	// ---- First transaction: create and persist. ----
	rootNode := NewEmptyTree(s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger, s.metricsHandler)
	rootComponent := &TestSkipIfCleanComponent{
		Data: &persistencespb.WorkflowExecutionState{
			RunId: "original-run-id",
		},
	}
	s.NoError(rootNode.SetRootComponent(rootComponent))

	firstMutation, err := rootNode.CloseTransaction()
	s.NoError(err)
	persistedNodes := common.CloneProtoMap(firstMutation.UpdatedNodes)

	// ---- Second transaction: load, mutate, close. ----
	s.nodeBackend.HandleNextTransitionCount = func() int64 { return 2 }
	s.nodeBackend.HandleGetCurrentVersion = func() int64 { return 1 }

	rootNode2, err := NewTreeFromDB(persistedNodes, s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger, s.metricsHandler)
	s.NoError(err)

	ctx := NewMutableContext(context.Background(), rootNode2)
	component, err := rootNode2.Component(ctx, ComponentRef{})
	s.NoError(err)

	// Mutate the data field.
	component.(*TestSkipIfCleanComponent).Data.RunId = "modified-run-id"

	secondMutation, err := rootNode2.CloseTransaction()
	s.NoError(err)

	// Data was changed, node must appear in UpdatedNodes.
	s.Contains(secondMutation.UpdatedNodes, "", "modified node must be in UpdatedNodes")
	s.Equal(
		int64(2),
		secondMutation.UpdatedNodes[""].GetMetadata().GetLastUpdateVersionedTransition().GetTransitionCount(),
		"LastUpdateVersionedTransition must be bumped for modified node",
	)
}

func (s *nodeSuite) TestSkipPersistenceIfClean_WithNewTask() {
	s.nodeBackend.HandleNextTransitionCount = func() int64 { return 1 }
	s.nodeBackend.HandleGetCurrentVersion = func() int64 { return 1 }

	// ---- First transaction: create and persist. ----
	rootNode := NewEmptyTree(s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger, s.metricsHandler)
	rootComponent := &TestSkipIfCleanComponent{
		Data: &persistencespb.WorkflowExecutionState{
			RunId: "some-run-id",
		},
	}
	s.NoError(rootNode.SetRootComponent(rootComponent))

	firstMutation, err := rootNode.CloseTransaction()
	s.NoError(err)
	persistedNodes := common.CloneProtoMap(firstMutation.UpdatedNodes)

	// ---- Second transaction: load, do NOT mutate data, but schedule a task. ----
	s.nodeBackend.HandleNextTransitionCount = func() int64 { return 2 }
	s.nodeBackend.HandleGetCurrentVersion = func() int64 { return 1 }

	rootNode2, err := NewTreeFromDB(persistedNodes, s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger, s.metricsHandler)
	s.NoError(err)

	ctx := NewMutableContext(context.Background(), rootNode2)
	component, err := rootNode2.Component(ctx, ComponentRef{})
	s.NoError(err)

	skipComponent := component.(*TestSkipIfCleanComponent)

	// Data is unchanged, but schedule a side-effect task on this component.
	s.testLibrary.mockSideEffectTaskHandler.EXPECT().
		Validate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).Times(1)
	ctx.AddTask(skipComponent, TaskAttributes{}, &TestSideEffectTask{
		Data: []byte("task-payload"),
	})

	secondMutation, err := rootNode2.CloseTransaction()
	s.NoError(err)

	// Despite unchanged data, the node must appear in UpdatedNodes so the task is persisted.
	s.Contains(secondMutation.UpdatedNodes, "", "node with a new task must be in UpdatedNodes even if data is unchanged")
	componentAttr := secondMutation.UpdatedNodes[""].GetMetadata().GetComponentAttributes()
	s.Len(componentAttr.SideEffectTasks, 1, "side-effect task must be written to the persisted node")
}

func (s *nodeSuite) TestSkipPersistenceIfClean_WithoutFlag() {
	s.nodeBackend.HandleNextTransitionCount = func() int64 { return 1 }
	s.nodeBackend.HandleGetCurrentVersion = func() int64 { return 1 }

	// ---- First transaction: create and persist a TestSubComponent2 (no skip flag). ----
	rootNode := NewEmptyTree(s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger, s.metricsHandler)
	// Use TestSubComponent2 which does NOT have WithSkipPersistenceIfClean.
	sc2 := &TestSubComponent2{
		SubComponent2Data: &persistencespb.WorkflowExecutionState{
			RunId: "sc2-run-id",
		},
	}
	// Wrap it in a TestComponent root so CloseTransaction works as expected.
	rootComponent := &TestComponent{
		MSPointer:     NewMSPointer(s.nodeBackend),
		SubComponent2: NewComponentField(nil, sc2),
	}
	// TestComponent needs minimal data to satisfy LifecycleState.
	rootComponent.ComponentData = &persistencespb.WorkflowExecutionState{}
	s.NoError(rootNode.SetRootComponent(rootComponent))

	firstMutation, err := rootNode.CloseTransaction()
	s.NoError(err)
	persistedNodes := common.CloneProtoMap(firstMutation.UpdatedNodes)

	// ---- Second transaction: load, access sub-component without mutation. ----
	s.nodeBackend.HandleNextTransitionCount = func() int64 { return 2 }
	s.nodeBackend.HandleGetCurrentVersion = func() int64 { return 1 }

	rootNode2, err := NewTreeFromDB(persistedNodes, s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger, s.metricsHandler)
	s.NoError(err)

	err = rootNode2.deserialize(reflect.TypeFor[*TestComponent]())
	s.NoError(err)

	ctx := NewMutableContext(context.Background(), rootNode2)
	component, err := rootNode2.Component(ctx, ComponentRef{})
	s.NoError(err)
	tc := component.(*TestComponent)

	// Access SubComponent2 via MutableContextm marks it dirty but don't mutate.
	sc2Loaded := tc.SubComponent2.Get(ctx)
	_ = sc2Loaded

	secondMutation, err := rootNode2.CloseTransaction()
	s.NoError(err)

	// Without the skip flag, the dirty sub component IS added to UpdatedNodes.
	sc2EncodedPath, err := rootNode2.children["SubComponent2"].getEncodedPath()
	s.NoError(err)
	s.Contains(secondMutation.UpdatedNodes, sc2EncodedPath, "node without skip flag must still be in UpdatedNodes when accessed via MutableContext")
}
