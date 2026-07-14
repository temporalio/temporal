package chasm

import (
	"context"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

func (s *nodeSuite) minimalTestComponent() *TestComponent {
	return &TestComponent{
		ComponentData: &persistencespb.WorkflowExecutionState{RunId: "initial-run-id"},
		MSPointer:     NewMSPointer(s.nodeBackend),
	}
}

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

func (s *nodeSuite) TestSkipPersistenceIfClean_LoadedUnmodified() {
	s.nodeBackend.HandleNextTransitionCount = func() int64 { return 1 }
	s.nodeBackend.HandleGetCurrentVersion = func() int64 { return 1 }

	rootNode := NewEmptyTree(s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger, s.metricsHandler)
	s.NoError(rootNode.SetRootComponent(s.minimalTestComponent()))

	firstMutation, err := rootNode.CloseTransaction()
	s.NoError(err)
	s.Contains(firstMutation.UpdatedNodes, "", "new node must be in UpdatedNodes")

	persistedNodes := common.CloneProtoMap(firstMutation.UpdatedNodes)
	originalTransition := proto.Clone(
		persistedNodes[""].GetMetadata().GetLastUpdateVersionedTransition(),
	).(*persistencespb.VersionedTransition)

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

	s.testLibrary.mockSideEffectTaskHandler.EXPECT().
		Validate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).Times(1)
	ctx.AddTask(component, TaskAttributes{}, &TestSideEffectTask{Data: []byte("task-payload")})

	secondMutation, err := rootNode2.CloseTransaction()
	s.NoError(err)

	s.Contains(secondMutation.UpdatedNodes, "", "node with a new task must be in UpdatedNodes even if data is unchanged")
	componentAttr := secondMutation.UpdatedNodes[""].GetMetadata().GetComponentAttributes()
	s.Len(componentAttr.SideEffectTasks, 1, "side-effect task must be written to the persisted node")
}

func (s *nodeSuite) TestSkipPersistenceIfClean_DeleteUnpersistedNode() {
	s.nodeBackend.HandleNextTransitionCount = func() int64 { return 1 }
	s.nodeBackend.HandleGetCurrentVersion = func() int64 { return 1 }

	rootNode := NewEmptyTree(s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger, s.metricsHandler)
	s.NoError(rootNode.SetRootComponent(&TestComponent{
		ComponentData: &persistencespb.WorkflowExecutionState{RunId: "root"},
		SubComponent1: NewComponentField(nil, &TestSubComponent1{
			SubComponent1Data: &persistencespb.WorkflowExecutionState{RunId: "created-then-deleted"},
		}),
		MSPointer: NewMSPointer(s.nodeBackend),
	}))

	component := rootNode.value.(*TestComponent)
	component.SubComponent1 = NewEmptyField[*TestSubComponent1]()

	mutation, err := rootNode.CloseTransaction()
	s.NoError(err)

	s.Contains(mutation.UpdatedNodes, "", "root still needs initial persistence")
	s.Empty(mutation.DeletedNodes, "node created and deleted before first persistence must not emit a tombstone")
}
