package chasm

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/testing/protoassert"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	nodeSuite struct {
		suite.Suite
		*require.Assertions
		protorequire.ProtoAssertions

		controller  *gomock.Controller
		nodeBackend *MockNodeBackend

		registry        *Registry
		timeSource      *clock.EventTimeSource
		nodePathEncoder NodePathEncoder
		logger          log.Logger
	}
)

func TestNodeSuite(t *testing.T) {
	suite.Run(t, new(nodeSuite))
}

func (s *nodeSuite) SetupTest() {
	s.initAssertions()
	s.controller = gomock.NewController(s.T())
	s.nodeBackend = NewMockNodeBackend(s.controller)

	s.registry = NewRegistry()
	err := s.registry.Register(newTestLibrary(s.controller))
	s.NoError(err)

	s.timeSource = clock.NewEventTimeSource()
	s.nodePathEncoder = &testNodePathEncoder{}
	s.logger = testlogger.NewTestLogger(s.T(), testlogger.FailOnAnyUnexpectedError)
}

func (s *nodeSuite) SetupSubTest() {
	s.initAssertions()
}

func (s *nodeSuite) initAssertions() {
	// `s.Assertions` (as well as other test helpers which depends on `s.T()`) must be initialized on
	// both test and subtest levels (but not suite level, where `s.T()` is `nil`).
	//
	// If these helpers are not reinitialized on subtest level, any failed `assert` in
	// subtest will fail the entire test (not subtest) immediately without running other subtests.

	s.Assertions = require.New(s.T())
	s.ProtoAssertions = protorequire.New(s.T())
}

func (s *nodeSuite) TestNewTree() {
	persistenceNodes := map[string]*persistencespb.ChasmNode{
		"": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
			},
		},
		"child1": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 2},
			},
		},
		"child2": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 3},
			},
		},
		"child1/grandchild1": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 4},
			},
		},
		"child2/grandchild1": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 5},
			},
		},
	}
	expectedPreorderNodes := []*persistencespb.ChasmNode{
		persistenceNodes[""],
		persistenceNodes["child1"],
		persistenceNodes["child1/grandchild1"],
		persistenceNodes["child2"],
		persistenceNodes["child2/grandchild1"],
	}

	root, err := NewTree(persistenceNodes, s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger)
	s.NoError(err)
	s.NotNil(root)

	preorderNodes := s.preorderAndAssertParent(root, nil)
	s.Len(preorderNodes, 5)
	s.Equal(expectedPreorderNodes, preorderNodes)
}

func (s *nodeSuite) TestInitSerializedNode_TypeComponent() {
	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(1)).Times(1)
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).Times(1)

	node := newNode(s.nodeBase(), nil, "")
	node.initSerializedNode(fieldTypeComponent)

	s.NotNil(node.serializedNode.GetMetadata().GetComponentAttributes(), "node serializedNode must have attributes created")
	s.Nil(node.serializedNode.GetData(), "node serializedNode must not have data before serialize is called")
}

func (s *nodeSuite) TestSerializeNode_ComponentAttributes() {
	node := s.testComponentTree()

	s.Len(node.children, 2)
	s.NotNil(node.children["SubComponent1"].value)
	s.Len(node.children["SubComponent1"].children, 2)
	s.NotNil(node.children["SubComponent1"].children["SubComponent11"].value)
	s.Empty(node.children["SubComponent1"].children["SubComponent11"].children)

	// Serialize root component.
	s.NotNil(node.serializedNode.GetMetadata().GetComponentAttributes())
	s.Nil(node.serializedNode.GetData())
	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(2)).Times(1) // for LastUpdatesVersionedTransition for TestComponent
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(2)).Times(1)
	err := node.serialize()
	s.NoError(err)
	s.NotNil(node.serializedNode)
	s.NotNil(node.serializedNode.GetData(), "node serialized value must have data after serialize is called")
	s.Equal("TestLibrary.test_component", node.serializedNode.GetMetadata().GetComponentAttributes().GetType(), "node serialized value must have type set")
	s.Equal(valueStateSynced, node.valueState)

	// Serialize subcomponents (there are 2 subcomponents).
	sc1Node := node.children["SubComponent1"]
	s.NotNil(sc1Node.serializedNode.GetMetadata().GetComponentAttributes())
	s.Nil(sc1Node.serializedNode.GetData())
	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(2)).Times(2) // for LastUpdatesVersionedTransition of TestSubComponent1
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(2)).Times(2)
	for _, childNode := range node.children {
		err = childNode.serialize()
		s.NoError(err)
		s.Equal(valueStateSynced, childNode.valueState)
	}
	s.NotNil(sc1Node.serializedNode.GetData(), "child node serialized value must have data after serialize is called")
	s.Equal("TestLibrary.test_sub_component_1", sc1Node.serializedNode.GetMetadata().GetComponentAttributes().GetType(), "node serialized value must have type set")

	// Check SubData too.
	sd1Node := node.children["SubData1"]
	s.NoError(err)
	s.NotNil(sd1Node.serializedNode.GetData(), "child node serialized value must have data after serialize is called")
}

func (s *nodeSuite) TestSerializeNode_ClearComponentData() {
	node := s.testComponentTree()

	node.value.(*TestComponent).ComponentData = nil

	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(2)).Times(1) // for LastUpdatesVersionedTransition for TestComponent
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(2)).Times(1)
	err := node.serialize()
	s.NoError(err)
	s.NotNil(node.serializedNode, "node serialized value must be not nil after serialize is called")
	s.NotNil(node.serializedNode.GetMetadata().GetComponentAttributes(), "metadata must have component attributes")
	s.Nil(node.serializedNode.GetData(), "data field must cleared to nil")
	s.Equal("TestLibrary.test_component", node.serializedNode.GetMetadata().GetComponentAttributes().GetType(), "type must present")
	s.Equal(valueStateSynced, node.valueState)
}

func (s *nodeSuite) TestSerializeNode_ClearSubDataField() {
	node := s.testComponentTree()

	node.value.(*TestComponent).SubData1 = NewEmptyField[*protoMessageType]()

	sd1Node := node.children["SubData1"]
	s.NotNil(sd1Node)

	err := node.syncSubComponents()
	s.NoError(err)
	s.Len(node.mutation.DeletedNodes, 1)

	sd1Node = node.children["SubData1"]
	s.Nil(sd1Node)
}

func (s *nodeSuite) TestInitSerializedNode_TypeData() {
	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(1)).Times(1) // for InitialVersionedTransition
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).Times(1)

	node := newNode(s.nodeBase(), nil, "")
	node.initSerializedNode(fieldTypeData)
	s.NotNil(node.serializedNode.GetMetadata().GetDataAttributes(), "node serializedNode must have attributes created")
	s.Nil(node.serializedNode.GetData(), "node serializedNode must not have data before serialize is called")
}

func (s *nodeSuite) TestSerializeNode_DataAttributes() {
	component := &protoMessageType{
		CreateRequestId: "22",
	}

	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(1)).Times(1) // for InitialVersionedTransition
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).Times(1)

	node := newNode(s.nodeBase(), nil, "")
	node.initSerializedNode(fieldTypeData)
	node.value = component
	node.valueState = valueStateNeedSerialize

	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(1)).Times(1)
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).Times(1)
	err := node.serialize()
	s.NoError(err)
	s.NotNil(node.serializedNode.GetData(), "child node serialized value must have data after serialize is called")
	s.Equal([]byte{0xa, 0x2, 0x32, 0x32}, node.serializedNode.GetData().GetData())
	s.Equal(valueStateSynced, node.valueState)
}

func (s *nodeSuite) TestSyncSubComponents_DeleteLeafNode() {
	node := s.testComponentTree()

	component := node.value.(*TestComponent)

	// Set very leaf node to empty.
	component.SubComponent1.Internal.v.(*TestSubComponent1).SubComponent11 = NewEmptyField[*TestSubComponent11]()
	s.NotNil(node.children["SubComponent1"].children["SubComponent11"])

	err := node.syncSubComponents()
	s.NoError(err)

	s.Len(node.mutation.DeletedNodes, 1)
	s.NotNil(node.mutation.DeletedNodes["SubComponent1/SubComponent11"])
	s.Nil(node.children["SubComponent1"].children["SubComponent11"])
}

func (s *nodeSuite) TestSyncSubComponents_DeleteMiddleNode() {
	node := s.testComponentTree()

	component := node.value.(*TestComponent)

	// Set subcomponent at middle node to nil.
	component.SubComponent1 = NewEmptyField[*TestSubComponent1]()
	s.NotNil(node.children["SubComponent1"])

	err := node.syncSubComponents()
	s.NoError(err)

	s.Len(node.mutation.DeletedNodes, 3)
	s.NotNil(node.mutation.DeletedNodes["SubComponent1/SubComponent11"])
	s.NotNil(node.mutation.DeletedNodes["SubComponent1/SubData11"])
	s.NotNil(node.mutation.DeletedNodes["SubComponent1"])

	s.Nil(node.children["SubComponent1"])
}

func (s *nodeSuite) TestDeserializeNode_EmptyPersistence() {
	var serializedNodes map[string]*persistencespb.ChasmNode

	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(1)).Times(1) // for InitialVersionedTransition
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).Times(1)

	node, err := NewTree(serializedNodes, s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger)
	s.NoError(err)
	s.Nil(node.value)
	s.NotNil(node.serializedNode)

	err = node.deserialize(reflect.TypeFor[*TestComponent]())
	s.NoError(err)
	s.NotNil(node.value)
	s.IsType(&TestComponent{}, node.value)
	tc := node.value.(*TestComponent)
	s.Equal(valueStateSynced, node.valueState)
	s.Nil(tc.SubComponent1.Internal.node)
	s.Nil(tc.SubComponent1.Internal.value())
	s.Nil(tc.ComponentData)
}

func (s *nodeSuite) TestDeserializeNode_ComponentAttributes() {
	serializedNodes := testComponentSerializedNodes()

	node, err := NewTree(serializedNodes, s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger)
	s.NoError(err)
	s.Nil(node.value)
	s.NotNil(node.serializedNode)
	s.Equal(valueStateNeedDeserialize, node.valueState)

	err = node.deserialize(reflect.TypeFor[*TestComponent]())
	s.NoError(err)
	s.NotNil(node.value)
	s.IsType(&TestComponent{}, node.value)
	tc := node.value.(*TestComponent)
	s.Equal(tc.SubComponent1.Internal.node, node.children["SubComponent1"])
	s.Equal(tc.ComponentData.CreateRequestId, "component-data")
	s.Equal(valueStateSynced, node.valueState)

	s.Nil(tc.SubComponent1.Internal.value())
	s.Equal(valueStateNeedDeserialize, tc.SubComponent1.Internal.node.valueState)
	err = tc.SubComponent1.Internal.node.deserialize(reflect.TypeFor[*TestSubComponent1]())
	s.NoError(err)
	s.NotNil(tc.SubComponent1.Internal.node.value)
	s.IsType(&TestSubComponent1{}, tc.SubComponent1.Internal.node.value)
	s.Equal("sub-component1-data", tc.SubComponent1.Internal.node.value.(*TestSubComponent1).SubComponent1Data.CreateRequestId)
	s.Equal(valueStateSynced, tc.SubComponent1.Internal.node.valueState)
}

func (s *nodeSuite) TestDeserializeNode_DataAttributes() {
	serializedNodes := testComponentSerializedNodes()

	node, err := NewTree(serializedNodes, s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger)
	s.NoError(err)
	s.Nil(node.value)
	s.NotNil(node.serializedNode)

	err = node.deserialize(reflect.TypeFor[*TestComponent]())
	s.NoError(err)
	s.NotNil(node.value)
	s.Equal(valueStateSynced, node.valueState)

	s.IsType(&TestComponent{}, node.value)
	tc := node.value.(*TestComponent)

	s.Equal(tc.SubData1.Internal.node, node.children["SubData1"])

	s.Nil(tc.SubData1.Internal.value())
	err = tc.SubData1.Internal.node.deserialize(reflect.TypeFor[*protoMessageType]())
	s.NoError(err)
	s.NotNil(tc.SubData1.Internal.node.value)
	s.Equal(valueStateSynced, tc.SubData1.Internal.node.valueState)
	s.IsType(&protoMessageType{}, tc.SubData1.Internal.node.value)
	s.Equal("sub-data1", tc.SubData1.Internal.node.value.(*protoMessageType).CreateRequestId)
}

func (s *nodeSuite) TestFieldInterface() {
	type testComponent struct {
		UnimplementedComponent
		Data          *protoMessageType
		SubComponent1 Field[TestSubComponent]
	}

	serializedNodes := testComponentSerializedNodes()
	node, err := NewTree(serializedNodes, s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger)
	s.NoError(err)
	s.Nil(node.value)
	s.NotNil(node.serializedNode)

	err = node.deserialize(reflect.TypeFor[*testComponent]())
	s.NoError(err)
	s.NotNil(node.value)
	s.IsType(&testComponent{}, node.value)
	tc := node.value.(*testComponent)

	chasmContext := NewMutableContext(context.Background(), node)
	sc1, err := tc.SubComponent1.Get(chasmContext)
	s.NoError(err)
	s.NotNil(sc1)
	s.Equal("sub-component1-data", sc1.GetData())
}

func (s *nodeSuite) TestGenerateSerializedNodes() {
	s.T().Skip("This test is used to generate serialized nodes for other tests.")

	node := s.testComponentTree()

	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(2)).Times(1) // for LastUpdatesVersionedTransition for TestComponent
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(2)).Times(1)
	err := node.serialize()
	s.NoError(err)
	serializedNodes := map[string]*persistencespb.ChasmNode{}
	serializedNodes[""] = node.serializedNode

	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(2)).Times(2) // for LastUpdatesVersionedTransition of TestSubComponent1
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(2)).Times(2)
	for childName, childNode := range node.children {
		err = childNode.serialize()
		s.NoError(err)
		serializedNodes[childName] = childNode.serializedNode
	}

	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(2)).Times(2) // for LastUpdatesVersionedTransition of TestSubComponent1
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(2)).Times(2)
	for childName, childNode := range node.children["SubComponent1"].children {
		err = childNode.serialize()
		s.NoError(err)
		serializedNodes["SubComponent1/"+childName] = childNode.serializedNode
	}

	generateMapInit(serializedNodes, "serializedNodes")
}

func (s *nodeSuite) TestNodeSnapshot() {
	persistenceNodes := map[string]*persistencespb.ChasmNode{
		"": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
			},
		},
		"child1": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 4},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 4},
			},
		},
		"child2": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 3},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 3},
			},
		},
		"child1/grandchild1": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 2},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 2},
			},
		},
		"child2/grandchild1": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 5},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 5},
			},
		},
	}

	root, err := NewTree(persistenceNodes, s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger)
	s.NoError(err)
	s.NotNil(root)

	// Test snapshot with nil exclusiveMinVT, which should return all nodes
	snapshot := root.Snapshot(nil)
	s.Equal(persistenceNodes, snapshot.Nodes)

	// Test snapshot with non-nil exclusiveMinVT, which should return only nodes with higher
	// LastUpdateVersionedTransition than the exclusiveMinVT
	expectedNodePaths := []string{"child1", "child2/grandchild1"}
	expectedNodes := make(map[string]*persistencespb.ChasmNode)
	for _, path := range expectedNodePaths {
		expectedNodes[path] = persistenceNodes[path]
	}
	snapshot = root.Snapshot(&persistencespb.VersionedTransition{TransitionCount: 3})
	s.Equal(expectedNodes, snapshot.Nodes)
}

func (s *nodeSuite) TestApplyMutation() {
	// Setup initial tree with a root, a child, and a grandchild.
	persistenceNodes := map[string]*persistencespb.ChasmNode{
		"": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
			},
		},
		"child": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 2},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 2},
			},
		},
		"child/grandchild": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 3},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 3},
			},
		},
		"child/grandchild/grandgrandchild": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 4},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 4},
			},
		},
	}
	root, err := NewTree(persistenceNodes, s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger)
	s.NoError(err)

	// This decoded value should be reset after applying the mutation
	root.children["child"].value = "some-random-decoded-value"

	// Prepare mutation: update "child" node, delete "child/grandchild", and add "newchild".
	updatedChild := &persistencespb.ChasmNode{
		Metadata: &persistencespb.ChasmNodeMetadata{
			InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 20},
			LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 20},
		},
	}
	newChild := &persistencespb.ChasmNode{
		Metadata: &persistencespb.ChasmNodeMetadata{
			InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 100},
			LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 100},
		},
	}
	mutation := NodesMutation{
		UpdatedNodes: map[string]*persistencespb.ChasmNode{
			"child":    updatedChild,
			"newchild": newChild,
		},
		DeletedNodes: map[string]struct{}{
			"child/grandchild":           {}, // this should remove the entire "grandchild" subtree
			"child/non-exist-grandchild": {},
		},
	}
	err = root.ApplyMutation(mutation)
	s.NoError(err)

	// Validate the "child" node got updated.
	childNode, ok := root.children["child"]
	s.True(ok)
	s.Equal(updatedChild, childNode.serializedNode)
	s.Nil(childNode.value) // value should be reset after mutation

	// Validate the "newchild" node is added.
	newChildNode, ok := root.children["newchild"]
	s.True(ok)
	s.Equal(newChild, newChildNode.serializedNode)

	// Validate the "grandchild" node is deleted.
	s.Empty(childNode.children)

	// Validate that nodeBase.mutation reflects the applied mutation.
	// Only updates on existing nodes are recorded; new nodes are inserted without a mutation record.
	expectedMutation := NodesMutation{
		UpdatedNodes: map[string]*persistencespb.ChasmNode{
			"child":    updatedChild,
			"newchild": newChild,
		},
		DeletedNodes: map[string]struct{}{
			"child/grandchild":                 {},
			"child/grandchild/grandgrandchild": {},
		},
	}
	s.Equal(expectedMutation, root.mutation)
}

func (s *nodeSuite) TestApplySnapshot() {
	// Setup initial tree with a root, a child, and a grandchild.
	persistenceNodes := map[string]*persistencespb.ChasmNode{
		"": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
			},
		},
		"child": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 2},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 2},
			},
		},
		"child/grandchild": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 3},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 3},
			},
		},
		"child/grandchild/grandgrandchild": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 4},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 4},
			},
		},
	}
	root, err := NewTree(persistenceNodes, s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger)
	s.NoError(err)

	// Set a decoded value that should be reset after applying the snapshot.
	root.children["child"].value = "decoded-value"

	// Prepare an incoming snapshot representing the target state:
	// - The "child" node is updated (LastUpdateTransition becomes 20),
	// - the "child/grandchild" node is removed,
	// - a new node "newchild" is added.
	incomingSnapshot := NodesSnapshot{
		Nodes: map[string]*persistencespb.ChasmNode{
			"": {
				Metadata: &persistencespb.ChasmNodeMetadata{
					InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
					LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				},
			},
			"child": {
				Metadata: &persistencespb.ChasmNodeMetadata{
					InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 2},
					LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 20},
				},
			},
			"newchild": {
				Metadata: &persistencespb.ChasmNodeMetadata{
					InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 100},
					LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 100},
				},
			},
		},
	}
	err = root.ApplySnapshot(incomingSnapshot)
	s.NoError(err)

	s.Equal(incomingSnapshot, root.Snapshot(nil))
	s.Nil(root.children["child"].value) // value should be reset after snapshot

	// Validate that nodeBase.mutation reflects the applied snapshot.
	expectedMutation := NodesMutation{
		UpdatedNodes: map[string]*persistencespb.ChasmNode{
			"child":    incomingSnapshot.Nodes["child"],
			"newchild": incomingSnapshot.Nodes["newchild"],
		},
		DeletedNodes: map[string]struct{}{
			"child/grandchild":                 {},
			"child/grandchild/grandgrandchild": {},
		},
	}
	s.Equal(expectedMutation, root.mutation)
}

func (s *nodeSuite) TestCarryOverTaskStatus() {
	now := s.timeSource.Now()
	persistenceNodes := map[string]*persistencespb.ChasmNode{
		"": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 2},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						Type: "TestLibrary.test_component",
						SideEffectTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								Type:                      "TestLibrary.test_side_effect_task",
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 1,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
							},
							{
								Type:                      "TestLibrary.test_side_effect_task",
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 2,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
							},
							{
								Type:                      "TestLibrary.test_side_effect_task",
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 2},
								VersionedTransitionOffset: 1,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
							},
						},
						PureTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								Type:                      "TestLibrary.test_pure_task",
								ScheduledTime:             timestamppb.New(now.Add(time.Minute)),
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 2},
								VersionedTransitionOffset: 2,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
							},
							{
								Type:                      "TestLibrary.test_pure_task",
								ScheduledTime:             timestamppb.New(now.Add(2 * time.Minute)),
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 3,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
							},
							{
								Type:                      "TestLibrary.test_pure_task",
								ScheduledTime:             timestamppb.New(now.Add(3 * time.Minute)),
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 2},
								VersionedTransitionOffset: 3,
								PhysicalTaskStatus:        physicalTaskStatusNone,
							},
						},
					},
				},
			},
		},
		"data": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 2},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 2},
				Attributes: &persistencespb.ChasmNodeMetadata_DataAttributes{
					DataAttributes: &persistencespb.ChasmDataAttributes{},
				},
			},
		},
	}
	root, err := NewTree(persistenceNodes, s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger)
	s.NoError(err)

	mutations := NodesMutation{
		UpdatedNodes: map[string]*persistencespb.ChasmNode{
			"": {
				Metadata: &persistencespb.ChasmNodeMetadata{
					InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
					LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 3},
					Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
						ComponentAttributes: &persistencespb.ChasmComponentAttributes{
							Type: "TestLibrary.test_component",
							SideEffectTasks: []*persistencespb.ChasmComponentAttributes_Task{
								{
									Type:                      "TestLibrary.test_side_effect_task",
									VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
									VersionedTransitionOffset: 2,
									PhysicalTaskStatus:        physicalTaskStatusCreated,
								},
								{
									Type:                      "TestLibrary.test_side_effect_task",
									VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 3},
									VersionedTransitionOffset: 1,
									PhysicalTaskStatus:        physicalTaskStatusCreated,
								},
							},
							PureTasks: []*persistencespb.ChasmComponentAttributes_Task{
								{
									Type:                      "TestLibrary.test_pure_task",
									ScheduledTime:             timestamppb.New(now.Add(time.Second)),
									VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 3},
									VersionedTransitionOffset: 2,
									PhysicalTaskStatus:        physicalTaskStatusCreated,
								},
								{
									Type:                      "TestLibrary.test_pure_task",
									ScheduledTime:             timestamppb.New(now.Add(2 * time.Minute)),
									VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
									VersionedTransitionOffset: 3,
									PhysicalTaskStatus:        physicalTaskStatusCreated,
								},
								{
									Type:                      "TestLibrary.test_pure_task",
									ScheduledTime:             timestamppb.New(now.Add(3 * time.Minute)),
									VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 2},
									VersionedTransitionOffset: 3,
									PhysicalTaskStatus:        physicalTaskStatusNone,
								},
							},
						},
					},
				},
			},
			"data": {
				Metadata: &persistencespb.ChasmNodeMetadata{
					InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 2},
					LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 3},
					Attributes: &persistencespb.ChasmNodeMetadata_DataAttributes{
						DataAttributes: &persistencespb.ChasmDataAttributes{},
					},
				},
			},
		},
	}

	expectedNodes := map[string]*persistencespb.ChasmNode{
		"": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 3},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						Type: "TestLibrary.test_component",
						SideEffectTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								Type:                      "TestLibrary.test_side_effect_task",
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 2,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
							},
							{
								Type:                      "TestLibrary.test_side_effect_task",
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 3},
								VersionedTransitionOffset: 1,
								PhysicalTaskStatus:        physicalTaskStatusNone,
							},
						},
						PureTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								Type:                      "TestLibrary.test_pure_task",
								ScheduledTime:             timestamppb.New(now.Add(time.Second)),
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 3},
								VersionedTransitionOffset: 2,
								PhysicalTaskStatus:        physicalTaskStatusNone,
							},
							{
								Type:                      "TestLibrary.test_pure_task",
								ScheduledTime:             timestamppb.New(now.Add(2 * time.Minute)),
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 3,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
							},
							{
								Type:                      "TestLibrary.test_pure_task",
								ScheduledTime:             timestamppb.New(now.Add(3 * time.Minute)),
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 2},
								VersionedTransitionOffset: 3,
								PhysicalTaskStatus:        physicalTaskStatusNone,
							},
						},
					},
				},
			},
		},
		"data": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 2},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 3},
				Attributes: &persistencespb.ChasmNodeMetadata_DataAttributes{
					DataAttributes: &persistencespb.ChasmDataAttributes{},
				},
			},
		},
	}

	err = root.ApplyMutation(mutations)
	s.NoError(err)

	s.Equal(expectedNodes, root.Snapshot(nil).Nodes)
}

func (s *nodeSuite) TestGetComponent() {
	root, err := NewTree(
		testComponentSerializedNodes(),
		s.registry,
		s.timeSource,
		s.nodeBackend,
		s.nodePathEncoder,
		s.logger,
	)
	s.NoError(err)

	errValidation := errors.New("some random validation error")
	expectedTestComponent := &TestComponent{}
	setTestComponentFields(expectedTestComponent)
	assertTestComponent := func(component Component) {
		testComponent, ok := component.(*TestComponent)
		s.True(ok)
		protoassert.ProtoEqual(s.T(), expectedTestComponent.ComponentData, testComponent.ComponentData)

		// TODO: Can we assert other fields?
		// Right now the chasm Field generated by setTestComponentFields() doesn't have a backing node.
	}

	testCases := []struct {
		name            string
		chasmContext    Context
		ref             ComponentRef
		expectedErr     error
		valueState      valueState
		assertComponent func(Component)
	}{
		{
			name:         "path not found",
			chasmContext: NewContext(context.Background(), root),
			ref: ComponentRef{
				componentPath: []string{"unknownComponent"},
			},
			expectedErr: errComponentNotFound,
		},
		{
			name:         "initialVT mismatch",
			chasmContext: NewContext(context.Background(), root),
			ref: ComponentRef{
				componentPath: []string{"SubComponent1", "SubComponent11"},
				// should be (1, 1) but we set it to (2, 2)
				componentInitialVT: &persistencespb.VersionedTransition{
					NamespaceFailoverVersion: 2,
					TransitionCount:          2,
				},
			},
			expectedErr: errComponentNotFound,
		},
		{
			name:         "validation failure",
			chasmContext: NewContext(context.Background(), root),
			ref: ComponentRef{
				componentPath: []string{"SubComponent1"},
				componentInitialVT: &persistencespb.VersionedTransition{
					NamespaceFailoverVersion: 1,
					TransitionCount:          1,
				},
				validationFn: func(_ Context, _ Component) error {
					return errValidation
				},
			},
			expectedErr: errValidation,
		},
		{
			name:         "success readonly access",
			chasmContext: NewContext(context.Background(), root),
			ref: ComponentRef{
				componentPath: []string{}, // root
				componentInitialVT: &persistencespb.VersionedTransition{
					NamespaceFailoverVersion: 1,
					TransitionCount:          1,
				},
				validationFn: func(_ Context, _ Component) error {
					return nil
				},
			},
			expectedErr:     nil,
			valueState:      valueStateSynced,
			assertComponent: assertTestComponent,
		},
		{
			name:         "success mutable access",
			chasmContext: NewMutableContext(context.Background(), root),
			ref: ComponentRef{
				componentPath: []string{}, // root
			},
			expectedErr:     nil,
			valueState:      valueStateNeedSerialize,
			assertComponent: assertTestComponent,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			component, err := root.Component(tc.chasmContext, tc.ref)
			s.Equal(tc.expectedErr, err)
			if tc.expectedErr == nil {
				// s.Equal(tc.expectedComponent, component)

				node, ok := root.getNodeByPath(tc.ref.componentPath)
				s.True(ok)
				s.Equal(component, node.value)
				s.Equal(tc.valueState, node.valueState)
			}
		})
	}
}

func (s *nodeSuite) TestSerializeDeserializeTask() {
	payload := &commonpb.Payload{
		Data: []byte("some-random-data"),
	}
	expectedBlob, err := serialization.ProtoEncodeBlob(payload, enumspb.ENCODING_TYPE_PROTO3)
	s.NoError(err)

	testCases := []struct {
		name         string
		task         any
		expectedData []byte
		equalFn      func(t1, t2 any)
	}{
		{
			name: "ProtoTask",
			task: &TestSideEffectTask{
				Data: []byte("some-random-data"),
			},
			expectedData: expectedBlob.GetData(),
			equalFn: func(t1, t2 any) {
				protorequire.ProtoEqual(s.T(), t1.(*TestSideEffectTask), t2.(*TestSideEffectTask))
			},
		},
		{
			name:         "EmptyTask",
			task:         TestOutboundSideEffectTask{},
			expectedData: nil,
			equalFn: func(t1, t2 any) {
				s.IsType(TestOutboundSideEffectTask{}, t1)
				s.IsType(TestOutboundSideEffectTask{}, t2)
				s.Equal(t1, t2)
			},
		},
		{
			name: "StructWithProtoField",
			task: &TestPureTask{
				Payload: payload,
			},
			expectedData: expectedBlob.GetData(),
			equalFn: func(t1, t2 any) {
				protorequire.ProtoEqual(s.T(), t1.(*TestPureTask).Payload, t2.(*TestPureTask).Payload)
			},
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			rt, ok := s.registry.taskFor(tc.task)
			s.True(ok)

			blob, err := serializeTask(rt, tc.task)
			s.NoError(err)

			s.NotNil(blob)
			s.Equal(enumspb.ENCODING_TYPE_PROTO3, blob.GetEncodingType())
			s.Equal(tc.expectedData, blob.GetData())

			deserializedTaskValue, err := deserializeTask(rt, blob)
			s.NoError(err)
			tc.equalFn(tc.task, deserializedTaskValue.Interface())
		})
	}
}

func (s *nodeSuite) TestCloseTransaction_Success() {
	node := s.testComponentTree()
	tv := testvars.New(s.T())

	chasmCtx := NewMutableContext(context.Background(), node)
	tc, err := node.Component(chasmCtx, ComponentRef{componentPath: RootPath})
	s.NoError(err)
	tc.(*TestComponent).SubData1 = NewEmptyField[*protoMessageType]()
	tc.(*TestComponent).ComponentData = &protoMessageType{CreateRequestId: tv.Any().String()}

	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(1)).AnyTimes()
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).AnyTimes()
	s.nodeBackend.EXPECT().GetWorkflowKey().Return(tv.Any().WorkflowKey()).AnyTimes()
	s.nodeBackend.EXPECT().UpdateWorkflowStateStatus(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	mutations, err := node.CloseTransaction()
	s.NoError(err)
	s.Len(mutations.UpdatedNodes, 4)
	s.Contains(mutations.UpdatedNodes, "", "root component must be in UpdatedNodes")
	s.Contains(mutations.UpdatedNodes, "SubComponent1", "SubComponent1 component must be in UpdatedNodes")
	s.Contains(mutations.UpdatedNodes, "SubComponent1/SubComponent11", "SubComponent1/SubComponent11 component must be in UpdatedNodes")
	s.Contains(mutations.UpdatedNodes, "SubComponent1/SubData11", "SubComponent1/SubData11 component must be in UpdatedNodes")
	s.Len(mutations.DeletedNodes, 1)
	s.Contains(mutations.DeletedNodes, "SubData1", "SubData1 was removed and must be in DeletedNodes")

	sc1, err := tc.(*TestComponent).SubComponent1.Get(chasmCtx)
	s.NoError(err)
	s.NotNil(sc1)

	mutations, err = node.CloseTransaction()
	s.NoError(err)
	s.Len(mutations.UpdatedNodes, 1)
	s.Contains(mutations.UpdatedNodes, "SubComponent1", "SubComponent1 component must be in UpdatedNodes")
	s.Empty(mutations.DeletedNodes)
}

func (s *nodeSuite) TestCloseTransaction_EmptyNode() {
	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(1)).Times(1) // for InitialVersionedTransition of the root component.
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).Times(1)

	var nilSerializedNodes map[string]*persistencespb.ChasmNode
	// Create an empty tree.
	node, err := NewTree(nilSerializedNodes, s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger)
	s.NoError(err)
	s.Nil(node.value)

	tv := testvars.New(s.T())

	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(1)).Times(1)
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).Times(1)
	s.nodeBackend.EXPECT().GetWorkflowKey().Return(tv.Any().WorkflowKey())

	mutations, err := node.CloseTransaction()
	s.NoError(err)
	s.Empty(mutations.UpdatedNodes, "there should be no updated nodes because tree was initialized with empty serialized nodes")
	s.Empty(mutations.DeletedNodes, "there should be no deleted nodes because tree was initialized with empty serialized nodes")
}

func (s *nodeSuite) TestCloseTransaction_LifecycleChange() {
	node := s.testComponentTree()
	tv := testvars.New(s.T())

	chasmCtx := NewMutableContext(context.Background(), node)
	_, err := node.Component(chasmCtx, ComponentRef{componentPath: RootPath})
	s.NoError(err)

	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(1)).AnyTimes()
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).AnyTimes()
	s.nodeBackend.EXPECT().GetWorkflowKey().Return(tv.Any().WorkflowKey()).AnyTimes()

	s.nodeBackend.EXPECT().UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	).Return(nil).Times(1)
	_, err = node.CloseTransaction()
	s.NoError(err)

	// Test force terminate case
	_, err = node.Component(chasmCtx, ComponentRef{componentPath: RootPath})
	s.NoError(err)
	node.terminated = true
	s.nodeBackend.EXPECT().UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
	).Return(nil).Times(1)
	_, err = node.CloseTransaction()
	s.NoError(err)
	node.terminated = false

	tc, err := node.Component(chasmCtx, ComponentRef{componentPath: RootPath})
	s.NoError(err)
	tc.(*TestComponent).Complete(chasmCtx)
	s.nodeBackend.EXPECT().UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	).Return(nil).Times(1)
	_, err = node.CloseTransaction()
	s.NoError(err)

	tc, err = node.Component(chasmCtx, ComponentRef{componentPath: RootPath})
	s.NoError(err)
	tc.(*TestComponent).Fail(chasmCtx)
	s.nodeBackend.EXPECT().UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
	).Return(nil).Times(1)
	_, err = node.CloseTransaction()
	s.NoError(err)
}

func (s *nodeSuite) TestCloseTransaction_InvalidateComponentTasks() {
	payload := &commonpb.Payload{
		Data: []byte("some-random-data"),
	}
	taskBlob, err := serialization.ProtoEncodeBlob(payload, enumspb.ENCODING_TYPE_PROTO3)
	s.NoError(err)

	persistenceNodes := map[string]*persistencespb.ChasmNode{
		"": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						Type: "TestLibrary.test_component",
						SideEffectTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								Type:                      "TestLibrary.test_side_effect_task",
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 1,
								Data:                      taskBlob,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
							},
							{
								Type:                      "TestLibrary.test_outbound_side_effect_task",
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 2,
								Data: &commonpb.DataBlob{
									Data:         nil,
									EncodingType: enumspb.ENCODING_TYPE_PROTO3,
								},
								PhysicalTaskStatus: physicalTaskStatusCreated,
							},
						},
						PureTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								Type:                      "TestLibrary.test_pure_task",
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 3,
								Data:                      taskBlob,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
							},
						},
					},
				},
			},
		},
	}
	root, err := NewTree(persistenceNodes, s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger)
	s.NoError(err)

	// The idea is to mark the node as dirty by accessing it with a mutable context.
	// Otherwise, CloseTransaction logic will skip validating tasks for this node.
	// This is a no-op change right now, since we are manually setting the LastUpdateVersionedTransition
	// for the node below.
	// Once CloseTransaction is fully implemented, this will be mark the node as dirty,
	// and CloseTransaction logic will take care of updating LastUpdateVersionedTransition.
	mutableContext := NewMutableContext(context.Background(), root)
	_, err = root.Component(mutableContext, ComponentRef{})
	s.NoError(err)

	// TODO: remove this when CloseTransaction is fully implemented.
	root.serializedNode.Metadata.LastUpdateVersionedTransition = &persistencespb.VersionedTransition{
		TransitionCount: 2,
	}

	rt, ok := s.registry.Task("TestLibrary.test_side_effect_task")
	s.True(ok)
	rt.validator.(*MockTaskValidator[any, *TestSideEffectTask]).EXPECT().
		Validate(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).Times(1)
	rt, ok = s.registry.Task("TestLibrary.test_outbound_side_effect_task")
	s.True(ok)
	rt.validator.(*MockTaskValidator[any, TestOutboundSideEffectTask]).EXPECT().
		Validate(gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).Times(1)
	rt, ok = s.registry.Task("TestLibrary.test_pure_task")
	s.True(ok)
	rt.validator.(*MockTaskValidator[any, *TestPureTask]).EXPECT().
		Validate(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).Times(1)

	err = root.closeTransactionUpdateComponentTasks(&persistencespb.VersionedTransition{TransitionCount: 2})
	s.NoError(err)

	componentAttr := root.serializedNode.Metadata.GetComponentAttributes()
	s.Empty(componentAttr.PureTasks)
	s.Len(componentAttr.SideEffectTasks, 1)
	s.Equal("TestLibrary.test_outbound_side_effect_task", componentAttr.SideEffectTasks[0].GetType())
}

func (s *nodeSuite) TestCloseTransaction_NewComponentTasks() {
	persistenceNodes := map[string]*persistencespb.ChasmNode{
		"": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						Type: "TestLibrary.test_component",
					},
				},
			},
		},
	}
	root, err := NewTree(persistenceNodes, s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger)
	s.NoError(err)

	mutableContext := NewMutableContext(context.Background(), root)
	c, err := root.Component(mutableContext, ComponentRef{})
	s.NoError(err)

	testComponent := c.(*TestComponent)
	err = mutableContext.AddTask(testComponent, TaskAttributes{}, &TestSideEffectTask{
		Data: []byte("some-random-data"),
	})
	s.NoError(err)
	err = mutableContext.AddTask(
		testComponent,
		TaskAttributes{ScheduledTime: s.timeSource.Now()},
		&TestPureTask{
			Payload: &commonpb.Payload{
				Data: []byte("some-random-data"),
			},
		},
	)
	s.NoError(err)

	// TODO: Call CloseTransaction() instead.
	// It's not fully implemented right now, and we have to manually
	// set the lastUpdateVersionedTransition for the updated nodes.
	componentAttr := root.serializedNode.Metadata.GetComponentAttributes()
	root.serializedNode.Metadata.LastUpdateVersionedTransition = &persistencespb.VersionedTransition{
		TransitionCount: 2,
	}
	err = root.closeTransactionUpdateComponentTasks(&persistencespb.VersionedTransition{
		TransitionCount: 2,
	})
	s.NoError(err)

	newSideEffectTask := componentAttr.SideEffectTasks[0]
	newSideEffectTask.Data = nil // This is tested by TestSerializeTask()
	s.Equal(&persistencespb.ChasmComponentAttributes_Task{
		Type:                      "TestLibrary.test_side_effect_task",
		ScheduledTime:             timestamppb.New(time.Time{}),
		VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 2},
		VersionedTransitionOffset: 1,
		PhysicalTaskStatus:        physicalTaskStatusNone,
	}, newSideEffectTask)

	newPureTask := componentAttr.PureTasks[0]
	newPureTask.Data = nil // This is tested by TestSerializeTask()
	s.Equal(&persistencespb.ChasmComponentAttributes_Task{
		Type:                      "TestLibrary.test_pure_task",
		ScheduledTime:             timestamppb.New(s.timeSource.Now()),
		VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 2},
		VersionedTransitionOffset: 2,
		PhysicalTaskStatus:        physicalTaskStatusNone,
	}, newPureTask)
}

func (s *nodeSuite) TestCloseTransaction_GeneratePhysicalSideEffectTasks() {
	persistenceNodes := map[string]*persistencespb.ChasmNode{
		"": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						Type: "TestLibrary.test_component",
						SideEffectTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								Type:                      "TestLibrary.test_side_effect_task",
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 1,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
							},
						},
					},
				},
			},
		},
	}

	mutation := NodesMutation{
		UpdatedNodes: map[string]*persistencespb.ChasmNode{
			"": {
				Metadata: &persistencespb.ChasmNodeMetadata{
					InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
					LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 2},
					Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
						ComponentAttributes: &persistencespb.ChasmComponentAttributes{
							Type: "TestLibrary.test_component",
							SideEffectTasks: []*persistencespb.ChasmComponentAttributes_Task{
								{
									Type:                      "TestLibrary.test_side_effect_task",
									VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
									VersionedTransitionOffset: 1,
									PhysicalTaskStatus:        physicalTaskStatusCreated,
								},
								{
									Type:                      "TestLibrary.test_side_effect_task",
									VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 2},
									VersionedTransitionOffset: 1,
									PhysicalTaskStatus:        physicalTaskStatusNone,
								},
								{
									Type:                      "TestLibrary.test_side_effect_task",
									Destination:               "destination",
									ScheduledTime:             timestamppb.New(TaskScheduledTimeImmediate),
									VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 2},
									VersionedTransitionOffset: 2,
									PhysicalTaskStatus:        physicalTaskStatusNone,
								},
								{
									Type:                      "TestLibrary.test_side_effect_task",
									ScheduledTime:             timestamppb.New(s.timeSource.Now().Add(time.Minute)),
									VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 2},
									VersionedTransitionOffset: 3,
									PhysicalTaskStatus:        physicalTaskStatusNone,
								},
							},
						},
					},
				},
			},
		},
	}

	root, err := NewTree(persistenceNodes, s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger)
	s.NoError(err)

	err = root.ApplyMutation(mutation)
	s.NoError(err)

	s.nodeBackend.EXPECT().GetWorkflowKey().Return(definition.WorkflowKey{
		NamespaceID: "ns-id",
		WorkflowID:  "wf-id",
		RunID:       "run-id",
	}).AnyTimes()

	expectedCategories := []tasks.Category{tasks.CategoryTimer, tasks.CategoryOutbound, tasks.CategoryTransfer}
	for _, category := range expectedCategories {
		s.nodeBackend.EXPECT().AddTasks(gomock.Any()).Do(func(addedTask tasks.Task) {
			s.IsType(&tasks.ChasmTask{}, addedTask)
			s.Equal(category, addedTask.GetCategory())
		})
	}

	err = root.closeTransactionGeneratePhysicalSideEffectTasks()
	s.NoError(err)
}

func (s *nodeSuite) TestCloseTransaction_GeneratePhysicalPureTask() {
	now := s.timeSource.Now()
	persistenceNodes := map[string]*persistencespb.ChasmNode{
		"": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						Type: "TestLibrary.test_component",
						PureTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								Type:                      "TestLibrary.test_pure_task",
								ScheduledTime:             timestamppb.New(now.Add(time.Second)),
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 1,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
							},
						},
					},
				},
			},
		},
		"child": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						Type: "TestLibrary.test_child_component",
						PureTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								Type:                      "TestLibrary.test_pure_task",
								ScheduledTime:             timestamppb.New(now.Add(time.Minute)),
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 2,
								PhysicalTaskStatus:        physicalTaskStatusNone,
							},
						},
					},
				},
			},
		},
	}

	mutation := NodesMutation{
		UpdatedNodes: map[string]*persistencespb.ChasmNode{
			"": {
				Metadata: &persistencespb.ChasmNodeMetadata{
					InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
					LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 2},
					Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
						ComponentAttributes: &persistencespb.ChasmComponentAttributes{
							Type: "TestLibrary.test_component",
							PureTasks: []*persistencespb.ChasmComponentAttributes_Task{
								{
									Type:                      "TestLibrary.test_pure_task",
									ScheduledTime:             timestamppb.New(now.Add(2 * time.Minute)),
									VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 2},
									VersionedTransitionOffset: 1,
									PhysicalTaskStatus:        physicalTaskStatusNone,
								},
							},
						},
					},
				},
			},
		},
	}

	root, err := NewTree(persistenceNodes, s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger)
	s.NoError(err)

	err = root.ApplyMutation(mutation)
	s.NoError(err)

	s.nodeBackend.EXPECT().GetWorkflowKey().Return(definition.WorkflowKey{
		NamespaceID: "ns-id",
		WorkflowID:  "wf-id",
		RunID:       "run-id",
	}).AnyTimes()

	s.nodeBackend.EXPECT().AddTasks(gomock.Any()).Do(func(addedTask tasks.Task) {
		s.IsType(&tasks.ChasmTaskPure{}, addedTask)
		s.Equal(tasks.CategoryTimer, addedTask.GetCategory())
		s.True(now.Add(time.Minute).Equal(addedTask.GetKey().FireTime))
	}).Times(1)

	err = root.closeTransactionGeneratePhysicalPureTask()
	s.NoError(err)

	// Although only root is mutated in ApplyMutation, we generated a pure task for the child node,
	// and need to persist that as well.
	s.Len(root.mutation.UpdatedNodes, 2)
}

func (s *nodeSuite) TestTerminate() {
	node := s.testComponentTree()
	tv := testvars.New(s.T())

	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(1)).AnyTimes()
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).AnyTimes()
	s.nodeBackend.EXPECT().GetWorkflowKey().Return(tv.Any().WorkflowKey()).AnyTimes()

	// First closeTransaction once to make the tree clean.
	s.nodeBackend.EXPECT().UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	).Return(nil).Times(1)

	_, err := node.CloseTransaction()
	s.NoError(err)

	// Then terminate the node and verify only that node will be in the mutation.
	err = node.Terminate(TerminateComponentRequest{})
	s.NoError(err)
	s.True(node.terminated)

	s.nodeBackend.EXPECT().UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
	).Return(nil).Times(1)

	mutations, err := node.CloseTransaction()
	s.NoError(err)
	s.Len(mutations.UpdatedNodes, 1)
	s.Empty(mutations.DeletedNodes)
}

func (s *nodeSuite) preorderAndAssertParent(
	n *Node,
	parent *Node,
) []*persistencespb.ChasmNode {
	s.Equal(parent, n.parent)

	var nodes []*persistencespb.ChasmNode
	nodes = append(nodes, n.serializedNode)

	childNames := make([]string, 0, len(n.children))
	for childName := range n.children {
		childNames = append(childNames, childName)
	}
	sort.Strings(childNames)

	for _, childName := range childNames {
		nodes = append(nodes, s.preorderAndAssertParent(n.children[childName], n)...)
	}

	return nodes
}

type testNodePathEncoder struct{}

var _ NodePathEncoder = (*testNodePathEncoder)(nil)

func (e *testNodePathEncoder) Encode(
	_ *Node,
	path []string,
) (string, error) {
	return strings.Join(path, "/"), nil
}

func (e *testNodePathEncoder) Decode(
	encodedPath string,
) ([]string, error) {
	if encodedPath == "" {
		return []string{}, nil
	}
	return strings.Split(encodedPath, "/"), nil
}

func (s *nodeSuite) nodeBase() *nodeBase {
	return &nodeBase{
		registry:    s.registry,
		timeSource:  s.timeSource,
		backend:     s.nodeBackend,
		pathEncoder: s.nodePathEncoder,
	}
}

// Helper method to create a test tree for TestComponent.
func (s *nodeSuite) testComponentTree() *Node {
	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(1)).Times(1) // for InitialVersionedTransition of the root component.
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).Times(1)

	var nilSerializedNodes map[string]*persistencespb.ChasmNode
	// Create an empty tree.
	node, err := NewTree(nilSerializedNodes, s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger)
	s.NoError(err)
	s.Nil(node.value)

	// Get an empty top-level component from the empty tree.
	err = node.deserialize(reflect.TypeFor[*TestComponent]())
	s.NoError(err)
	s.NotNil(node.value)
	s.IsType(&TestComponent{}, node.value)
	s.Equal(valueStateSynced, node.valueState)

	tc, err := node.Component(NewMutableContext(context.Background(), node), ComponentRef{componentPath: RootPath})
	s.NoError(err)
	s.Equal(valueStateNeedSerialize, node.valueState)
	// Create subcomponents by assigning fields to TestComponent instance.
	setTestComponentFields(tc.(*TestComponent))

	// Sync tree with subcomponents of TestComponent.
	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(1)).Times(4) // for InitialVersionedTransition of children.
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).Times(4)
	err = node.syncSubComponents()
	s.NoError(err)
	s.Empty(node.mutation.DeletedNodes)

	return node // maybe tc too
}
