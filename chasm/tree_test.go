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
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
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
		testLibrary *TestLibrary

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
	s.testLibrary = newTestLibrary(s.controller)

	s.logger = testlogger.NewTestLogger(s.T(), testlogger.FailOnAnyUnexpectedError)
	s.registry = NewRegistry(s.logger)
	err := s.registry.Register(s.testLibrary)
	s.NoError(err)
	err = s.registry.Register(&CoreLibrary{})
	s.NoError(err)

	s.timeSource = clock.NewEventTimeSource()
	s.nodePathEncoder = &testNodePathEncoder{}
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

	root, err := s.newTestTree(persistenceNodes)
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

	needsPointerResolution, err := node.syncSubComponents()
	s.NoError(err)
	s.False(needsPointerResolution)
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

func (s *nodeSuite) TestCollectionAttributes() {
	tv := testvars.New(s.T())

	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(1)).AnyTimes()
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).AnyTimes()
	s.nodeBackend.EXPECT().UpdateWorkflowStateStatus(gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()
	s.nodeBackend.EXPECT().GetWorkflowKey().Return(tv.Any().WorkflowKey()).AnyTimes()

	sc1 := &TestSubComponent1{
		SubComponent1Data: &protoMessageType{
			RunId: tv.WithWorkflowIDNumber(1).WorkflowID(),
		},
	}
	sc2 := &TestSubComponent1{
		SubComponent1Data: &protoMessageType{
			RunId: tv.WithWorkflowIDNumber(2).WorkflowID(),
		},
	}

	type testCase struct {
		name          string
		initComponent func() *TestComponent
		mapField      string
	}
	cases := []testCase{
		{
			name: "of string key",
			initComponent: func() *TestComponent {
				return &TestComponent{
					SubComponents: Map[string, *TestSubComponent1]{
						"SubComponent1": NewComponentField[*TestSubComponent1](nil, sc1),
						"SubComponent2": NewComponentField[*TestSubComponent1](nil, sc2),
					},
				}
			},
			mapField: "SubComponents",
		},
		{
			name: "of int key",
			initComponent: func() *TestComponent {
				return &TestComponent{
					PendingActivities: Map[int, *TestSubComponent1]{
						1: NewComponentField[*TestSubComponent1](nil, sc1),
						2: NewComponentField[*TestSubComponent1](nil, sc2),
					},
				}
			},
			mapField: "PendingActivities",
		},
	}

	for _, tc := range cases {

		var persistedNodes map[string]*persistencespb.ChasmNode

		s.Run("Sync and serialize component with map "+tc.name, func() {
			var nilSerializedNodes map[string]*persistencespb.ChasmNode
			rootNode, err := s.newTestTree(nilSerializedNodes)
			s.NoError(err)

			rootComponent := tc.initComponent()
			rootNode.value = rootComponent
			rootNode.valueState = valueStateNeedSerialize

			mutations, err := rootNode.CloseTransaction()
			s.NoError(err)
			s.Len(mutations.UpdatedNodes, 4, "root, collection, and 2 collection items must be updated")
			s.Empty(mutations.DeletedNodes)

			switch tc.mapField {
			case "SubComponents":
				s.NotEmpty(rootNode.children[tc.mapField].children["SubComponent1"].serializedNode.GetData().GetData())
				s.NotEmpty(rootNode.children[tc.mapField].children["SubComponent2"].serializedNode.GetData().GetData())
			case "PendingActivities":
				s.NotEmpty(rootNode.children[tc.mapField].children["1"].serializedNode.GetData().GetData())
				s.NotEmpty(rootNode.children[tc.mapField].children["2"].serializedNode.GetData().GetData())
			}

			// Save it use in other subtests.
			persistedNodes = common.CloneProtoMap(mutations.UpdatedNodes)
		})

		s.NotNil(persistedNodes)

		s.Run("Deserialize component with map "+tc.name, func() {
			rootNode, err := s.newTestTree(persistedNodes)
			s.NoError(err)

			err = rootNode.deserialize(reflect.TypeFor[*TestComponent]())
			s.NoError(err)

			rootComponent := rootNode.value.(*TestComponent)

			var sc1Field, sc2Field Field[*TestSubComponent1]
			switch tc.mapField {
			case "SubComponents":
				s.NotNil(rootComponent.SubComponents)
				s.Len(rootComponent.SubComponents, 2)
				sc1Field, sc2Field = rootComponent.SubComponents["SubComponent1"], rootComponent.SubComponents["SubComponent2"]
			case "PendingActivities":
				s.NotNil(rootComponent.PendingActivities)
				s.Len(rootComponent.PendingActivities, 2)
				sc1Field, sc2Field = rootComponent.PendingActivities[1], rootComponent.PendingActivities[2]
			}

			chasmContext := NewMutableContext(context.Background(), rootNode)
			sc1Des, err := sc1Field.Get(chasmContext)
			s.NoError(err)
			s.Equal(sc1.SubComponent1Data.GetRunId(), sc1Des.SubComponent1Data.GetRunId())

			sc2Des, err := sc2Field.Get(chasmContext)
			s.NoError(err)
			s.Equal(sc2.SubComponent1Data.GetRunId(), sc2Des.SubComponent1Data.GetRunId())
		})

		s.Run("Clear map "+tc.name+" by setting it to nil", func() {
			rootNode, err := s.newTestTree(persistedNodes)
			s.NoError(err)

			err = rootNode.deserialize(reflect.TypeFor[*TestComponent]())
			s.NoError(err)

			rootComponent := rootNode.value.(*TestComponent)

			rootNode.valueState = valueStateNeedSerialize
			switch tc.mapField {
			case "SubComponents":
				rootComponent.SubComponents = nil
			case "PendingActivities":
				rootComponent.PendingActivities = nil
			}

			mutation, err := rootNode.CloseTransaction()
			s.NoError(err)
			s.Len(mutation.UpdatedNodes, 1, "although root component is not updated, collection is tracked as part of component, therefore root must be updated")
			s.Len(mutation.DeletedNodes, 3, "collection and 2 collection items must be deleted")
		})

		s.Run("Delete single map "+tc.name+" item", func() {
			rootNode, err := s.newTestTree(persistedNodes)
			s.NoError(err)

			err = rootNode.deserialize(reflect.TypeFor[*TestComponent]())
			s.NoError(err)

			rootComponent := rootNode.value.(*TestComponent)

			// Delete collection item 1.
			rootNode.valueState = valueStateNeedSerialize
			switch tc.mapField {
			case "SubComponents":
				delete(rootComponent.SubComponents, "SubComponent1")
			case "PendingActivities":
				delete(rootComponent.PendingActivities, 1)
			}

			mutation, err := rootNode.CloseTransaction()
			s.NoError(err)
			s.Len(mutation.UpdatedNodes, 1, "although root component is not updated, collection is tracked as part of component, therefore root must be updated")
			s.Len(mutation.DeletedNodes, 1, "collection item 1 must be deleted")
		})

		s.Run("Clear map "+tc.name+" by deleting all items", func() {
			rootNode, err := s.newTestTree(persistedNodes)
			s.NoError(err)

			err = rootNode.deserialize(reflect.TypeFor[*TestComponent]())
			s.NoError(err)

			rootComponent := rootNode.value.(*TestComponent)

			// Delete both collection items.
			rootNode.valueState = valueStateNeedSerialize
			switch tc.mapField {
			case "SubComponents":
				delete(rootComponent.SubComponents, "SubComponent1")
				delete(rootComponent.SubComponents, "SubComponent2")
			case "PendingActivities":
				delete(rootComponent.PendingActivities, 1)
				delete(rootComponent.PendingActivities, 2)
			}

			// Now map is empty and must be deleted.
			mutation, err := rootNode.CloseTransaction()
			s.NoError(err)
			s.Len(mutation.UpdatedNodes, 1, "although root component is not updated, collection is tracked as part of component, therefore root must be updated")
			s.Len(mutation.DeletedNodes, 3, "collection and 2 items must be deleted")
		})
	}
}

func (s *nodeSuite) TestPointerAttributes() {
	tv := testvars.New(s.T())

	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(1)).AnyTimes()
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).AnyTimes()
	s.nodeBackend.EXPECT().UpdateWorkflowStateStatus(gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()
	s.nodeBackend.EXPECT().GetWorkflowKey().Return(tv.Any().WorkflowKey()).AnyTimes()

	var persistedNodes map[string]*persistencespb.ChasmNode

	sc11 := &TestSubComponent11{
		SubComponent11Data: &protoMessageType{
			RunId: tv.WithWorkflowIDNumber(11).WorkflowID(),
		},
	}

	sc1 := &TestSubComponent1{
		SubComponent1Data: &protoMessageType{
			RunId: tv.WithWorkflowIDNumber(1).WorkflowID(),
		},
		SubComponent11: NewComponentField[*TestSubComponent11](nil, sc11),
	}

	s.Run("Sync and serialize component with pointer", func() {
		var nilSerializedNodes map[string]*persistencespb.ChasmNode
		rootNode, err := s.newTestTree(nilSerializedNodes)
		s.NoError(err)

		rootComponent := &TestComponent{
			SubComponent1:                NewComponentField[*TestSubComponent1](nil, sc1),
			SubComponentInterfacePointer: NewComponentField[Component](nil, sc1),
		}

		rootNode.value = rootComponent
		rootNode.valueState = valueStateNeedSerialize

		ctx := NewMutableContext(context.Background(), rootNode)
		rootComponent.SubComponent11Pointer = ComponentPointerTo(ctx, sc11)
		s.Equal(fieldTypeDeferredPointer, rootComponent.SubComponent11Pointer.Internal.ft)

		mutations, err := rootNode.CloseTransaction()
		s.NoError(err)
		s.Len(mutations.UpdatedNodes, 5, "root, SubComponent1, SubComponent11, SubComponent11Pointer, and SubComponentInterfacePointer must be updated")
		s.Empty(mutations.DeletedNodes)

		s.Equal([]string{"SubComponent1", "SubComponent11"}, rootNode.children["SubComponent11Pointer"].serializedNode.GetMetadata().GetPointerAttributes().GetNodePath())

		// Save it use in other subtests.
		persistedNodes = common.CloneProtoMap(mutations.UpdatedNodes)
	})

	s.NotNil(persistedNodes)

	s.Run("Deserialize pointer component", func() {
		rootNode, err := s.newTestTree(persistedNodes)
		s.NoError(err)

		err = rootNode.deserialize(reflect.TypeFor[*TestComponent]())
		s.NoError(err)

		rootComponent := rootNode.value.(*TestComponent)

		chasmContext := NewMutableContext(context.Background(), rootNode)
		sc11Des, err := rootComponent.SubComponent11Pointer.Get(chasmContext)
		s.NoError(err)
		s.NotNil(sc11Des)
		s.Equal(sc11.SubComponent11Data.GetRunId(), sc11Des.SubComponent11Data.GetRunId())

		ifacePtr, err := rootComponent.SubComponentInterfacePointer.Get(chasmContext)
		s.NoError(err)
		s.NotNil(ifacePtr)

		sc1ptr, ok := ifacePtr.(*TestSubComponent1)
		s.True(ok)
		s.ProtoEqual(sc1ptr.SubComponent1Data, sc1.SubComponent1Data)
	})

	s.Run("Clear pointer by setting it to the empty field", func() {
		rootNode, err := s.newTestTree(persistedNodes)
		s.NoError(err)

		err = rootNode.deserialize(reflect.TypeFor[*TestComponent]())
		s.NoError(err)

		rootComponent := rootNode.value.(*TestComponent)

		rootComponent.SubComponent11Pointer = NewEmptyField[*TestSubComponent11]()

		mutation, err := rootNode.CloseTransaction()
		s.NoError(err)
		s.Empty(mutation.UpdatedNodes, "no nodes should be updated")
		s.Len(mutation.DeletedNodes, 1, "SubComponent11Pointer must be deleted")
	})
}

func (s *nodeSuite) TestSyncSubComponents_DeleteLeafNode() {
	node := s.testComponentTree()

	component := node.value.(*TestComponent)

	// Set very leaf node to empty.
	component.SubComponent1.Internal.v.(*TestSubComponent1).SubComponent11 = NewEmptyField[*TestSubComponent11]()
	s.NotNil(node.children["SubComponent1"].children["SubComponent11"])

	needsPointerResolution, err := node.syncSubComponents()
	s.NoError(err)
	s.False(needsPointerResolution)

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

	needsPointerResolution, err := node.syncSubComponents()
	s.NoError(err)
	s.False(needsPointerResolution)

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

	node, err := s.newTestTree(serializedNodes)
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

	// nil component data should decode into zero value
	s.NotNil(tc.ComponentData)
	s.ProtoEqual(&protoMessageType{}, tc.ComponentData)
}

func (s *nodeSuite) TestDeserializeNode_ComponentAttributes() {
	serializedNodes := testComponentSerializedNodes()

	node, err := s.newTestTree(serializedNodes)
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

	node, err := s.newTestTree(serializedNodes)
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
	node, err := s.newTestTree(serializedNodes)
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

	root, err := s.newTestTree(persistenceNodes)
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
	root, err := s.newTestTree(persistenceNodes)
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
	root, err := s.newTestTree(persistenceNodes)
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

func (s *nodeSuite) TestApply_OutOfOrder() {
	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(1)).AnyTimes()
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(0)).AnyTimes()

	root, err := s.newTestTree(nil)
	s.NoError(err)

	// Test the case where child node is applied before parent node.
	err = root.ApplySnapshot(NodesSnapshot{
		Nodes: map[string]*persistencespb.ChasmNode{
			"child/grandchild": {
				Metadata: &persistencespb.ChasmNodeMetadata{
					InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 2},
					LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 20},
				},
			},
		},
	})
	s.NoError(err)

	err = root.ApplyMutation(NodesMutation{
		UpdatedNodes: map[string]*persistencespb.ChasmNode{
			"": {
				Metadata: &persistencespb.ChasmNodeMetadata{
					InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
					LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				},
			},
			"child": {
				Metadata: &persistencespb.ChasmNodeMetadata{
					InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
					LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				},
			},
		},
	})
	s.NoError(err)

	snapshot := root.Snapshot(nil)
	s.Len(snapshot.Nodes, 3)
	s.Len(root.mutation.UpdatedNodes, 3)
}

func (s *nodeSuite) TestRefreshTasks() {
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
								ScheduledTime:             timestamppb.New(now.Add(time.Minute)),
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 1,
								PhysicalTaskStatus:        physicalTaskStatusNone,
							},
						},
					},
				},
			},
		},
		"child1": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						Type: "TestLibrary.test_child_component",
						PureTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								Type:                      "TestLibrary.test_pure_task",
								ScheduledTime:             timestamppb.New(now.Add(time.Second)),
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 2,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
							},
						},
					},
				},
			},
		},
		"child2": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						Type: "TestLibrary.test_child_component",
						SideEffectTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								Type:                      "TestLibrary.test_side_effect_task",
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 3,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
							},
						},
					},
				},
			},
		},
	}

	root, err := s.newTestTree(persistenceNodes)
	s.NoError(err)

	err = root.RefreshTasks()
	s.NoError(err)

	s.True(root.IsDirty())
	s.False(root.IsStateDirty())

	s.nodeBackend.EXPECT().GetWorkflowKey().Return(definition.WorkflowKey{
		NamespaceID: "ns-id",
		WorkflowID:  "wf-id",
		RunID:       "run-id",
	}).AnyTimes()
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(0)).AnyTimes()
	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(2)).AnyTimes()

	addedTasks := 0
	s.nodeBackend.EXPECT().AddTasks(gomock.Any()).Do(func(addedTask tasks.Task) {
		addedTasks++
	}).AnyTimes()

	mutation, err := root.CloseTransaction()
	s.NoError(err)
	s.Len(mutation.UpdatedNodes, 2) // TaskStatus for the root node is not reset, so no need to persist it.
	s.Equal(2, addedTasks)
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
	root, err := s.newTestTree(persistenceNodes)
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

func (s *nodeSuite) TestValidateAccess() {
	nodePath := []string{"SubComponent1", "SubComponent11"}

	// Because access checks are performed on ancestor nodes and not the target node,
	// test case properties are applied to the root node.
	testCases := []struct {
		name            string
		valid           bool
		intent          OperationIntent
		lifecycleStatus enumspb.WorkflowExecutionStatus // TestComponent borrows the WorkflowExecutionStatus struct
		terminated      bool

		setup func(*Node, Context) error
	}{
		{
			name:            "access check applies only to ancestors",
			valid:           true,
			intent:          OperationIntentProgress,
			lifecycleStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			terminated:      false,
			setup: func(target *Node, _ Context) error {
				// Set the terminated flag on the target node instead of an ancestor
				target.terminated = true
				return nil
			},
		},
		{
			name:            "read-only always succeeds",
			intent:          OperationIntentObserve,
			lifecycleStatus: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			terminated:      true,
			valid:           true,
		},
		{
			name:            "valid write access",
			intent:          OperationIntentProgress,
			lifecycleStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			terminated:      false,
			valid:           true,
		},
		{
			name:            "invalid write access (parent closed)",
			intent:          OperationIntentProgress,
			lifecycleStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			terminated:      false,
			valid:           false,
		},
		{
			name:            "invalid write access (component terminated)",
			intent:          OperationIntentProgress,
			lifecycleStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			terminated:      true,
			valid:           false,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			root, err := s.newTestTree(testComponentSerializedNodes())
			s.NoError(err)

			ctx := NewContext(
				newContextWithOperationIntent(context.Background(), tc.intent),
				root,
			)

			// Set fields on root node
			err = root.prepareComponentValue(ctx)
			s.NoError(err)
			root.terminated = tc.terminated
			component, ok := root.value.(*TestComponent)
			if ok {
				component.ComponentData.Status = tc.lifecycleStatus
			}

			// Find target node
			node, ok := root.findNode(nodePath)
			s.True(ok)
			err = node.prepareComponentValue(ctx)
			s.NoError(err)

			if tc.setup != nil {
				s.NoError(tc.setup(node, ctx))
			}

			// Validation always begins on the target node's parent.
			parent := node.parent
			s.NotNil(parent)
			err = parent.validateAccess(ctx)
			if tc.valid {
				s.NoError(err)
			} else {
				s.Error(err)
				s.ErrorIs(errAccessCheckFailed, err)
			}
		})
	}

}

func (s *nodeSuite) TestGetComponent() {
	root, err := s.newTestTree(testComponentSerializedNodes())
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
			name:         "archetype mismatch",
			chasmContext: NewContext(context.Background(), root),
			ref: ComponentRef{
				archetype: "TestLibrary.test_sub_component_1",
			},
			expectedErr: errComponentNotFound,
		},
		{
			name:         "entityGoType mismatch",
			chasmContext: NewContext(context.Background(), root),
			ref: ComponentRef{
				entityGoType: reflect.TypeFor[*TestSubComponent2](),
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
				validationFn: func(_ NodeBackend, _ Context, _ Component) error {
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
				validationFn: func(_ NodeBackend, _ Context, _ Component) error {
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

				node, ok := root.findNode(tc.ref.componentPath)
				s.True(ok)
				s.Equal(component, node.value)
				s.Equal(tc.valueState, node.valueState)
			}
		})
	}
}

func (s *nodeSuite) TestRef() {
	root, err := s.newTestTree(testComponentSerializedNodes())
	s.NoError(err)

	tv := testvars.New(s.T())
	workflowKey := tv.Any().WorkflowKey()
	entityKey := EntityKey{
		NamespaceID: workflowKey.NamespaceID,
		BusinessID:  workflowKey.WorkflowID,
		EntityID:    workflowKey.RunID,
	}
	currentVT := &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: 2,
		TransitionCount:          2,
	}

	s.nodeBackend.EXPECT().CurrentVersionedTransition().Return(currentVT).AnyTimes()
	s.nodeBackend.EXPECT().GetWorkflowKey().Return(workflowKey).AnyTimes()

	chasmContext := NewContext(context.Background(), root)
	rootComponent, err := root.Component(chasmContext, NewComponentRef[*TestComponent](entityKey))
	s.NoError(err)
	testComponent, ok := rootComponent.(*TestComponent)
	s.True(ok)

	rc, ok := s.registry.ComponentFor(testComponent)
	s.True(ok)
	archetype := Archetype(rc.FqType())

	subComponent1, err := testComponent.SubComponent1.Get(chasmContext)
	s.NoError(err)
	subComponent11, err := subComponent1.SubComponent11.Get(chasmContext)
	s.NoError(err)

	testCases := []struct {
		name             string
		component        Component
		expectErr        bool
		expectedPath     []string
		expectedInitalVT *persistencespb.VersionedTransition
	}{
		{
			name:         "root",
			component:    testComponent,
			expectErr:    false,
			expectedPath: nil, // same as []string{}
			expectedInitalVT: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 1,
				TransitionCount:          1,
			},
		},
		{
			name:         "subComponent1",
			component:    subComponent1,
			expectErr:    false,
			expectedPath: []string{"SubComponent1"},
			expectedInitalVT: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 1,
				TransitionCount:          1,
			},
		},
		{
			name:         "subComponent11",
			component:    subComponent11,
			expectErr:    false,
			expectedPath: []string{"SubComponent1", "SubComponent11"},
			expectedInitalVT: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 1,
				TransitionCount:          1,
			},
		},
		{
			name:      "unknown",
			component: &TestComponent{}, // a new instance of TestComponent
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {

			encodedRef, err := root.Ref(tc.component)
			if tc.expectErr {
				s.Error(err)
				return
			}

			s.NoError(err)
			expectedRef := ComponentRef{
				EntityKey:     entityKey,
				archetype:     archetype,
				componentPath: tc.expectedPath,

				// Proto fields are validated separately with ProtoEqual.
				// entityLastUpdateVT: currentVT,
				// componentInitialVT: tc.expectedInitalVT,
			}

			actualRef, err := DeserializeComponentRef(encodedRef)
			s.NoError(err)
			s.ProtoEqual(currentVT, actualRef.entityLastUpdateVT)
			s.ProtoEqual(tc.expectedInitalVT, actualRef.componentInitialVT)

			actualRef.entityLastUpdateVT = nil
			actualRef.componentInitialVT = nil
			s.Equal(expectedRef, actualRef)
		})
	}
}

func (s *nodeSuite) TestSerializeDeserializeTask() {
	payload := &commonpb.Payload{
		Data: []byte("some-random-data"),
	}
	expectedBlob, err := serialization.ProtoEncode(payload)
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
	tc, err := node.Component(chasmCtx, ComponentRef{componentPath: rootPath})
	s.NoError(err)
	tc.(*TestComponent).SubData1 = NewEmptyField[*protoMessageType]()
	tc.(*TestComponent).ComponentData = &protoMessageType{CreateRequestId: tv.Any().String()}

	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(1)).AnyTimes()
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).AnyTimes()
	s.nodeBackend.EXPECT().GetWorkflowKey().Return(tv.Any().WorkflowKey()).AnyTimes()
	s.nodeBackend.EXPECT().UpdateWorkflowStateStatus(gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()

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
	node, err := s.newTestTree(nilSerializedNodes)
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
	_, err := node.Component(chasmCtx, ComponentRef{componentPath: rootPath})
	s.NoError(err)

	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(1)).AnyTimes()
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).AnyTimes()
	s.nodeBackend.EXPECT().GetWorkflowKey().Return(tv.Any().WorkflowKey()).AnyTimes()

	s.nodeBackend.EXPECT().UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	).Return(true, nil).Times(1)
	_, err = node.CloseTransaction()
	s.NoError(err)

	// Test force terminate case
	_, err = node.Component(chasmCtx, ComponentRef{componentPath: rootPath})
	s.NoError(err)
	node.terminated = true
	s.nodeBackend.EXPECT().UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
	).Return(true, nil).Times(1)
	_, err = node.CloseTransaction()
	s.NoError(err)
	node.terminated = false

	tc, err := node.Component(chasmCtx, ComponentRef{componentPath: rootPath})
	s.NoError(err)
	tc.(*TestComponent).Complete(chasmCtx)
	s.nodeBackend.EXPECT().UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	).Return(true, nil).Times(1)
	_, err = node.CloseTransaction()
	s.NoError(err)

	tc, err = node.Component(chasmCtx, ComponentRef{componentPath: rootPath})
	s.NoError(err)
	tc.(*TestComponent).Fail(chasmCtx)
	s.nodeBackend.EXPECT().UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
	).Return(true, nil).Times(1)
	_, err = node.CloseTransaction()
	s.NoError(err)
}

func (s *nodeSuite) TestCloseTransaction_ForceUpdateVisibility() {
	node := s.testComponentTree()
	tv := testvars.New(s.T())

	chasmCtx := NewMutableContext(context.Background(), node)
	testComponent, err := node.Component(chasmCtx, ComponentRef{componentPath: rootPath})
	s.NoError(err)

	nextTransitionCount := int64(1)
	s.nodeBackend.EXPECT().NextTransitionCount().DoAndReturn(func() int64 {
		return nextTransitionCount
	}).AnyTimes()
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).AnyTimes()
	s.nodeBackend.EXPECT().GetWorkflowKey().Return(tv.Any().WorkflowKey()).AnyTimes()
	s.nodeBackend.EXPECT().AddTasks(gomock.Any()).AnyTimes()

	// Init visiblity component
	testComponent.(*TestComponent).Visibility = NewComponentField(chasmCtx, NewVisibility(chasmCtx))
	s.nodeBackend.EXPECT().UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	).Return(true, nil).Times(1)
	mutation, err := node.CloseTransaction()
	s.NoError(err)
	pVisibilityNode, ok := mutation.UpdatedNodes["Visibility"]
	s.True(ok)
	s.Len(pVisibilityNode.GetMetadata().GetComponentAttributes().SideEffectTasks, 1)

	// Some change unrelated to visibility
	// Visibility component should not be updated.
	nextTransitionCount = 2
	testComponent, err = node.Component(chasmCtx, ComponentRef{componentPath: rootPath})
	s.NoError(err)
	testComponent.(*TestComponent).ComponentData = &protoMessageType{
		CreateRequestId: "some-updated-component-data",
	}
	s.nodeBackend.EXPECT().UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	).Return(false, nil).Times(1)
	mutation, err = node.CloseTransaction()
	s.NoError(err)
	_, ok = mutation.UpdatedNodes["Visibility"]
	s.False(ok)

	// Close the run, visibility should be force updated
	// even if not explicitly updated.
	nextTransitionCount = 3
	testComponent, err = node.Component(chasmCtx, ComponentRef{componentPath: rootPath})
	s.NoError(err)
	testComponent.(*TestComponent).Complete(chasmCtx)
	s.nodeBackend.EXPECT().UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	).Return(true, nil).Times(1)
	mutation, err = node.CloseTransaction()
	s.NoError(err)
	pVisibilityNode, ok = mutation.UpdatedNodes["Visibility"]
	s.True(ok)
	s.Len(pVisibilityNode.GetMetadata().GetComponentAttributes().SideEffectTasks, 1)
}

func (s *nodeSuite) TestCloseTransaction_InvalidateComponentTasks() {
	payload := &commonpb.Payload{
		Data: []byte("some-random-data"),
	}
	taskBlob, err := serialization.ProtoEncode(payload)
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
	root, err := s.newTestTree(persistenceNodes)
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

	s.testLibrary.mockSideEffectTaskValidator.EXPECT().
		Validate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).Times(1)
	s.testLibrary.mockOutboundSideEffectTaskValidator.EXPECT().
		Validate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).Times(1)
	s.testLibrary.mockPureTaskValidator.EXPECT().
		Validate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).Times(1)

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
		"SubComponent1": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						Type: "TestLibrary.test_sub_component_1",
					},
				},
			},
		},
		"SubComponent2": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						Type: "TestLibrary.test_sub_component_2",
					},
				},
			},
		},
	}
	root, err := s.newTestTree(persistenceNodes)
	s.NoError(err)

	mutableContext := NewMutableContext(context.Background(), root)
	c, err := root.Component(mutableContext, ComponentRef{})
	s.NoError(err)

	// Add a valid side effect task.
	s.testLibrary.mockSideEffectTaskValidator.EXPECT().
		Validate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).Times(1)
	testComponent := c.(*TestComponent)
	mutableContext.AddTask(testComponent, TaskAttributes{}, &TestSideEffectTask{
		Data: []byte("some-random-data"),
	})

	// Add an invalid outbound side effect task.
	// the invalid task should not be created.
	s.testLibrary.mockOutboundSideEffectTaskValidator.EXPECT().
		Validate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).Times(1)
	mutableContext.AddTask(
		testComponent,
		TaskAttributes{Destination: "destination"},
		TestOutboundSideEffectTask{},
	)

	// Add a valid pure task.
	s.testLibrary.mockPureTaskValidator.EXPECT().
		Validate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).Times(1)
	mutableContext.AddTask(
		testComponent,
		TaskAttributes{ScheduledTime: s.timeSource.Now()},
		&TestPureTask{
			Payload: &commonpb.Payload{
				Data: []byte("valid-pure-task"),
			},
		},
	)

	// Add an invalid pure task.
	// the invalid task should not be created.
	s.testLibrary.mockPureTaskValidator.EXPECT().
		Validate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).Times(1)
	mutableContext.AddTask(
		testComponent,
		TaskAttributes{ScheduledTime: s.timeSource.Now()},
		&TestPureTask{
			Payload: &commonpb.Payload{
				Data: []byte("invalid-pure-task"),
			},
		},
	)

	// Add a valid side effect task to a sub-component.
	s.testLibrary.mockSideEffectTaskValidator.EXPECT().
		Validate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).Times(1)
	subComponent2, err := testComponent.SubComponent2.Get(mutableContext)
	s.NoError(err)
	mutableContext.AddTask(subComponent2, TaskAttributes{}, &TestSideEffectTask{
		Data: []byte("some-random-data"),
	})

	s.nodeBackend.EXPECT().GetWorkflowKey().Return(definition.WorkflowKey{
		NamespaceID: "ns-id",
		WorkflowID:  "wf-id",
		RunID:       "run-id",
	}).AnyTimes()
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(0)).AnyTimes()
	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(2)).AnyTimes()
	s.nodeBackend.EXPECT().UpdateWorkflowStateStatus(gomock.Any(), gomock.Any()).Return(false, nil).Times(1)
	s.nodeBackend.EXPECT().AddTasks(gomock.Any()).AnyTimes()
	mutation, err := root.CloseTransaction()
	s.NoError(err)

	rootAttr := mutation.UpdatedNodes[""].GetMetadata().GetComponentAttributes()
	s.Len(rootAttr.SideEffectTasks, 1) // Only one valid side effect task.
	newSideEffectTask := rootAttr.SideEffectTasks[0]
	newSideEffectTask.Data = nil // This is tested by TestSerializeTask()
	s.Equal(&persistencespb.ChasmComponentAttributes_Task{
		Type:                      "TestLibrary.test_side_effect_task",
		ScheduledTime:             timestamppb.New(time.Time{}),
		VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 2},
		VersionedTransitionOffset: 1,
		PhysicalTaskStatus:        physicalTaskStatusCreated,
	}, newSideEffectTask)

	s.Len(rootAttr.PureTasks, 1) // Only one valid side effect task.
	newPureTask := rootAttr.PureTasks[0]
	newPureTask.Data = nil // This is tested by TestSerializeTask()
	s.Equal(&persistencespb.ChasmComponentAttributes_Task{
		Type:                      "TestLibrary.test_pure_task",
		ScheduledTime:             timestamppb.New(s.timeSource.Now()),
		VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 2},
		VersionedTransitionOffset: 2,
		PhysicalTaskStatus:        physicalTaskStatusCreated,
	}, newPureTask)

	subComponent2Attr := mutation.UpdatedNodes["SubComponent2"].GetMetadata().GetComponentAttributes()
	newSideEffectTask = subComponent2Attr.SideEffectTasks[0]
	newSideEffectTask.Data = nil // This is tested by TestSerializeTask()
	s.Equal(&persistencespb.ChasmComponentAttributes_Task{
		Type:                      "TestLibrary.test_side_effect_task",
		ScheduledTime:             timestamppb.New(time.Time{}),
		VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 2},
		VersionedTransitionOffset: 3,
		PhysicalTaskStatus:        physicalTaskStatusCreated,
	}, newSideEffectTask)
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

	root, err := s.newTestTree(persistenceNodes)
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

	root, err := s.newTestTree(persistenceNodes)
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
	).Return(false, nil).Times(1)

	_, err := node.CloseTransaction()
	s.NoError(err)

	// Then terminate the node and verify only that node will be in the mutation.
	err = node.Terminate(TerminateComponentRequest{})
	s.NoError(err)
	s.True(node.terminated)

	s.nodeBackend.EXPECT().UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
	).Return(true, nil).Times(1)

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
		return rootPath, nil
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
	node, err := s.newTestTree(nilSerializedNodes)
	s.NoError(err)
	s.Nil(node.value)

	// Get an empty top-level component from the empty tree.
	err = node.deserialize(reflect.TypeFor[*TestComponent]())
	s.NoError(err)
	s.NotNil(node.value)
	s.IsType(&TestComponent{}, node.value)
	s.Equal(valueStateSynced, node.valueState)

	tc, err := node.Component(NewMutableContext(context.Background(), node), ComponentRef{componentPath: rootPath})
	s.NoError(err)
	s.Equal(valueStateNeedSerialize, node.valueState)
	// Create subcomponents by assigning fields to TestComponent instance.
	setTestComponentFields(tc.(*TestComponent))

	// Sync tree with subcomponents of TestComponent.
	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(1)).Times(4) // for InitialVersionedTransition of children.
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).Times(4)
	needsPointerResolution, err := node.syncSubComponents()
	s.False(needsPointerResolution)
	s.NoError(err)
	s.Empty(node.mutation.DeletedNodes)

	return node // maybe tc too
}

func (s *nodeSuite) TestEachPureTask() {
	now := s.timeSource.Now()

	payload := &commonpb.Payload{
		Data: []byte("some-random-data"),
	}
	taskBlob, err := serialization.ProtoEncode(payload)
	s.NoError(err)

	// Set up a tree with expired and unexpired pure tasks.
	persistenceNodes := map[string]*persistencespb.ChasmNode{
		"": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						Type: "TestLibrary.test_component",
						PureTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								// Expired
								Type:                      "TestLibrary.test_pure_task",
								ScheduledTime:             timestamppb.New(now),
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 1,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
								Data:                      taskBlob,
							},
						},
					},
				},
			},
		},
		"child": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						Type: "TestLibrary.test_component",
						PureTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								Type: "TestLibrary.test_pure_task",
								// Unexpired
								ScheduledTime:             timestamppb.New(now.Add(time.Hour)),
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 1,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
								Data:                      taskBlob,
							},
						},
					},
				},
			},
		},
		"child/grandchild1": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						Type: "TestLibrary.test_component",
						PureTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								Type: "TestLibrary.test_pure_task",
								// Expired, and physical task not created
								ScheduledTime:             timestamppb.New(now),
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 2,
								PhysicalTaskStatus:        physicalTaskStatusNone,
								Data:                      taskBlob,
							},
							{
								Type: "TestLibrary.test_pure_task",
								// Expired
								ScheduledTime:             timestamppb.New(now),
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 1,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
								Data:                      taskBlob,
							},
						},
					},
				},
			},
		},
	}

	root, err := s.newTestTree(persistenceNodes)
	s.NoError(err)
	s.NotNil(root)

	actualTaskCount := 0
	err = root.EachPureTask(now.Add(time.Minute), func(executor NodePureTask, taskAttributes TaskAttributes, task any) error {
		s.NotNil(executor)
		s.NotNil(taskAttributes)

		_, ok := task.(*TestPureTask)
		s.True(ok)

		actualTaskCount += 1
		return nil
	})
	s.NoError(err)
	s.Equal(3, actualTaskCount)
}

func (s *nodeSuite) TestExecutePureTask() {
	persistenceNodes := map[string]*persistencespb.ChasmNode{
		"": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						Type: "TestLibrary.test_component",
					},
				},
			},
		},
	}

	taskAttributes := TaskAttributes{}
	pureTask := &TestPureTask{
		Payload: &commonpb.Payload{
			Data: []byte("some-random-data"),
		},
	}

	root, err := s.newTestTree(persistenceNodes)
	s.NoError(err)
	s.NotNil(root)
	ctx := context.Background()

	expectExecute := func(result error) {
		s.testLibrary.mockPureTaskExecutor.EXPECT().
			Execute(
				gomock.AssignableToTypeOf(&MutableContextImpl{}),
				gomock.AssignableToTypeOf(&TestComponent{}),
				gomock.Eq(taskAttributes),
				gomock.Eq(pureTask),
			).Return(result).Times(1)
	}

	expectValidate := func(retValue bool, errValue error) {
		s.testLibrary.mockPureTaskValidator.EXPECT().
			Validate(gomock.Any(), gomock.Any(), gomock.Eq(taskAttributes), gomock.Any()).Return(retValue, errValue).Times(1)
	}

	// Succeed task execution and validation (happy case).
	expectExecute(nil)
	expectValidate(true, nil)
	err = root.ExecutePureTask(ctx, taskAttributes, pureTask)
	s.NoError(err)

	expectedErr := errors.New("dummy")

	// Succeed validation, fail execution.
	expectExecute(expectedErr)
	expectValidate(true, nil)
	err = root.ExecutePureTask(ctx, taskAttributes, pureTask)
	s.ErrorIs(expectedErr, err)

	// Fail task validation (no execution occurs).
	expectValidate(false, nil)
	err = root.ExecutePureTask(ctx, taskAttributes, pureTask)
	s.NoError(err)

	// Error during task validation (no execution occurs).
	expectValidate(false, expectedErr)
	err = root.ExecutePureTask(ctx, taskAttributes, pureTask)
	s.ErrorIs(expectedErr, err)
}

func (s *nodeSuite) TestExecuteSideEffectTask() {
	persistenceNodes := map[string]*persistencespb.ChasmNode{
		"": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						Type: "TestLibrary.test_component",
					},
				},
			},
		},
	}

	taskAttributes := TaskAttributes{}

	taskInfo := &persistencespb.ChasmTaskInfo{
		ComponentInitialVersionedTransition: &persistencespb.VersionedTransition{
			TransitionCount: 1,
		},
		ComponentLastUpdateVersionedTransition: &persistencespb.VersionedTransition{
			TransitionCount: 1,
		},
		Path: "",
		Type: "TestLibrary.test_side_effect_task",
		Data: &commonpb.DataBlob{
			Data:         nil,
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		},
	}
	entityKey := EntityKey{}

	root, err := s.newTestTree(persistenceNodes)
	s.NoError(err)
	s.NotNil(root)

	mockEngine := NewMockEngine(s.controller)
	ctx := NewEngineContext(context.Background(), mockEngine)

	chasmContext := NewMutableContext(ctx, root)
	var backendValidtionFnCalled bool
	// This won't be called until access time.
	dummyValidationFn := func(_ NodeBackend, _ Context, _ Component) error {
		backendValidtionFnCalled = true
		return nil
	}
	expectValidate := func(valid bool, validationErr error) {
		backendValidtionFnCalled = false
		s.testLibrary.mockSideEffectTaskValidator.EXPECT().Validate(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).Return(valid, validationErr).Times(1)
	}
	expectExecute := func(result error) {
		s.testLibrary.mockSideEffectTaskExecutor.EXPECT().
			Execute(
				gomock.Any(),
				gomock.Any(),
				gomock.Eq(taskAttributes),
				gomock.Any(),
			).DoAndReturn(
			func(_ context.Context, ref ComponentRef, _ TaskAttributes, _ *TestSideEffectTask) error {
				s.NotNil(ref.validationFn)

				// Accessing the Component should trigger the validationFn.
				_, err := root.Component(chasmContext, ref)
				if err != nil {
					return err
				}
				return result
			}).Times(1)
	}

	// Succeed task execution.
	expectValidate(true, nil)
	expectExecute(nil)
	err = root.ExecuteSideEffectTask(ctx, s.registry, entityKey, taskAttributes, taskInfo, dummyValidationFn)
	s.NoError(err)
	s.True(backendValidtionFnCalled)

	// Invalid task.
	expectValidate(false, nil)
	expectExecute(nil)
	err = root.ExecuteSideEffectTask(ctx, s.registry, entityKey, taskAttributes, taskInfo, dummyValidationFn)
	s.Error(err)
	s.IsType(&serviceerror.NotFound{}, err)

	// Failed to validate task.
	validationErr := errors.New("validation error")
	expectValidate(false, validationErr)
	expectExecute(nil)
	err = root.ExecuteSideEffectTask(ctx, s.registry, entityKey, taskAttributes, taskInfo, dummyValidationFn)
	s.ErrorIs(validationErr, err)

	// Fail task execution.
	expectValidate(true, nil)
	executionErr := errors.New("execution error")
	expectExecute(executionErr)
	err = root.ExecuteSideEffectTask(ctx, s.registry, entityKey, taskAttributes, taskInfo, dummyValidationFn)
	s.ErrorIs(executionErr, err)
	s.True(backendValidtionFnCalled)
}

func (s *nodeSuite) TestValidateSideEffectTask() {
	taskInfo := &persistencespb.ChasmTaskInfo{
		ComponentInitialVersionedTransition: &persistencespb.VersionedTransition{
			TransitionCount:          1,
			NamespaceFailoverVersion: 1,
		},
		ComponentLastUpdateVersionedTransition: &persistencespb.VersionedTransition{
			TransitionCount:          1,
			NamespaceFailoverVersion: 1,
		},
		Path: "",
		Type: "TestLibrary.test_side_effect_task",
		Data: &commonpb.DataBlob{
			Data:         nil,
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		},
	}

	root := s.testComponentTree()

	mockEngine := NewMockEngine(s.controller)
	ctx := NewEngineContext(context.Background(), mockEngine)
	taskAttributes := TaskAttributes{}

	expectValidate := func(componentType any, retValue bool, errValue error) {
		s.testLibrary.mockSideEffectTaskValidator.EXPECT().
			Validate(
				gomock.AssignableToTypeOf((*ContextImpl)(nil)),
				gomock.AssignableToTypeOf(componentType),
				gomock.Eq(taskAttributes),
				gomock.AssignableToTypeOf(&TestSideEffectTask{}),
			).Return(retValue, errValue).Times(1)
	}

	// Succeed validation as valid.
	expectValidate((*TestComponent)(nil), true, nil)
	task, err := root.ValidateSideEffectTask(ctx, taskAttributes, taskInfo)
	s.NotNil(task)
	s.IsType(&TestSideEffectTask{}, task)
	s.NoError(err)

	// Succeed validation as invalid.
	expectValidate((*TestComponent)(nil), false, nil)
	task, err = root.ValidateSideEffectTask(ctx, taskAttributes, taskInfo)
	s.Nil(task)
	s.NoError(err)

	// Fail validation.
	expectedErr := errors.New("validation failed")
	expectValidate((*TestComponent)(nil), false, expectedErr)
	task, err = root.ValidateSideEffectTask(ctx, taskAttributes, taskInfo)
	s.Nil(task)
	s.ErrorIs(expectedErr, err)

	// Succeed validation as valid for a sub component.
	childTaskInfo := taskInfo
	childTaskInfo.Path = "SubComponent1"
	expectValidate((*TestSubComponent1)(nil), true, nil)
	task, err = root.ValidateSideEffectTask(ctx, taskAttributes, childTaskInfo)
	s.NotNil(task)
	s.IsType(&TestSideEffectTask{}, task)
	s.NoError(err)
}

func (s *nodeSuite) newTestTree(
	serializedNodes map[string]*persistencespb.ChasmNode,
) (*Node, error) {
	return NewTree(serializedNodes, s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger)
}
