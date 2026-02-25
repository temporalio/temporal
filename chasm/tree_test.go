package chasm

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
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
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/protoassert"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
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
		metricsHandler  metrics.Handler
	}
)

func TestNodeSuite(t *testing.T) {
	suite.Run(t, new(nodeSuite))
}

func (s *nodeSuite) SetupTest() {
	s.initAssertions()
	s.controller = gomock.NewController(s.T())
	s.nodeBackend = &MockNodeBackend{}
	s.testLibrary = newTestLibrary(s.controller)

	s.logger = testlogger.NewTestLogger(s.T(), testlogger.FailOnAnyUnexpectedError)
	s.metricsHandler = metrics.NoopMetricsHandler
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
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						TypeId: testComponentTypeID,
					},
				},
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
	err := node.serialize()
	s.NoError(err)
	s.NotNil(node.serializedNode)
	s.NotNil(node.serializedNode.GetData(), "node serialized value must have data after serialize is called")
	s.Equal(testComponentTypeID, node.serializedNode.GetMetadata().GetComponentAttributes().GetTypeId(), "node serialized value must have type set")
	s.Equal(valueStateSynced, node.valueState)

	// Serialize subcomponents (there are 2 subcomponents).
	sc1Node := node.children["SubComponent1"]
	s.NotNil(sc1Node.serializedNode.GetMetadata().GetComponentAttributes())
	s.Nil(sc1Node.serializedNode.GetData())
	for _, childNode := range node.children {
		err = childNode.serialize()
		s.NoError(err)
		s.Equal(valueStateSynced, childNode.valueState)
	}
	s.NotNil(sc1Node.serializedNode.GetData(), "child node serialized value must have data after serialize is called")
	s.Equal(testSubComponent1TypeID, sc1Node.serializedNode.GetMetadata().GetComponentAttributes().GetTypeId(), "node serialized value must have type set")

	// Check SubData too.
	sd1Node := node.children["SubData1"]
	s.NoError(err)
	s.NotNil(sd1Node.serializedNode.GetData(), "child node serialized value must have data after serialize is called")
}

func (s *nodeSuite) TestSerializeNode_ClearComponentData() {
	node := s.testComponentTree()

	node.value.(*TestComponent).ComponentData = nil

	err := node.serialize()
	s.NoError(err)
	s.NotNil(node.serializedNode, "node serialized value must be not nil after serialize is called")
	s.NotNil(node.serializedNode.GetMetadata().GetComponentAttributes(), "metadata must have component attributes")
	s.Nil(node.serializedNode.GetData(), "data field must cleared to nil")
	s.Equal(testComponentTypeID, node.serializedNode.GetMetadata().GetComponentAttributes().GetTypeId(), "type must present")
	s.Equal(valueStateSynced, node.valueState)
}

func (s *nodeSuite) TestSerializeNode_ClearSubDataField() {
	node := s.testComponentTree()

	mutableContext := NewMutableContext(context.Background(), node)
	component, err := node.Component(mutableContext, ComponentRef{})
	s.NoError(err)
	testComponent := component.(*TestComponent)

	testComponent.SubData1 = NewEmptyField[*protoMessageType]()

	sd1Node := node.children["SubData1"]
	s.NotNil(sd1Node)

	err = node.syncSubComponents()
	s.NoError(err)
	s.False(node.needsPointerResolution)
	s.Len(node.mutation.DeletedNodes, 1)

	sd1Node = node.children["SubData1"]
	s.Nil(sd1Node)
}

func (s *nodeSuite) TestSetRootComponent_SetsArchetypeID() {
	rootNode := NewEmptyTree(s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger, s.metricsHandler)
	s.Equal(WorkflowArchetypeID, rootNode.ArchetypeID())
	rootComponent := &TestComponent{
		MSPointer: NewMSPointer(s.nodeBackend),
	}
	s.NoError(rootNode.SetRootComponent(rootComponent))
	s.Equal(testComponentTypeID, rootNode.ArchetypeID())
	s.NotEqual(WorkflowArchetypeID, rootNode.ArchetypeID())
}

func (s *nodeSuite) TestInitSerializedNode_TypeData() {
	node := newNode(s.nodeBase(), nil, "")
	node.initSerializedNode(fieldTypeData)
	s.NotNil(node.serializedNode.GetMetadata().GetDataAttributes(), "node serializedNode must have attributes created")
	s.Nil(node.serializedNode.GetData(), "node serializedNode must not have data before serialize is called")
}

func (s *nodeSuite) TestSerializeNode_DataAttributes() {
	component := &protoMessageType{
		CreateRequestId: "22",
	}

	node := newNode(s.nodeBase(), nil, "")
	node.initSerializedNode(fieldTypeData)
	node.value = component
	node.valueState = valueStateNeedSerialize

	err := node.serialize()
	s.NoError(err)
	s.NotNil(node.serializedNode.GetData(), "child node serialized value must have data after serialize is called")
	s.Equal([]byte{0xa, 0x2, 0x32, 0x32}, node.serializedNode.GetData().GetData())
	s.Equal(valueStateSynced, node.valueState)
}

func (s *nodeSuite) TestCollectionAttributes() {
	runID1 := fmt.Sprintf("workflow_id_%d", 1)
	runID2 := fmt.Sprintf("workflow_id_%d", 2)
	sc1 := &TestSubComponent1{
		SubComponent1Data: &protoMessageType{
			RunId: runID1,
		},
	}
	sc2 := &TestSubComponent1{
		SubComponent1Data: &protoMessageType{
			RunId: runID2,
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
						"SubComponent1": NewComponentField(nil, sc1),
						"SubComponent2": NewComponentField(nil, sc2),
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
						1: NewComponentField(nil, sc1),
						2: NewComponentField(nil, sc2),
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
			rootNode.valueState = valueStateNeedSyncStructure

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
			sc1Des := sc1Field.Get(chasmContext)
			s.Equal(sc1.SubComponent1Data.GetRunId(), sc1Des.SubComponent1Data.GetRunId())

			sc2Des := sc2Field.Get(chasmContext)
			s.Equal(sc2.SubComponent1Data.GetRunId(), sc2Des.SubComponent1Data.GetRunId())
		})

		s.Run("Clear map "+tc.name+" by setting it to nil", func() {
			rootNode, err := s.newTestTree(persistedNodes)
			s.NoError(err)

			err = rootNode.deserialize(reflect.TypeFor[*TestComponent]())
			s.NoError(err)

			rootComponent := rootNode.value.(*TestComponent)

			rootNode.valueState = valueStateNeedSyncStructure
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
			rootNode.valueState = valueStateNeedSyncStructure
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
			rootNode.valueState = valueStateNeedSyncStructure
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
	var persistedNodes map[string]*persistencespb.ChasmNode

	sc11 := &TestSubComponent11{
		SubComponent11Data: &protoMessageType{
			RunId: fmt.Sprintf("workflow_id_%d", 11),
		},
	}

	sc1 := &TestSubComponent1{
		SubComponent1Data: &protoMessageType{
			RunId: fmt.Sprintf("workflow_id_%d", 1),
		},
		SubComponent11: NewComponentField(nil, sc11),
	}

	s.Run("Sync and serialize component with pointer", func() {
		var nilSerializedNodes map[string]*persistencespb.ChasmNode
		rootNode, err := s.newTestTree(nilSerializedNodes)
		s.NoError(err)

		ctx := NewMutableContext(context.Background(), rootNode)

		rootComponent := &TestComponent{
			MSPointer:                    NewMSPointer(s.nodeBackend),
			SubComponent1:                NewComponentField(nil, sc1),
			SubComponentInterfacePointer: NewComponentField[Component](nil, sc1),
			SubComponent11Pointer:        ComponentPointerTo(ctx, sc11),
		}
		s.NoError(rootNode.SetRootComponent(rootComponent))

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

		mutableContext := NewMutableContext(context.Background(), rootNode)
		component, err := rootNode.Component(mutableContext, ComponentRef{})
		s.NoError(err)
		testComponent := component.(*TestComponent)

		s.NotNil(testComponent.MSPointer)

		chasmContext := NewMutableContext(context.Background(), rootNode)
		sc11Des := testComponent.SubComponent11Pointer.Get(chasmContext)
		s.NotNil(sc11Des)
		s.Equal(sc11.SubComponent11Data.GetRunId(), sc11Des.SubComponent11Data.GetRunId())

		ifacePtr := testComponent.SubComponentInterfacePointer.Get(chasmContext)
		s.NotNil(ifacePtr)

		sc1ptr, ok := ifacePtr.(*TestSubComponent1)
		s.True(ok)
		s.ProtoEqual(sc1ptr.SubComponent1Data, sc1.SubComponent1Data)
	})

	s.Run("Clear pointer by setting it to the empty field", func() {
		rootNode, err := s.newTestTree(persistedNodes)
		s.NoError(err)

		mutableContext := NewMutableContext(context.Background(), rootNode)
		component, err := rootNode.Component(mutableContext, ComponentRef{})
		s.NoError(err)
		testComponent := component.(*TestComponent)

		testComponent.SubComponent11Pointer = NewEmptyField[*TestSubComponent11]()

		mutation, err := rootNode.CloseTransaction()
		s.NoError(err)
		s.Len(mutation.UpdatedNodes, 1, "root should be updated")
		s.Len(mutation.DeletedNodes, 1, "SubComponent11Pointer must be deleted")
	})
}

func (s *nodeSuite) TestParentPointer_InMemory() {
	node := s.testComponentTree()

	s.assertParentPointer(node)

	// Additionally also test parentPtr for components inside a map.

	mutableContext := NewMutableContext(context.Background(), node)
	component, err := node.Component(mutableContext, ComponentRef{})
	s.NoError(err)
	testComponent := component.(*TestComponent)

	mapSubComponent1 := &TestSubComponent1{}
	// Try using the testComponent we get from the ParentPtr for the mutation.
	testComponent.SubComponents = Map[string, *TestSubComponent1]{
		"mapSubComponent1": NewComponentField(mutableContext, mapSubComponent1),
	}

	s.Panics(func() {
		_ = mapSubComponent1.ParentPtr.Get(mutableContext)
	})

	// Sync structure initializes the parent pointer
	err = node.syncSubComponents()
	s.NoError(err)

	testComponentFromPtr := mapSubComponent1.ParentPtr.Get(mutableContext)
	// Asserting they actually point to the same testComponent object.
	s.Same(testComponent, testComponentFromPtr)
}

func (s *nodeSuite) TestParentPointer_FromDB() {
	serializedNodes := testComponentSerializedNodes()

	node, err := s.newTestTree(serializedNodes)
	s.NoError(err)

	s.assertParentPointer(node)
}

func (s *nodeSuite) assertParentPointer(testComponentNode *Node) {
	chasmContext := NewContext(context.Background(), testComponentNode)
	component, err := testComponentNode.Component(chasmContext, ComponentRef{})
	s.NoError(err)
	testComponent := component.(*TestComponent)

	_, found := testComponent.ParentPtr.TryGet(chasmContext)
	s.False(found)

	subComponent1 := testComponent.SubComponent1.Get(chasmContext)
	testComponentFromPtr := subComponent1.ParentPtr.Get(chasmContext)
	// Asserting they actually point to the same testComponent object.
	s.Same(testComponent, testComponentFromPtr)

	subComponent11 := subComponent1.SubComponent11.Get(chasmContext)
	testSubComponent1FromPtr := subComponent11.ParentPtr.Get(chasmContext)
	// Asserting they actually point to the same testSubComponent1 object.
	s.Same(subComponent1, testSubComponent1FromPtr)
}

func (s *nodeSuite) TestSyncSubComponents_DeleteLeafNode() {
	node := s.testComponentTree()

	mutableContext := NewMutableContext(context.Background(), node)
	component, err := node.ComponentByPath(mutableContext, []string{"SubComponent1"})
	s.NoError(err)

	sc1 := component.(*TestSubComponent1)
	sc1.SubComponent11 = NewEmptyField[*TestSubComponent11]()
	s.NotNil(node.children["SubComponent1"].children["SubComponent11"])

	err = node.syncSubComponents()
	s.NoError(err)
	s.False(node.needsPointerResolution)

	s.Len(node.mutation.DeletedNodes, 1)
	s.NotNil(node.mutation.DeletedNodes["SubComponent1/SubComponent11"])
	s.Nil(node.children["SubComponent1"].children["SubComponent11"])
}

func (s *nodeSuite) TestSyncSubComponents_DeleteMiddleNode() {
	node := s.testComponentTree()

	mutableContext := NewMutableContext(context.Background(), node)
	component, err := node.Component(mutableContext, ComponentRef{})
	s.NoError(err)
	testComponent := component.(*TestComponent)

	// Set subcomponent at middle node to nil.
	testComponent.SubComponent1 = NewEmptyField[*TestSubComponent1]()
	s.NotNil(node.children["SubComponent1"])

	err = node.syncSubComponents()
	s.NoError(err)
	s.False(node.needsPointerResolution)

	s.Len(node.mutation.DeletedNodes, 3)
	s.NotNil(node.mutation.DeletedNodes["SubComponent1/SubComponent11"])
	s.NotNil(node.mutation.DeletedNodes["SubComponent1/SubData11"])
	s.NotNil(node.mutation.DeletedNodes["SubComponent1"])

	s.Nil(node.children["SubComponent1"])
}

func (s *nodeSuite) TestDeserializeNode_EmptyPersistence() {
	var serializedNodes map[string]*persistencespb.ChasmNode

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

	// Root component will be deserialized as part of the initialization process,
	// for initializing search attributes and memo.
	node, err := s.newTestTree(serializedNodes)
	s.NoError(err)
	s.NotNil(node.serializedNode)
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

	// Root component will be deserialized as part of the initialization process,
	// for initializing search attributes and memo.
	node, err := s.newTestTree(serializedNodes)
	s.NoError(err)
	s.NotNil(node.serializedNode)
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

	err = node.deserialize(reflect.TypeFor[*testComponent]())
	s.NoError(err)
	s.NotNil(node.value)
	s.IsType(&testComponent{}, node.value)
	tc := node.value.(*testComponent)

	chasmContext := NewMutableContext(context.Background(), node)
	sc1 := tc.SubComponent1.Get(chasmContext)
	s.NotNil(sc1)
	s.Equal("sub-component1-data", sc1.GetData())
}

func (s *nodeSuite) TestGenerateSerializedNodes() {
	s.T().Skip("This test is used to generate serialized nodes for other tests.")

	node := s.testComponentTree()

	err := node.serialize()
	s.NoError(err)
	serializedNodes := map[string]*persistencespb.ChasmNode{}
	serializedNodes[""] = node.serializedNode

	for childName, childNode := range node.children {
		err = childNode.serialize()
		s.NoError(err)
		serializedNodes[childName] = childNode.serializedNode
	}

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
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						TypeId: testComponentTypeID,
					},
				},
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
	mustEncode := func(m proto.Message) *commonpb.DataBlob {
		taskBlob, err := serialization.ProtoEncode(m)
		s.NoError(err)
		return taskBlob
	}

	now := s.timeSource.Now()
	persistenceNodes := map[string]*persistencespb.ChasmNode{
		"": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						TypeId: testComponentTypeID,
						PureTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								// This task is not updated, so it's deserialized version will
								// NOT be cleared below as part of the updateNode process.
								TypeId:                    testPureTaskTypeID,
								ScheduledTime:             timestamppb.New(now.Add(time.Second)),
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 1,
								PhysicalTaskStatus:        physicalTaskStatusNone,
								Data: mustEncode(&commonpb.Payload{
									Data: []byte("root-task-data-1"),
								}),
							},
							{
								// Task will be deleted, so deserialized version of this task should also be deleted from cache.
								TypeId:                    testPureTaskTypeID,
								ScheduledTime:             timestamppb.New(now.Add(time.Second)),
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 2,
								PhysicalTaskStatus:        physicalTaskStatusNone,
								Data: mustEncode(&commonpb.Payload{
									Data: []byte("root-task-data-2"),
								}),
							},
						},
					},
				},
			},
		},
		"SubComponent1": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 2},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 2},
			},
		},
		"SubComponent1/SubComponent11": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 3},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 3},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						TypeId: testSubComponent11TypeID,
						PureTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								// Node is deleted, so deserialized version of this task should be deleted from cache.
								TypeId:                    testPureTaskTypeID,
								ScheduledTime:             timestamppb.New(now.Add(time.Minute)),
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 3,
								PhysicalTaskStatus:        physicalTaskStatusNone,
								Data: mustEncode(&commonpb.Payload{
									Data: []byte("SubComponent11-task-data"),
								}),
							},
						},
					},
				},
			},
		},
		"SubComponent1/SubComponent11/SubComponent11Data": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 4},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 4},
			},
		},
	}
	root, err := s.newTestTree(persistenceNodes)
	s.NoError(err)
	s.Len(root.currentSA, 3)
	s.NotNil(root.currentMemo)
	initialMemo, ok := root.currentMemo.(*protoMessageType)
	s.True(ok)
	s.ProtoEqual(&protoMessageType{}, initialMemo)

	// Manually deserialize some tasks to populate the taskValueCache
	_, err = root.deserializeComponentTask(root.serializedNode.Metadata.GetComponentAttributes().PureTasks[0])
	s.NoError(err)
	_, err = root.deserializeComponentTask(root.serializedNode.Metadata.GetComponentAttributes().PureTasks[1])
	s.NoError(err)
	_, err = root.deserializeComponentTask(root.children["SubComponent1"].children["SubComponent11"].serializedNode.Metadata.GetComponentAttributes().PureTasks[0])
	s.NoError(err)
	s.Len(root.taskValueCache, 3)

	// This decoded value should be reset after applying the mutation
	root.children["SubComponent1"].value = "some-random-decoded-value"

	// Prepare mutation: update root and "SubComponent1" node, delete "SubComponent1/SubComponent11", and add "newchild".

	updatedRoot := &persistencespb.ChasmNode{
		Metadata: &persistencespb.ChasmNodeMetadata{
			InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 30},
			LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 30},
			Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
				ComponentAttributes: &persistencespb.ChasmComponentAttributes{
					TypeId: testComponentTypeID,
					PureTasks: []*persistencespb.ChasmComponentAttributes_Task{
						{
							TypeId:                    testPureTaskTypeID,
							ScheduledTime:             timestamppb.New(now.Add(time.Second)),
							VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
							VersionedTransitionOffset: 1,
							PhysicalTaskStatus:        physicalTaskStatusNone,
							Data: mustEncode(&commonpb.Payload{
								Data: []byte("root-task-data-1"),
							}),
						},
					},
				},
			},
		},
		Data: mustEncode(
			&protoMessageType{
				StartTime: timestamppb.New(now),
			}),
	}
	updatedSC1 := &persistencespb.ChasmNode{
		Metadata: &persistencespb.ChasmNodeMetadata{
			InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 20},
			LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 20},
		},
	}
	newSC2 := &persistencespb.ChasmNode{
		Metadata: &persistencespb.ChasmNodeMetadata{
			InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 100},
			LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 100},
		},
	}
	mutation := NodesMutation{
		UpdatedNodes: map[string]*persistencespb.ChasmNode{
			"":              updatedRoot,
			"SubComponent1": updatedSC1,
			"SubComponent2": newSC2,
		},
		DeletedNodes: map[string]struct{}{
			"SubComponent1/SubComponent11":  {}, // this should remove the entire "SubComponent11" subtree
			"SubComponent1/non-exist-child": {},
		},
	}
	err = root.ApplyMutation(mutation)
	s.NoError(err)

	// Validate root node got updated.
	s.Equal(updatedRoot, root.serializedNode)
	s.NotNil(root.value)
	s.Len(root.currentSA, 3)
	s.Len(root.currentSA, 3)
	s.Contains(root.currentSA, "TemporalDatetime01")
	s.True(root.currentSA["TemporalDatetime01"].(VisibilityValueTime).Equal(VisibilityValueTime(now)))

	// Validate memo content.
	s.NotNil(root.currentMemo)
	decodedMemo, ok := root.currentMemo.(*protoMessageType)
	s.True(ok, "currentMemo should be of type *protoMessageType")
	s.True(decodedMemo.StartTime.AsTime().Equal(now))

	// Validate the "child" node got updated.
	nodeSC1, ok := root.children["SubComponent1"]
	s.True(ok)
	s.Equal(updatedSC1, nodeSC1.serializedNode)
	s.Nil(nodeSC1.value) // value should be reset after mutation

	// Validate the "newchild" node is added.
	nodeSC2, ok := root.children["SubComponent2"]
	s.True(ok)
	s.Equal(newSC2, nodeSC2.serializedNode)

	// Validate the "grandchild" node is deleted.
	s.Empty(nodeSC1.children)

	// Validate that nodeBase.mutation reflects the applied mutation.
	// Only updates on existing nodes are recorded; new nodes are inserted without a mutation record.
	expectedMutation := NodesMutation{
		UpdatedNodes: map[string]*persistencespb.ChasmNode{
			"":              updatedRoot,
			"SubComponent1": updatedSC1,
			"SubComponent2": newSC2,
		},
		DeletedNodes: map[string]struct{}{
			"SubComponent1/SubComponent11":                    {},
			"SubComponent1/SubComponent11/SubComponent11Data": {},
		},
	}
	s.Equal(expectedMutation, root.mutation)

	s.Len(root.taskValueCache, 1)
}

func (s *nodeSuite) TestApplySnapshot() {
	persistenceNodes := map[string]*persistencespb.ChasmNode{
		"": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						TypeId: testComponentTypeID,
					},
				},
			},
		},
		"SubComponent1": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 2},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 2},
			},
		},
		"SubComponent1/SubComponent11": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 3},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 3},
			},
		},
		"SubComponent1/SubComponent11/SubComponent11Data": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 4},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 4},
			},
		},
	}
	root, err := s.newTestTree(persistenceNodes)
	s.NoError(err)

	// Set a decoded value that should be reset after applying the snapshot.
	root.children["SubComponent1"].value = "decoded-value"

	// Prepare an incoming snapshot representing the target state:
	// - The "SubComponent1" node is updated (LastUpdateTransition becomes 20),
	// - the "SubComponent1/SubComponent11" node is removed,
	// - a new node "SubComponent2" is added.

	now := timestamppb.Now()
	updatedRootData, err := serialization.ProtoEncode(&protoMessageType{
		StartTime: now,
	})
	s.NoError(err)
	incomingSnapshot := NodesSnapshot{
		Nodes: map[string]*persistencespb.ChasmNode{
			"": {
				Metadata: &persistencespb.ChasmNodeMetadata{
					InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
					LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 10},
					Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
						ComponentAttributes: &persistencespb.ChasmComponentAttributes{
							TypeId: testComponentTypeID,
						},
					},
				},
				Data: updatedRootData,
			},
			"SubComponent1": {
				Metadata: &persistencespb.ChasmNodeMetadata{
					InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 2},
					LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 20},
				},
			},
			"SubComponent2": {
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
	s.Nil(root.children["SubComponent1"].value) // value should be reset after snapshot

	// Validate that nodeBase.mutation reflects the applied snapshot.
	expectedMutation := NodesMutation{
		UpdatedNodes: map[string]*persistencespb.ChasmNode{
			"":              incomingSnapshot.Nodes[""],
			"SubComponent1": incomingSnapshot.Nodes["SubComponent1"],
			"SubComponent2": incomingSnapshot.Nodes["SubComponent2"],
		},
		DeletedNodes: map[string]struct{}{
			"SubComponent1/SubComponent11":                    {},
			"SubComponent1/SubComponent11/SubComponent11Data": {},
		},
	}
	s.Equal(expectedMutation, root.mutation)

	// Validate visibility search attributes and memo are updated as well.
	s.Len(root.currentSA, 3)
	s.Contains(root.currentSA, "TemporalDatetime01")
	s.True(root.currentSA["TemporalDatetime01"].(VisibilityValueTime).Equal(VisibilityValueTime(now.AsTime())))
}

func (s *nodeSuite) TestApplyMutation_OutOfOrder() {
	persistenceNodes := map[string]*persistencespb.ChasmNode{
		"": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						TypeId: testComponentTypeID,
					},
				},
			},
		},
	}

	root, err := s.newTestTree(persistenceNodes)
	s.NoError(err)

	// Test the case where child node is applied before parent node.
	err = root.ApplyMutation(NodesMutation{
		UpdatedNodes: map[string]*persistencespb.ChasmNode{
			"SubComponent1/SubComponent11": {
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
					LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 2},
					Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
						ComponentAttributes: &persistencespb.ChasmComponentAttributes{
							TypeId: testComponentTypeID,
						},
					},
				},
			},
			"SubComponent1": {
				Metadata: &persistencespb.ChasmNodeMetadata{
					InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
					LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 2},
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
	pureTaskScheduledTime := now.Add(time.Second).UTC()
	persistenceNodes := map[string]*persistencespb.ChasmNode{
		"": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						TypeId: testComponentTypeID,
						PureTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								TypeId:                    testPureTaskTypeID,
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
		"SubComponent1": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						TypeId: testSubComponent1TypeID,
						PureTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								TypeId:                    testPureTaskTypeID,
								ScheduledTime:             timestamppb.New(pureTaskScheduledTime),
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 2,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
							},
						},
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
						TypeId: testSubComponent2TypeID,
						SideEffectTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								TypeId:                    testSideEffectTaskTypeID,
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

	mutation, err := root.CloseTransaction()
	s.NoError(err)
	s.Len(mutation.UpdatedNodes, 2) // TaskStatus for the root node is not reset, so no need to persist it.
	s.Equal(2, s.nodeBackend.NumTasksAdded())
	s.Equal(pureTaskScheduledTime, s.nodeBackend.LastDeletePureTaskCall())
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
						TypeId: testComponentTypeID,
						SideEffectTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								TypeId:                    testSideEffectTaskTypeID,
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 1,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
							},
							{
								TypeId:                    testSideEffectTaskTypeID,
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 2,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
							},
							{
								TypeId:                    testSideEffectTaskTypeID,
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 2},
								VersionedTransitionOffset: 1,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
							},
						},
						PureTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								TypeId:                    testPureTaskTypeID,
								ScheduledTime:             timestamppb.New(now.Add(time.Minute)),
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 2},
								VersionedTransitionOffset: 2,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
							},
							{
								TypeId:                    testPureTaskTypeID,
								ScheduledTime:             timestamppb.New(now.Add(2 * time.Minute)),
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 3,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
							},
							{
								TypeId:                    testPureTaskTypeID,
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
							TypeId: testComponentTypeID,
							SideEffectTasks: []*persistencespb.ChasmComponentAttributes_Task{
								{
									TypeId:                    testSideEffectTaskTypeID,
									VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
									VersionedTransitionOffset: 2,
									PhysicalTaskStatus:        physicalTaskStatusCreated,
								},
								{
									TypeId:                    testSideEffectTaskTypeID,
									VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 3},
									VersionedTransitionOffset: 1,
									PhysicalTaskStatus:        physicalTaskStatusCreated,
								},
							},
							PureTasks: []*persistencespb.ChasmComponentAttributes_Task{
								{
									TypeId:                    testPureTaskTypeID,
									ScheduledTime:             timestamppb.New(now.Add(time.Second)),
									VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 3},
									VersionedTransitionOffset: 2,
									PhysicalTaskStatus:        physicalTaskStatusCreated,
								},
								{
									TypeId:                    testPureTaskTypeID,
									ScheduledTime:             timestamppb.New(now.Add(2 * time.Minute)),
									VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
									VersionedTransitionOffset: 3,
									PhysicalTaskStatus:        physicalTaskStatusCreated,
								},
								{
									TypeId:                    testPureTaskTypeID,
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
						TypeId: testComponentTypeID,
						SideEffectTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								TypeId:                    testSideEffectTaskTypeID,
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 2,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
							},
							{
								TypeId:                    testSideEffectTaskTypeID,
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 3},
								VersionedTransitionOffset: 1,
								PhysicalTaskStatus:        physicalTaskStatusNone,
							},
						},
						PureTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								TypeId:                    testPureTaskTypeID,
								ScheduledTime:             timestamppb.New(now.Add(time.Second)),
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 3},
								VersionedTransitionOffset: 2,
								PhysicalTaskStatus:        physicalTaskStatusNone,
							},
							{
								TypeId:                    testPureTaskTypeID,
								ScheduledTime:             timestamppb.New(now.Add(2 * time.Minute)),
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 3,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
							},
							{
								TypeId:                    testPureTaskTypeID,
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
		componentStatus enumspb.WorkflowExecutionStatus // TestComponent borrows the WorkflowExecutionStatus struct
		executionStatus enumspb.WorkflowExecutionStatus
		executionState  enumsspb.WorkflowExecutionState
		terminated      bool

		setup func(*Node, Context) error
	}{
		{
			name:            "access check applies only to ancestors (terminated)",
			valid:           true,
			intent:          OperationIntentProgress,
			componentStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			executionStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			executionState:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			terminated:      false,
			setup: func(target *Node, ctx Context) error {
				// Set the terminated flag on the target node instead of an ancestor
				target.terminated = true
				return nil
			},
		},
		{
			name:            "access check applies only to ancestors (closed)",
			valid:           true,
			intent:          OperationIntentProgress,
			componentStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			executionStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			executionState:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			terminated:      false,
			setup: func(target *Node, ctx Context) error {
				if err := target.prepareComponentValue(ctx); err != nil {
					return err
				}
				targetComponent, _ := target.value.(*TestSubComponent11)
				targetComponent.SubComponent11Data.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
				return nil
			},
		},
		{
			name:            "read-only always succeeds",
			intent:          OperationIntentObserve,
			componentStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			executionStatus: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			executionState:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			terminated:      true,
			valid:           true,
		},
		{
			name:            "valid write access",
			intent:          OperationIntentProgress,
			componentStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			executionStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			executionState:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			terminated:      false,
			valid:           true,
		},
		{
			name:            "invalid write access (parent closed)",
			intent:          OperationIntentProgress,
			componentStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			executionStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			executionState:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			terminated:      false,
			valid:           false,
		},
		{
			name:            "invalid write access (component terminated)",
			intent:          OperationIntentProgress,
			componentStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			executionStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			executionState:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			terminated:      true, // terminated in current transaction
			valid:           false,
		},
		{
			name:            "invalid write access (component terminated and reload)",
			intent:          OperationIntentProgress,
			componentStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			executionStatus: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			executionState:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			terminated:      false, // terminated in previous transaction and mutable state reloaded
			valid:           false,
		},
		{
			name:            "detached node skips parent validation",
			valid:           true,
			intent:          OperationIntentProgress,
			componentStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			executionStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, // root is closed
			executionState:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			terminated:      false,
			setup: func(target *Node, _ Context) error {
				// Set the parent node (SubComponent1) as detached.
				// When validateParentAccess is called on a detached node, it skips
				// ancestor validation entirely.
				target.parent.serializedNode.GetMetadata().GetComponentAttributes().Detached = true
				return nil
			},
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
				component.ComponentData.Status = tc.componentStatus
			}

			// Find target node
			node, ok := root.findNode(nodePath)
			s.True(ok)
			err = node.prepareComponentValue(ctx)
			s.NoError(err)

			if tc.setup != nil {
				s.NoError(tc.setup(node, ctx))
			}

			s.nodeBackend.HandleGetExecutionState = func() *persistencespb.WorkflowExecutionState {
				return &persistencespb.WorkflowExecutionState{
					State:  tc.executionState,
					Status: tc.executionStatus,
				}
			}

			// Validation begins on the target node, checking ancestors only.
			err = node.validateAccess(ctx)
			if tc.valid {
				s.NoError(err)
			} else {
				s.Error(err)
				s.ErrorIs(errAccessCheckFailed, err)
			}
		})
	}

}

func (s *nodeSuite) TestGetComponent_DetachedNodeBypassesParentValidation() {
	// Test that a detached node can be accessed even when its parent is closed.
	root, err := s.newTestTree(testComponentSerializedNodes())
	s.NoError(err)

	targetPath := []string{"SubComponent1", "SubComponent11"}
	targetNode, ok := root.findNode(targetPath)
	s.True(ok)

	// Mark the target node as detached.
	targetNode.serializedNode.GetMetadata().GetComponentAttributes().Detached = true

	// Close the root node (set lifecycle to COMPLETED).
	ctx := NewMutableContext(
		newContextWithOperationIntent(context.Background(), OperationIntentProgress),
		root,
	)
	err = root.prepareComponentValue(ctx)
	s.NoError(err)
	rootComponent, ok := root.value.(*TestComponent)
	s.True(ok)
	rootComponent.ComponentData.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED

	// GetComponent on the detached node should succeed despite root being closed.
	ref := ComponentRef{
		componentPath: targetPath,
	}
	component, err := root.Component(ctx, ref)
	s.NoError(err)
	s.NotNil(component)
}

func (s *nodeSuite) TestGetComponent_ClosedTargetSucceeds() {
	// Test that a closed target component can still be accessed via Component()
	// because we only check ancestor lifecycle, not the target's lifecycle.
	root, err := s.newTestTree(testComponentSerializedNodes())
	s.NoError(err)

	targetPath := []string{"SubComponent1", "SubComponent11"}
	targetNode, ok := root.findNode(targetPath)
	s.True(ok)

	ctx := NewMutableContext(
		newContextWithOperationIntent(context.Background(), OperationIntentProgress),
		root,
	)

	// Close the target node's lifecycle (set to COMPLETED).
	err = targetNode.prepareComponentValue(ctx)
	s.NoError(err)
	targetComponent, ok := targetNode.value.(*TestSubComponent11)
	s.True(ok)
	targetComponent.SubComponent11Data.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	s.True(targetComponent.LifecycleState(ctx).IsClosed())

	// GetComponent on the closed target should succeed because we only check ancestors.
	ref := ComponentRef{
		componentPath: targetPath,
	}
	component, err := root.Component(ctx, ref)
	s.NoError(err)
	s.NotNil(component)
}

func (s *nodeSuite) TestGetComponent() {
	errValidation := errors.New("some random validation error")

	expectedTestComponent := &TestComponent{}
	setTestComponentFields(expectedTestComponent, s.nodeBackend)
	assertTestComponent := func(component Component) {
		testComponent, ok := component.(*TestComponent)
		s.True(ok)
		protoassert.ProtoEqual(s.T(), expectedTestComponent.ComponentData, testComponent.ComponentData)

		// TODO: Can we assert other fields?
		// Right now the chasm Field generated by setTestComponentFields() doesn't have a backing node.
	}

	testCases := []struct {
		name            string
		chasmContextFn  func(root *Node) Context
		ref             ComponentRef
		expectedErr     error
		nodeDirty       bool
		assertComponent func(Component)
	}{
		{
			name: "path not found",
			chasmContextFn: func(root *Node) Context {
				return NewContext(context.Background(), root)
			},
			ref: ComponentRef{
				componentPath: []string{"unknownComponent"},
			},
			expectedErr: errComponentNotFound,
		},
		{
			name: "initialVT mismatch",
			chasmContextFn: func(root *Node) Context {
				return NewMutableContext(context.Background(), root)
			},
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
			name: "validation failure",
			chasmContextFn: func(root *Node) Context {
				return NewMutableContext(context.Background(), root)
			},
			ref: ComponentRef{
				componentPath: []string{"SubComponent1"},
				componentInitialVT: &persistencespb.VersionedTransition{
					NamespaceFailoverVersion: 1,
					TransitionCount:          1,
				},
				validationFn: func(_ NodeBackend, _ Context, _ Component, _ *Registry) error {
					return errValidation
				},
			},
			expectedErr: errValidation,
		},
		{
			name: "success readonly access",
			chasmContextFn: func(root *Node) Context {
				return NewContext(context.Background(), root)
			},
			ref: ComponentRef{
				componentPath: []string{}, // root
				componentInitialVT: &persistencespb.VersionedTransition{
					NamespaceFailoverVersion: 1,
					TransitionCount:          1,
				},
				validationFn: func(_ NodeBackend, _ Context, _ Component, _ *Registry) error {
					return nil
				},
			},
			expectedErr:     nil,
			assertComponent: assertTestComponent,
		},
		{
			name: "success mutable access",
			chasmContextFn: func(root *Node) Context {
				return NewMutableContext(context.Background(), root)
			},
			ref: ComponentRef{
				componentPath: []string{}, // root
			},
			expectedErr:     nil,
			nodeDirty:       true,
			assertComponent: assertTestComponent,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			root, err := s.newTestTree(testComponentSerializedNodes())
			s.NoError(err)

			component, err := root.Component(tc.chasmContextFn(root), tc.ref)
			s.Equal(tc.expectedErr, err)

			node, ok := root.findNode(tc.ref.componentPath)
			if tc.expectedErr == nil {
				s.True(ok)
				tc.assertComponent(component)
			}

			if ok {
				if tc.nodeDirty {
					s.Greater(node.valueState, valueStateSynced)
				} else {
					s.LessOrEqual(node.valueState, valueStateSynced)
				}
			}
		})
	}
}

func (s *nodeSuite) TestRef() {
	workflowKey := definition.NewWorkflowKey(
		primitives.NewUUID().String(),
		primitives.NewUUID().String(),
		primitives.NewUUID().String(),
	)
	executionKey := ExecutionKey{
		NamespaceID: workflowKey.NamespaceID,
		BusinessID:  workflowKey.WorkflowID,
		RunID:       workflowKey.RunID,
	}
	currentVT := &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: 2,
		TransitionCount:          2,
	}
	s.nodeBackend = &MockNodeBackend{
		HandleCurrentVersionedTransition: func() *persistencespb.VersionedTransition {
			return currentVT
		},
		HandleGetWorkflowKey: func() definition.WorkflowKey {
			return workflowKey
		},
	}

	root, err := s.newTestTree(testComponentSerializedNodes())
	s.NoError(err)

	chasmContext := NewContext(context.Background(), root)
	rootComponent, err := root.Component(chasmContext, NewComponentRef[*TestComponent](executionKey))
	s.NoError(err)
	testComponent, ok := rootComponent.(*TestComponent)
	s.True(ok)

	rc, ok := s.registry.ComponentFor(testComponent)
	s.True(ok)
	archetypeID := rc.componentID

	subComponent1 := testComponent.SubComponent1.Get(chasmContext)
	subComponent11 := subComponent1.SubComponent11.Get(chasmContext)

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
				ExecutionKey:  executionKey,
				archetypeID:   archetypeID,
				componentPath: tc.expectedPath,

				// Proto fields are validated separately with ProtoEqual.
				// executionLastUpdateVT: currentVT,
				// componentInitialVT: tc.expectedInitalVT,
			}

			actualRef, err := DeserializeComponentRef(encodedRef)
			s.NoError(err)
			s.ProtoEqual(currentVT, actualRef.executionLastUpdateVT)
			s.ProtoEqual(tc.expectedInitalVT, actualRef.componentInitialVT)

			actualRef.executionLastUpdateVT = nil
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

			blob, err := serializeTask(rt, reflect.ValueOf(tc.task))
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
	chasmCtx := NewMutableContext(context.Background(), node)
	tc, err := node.Component(chasmCtx, ComponentRef{componentPath: rootPath})
	s.NoError(err)
	tc.(*TestComponent).SubData1 = NewEmptyField[*protoMessageType]()
	tc.(*TestComponent).ComponentData = &protoMessageType{CreateRequestId: primitives.NewUUID().String()}

	mutations, err := node.CloseTransaction()
	s.NoError(err)
	s.Len(mutations.UpdatedNodes, 4)
	s.Contains(mutations.UpdatedNodes, "", "root component must be in UpdatedNodes")
	s.Contains(mutations.UpdatedNodes, "SubComponent1", "SubComponent1 component must be in UpdatedNodes")
	s.Contains(mutations.UpdatedNodes, "SubComponent1/SubComponent11", "SubComponent1/SubComponent11 component must be in UpdatedNodes")
	s.Contains(mutations.UpdatedNodes, "SubComponent1/SubData11", "SubComponent1/SubData11 component must be in UpdatedNodes")
	s.Len(mutations.DeletedNodes, 1)
	s.Contains(mutations.DeletedNodes, "SubData1", "SubData1 was removed and must be in DeletedNodes")

	sc1 := tc.(*TestComponent).SubComponent1.Get(chasmCtx)
	s.NotNil(sc1)

	mutations, err = node.CloseTransaction()
	s.NoError(err)
	s.Len(mutations.UpdatedNodes, 1)
	s.Contains(mutations.UpdatedNodes, "SubComponent1", "SubComponent1 component must be in UpdatedNodes")
	s.Empty(mutations.DeletedNodes)
}

func (s *nodeSuite) TestCloseTransaction_EmptyNode() {
	var nilSerializedNodes map[string]*persistencespb.ChasmNode
	// Create an empty tree.
	node, err := s.newTestTree(nilSerializedNodes)
	s.NoError(err)
	s.Nil(node.value)

	mutations, err := node.CloseTransaction()
	s.NoError(err)
	s.Empty(mutations.UpdatedNodes, "there should be no updated nodes because tree was initialized with empty serialized nodes")
	s.Empty(mutations.DeletedNodes, "there should be no deleted nodes because tree was initialized with empty serialized nodes")
}

func (s *nodeSuite) TestCloseTransaction_LifecycleChange() {
	node := s.testComponentTree()

	chasmCtx := NewMutableContext(context.Background(), node)
	_, err := node.Component(chasmCtx, ComponentRef{componentPath: rootPath})
	s.NoError(err)
	_, err = node.CloseTransaction()
	s.NoError(err)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, s.nodeBackend.LastUpdateWorkflowState())
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, s.nodeBackend.LastUpdateWorkflowStatus())

	// Test force terminate case
	_, err = node.Component(chasmCtx, ComponentRef{componentPath: rootPath})
	s.NoError(err)
	node.terminated = true
	_, err = node.CloseTransaction()
	s.NoError(err)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, s.nodeBackend.LastUpdateWorkflowState())
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, s.nodeBackend.LastUpdateWorkflowStatus())

	node.terminated = false
	tc, err := node.Component(chasmCtx, ComponentRef{componentPath: rootPath})
	s.NoError(err)
	tc.(*TestComponent).Complete(chasmCtx)
	_, err = node.CloseTransaction()
	s.NoError(err)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, s.nodeBackend.LastUpdateWorkflowState())
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, s.nodeBackend.LastUpdateWorkflowStatus())

	tc, err = node.Component(chasmCtx, ComponentRef{componentPath: rootPath})
	s.NoError(err)
	tc.(*TestComponent).Fail(chasmCtx)
	_, err = node.CloseTransaction()
	s.NoError(err)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, s.nodeBackend.LastUpdateWorkflowState())
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, s.nodeBackend.LastUpdateWorkflowStatus())
}

func (s *nodeSuite) TestCloseTransaction_ForceUpdateVisibility_RootLifecycleChanged() {
	node := s.testComponentTree()

	chasmCtx := NewMutableContext(context.Background(), node)
	testComponent, err := node.Component(chasmCtx, ComponentRef{componentPath: rootPath})
	s.NoError(err)

	nextTransitionCount := int64(1)
	s.nodeBackend.HandleGetCurrentVersion = func() int64 { return 1 }
	s.nodeBackend.HandleNextTransitionCount = func() int64 { return nextTransitionCount }
	s.nodeBackend.HandleUpdateWorkflowStateStatus = func(state enumsspb.WorkflowExecutionState, status enumspb.WorkflowExecutionStatus) (bool, error) {
		return true, nil
	}

	// Init visiblity component
	testComponent.(*TestComponent).Visibility = NewComponentField(chasmCtx, NewVisibility(chasmCtx))
	mutation, err := node.CloseTransaction()
	s.NoError(err)
	pVisibilityNode, ok := mutation.UpdatedNodes["Visibility"]
	s.True(ok)
	s.Len(pVisibilityNode.GetMetadata().GetComponentAttributes().SideEffectTasks, 1)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, s.nodeBackend.UpdateCalls[0].State)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, s.nodeBackend.UpdateCalls[0].Status)

	// Change ComponentData which is used as Memo. Even though lifecycle didn't change,
	// visibility should be updated because memo changed.
	nextTransitionCount = 2
	testComponent, err = node.Component(chasmCtx, ComponentRef{componentPath: rootPath})
	s.NoError(err)
	testComponent.(*TestComponent).ComponentData = &protoMessageType{
		CreateRequestId: "some-updated-component-data",
	}
	s.nodeBackend.HandleUpdateWorkflowStateStatus = func(state enumsspb.WorkflowExecutionState, status enumspb.WorkflowExecutionStatus) (bool, error) {
		return false, nil
	}
	mutation, err = node.CloseTransaction()
	s.NoError(err)
	pVisibilityNode, ok = mutation.UpdatedNodes["Visibility"]
	s.True(ok, "visibility should be updated when memo changes")
	s.Len(pVisibilityNode.GetMetadata().GetComponentAttributes().SideEffectTasks, 1)

	// Close the run, visibility should be force updated
	// even if not explicitly updated.
	nextTransitionCount = 3
	testComponent, err = node.Component(chasmCtx, ComponentRef{componentPath: rootPath})
	s.NoError(err)
	testComponent.(*TestComponent).Complete(chasmCtx)
	s.nodeBackend.HandleUpdateWorkflowStateStatus = func(state enumsspb.WorkflowExecutionState, status enumspb.WorkflowExecutionStatus) (bool, error) {
		return true, nil
	}
	mutation, err = node.CloseTransaction()
	s.NoError(err)
	pVisibilityNode, ok = mutation.UpdatedNodes["Visibility"]
	s.True(ok)
	s.Len(pVisibilityNode.GetMetadata().GetComponentAttributes().SideEffectTasks, 1)
}

func (s *nodeSuite) TestCloseTransaction_ForceUpdateVisibility_RootSAMemoChanged() {
	node := s.testComponentTree()
	chasmCtx := NewMutableContext(context.Background(), node)
	testComponent, err := node.Component(chasmCtx, ComponentRef{componentPath: rootPath})
	s.NoError(err)

	nextTransitionCount := int64(1)
	s.nodeBackend.HandleNextTransitionCount = func() int64 {
		return nextTransitionCount
	}

	// Init visiblity component
	testComponent.(*TestComponent).Visibility = NewComponentField(chasmCtx, NewVisibility(chasmCtx))
	s.nodeBackend.HandleUpdateWorkflowStateStatus = func(state enumsspb.WorkflowExecutionState, status enumspb.WorkflowExecutionStatus) (bool, error) {
		return true, nil
	}
	mutation, err := node.CloseTransaction()
	s.NoError(err)
	pVisibilityNode, ok := mutation.UpdatedNodes["Visibility"]
	s.True(ok)
	s.Len(pVisibilityNode.GetMetadata().GetComponentAttributes().SideEffectTasks, 1)

	// Update root component state, which results in a change to the search attributes and memo.
	// CHASM framework should automatically detect the change and generate a visibility task.
	nextTransitionCount = 2
	testComponent, err = node.Component(chasmCtx, ComponentRef{componentPath: rootPath})
	s.NoError(err)
	testComponent.(*TestComponent).ComponentData = &protoMessageType{
		StartTime: timestamppb.Now(),
	}
	s.nodeBackend.HandleUpdateWorkflowStateStatus = func(state enumsspb.WorkflowExecutionState, status enumspb.WorkflowExecutionStatus) (bool, error) {
		return false, nil
	}
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
						TypeId: testComponentTypeID,
						SideEffectTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								TypeId:                    testSideEffectTaskTypeID,
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 1,
								Data:                      taskBlob,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
							},
							{
								TypeId:                    testOutboundSideEffectTaskTypeID,
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
								TypeId:                    testPureTaskTypeID,
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
		"SubComponent1": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						TypeId: testSubComponent1TypeID,
						SideEffectTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								TypeId:                    testSideEffectTaskTypeID,
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 4,
								Data:                      taskBlob,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
							},
						},
						PureTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								TypeId:                    testPureTaskTypeID,
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 5,
								Data:                      taskBlob,
								PhysicalTaskStatus:        physicalTaskStatusNone,
							},
						},
					},
				},
			},
		},
	}
	root, err := s.newTestTree(persistenceNodes)
	s.NoError(err)

	nextTransitionCount := int64(2)
	s.nodeBackend.HandleNextTransitionCount = func() int64 { return nextTransitionCount }

	// The idea is to mark the node as dirty by accessing it with a mutable context.
	mutableContext := NewMutableContext(context.Background(), root)
	_, err = root.Component(mutableContext, ComponentRef{})
	s.NoError(err)

	s.testLibrary.mockSideEffectTaskValidator.EXPECT().
		Validate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).Times(2)
	s.testLibrary.mockOutboundSideEffectTaskValidator.EXPECT().
		Validate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).Times(1)
	s.testLibrary.mockPureTaskValidator.EXPECT().
		Validate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil).Times(2)

	mutation, err := root.CloseTransaction()
	s.NoError(err)

	s.Equal(tasks.MaximumKey.FireTime, s.nodeBackend.LastDeletePureTaskCall())

	s.Len(mutation.UpdatedNodes, 2)
	for _, updatedNode := range mutation.UpdatedNodes {
		s.Equal(nextTransitionCount, updatedNode.GetMetadata().GetLastUpdateVersionedTransition().TransitionCount)
	}
	s.Empty(mutation.DeletedNodes)

	componentAttr := root.serializedNode.Metadata.GetComponentAttributes()
	s.Empty(componentAttr.PureTasks)
	s.Len(componentAttr.SideEffectTasks, 1)
	s.Equal(testOutboundSideEffectTaskTypeID, componentAttr.SideEffectTasks[0].GetTypeId())

	componentAttr = root.children["SubComponent1"].serializedNode.Metadata.GetComponentAttributes()
	s.Empty(componentAttr.PureTasks)
	s.Empty(componentAttr.SideEffectTasks)
}

func (s *nodeSuite) TestCloseTransaction_NewComponentTasks() {
	persistenceNodes := map[string]*persistencespb.ChasmNode{
		"": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						TypeId: testComponentTypeID,
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
						TypeId: testSubComponent1TypeID,
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
						TypeId: testSubComponent2TypeID,
					},
				},
			},
		},
	}

	s.nodeBackend.HandleNextTransitionCount = func() int64 {
		return 2
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

	// Add a valid outbound side effect task to a sub-component.
	s.testLibrary.mockOutboundSideEffectTaskValidator.EXPECT().
		Validate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).Times(1)
	subComponent2 := testComponent.SubComponent2.Get(mutableContext)
	mutableContext.AddTask(
		subComponent2,
		TaskAttributes{Destination: "destination"},
		TestOutboundSideEffectTask{},
	)

	mutation, err := root.CloseTransaction()
	s.NoError(err)

	s.Equal(s.timeSource.Now().UTC(), s.nodeBackend.LastDeletePureTaskCall())

	rootAttr := mutation.UpdatedNodes[""].GetMetadata().GetComponentAttributes()
	s.Len(rootAttr.SideEffectTasks, 1) // Only one valid side effect task.
	newSideEffectTask := rootAttr.SideEffectTasks[0]
	newSideEffectTask.Data = nil // This is tested by TestSerializeTask()
	s.Equal(&persistencespb.ChasmComponentAttributes_Task{
		TypeId:                    testSideEffectTaskTypeID,
		ScheduledTime:             timestamppb.New(time.Time{}),
		VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 2},
		VersionedTransitionOffset: 1,
		PhysicalTaskStatus:        physicalTaskStatusCreated,
	}, newSideEffectTask)
	s.Len(s.nodeBackend.TasksByCategory[tasks.CategoryTransfer], 1)
	chasmTask := s.nodeBackend.TasksByCategory[tasks.CategoryTransfer][0].(*tasks.ChasmTask)
	s.ProtoEqual(&persistencespb.ChasmTaskInfo{
		ComponentInitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
		ComponentLastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 2},
		Path:                                   rootPath,
		TypeId:                                 testSideEffectTaskTypeID,
		Data:                                   chasmTask.Info.GetData(), // This is tested by TestSerializeTask()
		ArchetypeId:                            testComponentTypeID,
	}, chasmTask.Info)

	s.Len(rootAttr.PureTasks, 1) // Only one valid side effect task.
	newPureTask := rootAttr.PureTasks[0]
	newPureTask.Data = nil // This is tested by TestSerializeTask()
	s.Equal(&persistencespb.ChasmComponentAttributes_Task{
		TypeId:                    testPureTaskTypeID,
		ScheduledTime:             timestamppb.New(s.timeSource.Now()),
		VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 2},
		VersionedTransitionOffset: 2,
		PhysicalTaskStatus:        physicalTaskStatusCreated,
	}, newPureTask)
	s.Len(s.nodeBackend.TasksByCategory[tasks.CategoryTimer], 1)
	chasmPureTask := s.nodeBackend.TasksByCategory[tasks.CategoryTimer][0].(*tasks.ChasmTaskPure)
	s.Equal(tasks.CategoryTimer, chasmPureTask.GetCategory())
	s.True(chasmPureTask.VisibilityTimestamp.Equal(s.timeSource.Now()))

	subComponent2Attr := mutation.UpdatedNodes["SubComponent2"].GetMetadata().GetComponentAttributes()
	newOutboundSideEffectTask := subComponent2Attr.SideEffectTasks[0]
	newOutboundSideEffectTask.Data = nil // This is tested by TestSerializeTask()
	s.Equal(&persistencespb.ChasmComponentAttributes_Task{
		TypeId:                    testOutboundSideEffectTaskTypeID,
		Destination:               "destination",
		ScheduledTime:             timestamppb.New(time.Time{}),
		VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 2},
		VersionedTransitionOffset: 3,
		PhysicalTaskStatus:        physicalTaskStatusCreated,
	}, newOutboundSideEffectTask)
	s.Len(s.nodeBackend.TasksByCategory[tasks.CategoryOutbound], 1)
	chasmTask = s.nodeBackend.TasksByCategory[tasks.CategoryOutbound][0].(*tasks.ChasmTask)
	s.ProtoEqual(&persistencespb.ChasmTaskInfo{
		ComponentInitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
		ComponentLastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 2},
		Path:                                   []string{"SubComponent2"},
		TypeId:                                 testOutboundSideEffectTaskTypeID,
		Data:                                   chasmTask.Info.GetData(), // This is tested by TestSerializeTask()
		ArchetypeId:                            testComponentTypeID,
	}, chasmTask.Info)
}

func (s *nodeSuite) TestCloseTransaction_ApplyMutation_SideEffectTasks() {
	persistenceNodes := map[string]*persistencespb.ChasmNode{
		"": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						TypeId: testComponentTypeID,
						SideEffectTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								TypeId:                    testSideEffectTaskTypeID,
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

	incomingMutation := NodesMutation{
		UpdatedNodes: map[string]*persistencespb.ChasmNode{
			"": {
				Metadata: &persistencespb.ChasmNodeMetadata{
					InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
					LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 2},
					Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
						ComponentAttributes: &persistencespb.ChasmComponentAttributes{
							TypeId: testComponentTypeID,
							SideEffectTasks: []*persistencespb.ChasmComponentAttributes_Task{
								{
									TypeId:                    testSideEffectTaskTypeID,
									VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
									VersionedTransitionOffset: 1,
									PhysicalTaskStatus:        physicalTaskStatusCreated,
								},
								{
									TypeId:                    testSideEffectTaskTypeID,
									VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 2},
									VersionedTransitionOffset: 1,
									PhysicalTaskStatus:        physicalTaskStatusNone,
								},
								{
									TypeId:                    testSideEffectTaskTypeID,
									Destination:               "destination",
									ScheduledTime:             timestamppb.New(TaskScheduledTimeImmediate),
									VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 2},
									VersionedTransitionOffset: 2,
									PhysicalTaskStatus:        physicalTaskStatusNone,
								},
								{
									TypeId:                    testSideEffectTaskTypeID,
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

	err = root.ApplyMutation(incomingMutation)
	s.NoError(err)

	expectedCategories := []tasks.Category{tasks.CategoryTimer, tasks.CategoryOutbound, tasks.CategoryTransfer}
	_, err = root.CloseTransaction()
	for _, category := range expectedCategories {
		for _, task := range s.nodeBackend.TasksByCategory[category] {
			s.IsType(&tasks.ChasmTask{}, task)
			s.Equal(category, task.GetCategory())
		}
	}

	s.NoError(err)
}

func (s *nodeSuite) TestCloseTransaction_ApplyMutation_PureTasks() {
	now := s.timeSource.Now().UTC()
	persistenceNodes := map[string]*persistencespb.ChasmNode{
		"": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						TypeId: testComponentTypeID,
						PureTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								TypeId:                    testPureTaskTypeID,
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
		"SubComponent1": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						TypeId: testSubComponent1TypeID,
						PureTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								TypeId:                    testPureTaskTypeID,
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

	incomingMutation := NodesMutation{
		UpdatedNodes: map[string]*persistencespb.ChasmNode{
			"": {
				Metadata: &persistencespb.ChasmNodeMetadata{
					InitialVersionedTransition:    &persistencespb.VersionedTransition{TransitionCount: 1},
					LastUpdateVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 2},
					Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
						ComponentAttributes: &persistencespb.ChasmComponentAttributes{
							TypeId: testComponentTypeID,
							PureTasks: []*persistencespb.ChasmComponentAttributes_Task{
								{
									TypeId:                    testPureTaskTypeID,
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

	err = root.ApplyMutation(incomingMutation)
	s.NoError(err)

	mutation, err := root.CloseTransaction()
	s.NoError(err)

	s.Equal(now.Add(time.Minute), s.nodeBackend.LastDeletePureTaskCall())

	// Although only root is mutated in ApplyMutation, we generated a pure task for the child node,
	// and need to persist that as well.
	s.Len(mutation.UpdatedNodes, 2)

	s.Len(s.nodeBackend.TasksByCategory[tasks.CategoryTimer], 1)
	task := s.nodeBackend.TasksByCategory[tasks.CategoryTimer][0]
	s.IsType(&tasks.ChasmTaskPure{}, task)
	s.True(now.Add(time.Minute).Equal(task.GetKey().FireTime))
}

func (s *nodeSuite) TestTerminate() {
	node := s.testComponentTree()

	// First closeTransaction once to make the tree clean.
	_, err := node.CloseTransaction()
	s.NoError(err)

	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, s.nodeBackend.LastUpdateWorkflowState())
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, s.nodeBackend.LastUpdateWorkflowStatus())

	// Then terminate the node and verify only that node will be in the mutation.
	err = node.Terminate(TerminateComponentRequest{})
	s.NoError(err)
	s.True(node.terminated)

	mutations, err := node.CloseTransaction()
	s.NoError(err)
	s.Len(mutations.UpdatedNodes, 1)
	s.Empty(mutations.DeletedNodes)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, s.nodeBackend.LastUpdateWorkflowState())
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, s.nodeBackend.LastUpdateWorkflowStatus())

	// Test updating a terminated node will NOT change the state & status in mutable state.
	// Here we simulate mutable state reload case since the terminate flag is not persisted.
	s.nodeBackend.HandleGetExecutionState = func() *persistencespb.WorkflowExecutionState {
		return &persistencespb.WorkflowExecutionState{
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		}
	}

	snapshot := node.Snapshot(nil)
	node, err = s.newTestTree(snapshot.Nodes)
	s.NoError(err)

	mutableContext := NewMutableContext(context.Background(), node)
	_, err = node.Component(mutableContext, ComponentRef{})
	s.NoError(err)

	mutations, err = node.CloseTransaction()
	s.NoError(err)
	s.Len(mutations.UpdatedNodes, 1)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, s.nodeBackend.LastUpdateWorkflowState())
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, s.nodeBackend.LastUpdateWorkflowStatus())
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

		mutation: NodesMutation{
			UpdatedNodes: make(map[string]*persistencespb.ChasmNode),
			DeletedNodes: make(map[string]struct{}),
		},
		systemMutation: NodesMutation{
			UpdatedNodes: make(map[string]*persistencespb.ChasmNode),
			DeletedNodes: make(map[string]struct{}),
		},
		newTasks:       make(map[any][]taskWithAttributes),
		taskValueCache: make(map[*commonpb.DataBlob]reflect.Value),
	}
}

// Helper method to create a test tree for TestComponent.
func (s *nodeSuite) testComponentTree() *Node {
	s.nodeBackend.HandleNextTransitionCount = func() int64 { return 1 }
	s.nodeBackend.HandleGetCurrentVersion = func() int64 { return 1 }

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
	s.Equal(valueStateNeedSyncStructure, node.valueState)
	// Create subcomponents by assigning fields to TestComponent instance.
	setTestComponentFields(tc.(*TestComponent), s.nodeBackend)

	// Sync tree with subcomponents of TestComponent.
	err = node.syncSubComponents()
	s.False(node.needsPointerResolution)
	s.NoError(err)
	s.Empty(node.mutation.DeletedNodes)

	return node // maybe tc too
}

func (s *nodeSuite) TestExecuteImmediatePureTask() {
	root := s.testComponentTree()

	mutations, err := root.CloseTransaction()
	s.NoError(err)

	// Start a clean transaction.

	mutableContext := NewMutableContext(context.Background(), root)
	component, err := root.Component(mutableContext, ComponentRef{})
	s.NoError(err)
	testComponent := component.(*TestComponent)

	taskAttributes := TaskAttributes{ScheduledTime: TaskScheduledTimeImmediate}
	mutableContext.AddTask(
		testComponent,
		taskAttributes,
		&TestPureTask{
			Payload: &commonpb.Payload{Data: []byte("root-task-payload")},
		},
	)

	sc1 := testComponent.SubComponent1.Get(mutableContext)

	mutableContext.AddTask(
		sc1,
		taskAttributes,
		&TestPureTask{
			Payload: &commonpb.Payload{Data: []byte("sc1-task-payload")},
		},
	)

	// One valid task, one invalid task
	s.testLibrary.mockPureTaskValidator.EXPECT().
		Validate(gomock.Any(), gomock.Any(), gomock.Eq(taskAttributes), gomock.Any()).Return(false, nil).Times(1)
	s.testLibrary.mockPureTaskValidator.EXPECT().
		Validate(gomock.Any(), gomock.Any(), gomock.Eq(taskAttributes), gomock.Any()).Return(true, nil).Times(1)
	s.testLibrary.mockPureTaskExecutor.EXPECT().
		Execute(
			gomock.AssignableToTypeOf(&mutableCtx{}),
			gomock.Any(),
			gomock.Eq(taskAttributes),
			gomock.Any(),
		).Return(nil).Times(1)

	mutations, err = root.CloseTransaction()
	s.NoError(err)
	s.Len(mutations.UpdatedNodes, 2, "root and subcomponent1 should be updated")
	s.Empty(mutations.DeletedNodes)

	// immedidate pure tasks will be executed inline and no physical chasm pure task will be generated.
	s.Equal(tasks.MaximumKey.FireTime, s.nodeBackend.LastDeletePureTaskCall())
}

func (s *nodeSuite) TestEachPureTask() {
	now := s.timeSource.Now()

	mustEncode := func(m proto.Message) *commonpb.DataBlob {
		taskBlob, err := serialization.ProtoEncode(m)
		s.NoError(err)
		return taskBlob
	}

	// Set up a tree with expired and unexpired pure tasks.
	persistenceNodes := map[string]*persistencespb.ChasmNode{
		"": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						TypeId: testComponentTypeID,
						PureTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								// Expired
								TypeId:                    testPureTaskTypeID,
								ScheduledTime:             timestamppb.New(now),
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 1,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
								Data: mustEncode(&commonpb.Payload{
									Data: []byte("some-random-data-root"),
								}),
							},
						},
					},
				},
			},
		},
		"SubComponent1": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						TypeId: testSubComponent1TypeID,
						PureTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								TypeId: testPureTaskTypeID,
								// Not expired yet.
								ScheduledTime:             timestamppb.New(now.Add(time.Hour)),
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 2,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
								Data: mustEncode(&commonpb.Payload{
									Data: []byte("some-random-data-sc1"),
								}),
							},
						},
					},
				},
			},
		},
		"SubComponent1/SubComponent11": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						TypeId: testSubComponent11TypeID,
						PureTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								TypeId: testPureTaskTypeID,
								// Expired, and physical task not created
								ScheduledTime:             timestamppb.New(now),
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 3,
								PhysicalTaskStatus:        physicalTaskStatusNone,
								Data: mustEncode(&commonpb.Payload{
									Data: []byte("some-random-data-sc11-1"),
								}),
							},
							{
								TypeId: testPureTaskTypeID,
								// Expired, but when processing this task, delete the SubComponent11 itself.
								ScheduledTime:             timestamppb.New(now),
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 4,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
								Data: mustEncode(&commonpb.Payload{
									Data: []byte("some-random-data-sc11-2"),
								}),
							},
							{
								TypeId: testPureTaskTypeID,
								// Expired, but should not be executed because previous task deletes SubComponent1
								// (this node's parent).
								ScheduledTime:             timestamppb.New(now),
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 5,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
								Data: mustEncode(&commonpb.Payload{
									Data: []byte("some-random-data-sc11-3"),
								}),
							},
						},
					},
				},
			},
		},
		"SubComponent2": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						TypeId: testSubComponent2TypeID,
						PureTasks: []*persistencespb.ChasmComponentAttributes_Task{
							{
								TypeId: testPureTaskTypeID,
								// Expired. However, this task won't be executed because the node is deleted
								// when processing the pure task from the root component.
								ScheduledTime:             timestamppb.New(now),
								VersionedTransition:       &persistencespb.VersionedTransition{TransitionCount: 1},
								VersionedTransitionOffset: 6,
								PhysicalTaskStatus:        physicalTaskStatusCreated,
								Data: mustEncode(&commonpb.Payload{
									Data: []byte("some-random-data-sc2"),
								}),
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

	processedTaskData := [][]byte{}
	err = root.EachPureTask(now.Add(time.Minute), func(executor NodePureTask, taskAttributes TaskAttributes, task any) (bool, error) {
		s.NotNil(executor)
		s.NotNil(taskAttributes)

		testPureTask, ok := task.(*TestPureTask)
		s.True(ok)

		processedTaskData = append(processedTaskData, testPureTask.Payload.Data)

		// When processing root component task, delete SubComponent2 to verify its task is not executed.
		if slices.Equal(
			testPureTask.Payload.Data,
			[]byte("some-random-data-root"),
		) {
			mutableContext := NewMutableContext(context.Background(), root)
			rootComponent, err := root.Component(mutableContext, ComponentRef{})
			s.NoError(err)

			rootComponent.(*TestComponent).SubComponent2 = NewEmptyField[*TestSubComponent2]()
		}

		// When processing task for SubComponent11, delete its parent SubComponent1 so that the remaining task is not executed.
		if slices.Equal(
			testPureTask.Payload.Data,
			[]byte("some-random-data-sc11-2"),
		) {
			mutableContext := NewMutableContext(context.Background(), root)
			rootComponent, err := root.Component(mutableContext, ComponentRef{})
			s.NoError(err)

			rootComponent.(*TestComponent).SubComponent1 = NewEmptyField[*TestSubComponent1]()
		}

		return true, nil
	})
	s.NoError(err)
	s.Equal([][]byte{
		[]byte("some-random-data-root"),
		[]byte("some-random-data-sc11-1"),
		[]byte("some-random-data-sc11-2"),
	}, processedTaskData)
	s.Len(root.taskValueCache, 1) // only one task from root component
}

func (s *nodeSuite) TestExecutePureTask() {
	persistenceNodes := map[string]*persistencespb.ChasmNode{
		"": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						TypeId: testComponentTypeID,
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
				gomock.AssignableToTypeOf(&mutableCtx{}),
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
	root.setValueState(valueStateSynced)
	expectExecute(nil)
	expectValidate(true, nil)
	executed, err := root.ExecutePureTask(ctx, taskAttributes, pureTask)
	s.NoError(err)
	s.True(executed)
	s.Equal(valueStateNeedSyncStructure, root.valueState)

	expectedErr := errors.New("dummy")

	// Succeed validation, fail execution.
	root.setValueState(valueStateSynced)
	expectExecute(expectedErr)
	expectValidate(true, nil)
	_, err = root.ExecutePureTask(ctx, taskAttributes, pureTask)
	s.ErrorIs(expectedErr, err)
	s.Equal(valueStateNeedSyncStructure, root.valueState)

	// Fail task validation (no execution occurs).
	root.setValueState(valueStateSynced)
	expectValidate(false, nil)
	executed, err = root.ExecutePureTask(ctx, taskAttributes, pureTask)
	s.NoError(err)
	s.False(executed)
	s.Equal(valueStateSynced, root.valueState) // task not executed, so node is clean

	// Error during task validation (no execution occurs).
	root.setValueState(valueStateSynced)
	expectValidate(false, expectedErr)
	_, err = root.ExecutePureTask(ctx, taskAttributes, pureTask)
	s.ErrorIs(expectedErr, err)
	s.Equal(valueStateSynced, root.valueState) // task not executed, so node is clean
}

func (s *nodeSuite) TestValidatePureTask() {
	taskAttributes := TaskAttributes{}
	pureTask := &TestPureTask{
		Payload: &commonpb.Payload{
			Data: []byte("some-random-data"),
		},
	}

	root := s.testComponentTree()
	_, err := root.CloseTransaction()
	s.NoError(err)

	ctx := context.Background()
	expectValidate := func(retValue bool, errValue error) {
		s.testLibrary.mockPureTaskValidator.EXPECT().
			Validate(gomock.Any(), gomock.Any(), gomock.Eq(taskAttributes), gomock.Any()).Return(retValue, errValue).Times(1)
	}

	// Succeed task validation (happy case).
	expectValidate(true, nil)
	valid, err := root.ValidatePureTask(ctx, taskAttributes, pureTask)
	s.NoError(err)
	s.True(valid)
	s.Equal(valueStateSynced, root.valueState) // node is always clean for task validation

	// Invalid task (validation returns false).
	expectValidate(false, nil)
	valid, err = root.ValidatePureTask(ctx, taskAttributes, pureTask)
	s.NoError(err)
	s.False(valid)
	s.Equal(valueStateSynced, root.valueState) // node is always clean for task validation

	// Error during task validation (no execution occurs).
	expectedErr := errors.New("dummy")
	expectValidate(false, expectedErr)
	_, err = root.ValidatePureTask(ctx, taskAttributes, pureTask)
	s.ErrorIs(expectedErr, err)
	s.Equal(valueStateSynced, root.valueState) // node is always clean for task validation

	// Close the root component.
	mutableCtx := NewMutableContext(ctx, root)
	rootComponent, err := root.ComponentByPath(mutableCtx, rootPath)
	s.NoError(err)
	rootComponent.(*TestComponent).Complete(mutableCtx)
	_, err = root.CloseTransaction()
	s.NoError(err)

	// Invalid task for sub-component due to access rule.
	subComponent1, ok := root.children["SubComponent1"]
	s.True(ok)
	valid, err = subComponent1.ValidatePureTask(ctx, taskAttributes, pureTask)
	s.NoError(err)
	s.False(valid)
	s.Equal(valueStateSynced, subComponent1.valueState) // node is always clean for task validation
}

func (s *nodeSuite) TestExecuteSideEffectTask() {
	persistenceNodes := map[string]*persistencespb.ChasmNode{
		"": {
			Metadata: &persistencespb.ChasmNodeMetadata{
				InitialVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
				Attributes: &persistencespb.ChasmNodeMetadata_ComponentAttributes{
					ComponentAttributes: &persistencespb.ChasmComponentAttributes{
						TypeId: testComponentTypeID,
					},
				},
			},
		},
	}

	taskInfo := &persistencespb.ChasmTaskInfo{
		ComponentInitialVersionedTransition: &persistencespb.VersionedTransition{
			TransitionCount: 1,
		},
		ComponentLastUpdateVersionedTransition: &persistencespb.VersionedTransition{
			TransitionCount: 1,
		},
		Path:   rootPath,
		TypeId: testSideEffectTaskTypeID,
		Data: &commonpb.DataBlob{
			Data:         nil,
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		},
	}
	workflowKey := definition.NewWorkflowKey(
		primitives.NewUUID().String(),
		primitives.NewUUID().String(),
		primitives.NewUUID().String(),
	)
	chasmTask := &tasks.ChasmTask{
		WorkflowKey:         workflowKey,
		VisibilityTimestamp: s.timeSource.Now(),
		TaskID:              123,
		Category:            tasks.CategoryOutbound,
		Destination:         "destination",
		Info:                taskInfo,
	}
	executionKey := ExecutionKey{
		NamespaceID: chasmTask.NamespaceID,
		BusinessID:  chasmTask.WorkflowID,
		RunID:       chasmTask.RunID,
	}

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
				gomock.Eq(TaskAttributes{
					chasmTask.GetVisibilityTime(),
					chasmTask.Destination,
				}),
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
	err = root.ExecuteSideEffectTask(ctx, s.registry, executionKey, chasmTask, dummyValidationFn)
	s.NoError(err)
	s.True(backendValidtionFnCalled)
	s.True(chasmTask.DeserializedTask.IsValid())

	// Invalid task.
	expectValidate(false, nil)
	expectExecute(nil)
	err = root.ExecuteSideEffectTask(ctx, s.registry, executionKey, chasmTask, dummyValidationFn)
	s.Error(err)
	s.IsType(&serviceerror.NotFound{}, err)
	s.True(chasmTask.DeserializedTask.IsValid())

	// Failed to validate task.
	validationErr := errors.New("validation error")
	expectValidate(false, validationErr)
	expectExecute(nil)
	err = root.ExecuteSideEffectTask(ctx, s.registry, executionKey, chasmTask, dummyValidationFn)
	s.ErrorIs(validationErr, err)
	s.False(chasmTask.DeserializedTask.IsValid())

	// Fail task execution.
	expectValidate(true, nil)
	executionErr := errors.New("execution error")
	expectExecute(executionErr)
	err = root.ExecuteSideEffectTask(ctx, s.registry, executionKey, chasmTask, dummyValidationFn)
	s.ErrorIs(executionErr, err)
	s.True(backendValidtionFnCalled)
	s.False(chasmTask.DeserializedTask.IsValid())
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
		Path:   rootPath,
		TypeId: testSideEffectTaskTypeID,
		Data: &commonpb.DataBlob{
			Data:         nil,
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		},
	}
	workflowKey := definition.NewWorkflowKey(
		primitives.NewUUID().String(),
		primitives.NewUUID().String(),
		primitives.NewUUID().String(),
	)
	chasmTask := &tasks.ChasmTask{
		WorkflowKey:         workflowKey,
		VisibilityTimestamp: s.timeSource.Now(),
		TaskID:              123,
		Category:            tasks.CategoryTransfer,
		Info:                taskInfo,
	}

	root := s.testComponentTree()

	mockEngine := NewMockEngine(s.controller)
	ctx := NewEngineContext(context.Background(), mockEngine)

	expectValidate := func(componentType any, retValue bool, errValue error) {
		s.testLibrary.mockSideEffectTaskValidator.EXPECT().
			Validate(
				gomock.AssignableToTypeOf((*immutableCtx)(nil)),
				gomock.AssignableToTypeOf(componentType),
				gomock.Eq(TaskAttributes{
					ScheduledTime: chasmTask.GetVisibilityTime(),
					Destination:   chasmTask.Destination,
				}),
				gomock.AssignableToTypeOf(&TestSideEffectTask{}),
			).Return(retValue, errValue).Times(1)
	}

	// Succeed validation as valid.
	expectValidate((*TestComponent)(nil), true, nil)
	isValid, err := root.ValidateSideEffectTask(ctx, chasmTask)
	s.True(isValid)
	s.NoError(err)
	s.True(chasmTask.DeserializedTask.IsValid())

	// Succeed validation as invalid.
	expectValidate((*TestComponent)(nil), false, nil)
	isValid, err = root.ValidateSideEffectTask(ctx, chasmTask)
	s.False(isValid)
	s.NoError(err)
	s.True(chasmTask.DeserializedTask.IsValid())

	// Fail validation.
	expectedErr := errors.New("validation failed")
	expectValidate((*TestComponent)(nil), false, expectedErr)
	isValid, err = root.ValidateSideEffectTask(ctx, chasmTask)
	s.False(isValid)
	s.ErrorIs(expectedErr, err)
	s.False(chasmTask.DeserializedTask.IsValid())

	// Succeed validation as valid for a sub component.
	childTaskInfo := taskInfo
	childTaskInfo.Path = []string{"SubComponent1"}
	childWorkflowKey := definition.NewWorkflowKey(
		primitives.NewUUID().String(),
		primitives.NewUUID().String(),
		primitives.NewUUID().String(),
	)
	childChasmTask := &tasks.ChasmTask{
		WorkflowKey:         childWorkflowKey,
		VisibilityTimestamp: s.timeSource.Now(),
		TaskID:              124,
		Category:            tasks.CategoryTransfer,
		Info:                childTaskInfo,
	}
	expectValidate((*TestSubComponent1)(nil), true, nil)
	isValid, err = root.ValidateSideEffectTask(ctx, childChasmTask)
	s.True(isValid)
	s.NoError(err)
	s.True(childChasmTask.DeserializedTask.IsValid())

	// Succeed validation as invalid since parent is closed.
	mutableCtx := NewMutableContext(ctx, root)
	rootComponent, err := root.ComponentByPath(mutableCtx, rootPath)
	s.NoError(err)
	rootComponent.(*TestComponent).Complete(mutableCtx)
	// Note there's also no mock for task validator here in this case.
	// Access rule is checked first.
	isValid, err = root.ValidateSideEffectTask(ctx, childChasmTask)
	s.False(isValid)
	s.NoError(err)
	s.True(childChasmTask.DeserializedTask.IsValid())
}

func (s *nodeSuite) newTestTree(
	serializedNodes map[string]*persistencespb.ChasmNode,
) (*Node, error) {
	if len(serializedNodes) == 0 {
		return NewEmptyTree(s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger, s.metricsHandler), nil
	}
	return NewTreeFromDB(serializedNodes, s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger, s.metricsHandler)
}
