// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package chasm

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/protoassert"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testlogger"
	"go.uber.org/mock/gomock"
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
	s.Assertions = require.New(s.T())
	s.ProtoAssertions = protorequire.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.nodeBackend = NewMockNodeBackend(s.controller)

	s.registry = NewRegistry()
	err := s.registry.Register(newTestLibrary())
	s.NoError(err)

	s.timeSource = clock.NewEventTimeSource()
	s.nodePathEncoder = &testNodePathEncoder{}
	s.logger = testlogger.NewTestLogger(s.T(), testlogger.FailOnAnyUnexpectedError)
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

	root, err := NewTree(persistenceNodes, s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, log.NewTestLogger())
	s.NoError(err)
	s.NotNil(root)

	preorderNodes := s.preorderAndAssertParent(root, nil)
	s.Len(preorderNodes, 5)
	s.Equal(expectedPreorderNodes, preorderNodes)
}

func (s *nodeSuite) TestSetValue_TypeComponent() {
	component := &TestComponent{}

	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(1)).Times(1)
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).Times(1)

	node := newNode(s.nodeBase(), nil, "")
	err := node.setValue(component, fieldTypeComponent)
	s.NoError(err)

	// Assert that tree is constructed.
	s.Equal(component, node.value)
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
	s.NotNil(node.serializedValue.GetMetadata().GetComponentAttributes())
	s.Nil(node.serializedValue.GetData())
	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(2)).Times(1) // for LastUpdatesVersionedTransition for TestComponent
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(2)).Times(1)
	err := node.serialize()
	s.NoError(err)
	s.NotNil(node.serializedValue)
	s.NotNil(node.serializedValue.GetData(), "node serialized value must have data after serialize is called")
	s.Equal("test_component", node.serializedValue.GetMetadata().GetComponentAttributes().GetType(), "node serialized value must have type set")

	// Serialize subcomponents (there are 2 subcomponents).
	sc1Node := node.children["SubComponent1"]
	s.NotNil(sc1Node.serializedValue.GetMetadata().GetComponentAttributes())
	s.Nil(sc1Node.serializedValue.GetData())
	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(2)).Times(2) // for LastUpdatesVersionedTransition of TestSubComponent1
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(2)).Times(2)
	for _, childNode := range node.children {
		err = childNode.serialize()
		s.NoError(err)
	}
	s.NotNil(sc1Node.serializedValue.GetData(), "child node serialized value must have data after serialize is called")
	s.Equal("test_sub_component_1", sc1Node.serializedValue.GetMetadata().GetComponentAttributes().GetType(), "node serialized value must have type set")

	// Check SubData too.
	sd1Node := node.children["SubData1"]
	s.NoError(err)
	s.NotNil(sd1Node.serializedValue.GetData(), "child node serialized value must have data after serialize is called")
}

func (s *nodeSuite) TestSetValue_TypeData() {
	component := &protoMessageType{
		ActivityId: "22",
	}

	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(1)).Times(1) // for InitialVersionedTransition
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).Times(1)

	node := newNode(s.nodeBase(), nil, "")
	err := node.setValue(component, fieldTypeData)
	s.NoError(err)
	s.NotNil(node)
	s.Equal(component, node.value)
	s.NotNil(node.serializedNode.GetMetadata().GetDataAttributes(), "node serializedNode must have attributes created")
	s.Nil(node.serializedNode.GetData(), "node serializedNode must not have data before serialize is called")
}
func (s *nodeSuite) TestSerializeNode_DataAttributes() {
	component := &protoMessageType{
		ActivityId: "22",
	}

	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(1)).Times(1) // for InitialVersionedTransition
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).Times(1)

	node := newNode(s.nodeBase(), nil, "")
	err := node.setValue(component, fieldTypeData)
	s.NoError(err)

	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(1)).Times(1)
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).Times(1)
	err = node.serialize()
	s.NoError(err)
	s.NotNil(node.serializedValue.GetData(), "child node serialized value must have data after serialize is called")
	s.Equal([]byte{0x42, 0x2, 0x32, 0x32}, node.serializedValue.GetData().GetData())
}

func (s *nodeSuite) TestSyncSubComponents_DeleteLeafNode() {
	node := s.testComponentTree()

	component := node.value.(*TestComponent)

	// Set very leaf node to empty.
	component.SubComponent1.Internal.value.(*TestSubComponent1).SubComponent11 = NewEmptyField[*TestSubComponent11]()
	s.NotNil(node.children["SubComponent1"].children["SubComponent11"])

	rps, err := node.syncSubComponents()
	s.NoError(err)

	s.Len(rps, 1)
	s.Equal("SubComponent1/SubComponent11", rps[0])
	s.Nil(node.children["SubComponent1"].children["SubComponent11"])
}

func (s *nodeSuite) TestSyncSubComponents_DeleteMiddleNode() {
	node := s.testComponentTree()

	component := node.value.(*TestComponent)

	// Set subcomponent at middle node to nil.
	component.SubComponent1 = NewEmptyField[*TestSubComponent1]()
	s.NotNil(node.children["SubComponent1"])

	rps, err := node.syncSubComponents()
	s.NoError(err)

	s.Len(rps, 3)
	s.Contains(rps, "SubComponent1/SubComponent11")
	s.Contains(rps, "SubComponent1/SubData11")
	s.Contains(rps, "SubComponent1")

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

	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(1)).Times(1) // for InitialVersionedTransition
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).Times(1)

	err = node.deserialize(reflect.TypeOf(&TestComponent{}))
	s.NoError(err)
	s.NotNil(node.value)
	s.IsType(&TestComponent{}, node.value)
	tc := node.value.(*TestComponent)
	s.Nil(tc.SubComponent1.Internal.node)
	s.Nil(tc.SubComponent1.Internal.value)
	s.Nil(tc.ComponentData)
}

func (s *nodeSuite) TestDeserializeNode_ComponentAttributes() {
	serializedNodes := testComponentSerializedNodes()

	node, err := NewTree(serializedNodes, s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger)
	s.NoError(err)
	s.Nil(node.value)
	s.NotNil(node.serializedNode)

	err = node.deserialize(reflect.TypeOf(&TestComponent{}))
	s.NoError(err)
	s.NotNil(node.value)
	s.IsType(&TestComponent{}, node.value)
	tc := node.value.(*TestComponent)
	s.Equal(tc.SubComponent1.Internal.node, node.children["SubComponent1"])
	s.Equal(tc.ComponentData.ActivityId, "component-data")

	s.Nil(tc.SubComponent1.Internal.value)
	err = tc.SubComponent1.Internal.node.deserialize(reflect.TypeOf(&TestSubComponent1{}))
	s.NoError(err)
	s.NotNil(tc.SubComponent1.Internal.node.value)
	s.IsType(&TestSubComponent1{}, tc.SubComponent1.Internal.node.value)
	s.Equal("sub-component1-data", tc.SubComponent1.Internal.node.value.(*TestSubComponent1).SubComponent1Data.ActivityId)
}

func (s *nodeSuite) TestDeserializeNode_DataAttributes() {
	serializedNodes := testComponentSerializedNodes()

	node, err := NewTree(serializedNodes, s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger)
	s.NoError(err)
	s.Nil(node.value)
	s.NotNil(node.serializedNode)

	err = node.deserialize(reflect.TypeOf(&TestComponent{}))
	s.NoError(err)
	s.NotNil(node.value)
	s.IsType(&TestComponent{}, node.value)
	tc := node.value.(*TestComponent)

	s.Equal(tc.SubData1.Internal.node, node.children["SubData1"])

	s.Nil(tc.SubData1.Internal.value)
	err = tc.SubData1.Internal.node.deserialize(reflect.TypeOf(&protoMessageType{}))
	s.NoError(err)
	s.NotNil(tc.SubData1.Internal.node.value)
	s.IsType(&protoMessageType{}, tc.SubData1.Internal.node.value)
	s.Equal("sub-data1", tc.SubData1.Internal.node.value.(*protoMessageType).ActivityId)
}

func (s *nodeSuite) TestGenerateSerializedNodes() {
	s.T().Skip("This test is used to generate serialized nodes for other tests.")

	serializedNodes := map[string]*persistencespb.ChasmNode{}

	node := s.testComponentTree()

	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(2)).Times(1) // for LastUpdatesVersionedTransition for TestComponent
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(2)).Times(1)
	err := node.serialize()
	s.NoError(err)
	serializedNodes[""] = node.serializedValue

	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(2)).Times(2) // for LastUpdatesVersionedTransition of TestSubComponent1
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(2)).Times(2)
	for childName, childNode := range node.children {
		err = childNode.serialize()
		s.NoError(err)
		serializedNodes[childName] = childNode.serializedValue
	}

	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(2)).Times(2) // for LastUpdatesVersionedTransition of TestSubComponent1
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(2)).Times(2)
	for childName, childNode := range node.children["SubComponent1"].children {
		err = childNode.serialize()
		s.NoError(err)
		serializedNodes["SubComponent1/"+childName] = childNode.serializedValue
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

func (s *nodeSuite) TestComponent() {
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
		valueSynced     bool
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
			valueSynced:     true,
			assertComponent: assertTestComponent,
		},
		{
			name:         "success mutable access",
			chasmContext: NewMutableContext(context.Background(), root),
			ref: ComponentRef{
				componentPath: []string{}, // root
			},
			expectedErr:     nil,
			valueSynced:     false,
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
				s.Equal(tc.valueSynced, node.valueSynced)
			}
		})
	}
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
	var nilSerializedNodes map[string]*persistencespb.ChasmNode
	// Create an empty tree.
	node, err := NewTree(nilSerializedNodes, s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder, s.logger)
	s.NoError(err)
	s.Nil(node.value)

	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(1)).Times(1) // for InitialVersionedTransition of the root component.
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).Times(1)
	// Get an empty top level component from the empty tree.
	err = node.deserialize(reflect.TypeOf(&TestComponent{}))
	s.NoError(err)
	s.NotNil(node.value)

	// Create subcommponents by assigning fileds to TestComponent instance.
	setTestComponentFields(node.value.(*TestComponent))

	// Syno tree with subcomponents of TestComponent.
	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(1)).Times(4) // for InitialVersionedTransition of children.
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).Times(4)
	rps, err := node.syncSubComponents()
	s.NoError(err)
	s.Empty(rps)

	return node
}
