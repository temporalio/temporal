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
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/testing/protorequire"
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
	s.timeSource = clock.NewEventTimeSource()
	s.nodePathEncoder = &testNodePathEncoder{}
}

func (s *nodeSuite) TestNewTree() {
	persistenceNodes := map[string]*persistencespb.ChasmNode{
		"": {
			InitialVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 1},
		},
		"child1": {
			InitialVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 2},
		},
		"child2": {
			InitialVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 3},
		},
		"child1/grandchild1": {
			InitialVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 4},
		},
		"child2/grandchild1": {
			InitialVersionedTransition: &persistencespb.VersionedTransition{TransitionCount: 5},
		},
	}
	expectedPreorderNodes := []*persistencespb.ChasmNode{
		persistenceNodes[""],
		persistenceNodes["child1"],
		persistenceNodes["child1/grandchild1"],
		persistenceNodes["child2"],
		persistenceNodes["child2/grandchild1"],
	}

	root, err := NewTree(persistenceNodes, s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder)
	s.NoError(err)
	s.NotNil(root)

	preorderNodes := s.preorderAndAssertParent(root, nil)
	s.Len(preorderNodes, 5)
	s.Equal(expectedPreorderNodes, preorderNodes)
}

func (s *nodeSuite) TestSetValue_TypeComponent() {
	component := s.newTestComponent()

	node := newNode(s.nodeBase(), nil)
	err := node.setValue(component, fieldTypeComponent)
	s.NoError(err)

	// Assert that tree is constructed.
	s.Equal(component, node.value)
	s.NotNil(node.serializedValue.GetComponentAttributes(), "node serialized value must have attributes created")
	s.Nil(node.serializedValue.GetComponentAttributes().GetData(), "node serialized value must not have data before serialize is called")

	sc1Node := node.children["SubComponent1"]
	s.NotNil(sc1Node)
	s.NotNil(sc1Node.value, "child node value must be set and point to sub-component")
	s.Equal(component.SubComponent1.Internal.value, sc1Node.value, "child node value must be set and point to sub-component")
	s.Equal(sc1Node, component.SubComponent1.Internal.node, "Internal.node must be set and point to just created child node")
	s.NotNil(sc1Node.serializedValue.GetComponentAttributes(), "child node serialized value must have attributes created")
	s.Nil(sc1Node.serializedValue.GetComponentAttributes().GetData(), "child node serialized value must not have data before serialize is called")

	sc11Node := node.children["SubComponent1"].children["SubComponent11"]
	s.NotNil(sc11Node)
	s.NotNil(sc11Node.value, "child node value must be set and point to sub-component")
	s.Equal(component.SubComponent1.Internal.value.(*TestSubComponent1).SubComponent11.Internal.value, sc11Node.value, "child node value must be set and point to sub-component")
	s.Equal(sc11Node, component.SubComponent1.Internal.value.(*TestSubComponent1).SubComponent11.Internal.node, "Internal.node must be set and point to just created child node")
	s.NotNil(sc11Node.serializedValue.GetComponentAttributes(), "child node serialized value must have attributes created")
	s.Nil(sc11Node.serializedValue.GetComponentAttributes().GetData(), "child node serialized value must not have data before serialize is called")

	sd1Node := node.children["SubData1"]
	s.NotNil(sd1Node)
	s.NotNil(sd1Node.value, "child node value must be set and point to sub-component")
	s.Equal(component.SubData1.Internal.value, sd1Node.value, "child node value must be set and point to sub-component")
	s.Equal(sd1Node, component.SubData1.Internal.node, "Internal.node must be set and point to just created child node")
	s.NotNil(sd1Node.serializedValue.GetDataAttributes(), "child node serialized value must have attributes created")
	s.Nil(sd1Node.serializedValue.GetDataAttributes().GetData(), "child node serialized value must not have data before serialize is called")

	s.Nil(node.children["SubComponent2"])
}

func (s *nodeSuite) TestSerializeNode_ComponentAttributes() {
	component := s.newTestComponent()

	node := newNode(s.nodeBase(), nil)
	err := node.setValue(component, fieldTypeComponent)
	s.NoError(err)

	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(1)).Times(2) // 2 because component and sub-component
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).Times(2)

	err = node.serialize()
	s.NoError(err)

	s.NotNil(node.serializedValue)
	s.NotNil(node.serializedValue.GetComponentAttributes().GetData(), "node serialized value must have data after serialize is called")
	s.Equal("chasm.TestComponent", node.serializedValue.GetComponentAttributes().GetType(), "node serialized value must have type set")

	// serialization is not recursive and must be called for every node.
	sc1Node := node.children["SubComponent1"]
	err = sc1Node.serialize()
	s.NoError(err)
	s.NotNil(sc1Node.serializedValue.GetComponentAttributes().GetData(), "child node serialized value must have data after serialize is called")
	s.Equal("chasm.TestSubComponent1", sc1Node.serializedValue.GetComponentAttributes().GetType(), "node serialized value must have type set")

	sd1Node := node.children["SubData1"]
	err = sd1Node.serialize()
	s.NoError(err)
	s.NotNil(sd1Node.serializedValue.GetDataAttributes().GetData(), "child node serialized value must have data after serialize is called")
}

func (s *nodeSuite) TestSetValue_TypeData() {
	component := &persistencespb.ActivityInfo{ // Random proto type
		ActivityId: "22",
	}

	node := newNode(s.nodeBase(), nil)
	err := node.setValue(component, fieldTypeData)
	s.NoError(err)
	s.NotNil(node)
	s.Equal(component, node.value)
	s.NotNil(node.serializedValue.GetDataAttributes(), "node serialized value must have attributes created")
	s.Nil(node.serializedValue.GetDataAttributes().GetData(), "node serialized value must not have data before serialize is called")
}
func (s *nodeSuite) TestSerializeNode_DataAttributes() {
	component := &persistencespb.ActivityInfo{ // Random proto type
		ActivityId: "22",
	}

	node := newNode(s.nodeBase(), nil)
	err := node.setValue(component, fieldTypeData)
	s.NoError(err)

	err = node.serialize()
	s.NoError(err)
	s.NotNil(node.serializedValue.GetDataAttributes().GetData(), "child node serialized value must have data after serialize is called")
}

func (s *nodeSuite) TestSyncChildren_Leaf() {
	component := s.newTestComponent()

	node := newNode(s.nodeBase(), nil)
	err := node.setValue(component, fieldTypeComponent)
	s.NoError(err)

	// Set very leaf node to nil.
	component.SubComponent1.Internal.value.(*TestSubComponent1).SubComponent11 = nil
	s.NotNil(node.children["SubComponent1"].children["SubComponent11"])

	rps, err := node.syncChildren()
	s.NoError(err)

	s.Len(rps, 1)
	s.Equal("SubComponent1/SubComponent11", rps[0])
	s.Nil(node.children["SubComponent1"].children["SubComponent11"])
}

func (s *nodeSuite) TestSyncChildren_MiddleNode() {
	component := s.newTestComponent()

	node := newNode(s.nodeBase(), nil)
	err := node.setValue(component, fieldTypeComponent)
	s.NoError(err)
	s.NotNil(node)

	// Set subcomponent at middle node to nil.
	component.SubComponent1 = nil
	s.NotNil(node.children["SubComponent1"])

	rps, err := node.syncChildren()
	s.NoError(err)

	s.Len(rps, 3)
	s.Contains(rps, "SubComponent1/SubComponent11")
	s.Contains(rps, "SubComponent1/SubData11")
	s.Contains(rps, "SubComponent1")

	s.Nil(node.children["SubComponent1"])
}

func (s *nodeSuite) TestDeserializeNode_ComponentAttributes() {
	component := s.newTestComponent()

	node := newNode(s.nodeBase(), nil)
	err := node.setValue(component, fieldTypeComponent)
	s.NoError(err)

	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(1)).Times(2) // 2 because component and sub-component
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).Times(2)

	serializedNodes := map[string]*persistencespb.ChasmNode{}

	err = node.serialize()
	s.NoError(err)
	serializedNodes[""] = node.serializedValue

	for childName, childNode := range node.children {
		err = childNode.serialize()
		s.NoError(err)
		serializedNodes[childName] = childNode.serializedValue
	}

	node2, err := NewTree(serializedNodes, s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder)
	s.NoError(err)
	s.Nil(node2.value)

	err = node2.deserialize(reflect.TypeOf(&TestComponent{}))
	s.NoError(err)
	s.NotNil(node2.value)
	s.EqualExportedValues(node.value, node2.value)
	s.Equal(node2.value.(*TestComponent).SubComponent1.Internal.node, node2.children["SubComponent1"])

	s.Nil(node2.value.(*TestComponent).SubComponent1.Internal.value)
	err = node2.value.(*TestComponent).SubComponent1.Internal.node.deserialize(reflect.TypeOf(&TestSubComponent1{}))
	s.NoError(err)
	s.NotNil(node2.value.(*TestComponent).SubComponent1.Internal.node.value)

	s.NotNil(node2.children["SubComponent1"].value)
	node2.children["SubComponent1"].value = nil
	err = node2.children["SubComponent1"].deserialize(reflect.TypeOf(&TestSubComponent1{}))
	s.NotNil(node2.children["SubComponent1"].value)
}

func (s *nodeSuite) TestDeserializeNode_DataAttributes() {
	component := &persistencespb.ActivityInfo{ // Random proto type
		ActivityId: "22",
	}

	node := newNode(s.nodeBase(), nil)
	err := node.setValue(component, fieldTypeData)
	s.NoError(err)

	serializedNodes := map[string]*persistencespb.ChasmNode{}
	err = node.serialize()
	s.NoError(err)
	serializedNodes[""] = node.serializedValue

	node2, err := NewTree(serializedNodes, s.registry, s.timeSource, s.nodeBackend, s.nodePathEncoder)
	s.NoError(err)
	s.NotNil(node2)

	err = node2.deserialize(reflect.TypeOf(&persistencespb.ActivityInfo{}))
	s.NoError(err)
	s.NotNil(node2.value)
	s.ProtoEqual(node.value.(*persistencespb.ActivityInfo), node2.value.(*persistencespb.ActivityInfo))
}

func (s *nodeSuite) newTestComponent() *TestComponent {
	component := &TestComponent{
		ComponentData: &persistencespb.ActivityInfo{
			ActivityId: "component-data",
		},
		SubComponent1: NewComponentField[*TestSubComponent1](nil, &TestSubComponent1{
			SubComponent1Data: &persistencespb.ActivityInfo{ // Random proto type
				ActivityId: "sub-component1-data",
			},
			SubComponent11: NewComponentField[*TestSubComponent11](nil, &TestSubComponent11{
				SubComponent11Data: &persistencespb.ActivityInfo{ // Random proto type
					ActivityId: "sub-component1-sub-component11-data",
				},
			}),
			SubData11: NewDataField[*persistencespb.ActivityInfo](nil, &persistencespb.ActivityInfo{
				ActivityId: "sub-component1-sub-data11",
			}),
		}),
		SubComponent2: nil,
		SubData1: NewDataField[*persistencespb.ActivityInfo](nil,
			&persistencespb.ActivityInfo{
				ActivityId: "sub-data1",
			}),
	}

	return component
}

func (s *nodeSuite) preorderAndAssertParent(
	n *Node,
	parent *Node,
) []*persistencespb.ChasmNode {
	s.Equal(parent, n.parent)

	var nodes []*persistencespb.ChasmNode
	nodes = append(nodes, n.serializedValue)

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
