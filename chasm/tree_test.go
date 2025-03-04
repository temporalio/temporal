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
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.uber.org/mock/gomock"
)

type (
	nodeSuite struct {
		suite.Suite
		*require.Assertions

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

func (s *nodeSuite) TestSerializeNode_ComponentAttributes() {
	s.nodeBackend.EXPECT().NextTransitionCount().Return(int64(1)).Times(1)
	s.nodeBackend.EXPECT().GetCurrentVersion().Return(int64(1)).Times(1)

	component := TestComponent{
		ComponentData: &persistencespb.ActivityInfo{
			ActivityId: "22",
		},
		SubComponent1: NewComponentField[TestSubComponent1](nil, TestSubComponent1{
			SubComponent1Data: &persistencespb.ActivityInfo{ // Random proto type
				ActivityId: "22.8",
			},
		}),
		SubComponent2: nil,
		SubData1: NewDataField[*persistencespb.ActivityInfo](nil,
			&persistencespb.ActivityInfo{
				ActivityId: "22.78",
			}),
	}

	node := newNode(s.nodeBase(), nil)
	node.protoValue = &persistencespb.ChasmNode{
		Attributes: &persistencespb.ChasmNode_ComponentAttributes{
			ComponentAttributes: &persistencespb.ChasmComponentAttributes{},
		},
	}
	node.value = component

	mutation := &NodesMutation{
		UpdatedNodes: make(map[string]*persistencespb.ChasmNode),
		DeletedNodes: make(map[string]struct{}),
	}

	err := node.serializeNode(mutation, []string{})
	s.NoError(err)

	s.NotNil(node.protoValue)
	s.NotNil(node.protoValue.GetComponentAttributes().GetData())
	s.Equal("chasm.TestComponent", node.protoValue.GetComponentAttributes().GetType())

	s.NotNil(node.children["SubComponent1"])
	s.NotNil(node.children["SubComponent1"].value)
	s.IsType(TestSubComponent1{}, node.children["SubComponent1"].value)

	s.NotNil(node.children["SubData1"])
	s.IsType(&persistencespb.ActivityInfo{}, node.children["SubData1"].value)

	s.Nil(node.children["SubComponent2"])

	s.Empty(mutation.UpdatedNodes)
	s.Empty(mutation.DeletedNodes)
}

func (s *nodeSuite) TestSerializeNode_DataAttributes() {
	component := &persistencespb.ActivityInfo{ // Random proto type
		ActivityId: "22",
	}

	node := &Node{
		nodeBase: s.nodeBase(),
		protoValue: &persistencespb.ChasmNode{
			Attributes: &persistencespb.ChasmNode_DataAttributes{
				DataAttributes: &persistencespb.ChasmDataAttributes{},
			},
		},
		value:    component,
		children: make(map[string]*Node),
	}

	mutation := &NodesMutation{
		UpdatedNodes: make(map[string]*persistencespb.ChasmNode),
		DeletedNodes: make(map[string]struct{}),
	}

	err := node.serializeNode(mutation, []string{})
	s.NoError(err)
}

func (s *nodeSuite) preorderAndAssertParent(
	n *Node,
	parent *Node,
) []*persistencespb.ChasmNode {
	s.Equal(parent, n.parent)

	var nodes []*persistencespb.ChasmNode
	nodes = append(nodes, n.protoValue)

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
