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

	root, err := NewTree(s.registry, persistenceNodes, s.timeSource, s.nodeBackend, s.nodePathEncoder)
	s.NoError(err)
	s.NotNil(root)

	preorderNodes := s.preorderAndAssertParent(root, nil)
	s.Len(preorderNodes, 5)
	s.Equal(expectedPreorderNodes, preorderNodes)
}

func (s *nodeSuite) preorderAndAssertParent(
	n *Node,
	parent *Node,
) []*persistencespb.ChasmNode {
	s.Equal(parent, n.parent)

	var nodes []*persistencespb.ChasmNode
	nodes = append(nodes, n.persistence)

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
