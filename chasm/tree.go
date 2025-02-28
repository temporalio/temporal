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

//go:generate mockgen -copyright_file ../LICENSE -package $GOPACKAGE -source $GOFILE -destination tree_mock.go

package chasm

import (
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
)

type (
	// Node is the in-memory representation of a persisted CHASM node.
	//
	// Node and all its methods are NOT meant to be used by CHASM component authors.
	// They are exported for use by the CHASM engine and underlying MutableState implementation only.
	Node struct {
		*nodeBase

		parent      *Node
		children    map[string]*Node
		persistence *persistencespb.ChasmNode

		// TODO: add other necessary fields here, e.g.
		//
		// key string   // key of this node in parent's children map.
		// instance any // deserialized node state
		// dirty    bool
	}

	// NodesMutation is a set of mutations for all nodes rooted at a given node n,
	// including the node n itself.
	NodesMutation struct {
		UpdatedNodes map[string]*persistencespb.ChasmNode // flattened node path -> chasm node
		DeletedNodes map[string]struct{}
	}

	// NodesSnapshot is a snapshot for all nodes rooted at a given node n,
	// including the node n itself.
	NodesSnapshot struct {
		Nodes map[string]*persistencespb.ChasmNode // flattened node path -> chasm node
	}

	// NodeBackend is a set of methods needed from MutableState
	//
	// This is for breaking cycle dependency between
	// this package and service/history/workflow package
	// where MutableState is defined.
	NodeBackend interface {
		// TODO: Add methods needed from MutateState here.
	}

	// NodePathEncoder is an interface for encoding and decoding node paths.
	// Logic outside the chasm package should only work with encoded paths.
	NodePathEncoder interface {
		Encode(node *Node, path []string) (string, error)
		Decode(encodedPath string) ([]string, error)
	}
)

type (
	// nodeBase is a set of dependencies and states shared by all nodes in a CHASM tree.
	nodeBase struct {
		registry    *Registry
		timeSource  clock.TimeSource
		backend     NodeBackend
		pathEncoder NodePathEncoder
	}
)

// NewTree creates a new in-memory CHASM tree from a collection of flattened persistence CHASM nodes.
func NewTree(
	registry *Registry,
	persistenceNodes map[string]*persistencespb.ChasmNode,
	timeSource clock.TimeSource,
	backend NodeBackend,
	pathEncoder NodePathEncoder,
) (*Node, error) {
	base := &nodeBase{
		registry:    registry,
		timeSource:  timeSource,
		backend:     backend,
		pathEncoder: pathEncoder,
	}

	root := newNode(base, nil)
	for encodedPath, pNode := range persistenceNodes {
		nodePath, err := pathEncoder.Decode(encodedPath)
		if err != nil {
			return nil, err
		}
		root.insert(nodePath, pNode, base)
	}

	return root, nil
}

// Component retrieves a component from the tree rooted at node n
// using the provided component reference
// It also performs consistency, access rule, and task validation checks
// (for task processing requests) before returning the component.
func (n *Node) Component(
	chasmContext Context,
	ref ComponentRef,
) (Component, error) {
	panic("not implemented")
}

// Ref implements the CHASM Context interface
func (n *Node) Ref(
	component Component,
) (ComponentRef, bool) {
	panic("not implemented")
}

// Now implements the CHASM Context interface
func (n *Node) Now(
	_ Component,
) time.Time {
	// TODO: Now() could be different for components after we support Pause for CHASM components.
	return n.timeSource.Now()
}

// AddTask implements the CHASM MutableContext interface
func (n *Node) AddTask(
	component Component,
	taskAttributes TaskAttributes,
	task interface{},
) error {
	panic("not implemented")
}

// CloseTransactionAsMutation is used by MutableState
// to close the transaction and persist mutations into DB.
func (n *Node) CloseTransactionAsMutation() (*NodesMutation, error) {
	panic("not implemented")
}

// CloseTransactionAsSnapshot is used by MutableState
// to close the transaction and persist entire CHASM tree into DB.
func (n *Node) CloseTransactionAsSnapshot() (*NodesSnapshot, error) {
	panic("not implemented")
}

// ApplyMutation is used by replication stack to apply node
// mutations from the source cluster.
func (n *Node) ApplyMutation(
	mutation NodesMutation,
) error {
	panic("not implemented")
}

// ApplySnapshot is used by replication stack to apply node
// snapshot from the source cluster.
//
// If we simply substituting the entire CHASM tree, we will be
// forced to close the transaction as snapshot and potentially
// write extra data to persistence.
// This method will instead figure out the mutations needed to
// bring the current tree to the be the same as the snapshot,
// thus allowing us to close the transaction as mutation.
func (n *Node) ApplySnapshot(
	snapshot NodesSnapshot,
) error {
	panic("not implemented")
}

func (n *Node) insert(
	nodePath []string,
	pNode *persistencespb.ChasmNode,
	nodeBase *nodeBase,
) {
	if len(nodePath) == 0 {
		n.persistence = pNode
		return
	}

	childName := nodePath[0]
	childNode, ok := n.children[childName]
	if !ok {
		childNode = newNode(nodeBase, n)
		n.children[childName] = childNode
	}
	childNode.insert(nodePath[1:], pNode, nodeBase)
}

func newNode(
	base *nodeBase,
	parent *Node,
) *Node {
	return &Node{
		nodeBase: base,
		parent:   parent,
		children: make(map[string]*Node),
	}
}
