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
	"fmt"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/common/softassert"
)

type (
	// Node is the in-memory representation of a persisted CHASM node.
	//
	// Node and all its methods are NOT meant to be used by CHASM component authors.
	// They are exported for use by the CHASM engine and underlying MutableState implementation only.
	Node struct {
		*nodeBase

		parent   *Node
		children map[string]*Node
		nodeName string // key of this node in parent's children map.

		persistence *persistencespb.ChasmNode
		value       any // deserialized component | data | collection

		// TODO: add other necessary fields here, e.g.
		//
		// dirty    bool
	}

	// NodesMutation is a set of mutations for all nodes rooted at a given node n,
	// including the node n itself.
	//
	// TODO: Return tree size changes in NodesMutation as well. MutateState needs to
	// track the overall size of itself and terminate workflow if it exceeds the limit.
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
		logger      log.Logger

		// Mutations accumulated so far in this transaction.
		mutation NodesMutation
	}
)

// NewTree creates a new in-memory CHASM tree from a collection of flattened persistence CHASM nodes.
func NewTree(
	registry *Registry,
	persistenceNodes map[string]*persistencespb.ChasmNode,
	timeSource clock.TimeSource,
	backend NodeBackend,
	pathEncoder NodePathEncoder,
	logger log.Logger,
) (*Node, error) {
	base := &nodeBase{
		registry:    registry,
		timeSource:  timeSource,
		backend:     backend,
		pathEncoder: pathEncoder,
		logger:      logger,

		mutation: NodesMutation{
			UpdatedNodes: make(map[string]*persistencespb.ChasmNode),
			DeletedNodes: make(map[string]struct{}),
		},
	}

	root := newNode(base, nil, "")
	for encodedPath, pNode := range persistenceNodes {
		nodePath, err := pathEncoder.Decode(encodedPath)
		if err != nil {
			return nil, err
		}
		root.insert(nodePath, pNode, base)
	}

	return root, nil
}

// NewEmptyTree creates a new empty in-memory CHASM tree.
func NewEmptyTree(
	registry *Registry,
	timeSource clock.TimeSource,
	backend NodeBackend,
	pathEncoder NodePathEncoder,
) *Node {
	base := &nodeBase{
		registry:    registry,
		timeSource:  timeSource,
		backend:     backend,
		pathEncoder: pathEncoder,
	}
	root := newNode(base, nil, "")
	return root
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

// CloseTransaction is used by MutableState to close the transaction and
// track changes made in the current transaction.
func (n *Node) CloseTransaction() (NodesMutation, error) {
	defer n.cleanupTransaction()

	panic("not implemented")
	// return n.mutation, nil
}

func (n *Node) cleanupTransaction() {
	n.mutation = NodesMutation{
		UpdatedNodes: make(map[string]*persistencespb.ChasmNode),
		DeletedNodes: make(map[string]struct{}),
	}
}

// Snapshot returns all nodes in the tree that have been modified after the given min versioned transition.
// A nil exclusiveMinVT will be treated as the same as the zero versioned transition and returns all nodes in the tree.
// This method should only be invoked on root CHASM node when IsDirty() is false.
func (n *Node) Snapshot(
	exclusiveMinVT *persistencespb.VersionedTransition,
) NodesSnapshot {
	if !softassert.That(n.logger, n.parent == nil, "chasm.Snapshot() should only be called on the root node") {
		panic(fmt.Sprintf("chasm.Snapshot() called on child node: %+v", n))
	}

	// TODO: add assertion on IsDirty() once implemented

	nodes := make(map[string]*persistencespb.ChasmNode)
	n.snapshotInternal(exclusiveMinVT, []string{}, nodes)

	return NodesSnapshot{
		Nodes: nodes,
	}
}

func (n *Node) snapshotInternal(
	exclusiveMinVT *persistencespb.VersionedTransition,
	currentPath []string,
	nodes map[string]*persistencespb.ChasmNode,
) {
	if n == nil {
		return
	}

	if transitionhistory.Compare(n.persistence.Metadata.LastUpdateVersionedTransition, exclusiveMinVT) > 0 {
		encodedPath, err := n.pathEncoder.Encode(n, currentPath)
		if !softassert.That(n.logger, err == nil, "chasm path encoding should always succeed on clean tree") {
			panic(fmt.Sprintf("failed to encode chasm path on clean tree: %v", err))
		}
		nodes[encodedPath] = n.persistence
	}

	for childName, childNode := range n.children {
		childNode.snapshotInternal(
			exclusiveMinVT,
			append(currentPath, childName),
			nodes,
		)
	}
}

// ApplyMutation is used by replication stack to apply node
// mutations from the source cluster.
//
// NOTE: It will be an error if UpdatedNodes and DeletedNodes have overlapping keys,
// as the CHASM tree does not have enough information to tell if the deletion happens
// before or after the update.
func (n *Node) ApplyMutation(
	mutation NodesMutation,
) error {
	if err := n.applyDeletions(mutation.DeletedNodes); err != nil {
		return err
	}

	return n.applyUpdates(mutation.UpdatedNodes)
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
	incomingSnapshot NodesSnapshot,
) error {
	currentSnapshot := n.Snapshot(nil)

	mutation := NodesMutation{
		UpdatedNodes: make(map[string]*persistencespb.ChasmNode),
		DeletedNodes: make(map[string]struct{}),
	}

	for encodedPath := range currentSnapshot.Nodes {
		if _, ok := incomingSnapshot.Nodes[encodedPath]; !ok {
			mutation.DeletedNodes[encodedPath] = struct{}{}
		}
	}

	for encodedPath, incomingNode := range incomingSnapshot.Nodes {
		currentNode, ok := currentSnapshot.Nodes[encodedPath]
		if !ok {
			mutation.UpdatedNodes[encodedPath] = incomingNode
			continue
		}

		if transitionhistory.Compare(
			currentNode.Metadata.LastUpdateVersionedTransition,
			incomingNode.Metadata.LastUpdateVersionedTransition,
		) != 0 {
			mutation.UpdatedNodes[encodedPath] = incomingNode
		}
	}

	return n.ApplyMutation(mutation)
}

func (n *Node) applyDeletions(
	deletedNodes map[string]struct{},
) error {
	for encodedPath := range deletedNodes {
		path, err := n.pathEncoder.Decode(encodedPath)
		if err != nil {
			return err
		}

		node, ok := n.getNodeByPath(path)
		if !ok {
			// Already deleted.
			// This could happen when:
			// - If the mutations passed in include changes
			// older than the current state of the tree.
			// - We are already applied the deletion on a parent node.
			continue
		}

		if err := node.delete(path, &encodedPath); err != nil {
			return err
		}
	}

	return nil
}

func (n *Node) applyUpdates(
	updatedNodes map[string]*persistencespb.ChasmNode,
) error {
	for encodedPath, updatedNode := range updatedNodes {
		path, err := n.pathEncoder.Decode(encodedPath)
		if err != nil {
			return err
		}

		node, ok := n.getNodeByPath(path)
		if !ok {
			// Node doesn't exist, we need to create it.
			n.insert(path, updatedNode, n.nodeBase)
			n.mutation.UpdatedNodes[encodedPath] = updatedNode
			continue
		}

		if transitionhistory.Compare(
			node.persistence.Metadata.LastUpdateVersionedTransition,
			updatedNode.Metadata.LastUpdateVersionedTransition,
		) != 0 {
			n.mutation.UpdatedNodes[encodedPath] = updatedNode
			node.persistence = updatedNode
			node.value = nil

			// Clearing decoded value for ancestor nodes is not necessary because the value field is not referenced directly.
			// Parent node is pointing to the Node struct.
		}
	}

	return nil
}

func (n *Node) getNodeByPath(
	path []string,
) (*Node, bool) {
	if len(path) == 0 {
		return n, true
	}

	childName := path[0]
	childNode, ok := n.children[childName]
	if !ok {
		return nil, false
	}
	return childNode.getNodeByPath(path[1:])
}

func (n *Node) delete(
	currentNodePath []string,
	currentEncodedPathPtr *string,
) error {
	for childName, childNode := range n.children {
		if err := childNode.delete(append(currentNodePath, childName), nil); err != nil {
			return err
		}
	}

	if n.parent != nil {
		delete(n.parent.children, n.nodeName)
	}

	var encodedPath string
	if currentEncodedPathPtr == nil {
		var err error
		encodedPath, err = n.pathEncoder.Encode(n, currentNodePath)
		if err != nil {
			return err
		}
	} else {
		encodedPath = *currentEncodedPathPtr
	}

	n.mutation.DeletedNodes[encodedPath] = struct{}{}

	return nil
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
		childNode = newNode(nodeBase, n, childName)
		n.children[childName] = childNode
	}
	childNode.insert(nodePath[1:], pNode, nodeBase)
}

// IsDirty returns true if any node rooted at Node n has been modified.
// The result will be reset to false after a call to CloseTransaction().
func (n *Node) IsDirty() bool {
	panic("not implemented")
}

func newNode(
	base *nodeBase,
	parent *Node,
	nodeName string,
) *Node {
	return &Node{
		nodeBase: base,
		parent:   parent,
		children: make(map[string]*Node),
		nodeName: nodeName,
	}
}
