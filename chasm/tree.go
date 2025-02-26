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
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
)

type (
	Node struct {
		// TODO: add necessary fields here, e.g.
		//
		// parent   *Node
		// children map[string]*Node
		// persistence *persistencespb.ChasmNode
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

	NodeBackend interface {
		// TODO: Add methods needed from MutateState here.
		//
		// This is for breaking cycle dependency between
		// this package and service/history/workflow package
		// where MutableState is defined.
	}
)

// NewTree creates a new in-memory CHASM tree from flattened persistence CHASM nodes.
//
// CHASM Tree and all its methods are NOT meant to be used by CHASM component authors.
// They are exported for use by the CHASM engine and underlying MutableState implementation only.
func NewTree(
	registry *Registry,
	persistenceNodes map[string]*persistencespb.ChasmNode,
	nodeBackend NodeBackend,
) (*Node, error) {
	panic("not implemented")
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
	component Component,
) time.Time {
	panic("not implemented")
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
