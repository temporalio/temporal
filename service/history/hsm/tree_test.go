// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package hsm_test

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
)

var def1 = hsmtest.NewDefinition("type1")
var def2 = hsmtest.NewDefinition("type2")
var defs = []hsmtest.Definition{def1, def2}
var reg = hsm.NewRegistry()

type backend struct{}

func (b *backend) GetCurrentVersion() int64 {
	return 1
}

func (b *backend) NextTransitionCount() int64 {
	return 3
}

func (b *backend) AddHistoryEvent(t enumspb.EventType, setAttributes func(*historypb.HistoryEvent)) *historypb.HistoryEvent {
	return nil
}

func (b *backend) GenerateEventLoadToken(event *historypb.HistoryEvent) ([]byte, error) {
	panic("unimplemented - not used in test")
}

func (b *backend) LoadHistoryEvent(ctx context.Context, token []byte) (*historypb.HistoryEvent, error) {
	panic("unimplemented - not used in test")
}

func init() {
	for _, def := range defs {
		if err := reg.RegisterMachine(def); err != nil {
			panic(err)
		}
	}
}

func TestNode_MaintainsCachedData(t *testing.T) {
	root, err := hsm.NewRoot(reg, def1.Type(), hsmtest.NewData(hsmtest.State1), make(map[string]*persistencespb.StateMachineMap), &backend{})
	require.NoError(t, err)

	v1, err := hsm.MachineData[*hsmtest.Data](root)
	require.NoError(t, err)

	require.False(t, root.Dirty())
	require.Equal(t, 0, len(root.Outputs()))

	err = hsm.MachineTransition(root, func(d *hsmtest.Data) (hsm.TransitionOutput, error) {
		d.SetState(hsmtest.State2)
		return hsm.TransitionOutput{}, nil
	})
	require.NoError(t, err)

	// Our data variable is a pointer to the cache.
	require.Equal(t, hsmtest.State2, v1.State())

	require.NoError(t, err)
	require.True(t, root.Dirty())
	require.Equal(t, 1, len(root.Outputs()))

	transOp, ok := root.Outputs()[0].(hsm.TransitionOperation)
	require.True(t, ok)
	require.Equal(t, []hsm.Key{}, transOp.Path())
}

func TestNode_MaintainsChildCache(t *testing.T) {
	be := &backend{}
	root, err := hsm.NewRoot(reg, def1.Type(), hsmtest.NewData(hsmtest.State1), map[string]*persistencespb.StateMachineMap{
		def1.Type(): {
			MachinesById: map[string]*persistencespb.StateMachineNode{
				"persisted": {
					TransitionCount: 1,
					Data:            []byte(hsmtest.State1),
					Children: map[string]*persistencespb.StateMachineMap{
						def1.Type(): {
							MachinesById: map[string]*persistencespb.StateMachineNode{
								"persisted-child": {
									TransitionCount: 2,
									Data:            []byte(hsmtest.State2),
								},
							},
						},
					},
				},
			},
		},
	}, be)
	require.NoError(t, err)

	key := hsm.Key{Type: def1.Type(), ID: "cached"}

	// Cache when a new child is added.
	child, err := root.AddChild(key, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)

	require.True(t, root.Dirty()) // As soon as we mutate the tree, it should be marked dirty.
	root.ClearTransactionState()  // Reset and check later that we're dirty after applying the transition.

	// Verify this doesn't panic and the backend is propagated to the new child.
	child.AddHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED, func(e *historypb.HistoryEvent) {})

	childRef, err := root.Child([]hsm.Key{key})
	require.NoError(t, err)
	require.Equal(t, child, childRef)

	err = hsm.MachineTransition(child, func(d *hsmtest.Data) (hsm.TransitionOutput, error) {
		d.SetState(hsmtest.State2)
		return hsm.TransitionOutput{}, nil
	})
	require.NoError(t, err)
	require.True(t, root.Dirty()) // Should now be dirty again.

	operations := root.Outputs()
	require.Equal(t, 1, len(operations))
	transOp, ok := operations[0].(hsm.TransitionOperation)
	require.True(t, ok)
	require.Equal(t, int64(1), transOp.Output.TransitionCount)
	require.Equal(t, []hsm.Key{key}, transOp.Path())

	// Cache when loaded from persistence.
	path := []hsm.Key{{Type: def1.Type(), ID: "persisted"}, {Type: def1.Type(), ID: "persisted-child"}}
	child, err = root.Child(path)
	require.NoError(t, err)
	// Verify this doesn't panic and the backend is propagated to the loaded child.
	child.AddHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED, func(e *historypb.HistoryEvent) {})

	err = hsm.MachineTransition(child, func(d *hsmtest.Data) (hsm.TransitionOutput, error) {
		d.SetState(hsmtest.State3)
		return hsm.TransitionOutput{}, nil
	})
	require.NoError(t, err)
	require.True(t, child.Dirty())
	childRef, err = root.Child(path)
	require.NoError(t, err)
	require.True(t, childRef.Dirty())

	// Also verify that transaction state is properly reset.
	root.ClearTransactionState()
	require.False(t, root.Dirty())
}

func TestNode_Path(t *testing.T) {
	root, err := hsm.NewRoot(reg, def1.Type(), hsmtest.NewData(hsmtest.State1), make(map[string]*persistencespb.StateMachineMap), &backend{})
	require.NoError(t, err)

	l1, err := root.AddChild(hsm.Key{Type: def1.Type(), ID: "l1"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)
	l2, err := l1.AddChild(hsm.Key{Type: def1.Type(), ID: "l2"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)

	require.Equal(t, []hsm.Key{}, root.Path())
	require.Equal(t, []hsm.Key{l1.Key}, l1.Path())
	require.Equal(t, []hsm.Key{l1.Key, l2.Key}, l2.Path())
}

func TestNode_AddChild(t *testing.T) {
	nodeBackend := &backend{}

	root, err := hsm.NewRoot(reg, def1.Type(), hsmtest.NewData(hsmtest.State1), make(map[string]*persistencespb.StateMachineMap), nodeBackend)
	require.NoError(t, err)

	_, err = root.AddChild(hsm.Key{Type: "not-found", ID: "dont-care"}, "data")
	require.ErrorIs(t, err, hsm.ErrNotRegistered)

	_, err = root.AddChild(hsm.Key{Type: def1.Type(), ID: "dont-care"}, "data")
	require.ErrorContains(t, err, "invalid state type")

	childNode, err := root.AddChild(hsm.Key{Type: def1.Type(), ID: "id"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: nodeBackend.GetCurrentVersion(),
		TransitionCount:          nodeBackend.NextTransitionCount(),
	}, childNode.InternalRepr().InitialVersionedTransition)
	protorequire.ProtoEqual(t, &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: nodeBackend.GetCurrentVersion(),
		TransitionCount:          nodeBackend.NextTransitionCount(),
	}, childNode.InternalRepr().LastUpdateVersionedTransition)
	require.Equal(t, int64(0), childNode.InternalRepr().TransitionCount)

	_, err = root.AddChild(hsm.Key{Type: def1.Type(), ID: "id"}, hsmtest.NewData(hsmtest.State1))
	require.ErrorIs(t, err, hsm.ErrStateMachineAlreadyExists)
}

func TestNode_Child(t *testing.T) {
	root, err := hsm.NewRoot(reg, def1.Type(), hsmtest.NewData(hsmtest.State1), make(map[string]*persistencespb.StateMachineMap), &backend{})
	require.NoError(t, err)
	l1, err := root.AddChild(hsm.Key{Type: def1.Type(), ID: "l1"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)
	l2, err := l1.AddChild(hsm.Key{Type: def1.Type(), ID: "l2"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)

	// Recursive cached lookup.
	child, err := root.Child([]hsm.Key{{Type: def1.Type(), ID: "l1"}, {Type: def1.Type(), ID: "l2"}})
	require.NoError(t, err)
	require.Equal(t, l2, child)

	// Not registered.
	_, err = root.Child([]hsm.Key{{Type: "not-found", ID: "dont-care"}})
	require.ErrorIs(t, err, hsm.ErrNotRegistered)

	_, err = root.Child([]hsm.Key{{Type: def1.Type(), ID: "l3"}})
	require.ErrorIs(t, err, hsm.ErrStateMachineNotFound)

	// Lookup from persistence.
	root, err = hsm.NewRoot(reg, def1.Type(), hsmtest.NewData(hsmtest.State1), map[string]*persistencespb.StateMachineMap{
		def1.Type(): {
			MachinesById: map[string]*persistencespb.StateMachineNode{
				"p1": {
					TransitionCount: 1,
					Data:            []byte(hsmtest.State1),
				},
			},
		},
	}, nil)
	require.NoError(t, err)
	child, err = root.Child([]hsm.Key{{Type: def1.Type(), ID: "p1"}})
	require.NoError(t, err)

	// Verify child was properly loaded.
	require.Equal(t, hsm.Key{Type: def1.Type(), ID: "p1"}, child.Key)
	d, err := hsm.MachineData[*hsmtest.Data](child)
	require.NoError(t, err)
	require.Equal(t, hsmtest.State1, d.State())
}

func TestNode_Walk(t *testing.T) {
	root, err := hsm.NewRoot(reg, def1.Type(), hsmtest.NewData(hsmtest.State1), make(map[string]*persistencespb.StateMachineMap), &backend{})
	require.NoError(t, err)

	l1_1, err := root.AddChild(hsm.Key{Type: def1.Type(), ID: "l1_1"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)
	_, err = l1_1.AddChild(hsm.Key{Type: def1.Type(), ID: "l2_2"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)

	_, err = root.AddChild(hsm.Key{Type: def1.Type(), ID: "l1_2"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)

	_, err = root.AddChild(hsm.Key{Type: def2.Type(), ID: "l1_3"}, hsmtest.NewData(hsmtest.State2))
	require.NoError(t, err)

	nodeCount := 0
	err = root.Walk(func(n *hsm.Node) error {
		nodeCount++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 5, nodeCount)
}

func TestNode_Sync(t *testing.T) {
	currentState := hsmtest.State2
	currentInitialVersionedTransition := &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: 100,
		TransitionCount:          23,
	}
	currentLastUpdateVersionedTransition := &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: 100,
		TransitionCount:          25,
	}
	incomingLastUpdateVersionedTransition := &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: 200,
		TransitionCount:          50,
	}

	testCases := []struct {
		name                               string
		incomingInitialVersionedTransition *persistencespb.VersionedTransition
		incomingState                      hsmtest.State
		expectedErr                        error
	}{
		{
			name: "NodeMisMatch/InitialVersionMismatch",
			incomingInitialVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 200,
				TransitionCount:          23,
			},
			incomingState: hsmtest.State1,
			expectedErr:   hsm.ErrInitialTransitionMismatch,
		},
		{
			name: "NodeMismatch/InitialTransitionCountMismatch",
			incomingInitialVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 100,
				TransitionCount:          32,
			},
			incomingState: hsmtest.State1,
			expectedErr:   hsm.ErrInitialTransitionMismatch,
		},
		{
			name: "NodeMatch/TransitionHistoryDisabled",
			incomingInitialVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 100,
				// transition history disabled for incoming node,
				// should only compare initial failover version
				TransitionCount: 0,
			},
			incomingState: hsmtest.State1,
			expectedErr:   nil,
		},
		{
			name: "NodeMatch/SyncNewerState",
			incomingInitialVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 100,
				TransitionCount:          23,
			},
			incomingState: hsmtest.State3,
			expectedErr:   nil,
		},
		{
			name: "NodeMatch/SyncOlderState",
			incomingInitialVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 100,
				TransitionCount:          23,
			},
			// Sync method() is force sync and can sync to older state.
			incomingState: hsmtest.State1,
			expectedErr:   nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			initNode := func(
				state hsmtest.State,
				initialVersionedTransition *persistencespb.VersionedTransition,
				lastUpdateVersionedTransition *persistencespb.VersionedTransition,
			) *hsm.Node {
				node, err := hsm.NewRoot(reg, def1.Type(), hsmtest.NewData(state), make(map[string]*persistencespb.StateMachineMap), &backend{})
				require.NoError(t, err)

				node.InternalRepr().InitialVersionedTransition = initialVersionedTransition
				node.InternalRepr().LastUpdateVersionedTransition = lastUpdateVersionedTransition

				return node
			}

			currentNode := initNode(currentState, currentInitialVersionedTransition, currentLastUpdateVersionedTransition)
			incomingNode := initNode(tc.incomingState, tc.incomingInitialVersionedTransition, incomingLastUpdateVersionedTransition)

			currentNodeTransitionCount := currentNode.InternalRepr().TransitionCount

			err := currentNode.Sync(incomingNode)
			if tc.expectedErr != nil {
				require.ErrorIs(t, err, tc.expectedErr)
				return
			}

			require.NoError(t, err)

			incomingData, err := def1.Serialize(hsmtest.NewData(tc.incomingState))
			require.NoError(t, err)
			require.Equal(t, incomingData, currentNode.InternalRepr().Data)
			protorequire.ProtoEqual(t, incomingNode.InternalRepr().LastUpdateVersionedTransition, currentNode.InternalRepr().LastUpdateVersionedTransition)
			require.Equal(t, currentNodeTransitionCount+1, currentNode.InternalRepr().TransitionCount)

			paos := currentNode.Outputs()
			require.Len(t, paos, 1)
			transOp, ok := paos[0].(hsm.TransitionOperation)
			require.True(t, ok)
			require.Equal(t, currentNode.Path(), transOp.Path())
			require.Len(t, transOp.Output.Tasks, 2)
		})
	}
}

func TestMachineData(t *testing.T) {
	root, err := hsm.NewRoot(reg, def1.Type(), hsmtest.NewData(hsmtest.State1), make(map[string]*persistencespb.StateMachineMap), &backend{})
	require.NoError(t, err)

	_, err = hsm.MachineData[string](root)
	require.ErrorIs(t, err, hsm.ErrIncompatibleType)

	// OK.
	value, err := hsm.MachineData[*hsmtest.Data](root)
	require.NoError(t, err)
	require.Equal(t, hsmtest.NewData(hsmtest.State1), value)
}

func TestMachineTransition(t *testing.T) {
	root, err := hsm.NewRoot(reg, def1.Type(), hsmtest.NewData(hsmtest.State1), make(map[string]*persistencespb.StateMachineMap), &backend{})
	require.NoError(t, err)

	err = hsm.MachineTransition(root, func(string) (hsm.TransitionOutput, error) {
		return hsm.TransitionOutput{}, nil
	})
	require.ErrorIs(t, err, hsm.ErrIncompatibleType)

	// Transition fails.
	err = hsm.MachineTransition(root, func(d *hsmtest.Data) (hsm.TransitionOutput, error) {
		// Mutate state and make sure the cache is marked stale.
		d.SetState(hsmtest.State2)
		return hsm.TransitionOutput{}, fmt.Errorf("test")
	})
	require.ErrorContains(t, err, "test")
	require.Equal(t, int64(0), root.InternalRepr().TransitionCount)
	protorequire.ProtoEqual(t, &persistencespb.VersionedTransition{}, root.InternalRepr().LastUpdateVersionedTransition)
	d, err := hsm.MachineData[*hsmtest.Data](root)
	require.NoError(t, err)
	// Got the pre-mutation value back.
	require.Equal(t, hsmtest.State1, d.State())
	require.False(t, root.Dirty())

	err = hsm.MachineTransition(root, func(d *hsmtest.Data) (hsm.TransitionOutput, error) {
		d.SetState(hsmtest.State2)
		return hsm.TransitionOutput{}, nil
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), root.InternalRepr().TransitionCount)
	protorequire.ProtoEqual(t, &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: 1,
		TransitionCount:          3,
	}, root.InternalRepr().LastUpdateVersionedTransition)
	d, err = hsm.MachineData[*hsmtest.Data](root)
	require.NoError(t, err)
	require.Equal(t, hsmtest.State2, d.State())
}

func TestCollection(t *testing.T) {
	root, err := hsm.NewRoot(reg, def1.Type(), hsmtest.NewData(hsmtest.State1), make(map[string]*persistencespb.StateMachineMap), &backend{})
	require.NoError(t, err)

	coll := hsm.NewCollection[*hsmtest.Data](root, def1.Type())
	a, err := coll.Add("a", hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)
	b, err := coll.Add("b", hsmtest.NewData(hsmtest.State2))
	require.NoError(t, err)

	n, err := coll.Node("a")
	require.NoError(t, err)
	require.Equal(t, a, n)
	n, err = coll.Node("b")
	require.NoError(t, err)
	require.Equal(t, b, n)

	require.Equal(t, 2, coll.Size())
	nodes := coll.List()
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].Key.ID < nodes[j].Key.ID
	})

	require.Equal(t, []*hsm.Node{a, b}, nodes)

	err = coll.Transition("a", func(d *hsmtest.Data) (hsm.TransitionOutput, error) {
		d.SetState(hsmtest.State2)
		return hsm.TransitionOutput{}, nil
	})
	require.NoError(t, err)
	d, err := coll.Data("a")
	require.NoError(t, err)
	require.Equal(t, hsmtest.NewData(hsmtest.State2), d)
}

func TestNode_DeleteChild(t *testing.T) {
	root, err := hsm.NewRoot(reg, def1.Type(), hsmtest.NewData(hsmtest.State1), make(map[string]*persistencespb.StateMachineMap), &backend{})
	require.NoError(t, err)

	l1, err := root.AddChild(hsm.Key{Type: def1.Type(), ID: "l1"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)
	l2, err := l1.AddChild(hsm.Key{Type: def1.Type(), ID: "l2"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)
	_, err = l1.AddChild(hsm.Key{Type: def1.Type(), ID: "l2_sibling"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)

	err = hsm.MachineTransition(l2, func(d *hsmtest.Data) (hsm.TransitionOutput, error) {
		d.SetState(hsmtest.State2)
		return hsm.TransitionOutput{}, nil
	})
	require.NoError(t, err)

	err = l1.DeleteChild(hsm.Key{Type: def1.Type(), ID: "l2"})
	require.NoError(t, err)

	err = hsm.MachineTransition(l2, func(d *hsmtest.Data) (hsm.TransitionOutput, error) {
		return hsm.TransitionOutput{}, nil
	})
	require.ErrorIs(t, err, hsm.ErrStateMachineInvalidState)

	l2Outputs := l2.Outputs()
	require.Len(t, l2Outputs, 1) // Should see its delete operation
	_, ok := l2Outputs[0].(hsm.DeleteOperation)
	require.True(t, ok) // Should only see the delete operation, no transitions

	// Cannot delete non-existent or already deleted nodes
	err = l1.DeleteChild(hsm.Key{Type: def1.Type(), ID: "nonexistent"})
	require.ErrorIs(t, err, hsm.ErrStateMachineNotFound)

	err = l1.DeleteChild(hsm.Key{Type: def1.Type(), ID: "l2"})
	require.ErrorIs(t, err, hsm.ErrStateMachineNotFound)
}

func TestOperationLog_IsDeleted(t *testing.T) {
	root, err := hsm.NewRoot(reg, def1.Type(), hsmtest.NewData(hsmtest.State1), make(map[string]*persistencespb.StateMachineMap), &backend{})
	require.NoError(t, err)

	l1, err := root.AddChild(hsm.Key{Type: def1.Type(), ID: "l1"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)
	l2, err := l1.AddChild(hsm.Key{Type: def1.Type(), ID: "l2"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)
	l2_sibling, err := l1.AddChild(hsm.Key{Type: def1.Type(), ID: "l2_sibling"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)

	err = hsm.MachineTransition(l2, func(d *hsmtest.Data) (hsm.TransitionOutput, error) {
		d.SetState(hsmtest.State2)
		return hsm.TransitionOutput{}, nil
	})
	require.NoError(t, err)

	err = hsm.MachineTransition(l2_sibling, func(d *hsmtest.Data) (hsm.TransitionOutput, error) {
		d.SetState(hsmtest.State2)
		return hsm.TransitionOutput{}, nil
	})
	require.NoError(t, err)

	err = l1.DeleteChild(hsm.Key{Type: def1.Type(), ID: "l2"})
	require.NoError(t, err)

	// Deleted node should see only its delete operation
	l2Outputs := l2.Outputs()
	require.Len(t, l2Outputs, 1)
	_, ok := l2Outputs[0].(hsm.DeleteOperation)
	require.True(t, ok)

	// Non-deleted sibling should still have only its transition
	siblingOutputs := l2_sibling.Outputs()
	require.Len(t, siblingOutputs, 1)
	transOp, ok := siblingOutputs[0].(hsm.TransitionOperation)
	require.True(t, ok)
	require.Equal(t, l2_sibling.Path(), transOp.Path())
}

func TestNode_OutputsWithDeletion(t *testing.T) {
	root, err := hsm.NewRoot(reg, def1.Type(), hsmtest.NewData(hsmtest.State1), make(map[string]*persistencespb.StateMachineMap), &backend{})
	require.NoError(t, err)

	l1, err := root.AddChild(hsm.Key{Type: def1.Type(), ID: "l1"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)
	l2, err := l1.AddChild(hsm.Key{Type: def1.Type(), ID: "l2"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)

	err = hsm.MachineTransition(root, func(d *hsmtest.Data) (hsm.TransitionOutput, error) {
		d.SetState(hsmtest.State2)
		return hsm.TransitionOutput{}, nil
	})
	require.NoError(t, err)

	err = hsm.MachineTransition(l2, func(d *hsmtest.Data) (hsm.TransitionOutput, error) {
		d.SetState(hsmtest.State2)
		return hsm.TransitionOutput{}, nil
	})
	require.NoError(t, err)

	rootOutputs := root.Outputs()
	require.Len(t, rootOutputs, 2) // root and l2 transitions

	// Delete l1 (and by extension l2)
	err = root.DeleteChild(hsm.Key{Type: def1.Type(), ID: "l1"})
	require.NoError(t, err)

	rootOutputs = root.Outputs()
	require.Len(t, rootOutputs, 3) // root's transition, l1's deletion, and l2's deletion

	var foundTransition, foundL1Deletion, foundL2Deletion bool
	for _, op := range rootOutputs {
		switch o := op.(type) {
		case hsm.TransitionOperation:
			if slices.Equal(o.Path(), []hsm.Key{}) { // root's path
				foundTransition = true
			}
		case hsm.DeleteOperation:
			if slices.Equal(o.Path(), l1.Path()) {
				foundL1Deletion = true
			}
			if slices.Equal(o.Path(), l2.Path()) {
				foundL2Deletion = true
			}
		}
	}
	require.True(t, foundTransition, "should have root's transition")
	require.True(t, foundL1Deletion, "should have l1's deletion")
	require.True(t, foundL2Deletion, "should have l2's deletion")

	// Deleted nodes have no transitions but see their deletion
	l1Outputs := l1.Outputs()
	require.Len(t, l1Outputs, 2) // l1's deletion and l2's deletion

	for _, op := range l1Outputs {
		if del, ok := op.(hsm.DeleteOperation); ok {
			if slices.Equal(del.Path(), l1.Path()) {
				foundL1Deletion = true
			}
			if slices.Equal(del.Path(), l2.Path()) {
				foundL2Deletion = true
			}
		}
	}
	require.True(t, foundL1Deletion)
	require.True(t, foundL2Deletion)

	// Leaf deleted node only sees its own deletion
	l2Outputs := l2.Outputs()
	require.Len(t, l2Outputs, 1)
	_, ok := l2Outputs[0].(hsm.DeleteOperation)
	require.True(t, ok)
}

func TestNode_ClearTransactionState(t *testing.T) {
	root, err := hsm.NewRoot(reg, def1.Type(), hsmtest.NewData(hsmtest.State1), make(map[string]*persistencespb.StateMachineMap), &backend{})
	require.NoError(t, err)

	l1, err := root.AddChild(hsm.Key{Type: def1.Type(), ID: "l1"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)

	err = hsm.MachineTransition(root, func(d *hsmtest.Data) (hsm.TransitionOutput, error) {
		d.SetState(hsmtest.State2)
		return hsm.TransitionOutput{}, nil
	})
	require.NoError(t, err)

	err = root.DeleteChild(hsm.Key{Type: def1.Type(), ID: "l1"})
	require.NoError(t, err)

	outputs := root.Outputs()
	require.NotEmpty(t, outputs)

	root.ClearTransactionState()

	outputs = root.Outputs()
	require.Empty(t, outputs)
	require.False(t, root.Dirty())

	err = hsm.MachineTransition(l1, func(d *hsmtest.Data) (hsm.TransitionOutput, error) {
		return hsm.TransitionOutput{}, nil
	})
	require.ErrorIs(t, err, hsm.ErrStateMachineInvalidState)
}

func TestNode_DeleteDeepHierarchy(t *testing.T) {
	root, err := hsm.NewRoot(reg, def1.Type(), hsmtest.NewData(hsmtest.State1), make(map[string]*persistencespb.StateMachineMap), &backend{})
	require.NoError(t, err)

	// Build hierarchy
	current := root
	var nodes []*hsm.Node
	for i := 0; i < 5; i++ {
		node, err := current.AddChild(hsm.Key{Type: def1.Type(), ID: fmt.Sprintf("node%d", i)}, hsmtest.NewData(hsmtest.State1))
		require.NoError(t, err)
		nodes = append(nodes, node)
		current = node
	}

	for _, node := range nodes {
		err = hsm.MachineTransition(node, func(d *hsmtest.Data) (hsm.TransitionOutput, error) {
			d.SetState(hsmtest.State2)
			return hsm.TransitionOutput{}, nil
		})
		require.NoError(t, err)
	}

	// Delete from middle
	err = nodes[1].DeleteChild(hsm.Key{Type: def1.Type(), ID: "node2"})
	require.NoError(t, err)

	// Verify outputs at each level
	for i, node := range nodes {
		outputs := node.Outputs()
		if i <= 1 { // Above deletion
			require.NotEmpty(t, outputs)
			// Should see deletions of all descendants
			deletionCount := 0
			for _, op := range outputs {
				if _, ok := op.(hsm.DeleteOperation); ok {
					deletionCount++
				}
			}
			require.Equal(t, len(nodes)-2, deletionCount)
		} else { // At or below deletion
			require.NotEmpty(t, outputs) // Should see deletion ops
			// Should only see deletions, no transitions
			for _, op := range outputs {
				_, ok := op.(hsm.DeleteOperation)
				require.True(t, ok)
			}
			// Should see own deletion and all descendant deletions
			require.Equal(t, len(nodes)-i, len(outputs))
		}
	}
}

func TestNode_MixedOperationsBeforeDeletion(t *testing.T) {
	root, err := hsm.NewRoot(reg, def1.Type(), hsmtest.NewData(hsmtest.State1), make(map[string]*persistencespb.StateMachineMap), &backend{})
	require.NoError(t, err)

	l1, err := root.AddChild(hsm.Key{Type: def1.Type(), ID: "l1"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		err = hsm.MachineTransition(l1, func(d *hsmtest.Data) (hsm.TransitionOutput, error) {
			d.SetState(hsmtest.State2)
			return hsm.TransitionOutput{}, nil
		})
		require.NoError(t, err)
	}

	// Count transition operations for l1
	outputs := l1.Outputs()
	transitionCount := 0
	for _, op := range outputs {
		if transOp, ok := op.(hsm.TransitionOperation); ok {
			if slices.Equal(transOp.Path(), l1.Path()) {
				transitionCount++
			}
		}
	}
	require.Equal(t, 3, transitionCount)

	err = root.DeleteChild(hsm.Key{Type: def1.Type(), ID: "l1"})
	require.NoError(t, err)

	// After deletion, no transitions should remain for l1
	outputs = l1.Outputs()
	require.NotEmpty(t, outputs) // Should still have the delete operation
	for _, op := range outputs {
		if _, ok := op.(hsm.TransitionOperation); ok {
			// Should not find any transition operations for deleted node
			require.NotEqual(t, l1.Path(), op.Path())
		}
	}
}

func TestNode_MultipleDeletedPaths(t *testing.T) {
	root, err := hsm.NewRoot(reg, def1.Type(), hsmtest.NewData(hsmtest.State1), make(map[string]*persistencespb.StateMachineMap), &backend{})
	require.NoError(t, err)

	branch1, err := root.AddChild(hsm.Key{Type: def1.Type(), ID: "branch1"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)
	branch2, err := root.AddChild(hsm.Key{Type: def1.Type(), ID: "branch2"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)

	b1child, err := branch1.AddChild(hsm.Key{Type: def1.Type(), ID: "b1child"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)
	b2child, err := branch2.AddChild(hsm.Key{Type: def1.Type(), ID: "b2child"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)

	err = hsm.MachineTransition(branch1, func(d *hsmtest.Data) (hsm.TransitionOutput, error) {
		d.SetState(hsmtest.State2)
		return hsm.TransitionOutput{}, nil
	})
	require.NoError(t, err)

	err = hsm.MachineTransition(branch2, func(d *hsmtest.Data) (hsm.TransitionOutput, error) {
		d.SetState(hsmtest.State2)
		return hsm.TransitionOutput{}, nil
	})
	require.NoError(t, err)

	err = branch1.DeleteChild(hsm.Key{Type: def1.Type(), ID: "b1child"})
	require.NoError(t, err)
	err = branch2.DeleteChild(hsm.Key{Type: def1.Type(), ID: "b2child"})
	require.NoError(t, err)

	b1outputs := b1child.Outputs()
	require.Len(t, b1outputs, 1)
	del, ok := b1outputs[0].(hsm.DeleteOperation)
	require.True(t, ok)
	require.Equal(t, b1child.Path(), del.Path())

	b2outputs := b2child.Outputs()
	require.Len(t, b2outputs, 1)
	del, ok = b2outputs[0].(hsm.DeleteOperation)
	require.True(t, ok)
	require.Equal(t, b2child.Path(), del.Path())
}
