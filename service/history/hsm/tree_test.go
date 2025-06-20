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
	opLog, err := root.OpLog()
	require.NoError(t, err)
	require.Equal(t, 0, len(opLog))

	err = hsm.MachineTransition(root, func(d *hsmtest.Data) (hsm.TransitionOutput, error) {
		d.SetState(hsmtest.State2)
		return hsm.TransitionOutput{}, nil
	})
	require.NoError(t, err)

	// Our data variable is a pointer to the cache.
	require.Equal(t, hsmtest.State2, v1.State())

	require.True(t, root.Dirty())
	opLog, err = root.OpLog()
	require.NoError(t, err)
	require.Equal(t, 1, len(opLog))

	transOp, ok := opLog[0].(hsm.TransitionOperation)
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

	opLog, err := root.OpLog()
	require.NoError(t, err)
	require.Equal(t, 1, len(opLog))
	transOp, ok := opLog[0].(hsm.TransitionOperation)
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

			opLog, err := currentNode.OpLog()
			require.NoError(t, err)
			require.Len(t, opLog, 1)
			transOp, ok := opLog[0].(hsm.TransitionOperation)
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

	err = l1.DeleteChild(l2.Key)
	require.NoError(t, err)

	err = hsm.MachineTransition(l2, func(d *hsmtest.Data) (hsm.TransitionOutput, error) {
		return hsm.TransitionOutput{}, nil
	})
	require.ErrorIs(t, err, hsm.ErrStateMachineInvalidState)

	_, err = l2.OpLog()
	require.Error(t, err)

	opLog, err := root.OpLog()
	require.NoError(t, err)
	require.Len(t, opLog, 1) // After compaction, only the delete operation remains
	_, ok := opLog[0].(hsm.DeleteOperation)
	require.True(t, ok)

	// Cannot delete non-existent or already deleted nodes
	err = l1.DeleteChild(hsm.Key{Type: def1.Type(), ID: "nonexistent"})
	require.ErrorIs(t, err, hsm.ErrStateMachineNotFound)

	err = l1.DeleteChild(l2.Key)
	require.ErrorIs(t, err, hsm.ErrStateMachineNotFound)
}

func TestNode_PreservesUnrelatedOperations(t *testing.T) {
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

	err = l1.DeleteChild(l2.Key)
	require.NoError(t, err)

	opLog, err := root.OpLog()
	require.NoError(t, err)

	foundDelete := slices.ContainsFunc(opLog, func(op hsm.Operation) bool {
		if delOp, ok := op.(hsm.DeleteOperation); ok {
			return slices.Equal(delOp.Path(), l2.Path())
		}
		return false
	})
	require.True(t, foundDelete, "should find l2's delete operation")

	foundTransition := slices.ContainsFunc(opLog, func(op hsm.Operation) bool {
		if transOp, ok := op.(hsm.TransitionOperation); ok {
			return slices.Equal(transOp.Path(), l2_sibling.Path())
		}
		return false
	})
	require.True(t, foundTransition, "should find l2_sibling's transition")
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

	err = root.DeleteChild(l1.Key)
	require.NoError(t, err)

	outputs, err := root.OpLog()
	require.NoError(t, err)
	require.Len(t, outputs, 2) // root's transition and l1's deletion (l2's operations excluded due to l1 deletion)

	var foundTransition, foundL1Deletion bool
	for _, op := range outputs {
		switch o := op.(type) {
		case hsm.TransitionOperation:
			if slices.Equal(o.Path(), []hsm.Key{}) { // root's path
				foundTransition = true
			}
		case hsm.DeleteOperation:
			if slices.Equal(o.Path(), l1.Path()) {
				foundL1Deletion = true
			}
		}
	}
	require.True(t, foundTransition, "should have root's transition")
	require.True(t, foundL1Deletion, "should have l1's deletion")
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

	err = root.DeleteChild(l1.Key)
	require.NoError(t, err)

	opLog, err := root.OpLog()
	require.NoError(t, err)
	require.NotEmpty(t, opLog)

	root.ClearTransactionState()

	opLog, err = root.OpLog()
	require.NoError(t, err)
	require.Empty(t, opLog)
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

	opLog, err := root.OpLog()
	require.NoError(t, err)
	require.NotEmpty(t, opLog)

	// Count transitions and deletions
	var transitionCount, deletionCount int
	for _, op := range opLog {
		switch o := op.(type) {
		case hsm.TransitionOperation:
			transitionCount++
			pathLen := len(o.Path())
			require.True(t, pathLen <= 3, "should not see transitions for deleted nodes")
		case hsm.DeleteOperation:
			deletionCount++
		}
	}

	require.Equal(t, 2, transitionCount, "should see transitions for nodes above deletion")
	require.Equal(t, 1, deletionCount, "should see one deletion operation")
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

	// Count transition operations for l1 from root's outputs
	opLog, err := root.OpLog()
	require.NoError(t, err)
	transitionCount := 0
	for _, op := range opLog {
		if transOp, ok := op.(hsm.TransitionOperation); ok {
			if slices.Equal(transOp.Path(), l1.Path()) {
				transitionCount++
			}
		}
	}
	require.Equal(t, 3, transitionCount, "should see all transitions before deletion")

	err = root.DeleteChild(l1.Key)
	require.NoError(t, err)

	opLog, err = root.OpLog()
	require.NoError(t, err)
	require.NotEmpty(t, opLog)

	var foundDelete bool
	for _, op := range opLog {
		if delOp, ok := op.(hsm.DeleteOperation); ok {
			if slices.Equal(delOp.Path(), l1.Path()) {
				foundDelete = true
			}
		}
		if transOp, ok := op.(hsm.TransitionOperation); ok {
			require.NotEqual(t, l1.Path(), transOp.Path(), "should not see transitions for deleted node")
		}
	}
	require.True(t, foundDelete, "should find deletion operation")
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

	err = branch1.DeleteChild(b1child.Key)
	require.NoError(t, err)
	err = branch2.DeleteChild(b2child.Key)
	require.NoError(t, err)

	outputs, err := root.OpLog()
	require.NoError(t, err)

	// Verify both deletion operations exist
	var foundB1ChildDel, foundB2ChildDel bool
	for _, op := range outputs {
		if del, ok := op.(hsm.DeleteOperation); ok {
			if slices.Equal(del.Path(), b1child.Path()) {
				foundB1ChildDel = true
			}
			if slices.Equal(del.Path(), b2child.Path()) {
				foundB2ChildDel = true
			}
		}
	}
	require.True(t, foundB1ChildDel, "should find b1child deletion")
	require.True(t, foundB2ChildDel, "should find b2child deletion")
}

func TestNode_PathPrefixEdgeCases(t *testing.T) {
	root, err := hsm.NewRoot(reg, def1.Type(), hsmtest.NewData(hsmtest.State1), make(map[string]*persistencespb.StateMachineMap), &backend{})
	require.NoError(t, err)

	// Test similar paths at different levels:
	// root
	// ├── node1
	// │   └── child1
	// │       └── node1  (same name as parent)
	// ├── node10
	// └── node11

	node1, err := root.AddChild(hsm.Key{Type: def1.Type(), ID: "node1"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)
	node10, err := root.AddChild(hsm.Key{Type: def1.Type(), ID: "node10"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)
	node11, err := root.AddChild(hsm.Key{Type: def1.Type(), ID: "node11"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)

	// Add a path that reuses "node1" at a deeper level
	child1, err := node1.AddChild(hsm.Key{Type: def1.Type(), ID: "child1"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)
	deepNode1, err := child1.AddChild(hsm.Key{Type: def1.Type(), ID: "node1"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)

	// Add transitions to all nodes
	for _, node := range []*hsm.Node{node1, node10, node11, child1, deepNode1} {
		err = hsm.MachineTransition(node, func(d *hsmtest.Data) (hsm.TransitionOutput, error) {
			d.SetState(hsmtest.State2)
			return hsm.TransitionOutput{}, nil
		})
		require.NoError(t, err)
	}

	err = root.DeleteChild(node1.Key)
	require.NoError(t, err)

	// Verify operations
	opLog, err := root.OpLog()
	require.NoError(t, err)

	var transitions, deletes [][]hsm.Key
	for _, op := range opLog {
		switch o := op.(type) {
		case hsm.TransitionOperation:
			transitions = append(transitions, o.Path())
		case hsm.DeleteOperation:
			deletes = append(deletes, o.Path())
		}
	}

	// Should see:
	// - Delete operation for node1 and its subtree
	// - Transitions for node10 and node11
	// - No transitions from node1's subtree
	require.Len(t, deletes, 1, "should have one delete operation")
	require.Equal(t, node1.Path(), deletes[0], "delete should be for top-level node1")

	// Only node10 and node11 transitions should remain
	require.Len(t, transitions, 2, "should have only node10 and node11 transitions")
	for _, transition := range transitions {
		require.True(t, slices.Equal(transition, node10.Path()) || slices.Equal(transition, node11.Path()),
			"remaining transitions should only be for node10 or node11")
	}

	// Verify root operations are preserved
	err = hsm.MachineTransition(root, func(d *hsmtest.Data) (hsm.TransitionOutput, error) {
		d.SetState(hsmtest.State2)
		return hsm.TransitionOutput{}, nil
	})
	require.NoError(t, err)

	opLog, err = root.OpLog()
	require.NoError(t, err)

	// Find root's transition
	foundRootTransition := false
	for _, op := range opLog {
		if transOp, ok := op.(hsm.TransitionOperation); ok {
			if len(transOp.Path()) == 0 {
				foundRootTransition = true
				break
			}
		}
	}
	require.True(t, foundRootTransition, "root's transition should be preserved")
}

func TestNode_ComplexHierarchicalDeletions(t *testing.T) {
	root, err := hsm.NewRoot(reg, def1.Type(), hsmtest.NewData(hsmtest.State1), make(map[string]*persistencespb.StateMachineMap), &backend{})
	require.NoError(t, err)

	// Create a three-level hierarchy with siblings
	parent, err := root.AddChild(hsm.Key{Type: def1.Type(), ID: "parent"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)

	child1, err := parent.AddChild(hsm.Key{Type: def1.Type(), ID: "child1"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)
	child2, err := parent.AddChild(hsm.Key{Type: def1.Type(), ID: "child2"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)

	grandchild1, err := child1.AddChild(hsm.Key{Type: def1.Type(), ID: "grandchild1"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)
	grandchild2, err := child2.AddChild(hsm.Key{Type: def1.Type(), ID: "grandchild2"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)

	// Add operations to all nodes
	for _, node := range []*hsm.Node{parent, child1, child2, grandchild1, grandchild2} {
		err = hsm.MachineTransition(node, func(d *hsmtest.Data) (hsm.TransitionOutput, error) {
			d.SetState(hsmtest.State2)
			return hsm.TransitionOutput{}, nil
		})
		require.NoError(t, err)
	}

	// Delete parent while children have pending operations
	err = root.DeleteChild(parent.Key)
	require.NoError(t, err)

	// Verify only parent's delete operation remains, all child operations are removed
	opLog, err := root.OpLog()
	require.NoError(t, err)

	require.Len(t, opLog, 1, "should only see parent's delete operation")
	deleteOp, ok := opLog[0].(hsm.DeleteOperation)
	require.True(t, ok)
	require.Equal(t, parent.Path(), deleteOp.Path())
}

func TestNode_CompactionOrderPreservation(t *testing.T) {
	root, err := hsm.NewRoot(reg, def1.Type(), hsmtest.NewData(hsmtest.State1), make(map[string]*persistencespb.StateMachineMap), &backend{})
	require.NoError(t, err)

	node1, err := root.AddChild(hsm.Key{Type: def1.Type(), ID: "node1"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)
	node2, err := root.AddChild(hsm.Key{Type: def1.Type(), ID: "node2"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)

	// Record sequence of operations:
	// 1. Transition on node1
	// 2. Transition on node2
	// 3. Delete node1 (should remove node1's transition)
	// 4. Another transition on node2

	err = hsm.MachineTransition(node1, func(d *hsmtest.Data) (hsm.TransitionOutput, error) {
		return hsm.TransitionOutput{}, nil
	})
	require.NoError(t, err)

	err = hsm.MachineTransition(node2, func(d *hsmtest.Data) (hsm.TransitionOutput, error) {
		return hsm.TransitionOutput{}, nil
	})
	require.NoError(t, err)

	err = root.DeleteChild(node1.Key)
	require.NoError(t, err)

	err = hsm.MachineTransition(node2, func(d *hsmtest.Data) (hsm.TransitionOutput, error) {
		return hsm.TransitionOutput{}, nil
	})
	require.NoError(t, err)

	// After compaction:
	// - node1's transition should be gone
	// - delete operation should remain
	// - node2's transitions should remain in original order
	opLog, err := root.OpLog()
	require.NoError(t, err)

	var ops []string
	for _, op := range opLog {
		switch o := op.(type) {
		case hsm.TransitionOperation:
			if slices.Equal(o.Path(), node2.Path()) {
				ops = append(ops, "node2_transition")
			} else if slices.Equal(o.Path(), node1.Path()) {
				ops = append(ops, "node1_transition")
			}
		case hsm.DeleteOperation:
			if slices.Equal(o.Path(), node1.Path()) {
				ops = append(ops, "node1_delete")
			}
		}
	}

	expected := []string{
		"node2_transition",
		"node1_delete",
		"node2_transition",
	}
	require.Equal(t, expected, ops, "operations should maintain chronological order after compaction")
}
