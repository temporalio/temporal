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
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
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

func (b *backend) TransitionCount() int64 {
	return 2
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
	require.Equal(t, []hsm.Key{}, root.Outputs()[0].Path)
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
	require.Equal(t, 1, len(root.Outputs()))
	require.Equal(t, []hsm.Key{key}, root.Outputs()[0].Path)

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
	root, err := hsm.NewRoot(reg, def1.Type(), hsmtest.NewData(hsmtest.State1), make(map[string]*persistencespb.StateMachineMap), &backend{})
	require.NoError(t, err)

	_, err = root.AddChild(hsm.Key{Type: "not-found", ID: "dont-care"}, "data")
	require.ErrorIs(t, err, hsm.ErrNotRegistered)
	_, err = root.AddChild(hsm.Key{Type: def1.Type(), ID: "dont-care"}, "data")
	require.ErrorContains(t, err, "invalid state type")
	_, err = root.AddChild(hsm.Key{Type: def1.Type(), ID: "id"}, hsmtest.NewData(hsmtest.State1))
	require.NoError(t, err)
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
		return hsm.TransitionOutput{}, fmt.Errorf("test") // nolint:goerr113
	})
	require.ErrorContains(t, err, "test")
	require.Equal(t, int64(0), root.InternalRepr().TransitionCount)
	require.Equal(t, int64(0), root.InternalRepr().LastUpdateMutableStateTransitionCount)
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
	// 3 = 2 - which is a constant returned by our test backend - and an increment of 1 to account for backend
	// incrementing its own transition count at the end of a transaction.
	require.Equal(t, int64(3), root.InternalRepr().LastUpdateMutableStateTransitionCount)
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
