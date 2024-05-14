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
)

var def1 = hsm.NewTestDefinition(1)
var def2 = hsm.NewTestDefinition(2)
var defs = []hsm.TestDefinition{def1, def2}
var reg = hsm.NewRegistry()

type backend struct{}

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
	root, err := hsm.NewRoot(reg, def1.Type().ID, hsm.NewTestData(hsm.TestState1), make(map[int32]*persistencespb.StateMachineMap), nil)
	require.NoError(t, err)

	v1, err := hsm.MachineData[*hsm.TestData](root)
	require.NoError(t, err)

	require.False(t, root.Dirty())
	require.Equal(t, 0, len(root.Outputs()))

	err = hsm.MachineTransition(root, func(d *hsm.TestData) (hsm.TransitionOutput, error) {
		d.SetState(hsm.TestState2)
		return hsm.TransitionOutput{}, nil
	})
	require.NoError(t, err)

	// Our data variable is a pointer to the cache.
	require.Equal(t, hsm.TestState2, v1.State())

	require.NoError(t, err)
	require.True(t, root.Dirty())
	require.Equal(t, 1, len(root.Outputs()))
	require.Equal(t, []hsm.Key{}, root.Outputs()[0].Path)
}

func TestNode_MaintainsChildCache(t *testing.T) {
	be := &backend{}
	root, err := hsm.NewRoot(reg, def1.Type().ID, hsm.NewTestData(hsm.TestState1), map[int32]*persistencespb.StateMachineMap{
		def1.Type().ID: {
			MachinesById: map[string]*persistencespb.StateMachineNode{
				"persisted": {
					TransitionCount: 1,
					Data:            []byte(hsm.TestState1),
					Children: map[int32]*persistencespb.StateMachineMap{
						def1.Type().ID: {
							MachinesById: map[string]*persistencespb.StateMachineNode{
								"persisted-child": {
									TransitionCount: 2,
									Data:            []byte(hsm.TestState2),
								},
							},
						},
					},
				},
			},
		},
	}, be)
	require.NoError(t, err)

	key := hsm.Key{Type: def1.Type().ID, ID: "cached"}

	// Cache when a new child is added.
	child, err := root.AddChild(key, hsm.NewTestData(hsm.TestState1))
	require.NoError(t, err)
	// Verify this doesn't panic and the backend is propagated to the new child.
	child.AddHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED, func(e *historypb.HistoryEvent) {})

	childRef, err := root.Child([]hsm.Key{key})
	require.NoError(t, err)
	require.Equal(t, child, childRef)

	err = hsm.MachineTransition(child, func(d *hsm.TestData) (hsm.TransitionOutput, error) {
		d.SetState(hsm.TestState2)
		return hsm.TransitionOutput{}, nil
	})
	require.NoError(t, err)
	require.True(t, root.Dirty())
	require.Equal(t, 1, len(root.Outputs()))
	require.Equal(t, []hsm.Key{key}, root.Outputs()[0].Path)

	// Cache when loaded from persistence.
	path := []hsm.Key{{Type: def1.Type().ID, ID: "persisted"}, {Type: def1.Type().ID, ID: "persisted-child"}}
	child, err = root.Child(path)
	require.NoError(t, err)
	// Verify this doesn't panic and the backend is propagated to the loaded child.
	child.AddHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED, func(e *historypb.HistoryEvent) {})

	err = hsm.MachineTransition(child, func(d *hsm.TestData) (hsm.TransitionOutput, error) {
		d.SetState(hsm.TestState3)
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
	root, err := hsm.NewRoot(reg, def1.Type().ID, hsm.NewTestData(hsm.TestState1), make(map[int32]*persistencespb.StateMachineMap), nil)
	require.NoError(t, err)

	l1, err := root.AddChild(hsm.Key{Type: def1.Type().ID, ID: "l1"}, hsm.NewTestData(hsm.TestState1))
	require.NoError(t, err)
	l2, err := l1.AddChild(hsm.Key{Type: def1.Type().ID, ID: "l2"}, hsm.NewTestData(hsm.TestState1))
	require.NoError(t, err)

	require.Equal(t, []hsm.Key{}, root.Path())
	require.Equal(t, []hsm.Key{l1.Key}, l1.Path())
	require.Equal(t, []hsm.Key{l1.Key, l2.Key}, l2.Path())
}

func TestNode_AddChild(t *testing.T) {
	root, err := hsm.NewRoot(reg, def1.Type().ID, hsm.NewTestData(hsm.TestState1), make(map[int32]*persistencespb.StateMachineMap), nil)
	require.NoError(t, err)

	_, err = root.AddChild(hsm.Key{Type: 100, ID: "dont-care"}, "data")
	require.ErrorIs(t, err, hsm.ErrNotRegistered)
	_, err = root.AddChild(hsm.Key{Type: def1.Type().ID, ID: "dont-care"}, "data")
	require.ErrorContains(t, err, "invalid state type")
	_, err = root.AddChild(hsm.Key{Type: def1.Type().ID, ID: "id"}, hsm.NewTestData(hsm.TestState1))
	require.NoError(t, err)
	_, err = root.AddChild(hsm.Key{Type: def1.Type().ID, ID: "id"}, hsm.NewTestData(hsm.TestState1))
	require.ErrorIs(t, err, hsm.ErrStateMachineAlreadyExists)
}

func TestNode_Child(t *testing.T) {
	root, err := hsm.NewRoot(reg, def1.Type().ID, hsm.NewTestData(hsm.TestState1), make(map[int32]*persistencespb.StateMachineMap), nil)
	require.NoError(t, err)
	l1, err := root.AddChild(hsm.Key{Type: def1.Type().ID, ID: "l1"}, hsm.NewTestData(hsm.TestState1))
	require.NoError(t, err)
	l2, err := l1.AddChild(hsm.Key{Type: def1.Type().ID, ID: "l2"}, hsm.NewTestData(hsm.TestState1))
	require.NoError(t, err)

	// Recursive cached lookup.
	child, err := root.Child([]hsm.Key{{Type: def1.Type().ID, ID: "l1"}, {Type: 1, ID: "l2"}})
	require.NoError(t, err)
	require.Equal(t, l2, child)

	// Not registered.
	_, err = root.Child([]hsm.Key{{Type: 100, ID: "dont-care"}})
	require.ErrorIs(t, err, hsm.ErrNotRegistered)

	_, err = root.Child([]hsm.Key{{Type: def1.Type().ID, ID: "l3"}})
	require.ErrorIs(t, err, hsm.ErrStateMachineNotFound)

	// Lookup from persistence.
	root, err = hsm.NewRoot(reg, def1.Type().ID, hsm.NewTestData(hsm.TestState1), map[int32]*persistencespb.StateMachineMap{
		1: {
			MachinesById: map[string]*persistencespb.StateMachineNode{
				"p1": {
					TransitionCount: 1,
					Data:            []byte(hsm.TestState1),
				},
			},
		},
	}, nil)
	require.NoError(t, err)
	child, err = root.Child([]hsm.Key{{Type: def1.Type().ID, ID: "p1"}})
	require.NoError(t, err)

	// Verify child was properly loaded.
	require.Equal(t, hsm.Key{Type: def1.Type().ID, ID: "p1"}, child.Key)
	d, err := hsm.MachineData[*hsm.TestData](child)
	require.NoError(t, err)
	require.Equal(t, hsm.TestState1, d.State())
}

func TestNode_Walk(t *testing.T) {
	root, err := hsm.NewRoot(reg, def1.Type().ID, hsm.NewTestData(hsm.TestState1), make(map[int32]*persistencespb.StateMachineMap), nil)
	require.NoError(t, err)

	l1_1, err := root.AddChild(hsm.Key{Type: def1.Type().ID, ID: "l1_1"}, hsm.NewTestData(hsm.TestState1))
	require.NoError(t, err)
	_, err = l1_1.AddChild(hsm.Key{Type: def1.Type().ID, ID: "l2_2"}, hsm.NewTestData(hsm.TestState1))
	require.NoError(t, err)

	_, err = root.AddChild(hsm.Key{Type: def1.Type().ID, ID: "l1_2"}, hsm.NewTestData(hsm.TestState1))
	require.NoError(t, err)

	_, err = root.AddChild(hsm.Key{Type: def2.Type().ID, ID: "l1_3"}, hsm.NewTestData(hsm.TestState2))
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
	root, err := hsm.NewRoot(reg, def1.Type().ID, hsm.NewTestData(hsm.TestState1), make(map[int32]*persistencespb.StateMachineMap), nil)
	require.NoError(t, err)

	_, err = hsm.MachineData[string](root)
	require.ErrorIs(t, err, hsm.ErrIncompatibleType)

	// OK.
	value, err := hsm.MachineData[*hsm.TestData](root)
	require.NoError(t, err)
	require.Equal(t, hsm.NewTestData(hsm.TestState1), value)
}

func TestMachineTransition(t *testing.T) {
	root, err := hsm.NewRoot(reg, def1.Type().ID, hsm.NewTestData(hsm.TestState1), make(map[int32]*persistencespb.StateMachineMap), nil)
	require.NoError(t, err)

	err = hsm.MachineTransition(root, func(string) (hsm.TransitionOutput, error) {
		return hsm.TransitionOutput{}, nil
	})
	require.ErrorIs(t, err, hsm.ErrIncompatibleType)

	// Transition fails.
	err = hsm.MachineTransition(root, func(d *hsm.TestData) (hsm.TransitionOutput, error) {
		// Mutate state and make sure the cache is marked stale.
		d.SetState(hsm.TestState2)
		return hsm.TransitionOutput{}, fmt.Errorf("test") // nolint:goerr113
	})
	require.ErrorContains(t, err, "test")
	require.Equal(t, int64(0), root.TransitionCount())
	d, err := hsm.MachineData[*hsm.TestData](root)
	require.NoError(t, err)
	// Got the pre-mutation value back.
	require.Equal(t, hsm.TestState1, d.State())
	require.False(t, root.Dirty())

	err = hsm.MachineTransition(root, func(d *hsm.TestData) (hsm.TransitionOutput, error) {
		d.SetState(hsm.TestState2)
		return hsm.TransitionOutput{}, nil
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), root.TransitionCount())
	d, err = hsm.MachineData[*hsm.TestData](root)
	require.NoError(t, err)
	require.Equal(t, hsm.TestState2, d.State())
}

func TestCollection(t *testing.T) {
	root, err := hsm.NewRoot(reg, def1.Type().ID, hsm.NewTestData(hsm.TestState1), make(map[int32]*persistencespb.StateMachineMap), nil)
	require.NoError(t, err)

	coll := hsm.NewCollection[*hsm.TestData](root, def1.Type().ID)
	a, err := coll.Add("a", hsm.NewTestData(hsm.TestState1))
	require.NoError(t, err)
	b, err := coll.Add("b", hsm.NewTestData(hsm.TestState2))
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

	err = coll.Transition("a", func(d *hsm.TestData) (hsm.TransitionOutput, error) {
		d.SetState(hsm.TestState2)
		return hsm.TransitionOutput{}, nil
	})
	require.NoError(t, err)
	d, err := coll.Data("a")
	require.NoError(t, err)
	require.Equal(t, hsm.NewTestData(hsm.TestState2), d)
}
