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

type definition struct {
}

func (definition) Deserialize(b []byte) (any, error) {
	return &data{state(string(b))}, nil
}

// Serialize implements hsm.StateMachineDefinition.
func (definition) Serialize(s any) ([]byte, error) {
	t, ok := s.(*data)
	if !ok {
		return nil, fmt.Errorf("invalid type")
	}
	return []byte(t.state), nil
}

// Type implements hsm.StateMachineDefinition.
func (definition) Type() hsm.MachineType {
	return hsm.MachineType{
		ID:   1,
		Name: "test",
	}
}

var def = definition{}
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
	if err := reg.RegisterMachine(def); err != nil {
		panic(err)
	}
}

func TestNode_MaintainsCachedData(t *testing.T) {
	root, err := hsm.NewRoot(reg, def.Type().ID, &data{state1}, make(map[int32]*persistencespb.StateMachineMap), nil)
	require.NoError(t, err)

	v1, err := hsm.MachineData[*data](root)
	require.NoError(t, err)

	require.False(t, root.Dirty())
	require.Equal(t, 0, len(root.Outputs()))

	err = hsm.MachineTransition(root, func(d *data) (hsm.TransitionOutput, error) {
		d.state = state2
		return hsm.TransitionOutput{}, nil
	})
	require.NoError(t, err)

	// Our data variable is a pointer to the cache.
	require.Equal(t, state2, v1.state)

	require.NoError(t, err)
	require.True(t, root.Dirty())
	require.Equal(t, 1, len(root.Outputs()))
	require.Equal(t, []hsm.Key{}, root.Outputs()[0].Path)
}

func TestNode_MaintainsChildCache(t *testing.T) {
	be := &backend{}
	root, err := hsm.NewRoot(reg, def.Type().ID, &data{state1}, map[int32]*persistencespb.StateMachineMap{
		def.Type().ID: {
			MachinesById: map[string]*persistencespb.StateMachineNode{
				"persisted": {
					TransitionCount: 1,
					Data:            []byte(state1),
					Children: map[int32]*persistencespb.StateMachineMap{
						def.Type().ID: {
							MachinesById: map[string]*persistencespb.StateMachineNode{
								"persisted-child": {
									TransitionCount: 2,
									Data:            []byte(state2),
								},
							},
						},
					},
				},
			},
		},
	}, be)
	require.NoError(t, err)

	key := hsm.Key{Type: def.Type().ID, ID: "cached"}

	// Cache when a new child is added.
	child, err := root.AddChild(key, &data{state1})
	require.NoError(t, err)
	// Verify this doesn't panic and the backend is propagated to the new child.
	child.AddHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED, func(e *historypb.HistoryEvent) {})

	childRef, err := root.Child([]hsm.Key{key})
	require.NoError(t, err)
	require.Equal(t, child, childRef)

	err = hsm.MachineTransition(child, func(d *data) (hsm.TransitionOutput, error) {
		d.state = state2
		return hsm.TransitionOutput{}, nil
	})
	require.NoError(t, err)
	require.True(t, root.Dirty())
	require.Equal(t, 1, len(root.Outputs()))
	require.Equal(t, []hsm.Key{key}, root.Outputs()[0].Path)

	// Cache when loaded from persistence.
	path := []hsm.Key{{Type: def.Type().ID, ID: "persisted"}, {Type: def.Type().ID, ID: "persisted-child"}}
	child, err = root.Child(path)
	require.NoError(t, err)
	// Verify this doesn't panic and the backend is propagated to the loaded child.
	child.AddHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED, func(e *historypb.HistoryEvent) {})

	err = hsm.MachineTransition(child, func(d *data) (hsm.TransitionOutput, error) {
		d.state = state3
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
	root, err := hsm.NewRoot(reg, def.Type().ID, &data{state1}, make(map[int32]*persistencespb.StateMachineMap), nil)
	require.NoError(t, err)

	l1, err := root.AddChild(hsm.Key{Type: 1, ID: "l1"}, &data{state1})
	require.NoError(t, err)
	l2, err := l1.AddChild(hsm.Key{Type: 1, ID: "l2"}, &data{state1})
	require.NoError(t, err)

	require.Equal(t, []hsm.Key{}, root.Path())
	require.Equal(t, []hsm.Key{l1.Key}, l1.Path())
	require.Equal(t, []hsm.Key{l1.Key, l2.Key}, l2.Path())
}

func TestNode_AddChild(t *testing.T) {
	root, err := hsm.NewRoot(reg, def.Type().ID, &data{state1}, make(map[int32]*persistencespb.StateMachineMap), nil)
	require.NoError(t, err)

	_, err = root.AddChild(hsm.Key{Type: 2, ID: "dont-care"}, "data")
	require.ErrorIs(t, err, hsm.ErrNotRegistered)
	_, err = root.AddChild(hsm.Key{Type: 1, ID: "dont-care"}, "data")
	require.ErrorContains(t, err, "invalid type")
	_, err = root.AddChild(hsm.Key{Type: 1, ID: "id"}, &data{state1})
	require.NoError(t, err)
	_, err = root.AddChild(hsm.Key{Type: 1, ID: "id"}, &data{state1})
	require.ErrorIs(t, err, hsm.ErrStateMachineAlreadyExists)
}

func TestNode_Child(t *testing.T) {
	root, err := hsm.NewRoot(reg, def.Type().ID, &data{state1}, make(map[int32]*persistencespb.StateMachineMap), nil)
	require.NoError(t, err)
	l1, err := root.AddChild(hsm.Key{Type: 1, ID: "l1"}, &data{state1})
	require.NoError(t, err)
	l2, err := l1.AddChild(hsm.Key{Type: 1, ID: "l2"}, &data{state1})
	require.NoError(t, err)

	// Recursive cached lookup.
	child, err := root.Child([]hsm.Key{{Type: 1, ID: "l1"}, {Type: 1, ID: "l2"}})
	require.NoError(t, err)
	require.Equal(t, l2, child)

	// Not registered.
	_, err = root.Child([]hsm.Key{{Type: 2, ID: "dont-care"}})
	require.ErrorIs(t, err, hsm.ErrNotRegistered)

	_, err = root.Child([]hsm.Key{{Type: 1, ID: "l3"}})
	require.ErrorIs(t, err, hsm.ErrStateMachineNotFound)

	// Lookup from persistence.
	root, err = hsm.NewRoot(reg, def.Type().ID, &data{state1}, map[int32]*persistencespb.StateMachineMap{
		1: {
			MachinesById: map[string]*persistencespb.StateMachineNode{
				"p1": {
					TransitionCount: 1,
					Data:            []byte(state1),
				},
			},
		},
	}, nil)
	require.NoError(t, err)
	child, err = root.Child([]hsm.Key{{Type: 1, ID: "p1"}})
	require.NoError(t, err)

	// Verify child was properly loaded.
	require.Equal(t, hsm.Key{Type: 1, ID: "p1"}, child.Key)
	d, err := hsm.MachineData[*data](child)
	require.NoError(t, err)
	require.Equal(t, state1, d.state)
}

func TestMachineData(t *testing.T) {
	root, err := hsm.NewRoot(reg, def.Type().ID, &data{state1}, make(map[int32]*persistencespb.StateMachineMap), nil)
	require.NoError(t, err)

	_, err = hsm.MachineData[string](root)
	require.ErrorIs(t, err, hsm.ErrIncompatibleType)

	// OK.
	value, err := hsm.MachineData[*data](root)
	require.NoError(t, err)
	require.Equal(t, &data{state1}, value)
}

func TestMachineTransition(t *testing.T) {
	root, err := hsm.NewRoot(reg, def.Type().ID, &data{state1}, make(map[int32]*persistencespb.StateMachineMap), nil)
	require.NoError(t, err)

	err = hsm.MachineTransition(root, func(string) (hsm.TransitionOutput, error) {
		return hsm.TransitionOutput{}, nil
	})
	require.ErrorIs(t, err, hsm.ErrIncompatibleType)

	// Transition fails.
	err = hsm.MachineTransition(root, func(d *data) (hsm.TransitionOutput, error) {
		// Mutate state and make sure the cache is marked stale.
		d.state = state2
		return hsm.TransitionOutput{}, fmt.Errorf("test") // nolint:goerr113
	})
	require.ErrorContains(t, err, "test")
	require.Equal(t, int64(0), root.TransitionCount())
	d, err := hsm.MachineData[*data](root)
	require.NoError(t, err)
	// Got the pre-mutation value back.
	require.Equal(t, state1, d.state)
	require.False(t, root.Dirty())

	err = hsm.MachineTransition(root, func(d *data) (hsm.TransitionOutput, error) {
		d.state = state2
		return hsm.TransitionOutput{}, nil
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), root.TransitionCount())
	d, err = hsm.MachineData[*data](root)
	require.NoError(t, err)
	require.Equal(t, state2, d.state)
}

func TestCollection(t *testing.T) {
	root, err := hsm.NewRoot(reg, def.Type().ID, &data{state1}, make(map[int32]*persistencespb.StateMachineMap), nil)
	require.NoError(t, err)

	coll := hsm.NewCollection[*data](root, def.Type().ID)
	a, err := coll.Add("a", &data{state1})
	require.NoError(t, err)
	b, err := coll.Add("b", &data{state2})
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

	err = coll.Transition("a", func(d *data) (hsm.TransitionOutput, error) {
		d.state = state2
		return hsm.TransitionOutput{}, nil
	})
	require.NoError(t, err)
	d, err := coll.Data("a")
	require.NoError(t, err)
	require.Equal(t, &data{state2}, d)
}
