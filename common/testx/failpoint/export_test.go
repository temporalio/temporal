package failpoint

import (
	"maps"
	"slices"
)

// Exports of internals for the black-box tests in package failpoint_test.

// State is the observable configuration of a failpoint.
type State struct {
	Key     AnyKey
	Action  Action
	Trigger Trigger
}

// State returns the configuration of the failpoint.
func (f *Failpoint[T]) State() State {
	f.mu.Lock()
	defer f.mu.Unlock()
	return State{
		Key:     f.key,
		Action:  f.action,
		Trigger: f.trigger,
	}
}

// Keys returns the keys of all registered failpoints, as their strings.
func (r *Registry) Keys() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var keys []string
	for id := range maps.Keys(r.failpoints) {
		keys = append(keys, id.String())
	}
	slices.Sort(keys)
	return keys
}

// States returns the State of every failpoint of the registry that has one, by key string.
func (r *Registry) States() map[string]State {
	r.mu.Lock()
	defer r.mu.Unlock()
	states := map[string]State{}
	for id, fp := range r.failpoints {
		if withState, ok := fp.(interface{ State() State }); ok {
			states[id.String()] = withState.State()
		}
	}
	return states
}

// UndeclaredKey returns a key that no NewKey declared, for tests of the failpoint mechanics that rules
// don't reach.
func UndeclaredKey[T any](name string) Key[T] {
	return Key[T]{name: name}
}
