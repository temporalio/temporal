// Package set provides Set, a CHASM component holding a set of keys, for use as a child of other
// libraries' components.
package set

import (
	"maps"
	"slices"

	"go.temporal.io/server/chasm"
	setpb "go.temporal.io/server/chasm/lib/set/gen/setpb/v1"
)

// Mutation is a batch of changes to a Set.
type Mutation[K comparable] struct {
	Add    []K
	Remove []K
}

// IsEmpty reports whether the mutation neither adds nor removes any key.
func (m Mutation[K]) IsEmpty() bool {
	return len(m.Add) == 0 && len(m.Remove) == 0
}

// Set is a CHASM component holding a set of keys.
//
// Update is idempotent: adding a key that is present, or removing one that is absent, is a no-op.
// If a key appears in both Add and Remove of one mutation, Remove wins, so the key is absent after
// the update whether or not it was present before.
//
// Read returns each key once, in ascending byte order.
//
// Callers depend only on this interface so that the implementation can change, for example to one
// that persists deltas instead of the full set.
type Set[K comparable] interface {
	chasm.Component

	Update(ctx chasm.MutableContext, mutation Mutation[K]) error
	Read(ctx chasm.Context) ([]K, error)
}

var _ Set[string] = (*stringSet)(nil)

// stringSet is a Set that persists its full contents in one node on every update. Its cost grows
// with the set size rather than the mutation size.
//
// TODO: Replace with a Set backed by a CHASM log store that persists deltas, once one exists.
// Register it under a new component name rather than changing this one, so that persisted
// string_set nodes still decode. Callers hold the set as a Set[string], so only NewStringSet
// needs to change.
type stringSet struct {
	chasm.UnimplementedComponent
	*setpb.StringSetState
}

// NewStringSet returns an empty Set of strings, to be attached to a component as a child, e.g. with
// chasm.NewComponentField[Set[string]].
func NewStringSet() Set[string] {
	return newStringSet()
}

func newStringSet() *stringSet {
	return &stringSet{StringSetState: &setpb.StringSetState{}}
}

func (s *stringSet) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

func (s *stringSet) Update(_ chasm.MutableContext, mutation Mutation[string]) error {
	if mutation.IsEmpty() {
		return nil
	}
	keys := make(map[string]struct{}, len(s.Keys)+len(mutation.Add))
	for _, key := range s.Keys {
		keys[string(key)] = struct{}{}
	}
	for _, key := range mutation.Add {
		keys[key] = struct{}{}
	}
	for _, key := range mutation.Remove {
		delete(keys, key)
	}
	// Sorted so that the persisted state does not depend on map iteration order.
	// Go compares strings bytewise, so this is ascending byte order.
	sorted := slices.Sorted(maps.Keys(keys))
	if slices.EqualFunc(s.Keys, sorted, func(a []byte, b string) bool { return string(a) == b }) {
		// The mutation changed nothing, so leave the state as is.
		return nil
	}
	s.Keys = make([][]byte, len(sorted))
	for i, key := range sorted {
		s.Keys[i] = []byte(key)
	}
	return nil
}

func (s *stringSet) Read(_ chasm.Context) ([]string, error) {
	keys := make([]string, len(s.Keys))
	for i, key := range s.Keys {
		keys[i] = string(key)
	}
	return keys, nil
}
