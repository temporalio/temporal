package dynamicconfig

import "go.temporal.io/server/common/log"

type (
	// StaticClient is a simple implementation of Client that just looks up in a map.
	// Values can be either plain values or []ConstrainedValue for a constrained value.
	// A StaticClient should never be mutated or you'll create race conditions!
	StaticClient map[Key]any
)

func (s StaticClient) GetValue(key Key) []ConstrainedValue {
	if v, ok := s[key]; ok {
		if cvs, ok := v.([]ConstrainedValue); ok {
			return cvs
		}
		return []ConstrainedValue{{Value: v}}
	}
	return nil
}

// NewNoopClient returns a Client that has no keys (a Collection using it will always return
// default values).
func NewNoopClient() Client {
	return StaticClient(nil)
}

// NewNoopCollection creates a new noop collection.
func NewNoopCollection() *Collection {
	return NewCollection(NewNoopClient(), log.NewNoopLogger())
}
