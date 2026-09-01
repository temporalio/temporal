package types

// Lookup resolves a constraint key to its value for one evaluation.
//
// LOCAL MODIFICATION (not upstream): expression matching takes a Lookup rather than a
// Constraints map, so a caller holding several layers of constraints — for example
// process-ambient ones plus per-request ones — can present them as a single view without
// copying them into a new map on every evaluation. Constraints itself implements Lookup, so
// passing a plain map still works.
type Lookup interface {
	Get(key string) (any, bool)
}

type Constraints map[string]any

// Get implements Lookup.
func (c Constraints) Get(key string) (any, bool) {
	v, ok := c[key]
	return v, ok
}

type Key string

type Operator int

const (
	OpEqual Operator = iota + 1
	OpNotEqual
	OpGreater
	OpLess
	OpAnd
	OpOr
)

// Override is one conditional value: if MatchString evaluates true against the constraints,
// MatchResult is the answer.
type Override[V any] struct {
	MatchString string
	MatchResult V
}

// Config is one configuration entry: a default plus overrides tried in order.
//
// V is opaque. The library parses and evaluates the match expressions and hands back
// whichever V won; it never inspects, decodes or converts a value. Callers that want values
// decoded from JSON can use configurator.JSONConfig.
//
// Keeping values opaque is deliberate. Decoding here would mean the library imposing its own
// type system on the caller's, and for a caller whose "type" of a setting is an arbitrary
// conversion function rather than a fixed set, there is no type system to impose.
type Config[V any] struct {
	DefaultValue V
	Overrides    []Override[V]
}
