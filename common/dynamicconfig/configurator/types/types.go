package types

import "encoding/json"

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

type Override struct {
	MatchString string
	MatchResult json.RawMessage
}

// Config is the persisted representation of a configuration entry.
type Config struct {
	DefaultValue json.RawMessage
	Overrides    []Override
}
