package types

import "encoding/json"

type Constraints map[string]any

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
