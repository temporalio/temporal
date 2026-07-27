package configurator

import "go.temporal.io/server/common/dynamicconfig/configurator/types"

// ParsedValueKind identifies the type of a parsed DSL value.
type ParsedValueKind int

const (
	KindString  ParsedValueKind = iota
	KindInteger                 // unquoted integer literal, e.g. 100
	KindFloat                   // unquoted float literal, e.g. 9.99
)

// ParsedValue holds a typed constant from the DSL, parsed at load time.
// Str is always set; Num is set for KindInteger and KindFloat.
type ParsedValue struct {
	Str  string
	Num  float64
	Kind ParsedValueKind
}

const (
	OpEqual types.Operator = iota + 1
	OpNotEqual
	OpGreater
	OpLess
	OpAnd
	OpOr
)

type Expression struct {
	Key            types.Key
	Value          ParsedValue
	Operator       types.Operator
	Subexpressions []*Expression
}

// Condition pairs a parsed expression with the value it yields when it matches. V is opaque;
// see types.Config.
type Condition[V any] struct {
	Expression  Expression
	MatchResult V
}
