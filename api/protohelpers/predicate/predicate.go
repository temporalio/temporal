// Package predicate holds the field predicates shared by the test matcher
// (api/protohelpers/match) and the production validator
// (api/protohelpers/validate).
//
// A Predicate answers a single yes/no question about a field value, with a
// human-readable reason on failure. The presence predicates here (Any,
// NotEmpty, IsEmpty) are the shared vocabulary: the matcher reports a failed
// predicate as a test failure, the validator returns it as an error. This
// package is dependency-light so it is safe to import from production code.
package predicate

import (
	"fmt"
	"reflect"
)

// Result is the outcome of evaluating a Predicate against a value.
type Result struct {
	OK     bool
	Reason string // populated only when OK is false
}

// Predicate answers a yes/no question about a field value.
type Predicate interface {
	Eval(got any) Result
}

// Func adapts a function to the Predicate interface.
type Func func(got any) Result

// Eval implements Predicate.
func (f Func) Eval(got any) Result { return f(got) }

// pass is a reusable successful result.
var pass = Result{OK: true}

// anyPredicate matches any value. It is a distinct type (rather than a Func) so
// callers can detect an explicit Any via IsAny — e.g. to reject it where
// ignoring a field is expressed by omission instead.
type anyPredicate struct{}

func (anyPredicate) Eval(any) Result { return pass }

// Any matches any value.
func Any() Predicate {
	return anyPredicate{}
}

// IsAny reports whether p is the predicate returned by Any.
func IsAny(p any) bool {
	_, ok := p.(anyPredicate)
	return ok
}

// NotEmpty matches when got holds a non-zero value: a non-empty string, a
// non-nil message, a non-empty repeated/map, a non-zero number, or true.
func NotEmpty() Predicate {
	return Func(func(got any) Result {
		if Empty(got) {
			return Result{Reason: "expected a non-empty value, got empty"}
		}
		return pass
	})
}

// IsEmpty matches when got holds its zero value.
func IsEmpty() Predicate {
	return Func(func(got any) Result {
		if !Empty(got) {
			return Result{Reason: fmt.Sprintf("expected empty, got %v", got)}
		}
		return pass
	})
}

// Empty reports whether v is a proto field's zero value: nil, a nil pointer, an
// empty string/slice/map/array, or a zero scalar.
func Empty(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer, reflect.Interface:
		return rv.IsNil()
	case reflect.Slice, reflect.Map, reflect.Array, reflect.String:
		return rv.Len() == 0
	default:
		return rv.IsZero()
	}
}
