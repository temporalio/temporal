// Package match provides exhaustive, per-field matchers for proto messages.
//
// A generated matcher struct (see match_gen.go, produced by
// cmd/tools/genprotofields) has one field per proto field. Every field must be
// assigned a Matcher or a literal; a field left unset fails the match. This
// makes assertions exhaustive: when a proto field is added, existing matchers
// fail until the new field is accounted for.
//
//	match.StartWorkflowExecutionRequest{
//		Namespace:  match.Eq("my-ns"),
//		WorkflowId: match.NotEmpty(),
//		Identity:   "worker-1",  // bare literal == match.Eq("worker-1")
//		// ... every other field, or match.Any() to ignore it
//	}.Test(t, actual)
package match

import (
	"fmt"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/protohelpers/predicate"
	"google.golang.org/protobuf/testing/protocmp"
)

// Matcher tests a single proto field value. Use the constructors below (Eq,
// NotEmpty, IsEmpty, Any, AnyOf) to build one. Any non-Matcher value passed
// where a Matcher is expected is treated as Eq(value).
type Matcher = predicate.Predicate

// Any matches any value. Use it to explicitly ignore a field in an otherwise
// exhaustive matcher.
func Any() Matcher { return predicate.Any() }

// NotEmpty matches when got holds a non-zero value: a non-empty string, a
// non-nil message, a non-empty repeated/map, a non-zero number, or true.
func NotEmpty() Matcher { return predicate.NotEmpty() }

// IsEmpty matches when got holds its zero value.
func IsEmpty() Matcher { return predicate.IsEmpty() }

// Eq matches when got equals want using proto-aware equality (protocmp), so it
// works for scalars, messages, and repeated/map fields containing messages.
func Eq(want any) Matcher {
	return predicate.Func(func(got any) predicate.Result {
		if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
			return predicate.Result{Reason: fmt.Sprintf("unexpected value (-want +got):\n%s", diff)}
		}
		return predicate.Result{OK: true}
	})
}

// AnyOf matches when got satisfies any of the given matchers or equals any of
// the given literals.
func AnyOf(options ...any) Matcher {
	return predicate.Func(func(got any) predicate.Result {
		for _, o := range options {
			if coerce(o).Eval(got).OK {
				return predicate.Result{OK: true}
			}
		}
		return predicate.Result{Reason: fmt.Sprintf("value %v matched none of the %d options", got, len(options))}
	})
}

// coerce returns want as a Matcher, wrapping a bare literal in Eq.
func coerce(want any) Matcher {
	if m, ok := want.(Matcher); ok {
		return m
	}
	return Eq(want)
}

// nested is implemented by every generated matcher, letting one be embedded in
// another via Nested/NestedPartial. checkAny runs the matcher against got (the
// sub-message, type-asserted internally) and returns its field failures.
type nested interface {
	checkAny(got any, exhaustive bool) []string
}

// Nested adapts a generated matcher into a Matcher that matches a message-typed
// field exhaustively (every sub-field must be specified). The field must be a
// non-nil message.
func Nested(m nested) Matcher { return nestedMatcher(m, true) }

// NestedPartial is like Nested but matches only the sub-fields that are set.
func NestedPartial(m nested) Matcher { return nestedMatcher(m, false) }

func nestedMatcher(m nested, exhaustive bool) Matcher {
	return predicate.Func(func(got any) predicate.Result {
		if predicate.Empty(got) {
			return predicate.Result{Reason: "expected a non-nil message"}
		}
		if fs := m.checkAny(got, exhaustive); len(fs) > 0 {
			return predicate.Result{Reason: "nested mismatch: " + strings.Join(fs, "; ")}
		}
		return predicate.Result{OK: true}
	})
}

// eval accumulates per-field match failures for a single message. Generated
// check methods drive it: one field call per proto field.
type eval struct {
	exhaustive bool
	failures   []string
}

func newEval(exhaustive bool) *eval {
	return &eval{exhaustive: exhaustive}
}

// field checks got against want (a Matcher or a bare literal). A nil want means
// the field was not specified: in exhaustive mode that is a failure (every field
// must be given a matcher; use Any to ignore one), in partial mode it is skipped.
func (e *eval) field(name string, want any, got any) {
	if want == nil {
		if e.exhaustive {
			e.failures = append(e.failures, fmt.Sprintf("%s: no matcher specified (use match.Any() to ignore)", name))
		}
		return
	}
	if !e.exhaustive && predicate.IsAny(want) {
		// In partial mode a field is ignored by omission; an explicit Any() is a
		// redundant misuse and likely a mistake.
		e.failures = append(e.failures, fmt.Sprintf("%s: match.Any() is redundant in EqualPartial; omit the field instead", name))
		return
	}
	if res := coerce(want).Eval(got); !res.OK {
		e.failures = append(e.failures, fmt.Sprintf("%s: %s", name, res.Reason))
	}
}

// reportFailures fails t (via require) with all field failures, or does nothing
// if there are none.
func reportFailures(t require.TestingT, typeName string, failures []string) {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}
	if len(failures) == 0 {
		return
	}
	require.Fail(t, fmt.Sprintf("%s did not match", typeName), strings.Join(failures, "\n"))
}
