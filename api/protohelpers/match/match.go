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

// eval accumulates per-field match failures for a single message. Generated
// Test methods drive it: one field call per proto field, then report.
type eval struct {
	typeName string
	failures []string
}

func newEval(typeName string) *eval {
	return &eval{typeName: typeName}
}

// field checks got against want (a Matcher or a bare literal). A nil want is a
// failure, enforcing exhaustiveness: every field must be given a matcher (use
// Any to ignore one).
func (e *eval) field(name string, want any, got any) {
	if want == nil {
		e.failures = append(e.failures, fmt.Sprintf("%s: no matcher specified (use match.Any() to ignore)", name))
		return
	}
	if res := coerce(want).Eval(got); !res.OK {
		e.failures = append(e.failures, fmt.Sprintf("%s: %s", name, res.Reason))
	}
}

func (e *eval) report(t require.TestingT) {
	if h, ok := t.(interface{ Helper() }); ok {
		h.Helper()
	}
	if len(e.failures) == 0 {
		return
	}
	require.Fail(t, fmt.Sprintf("%s did not match", e.typeName), strings.Join(e.failures, "\n"))
}
