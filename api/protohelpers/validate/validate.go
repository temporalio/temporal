// Package validate provides exhaustive, per-field validation for proto
// messages, intended as a thin layer in production code that rejects malformed
// requests before they reach business logic.
//
// A generated validator struct (see validate_gen.go, produced by
// cmd/tools/genprotofields) has one field per proto field. Every field must be
// assigned a Rule; a field left unset makes validation fail loudly, so adding a
// proto field forces the validator to account for it. Validators are typically
// declared once, at package scope, next to the handler they guard:
//
//	var startRequest = validate.StartWorkflowExecutionRequest{
//		Namespace:  validate.Required(),
//		WorkflowId: validate.Required(),
//		Identity:   validate.Optional(),
//		// ... a rule for every other field
//	}
//
//	if err := startRequest.Validate(req); err != nil {
//		return nil, serviceerror.NewInvalidArgument(err.Error())
//	}
//
// Rules share their vocabulary with the test matcher via
// api/protohelpers/predicate: Required is NotEmpty, Optional is Any.
package validate

import (
	"fmt"
	"strings"

	"go.temporal.io/server/api/protohelpers/predicate"
)

// Rule validates a single proto field value.
type Rule = predicate.Predicate

// Required rejects a field that is unset (empty string, nil message, empty
// repeated/map, zero number, or false).
func Required() Rule { return predicate.NotEmpty() }

// Optional accepts any value, including unset. Use it to explicitly mark a
// field as not validated.
func Optional() Rule { return predicate.Any() }

// Custom builds a Rule from fn, which returns an error describing why the value
// is invalid (or nil if valid).
func Custom(fn func(got any) error) Rule {
	return predicate.Func(func(got any) predicate.Result {
		if err := fn(got); err != nil {
			return predicate.Result{Reason: err.Error()}
		}
		return predicate.Result{OK: true}
	})
}

// eval accumulates per-field violations for a single message. Generated
// Validate methods drive it: one field call per proto field, then err.
type eval struct {
	typeName   string
	violations []string
}

func newEval(typeName string) *eval {
	return &eval{typeName: typeName}
}

// field checks got against rule. A nil rule is itself a violation, enforcing
// exhaustiveness: every field must be given a rule (use Optional to skip one).
func (e *eval) field(name string, rule Rule, got any) {
	if rule == nil {
		e.violations = append(e.violations, fmt.Sprintf("%s: no rule specified (use validate.Optional() to skip)", name))
		return
	}
	if res := rule.Eval(got); !res.OK {
		e.violations = append(e.violations, fmt.Sprintf("%s: %s", name, res.Reason))
	}
}

// err returns a single error describing all violations, or nil if valid.
func (e *eval) err() error {
	if len(e.violations) == 0 {
		return nil
	}
	return fmt.Errorf("invalid %s: %s", e.typeName, strings.Join(e.violations, "; "))
}
