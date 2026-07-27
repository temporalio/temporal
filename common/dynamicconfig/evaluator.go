package dynamicconfig

import (
	"go.temporal.io/server/common/log/tag"
)

// Evaluator resolves a setting against an open set of caller-supplied constraints.
//
// It exists because Client can only answer "what is this key's value for this process":
// GetValue is handed a Key and nothing else. An Evaluator is handed the caller's dimensions
// too, so a value can depend on the namespace, the calling SDK, or anything else the caller
// knows, without those dimensions needing a field on Constraints and a regenerated
// setting_gen.go.
//
// A Client may also implement Evaluator; Collection picks it up automatically.
type Evaluator interface {
	// Eval resolves key against c, layered over whatever process-scoped constraints the
	// Evaluator holds. Returns nil when the key is not configured here, in which case the
	// Client path applies.
	//
	// It is called on every GetC read, including for the great majority of settings that are
	// not expression-configured at all, so returning nil must be cheap and must not allocate.
	//
	// The result is shaped like Client.GetValue's — a slice, in practice of one unconstrained
	// value — so that it can be fed to the same matching machinery, which matters for settings
	// with constrained defaults. The slice must be stable: the same slice for the same inputs
	// until the configuration is reloaded, because Collection caches conversions against
	// pointers into it using weak pointers. See the comment on Client.GetValue.
	Eval(key Key, c ConstraintsMap) []ConstrainedValue
}

// matchAndConvertC is the GetC counterpart of matchAndConvert. It consults the Evaluator with
// the caller's constraints, and otherwise falls back to the ordinary Client lookup, using the
// precedence list projected out of the same constraints so that a call site migrated to GetC
// still honours constrained values in the dynamic config file.
func matchAndConvertC[T any](
	c *Collection,
	key Key,
	def T,
	convert func(value any) (T, error),
	cm ConstraintsMap,
	precedence []Constraints,
) T {
	if v, ok := evalConstraints(c, key, convert, cm); ok {
		return v
	}
	cvs := c.client.GetValue(key)
	v, _ := matchAndConvertCvs(c, key, def, convert, precedence, cvs)
	return v
}

// matchAndConvertCWithConstrainedDefault is matchAndConvertC for settings whose default is
// itself constrained.
//
// The expression value is fed into the normal constrained-default resolution rather than
// short-circuiting it, so a built-in constrained default that is more specific still wins.
// That keeps GetC in step with Get, which reaches the same value through Client.GetValue and
// therefore always went through this path. It matters: the constrained default of one
// partition for the per-namespace worker task queue is a correctness invariant, not a
// preference, and an operator setting a fleet-wide partition count does not mean to override
// it.
func matchAndConvertCWithConstrainedDefault[T any](
	c *Collection,
	key Key,
	cdef []TypedConstrainedValue[T],
	convert func(value any) (T, error),
	cm ConstraintsMap,
	precedence []Constraints,
) T {
	cvs := evalConstraintValues(c, key, cm)
	if cvs == nil {
		cvs = c.client.GetValue(key)
	}
	value, _ := findAndResolveWithConstrainedDefaults(c, key, convert, cvs, cdef, precedence)
	return value
}

// evalConstraints asks the Collection's Evaluator, if any, for key.
//
// A key the Evaluator does not configure, or whose value fails conversion, reports false so
// the caller falls back to the Client — degrading to the configured file value rather than to
// the compiled-in default.
func evalConstraints[T any](
	c *Collection,
	key Key,
	convert func(value any) (T, error),
	cm ConstraintsMap,
) (value T, ok bool) {
	cvs := evalConstraintValues(c, key, cm)
	if len(cvs) == 0 {
		return value, false
	}
	// The slice is owned by the Evaluator's snapshot, so this pointer is stable and the
	// conversion cache can key on it.
	cvp := &cvs[0]
	typedVal, err := convertWithCache(c, key, convert, cvp)
	if err != nil {
		if c.throttleLog() {
			c.logger.Warn("Failed to convert expression config value, falling back to file config",
				tag.Key(key.String()), tag.IgnoredValue(cvp), tag.Error(err))
		}
		return value, false
	}
	return typedVal, true
}

// evalConstraintValues asks the Collection's Evaluator, if any, for key. Returns nil when
// there is no Evaluator or it does not configure key.
func evalConstraintValues(c *Collection, key Key, cm ConstraintsMap) []ConstrainedValue {
	if c.evaluator == nil {
		return nil
	}
	return c.evaluator.Eval(key, cm)
}
