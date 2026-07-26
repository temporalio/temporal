package dynamicconfig

import (
	"reflect"

	"go.temporal.io/server/common/log/tag"
)

type (
	// Evaluator is an optional, expression-based source of dynamic config values that is
	// consulted before the Client's ConstrainedValue lookup.
	//
	// It exists because Client/ConstrainedValue can only express a closed set of constraint
	// dimensions (the fields of Constraints) combined by exact equality. An Evaluator instead
	// receives an open map of constraints and may resolve a value using arbitrary logic, so
	// dimensions such as deployment zone, host, or task queue partition index can constrain a
	// setting without adding a field to Constraints and regenerating setting_gen.go.
	//
	// An Evaluator never changes which settings exist or what their compiled-in defaults are.
	// Any key it does not answer for falls through to the normal Client path unchanged.
	Evaluator interface {
		// Has reports whether this Evaluator has any configuration for key. It is called on
		// every read of every setting, so it must be a cheap in-memory lookup and must not
		// allocate.
		Has(key Key) bool

		// Eval resolves key for the given constraints, or returns nil if the key is not
		// configured here (in which case the Client path runs as usual).
		//
		// base is the most specific Constraints for this call, i.e. the first element of the
		// setting's precedence list. extra carries ad-hoc dimensions attached with
		// Collection.WithConstraints and is usually nil.
		//
		// The Constraints field of the returned ConstrainedValue is unused. The pointer must
		// be stable: the same value must be returned for the same inputs until the
		// Evaluator's configuration is reloaded, because Collection caches conversions using
		// weak pointers into it. See the comment on Client.GetValue.
		Eval(key Key, base Constraints, extra map[string]any) *ConstrainedValue
	}

	// NotifyingEvaluator is an optional interface an Evaluator can implement to report that
	// its configuration changed, so that settings subscribers see expression config updates
	// the same way they see file config updates. It mirrors NotifyingClient.
	NotifyingEvaluator interface {
		// Subscribe registers a callback invoked with the keys whose configuration changed.
		// The caller should call cancel to unsubscribe.
		Subscribe(update func([]Key)) (cancel func())
	}
)

// evalOverride consults c's Evaluator, if any, for key.
//
// It returns the converted value, the raw pre-conversion value (for subscription change
// detection), and whether the Evaluator supplied a usable value. A key that the Evaluator
// does not configure, or whose value fails conversion, reports ok=false so that the caller
// falls back to the Client — degrading to the currently configured value rather than to the
// compiled-in default.
func evalOverride[T any](
	c *Collection,
	key Key,
	convert func(value any) (T, error),
	precedence []Constraints,
) (value T, raw any, ok bool) {
	if c.evaluator == nil || !c.evaluator.Has(key) {
		return value, nil, false
	}

	var base Constraints
	if len(precedence) > 0 {
		base = precedence[0]
	}

	cvp := c.evaluator.Eval(key, base, c.extra)
	if cvp == nil {
		return value, nil, false
	}

	typedVal, err := convertWithCache(c, key, convert, cvp)
	if err != nil {
		if c.throttleLog() {
			c.logger.Warn("Failed to convert expression config value, falling back to file config",
				tag.Key(key.String()), tag.IgnoredValue(cvp), tag.Error(err))
		}
		return value, nil, false
	}
	return typedVal, cvp.Value, true
}

// dispatchIfChanged invokes sub's callback with value if raw differs from the raw value the
// subscriber last saw, mirroring the change detection in dispatchUpdate.
//
// Called with subscriptionLock held.
func dispatchIfChanged[T any](sub *subscription[T], value T, raw any) {
	if reflect.DeepEqual(sub.raw, raw) {
		// point at the new raw value, not the old one, so old loaded files can be collected
		sub.raw = raw
		return
	}
	sub.raw = raw
	go sub.f(value)
}
