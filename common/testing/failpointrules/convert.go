// Package failpointrules configures the failpoints of a server in test builds from the
// testing.failpointRules dynamic config setting.
package failpointrules

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/mitchellh/mapstructure"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/testx/failpoint"
)

// ruleConfig is a rule as it is written in dynamic config.
type ruleConfig struct {
	Key        string
	Scope      string
	Action     string
	Error      string
	Message    string
	Behavior   string
	Delay      time.Duration
	Recurrence string
	Trigger    failpoint.Trigger
	// Enabled defaults to true.
	Enabled *bool
}

// ruleActions are the actions that rules can name.
var ruleActions = map[string]failpoint.Action{
	"Return": failpoint.Return,
	"Panic":  failpoint.Panic,
	"Block":  failpoint.Block,
	"Pause":  failpoint.Pause,
	"Call":   failpoint.Call,
	"On":     failpoint.On,
}

var ruleRecurrences = map[string]failpoint.Recurrence{
	"":       failpoint.RecurrenceUnspecified,
	"Once":   failpoint.Once,
	"Always": failpoint.Always,
}

var durationType = reflect.TypeFor[time.Duration]()

// ConvertRules converts a dynamic config value to failpoint rules. The value is a list of maps
// with the fields key, scope, action, error, message, behavior, delay, recurrence, trigger
// (probability, seed, after, max) and enabled. It returns an error if a field is unknown or
// malformed, or if the rules are not valid for failpoint.RuleSet.Apply.
func ConvertRules(value any) ([]failpoint.Rule, error) {
	if rules, ok := value.([]failpoint.Rule); ok {
		return rules, failpoint.ValidateRules(rules)
	}
	var configs []ruleConfig
	if err := DecodeStrict(value, &configs); err != nil {
		return nil, err
	}
	rules := make([]failpoint.Rule, 0, len(configs))
	for i, c := range configs {
		rule, err := c.rule()
		if err != nil {
			return nil, fmt.Errorf("rule %d: %w", i, err)
		}
		rules = append(rules, rule)
	}
	if err := failpoint.ValidateRules(rules); err != nil {
		return nil, err
	}
	return rules, nil
}

// DecodeStrict decodes a dynamic config value into result, which points to a struct, a slice of
// structs or a map. Unlike dynamicconfig.ConvertStructure, it returns an error for unknown fields,
// for a fractional number in an integer field and for a non-string map key, and it parses
// durations written as strings, such as "1.5s".
func DecodeStrict(value any, result any) error {
	// mapstructure assumes that the keys of maps decoded into structs are strings.
	value, err := stringKeys(value)
	if err != nil {
		return err
	}
	dec, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Result:      result,
		ErrorUnused: true,
		DecodeHook:  mapstructure.ComposeDecodeHookFunc(decodeDuration, decodeInteger),
	})
	if err != nil {
		return err
	}
	return dec.Decode(value)
}

func (c ruleConfig) rule() (failpoint.Rule, error) {
	action, ok := ruleActions[c.Action]
	if !ok {
		return failpoint.Rule{}, fmt.Errorf("unknown action %q", c.Action)
	}
	recurrence, ok := ruleRecurrences[c.Recurrence]
	if !ok {
		return failpoint.Rule{}, fmt.Errorf("unknown recurrence %q", c.Recurrence)
	}
	return failpoint.Rule{
		Key:        c.Key,
		Scope:      c.Scope,
		Action:     action,
		Error:      c.Error,
		Message:    c.Message,
		Behavior:   c.Behavior,
		Delay:      c.Delay,
		Recurrence: recurrence,
		Trigger:    c.Trigger,
		Disabled:   c.Enabled != nil && !*c.Enabled,
	}, nil
}

// stringKeys returns value with every map converted to map[string]any. It returns an error if a
// map has a key that is not a string.
func stringKeys(value any) (any, error) {
	switch v := value.(type) {
	case []any:
		result := make([]any, len(v))
		for i, elem := range v {
			converted, err := stringKeys(elem)
			if err != nil {
				return nil, err
			}
			result[i] = converted
		}
		return result, nil
	case []map[string]any:
		result := make([]any, len(v))
		for i, elem := range v {
			result[i] = elem
		}
		return stringKeys(result)
	case map[string]any:
		result := make(map[string]any, len(v))
		for key, elem := range v {
			converted, err := stringKeys(elem)
			if err != nil {
				return nil, err
			}
			result[key] = converted
		}
		return result, nil
	case map[any]any:
		result := make(map[string]any, len(v))
		for key, elem := range v {
			s, ok := key.(string)
			if !ok {
				return nil, fmt.Errorf("key %v is not a string", key)
			}
			converted, err := stringKeys(elem)
			if err != nil {
				return nil, err
			}
			result[s] = converted
		}
		return result, nil
	}
	return value, nil
}

var (
	int64Type  = reflect.TypeFor[int64]()
	uint64Type = reflect.TypeFor[uint64]()
)

// decodeInteger rejects a fractional number for an integer field, which mapstructure would
// truncate.
func decodeInteger(_, t reflect.Type, data any) (any, error) {
	if t == uint64Type {
		return decodeUnsigned(data)
	}
	if t != int64Type {
		return data, nil
	}
	var f float64
	switch v := data.(type) {
	case float32:
		f = float64(v)
	case float64:
		f = v
	default:
		return data, nil
	}
	if f != math.Trunc(f) || f < math.MinInt64 || f >= math.MaxInt64 {
		return nil, fmt.Errorf("%v is not an integer", data)
	}
	return int64(f), nil
}

// decodeDuration parses a delay written as a duration string, such as "1.5s".
func decodeDuration(_, t reflect.Type, data any) (any, error) {
	if t != durationType {
		return data, nil
	}
	switch v := data.(type) {
	case time.Duration:
		return v, nil
	case string:
		return timestamp.ParseDurationDefaultSeconds(v)
	}
	return nil, errors.New("delay must be a duration string")
}

// decodeUnsigned rejects a negative or fractional number for an unsigned field, which mapstructure
// would wrap or truncate. A float64 cannot represent every uint64, so large values must be
// integers.
func decodeUnsigned(data any) (any, error) {
	switch v := data.(type) {
	case float32, float64:
		f := reflect.ValueOf(v).Float()
		if f != math.Trunc(f) || f < 0 || f >= math.MaxUint64 {
			return nil, fmt.Errorf("%v is not an unsigned integer", data)
		}
		return uint64(f), nil
	case int:
		if v < 0 {
			return nil, fmt.Errorf("%v is not an unsigned integer", data)
		}
	case int64:
		if v < 0 {
			return nil, fmt.Errorf("%v is not an unsigned integer", data)
		}
	default:
	}
	return data, nil
}
