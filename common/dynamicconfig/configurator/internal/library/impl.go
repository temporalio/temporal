package configurator

import (
	"context"

	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"go.temporal.io/server/common/dynamicconfig/configurator/types"
)

func StringValue(s string) ParsedValue {
	return ParsedValue{Str: s, Kind: KindString}
}

func IntegerValue(n int64) ParsedValue {
	return ParsedValue{Str: strconv.FormatInt(n, 10), Num: float64(n), Kind: KindInteger}
}

func FloatValue(f float64) ParsedValue {
	return ParsedValue{Str: strconv.FormatFloat(f, 'f', -1, 64), Num: f, Kind: KindFloat}
}

// CompareAny returns negative if constraint < v, 0 if equal, positive if constraint > v.
// Handles int, int64, float64, bool, and string natively; falls back to fmt.Sprintf for other types.
func (v ParsedValue) CompareAny(constraint any) int {
	switch cv := constraint.(type) {
	case int:
		return v.compareNum(float64(cv))
	case int64:
		return v.compareNum(float64(cv))
	case float64:
		return v.compareNum(cv)
	case bool:
		return strings.Compare(strconv.FormatBool(cv), v.Str)
	case string:
		if v.Kind == KindInteger || v.Kind == KindFloat {
			if cf, err := strconv.ParseFloat(cv, 64); err == nil {
				return v.compareNum(cf)
			}
		}
		return strings.Compare(cv, v.Str)
	default:
		return strings.Compare(fmt.Sprintf("%v", cv), v.Str)
	}
}

func (v ParsedValue) compareNum(cf float64) int {
	switch {
	case cf > v.Num:
		return 1
	case cf < v.Num:
		return -1
	default:
		return 0
	}
}

// parsedConfig is the runtime representation of a loaded config entry.
type parsedConfig[V any] struct {
	DefaultValue V
	Overrides    []*Condition[V]
}

// loadConfig parses the DSL expressions. Values are carried through untouched, so a parse
// error is the only thing that can fail here, and it fails at load rather than at Eval.
func loadConfig[V any](c types.Config[V]) (*parsedConfig[V], error) {
	out := &parsedConfig[V]{DefaultValue: c.DefaultValue}
	for _, v := range c.Overrides {
		parsedExpression, err := ParseExpression(v.MatchString)
		if err != nil {
			return nil, fmt.Errorf("couldn't parse %v: %w", v.MatchString, err)
		}
		out.Overrides = append(out.Overrides, &Condition[V]{
			Expression:  *parsedExpression,
			MatchResult: v.MatchResult,
		})
	}
	return out, nil
}

type configurator[V any] struct {
	loads map[string]*atomic.Pointer[parsedConfig[V]]
}

// New constructs a Configurator[V]. V is opaque to the library; see types.Config.
func New[V any]() *configurator[V] {
	return &configurator[V]{
		loads: map[string]*atomic.Pointer[parsedConfig[V]]{},
	}
}

func (c *configurator[V]) Load(key string, cfg types.Config[V]) error {
	parsed, err := loadConfig(cfg)
	if err != nil {
		return fmt.Errorf("couldn't load %q: %w", key, err)
	}

	existing, ok := c.loads[key]
	if !ok {
		p := &atomic.Pointer[parsedConfig[V]]{}
		p.Store(parsed)
		c.loads[key] = p
		return nil
	}
	existing.Store(parsed)
	return nil
}

func (c *configurator[V]) Eval(ctx context.Context, key string, constraints types.Lookup) (V, error) {
	var zero V
	ptr, ok := c.loads[key]
	if !ok {
		return zero, fmt.Errorf("no configured value for %q", key)
	}
	cfg := ptr.Load()

	for _, condition := range cfg.Overrides {
		matches, err := condition.Expression.Matches(constraints)
		if err != nil {
			return zero, err
		}
		if matches {
			return condition.MatchResult, nil
		}
	}

	return cfg.DefaultValue, nil
}

// ReferencedKeys returns the set of constraint keys the expressions for key can test.
//
// LOCAL ADDITION (not upstream): lets a caller tell, at load time, whether a config entry can
// be resolved from a fixed set of constraints — and therefore evaluated once up front —
// rather than needing evaluation on every read.
func (c *configurator[V]) ReferencedKeys(key string) ([]string, bool) {
	ptr, ok := c.loads[key]
	if !ok {
		return nil, false
	}
	seen := map[string]struct{}{}
	for _, condition := range ptr.Load().Overrides {
		collectKeys(&condition.Expression, seen)
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys, true
}

func collectKeys(e *Expression, seen map[string]struct{}) {
	if k := string(e.Key); k != "" {
		seen[k] = struct{}{}
	}
	for _, sub := range e.Subexpressions {
		collectKeys(sub, seen)
	}
}
