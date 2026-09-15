package elasticsearch

import (
	"cmp"

	"github.com/olivere/elastic/v7"
)

// This is a wrapper for elastic.RangeQuery so we can access the clauses and be able to combine
// queries and avoid nesting queries when possible.
type rangeQuery struct {
	Field string `json:"-"`
	Gt    any    `json:"gt,omitempty"`
	Gte   any    `json:"gte,omitempty"`
	Lt    any    `json:"lt,omitempty"`
	Lte   any    `json:"lte,omitempty"`
}

var _ elastic.Query = (*rangeQuery)(nil)

func (q *rangeQuery) Source() (any, error) {
	return map[string]any{
		"range": map[string]any{
			q.Field: q,
		},
	}, nil
}

func mergeRangeQueries(a, b *rangeQuery) (*rangeQuery, bool) {
	if a.Field != b.Field {
		return nil, false
	}

	getGreater := func(s, t any) (any, bool) {
		return compareAnyAndGet(s, t, func(c int) bool { return c > 0 })
	}
	getLesser := func(s, t any) (any, bool) {
		return compareAnyAndGet(s, t, func(c int) bool { return c < 0 })
	}

	ret := &rangeQuery{
		Field: a.Field,
	}

	var ok bool
	ret.Gt, ok = getGreater(a.Gt, b.Gt)
	if !ok {
		return nil, false
	}
	ret.Gte, ok = getGreater(a.Gte, b.Gte)
	if !ok {
		return nil, false
	}
	ret.Lt, ok = getLesser(a.Lt, b.Lt)
	if !ok {
		return nil, false
	}
	ret.Lte, ok = getLesser(a.Lte, b.Lte)
	if !ok {
		return nil, false
	}

	if ret.Gt != nil && ret.Gte != nil {
		c, ok := compareAny(ret.Gt, ret.Gte)
		if !ok {
			return nil, false
		}
		if c >= 0 {
			ret.Gte = nil
		} else {
			ret.Gt = nil
		}
	}

	if ret.Lt != nil && ret.Lte != nil {
		c, ok := compareAny(ret.Lt, ret.Lte)
		if !ok {
			return nil, false
		}
		if c <= 0 {
			ret.Lte = nil
		} else {
			ret.Lt = nil
		}
	}

	return ret, true
}

// compareAnyAndGet compares two any type values.
// Accepted types are string, int64 and float64.
// If one of the values is nil, then it returns the other without calling fn.
// Particularly, if both values are nil, then it returns nil, true.
// Otherwise, it calls compareAny to compare the two values. The int output is
// used to call fn.
// If fn returns true, then the function return a. Otherwise, it returns b.
// The function return nil, false if the values are not comparable.
func compareAnyAndGet(a, b any, fn func(c int) bool) (any, bool) {
	if a == nil {
		return b, true
	}
	if b == nil {
		return a, true
	}
	c, ok := compareAny(a, b)
	if !ok {
		return nil, false
	}
	if fn(c) {
		return a, true
	}
	return b, true
}

// compareAny compares two any type values.
// Accepted types are string, int64 and float64.
// string type value is only comparable with another string type value.
// int64 and float64 are comparable between between them and casted as float64
// when necessary.
// If the values are not comparable, the function returns 0 and false.
// Otherwise, it returns the output of cmp.Compare and true.
func compareAny(a, b any) (int, bool) {
	switch typedA := a.(type) {
	case string:
		if typedB, ok := b.(string); ok {
			return cmp.Compare(typedA, typedB), true
		}
	case int64:
		if typedB, ok := b.(int64); ok {
			return cmp.Compare(typedA, typedB), true
		}
		if typedB, ok := b.(float64); ok {
			return cmp.Compare(float64(typedA), typedB), true
		}
	case float64:
		if typedB, ok := b.(int64); ok {
			return cmp.Compare(typedA, float64(typedB)), true
		}
		if typedB, ok := b.(float64); ok {
			return cmp.Compare(typedA, typedB), true
		}
	default:
	}
	return 0, false
}
