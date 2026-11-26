package elasticsearch

import (
	"cmp"
	"time"

	"github.com/olivere/elastic/v7"
)

type rangeQueryBound struct {
	value     any
	inclusive bool
}

// This is a wrapper for elastic.RangeQuery so we can access the clauses and be able to combine
// queries and avoid nesting queries when possible.
type rangeQuery struct {
	field string
	lower *rangeQueryBound
	upper *rangeQueryBound
}

var _ elastic.Query = (*rangeQuery)(nil)

func (q *rangeQuery) Source() (any, error) {
	rangeMap := make(map[string]any)
	if q.lower != nil && q.lower.value != nil {
		if q.lower.inclusive {
			rangeMap["gte"] = q.lower.value
		} else {
			rangeMap["gt"] = q.lower.value
		}
	}
	if q.upper != nil && q.upper.value != nil {
		if q.upper.inclusive {
			rangeMap["lte"] = q.upper.value
		} else {
			rangeMap["lt"] = q.upper.value
		}
	}
	return map[string]any{
		"range": map[string]any{
			q.field: rangeMap,
		},
	}, nil
}

func mergeRangeQueries(a, b *rangeQuery) (*rangeQuery, bool) {
	if a.field != b.field {
		return nil, false
	}

	ret := &rangeQuery{
		field: a.field,
	}

	var ok bool
	ret.lower, ok = compareBoundAndGet(a.lower, b.lower, func(c int) bool { return c > 0 })
	if !ok {
		return nil, false
	}
	ret.upper, ok = compareBoundAndGet(a.upper, b.upper, func(c int) bool { return c < 0 })
	if !ok {
		return nil, false
	}

	return ret, true
}

// compareBoundAndGet compares two bounds and returns the more restrictive one
// according to fn.
// If one of the bounds is nil, then it returns the other without calling fn.
// Particularly, if both values are nil, then it returns nil, true.
// Otherwise, it calls compareAny to compare the values in the bounds.
// The int output c is the output of cmp.Compare if they are comparable.
// If c != 0, then call fn. If fn returns true, then the function return s.
// Otherwise, it returns t.
// The function returns nil, false if the values are not comparable.
func compareBoundAndGet(s, t *rangeQueryBound, fn func(c int) bool) (*rangeQueryBound, bool) {
	if s == nil || s.value == nil {
		return t, true
	}
	if t == nil || t.value == nil {
		return s, true
	}
	c, ok := compareAny(s.value, t.value)
	if !ok {
		return nil, false
	}
	if c == 0 {
		if s.inclusive {
			return t, true
		}
		return s, true
	}
	if fn(c) {
		return s, true
	}
	return t, true
}

// compareAny compares two any type values.
// Accepted types are string, int64 and float64.
// string type value is only comparable with another string type value.
// If the string values are datetimes in time.RFC3339Nano format, compare them
// as time.Time instead of lexicografically.
// int64 and float64 are comparable between between them and casted as float64
// when necessary.
// If the values are not comparable, the function returns 0 and false.
// Otherwise, it returns the output of cmp.Compare and true.
func compareAny(a, b any) (int, bool) {
	switch typedA := a.(type) {
	case string:
		if typedB, ok := b.(string); ok {
			if c, ok := compareTime(typedA, typedB); ok {
				return c, true
			}
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

// compareTime compares two times in RFC3339Nano format.
// If any of the input fails to parse, return 0, false.
// Otherwise, return the value of time.Time.Compare, true.
func compareTime(a, b string) (int, bool) {
	timeA, err := time.Parse(time.RFC3339Nano, a)
	if err != nil {
		return 0, false
	}
	timeB, err := time.Parse(time.RFC3339Nano, b)
	if err != nil {
		return 0, false
	}
	return timeA.Compare(timeB), true
}
