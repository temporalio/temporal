package elasticsearch

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRangeQuery_Source(t *testing.T) {
	testCases := []struct {
		name string
		in   *rangeQuery
		out  string
	}{
		{
			name: "no bounds",
			in:   &rangeQuery{Field: "Keyword01"},
			out:  `{"range":{"Keyword01":{}}}`,
		},
		{
			name: "gte only",
			in:   &rangeQuery{Field: "Keyword01", Gte: "foo"},
			out:  `{"range":{"Keyword01":{"gte":"foo"}}}`,
		},
		{
			name: "gt and lt",
			in:   &rangeQuery{Field: "Int01", Gt: int64(1), Lt: int64(10)},
			out:  `{"range":{"Int01":{"gt":1,"lt":10}}}`,
		},
		{
			name: "all bounds",
			in: &rangeQuery{
				Field: "Double01",
				Gt:    1.5,
				Gte:   2.5,
				Lt:    10.5,
				Lte:   20.5,
			},
			out: `{"range":{"Double01":{"gt":1.5,"gte":2.5,"lt":10.5,"lte":20.5}}}`,
		},
		{
			// The `omitempty` tags only omit nil bounds: zero values are still valid bounds
			// and must be part of the query.
			name: "zero valued bounds are not omitted",
			in: &rangeQuery{
				Field: "Int01",
				Gt:    int64(0),
				Gte:   0.0,
				Lt:    "",
				Lte:   false,
			},
			out: `{"range":{"Int01":{"gt":0,"gte":0,"lt":"","lte":false}}}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			src, err := tc.in.Source()
			r.NoError(err)
			// Source must return a value that serializes to the range query body, not the
			// serialized bytes themselves (those would be re-encoded as a base64 string).
			b, err := json.Marshal(src)
			r.NoError(err)
			r.JSONEq(tc.out, string(b))
		})
	}
}

func TestRangeQuery_MergeRangeQueries(t *testing.T) {
	testCases := []struct {
		name string
		a    *rangeQuery
		b    *rangeQuery
		out  *rangeQuery
		ok   bool
	}{
		{
			name: "different fields",
			a:    &rangeQuery{Field: "Keyword01", Gt: "foo"},
			b:    &rangeQuery{Field: "Keyword02", Gt: "foo"},
			ok:   false,
		},
		{
			name: "both empty",
			a:    &rangeQuery{Field: "Keyword01"},
			b:    &rangeQuery{Field: "Keyword01"},
			out:  &rangeQuery{Field: "Keyword01"},
			ok:   true,
		},
		{
			name: "disjoint bounds are combined",
			a:    &rangeQuery{Field: "Int01", Gt: int64(1)},
			b:    &rangeQuery{Field: "Int01", Lt: int64(10)},
			out:  &rangeQuery{Field: "Int01", Gt: int64(1), Lt: int64(10)},
			ok:   true,
		},
		{
			// Gt and Gte are merged separately (Gt: 5, Gte: 20) and then collapsed into the
			// most restrictive of the two.
			name: "lower bounds keep the most restrictive",
			a:    &rangeQuery{Field: "Int01", Gt: int64(1), Gte: int64(20)},
			b:    &rangeQuery{Field: "Int01", Gt: int64(5), Gte: int64(10)},
			out:  &rangeQuery{Field: "Int01", Gte: int64(20)},
			ok:   true,
		},
		{
			// Lt and Lte are merged separately (Lt: 1, Lte: 10) and then collapsed into the
			// most restrictive of the two.
			name: "upper bounds keep the most restrictive",
			a:    &rangeQuery{Field: "Int01", Lt: int64(1), Lte: int64(20)},
			b:    &rangeQuery{Field: "Int01", Lt: int64(5), Lte: int64(10)},
			out:  &rangeQuery{Field: "Int01", Lt: int64(1)},
			ok:   true,
		},
		{
			name: "gt is kept over a lower gte",
			a:    &rangeQuery{Field: "Int01", Gt: int64(10)},
			b:    &rangeQuery{Field: "Int01", Gte: int64(5)},
			out:  &rangeQuery{Field: "Int01", Gt: int64(10)},
			ok:   true,
		},
		{
			// Gt is more restrictive than Gte for the same value.
			name: "gt is kept over an equal gte",
			a:    &rangeQuery{Field: "Int01", Gt: int64(5)},
			b:    &rangeQuery{Field: "Int01", Gte: int64(5)},
			out:  &rangeQuery{Field: "Int01", Gt: int64(5)},
			ok:   true,
		},
		{
			name: "gte is kept over a lower gt",
			a:    &rangeQuery{Field: "Int01", Gt: int64(1)},
			b:    &rangeQuery{Field: "Int01", Gte: int64(5)},
			out:  &rangeQuery{Field: "Int01", Gte: int64(5)},
			ok:   true,
		},
		{
			name: "lt is kept over a greater lte",
			a:    &rangeQuery{Field: "Int01", Lt: int64(1)},
			b:    &rangeQuery{Field: "Int01", Lte: int64(5)},
			out:  &rangeQuery{Field: "Int01", Lt: int64(1)},
			ok:   true,
		},
		{
			// Lt is more restrictive than Lte for the same value.
			name: "lt is kept over an equal lte",
			a:    &rangeQuery{Field: "Int01", Lt: int64(5)},
			b:    &rangeQuery{Field: "Int01", Lte: int64(5)},
			out:  &rangeQuery{Field: "Int01", Lt: int64(5)},
			ok:   true,
		},
		{
			name: "lte is kept over a greater lt",
			a:    &rangeQuery{Field: "Int01", Lt: int64(10)},
			b:    &rangeQuery{Field: "Int01", Lte: int64(5)},
			out:  &rangeQuery{Field: "Int01", Lte: int64(5)},
			ok:   true,
		},
		{
			name: "lower bounds with mixed int64 and float64 are collapsed",
			a:    &rangeQuery{Field: "Double01", Gt: int64(5)},
			b:    &rangeQuery{Field: "Double01", Gte: 5.0},
			out:  &rangeQuery{Field: "Double01", Gt: int64(5)},
			ok:   true,
		},
		{
			name: "incompatible gt and gte types",
			a:    &rangeQuery{Field: "Int01", Gt: int64(1)},
			b:    &rangeQuery{Field: "Int01", Gte: "foo"},
			ok:   false,
		},
		{
			name: "incompatible lt and lte types",
			a:    &rangeQuery{Field: "Int01", Lt: int64(1)},
			b:    &rangeQuery{Field: "Int01", Lte: "foo"},
			ok:   false,
		},
		{
			name: "equal bounds",
			a:    &rangeQuery{Field: "Keyword01", Gte: "foo", Lte: "bar"},
			b:    &rangeQuery{Field: "Keyword01", Gte: "foo", Lte: "bar"},
			out:  &rangeQuery{Field: "Keyword01", Gte: "foo", Lte: "bar"},
			ok:   true,
		},
		{
			name: "string bounds",
			a:    &rangeQuery{Field: "Keyword01", Gte: "bar", Lte: "foo"},
			b:    &rangeQuery{Field: "Keyword01", Gte: "baz", Lte: "qux"},
			out:  &rangeQuery{Field: "Keyword01", Gte: "baz", Lte: "foo"},
			ok:   true,
		},
		{
			name: "mixed int64 and float64 bounds",
			a:    &rangeQuery{Field: "Double01", Gte: int64(3), Lte: 10.5},
			b:    &rangeQuery{Field: "Double01", Gte: 2.5, Lte: int64(20)},
			out:  &rangeQuery{Field: "Double01", Gte: int64(3), Lte: 10.5},
			ok:   true,
		},
		{
			// Merging with an empty query is still a merge: the redundant bounds of the
			// non-empty side are collapsed.
			name: "empty merged with bounds",
			a:    &rangeQuery{Field: "Int01"},
			b:    &rangeQuery{Field: "Int01", Gt: int64(1), Gte: int64(2), Lt: int64(9), Lte: int64(10)},
			out:  &rangeQuery{Field: "Int01", Gte: int64(2), Lt: int64(9)},
			ok:   true,
		},
		{
			name: "bounds merged with empty",
			a:    &rangeQuery{Field: "Int01", Gt: int64(1), Gte: int64(2), Lt: int64(9), Lte: int64(10)},
			b:    &rangeQuery{Field: "Int01"},
			out:  &rangeQuery{Field: "Int01", Gte: int64(2), Lt: int64(9)},
			ok:   true,
		},
		{
			name: "incompatible bound types",
			a:    &rangeQuery{Field: "Keyword01", Gte: "foo"},
			b:    &rangeQuery{Field: "Keyword01", Gte: int64(1)},
			ok:   false,
		},
		{
			name: "unsupported bound type",
			a:    &rangeQuery{Field: "Bool01", Gte: true},
			b:    &rangeQuery{Field: "Bool01", Gte: false},
			ok:   false,
		},
		{
			name: "int is not a supported bound type",
			a:    &rangeQuery{Field: "Int01", Gte: 1},
			b:    &rangeQuery{Field: "Int01", Gte: 2},
			ok:   false,
		},
		{
			name: "failure on any bound fails the merge",
			a:    &rangeQuery{Field: "Int01", Gte: int64(1), Lte: int64(10)},
			b:    &rangeQuery{Field: "Int01", Gte: int64(2), Lte: "foo"},
			ok:   false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			out, ok := mergeRangeQueries(tc.a, tc.b)
			if !tc.ok {
				r.False(ok)
				r.Nil(out)
				return
			}
			r.True(ok)
			r.Equal(tc.out, out)
		})
	}
}

func TestRangeQuery_MergeRangeQueriesDoesNotMutateInputs(t *testing.T) {
	r := require.New(t)
	a := &rangeQuery{Field: "Int01", Gte: int64(1), Lte: int64(10)}
	b := &rangeQuery{Field: "Int01", Gte: int64(2), Lte: int64(20)}
	out, ok := mergeRangeQueries(a, b)
	r.True(ok)
	r.Equal(&rangeQuery{Field: "Int01", Gte: int64(2), Lte: int64(10)}, out)
	r.Equal(&rangeQuery{Field: "Int01", Gte: int64(1), Lte: int64(10)}, a)
	r.Equal(&rangeQuery{Field: "Int01", Gte: int64(2), Lte: int64(20)}, b)
}

// Datetime bounds reach the merge as RFC3339Nano strings, and RFC3339Nano is variable width: it
// drops trailing zeros from the fractional seconds, and the whole fractional part when it is zero.
// So "...:00Z" and "...:00.000000001Z" first differ at 'Z' (0x5A) against '.' (0x2E), which puts
// the earlier instant later in byte order. Elasticsearch compares these fields as dates, so the
// merge has to keep the bound Elasticsearch would find more restrictive: the later instant for a
// lower bound, the earlier one for an upper bound.
func TestRangeQuery_MergeRangeQueriesDatetimeBounds(t *testing.T) {
	testCases := []struct {
		name string
		a    *rangeQuery
		b    *rangeQuery
		out  *rangeQuery
	}{
		{
			// Byte order agrees with time order when the seconds differ, so this case already
			// holds. It guards the common path against a fix that only inspects fractions.
			name: "lower bounds at different seconds keep the later instant",
			a:    &rangeQuery{Field: "StartTime", Gte: "2026-01-01T00:00:00Z"},
			b:    &rangeQuery{Field: "StartTime", Gte: "2026-02-01T00:00:00Z"},
			out:  &rangeQuery{Field: "StartTime", Gte: "2026-02-01T00:00:00Z"},
		},
		{
			name: "lower bounds differing only in sub-second precision keep the later instant",
			a:    &rangeQuery{Field: "StartTime", Gte: "2026-01-01T00:00:00Z"},
			b:    &rangeQuery{Field: "StartTime", Gte: "2026-01-01T00:00:00.000000001Z"},
			out:  &rangeQuery{Field: "StartTime", Gte: "2026-01-01T00:00:00.000000001Z"},
		},
		{
			name: "upper bounds differing only in sub-second precision keep the earlier instant",
			a:    &rangeQuery{Field: "CloseTime", Lte: "2026-01-01T00:00:00Z"},
			b:    &rangeQuery{Field: "CloseTime", Lte: "2026-01-01T00:00:00.5Z"},
			out:  &rangeQuery{Field: "CloseTime", Lte: "2026-01-01T00:00:00Z"},
		},
		{
			// The strict bound is the earlier instant here, so it is the redundant one and the
			// non-strict bound has to survive the collapse.
			name: "strict lower bound collapses against a later non-strict bound",
			a:    &rangeQuery{Field: "StartTime", Gt: "2026-01-01T00:00:00Z"},
			b:    &rangeQuery{Field: "StartTime", Gte: "2026-01-01T00:00:00.000000001Z"},
			out:  &rangeQuery{Field: "StartTime", Gte: "2026-01-01T00:00:00.000000001Z"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			out, ok := mergeRangeQueries(tc.a, tc.b)
			r.True(ok)
			r.Equal(tc.out, out)
		})
	}
}

func TestRangeQuery_CompareAnyAndGet(t *testing.T) {
	maxFn := func(c int) bool { return c > 0 }
	minFn := func(c int) bool { return c < 0 }

	testCases := []struct {
		name string
		s    any
		t    any
		fn   func(c int) bool
		out  any
		ok   bool
	}{
		{
			name: "both nil",
			s:    nil,
			t:    nil,
			fn:   maxFn,
			out:  nil,
			ok:   true,
		},
		{
			name: "s nil",
			s:    nil,
			t:    int64(1),
			fn:   maxFn,
			out:  int64(1),
			ok:   true,
		},
		{
			name: "t nil",
			s:    int64(1),
			t:    nil,
			fn:   maxFn,
			out:  int64(1),
			ok:   true,
		},
		{
			name: "max picks s",
			s:    int64(2),
			t:    int64(1),
			fn:   maxFn,
			out:  int64(2),
			ok:   true,
		},
		{
			name: "max picks t",
			s:    int64(1),
			t:    int64(2),
			fn:   maxFn,
			out:  int64(2),
			ok:   true,
		},
		{
			name: "min picks s",
			s:    int64(1),
			t:    int64(2),
			fn:   minFn,
			out:  int64(1),
			ok:   true,
		},
		{
			name: "min picks t",
			s:    int64(2),
			t:    int64(1),
			fn:   minFn,
			out:  int64(1),
			ok:   true,
		},
		{
			// When the values are equal, fn returns false and t is returned.
			name: "equal values",
			s:    int64(1),
			t:    int64(1),
			fn:   maxFn,
			out:  int64(1),
			ok:   true,
		},
		{
			name: "strings",
			s:    "foo",
			t:    "bar",
			fn:   maxFn,
			out:  "foo",
			ok:   true,
		},
		{
			name: "floats",
			s:    1.5,
			t:    2.5,
			fn:   maxFn,
			out:  2.5,
			ok:   true,
		},
		{
			name: "int64 and float64",
			s:    int64(3),
			t:    2.5,
			fn:   maxFn,
			out:  int64(3),
			ok:   true,
		},
		{
			name: "float64 and int64",
			s:    2.5,
			t:    int64(3),
			fn:   maxFn,
			out:  int64(3),
			ok:   true,
		},
		{
			name: "string and int64",
			s:    "foo",
			t:    int64(1),
			fn:   maxFn,
			ok:   false,
		},
		{
			name: "int64 and string",
			s:    int64(1),
			t:    "foo",
			fn:   maxFn,
			ok:   false,
		},
		{
			name: "float64 and string",
			s:    1.5,
			t:    "foo",
			fn:   maxFn,
			ok:   false,
		},
		{
			name: "unsupported types",
			s:    true,
			t:    false,
			fn:   maxFn,
			ok:   false,
		},
		{
			name: "int is not supported",
			s:    1,
			t:    2,
			fn:   maxFn,
			ok:   false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			out, ok := compareAnyAndGet(tc.s, tc.t, tc.fn)
			r.Equal(tc.ok, ok)
			r.Equal(tc.out, out)
		})
	}
}

func TestRangeQuery_CompareAny(t *testing.T) {
	testCases := []struct {
		name string
		a    any
		b    any
		out  int
		ok   bool
	}{
		{
			name: "equal int64",
			a:    int64(1),
			b:    int64(1),
			out:  0,
			ok:   true,
		},
		{
			name: "greater int64",
			a:    int64(2),
			b:    int64(1),
			out:  1,
			ok:   true,
		},
		{
			name: "lesser int64",
			a:    int64(1),
			b:    int64(2),
			out:  -1,
			ok:   true,
		},
		{
			name: "float64",
			a:    2.5,
			b:    1.5,
			out:  1,
			ok:   true,
		},
		{
			name: "int64 and float64 are compared as float64",
			a:    int64(2),
			b:    2.5,
			out:  -1,
			ok:   true,
		},
		{
			name: "float64 and int64 are compared as float64",
			a:    2.5,
			b:    int64(2),
			out:  1,
			ok:   true,
		},
		{
			name: "int64 and float64 with same value",
			a:    int64(2),
			b:    2.0,
			out:  0,
			ok:   true,
		},
		{
			name: "strings",
			a:    "bar",
			b:    "foo",
			out:  -1,
			ok:   true,
		},
		{
			name: "equal strings",
			a:    "foo",
			b:    "foo",
			out:  0,
			ok:   true,
		},
		{
			name: "string and int64 are not comparable",
			a:    "foo",
			b:    int64(1),
			ok:   false,
		},
		{
			name: "int64 and string are not comparable",
			a:    int64(1),
			b:    "foo",
			ok:   false,
		},
		{
			name: "float64 and string are not comparable",
			a:    1.5,
			b:    "foo",
			ok:   false,
		},
		{
			name: "bool is not a supported type",
			a:    true,
			b:    false,
			ok:   false,
		},
		{
			name: "int is not a supported type",
			a:    1,
			b:    2,
			ok:   false,
		},
		{
			name: "nil is not a supported type",
			a:    nil,
			b:    int64(1),
			ok:   false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			out, ok := compareAny(tc.a, tc.b)
			r.Equal(tc.ok, ok)
			r.Equal(tc.out, out)
		})
	}
}
