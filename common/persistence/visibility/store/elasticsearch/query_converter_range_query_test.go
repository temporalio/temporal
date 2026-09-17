package elasticsearch

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// incl and excl build inclusive and exclusive bounds, keeping the test tables readable.
func incl(value any) *rangeQueryBound {
	return &rangeQueryBound{value: value, inclusive: true}
}

func excl(value any) *rangeQueryBound {
	return &rangeQueryBound{value: value, inclusive: false}
}

func TestRangeQuery_Source(t *testing.T) {
	testCases := []struct {
		name string
		in   *rangeQuery
		out  string
	}{
		{
			name: "no bounds",
			in:   &rangeQuery{field: "Keyword01"},
			out:  `{"range":{"Keyword01":{}}}`,
		},
		{
			name: "inclusive lower bound only",
			in:   &rangeQuery{field: "Keyword01", lower: incl("foo")},
			out:  `{"range":{"Keyword01":{"gte":"foo"}}}`,
		},
		{
			name: "exclusive lower bound only",
			in:   &rangeQuery{field: "Keyword01", lower: excl("foo")},
			out:  `{"range":{"Keyword01":{"gt":"foo"}}}`,
		},
		{
			name: "inclusive upper bound only",
			in:   &rangeQuery{field: "Keyword01", upper: incl("foo")},
			out:  `{"range":{"Keyword01":{"lte":"foo"}}}`,
		},
		{
			name: "exclusive upper bound only",
			in:   &rangeQuery{field: "Keyword01", upper: excl("foo")},
			out:  `{"range":{"Keyword01":{"lt":"foo"}}}`,
		},
		{
			name: "inclusive bounds",
			in:   &rangeQuery{field: "Int01", lower: incl(int64(1)), upper: incl(int64(10))},
			out:  `{"range":{"Int01":{"gte":1,"lte":10}}}`,
		},
		{
			name: "exclusive bounds",
			in:   &rangeQuery{field: "Int01", lower: excl(int64(1)), upper: excl(int64(10))},
			out:  `{"range":{"Int01":{"gt":1,"lt":10}}}`,
		},
		{
			name: "mixed inclusive and exclusive bounds",
			in:   &rangeQuery{field: "Double01", lower: excl(1.5), upper: incl(20.5)},
			out:  `{"range":{"Double01":{"gt":1.5,"lte":20.5}}}`,
		},
		{
			// Zero values are still valid bounds and must be part of the query.
			name: "zero valued bounds are not omitted",
			in:   &rangeQuery{field: "Int01", lower: excl(int64(0)), upper: incl("")},
			out:  `{"range":{"Int01":{"gt":0,"lte":""}}}`,
		},
		{
			// A bound holding a nil value is an absent bound, same as a nil bound.
			name: "bounds with nil value are omitted",
			in:   &rangeQuery{field: "Int01", lower: incl(nil), upper: excl(nil)},
			out:  `{"range":{"Int01":{}}}`,
		},
		{
			name: "bound with nil value is omitted next to a bound with a value",
			in:   &rangeQuery{field: "Int01", lower: incl(int64(1)), upper: incl(nil)},
			out:  `{"range":{"Int01":{"gte":1}}}`,
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
			a:    &rangeQuery{field: "Keyword01", lower: excl("foo")},
			b:    &rangeQuery{field: "Keyword02", lower: excl("foo")},
			ok:   false,
		},
		{
			name: "both empty",
			a:    &rangeQuery{field: "Keyword01"},
			b:    &rangeQuery{field: "Keyword01"},
			out:  &rangeQuery{field: "Keyword01"},
			ok:   true,
		},
		{
			name: "bounds on opposite sides are combined",
			a:    &rangeQuery{field: "Int01", lower: excl(int64(1))},
			b:    &rangeQuery{field: "Int01", upper: excl(int64(10))},
			out:  &rangeQuery{field: "Int01", lower: excl(int64(1)), upper: excl(int64(10))},
			ok:   true,
		},
		{
			name: "lower bounds keep the greater value",
			a:    &rangeQuery{field: "Int01", lower: incl(int64(1))},
			b:    &rangeQuery{field: "Int01", lower: incl(int64(5))},
			out:  &rangeQuery{field: "Int01", lower: incl(int64(5))},
			ok:   true,
		},
		{
			name: "upper bounds keep the lesser value",
			a:    &rangeQuery{field: "Int01", upper: incl(int64(20))},
			b:    &rangeQuery{field: "Int01", upper: incl(int64(10))},
			out:  &rangeQuery{field: "Int01", upper: incl(int64(10))},
			ok:   true,
		},
		{
			// The value decides first: the greater lower bound wins even when it is the
			// inclusive one.
			name: "greater lower bound wins over a lesser exclusive one",
			a:    &rangeQuery{field: "Int01", lower: excl(int64(1))},
			b:    &rangeQuery{field: "Int01", lower: incl(int64(5))},
			out:  &rangeQuery{field: "Int01", lower: incl(int64(5))},
			ok:   true,
		},
		{
			name: "lesser upper bound wins over a greater exclusive one",
			a:    &rangeQuery{field: "Int01", upper: excl(int64(10))},
			b:    &rangeQuery{field: "Int01", upper: incl(int64(5))},
			out:  &rangeQuery{field: "Int01", upper: incl(int64(5))},
			ok:   true,
		},
		{
			// On equal values, the exclusive bound is the more restrictive one.
			name: "equal lower bounds keep the exclusive one",
			a:    &rangeQuery{field: "Int01", lower: excl(int64(5))},
			b:    &rangeQuery{field: "Int01", lower: incl(int64(5))},
			out:  &rangeQuery{field: "Int01", lower: excl(int64(5))},
			ok:   true,
		},
		{
			name: "equal lower bounds keep the exclusive one regardless of the argument order",
			a:    &rangeQuery{field: "Int01", lower: incl(int64(5))},
			b:    &rangeQuery{field: "Int01", lower: excl(int64(5))},
			out:  &rangeQuery{field: "Int01", lower: excl(int64(5))},
			ok:   true,
		},
		{
			name: "equal upper bounds keep the exclusive one",
			a:    &rangeQuery{field: "Int01", upper: incl(int64(5))},
			b:    &rangeQuery{field: "Int01", upper: excl(int64(5))},
			out:  &rangeQuery{field: "Int01", upper: excl(int64(5))},
			ok:   true,
		},
		{
			name: "equal bounds",
			a:    &rangeQuery{field: "Keyword01", lower: incl("bar"), upper: incl("foo")},
			b:    &rangeQuery{field: "Keyword01", lower: incl("bar"), upper: incl("foo")},
			out:  &rangeQuery{field: "Keyword01", lower: incl("bar"), upper: incl("foo")},
			ok:   true,
		},
		{
			name: "string bounds",
			a:    &rangeQuery{field: "Keyword01", lower: incl("bar"), upper: incl("foo")},
			b:    &rangeQuery{field: "Keyword01", lower: incl("baz"), upper: incl("qux")},
			out:  &rangeQuery{field: "Keyword01", lower: incl("baz"), upper: incl("foo")},
			ok:   true,
		},
		{
			name: "mixed int64 and float64 bounds",
			a:    &rangeQuery{field: "Double01", lower: incl(int64(3)), upper: incl(10.5)},
			b:    &rangeQuery{field: "Double01", lower: incl(2.5), upper: incl(int64(20))},
			out:  &rangeQuery{field: "Double01", lower: incl(int64(3)), upper: incl(10.5)},
			ok:   true,
		},
		{
			// int64 and float64 bounds with the same value still fall into the equal values
			// case, so the exclusive one wins.
			name: "equal lower bounds with mixed int64 and float64",
			a:    &rangeQuery{field: "Double01", lower: excl(int64(5))},
			b:    &rangeQuery{field: "Double01", lower: incl(5.0)},
			out:  &rangeQuery{field: "Double01", lower: excl(int64(5))},
			ok:   true,
		},
		{
			name: "empty merged with bounds",
			a:    &rangeQuery{field: "Int01"},
			b:    &rangeQuery{field: "Int01", lower: incl(int64(1)), upper: excl(int64(10))},
			out:  &rangeQuery{field: "Int01", lower: incl(int64(1)), upper: excl(int64(10))},
			ok:   true,
		},
		{
			name: "bounds merged with empty",
			a:    &rangeQuery{field: "Int01", lower: incl(int64(1)), upper: excl(int64(10))},
			b:    &rangeQuery{field: "Int01"},
			out:  &rangeQuery{field: "Int01", lower: incl(int64(1)), upper: excl(int64(10))},
			ok:   true,
		},
		{
			// A bound holding a nil value is an absent bound, so the other side survives and
			// the values are never compared.
			name: "bounds with nil value are treated as absent",
			a:    &rangeQuery{field: "Int01", lower: incl(nil), upper: incl(int64(10))},
			b:    &rangeQuery{field: "Int01", lower: incl(int64(1)), upper: incl(nil)},
			out:  &rangeQuery{field: "Int01", lower: incl(int64(1)), upper: incl(int64(10))},
			ok:   true,
		},
		{
			name: "incompatible bound types",
			a:    &rangeQuery{field: "Keyword01", lower: incl("foo")},
			b:    &rangeQuery{field: "Keyword01", lower: incl(int64(1))},
			ok:   false,
		},
		{
			name: "unsupported bound type",
			a:    &rangeQuery{field: "Bool01", lower: incl(true)},
			b:    &rangeQuery{field: "Bool01", lower: incl(false)},
			ok:   false,
		},
		{
			name: "int is not a supported bound type",
			a:    &rangeQuery{field: "Int01", lower: incl(1)},
			b:    &rangeQuery{field: "Int01", lower: incl(2)},
			ok:   false,
		},
		{
			name: "failure on the lower bound fails the merge",
			a:    &rangeQuery{field: "Int01", lower: incl(int64(1)), upper: incl(int64(10))},
			b:    &rangeQuery{field: "Int01", lower: incl("foo"), upper: incl(int64(20))},
			ok:   false,
		},
		{
			name: "failure on the upper bound fails the merge",
			a:    &rangeQuery{field: "Int01", lower: incl(int64(1)), upper: incl(int64(10))},
			b:    &rangeQuery{field: "Int01", lower: incl(int64(2)), upper: incl("foo")},
			ok:   false,
		},
		// Datetime bounds reach the merge as RFC3339Nano strings, and RFC3339Nano is variable
		// width: it drops trailing zeros from the fractional seconds, and the whole fractional
		// part when it is zero. So "...:00Z" and "...:00.000000001Z" first differ at 'Z' (0x5A)
		// against '.' (0x2E), which puts the earlier instant later in byte order. Elasticsearch
		// compares these fields as dates, so the merge has to keep the bound Elasticsearch would
		// find more restrictive: the later instant for a lower bound, the earlier one for an
		// upper bound.
		{
			// Byte order agrees with time order when the seconds differ, so this case holds
			// either way. It guards the common path against a fix that only inspects fractions.
			name: "datetime lower bounds at different seconds keep the later instant",
			a:    &rangeQuery{field: "StartTime", lower: incl("2026-01-01T00:00:00Z")},
			b:    &rangeQuery{field: "StartTime", lower: incl("2026-02-01T00:00:00Z")},
			out:  &rangeQuery{field: "StartTime", lower: incl("2026-02-01T00:00:00Z")},
			ok:   true,
		},
		{
			name: "datetime lower bounds differing only in sub-second precision keep the later instant",
			a:    &rangeQuery{field: "StartTime", lower: incl("2026-01-01T00:00:00Z")},
			b:    &rangeQuery{field: "StartTime", lower: incl("2026-01-01T00:00:00.000000001Z")},
			out:  &rangeQuery{field: "StartTime", lower: incl("2026-01-01T00:00:00.000000001Z")},
			ok:   true,
		},
		{
			name: "datetime upper bounds differing only in sub-second precision keep the earlier instant",
			a:    &rangeQuery{field: "CloseTime", upper: incl("2026-01-01T00:00:00Z")},
			b:    &rangeQuery{field: "CloseTime", upper: incl("2026-01-01T00:00:00.5Z")},
			out:  &rangeQuery{field: "CloseTime", upper: incl("2026-01-01T00:00:00Z")},
			ok:   true,
		},
		{
			// The exclusive bound is the earlier instant here, so the later inclusive bound is
			// the more restrictive one and has to win despite the byte order.
			name: "exclusive datetime lower bound loses against a later inclusive bound",
			a:    &rangeQuery{field: "StartTime", lower: excl("2026-01-01T00:00:00Z")},
			b:    &rangeQuery{field: "StartTime", lower: incl("2026-01-01T00:00:00.000000001Z")},
			out:  &rangeQuery{field: "StartTime", lower: incl("2026-01-01T00:00:00.000000001Z")},
			ok:   true,
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
	a := &rangeQuery{field: "Int01", lower: incl(int64(1)), upper: incl(int64(10))}
	b := &rangeQuery{field: "Int01", lower: incl(int64(2)), upper: incl(int64(20))}
	out, ok := mergeRangeQueries(a, b)
	r.True(ok)
	r.Equal(&rangeQuery{field: "Int01", lower: incl(int64(2)), upper: incl(int64(10))}, out)
	r.Equal(&rangeQuery{field: "Int01", lower: incl(int64(1)), upper: incl(int64(10))}, a)
	r.Equal(&rangeQuery{field: "Int01", lower: incl(int64(2)), upper: incl(int64(20))}, b)
}

func TestRangeQuery_CompareBoundAndGet(t *testing.T) {
	maxFn := func(c int) bool { return c > 0 }
	minFn := func(c int) bool { return c < 0 }

	testCases := []struct {
		name string
		s    *rangeQueryBound
		t    *rangeQueryBound
		fn   func(c int) bool
		out  *rangeQueryBound
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
			t:    incl(int64(1)),
			fn:   maxFn,
			out:  incl(int64(1)),
			ok:   true,
		},
		{
			name: "t nil",
			s:    incl(int64(1)),
			t:    nil,
			fn:   maxFn,
			out:  incl(int64(1)),
			ok:   true,
		},
		{
			name: "max picks s",
			s:    incl(int64(2)),
			t:    incl(int64(1)),
			fn:   maxFn,
			out:  incl(int64(2)),
			ok:   true,
		},
		{
			name: "max picks t",
			s:    incl(int64(1)),
			t:    incl(int64(2)),
			fn:   maxFn,
			out:  incl(int64(2)),
			ok:   true,
		},
		{
			name: "min picks s",
			s:    incl(int64(1)),
			t:    incl(int64(2)),
			fn:   minFn,
			out:  incl(int64(1)),
			ok:   true,
		},
		{
			name: "min picks t",
			s:    incl(int64(2)),
			t:    incl(int64(1)),
			fn:   minFn,
			out:  incl(int64(1)),
			ok:   true,
		},
		{
			// On equal values fn is not called: the exclusive bound is the more restrictive
			// one on either side of the range.
			name: "equal values pick the exclusive s",
			s:    excl(int64(1)),
			t:    incl(int64(1)),
			fn:   maxFn,
			out:  excl(int64(1)),
			ok:   true,
		},
		{
			name: "equal values pick the exclusive t",
			s:    incl(int64(1)),
			t:    excl(int64(1)),
			fn:   maxFn,
			out:  excl(int64(1)),
			ok:   true,
		},
		{
			name: "equal values with both bounds inclusive",
			s:    incl(int64(1)),
			t:    incl(int64(1)),
			fn:   minFn,
			out:  incl(int64(1)),
			ok:   true,
		},
		{
			name: "equal values with both bounds exclusive",
			s:    excl(int64(1)),
			t:    excl(int64(1)),
			fn:   minFn,
			out:  excl(int64(1)),
			ok:   true,
		},
		{
			name: "strings",
			s:    incl("foo"),
			t:    incl("bar"),
			fn:   maxFn,
			out:  incl("foo"),
			ok:   true,
		},
		{
			name: "floats",
			s:    incl(1.5),
			t:    incl(2.5),
			fn:   maxFn,
			out:  incl(2.5),
			ok:   true,
		},
		{
			name: "int64 and float64",
			s:    incl(int64(3)),
			t:    incl(2.5),
			fn:   maxFn,
			out:  incl(int64(3)),
			ok:   true,
		},
		{
			name: "float64 and int64",
			s:    incl(2.5),
			t:    incl(int64(3)),
			fn:   maxFn,
			out:  incl(int64(3)),
			ok:   true,
		},
		{
			name: "int64 and float64 with the same value",
			s:    excl(int64(2)),
			t:    incl(2.0),
			fn:   maxFn,
			out:  excl(int64(2)),
			ok:   true,
		},
		{
			name: "string and int64",
			s:    incl("foo"),
			t:    incl(int64(1)),
			fn:   maxFn,
			ok:   false,
		},
		{
			name: "int64 and string",
			s:    incl(int64(1)),
			t:    incl("foo"),
			fn:   maxFn,
			ok:   false,
		},
		{
			name: "float64 and string",
			s:    incl(1.5),
			t:    incl("foo"),
			fn:   maxFn,
			ok:   false,
		},
		{
			name: "unsupported types",
			s:    incl(true),
			t:    incl(false),
			fn:   maxFn,
			ok:   false,
		},
		{
			name: "int is not supported",
			s:    incl(1),
			t:    incl(2),
			fn:   maxFn,
			ok:   false,
		},
		{
			// A bound holding a nil value is an absent bound, same as a nil bound.
			name: "s value nil",
			s:    incl(nil),
			t:    incl(int64(1)),
			fn:   maxFn,
			out:  incl(int64(1)),
			ok:   true,
		},
		{
			name: "t value nil",
			s:    incl(int64(1)),
			t:    excl(nil),
			fn:   maxFn,
			out:  incl(int64(1)),
			ok:   true,
		},
		{
			name: "both values nil",
			s:    incl(nil),
			t:    excl(nil),
			fn:   maxFn,
			out:  excl(nil),
			ok:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			out, ok := compareBoundAndGet(tc.s, tc.t, tc.fn)
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
			// RFC3339Nano is variable width, so these two would compare the other way around
			// as byte strings: they first differ at 'Z' (0x5A) against '.' (0x2E).
			name: "datetimes differing only in sub-second precision are compared as time",
			a:    "2026-01-01T00:00:00Z",
			b:    "2026-01-01T00:00:00.000000001Z",
			out:  -1,
			ok:   true,
		},
		{
			name: "datetimes are compared as time across offsets",
			a:    "2026-01-01T00:00:00Z",
			b:    "2025-12-31T19:00:00-05:00",
			out:  0,
			ok:   true,
		},
		{
			name: "equal datetimes written with different precision",
			a:    "2026-01-01T00:00:00Z",
			b:    "2026-01-01T00:00:00.000Z",
			out:  0,
			ok:   true,
		},
		{
			// Only one side is a datetime, so the comparison falls back to byte order.
			name: "datetime and non datetime string are compared lexicographically",
			a:    "2026-01-01T00:00:00Z",
			b:    "foo",
			out:  -1,
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

func TestRangeQuery_CompareTime(t *testing.T) {
	testCases := []struct {
		name string
		a    string
		b    string
		out  int
		ok   bool
	}{
		{
			name: "equal",
			a:    "2026-01-01T00:00:00Z",
			b:    "2026-01-01T00:00:00Z",
			out:  0,
			ok:   true,
		},
		{
			name: "before",
			a:    "2026-01-01T00:00:00Z",
			b:    "2026-02-01T00:00:00Z",
			out:  -1,
			ok:   true,
		},
		{
			name: "after",
			a:    "2026-02-01T00:00:00Z",
			b:    "2026-01-01T00:00:00Z",
			out:  1,
			ok:   true,
		},
		{
			// RFC3339Nano drops the fractional part when it is zero, so the fractions have
			// different widths and byte order would compare these the other way around.
			name: "differing only in sub-second precision",
			a:    "2026-01-01T00:00:00Z",
			b:    "2026-01-01T00:00:00.000000001Z",
			out:  -1,
			ok:   true,
		},
		{
			name: "fractions of different widths",
			a:    "2026-01-01T00:00:00.5Z",
			b:    "2026-01-01T00:00:00.25Z",
			out:  1,
			ok:   true,
		},
		{
			// RFC3339Nano drops trailing zeros from the fraction, so the same instant can be
			// written with different widths.
			name: "same instant written with different precision",
			a:    "2026-01-01T00:00:00Z",
			b:    "2026-01-01T00:00:00.000Z",
			out:  0,
			ok:   true,
		},
		{
			name: "same instant in different offsets",
			a:    "2026-01-01T00:00:00Z",
			b:    "2025-12-31T19:00:00-05:00",
			out:  0,
			ok:   true,
		},
		{
			name: "offsets are taken into account",
			a:    "2026-01-01T00:00:00-05:00",
			b:    "2026-01-01T00:00:00Z",
			out:  1,
			ok:   true,
		},
		{
			name: "nanosecond precision",
			a:    "2026-01-01T00:00:00.000000001Z",
			b:    "2026-01-01T00:00:00.000000002Z",
			out:  -1,
			ok:   true,
		},
		{
			name: "a is not a datetime",
			a:    "foo",
			b:    "2026-01-01T00:00:00Z",
			ok:   false,
		},
		{
			name: "b is not a datetime",
			a:    "2026-01-01T00:00:00Z",
			b:    "foo",
			ok:   false,
		},
		{
			name: "neither is a datetime",
			a:    "foo",
			b:    "bar",
			ok:   false,
		},
		{
			name: "empty strings",
			a:    "",
			b:    "",
			ok:   false,
		},
		{
			// Only RFC3339Nano is accepted: a date without a time of day doesn't parse.
			name: "date only is not a datetime",
			a:    "2026-01-01",
			b:    "2026-02-01",
			ok:   false,
		},
		{
			// The format Elasticsearch uses for these fields, but not the one the query
			// converter emits.
			name: "datetime without offset is not a datetime",
			a:    "2026-01-01T00:00:00",
			b:    "2026-02-01T00:00:00",
			ok:   false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			out, ok := compareTime(tc.a, tc.b)
			r.Equal(tc.ok, ok)
			r.Equal(tc.out, out)
		})
	}
}
