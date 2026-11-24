package elasticsearch

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTermQuery_Source(t *testing.T) {
	testCases := []struct {
		name string
		in   *termQuery
		out  string
	}{
		{
			name: "string value",
			in:   newTermQuery("Keyword01", "foo"),
			out:  `{"term":{"Keyword01":"foo"}}`,
		},
		{
			name: "int value",
			in:   newTermQuery("Int01", int64(123)),
			out:  `{"term":{"Int01":123}}`,
		},
		{
			name: "bool value",
			in:   newTermQuery("Bool01", true),
			out:  `{"term":{"Bool01":true}}`,
		},
		{
			// Zero values are valid values and must be part of the query.
			name: "zero value",
			in:   newTermQuery("Keyword01", ""),
			out:  `{"term":{"Keyword01":""}}`,
		},
		{
			name: "nil value",
			in:   newTermQuery("Keyword01", nil),
			out:  `{"term":{"Keyword01":null}}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			src, err := tc.in.Source()
			r.NoError(err)
			// Source must return a value that serializes to the term query body, not the
			// serialized bytes themselves (those would be re-encoded as a base64 string).
			b, err := json.Marshal(src)
			r.NoError(err)
			r.JSONEq(tc.out, string(b))
		})
	}
}

func TestTermQuery_FieldAndValues(t *testing.T) {
	r := require.New(t)
	q := newTermQuery("Keyword01", "foo")
	r.Equal("Keyword01", q.getField())
	r.Equal([]any{"foo"}, q.getValues())
}

func TestTermsQuery_Source(t *testing.T) {
	testCases := []struct {
		name string
		in   *termsQuery
		out  string
	}{
		{
			name: "single value",
			in:   newTermsQuery("Keyword01", "foo"),
			out:  `{"terms":{"Keyword01":["foo"]}}`,
		},
		{
			name: "multiple values",
			in:   newTermsQuery("Keyword01", "foo", "bar"),
			out:  `{"terms":{"Keyword01":["foo","bar"]}}`,
		},
		{
			name: "values of mixed types",
			in:   newTermsQuery("Keyword01", "foo", int64(123), true),
			out:  `{"terms":{"Keyword01":["foo",123,true]}}`,
		},
		{
			// A terms query with no values holds a nil slice, which serializes to null. The
			// query parser rejects an empty value list, so this can't come from a query.
			name: "no values",
			in:   newTermsQuery("Keyword01"),
			out:  `{"terms":{"Keyword01":null}}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			src, err := tc.in.Source()
			r.NoError(err)
			b, err := json.Marshal(src)
			r.NoError(err)
			r.JSONEq(tc.out, string(b))
		})
	}
}

func TestTermsQuery_FieldAndValues(t *testing.T) {
	r := require.New(t)
	q := newTermsQuery("Keyword01", "foo", "bar")
	r.Equal("Keyword01", q.getField())
	r.Equal([]any{"foo", "bar"}, q.getValues())
}

func TestTermQuery_MergeTermsQueries(t *testing.T) {
	testCases := []struct {
		name string
		a    termsQueryIf
		b    termsQueryIf
		out  *termsQuery
		ok   bool
	}{
		{
			name: "different fields",
			a:    newTermQuery("Keyword01", "foo"),
			b:    newTermQuery("Keyword02", "foo"),
			ok:   false,
		},
		{
			name: "two term queries",
			a:    newTermQuery("Keyword01", "foo"),
			b:    newTermQuery("Keyword01", "bar"),
			out:  newTermsQuery("Keyword01", "foo", "bar"),
			ok:   true,
		},
		{
			name: "term and terms queries",
			a:    newTermQuery("Keyword01", "foo"),
			b:    newTermsQuery("Keyword01", "bar", "baz"),
			out:  newTermsQuery("Keyword01", "foo", "bar", "baz"),
			ok:   true,
		},
		{
			name: "terms and term queries",
			a:    newTermsQuery("Keyword01", "foo", "bar"),
			b:    newTermQuery("Keyword01", "baz"),
			out:  newTermsQuery("Keyword01", "foo", "bar", "baz"),
			ok:   true,
		},
		{
			name: "two terms queries",
			a:    newTermsQuery("Keyword01", "foo", "bar"),
			b:    newTermsQuery("Keyword01", "baz", "qux"),
			out:  newTermsQuery("Keyword01", "foo", "bar", "baz", "qux"),
			ok:   true,
		},
		{
			// Duplicate values are kept: Elasticsearch ignores them.
			name: "duplicate values are kept",
			a:    newTermQuery("Keyword01", "foo"),
			b:    newTermsQuery("Keyword01", "foo", "bar"),
			out:  newTermsQuery("Keyword01", "foo", "foo", "bar"),
			ok:   true,
		},
		{
			name: "terms queries without values",
			a:    newTermsQuery("Keyword01"),
			b:    newTermsQuery("Keyword01"),
			out:  newTermsQuery("Keyword01"),
			ok:   true,
		},
		{
			name: "values of mixed types",
			a:    newTermQuery("Int01", int64(1)),
			b:    newTermQuery("Int01", 2.5),
			out:  newTermsQuery("Int01", int64(1), 2.5),
			ok:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			out, ok := mergeTermsQueries(tc.a, tc.b)
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

func TestTermQuery_MergeTermsQueriesDoesNotMutateInputs(t *testing.T) {
	r := require.New(t)
	// Extra capacity so that appending to a's values in place would be possible.
	values := make([]any, 1, 4)
	values[0] = "foo"
	a := &termsQuery{field: "Keyword01", values: values}
	b := newTermsQuery("Keyword01", "bar")

	out, ok := mergeTermsQueries[termsQueryIf](a, b)
	r.True(ok)
	r.Equal(newTermsQuery("Keyword01", "foo", "bar"), out)

	// Merging again must produce the same result: the first merge can't have written "bar"
	// into a's backing array.
	out, ok = mergeTermsQueries[termsQueryIf](a, newTermsQuery("Keyword01", "baz"))
	r.True(ok)
	r.Equal(newTermsQuery("Keyword01", "foo", "baz"), out)

	r.Equal(newTermsQuery("Keyword01", "foo"), a)
	r.Equal(newTermsQuery("Keyword01", "bar"), b)
}
