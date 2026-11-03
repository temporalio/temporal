package elasticsearch

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/olivere/elastic/v7"
	"github.com/stretchr/testify/require"
)

// errQuery is a query whose Source always fails, to exercise error propagation.
type errQuery struct{}

var _ elastic.Query = (*errQuery)(nil)

func (*errQuery) Source() (any, error) {
	return nil, errors.New("source failed")
}

func TestBoolQuery_NewBoolQuery(t *testing.T) {
	r := require.New(t)
	r.Equal(&boolQuery{}, newBoolQuery())
}

func TestBoolQuery_Clauses(t *testing.T) {
	r := require.New(t)

	q1 := elastic.NewTermQuery("Keyword01", "foo")
	q2 := elastic.NewTermQuery("Keyword01", "bar")
	q3 := elastic.NewTermQuery("Keyword02", "baz")

	q := newBoolQuery()
	// Each builder returns the same query so calls can be chained.
	r.Same(q, q.MustNot(q1))
	r.Same(q, q.Filter(q1))
	r.Same(q, q.Should(q1))

	// Successive calls append to the existing clauses.
	q.MustNot(q2, q3)
	q.Filter(q2, q3)
	q.Should(q2, q3)

	r.Equal(&boolQuery{
		mustNotClauses: []elastic.Query{q1, q2, q3},
		filterClauses:  []elastic.Query{q1, q2, q3},
		shouldClauses:  []elastic.Query{q1, q2, q3},
	}, q)

	// Calls without arguments are no-ops.
	q.MustNot()
	q.Filter()
	q.Should()
	r.Equal(&boolQuery{
		mustNotClauses: []elastic.Query{q1, q2, q3},
		filterClauses:  []elastic.Query{q1, q2, q3},
		shouldClauses:  []elastic.Query{q1, q2, q3},
	}, q)
}

func TestBoolQuery_ClausesOnEmptyQuery(t *testing.T) {
	r := require.New(t)
	q := newBoolQuery()
	q.MustNot()
	q.Filter()
	q.Should()
	r.Equal(&boolQuery{}, q)
}

func TestBoolQuery_Source(t *testing.T) {
	termFoo := elastic.NewTermQuery("Keyword01", "foo")
	termBar := elastic.NewTermQuery("Keyword01", "bar")

	testCases := []struct {
		name string
		in   *boolQuery
		out  string
	}{
		{
			name: "empty",
			in:   newBoolQuery(),
			out:  `{"bool":{}}`,
		},
		{
			name: "single must not clause",
			in:   newBoolQuery().MustNot(termFoo),
			out:  `{"bool":{"must_not":{"term":{"Keyword01":"foo"}}}}`,
		},
		{
			name: "multiple must not clauses",
			in:   newBoolQuery().MustNot(termFoo, termBar),
			out: `{"bool":{"must_not":[
				{"term":{"Keyword01":"foo"}},
				{"term":{"Keyword01":"bar"}}
			]}}`,
		},
		{
			name: "single filter clause",
			in:   newBoolQuery().Filter(termFoo),
			out:  `{"bool":{"filter":{"term":{"Keyword01":"foo"}}}}`,
		},
		{
			name: "multiple filter clauses",
			in:   newBoolQuery().Filter(termFoo, termBar),
			out: `{"bool":{"filter":[
				{"term":{"Keyword01":"foo"}},
				{"term":{"Keyword01":"bar"}}
			]}}`,
		},
		{
			// A lone should clause is equivalent to a filter clause, and a filter clause
			// doesn't need minimum_should_match.
			name: "single should clause becomes a filter clause",
			in:   newBoolQuery().Should(termFoo),
			out:  `{"bool":{"filter":{"term":{"Keyword01":"foo"}}}}`,
		},
		{
			name: "single should clause is appended after the filter clauses",
			in:   newBoolQuery().Filter(termFoo).Should(termBar),
			out: `{"bool":{"filter":[
				{"term":{"Keyword01":"foo"}},
				{"term":{"Keyword01":"bar"}}
			]}}`,
		},
		{
			// Elasticsearch defaults minimum_should_match to 0 when the bool query has filter
			// or must clauses, so it's always set explicitly.
			name: "minimum should match is always set with multiple should clauses",
			in:   newBoolQuery().Should(termFoo, termBar),
			out: `{"bool":{
				"should":[
					{"term":{"Keyword01":"foo"}},
					{"term":{"Keyword01":"bar"}}
				],
				"minimum_should_match":"1"
			}}`,
		},
		{
			name: "all clause types",
			in: newBoolQuery().
				MustNot(termFoo).
				Filter(termBar).
				Should(termFoo, termBar),
			out: `{"bool":{
				"must_not":{"term":{"Keyword01":"foo"}},
				"filter":{"term":{"Keyword01":"bar"}},
				"should":[
					{"term":{"Keyword01":"foo"}},
					{"term":{"Keyword01":"bar"}}
				],
				"minimum_should_match":"1"
			}}`,
		},
		{
			name: "nested bool query",
			in:   newBoolQuery().Filter(newBoolQuery().MustNot(termFoo)),
			out:  `{"bool":{"filter":{"bool":{"must_not":{"term":{"Keyword01":"foo"}}}}}}`,
		},
		{
			name: "single range query in filter clauses",
			in:   newBoolQuery().Filter(&rangeQuery{field: "Int01", lower: incl(int64(1))}),
			out: `{"bool":{"filter":{
				"range":{"Int01":{"gte":1}}
			}}}`,
		},
		{
			name: "range queries on same field are merged",
			in: newBoolQuery().Filter(
				&rangeQuery{field: "Int01", lower: incl(int64(1))},
				&rangeQuery{field: "Int01", upper: incl(int64(10))},
			),
			out: `{"bool":{"filter":{
				"range":{"Int01":{"gte":1,"lte":10}}
			}}}`,
		},
		{
			name: "range queries on same field are merged keeping the most restrictive bounds",
			in: newBoolQuery().Filter(
				&rangeQuery{field: "Int01", lower: incl(int64(1)), upper: incl(int64(10))},
				&rangeQuery{field: "Int01", lower: incl(int64(2)), upper: incl(int64(20))},
				&rangeQuery{field: "Int01", lower: incl(int64(0)), upper: incl(int64(5))},
			),
			out: `{"bool":{"filter":{
				"range":{"Int01":{"gte":2,"lte":5}}
			}}}`,
		},
		{
			name: "range queries on distinct fields are merged independently",
			in: newBoolQuery().Filter(
				&rangeQuery{field: "Int01", lower: incl(int64(1))},
				&rangeQuery{field: "Int02", lower: incl(int64(2))},
				&rangeQuery{field: "Int01", upper: incl(int64(10))},
				&rangeQuery{field: "Int02", upper: incl(int64(20))},
			),
			out: `{"bool":{"filter":[
				{"range":{"Int01":{"gte":1,"lte":10}}},
				{"range":{"Int02":{"gte":2,"lte":20}}}
			]}}`,
		},
		{
			name: "clauses keep their relative order and merged range queries stay in place",
			in: newBoolQuery().Filter(
				termFoo,
				&rangeQuery{field: "Int01", lower: incl(int64(1))},
				termBar,
				&rangeQuery{field: "Int01", upper: incl(int64(10))},
			),
			out: `{"bool":{"filter":[
				{"term":{"Keyword01":"foo"}},
				{"range":{"Int01":{"gte":1,"lte":10}}},
				{"term":{"Keyword01":"bar"}}
			]}}`,
		},
		{
			name: "unmergeable range queries are left as is",
			in: newBoolQuery().Filter(
				&rangeQuery{field: "Int01", lower: incl(int64(1))},
				&rangeQuery{field: "Int01", lower: incl("foo")},
			),
			out: `{"bool":{"filter":[
				{"range":{"Int01":{"gte":1}}},
				{"range":{"Int01":{"gte":"foo"}}}
			]}}`,
		},
		{
			name: "range queries in must not and should clauses are not merged",
			in: newBoolQuery().
				MustNot(
					&rangeQuery{field: "Int01", lower: incl(int64(1))},
					&rangeQuery{field: "Int01", upper: incl(int64(10))},
				).
				Should(
					&rangeQuery{field: "Int02", lower: incl(int64(1))},
					&rangeQuery{field: "Int02", upper: incl(int64(10))},
				),
			out: `{"bool":{
				"must_not":[
					{"range":{"Int01":{"gte":1}}},
					{"range":{"Int01":{"lte":10}}}
				],
				"should":[
					{"range":{"Int02":{"gte":1}}},
					{"range":{"Int02":{"lte":10}}}
				],
				"minimum_should_match":"1"
			}}`,
		},
		{
			name: "term queries on same field are merged into a terms query",
			in: newBoolQuery().Should(
				newTermQuery("Keyword01", "foo"),
				newTermQuery("Keyword01", "bar"),
			),
			out: `{"bool":{"filter":{
				"terms":{"Keyword01":["foo","bar"]}
			}}}`,
		},
		{
			name: "term and terms queries on same field are merged into a terms query",
			in: newBoolQuery().Should(
				newTermQuery("Keyword01", "foo"),
				newTermsQuery("Keyword01", "bar", "baz"),
				newTermQuery("Keyword01", "qux"),
			),
			out: `{"bool":{"filter":{
				"terms":{"Keyword01":["foo","bar","baz","qux"]}
			}}}`,
		},
		{
			name: "term queries on distinct fields are merged independently",
			in: newBoolQuery().Should(
				newTermQuery("Keyword01", "foo"),
				newTermQuery("Keyword02", "bar"),
				newTermQuery("Keyword01", "baz"),
				newTermQuery("Keyword02", "qux"),
			),
			out: `{"bool":{
				"should":[
					{"terms":{"Keyword01":["foo","baz"]}},
					{"terms":{"Keyword02":["bar","qux"]}}
				],
				"minimum_should_match":"1"
			}}`,
		},
		{
			name: "should clauses keep their relative order and merged term queries stay in place",
			in: newBoolQuery().Should(
				termFoo, // this is elastic.Term, not mergeable
				newTermQuery("Keyword02", "foo"),
				termBar, // this is elastic.Term, not mergeable
				newTermQuery("Keyword02", "bar"),
			),
			out: `{"bool":{
				"should":[
					{"term":{"Keyword01":"foo"}},
					{"terms":{"Keyword02":["foo","bar"]}},
					{"term":{"Keyword01":"bar"}}
				],
				"minimum_should_match":"1"
			}}`,
		},
		{
			// Only the term query wrappers are merged: elastic.TermQuery is left as is, even
			// on the same field.
			name: "elastic term queries are not merged",
			in:   newBoolQuery().Should(termFoo, termBar),
			out: `{"bool":{
				"should":[
					{"term":{"Keyword01":"foo"}},
					{"term":{"Keyword01":"bar"}}
				],
				"minimum_should_match":"1"
			}}`,
		},
		{
			name: "term queries in must not and filter clauses are not merged",
			in: newBoolQuery().
				MustNot(
					newTermQuery("Keyword01", "foo"),
					newTermQuery("Keyword01", "bar"),
				).
				Filter(
					newTermQuery("Keyword02", "foo"),
					newTermQuery("Keyword02", "bar"),
				),
			out: `{"bool":{
				"must_not":[
					{"term":{"Keyword01":"foo"}},
					{"term":{"Keyword01":"bar"}}
				],
				"filter":[
					{"term":{"Keyword02":"foo"}},
					{"term":{"Keyword02":"bar"}}
				]
			}}`,
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

func TestBoolQuery_SourceDoesNotModifyQuery(t *testing.T) {
	// Source merges the range queries into a new slice, so the query itself is left untouched
	// and calling Source twice returns the same result.
	r := require.New(t)
	termFoo := elastic.NewTermQuery("Keyword01", "foo")
	rq1 := &rangeQuery{field: "Int01", lower: incl(int64(1))}
	rq2 := &rangeQuery{field: "Int01", upper: incl(int64(10))}
	q := newBoolQuery().Filter(termFoo, rq1, rq2)

	src1, err := q.Source()
	r.NoError(err)
	b1, err := json.Marshal(src1)
	r.NoError(err)

	r.Equal([]elastic.Query{termFoo, rq1, rq2}, q.filterClauses)
	r.Equal(&rangeQuery{field: "Int01", lower: incl(int64(1))}, rq1)
	r.Equal(&rangeQuery{field: "Int01", upper: incl(int64(10))}, rq2)

	src2, err := q.Source()
	r.NoError(err)
	b2, err := json.Marshal(src2)
	r.NoError(err)

	r.JSONEq(string(b1), string(b2))
}

func TestBoolQuery_SourceError(t *testing.T) {
	testCases := []struct {
		name string
		in   *boolQuery
	}{
		{
			name: "must not clause",
			in:   newBoolQuery().MustNot(&errQuery{}),
		},
		{
			name: "filter clause",
			in:   newBoolQuery().Filter(&errQuery{}),
		},
		{
			name: "should clause",
			in:   newBoolQuery().Should(&errQuery{}),
		},
		{
			name: "nested clause",
			in:   newBoolQuery().Filter(newBoolQuery().Filter(&errQuery{})),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			src, err := tc.in.Source()
			r.ErrorContains(err, "source failed")
			r.Nil(src)
		})
	}
}

func TestBoolQuery_MergeFilterClauses(t *testing.T) {
	termFoo := elastic.NewTermQuery("Keyword01", "foo")
	termBar := elastic.NewTermQuery("Keyword01", "bar")

	testCases := []struct {
		name string
		in   []elastic.Query
		out  []elastic.Query
	}{
		{
			name: "no clauses",
			in:   nil,
			out:  nil,
		},
		{
			name: "no range queries",
			in:   []elastic.Query{termFoo, termBar},
			out:  []elastic.Query{termFoo, termBar},
		},
		{
			name: "single range query",
			in:   []elastic.Query{&rangeQuery{field: "Int01", lower: incl(int64(1))}},
			out:  []elastic.Query{&rangeQuery{field: "Int01", lower: incl(int64(1))}},
		},
		{
			name: "range queries on same field",
			in: []elastic.Query{
				&rangeQuery{field: "Int01", lower: excl(int64(1))},
				&rangeQuery{field: "Int01", upper: excl(int64(10))},
			},
			out: []elastic.Query{
				&rangeQuery{field: "Int01", lower: excl(int64(1)), upper: excl(int64(10))},
			},
		},
		{
			name: "range queries on distinct fields",
			in: []elastic.Query{
				&rangeQuery{field: "Int01", lower: incl(int64(1))},
				&rangeQuery{field: "Int02", lower: incl(int64(2))},
			},
			out: []elastic.Query{
				&rangeQuery{field: "Int01", lower: incl(int64(1))},
				&rangeQuery{field: "Int02", lower: incl(int64(2))},
			},
		},
		{
			name: "range queries on distinct fields interleaved with each other",
			in: []elastic.Query{
				&rangeQuery{field: "Int01", lower: incl(int64(1))},
				&rangeQuery{field: "Int02", lower: incl(int64(2))},
				&rangeQuery{field: "Int01", upper: incl(int64(10))},
				&rangeQuery{field: "Int02", upper: incl(int64(20))},
			},
			out: []elastic.Query{
				&rangeQuery{field: "Int01", lower: incl(int64(1)), upper: incl(int64(10))},
				&rangeQuery{field: "Int02", lower: incl(int64(2)), upper: incl(int64(20))},
			},
		},
		{
			name: "mixed range and non range queries keep their relative order",
			in: []elastic.Query{
				&rangeQuery{field: "Int01", lower: incl(int64(1))},
				termFoo,
				&rangeQuery{field: "Int01", upper: incl(int64(10))},
				termBar,
			},
			out: []elastic.Query{
				&rangeQuery{field: "Int01", lower: incl(int64(1)), upper: incl(int64(10))},
				termFoo,
				termBar,
			},
		},
		{
			name: "unmergeable range queries abort the merge",
			in: []elastic.Query{
				&rangeQuery{field: "Int01", lower: incl(int64(1))},
				&rangeQuery{field: "Int01", lower: incl("foo")},
			},
			out: []elastic.Query{
				&rangeQuery{field: "Int01", lower: incl(int64(1))},
				&rangeQuery{field: "Int01", lower: incl("foo")},
			},
		},
		{
			name: "unmergeable range queries abort the merge on other fields too",
			in: []elastic.Query{
				&rangeQuery{field: "Int02", lower: incl(int64(1))},
				&rangeQuery{field: "Int02", upper: incl(int64(10))},
				&rangeQuery{field: "Int01", lower: incl(int64(1))},
				&rangeQuery{field: "Int01", lower: incl("foo")},
			},
			out: []elastic.Query{
				&rangeQuery{field: "Int02", lower: incl(int64(1))},
				&rangeQuery{field: "Int02", upper: incl(int64(10))},
				&rangeQuery{field: "Int01", lower: incl(int64(1))},
				&rangeQuery{field: "Int01", lower: incl("foo")},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			q := newBoolQuery().Filter(tc.in...)
			r.Equal(tc.out, q.mergeFilterClauses())
			// Clauses are merged into a new slice, leaving the query untouched.
			r.Equal(tc.in, q.filterClauses)
		})
	}
}

func TestBoolQuery_MergeFilterClausesOnlyReadsFilterClauses(t *testing.T) {
	r := require.New(t)
	rq1 := &rangeQuery{field: "Int01", lower: incl(int64(1))}
	rq2 := &rangeQuery{field: "Int01", upper: incl(int64(10))}
	q := newBoolQuery().MustNot(rq1, rq2).Should(rq1, rq2)
	r.Empty(q.mergeFilterClauses())
	r.Equal([]elastic.Query{rq1, rq2}, q.mustNotClauses)
	r.Equal([]elastic.Query{rq1, rq2}, q.shouldClauses)
	r.Empty(q.filterClauses)
}

func TestBoolQuery_MergeShouldClauses(t *testing.T) {
	termFoo := elastic.NewTermQuery("Keyword01", "foo")
	termBar := elastic.NewTermQuery("Keyword01", "bar")

	testCases := []struct {
		name string
		in   []elastic.Query
		out  []elastic.Query
	}{
		{
			name: "no clauses",
			in:   nil,
			out:  nil,
		},
		{
			// Only the term query wrappers implement termsQueryIf: elastic.TermQuery is left
			// as is, even on the same field.
			name: "no term queries",
			in:   []elastic.Query{termFoo, termBar},
			out:  []elastic.Query{termFoo, termBar},
		},
		{
			name: "single term query",
			in:   []elastic.Query{newTermQuery("Keyword01", "foo")},
			out:  []elastic.Query{newTermQuery("Keyword01", "foo")},
		},
		{
			name: "single terms query",
			in:   []elastic.Query{newTermsQuery("Keyword01", "foo", "bar")},
			out:  []elastic.Query{newTermsQuery("Keyword01", "foo", "bar")},
		},
		{
			name: "term queries on same field",
			in: []elastic.Query{
				newTermQuery("Keyword01", "foo"),
				newTermQuery("Keyword01", "bar"),
			},
			out: []elastic.Query{newTermsQuery("Keyword01", "foo", "bar")},
		},
		{
			name: "term and terms queries on same field",
			in: []elastic.Query{
				newTermQuery("Keyword01", "foo"),
				newTermsQuery("Keyword01", "bar", "baz"),
				newTermQuery("Keyword01", "qux"),
			},
			out: []elastic.Query{newTermsQuery("Keyword01", "foo", "bar", "baz", "qux")},
		},
		{
			// Duplicate values are kept: Elasticsearch ignores them.
			name: "term queries with the same value",
			in: []elastic.Query{
				newTermQuery("Keyword01", "foo"),
				newTermQuery("Keyword01", "foo"),
			},
			out: []elastic.Query{newTermsQuery("Keyword01", "foo", "foo")},
		},
		{
			name: "term queries on distinct fields",
			in: []elastic.Query{
				newTermQuery("Keyword01", "foo"),
				newTermQuery("Keyword02", "bar"),
			},
			out: []elastic.Query{
				newTermQuery("Keyword01", "foo"),
				newTermQuery("Keyword02", "bar"),
			},
		},
		{
			name: "term queries on distinct fields interleaved with each other",
			in: []elastic.Query{
				newTermQuery("Keyword01", "foo"),
				newTermQuery("Keyword02", "bar"),
				newTermQuery("Keyword01", "baz"),
				newTermQuery("Keyword02", "qux"),
			},
			out: []elastic.Query{
				newTermsQuery("Keyword01", "foo", "baz"),
				newTermsQuery("Keyword02", "bar", "qux"),
			},
		},
		{
			name: "mixed term and non term queries keep their relative order",
			in: []elastic.Query{
				newTermQuery("Keyword02", "foo"),
				termFoo,
				newTermQuery("Keyword02", "bar"),
				termBar,
			},
			out: []elastic.Query{
				newTermsQuery("Keyword02", "foo", "bar"),
				termFoo,
				termBar,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := require.New(t)
			q := newBoolQuery().Should(tc.in...)
			r.Equal(tc.out, q.mergeShouldClauses())
			// Clauses are merged into a new slice, leaving the query untouched.
			r.Equal(tc.in, q.shouldClauses)
		})
	}
}

func TestBoolQuery_MergeShouldClausesOnlyReadsShouldClauses(t *testing.T) {
	r := require.New(t)
	tq1 := newTermQuery("Keyword01", "foo")
	tq2 := newTermQuery("Keyword01", "bar")
	q := newBoolQuery().MustNot(tq1, tq2).Filter(tq1, tq2)
	r.Empty(q.mergeShouldClauses())
	r.Equal([]elastic.Query{tq1, tq2}, q.mustNotClauses)
	r.Equal([]elastic.Query{tq1, tq2}, q.filterClauses)
	r.Empty(q.shouldClauses)
}
