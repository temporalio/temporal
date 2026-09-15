package elasticsearch

import (
	"fmt"

	"github.com/olivere/elastic/v7"
)

// This is a wrapper for elastic.BoolQuery so we can access the clauses and be able to combine
// queries and avoid nesting queries when possible.
type boolQuery struct {
	mustNotClauses     []elastic.Query
	filterClauses      []elastic.Query
	shouldClauses      []elastic.Query
	minimumShouldMatch string
}

var _ elastic.Query = (*boolQuery)(nil)

func newBoolQuery() *boolQuery {
	return &boolQuery{}
}

func (q *boolQuery) MustNot(queries ...elastic.Query) *boolQuery {
	q.mustNotClauses = append(q.mustNotClauses, queries...)
	return q
}

func (q *boolQuery) Filter(filters ...elastic.Query) *boolQuery {
	q.filterClauses = append(q.filterClauses, filters...)
	return q
}

func (q *boolQuery) Should(queries ...elastic.Query) *boolQuery {
	q.shouldClauses = append(q.shouldClauses, queries...)
	return q
}

func (q *boolQuery) MinimumNumberShouldMatch(minimumNumberShouldMatch int) *boolQuery {
	q.minimumShouldMatch = fmt.Sprintf("%d", minimumNumberShouldMatch)
	return q
}

func (q *boolQuery) Source() (any, error) {
	// Merge multiple range queries on the same field into a single one.
	q.mergeRangeQueries()
	return elastic.NewBoolQuery().
		MustNot(q.mustNotClauses...).
		Filter(q.filterClauses...).
		Should(q.shouldClauses...).
		MinimumShouldMatch(q.minimumShouldMatch).
		Source()
}

func (q *boolQuery) mergeRangeQueries() {
	rqs := make(map[string]*rangeQuery)
	queries := make([]elastic.Query, 0, len(q.filterClauses))
	for _, q := range q.filterClauses {
		if rq, ok := q.(*rangeQuery); !ok {
			// Non-range queries are left as it is.
			queries = append(queries, q)
		} else if otherRQ, ok := rqs[rq.Field]; !ok {
			rqs[rq.Field] = rq
		} else {
			newRQ, ok := mergeRangeQueries(otherRQ, rq)
			if !ok {
				// Merge returns an error if trying to compare values with different types.
				// Thus, error should not be possible since validation already happened.
				// If something unexpected happens, ignore it and abort merging.
				return
			}
			rqs[rq.Field] = newRQ
		}
	}
	for _, rq := range rqs {
		queries = append(queries, rq)
	}
	q.filterClauses = queries
}
