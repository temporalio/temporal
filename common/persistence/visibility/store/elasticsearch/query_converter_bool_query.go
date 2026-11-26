package elasticsearch

import (
	"github.com/olivere/elastic/v7"
)

// This is a wrapper for elastic.BoolQuery so we can access the clauses and be able to combine
// queries and avoid nesting queries when possible.
type boolQuery struct {
	mustNotClauses []elastic.Query
	filterClauses  []elastic.Query
	shouldClauses  []elastic.Query
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

func (q *boolQuery) Source() (any, error) {
	filterClauses := q.mergeFilterClauses()
	shouldClauses := q.mergeShouldClauses()
	if len(shouldClauses) == 1 {
		// If there's only one query, it can be moved to filterClauses.
		filterClauses = append(filterClauses, shouldClauses[0])
		shouldClauses = nil
	}
	minShouldMatch := ""
	if len(shouldClauses) > 0 {
		minShouldMatch = "1"
	}
	return elastic.NewBoolQuery().
		MustNot(q.mustNotClauses...).
		Filter(filterClauses...).
		Should(shouldClauses...).
		MinimumShouldMatch(minShouldMatch).
		Source()
}

func (q *boolQuery) mergeFilterClauses() []elastic.Query {
	if len(q.filterClauses) == 0 {
		return nil
	}

	queries := make([]elastic.Query, 0, len(q.filterClauses))
	rangeQueriesIndices := make(map[string]int)
	for _, query := range q.filterClauses {
		if rq, ok := query.(*rangeQuery); !ok {
			// Non-range queries are left as it is.
			queries = append(queries, query)
		} else if index, ok := rangeQueriesIndices[rq.field]; !ok {
			rangeQueriesIndices[rq.field] = len(queries)
			queries = append(queries, rq)
		} else {
			otherRQ := queries[index].(*rangeQuery) //nolint:revive // panic is not possible
			newRQ, ok := mergeRangeQueries(otherRQ, rq)
			if !ok {
				// Merge returns an error if trying to compare values with different types.
				// Thus, error should not be possible since validation already happened.
				// If something unexpected happens, ignore it and abort merging.
				return q.filterClauses
			}
			queries[index] = newRQ
		}
	}
	return queries
}

func (q *boolQuery) mergeShouldClauses() []elastic.Query {
	if len(q.shouldClauses) == 0 {
		return nil
	}

	queries := make([]elastic.Query, 0, len(q.shouldClauses))
	tqIndices := make(map[string]int)
	for _, query := range q.shouldClauses {
		if tq, ok := query.(termsQueryIf); !ok {
			queries = append(queries, query)
		} else if index, ok := tqIndices[tq.getField()]; !ok {
			tqIndices[tq.getField()] = len(queries)
			queries = append(queries, query)
		} else {
			otherTQ := queries[index].(termsQueryIf) //nolint:revive // panic is not possible
			newTQ, ok := mergeTermsQueries(otherTQ, tq)
			if !ok {
				// Merge returns an error if trying to merge queries with different fields.
				// Thus, error should not be possible since the queries are grouped by field.
				// If something unexpected happens, ignore it and abort merging.
				return q.shouldClauses
			}
			queries[index] = newTQ
		}
	}
	return queries
}
