package client

// Native query builders for go-elasticsearch v8 that generate JSON directly
// These are native implementations for the go-elasticsearch v8 client.

// V8BoolQuery represents a bool query that can combine multiple queries
type V8BoolQuery struct {
	mustQueries        []Query
	filterQueries      []Query
	shouldQueries      []Query
	mustNotQueries     []Query
	minimumShouldMatch interface{}
}

// NewV8BoolQuery creates a new boolean query
func NewV8BoolQuery() *V8BoolQuery {
	return &V8BoolQuery{}
}

// Must adds queries that must match
func (q *V8BoolQuery) Must(queries ...Query) *V8BoolQuery {
	q.mustQueries = append(q.mustQueries, queries...)
	return q
}

// Filter adds queries that must match but don't contribute to scoring
func (q *V8BoolQuery) Filter(queries ...Query) *V8BoolQuery {
	q.filterQueries = append(q.filterQueries, queries...)
	return q
}

// Should adds queries that should match (optional)
func (q *V8BoolQuery) Should(queries ...Query) *V8BoolQuery {
	q.shouldQueries = append(q.shouldQueries, queries...)
	return q
}

// MustNot adds queries that must not match
func (q *V8BoolQuery) MustNot(queries ...Query) *V8BoolQuery {
	q.mustNotQueries = append(q.mustNotQueries, queries...)
	return q
}

// MinimumNumberShouldMatch sets the minimum number of should clauses that must match
func (q *V8BoolQuery) MinimumNumberShouldMatch(min interface{}) *V8BoolQuery {
	q.minimumShouldMatch = min
	return q
}

// Source generates the JSON source for this query
func (q *V8BoolQuery) Source() (interface{}, error) {
	boolQuery := make(map[string]interface{})

	if len(q.mustQueries) > 0 {
		must, err := queriesToSource(q.mustQueries)
		if err != nil {
			return nil, err
		}
		boolQuery["must"] = must
	}

	if len(q.filterQueries) > 0 {
		filter, err := queriesToSource(q.filterQueries)
		if err != nil {
			return nil, err
		}
		boolQuery["filter"] = filter
	}

	if len(q.shouldQueries) > 0 {
		should, err := queriesToSource(q.shouldQueries)
		if err != nil {
			return nil, err
		}
		boolQuery["should"] = should
	}

	if len(q.mustNotQueries) > 0 {
		mustNot, err := queriesToSource(q.mustNotQueries)
		if err != nil {
			return nil, err
		}
		boolQuery["must_not"] = mustNot
	}

	if q.minimumShouldMatch != nil {
		boolQuery["minimum_should_match"] = q.minimumShouldMatch
	}

	return map[string]interface{}{
		"bool": boolQuery,
	}, nil
}

// V8TermQuery represents a term query for exact matches
type V8TermQuery struct {
	field string
	value interface{}
}

// NewV8TermQuery creates a new term query
func NewV8TermQuery(field string, value interface{}) *V8TermQuery {
	return &V8TermQuery{
		field: field,
		value: value,
	}
}

// Source generates the JSON source for this query
func (q *V8TermQuery) Source() (interface{}, error) {
	return map[string]interface{}{
		"term": map[string]interface{}{
			q.field: q.value,
		},
	}, nil
}

// V8RangeQuery represents a range query for numeric/date ranges
type V8RangeQuery struct {
	field string
	gte   interface{}
	lte   interface{}
	gt    interface{}
	lt    interface{}
}

// NewV8RangeQuery creates a new range query
func NewV8RangeQuery(field string) *V8RangeQuery {
	return &V8RangeQuery{field: field}
}

// Gte sets the greater than or equal to value
func (q *V8RangeQuery) Gte(value interface{}) *V8RangeQuery {
	q.gte = value
	return q
}

// Lte sets the less than or equal to value
func (q *V8RangeQuery) Lte(value interface{}) *V8RangeQuery {
	q.lte = value
	return q
}

// Gt sets the greater than value
func (q *V8RangeQuery) Gt(value interface{}) *V8RangeQuery {
	q.gt = value
	return q
}

// Lt sets the less than value
func (q *V8RangeQuery) Lt(value interface{}) *V8RangeQuery {
	q.lt = value
	return q
}

// Source generates the JSON source for this query
func (q *V8RangeQuery) Source() (interface{}, error) {
	rangeQuery := make(map[string]interface{})

	// Use v8-style range query format
	if q.gte != nil {
		rangeQuery["gte"] = q.gte
	}
	if q.lte != nil {
		rangeQuery["lte"] = q.lte
	}
	if q.gt != nil {
		rangeQuery["gt"] = q.gt
	}
	if q.lt != nil {
		rangeQuery["lt"] = q.lt
	}

	return map[string]interface{}{
		"range": map[string]interface{}{
			q.field: rangeQuery,
		},
	}, nil
}

// V8MatchQuery represents a match query for full-text search
type V8MatchQuery struct {
	field string
	text  interface{}
}

// NewV8MatchQuery creates a new match query
func NewV8MatchQuery(field string, text interface{}) *V8MatchQuery {
	return &V8MatchQuery{
		field: field,
		text:  text,
	}
}

// Source generates the JSON source for this query
func (q *V8MatchQuery) Source() (interface{}, error) {
	return map[string]interface{}{
		"match": map[string]interface{}{
			q.field: q.text,
		},
	}, nil
}

// V8ExistsQuery represents an exists query to check if a field exists
type V8ExistsQuery struct {
	field string
}

// NewV8ExistsQuery creates a new exists query
func NewV8ExistsQuery(field string) *V8ExistsQuery {
	return &V8ExistsQuery{field: field}
}

// Source generates the JSON source for this query
func (q *V8ExistsQuery) Source() (interface{}, error) {
	return map[string]interface{}{
		"exists": map[string]interface{}{
			"field": q.field,
		},
	}, nil
}

// V8FieldSort represents a field-based sort
type V8FieldSort struct {
	field   string
	order   string
	missing string
}

// NewV8FieldSort creates a new field sort
func NewV8FieldSort(field string) *V8FieldSort {
	return &V8FieldSort{field: field}
}

// Order sets the sort order (true for ascending, false for descending)
func (s *V8FieldSort) Order(ascending bool) *V8FieldSort {
	if ascending {
		s.order = "asc"
	} else {
		s.order = "desc"
	}
	return s
}

// Asc sets ascending order
func (s *V8FieldSort) Asc() *V8FieldSort {
	s.order = "asc"
	return s
}

// Desc sets descending order
func (s *V8FieldSort) Desc() *V8FieldSort {
	s.order = "desc"
	return s
}

// Missing sets how to handle missing values
func (s *V8FieldSort) Missing(value string) *V8FieldSort {
	s.missing = value
	return s
}

// Source generates the JSON source for this sorter
func (s *V8FieldSort) Source() (interface{}, error) {
	sort := make(map[string]interface{})
	fieldSort := make(map[string]interface{})

	if s.order != "" {
		fieldSort["order"] = s.order
	}

	if s.missing != "" {
		fieldSort["missing"] = s.missing
	}

	if len(fieldSort) > 0 {
		sort[s.field] = fieldSort
	} else {
		sort[s.field] = map[string]interface{}{}
	}

	return sort, nil
}

// V8TermsAggregation represents a terms aggregation
type V8TermsAggregation struct {
	field   string
	size    *int
	subAggs map[string]Aggregation
}

// NewV8TermsAggregation creates a new terms aggregation
func NewV8TermsAggregation() *V8TermsAggregation {
	return &V8TermsAggregation{
		subAggs: make(map[string]Aggregation),
	}
}

// Field sets the field to aggregate on
func (a *V8TermsAggregation) Field(field string) *V8TermsAggregation {
	a.field = field
	return a
}

// Size sets the number of terms to return
func (a *V8TermsAggregation) Size(size int) *V8TermsAggregation {
	a.size = &size
	return a
}

// SubAggregation adds a sub-aggregation
func (a *V8TermsAggregation) SubAggregation(name string, subAgg Aggregation) *V8TermsAggregation {
	a.subAggs[name] = subAgg
	return a
}

// Source generates the JSON source for this aggregation
func (a *V8TermsAggregation) Source() (interface{}, error) {
	terms := make(map[string]interface{})
	terms["field"] = a.field

	if a.size != nil {
		terms["size"] = *a.size
	}

	agg := map[string]interface{}{
		"terms": terms,
	}

	if len(a.subAggs) > 0 {
		subAggs := make(map[string]interface{})
		for name, subAgg := range a.subAggs {
			src, err := subAgg.Source()
			if err != nil {
				return nil, err
			}
			subAggs[name] = src
		}
		agg["aggs"] = subAggs
	}

	return agg, nil
}

// V8TermsQuery represents a terms query for multiple values
type V8TermsQuery struct {
	field  string
	values []interface{}
}

// NewV8TermsQuery creates a new terms query
func NewV8TermsQuery(field string, values ...interface{}) *V8TermsQuery {
	return &V8TermsQuery{
		field:  field,
		values: values,
	}
}

// Source generates the JSON source for this query
func (q *V8TermsQuery) Source() (interface{}, error) {
	return map[string]interface{}{
		"terms": map[string]interface{}{
			q.field: q.values,
		},
	}, nil
}

// V8PrefixQuery represents a prefix query
type V8PrefixQuery struct {
	field string
	value string
}

// NewV8PrefixQuery creates a new prefix query
func NewV8PrefixQuery(field string, value string) *V8PrefixQuery {
	return &V8PrefixQuery{
		field: field,
		value: value,
	}
}

// Source generates the JSON source for this query
func (q *V8PrefixQuery) Source() (interface{}, error) {
	return map[string]interface{}{
		"prefix": map[string]interface{}{
			q.field: q.value,
		},
	}, nil
}

// V8DocSort represents a _doc sort (for scrolling)
type V8DocSort struct{}

// NewV8DocSort creates a new _doc sort
func NewV8DocSort() *V8DocSort {
	return &V8DocSort{}
}

// Source generates the JSON source for this sorter
func (s *V8DocSort) Source() (interface{}, error) {
	return "_doc", nil
}

// Helper functions

// queriesToSource converts a slice of queries to their JSON sources
func queriesToSource(queries []Query) ([]interface{}, error) {
	sources := make([]interface{}, len(queries))
	for i, q := range queries {
		src, err := q.Source()
		if err != nil {
			return nil, err
		}
		sources[i] = src
	}
	return sources, nil
}
