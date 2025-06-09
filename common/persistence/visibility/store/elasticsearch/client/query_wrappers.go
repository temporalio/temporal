package client

// This file provides compatibility wrappers that are now deprecated.
// All new code should use the V8-prefixed query builders from query_builders.go
// These are kept for backward compatibility during the migration.

// Deprecated: Use V8BoolQuery from query_builders.go instead
type BoolQuery = V8BoolQuery

// Deprecated: Use NewV8BoolQuery from query_builders.go instead
func NewBoolQuery() *BoolQuery {
	return NewV8BoolQuery()
}

// Deprecated: Use V8TermQuery from query_builders.go instead
type TermQuery = V8TermQuery

// Deprecated: Use NewV8TermQuery from query_builders.go instead
func NewTermQuery(name string, value interface{}) *TermQuery {
	return NewV8TermQuery(name, value)
}

// Deprecated: Use V8RangeQuery from query_builders.go instead
type RangeQuery = V8RangeQuery

// Deprecated: Use NewV8RangeQuery from query_builders.go instead
func NewRangeQuery(name string) *RangeQuery {
	return NewV8RangeQuery(name)
}

// Deprecated: Use V8MatchQuery from query_builders.go instead
type MatchQuery = V8MatchQuery

// Deprecated: Use NewV8MatchQuery from query_builders.go instead
func NewMatchQuery(name string, text interface{}) *MatchQuery {
	return NewV8MatchQuery(name, text)
}

// Deprecated: Use V8ExistsQuery from query_builders.go instead
type ExistsQuery = V8ExistsQuery

// Deprecated: Use NewV8ExistsQuery from query_builders.go instead
func NewExistsQuery(name string) *ExistsQuery {
	return NewV8ExistsQuery(name)
}

// Deprecated: Use V8FieldSort from query_builders.go instead
type FieldSort = V8FieldSort

// Deprecated: Use NewV8FieldSort from query_builders.go instead
func NewFieldSort(fieldName string) *FieldSort {
	return NewV8FieldSort(fieldName)
}

// Deprecated: Use V8TermsAggregation from query_builders.go instead
type TermsAggregation = V8TermsAggregation

// Deprecated: Use NewV8TermsAggregation from query_builders.go instead
func NewTermsAggregation() *TermsAggregation {
	return NewV8TermsAggregation()
}

// PointInTime wrapper for compatibility
type PointInTimeWrapper struct {
	ID        string
	KeepAlive string
}

func NewPointInTimeWithKeepAlive(id, keepAlive string) *PointInTimeWrapper {
	return &PointInTimeWrapper{
		ID:        id,
		KeepAlive: keepAlive,
	}
}
