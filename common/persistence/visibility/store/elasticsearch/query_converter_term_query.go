package elasticsearch

import (
	"slices"

	"github.com/olivere/elastic/v7"
)

type termQuery struct {
	field string
	value any
}

func newTermQuery(field string, value any) *termQuery {
	return &termQuery{
		field: field,
		value: value,
	}
}

var _ elastic.Query = (*termQuery)(nil)

func (q *termQuery) Source() (any, error) {
	return map[string]any{
		"term": map[string]any{
			q.field: q.value,
		},
	}, nil
}

func (q *termQuery) getField() string {
	return q.field
}

func (q *termQuery) getValues() []any {
	return []any{q.value}
}

type termsQuery struct {
	field  string
	values []any
}

func newTermsQuery(field string, values ...any) *termsQuery {
	return &termsQuery{
		field:  field,
		values: values,
	}
}

var _ elastic.Query = (*termsQuery)(nil)

func (q *termsQuery) Source() (any, error) {
	return map[string]any{
		"terms": map[string]any{
			q.field: q.values,
		},
	}, nil
}

func (q *termsQuery) getField() string {
	return q.field
}

func (q *termsQuery) getValues() []any {
	return q.values
}

type termsQueryIf interface {
	getField() string
	getValues() []any
}

func mergeTermsQueries[T termsQueryIf](a, b T) (*termsQuery, bool) {
	if a.getField() != b.getField() {
		return nil, false
	}
	return &termsQuery{
		field: a.getField(),
		// Concat instead of append to not modify the values of the input queries.
		values: slices.Concat(a.getValues(), b.getValues()),
	}, true
}
