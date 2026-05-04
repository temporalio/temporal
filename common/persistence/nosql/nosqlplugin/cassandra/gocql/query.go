package gocql

import (
	"context"

	"github.com/gocql/gocql"
)

var _ Query = (*query)(nil)

type (
	query struct {
		session    *session
		gocqlQuery *gocql.Query
	}
)

func newQuery(
	session *session,
	gocqlQuery *gocql.Query,
) *query {
	return &query{
		session:    session,
		gocqlQuery: gocqlQuery,
	}
}

func (q *query) Exec() (retError error) {
	defer func() { q.session.handleError(retError) }()

	return q.gocqlQuery.Exec()
}

func (q *query) Scan(
	dest ...any,
) (retError error) {
	defer func() { q.session.handleError(retError) }()

	return q.gocqlQuery.Scan(dest...)
}

func (q *query) ScanCAS(
	dest ...any,
) (_ bool, retError error) {
	defer func() { q.session.handleError(retError) }()

	return q.gocqlQuery.ScanCAS(dest...)
}

func (q *query) MapScan(
	m map[string]any,
) (retError error) {
	defer func() { q.session.handleError(retError) }()

	return q.gocqlQuery.MapScan(m)
}

func (q *query) MapScanCAS(
	dest map[string]any,
) (_ bool, retError error) {
	defer func() { q.session.handleError(retError) }()

	return q.gocqlQuery.MapScanCAS(dest)
}

func (q *query) Iter() Iter {
	iter := q.gocqlQuery.Iter()
	return newIter(q.session, iter)
}

func (q *query) PageSize(n int) Query {
	q.gocqlQuery.PageSize(n)
	return q
}

func (q *query) PageState(state []byte) Query {
	q.gocqlQuery.PageState(state)
	return q
}

func (q *query) Consistency(c Consistency) Query {
	q.gocqlQuery.Consistency(mustConvertConsistency(c))
	return q
}

func (q *query) WithTimestamp(timestamp int64) Query {
	q.gocqlQuery.WithTimestamp(timestamp)
	return q
}

// WithContext mutates q in place and returns it. The caller must not retain
// a reference to q before this call and use it concurrently afterward.
func (q *query) WithContext(ctx context.Context) Query {
	q2 := q.gocqlQuery.WithContext(ctx)
	if q2 == nil {
		return nil
	}
	q.gocqlQuery = q2
	return q
}

func (q *query) Bind(v ...any) Query {
	q.gocqlQuery.Bind(v...)
	return q
}

func (q *query) Idempotent(value bool) Query {
	q.gocqlQuery.Idempotent(value)
	return q
}

func (q *query) SetSpeculativeExecutionPolicy(policy SpeculativeExecutionPolicy) Query {
	q.gocqlQuery.SetSpeculativeExecutionPolicy(policy)
	return q
}
