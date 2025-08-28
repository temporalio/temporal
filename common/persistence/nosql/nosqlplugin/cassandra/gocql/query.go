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
	dest ...interface{},
) (retError error) {
	defer func() { q.session.handleError(retError) }()

	return q.gocqlQuery.Scan(dest...)
}

func (q *query) ScanCAS(
	dest ...interface{},
) (_ bool, retError error) {
	defer func() { q.session.handleError(retError) }()

	return q.gocqlQuery.ScanCAS(dest...)
}

func (q *query) MapScan(
	m map[string]interface{},
) (retError error) {
	defer func() { q.session.handleError(retError) }()

	return q.gocqlQuery.MapScan(m)
}

func (q *query) MapScanCAS(
	dest map[string]interface{},
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
	return newQuery(q.session, q.gocqlQuery)
}

func (q *query) PageState(state []byte) Query {
	q.gocqlQuery.PageState(state)
	return newQuery(q.session, q.gocqlQuery)
}

func (q *query) Consistency(c Consistency) Query {
	q.gocqlQuery.Consistency(mustConvertConsistency(c))
	return newQuery(q.session, q.gocqlQuery)
}

func (q *query) WithTimestamp(timestamp int64) Query {
	q.gocqlQuery.WithTimestamp(timestamp)
	return newQuery(q.session, q.gocqlQuery)
}

func (q *query) WithContext(ctx context.Context) Query {
	q2 := q.gocqlQuery.WithContext(ctx)
	if q2 == nil {
		return nil
	}
	return newQuery(q.session, q2)
}

func (q *query) Bind(v ...interface{}) Query {
	q.gocqlQuery.Bind(v...)
	return newQuery(q.session, q.gocqlQuery)
}

func (q *query) Idempotent(value bool) Query {
	return newQuery(q.session, q.gocqlQuery.Idempotent(value))
}

func (q *query) SetSpeculativeExecutionPolicy(policy SpeculativeExecutionPolicy) Query {
	return newQuery(q.session, q.gocqlQuery.SetSpeculativeExecutionPolicy(policy))
}
