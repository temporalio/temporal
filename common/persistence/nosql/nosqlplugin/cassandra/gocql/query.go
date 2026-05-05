package gocql

import (
	"context"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
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

func (q *query) Exec(ctx context.Context) (retError error) {
	defer func() { q.session.handleError(retError) }()

	return q.gocqlQuery.ExecContext(ctx)
}

func (q *query) Scan(
	ctx context.Context,
	dest ...any,
) (retError error) {
	defer func() { q.session.handleError(retError) }()

	return q.gocqlQuery.ScanContext(ctx, dest...)
}

func (q *query) ScanCAS(
	ctx context.Context,
	dest ...any,
) (_ bool, retError error) {
	defer func() { q.session.handleError(retError) }()

	return q.gocqlQuery.ScanCASContext(ctx, dest...)
}

func (q *query) MapScan(
	ctx context.Context,
	m map[string]any,
) (retError error) {
	defer func() { q.session.handleError(retError) }()

	return q.gocqlQuery.MapScanContext(ctx, m)
}

func (q *query) MapScanCAS(
	ctx context.Context,
	dest map[string]any,
) (_ bool, retError error) {
	defer func() { q.session.handleError(retError) }()

	return q.gocqlQuery.MapScanCASContext(ctx, dest)
}

func (q *query) Iter(ctx context.Context) Iter {
	iter := q.gocqlQuery.IterContext(ctx)
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

func (q *query) Bind(v ...any) Query {
	q.gocqlQuery.Bind(v...)
	return newQuery(q.session, q.gocqlQuery)
}

func (q *query) Idempotent(value bool) Query {
	return newQuery(q.session, q.gocqlQuery.Idempotent(value))
}

func (q *query) SetSpeculativeExecutionPolicy(policy SpeculativeExecutionPolicy) Query {
	return newQuery(q.session, q.gocqlQuery.SetSpeculativeExecutionPolicy(policy))
}
