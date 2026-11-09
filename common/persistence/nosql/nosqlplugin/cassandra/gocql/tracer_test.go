package gocql

import (
	"context"
	"testing"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/stretchr/testify/require"
)

type recordingTracerFactory struct {
	queries []*gocql.Query
	batches []*gocql.Batch
}

var _ TracerFactory = (*recordingTracerFactory)(nil)

func (f *recordingTracerFactory) GetTracerForQuery(_ context.Context, q *gocql.Query) gocql.Tracer {
	f.queries = append(f.queries, q)
	return nil
}

func (f *recordingTracerFactory) GetTracerForBatch(_ context.Context, b *gocql.Batch) gocql.Tracer {
	f.batches = append(f.batches, b)
	return nil
}

func TestQueryBuildersKeepTracerFactory(t *testing.T) {
	factory := &recordingTracerFactory{}
	gocqlQuery := &gocql.Query{}

	q := newQuery(nil, gocqlQuery).
		WithTrace(factory).
		PageSize(10).
		PageState(nil).
		Consistency(One).
		WithTimestamp(1).
		Bind().
		Idempotent(true).
		SetSpeculativeExecutionPolicy(&gocql.SimpleSpeculativeExecution{})
	q.(*query).maybeTrace(t.Context())

	require.Equal(t, []*gocql.Query{gocqlQuery}, factory.queries)
}

func TestBatchBuildersKeepTracerFactory(t *testing.T) {
	factory := &recordingTracerFactory{}
	gocqlBatch := &gocql.Batch{}

	b := newBatch(nil, gocqlBatch).
		WithTrace(factory).
		WithTimestamp(1)
	b.maybeTrace(t.Context())

	require.Equal(t, []*gocql.Batch{gocqlBatch}, factory.batches)
}
