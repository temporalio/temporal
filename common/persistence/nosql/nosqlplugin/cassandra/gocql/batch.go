package gocql

import (
	"context"
	"fmt"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

type (
	Batch struct {
		session       *session
		gocqlBatch    *gocql.Batch
		tracerFactory TracerFactory
	}
)

// Definition of all BatchTypes
const (
	LoggedBatch BatchType = iota
	UnloggedBatch
	CounterBatch
)

func newBatch(
	session *session,
	gocqlBatch *gocql.Batch,
) *Batch {
	return &Batch{
		session:    session,
		gocqlBatch: gocqlBatch,
	}
}

// with returns a copy of b that wraps gocqlBatch and keeps b's session and tracer factory.
func (b *Batch) with(gocqlBatch *gocql.Batch) *Batch {
	return &Batch{
		session:       b.session,
		gocqlBatch:    gocqlBatch,
		tracerFactory: b.tracerFactory,
	}
}

func (b *Batch) Query(stmt string, args ...any) {
	b.gocqlBatch.Query(stmt, args...)
}

func (b *Batch) Exec(ctx context.Context) (retError error) {
	defer func() { b.session.handleError(retError) }()
	b.maybeTrace(ctx)
	return b.gocqlBatch.ExecContext(ctx)
}

func (b *Batch) MapExecCAS(ctx context.Context, dest map[string]any) (_ bool, _ Iter, retError error) {
	defer func() { b.session.handleError(retError) }()
	b.maybeTrace(ctx)
	applied, iter, err := b.gocqlBatch.MapExecCASContext(ctx, dest)
	if iter != nil {
		return applied, newIter(b.session, iter), err
	}
	return applied, nil, err
}

func (b *Batch) WithTimestamp(timestamp int64) *Batch {
	b.gocqlBatch.WithTimestamp(timestamp)
	return b.with(b.gocqlBatch)
}

func (b *Batch) WithTrace(tr TracerFactory) *Batch {
	b.tracerFactory = tr
	return b.with(b.gocqlBatch)
}

func mustConvertBatchType(batchType BatchType) gocql.BatchType {
	switch batchType {
	case LoggedBatch:
		return gocql.LoggedBatch
	case UnloggedBatch:
		return gocql.UnloggedBatch
	case CounterBatch:
		return gocql.CounterBatch
	default:
		panic(fmt.Sprintf("Unknown gocql BatchType: %v", batchType))
	}
}

func (b *Batch) maybeTrace(ctx context.Context) {
	if b.tracerFactory != nil {
		tr := b.tracerFactory.GetTracerForBatch(ctx, b.gocqlBatch)
		if tr != nil {
			b.gocqlBatch.Trace(tr)
		}
	}
}
