package gocql

import (
	"context"
	"fmt"

	"github.com/gocql/gocql"
)

type (
	Batch struct {
		session *session

		gocqlBatch *gocql.Batch
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

func (b *Batch) Query(stmt string, args ...any) {
	b.gocqlBatch.Query(stmt, args...)
}

// WithContext mutates b in place and returns it. The caller must not retain
// a reference to b before this call and use it concurrently afterward.
func (b *Batch) WithContext(ctx context.Context) *Batch {
	b.gocqlBatch = b.gocqlBatch.WithContext(ctx)
	return b
}

func (b *Batch) WithTimestamp(timestamp int64) *Batch {
	b.gocqlBatch.WithTimestamp(timestamp)
	return b
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
