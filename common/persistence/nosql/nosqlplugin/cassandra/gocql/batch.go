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

func (b *Batch) Query(stmt string, args ...interface{}) {
	b.gocqlBatch.Query(stmt, args...)
}

func (b *Batch) WithContext(ctx context.Context) *Batch {
	return newBatch(b.session, b.gocqlBatch.WithContext(ctx))
}

func (b *Batch) WithTimestamp(timestamp int64) *Batch {
	b.gocqlBatch.WithTimestamp(timestamp)
	return newBatch(b.session, b.gocqlBatch)
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
