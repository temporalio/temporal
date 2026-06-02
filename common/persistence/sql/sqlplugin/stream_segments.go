package sqlplugin

import (
	"context"
	"database/sql"
)

type (
	StreamSegmentRow struct {
		ShardID      int32
		NamespaceID  []byte
		StreamID     string
		SegmentID    int64
		TxnID        int64
		FirstOffset  int64
		LastOffset   int64
		ItemCount    int32
		PayloadHash  []byte
		Data         []byte
		DataEncoding string
	}

	StreamSegmentRangeFilter struct {
		ShardID        int32
		NamespaceID    []byte
		StreamID       string
		StartOffset    int64
		EndOffset      int64
		CommittedTxnID int64
		Limit          int
	}

	StreamSegmentTxnFilter struct {
		ShardID     int32
		NamespaceID []byte
		StreamID    string
		TxnID       int64
	}

	StreamSegmentPrefixFilter struct {
		ShardID     int32
		NamespaceID []byte
		StreamID    string
		BelowOffset int64
	}

	StreamSegmentStreamFilter struct {
		ShardID     int32
		NamespaceID []byte
		StreamID    string
	}

	// StreamSegments is the SQL persistence interface for native stream
	// segment rows.
	StreamSegments interface {
		InsertIntoStreamSegments(ctx context.Context, row *StreamSegmentRow) (sql.Result, error)
		RangeSelectFromStreamSegments(ctx context.Context, filter StreamSegmentRangeFilter) ([]StreamSegmentRow, error)
		DeleteFromStreamSegmentsByTxn(ctx context.Context, filter StreamSegmentTxnFilter) (sql.Result, error)
		DeleteFromStreamSegmentsPrefix(ctx context.Context, filter StreamSegmentPrefixFilter) (sql.Result, error)
		DeleteFromStreamSegments(ctx context.Context, filter StreamSegmentStreamFilter) (sql.Result, error)
	}
)
