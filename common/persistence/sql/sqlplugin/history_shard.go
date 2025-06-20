package sqlplugin

import (
	"context"
	"database/sql"
)

type (
	// ShardsRow represents a row in shards table
	ShardsRow struct {
		ShardID      int32
		RangeID      int64
		Data         []byte
		DataEncoding string
	}

	// ShardsFilter contains the column names within shards table that
	// can be used to filter results through a WHERE clause
	ShardsFilter struct {
		ShardID int32
	}

	// HistoryShard is the SQL persistence interface for history shards
	HistoryShard interface {
		InsertIntoShards(ctx context.Context, rows *ShardsRow) (sql.Result, error)
		UpdateShards(ctx context.Context, row *ShardsRow) (sql.Result, error)
		SelectFromShards(ctx context.Context, filter ShardsFilter) (*ShardsRow, error)
		ReadLockShards(ctx context.Context, filter ShardsFilter) (int64, error)
		WriteLockShards(ctx context.Context, filter ShardsFilter) (int64, error)
	}
)
