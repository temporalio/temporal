package sqlplugin

import (
	"context"
	"database/sql"

	"go.temporal.io/server/common/primitives"
)

type (
	// HistoryNodeRow represents a row in history_node table
	HistoryNodeRow struct {
		ShardID      int32
		TreeID       primitives.UUID
		BranchID     primitives.UUID
		NodeID       int64
		PrevTxnID    int64
		TxnID        int64
		Data         []byte
		DataEncoding string
	}

	// HistoryNodeSelectFilter contains the column names within history_node table that
	// can be used to filter results through a WHERE clause
	HistoryNodeSelectFilter struct {
		ShardID      int32
		TreeID       primitives.UUID
		BranchID     primitives.UUID
		MinNodeID    int64
		MinTxnID     int64
		MaxNodeID    int64
		MaxTxnID     int64
		PageSize     int
		MetadataOnly bool
		ReverseOrder bool
	}

	// HistoryNodeDeleteFilter contains the column names within history_node table that
	// can be used to filter results through a WHERE clause
	HistoryNodeDeleteFilter struct {
		ShardID   int32
		TreeID    primitives.UUID
		BranchID  primitives.UUID
		MinNodeID int64
	}

	// HistoryNode is the SQL persistence interface for history nodes
	HistoryNode interface {
		InsertIntoHistoryNode(ctx context.Context, row *HistoryNodeRow) (sql.Result, error)
		DeleteFromHistoryNode(ctx context.Context, row *HistoryNodeRow) (sql.Result, error)
		RangeSelectFromHistoryNode(ctx context.Context, filter HistoryNodeSelectFilter) ([]HistoryNodeRow, error)
		RangeDeleteFromHistoryNode(ctx context.Context, filter HistoryNodeDeleteFilter) (sql.Result, error)
	}
)
