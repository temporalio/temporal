package sqlplugin

import (
	"context"
	"database/sql"

	"go.temporal.io/server/common/primitives"
)

type (
	// HistoryTreeRow represents a row in history_tree table
	HistoryTreeRow struct {
		ShardID      int32
		TreeID       primitives.UUID
		BranchID     primitives.UUID
		Data         []byte
		DataEncoding string
	}

	// HistoryTreeSelectFilter contains the column names within history_tree table that
	// can be used to filter results through a WHERE clause
	HistoryTreeSelectFilter struct {
		ShardID int32
		TreeID  primitives.UUID
	}

	// HistoryTreeDeleteFilter contains the column names within history_tree table that
	// can be used to filter results through a WHERE clause
	HistoryTreeDeleteFilter struct {
		ShardID  int32
		TreeID   primitives.UUID
		BranchID primitives.UUID
	}

	// HistoryTreeBranchPage is a struct which represents a page of history tree branches to query.
	HistoryTreeBranchPage struct {
		ShardID  int32
		TreeID   primitives.UUID
		BranchID primitives.UUID
		Limit    int
	}

	// HistoryTree is the SQL persistence interface for history trees
	HistoryTree interface {
		InsertIntoHistoryTree(ctx context.Context, row *HistoryTreeRow) (sql.Result, error)
		SelectFromHistoryTree(ctx context.Context, filter HistoryTreeSelectFilter) ([]HistoryTreeRow, error)
		DeleteFromHistoryTree(ctx context.Context, filter HistoryTreeDeleteFilter) (sql.Result, error)
		PaginateBranchesFromHistoryTree(ctx context.Context, filter HistoryTreeBranchPage) ([]HistoryTreeRow, error)
	}
)
