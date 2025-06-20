package sqlplugin

import (
	"context"
	"database/sql"

	"go.temporal.io/server/common/primitives"
)

type (
	// ChildExecutionInfoMapsRow represents a row in child_execution_info_maps table
	ChildExecutionInfoMapsRow struct {
		ShardID      int32
		NamespaceID  primitives.UUID
		WorkflowID   string
		RunID        primitives.UUID
		InitiatedID  int64
		Data         []byte
		DataEncoding string
	}

	ChildExecutionInfoMapsFilter struct {
		ShardID      int32
		NamespaceID  primitives.UUID
		WorkflowID   string
		RunID        primitives.UUID
		InitiatedIDs []int64
	}

	ChildExecutionInfoMapsAllFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
	}

	// HistoryExecutionChildWorkflow is the SQL persistence interface for history execution child workflows
	HistoryExecutionChildWorkflow interface {
		// DeleteFromChildExecutionInfoMaps replace one or more rows into child_execution_info_maps table
		ReplaceIntoChildExecutionInfoMaps(ctx context.Context, rows []ChildExecutionInfoMapsRow) (sql.Result, error)
		// SelectAllFromChildExecutionInfoMaps returns all rows into child_execution_info_maps table
		SelectAllFromChildExecutionInfoMaps(ctx context.Context, filter ChildExecutionInfoMapsAllFilter) ([]ChildExecutionInfoMapsRow, error)
		// DeleteFromChildExecutionInfoMaps deletes one or more rows from child_execution_info_maps table
		DeleteFromChildExecutionInfoMaps(ctx context.Context, filter ChildExecutionInfoMapsFilter) (sql.Result, error)
		// DeleteAllFromChildExecutionInfoMaps deletes all rows from child_execution_info_maps table
		DeleteAllFromChildExecutionInfoMaps(ctx context.Context, filter ChildExecutionInfoMapsAllFilter) (sql.Result, error)
	}
)
