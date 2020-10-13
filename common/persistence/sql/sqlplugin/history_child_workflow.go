package sqlplugin

import (
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

	ChildExecutionInfoMapsSelectFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
	}

	ChildExecutionInfoMapsDeleteFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
		InitiatedID *int64
	}

	// HistoryExecutionChildWorkflow is the SQL persistence interface for history nodes and history execution child workflows
	HistoryExecutionChildWorkflow interface {
		ReplaceIntoChildExecutionInfoMaps(rows []ChildExecutionInfoMapsRow) (sql.Result, error)
		// SelectFromChildExecutionInfoMaps returns one or more rows form child_execution_info_maps table
		SelectFromChildExecutionInfoMaps(filter ChildExecutionInfoMapsSelectFilter) ([]ChildExecutionInfoMapsRow, error)
		// DeleteFromChildExecutionInfoMaps deletes one or more rows from child_execution_info_maps
		// Required filter params
		// - single row - {shardID, namespaceID, workflowID, runID, initiatedID}
		// - multiple rows - {shardID, namespaceID, workflowID, runID}
		DeleteFromChildExecutionInfoMaps(filter ChildExecutionInfoMapsDeleteFilter) (sql.Result, error)
	}
)
