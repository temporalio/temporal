package sqlplugin

import (
	"database/sql"

	"go.temporal.io/server/common/primitives"
)

type (
	// SignalInfoMapsRow represents a row in signal_info_maps table
	SignalInfoMapsRow struct {
		ShardID      int32
		NamespaceID  primitives.UUID
		WorkflowID   string
		RunID        primitives.UUID
		InitiatedID  int64
		Data         []byte
		DataEncoding string
	}

	SignalInfoMapsSelectFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
	}

	SignalInfoMapsDeleteFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
		InitiatedID *int64
	}

	// HistoryExecutionSignal is the SQL persistence interface for history nodes and history execution signals
	HistoryExecutionSignal interface {
		ReplaceIntoSignalInfoMaps(rows []SignalInfoMapsRow) (sql.Result, error)
		// SelectFromSignalInfoMaps returns one or more rows form signal_info_maps table
		SelectFromSignalInfoMaps(filter SignalInfoMapsSelectFilter) ([]SignalInfoMapsRow, error)
		// DeleteFromSignalInfoMaps deletes one or more rows from signal_info_maps table
		// Required filter params
		// - single row - {shardID, namespaceID, workflowID, runID, initiatedID}
		// - multiple rows - {shardID, namespaceID, workflowID, runID}
		DeleteFromSignalInfoMaps(filter SignalInfoMapsDeleteFilter) (sql.Result, error)
	}
)
