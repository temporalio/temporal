package sqlplugin

import (
	"database/sql"

	"go.temporal.io/server/common/primitives"
)

type (
	// SignalsRequestedSetsRow represents a row in signals_requested_sets table
	SignalsRequestedSetsRow struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
		SignalID    string
	}

	SignalsRequestedSetsSelectFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
	}

	SignalsRequestedSetsDeleteFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
		SignalID    *string
	}

	// HistoryExecutionSignalRequest is the SQL persistence interface for history nodes and history execution signal request
	HistoryExecutionSignalRequest interface {
		ReplaceIntoSignalsRequestedSets(rows []SignalsRequestedSetsRow) (sql.Result, error)
		// SelectFromSignalInfoMaps returns one or more rows form signals_requested_sets table
		SelectFromSignalsRequestedSets(filter SignalsRequestedSetsSelectFilter) ([]SignalsRequestedSetsRow, error)
		// DeleteFromSignalsRequestedSets deletes one or more rows from signals_requested_sets
		// Required filter params
		// - single row - {shardID, namespaceID, workflowID, runID, signalID}
		// - multiple rows - {shardID, namespaceID, workflowID, runID}
		DeleteFromSignalsRequestedSets(filter SignalsRequestedSetsDeleteFilter) (sql.Result, error)
	}
)
