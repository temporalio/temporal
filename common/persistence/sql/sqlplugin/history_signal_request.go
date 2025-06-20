package sqlplugin

import (
	"context"
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

	SignalsRequestedSetsFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
		SignalIDs   []string
	}

	SignalsRequestedSetsAllFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
	}

	// HistoryExecutionSignalRequest is the SQL persistence interface for history execution signal request
	HistoryExecutionSignalRequest interface {
		// ReplaceIntoSignalsRequestedSets replace one or more rows into signals_requested_sets table
		ReplaceIntoSignalsRequestedSets(ctx context.Context, rows []SignalsRequestedSetsRow) (sql.Result, error)
		// SelectAllFromSignalsRequestedSets returns all rows from signals_requested_sets table
		SelectAllFromSignalsRequestedSets(ctx context.Context, filter SignalsRequestedSetsAllFilter) ([]SignalsRequestedSetsRow, error)
		// DeleteFromSignalsRequestedSets deletes one or more rows from signals_requested_sets table
		DeleteFromSignalsRequestedSets(ctx context.Context, filter SignalsRequestedSetsFilter) (sql.Result, error)
		// DeleteAllFromSignalsRequestedSets deletes all rows from signals_requested_sets table
		DeleteAllFromSignalsRequestedSets(ctx context.Context, filter SignalsRequestedSetsAllFilter) (sql.Result, error)
	}
)
