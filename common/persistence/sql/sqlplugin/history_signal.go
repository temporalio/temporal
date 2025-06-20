package sqlplugin

import (
	"context"
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

	SignalInfoMapsFilter struct {
		ShardID      int32
		NamespaceID  primitives.UUID
		WorkflowID   string
		RunID        primitives.UUID
		InitiatedIDs []int64
	}

	SignalInfoMapsAllFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
	}

	// HistoryExecutionSignal is the SQL persistence interface for history execution signals
	HistoryExecutionSignal interface {
		// ReplaceIntoSignalInfoMaps replace one or more rows into signal_info_maps table
		ReplaceIntoSignalInfoMaps(ctx context.Context, rows []SignalInfoMapsRow) (sql.Result, error)
		// SelectAllFromSignalInfoMaps returns one or more rows from signal_info_maps table
		SelectAllFromSignalInfoMaps(ctx context.Context, filter SignalInfoMapsAllFilter) ([]SignalInfoMapsRow, error)
		// DeleteFromSignalInfoMaps deletes one or more rows from signal_info_maps table
		DeleteFromSignalInfoMaps(ctx context.Context, filter SignalInfoMapsFilter) (sql.Result, error)
		// DeleteFromSignalInfoMaps deletes one or more rows from signal_info_maps table
		DeleteAllFromSignalInfoMaps(ctx context.Context, filter SignalInfoMapsAllFilter) (sql.Result, error)
	}
)
