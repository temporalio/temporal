package sqlplugin

import (
	"context"
	"database/sql"

	"go.temporal.io/server/common/primitives"
)

type (
	// TimerInfoMapsRow represents a row in timer_info_maps table
	TimerInfoMapsRow struct {
		ShardID      int32
		NamespaceID  primitives.UUID
		WorkflowID   string
		RunID        primitives.UUID
		TimerID      string
		Data         []byte
		DataEncoding string
	}

	TimerInfoMapsFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
		TimerIDs    []string
	}

	TimerInfoMapsAllFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
	}

	// HistoryExecutionTimer is the SQL persistence interface for history execution timers
	HistoryExecutionTimer interface {
		// ReplaceIntoTimerInfoMaps replace one or more rows into timer_info_maps table
		ReplaceIntoTimerInfoMaps(ctx context.Context, rows []TimerInfoMapsRow) (sql.Result, error)
		// SelectAllFromTimerInfoMaps returns all rows from timer_info_maps table
		SelectAllFromTimerInfoMaps(ctx context.Context, filter TimerInfoMapsAllFilter) ([]TimerInfoMapsRow, error)
		// DeleteFromTimerInfoMaps deletes one or more rows from timer_info_maps table
		DeleteFromTimerInfoMaps(ctx context.Context, filter TimerInfoMapsFilter) (sql.Result, error)
		// DeleteAllFromTimerInfoMaps deletes all rows from timer_info_maps table
		DeleteAllFromTimerInfoMaps(ctx context.Context, filter TimerInfoMapsAllFilter) (sql.Result, error)
	}
)
