package sqlplugin

import (
	"context"
	"database/sql"

	"go.temporal.io/server/common/primitives"
)

type (
	// ActivityInfoMapsRow represents a row in activity_info_maps table
	ActivityInfoMapsRow struct {
		ShardID      int32
		NamespaceID  primitives.UUID
		WorkflowID   string
		RunID        primitives.UUID
		ScheduleID   int64
		Data         []byte
		DataEncoding string
	}

	ActivityInfoMapsFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
		ScheduleIDs []int64
	}

	ActivityInfoMapsAllFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
	}

	// HistoryExecutionActivity is the SQL persistence interface for history nodes and history execution activities
	HistoryExecutionActivity interface {
		// ReplaceIntoActivityInfoMaps replace one or more into activity_info_maps table
		ReplaceIntoActivityInfoMaps(ctx context.Context, rows []ActivityInfoMapsRow) (sql.Result, error)
		// SelectAllFromActivityInfoMaps returns all rows from activity_info_maps table
		SelectAllFromActivityInfoMaps(ctx context.Context, filter ActivityInfoMapsAllFilter) ([]ActivityInfoMapsRow, error)
		// DeleteFromActivityInfoMaps deletes one or more row from activity_info_maps table
		DeleteFromActivityInfoMaps(ctx context.Context, filter ActivityInfoMapsFilter) (sql.Result, error)
		// DeleteAllFromActivityInfoMaps deletes all from activity_info_maps table
		DeleteAllFromActivityInfoMaps(ctx context.Context, filter ActivityInfoMapsAllFilter) (sql.Result, error)
	}
)
