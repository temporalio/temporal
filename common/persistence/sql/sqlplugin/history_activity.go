package sqlplugin

import (
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

	ActivityInfoMapsSelectFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
	}

	ActivityInfoMapsDeleteFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
		ScheduleID  *int64
	}

	// HistoryExecutionActivity is the SQL persistence interface for history nodes and history execution activities
	HistoryExecutionActivity interface {
		ReplaceIntoActivityInfoMaps(rows []ActivityInfoMapsRow) (sql.Result, error)
		// SelectFromActivityInfoMaps returns one or more rows from activity_info_maps
		SelectFromActivityInfoMaps(filter ActivityInfoMapsSelectFilter) ([]ActivityInfoMapsRow, error)
		// DeleteFromActivityInfoMaps deletes a row from activity_info_maps table
		// Required filter params
		// - single row delete - {shardID, namespaceID, workflowID, runID, scheduleID}
		// - range delete - {shardID, namespaceID, workflowID, runID}
		DeleteFromActivityInfoMaps(filter ActivityInfoMapsDeleteFilter) (sql.Result, error)
	}
)
