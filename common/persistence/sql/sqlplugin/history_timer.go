package sqlplugin

import (
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

	TimerInfoMapsSelectFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
	}

	TimerInfoMapsDeleteFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
		TimerID     *string
	}

	// HistoryExecutionTimer is the SQL persistence interface for history nodes and history execution timers
	HistoryExecutionTimer interface {
		ReplaceIntoTimerInfoMaps(rows []TimerInfoMapsRow) (sql.Result, error)
		// SelectFromTimerInfoMaps returns one or more rows form timer_info_maps table
		SelectFromTimerInfoMaps(filter TimerInfoMapsSelectFilter) ([]TimerInfoMapsRow, error)
		// DeleteFromTimerInfoMaps deletes one or more rows from timer_info_maps
		// Required filter params
		// - single row - {shardID, namespaceID, workflowID, runID, timerID}
		// - multiple rows - {shardID, namespaceID, workflowID, runID}
		DeleteFromTimerInfoMaps(filter TimerInfoMapsDeleteFilter) (sql.Result, error)
	}
)
