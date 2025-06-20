package sqlplugin

import (
	"context"
	"database/sql"

	"go.temporal.io/server/common/primitives"
)

type (
	// BufferedEventsRow represents a row in buffered_events table
	BufferedEventsRow struct {
		ShardID      int32
		NamespaceID  primitives.UUID
		WorkflowID   string
		RunID        primitives.UUID
		Data         []byte
		DataEncoding string
	}

	// BufferedEventsFilter contains the column names within buffered_events table that
	// can be used to filter results through a WHERE clause
	BufferedEventsFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
	}

	// HistoryExecutionBuffer is the SQL persistence interface for history nodes and history execution buffer events
	HistoryExecutionBuffer interface {
		InsertIntoBufferedEvents(ctx context.Context, rows []BufferedEventsRow) (sql.Result, error)
		SelectFromBufferedEvents(ctx context.Context, filter BufferedEventsFilter) ([]BufferedEventsRow, error)
		DeleteFromBufferedEvents(ctx context.Context, filter BufferedEventsFilter) (sql.Result, error)
	}
)
