package sqlplugin

import (
	"context"
	"database/sql"

	"go.temporal.io/server/common/primitives"
)

type (
	// RequestCancelInfoMapsRow represents a row in request_cancel_info_maps table
	RequestCancelInfoMapsRow struct {
		ShardID      int32
		NamespaceID  primitives.UUID
		WorkflowID   string
		RunID        primitives.UUID
		InitiatedID  int64
		Data         []byte
		DataEncoding string
	}

	RequestCancelInfoMapsFilter struct {
		ShardID      int32
		NamespaceID  primitives.UUID
		WorkflowID   string
		RunID        primitives.UUID
		InitiatedIDs []int64
	}

	RequestCancelInfoMapsAllFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
	}

	// HistoryExecutionRequestCancel is the SQL persistence interface for history execution request cancels
	HistoryExecutionRequestCancel interface {
		// ReplaceIntoRequestCancelInfoMaps replace one or more rows into request_cancel_info_maps table
		ReplaceIntoRequestCancelInfoMaps(ctx context.Context, rows []RequestCancelInfoMapsRow) (sql.Result, error)
		// SelectAllFromRequestCancelInfoMaps returns all rows from request_cancel_info_maps table
		SelectAllFromRequestCancelInfoMaps(ctx context.Context, filter RequestCancelInfoMapsAllFilter) ([]RequestCancelInfoMapsRow, error)
		// DeleteFromRequestCancelInfoMaps deletes one or more rows from request_cancel_info_maps table
		DeleteFromRequestCancelInfoMaps(ctx context.Context, filter RequestCancelInfoMapsFilter) (sql.Result, error)
		// DeleteAllFromRequestCancelInfoMaps deletes all rows from request_cancel_info_maps table
		DeleteAllFromRequestCancelInfoMaps(ctx context.Context, filter RequestCancelInfoMapsAllFilter) (sql.Result, error)
	}
)
