package sqlplugin

import (
	"context"
	"database/sql"

	"go.temporal.io/server/common/primitives"
)

type (
	// ChasmNodeMapsRow represents a row in the chasm_node_maps table.
	ChasmNodeMapsRow struct {
		ShardID          int32
		NamespaceID      primitives.UUID
		WorkflowID       string
		RunID            primitives.UUID
		ChasmPath        string
		Metadata         []byte
		MetadataEncoding string
		Data             []byte
		DataEncoding     string
	}

	// ChasmNodeMapsFilter represents parameters to selecting a particular subset of
	// a workflow's CHASM nodes.
	ChasmNodeMapsFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
		ChasmPaths  []string
	}

	// ChasmNodeMapsAllFilter represents parameters to selecting all of a workflow's
	// CHASM nodes.
	ChasmNodeMapsAllFilter struct {
		ShardID     int32
		NamespaceID primitives.UUID
		WorkflowID  string
		RunID       primitives.UUID
	}

	HistoryExecutionChasm interface {
		// SelectAllFromChasmNodeMaps returns all rows related to a particular workflow from the chasm_node_maps table.
		SelectAllFromChasmNodeMaps(ctx context.Context, filter ChasmNodeMapsAllFilter) ([]ChasmNodeMapsRow, error)

		// ReplaceIntoChasmNodeMaps replaces one or more rows in the chasm_node_maps table.
		ReplaceIntoChasmNodeMaps(ctx context.Context, rows []ChasmNodeMapsRow) (sql.Result, error)

		// DeleteFromChasmNodeMaps deletes one or more rows in the chasm_node_maps table.
		DeleteFromChasmNodeMaps(ctx context.Context, filter ChasmNodeMapsFilter) (sql.Result, error)

		// DeleteAllFromChasmNodeMaps deletes all rows related to a particular workflow in the chasm_node_maps table.
		DeleteAllFromChasmNodeMaps(ctx context.Context, filter ChasmNodeMapsAllFilter) (sql.Result, error)
	}
)
