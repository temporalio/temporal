package sqlplugin

import (
	"context"
	"database/sql"

	"go.temporal.io/server/common/persistence"
)

type (
	// QueueMetadataRow represents a row in queue_metadata table
	QueueMetadataRow struct {
		QueueType    persistence.QueueType
		Data         []byte
		DataEncoding string
		Version      int64
	}

	QueueMetadataFilter struct {
		QueueType persistence.QueueType
	}

	QueueMetadata interface {
		InsertIntoQueueMetadata(ctx context.Context, row *QueueMetadataRow) (sql.Result, error)
		UpdateQueueMetadata(ctx context.Context, row *QueueMetadataRow) (sql.Result, error)
		SelectFromQueueMetadata(ctx context.Context, filter QueueMetadataFilter) (*QueueMetadataRow, error)

		LockQueueMetadata(ctx context.Context, filter QueueMetadataFilter) (*QueueMetadataRow, error)
	}
)
