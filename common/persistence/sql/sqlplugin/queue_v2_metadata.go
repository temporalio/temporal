package sqlplugin

import (
	"context"
	"database/sql"

	"go.temporal.io/server/common/persistence"
)

type (
	// QueueV2MetadataRow represents a row in queue_metadata table
	QueueV2MetadataRow struct {
		QueueType        persistence.QueueV2Type
		QueueName        string
		MetadataPayload  []byte
		MetadataEncoding string
	}

	QueueV2MetadataFilter struct {
		QueueType persistence.QueueV2Type
		QueueName string
	}

	QueueV2MetadataTypeFilter struct {
		QueueType  persistence.QueueV2Type
		PageSize   int
		PageOffset int64
	}

	QueueV2Metadata interface {
		InsertIntoQueueV2Metadata(ctx context.Context, row *QueueV2MetadataRow) (sql.Result, error)
		UpdateQueueV2Metadata(ctx context.Context, row *QueueV2MetadataRow) (sql.Result, error)
		SelectFromQueueV2Metadata(ctx context.Context, filter QueueV2MetadataFilter) (*QueueV2MetadataRow, error)
		SelectFromQueueV2MetadataForUpdate(ctx context.Context, filter QueueV2MetadataFilter) (*QueueV2MetadataRow, error)
		SelectNameFromQueueV2Metadata(ctx context.Context, filter QueueV2MetadataTypeFilter) ([]QueueV2MetadataRow, error)
	}
)
