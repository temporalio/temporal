package sqlplugin

import (
	"context"
	"database/sql"

	"go.temporal.io/server/common/persistence"
)

type (
	// QueueMessageRow represents a row in queue table
	QueueMessageRow struct {
		QueueType       persistence.QueueType
		MessageID       int64
		MessagePayload  []byte
		MessageEncoding string
	}

	// QueueMessagesFilter
	QueueMessagesFilter struct {
		QueueType persistence.QueueType
		MessageID int64
	}

	// QueueMessagesRangeFilter
	QueueMessagesRangeFilter struct {
		QueueType    persistence.QueueType
		MinMessageID int64
		MaxMessageID int64
		PageSize     int
	}

	QueueMessage interface {
		InsertIntoMessages(ctx context.Context, row []QueueMessageRow) (sql.Result, error)
		SelectFromMessages(ctx context.Context, filter QueueMessagesFilter) ([]QueueMessageRow, error)
		RangeSelectFromMessages(ctx context.Context, filter QueueMessagesRangeFilter) ([]QueueMessageRow, error)
		DeleteFromMessages(ctx context.Context, filter QueueMessagesFilter) (sql.Result, error)
		RangeDeleteFromMessages(ctx context.Context, filter QueueMessagesRangeFilter) (sql.Result, error)

		GetLastEnqueuedMessageIDForUpdate(ctx context.Context, queueType persistence.QueueType) (int64, error)
	}
)
