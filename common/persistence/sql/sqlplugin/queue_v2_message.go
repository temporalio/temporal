package sqlplugin

import (
	"context"
	"database/sql"

	"go.temporal.io/server/common/persistence"
)

type (
	// QueueV2MessageRow represents a row in queue_messages table
	QueueV2MessageRow struct {
		QueueType       persistence.QueueV2Type
		QueueName       string
		QueuePartition  int64
		MessageID       int64
		MessagePayload  []byte
		MessageEncoding string
	}

	// QueueV2MessagesFilter is used to filter rows in queue_messages table
	QueueV2MessagesFilter struct {
		QueueType    persistence.QueueV2Type
		QueueName    string
		Partition    int64
		MinMessageID int64
		MaxMessageID int64 // used for RangeDelete
		PageSize     int   // used for RangeSelect
	}

	// QueueV2Filter is used to filter rows in queues table
	QueueV2Filter struct {
		QueueType persistence.QueueV2Type
		QueueName string
		Partition int
	}

	QueueV2Message interface {
		InsertIntoQueueV2Messages(ctx context.Context, row []QueueV2MessageRow) (sql.Result, error)
		RangeSelectFromQueueV2Messages(ctx context.Context, filter QueueV2MessagesFilter) ([]QueueV2MessageRow, error)
		RangeDeleteFromQueueV2Messages(ctx context.Context, filter QueueV2MessagesFilter) (sql.Result, error)
		GetLastEnqueuedMessageIDForUpdateV2(ctx context.Context, filter QueueV2Filter) (int64, error)
	}
)
