package postgresql

import (
	"context"
	"database/sql"

	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	templateEnqueueMessageQuery      = `INSERT INTO queue (queue_type, message_id, message_payload, message_encoding) VALUES(:queue_type, :message_id, :message_payload, :message_encoding)`
	templateGetMessageQuery          = `SELECT message_id, message_payload, message_encoding FROM queue WHERE queue_type = $1 and message_id = $2`
	templateGetMessagesQuery         = `SELECT message_id, message_payload, message_encoding FROM queue WHERE queue_type = $1 and message_id > $2 and message_id <= $3 ORDER BY message_id ASC LIMIT $4`
	templateDeleteMessageQuery       = `DELETE FROM queue WHERE queue_type = $1 and message_id = $2`
	templateRangeDeleteMessagesQuery = `DELETE FROM queue WHERE queue_type = $1 and message_id > $2 and message_id <= $3`

	templateGetLastMessageIDQuery = `SELECT message_id FROM queue WHERE message_id >= (SELECT message_id FROM queue WHERE queue_type=$1 ORDER BY message_id DESC LIMIT 1) FOR UPDATE`

	templateCreateQueueMetadataQuery = `INSERT INTO queue_metadata (queue_type, data, data_encoding, version) VALUES(:queue_type, :data, :data_encoding, :version)`
	templateUpdateQueueMetadataQuery = `UPDATE queue_metadata SET data = :data, data_encoding = :data_encoding, version = :version+1 WHERE queue_type = :queue_type and version = :version`
	templateGetQueueMetadataQuery    = `SELECT data, data_encoding, version from queue_metadata WHERE queue_type = $1`
	templateLockQueueMetadataQuery   = templateGetQueueMetadataQuery + " FOR UPDATE"
)

// InsertIntoMessages inserts a new row into queue table
func (pdb *db) InsertIntoMessages(
	ctx context.Context,
	row []sqlplugin.QueueMessageRow,
) (sql.Result, error) {
	return pdb.NamedExecContext(ctx,
		templateEnqueueMessageQuery,
		row,
	)
}

func (pdb *db) SelectFromMessages(
	ctx context.Context,
	filter sqlplugin.QueueMessagesFilter,
) ([]sqlplugin.QueueMessageRow, error) {
	var rows []sqlplugin.QueueMessageRow
	err := pdb.SelectContext(ctx,
		&rows,
		templateGetMessageQuery,
		filter.QueueType,
		filter.MessageID,
	)
	return rows, err
}

func (pdb *db) RangeSelectFromMessages(
	ctx context.Context,
	filter sqlplugin.QueueMessagesRangeFilter,
) ([]sqlplugin.QueueMessageRow, error) {
	var rows []sqlplugin.QueueMessageRow
	err := pdb.SelectContext(ctx,
		&rows,
		templateGetMessagesQuery,
		filter.QueueType,
		filter.MinMessageID,
		filter.MaxMessageID,
		filter.PageSize,
	)
	return rows, err
}

// DeleteFromMessages deletes message with a messageID from the queue
func (pdb *db) DeleteFromMessages(
	ctx context.Context,
	filter sqlplugin.QueueMessagesFilter,
) (sql.Result, error) {
	return pdb.ExecContext(ctx,
		templateDeleteMessageQuery,
		filter.QueueType,
		filter.MessageID,
	)
}

// RangeDeleteFromMessages deletes messages before messageID from the queue
func (pdb *db) RangeDeleteFromMessages(
	ctx context.Context,
	filter sqlplugin.QueueMessagesRangeFilter,
) (sql.Result, error) {
	return pdb.ExecContext(ctx,
		templateRangeDeleteMessagesQuery,
		filter.QueueType,
		filter.MinMessageID,
		filter.MaxMessageID,
	)
}

// GetLastEnqueuedMessageIDForUpdate returns the last enqueued message ID
func (pdb *db) GetLastEnqueuedMessageIDForUpdate(
	ctx context.Context,
	queueType persistence.QueueType,
) (int64, error) {
	var lastMessageID int64
	err := pdb.GetContext(ctx,
		&lastMessageID,
		templateGetLastMessageIDQuery,
		queueType,
	)
	return lastMessageID, err
}

func (pdb *db) InsertIntoQueueMetadata(
	ctx context.Context,
	row *sqlplugin.QueueMetadataRow,
) (sql.Result, error) {
	return pdb.NamedExecContext(ctx,
		templateCreateQueueMetadataQuery,
		row,
	)
}

func (pdb *db) UpdateQueueMetadata(
	ctx context.Context,
	row *sqlplugin.QueueMetadataRow,
) (sql.Result, error) {
	return pdb.NamedExecContext(ctx,
		templateUpdateQueueMetadataQuery,
		row,
	)
}

func (pdb *db) SelectFromQueueMetadata(
	ctx context.Context,
	filter sqlplugin.QueueMetadataFilter,
) (*sqlplugin.QueueMetadataRow, error) {
	var row sqlplugin.QueueMetadataRow
	err := pdb.GetContext(ctx,
		&row,
		templateGetQueueMetadataQuery,
		filter.QueueType,
	)
	if err != nil {
		return nil, err
	}
	return &row, err
}

func (pdb *db) LockQueueMetadata(
	ctx context.Context,
	filter sqlplugin.QueueMetadataFilter,
) (*sqlplugin.QueueMetadataRow, error) {
	var row sqlplugin.QueueMetadataRow
	err := pdb.GetContext(ctx,
		&row,
		templateLockQueueMetadataQuery,
		filter.QueueType,
	)
	if err != nil {
		return nil, err
	}
	return &row, err
}
