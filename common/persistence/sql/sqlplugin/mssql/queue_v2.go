package mssql

import (
	"context"
	"database/sql"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	templateEnqueueMessageQueryV2      = `INSERT INTO queue_messages (queue_type, queue_name, queue_partition, message_id, message_payload, message_encoding) VALUES(:queue_type, :queue_name, :queue_partition, :message_id, :message_payload, :message_encoding)`
	templateGetMessagesQueryV2         = `SELECT TOP (?) message_id, message_payload, message_encoding FROM queue_messages WHERE queue_type = ? and queue_name = ? and queue_partition = ? and message_id >= ? ORDER BY message_id ASC`
	templateRangeDeleteMessagesQueryV2 = `DELETE FROM queue_messages WHERE queue_type = ? and queue_name = ? and queue_partition = ? and message_id >= ? and message_id <= ?`
	// UPDLOCK serializes concurrent enqueue-ers on the newest row. TOP (1)
	// with ORDER BY message_id DESC avoids MAX(), so an empty queue surfaces
	// as sql.ErrNoRows from the driver instead of a NULL row.
	templateGetLastMessageIDQueryV2          = `SELECT TOP (1) message_id FROM queue_messages WITH (UPDLOCK, ROWLOCK) WHERE queue_type = ? and queue_name = ? and queue_partition = ? ORDER BY message_id DESC`
	templateCreateQueueMetadataQueryV2       = `INSERT INTO queues (queue_type, queue_name, metadata_payload, metadata_encoding) VALUES(:queue_type, :queue_name, :metadata_payload, :metadata_encoding)`
	templateUpdateQueueMetadataQueryV2       = `UPDATE queues SET metadata_payload = :metadata_payload, metadata_encoding = :metadata_encoding WHERE queue_type = :queue_type and queue_name = :queue_name`
	templateGetQueueMetadataQueryV2          = `SELECT metadata_payload, metadata_encoding from queues WHERE queue_type = ? and queue_name = ?`
	templateGetQueueMetadataQueryV2ForUpdate = `SELECT metadata_payload, metadata_encoding from queues WITH (UPDLOCK, ROWLOCK) WHERE queue_type = ? and queue_name = ?`
	// The source plugins' LIMIT/OFFSET form has no ORDER BY; OFFSET-FETCH in
	// T-SQL requires one, so order deterministically by the primary key
	// columns of the queues table.
	templateGetNameFromQueueMetadataV2 = `SELECT queue_type, queue_name, metadata_payload, metadata_encoding from queues WHERE queue_type = ? ORDER BY queue_type ASC, queue_name ASC OFFSET ? ROWS FETCH NEXT ? ROWS ONLY`
)

func (mdb *db) InsertIntoQueueV2Metadata(ctx context.Context, row *sqlplugin.QueueV2MetadataRow) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		templateCreateQueueMetadataQueryV2,
		row,
	)
}

func (mdb *db) UpdateQueueV2Metadata(ctx context.Context, row *sqlplugin.QueueV2MetadataRow) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		templateUpdateQueueMetadataQueryV2,
		row,
	)
}

func (mdb *db) SelectFromQueueV2Metadata(ctx context.Context, filter sqlplugin.QueueV2MetadataFilter) (*sqlplugin.QueueV2MetadataRow, error) {
	var row sqlplugin.QueueV2MetadataRow
	err := mdb.GetContext(ctx,
		&row,
		templateGetQueueMetadataQueryV2,
		filter.QueueType,
		filter.QueueName,
	)
	if err != nil {
		return nil, err
	}
	return &row, nil
}

func (mdb *db) SelectFromQueueV2MetadataForUpdate(ctx context.Context, filter sqlplugin.QueueV2MetadataFilter) (*sqlplugin.QueueV2MetadataRow, error) {
	var row sqlplugin.QueueV2MetadataRow
	err := mdb.GetContext(ctx,
		&row,
		templateGetQueueMetadataQueryV2ForUpdate,
		filter.QueueType,
		filter.QueueName,
	)
	if err != nil {
		return nil, err
	}
	return &row, nil
}

func (mdb *db) SelectNameFromQueueV2Metadata(ctx context.Context, filter sqlplugin.QueueV2MetadataTypeFilter) ([]sqlplugin.QueueV2MetadataRow, error) {
	var rows []sqlplugin.QueueV2MetadataRow
	err := mdb.SelectContext(ctx,
		&rows,
		templateGetNameFromQueueMetadataV2,
		filter.QueueType,
		filter.PageOffset,
		filter.PageSize,
	)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (mdb *db) InsertIntoQueueV2Messages(ctx context.Context, row []sqlplugin.QueueV2MessageRow) (sql.Result, error) {
	return mdb.NamedExecContext(ctx,
		templateEnqueueMessageQueryV2,
		row,
	)
}

func (mdb *db) RangeSelectFromQueueV2Messages(ctx context.Context, filter sqlplugin.QueueV2MessagesFilter) ([]sqlplugin.QueueV2MessageRow, error) {
	var rows []sqlplugin.QueueV2MessageRow
	err := mdb.SelectContext(ctx,
		&rows,
		templateGetMessagesQueryV2,
		filter.PageSize,
		filter.QueueType,
		filter.QueueName,
		filter.Partition,
		filter.MinMessageID,
	)
	return rows, err
}

func (mdb *db) RangeDeleteFromQueueV2Messages(ctx context.Context, filter sqlplugin.QueueV2MessagesFilter) (sql.Result, error) {
	return mdb.ExecContext(ctx,
		templateRangeDeleteMessagesQueryV2,
		filter.QueueType,
		filter.QueueName,
		filter.Partition,
		filter.MinMessageID,
		filter.MaxMessageID,
	)
}

func (mdb *db) GetLastEnqueuedMessageIDForUpdateV2(ctx context.Context, filter sqlplugin.QueueV2Filter) (int64, error) {
	var lastMessageID *int64
	err := mdb.GetContext(ctx,
		&lastMessageID,
		templateGetLastMessageIDQueryV2,
		filter.QueueType,
		filter.QueueName,
		filter.Partition,
	)
	if err != nil {
		return 0, err
	}
	if lastMessageID == nil {
		// The layer of code above us expects ErrNoRows when the queue is empty.
		// TOP (1) on an empty queue already yields ErrNoRows from GetContext;
		// this NULL guard mirrors the mysql plugin's MAX() handling in case a
		// NULL is ever scanned.
		return 0, sql.ErrNoRows
	}
	return *lastMessageID, nil
}
