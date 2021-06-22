// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
	return pdb.conn.NamedExecContext(ctx,
		templateEnqueueMessageQuery,
		row,
	)
}

func (pdb *db) SelectFromMessages(
	ctx context.Context,
	filter sqlplugin.QueueMessagesFilter,
) ([]sqlplugin.QueueMessageRow, error) {
	var rows []sqlplugin.QueueMessageRow
	err := pdb.conn.SelectContext(ctx,
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
	err := pdb.conn.SelectContext(ctx,
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
	return pdb.conn.ExecContext(ctx,
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
	return pdb.conn.ExecContext(ctx,
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
	err := pdb.conn.GetContext(ctx,
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
	return pdb.conn.NamedExecContext(ctx,
		templateCreateQueueMetadataQuery,
		row,
	)
}

func (pdb *db) UpdateQueueMetadata(
	ctx context.Context,
	row *sqlplugin.QueueMetadataRow,
) (sql.Result, error) {
	return pdb.conn.NamedExecContext(ctx,
		templateUpdateQueueMetadataQuery,
		row,
	)
}

func (pdb *db) SelectFromQueueMetadata(
	ctx context.Context,
	filter sqlplugin.QueueMetadataFilter,
) (*sqlplugin.QueueMetadataRow, error) {
	var row sqlplugin.QueueMetadataRow
	err := pdb.conn.GetContext(ctx,
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
	err := pdb.conn.GetContext(ctx,
		&row,
		templateLockQueueMetadataQuery,
		filter.QueueType,
	)
	if err != nil {
		return nil, err
	}
	return &row, err
}
