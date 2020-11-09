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

package mysql

import (
	"database/sql"

	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	templateEnqueueMessageQuery      = `INSERT INTO queue (queue_type, message_id, message_payload, message_encoding) VALUES(:queue_type, :message_id, :message_payload, :message_encoding)`
	templateGetMessageQuery          = `SELECT message_id, message_payload, message_encoding FROM queue WHERE queue_type = ? and message_id = ?`
	templateGetMessagesQuery         = `SELECT message_id, message_payload, message_encoding FROM queue WHERE queue_type = ? and message_id > ? and message_id <= ? ORDER BY message_id ASC LIMIT ?`
	templateDeleteMessageQuery       = `DELETE FROM queue WHERE queue_type = ? and message_id = ?`
	templateRangeDeleteMessagesQuery = `DELETE FROM queue WHERE queue_type = ? and message_id > ? and message_id <= ?`

	templateGetLastMessageIDQuery = `SELECT message_id FROM queue WHERE message_id >= (SELECT message_id FROM queue WHERE queue_type=? ORDER BY message_id DESC LIMIT 1) FOR UPDATE`

	templateCreateQueueMetadataQuery = `INSERT INTO queue_metadata (queue_type, data, data_encoding) VALUES(:queue_type, :data, :data_encoding)`
	templateUpdateQueueMetadataQuery = `UPDATE queue_metadata SET data = :data, data_encoding = :data_encoding WHERE queue_type = :queue_type`
	templateGetQueueMetadataQuery    = `SELECT data, data_encoding from queue_metadata WHERE queue_type = ?`
	templateLockQueueMetadataQuery   = templateGetQueueMetadataQuery + " FOR UPDATE"
)

// InsertIntoMessages inserts a new row into queue table
func (mdb *db) InsertIntoMessages(row []sqlplugin.QueueMessageRow) (sql.Result, error) {
	return mdb.conn.NamedExec(templateEnqueueMessageQuery, row)
}

func (mdb *db) SelectFromMessages(filter sqlplugin.QueueMessagesFilter) ([]sqlplugin.QueueMessageRow, error) {
	var rows []sqlplugin.QueueMessageRow
	err := mdb.conn.Select(&rows, templateGetMessageQuery, filter.QueueType, filter.MessageID)
	return rows, err
}

func (mdb *db) RangeSelectFromMessages(filter sqlplugin.QueueMessagesRangeFilter) ([]sqlplugin.QueueMessageRow, error) {
	var rows []sqlplugin.QueueMessageRow
	err := mdb.conn.Select(&rows, templateGetMessagesQuery, filter.QueueType, filter.MinMessageID, filter.MaxMessageID, filter.PageSize)
	return rows, err
}

// DeleteFromMessages deletes message with a messageID from the queue
func (mdb *db) DeleteFromMessages(filter sqlplugin.QueueMessagesFilter) (sql.Result, error) {
	return mdb.conn.Exec(templateDeleteMessageQuery, filter.QueueType, filter.MessageID)
}

// RangeDeleteFromMessages deletes messages before messageID from the queue
func (mdb *db) RangeDeleteFromMessages(filter sqlplugin.QueueMessagesRangeFilter) (sql.Result, error) {
	return mdb.conn.Exec(templateRangeDeleteMessagesQuery, filter.QueueType, filter.MinMessageID, filter.MaxMessageID)
}

// GetLastEnqueuedMessageIDForUpdate returns the last enqueued message ID
func (mdb *db) GetLastEnqueuedMessageIDForUpdate(queueType persistence.QueueType) (int64, error) {
	var lastMessageID int64
	err := mdb.conn.Get(&lastMessageID, templateGetLastMessageIDQuery, queueType)
	return lastMessageID, err
}

func (mdb *db) InsertIntoQueueMetadata(row *sqlplugin.QueueMetadataRow) (sql.Result, error) {
	return mdb.conn.NamedExec(templateCreateQueueMetadataQuery, row)
}

func (mdb *db) UpdateQueueMetadata(row *sqlplugin.QueueMetadataRow) (sql.Result, error) {
	return mdb.conn.NamedExec(templateUpdateQueueMetadataQuery, row)
}

func (mdb *db) SelectFromQueueMetadata(filter sqlplugin.QueueMetadataFilter) (*sqlplugin.QueueMetadataRow, error) {
	var row sqlplugin.QueueMetadataRow
	err := mdb.conn.Get(&row, templateGetQueueMetadataQuery, filter.QueueType)
	if err != nil {
		return nil, err
	}
	return &row, err
}

func (mdb *db) LockQueueMetadata(filter sqlplugin.QueueMetadataFilter) (*sqlplugin.QueueMetadataRow, error) {
	var row sqlplugin.QueueMetadataRow
	err := mdb.conn.Get(&row, templateLockQueueMetadataQuery, filter.QueueType)
	if err != nil {
		return nil, err
	}
	return &row, err
}
