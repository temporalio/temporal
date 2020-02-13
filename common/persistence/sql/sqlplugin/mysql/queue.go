// Copyright (c) 2017 Uber Technologies, Inc.
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
	"encoding/json"

	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
)

const (
	templateEnqueueMessageQuery            = `INSERT INTO queue (queue_type, message_id, message_payload) VALUES(:queue_type, :message_id, :message_payload)`
	templateGetLastMessageIDQuery          = `SELECT message_id FROM queue WHERE message_id >= (SELECT message_id FROM queue WHERE queue_type=? ORDER BY message_id DESC LIMIT 1) FOR UPDATE`
	templateGetMessagesQuery               = `SELECT message_id, message_payload FROM queue WHERE queue_type = ? and message_id > ? ORDER BY message_id ASC LIMIT ?`
	templateGetMessagesBetweenQuery        = `SELECT message_id, message_payload FROM queue WHERE queue_type = ? and message_id > ? and message_id <= ? ORDER BY message_id ASC LIMIT ?`
	templateDeleteMessagesBeforeQuery      = `DELETE FROM queue WHERE queue_type = ? and message_id < ?`
	templateRangeDeleteMessagesQuery       = `DELETE FROM queue WHERE queue_type = ? and message_id > ? and message_id <= ?`
	templateDeleteMessageQuery             = `DELETE FROM queue WHERE queue_type = ? and message_id = ?`
	templateGetQueueMetadataQuery          = `SELECT data from queue_metadata WHERE queue_type = ?`
	templateGetQueueMetadataForUpdateQuery = templateGetQueueMetadataQuery + ` FOR UPDATE`
	templateInsertQueueMetadataQuery       = `INSERT INTO queue_metadata (queue_type, data) VALUES(:queue_type, :data)`
	templateUpdateQueueMetadataQuery       = `UPDATE queue_metadata SET data = ? WHERE queue_type = ?`
)

// InsertIntoQueue inserts a new row into queue table
func (mdb *db) InsertIntoQueue(row *sqlplugin.QueueRow) (sql.Result, error) {
	return mdb.conn.NamedExec(templateEnqueueMessageQuery, row)
}

// GetLastEnqueuedMessageIDForUpdate returns the last enqueued message ID
func (mdb *db) GetLastEnqueuedMessageIDForUpdate(queueType persistence.QueueType) (int, error) {
	var lastMessageID int
	err := mdb.conn.Get(&lastMessageID, templateGetLastMessageIDQuery, queueType)
	return lastMessageID, err
}

// GetMessagesFromQueue retrieves messages from the queue
func (mdb *db) GetMessagesFromQueue(queueType persistence.QueueType, lastMessageID, maxRows int) ([]sqlplugin.QueueRow, error) {
	var rows []sqlplugin.QueueRow
	err := mdb.conn.Select(&rows, templateGetMessagesQuery, queueType, lastMessageID, maxRows)
	return rows, err
}

// GetMessagesBetween retrieves messages from the queue
func (mdb *db) GetMessagesBetween(queueType persistence.QueueType, firstMessageID int, lastMessageID int, maxRows int) ([]sqlplugin.QueueRow, error) {
	var rows []sqlplugin.QueueRow
	err := mdb.conn.Select(&rows, templateGetMessagesBetweenQuery, queueType, firstMessageID, lastMessageID, maxRows)
	return rows, err
}

// DeleteMessagesBefore deletes messages before messageID from the queue
func (mdb *db) DeleteMessagesBefore(queueType persistence.QueueType, messageID int) (sql.Result, error) {
	return mdb.conn.Exec(templateDeleteMessagesBeforeQuery, queueType, messageID)
}

// RangeDeleteMessages deletes messages before messageID from the queue
func (mdb *db) RangeDeleteMessages(queueType persistence.QueueType, exclusiveBeginMessageID int, inclusiveEndMessageID int) (sql.Result, error) {
	return mdb.conn.Exec(templateRangeDeleteMessagesQuery, queueType, exclusiveBeginMessageID, inclusiveEndMessageID)
}

// DeleteMessage deletes message with a messageID from the queue
func (mdb *db) DeleteMessage(queueType persistence.QueueType, messageID int) (sql.Result, error) {
	return mdb.conn.Exec(templateDeleteMessageQuery, queueType, messageID)
}

// InsertAckLevel inserts ack level
func (mdb *db) InsertAckLevel(queueType persistence.QueueType, messageID int, clusterName string) error {
	clusterAckLevels := map[string]int{clusterName: messageID}
	data, err := json.Marshal(clusterAckLevels)
	if err != nil {
		return err
	}

	_, err = mdb.conn.NamedExec(templateInsertQueueMetadataQuery, sqlplugin.QueueMetadataRow{QueueType: queueType, Data: data})
	return err

}

// UpdateAckLevels updates cluster ack levels
func (mdb *db) UpdateAckLevels(queueType persistence.QueueType, clusterAckLevels map[string]int) error {
	data, err := json.Marshal(clusterAckLevels)
	if err != nil {
		return err
	}

	_, err = mdb.conn.Exec(templateUpdateQueueMetadataQuery, data, queueType)
	return err
}

// GetAckLevels returns ack levels for pulling clusters
func (mdb *db) GetAckLevels(queueType persistence.QueueType, forUpdate bool) (map[string]int, error) {
	queryStr := templateGetQueueMetadataQuery
	if forUpdate {
		queryStr = templateGetQueueMetadataForUpdateQuery
	}

	var data []byte
	err := mdb.conn.Get(&data, queryStr, queueType)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}

		return nil, err
	}

	var clusterAckLevels map[string]int
	if err := json.Unmarshal(data, &clusterAckLevels); err != nil {
		return nil, err
	}

	return clusterAckLevels, nil
}
