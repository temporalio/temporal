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

package sql

import (
	"database/sql"
	"fmt"

	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

type (
	sqlQueue struct {
		queueType persistence.QueueType
		logger    log.Logger
		sqlStore
	}
)

func newQueue(
	db sqlplugin.DB,
	logger log.Logger,
	queueType persistence.QueueType,
) (persistence.Queue, error) {
	queue := &sqlQueue{
		sqlStore: sqlStore{
			db:     db,
			logger: logger,
		},
		queueType: queueType,
		logger:    logger,
	}

	if err := queue.initializeQueueMetadata(); err != nil {
		return nil, err
	}
	if err := queue.initializeDLQMetadata(); err != nil {
		return nil, err
	}

	return queue, nil
}

func (q *sqlQueue) EnqueueMessage(
	messagePayload []byte,
) error {

	err := q.txExecute("EnqueueMessage", func(tx sqlplugin.Tx) error {
		lastMessageID, err := tx.GetLastEnqueuedMessageIDForUpdate(q.queueType)
		switch err {
		case nil:
			_, err = tx.InsertIntoMessages([]sqlplugin.QueueMessageRow{
				newQueueRow(q.queueType, lastMessageID+1, messagePayload),
			})
			return err
		case sql.ErrNoRows:
			_, err = tx.InsertIntoMessages([]sqlplugin.QueueMessageRow{
				newQueueRow(q.queueType, sqlplugin.EmptyMessageID+1, messagePayload),
			})
			return err
		default:
			return fmt.Errorf("failed to get last enqueued message id: %v", err)
		}
	})
	if err != nil {
		return serviceerror.NewInternal(err.Error())
	}
	return nil
}

func (q *sqlQueue) ReadMessages(
	lastMessageID int64,
	pageSize int,
) ([]*persistence.QueueMessage, error) {

	rows, err := q.db.RangeSelectFromMessages(sqlplugin.QueueMessagesRangeFilter{
		QueueType:    q.queueType,
		MinMessageID: lastMessageID,
		MaxMessageID: sqlplugin.MaxMessageID,
		PageSize:     pageSize,
	})
	if err != nil {
		return nil, err
	}

	var messages []*persistence.QueueMessage
	for _, row := range rows {
		messages = append(messages, &persistence.QueueMessage{ID: row.MessageID, Payload: row.MessagePayload})
	}
	return messages, nil
}

func newQueueRow(
	queueType persistence.QueueType,
	messageID int64,
	payload []byte,
) sqlplugin.QueueMessageRow {

	return sqlplugin.QueueMessageRow{QueueType: queueType, MessageID: messageID, MessagePayload: payload}
}

func (q *sqlQueue) DeleteMessagesBefore(
	messageID int64,
) error {

	_, err := q.db.RangeDeleteFromMessages(sqlplugin.QueueMessagesRangeFilter{
		QueueType:    q.queueType,
		MinMessageID: sqlplugin.EmptyMessageID,
		MaxMessageID: messageID - 1,
	})
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("DeleteMessagesBefore operation failed. Error %v", err))
	}
	return nil
}

func (q *sqlQueue) UpdateAckLevel(
	messageID int64,
	clusterName string,
) error {

	err := q.txExecute("UpdateAckLevel", func(tx sqlplugin.Tx) error {
		row, err := tx.LockQueueMetadata(sqlplugin.QueueMetadataFilter{
			QueueType: q.queueType,
		})
		if err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("UpdateAckLevel operation failed. Error %v", err))
		}

		queueMetadata, err := serialization.QueueMetadataFromBlob(row.Data, row.DataEncoding)
		if err != nil {
			return err
		}
		if queueMetadata.ClusterAckLevels == nil {
			queueMetadata.ClusterAckLevels = make(map[string]int64)
		}

		// Ignore possibly delayed message
		if queueMetadata.ClusterAckLevels[clusterName] > messageID {
			return nil
		}
		queueMetadata.ClusterAckLevels[clusterName] = messageID

		blob, err := serialization.QueueMetadataToBlob(queueMetadata)
		if err != nil {
			return err
		}

		result, err := tx.UpdateQueueMetadata(&sqlplugin.QueueMetadataRow{
			QueueType:    q.queueType,
			Data:         blob.Data,
			DataEncoding: blob.EncodingType.String(),
		})
		if err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("UpdateAckLevel operation failed. Error %v", err))
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("rowsAffected returned error for queue metadata %v: %v", q.queueType, err)
		}
		if rowsAffected != 1 {
			return fmt.Errorf("rowsAffected returned %v queue metadata instead of one", rowsAffected)
		}
		return nil
	})

	if err != nil {
		return serviceerror.NewInternal(err.Error())
	}
	return nil
}

func (q *sqlQueue) GetAckLevels() (map[string]int64, error) {
	row, err := q.db.SelectFromQueueMetadata(sqlplugin.QueueMetadataFilter{
		QueueType: q.queueType,
	})
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("GetAckLevels operation failed. Error %v", err))
	}

	queueMetadata, err := serialization.QueueMetadataFromBlob(row.Data, row.DataEncoding)
	if err != nil {
		return nil, err
	}

	clusterAckLevels := queueMetadata.ClusterAckLevels
	if clusterAckLevels == nil {
		clusterAckLevels = make(map[string]int64)
	}
	return clusterAckLevels, nil
}

func (q *sqlQueue) EnqueueMessageToDLQ(
	messagePayload []byte,
) (int64, error) {

	var lastMessageID int64
	err := q.txExecute("EnqueueMessageToDLQ", func(tx sqlplugin.Tx) error {
		var err error
		lastMessageID, err = tx.GetLastEnqueuedMessageIDForUpdate(q.getDLQTypeFromQueueType())
		switch err {
		case nil:
			_, err = tx.InsertIntoMessages([]sqlplugin.QueueMessageRow{
				newQueueRow(q.getDLQTypeFromQueueType(), lastMessageID+1, messagePayload),
			})
			return err
		case sql.ErrNoRows:
			_, err = tx.InsertIntoMessages([]sqlplugin.QueueMessageRow{
				newQueueRow(q.getDLQTypeFromQueueType(), sqlplugin.EmptyMessageID+1, messagePayload),
			})
			return err
		default:
			return fmt.Errorf("failed to get last enqueued message id from DLQ: %v", err)
		}
	})
	if err != nil {
		return sqlplugin.EmptyMessageID, serviceerror.NewInternal(err.Error())
	}
	return lastMessageID + 1, nil
}

func (q *sqlQueue) ReadMessagesFromDLQ(
	firstMessageID int64,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) ([]*persistence.QueueMessage, []byte, error) {

	if pageToken != nil && len(pageToken) != 0 {
		lastReadMessageID, err := deserializePageToken(pageToken)
		if err != nil {
			return nil, nil, serviceerror.NewInternal(fmt.Sprintf("invalid next page token %v", pageToken))
		}
		firstMessageID = lastReadMessageID
	}

	rows, err := q.db.RangeSelectFromMessages(sqlplugin.QueueMessagesRangeFilter{
		QueueType:    q.getDLQTypeFromQueueType(),
		MinMessageID: firstMessageID,
		MaxMessageID: lastMessageID,
		PageSize:     pageSize,
	})
	if err != nil {
		return nil, nil, serviceerror.NewInternal(fmt.Sprintf("ReadMessagesFromDLQ operation failed. Error %v", err))
	}

	var messages []*persistence.QueueMessage
	for _, row := range rows {
		messages = append(messages, &persistence.QueueMessage{ID: row.MessageID, Payload: row.MessagePayload})
	}

	var newPagingToken []byte
	if messages != nil && len(messages) >= pageSize {
		lastReadMessageID := messages[len(messages)-1].ID
		newPagingToken = serializePageToken(lastReadMessageID)
	}
	return messages, newPagingToken, nil
}

func (q *sqlQueue) DeleteMessageFromDLQ(
	messageID int64,
) error {

	_, err := q.db.DeleteFromMessages(sqlplugin.QueueMessagesFilter{
		QueueType: q.getDLQTypeFromQueueType(),
		MessageID: messageID,
	})
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("DeleteMessageFromDLQ operation failed. Error %v", err))
	}
	return nil
}

func (q *sqlQueue) RangeDeleteMessagesFromDLQ(
	firstMessageID int64,
	lastMessageID int64,
) error {

	_, err := q.db.RangeDeleteFromMessages(sqlplugin.QueueMessagesRangeFilter{
		QueueType:    q.getDLQTypeFromQueueType(),
		MinMessageID: firstMessageID,
		MaxMessageID: lastMessageID,
	})
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("RangeDeleteMessagesFromDLQ operation failed. Error %v", err))
	}
	return nil
}

func (q *sqlQueue) UpdateDLQAckLevel(
	messageID int64,
	clusterName string,
) error {

	err := q.txExecute("UpdateDLQAckLevel", func(tx sqlplugin.Tx) error {
		row, err := tx.LockQueueMetadata(sqlplugin.QueueMetadataFilter{
			QueueType: q.getDLQTypeFromQueueType(),
		})
		if err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("UpdateDLQAckLevel operation failed. Error %v", err))
		}

		queueMetadata, err := serialization.QueueMetadataFromBlob(row.Data, row.DataEncoding)
		if err != nil {
			return err
		}
		if queueMetadata.ClusterAckLevels == nil {
			queueMetadata.ClusterAckLevels = make(map[string]int64)
		}

		// Ignore possibly delayed message
		if queueMetadata.ClusterAckLevels[clusterName] > messageID {
			return nil
		}
		queueMetadata.ClusterAckLevels[clusterName] = messageID

		blob, err := serialization.QueueMetadataToBlob(queueMetadata)
		if err != nil {
			return err
		}

		result, err := tx.UpdateQueueMetadata(&sqlplugin.QueueMetadataRow{
			QueueType:    q.getDLQTypeFromQueueType(),
			Data:         blob.Data,
			DataEncoding: blob.EncodingType.String(),
		})
		if err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("UpdateDLQAckLevel operation failed. Error %v", err))
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("rowsAffected returned error for DLQ metadata %v: %v", q.queueType, err)
		}
		if rowsAffected != 1 {
			return fmt.Errorf("rowsAffected returned %v DLQ metadata instead of one", rowsAffected)
		}
		return nil
	})

	if err != nil {
		return serviceerror.NewInternal(err.Error())
	}
	return nil
}

func (q *sqlQueue) GetDLQAckLevels() (map[string]int64, error) {
	row, err := q.db.SelectFromQueueMetadata(sqlplugin.QueueMetadataFilter{
		QueueType: q.getDLQTypeFromQueueType(),
	})
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("GetDLQAckLevels operation failed. Error %v", err))
	}

	queueMetadata, err := serialization.QueueMetadataFromBlob(row.Data, row.DataEncoding)
	if err != nil {
		return nil, err
	}

	clusterAckLevels := queueMetadata.ClusterAckLevels
	if clusterAckLevels == nil {
		clusterAckLevels = make(map[string]int64)
	}
	return clusterAckLevels, nil
}

func (q *sqlQueue) getDLQTypeFromQueueType() persistence.QueueType {
	return -q.queueType
}

func (q *sqlQueue) initializeQueueMetadata() error {
	_, err := q.db.SelectFromQueueMetadata(sqlplugin.QueueMetadataFilter{
		QueueType: q.queueType,
	})
	switch err {
	case nil:
		return nil
	case sql.ErrNoRows:
		queueMetadata := &persistencespb.QueueMetadata{}
		blob, err := serialization.QueueMetadataToBlob(queueMetadata)
		if err != nil {
			return err
		}
		result, err := q.db.InsertIntoQueueMetadata(&sqlplugin.QueueMetadataRow{
			QueueType:    q.queueType,
			Data:         blob.Data,
			DataEncoding: blob.EncodingType.String(),
		})
		if err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("initializeQueueMetadata operation failed. Error %v", err))
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("rowsAffected returned error when initializing queue metadata  %v: %v", q.queueType, err)
		}
		if rowsAffected != 1 {
			return fmt.Errorf("rowsAffected returned %v queue metadata instead of one", rowsAffected)
		}
		return nil
	default:
		return err
	}
}

func (q *sqlQueue) initializeDLQMetadata() error {
	_, err := q.db.SelectFromQueueMetadata(sqlplugin.QueueMetadataFilter{
		QueueType: q.getDLQTypeFromQueueType(),
	})
	switch err {
	case nil:
		return nil
	case sql.ErrNoRows:
		queueMetadata := &persistencespb.QueueMetadata{}
		blob, err := serialization.QueueMetadataToBlob(queueMetadata)
		if err != nil {
			return err
		}
		result, err := q.db.InsertIntoQueueMetadata(&sqlplugin.QueueMetadataRow{
			QueueType:    q.getDLQTypeFromQueueType(),
			Data:         blob.Data,
			DataEncoding: blob.EncodingType.String(),
		})
		if err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("initializeDLQMetadata operation failed. Error %v", err))
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("rowsAffected returned error when initializing DLQ metadata  %v: %v", q.queueType, err)
		}
		if rowsAffected != 1 {
			return fmt.Errorf("rowsAffected returned %v DLQ metadata instead of one", rowsAffected)
		}
		return nil
	default:
		return err
	}
}
