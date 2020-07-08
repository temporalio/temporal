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

	"go.temporal.io/temporal-proto/serviceerror"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	emptyMessageID = -1
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
	return &sqlQueue{
		sqlStore: sqlStore{
			db:     db,
			logger: logger,
		},
		queueType: queueType,
		logger:    logger,
	}, nil
}

func (q *sqlQueue) EnqueueMessage(
	messagePayload []byte,
) error {

	err := q.txExecute("EnqueueMessage", func(tx sqlplugin.Tx) error {
		lastMessageID, err := tx.GetLastEnqueuedMessageIDForUpdate(q.queueType)
		if err != nil {
			if err == sql.ErrNoRows {
				lastMessageID = -1
			} else {
				return fmt.Errorf("failed to get last enqueued message id: %v", err)
			}
		}

		_, err = tx.InsertIntoQueue(newQueueRow(q.queueType, lastMessageID+1, messagePayload))
		return err
	})
	if err != nil {
		return serviceerror.NewInternal(err.Error())
	}
	return nil
}

func (q *sqlQueue) ReadMessages(
	lastMessageID int64,
	maxCount int,
) ([]*persistence.QueueMessage, error) {

	rows, err := q.db.GetMessagesFromQueue(q.queueType, lastMessageID, maxCount)
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
) *sqlplugin.QueueRow {

	return &sqlplugin.QueueRow{QueueType: queueType, MessageID: messageID, MessagePayload: payload}
}

func (q *sqlQueue) DeleteMessagesBefore(
	messageID int64,
) error {

	_, err := q.db.DeleteMessagesBefore(q.queueType, messageID)
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
		clusterAckLevels, err := tx.GetAckLevels(q.queueType, true)
		if err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("UpdateAckLevel operation failed. Error %v", err))
		}

		if clusterAckLevels == nil {
			err := tx.InsertAckLevel(q.queueType, messageID, clusterName)
			if err != nil {
				return serviceerror.NewInternal(fmt.Sprintf("UpdateAckLevel operation failed. Error %v", err))
			}
			return nil
		}

		// Ignore possibly delayed message
		if clusterAckLevels[clusterName] > messageID {
			return nil
		}

		clusterAckLevels[clusterName] = messageID
		err = tx.UpdateAckLevels(q.queueType, clusterAckLevels)
		if err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("UpdateAckLevel operation failed. Error %v", err))
		}
		return nil
	})

	if err != nil {
		return serviceerror.NewInternal(err.Error())
	}
	return nil
}

func (q *sqlQueue) GetAckLevels() (map[string]int64, error) {
	return q.db.GetAckLevels(q.queueType, false)
}

func (q *sqlQueue) EnqueueMessageToDLQ(
	messagePayload []byte,
) (int64, error) {

	var lastMessageID int64
	err := q.txExecute("EnqueueMessageToDLQ", func(tx sqlplugin.Tx) error {
		var err error
		lastMessageID, err = tx.GetLastEnqueuedMessageIDForUpdate(q.getDLQTypeFromQueueType())
		if err != nil {
			if err == sql.ErrNoRows {
				lastMessageID = -1
			} else {
				return fmt.Errorf("failed to get last enqueued message id from DLQ: %v", err)
			}
		}
		_, err = tx.InsertIntoQueue(newQueueRow(q.getDLQTypeFromQueueType(), lastMessageID+1, messagePayload))
		return err
	})
	if err != nil {
		return emptyMessageID, serviceerror.NewInternal(err.Error())
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

	rows, err := q.db.GetMessagesBetween(q.getDLQTypeFromQueueType(), firstMessageID, lastMessageID, pageSize)
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
		newPagingToken = serializePageToken(int64(lastReadMessageID))
	}
	return messages, newPagingToken, nil
}

func (q *sqlQueue) DeleteMessageFromDLQ(
	messageID int64,
) error {

	_, err := q.db.DeleteMessage(q.getDLQTypeFromQueueType(), messageID)
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("DeleteMessageFromDLQ operation failed. Error %v", err))
	}
	return nil
}

func (q *sqlQueue) RangeDeleteMessagesFromDLQ(
	firstMessageID int64,
	lastMessageID int64,
) error {

	_, err := q.db.RangeDeleteMessages(q.getDLQTypeFromQueueType(), firstMessageID, lastMessageID)
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
		clusterAckLevels, err := tx.GetAckLevels(q.getDLQTypeFromQueueType(), true)
		if err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("UpdateDLQAckLevel operation failed. Error %v", err))
		}

		if clusterAckLevels == nil {
			err := tx.InsertAckLevel(q.getDLQTypeFromQueueType(), messageID, clusterName)
			if err != nil {
				return serviceerror.NewInternal(fmt.Sprintf("UpdateDLQAckLevel operation failed. Error %v", err))
			}
			return nil
		}

		// Ignore possibly delayed message
		if clusterAckLevels[clusterName] > messageID {
			return nil
		}

		clusterAckLevels[clusterName] = messageID
		err = tx.UpdateAckLevels(q.getDLQTypeFromQueueType(), clusterAckLevels)
		if err != nil {
			return serviceerror.NewInternal(fmt.Sprintf("UpdateDLQAckLevel operation failed. Error %v", err))
		}
		return nil
	})

	if err != nil {
		return serviceerror.NewInternal(err.Error())
	}
	return nil
}

func (q *sqlQueue) GetDLQAckLevels() (map[string]int64, error) {

	return q.db.GetAckLevels(q.getDLQTypeFromQueueType(), false)
}

func (q *sqlQueue) getDLQTypeFromQueueType() persistence.QueueType {
	return -q.queueType
}
