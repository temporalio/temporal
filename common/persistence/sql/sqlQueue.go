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
	"context"
	"database/sql"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
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

	ctx, cancel := newVisibilityContext()
	defer cancel()
	if err := queue.initializeQueueMetadata(ctx); err != nil {
		return nil, err
	}
	if err := queue.initializeDLQMetadata(ctx); err != nil {
		return nil, err
	}

	return queue, nil
}

func (q *sqlQueue) EnqueueMessage(
	blob commonpb.DataBlob,
) error {
	ctx, cancel := newExecutionContext()
	defer cancel()
	err := q.txExecute(ctx, "EnqueueMessage", func(tx sqlplugin.Tx) error {
		lastMessageID, err := tx.GetLastEnqueuedMessageIDForUpdate(ctx, q.queueType)
		switch err {
		case nil:
			_, err = tx.InsertIntoMessages(ctx, []sqlplugin.QueueMessageRow{
				newQueueRow(q.queueType, lastMessageID+1, blob),
			})
			return err
		case sql.ErrNoRows:
			_, err = tx.InsertIntoMessages(ctx, []sqlplugin.QueueMessageRow{
				newQueueRow(q.queueType, persistence.EmptyQueueMessageID+1, blob),
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
	ctx, cancel := newExecutionContext()
	defer cancel()
	rows, err := q.db.RangeSelectFromMessages(ctx, sqlplugin.QueueMessagesRangeFilter{
		QueueType:    q.queueType,
		MinMessageID: lastMessageID,
		MaxMessageID: persistence.MaxQueueMessageID,
		PageSize:     pageSize,
	})
	if err != nil {
		return nil, err
	}

	var messages []*persistence.QueueMessage
	for _, row := range rows {
		messages = append(messages, &persistence.QueueMessage{
			QueueType: q.queueType,
			ID:        row.MessageID,
			Data:      row.MessagePayload,
			Encoding:  row.MessageEncoding,
		})
	}
	return messages, nil
}

func (q *sqlQueue) DeleteMessagesBefore(
	messageID int64,
) error {
	ctx, cancel := newExecutionContext()
	defer cancel()
	_, err := q.db.RangeDeleteFromMessages(ctx, sqlplugin.QueueMessagesRangeFilter{
		QueueType:    q.queueType,
		MinMessageID: persistence.EmptyQueueMessageID,
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
	ctx, cancel := newExecutionContext()
	defer cancel()
	err := q.txExecute(ctx, "UpdateAckLevel", func(tx sqlplugin.Tx) error {
		row, err := tx.LockQueueMetadata(ctx, sqlplugin.QueueMetadataFilter{
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

		result, err := tx.UpdateQueueMetadata(ctx, &sqlplugin.QueueMetadataRow{
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
	ctx, cancel := newExecutionContext()
	defer cancel()
	row, err := q.db.SelectFromQueueMetadata(ctx, sqlplugin.QueueMetadataFilter{
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
	blob commonpb.DataBlob,
) (int64, error) {
	ctx, cancel := newExecutionContext()
	defer cancel()
	var lastMessageID int64
	err := q.txExecute(ctx, "EnqueueMessageToDLQ", func(tx sqlplugin.Tx) error {
		var err error
		lastMessageID, err = tx.GetLastEnqueuedMessageIDForUpdate(ctx, q.getDLQTypeFromQueueType())
		switch err {
		case nil:
			_, err = tx.InsertIntoMessages(ctx, []sqlplugin.QueueMessageRow{
				newQueueRow(q.getDLQTypeFromQueueType(), lastMessageID+1, blob),
			})
			return err
		case sql.ErrNoRows:
			_, err = tx.InsertIntoMessages(ctx, []sqlplugin.QueueMessageRow{
				newQueueRow(q.getDLQTypeFromQueueType(), persistence.EmptyQueueMessageID+1, blob),
			})
			return err
		default:
			return fmt.Errorf("failed to get last enqueued message id from DLQ: %v", err)
		}
	})
	if err != nil {
		return persistence.EmptyQueueMessageID, serviceerror.NewInternal(err.Error())
	}
	return lastMessageID + 1, nil
}

func (q *sqlQueue) ReadMessagesFromDLQ(
	firstMessageID int64,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) ([]*persistence.QueueMessage, []byte, error) {
	ctx, cancel := newExecutionContext()
	defer cancel()
	if pageToken != nil && len(pageToken) != 0 {
		lastReadMessageID, err := deserializePageToken(pageToken)
		if err != nil {
			return nil, nil, serviceerror.NewInternal(fmt.Sprintf("invalid next page token %v", pageToken))
		}
		firstMessageID = lastReadMessageID
	}

	rows, err := q.db.RangeSelectFromMessages(ctx, sqlplugin.QueueMessagesRangeFilter{
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
		messages = append(messages, &persistence.QueueMessage{
			QueueType: q.getDLQTypeFromQueueType(),
			ID:        row.MessageID,
			Data:      row.MessagePayload,
			Encoding:  row.MessageEncoding,
		})
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
	ctx, cancel := newExecutionContext()
	defer cancel()
	_, err := q.db.DeleteFromMessages(ctx, sqlplugin.QueueMessagesFilter{
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
	ctx, cancel := newExecutionContext()
	defer cancel()
	_, err := q.db.RangeDeleteFromMessages(ctx, sqlplugin.QueueMessagesRangeFilter{
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
	ctx, cancel := newExecutionContext()
	defer cancel()
	err := q.txExecute(ctx, "UpdateDLQAckLevel", func(tx sqlplugin.Tx) error {
		row, err := tx.LockQueueMetadata(ctx, sqlplugin.QueueMetadataFilter{
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

		result, err := tx.UpdateQueueMetadata(ctx, &sqlplugin.QueueMetadataRow{
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
	ctx, cancel := newExecutionContext()
	defer cancel()
	row, err := q.db.SelectFromQueueMetadata(ctx, sqlplugin.QueueMetadataFilter{
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

func (q *sqlQueue) initializeQueueMetadata(
	ctx context.Context,
) error {
	_, err := q.db.SelectFromQueueMetadata(ctx, sqlplugin.QueueMetadataFilter{
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
		result, err := q.db.InsertIntoQueueMetadata(ctx, &sqlplugin.QueueMetadataRow{
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

func (q *sqlQueue) initializeDLQMetadata(
	ctx context.Context,
) error {
	_, err := q.db.SelectFromQueueMetadata(ctx, sqlplugin.QueueMetadataFilter{
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
		result, err := q.db.InsertIntoQueueMetadata(ctx, &sqlplugin.QueueMetadataRow{
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

func newQueueRow(
	queueType persistence.QueueType,
	messageID int64,
	blob commonpb.DataBlob,
) sqlplugin.QueueMessageRow {

	return sqlplugin.QueueMessageRow{
		QueueType:       queueType,
		MessageID:       messageID,
		MessagePayload:  blob.Data,
		MessageEncoding: blob.EncodingType.String(),
	}
}
