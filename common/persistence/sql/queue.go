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

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

type (
	sqlQueue struct {
		queueType persistence.QueueType
		logger    log.Logger
		SqlStore
	}
)

func newQueue(
	db sqlplugin.DB,
	logger log.Logger,
	queueType persistence.QueueType,
) (persistence.Queue, error) {
	queue := &sqlQueue{
		SqlStore:  NewSqlStore(db, logger),
		queueType: queueType,
		logger:    logger,
	}
	return queue, nil
}

func (q *sqlQueue) Init(
	ctx context.Context,
	blob *commonpb.DataBlob,
) error {
	if err := q.initializeQueueMetadata(ctx, blob); err != nil {
		return err
	}
	if err := q.initializeDLQMetadata(ctx, blob); err != nil {
		return err
	}
	return nil
}

func (q *sqlQueue) EnqueueMessage(
	ctx context.Context,
	blob commonpb.DataBlob,
) error {
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
		return serviceerror.NewUnavailable(err.Error())
	}
	return nil
}

func (q *sqlQueue) ReadMessages(
	ctx context.Context,
	lastMessageID int64,
	pageSize int,
) ([]*persistence.QueueMessage, error) {
	rows, err := q.Db.RangeSelectFromMessages(ctx, sqlplugin.QueueMessagesRangeFilter{
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
	ctx context.Context,
	messageID int64,
) error {
	_, err := q.Db.RangeDeleteFromMessages(ctx, sqlplugin.QueueMessagesRangeFilter{
		QueueType:    q.queueType,
		MinMessageID: persistence.EmptyQueueMessageID,
		MaxMessageID: messageID - 1,
	})
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("DeleteMessagesBefore operation failed. Error %v", err))
	}
	return nil
}

func (q *sqlQueue) UpdateAckLevel(
	ctx context.Context,
	metadata *persistence.InternalQueueMetadata,
) error {
	err := q.txExecute(ctx, "UpdateAckLevel", func(tx sqlplugin.Tx) error {
		result, err := tx.UpdateQueueMetadata(ctx, &sqlplugin.QueueMetadataRow{
			QueueType:    q.queueType,
			Data:         metadata.Blob.Data,
			DataEncoding: metadata.Blob.EncodingType.String(),
			Version:      metadata.Version,
		})
		if err != nil {
			return serviceerror.NewUnavailable(fmt.Sprintf("UpdateAckLevel operation failed. Error %v", err))
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("rowsAffected returned error for queue metadata %v: %v", q.queueType, err)
		}
		if rowsAffected != 1 {
			return &persistence.ConditionFailedError{Msg: "UpdateAckLevel operation encountered concurrent write."}
		}
		return nil
	})

	if err != nil {
		return serviceerror.NewUnavailable(err.Error())
	}
	return nil
}

func (q *sqlQueue) GetAckLevels(
	ctx context.Context,
) (*persistence.InternalQueueMetadata, error) {
	row, err := q.Db.SelectFromQueueMetadata(ctx, sqlplugin.QueueMetadataFilter{
		QueueType: q.queueType,
	})
	if err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetAckLevels operation failed. Error %v", err))
	}

	return &persistence.InternalQueueMetadata{
		Blob:    persistence.NewDataBlob(row.Data, row.DataEncoding),
		Version: row.Version,
	}, nil
}

func (q *sqlQueue) EnqueueMessageToDLQ(
	ctx context.Context,
	blob commonpb.DataBlob,
) (int64, error) {
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
		return persistence.EmptyQueueMessageID, serviceerror.NewUnavailable(err.Error())
	}
	return lastMessageID + 1, nil
}

func (q *sqlQueue) ReadMessagesFromDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) ([]*persistence.QueueMessage, []byte, error) {
	if len(pageToken) != 0 {
		lastReadMessageID, err := deserializePageToken(pageToken)
		if err != nil {
			return nil, nil, serviceerror.NewInternal(fmt.Sprintf("invalid next page token %v", pageToken))
		}
		firstMessageID = lastReadMessageID
	}

	rows, err := q.Db.RangeSelectFromMessages(ctx, sqlplugin.QueueMessagesRangeFilter{
		QueueType:    q.getDLQTypeFromQueueType(),
		MinMessageID: firstMessageID,
		MaxMessageID: lastMessageID,
		PageSize:     pageSize,
	})
	if err != nil {
		return nil, nil, serviceerror.NewUnavailable(fmt.Sprintf("ReadMessagesFromDLQ operation failed. Error %v", err))
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
	ctx context.Context,
	messageID int64,
) error {
	_, err := q.Db.DeleteFromMessages(ctx, sqlplugin.QueueMessagesFilter{
		QueueType: q.getDLQTypeFromQueueType(),
		MessageID: messageID,
	})
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("DeleteMessageFromDLQ operation failed. Error %v", err))
	}
	return nil
}

func (q *sqlQueue) RangeDeleteMessagesFromDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
) error {
	_, err := q.Db.RangeDeleteFromMessages(ctx, sqlplugin.QueueMessagesRangeFilter{
		QueueType:    q.getDLQTypeFromQueueType(),
		MinMessageID: firstMessageID,
		MaxMessageID: lastMessageID,
	})
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("RangeDeleteMessagesFromDLQ operation failed. Error %v", err))
	}
	return nil
}

func (q *sqlQueue) UpdateDLQAckLevel(
	ctx context.Context,
	metadata *persistence.InternalQueueMetadata,
) error {
	err := q.txExecute(ctx, "UpdateDLQAckLevel", func(tx sqlplugin.Tx) error {

		result, err := tx.UpdateQueueMetadata(ctx, &sqlplugin.QueueMetadataRow{
			QueueType:    q.getDLQTypeFromQueueType(),
			Data:         metadata.Blob.Data,
			DataEncoding: metadata.Blob.EncodingType.String(),
		})
		if err != nil {
			return serviceerror.NewUnavailable(fmt.Sprintf("UpdateDLQAckLevel operation failed. Error %v", err))
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
		return serviceerror.NewUnavailable(err.Error())
	}
	return nil
}

func (q *sqlQueue) GetDLQAckLevels(
	ctx context.Context,
) (*persistence.InternalQueueMetadata, error) {
	row, err := q.Db.SelectFromQueueMetadata(ctx, sqlplugin.QueueMetadataFilter{
		QueueType: q.getDLQTypeFromQueueType(),
	})
	if err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetDLQAckLevels operation failed. Error %v", err))
	}

	return &persistence.InternalQueueMetadata{
		Blob:    persistence.NewDataBlob(row.Data, row.DataEncoding),
		Version: row.Version,
	}, nil
}

func (q *sqlQueue) getDLQTypeFromQueueType() persistence.QueueType {
	return -q.queueType
}

func (q *sqlQueue) initializeQueueMetadata(
	ctx context.Context,
	blob *commonpb.DataBlob,
) error {
	_, err := q.Db.SelectFromQueueMetadata(ctx, sqlplugin.QueueMetadataFilter{
		QueueType: q.queueType,
	})
	switch err {
	case nil:
		return nil
	case sql.ErrNoRows:
		result, err := q.Db.InsertIntoQueueMetadata(ctx, &sqlplugin.QueueMetadataRow{
			QueueType:    q.queueType,
			Data:         blob.Data,
			DataEncoding: blob.EncodingType.String(),
		})
		if err != nil {
			return serviceerror.NewUnavailable(fmt.Sprintf("initializeQueueMetadata operation failed. Error %v", err))
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
	blob *commonpb.DataBlob,
) error {
	_, err := q.Db.SelectFromQueueMetadata(ctx, sqlplugin.QueueMetadataFilter{
		QueueType: q.getDLQTypeFromQueueType(),
	})
	switch err {
	case nil:
		return nil
	case sql.ErrNoRows:
		result, err := q.Db.InsertIntoQueueMetadata(ctx, &sqlplugin.QueueMetadataRow{
			QueueType:    q.getDLQTypeFromQueueType(),
			Data:         blob.Data,
			DataEncoding: blob.EncodingType.String(),
		})
		if err != nil {
			return serviceerror.NewUnavailable(fmt.Sprintf("initializeDLQMetadata operation failed. Error %v", err))
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
