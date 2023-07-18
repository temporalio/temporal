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
	"encoding/json"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	EmptyPartition = 0
)

type (
	sqlQueueV2 struct {
		logger log.Logger
		SqlStore
	}

	// TODO: add a proto at some point
	QueueV2MetadataPayload struct {
		AckLevel int64
	}
)

func newQueueV2(
	db sqlplugin.DB,
	logger log.Logger,
	queueType persistence.QueueV2Type,
) (persistence.QueueV2, error) {
	queue := &sqlQueueV2{
		SqlStore: NewSqlStore(db, logger),
		logger:   logger,
	}
	return queue, nil
}

func (q *sqlQueueV2) CreateQueue(
	ctx context.Context,
	request persistence.InternalCreateQueueRequest,
) (*persistence.InternalCreateQueueResponse, error) {
	response := &persistence.InternalCreateQueueResponse{}
	_, err := q.Db.SelectFromQueueV2Metadata(ctx, sqlplugin.QueueV2MetadataFilter{
		QueueType: request.QueueType,
		QueueName: request.QueueName,
	})
	switch err {
	case nil:
		// already exists
		return response, nil
	case sql.ErrNoRows:
		version := 0
		blob, err := convertQueueV2MetadataToBlob(&QueueV2MetadataPayload{AckLevel: 0})
		result, err := q.Db.InsertIntoQueueV2Metadata(ctx, &sqlplugin.QueueV2MetadataRow{
			QueueType:       request.QueueType,
			QueueName:       request.QueueName,
			Payload:         blob.Data,
			PayloadEncoding: blob.EncodingType.String(),
			Version:         int64(version),
		})
		if err != nil {
			return nil, serviceerror.NewUnavailable(fmt.Sprintf("initializeQueueMetadata operation failed. Error %v", err))
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf("rowsAffected returned error when initializing queue metadata  %v: %v", request.QueueType, err)
		}
		if rowsAffected != 1 {
			return nil, fmt.Errorf("rowsAffected returned %v queue metadata instead of one", rowsAffected)
		}
		return response, nil
	default:
		return nil, err
	}
}

func (q *sqlQueueV2) EnqueueMessage(
	ctx context.Context,
	request persistence.InternalEnqueueMessageRequest,
) (*persistence.InternalEnqueueMessageResponse, error) {
	var lastMessageID int64
	err := q.txExecute(ctx, "EnqueueMessage", func(tx sqlplugin.Tx) error {
		lastMessageID, err := tx.GetLastEnqueuedMessageIDForUpdateV2(ctx, sqlplugin.QueueV2Filter{
			QueueType: request.QueueType,
			QueueName: request.QueueName,
			Partition: EmptyPartition,
		},
		)
		switch err {
		case nil:
			_, err = tx.InsertIntoQueueV2Messages(ctx, []sqlplugin.QueueV2MessageRow{
				newQueueV2Row(request.QueueType, request.QueueName, lastMessageID+1, request.Blob),
			})
			return err
		case sql.ErrNoRows:
			_, err = tx.InsertIntoQueueV2Messages(ctx, []sqlplugin.QueueV2MessageRow{
				newQueueV2Row(request.QueueType, request.QueueName, persistence.EmptyQueueMessageID+1, request.Blob),
			})
			return err
		default:
			return fmt.Errorf("failed to get last enqueued message id: %v", err)
		}
	})
	if err != nil {
		return nil, serviceerror.NewUnavailable(err.Error())
	}
	return &persistence.InternalEnqueueMessageResponse{Metadata: persistence.MessageMetadata{ID: lastMessageID}}, nil
}

func (q *sqlQueueV2) ListQueues(
	ctx context.Context,
	request persistence.InternalListQueuesRequest,
) (*persistence.InternalListQueuesResponse, error) {
	var minMessageID int64
	var numRows int64
	var queueNames []string
	if len(request.NextPageToken) != 0 {
		lastReadMessageID, err := deserializePageToken(request.NextPageToken)
		if err != nil {
			return nil, serviceerror.NewInternal(fmt.Sprintf("invalid next page token %v", request.NextPageToken))
		}
		minMessageID = lastReadMessageID
	}
	err := q.txExecute(ctx, "EnqueueMessage", func(tx sqlplugin.Tx) error {
		rows, err := tx.SelectNameFromQueueV2Metadata(ctx, sqlplugin.QueueV2MetadataTypeFilter{
			QueueType:    request.QueueType,
			PageSize:     request.PageSize,
			MinMessageID: minMessageID,
		},
		)
		switch err {
		case nil:
			numRows = int64(len(rows))
			for _, row := range rows {
				queueNames = append(queueNames, row.QueueName)
			}
			return nil
		case sql.ErrNoRows:
			return nil
		default:
			return fmt.Errorf("failed to fetch list of queues: %v", err)
		}
	})
	if err != nil {
		return nil, serviceerror.NewUnavailable(err.Error())
	}
	return &persistence.InternalListQueuesResponse{
		QueueNames:    queueNames,
		NextPageToken: serializePageToken(int64(minMessageID + numRows)),
	}, nil

}

func (q *sqlQueueV2) RangeDeleteMessages(
	ctx context.Context,
	request persistence.InternalRangeDeleteMessagesRequest,
) (*persistence.InternalRangeDeleteMessagesResponse, error) {
	ackLevel, version, err := q.getCurrentAckLevelAndVersion(ctx, request.QueueType, request.QueueName)
	if err != nil {
		return nil, err
	}
	_, err = q.Db.RangeDeleteFromQueueV2Messages(ctx, sqlplugin.QueueV2MessagesRangeFilter{
		QueueType:    request.QueueType,
		MinMessageID: ackLevel,
		MaxMessageID: request.InclusiveMaxMessageMetadata.ID,
	})
	if err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("DeleteMessagesBefore operation failed. Error %v", err))
	}

	return &persistence.InternalRangeDeleteMessagesResponse{}, q.updateAckLevel(ctx, request, version)
}

func (q *sqlQueueV2) ReadMessages(
	ctx context.Context,
	request persistence.InternalReadMessagesRequest,
) (*persistence.InternalReadMessagesResponse, error) {
	var minMessageID int64
	if len(request.NextPageToken.PageToken) != 0 {
		lastReadMessageID, err := deserializePageToken(request.NextPageToken.PageToken)
		if err != nil {
			return nil, serviceerror.NewInternal(fmt.Sprintf("invalid next page token %v", request.NextPageToken.PageToken))
		}
		minMessageID = lastReadMessageID
	} else {
		ackLevel, _, err := q.getCurrentAckLevelAndVersion(ctx, request.QueueType, request.QueueName)
		if err != nil {
			return nil, err
		}
		minMessageID = ackLevel
	}

	rows, err := q.Db.RangeSelectFromQueueV2Messages(ctx, sqlplugin.QueueV2MessagesRangeFilter{
		QueueType:    request.QueueType,
		MinMessageID: minMessageID,
		PageSize:     request.PageSize,
	})
	if err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("ReadMessagesFromDLQ operation failed. Error %v", err))
	}

	var messages []persistence.Message
	for _, row := range rows {
		messages = append(messages, persistence.Message{
			MetaData: persistence.MessageMetadata{ID: row.MessageID},
			Data:     *persistence.NewDataBlob(row.MessagePayload, row.MessageEncoding),
		})
	}

	var newPagingToken []byte
	lastReadMessageID := int64(0)
	if messages != nil && len(messages) >= request.PageSize {
		lastReadMessageID = messages[len(messages)-1].MetaData.ID
		newPagingToken = serializePageToken(int64(lastReadMessageID))
	}
	response := &persistence.InternalReadMessagesResponse{
		Messages:      messages,
		NextPageToken: persistence.InternalReadMessagePageToken{MessageID: lastReadMessageID, PageToken: newPagingToken},
	}
	return response, nil
}

func (q *sqlQueueV2) getCurrentAckLevelAndVersion(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
) (ackLevel int64, version int64, err error) {
	// get current ack level and assign to minMessageID
	err = q.txExecute(ctx, "EnqueueMessage", func(tx sqlplugin.Tx) error {
		metaDataRow, err := tx.SelectFromQueueV2Metadata(ctx, sqlplugin.QueueV2MetadataFilter{
			QueueType: queueType,
			QueueName: queueName,
		},
		)
		switch err {
		case nil:
			ackLevel, err = getAckLevelFromQueueV2Metadata(metaDataRow.Payload, metaDataRow.PayloadEncoding)
			if err != nil {
				return fmt.Errorf("failed to get queue with type and name: %v, %v. got error: %v", queueType, queueName, err)
			}
			return nil
		case sql.ErrNoRows:
			return serviceerror.NewUnavailable(fmt.Sprintf("queue of Type %v and with Name %v could not be found", queueType, queueName))
		default:
			return fmt.Errorf("failed to get queue with type and name: %v, %v", queueType, queueName)
		}
	})
	if err != nil {
		return 0, 0, serviceerror.NewUnavailable(err.Error())
	}

	return ackLevel, version, nil
}

func (q *sqlQueueV2) updateAckLevel(ctx context.Context, request persistence.InternalRangeDeleteMessagesRequest, version int64) error {

	return nil
}

// TODO: add these to to common/persistence/serialization/blob.go at some point
func convertQueueV2MetadataToBlob(payload *QueueV2MetadataPayload) (*commonpb.DataBlob, error) {
	// TODO: use proto here?
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	blob := &commonpb.DataBlob{
		Data:         data,
		EncodingType: enumspb.ENCODING_TYPE_JSON,
	}
	return blob, nil
}

func getAckLevelFromQueueV2Metadata(payload []byte, encoding string) (int64, error) {
	if encoding != enumspb.ENCODING_TYPE_JSON.String() {
		return 0, fmt.Errorf("unsupported encoding type: %v", encoding)
	}
	var queueMetadata QueueV2MetadataPayload
	if err := json.Unmarshal(payload, &queueMetadata); err != nil {
		return 0, err
	}
	return queueMetadata.AckLevel, nil
}

func newQueueV2Row(
	queueType persistence.QueueV2Type,
	queueName string,
	messageID int64,
	blob commonpb.DataBlob,
) sqlplugin.QueueV2MessageRow {

	return sqlplugin.QueueV2MessageRow{
		QueueType:       queueType,
		QueueName:       queueName,
		Partition:       EmptyPartition,
		MessageID:       messageID,
		MessagePayload:  blob.Data,
		MessageEncoding: blob.EncodingType.String(),
	}
}
