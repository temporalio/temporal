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
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	EmptyPartition = 0
	// pageTokenPrefixByte is the first byte of the serialized page token. It's used to ensure that the page token is
	// not empty. Without this, if the last_read_message_id is 0, the serialized page token would be empty, and clients
	// could erroneously assume that there are no more messages beyond the first page. This is purely used to ensure
	// that tokens are non-empty; it is not used to verify that the token is valid like the magic byte in some other
	// protocols.
	pageTokenPrefixByte = 0
)

type (
	queueV2 struct {
		logger log.Logger
		SqlStore
	}

	QueueV2MetadataPayload struct {
		AckLevel int64
	}
)

// NewQueueV2 returns an implementation of persistence.QueueV2.
func NewQueueV2(db sqlplugin.DB,
	logger log.Logger) (persistence.QueueV2, error) {

	queue := &queueV2{
		SqlStore: NewSqlStore(db, logger),
		logger:   logger,
	}
	return queue, nil
}

func (q queueV2) EnqueueMessage(
	ctx context.Context,
	request *persistence.InternalEnqueueMessageRequest,
) (*persistence.InternalEnqueueMessageResponse, error) {
	var lastMessageID int64
	tx, err := q.Db.BeginTx(ctx)
	if err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("EnqueueMessage failed. Failed to start transaction. Error: %v", err))
	}
	lastMessageID, err = tx.GetLastEnqueuedMessageIDForUpdateV2(ctx, sqlplugin.QueueV2Filter{
		QueueType: request.QueueType,
		QueueName: request.QueueName,
		Partition: EmptyPartition,
	})
	switch err {
	case nil:
		lastMessageID = lastMessageID + 1
	case sql.ErrNoRows:
		lastMessageID = persistence.FirstQueueMessageID
	default:
		rollBackErr := tx.Rollback()
		if rollBackErr != nil {
			q.logger.Error("transaction rollback error", tag.Error(rollBackErr))
		}
		return nil, fmt.Errorf("failed to get last enqueued message id: %w", err)
	}
	_, err = tx.InsertIntoQueueV2Messages(ctx, []sqlplugin.QueueV2MessageRow{
		newQueueV2Row(request.QueueType, request.QueueName, lastMessageID, request.Blob),
	})
	if err != nil {
		rollBackErr := tx.Rollback()
		if rollBackErr != nil {
			q.logger.Error("transaction rollback error", tag.Error(rollBackErr))
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("EnqueueMessage failed. Failed to commit transaction. Error: %v", err))
	}
	return &persistence.InternalEnqueueMessageResponse{Metadata: persistence.MessageMetadata{ID: lastMessageID}}, err
}

func (q *queueV2) ReadMessages(
	ctx context.Context,
	request *persistence.InternalReadMessagesRequest,
) (*persistence.InternalReadMessagesResponse, error) {
	if request.PageSize <= 0 {
		return nil, persistence.ErrNonPositiveReadQueueMessagesPageSize
	}
	var minMessageID int64
	minMessageID, err := q.getMinMessageID(request)
	if err != nil {
		return nil, err
	}

	rows, err := q.Db.RangeSelectFromQueueV2Messages(ctx, sqlplugin.QueueV2MessagesFilter{
		QueueType:    request.QueueType,
		QueueName:    request.QueueName,
		Partition:    EmptyPartition,
		MinMessageID: minMessageID,
		PageSize:     request.PageSize,
	})

	if err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("RangeSelectFromQueueV2Messages operation failed. Error %v", err))
	}

	var messages []persistence.QueueV2Message
	for _, row := range rows {
		messages = append(messages, persistence.QueueV2Message{
			MetaData: persistence.MessageMetadata{ID: row.MessageID},
			Data:     *persistence.NewDataBlob(row.MessagePayload, row.MessageEncoding),
		})
	}
	nextPageToken := q.getNextPageToken(messages)

	response := &persistence.InternalReadMessagesResponse{
		Messages:      messages,
		NextPageToken: nextPageToken,
	}
	return response, nil
}

func (q *queueV2) getNextPageToken(result []persistence.QueueV2Message) []byte {
	if len(result) == 0 {
		return nil
	}
	lastReadMessageID := result[len(result)-1].MetaData.ID
	token := &persistencespb.ReadQueueMessagesNextPageToken{
		LastReadMessageId: lastReadMessageID,
	}
	// This can never fail if you inspect the implementation.
	b, _ := token.Marshal()

	// See the comment above pageTokenPrefixByte for why we want to do this.
	return append([]byte{pageTokenPrefixByte}, b...)
}

func (q *queueV2) getMinMessageID(request *persistence.InternalReadMessagesRequest) (int64, error) {
	// TODO: start from the ack level of the queue partition instead of the first message ID when there is no token.
	if len(request.NextPageToken) == 0 {
		return persistence.FirstQueueMessageID, nil
	}

	var token persistencespb.ReadQueueMessagesNextPageToken

	// Skip the first byte. See the comment on pageTokenPrefixByte for more details.
	err := token.Unmarshal(request.NextPageToken[1:])
	if err != nil {
		return 0, fmt.Errorf(
			"%w: %q: %v",
			persistence.ErrInvalidReadQueueMessagesNextPageToken,
			request.NextPageToken,
			err,
		)
	}

	return token.LastReadMessageId + 1, nil
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
		QueuePartition:  EmptyPartition,
		MessageID:       messageID,
		MessagePayload:  blob.Data,
		MessageEncoding: blob.EncodingType.String(),
	}
}
