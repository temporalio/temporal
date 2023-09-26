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

package cassandra

import (
	"context"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/persistence/serialization"
)

type (
	// queueV2Store contains the SQL queries and serialization/deserialization functions to interact with the queues and
	// queue_messages tables that implement the QueueV2 interface. The schema is located at:
	//	schema/cassandra/temporal/versioned/v1.9/queues.cql
	queueV2Store struct {
		session gocql.Session
		logger  log.Logger
	}
)

const (
	TemplateEnqueueMessageQuery  = `INSERT INTO queue_messages (queue_type, queue_name, queue_partition, message_id, message_payload, message_encoding) VALUES (?, ?, ?, ?, ?, ?) IF NOT EXISTS`
	TemplateGetMessagesQuery     = `SELECT message_id, message_payload, message_encoding FROM queue_messages WHERE queue_type = ? AND queue_name = ? AND queue_partition = ? AND message_id >= ? ORDER BY message_id ASC LIMIT ?`
	TemplateGetMaxMessageIDQuery = `SELECT message_id FROM queue_messages WHERE queue_type = ? AND queue_name = ? AND queue_partition = ? ORDER BY message_id DESC LIMIT 1`
	TemplateCreateQueueQuery     = `INSERT INTO queues (queue_type, queue_name, metadata_payload, metadata_encoding, version) VALUES (?, ?, ?, ?, ?) IF NOT EXISTS`
	TemplateGetQueueQuery        = `SELECT metadata_payload, metadata_encoding, version FROM queues WHERE queue_type = ? AND queue_name = ?`

	// QueueMessageIDConflict will be part of the error message when a message with the same ID already exists in the
	// queue. This is possible when there are concurrent writes to the queue because we enqueue a message using two
	// queries:
	//
	// 	1. SELECT MAX(ID) to get the next message ID (for a given queue partition)
	// 	2. INSERT (ID, message) with CAS
	//
	// See the following example:
	//
	//  Client A           Client B                  Cassandra DB
	//  |                  |                                    |
	//  |---1. SELECT MAX(ID)---------------------------------->|
	//  |                  |                                    |
	//  |<--2. Return ID X--------------------------------------|
	//  |                  |                                    |
	//  |                  |---3. SELECT MAX(ID)--------------->|
	//  |                  |                                    |
	//  |                  |<--4. Return ID X-------------------|
	//  |                  |                                    |
	//  |---5. INSERT (X+1, msgA) with CAS--------------------->|
	//  |                  |                                    |
	//  |<--6. Acknowledge--------------------------------------|
	//  |                  |                                    |
	//  |                  |---7. INSERT (X+1, msgB) with CAS-->|
	//  |                  |                                    |
	//  |                  |<--8. Conflict/Error----------------|
	//  |                  |                                    |
	QueueMessageIDConflict = "queue message with id already exists, likely due to concurrent writes"

	// pageTokenPrefixByte is the first byte of the serialized page token. It's used to ensure that the page token is
	// not empty. Without this, if the last_read_message_id is 0, the serialized page token would be empty, and clients
	// could erroneously assume that there are no more messages beyond the first page. This is purely used to ensure
	// that tokens are non-empty; it is not used to verify that the token is valid like the magic byte in some other
	// protocols.
	pageTokenPrefixByte = 0
)

func NewQueueV2Store(session gocql.Session, logger log.Logger) persistence.QueueV2 {
	return &queueV2Store{
		session: session,
		logger:  logger,
	}
}

func (q *queueV2Store) EnqueueMessage(
	ctx context.Context,
	request *persistence.InternalEnqueueMessageRequest,
) (*persistence.InternalEnqueueMessageResponse, error) {
	// TODO: add concurrency control around this method to avoid things like QueueMessageIDConflict.
	// TODO: cache the queue in memory to avoid querying the database every time.
	_, err := q.getQueue(ctx, request.QueueType, request.QueueName)
	if err != nil {
		return nil, err
	}
	messageID, err := q.getNextMessageID(ctx, request.QueueType, request.QueueName)
	if err != nil {
		return nil, err
	}

	err = q.tryInsert(ctx, request.QueueType, request.QueueName, request.Blob, messageID)
	if err != nil {
		return nil, err
	}

	return &persistence.InternalEnqueueMessageResponse{
		Metadata: persistence.MessageMetadata{ID: messageID},
	}, nil
}

func (q *queueV2Store) ReadMessages(
	ctx context.Context,
	request *persistence.InternalReadMessagesRequest,
) (*persistence.InternalReadMessagesResponse, error) {
	queue, err := q.getQueue(ctx, request.QueueType, request.QueueName)
	if err != nil {
		return nil, err
	}
	if request.PageSize <= 0 {
		return nil, persistence.ErrNonPositiveReadQueueMessagesPageSize
	}
	minMessageID, err := q.getMinMessageID(request.QueueType, request.QueueName, request.NextPageToken, queue)
	if err != nil {
		return nil, err
	}

	iter := q.session.Query(
		TemplateGetMessagesQuery,
		request.QueueType,
		request.QueueName,
		0,
		minMessageID,
		request.PageSize,
	).WithContext(ctx).Iter()

	var (
		messages []persistence.QueueV2Message
		// messageID is the ID of the last message returned by the query.
		messageID int64
	)

	for {
		var (
			messagePayload  []byte
			messageEncoding string
		)
		if !iter.Scan(&messageID, &messagePayload, &messageEncoding) {
			break
		}
		encoding, ok := enums.EncodingType_value[messageEncoding]
		if !ok {
			return nil, serialization.NewUnknownEncodingTypeError(messageEncoding)
		}

		encodingType := enums.EncodingType(encoding)

		message := persistence.QueueV2Message{
			MetaData: persistence.MessageMetadata{ID: messageID},
			Data: commonpb.DataBlob{
				EncodingType: encodingType,
				Data:         messagePayload,
			},
		}
		messages = append(messages, message)
	}

	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("QueueV2ReadMessages", err)
	}

	nextPageToken := q.getNextPageToken(messages, messageID)

	return &persistence.InternalReadMessagesResponse{
		Messages:      messages,
		NextPageToken: nextPageToken,
	}, nil
}

func (q *queueV2Store) CreateQueue(
	ctx context.Context,
	request *persistence.InternalCreateQueueRequest,
) (*persistence.InternalCreateQueueResponse, error) {
	queueType := request.QueueType
	queueName := request.QueueName
	queue := persistencespb.Queue{
		Partitions: map[int32]*persistencespb.QueuePartition{
			0: {
				MinMessageId: persistence.FirstQueueMessageID,
			},
		},
	}
	bytes, _ := queue.Marshal()
	applied, err := q.session.Query(
		TemplateCreateQueueQuery,
		queueType,
		queueName,
		bytes,
		enums.ENCODING_TYPE_PROTO3.String(),
		0,
	).WithContext(ctx).MapScanCAS(make(map[string]interface{}))
	if err != nil {
		return nil, gocql.ConvertError("QueueV2CreateQueue", err)
	}

	if !applied {
		return nil, fmt.Errorf(
			"%w: queue type %v and name %v",
			persistence.ErrQueueAlreadyExists,
			queueType,
			queueName,
		)
	}
	return &persistence.InternalCreateQueueResponse{}, nil
}

func (q *queueV2Store) getMinMessageID(queueType persistence.QueueV2Type, name string, nextPageToken []byte, queue *persistencespb.Queue) (int, error) {
	if len(nextPageToken) == 0 {
		// Currently, we only have one partition for each queue. However, that might change in the future. If a queue is
		// created with more than 1 partition by a server on a future release, and then that server is downgraded, we
		// will need to handle this case. Since all DLQ tasks are retried infinitely, we just return an error.
		numPartitions := len(queue.Partitions)
		if numPartitions != 1 {
			return 0, serviceerror.NewInternal(
				fmt.Sprintf(
					"queue with type %v and name %v has %d partitions, but this implementation only supports"+
						" queues with 1 partition. Did you downgrade your Temporal server?",
					queueType,
					name,
					numPartitions,
				),
			)
		}
		return int(queue.Partitions[0].MinMessageId), nil
	}

	var token persistencespb.ReadQueueMessagesNextPageToken

	// Skip the first byte. See the comment on pageTokenPrefixByte for more details.
	err := token.Unmarshal(nextPageToken[1:])
	if err != nil {
		return 0, fmt.Errorf(
			"%w: %q: %v",
			persistence.ErrInvalidReadQueueMessagesNextPageToken,
			nextPageToken,
			err,
		)
	}

	return int(token.LastReadMessageId) + 1, nil
}

func (q *queueV2Store) getNextPageToken(result []persistence.QueueV2Message, messageID int64) []byte {
	if len(result) == 0 {
		return nil
	}

	token := &persistencespb.ReadQueueMessagesNextPageToken{
		LastReadMessageId: messageID,
	}
	// This can never fail if you inspect the implementation.
	b, _ := token.Marshal()

	// See the comment above pageTokenPrefixByte for why we want to do this.
	return append([]byte{pageTokenPrefixByte}, b...)
}

func (q *queueV2Store) tryInsert(ctx context.Context, queueType persistence.QueueV2Type, queueName string, blob commonpb.DataBlob, messageID int64) error {
	applied, err := q.session.Query(
		TemplateEnqueueMessageQuery,
		queueType,
		queueName,
		0,
		messageID,
		blob.Data,
		blob.EncodingType.String(),
	).WithContext(ctx).MapScanCAS(make(map[string]interface{}))
	if err != nil {
		return gocql.ConvertError("QueueV2EnqueueMessage", err)
	}

	if !applied {
		return &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("%s: insert with message ID %v was not applied", QueueMessageIDConflict, messageID),
		}
	}

	return nil
}

func (q *queueV2Store) getNextMessageID(ctx context.Context, queueType persistence.QueueV2Type, queueName string) (int64, error) {
	var maxMessageID int64

	err := q.session.Query(TemplateGetMaxMessageIDQuery, queueType, queueName, 0).WithContext(ctx).Scan(&maxMessageID)
	if err != nil {
		if gocql.IsNotFoundError(err) {
			// There are no messages in the queue, so the next message ID is the first message ID.
			return persistence.FirstQueueMessageID, nil
		}
		return 0, gocql.ConvertError("QueueV2GetMaxMessageID", err)
	}

	// The next message ID is the max message ID + 1.
	return maxMessageID + 1, nil
}

func (q *queueV2Store) getQueue(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	name string,
) (*persistencespb.Queue, error) {
	var (
		queueBytes       []byte
		queueEncodingStr string
		version          int64
	)

	err := q.session.Query(TemplateGetQueueQuery, queueType, name).WithContext(ctx).Scan(
		&queueBytes,
		&queueEncodingStr,
		&version,
	)
	if err != nil {
		if gocql.IsNotFoundError(err) {
			return nil, persistence.NewQueueNotFoundError(queueType, name)
		}
		return nil, gocql.ConvertError("QueueV2GetQueue", err)
	}

	if queueEncodingStr != enums.ENCODING_TYPE_PROTO3.String() {
		return nil, fmt.Errorf(
			"queue with type %v and name %v has invalid encoding: %w",
			queueType,
			name,
			serialization.NewUnknownEncodingTypeError(queueEncodingStr, enums.ENCODING_TYPE_PROTO3),
		)
	}

	queue := &persistencespb.Queue{}
	err = queue.Unmarshal(queueBytes)
	if err != nil {
		return nil, serialization.NewDeserializationError(
			enums.ENCODING_TYPE_PROTO3,
			fmt.Errorf("unmarshal payload for queue with type %v and name %v failed: %w", queueType, name, err),
		)
	}

	return queue, nil
}
