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

	Queue struct {
		Metadata *persistencespb.Queue
		Version  int64
	}
)

const (
	TemplateEnqueueMessageQuery      = `INSERT INTO queue_messages (queue_type, queue_name, queue_partition, message_id, message_payload, message_encoding) VALUES (?, ?, ?, ?, ?, ?) IF NOT EXISTS`
	TemplateGetMessagesQuery         = `SELECT message_id, message_payload, message_encoding FROM queue_messages WHERE queue_type = ? AND queue_name = ? AND queue_partition = ? AND message_id >= ? ORDER BY message_id ASC LIMIT ?`
	TemplateGetMaxMessageIDQuery     = `SELECT message_id FROM queue_messages WHERE queue_type = ? AND queue_name = ? AND queue_partition = ? ORDER BY message_id DESC LIMIT 1`
	TemplateCreateQueueQuery         = `INSERT INTO queues (queue_type, queue_name, metadata_payload, metadata_encoding, version) VALUES (?, ?, ?, ?, ?) IF NOT EXISTS`
	TemplateGetQueueQuery            = `SELECT metadata_payload, metadata_encoding, version FROM queues WHERE queue_type = ? AND queue_name = ?`
	TemplateRangeDeleteMessagesQuery = `DELETE FROM queue_messages WHERE queue_type = ? AND queue_name = ? AND queue_partition = ? AND message_id >= ? AND message_id <= ?`
	TemplateUpdateQueueMetadataQuery = `UPDATE queues SET metadata_payload = ?, metadata_encoding = ?, version = ? WHERE queue_type = ? AND queue_name = ? IF version = ?`

	// pageTokenPrefixByte is the first byte of the serialized page token. It's used to ensure that the page token is
	// not empty. Without this, if the last_read_message_id is 0, the serialized page token would be empty, and clients
	// could erroneously assume that there are no more messages beyond the first page. This is purely used to ensure
	// that tokens are non-empty; it is not used to verify that the token is valid like the magic byte in some other
	// protocols.
	pageTokenPrefixByte = 0
)

var (
	// ErrEnqueueMessageConflict is returned when a message with the same ID already exists in the queue. This is
	// possible when there are concurrent writes to the queue because we enqueue a message using two queries:
	//
	// 	1. SELECT MAX(ID) to get the next message ID (for a given queue partition)
	// 	2. INSERT (ID, message) with IF NOT EXISTS
	//
	// See the following example:
	//
	//  Client A           Client B                          Cassandra DB
	//  |                  |                                            |
	//  |--1. SELECT MAX(ID) FROM queue_messages----------------------->|
	//  |                  |                                            |
	//  |<-2. Return X--------------------------------------------------|
	//  |                  |                                            |
	//  |                  |--3. SELECT MAX(ID) FROM queue_messages---->|
	//  |                  |                                            |
	//  |                  |<-4. Return X-------------------------------|
	//  |                  |                                            |
	//  |--5. INSERT INTO queue_messages (ID = X)---------------------->|
	//  |                  |                                            |
	//  |<-6. Acknowledge-----------------------------------------------|
	//  |                  |                                            |
	//  |                  |--7. INSERT INTO queue_messages (ID = X)--->|
	//  |                  |                                            |
	//  |                  |<-8. Conflict/Error-------------------------|
	//  |                  |                                            |
	ErrEnqueueMessageConflict = &persistence.ConditionFailedError{
		Msg: "conflict inserting queue message, likely due to concurrent writes",
	}
	// ErrUpdateQueueConflict is returned when a queue is updated with the wrong version. This happens when there are
	// concurrent writes to the queue because we update a queue using two queries, similar to the enqueue message query.
	//
	// 	1. SELECT (queue, version) FROM queues
	// 	2. UPDATE queue, version IF version = version from step 1
	//
	// See the following example:
	//
	//  Client A           Client B                           Cassandra DB
	//  |                  |                                            |
	//  |--1. SELECT (queue, version) FROM queues---------------------->|
	//  |                  |                                            |
	//  |<-2. Return (queue, v1)----------------------------------------|
	//  |                  |                                            |
	//  |                  |--3. SELECT (queue, version) FROM queues--->|
	//  |                  |                                            |
	//  |                  |<-4. Return (queue, v1)---------------------|
	//  |                  |                                            |
	//  |--5. UPDATE queue, version IF version = v1-------------------->|
	//  |                  |                                            |
	//  |<-6. Acknowledge-----------------------------------------------|
	//  |                  |                                            |
	//  |                  |--7. UPDATE queue, version IF version = v1->|
	//  |                  |                                            |
	//  |                  |<-8. Conflict/Error-------------------------|
	//  |                  |                                            |
	ErrUpdateQueueConflict = &persistence.ConditionFailedError{
		Msg: "conflict updating queue, likely due to concurrent writes",
	}
)

func NewQueueV2Store(session gocql.Session, logger log.Logger) persistence.QueueV2 {
	return &queueV2Store{
		session: session,
		logger:  logger,
	}
}

func (s *queueV2Store) EnqueueMessage(
	ctx context.Context,
	request *persistence.InternalEnqueueMessageRequest,
) (*persistence.InternalEnqueueMessageResponse, error) {
	// TODO: add concurrency control around this method to avoid things like QueueMessageIDConflict.
	// TODO: cache the queue in memory to avoid querying the database every time.
	_, err := s.getQueue(ctx, request.QueueType, request.QueueName)
	if err != nil {
		return nil, err
	}
	messageID, err := s.getNextMessageID(ctx, request.QueueType, request.QueueName)
	if err != nil {
		return nil, err
	}
	err = s.tryInsert(ctx, request.QueueType, request.QueueName, request.Blob, messageID)
	if err != nil {
		return nil, err
	}
	return &persistence.InternalEnqueueMessageResponse{
		Metadata: persistence.MessageMetadata{ID: messageID},
	}, nil
}

func (s *queueV2Store) ReadMessages(
	ctx context.Context,
	request *persistence.InternalReadMessagesRequest,
) (*persistence.InternalReadMessagesResponse, error) {
	q, err := s.getQueue(ctx, request.QueueType, request.QueueName)
	if err != nil {
		return nil, err
	}
	if request.PageSize <= 0 {
		return nil, persistence.ErrNonPositiveReadQueueMessagesPageSize
	}
	minMessageID, err := s.getMinMessageID(request.QueueType, request.QueueName, request.NextPageToken, q.Metadata)
	if err != nil {
		return nil, err
	}

	iter := s.session.Query(
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

	nextPageToken := persistence.GetNextPageTokenForQueueV2(messages)

	return &persistence.InternalReadMessagesResponse{
		Messages:      messages,
		NextPageToken: nextPageToken,
	}, nil
}

func (s *queueV2Store) CreateQueue(
	ctx context.Context,
	request *persistence.InternalCreateQueueRequest,
) (*persistence.InternalCreateQueueResponse, error) {
	queueType := request.QueueType
	queueName := request.QueueName
	q := persistencespb.Queue{
		Partitions: map[int32]*persistencespb.QueuePartition{
			0: {
				MinMessageId: persistence.FirstQueueMessageID,
			},
		},
	}
	bytes, _ := q.Marshal()
	applied, err := s.session.Query(
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

func (s *queueV2Store) RangeDeleteMessages(
	ctx context.Context,
	request *persistence.InternalRangeDeleteMessagesRequest,
) (*persistence.InternalRangeDeleteMessagesResponse, error) {
	if request.InclusiveMaxMessageMetadata.ID < persistence.FirstQueueMessageID {
		return nil, fmt.Errorf(
			"%w: id is %d but must be >= %d",
			persistence.ErrInvalidQueueRangeDeleteMaxMessageID,
			request.InclusiveMaxMessageMetadata.ID,
			persistence.FirstQueueMessageID,
		)
	}
	queueType := request.QueueType
	queueName := request.QueueName
	q, err := s.getQueue(ctx, queueType, queueName)
	if err != nil {
		return nil, err
	}
	partition, err := getPartition(queueType, queueName, q.Metadata)
	if err != nil {
		return nil, err
	}
	minMessageID := partition.MinMessageId
	lastIDToDelete := request.InclusiveMaxMessageMetadata.ID
	nextMessageID, err := s.getNextMessageID(ctx, queueType, queueName)
	if err != nil {
		return nil, err
	}
	if lastIDToDelete >= nextMessageID {
		// We need to clamp the lastIDToDelete to the last message ID in the queue. This is because we never actually
		// delete the last message (so that we can keep track of the max message ID). If we don't do this, a request to
		// delete messages with a lastIDToDelete that is greater than the last message ID will delete all messages in
		// the queue, and we will lose track of the max message ID, so the next message ID from an enqueue request will
		// be wrong.
		lastIDToDelete = nextMessageID - 1
	}
	if lastIDToDelete < minMessageID {
		// This is more than just an optimization; we need it for correctness. If the lastIDToDelete is more than one
		// less than the minMessageID, then if we update the minMessageID to be lastIDToDelete + 1, we will have
		// decreased the minMessageID, which doesn't make sense because there would be messages >= minMessageID that
		// have not been deleted. For example, if the minMessageID is 10 and the lastIDToDelete is 7, then we would
		// update the minMessageID to 8, which is incorrect because messages 8 and 9 have been deleted.
		//
		// If lastIDToDelete = minMessageID - 1, then we wouldn't update the minMessageID to something incorrect, but we
		// would waste two queries because we wouldn't delete anything, and the queue metadata would be updated to be
		// the same as it was before. For example, if the minMessageID is 10 and the lastIDToDelete is 9, then we would
		// send a query to delete messages < 9 (there are none), and then we would update the minMessageID to 10, which
		// is the same as it was before.
		//
		// If the lastIDToDelete is 10, then we would send a query to delete messages < 10 (there would be one because
		// we never delete all elements from the queue), and then we would update the minMessageID to be 11, which is
		// necessary because subsequent queries would need to start at 11.
		return &persistence.InternalRangeDeleteMessagesResponse{}, nil
	}
	err = s.session.Query(
		TemplateRangeDeleteMessagesQuery,
		queueType,
		queueName,
		0,                // partition
		minMessageID-1,   // We have to subtract 1 because we never delete the last message
		lastIDToDelete-1, // Always preserve the last message
	).WithContext(ctx).Exec()
	if err != nil {
		return nil, gocql.ConvertError("QueueV2RangeDeleteMessages", err)
	}
	partition.MinMessageId = lastIDToDelete + 1
	err = s.updateQueue(ctx, q, queueType, queueName)
	if err != nil {
		return nil, err
	}
	return &persistence.InternalRangeDeleteMessagesResponse{}, nil
}

func (s *queueV2Store) updateQueue(
	ctx context.Context,
	q *Queue,
	queueType persistence.QueueV2Type,
	queueName string,
) error {
	bytes, _ := q.Metadata.Marshal()
	version := q.Version
	nextVersion := version + 1
	q.Version = nextVersion
	applied, err := s.session.Query(
		TemplateUpdateQueueMetadataQuery,
		bytes,
		enums.ENCODING_TYPE_PROTO3.String(),
		nextVersion,
		queueType,
		queueName,
		version,
	).WithContext(ctx).MapScanCAS(make(map[string]interface{}))
	if err != nil {
		return gocql.ConvertError("QueueV2UpdateQueueMetadata", err)
	}
	if !applied {
		return fmt.Errorf(
			"%w: queue type %v and name %v",
			ErrUpdateQueueConflict,
			queueType,
			queueName,
		)
	}
	return nil
}

func (s *queueV2Store) getMinMessageID(
	queueType persistence.QueueV2Type,
	name string,
	nextPageToken []byte,
	queue *persistencespb.Queue,
) (int, error) {
	if len(nextPageToken) == 0 {
		partition, err := getPartition(queueType, name, queue)
		if err != nil {
			return 0, err
		}
		return int(partition.MinMessageId), nil
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

func getPartition(
	queueType persistence.QueueV2Type,
	queueName string,
	queue *persistencespb.Queue,
) (*persistencespb.QueuePartition, error) {
	// Currently, we only have one partition for each queue. However, that might change in the future. If a queue is
	// created with more than 1 partition by a server on a future release, and then that server is downgraded, we
	// will need to handle this case. Since all DLQ tasks are retried infinitely, we just return an error.
	numPartitions := len(queue.Partitions)
	if numPartitions != 1 {
		return nil, serviceerror.NewInternal(
			fmt.Sprintf(
				"Invalid partition count for queue: queue with type = %v and queueName = %v has %d "+
					"partitions, but this implementation only supports queues with 1 partition. Did you downgrade "+
					"your Temporal server?",
				queueType,
				queueName,
				numPartitions,
			),
		)
	}
	partition := queue.Partitions[0]
	return partition, nil
}

func (s *queueV2Store) getNextPageToken(result []persistence.QueueV2Message, messageID int64) []byte {
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

func (s *queueV2Store) tryInsert(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	blob commonpb.DataBlob,
	messageID int64,
) error {
	applied, err := s.session.Query(
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
		return fmt.Errorf(
			"%w: queue type %v and name %v already has a message with ID %v",
			ErrEnqueueMessageConflict,
			queueType,
			queueName,
			messageID,
		)
	}

	return nil
}

func (s *queueV2Store) getQueue(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	name string,
) (*Queue, error) {
	return GetQueue(ctx, s.session, name, queueType)
}

func GetQueue(
	ctx context.Context,
	session gocql.Session,
	queueName string,
	queueType persistence.QueueV2Type,
) (*Queue, error) {
	var (
		queueBytes       []byte
		queueEncodingStr string
		version          int64
	)

	err := session.Query(TemplateGetQueueQuery, queueType, queueName).WithContext(ctx).Scan(
		&queueBytes,
		&queueEncodingStr,
		&version,
	)
	if err != nil {
		if gocql.IsNotFoundError(err) {
			return nil, persistence.NewQueueNotFoundError(queueType, queueName)
		}
		return nil, gocql.ConvertError("QueueV2GetQueue", err)
	}

	if queueEncodingStr != enums.ENCODING_TYPE_PROTO3.String() {
		return nil, fmt.Errorf(
			"%w: invalid queue encoding type: queue with type %v and name %v has invalid encoding",
			serialization.NewUnknownEncodingTypeError(queueEncodingStr, enums.ENCODING_TYPE_PROTO3),
			queueType,
			queueName,
		)
	}

	q := &persistencespb.Queue{}
	err = q.Unmarshal(queueBytes)
	if err != nil {
		return nil, serialization.NewDeserializationError(
			enums.ENCODING_TYPE_PROTO3,
			fmt.Errorf("%w: unmarshal queue payload: failed for queue with type %v and name %v",
				err, queueType, queueName),
		)
	}

	return &Queue{
		Metadata: q,
		Version:  version,
	}, nil
}

func (s *queueV2Store) getNextMessageID(ctx context.Context, queueType persistence.QueueV2Type, queueName string) (int64, error) {
	var maxMessageID int64

	err := s.session.Query(TemplateGetMaxMessageIDQuery, queueType, queueName, 0).WithContext(ctx).Scan(&maxMessageID)
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
