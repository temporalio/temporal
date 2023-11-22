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
	// We will have to ALLOW FILTERING for this query since partition key consists of both queue_type and queue_name.
	templateGetQueueNamesQuery = `SELECT queue_name, metadata_payload, metadata_encoding, version FROM queues WHERE queue_type = ? ALLOW FILTERING`
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
	minMessageID, err := persistence.GetMinMessageIDToReadForQueueV2(request.QueueType, request.QueueName, request.NextPageToken, q.Metadata)
	if err != nil {
		return nil, err
	}

	iter := s.session.Query(
		TemplateGetMessagesQuery,
		request.QueueType,
		request.QueueName,
		0,
		int(minMessageID),
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
		encoding, err := enums.EncodingTypeFromString(messageEncoding)
		if err != nil {
			return nil, serialization.NewUnknownEncodingTypeError(messageEncoding)
		}

		encodingType := enums.EncodingType(encoding)

		message := persistence.QueueV2Message{
			MetaData: persistence.MessageMetadata{ID: messageID},
			Data: &commonpb.DataBlob{
				EncodingType: encodingType,
				Data:         messagePayload,
			},
		}
		messages = append(messages, message)
	}

	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("QueueV2ReadMessages", err)
	}

	nextPageToken := persistence.GetNextPageTokenForReadMessages(messages)
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
	partition, err := persistence.GetPartitionForQueueV2(queueType, queueName, q.Metadata)
	if err != nil {
		return nil, err
	}
	maxMessageID, ok, err := s.getMaxMessageID(ctx, queueType, queueName)
	if err != nil {
		return nil, err
	}
	if !ok {
		// Nothing in the queue to delete.
		return &persistence.InternalRangeDeleteMessagesResponse{}, nil
	}
	deleteRange, ok := persistence.GetDeleteRange(persistence.DeleteRequest{
		LastIDToDeleteInclusive: request.InclusiveMaxMessageMetadata.ID,
		ExistingMessageRange: persistence.InclusiveMessageRange{
			MinMessageID: partition.MinMessageId,
			MaxMessageID: maxMessageID,
		},
	})
	if !ok {
		return &persistence.InternalRangeDeleteMessagesResponse{}, nil
	}
	err = s.session.Query(
		TemplateRangeDeleteMessagesQuery,
		queueType,
		queueName,
		0, // partition
		deleteRange.MinMessageID,
		deleteRange.MaxMessageID,
	).WithContext(ctx).Exec()
	if err != nil {
		return nil, gocql.ConvertError("QueueV2RangeDeleteMessages", err)
	}
	partition.MinMessageId = deleteRange.NewMinMessageID
	err = s.updateQueue(ctx, q, queueType, queueName)
	if err != nil {
		return nil, err
	}
	return &persistence.InternalRangeDeleteMessagesResponse{
		MessagesDeleted: deleteRange.MessagesToDelete,
	}, nil
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

func (s *queueV2Store) tryInsert(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	blob *commonpb.DataBlob,
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
	return getQueueFromMetadata(queueType, queueName, queueBytes, queueEncodingStr, version)
}

func getQueueFromMetadata(
	queueType persistence.QueueV2Type,
	queueName string,
	queueBytes []byte,
	queueEncodingStr string,
	version int64,
) (*Queue, error) {
	if queueEncodingStr != enums.ENCODING_TYPE_PROTO3.String() {
		return nil, fmt.Errorf(
			"%w: invalid queue encoding type: queue with type %v and name %v has invalid encoding",
			serialization.NewUnknownEncodingTypeError(queueEncodingStr, enums.ENCODING_TYPE_PROTO3),
			queueType,
			queueName,
		)
	}

	q := &persistencespb.Queue{}
	err := q.Unmarshal(queueBytes)
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
	maxMessageID, ok, err := s.getMaxMessageID(ctx, queueType, queueName)
	if err != nil {
		return 0, err
	}
	if !ok {
		return persistence.FirstQueueMessageID, nil
	}

	// The next message ID is the max message ID + 1.
	return maxMessageID + 1, nil
}

func (s *queueV2Store) getMaxMessageID(ctx context.Context, queueType persistence.QueueV2Type, queueName string) (int64, bool, error) {
	var maxMessageID int64

	err := s.session.Query(TemplateGetMaxMessageIDQuery, queueType, queueName, 0).WithContext(ctx).Scan(&maxMessageID)
	if err != nil {
		if gocql.IsNotFoundError(err) {
			return 0, false, nil
		}
		return 0, false, gocql.ConvertError("QueueV2GetMaxMessageID", err)
	}
	return maxMessageID, true, nil
}

func (s *queueV2Store) ListQueues(
	ctx context.Context,
	request *persistence.InternalListQueuesRequest,
) (*persistence.InternalListQueuesResponse, error) {
	if request.PageSize <= 0 {
		return nil, persistence.ErrNonPositiveListQueuesPageSize
	}
	iter := s.session.Query(
		templateGetQueueNamesQuery,
		request.QueueType,
	).PageSize(request.PageSize).PageState(request.NextPageToken).WithContext(ctx).Iter()

	var queues []persistence.QueueInfo
	for {
		var (
			queueName        string
			metadataBytes    []byte
			metadataEncoding string
			version          int64
		)
		if !iter.Scan(&queueName, &metadataBytes, &metadataEncoding, &version) {
			break
		}
		q, err := getQueueFromMetadata(request.QueueType, queueName, metadataBytes, metadataEncoding, version)
		if err != nil {
			return nil, err
		}
		partition, err := persistence.GetPartitionForQueueV2(request.QueueType, queueName, q.Metadata)
		if err != nil {
			return nil, err
		}
		nextMessageID, err := s.getNextMessageID(ctx, request.QueueType, queueName)
		if err != nil {
			return nil, err
		}
		messageCount := nextMessageID - partition.MinMessageId
		queues = append(queues, persistence.QueueInfo{
			QueueName:    queueName,
			MessageCount: messageCount,
		})
	}
	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("QueueV2ListQueues", err)
	}
	return &persistence.InternalListQueuesResponse{
		Queues:        queues,
		NextPageToken: iter.PageState(),
	}, nil
}
