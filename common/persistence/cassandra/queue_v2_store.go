package cassandra

import (
	"context"
	"fmt"
	"sync"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/persistence/serialization"
	"google.golang.org/protobuf/proto"
)

type (
	// queueV2Store contains the SQL queries and serialization/deserialization functions to interact with the queues and
	// queue_messages tables that implement the QueueV2 interface. The schema is located at:
	//	schema/cassandra/temporal/versioned/v1.9/queues.cql
	queueV2Store struct {
		session         gocql.Session
		logger          log.Logger
		knownQueues     sync.Map
		messageIDRanges sync.Map
		queueLocks      sync.Map
	}

	Queue struct {
		Metadata *persistencespb.Queue
		Version  int64
	}

	queueV2Key struct {
		queueType persistence.QueueV2Type
		queueName string
	}

	queueV2MessageIDRange struct {
		nextMessageID         int64
		exclusiveMaxMessageID int64
	}
)

const (
	TemplateEnqueueMessageQuery            = `INSERT INTO queue_messages (queue_type, queue_name, queue_partition, message_id, message_payload, message_encoding) VALUES (?, ?, ?, ?, ?, ?)`
	TemplateGetMessagesQuery               = `SELECT message_id, message_payload, message_encoding FROM queue_messages WHERE queue_type = ? AND queue_name = ? AND queue_partition = ? AND message_id >= ? ORDER BY message_id ASC LIMIT ?`
	TemplateGetMaxMessageIDQuery           = `SELECT message_id FROM queue_messages WHERE queue_type = ? AND queue_name = ? AND queue_partition = ? ORDER BY message_id DESC LIMIT 1`
	TemplateCreateQueueQuery               = `INSERT INTO queues (queue_type, queue_name, metadata_payload, metadata_encoding, version) VALUES (?, ?, ?, ?, ?) IF NOT EXISTS`
	TemplateGetQueueQuery                  = `SELECT metadata_payload, metadata_encoding, version FROM queues WHERE queue_type = ? AND queue_name = ?`
	TemplateRangeDeleteMessagesQuery       = `DELETE FROM queue_messages WHERE queue_type = ? AND queue_name = ? AND queue_partition = ? AND message_id >= ? AND message_id <= ?`
	TemplateUpdateQueueMetadataQuery       = `UPDATE queues SET metadata_payload = ?, metadata_encoding = ?, version = ? WHERE queue_type = ? AND queue_name = ? IF version = ?`
	TemplateCreateQueueMessageIDRangeQuery = `INSERT INTO queue_message_id_ranges (queue_type, queue_name, next_message_id, version) VALUES (?, ?, ?, ?) IF NOT EXISTS`
	TemplateGetQueueMessageIDRangeQuery    = `SELECT next_message_id, version FROM queue_message_id_ranges WHERE queue_type = ? AND queue_name = ?`
	TemplateUpdateQueueMessageIDRangeQuery = `UPDATE queue_message_id_ranges SET next_message_id = ?, version = ? WHERE queue_type = ? AND queue_name = ? IF version = ?`
	templateGetQueueNamesQuery             = `SELECT queue_name, metadata_payload, metadata_encoding, version FROM queues WHERE queue_type = ? ALLOW FILTERING`
	queueV2MessageIDRangeAllocationSize    = 1024
)

var (
	// ErrEnqueueMessageConflict is returned by queue implementations that use conditional inserts to allocate message
	// IDs and lose a race with another writer.
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
	queueType := request.QueueType
	queueName := request.QueueName
	unlock := s.lockQueue(queueType, queueName)
	defer unlock()

	if _, ok := s.getCachedQueue(queueType, queueName); !ok {
		_, err := s.getQueue(ctx, queueType, queueName)
		if err != nil {
			return nil, err
		}
	}
	nextMessageID, err := s.nextMessageID(ctx, queueType, queueName)
	if err != nil {
		return nil, err
	}
	err = s.tryInsert(ctx, request.QueueType, request.QueueName, request.Blob, nextMessageID)
	if err != nil {
		return nil, err
	}
	return &persistence.InternalEnqueueMessageResponse{
		Metadata: persistence.MessageMetadata{ID: nextMessageID},
	}, nil
}

func (s *queueV2Store) ReadMessages(
	ctx context.Context,
	request *persistence.InternalReadMessagesRequest,
) (*persistence.InternalReadMessagesResponse, error) {
	q, ok := s.getCachedQueue(request.QueueType, request.QueueName)
	if !ok {
		var err error
		q, err = s.getQueue(ctx, request.QueueType, request.QueueName)
		if err != nil {
			return nil, err
		}
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
		encoding, err := enumspb.EncodingTypeFromString(messageEncoding)
		if err != nil {
			return nil, serialization.NewUnknownEncodingTypeError(messageEncoding)
		}

		encodingType := enumspb.EncodingType(encoding)

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
		enumspb.ENCODING_TYPE_PROTO3.String(),
		0,
	).WithContext(ctx).MapScanCAS(make(map[string]any))
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
	s.markKnownQueue(queueType, queueName, &Queue{
		Metadata: &q,
		Version:  0,
	})
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
	q, ok := s.getCachedQueue(queueType, queueName)
	if !ok {
		var err error
		q, err = s.getQueue(ctx, queueType, queueName)
		if err != nil {
			return nil, err
		}
	}
	partition, err := persistence.GetPartitionForQueueV2(queueType, queueName, q.Metadata)
	if err != nil {
		return nil, err
	}
	if request.InclusiveMaxMessageMetadata.ID < partition.MinMessageId {
		return &persistence.InternalRangeDeleteMessagesResponse{}, nil
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
		enumspb.ENCODING_TYPE_PROTO3.String(),
		nextVersion,
		queueType,
		queueName,
		version,
	).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return gocql.ConvertError("QueueV2UpdateQueueMetadata", err)
	}
	if !applied {
		s.forgetKnownQueue(queueType, queueName)
		return fmt.Errorf(
			"%w: queue type %v and name %v",
			ErrUpdateQueueConflict,
			queueType,
			queueName,
		)
	}
	s.markKnownQueue(queueType, queueName, q)
	return nil
}

func (s *queueV2Store) tryInsert(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	blob *commonpb.DataBlob,
	messageID int64,
) error {
	err := s.session.Query(
		TemplateEnqueueMessageQuery,
		queueType,
		queueName,
		0,
		messageID,
		blob.Data,
		blob.EncodingType.String(),
	).WithContext(ctx).Exec()
	if err != nil {
		return gocql.ConvertError("QueueV2EnqueueMessage", err)
	}

	return nil
}

func (s *queueV2Store) getQueue(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	name string,
) (*Queue, error) {
	q, err := GetQueue(ctx, s.session, name, queueType)
	if err != nil {
		return nil, err
	}
	s.markKnownQueue(queueType, name, q)
	return q, nil
}

func (s *queueV2Store) getCachedQueue(queueType persistence.QueueV2Type, queueName string) (*Queue, bool) {
	q, ok := s.knownQueues.Load(queueV2Key{
		queueType: queueType,
		queueName: queueName,
	})
	if !ok {
		return nil, false
	}
	return cloneQueue(q.(*Queue)), true
}

func (s *queueV2Store) markKnownQueue(queueType persistence.QueueV2Type, queueName string, queue *Queue) {
	s.knownQueues.Store(queueV2Key{
		queueType: queueType,
		queueName: queueName,
	}, cloneQueue(queue))
}

func (s *queueV2Store) forgetKnownQueue(queueType persistence.QueueV2Type, queueName string) {
	s.knownQueues.Delete(queueV2Key{
		queueType: queueType,
		queueName: queueName,
	})
}

func (s *queueV2Store) nextMessageID(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
) (int64, error) {
	key := queueV2Key{
		queueType: queueType,
		queueName: queueName,
	}
	if cachedRange, ok := s.messageIDRanges.Load(key); ok {
		messageIDRange := cachedRange.(queueV2MessageIDRange)
		if messageIDRange.nextMessageID < messageIDRange.exclusiveMaxMessageID {
			messageID := messageIDRange.nextMessageID
			messageIDRange.nextMessageID++
			s.messageIDRanges.Store(key, messageIDRange)
			return messageID, nil
		}
	}

	messageIDRange, err := s.reserveMessageIDRange(ctx, queueType, queueName)
	if err != nil {
		return 0, err
	}
	messageID := messageIDRange.nextMessageID
	messageIDRange.nextMessageID++
	s.messageIDRanges.Store(key, messageIDRange)
	return messageID, nil
}

func (s *queueV2Store) reserveMessageIDRange(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
) (queueV2MessageIDRange, error) {
	for {
		nextMessageID, version, ok, err := s.getQueueMessageIDRange(ctx, queueType, queueName)
		if err != nil {
			return queueV2MessageIDRange{}, err
		}
		if !ok {
			nextMessageID, err = s.firstMessageIDToReserve(ctx, queueType, queueName)
			if err != nil {
				return queueV2MessageIDRange{}, err
			}
			nextAllocatedMessageID := nextMessageID + queueV2MessageIDRangeAllocationSize
			applied, err := s.session.Query(
				TemplateCreateQueueMessageIDRangeQuery,
				queueType,
				queueName,
				nextAllocatedMessageID,
				int64(0),
			).WithContext(ctx).MapScanCAS(make(map[string]any))
			if err != nil {
				return queueV2MessageIDRange{}, gocql.ConvertError("QueueV2CreateMessageIDRange", err)
			}
			if applied {
				return queueV2MessageIDRange{
					nextMessageID:         nextMessageID,
					exclusiveMaxMessageID: nextAllocatedMessageID,
				}, nil
			}
			continue
		}

		nextAllocatedMessageID := nextMessageID + queueV2MessageIDRangeAllocationSize
		applied, err := s.session.Query(
			TemplateUpdateQueueMessageIDRangeQuery,
			nextAllocatedMessageID,
			version+1,
			queueType,
			queueName,
			version,
		).WithContext(ctx).MapScanCAS(make(map[string]any))
		if err != nil {
			return queueV2MessageIDRange{}, gocql.ConvertError("QueueV2UpdateMessageIDRange", err)
		}
		if applied {
			return queueV2MessageIDRange{
				nextMessageID:         nextMessageID,
				exclusiveMaxMessageID: nextAllocatedMessageID,
			}, nil
		}
	}
}

func (s *queueV2Store) firstMessageIDToReserve(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
) (int64, error) {
	maxMessageID, ok, err := s.getMaxMessageID(ctx, queueType, queueName)
	if err != nil {
		return 0, err
	}
	if !ok {
		return persistence.FirstQueueMessageID, nil
	}
	return maxMessageID + 1, nil
}

func (s *queueV2Store) getQueueMessageIDRange(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
) (nextMessageID int64, version int64, ok bool, err error) {
	err = s.session.Query(
		TemplateGetQueueMessageIDRangeQuery,
		queueType,
		queueName,
	).WithContext(ctx).Scan(&nextMessageID, &version)
	if err != nil {
		if gocql.IsNotFoundError(err) {
			return 0, 0, false, nil
		}
		return 0, 0, false, gocql.ConvertError("QueueV2GetMessageIDRange", err)
	}
	return nextMessageID, version, true, nil
}

func (s *queueV2Store) lockQueue(queueType persistence.QueueV2Type, queueName string) func() {
	lock, _ := s.queueLocks.LoadOrStore(queueV2Key{
		queueType: queueType,
		queueName: queueName,
	}, &sync.Mutex{})
	mutex := lock.(*sync.Mutex)
	mutex.Lock()
	return mutex.Unlock
}

func cloneQueue(queue *Queue) *Queue {
	return &Queue{
		Metadata: proto.Clone(queue.Metadata).(*persistencespb.Queue),
		Version:  queue.Version,
	}
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
	if queueEncodingStr != enumspb.ENCODING_TYPE_PROTO3.String() {
		return nil, fmt.Errorf(
			"%w: invalid queue encoding type: queue with type %v and name %v has invalid encoding",
			serialization.NewUnknownEncodingTypeError(queueEncodingStr, enumspb.ENCODING_TYPE_PROTO3),
			queueType,
			queueName,
		)
	}

	q := &persistencespb.Queue{}
	err := q.Unmarshal(queueBytes)
	if err != nil {
		return nil, serialization.NewDeserializationError(
			enumspb.ENCODING_TYPE_PROTO3,
			fmt.Errorf("%w: unmarshal queue payload: failed for queue with type %v and name %v",
				err, queueType, queueName),
		)
	}

	return &Queue{
		Metadata: q,
		Version:  version,
	}, nil
}

func (s *queueV2Store) getMessageCountAndLastID(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
	partition *persistencespb.QueuePartition,
) (messageCount int64, maxMessageID int64, err error) {
	var ok bool
	maxMessageID, ok, err = s.getMaxMessageID(ctx, queueType, queueName)
	if err != nil {
		return 0, 0, err
	}
	if !ok {
		return 0, -1, nil // No messages
	}
	messageCount = maxMessageID - partition.MinMessageId + 1
	return messageCount, maxMessageID, nil
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

	closeIter := func() {
		_ = iter.Close()
	}
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
			closeIter()
			return nil, err
		}
		partition, err := persistence.GetPartitionForQueueV2(request.QueueType, queueName, q.Metadata)
		if err != nil {
			closeIter()
			return nil, err
		}
		messageCount, lastMessageID, err := s.getMessageCountAndLastID(ctx, request.QueueType, queueName, partition)
		if err != nil {
			closeIter()
			return nil, err
		}
		queues = append(queues, persistence.QueueInfo{
			QueueName:     queueName,
			MessageCount:  messageCount,
			LastMessageID: lastMessageID,
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
