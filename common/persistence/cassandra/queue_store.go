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
)

const (
	templateEnqueueMessageQuery           = `INSERT INTO queue (queue_type, message_id, message_payload, message_encoding) VALUES(?, ?, ?, ?) IF NOT EXISTS`
	templateEnqueueMessageWithoutCASQuery = `INSERT INTO queue (queue_type, message_id, message_payload, message_encoding) VALUES(?, ?, ?, ?)`
	templateGetLastMessageIDQuery         = `SELECT message_id FROM queue WHERE queue_type=? ORDER BY message_id DESC LIMIT 1`
	templateGetMessagesQuery              = `SELECT message_id, message_payload, message_encoding FROM queue WHERE queue_type = ? and message_id > ? LIMIT ?`
	templateGetMessagesFromDLQQuery       = `SELECT message_id, message_payload, message_encoding FROM queue WHERE queue_type = ? and message_id > ? and message_id <= ?`
	templateDeleteMessagesBeforeQuery     = `DELETE FROM queue WHERE queue_type = ? and message_id < ?`
	templateDeleteMessagesQuery           = `DELETE FROM queue WHERE queue_type = ? and message_id > ? and message_id <= ?`
	templateDeleteMessageQuery            = `DELETE FROM queue WHERE queue_type = ? and message_id = ?`

	templateGetQueueMetadataQuery                = `SELECT cluster_ack_level, data, data_encoding, version FROM queue_metadata WHERE queue_type = ?`
	templateInsertQueueMetadataQuery             = `INSERT INTO queue_metadata (queue_type, cluster_ack_level, data, data_encoding, version) VALUES(?, ?, ?, ?, ?) IF NOT EXISTS`
	templateUpdateQueueMetadataQuery             = `UPDATE queue_metadata SET cluster_ack_level = ?, data = ?, data_encoding = ?, version = ? WHERE queue_type = ? IF version = ?`
	templateCreateQueueMessageIDRangeQuery       = `INSERT INTO queue_message_id_range (queue_type, next_message_id, version) VALUES (?, ?, ?) IF NOT EXISTS`
	templateGetQueueMessageIDRangeQuery          = `SELECT next_message_id, version FROM queue_message_id_range WHERE queue_type = ?`
	templateUpdateQueueMessageIDRangeQuery       = `UPDATE queue_message_id_range SET next_message_id = ?, version = ? WHERE queue_type = ? IF version = ?`
	queueMessageIDRangeAllocationSize      int64 = 1024
)

type (
	QueueStore struct {
		queueType        persistence.QueueType
		session          gocql.Session
		logger           log.Logger
		serializer       serialization.Serializer
		messageIDRanges  sync.Map
		queueLocks       sync.Map
		disableInsertCAS bool
	}

	queueMessageIDRange struct {
		nextMessageID         int64
		exclusiveMaxMessageID int64
	}
)

func NewQueueStore(
	queueType persistence.QueueType,
	session gocql.Session,
	logger log.Logger,
	disableInsertCAS ...bool,
) (persistence.Queue, error) {
	return &QueueStore{
		queueType:        queueType,
		session:          session,
		logger:           logger,
		serializer:       serialization.NewSerializer(),
		disableInsertCAS: len(disableInsertCAS) > 0 && disableInsertCAS[0],
	}, nil
}

func (q *QueueStore) Init(
	ctx context.Context,
	blob *commonpb.DataBlob,
) error {
	if err := q.initializeQueueMetadata(ctx, blob); err != nil {
		return err
	}
	return q.initializeDLQMetadata(ctx, blob)
}

func (q *QueueStore) EnqueueMessage(
	ctx context.Context,
	blob *commonpb.DataBlob,
) error {
	_, err := q.enqueueMessage(ctx, q.queueType, blob)
	return err
}

func (q *QueueStore) EnqueueMessageToDLQ(
	ctx context.Context,
	blob *commonpb.DataBlob,
) (int64, error) {
	// Use negative queue type as the dlq type
	return q.enqueueMessage(ctx, q.getDLQTypeFromQueueType(), blob)
}

func (q *QueueStore) enqueueMessage(
	ctx context.Context,
	queueType persistence.QueueType,
	blob *commonpb.DataBlob,
) (int64, error) {
	unlock := q.lockQueue(queueType)
	defer unlock()

	messageID, err := q.nextMessageID(ctx, queueType)
	if err != nil {
		return persistence.EmptyQueueMessageID, err
	}
	err = q.tryEnqueue(ctx, queueType, messageID, blob)
	if err != nil {
		return persistence.EmptyQueueMessageID, err
	}
	return messageID, nil
}

func (q *QueueStore) tryEnqueue(
	ctx context.Context,
	queueType persistence.QueueType,
	messageID int64,
	blob *commonpb.DataBlob,
) error {
	if q.disableInsertCAS {
		err := q.session.Query(templateEnqueueMessageWithoutCASQuery, queueType, messageID, blob.Data, blob.EncodingType.String()).WithContext(ctx).Exec()
		if err != nil {
			return gocql.ConvertError("tryEnqueue", err)
		}
		return nil
	}

	applied, err := q.session.Query(templateEnqueueMessageQuery, queueType, messageID, blob.Data, blob.EncodingType.String()).WithContext(ctx).MapScanCAS(make(map[string]any))
	if err != nil {
		return gocql.ConvertError("tryEnqueue", err)
	}
	if !applied {
		return ErrEnqueueMessageConflict
	}

	return nil
}

func (q *QueueStore) getLastMessageID(
	ctx context.Context,
	queueType persistence.QueueType,
) (int64, error) {

	query := q.session.Query(templateGetLastMessageIDQuery, queueType).WithContext(ctx)
	result := make(map[string]any)
	err := query.MapScan(result)
	if err != nil {
		if gocql.IsNotFoundError(err) {
			return persistence.EmptyQueueMessageID, nil
		}
		return persistence.EmptyQueueMessageID, gocql.ConvertError("getLastMessageID", err)
	}
	return result["message_id"].(int64), nil
}

func (q *QueueStore) ReadMessages(
	ctx context.Context,
	lastMessageID int64,
	maxCount int,
) ([]*persistence.QueueMessage, error) {
	// Reading replication tasks need to be quorum level consistent, otherwise we could lose tasks
	query := q.session.Query(templateGetMessagesQuery,
		q.queueType,
		lastMessageID,
		maxCount,
	).WithContext(ctx)

	iter := query.Iter()

	var result []*persistence.QueueMessage
	message := make(map[string]any)
	for iter.MapScan(message) {
		queueMessage := convertQueueMessage(message)
		result = append(result, queueMessage)
		message = make(map[string]any)
	}

	if err := iter.Close(); err != nil {
		return nil, gocql.ConvertError("ReadMessages", err)
	}

	return result, nil
}

func (q *QueueStore) ReadMessagesFromDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) ([]*persistence.QueueMessage, []byte, error) {
	// Reading replication tasks need to be quorum level consistent, otherwise we could lose tasks
	// Use negative queue type as the dlq type
	query := q.session.Query(templateGetMessagesFromDLQQuery,
		q.getDLQTypeFromQueueType(),
		firstMessageID,
		lastMessageID,
	).WithContext(ctx)
	iter := query.PageSize(pageSize).PageState(pageToken).Iter()

	var result []*persistence.QueueMessage
	message := make(map[string]any)
	for iter.MapScan(message) {
		queueMessage := convertQueueMessage(message)
		result = append(result, queueMessage)
		message = make(map[string]any)
	}

	var nextPageToken []byte
	if len(iter.PageState()) > 0 {
		nextPageToken = iter.PageState()
	}
	if err := iter.Close(); err != nil {
		return nil, nil, gocql.ConvertError("ReadMessagesFromDLQ", err)
	}

	return result, nextPageToken, nil
}

func (q *QueueStore) DeleteMessagesBefore(
	ctx context.Context,
	messageID int64,
) error {

	query := q.session.Query(templateDeleteMessagesBeforeQuery, q.queueType, messageID).WithContext(ctx)
	if err := query.Exec(); err != nil {
		return gocql.ConvertError("DeleteMessagesBefore", err)
	}
	return nil
}

func (q *QueueStore) DeleteMessageFromDLQ(
	ctx context.Context,
	messageID int64,
) error {

	// Use negative queue type as the dlq type
	query := q.session.Query(templateDeleteMessageQuery, q.getDLQTypeFromQueueType(), messageID).WithContext(ctx)
	if err := query.Exec(); err != nil {
		return gocql.ConvertError("DeleteMessageFromDLQ", err)
	}

	return nil
}

func (q *QueueStore) RangeDeleteMessagesFromDLQ(
	ctx context.Context,
	firstMessageID int64,
	lastMessageID int64,
) error {

	// Use negative queue type as the dlq type
	query := q.session.Query(templateDeleteMessagesQuery, q.getDLQTypeFromQueueType(), firstMessageID, lastMessageID).WithContext(ctx)
	if err := query.Exec(); err != nil {
		return gocql.ConvertError("RangeDeleteMessagesFromDLQ", err)
	}

	return nil
}

func (q *QueueStore) UpdateAckLevel(
	ctx context.Context,
	metadata *persistence.InternalQueueMetadata,
) error {
	return q.updateAckLevel(ctx, metadata, q.queueType)
}

func (q *QueueStore) GetAckLevels(
	ctx context.Context,
) (*persistence.InternalQueueMetadata, error) {
	queueMetadata, err := q.getQueueMetadata(ctx, q.queueType)
	if err != nil {
		return nil, gocql.ConvertError("GetAckLevels", err)
	}

	return queueMetadata, nil
}

func (q *QueueStore) UpdateDLQAckLevel(
	ctx context.Context,
	metadata *persistence.InternalQueueMetadata,
) error {
	return q.updateAckLevel(ctx, metadata, q.getDLQTypeFromQueueType())
}

func (q *QueueStore) GetDLQAckLevels(
	ctx context.Context,
) (*persistence.InternalQueueMetadata, error) {
	// Use negative queue type as the dlq type
	queueMetadata, err := q.getQueueMetadata(ctx, q.getDLQTypeFromQueueType())
	if err != nil {
		return nil, gocql.ConvertError("GetDLQAckLevels", err)
	}

	return queueMetadata, nil
}

func (q *QueueStore) insertInitialQueueMetadataRecord(
	ctx context.Context,
	queueType persistence.QueueType,
	blob *commonpb.DataBlob,
) error {

	version := 0
	// TODO: remove once cluster_ack_level is removed from DB
	clusterAckLevels := map[string]int64{}
	query := q.session.Query(templateInsertQueueMetadataQuery,
		queueType,
		clusterAckLevels,
		blob.Data,
		blob.EncodingType.String(),
		version,
	).WithContext(ctx)
	_, err := query.MapScanCAS(make(map[string]any))
	if err != nil {
		return fmt.Errorf("failed to insert initial queue metadata record: %v, Type: %v", err, queueType)
	}
	// it's ok if the query is not applied, which means that the record exists already.
	return nil
}

func (q *QueueStore) getQueueMetadata(
	ctx context.Context,
	queueType persistence.QueueType,
) (*persistence.InternalQueueMetadata, error) {

	query := q.session.Query(templateGetQueueMetadataQuery, queueType).WithContext(ctx)
	message := make(map[string]any)
	err := query.MapScan(message)
	if err != nil {
		return nil, err
	}

	return convertQueueMetadata(message, q.serializer)
}

func (q *QueueStore) updateAckLevel(
	ctx context.Context,
	metadata *persistence.InternalQueueMetadata,
	queueType persistence.QueueType,
) error {

	// TODO: remove this once cluster_ack_level is removed from DB
	metadataStruct, err := q.serializer.QueueMetadataFromBlob(metadata.Blob)
	if err != nil {
		return gocql.ConvertError("updateAckLevel", err)
	}

	query := q.session.Query(templateUpdateQueueMetadataQuery,
		metadataStruct.ClusterAckLevels,
		metadata.Blob.Data,
		metadata.Blob.EncodingType.String(),
		metadata.Version+1, // always increase version number on update
		queueType,
		metadata.Version, // condition update
	).WithContext(ctx)
	applied, err := query.MapScanCAS(make(map[string]any))
	if err != nil {
		return gocql.ConvertError("updateAckLevel", err)
	}
	if !applied {
		return &persistence.ConditionFailedError{Msg: "UpdateAckLevel operation encountered concurrent write."}
	}

	return nil
}

func (q *QueueStore) Close() {
	if q.session != nil {
		q.session.Close()
	}
}

func (q *QueueStore) lockQueue(queueType persistence.QueueType) func() {
	lock, _ := q.queueLocks.LoadOrStore(queueType, &sync.Mutex{})
	mutex, ok := lock.(*sync.Mutex)
	if !ok {
		mutex = &sync.Mutex{}
	}
	mutex.Lock()
	return mutex.Unlock
}

func (q *QueueStore) getDLQTypeFromQueueType() persistence.QueueType {
	return -q.queueType
}

func (q *QueueStore) nextMessageID(
	ctx context.Context,
	queueType persistence.QueueType,
) (int64, error) {
	if cachedRange, ok := q.messageIDRanges.Load(queueType); ok {
		if messageIDRange, ok := cachedRange.(queueMessageIDRange); ok && messageIDRange.nextMessageID < messageIDRange.exclusiveMaxMessageID {
			messageID := messageIDRange.nextMessageID
			messageIDRange.nextMessageID++
			q.messageIDRanges.Store(queueType, messageIDRange)
			return messageID, nil
		}
	}

	messageIDRange, err := q.reserveMessageIDRange(ctx, queueType)
	if err != nil {
		return 0, err
	}
	messageID := messageIDRange.nextMessageID
	messageIDRange.nextMessageID++
	q.messageIDRanges.Store(queueType, messageIDRange)
	return messageID, nil
}

func (q *QueueStore) reserveMessageIDRange(
	ctx context.Context,
	queueType persistence.QueueType,
) (queueMessageIDRange, error) {
	for {
		nextMessageID, version, ok, err := q.getQueueMessageIDRange(ctx, queueType)
		if err != nil {
			return queueMessageIDRange{}, err
		}
		if !ok {
			lastMessageID, err := q.getLastMessageID(ctx, queueType)
			if err != nil {
				return queueMessageIDRange{}, err
			}
			nextMessageID = lastMessageID + 1
			nextAllocatedMessageID := nextMessageID + queueMessageIDRangeAllocationSize
			applied, err := q.session.Query(
				templateCreateQueueMessageIDRangeQuery,
				queueType,
				nextAllocatedMessageID,
				int64(0),
			).WithContext(ctx).MapScanCAS(make(map[string]any))
			if err != nil {
				return queueMessageIDRange{}, gocql.ConvertError("CreateQueueMessageIDRange", err)
			}
			if applied {
				return queueMessageIDRange{
					nextMessageID:         nextMessageID,
					exclusiveMaxMessageID: nextAllocatedMessageID,
				}, nil
			}
			continue
		}

		nextAllocatedMessageID := nextMessageID + queueMessageIDRangeAllocationSize
		applied, err := q.session.Query(
			templateUpdateQueueMessageIDRangeQuery,
			nextAllocatedMessageID,
			version+1,
			queueType,
			version,
		).WithContext(ctx).MapScanCAS(make(map[string]any))
		if err != nil {
			return queueMessageIDRange{}, gocql.ConvertError("UpdateQueueMessageIDRange", err)
		}
		if applied {
			return queueMessageIDRange{
				nextMessageID:         nextMessageID,
				exclusiveMaxMessageID: nextAllocatedMessageID,
			}, nil
		}
	}
}

func (q *QueueStore) getQueueMessageIDRange(
	ctx context.Context,
	queueType persistence.QueueType,
) (nextMessageID int64, version int64, ok bool, err error) {
	err = q.session.Query(
		templateGetQueueMessageIDRangeQuery,
		queueType,
	).WithContext(ctx).Scan(&nextMessageID, &version)
	if err != nil {
		if gocql.IsNotFoundError(err) {
			return 0, 0, false, nil
		}
		return 0, 0, false, gocql.ConvertError("GetQueueMessageIDRange", err)
	}
	return nextMessageID, version, true, nil
}

func (q *QueueStore) initializeQueueMetadata(
	ctx context.Context,
	blob *commonpb.DataBlob,
) error {
	_, err := q.getQueueMetadata(ctx, q.queueType)
	if gocql.IsNotFoundError(err) {
		return q.insertInitialQueueMetadataRecord(ctx, q.queueType, blob)
	}
	return err
}

func (q *QueueStore) initializeDLQMetadata(
	ctx context.Context,
	blob *commonpb.DataBlob,
) error {
	_, err := q.getQueueMetadata(ctx, q.getDLQTypeFromQueueType())
	if gocql.IsNotFoundError(err) {
		return q.insertInitialQueueMetadataRecord(ctx, q.getDLQTypeFromQueueType(), blob)
	}
	return err
}

func convertQueueMessage(
	message map[string]any,
) *persistence.QueueMessage {

	id := message["message_id"].(int64)
	data := message["message_payload"].([]byte)
	encoding := message["message_encoding"].(string)
	if encoding == "" {
		encoding = enumspb.ENCODING_TYPE_PROTO3.String()
	}
	return &persistence.QueueMessage{
		ID:       id,
		Data:     data,
		Encoding: encoding,
	}
}

func convertQueueMetadata(
	message map[string]any,
	serializer serialization.Serializer,
) (*persistence.InternalQueueMetadata, error) {

	metadata := &persistence.InternalQueueMetadata{
		Version: message["version"].(int64),
	}
	_, ok := message["cluster_ack_level"]
	if ok {
		clusterAckLevel := message["cluster_ack_level"].(map[string]int64)
		// TODO: remove this once we remove cluster_ack_level from DB.
		blob, err := serializer.QueueMetadataToBlob(&persistencespb.QueueMetadata{ClusterAckLevels: clusterAckLevel})
		if err != nil {
			return nil, err
		}
		metadata.Blob = blob
	} else {
		data := message["data"].([]byte)
		encoding := message["data_encoding"].(string)

		metadata.Blob = persistence.NewDataBlob(data, encoding)
	}

	return metadata, nil
}
