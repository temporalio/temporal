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
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/persistence/serialization"
)

const (
	templateEnqueueMessageQuery       = `INSERT INTO queue (queue_type, message_id, message_payload, message_encoding) VALUES(?, ?, ?, ?) IF NOT EXISTS`
	templateGetLastMessageIDQuery     = `SELECT message_id FROM queue WHERE queue_type=? ORDER BY message_id DESC LIMIT 1`
	templateGetMessagesQuery          = `SELECT message_id, message_payload, message_encoding FROM queue WHERE queue_type = ? and message_id > ? LIMIT ?`
	templateGetMessagesFromDLQQuery   = `SELECT message_id, message_payload, message_encoding FROM queue WHERE queue_type = ? and message_id > ? and message_id <= ?`
	templateDeleteMessagesBeforeQuery = `DELETE FROM queue WHERE queue_type = ? and message_id < ?`
	templateDeleteMessagesQuery       = `DELETE FROM queue WHERE queue_type = ? and message_id > ? and message_id <= ?`
	templateDeleteMessageQuery        = `DELETE FROM queue WHERE queue_type = ? and message_id = ?`

	templateGetQueueMetadataQuery    = `SELECT cluster_ack_level, data, data_encoding, version FROM queue_metadata WHERE queue_type = ?`
	templateInsertQueueMetadataQuery = `INSERT INTO queue_metadata (queue_type, cluster_ack_level, data, data_encoding, version) VALUES(?, ?, ?, ?, ?) IF NOT EXISTS`
	templateUpdateQueueMetadataQuery = `UPDATE queue_metadata SET cluster_ack_level = ?, data = ?, data_encoding = ?, version = ? WHERE queue_type = ? IF version = ?`
)

type (
	QueueStore struct {
		queueType persistence.QueueType
		session   gocql.Session
		logger    log.Logger
	}
)

func NewQueueStore(
	queueType persistence.QueueType,
	session gocql.Session,
	logger log.Logger,
) (persistence.Queue, error) {
	return &QueueStore{
		queueType: queueType,
		session:   session,
		logger:    logger,
	}, nil
}

func (q *QueueStore) Init(
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

func (q *QueueStore) EnqueueMessage(
	ctx context.Context,
	blob commonpb.DataBlob,
) error {
	lastMessageID, err := q.getLastMessageID(ctx, q.queueType)
	if err != nil {
		return err
	}

	_, err = q.tryEnqueue(ctx, q.queueType, lastMessageID+1, blob)
	return err
}

func (q *QueueStore) EnqueueMessageToDLQ(
	ctx context.Context,
	blob commonpb.DataBlob,
) (int64, error) {
	// Use negative queue type as the dlq type
	lastMessageID, err := q.getLastMessageID(ctx, q.getDLQTypeFromQueueType())
	if err != nil {
		return persistence.EmptyQueueMessageID, err
	}

	// Use negative queue type as the dlq type
	return q.tryEnqueue(ctx, q.getDLQTypeFromQueueType(), lastMessageID+1, blob)
}

func (q *QueueStore) tryEnqueue(
	ctx context.Context,
	queueType persistence.QueueType,
	messageID int64,
	blob commonpb.DataBlob,
) (int64, error) {
	query := q.session.Query(templateEnqueueMessageQuery, queueType, messageID, blob.Data, blob.EncodingType.String()).WithContext(ctx)
	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		return persistence.EmptyQueueMessageID, gocql.ConvertError("tryEnqueue", err)
	}

	if !applied {
		return persistence.EmptyQueueMessageID, &persistence.ConditionFailedError{Msg: fmt.Sprintf("message ID %v exists in queue", previous["message_id"])}
	}

	return messageID, nil
}

func (q *QueueStore) getLastMessageID(
	ctx context.Context,
	queueType persistence.QueueType,
) (int64, error) {

	query := q.session.Query(templateGetLastMessageIDQuery, queueType).WithContext(ctx)
	result := make(map[string]interface{})
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
	message := make(map[string]interface{})
	for iter.MapScan(message) {
		queueMessage := convertQueueMessage(message)
		result = append(result, queueMessage)
		message = make(map[string]interface{})
	}

	if err := iter.Close(); err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("ReadMessages operation failed. Error: %v", err))
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
	message := make(map[string]interface{})
	for iter.MapScan(message) {
		queueMessage := convertQueueMessage(message)
		result = append(result, queueMessage)
		message = make(map[string]interface{})
	}

	var nextPageToken []byte
	if len(iter.PageState()) > 0 {
		nextPageToken = iter.PageState()
	}
	if err := iter.Close(); err != nil {
		return nil, nil, serviceerror.NewUnavailable(fmt.Sprintf("ReadMessagesFromDLQ operation failed. Error: %v", err))
	}

	return result, nextPageToken, nil
}

func (q *QueueStore) DeleteMessagesBefore(
	ctx context.Context,
	messageID int64,
) error {

	query := q.session.Query(templateDeleteMessagesBeforeQuery, q.queueType, messageID).WithContext(ctx)
	if err := query.Exec(); err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("DeleteMessagesBefore operation failed. Error %v", err))
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
		return serviceerror.NewUnavailable(fmt.Sprintf("DeleteMessageFromDLQ operation failed. Error %v", err))
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
		return serviceerror.NewUnavailable(fmt.Sprintf("RangeDeleteMessagesFromDLQ operation failed. Error %v", err))
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
	_, err := query.MapScanCAS(make(map[string]interface{}))
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
	message := make(map[string]interface{})
	err := query.MapScan(message)
	if err != nil {
		return nil, err
	}

	return convertQueueMetadata(message)
}

func (q *QueueStore) updateAckLevel(
	ctx context.Context,
	metadata *persistence.InternalQueueMetadata,
	queueType persistence.QueueType,
) error {

	// TODO: remove this once cluster_ack_level is removed from DB
	metadataStruct, err := serialization.QueueMetadataFromBlob(metadata.Blob.Data, metadata.Blob.EncodingType.String())
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
	applied, err := query.MapScanCAS(make(map[string]interface{}))
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

func (q *QueueStore) getDLQTypeFromQueueType() persistence.QueueType {
	return -q.queueType
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
	message map[string]interface{},
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
	message map[string]interface{},
) (*persistence.InternalQueueMetadata, error) {

	metadata := &persistence.InternalQueueMetadata{
		Version: message["version"].(int64),
	}
	_, ok := message["cluster_ack_level"]
	if ok {
		clusterAckLevel := message["cluster_ack_level"].(map[string]int64)
		// TODO: remove this once we remove cluster_ack_level from DB.
		blob, err := serialization.QueueMetadataToBlob(&persistencespb.QueueMetadata{ClusterAckLevels: clusterAckLevel})
		if err != nil {
			return nil, err
		}
		metadata.Blob = &blob
	} else {
		data := message["data"].([]byte)
		encoding := message["data_encoding"].(string)

		metadata.Blob = persistence.NewDataBlob(data, encoding)
	}

	return metadata, nil
}
