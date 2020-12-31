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
	"fmt"
	"time"

	"github.com/gocql/gocql"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
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
	cassandraQueue struct {
		queueType persistence.QueueType
		logger    log.Logger
		cassandraStore
	}

	// Note that this struct is defined in the cassandra package not the persistence interface
	// because we only have ack levels in metadata (version is a cassandra only concept because
	// of the CAS operation). Consider moving this to persistence interface if we end up having
	// more shared fields.
	queueMetadata struct {
		clusterAckLevels map[string]int64
		// version is used for CAS operation.
		version int64
	}
)

func newQueue(
	session *gocql.Session,
	logger log.Logger,
	queueType persistence.QueueType,
) (persistence.Queue, error) {

	retryPolicy := backoff.NewExponentialRetryPolicy(100 * time.Millisecond)
	retryPolicy.SetBackoffCoefficient(1.5)
	retryPolicy.SetMaximumAttempts(5)

	queue := &cassandraQueue{
		cassandraStore: cassandraStore{session: session, logger: logger},
		logger:         logger,
		queueType:      queueType,
	}
	if err := queue.initializeQueueMetadata(); err != nil {
		return nil, err
	}
	if err := queue.initializeDLQMetadata(); err != nil {
		return nil, err
	}

	return queue, nil
}

func (q *cassandraQueue) EnqueueMessage(
	blob commonpb.DataBlob,
) error {
	lastMessageID, err := q.getLastMessageID(q.queueType)
	if err != nil {
		return err
	}

	_, err = q.tryEnqueue(q.queueType, lastMessageID+1, blob)
	return err
}

func (q *cassandraQueue) EnqueueMessageToDLQ(
	blob commonpb.DataBlob,
) (int64, error) {
	// Use negative queue type as the dlq type
	lastMessageID, err := q.getLastMessageID(q.getDLQTypeFromQueueType())
	if err != nil {
		return persistence.EmptyQueueMessageID, err
	}

	// Use negative queue type as the dlq type
	return q.tryEnqueue(q.getDLQTypeFromQueueType(), lastMessageID+1, blob)
}

func (q *cassandraQueue) tryEnqueue(
	queueType persistence.QueueType,
	messageID int64,
	blob commonpb.DataBlob,
) (int64, error) {
	query := q.session.Query(templateEnqueueMessageQuery, queueType, messageID, blob.Data, blob.EncodingType.String())
	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		if isThrottlingError(err) {
			return persistence.EmptyQueueMessageID, serviceerror.NewResourceExhausted(fmt.Sprintf("Failed to enqueue message. Error: %v, Type: %v.", err, queueType))
		}
		return persistence.EmptyQueueMessageID, serviceerror.NewInternal(fmt.Sprintf("Failed to enqueue message. Error: %v, Type: %v.", err, queueType))
	}

	if !applied {
		return persistence.EmptyQueueMessageID, &persistence.ConditionFailedError{Msg: fmt.Sprintf("message ID %v exists in queue", previous["message_id"])}
	}

	return messageID, nil
}

func (q *cassandraQueue) getLastMessageID(
	queueType persistence.QueueType,
) (int64, error) {

	query := q.session.Query(templateGetLastMessageIDQuery, queueType)
	result := make(map[string]interface{})
	err := query.MapScan(result)
	if err != nil {
		if err == gocql.ErrNotFound {
			return persistence.EmptyQueueMessageID, nil
		} else if isThrottlingError(err) {
			return persistence.EmptyQueueMessageID, serviceerror.NewResourceExhausted(fmt.Sprintf("Failed to get last message ID for queue %v. Error: %v", queueType, err))
		}

		return persistence.EmptyQueueMessageID, serviceerror.NewInternal(fmt.Sprintf("Failed to get last message ID for queue %v. Error: %v", queueType, err))
	}

	return result["message_id"].(int64), nil
}

func (q *cassandraQueue) ReadMessages(
	lastMessageID int64,
	maxCount int,
) ([]*persistence.QueueMessage, error) {
	// Reading replication tasks need to be quorum level consistent, otherwise we could lose tasks
	query := q.session.Query(templateGetMessagesQuery,
		q.queueType,
		lastMessageID,
		maxCount,
	)

	iter := query.Iter()
	if iter == nil {
		return nil, serviceerror.NewInternal("ReadMessages operation failed. Not able to create query iterator.")
	}

	var result []*persistence.QueueMessage
	message := make(map[string]interface{})
	for iter.MapScan(message) {
		queueMessage := convertQueueMessage(message)
		result = append(result, queueMessage)
		message = make(map[string]interface{})
	}

	if err := iter.Close(); err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ReadMessages operation failed. Error: %v", err))
	}

	return result, nil
}

func (q *cassandraQueue) ReadMessagesFromDLQ(
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
	).PageSize(pageSize).PageState(pageToken)

	iter := query.Iter()
	if iter == nil {
		return nil, nil, serviceerror.NewInternal(fmt.Sprintf("ReadMessagesFromDLQ operation failed. Not able to create query iterator."))
	}

	var result []*persistence.QueueMessage
	message := make(map[string]interface{})
	for iter.MapScan(message) {
		queueMessage := convertQueueMessage(message)
		result = append(result, queueMessage)
		message = make(map[string]interface{})
	}

	nextPageToken := iter.PageState()
	newPageToken := make([]byte, len(nextPageToken))
	copy(newPageToken, nextPageToken)
	if err := iter.Close(); err != nil {
		return nil, nil, serviceerror.NewInternal(fmt.Sprintf("ReadMessagesFromDLQ operation failed. Error: %v", err))
	}

	return result, newPageToken, nil
}

func (q *cassandraQueue) DeleteMessagesBefore(
	messageID int64,
) error {

	query := q.session.Query(templateDeleteMessagesBeforeQuery, q.queueType, messageID)
	if err := query.Exec(); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("DeleteMessagesBefore operation failed. Error %v", err))
	}
	return nil
}

func (q *cassandraQueue) DeleteMessageFromDLQ(
	messageID int64,
) error {

	// Use negative queue type as the dlq type
	query := q.session.Query(templateDeleteMessageQuery, q.getDLQTypeFromQueueType(), messageID)
	if err := query.Exec(); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("DeleteMessageFromDLQ operation failed. Error %v", err))
	}

	return nil
}

func (q *cassandraQueue) RangeDeleteMessagesFromDLQ(
	firstMessageID int64,
	lastMessageID int64,
) error {

	// Use negative queue type as the dlq type
	query := q.session.Query(templateDeleteMessagesQuery, q.getDLQTypeFromQueueType(), firstMessageID, lastMessageID)
	if err := query.Exec(); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("RangeDeleteMessagesFromDLQ operation failed. Error %v", err))
	}

	return nil
}

func (q *cassandraQueue) UpdateAckLevel(
	messageID int64,
	clusterName string,
) error {

	return q.updateAckLevel(messageID, clusterName, q.queueType)
}

func (q *cassandraQueue) GetAckLevels() (map[string]int64, error) {
	queueMetadata, err := q.getQueueMetadata(q.queueType)
	if err != nil {
		return nil, err
	}

	return queueMetadata.clusterAckLevels, nil
}

func (q *cassandraQueue) UpdateDLQAckLevel(
	messageID int64,
	clusterName string,
) error {

	return q.updateAckLevel(messageID, clusterName, q.getDLQTypeFromQueueType())
}

func (q *cassandraQueue) GetDLQAckLevels() (map[string]int64, error) {

	// Use negative queue type as the dlq type
	queueMetadata, err := q.getQueueMetadata(q.getDLQTypeFromQueueType())
	if err != nil {
		return nil, err
	}

	return queueMetadata.clusterAckLevels, nil
}

func (q *cassandraQueue) insertInitialQueueMetadataRecord(
	queueType persistence.QueueType,
) error {

	version := 0
	clusterAckLevels := map[string]int64{}
	blob, err := serialization.QueueMetadataToBlob(&persistencespb.QueueMetadata{
		ClusterAckLevels: clusterAckLevels,
	})
	if err != nil {
		return err
	}

	query := q.session.Query(templateInsertQueueMetadataQuery,
		queueType,
		clusterAckLevels,
		blob.Data,
		blob.EncodingType.String(),
		version,
	)
	_, err = query.MapScanCAS(make(map[string]interface{}))
	if err != nil {
		return fmt.Errorf("failed to insert initial queue metadata record: %v, Type: %v", err, queueType)
	}
	// it's ok if the query is not applied, which means that the record exists already.
	return nil
}

func (q *cassandraQueue) getQueueMetadata(
	queueType persistence.QueueType,
) (*queueMetadata, error) {

	query := q.session.Query(templateGetQueueMetadataQuery, queueType)
	message := make(map[string]interface{})
	err := query.MapScan(message)
	if err != nil {
		return nil, err
	}

	return convertQueueMetadata(message)
}

func (q *cassandraQueue) updateQueueMetadata(
	metadata *queueMetadata,
	queueType persistence.QueueType,
) error {

	blob, err := serialization.QueueMetadataToBlob(&persistencespb.QueueMetadata{
		ClusterAckLevels: metadata.clusterAckLevels,
	})
	if err != nil {
		return err
	}

	query := q.session.Query(templateUpdateQueueMetadataQuery,
		metadata.clusterAckLevels,
		blob.Data,
		blob.EncodingType.String(),
		metadata.version,
		queueType,
		metadata.version-1,
	)
	applied, err := query.MapScanCAS(make(map[string]interface{}))
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("UpdateAckLevel operation failed. Error %v", err))
	}
	if !applied {
		return serviceerror.NewInternal(fmt.Sprintf("UpdateAckLevel operation encounter concurrent write."))
	}

	return nil
}

func (q *cassandraQueue) updateAckLevel(
	messageID int64,
	clusterName string,
	queueType persistence.QueueType,
) error {

	queueMetadata, err := q.getQueueMetadata(queueType)
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("UpdateDLQAckLevel operation failed. Error %v", err))
	}

	// Ignore possibly delayed message
	if queueMetadata.clusterAckLevels[clusterName] > messageID {
		return nil
	}

	queueMetadata.clusterAckLevels[clusterName] = messageID
	queueMetadata.version++

	// Use negative queue type as the dlq type
	err = q.updateQueueMetadata(queueMetadata, queueType)
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("UpdateDLQAckLevel operation failed. Error %v", err))
	}
	return nil
}

func (q *cassandraQueue) Close() {
	if q.session != nil {
		q.session.Close()
	}
}

func (q *cassandraQueue) getDLQTypeFromQueueType() persistence.QueueType {
	return -q.queueType
}

func (q *cassandraQueue) initializeQueueMetadata() error {
	_, err := q.getQueueMetadata(q.queueType)
	switch err {
	case nil:
		return nil
	case gocql.ErrNotFound:
		return q.insertInitialQueueMetadataRecord(q.queueType)
	default:
		return err
	}
}

func (q *cassandraQueue) initializeDLQMetadata() error {
	_, err := q.getQueueMetadata(q.getDLQTypeFromQueueType())
	switch err {
	case nil:
		return nil
	case gocql.ErrNotFound:
		return q.insertInitialQueueMetadataRecord(q.getDLQTypeFromQueueType())
	default:
		return err
	}
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
) (*queueMetadata, error) {

	metadata := &queueMetadata{
		version: message["version"].(int64),
	}

	_, ok := message["cluster_ack_level"]
	if ok {
		clusterAckLevel := message["cluster_ack_level"].(map[string]int64)
		metadata.clusterAckLevels = clusterAckLevel
	} else {
		data := message["data"].([]byte)
		encoding := message["data_encoding"].(string)

		clusterAckLevels, err := serialization.QueueMetadataFromBlob(data, encoding)
		if err != nil {
			return nil, err
		}

		metadata.clusterAckLevels = clusterAckLevels.ClusterAckLevels
	}

	if metadata.clusterAckLevels == nil {
		metadata.clusterAckLevels = make(map[string]int64)
	}

	return metadata, nil
}
