// Copyright (c) 2019 Uber Technologies, Inc.
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

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/cassandra"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/config"
)

const (
	emptyMessageID = -1
)

const (
	templateEnqueueMessageQuery      = `INSERT INTO queue (queue_type, message_id, message_payload) VALUES(?, ?, ?) IF NOT EXISTS`
	templateGetLastMessageIDQuery    = `SELECT message_id FROM queue WHERE queue_type=? ORDER BY message_id DESC LIMIT 1`
	templateGetMessagesQuery         = `SELECT message_id, message_payload FROM queue WHERE queue_type = ? and message_id > ? LIMIT ?`
	templateGetMessagesFromDLQQuery  = `SELECT message_id, message_payload FROM queue WHERE queue_type = ? and message_id > ? and message_id <= ?`
	templateDeleteMessagesQuery      = `DELETE FROM queue WHERE queue_type = ? and message_id > ? and message_id <= ?`
	templateDeleteMessageQuery       = `DELETE FROM queue WHERE queue_type = ? and message_id = ?`
	templateGetQueueMetadataQuery    = `SELECT cluster_ack_level, version FROM queue_metadata WHERE queue_type = ?`
	templateInsertQueueMetadataQuery = `INSERT INTO queue_metadata (queue_type, cluster_ack_level, version) VALUES(?, ?, ?) IF NOT EXISTS`
	templateUpdateQueueMetadataQuery = `UPDATE queue_metadata SET cluster_ack_level = ?, version = ? WHERE queue_type = ? IF version = ?`
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
		clusterAckLevels map[string]int
		// version is used for CAS operation.
		version int
	}
)

func newQueue(
	cfg config.Cassandra,
	logger log.Logger,
	queueType persistence.QueueType,
) (persistence.Queue, error) {
	cluster := cassandra.NewCassandraCluster(cfg)
	cluster.ProtoVersion = cassandraProtoVersion
	cluster.Consistency = gocql.LocalQuorum
	cluster.SerialConsistency = gocql.LocalSerial
	cluster.Timeout = defaultSessionTimeout

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	retryPolicy := backoff.NewExponentialRetryPolicy(100 * time.Millisecond)
	retryPolicy.SetBackoffCoefficient(1.5)
	retryPolicy.SetMaximumAttempts(5)

	queue := &cassandraQueue{
		cassandraStore: cassandraStore{session: session, logger: logger},
		logger:         logger,
		queueType:      queueType,
	}
	if err := queue.createQueueMetadataEntryIfNotExist(); err != nil {
		return nil, fmt.Errorf("failed to check and create queue metadata entry: %v", err)
	}

	return queue, nil
}

func (q *cassandraQueue) createQueueMetadataEntryIfNotExist() error {
	queueMetadata, err := q.getQueueMetadata(q.queueType)
	if err != nil {
		return err
	}

	if queueMetadata == nil {
		if err := q.insertInitialQueueMetadataRecord(q.queueType); err != nil {
			return err
		}
	}

	dlqMetadata, err := q.getQueueMetadata(q.getDLQTypeFromQueueType())
	if err != nil {
		return err
	}

	if dlqMetadata == nil {
		return q.insertInitialQueueMetadataRecord(q.getDLQTypeFromQueueType())
	}

	return nil
}

func (q *cassandraQueue) EnqueueMessage(
	messagePayload []byte,
) error {
	lastMessageID, err := q.getLastMessageID(q.queueType)
	if err != nil {
		return err
	}

	_, err = q.tryEnqueue(q.queueType, lastMessageID+1, messagePayload)
	return err
}

func (q *cassandraQueue) EnqueueMessageToDLQ(
	messagePayload []byte,
) (int, error) {
	// Use negative queue type as the dlq type
	lastMessageID, err := q.getLastMessageID(q.getDLQTypeFromQueueType())
	if err != nil {
		return emptyMessageID, err
	}

	// Use negative queue type as the dlq type
	return q.tryEnqueue(q.getDLQTypeFromQueueType(), lastMessageID+1, messagePayload)
}

func (q *cassandraQueue) tryEnqueue(
	queueType persistence.QueueType,
	messageID int,
	messagePayload []byte,
) (int, error) {
	query := q.session.Query(templateEnqueueMessageQuery, queueType, messageID, messagePayload)
	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		if isThrottlingError(err) {
			return emptyMessageID, &workflow.ServiceBusyError{
				Message: fmt.Sprintf("Failed to enqueue message. Error: %v, Type: %v.", err, queueType),
			}
		}
		return emptyMessageID, &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to enqueue message. Error: %v, Type: %v.", err, queueType),
		}
	}

	if !applied {
		return emptyMessageID, &persistence.ConditionFailedError{Msg: fmt.Sprintf("message ID %v exists in queue", previous["message_id"])}
	}

	return messageID, nil
}

func (q *cassandraQueue) getLastMessageID(
	queueType persistence.QueueType,
) (int, error) {

	query := q.session.Query(templateGetLastMessageIDQuery, queueType)
	result := make(map[string]interface{})
	err := query.MapScan(result)
	if err != nil {
		if err == gocql.ErrNotFound {
			return emptyMessageID, nil
		} else if isThrottlingError(err) {
			return emptyMessageID, &workflow.ServiceBusyError{
				Message: fmt.Sprintf("Failed to get last message ID for queue %v. Error: %v", queueType, err),
			}
		}

		return emptyMessageID, &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to get last message ID for queue %v. Error: %v", queueType, err),
		}
	}

	return result["message_id"].(int), nil
}

func (q *cassandraQueue) ReadMessages(
	lastMessageID int,
	maxCount int,
) ([]*persistence.QueueMessage, error) {
	// Reading replication tasks need to be quorum level consistent, otherwise we could loose task
	query := q.session.Query(templateGetMessagesQuery,
		q.queueType,
		lastMessageID,
		maxCount,
	)

	iter := query.Iter()
	if iter == nil {
		return nil, &workflow.InternalServiceError{
			Message: "ReadMessages operation failed. Not able to create query iterator.",
		}
	}

	var result []*persistence.QueueMessage
	message := make(map[string]interface{})
	for iter.MapScan(message) {
		payload := getMessagePayload(message)
		id := getMessageID(message)
		result = append(result, &persistence.QueueMessage{ID: id, Payload: payload})
		message = make(map[string]interface{})
	}

	if err := iter.Close(); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("ReadMessages operation failed. Error: %v", err),
		}
	}

	return result, nil
}

func (q *cassandraQueue) ReadMessagesFromDLQ(
	firstMessageID int,
	lastMessageID int,
	pageSize int,
	pageToken []byte,
) ([]*persistence.QueueMessage, []byte, error) {
	// Reading replication tasks need to be quorum level consistent, otherwise we could loose task
	// Use negative queue type as the dlq type
	query := q.session.Query(templateGetMessagesFromDLQQuery,
		q.getDLQTypeFromQueueType(),
		firstMessageID,
		lastMessageID,
	).PageSize(pageSize).PageState(pageToken)

	iter := query.Iter()
	if iter == nil {
		return nil, nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("ReadMessagesFromDLQ operation failed. Not able to create query iterator."),
		}
	}

	var result []*persistence.QueueMessage
	message := make(map[string]interface{})
	for iter.MapScan(message) {
		payload := getMessagePayload(message)
		id := getMessageID(message)
		result = append(result, &persistence.QueueMessage{ID: id, Payload: payload})
		message = make(map[string]interface{})
	}

	nextPageToken := iter.PageState()
	newPageToken := make([]byte, len(nextPageToken))
	copy(newPageToken, nextPageToken)
	if err := iter.Close(); err != nil {
		return nil, nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("ReadMessagesFromDLQ operation failed. Error: %v", err),
		}
	}

	return result, newPageToken, nil
}

func getMessagePayload(
	message map[string]interface{},
) []byte {

	return message["message_payload"].([]byte)
}

func getMessageID(
	message map[string]interface{},
) int {

	return message["message_id"].(int)
}

func (q *cassandraQueue) DeleteMessagesBefore(
	messageID int,
) error {

	query := q.session.Query(templateDeleteMessagesQuery, q.queueType, messageID)
	if err := query.Exec(); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("DeleteMessagesBefore operation failed. Error %v", err),
		}
	}

	return nil
}

func (q *cassandraQueue) DeleteMessageFromDLQ(
	messageID int,
) error {

	// Use negative queue type as the dlq type
	query := q.session.Query(templateDeleteMessageQuery, q.getDLQTypeFromQueueType(), messageID)
	if err := query.Exec(); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("DeleteMessageFromDLQ operation failed. Error %v", err),
		}
	}

	return nil
}

func (q *cassandraQueue) RangeDeleteMessagesFromDLQ(
	firstMessageID int,
	lastMessageID int,
) error {

	// Use negative queue type as the dlq type
	query := q.session.Query(templateDeleteMessagesQuery, q.getDLQTypeFromQueueType(), firstMessageID, lastMessageID)
	if err := query.Exec(); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("RangeDeleteMessagesFromDLQ operation failed. Error %v", err),
		}
	}

	return nil
}

func (q *cassandraQueue) insertInitialQueueMetadataRecord(
	queueType persistence.QueueType,
) error {

	version := 0
	clusterAckLevels := map[string]int{}
	query := q.session.Query(templateInsertQueueMetadataQuery, queueType, clusterAckLevels, version)
	_, err := query.ScanCAS()
	if err != nil {
		return fmt.Errorf("failed to insert initial queue metadata record: %v, Type: %v", err, queueType)
	}
	// it's ok if the query is not applied, which means that the record exists already.
	return nil
}

func (q *cassandraQueue) UpdateAckLevel(
	messageID int,
	clusterName string,
) error {

	return q.updateAckLevel(messageID, clusterName, q.queueType)
}

func (q *cassandraQueue) GetAckLevels() (map[string]int, error) {
	queueMetadata, err := q.getQueueMetadata(q.queueType)
	if err != nil {
		return nil, err
	}

	return queueMetadata.clusterAckLevels, nil
}

func (q *cassandraQueue) UpdateDLQAckLevel(
	messageID int,
	clusterName string,
) error {

	return q.updateAckLevel(messageID, clusterName, q.getDLQTypeFromQueueType())
}

func (q *cassandraQueue) GetDLQAckLevels() (map[string]int, error) {

	// Use negative queue type as the dlq type
	queueMetadata, err := q.getQueueMetadata(q.getDLQTypeFromQueueType())
	if err != nil {
		return nil, err
	}

	return queueMetadata.clusterAckLevels, nil
}

func (q *cassandraQueue) getQueueMetadata(
	queueType persistence.QueueType,
) (*queueMetadata, error) {

	query := q.session.Query(templateGetQueueMetadataQuery, queueType)
	var ackLevels map[string]int
	var version int
	err := query.Scan(&ackLevels, &version)
	if err != nil {
		if err == gocql.ErrNotFound {
			return nil, nil
		}

		return nil, fmt.Errorf("failed to get queue metadata: %v", err)
	}

	// if record exist but ackLevels is empty, we initialize the map
	if ackLevels == nil {
		ackLevels = make(map[string]int)
	}

	return &queueMetadata{clusterAckLevels: ackLevels, version: version}, nil
}

func (q *cassandraQueue) updateQueueMetadata(
	metadata *queueMetadata,
	queueType persistence.QueueType,
) error {

	query := q.session.Query(templateUpdateQueueMetadataQuery,
		metadata.clusterAckLevels,
		metadata.version,
		queueType,
		metadata.version-1,
	)
	applied, err := query.ScanCAS()
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateAckLevel operation failed. Error %v", err),
		}
	}
	if !applied {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateAckLevel operation encounter concurrent write."),
		}
	}

	return nil
}

func (q *cassandraQueue) getDLQTypeFromQueueType() persistence.QueueType {
	return -q.queueType
}

func (q *cassandraQueue) updateAckLevel(
	messageID int,
	clusterName string,
	queueType persistence.QueueType,
) error {

	queueMetadata, err := q.getQueueMetadata(queueType)
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateDLQAckLevel operation failed. Error %v", err),
		}
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
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateDLQAckLevel operation failed. Error %v", err),
		}
	}
	return nil
}

func (q *cassandraQueue) Close() {
	if q.session != nil {
		q.session.Close()
	}
}
