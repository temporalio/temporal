// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
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
	"encoding/json"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

const (
	templateEnqueueMessageQueryV2   = `INSERT INTO queue_v2 (queue_type, queue_name, queue_partition, message_id, message_payload, message_encoding) VALUES(?, ?, ?, ?, ?, ?) IF NOT EXISTS`
	templateGetLastMessageIDQueryV2 = `SELECT message_id FROM queue WHERE queue_type=? and queue_name=? and queue_partition=? ORDER BY message_id DESC LIMIT 1`
	templateDeleteMessagesQueryV2   = `DELETE FROM queue WHERE queue_type = ? and queue_name = ? and message_id > ? and message_id <= ?`
	templateGetMessagesQueryV2      = `SELECT message_id, message_payload, message_encoding FROM queue WHERE queue_type = ? and queue_name = ? and partition = ? and message_id > ? LIMIT ?`

	templateInsertQueueMetadataQueryV2          = `INSERT INTO queue_metadata_v2 (queue_type, queue_name, metadata_payload, metadata_encoding, version) VALUES(?, ?, ?, ?, ?) IF NOT EXISTS`
	templateGetQueuesListQueryV2                = `SELECT queue_name FROM queue_metadata_v2 WHERE queue_type=?`
	templateGetCurrentAckLevelAndVersionQueryV2 = `SELECT metadata_payload, metadata_encoding, version FROM queue_metadata_v2 WHERE queue_type = ? and queue_name = ?`
	templateUpdateQueueMetadataQueryV2          = `UPDATE queue_metadata SET metadata_payload = ?, metadata_encoding= ?, version = ? WHERE queue_type = ? and queue_name = ? IF version = ?`

	EmptyPartition = 0
)

type (
	QueueStoreV2 struct {
		session gocql.Session
		logger  log.Logger
	}

	// TODO: add a proto at some point
	QueueV2MetadataPayload struct {
		AckLevel int64
	}
)

func NewQueueStoreV2(
	queueType persistence.QueueV2Type,
	session gocql.Session,
	logger log.Logger,
) (persistence.QueueV2, error) {
	return &QueueStoreV2{
		session: session,
		logger:  logger,
	}, nil
}

func (q *QueueStoreV2) CreateQueue(
	ctx context.Context,
	request persistence.InternalCreateQueueRequest,
) (*persistence.InternalCreateQueueResponse, error) {
	version := 0
	blob, err := convertQueueV2MetadataToBlob(&QueueV2MetadataPayload{AckLevel: 0})
	if err != nil {
		return nil, fmt.Errorf("failed to insert initial queue metadata record: %v, Type: %v", err, request.QueueType)
	}
	query := q.session.Query(templateInsertQueueMetadataQueryV2,
		request.QueueType,
		request.QueueName,
		blob.Data,
		blob.EncodingType.String(),
		version,
	).WithContext(ctx)
	_, err = query.MapScanCAS(make(map[string]interface{}))
	if err != nil {
		return nil, fmt.Errorf("failed to insert initial queue metadata record: %v, Type: %v", err, request.QueueType)
	}
	// it's ok if the query is not applied, which means that the record exists already.
	return &persistence.InternalCreateQueueResponse{}, nil
}

func (q *QueueStoreV2) EnqueueMessage(ctx context.Context, request persistence.InternalEnqueueMessageRequest) (*persistence.InternalEnqueueMessageResponse, error) {
	lastMessageID, err := q.getLastMessageID(ctx, request, EmptyPartition)
	if err != nil {
		return nil, err
	}

	messageID, err := q.tryEnqueue(ctx, request, lastMessageID+1, EmptyPartition)
	if err != nil {
		return nil, err
	}

	return &persistence.InternalEnqueueMessageResponse{Metadata: persistence.MessageMetadata{ID: messageID}}, nil
}

func (q *QueueStoreV2) ReadMessages(ctx context.Context, request persistence.InternalReadMessagesRequest) (*persistence.InternalReadMessagesResponse, error) {
	// Reading replication tasks need to be quorum level consistent, otherwise we could lose tasks
	var minMessageID int64
	var pageToken *[]byte
	if request.NextPageToken.PageToken != nil {
		minMessageID = request.NextPageToken.MessageID
		pageToken = &request.NextPageToken.PageToken
	} else {
		ackLevel, _, err := q.getCurrentAckLevelAndVersion(ctx, request.QueueType, request.QueueName)
		if err != nil {
			return nil, err
		}
		minMessageID = ackLevel
	}

	query := q.session.Query(templateGetMessagesQueryV2,
		request.QueueType,
		request.QueueName,
		EmptyPartition,
		minMessageID,
	).WithContext(ctx)
	iter := query.PageSize(request.PageSize).PageState(*pageToken).Iter()

	var result []persistence.Message
	message := make(map[string]interface{})
	for iter.MapScan(message) {
		queueMessage := convertQueueV2Message(message)
		result = append(result, *queueMessage)
		message = make(map[string]interface{})
	}

	var nextPageToken []byte
	if len(iter.PageState()) > 0 {
		nextPageToken = iter.PageState()
	}

	if err := iter.Close(); err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("ReadMessages operation failed. Error: %v", err))
	}

	return &persistence.InternalReadMessagesResponse{Messages: result,
		NextPageToken: persistence.InternalReadMessagePageToken{
			MessageID: minMessageID,
			PageToken: nextPageToken,
		},
	}, nil
}

func (q *QueueStoreV2) RangeDeleteMessages(ctx context.Context, request persistence.InternalRangeDeleteMessagesRequest) (*persistence.InternalRangeDeleteMessagesResponse, error) {
	ackLevel, version, err := q.getCurrentAckLevelAndVersion(ctx, request.QueueType, request.QueueName)
	if err != nil {
		return nil, err
	}

	query := q.session.Query(templateDeleteMessagesQueryV2, request.QueueType, request.QueueName, ackLevel, request.InclusiveMaxMessageMetadata.ID).WithContext(ctx)
	if err := query.Exec(); err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("RangeDeleteMessagesFromDLQ operation failed. Error %v, Type: %v, Name: %v", err, request.QueueType, request.QueueName))
	}

	return &persistence.InternalRangeDeleteMessagesResponse{}, q.updateAckLevel(ctx, request, version)
}

func (q *QueueStoreV2) ListQueues(ctx context.Context, request persistence.InternalListQueuesRequest) (*persistence.InternalListQueuesResponse, error) {
	query := q.session.Query(templateGetQueuesListQueryV2, request.QueueType).WithContext(ctx)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()

	response := persistence.InternalListQueuesResponse{}
	queueName := make(map[string]interface{})
	for iter.MapScan(queueName) {
		response.QueueNames = append(response.QueueNames, queueName["queue_name"].(string))
		queueName = make(map[string]interface{})
	}

	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}

	if err := iter.Close(); err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("ListQueues operation failed. Error: %v, Type: %v", err, request.QueueType))
	}

	return &response, nil
}

func (q *QueueStoreV2) Close() {
	if q.session != nil {
		q.session.Close()
	}
}

func (q *QueueStoreV2) tryEnqueue(
	ctx context.Context,
	request persistence.InternalEnqueueMessageRequest,
	messageID int64,
	partition int,
) (int64, error) {
	query := q.session.Query(templateEnqueueMessageQuery, request.QueueType, request.QueueName, partition, messageID, request.Blob.Data, request.Blob.EncodingType.String()).WithContext(ctx)
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

func (q *QueueStoreV2) getLastMessageID(
	ctx context.Context,
	request persistence.InternalEnqueueMessageRequest,
	partition int,
) (int64, error) {
	query := q.session.Query(templateGetLastMessageIDQuery, request.QueueType, request.QueueName, partition).WithContext(ctx)
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

func (q *QueueStoreV2) getCurrentAckLevelAndVersion(ctx context.Context, queueType persistence.QueueV2Type, queueName string) (int64, int64, error) {
	query := q.session.Query(templateGetCurrentAckLevelAndVersionQueryV2, queueType, queueName).WithContext(ctx)
	result := make(map[string]interface{})
	err := query.MapScan(result)
	if err != nil {
		if gocql.IsNotFoundError(err) {
			return 0, 0, nil
		}
		return 0, 0, gocql.ConvertError("getLastMessageID", err)
	}
	version := result["version"].(int64)
	payload := result["metadata_payload"].([]byte)
	enconding := result["metadata_encoding"].(string)

	ackLevel, err := getAckLevelFromQueueV2Metadata(payload, enconding)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get ack level from queue metadata: %v, Type: %v, Name: %v", err, queueType, queueName)
	}
	return ackLevel, version, nil
}

func (q *QueueStoreV2) updateAckLevel(ctx context.Context, request persistence.InternalRangeDeleteMessagesRequest, version int64) error {
	queueMetadata := QueueV2MetadataPayload{
		AckLevel: request.InclusiveMaxMessageMetadata.ID,
	}
	blob, err := convertQueueV2MetadataToBlob(&queueMetadata)
	if err != nil {
		return fmt.Errorf("failed to update ack level: %v, Type: %v, Name: %v", err, request.QueueType, request.QueueName)
	}

	for {
		query := q.session.Query(templateUpdateQueueMetadataQueryV2,
			blob.Data,
			blob.EncodingType.String(),
			version+1, // always increase version number on update
			request.QueueType,
			request.QueueName,
			version, // condition update
		).WithContext(ctx)

		result := make(map[string]interface{})
		applied, err := query.MapScanCAS(result)
		if err != nil {
			return gocql.ConvertError("updateAckLevel", err)
		}
		if applied {
			break
		}
		version = result["version"].(int64)
		payload := result["metadata_payload"].([]byte)
		enconding := result["metadata_encoding"].(string)

		ackLevel, err := getAckLevelFromQueueV2Metadata(payload, enconding)
		if err != nil {
			return fmt.Errorf("failed to get ack level from queue metadata: %v, Type: %v, Name: %v", err, request.QueueType, request.QueueName)
		}
		if ackLevel >= request.InclusiveMaxMessageMetadata.ID {
			break
		}
		// otherwise repeat until we get it right
		// TODO: do we need to limit the number of retries?
	}
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

func convertQueueV2Message(
	message map[string]interface{},
) *persistence.Message {

	id := message["message_id"].(int64)
	data := message["message_payload"].([]byte)
	encoding := message["message_encoding"].(string)
	if encoding == "" {
		encoding = enumspb.ENCODING_TYPE_PROTO3.String()
	}
	return &persistence.Message{
		MetaData: persistence.MessageMetadata{ID: id},
		Data:     *persistence.NewDataBlob(data, encoding),
	}
}
