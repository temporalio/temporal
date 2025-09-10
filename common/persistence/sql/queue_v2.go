package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	defaultPartition = 0
)

type (
	queueV2 struct {
		SqlStore
	}

	QueueV2Metadata struct {
		Metadata *persistencespb.Queue
		Version  int64
	}
)

// NewQueueV2 returns an implementation of persistence.QueueV2.
func NewQueueV2(db sqlplugin.DB,
	logger log.Logger,
) persistence.QueueV2 {
	return &queueV2{
		SqlStore: NewSqlStore(db, logger),
	}
}

func (q *queueV2) EnqueueMessage(
	ctx context.Context,
	request *persistence.InternalEnqueueMessageRequest,
) (*persistence.InternalEnqueueMessageResponse, error) {

	_, err := q.getQueueMetadata(ctx, q.DB, request.QueueType, request.QueueName)
	if err != nil {
		return nil, err
	}
	tx, err := q.DB.BeginTx(ctx)
	if err != nil {
		return nil, serviceerror.NewUnavailablef(
			"EnqueueMessage failed for queue with type: %v and name: %v. BeginTx operation failed. Error: %v",
			request.QueueType,
			request.QueueName,
			err,
		)
	}
	lastMessageID, ok, err := q.getMaxMessageID(ctx, request.QueueType, request.QueueName, tx)
	if err != nil {
		rollBackErr := tx.Rollback()
		if rollBackErr != nil {
			q.SqlStore.logger.Error("transaction rollback error", tag.Error(rollBackErr))
		}
		return nil, serviceerror.NewUnavailablef(
			"EnqueueMessage failed for queue with type: %v and name: %v. failed to get last messageId. Error: %v",
			request.QueueType,
			request.QueueName,
			err,
		)
	}
	nextMessageID := int64(persistence.FirstQueueMessageID)
	if ok {
		nextMessageID = lastMessageID + 1
	}
	_, err = tx.InsertIntoQueueV2Messages(ctx, []sqlplugin.QueueV2MessageRow{
		newQueueV2Row(request.QueueType, request.QueueName, nextMessageID, request.Blob),
	})
	if err != nil {
		rollBackErr := tx.Rollback()
		if rollBackErr != nil {
			q.SqlStore.logger.Error("transaction rollback error", tag.Error(rollBackErr))
		}
		return nil, serviceerror.NewUnavailablef(
			"EnqueueMessage failed for queue with type: %v and name: %v. InsertIntoQueueV2Messages operation failed. Error: %v",
			request.QueueType,
			request.QueueName,
			err,
		)
	}

	if err := tx.Commit(); err != nil {
		return nil, serviceerror.NewUnavailablef(
			"EnqueueMessage failed for queue with type: %v and name: %v. commit operation failed. Error: %v",
			request.QueueType,
			request.QueueName,
			err,
		)
	}
	return &persistence.InternalEnqueueMessageResponse{Metadata: persistence.MessageMetadata{ID: nextMessageID}}, err
}

func (q *queueV2) ReadMessages(
	ctx context.Context,
	request *persistence.InternalReadMessagesRequest,
) (*persistence.InternalReadMessagesResponse, error) {

	if request.PageSize <= 0 {
		return nil, persistence.ErrNonPositiveReadQueueMessagesPageSize
	}
	qm, err := q.getQueueMetadata(ctx, q.DB, request.QueueType, request.QueueName)
	if err != nil {
		return nil, err
	}
	minMessageID, err := persistence.GetMinMessageIDToReadForQueueV2(
		request.QueueType,
		request.QueueName,
		request.NextPageToken,
		qm,
	)
	if err != nil {
		return nil, err
	}
	rows, err := q.DB.RangeSelectFromQueueV2Messages(ctx, sqlplugin.QueueV2MessagesFilter{
		QueueType:    request.QueueType,
		QueueName:    request.QueueName,
		Partition:    defaultPartition,
		MinMessageID: minMessageID,
		PageSize:     request.PageSize,
	})
	if err != nil {
		return nil, serviceerror.NewUnavailablef(
			"ReadMessages failed for queue with type: %v and name: %v. RangeSelectFromQueueV2Messages operation failed. Error: %v",
			request.QueueType,
			request.QueueName,
			err,
		)
	}
	var messages []persistence.QueueV2Message
	for _, row := range rows {
		encoding, err := enumspb.EncodingTypeFromString(row.MessageEncoding)
		if err != nil {
			return nil, serialization.NewUnknownEncodingTypeError(row.MessageEncoding)
		}
		encodingType := enumspb.EncodingType(encoding)
		message := persistence.QueueV2Message{
			MetaData: persistence.MessageMetadata{ID: row.MessageID},
			Data: &commonpb.DataBlob{
				EncodingType: encodingType,
				Data:         row.MessagePayload,
			},
		}
		messages = append(messages, message)
	}
	nextPageToken := persistence.GetNextPageTokenForReadMessages(messages)
	response := &persistence.InternalReadMessagesResponse{
		Messages:      messages,
		NextPageToken: nextPageToken,
	}
	return response, nil
}

func newQueueV2Row(
	queueType persistence.QueueV2Type,
	queueName string,
	messageID int64,
	blob *commonpb.DataBlob,
) sqlplugin.QueueV2MessageRow {

	return sqlplugin.QueueV2MessageRow{
		QueueType:       queueType,
		QueueName:       queueName,
		QueuePartition:  defaultPartition,
		MessageID:       messageID,
		MessagePayload:  blob.Data,
		MessageEncoding: blob.EncodingType.String(),
	}
}

func (q *queueV2) CreateQueue(
	ctx context.Context,
	request *persistence.InternalCreateQueueRequest,
) (*persistence.InternalCreateQueueResponse, error) {
	payload := persistencespb.Queue{
		Partitions: map[int32]*persistencespb.QueuePartition{
			defaultPartition: {
				MinMessageId: persistence.FirstQueueMessageID,
			},
		},
	}
	bytes, _ := payload.Marshal()
	row := sqlplugin.QueueV2MetadataRow{
		QueueType:        request.QueueType,
		QueueName:        request.QueueName,
		MetadataPayload:  bytes,
		MetadataEncoding: enumspb.ENCODING_TYPE_PROTO3.String(),
	}
	_, err := q.DB.InsertIntoQueueV2Metadata(ctx, &row)
	if q.DB.IsDupEntryError(err) {
		return nil, fmt.Errorf(
			"%w: queue type %v and name %v",
			persistence.ErrQueueAlreadyExists,
			request.QueueType,
			request.QueueName,
		)
	}
	if err != nil {
		return nil, serviceerror.NewUnavailablef(
			"CreateQueue failed for queue with type: %v and name: %v. InsertIntoQueueV2Metadata operation failed. Error: %v",
			request.QueueType,
			request.QueueName,
			err,
		)
	}
	return &persistence.InternalCreateQueueResponse{}, nil
}

func (q *queueV2) RangeDeleteMessages(
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
	var resp *persistence.InternalRangeDeleteMessagesResponse
	err := q.txExecute(ctx, "RangeDeleteMessages", func(tx sqlplugin.Tx) error {
		qm, err := q.getQueueMetadata(ctx, tx, request.QueueType, request.QueueName)
		if err != nil {
			return err
		}
		partition, err := persistence.GetPartitionForQueueV2(request.QueueType, request.QueueName, qm)
		if err != nil {
			return serviceerror.NewUnavailablef(
				"RangeDeleteMessages failed for queue with type: %v and name: %v. GetPartitionForQueueV2 operation failed. Error: %v",
				request.QueueType,
				request.QueueName,
				err,
			)
		}
		maxMessageID, ok, err := q.getMaxMessageID(ctx, request.QueueType, request.QueueName, tx)
		if err != nil {
			return serviceerror.NewUnavailablef(
				"RangeDeleteMessages failed for queue with type: %v and name: %v. failed to get MaxMessageID. Error: %v",
				request.QueueType,
				request.QueueName,
				err,
			)
		}
		if !ok {
			return nil
		}
		deleteRange, ok := persistence.GetDeleteRange(persistence.DeleteRequest{
			LastIDToDeleteInclusive: request.InclusiveMaxMessageMetadata.ID,
			ExistingMessageRange: persistence.InclusiveMessageRange{
				MinMessageID: partition.MinMessageId,
				MaxMessageID: maxMessageID,
			},
		})
		if !ok {
			resp = &persistence.InternalRangeDeleteMessagesResponse{
				MessagesDeleted: 0,
			}
			return nil
		}
		msgFilter := sqlplugin.QueueV2MessagesFilter{
			QueueType:    request.QueueType,
			QueueName:    request.QueueName,
			Partition:    defaultPartition,
			MinMessageID: deleteRange.MinMessageID,
			MaxMessageID: deleteRange.MaxMessageID,
		}
		_, err = tx.RangeDeleteFromQueueV2Messages(ctx, msgFilter)
		if err != nil {
			return serviceerror.NewUnavailablef(
				"RangeDeleteMessages failed for queue with type: %v and name: %v. RangeDeleteFromQueueV2Messages operation failed. Error: %v",
				request.QueueType,
				request.QueueName,
				err,
			)
		}
		partition.MinMessageId = deleteRange.NewMinMessageID
		bytes, _ := qm.Marshal()
		row := sqlplugin.QueueV2MetadataRow{
			QueueType:        request.QueueType,
			QueueName:        request.QueueName,
			MetadataPayload:  bytes,
			MetadataEncoding: enumspb.ENCODING_TYPE_PROTO3.String(),
		}
		_, err = tx.UpdateQueueV2Metadata(ctx, &row)
		if err != nil {
			return serviceerror.NewUnavailablef(
				"RangeDeleteMessages failed for queue with type: %v and name: %v. UpdateQueueV2Metadata operation failed. Error: %v",
				request.QueueType,
				request.QueueName,
				err,
			)
		}
		resp = &persistence.InternalRangeDeleteMessagesResponse{
			MessagesDeleted: deleteRange.MessagesToDelete,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (q *queueV2) getQueueMetadata(
	ctx context.Context,
	tc sqlplugin.TableCRUD,
	queueType persistence.QueueV2Type,
	queueName string,
) (*persistencespb.Queue, error) {

	filter := sqlplugin.QueueV2MetadataFilter{
		QueueType: queueType,
		QueueName: queueName,
	}
	var (
		metadata *sqlplugin.QueueV2MetadataRow
		err      error
	)
	switch tc.(type) {
	case sqlplugin.Tx:
		metadata, err = tc.SelectFromQueueV2MetadataForUpdate(ctx, filter)
	default:
		metadata, err = tc.SelectFromQueueV2Metadata(ctx, filter)
	}
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, persistence.NewQueueNotFoundError(queueType, queueName)
		}
		return nil, serviceerror.NewUnavailablef(
			"failed to get metadata for queue with type: %v and name: %v. Error: %v", queueType, queueName, err,
		)
	}
	return q.extractQueueMetadata(metadata)
}

func (q queueV2) extractQueueMetadata(metadataRow *sqlplugin.QueueV2MetadataRow) (*persistencespb.Queue, error) {
	if metadataRow.MetadataEncoding != enumspb.ENCODING_TYPE_PROTO3.String() {
		return nil, fmt.Errorf(
			"queue with type %v and name %v has invalid encoding: %w",
			metadataRow.QueueType,
			metadataRow.QueueName,
			serialization.NewUnknownEncodingTypeError(metadataRow.MetadataEncoding, enumspb.ENCODING_TYPE_PROTO3),
		)
	}
	qm := &persistencespb.Queue{}
	err := qm.Unmarshal(metadataRow.MetadataPayload)
	if err != nil {
		return nil, serialization.NewDeserializationError(
			enumspb.ENCODING_TYPE_PROTO3,
			fmt.Errorf("unmarshal payload for queue with type %v and name %v failed: %w",
				metadataRow.QueueType,
				metadataRow.QueueName,
				err),
		)
	}
	return qm, nil
}

func (q *queueV2) getMaxMessageID(ctx context.Context, queueType persistence.QueueV2Type, queueName string, tc sqlplugin.TableCRUD) (int64, bool, error) {
	lastMessageID, err := tc.GetLastEnqueuedMessageIDForUpdateV2(ctx, sqlplugin.QueueV2Filter{
		QueueType: queueType,
		QueueName: queueName,
		Partition: defaultPartition,
	})
	switch {
	case err == nil:
		return lastMessageID, true, nil
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	default:
		return 0, false, err
	}
}

func (q *queueV2) ListQueues(
	ctx context.Context,
	request *persistence.InternalListQueuesRequest,
) (*persistence.InternalListQueuesResponse, error) {
	if request.PageSize <= 0 {
		return nil, persistence.ErrNonPositiveListQueuesPageSize
	}
	offset, err := persistence.GetOffsetForListQueues(request.NextPageToken)
	if err != nil {
		return nil, err
	}
	if offset < 0 {
		return nil, persistence.ErrNegativeListQueuesOffset
	}
	rows, err := q.DB.SelectNameFromQueueV2Metadata(ctx, sqlplugin.QueueV2MetadataTypeFilter{
		QueueType:  request.QueueType,
		PageSize:   request.PageSize,
		PageOffset: offset,
	})
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, serviceerror.NewUnavailablef(
			"ListQueues failed for type: %v. SelectNameFromQueueV2Metadata operation failed. Error: %v",
			request.QueueType,
			err,
		)
	}
	var queues []persistence.QueueInfo
	for _, row := range rows {
		messageCount, lastMessageID, err := q.getMessageCountAndLastID(ctx, &row)
		if err != nil {
			return nil, err
		}
		queues = append(queues, persistence.QueueInfo{
			QueueName:     row.QueueName,
			MessageCount:  messageCount,
			LastMessageID: lastMessageID,
		})
	}
	lastReadQueueNumber := offset + int64(len(queues))
	var nextPageToken []byte
	if len(queues) > 0 {
		nextPageToken = persistence.GetNextPageTokenForListQueues(lastReadQueueNumber)
	}
	response := &persistence.InternalListQueuesResponse{
		Queues:        queues,
		NextPageToken: nextPageToken,
	}
	return response, nil
}

func (q *queueV2) getMessageCountAndLastID(
	ctx context.Context,
	row *sqlplugin.QueueV2MetadataRow,
) (messageCount int64, lastMessageID int64, err error) {
	var ok bool
	lastMessageID, ok, err = q.getMaxMessageID(ctx, row.QueueType, row.QueueName, q.DB)
	if err != nil {
		return 0, 0, serviceerror.NewUnavailablef(
			"getLastMessageID operation failed for queue with type %v and name %v. Error: %v",
			row.QueueType,
			row.QueueName,
			err,
		)
	}
	if !ok {
		return 0, -1, nil // Empty queue
	}
	qm, err := q.extractQueueMetadata(row)
	if err != nil {
		return 0, 0, err
	}
	partition, err := persistence.GetPartitionForQueueV2(row.QueueType, row.QueueName, qm)
	if err != nil {
		return 0, 0, err
	}
	messageCount = lastMessageID - partition.MinMessageId + 1
	return messageCount, lastMessageID, nil
}
