package mongodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mongodb/client"
	"go.temporal.io/server/common/persistence/serialization"
	"google.golang.org/protobuf/proto"
)

const (
	collectionQueueV2Metadata = "queue_v2_metadata"
	collectionQueueV2Messages = "queue_v2_messages"
	defaultQueueV2Partition   = int32(0)
)

type queueV2Store struct {
	transactionalStore

	db          client.Database
	logger      log.Logger
	metadataCol client.Collection
	messagesCol client.Collection
}

type queueV2MetadataDocument struct {
	ID            string    `bson:"_id"`
	QueueType     int32     `bson:"queue_type"`
	QueueName     string    `bson:"queue_name"`
	Payload       []byte    `bson:"payload,omitempty"`
	Encoding      string    `bson:"encoding,omitempty"`
	LastMessageID int64     `bson:"last_message_id"`
	UpdatedAt     time.Time `bson:"updated_at"`
}

type queueV2MessageDocument struct {
	ID        string    `bson:"_id"`
	QueueType int32     `bson:"queue_type"`
	QueueName string    `bson:"queue_name"`
	Partition int32     `bson:"partition"`
	MessageID int64     `bson:"message_id"`
	Payload   []byte    `bson:"payload,omitempty"`
	Encoding  string    `bson:"encoding,omitempty"`
	CreatedAt time.Time `bson:"created_at"`
}

// NewQueueV2Store creates a MongoDB-backed QueueV2 store.
func NewQueueV2Store(
	db client.Database,
	mongoClient client.Client,
	cfg config.MongoDB,
	logger log.Logger,
	metricsHandler metrics.Handler,
	transactionsEnabled bool,
) (persistence.QueueV2, error) {
	_ = cfg
	if !transactionsEnabled {
		return nil, errors.New("mongodb queue v2 store requires transactions-enabled topology")
	}
	if mongoClient == nil {
		return nil, errors.New("mongodb queue v2 store requires client with session support")
	}

	store := &queueV2Store{
		transactionalStore: newTransactionalStore(mongoClient, metricsHandler),
		db:                 db,
		logger:             logger,
		metadataCol:        db.Collection(collectionQueueV2Metadata),
		messagesCol:        db.Collection(collectionQueueV2Messages),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := store.ensureIndexes(ctx); err != nil {
		return nil, err
	}

	return store, nil
}

func (s *queueV2Store) ensureIndexes(ctx context.Context) error {
	if err := s.ensureMetadataIndexes(ctx); err != nil {
		return err
	}
	return s.ensureMessageIndexes(ctx)
}

func (s *queueV2Store) ensureMetadataIndexes(ctx context.Context) error {
	idxView := s.metadataCol.Indexes()
	model := mongo.IndexModel{
		Keys: bson.D{
			{Key: "queue_type", Value: 1},
			{Key: "queue_name", Value: 1},
		},
		Options: options.Index().SetName("queue_v2_metadata_lookup").SetUnique(true),
	}
	if _, err := idxView.CreateOne(ctx, model); err != nil && !isDuplicateIndexError(err) {
		return serviceerror.NewUnavailablef("failed to ensure queue v2 metadata index: %v", err)
	}
	return nil
}

func (s *queueV2Store) ensureMessageIndexes(ctx context.Context) error {
	idxView := s.messagesCol.Indexes()
	specs := []struct {
		name  string
		model mongo.IndexModel
	}{
		{
			name: "queue_v2_messages_lookup",
			model: mongo.IndexModel{
				Keys: bson.D{
					{Key: "queue_type", Value: 1},
					{Key: "queue_name", Value: 1},
					{Key: "partition", Value: 1},
					{Key: "message_id", Value: 1},
				},
				Options: options.Index().SetName("queue_v2_messages_lookup"),
			},
		},
		{
			name: "queue_v2_messages_lookup_desc",
			model: mongo.IndexModel{
				Keys: bson.D{
					{Key: "queue_type", Value: 1},
					{Key: "queue_name", Value: 1},
					{Key: "partition", Value: 1},
					{Key: "message_id", Value: -1},
				},
				Options: options.Index().SetName("queue_v2_messages_lookup_desc"),
			},
		},
	}

	for _, spec := range specs {
		if _, err := idxView.CreateOne(ctx, spec.model); err != nil && !isDuplicateIndexError(err) {
			return serviceerror.NewUnavailablef("failed to ensure queue v2 messages index %s: %v", spec.name, err)
		}
	}
	return nil
}

func (s *queueV2Store) EnqueueMessage(
	ctx context.Context,
	request *persistence.InternalEnqueueMessageRequest,
) (*persistence.InternalEnqueueMessageResponse, error) {
	if request == nil {
		return nil, serviceerror.NewInvalidArgument("EnqueueMessage request is invalid")
	}

	result, err := s.executeTransaction(ctx, func(txCtx context.Context) (interface{}, error) {
		_, queueProto, err := s.loadQueueMetadata(txCtx, request.QueueType, request.QueueName)
		if err != nil {
			return nil, err
		}
		if request.Blob == nil {
			return nil, serviceerror.NewInvalidArgument("EnqueueMessage request is invalid")
		}

		nextID, err := s.allocateMessageID(txCtx, request.QueueType, request.QueueName)
		if err != nil {
			return nil, err
		}

		if _, err := persistence.GetPartitionForQueueV2(request.QueueType, request.QueueName, queueProto); err != nil {
			return nil, err
		}

		doc := queueV2MessageDocument{
			ID:        queueV2MessageDocID(request.QueueType, request.QueueName, defaultQueueV2Partition, nextID),
			QueueType: int32(request.QueueType),
			QueueName: request.QueueName,
			Partition: defaultQueueV2Partition,
			MessageID: nextID,
			Payload:   append([]byte(nil), request.Blob.Data...),
			Encoding:  request.Blob.EncodingType.String(),
			CreatedAt: time.Now().UTC(),
		}

		if _, err := s.messagesCol.InsertOne(txCtx, doc); err != nil {
			if mongo.IsDuplicateKeyError(err) {
				return nil, &persistence.ConditionFailedError{Msg: fmt.Sprintf("queue message %d already exists", nextID)}
			}
			return nil, serviceerror.NewUnavailablef("failed to insert queue v2 message: %v", err)
		}

		return nextID, nil
	})
	if err != nil {
		return nil, err
	}

	return &persistence.InternalEnqueueMessageResponse{
		Metadata: persistence.MessageMetadata{ID: result.(int64)},
	}, nil
}

func (s *queueV2Store) ReadMessages(
	ctx context.Context,
	request *persistence.InternalReadMessagesRequest,
) (*persistence.InternalReadMessagesResponse, error) {
	if request == nil {
		return nil, serviceerror.NewInvalidArgument("ReadMessages request is nil")
	}
	if request.PageSize <= 0 {
		return nil, persistence.ErrNonPositiveReadQueueMessagesPageSize
	}

	_, queueProto, err := s.loadQueueMetadata(ctx, request.QueueType, request.QueueName)
	if err != nil {
		return nil, err
	}

	minID, err := persistence.GetMinMessageIDToReadForQueueV2(request.QueueType, request.QueueName, request.NextPageToken, queueProto)
	if err != nil {
		return nil, err
	}

	filter := bson.M{
		"queue_type": int32(request.QueueType),
		"queue_name": request.QueueName,
		"partition":  defaultQueueV2Partition,
		"message_id": bson.M{"$gte": minID},
	}

	findOpts := options.Find().
		SetSort(bson.D{{Key: "message_id", Value: 1}}).
		SetLimit(int64(request.PageSize))

	cursor, err := s.messagesCol.Find(ctx, filter, findOpts)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("failed to read queue v2 messages: %v", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var messages []persistence.QueueV2Message
	for cursor.Next(ctx) {
		var doc queueV2MessageDocument
		if err := cursor.Decode(&doc); err != nil {
			return nil, serviceerror.NewUnavailablef("failed to decode queue v2 message: %v", err)
		}
		encoding, err := enumspb.EncodingTypeFromString(doc.Encoding)
		if err != nil {
			return nil, serialization.NewUnknownEncodingTypeError(doc.Encoding)
		}
		encodingType := enumspb.EncodingType(encoding)
		messages = append(messages, persistence.QueueV2Message{
			MetaData: persistence.MessageMetadata{ID: doc.MessageID},
			Data: &commonpb.DataBlob{
				EncodingType: encodingType,
				Data:         doc.Payload,
			},
		})
	}
	if err := cursor.Err(); err != nil {
		return nil, serviceerror.NewUnavailablef("iteration error reading queue v2 messages: %v", err)
	}

	return &persistence.InternalReadMessagesResponse{
		Messages:      messages,
		NextPageToken: persistence.GetNextPageTokenForReadMessages(messages),
	}, nil
}

func (s *queueV2Store) CreateQueue(
	ctx context.Context,
	request *persistence.InternalCreateQueueRequest,
) (*persistence.InternalCreateQueueResponse, error) {
	if request == nil {
		return nil, serviceerror.NewInvalidArgument("CreateQueue request is nil")
	}

	queueProto := &persistencespb.Queue{
		Partitions: map[int32]*persistencespb.QueuePartition{
			defaultQueueV2Partition: {
				MinMessageId: persistence.FirstQueueMessageID,
			},
		},
	}
	payload, err := proto.Marshal(queueProto)
	if err != nil {
		return nil, serialization.NewSerializationError(enumspb.ENCODING_TYPE_PROTO3, err)
	}

	doc := queueV2MetadataDocument{
		ID:            queueV2MetadataDocID(request.QueueType, request.QueueName),
		QueueType:     int32(request.QueueType),
		QueueName:     request.QueueName,
		Payload:       payload,
		Encoding:      enumspb.ENCODING_TYPE_PROTO3.String(),
		LastMessageID: persistence.FirstQueueMessageID - 1,
		UpdatedAt:     time.Now().UTC(),
	}

	if _, err := s.metadataCol.InsertOne(ctx, doc); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return nil, fmt.Errorf("%w: queue type %v name %v", persistence.ErrQueueAlreadyExists, request.QueueType, request.QueueName)
		}
		return nil, serviceerror.NewUnavailablef("failed to insert queue metadata: %v", err)
	}

	return &persistence.InternalCreateQueueResponse{}, nil
}

func (s *queueV2Store) RangeDeleteMessages(
	ctx context.Context,
	request *persistence.InternalRangeDeleteMessagesRequest,
) (*persistence.InternalRangeDeleteMessagesResponse, error) {
	if request == nil {
		return nil, serviceerror.NewInvalidArgument("RangeDeleteMessages request is nil")
	}
	if request.InclusiveMaxMessageMetadata.ID < persistence.FirstQueueMessageID {
		return nil, fmt.Errorf(
			"%w: id is %d but must be >= %d",
			persistence.ErrInvalidQueueRangeDeleteMaxMessageID,
			request.InclusiveMaxMessageMetadata.ID,
			persistence.FirstQueueMessageID,
		)
	}

	metaDoc, queueProto, err := s.loadQueueMetadata(ctx, request.QueueType, request.QueueName)
	if err != nil {
		return nil, err
	}

	partition, err := persistence.GetPartitionForQueueV2(request.QueueType, request.QueueName, queueProto)
	if err != nil {
		return nil, err
	}

	deleteRange, ok := persistence.GetDeleteRange(persistence.DeleteRequest{
		LastIDToDeleteInclusive: request.InclusiveMaxMessageMetadata.ID,
		ExistingMessageRange: persistence.InclusiveMessageRange{
			MinMessageID: partition.MinMessageId,
			MaxMessageID: metaDoc.LastMessageID,
		},
	})
	if !ok {
		return &persistence.InternalRangeDeleteMessagesResponse{MessagesDeleted: 0}, nil
	}

	filter := bson.M{
		"queue_type": int32(request.QueueType),
		"queue_name": request.QueueName,
		"partition":  defaultQueueV2Partition,
		"message_id": bson.M{"$gte": deleteRange.MinMessageID, "$lte": deleteRange.MaxMessageID},
	}
	if _, err := s.messagesCol.DeleteMany(ctx, filter); err != nil {
		return nil, serviceerror.NewUnavailablef("failed to delete queue v2 messages: %v", err)
	}

	partition.MinMessageId = deleteRange.NewMinMessageID
	if err := s.updateQueueMetadataPayload(ctx, metaDoc.ID, queueProto); err != nil {
		return nil, err
	}

	return &persistence.InternalRangeDeleteMessagesResponse{MessagesDeleted: deleteRange.MessagesToDelete}, nil
}

func (s *queueV2Store) ListQueues(
	ctx context.Context,
	request *persistence.InternalListQueuesRequest,
) (*persistence.InternalListQueuesResponse, error) {
	if request == nil {
		return nil, serviceerror.NewInvalidArgument("ListQueues request is nil")
	}
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

	filter := bson.M{"queue_type": int32(request.QueueType)}
	findOpts := options.Find().
		SetSort(bson.D{{Key: "queue_name", Value: 1}}).
		SetSkip(offset).
		SetLimit(int64(request.PageSize))

	cursor, err := s.metadataCol.Find(ctx, filter, findOpts)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("failed to list queue metadata: %v", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var queueInfos []persistence.QueueInfo
	for cursor.Next(ctx) {
		var doc queueV2MetadataDocument
		if err := cursor.Decode(&doc); err != nil {
			return nil, serviceerror.NewUnavailablef("failed to decode queue metadata: %v", err)
		}
		queueProto, err := s.extractQueueProto(&doc)
		if err != nil {
			return nil, err
		}
		partition, err := persistence.GetPartitionForQueueV2(persistence.QueueV2Type(doc.QueueType), doc.QueueName, queueProto)
		if err != nil {
			return nil, err
		}
		messageCount := int64(0)
		if doc.LastMessageID >= partition.MinMessageId {
			messageCount = doc.LastMessageID - partition.MinMessageId + 1
		}
		queueInfos = append(queueInfos, persistence.QueueInfo{
			QueueName:     doc.QueueName,
			MessageCount:  messageCount,
			LastMessageID: doc.LastMessageID,
		})
	}
	if err := cursor.Err(); err != nil {
		return nil, serviceerror.NewUnavailablef("iteration error listing queue metadata: %v", err)
	}

	var nextPageToken []byte
	if len(queueInfos) == request.PageSize {
		nextPageToken = persistence.GetNextPageTokenForListQueues(offset + int64(len(queueInfos)))
	}

	return &persistence.InternalListQueuesResponse{
		Queues:        queueInfos,
		NextPageToken: nextPageToken,
	}, nil
}

func (s *queueV2Store) allocateMessageID(ctx context.Context, queueType persistence.QueueV2Type, queueName string) (int64, error) {
	filter := bson.M{"_id": queueV2MetadataDocID(queueType, queueName)}
	update := bson.M{
		"$inc": bson.M{"last_message_id": int64(1)},
		"$set": bson.M{"updated_at": time.Now().UTC()},
	}

	res, err := s.metadataCol.UpdateOne(ctx, filter, update)
	if err != nil {
		return 0, serviceerror.NewUnavailablef("failed to update queue metadata: %v", err)
	}
	if res.MatchedCount == 0 {
		return 0, persistence.NewQueueNotFoundError(queueType, queueName)
	}

	var doc queueV2MetadataDocument
	if err := s.metadataCol.FindOne(ctx, filter).Decode(&doc); err != nil {
		return 0, serviceerror.NewUnavailablef("failed to reload queue metadata: %v", err)
	}
	return doc.LastMessageID, nil
}

func (s *queueV2Store) loadQueueMetadata(
	ctx context.Context,
	queueType persistence.QueueV2Type,
	queueName string,
) (*queueV2MetadataDocument, *persistencespb.Queue, error) {
	filter := bson.M{"_id": queueV2MetadataDocID(queueType, queueName)}
	var doc queueV2MetadataDocument
	if err := s.metadataCol.FindOne(ctx, filter).Decode(&doc); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil, persistence.NewQueueNotFoundError(queueType, queueName)
		}
		return nil, nil, serviceerror.NewUnavailablef("failed to load queue metadata: %v", err)
	}

	queueProto, err := s.extractQueueProto(&doc)
	if err != nil {
		return nil, nil, err
	}
	return &doc, queueProto, nil
}

func (s *queueV2Store) extractQueueProto(doc *queueV2MetadataDocument) (*persistencespb.Queue, error) {
	if doc.Encoding != enumspb.ENCODING_TYPE_PROTO3.String() {
		return nil, serialization.NewUnknownEncodingTypeError(doc.Encoding)
	}

	queueProto := &persistencespb.Queue{}
	if err := proto.Unmarshal(doc.Payload, queueProto); err != nil {
		return nil, serialization.NewDeserializationError(enumspb.ENCODING_TYPE_PROTO3, err)
	}

	if queueProto.Partitions == nil {
		queueProto.Partitions = map[int32]*persistencespb.QueuePartition{}
	}
	if _, ok := queueProto.Partitions[defaultQueueV2Partition]; !ok {
		queueProto.Partitions[defaultQueueV2Partition] = &persistencespb.QueuePartition{
			MinMessageId: persistence.FirstQueueMessageID,
		}
	}
	return queueProto, nil
}

func (s *queueV2Store) updateQueueMetadataPayload(ctx context.Context, metadataID string, queueProto *persistencespb.Queue) error {
	payload, err := proto.Marshal(queueProto)
	if err != nil {
		return serialization.NewSerializationError(enumspb.ENCODING_TYPE_PROTO3, err)
	}
	update := bson.M{
		"$set": bson.M{
			"payload":    payload,
			"encoding":   enumspb.ENCODING_TYPE_PROTO3.String(),
			"updated_at": time.Now().UTC(),
		},
	}
	if _, err := s.metadataCol.UpdateOne(ctx, bson.M{"_id": metadataID}, update); err != nil {
		return serviceerror.NewUnavailablef("failed to update queue metadata payload: %v", err)
	}
	return nil
}

func queueV2MetadataDocID(queueType persistence.QueueV2Type, queueName string) string {
	return fmt.Sprintf("%d|%s", queueType, queueName)
}

func queueV2MessageDocID(queueType persistence.QueueV2Type, queueName string, partition int32, messageID int64) string {
	return fmt.Sprintf("%d|%s|%d|%d", queueType, queueName, partition, messageID)
}
