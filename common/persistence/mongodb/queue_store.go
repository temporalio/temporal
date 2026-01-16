package mongodb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mongodb/client"
)

const (
	collectionQueueMessages = "queue_messages"
	collectionQueueMetadata = "queue_metadata"
)

type queueStore struct {
	transactionalStore

	queueType persistence.QueueType

	db     client.Database
	logger log.Logger

	messagesCol client.Collection
	metadataCol client.Collection
}

type queueMessageDocument struct {
	ID        string    `bson:"_id"`
	QueueType int32     `bson:"queue_type"`
	MessageID int64     `bson:"message_id"`
	Payload   []byte    `bson:"payload,omitempty"`
	Encoding  string    `bson:"encoding,omitempty"`
	CreatedAt time.Time `bson:"created_at"`
}

type queueMetadataDocument struct {
	ID            string    `bson:"_id"`
	QueueType     int32     `bson:"queue_type"`
	Blob          []byte    `bson:"blob,omitempty"`
	Encoding      string    `bson:"encoding,omitempty"`
	Version       int64     `bson:"version"`
	LastMessageID int64     `bson:"last_message_id"`
	UpdatedAt     time.Time `bson:"updated_at"`
}

// NewQueueStore creates a MongoDB-backed implementation of persistence.Queue for the provided queue type.
func NewQueueStore(
	db client.Database,
	mongoClient client.Client,
	cfg config.MongoDB,
	logger log.Logger,
	metricsHandler metrics.Handler,
	transactionsEnabled bool,
	queueType persistence.QueueType,
) (persistence.Queue, error) {
	_ = cfg
	if !transactionsEnabled {
		return nil, errors.New("mongodb queue store requires transactions-enabled topology")
	}
	if mongoClient == nil {
		return nil, errors.New("mongodb queue store requires client with session support")
	}

	store := &queueStore{
		transactionalStore: newTransactionalStore(mongoClient, metricsHandler),
		queueType:          queueType,
		db:                 db,
		logger:             logger,
		messagesCol:        db.Collection(collectionQueueMessages),
		metadataCol:        db.Collection(collectionQueueMetadata),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := store.ensureIndexes(ctx); err != nil {
		return nil, err
	}

	return store, nil
}

func (s *queueStore) GetName() string {
	return "mongodb"
}

func (s *queueStore) Close() {}

func (s *queueStore) ensureIndexes(ctx context.Context) error {
	if err := s.ensureMessageIndexes(ctx); err != nil {
		return err
	}
	return s.ensureMetadataIndexes(ctx)
}

func (s *queueStore) ensureMessageIndexes(ctx context.Context) error {
	idxView := s.messagesCol.Indexes()
	models := []struct {
		name  string
		model mongo.IndexModel
	}{
		{
			name: "queue_messages_lookup",
			model: mongo.IndexModel{
				Keys: bson.D{
					{Key: "queue_type", Value: 1},
					{Key: "message_id", Value: 1},
				},
				Options: options.Index().SetName("queue_messages_lookup"),
			},
		},
		{
			name: "queue_messages_lookup_desc",
			model: mongo.IndexModel{
				Keys: bson.D{
					{Key: "queue_type", Value: 1},
					{Key: "message_id", Value: -1},
				},
				Options: options.Index().SetName("queue_messages_lookup_desc"),
			},
		},
	}
	for _, spec := range models {
		if _, err := idxView.CreateOne(ctx, spec.model); err != nil && !isDuplicateIndexError(err) {
			return serviceerror.NewUnavailablef("failed to ensure queue messages index %s: %v", spec.name, err)
		}
	}
	return nil
}

func (s *queueStore) ensureMetadataIndexes(ctx context.Context) error {
	idxView := s.metadataCol.Indexes()
	model := mongo.IndexModel{
		Keys:    bson.D{{Key: "queue_type", Value: 1}},
		Options: options.Index().SetName("queue_metadata_lookup").SetUnique(true),
	}
	if _, err := idxView.CreateOne(ctx, model); err != nil && !isDuplicateIndexError(err) {
		return serviceerror.NewUnavailablef("failed to ensure queue metadata index: %v", err)
	}
	return nil
}

func (s *queueStore) Init(ctx context.Context, blob *commonpb.DataBlob) error {
	if blob == nil {
		return serviceerror.NewInvalidArgument("queue init requires metadata blob")
	}

	if err := s.ensureMetadataRecord(ctx, s.queueType, blob); err != nil {
		return err
	}
	return s.ensureMetadataRecord(ctx, s.dlqType(), blob)
}

func (s *queueStore) ensureMetadataRecord(ctx context.Context, queueType persistence.QueueType, blob *commonpb.DataBlob) error {
	update := bson.M{
		"$setOnInsert": bson.M{
			"_id":             queueMetadataDocID(queueType),
			"queue_type":      int32(queueType),
			"blob":            append([]byte(nil), blob.Data...),
			"encoding":        blob.EncodingType.String(),
			"version":         int64(0),
			"last_message_id": persistence.EmptyQueueMessageID,
			"updated_at":      time.Now().UTC(),
		},
	}
	opts := options.Update().SetUpsert(true)
	if _, err := s.metadataCol.UpdateOne(ctx, bson.M{"_id": queueMetadataDocID(queueType)}, update, opts); err != nil {
		return serviceerror.NewUnavailablef("failed to ensure queue metadata: %v", err)
	}
	return nil
}

func (s *queueStore) EnqueueMessage(ctx context.Context, blob *commonpb.DataBlob) error {
	if blob == nil {
		return serviceerror.NewInvalidArgument("EnqueueMessage blob is nil")
	}

	_, err := s.executeTransaction(ctx, func(txCtx context.Context) (interface{}, error) {
		nextID, err := s.allocateMessageID(txCtx, s.queueType)
		if err != nil {
			return nil, err
		}

		doc := queueMessageDocument{
			ID:        queueMessageDocID(s.queueType, nextID),
			QueueType: int32(s.queueType),
			MessageID: nextID,
			Payload:   append([]byte(nil), blob.Data...),
			Encoding:  blob.EncodingType.String(),
			CreatedAt: time.Now().UTC(),
		}
		if _, err := s.messagesCol.InsertOne(txCtx, doc); err != nil {
			if mongo.IsDuplicateKeyError(err) {
				return nil, &persistence.ConditionFailedError{Msg: fmt.Sprintf("message %d already exists", nextID)}
			}
			return nil, serviceerror.NewUnavailablef("failed to insert queue message: %v", err)
		}
		return nil, nil
	})
	return err
}

func (s *queueStore) allocateMessageID(ctx context.Context, queueType persistence.QueueType) (int64, error) {
	inc := bson.M{"$inc": bson.M{"last_message_id": int64(1)}, "$set": bson.M{"updated_at": time.Now().UTC()}}
	res, err := s.metadataCol.UpdateOne(ctx, bson.M{"_id": queueMetadataDocID(queueType)}, inc)
	if err != nil {
		return persistence.EmptyQueueMessageID, serviceerror.NewUnavailablef("failed to increment message id: %v", err)
	}
	if res.MatchedCount == 0 {
		return persistence.EmptyQueueMessageID, persistence.NewQueueNotFoundError(persistence.QueueV2Type(queueType), "")
	}

	var meta queueMetadataDocument
	if err := s.metadataCol.FindOne(ctx, bson.M{"_id": queueMetadataDocID(queueType)}).Decode(&meta); err != nil {
		return persistence.EmptyQueueMessageID, serviceerror.NewUnavailablef("failed to load queue metadata: %v", err)
	}
	return meta.LastMessageID, nil
}

func (s *queueStore) ReadMessages(ctx context.Context, lastMessageID int64, maxCount int) ([]*persistence.QueueMessage, error) {
	filter := bson.M{
		"queue_type": s.queueType,
		"message_id": bson.M{"$gt": lastMessageID},
	}
	opts := options.Find().SetSort(bson.D{{Key: "message_id", Value: 1}}).SetLimit(int64(maxCount))
	cursor, err := s.messagesCol.Find(ctx, filter, opts)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("failed to read queue messages: %v", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var messages []*persistence.QueueMessage
	for cursor.Next(ctx) {
		var doc queueMessageDocument
		if err := cursor.Decode(&doc); err != nil {
			return nil, serviceerror.NewUnavailablef("failed to decode queue message: %v", err)
		}
		messages = append(messages, &persistence.QueueMessage{
			QueueType: s.queueType,
			ID:        doc.MessageID,
			Data:      append([]byte(nil), doc.Payload...),
			Encoding:  doc.Encoding,
		})
	}
	if err := cursor.Err(); err != nil {
		return nil, serviceerror.NewUnavailablef("iteration error reading queue messages: %v", err)
	}
	return messages, nil
}

func (s *queueStore) DeleteMessagesBefore(ctx context.Context, messageID int64) error {
	filter := bson.M{
		"queue_type": s.queueType,
		"message_id": bson.M{"$lt": messageID},
	}
	if _, err := s.messagesCol.DeleteMany(ctx, filter); err != nil {
		return serviceerror.NewUnavailablef("failed to delete queue messages before %d: %v", messageID, err)
	}
	return nil
}

func (s *queueStore) UpdateAckLevel(ctx context.Context, metadata *persistence.InternalQueueMetadata) error {
	return s.updateAckMetadata(ctx, metadata, s.queueType)
}

func (s *queueStore) GetAckLevels(ctx context.Context) (*persistence.InternalQueueMetadata, error) {
	return s.getAckMetadata(ctx, s.queueType)
}

func (s *queueStore) EnqueueMessageToDLQ(ctx context.Context, blob *commonpb.DataBlob) (int64, error) {
	if blob == nil {
		return persistence.EmptyQueueMessageID, serviceerror.NewInvalidArgument("EnqueueMessageToDLQ blob is nil")
	}

	result, err := s.executeTransaction(ctx, func(txCtx context.Context) (interface{}, error) {
		nextID, err := s.allocateMessageID(txCtx, s.dlqType())
		if err != nil {
			return nil, err
		}

		doc := queueMessageDocument{
			ID:        queueMessageDocID(s.dlqType(), nextID),
			QueueType: int32(s.dlqType()),
			MessageID: nextID,
			Payload:   append([]byte(nil), blob.Data...),
			Encoding:  blob.EncodingType.String(),
			CreatedAt: time.Now().UTC(),
		}
		if _, err := s.messagesCol.InsertOne(txCtx, doc); err != nil {
			if mongo.IsDuplicateKeyError(err) {
				return nil, &persistence.ConditionFailedError{Msg: fmt.Sprintf("dlq message %d already exists", nextID)}
			}
			return nil, serviceerror.NewUnavailablef("failed to insert DLQ message: %v", err)
		}
		return nextID, nil
	})
	if err != nil {
		return persistence.EmptyQueueMessageID, err
	}
	messageID, ok := result.(int64)
	if !ok {
		return persistence.EmptyQueueMessageID, serviceerror.NewInternal("unexpected DLQ enqueue result type")
	}
	return messageID, nil
}

func (s *queueStore) ReadMessagesFromDLQ(ctx context.Context, firstMessageID, lastMessageID int64, pageSize int, pageToken []byte) ([]*persistence.QueueMessage, []byte, error) {
	if len(pageToken) != 0 {
		offset, err := deserializeQueuePageToken(pageToken)
		if err != nil {
			return nil, nil, serviceerror.NewInvalidArgument(err.Error())
		}
		firstMessageID = offset
	}

	filter := bson.M{
		"queue_type": s.dlqType(),
		"message_id": bson.M{"$gt": firstMessageID, "$lte": lastMessageID},
	}
	opts := options.Find().SetSort(bson.D{{Key: "message_id", Value: 1}}).SetLimit(int64(pageSize))
	cursor, err := s.messagesCol.Find(ctx, filter, opts)
	if err != nil {
		return nil, nil, serviceerror.NewUnavailablef("failed to read DLQ messages: %v", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var (
		messages []*persistence.QueueMessage
		lastRead int64
	)
	for cursor.Next(ctx) {
		var doc queueMessageDocument
		if err := cursor.Decode(&doc); err != nil {
			return nil, nil, serviceerror.NewUnavailablef("failed to decode DLQ message: %v", err)
		}
		messages = append(messages, &persistence.QueueMessage{
			QueueType: s.dlqType(),
			ID:        doc.MessageID,
			Data:      append([]byte(nil), doc.Payload...),
			Encoding:  doc.Encoding,
		})
		lastRead = doc.MessageID
	}
	if err := cursor.Err(); err != nil {
		return nil, nil, serviceerror.NewUnavailablef("iteration error reading DLQ: %v", err)
	}

	var nextToken []byte
	if len(messages) == pageSize && lastRead != 0 {
		nextToken = serializeQueuePageToken(lastRead)
	}
	return messages, nextToken, nil
}

func (s *queueStore) DeleteMessageFromDLQ(ctx context.Context, messageID int64) error {
	filter := bson.M{
		"queue_type": s.dlqType(),
		"message_id": messageID,
	}
	if _, err := s.messagesCol.DeleteOne(ctx, filter); err != nil {
		return serviceerror.NewUnavailablef("failed to delete DLQ message %d: %v", messageID, err)
	}
	return nil
}

func (s *queueStore) RangeDeleteMessagesFromDLQ(ctx context.Context, firstMessageID, lastMessageID int64) error {
	filter := bson.M{
		"queue_type": s.dlqType(),
		"message_id": bson.M{"$gt": firstMessageID, "$lte": lastMessageID},
	}
	if _, err := s.messagesCol.DeleteMany(ctx, filter); err != nil {
		return serviceerror.NewUnavailablef("failed to range delete DLQ messages: %v", err)
	}
	return nil
}

func (s *queueStore) UpdateDLQAckLevel(ctx context.Context, metadata *persistence.InternalQueueMetadata) error {
	return s.updateAckMetadata(ctx, metadata, s.dlqType())
}

func (s *queueStore) GetDLQAckLevels(ctx context.Context) (*persistence.InternalQueueMetadata, error) {
	return s.getAckMetadata(ctx, s.dlqType())
}

func (s *queueStore) updateAckMetadata(ctx context.Context, metadata *persistence.InternalQueueMetadata, queueType persistence.QueueType) error {
	if metadata == nil || metadata.Blob == nil {
		return serviceerror.NewInvalidArgument("ack metadata blob is nil")
	}

	filter := bson.M{
		"_id":     queueMetadataDocID(queueType),
		"version": metadata.Version,
	}
	update := bson.M{
		"$set": bson.M{
			"blob":       append([]byte(nil), metadata.Blob.Data...),
			"encoding":   metadata.Blob.EncodingType.String(),
			"version":    metadata.Version + 1,
			"updated_at": time.Now().UTC(),
		},
	}

	res, err := s.metadataCol.UpdateOne(ctx, filter, update)
	if err != nil {
		return serviceerror.NewUnavailablef("failed to update ack metadata: %v", err)
	}
	if res.MatchedCount == 0 {
		return &persistence.ConditionFailedError{Msg: "UpdateAckLevel operation encountered concurrent write."}
	}
	return nil
}

func (s *queueStore) getAckMetadata(ctx context.Context, queueType persistence.QueueType) (*persistence.InternalQueueMetadata, error) {
	var doc queueMetadataDocument
	if err := s.metadataCol.FindOne(ctx, bson.M{"_id": queueMetadataDocID(queueType)}).Decode(&doc); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, serviceerror.NewNotFound("queue metadata not found")
		}
		return nil, serviceerror.NewUnavailablef("failed to load queue metadata: %v", err)
	}
	return &persistence.InternalQueueMetadata{
		Blob:    persistence.NewDataBlob(doc.Blob, doc.Encoding),
		Version: doc.Version,
	}, nil
}

func (s *queueStore) dlqType() persistence.QueueType {
	return -s.queueType
}

func queueMessageDocID(queueType persistence.QueueType, messageID int64) string {
	return fmt.Sprintf("%d|%d", queueType, messageID)
}

func queueMetadataDocID(queueType persistence.QueueType) string {
	return fmt.Sprintf("%d", queueType)
}

func serializeQueuePageToken(value int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(value))
	return b
}

func deserializeQueuePageToken(data []byte) (int64, error) {
	if len(data) != 8 {
		return 0, fmt.Errorf("invalid token of %d length", len(data))
	}
	return int64(binary.LittleEndian.Uint64(data)), nil
}
