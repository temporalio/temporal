package mongodb

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mongodb/client"
)

const (
	collectionNexusEndpoints        = "nexus_endpoints"
	collectionNexusEndpointsVersion = "nexus_endpoints_metadata"

	nexusEndpointsMetadataID = "table_version"
)

type nexusEndpointStore struct {
	transactionalStore

	db           client.Database
	logger       log.Logger
	endpointsCol client.Collection
	metadataCol  client.Collection
}

type nexusEndpointDocument struct {
	ID           string    `bson:"_id"`
	Version      int64     `bson:"version"`
	Data         []byte    `bson:"data,omitempty"`
	DataEncoding string    `bson:"data_encoding,omitempty"`
	UpdatedAt    time.Time `bson:"updated_at"`
}

type nexusEndpointMetadataDocument struct {
	ID        string    `bson:"_id"`
	Version   int64     `bson:"version"`
	UpdatedAt time.Time `bson:"updated_at"`
}

type listEndpointsPageToken struct {
	LastID string `json:"last_id,omitempty"`
}

// NewNexusEndpointStore returns a MongoDB-backed implementation of persistence.NexusEndpointStore.
func NewNexusEndpointStore(
	db client.Database,
	mongoClient client.Client,
	metricsHandler metrics.Handler,
	logger log.Logger,
	transactionsEnabled bool,
) (persistence.NexusEndpointStore, error) {
	if !transactionsEnabled {
		return nil, errors.New("mongodb nexus endpoint store requires transactions-enabled topology")
	}
	if mongoClient == nil {
		return nil, errors.New("mongodb nexus endpoint store requires client with session support")
	}

	store := &nexusEndpointStore{
		transactionalStore: newTransactionalStore(mongoClient, metricsHandler),
		db:                 db,
		logger:             logger,
		endpointsCol:       db.Collection(collectionNexusEndpoints),
		metadataCol:        db.Collection(collectionNexusEndpointsVersion),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := store.ensureIndexes(ctx); err != nil {
		return nil, err
	}

	return store, nil
}

func (s *nexusEndpointStore) GetName() string {
	return "mongodb"
}

func (s *nexusEndpointStore) Close() {}

func (s *nexusEndpointStore) ensureIndexes(ctx context.Context) error {
	if err := s.ensureEndpointsIndexes(ctx); err != nil {
		return err
	}
	return s.ensureMetadataIndexes(ctx)
}

func (s *nexusEndpointStore) ensureEndpointsIndexes(ctx context.Context) error {
	// MongoDB always has a unique index on _id for every collection.
	// Attempting to create another _id index with options (e.g. unique=true) can fail.
	return nil
}

func (s *nexusEndpointStore) ensureMetadataIndexes(ctx context.Context) error {
	// MongoDB always has a unique index on _id for every collection.
	return nil
}

func (s *nexusEndpointStore) CreateOrUpdateNexusEndpoint(
	ctx context.Context,
	request *persistence.InternalCreateOrUpdateNexusEndpointRequest,
) error {
	if request == nil {
		return serviceerror.NewInvalidArgument("CreateOrUpdateNexusEndpoint request is nil")
	}
	if request.Endpoint.Data == nil {
		return serviceerror.NewInvalidArgument("CreateOrUpdateNexusEndpoint data blob is nil")
	}

	_, err := s.executeTransaction(ctx, func(txCtx context.Context) (interface{}, error) {
		if err := s.ensureTableVersion(txCtx); err != nil {
			return nil, err
		}

		if err := s.compareAndSetTableVersion(txCtx, request.LastKnownTableVersion); err != nil {
			return nil, err
		}

		blobCopy := append([]byte(nil), request.Endpoint.Data.Data...)
		doc := nexusEndpointDocument{
			ID:           request.Endpoint.ID,
			Version:      request.Endpoint.Version + 1,
			Data:         blobCopy,
			DataEncoding: request.Endpoint.Data.EncodingType.String(),
			UpdatedAt:    time.Now().UTC(),
		}

		if request.Endpoint.Version == 0 {
			insertDoc := doc
			insertDoc.Version = 1
			_, err := s.endpointsCol.InsertOne(txCtx, insertDoc)
			if err != nil {
				if mongo.IsDuplicateKeyError(err) {
					return nil, persistence.ErrNexusEndpointVersionConflict
				}
				s.logger.Error("failed to insert Nexus endpoint", tag.Error(err))
				return nil, serviceerror.NewUnavailable(err.Error())
			}
		} else {
			filter := bson.M{
				"_id":     request.Endpoint.ID,
				"version": request.Endpoint.Version,
			}
			update := bson.M{
				"$set": bson.M{
					"version":       doc.Version,
					"data":          doc.Data,
					"data_encoding": doc.DataEncoding,
					"updated_at":    doc.UpdatedAt,
				},
			}
			result, err := s.endpointsCol.UpdateOne(txCtx, filter, update)
			if err != nil {
				s.logger.Error("failed to update Nexus endpoint", tag.Error(err))
				return nil, serviceerror.NewUnavailable(err.Error())
			}
			if result.MatchedCount != 1 {
				return nil, persistence.ErrNexusEndpointVersionConflict
			}
		}

		return nil, nil
	})

	return err
}

func (s *nexusEndpointStore) DeleteNexusEndpoint(
	ctx context.Context,
	request *persistence.DeleteNexusEndpointRequest,
) error {
	if request == nil {
		return serviceerror.NewInvalidArgument("DeleteNexusEndpoint request is nil")
	}

	_, err := s.executeTransaction(ctx, func(txCtx context.Context) (interface{}, error) {
		if err := s.ensureTableVersion(txCtx); err != nil {
			return nil, err
		}

		if err := s.compareAndSetTableVersion(txCtx, request.LastKnownTableVersion); err != nil {
			return nil, err
		}

		res, err := s.endpointsCol.DeleteOne(txCtx, bson.M{"_id": request.ID})
		if err != nil {
			s.logger.Error("failed to delete Nexus endpoint", tag.Error(err))
			return nil, serviceerror.NewUnavailable(err.Error())
		}
		if res.DeletedCount != 1 {
			return nil, serviceerror.NewNotFoundf("nexus endpoint not found: %v", request.ID)
		}
		return nil, nil
	})

	return err
}

func (s *nexusEndpointStore) GetNexusEndpoint(
	ctx context.Context,
	request *persistence.GetNexusEndpointRequest,
) (*persistence.InternalNexusEndpoint, error) {
	if request == nil {
		return nil, serviceerror.NewInvalidArgument("GetNexusEndpoint request is nil")
	}

	result := s.endpointsCol.FindOne(ctx, bson.M{"_id": request.ID})
	var doc nexusEndpointDocument
	if err := result.Decode(&doc); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, serviceerror.NewNotFoundf("nexus endpoint not found: %v", request.ID)
		}
		s.logger.Error("failed to read Nexus endpoint", tag.Error(err))
		return nil, serviceerror.NewUnavailable(err.Error())
	}

	return &persistence.InternalNexusEndpoint{
		ID:      doc.ID,
		Version: doc.Version,
		Data:    persistence.NewDataBlob(doc.Data, doc.DataEncoding),
	}, nil
}

func (s *nexusEndpointStore) ListNexusEndpoints(
	ctx context.Context,
	request *persistence.ListNexusEndpointsRequest,
) (*persistence.InternalListNexusEndpointsResponse, error) {
	if request == nil {
		return nil, serviceerror.NewInvalidArgument("ListNexusEndpoints request is nil")
	}
	if request.PageSize < 0 {
		return nil, serviceerror.NewInvalidArgument("ListNexusEndpoints page size must be non-negative")
	}

	response := &persistence.InternalListNexusEndpointsResponse{}

	err := s.ensureTableVersion(ctx)
	if err != nil {
		return response, err
	}

	var metaDoc nexusEndpointMetadataDocument
	metaErr := s.metadataCol.FindOne(ctx, bson.M{"_id": nexusEndpointsMetadataID}).Decode(&metaDoc)
	if metaErr != nil {
		if !errors.Is(metaErr, mongo.ErrNoDocuments) {
			s.logger.Error("failed to read Nexus endpoints metadata", tag.Error(metaErr))
			return nil, serviceerror.NewUnavailable(metaErr.Error())
		}
		response.TableVersion = 0
	} else {
		response.TableVersion = metaDoc.Version
		if request.LastKnownTableVersion != 0 && request.LastKnownTableVersion != metaDoc.Version {
			return response, persistence.ErrNexusTableVersionConflict
		}
	}

	if request.PageSize == 0 {
		return response, nil
	}

	filter := bson.M{}
	opts := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(int64(request.PageSize))
	if len(request.NextPageToken) > 0 {
		token, err := deserializeListEndpointsToken(request.NextPageToken)
		if err != nil {
			return nil, err
		}
		if token.LastID != "" {
			filter["_id"] = bson.M{"$gt": token.LastID}
		}
	}

	cursor, err := s.endpointsCol.Find(ctx, filter, opts)
	if err != nil {
		s.logger.Error("failed to list Nexus endpoints", tag.Error(err))
		return nil, serviceerror.NewUnavailable(err.Error())
	}
	defer func() {
		_ = cursor.Close(ctx)
	}()

	var docs []nexusEndpointDocument
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, serviceerror.NewUnavailable(err.Error())
	}

	response.Endpoints = make([]persistence.InternalNexusEndpoint, len(docs))
	for i, doc := range docs {
		response.Endpoints[i] = persistence.InternalNexusEndpoint{
			ID:      doc.ID,
			Version: doc.Version,
			Data:    persistence.NewDataBlob(doc.Data, doc.DataEncoding),
		}
	}

	if len(docs) == request.PageSize {
		tokenBytes, err := serializeListEndpointsToken(&listEndpointsPageToken{LastID: docs[len(docs)-1].ID})
		if err != nil {
			s.logger.Error("failed to serialize Nexus endpoints next page token", tag.Error(err))
			return nil, serviceerror.NewInternal(err.Error())
		}
		response.NextPageToken = tokenBytes
	}

	return response, nil
}

func (s *nexusEndpointStore) ensureTableVersion(ctx context.Context) error {
	update := bson.M{
		"$setOnInsert": nexusEndpointMetadataDocument{
			ID:        nexusEndpointsMetadataID,
			Version:   0,
			UpdatedAt: time.Now().UTC(),
		},
	}
	opts := options.Update().SetUpsert(true)
	_, err := s.metadataCol.UpdateOne(ctx, bson.M{"_id": nexusEndpointsMetadataID}, update, opts)
	if err != nil {
		s.logger.Error("failed to ensure Nexus endpoints metadata", tag.Error(err))
		return serviceerror.NewUnavailable(err.Error())
	}
	return nil
}

func (s *nexusEndpointStore) compareAndSetTableVersion(ctx context.Context, expected int64) error {
	filter := bson.M{"_id": nexusEndpointsMetadataID}
	update := bson.M{
		"$inc": bson.M{"version": 1},
		"$set": bson.M{"updated_at": time.Now().UTC()},
	}

	filter["version"] = expected

	result, err := s.metadataCol.UpdateOne(ctx, filter, update)
	if err != nil {
		s.logger.Error("failed to update Nexus endpoints metadata", tag.Error(err))
		return serviceerror.NewUnavailable(err.Error())
	}
	if result.ModifiedCount != 1 {
		return persistence.ErrNexusTableVersionConflict
	}
	return nil
}

func serializeListEndpointsToken(token *listEndpointsPageToken) ([]byte, error) {
	return json.Marshal(token)
}

func deserializeListEndpointsToken(payload []byte) (*listEndpointsPageToken, error) {
	var token listEndpointsPageToken
	if err := json.Unmarshal(payload, &token); err != nil {
		return nil, serviceerror.NewInvalidArgumentf("invalid Nexus endpoints page token: %v", err)
	}
	return &token, nil
}
