package mongodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mongodb/client"
	"go.temporal.io/server/common/primitives"
)

const (
	collectionNamespaces        = "namespaces"
	collectionNamespaceMetadata = "namespace_metadata"

	namespaceMetadataDocumentID = "temporal-namespace-metadata"
)

type (
	metadataStore struct {
		db                  client.Database
		cfg                 config.MongoDB
		logger              log.Logger
		transactionsEnabled bool
	}

	namespaceDocument struct {
		ID                  string    `bson:"_id"`
		Name                string    `bson:"name"`
		Data                []byte    `bson:"data"`
		DataEncoding        string    `bson:"data_encoding"`
		IsGlobal            bool      `bson:"is_global"`
		NotificationVersion int64     `bson:"notification_version"`
		UpdatedAt           time.Time `bson:"updated_at"`
	}

	namespaceMetadataDocument struct {
		ID                  string `bson:"_id"`
		NotificationVersion int64  `bson:"notification_version"`
	}
)

// NewMetadataStore returns a MongoDB-backed implementation of persistence.MetadataStore.
func NewMetadataStore(
	db client.Database,
	cfg config.MongoDB,
	logger log.Logger,
	transactionsEnabled bool,
) (persistence.MetadataStore, error) {
	store := &metadataStore{
		db:                  db,
		cfg:                 cfg,
		logger:              logger,
		transactionsEnabled: transactionsEnabled,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := store.ensureIndexes(ctx); err != nil {
		return nil, err
	}

	return store, nil
}

func (m *metadataStore) GetName() string {
	return "mongodb"
}

func (m *metadataStore) Close() {
	// Factory manages client lifecycle.
}

func (m *metadataStore) ensureIndexes(ctx context.Context) error {
	collection := m.db.Collection(collectionNamespaces)
	model := mongo.IndexModel{
		Keys:    bson.D{{Key: "name", Value: 1}},
		Options: options.Index().SetUnique(true).SetName("namespaces_name_unique"),
	}

	_, err := collection.Indexes().CreateOne(ctx, model)
	if err != nil {
		if isDuplicateIndexError(err) {
			return nil
		}
		return serviceerror.NewUnavailablef("failed to ensure namespace indexes: %v", err)
	}

	return nil
}

func (m *metadataStore) CreateNamespace(
	ctx context.Context,
	request *persistence.InternalCreateNamespaceRequest,
) (*persistence.CreateNamespaceResponse, error) {
	metadata, err := m.getOrCreateMetadata(ctx)
	if err != nil {
		return nil, err
	}

	collection := m.db.Collection(collectionNamespaces)

	doc := namespaceDocument{
		ID:                  request.ID,
		Name:                request.Name,
		Data:                request.Namespace.Data,
		DataEncoding:        request.Namespace.EncodingType.String(),
		IsGlobal:            request.IsGlobal,
		NotificationVersion: metadata.NotificationVersion,
		UpdatedAt:           time.Now().UTC(),
	}

	_, err = collection.InsertOne(ctx, doc)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return nil, serviceerror.NewNamespaceAlreadyExists(fmt.Sprintf("namespace %s already exists", request.Name))
		}
		return nil, serviceerror.NewUnavailablef("CreateNamespace failed: %v", err)
	}

	if err := m.bumpMetadata(ctx, metadata.NotificationVersion); err != nil {
		// best-effort cleanup to avoid orphaned namespace
		if _, deleteErr := collection.DeleteOne(ctx, bson.M{"_id": request.ID}); deleteErr != nil {
			m.logger.Warn("CreateNamespace cleanup failed", tag.Error(deleteErr), tag.NewStringTag("namespace-id", request.ID))
		}
		return nil, err
	}

	return &persistence.CreateNamespaceResponse{ID: request.ID}, nil
}

func (m *metadataStore) GetNamespace(
	ctx context.Context,
	request *persistence.GetNamespaceRequest,
) (*persistence.InternalGetNamespaceResponse, error) {
	if request.ID == "" && request.Name == "" {
		return nil, serviceerror.NewInvalidArgument("GetNamespace requires either ID or Name")
	}
	if request.ID != "" && request.Name != "" {
		return nil, serviceerror.NewInvalidArgument("GetNamespace cannot be queried by both ID and Name")
	}

	collection := m.db.Collection(collectionNamespaces)

	filter := bson.M{}
	if request.ID != "" {
		filter["_id"] = request.ID
	} else if request.Name != "" {
		filter["name"] = request.Name
	} else {
		return nil, serviceerror.NewInvalidArgument("GetNamespace requires either ID or Name")
	}

	var doc namespaceDocument
	if err := collection.FindOne(ctx, filter).Decode(&doc); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			identity := request.Name
			if identity == "" {
				identity = request.ID
			}
			return nil, serviceerror.NewNamespaceNotFound(identity)
		}
		return nil, serviceerror.NewUnavailablef("GetNamespace failed: %v", err)
	}

	return &persistence.InternalGetNamespaceResponse{
		Namespace:           persistence.NewDataBlob(doc.Data, doc.DataEncoding),
		IsGlobal:            doc.IsGlobal,
		NotificationVersion: doc.NotificationVersion,
	}, nil
}

func (m *metadataStore) UpdateNamespace(
	ctx context.Context,
	request *persistence.InternalUpdateNamespaceRequest,
) error {
	return m.updateNamespace(ctx, request, request.Name)
}

func (m *metadataStore) RenameNamespace(
	ctx context.Context,
	request *persistence.InternalRenameNamespaceRequest,
) error {
	if request.PreviousName == request.Name {
		return serviceerror.NewUnavailable("Renaming to the same name fails in Cassandra due to IF NOT EXISTS")
	}
	return m.updateNamespace(ctx, request.InternalUpdateNamespaceRequest, request.PreviousName)
}

func (m *metadataStore) updateNamespace(
	ctx context.Context,
	request *persistence.InternalUpdateNamespaceRequest,
	currentName string,
) error {
	if request == nil {
		return serviceerror.NewInvalidArgument("updateNamespace request is nil")
	}

	metadata, err := m.getOrCreateMetadata(ctx)
	if err != nil {
		return err
	}

	if metadata.NotificationVersion != request.NotificationVersion {
		return serviceerror.NewUnavailablef("UpdateNamespace version mismatch: expected %d got %d", metadata.NotificationVersion, request.NotificationVersion)
	}

	collection := m.db.Collection(collectionNamespaces)

	update := bson.M{
		"$set": bson.M{
			"name":                 request.Name,
			"data":                 request.Namespace.Data,
			"data_encoding":        request.Namespace.EncodingType.String(),
			"is_global":            request.IsGlobal,
			"notification_version": request.NotificationVersion,
			"updated_at":           time.Now().UTC(),
		},
	}

	filter := bson.M{"_id": request.Id}
	if currentName != "" {
		filter["name"] = currentName
	}

	result, err := collection.UpdateOne(ctx, filter, update)
	if err != nil {
		if mapped := mapDuplicateNamespaceError(err, request.Name); mapped != nil {
			return mapped
		}
		if mongo.IsDuplicateKeyError(err) {
			return serviceerror.NewNamespaceAlreadyExists(fmt.Sprintf("namespace %s already exists", request.Name))
		}
		return serviceerror.NewUnavailablef("UpdateNamespace failed: %v", err)
	}

	if result.MatchedCount == 0 {
		return serviceerror.NewUnavailable("UpdateNamespace operation encountered concurrent write")
	}

	return m.bumpMetadata(ctx, metadata.NotificationVersion)
}

func (m *metadataStore) DeleteNamespace(
	ctx context.Context,
	request *persistence.DeleteNamespaceRequest,
) error {
	collection := m.db.Collection(collectionNamespaces)
	_, err := collection.DeleteOne(ctx, bson.M{"_id": request.ID})
	if err != nil {
		return serviceerror.NewUnavailablef("DeleteNamespace failed: %v", err)
	}
	return nil
}

func (m *metadataStore) DeleteNamespaceByName(
	ctx context.Context,
	request *persistence.DeleteNamespaceByNameRequest,
) error {
	collection := m.db.Collection(collectionNamespaces)
	_, err := collection.DeleteOne(ctx, bson.M{"name": request.Name})
	if err != nil {
		return serviceerror.NewUnavailablef("DeleteNamespaceByName failed: %v", err)
	}
	return nil
}

func (m *metadataStore) ListNamespaces(
	ctx context.Context,
	request *persistence.InternalListNamespacesRequest,
) (*persistence.InternalListNamespacesResponse, error) {
	collection := m.db.Collection(collectionNamespaces)

	filter := bson.M{}
	if len(request.NextPageToken) > 0 {
		filter["_id"] = bson.M{"$gt": tokenString(request.NextPageToken)}
	}

	limit := int64(request.PageSize)
	if limit <= 0 {
		limit = 100
	}

	opts := options.Find().
		SetSort(bson.D{{Key: "_id", Value: 1}}).
		SetLimit(limit)

	cursor, err := collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("ListNamespaces failed: %v", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var (
		results []*persistence.InternalGetNamespaceResponse
		lastID  string
	)

	for cursor.Next(ctx) {
		var doc namespaceDocument
		if err := cursor.Decode(&doc); err != nil {
			return nil, serviceerror.NewUnavailablef("ListNamespaces decode failed: %v", err)
		}

		results = append(results, &persistence.InternalGetNamespaceResponse{
			Namespace:           persistence.NewDataBlob(doc.Data, doc.DataEncoding),
			IsGlobal:            doc.IsGlobal,
			NotificationVersion: doc.NotificationVersion,
		})
		lastID = doc.ID
	}

	if err := cursor.Err(); err != nil {
		return nil, serviceerror.NewUnavailablef("ListNamespaces cursor error: %v", err)
	}

	response := &persistence.InternalListNamespacesResponse{Namespaces: results}
	if int64(len(results)) == limit && lastID != "" {
		response.NextPageToken = bestEffortUUIDBytes(lastID)
	}

	return response, nil
}

func (m *metadataStore) GetMetadata(
	ctx context.Context,
) (*persistence.GetMetadataResponse, error) {
	metadata, err := m.getOrCreateMetadata(ctx)
	if err != nil {
		return nil, err
	}
	return &persistence.GetMetadataResponse{NotificationVersion: metadata.NotificationVersion}, nil
}

func (m *metadataStore) getOrCreateMetadata(ctx context.Context) (*namespaceMetadataDocument, error) {
	collection := m.db.Collection(collectionNamespaceMetadata)

	var doc namespaceMetadataDocument
	if err := collection.FindOne(ctx, bson.M{"_id": namespaceMetadataDocumentID}).Decode(&doc); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			doc = namespaceMetadataDocument{ID: namespaceMetadataDocumentID, NotificationVersion: 0}
			if _, err := collection.InsertOne(ctx, doc); err != nil {
				if mongo.IsDuplicateKeyError(err) {
					return m.getOrCreateMetadata(ctx)
				}
				return nil, serviceerror.NewUnavailablef("failed to init namespace metadata: %v", err)
			}
			return &doc, nil
		}
		return nil, serviceerror.NewUnavailablef("GetMetadata failed: %v", err)
	}

	return &doc, nil
}

func (m *metadataStore) bumpMetadata(ctx context.Context, current int64) error {
	collection := m.db.Collection(collectionNamespaceMetadata)
	update := bson.M{"$set": bson.M{"notification_version": current + 1}}
	result, err := collection.UpdateOne(ctx, bson.M{"_id": namespaceMetadataDocumentID, "notification_version": current}, update)
	if err != nil {
		return serviceerror.NewUnavailablef("failed to update namespace metadata: %v", err)
	}
	if result.MatchedCount == 0 {
		return serviceerror.NewUnavailable("namespace metadata version conflict")
	}
	return nil
}

// bestEffortUUIDBytes converts a string UUID to []byte for pagination tokens. If parsing fails, returns raw bytes.
func bestEffortUUIDBytes(id string) []byte {
	if parsed, err := primitives.ParseUUID(id); err == nil && parsed != nil {
		return parsed
	}
	return []byte(id)
}

func tokenString(token []byte) string {
	if len(token) == 16 {
		if val := primitives.UUIDString(token); val != "" {
			return val
		}
	}
	return string(token)
}
