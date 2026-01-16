package mongodb

import (
	"context"
	"net"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mongodb/client"
)

const (
	collectionClusterMetadata   = "cluster_metadata"
	collectionClusterMembership = "cluster_membership"
)

type (
	clusterMetadataStore struct {
		db     client.Database
		logger log.Logger
	}

	clusterMetadataDocument struct {
		ClusterName  string `bson:"_id"`
		Data         []byte `bson:"data"`
		DataEncoding string `bson:"data_encoding"`
		Version      int64  `bson:"version"`
	}

	clusterMembershipDocument struct {
		HostID        []byte    `bson:"_id"`
		RPCAddress    string    `bson:"rpc_address"`
		RPCPort       uint16    `bson:"rpc_port"`
		Role          int32     `bson:"role"`
		SessionStart  time.Time `bson:"session_start"`
		LastHeartbeat time.Time `bson:"last_heartbeat"`
		RecordExpiry  time.Time `bson:"record_expiry"`
	}
)

// NewClusterMetadataStore creates a new instance of ClusterMetadataStore
func NewClusterMetadataStore(
	db client.Database,
	logger log.Logger,
) (persistence.ClusterMetadataStore, error) {
	return &clusterMetadataStore{
		db:     db,
		logger: logger,
	}, nil
}

func (s *clusterMetadataStore) GetName() string {
	return "mongodb"
}

func (s *clusterMetadataStore) Close() {
	// Database connection is managed by the factory
}

func (s *clusterMetadataStore) ListClusterMetadata(
	ctx context.Context,
	request *persistence.InternalListClusterMetadataRequest,
) (*persistence.InternalListClusterMetadataResponse, error) {
	collection := s.db.Collection(collectionClusterMetadata)

	filter := bson.M{}
	if len(request.NextPageToken) > 0 {
		filter["_id"] = bson.M{"$gt": string(request.NextPageToken)}
	}

	opts := options.Find().
		SetSort(bson.D{{Key: "_id", Value: 1}}).
		SetLimit(int64(request.PageSize))

	cursor, err := collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("ListClusterMetadata failed: %v", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var response persistence.InternalListClusterMetadataResponse
	var lastClusterName string

	for cursor.Next(ctx) {
		var doc clusterMetadataDocument
		if err := cursor.Decode(&doc); err != nil {
			return nil, serviceerror.NewUnavailablef("ListClusterMetadata decode failed: %v", err)
		}

		response.ClusterMetadata = append(response.ClusterMetadata, &persistence.InternalGetClusterMetadataResponse{
			ClusterMetadata: persistence.NewDataBlob(doc.Data, doc.DataEncoding),
			Version:         doc.Version,
		})
		lastClusterName = doc.ClusterName
	}

	if err := cursor.Err(); err != nil {
		return nil, serviceerror.NewUnavailablef("ListClusterMetadata cursor error: %v", err)
	}

	if len(response.ClusterMetadata) == request.PageSize {
		response.NextPageToken = []byte(lastClusterName)
	}

	return &response, nil
}

func (s *clusterMetadataStore) GetClusterMetadata(
	ctx context.Context,
	request *persistence.InternalGetClusterMetadataRequest,
) (*persistence.InternalGetClusterMetadataResponse, error) {
	collection := s.db.Collection(collectionClusterMetadata)

	var doc clusterMetadataDocument
	err := collection.FindOne(ctx, bson.M{"_id": request.ClusterName}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, serviceerror.NewNotFound("cluster metadata not found")
		}
		return nil, serviceerror.NewUnavailablef("GetClusterMetadata failed: %v", err)
	}

	return &persistence.InternalGetClusterMetadataResponse{
		ClusterMetadata: persistence.NewDataBlob(doc.Data, doc.DataEncoding),
		Version:         doc.Version,
	}, nil
}

func (s *clusterMetadataStore) SaveClusterMetadata(
	ctx context.Context,
	request *persistence.InternalSaveClusterMetadataRequest,
) (bool, error) {
	collection := s.db.Collection(collectionClusterMetadata)

	if request.Version == 0 {
		doc := clusterMetadataDocument{
			ClusterName:  request.ClusterName,
			Data:         request.ClusterMetadata.Data,
			DataEncoding: request.ClusterMetadata.EncodingType.String(),
			Version:      1,
		}
		_, err := collection.InsertOne(ctx, doc)
		if err != nil {
			if mongo.IsDuplicateKeyError(err) {
				return false, serviceerror.NewUnavailable("SaveClusterMetadata: concurrent write detected")
			}
			return false, serviceerror.NewUnavailablef("SaveClusterMetadata insert failed: %v", err)
		}
		s.logger.Info("Created cluster metadata", tag.NewStringTag("cluster-name", request.ClusterName))
		return true, nil
	}

	filter := bson.M{
		"_id":     request.ClusterName,
		"version": request.Version,
	}
	update := bson.M{
		"$set": bson.M{
			"data":          request.ClusterMetadata.Data,
			"data_encoding": request.ClusterMetadata.EncodingType.String(),
			"version":       request.Version + 1,
		},
	}

	result, err := collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return false, serviceerror.NewUnavailablef("SaveClusterMetadata update failed: %v", err)
	}

	if result.MatchedCount == 0 {
		return false, serviceerror.NewUnavailable("SaveClusterMetadata: version mismatch")
	}

	s.logger.Debug("Updated cluster metadata",
		tag.NewStringTag("cluster-name", request.ClusterName),
		tag.NewInt64("version", request.Version+1))
	return true, nil
}

func (s *clusterMetadataStore) DeleteClusterMetadata(
	ctx context.Context,
	request *persistence.InternalDeleteClusterMetadataRequest,
) error {
	collection := s.db.Collection(collectionClusterMetadata)

	_, err := collection.DeleteOne(ctx, bson.M{"_id": request.ClusterName})
	if err != nil {
		return serviceerror.NewUnavailablef("DeleteClusterMetadata failed: %v", err)
	}

	s.logger.Info("Deleted cluster metadata", tag.NewStringTag("cluster-name", request.ClusterName))
	return nil
}

func (s *clusterMetadataStore) GetClusterMembers(
	ctx context.Context,
	request *persistence.GetClusterMembersRequest,
) (*persistence.GetClusterMembersResponse, error) {
	collection := s.db.Collection(collectionClusterMembership)

	filter := bson.M{
		"record_expiry": bson.M{"$gt": time.Now().UTC()},
	}

	if len(request.HostIDEquals) > 0 {
		filter["_id"] = request.HostIDEquals
	}

	if request.RPCAddressEquals != nil {
		filter["rpc_address"] = request.RPCAddressEquals.String()
	}

	if request.RoleEquals != persistence.All {
		filter["role"] = request.RoleEquals
	}

	if !request.SessionStartedAfter.IsZero() {
		filter["session_start"] = bson.M{"$gt": request.SessionStartedAfter}
	}

	if request.LastHeartbeatWithin > 0 {
		filter["last_heartbeat"] = bson.M{"$gt": time.Now().UTC().Add(-request.LastHeartbeatWithin)}
	}

	if len(request.NextPageToken) > 0 {
		filter["_id"] = bson.M{"$gt": request.NextPageToken}
	}

	opts := options.Find().
		SetSort(bson.D{{Key: "_id", Value: 1}}).
		SetLimit(int64(request.PageSize))

	cursor, err := collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("GetClusterMembers failed: %v", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var members []*persistence.ClusterMember
	var lastHostID []byte

	for cursor.Next(ctx) {
		var doc clusterMembershipDocument
		if err := cursor.Decode(&doc); err != nil {
			return nil, serviceerror.NewUnavailablef("GetClusterMembers decode failed: %v", err)
		}

		members = append(members, &persistence.ClusterMember{
			HostID:        doc.HostID,
			RPCAddress:    net.ParseIP(doc.RPCAddress),
			RPCPort:       doc.RPCPort,
			Role:          persistence.ServiceType(doc.Role),
			SessionStart:  doc.SessionStart,
			LastHeartbeat: doc.LastHeartbeat,
			RecordExpiry:  doc.RecordExpiry,
		})
		lastHostID = doc.HostID
	}

	if err := cursor.Err(); err != nil {
		return nil, serviceerror.NewUnavailablef("GetClusterMembers cursor error: %v", err)
	}

	var nextPageToken []byte
	if len(members) == request.PageSize {
		nextPageToken = lastHostID
	}

	return &persistence.GetClusterMembersResponse{
		ActiveMembers: members,
		NextPageToken: nextPageToken,
	}, nil
}

func (s *clusterMetadataStore) UpsertClusterMembership(
	ctx context.Context,
	request *persistence.UpsertClusterMembershipRequest,
) error {
	collection := s.db.Collection(collectionClusterMembership)

	doc := clusterMembershipDocument{
		HostID:        request.HostID,
		RPCAddress:    request.RPCAddress.String(),
		RPCPort:       request.RPCPort,
		Role:          int32(request.Role),
		SessionStart:  request.SessionStart,
		LastHeartbeat: time.Now().UTC(),
		RecordExpiry:  time.Now().UTC().Add(request.RecordExpiry),
	}

	opts := options.Update().SetUpsert(true)
	_, err := collection.UpdateOne(
		ctx,
		bson.M{"_id": request.HostID},
		bson.M{"$set": doc},
		opts,
	)
	if err != nil {
		return serviceerror.NewUnavailablef("UpsertClusterMembership failed: %v", err)
	}

	return nil
}

func (s *clusterMetadataStore) PruneClusterMembership(
	ctx context.Context,
	request *persistence.PruneClusterMembershipRequest,
) error {
	collection := s.db.Collection(collectionClusterMembership)

	filter := bson.M{
		"record_expiry": bson.M{"$lt": time.Now().UTC()},
	}

	result, err := collection.DeleteMany(ctx, filter)
	if err != nil {
		return serviceerror.NewUnavailablef("PruneClusterMembership failed: %v", err)
	}

	if result.DeletedCount > 0 {
		s.logger.Debug("Pruned cluster membership records", tag.NewInt64("count", result.DeletedCount))
	}

	return nil
}
