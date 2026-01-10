package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mongodb/client"
)

const (
	collectionShards = "shards"
)

type (
	shardStore struct {
		db                  client.Database
		cfg                 config.MongoDB
		logger              log.Logger
		clusterName         string
		transactionsEnabled bool
	}

	shardDocument struct {
		ShardID      int32  `bson:"_id"`
		RangeID      int64  `bson:"range_id"`
		Data         []byte `bson:"data"`
		DataEncoding string `bson:"data_encoding"`
	}
)

// NewShardStore creates a new instance of MongoDB-based ShardStore
func NewShardStore(
	db client.Database,
	cfg config.MongoDB,
	logger log.Logger,
	clusterName string,
	transactionsEnabled bool,
) (persistence.ShardStore, error) {
	return &shardStore{
		db:                  db,
		cfg:                 cfg,
		logger:              logger,
		clusterName:         clusterName,
		transactionsEnabled: transactionsEnabled,
	}, nil
}

func (s *shardStore) GetName() string {
	return "mongodb"
}

func (s *shardStore) GetClusterName() string {
	return s.clusterName
}

func (s *shardStore) Close() {
	// Database connection is managed by Factory
}

func (s *shardStore) GetOrCreateShard(
	ctx context.Context,
	request *persistence.InternalGetOrCreateShardRequest,
) (*persistence.InternalGetOrCreateShardResponse, error) {
	collection := s.db.Collection(collectionShards)

	filter := bson.M{"_id": request.ShardID}

	var doc shardDocument
	err := collection.FindOne(ctx, filter).Decode(&doc)

	if err == nil {
		// Shard exists, return it
		return &persistence.InternalGetOrCreateShardResponse{
			ShardInfo: persistence.NewDataBlob(doc.Data, doc.DataEncoding),
		}, nil
	}

	if err != mongo.ErrNoDocuments {
		// Unexpected error
		return nil, serviceerror.NewUnavailablef(
			"GetOrCreateShard: failed to get ShardID %v. Error: %v",
			request.ShardID,
			err,
		)
	}

	// Shard doesn't exist
	if request.CreateShardInfo == nil {
		return nil, serviceerror.NewNotFoundf(
			"GetOrCreateShard: ShardID %v not found",
			request.ShardID,
		)
	}

	// Create new shard
	rangeID, shardInfo, err := request.CreateShardInfo()
	if err != nil {
		return nil, serviceerror.NewUnavailablef(
			"GetOrCreateShard: failed to encode shard info for ShardID %v. Error: %v",
			request.ShardID,
			err,
		)
	}

	newDoc := shardDocument{
		ShardID:      request.ShardID,
		RangeID:      rangeID,
		Data:         shardInfo.Data,
		DataEncoding: shardInfo.EncodingType.String(),
	}

	_, err = collection.InsertOne(ctx, newDoc)
	if err == nil {
		s.logger.Info("Created new shard",
			tag.ShardID(request.ShardID),
			tag.NewInt64("range-id", rangeID),
		)
		return &persistence.InternalGetOrCreateShardResponse{
			ShardInfo: shardInfo,
		}, nil
	}

	// Check for duplicate key error
	if mongo.IsDuplicateKeyError(err) {
		// Race condition: another process created the shard
		// Retry without creating
		request.CreateShardInfo = nil
		return s.GetOrCreateShard(ctx, request)
	}

	return nil, serviceerror.NewUnavailablef(
		"GetOrCreateShard: failed to insert shard %v. Error: %v",
		request.ShardID,
		err,
	)
}

func (s *shardStore) UpdateShard(
	ctx context.Context,
	request *persistence.InternalUpdateShardRequest,
) error {
	collection := s.db.Collection(collectionShards)

	// Optimistic concurrency control: update only if PreviousRangeID matches
	filter := bson.M{
		"_id":      request.ShardID,
		"range_id": request.PreviousRangeID,
	}

	update := bson.M{
		"$set": bson.M{
			"range_id":      request.RangeID,
			"data":          request.ShardInfo.Data,
			"data_encoding": request.ShardInfo.EncodingType.String(),
		},
	}

	result, err := collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return serviceerror.NewUnavailablef(
			"UpdateShard: failed to update ShardID %v. Error: %v",
			request.ShardID,
			err,
		)
	}

	if result.MatchedCount == 0 {
		// No document matched, meaning PreviousRangeID didn't match
		return &persistence.ShardOwnershipLostError{
			ShardID: request.ShardID,
			Msg: fmt.Sprintf(
				"UpdateShard: failed to update shard %v. Previous range ID mismatch (expected %v, current is different)",
				request.ShardID,
				request.PreviousRangeID,
			),
		}
	}

	s.logger.Debug("Updated shard",
		tag.ShardID(request.ShardID),
		tag.NewInt64("new-range-id", request.RangeID),
		tag.NewInt64("previous-range-id", request.PreviousRangeID),
	)

	return nil
}

func (s *shardStore) AssertShardOwnership(
	ctx context.Context,
	request *persistence.AssertShardOwnershipRequest,
) error {
	// AssertShardOwnership is not implemented for MongoDB shard store
	// Similar to SQL and Cassandra implementations
	return nil
}

// createIndexes creates indexes for the shards collection
func (s *shardStore) createIndexes(ctx context.Context) error {
	// The _id field (ShardID) already has an implicit unique index
	// range_id is used for optimistic locking but doesn't need a separate index
	// since we always query by _id first

	s.logger.Info("Shard store indexes ready",
		tag.NewStringTag("collection", collectionShards),
	)

	return nil
}
