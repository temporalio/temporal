package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

type sqlShardStore struct {
	SqlStore
	currentClusterName string
}

// newShardPersistence creates an instance of ShardManager
func newShardPersistence(
	db sqlplugin.DB,
	currentClusterName string,
	logger log.Logger,
	serializer serialization.Serializer,
) (persistence.ShardStore, error) {
	return &sqlShardStore{
		SqlStore:           NewSQLStore(db, logger, serializer),
		currentClusterName: currentClusterName,
	}, nil
}

func (m *sqlShardStore) GetClusterName() string {
	return m.currentClusterName
}

func (m *sqlShardStore) GetOrCreateShard(
	ctx context.Context,
	request *persistence.InternalGetOrCreateShardRequest,
) (*persistence.InternalGetOrCreateShardResponse, error) {
	resp, err := m.GetShard(ctx, &persistence.GetShardRequest{ShardID: request.ShardID})
	if err == nil {
		return &persistence.InternalGetOrCreateShardResponse{
			ShardInfo: resp.ShardInfo,
		}, nil
	} else if _, isNotFound := errors.AsType[*serviceerror.NotFound](err); !isNotFound {
		return nil, err
	}

	rangeID, shardInfo, err := request.CreateShardInfo()
	if err != nil {
		return nil, serviceerror.NewUnavailablef("GetOrCreateShard: failed to encode shard info for ShardID %v. Error: %v", request.ShardID, err)
	}
	row := &sqlplugin.ShardsRow{
		ShardID:      request.ShardID,
		RangeID:      rangeID,
		Data:         shardInfo.Data,
		DataEncoding: shardInfo.EncodingType.String(),
	}
	_, err = m.DB.InsertIntoShards(ctx, row)
	if err == nil {
		return &persistence.InternalGetOrCreateShardResponse{
			ShardInfo: shardInfo,
		}, nil
	} else if m.DB.IsDupEntryError(err) {
		// conflict, return the shard created concurrently
		resp, err := m.GetShard(ctx, &persistence.GetShardRequest{ShardID: request.ShardID})
		if err != nil {
			return nil, err
		}
		return &persistence.InternalGetOrCreateShardResponse{
			ShardInfo: resp.ShardInfo,
		}, nil
	} else {
		return nil, serviceerror.NewUnavailablef("GetOrCreateShard: failed to insert into shards table. Error: %v", err)
	}
}

func (m *sqlShardStore) GetShard(
	ctx context.Context,
	request *persistence.GetShardRequest,
) (*persistence.InternalGetShardResponse, error) {
	row, err := m.DB.SelectFromShards(ctx, sqlplugin.ShardsFilter{
		ShardID: request.ShardID,
	})
	switch err {
	case nil:
		return &persistence.InternalGetShardResponse{
			ShardInfo: persistence.NewDataBlob(row.Data, row.DataEncoding),
		}, nil
	case sql.ErrNoRows:
		return nil, serviceerror.NewNotFoundf("GetShard: ShardID %v not found", request.ShardID)
	default:
		return nil, serviceerror.NewUnavailablef("GetShard: failed to get ShardID %v. Error: %v", request.ShardID, err)
	}
}

func (m *sqlShardStore) UpdateShard(
	ctx context.Context,
	request *persistence.InternalUpdateShardRequest,
) error {
	return m.txExecute(ctx, "UpdateShard", func(tx sqlplugin.Tx) error {
		if err := lockShard(ctx,
			tx,
			request.ShardID,
			request.PreviousRangeID,
			m.logger,
		); err != nil {
			return err
		}
		result, err := tx.UpdateShards(ctx, &sqlplugin.ShardsRow{
			ShardID:      request.ShardID,
			RangeID:      request.RangeID,
			Data:         request.ShardInfo.Data,
			DataEncoding: request.ShardInfo.EncodingType.String(),
		})
		if err != nil {
			return err
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("rowsAffected returned error for shardID %v: %v", request.ShardID, err)
		}
		if rowsAffected != 1 {
			return fmt.Errorf("rowsAffected returned %v shards instead of one", rowsAffected)
		}
		return nil
	})
}

func (m *sqlShardStore) AssertShardOwnership(
	ctx context.Context,
	request *persistence.AssertShardOwnershipRequest,
) error {
	// AssertShardOwnership is not implemented for sql shard store
	return nil
}

// initiated by the owning shard
func lockShard(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	oldRangeID int64,
	logger log.Logger,
) error {

	rangeID, err := tx.WriteLockShards(ctx, sqlplugin.ShardsFilter{
		ShardID: shardID,
	})
	switch err {
	case nil:
		if rangeID != oldRangeID {
			return &persistence.ShardOwnershipLostError{
				ShardID: shardID,
				Msg:     fmt.Sprintf("Failed to update shard. Previous range ID: %v; new range ID: %v", oldRangeID, rangeID),
			}
		}
		return nil
	case sql.ErrNoRows:
		return serviceerror.NewUnavailablef("Failed to lock shard with ID %v that does not exist.", shardID)
	default:
		return serviceerror.NewUnavailablef("Failed to lock shard with ID: %v. Error: %v", shardID, err)
	}
}

// initiated by the owning shard
func readLockShard(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	oldRangeID int64,
) error {
	rangeID, err := tx.ReadLockShards(ctx, sqlplugin.ShardsFilter{
		ShardID: shardID,
	})
	switch err {
	case nil:
		if rangeID != oldRangeID {
			return &persistence.ShardOwnershipLostError{
				ShardID: shardID,
				Msg:     fmt.Sprintf("Failed to lock shard. Previous range ID: %v; new range ID: %v", oldRangeID, rangeID),
			}
		}
		return nil
	case sql.ErrNoRows:
		return serviceerror.NewUnavailablef("Failed to lock shard with ID %v that does not exist.", shardID)
	default:
		return serviceerror.NewUnavailablef("Failed to lock shard with ID: %v. Error: %v", shardID, err)
	}
}
