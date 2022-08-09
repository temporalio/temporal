// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package sql

import (
	"context"
	"database/sql"
	"fmt"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
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
) (persistence.ShardStore, error) {
	return &sqlShardStore{
		SqlStore:           NewSqlStore(db, logger),
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
	row, err := m.Db.SelectFromShards(ctx, sqlplugin.ShardsFilter{
		ShardID: request.ShardID,
	})
	switch err {
	case nil:
		return &persistence.InternalGetOrCreateShardResponse{
			ShardInfo: persistence.NewDataBlob(row.Data, row.DataEncoding),
		}, nil
	case sql.ErrNoRows:
	default:
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetOrCreateShard: failed to get ShardID %v. Error: %v", request.ShardID, err))
	}

	if request.CreateShardInfo == nil {
		return nil, serviceerror.NewNotFound(fmt.Sprintf("GetOrCreateShard: ShardID %v not found. Error: %v", request.ShardID, err))
	}

	rangeID, shardInfo, err := request.CreateShardInfo()
	if err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetOrCreateShard: failed to encode shard info for ShardID %v. Error: %v", request.ShardID, err))
	}
	row = &sqlplugin.ShardsRow{
		ShardID:      request.ShardID,
		RangeID:      rangeID,
		Data:         shardInfo.Data,
		DataEncoding: shardInfo.EncodingType.String(),
	}
	_, err = m.Db.InsertIntoShards(ctx, row)
	if err == nil {
		return &persistence.InternalGetOrCreateShardResponse{
			ShardInfo: shardInfo,
		}, nil
	} else if m.Db.IsDupEntryError(err) {
		// conflict, try again
		request.CreateShardInfo = nil // prevent loop
		return m.GetOrCreateShard(ctx, request)
	} else {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetOrCreateShard: failed to insert into shards table. Error: %v", err))
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
	return nil
}

// initiated by the owning shard
func lockShard(
	ctx context.Context,
	tx sqlplugin.Tx,
	shardID int32,
	oldRangeID int64,
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
		return serviceerror.NewUnavailable(fmt.Sprintf("Failed to lock shard with ID %v that does not exist.", shardID))
	default:
		return serviceerror.NewUnavailable(fmt.Sprintf("Failed to lock shard with ID: %v. Error: %v", shardID, err))
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
		return serviceerror.NewUnavailable(fmt.Sprintf("Failed to lock shard with ID %v that does not exist.", shardID))
	default:
		return serviceerror.NewUnavailable(fmt.Sprintf("Failed to lock shard with ID: %v. Error: %v", shardID, err))
	}
}
