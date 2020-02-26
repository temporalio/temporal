// Copyright (c) 2018 Uber Technologies, Inc.
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
	"database/sql"
	"fmt"

	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/persistence/serialization"
	"github.com/temporalio/temporal/common/persistence/sql/sqlplugin"
)

type sqlShardManager struct {
	sqlStore
	currentClusterName string
}

// newShardPersistence creates an instance of ShardManager
func newShardPersistence(db sqlplugin.DB, currentClusterName string, log log.Logger) (persistence.ShardManager, error) {
	return &sqlShardManager{
		sqlStore: sqlStore{
			db:     db,
			logger: log,
		},
		currentClusterName: currentClusterName,
	}, nil
}

func (m *sqlShardManager) CreateShard(request *persistence.CreateShardRequest) error {
	if _, err := m.GetShard(&persistence.GetShardRequest{
		ShardID: request.ShardInfo.ShardID,
	}); err == nil {
		return &persistence.ShardAlreadyExistError{
			Msg: fmt.Sprintf("CreateShard operaiton failed. Shard with ID %v already exists.", request.ShardInfo.ShardID),
		}
	}

	row, err := shardInfoToShardsRow(*request.ShardInfo)
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("CreateShard operation failed. Error: %v", err))
	}

	if _, err := m.db.InsertIntoShards(row); err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("CreateShard operation failed. Failed to insert into shards table. Error: %v", err))
	}

	return nil
}

func (m *sqlShardManager) GetShard(request *persistence.GetShardRequest) (*persistence.GetShardResponse, error) {
	row, err := m.db.SelectFromShards(&sqlplugin.ShardsFilter{ShardID: int64(request.ShardID)})
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, serviceerror.NewNotFound(fmt.Sprintf("GetShard operation failed. Shard with ID %v not found. Error: %v", request.ShardID, err))
		}
		return nil, serviceerror.NewInternal(fmt.Sprintf("GetShard operation failed. Failed to get record. ShardId: %v. Error: %v", request.ShardID, err))
	}

	shardInfo, err := serialization.ShardInfoFromBlob(row.Data, row.DataEncoding, m.currentClusterName)
	if err != nil {
		return nil, err
	}

	resp := &persistence.GetShardResponse{ShardInfo: shardInfo}

	return resp, nil
}

func (m *sqlShardManager) UpdateShard(request *persistence.UpdateShardRequest) error {
	row, err := shardInfoToShardsRow(*request.ShardInfo)
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("UpdateShard operation failed. Error: %v", err))
	}
	return m.txExecute("UpdateShard", func(tx sqlplugin.Tx) error {
		if err := lockShard(tx, int(request.ShardInfo.ShardID), request.PreviousRangeID); err != nil {
			return err
		}
		result, err := tx.UpdateShards(row)
		if err != nil {
			return err
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("rowsAffected returned error for shardID %v: %v", request.ShardInfo.ShardID, err)
		}
		if rowsAffected != 1 {
			return fmt.Errorf("rowsAffected returned %v shards instead of one", rowsAffected)
		}
		return nil
	})
}

// initiated by the owning shard
func lockShard(tx sqlplugin.Tx, shardID int, oldRangeID int64) error {
	rangeID, err := tx.WriteLockShards(&sqlplugin.ShardsFilter{ShardID: int64(shardID)})
	if err != nil {
		if err == sql.ErrNoRows {
			return serviceerror.NewInternal(fmt.Sprintf("Failed to lock shard with ID %v that does not exist.", shardID))
		}
		return serviceerror.NewInternal(fmt.Sprintf("Failed to lock shard with ID: %v. Error: %v", shardID, err))
	}

	if int64(rangeID) != oldRangeID {
		return &persistence.ShardOwnershipLostError{
			ShardID: shardID,
			Msg:     fmt.Sprintf("Failed to update shard. Previous range ID: %v; new range ID: %v", oldRangeID, rangeID),
		}
	}

	return nil
}

// initiated by the owning shard
func readLockShard(tx sqlplugin.Tx, shardID int, oldRangeID int64) error {
	rangeID, err := tx.ReadLockShards(&sqlplugin.ShardsFilter{ShardID: int64(shardID)})
	if err != nil {
		if err == sql.ErrNoRows {
			return serviceerror.NewInternal(fmt.Sprintf("Failed to lock shard with ID %v that does not exist.", shardID))
		}
		return serviceerror.NewInternal(fmt.Sprintf("Failed to lock shard with ID: %v. Error: %v", shardID, err))
	}

	if int64(rangeID) != oldRangeID {
		return &persistence.ShardOwnershipLostError{
			ShardID: shardID,
			Msg:     fmt.Sprintf("Failed to lock shard. Previous range ID: %v; new range ID: %v", oldRangeID, rangeID),
		}
	}
	return nil
}

func shardInfoToShardsRow(s persistenceblobs.ShardInfo) (*sqlplugin.ShardsRow, error) {
	blob, err := serialization.ShardInfoToBlob(&s)
	if err != nil {
		return nil, err
	}
	return &sqlplugin.ShardsRow{
		ShardID:      int64(s.ShardID),
		RangeID:      s.RangeID,
		Data:         blob.Data,
		DataEncoding: string(blob.Encoding),
	}, nil
}
