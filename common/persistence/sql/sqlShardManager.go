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
	"time"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/.gen/go/sqlblobs"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
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
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateShard operation failed. Error: %v", err),
		}
	}

	if _, err := m.db.InsertIntoShards(row); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateShard operation failed. Failed to insert into shards table. Error: %v", err),
		}
	}

	return nil
}

func (m *sqlShardManager) GetShard(request *persistence.GetShardRequest) (*persistence.GetShardResponse, error) {
	row, err := m.db.SelectFromShards(&sqlplugin.ShardsFilter{ShardID: int64(request.ShardID)})
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, &workflow.EntityNotExistsError{
				Message: fmt.Sprintf("GetShard operation failed. Shard with ID %v not found. Error: %v", request.ShardID, err),
			}
		}
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetShard operation failed. Failed to get record. ShardId: %v. Error: %v", request.ShardID, err),
		}
	}

	shardInfo, err := shardInfoFromBlob(row.Data, row.DataEncoding)
	if err != nil {
		return nil, err
	}

	if len(shardInfo.ClusterTransferAckLevel) == 0 {
		shardInfo.ClusterTransferAckLevel = map[string]int64{
			m.currentClusterName: shardInfo.GetTransferAckLevel(),
		}
	}

	timerAckLevel := make(map[string]time.Time, len(shardInfo.ClusterTimerAckLevel))
	for k, v := range shardInfo.ClusterTimerAckLevel {
		timerAckLevel[k] = time.Unix(0, v)
	}

	if len(timerAckLevel) == 0 {
		timerAckLevel = map[string]time.Time{
			m.currentClusterName: time.Unix(0, shardInfo.GetTimerAckLevelNanos()),
		}
	}

	if shardInfo.ClusterReplicationLevel == nil {
		shardInfo.ClusterReplicationLevel = make(map[string]int64)
	}

	resp := &persistence.GetShardResponse{ShardInfo: &persistence.ShardInfo{
		ShardID:                   int(row.ShardID),
		RangeID:                   row.RangeID,
		Owner:                     shardInfo.GetOwner(),
		StolenSinceRenew:          int(shardInfo.GetStolenSinceRenew()),
		UpdatedAt:                 time.Unix(0, shardInfo.GetUpdatedAtNanos()),
		ReplicationAckLevel:       shardInfo.GetReplicationAckLevel(),
		TransferAckLevel:          shardInfo.GetTransferAckLevel(),
		TimerAckLevel:             time.Unix(0, shardInfo.GetTimerAckLevelNanos()),
		ClusterTransferAckLevel:   shardInfo.ClusterTransferAckLevel,
		ClusterTimerAckLevel:      timerAckLevel,
		DomainNotificationVersion: shardInfo.GetDomainNotificationVersion(),
		ClusterReplicationLevel:   shardInfo.ClusterReplicationLevel,
	}}

	return resp, nil
}

func (m *sqlShardManager) UpdateShard(request *persistence.UpdateShardRequest) error {
	row, err := shardInfoToShardsRow(*request.ShardInfo)
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateShard operation failed. Error: %v", err),
		}
	}
	return m.txExecute("UpdateShard", func(tx sqlplugin.Tx) error {
		if err := lockShard(tx, request.ShardInfo.ShardID, request.PreviousRangeID); err != nil {
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
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to lock shard with ID %v that does not exist.", shardID),
			}
		}
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to lock shard with ID: %v. Error: %v", shardID, err),
		}
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
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to lock shard with ID %v that does not exist.", shardID),
			}
		}
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to lock shard with ID: %v. Error: %v", shardID, err),
		}
	}

	if int64(rangeID) != oldRangeID {
		return &persistence.ShardOwnershipLostError{
			ShardID: shardID,
			Msg:     fmt.Sprintf("Failed to lock shard. Previous range ID: %v; new range ID: %v", oldRangeID, rangeID),
		}
	}
	return nil
}

func shardInfoToShardsRow(s persistence.ShardInfo) (*sqlplugin.ShardsRow, error) {
	timerAckLevels := make(map[string]int64, len(s.ClusterTimerAckLevel))
	for k, v := range s.ClusterTimerAckLevel {
		timerAckLevels[k] = v.UnixNano()
	}

	shardInfo := &sqlblobs.ShardInfo{
		StolenSinceRenew:          common.Int32Ptr(int32(s.StolenSinceRenew)),
		UpdatedAtNanos:            common.Int64Ptr(s.UpdatedAt.UnixNano()),
		ReplicationAckLevel:       common.Int64Ptr(s.ReplicationAckLevel),
		TransferAckLevel:          common.Int64Ptr(s.TransferAckLevel),
		TimerAckLevelNanos:        common.Int64Ptr(s.TimerAckLevel.UnixNano()),
		ClusterTransferAckLevel:   s.ClusterTransferAckLevel,
		ClusterTimerAckLevel:      timerAckLevels,
		DomainNotificationVersion: common.Int64Ptr(s.DomainNotificationVersion),
		Owner:                     &s.Owner,
		ClusterReplicationLevel:   s.ClusterReplicationLevel,
	}

	blob, err := shardInfoToBlob(shardInfo)
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
