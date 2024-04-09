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

package shard

import (
	"context"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func convertPersistenceAckLevelToTaskKey(
	categoryType tasks.CategoryType,
	ackLevel int64,
) tasks.Key {
	if categoryType == tasks.CategoryTypeImmediate {
		return tasks.NewImmediateKey(ackLevel)
	}
	return tasks.NewKey(timestamp.UnixOrZeroTime(ackLevel), 0)
}

func convertTaskKeyToPersistenceAckLevel(
	categoryType tasks.CategoryType,
	taskKey tasks.Key,
) int64 {
	if categoryType == tasks.CategoryTypeImmediate {
		return taskKey.TaskID
	}
	return taskKey.FireTime.UnixNano()
}

func ConvertFromPersistenceTaskKey(
	key *persistencespb.TaskKey,
) tasks.Key {
	return tasks.NewKey(
		timestamp.TimeValue(key.FireTime),
		key.TaskId,
	)
}

func ConvertToPersistenceTaskKey(
	key tasks.Key,
) *persistencespb.TaskKey {
	return &persistencespb.TaskKey{
		FireTime: timestamppb.New(key.FireTime),
		TaskId:   key.TaskID,
	}
}

// ReplicationReaderIDFromClusterShardID convert from cluster ID & shard ID to reader ID
// NOTE: cluster metadata guarantee
//  1. initial failover version <= int32 max
//  2. failover increment <= int32 max
//  3. initial failover version == cluster ID
func ReplicationReaderIDFromClusterShardID(
	clusterID int64,
	shardID int32,
) int64 {
	return clusterID<<32 + int64(shardID)
}

// ReplicationReaderIDToClusterShardID convert from reader ID to cluster ID & shard ID
// NOTE: see ReplicationReaderIDFromClusterShardID
func ReplicationReaderIDToClusterShardID(
	readerID int64,
) (int64, int32) {
	return readerID >> 32, int32(readerID & 0xffffffff)
}

func getMinTaskKey(
	queueState *persistencespb.QueueState,
) *tasks.Key {
	var minTaskKey *tasks.Key
	if queueState.ExclusiveReaderHighWatermark != nil {
		taskKey := ConvertFromPersistenceTaskKey(queueState.ExclusiveReaderHighWatermark)
		minTaskKey = &taskKey
	}
	for _, readerState := range queueState.ReaderStates {
		taskKey := ConvertFromPersistenceTaskKey(readerState.Scopes[0].Range.InclusiveMin)
		if minTaskKey == nil || taskKey.CompareTo(*minTaskKey) < 0 {
			minTaskKey = &taskKey
		}
	}
	return minTaskKey
}

// TODO: deprecate this function after disabling ShardOwnershipAssertionEnabled
func AssertShardOwnership(
	ctx context.Context,
	shardContext Context,
	shardOwnershipAsserted *bool,
) error {
	if !shardContext.GetConfig().ShardOwnershipAssertionEnabled() {
		return nil
	}

	if shardOwnershipAsserted == nil || !*shardOwnershipAsserted {
		if err := shardContext.AssertOwnership(ctx); err != nil {
			return err
		}

		if shardOwnershipAsserted != nil {
			*shardOwnershipAsserted = true
		}
	}

	return nil
}
