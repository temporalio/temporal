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
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/tasks"
)

func emitShardInfoMetricsLogsLocked(
	metricsHandler metrics.Handler,
	logger log.Logger,
	queueStates map[int32]*persistencespb.QueueState,
	immediateTaskExclusiveMaxReadLevel int64,
	scheduledTaskMaxReadLevel time.Time,
) {
Loop:
	for categoryID, queueState := range queueStates {
		category, ok := tasks.GetCategoryByID(categoryID)
		if !ok {
			continue Loop
		}

		switch category.Type() {
		case tasks.CategoryTypeImmediate:
			minTaskKey := getMinTaskKey(queueState)
			if minTaskKey == nil {
				continue Loop
			}
			lag := immediateTaskExclusiveMaxReadLevel - minTaskKey.TaskID
			if lag > logWarnImmediateTaskLag {
				logger.Warn(
					"Shard queue lag exceeds warn threshold.",
					tag.ShardQueueAcks(category.Name(), minTaskKey.TaskID),
				)
			}
			metricsHandler.Histogram(
				metrics.ShardInfoImmediateQueueLagHistogram.GetMetricName(),
				metrics.ShardInfoImmediateQueueLagHistogram.GetMetricUnit(),
			).Record(lag, metrics.TaskCategoryTag(category.Name()))

		case tasks.CategoryTypeScheduled:
			minTaskKey := getMinTaskKey(queueState)
			if minTaskKey == nil {
				continue Loop
			}
			lag := scheduledTaskMaxReadLevel.Sub(minTaskKey.FireTime)
			if lag > logWarnScheduledTaskLag {
				logger.Warn(
					"Shard queue lag exceeds warn threshold.",
					tag.ShardQueueAcks(category.Name(), minTaskKey.FireTime),
				)
			}
			metricsHandler.Timer(
				metrics.ShardInfoScheduledQueueLagTimer.GetMetricName(),
			).Record(lag, metrics.TaskCategoryTag(category.Name()))
		default:
			logger.Error("Unknown task category type", tag.NewStringTag("task-category", category.Type().String()))
		}
	}
}

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
		FireTime: timestamp.TimePtr(key.FireTime),
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
