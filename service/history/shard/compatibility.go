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
	"fmt"
	"math"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/service/history/tasks"
)

func loadShardInfoCompatibilityCheck(
	clusterMetadata cluster.Metadata,
	shardInfo *persistencespb.ShardInfo,
) *persistencespb.ShardInfo {
	// TODO this section maintains the forward / backward compatibility
	//  should be removed once the migration is done
	//  also see ShardInfoToBlob

	allClusterInfo := clusterMetadata.GetAllClusterInfo()
	shardInfo = loadShardInfoCompatibilityCheckWithoutReplication(shardInfo)
	shardInfo = loadShardInfoCompatibilityCheckWithReplication(allClusterInfo, shardInfo)

	// clear QueueAckLevels to force new logic to only use QueueStates
	shardInfo.QueueAckLevels = nil
	return shardInfo
}

func loadShardInfoCompatibilityCheckWithoutReplication(
	shardInfo *persistencespb.ShardInfo,
) *persistencespb.ShardInfo {
	for queueCategoryID, queueAckLevel := range shardInfo.QueueAckLevels {
		if queueCategoryID == tasks.CategoryIDReplication {
			continue
		}

		queueCategory, ok := tasks.GetCategoryByID(queueCategoryID)
		if !ok {
			panic(fmt.Sprintf("unable to find queue category by queye category ID: %v", queueCategoryID))
		}
		minCursor := convertPersistenceAckLevelToTaskKey(queueCategory.Type(), queueAckLevel.AckLevel)
		if queueCategory.Type() == tasks.CategoryTypeImmediate && minCursor.TaskID > 0 {
			// for immediate task type, the ack level is inclusive
			// for scheduled task type, the ack level is exclusive
			minCursor = minCursor.Next()
		}

		queueState, ok := shardInfo.QueueStates[queueCategoryID]
		if !ok || minCursor.CompareTo(ConvertFromPersistenceTaskKey(queueState.ExclusiveReaderHighWatermark)) > 0 {
			queueState = &persistencespb.QueueState{
				ExclusiveReaderHighWatermark: ConvertToPersistenceTaskKey(minCursor),
				ReaderStates:                 make(map[int64]*persistencespb.QueueReaderState),
			}
			shardInfo.QueueStates[queueCategoryID] = queueState
		}
	}
	return shardInfo
}

func loadShardInfoCompatibilityCheckWithReplication(
	allClusterInfo map[string]cluster.ClusterInformation,
	shardInfo *persistencespb.ShardInfo,
) *persistencespb.ShardInfo {
	shardInfo = trimShardInfo(allClusterInfo, shardInfo)
	if shardInfo.QueueAckLevels == nil {
		return shardInfo
	}
	queueAckLevel, ok := shardInfo.QueueAckLevels[tasks.CategoryIDReplication]
	if !ok {
		return shardInfo
	}

	for clusterName, ackLevel := range queueAckLevel.ClusterAckLevel {
		minCursor := convertPersistenceAckLevelToTaskKey(tasks.CategoryReplication.Type(), ackLevel).Next()
		readerID := ReplicationReaderIDFromClusterShardID(
			allClusterInfo[clusterName].InitialFailoverVersion,
			shardInfo.ShardId,
		)

		queueStates, ok := shardInfo.QueueStates[tasks.CategoryIDReplication]
		if !ok {
			queueStates = &persistencespb.QueueState{
				ExclusiveReaderHighWatermark: nil,
				ReaderStates:                 map[int64]*persistencespb.QueueReaderState{},
			}
			shardInfo.QueueStates[tasks.CategoryIDReplication] = queueStates
		}
		readerState, ok := queueStates.ReaderStates[readerID]
		if !ok || minCursor.CompareTo(ConvertFromPersistenceTaskKey(readerState.Scopes[0].Range.InclusiveMin)) > 0 {
			queueStates.ReaderStates[readerID] = &persistencespb.QueueReaderState{
				Scopes: []*persistencespb.QueueSliceScope{{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: ConvertToPersistenceTaskKey(minCursor),
						ExclusiveMax: ConvertToPersistenceTaskKey(
							convertPersistenceAckLevelToTaskKey(
								tasks.CategoryReplication.Type(),
								math.MaxInt64,
							),
						),
					},
					Predicate: &persistencespb.Predicate{
						PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
						Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
					},
				}},
			}
		}
	}
	return shardInfo
}

func storeShardInfoCompatibilityCheck(
	clusterMetadata cluster.Metadata,
	shardInfo *persistencespb.ShardInfo,
) *persistencespb.ShardInfo {
	// TODO this section maintains the forward / backward compatibility
	//  should be removed once the migration is done
	//  also see ShardInfoFromBlob
	allClusterInfo := clusterMetadata.GetAllClusterInfo()
	shardInfo = storeShardInfoCompatibilityCheckWithoutReplication(shardInfo)
	shardInfo = storeShardInfoCompatibilityCheckWithReplication(allClusterInfo, shardInfo)
	return shardInfo
}

func storeShardInfoCompatibilityCheckWithoutReplication(
	shardInfo *persistencespb.ShardInfo,
) *persistencespb.ShardInfo {
	for queueCategoryID, queueState := range shardInfo.QueueStates {
		if queueCategoryID == tasks.CategoryIDReplication {
			continue
		}

		queueCategory, ok := tasks.GetCategoryByID(queueCategoryID)
		if !ok {
			panic(fmt.Sprintf("unable to find queue category by queye category ID: %v", queueCategoryID))
		}

		// for compatability, update ack level and cluster ack level as well
		// so after rollback or disabling the feature, we won't load too many tombstones
		minAckLevel := ConvertFromPersistenceTaskKey(queueState.ExclusiveReaderHighWatermark)
		for _, readerState := range queueState.ReaderStates {
			if len(readerState.Scopes) != 0 {
				minAckLevel = tasks.MinKey(
					minAckLevel,
					ConvertFromPersistenceTaskKey(readerState.Scopes[0].Range.InclusiveMin),
				)
			}
		}

		if queueCategory.Type() == tasks.CategoryTypeImmediate && minAckLevel.TaskID > 0 {
			// for immediate task type, the ack level is inclusive
			// for scheduled task type, the ack level is exclusive
			minAckLevel = minAckLevel.Prev()
		}
		persistenceAckLevel := convertTaskKeyToPersistenceAckLevel(queueCategory.Type(), minAckLevel)

		shardInfo.QueueAckLevels[queueCategoryID] = &persistencespb.QueueAckLevel{
			AckLevel:        persistenceAckLevel,
			ClusterAckLevel: make(map[string]int64),
		}
	}
	return shardInfo
}

func storeShardInfoCompatibilityCheckWithReplication(
	allClusterInfo map[string]cluster.ClusterInformation,
	shardInfo *persistencespb.ShardInfo,
) *persistencespb.ShardInfo {
	shardInfo = trimShardInfo(allClusterInfo, shardInfo)
	if shardInfo.QueueStates == nil {
		return shardInfo
	}
	queueStates, ok := shardInfo.QueueStates[tasks.CategoryIDReplication]
	if !ok {
		return shardInfo
	}

	for readerID, readerState := range queueStates.ReaderStates {
		clusterID, _ := ReplicationReaderIDToClusterShardID(readerID)
		clusterName, _, _ := ClusterNameInfoFromClusterID(allClusterInfo, clusterID)
		queueAckLevel, ok := shardInfo.QueueAckLevels[tasks.CategoryIDReplication]
		if !ok {
			queueAckLevel = &persistencespb.QueueAckLevel{
				AckLevel:        0,
				ClusterAckLevel: make(map[string]int64),
			}
			shardInfo.QueueAckLevels[tasks.CategoryIDReplication] = queueAckLevel
		}
		readerAckLevel := convertTaskKeyToPersistenceAckLevel(
			tasks.CategoryReplication.Type(),
			ConvertFromPersistenceTaskKey(readerState.Scopes[0].Range.InclusiveMin).Prev(),
		)
		if ackLevel, ok := queueAckLevel.ClusterAckLevel[clusterName]; !ok {
			queueAckLevel.ClusterAckLevel[clusterName] = readerAckLevel
		} else if ackLevel > readerAckLevel {
			queueAckLevel.ClusterAckLevel[clusterName] = readerAckLevel
		}
	}
	return shardInfo
}

func trimShardInfo(
	allClusterInfo map[string]cluster.ClusterInformation,
	shardInfo *persistencespb.ShardInfo,
) *persistencespb.ShardInfo {
	// clean up replication info if cluster is disabled || missing
	if shardInfo.QueueAckLevels != nil && shardInfo.QueueAckLevels[tasks.CategoryIDReplication] != nil {
		for clusterName := range shardInfo.QueueAckLevels[tasks.CategoryIDReplication].ClusterAckLevel {
			clusterInfo, ok := allClusterInfo[clusterName]
			if !ok || !clusterInfo.Enabled {
				delete(shardInfo.QueueAckLevels[tasks.CategoryIDReplication].ClusterAckLevel, clusterName)
			}
		}
	}

	if shardInfo.QueueStates != nil && shardInfo.QueueStates[tasks.CategoryIDReplication] != nil {
		for readerID := range shardInfo.QueueStates[tasks.CategoryIDReplication].ReaderStates {
			clusterID, _ := ReplicationReaderIDToClusterShardID(readerID)
			_, clusterInfo, found := ClusterNameInfoFromClusterID(allClusterInfo, clusterID)
			if !found || !clusterInfo.Enabled {
				delete(shardInfo.QueueStates[tasks.CategoryIDReplication].ReaderStates, readerID)
			}
		}
	}
	return shardInfo
}

func ClusterNameInfoFromClusterID(
	allClusterInfo map[string]cluster.ClusterInformation,
	clusterID int64,
) (string, cluster.ClusterInformation, bool) {
	for name, info := range allClusterInfo {
		if info.InitialFailoverVersion == clusterID {
			return name, info, true
		}
	}
	return "", cluster.ClusterInformation{}, false
}
