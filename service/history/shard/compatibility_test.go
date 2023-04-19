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
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/tasks"
)

type (
	compatibilitySuite struct {
		suite.Suite
		*require.Assertions
	}
)

func TestCompatibilitySuite(t *testing.T) {
	s := &compatibilitySuite{}
	suite.Run(t, s)
}

func (s *compatibilitySuite) SetupTest() {
	s.Assertions = require.New(s.T())

}

func (s *compatibilitySuite) TestLoadShardInfoCompatibilityCheckWithoutReplication_OnlyQueueAckLevel() {
	transferAckTaskID := rand.Int63()
	timerAckTime := rand.Int63()
	persistenceShardInfo := &persistencespb.ShardInfo{
		QueueAckLevels: map[int32]*persistencespb.QueueAckLevel{
			tasks.CategoryIDTransfer: {
				AckLevel:        transferAckTaskID,
				ClusterAckLevel: map[string]int64{},
			},
			tasks.CategoryIDTimer: {
				AckLevel:        timerAckTime,
				ClusterAckLevel: map[string]int64{},
			},
		},
		QueueStates: make(map[int32]*persistencespb.QueueState),
	}

	expectedMemShardInfo := &persistencespb.ShardInfo{
		QueueAckLevels: map[int32]*persistencespb.QueueAckLevel{},
		QueueStates: map[int32]*persistencespb.QueueState{
			tasks.CategoryIDTransfer: {
				ExclusiveReaderHighWatermark: &persistencespb.TaskKey{
					FireTime: timestamp.TimePtr(time.Unix(0, 0)),
					TaskId:   transferAckTaskID + 1,
				},
				ReaderStates: map[int64]*persistencespb.QueueReaderState{},
			},
			tasks.CategoryIDTimer: {
				ExclusiveReaderHighWatermark: &persistencespb.TaskKey{
					FireTime: timestamp.TimePtr(time.Unix(0, timerAckTime)),
					TaskId:   0,
				},
				ReaderStates: map[int64]*persistencespb.QueueReaderState{},
			},
		},
	}
	actualMemShardInfo := loadShardInfoCompatibilityCheckWithoutReplication(copyShardInfo(persistenceShardInfo))
	actualMemShardInfo.QueueAckLevels = map[int32]*persistencespb.QueueAckLevel{}
	s.EqualShardInfo(expectedMemShardInfo, actualMemShardInfo)
}

func (s *compatibilitySuite) TestLoadShardInfoCompatibilityCheckWithoutReplication_OnlyQueueState() {
	transferAckTaskID := rand.Int63()
	timerAckTime := rand.Int63()

	persistenceShardInfo := &persistencespb.ShardInfo{
		QueueAckLevels: map[int32]*persistencespb.QueueAckLevel{},
		QueueStates: map[int32]*persistencespb.QueueState{
			tasks.CategoryIDTransfer: {
				ExclusiveReaderHighWatermark: &persistencespb.TaskKey{
					FireTime: timestamp.TimePtr(time.Unix(0, 0)),
					TaskId:   transferAckTaskID + 1,
				},
				ReaderStates: map[int64]*persistencespb.QueueReaderState{},
			},
			tasks.CategoryIDTimer: {
				ExclusiveReaderHighWatermark: &persistencespb.TaskKey{
					FireTime: timestamp.TimePtr(time.Unix(0, timerAckTime)),
					TaskId:   0,
				},
				ReaderStates: map[int64]*persistencespb.QueueReaderState{},
			},
		},
	}

	expectedMemShardInfo := copyShardInfo(persistenceShardInfo)
	actualMemShardInfo := loadShardInfoCompatibilityCheckWithoutReplication(copyShardInfo(persistenceShardInfo))
	s.EqualShardInfo(expectedMemShardInfo, actualMemShardInfo)
}

func (s *compatibilitySuite) TestLoadShardInfoCompatibilityCheckWithoutReplication_Both() {
	ackLevelTransferAckTaskID := rand.Int63()
	ackLevelTimerAckTime := rand.Int63()
	queueStateTransferAckTaskID := rand.Int63()
	queueStateTimerAckTime := rand.Int63()

	persistenceShardInfo := &persistencespb.ShardInfo{
		QueueAckLevels: map[int32]*persistencespb.QueueAckLevel{
			tasks.CategoryIDTransfer: {
				AckLevel:        ackLevelTransferAckTaskID,
				ClusterAckLevel: map[string]int64{},
			},
			tasks.CategoryIDTimer: {
				AckLevel:        ackLevelTimerAckTime,
				ClusterAckLevel: map[string]int64{},
			},
		},
		QueueStates: map[int32]*persistencespb.QueueState{
			tasks.CategoryIDTransfer: {
				ExclusiveReaderHighWatermark: &persistencespb.TaskKey{
					FireTime: timestamp.TimePtr(time.Unix(0, 0)),
					TaskId:   queueStateTransferAckTaskID + 1,
				},
				ReaderStates: map[int64]*persistencespb.QueueReaderState{},
			},
			tasks.CategoryIDTimer: {
				ExclusiveReaderHighWatermark: &persistencespb.TaskKey{
					FireTime: timestamp.TimePtr(time.Unix(0, queueStateTimerAckTime)),
					TaskId:   0,
				},
				ReaderStates: map[int64]*persistencespb.QueueReaderState{},
			},
		},
	}

	expectedMemShardInfo := &persistencespb.ShardInfo{
		QueueAckLevels: map[int32]*persistencespb.QueueAckLevel{},
		QueueStates: map[int32]*persistencespb.QueueState{
			tasks.CategoryIDTransfer: {
				ExclusiveReaderHighWatermark: &persistencespb.TaskKey{
					FireTime: timestamp.TimePtr(time.Unix(0, 0)),
					TaskId:   s.MaxInt64(ackLevelTransferAckTaskID, queueStateTransferAckTaskID) + 1,
				},
				ReaderStates: map[int64]*persistencespb.QueueReaderState{},
			},
			tasks.CategoryIDTimer: {
				ExclusiveReaderHighWatermark: &persistencespb.TaskKey{
					FireTime: timestamp.TimePtr(time.Unix(0, s.MaxInt64(ackLevelTimerAckTime, queueStateTimerAckTime))),
					TaskId:   0,
				},
				ReaderStates: map[int64]*persistencespb.QueueReaderState{},
			},
		},
	}
	actualMemShardInfo := loadShardInfoCompatibilityCheckWithoutReplication(copyShardInfo(persistenceShardInfo))
	actualMemShardInfo.QueueAckLevels = map[int32]*persistencespb.QueueAckLevel{}
	s.EqualShardInfo(expectedMemShardInfo, actualMemShardInfo)
}

func (s *compatibilitySuite) TestStoreShardInfoCompatibilityCheckWithoutReplication_NoOverride() {
	transferAckTaskID := rand.Int63()
	timerAckTime := rand.Int63()

	memShardInfo := &persistencespb.ShardInfo{
		QueueAckLevels: map[int32]*persistencespb.QueueAckLevel{},
		QueueStates: map[int32]*persistencespb.QueueState{
			tasks.CategoryIDTransfer: {
				ExclusiveReaderHighWatermark: &persistencespb.TaskKey{
					FireTime: timestamp.TimePtr(time.Unix(0, 0)),
					TaskId:   transferAckTaskID + 1,
				},
				ReaderStates: map[int64]*persistencespb.QueueReaderState{},
			},
			tasks.CategoryIDTimer: {
				ExclusiveReaderHighWatermark: &persistencespb.TaskKey{
					FireTime: timestamp.TimePtr(time.Unix(0, timerAckTime)),
					TaskId:   0,
				},
				ReaderStates: map[int64]*persistencespb.QueueReaderState{},
			},
		},
	}

	expectedMemShardInfo := copyShardInfo(memShardInfo)
	expectedMemShardInfo.QueueAckLevels = map[int32]*persistencespb.QueueAckLevel{
		tasks.CategoryIDTransfer: {
			AckLevel:        transferAckTaskID,
			ClusterAckLevel: map[string]int64{},
		},
		tasks.CategoryIDTimer: {
			AckLevel:        timerAckTime,
			ClusterAckLevel: map[string]int64{},
		},
	}
	actualMemShardInfo := storeShardInfoCompatibilityCheckWithoutReplication(copyShardInfo(memShardInfo))
	s.EqualShardInfo(expectedMemShardInfo, actualMemShardInfo)
}

func (s *compatibilitySuite) TestStoreShardInfoCompatibilityCheckWithoutReplication_Override() {
	transferAckTaskID := rand.Int63()
	timerAckTime := rand.Int63()

	memShardInfo := &persistencespb.ShardInfo{
		QueueAckLevels: map[int32]*persistencespb.QueueAckLevel{},
		QueueStates: map[int32]*persistencespb.QueueState{
			tasks.CategoryIDTransfer: {
				ExclusiveReaderHighWatermark: &persistencespb.TaskKey{
					FireTime: timestamp.TimePtr(time.Unix(0, 0)),
					TaskId:   transferAckTaskID + 1 + rand.Int63n(100),
				},
				ReaderStates: map[int64]*persistencespb.QueueReaderState{
					0: {
						Scopes: []*persistencespb.QueueSliceScope{{
							Range: &persistencespb.QueueSliceRange{
								InclusiveMin: &persistencespb.TaskKey{
									FireTime: timestamp.TimePtr(time.Unix(0, 0)),
									TaskId:   transferAckTaskID + 1,
								},
								ExclusiveMax: nil, // not used
							},
							Predicate: &persistencespb.Predicate{
								PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
								Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
							},
						}},
					},
				},
			},
			tasks.CategoryIDTimer: {
				ExclusiveReaderHighWatermark: &persistencespb.TaskKey{
					FireTime: timestamp.TimePtr(time.Unix(0, timerAckTime+rand.Int63n(100))),
					TaskId:   0,
				},
				ReaderStates: map[int64]*persistencespb.QueueReaderState{
					0: {
						Scopes: []*persistencespb.QueueSliceScope{{
							Range: &persistencespb.QueueSliceRange{
								InclusiveMin: &persistencespb.TaskKey{
									FireTime: timestamp.TimePtr(time.Unix(0, timerAckTime)),
									TaskId:   0,
								},
								ExclusiveMax: nil, // not used
							},
							Predicate: &persistencespb.Predicate{
								PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
								Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
							},
						}},
					},
				},
			},
		},
	}

	expectedMemShardInfo := copyShardInfo(memShardInfo)
	expectedMemShardInfo.QueueAckLevels = map[int32]*persistencespb.QueueAckLevel{
		tasks.CategoryIDTransfer: {
			AckLevel:        transferAckTaskID,
			ClusterAckLevel: map[string]int64{},
		},
		tasks.CategoryIDTimer: {
			AckLevel:        timerAckTime,
			ClusterAckLevel: map[string]int64{},
		},
	}
	actualMemShardInfo := storeShardInfoCompatibilityCheckWithoutReplication(copyShardInfo(memShardInfo))
	s.EqualShardInfo(expectedMemShardInfo, actualMemShardInfo)
}

func (s *compatibilitySuite) TestLoadShardInfoCompatibilityCheckWithReplication_OnlyQueueAckLevel() {
	allClusterInfo := cluster.TestAllClusterInfo
	shardID := rand.Int31()
	replicationAckTaskID := rand.Int63()
	persistenceShardInfo := &persistencespb.ShardInfo{
		ShardId: shardID,
		QueueAckLevels: map[int32]*persistencespb.QueueAckLevel{
			tasks.CategoryIDReplication: {
				AckLevel: 0,
				ClusterAckLevel: map[string]int64{
					cluster.TestAlternativeClusterName: replicationAckTaskID,
				},
			},
		},
		QueueStates: make(map[int32]*persistencespb.QueueState),
	}

	expectedMemShardInfo := &persistencespb.ShardInfo{
		ShardId:        shardID,
		QueueAckLevels: map[int32]*persistencespb.QueueAckLevel{},
		QueueStates: map[int32]*persistencespb.QueueState{
			tasks.CategoryIDReplication: {
				ExclusiveReaderHighWatermark: nil,
				ReaderStates: map[int64]*persistencespb.QueueReaderState{
					ReplicationReaderIDFromClusterShardID(cluster.TestAlternativeClusterInitialFailoverVersion, shardID): {
						Scopes: []*persistencespb.QueueSliceScope{{
							Range: &persistencespb.QueueSliceRange{
								InclusiveMin: &persistencespb.TaskKey{
									FireTime: timestamp.TimePtr(time.Unix(0, 0)),
									TaskId:   replicationAckTaskID + 1,
								},
								ExclusiveMax: &persistencespb.TaskKey{
									FireTime: timestamp.TimePtr(time.Unix(0, 0)),
									TaskId:   math.MaxInt64,
								},
							},
							Predicate: &persistencespb.Predicate{
								PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
								Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
							},
						}},
					},
				},
			},
		},
	}
	actualMemShardInfo := loadShardInfoCompatibilityCheckWithReplication(allClusterInfo, copyShardInfo(persistenceShardInfo))
	actualMemShardInfo.QueueAckLevels = map[int32]*persistencespb.QueueAckLevel{}
	s.EqualShardInfo(expectedMemShardInfo, actualMemShardInfo)
}

func (s *compatibilitySuite) TestLoadShardInfoCompatibilityCheckWithReplication_OnlyQueueState() {
	allClusterInfo := cluster.TestAllClusterInfo
	shardID := rand.Int31()
	replicationAckTaskID := rand.Int63()
	persistenceShardInfo := &persistencespb.ShardInfo{
		ShardId:        shardID,
		QueueAckLevels: map[int32]*persistencespb.QueueAckLevel{},
		QueueStates: map[int32]*persistencespb.QueueState{
			tasks.CategoryIDReplication: {
				ExclusiveReaderHighWatermark: nil,
				ReaderStates: map[int64]*persistencespb.QueueReaderState{
					ReplicationReaderIDFromClusterShardID(cluster.TestAlternativeClusterInitialFailoverVersion, shardID): {
						Scopes: []*persistencespb.QueueSliceScope{{
							Range: &persistencespb.QueueSliceRange{
								InclusiveMin: &persistencespb.TaskKey{
									FireTime: timestamp.TimePtr(time.Unix(0, 0)),
									TaskId:   replicationAckTaskID + 1,
								},
								ExclusiveMax: &persistencespb.TaskKey{
									FireTime: timestamp.TimePtr(time.Unix(0, 0)),
									TaskId:   math.MaxInt64,
								},
							},
							Predicate: &persistencespb.Predicate{
								PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
								Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
							},
						}},
					},
				},
			},
		},
	}

	expectedMemShardInfo := copyShardInfo(persistenceShardInfo)
	actualMemShardInfo := loadShardInfoCompatibilityCheckWithReplication(allClusterInfo, copyShardInfo(persistenceShardInfo))
	actualMemShardInfo.QueueAckLevels = map[int32]*persistencespb.QueueAckLevel{}
	s.EqualShardInfo(expectedMemShardInfo, actualMemShardInfo)
}

func (s *compatibilitySuite) TestLoadShardInfoCompatibilityCheckWithReplication_Both() {
	allClusterInfo := cluster.TestAllClusterInfo
	shardID := rand.Int31()
	ackLevelReplicationAckTaskID := rand.Int63()
	queueStateReplicationAckTaskID := rand.Int63()
	persistenceShardInfo := &persistencespb.ShardInfo{
		ShardId: shardID,
		QueueAckLevels: map[int32]*persistencespb.QueueAckLevel{
			tasks.CategoryIDReplication: {
				AckLevel: 0,
				ClusterAckLevel: map[string]int64{
					cluster.TestAlternativeClusterName: ackLevelReplicationAckTaskID,
				},
			},
		},
		QueueStates: map[int32]*persistencespb.QueueState{
			tasks.CategoryIDReplication: {
				ExclusiveReaderHighWatermark: nil,
				ReaderStates: map[int64]*persistencespb.QueueReaderState{
					ReplicationReaderIDFromClusterShardID(cluster.TestAlternativeClusterInitialFailoverVersion, shardID): {
						Scopes: []*persistencespb.QueueSliceScope{{
							Range: &persistencespb.QueueSliceRange{
								InclusiveMin: &persistencespb.TaskKey{
									FireTime: timestamp.TimePtr(time.Unix(0, 0)),
									TaskId:   queueStateReplicationAckTaskID + 1,
								},
								ExclusiveMax: &persistencespb.TaskKey{
									FireTime: timestamp.TimePtr(time.Unix(0, 0)),
									TaskId:   math.MaxInt64,
								},
							},
							Predicate: &persistencespb.Predicate{
								PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
								Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
							},
						}},
					},
				},
			},
		},
	}

	expectedMemShardInfo := &persistencespb.ShardInfo{
		ShardId:        shardID,
		QueueAckLevels: map[int32]*persistencespb.QueueAckLevel{},
		QueueStates: map[int32]*persistencespb.QueueState{
			tasks.CategoryIDReplication: {
				ExclusiveReaderHighWatermark: nil,
				ReaderStates: map[int64]*persistencespb.QueueReaderState{
					ReplicationReaderIDFromClusterShardID(cluster.TestAlternativeClusterInitialFailoverVersion, shardID): {
						Scopes: []*persistencespb.QueueSliceScope{{
							Range: &persistencespb.QueueSliceRange{
								InclusiveMin: &persistencespb.TaskKey{
									FireTime: timestamp.TimePtr(time.Unix(0, 0)),
									TaskId:   s.MaxInt64(ackLevelReplicationAckTaskID, queueStateReplicationAckTaskID) + 1,
								},
								ExclusiveMax: &persistencespb.TaskKey{
									FireTime: timestamp.TimePtr(time.Unix(0, 0)),
									TaskId:   math.MaxInt64,
								},
							},
							Predicate: &persistencespb.Predicate{
								PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
								Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
							},
						}},
					},
				},
			},
		},
	}
	actualMemShardInfo := loadShardInfoCompatibilityCheckWithReplication(allClusterInfo, copyShardInfo(persistenceShardInfo))
	actualMemShardInfo.QueueAckLevels = map[int32]*persistencespb.QueueAckLevel{}
	s.EqualShardInfo(expectedMemShardInfo, actualMemShardInfo)
}

func (s *compatibilitySuite) TestStoreShardInfoCompatibilityCheckWithReplication_NoOverride() {
	allClusterInfo := cluster.TestAllClusterInfo
	shardID := rand.Int31()
	replicationAckTaskID := rand.Int63()
	memShardInfo := &persistencespb.ShardInfo{
		ShardId:        shardID,
		QueueAckLevels: map[int32]*persistencespb.QueueAckLevel{},
		QueueStates: map[int32]*persistencespb.QueueState{
			tasks.CategoryIDReplication: {
				ExclusiveReaderHighWatermark: nil,
				ReaderStates: map[int64]*persistencespb.QueueReaderState{
					ReplicationReaderIDFromClusterShardID(cluster.TestAlternativeClusterInitialFailoverVersion, shardID): {
						Scopes: []*persistencespb.QueueSliceScope{{
							Range: &persistencespb.QueueSliceRange{
								InclusiveMin: &persistencespb.TaskKey{
									FireTime: timestamp.TimePtr(time.Unix(0, 0)),
									TaskId:   replicationAckTaskID + 1,
								},
								ExclusiveMax: &persistencespb.TaskKey{
									FireTime: timestamp.TimePtr(time.Unix(0, 0)),
									TaskId:   math.MaxInt64,
								},
							},
							Predicate: &persistencespb.Predicate{
								PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
								Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
							},
						}},
					},
				},
			},
		},
	}

	expectedMemShardInfo := copyShardInfo(memShardInfo)
	expectedMemShardInfo.QueueAckLevels = map[int32]*persistencespb.QueueAckLevel{
		tasks.CategoryIDReplication: {
			AckLevel: 0,
			ClusterAckLevel: map[string]int64{
				cluster.TestAlternativeClusterName: replicationAckTaskID,
			},
		},
	}
	actualMemShardInfo := storeShardInfoCompatibilityCheckWithReplication(allClusterInfo, copyShardInfo(memShardInfo))
	s.EqualShardInfo(expectedMemShardInfo, actualMemShardInfo)
}

func (s *compatibilitySuite) TestStoreShardInfoCompatibilityCheckWithReplication_Override() {
	allClusterInfo := cluster.TestAllClusterInfo
	shardID := rand.Int31()
	replicationAckTaskID := rand.Int63()
	memShardInfo := &persistencespb.ShardInfo{
		ShardId:        shardID,
		QueueAckLevels: map[int32]*persistencespb.QueueAckLevel{},
		QueueStates: map[int32]*persistencespb.QueueState{
			tasks.CategoryIDReplication: {
				ExclusiveReaderHighWatermark: nil,
				ReaderStates: map[int64]*persistencespb.QueueReaderState{
					ReplicationReaderIDFromClusterShardID(cluster.TestAlternativeClusterInitialFailoverVersion, shardID): {
						Scopes: []*persistencespb.QueueSliceScope{{
							Range: &persistencespb.QueueSliceRange{
								InclusiveMin: &persistencespb.TaskKey{
									FireTime: timestamp.TimePtr(time.Unix(0, 0)),
									TaskId:   replicationAckTaskID + 1,
								},
								ExclusiveMax: &persistencespb.TaskKey{
									FireTime: timestamp.TimePtr(time.Unix(0, 0)),
									TaskId:   math.MaxInt64,
								},
							},
							Predicate: &persistencespb.Predicate{
								PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
								Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
							},
						}},
					},
					ReplicationReaderIDFromClusterShardID(cluster.TestAlternativeClusterInitialFailoverVersion, shardID+1): {
						Scopes: []*persistencespb.QueueSliceScope{{
							Range: &persistencespb.QueueSliceRange{
								InclusiveMin: &persistencespb.TaskKey{
									FireTime: timestamp.TimePtr(time.Unix(0, 0)),
									TaskId:   replicationAckTaskID + 1 + rand.Int63n(100),
								},
								ExclusiveMax: &persistencespb.TaskKey{
									FireTime: timestamp.TimePtr(time.Unix(0, 0)),
									TaskId:   math.MaxInt64,
								},
							},
							Predicate: &persistencespb.Predicate{
								PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
								Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
							},
						}},
					},
				},
			},
		},
	}

	expectedMemShardInfo := copyShardInfo(memShardInfo)
	expectedMemShardInfo.QueueAckLevels = map[int32]*persistencespb.QueueAckLevel{
		tasks.CategoryIDReplication: {
			AckLevel: 0,
			ClusterAckLevel: map[string]int64{
				cluster.TestAlternativeClusterName: replicationAckTaskID,
			},
		},
	}
	actualMemShardInfo := storeShardInfoCompatibilityCheckWithReplication(allClusterInfo, copyShardInfo(memShardInfo))
	s.EqualShardInfo(expectedMemShardInfo, actualMemShardInfo)
}

func (s *compatibilitySuite) TestReplicationReaderIDConversion() {
	expectedClusterID := int64(rand.Int31())
	expectedShardID := rand.Int31()

	actualClusterID, actualShardID := ReplicationReaderIDToClusterShardID(
		ReplicationReaderIDFromClusterShardID(expectedClusterID, expectedShardID),
	)
	s.Equal(expectedClusterID, actualClusterID)
	s.Equal(expectedShardID, actualShardID)
}

func (s *compatibilitySuite) TestReplicationReaderIDConversion_1() {
	expectedClusterID := int64(1)
	expectedShardID := int32(1)

	actualClusterID, actualShardID := ReplicationReaderIDToClusterShardID(
		ReplicationReaderIDFromClusterShardID(expectedClusterID, expectedShardID),
	)
	s.Equal(expectedClusterID, actualClusterID)
	s.Equal(expectedShardID, actualShardID)
}

func (s *compatibilitySuite) TestReplicationReaderIDConversion_Int32Max() {
	expectedClusterID := int64(math.MaxInt32)
	expectedShardID := int32(math.MaxInt32)

	actualClusterID, actualShardID := ReplicationReaderIDToClusterShardID(
		ReplicationReaderIDFromClusterShardID(expectedClusterID, expectedShardID),
	)
	s.Equal(expectedClusterID, actualClusterID)
	s.Equal(expectedShardID, actualShardID)
}

func (s *compatibilitySuite) EqualShardInfo(
	expected *persistencespb.ShardInfo,
	actual *persistencespb.ShardInfo,
) {
	// this helper function exists to deal with time comparison issue

	serializer := serialization.NewSerializer()
	expectedBlob, err := serializer.ShardInfoToBlob(expected, enumspb.ENCODING_TYPE_PROTO3)
	s.NoError(err)
	expected, err = serializer.ShardInfoFromBlob(expectedBlob)
	s.NoError(err)

	actualBlob, err := serializer.ShardInfoToBlob(actual, enumspb.ENCODING_TYPE_PROTO3)
	s.NoError(err)
	actual, err = serializer.ShardInfoFromBlob(actualBlob)
	s.NoError(err)

	s.Equal(expected, actual)
}

func (s *compatibilitySuite) MaxInt64(
	this int64,
	that int64,
) int64 {
	if this < that {
		return that
	}
	return this
}
