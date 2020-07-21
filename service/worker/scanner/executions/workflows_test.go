// The MIT License (MIT)
//
// Copyright (c) 2017-2020 Uber Technologies Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package executions

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/testsuite"
	"go.uber.org/cadence/workflow"

	"github.com/uber/cadence/common"
	c "github.com/uber/cadence/common/reconciliation/common"
)

type workflowsSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
}

func TestWorkflowsSuite(t *testing.T) {
	suite.Run(t, new(workflowsSuite))
}

func (s *workflowsSuite) SetupSuite() {
	workflow.Register(ScannerWorkflow)
	activity.RegisterWithOptions(ScannerConfigActivity, activity.RegisterOptions{Name: ScannerConfigActivityName})
	activity.RegisterWithOptions(ScanShardActivity, activity.RegisterOptions{Name: ScannerScanShardActivityName})
	activity.RegisterWithOptions(ScannerEmitMetricsActivity, activity.RegisterOptions{Name: ScannerEmitMetricsActivityName})

	workflow.Register(FixerWorkflow)
	workflow.Register(getCorruptedKeys)
	activity.RegisterWithOptions(FixShardActivity, activity.RegisterOptions{Name: FixerFixShardActivityName})
	activity.RegisterWithOptions(FixerCorruptedKeysActivity, activity.RegisterOptions{Name: FixerCorruptedKeysActivityName})
}

func (s *workflowsSuite) TestScannerWorkflow_Failure_ScannerConfigActivity() {
	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(ScannerConfigActivityName, mock.Anything, mock.Anything).Return(ResolvedScannerWorkflowConfig{}, errors.New("got error getting config"))
	env.ExecuteWorkflow(ScannerWorkflow, ScannerWorkflowParams{
		Shards: Shards{
			List: []int{1, 2, 3},
		},
	})
	s.True(env.IsWorkflowCompleted())
	s.Equal("got error getting config", env.GetWorkflowError().Error())
}

func (s *workflowsSuite) TestScannerWorkflow_Success_Disabled() {
	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(ScannerConfigActivityName, mock.Anything, mock.Anything).Return(ResolvedScannerWorkflowConfig{
		Enabled: false,
	}, nil)
	env.ExecuteWorkflow(ScannerWorkflow, ScannerWorkflowParams{
		Shards: Shards{
			List: []int{1, 2, 3},
		},
	})
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *workflowsSuite) TestScannerWorkflow_Success() {
	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(ScannerConfigActivityName, mock.Anything, mock.Anything).Return(ResolvedScannerWorkflowConfig{
		Enabled:           true,
		Concurrency:       3,
		ActivityBatchSize: 5,
	}, nil)
	env.OnActivity(ScannerEmitMetricsActivityName, mock.Anything, mock.Anything).Return(nil)
	shards := Shards{
		Range: &ShardRange{
			Min: 0,
			Max: 30,
		},
	}

	batches := [][]int{
		{0, 3, 6, 9, 12},
		{15, 18, 21, 24, 27},
		{1, 4, 7, 10, 13},
		{16, 19, 22, 25, 28},
		{2, 5, 8, 11, 14},
		{17, 20, 23, 26, 29},
	}

	for _, batch := range batches {
		var reports []c.ShardScanReport
		for i := range batch {
			if i == 0 {
				reports = append(reports, c.ShardScanReport{
					ShardID: batch[i],
					Stats: c.ShardScanStats{
						ExecutionsCount: 10,
					},
					Result: c.ShardScanResult{
						ControlFlowFailure: &c.ControlFlowFailure{
							Info: "got control flow failure",
						},
					},
				})
			} else {
				reports = append(reports, c.ShardScanReport{
					ShardID: batch[i],
					Stats: c.ShardScanStats{
						ExecutionsCount:  10,
						CorruptedCount:   2,
						CheckFailedCount: 1,
						CorruptionByType: map[c.InvariantType]int64{
							c.HistoryExistsInvariantType: 1,
						},
						CorruptedOpenExecutionCount: 0,
					},
					Result: c.ShardScanResult{
						ShardScanKeys: &c.ShardScanKeys{
							Corrupt: &c.Keys{
								UUID:    "test_uuid",
								MinPage: 0,
								MaxPage: 10,
							},
						},
					},
				})
			}
		}
		env.OnActivity(ScannerScanShardActivityName, mock.Anything, ScanShardActivityParams{
			Shards: batch,
		}).Return(reports, nil)
	}

	env.ExecuteWorkflow(ScannerWorkflow, ScannerWorkflowParams{
		Shards: shards,
	})
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())

	aggValue, err := env.QueryWorkflow(AggregateReportQuery)
	s.NoError(err)
	var agg AggregateScanReportResult
	s.NoError(aggValue.Get(&agg))
	s.Equal(AggregateScanReportResult{
		ExecutionsCount:  240,
		CorruptedCount:   48,
		CheckFailedCount: 24,
		CorruptionByType: map[c.InvariantType]int64{
			c.HistoryExistsInvariantType: 24,
		},
	}, agg)

	for i := 0; i < 30; i++ {
		shardReportValue, err := env.QueryWorkflow(ShardReportQuery, i)
		s.NoError(err)
		var shardReport *c.ShardScanReport
		s.NoError(shardReportValue.Get(&shardReport))
		if i == 0 || i == 1 || i == 2 || i == 15 || i == 16 || i == 17 {
			s.Equal(&c.ShardScanReport{
				ShardID: i,
				Stats: c.ShardScanStats{
					ExecutionsCount: 10,
				},
				Result: c.ShardScanResult{
					ControlFlowFailure: &c.ControlFlowFailure{
						Info: "got control flow failure",
					},
				},
			}, shardReport)
		} else {
			s.Equal(&c.ShardScanReport{
				ShardID: i,
				Stats: c.ShardScanStats{
					ExecutionsCount:  10,
					CorruptedCount:   2,
					CheckFailedCount: 1,
					CorruptionByType: map[c.InvariantType]int64{
						c.HistoryExistsInvariantType: 1,
					},
					CorruptedOpenExecutionCount: 0,
				},
				Result: c.ShardScanResult{
					ShardScanKeys: &c.ShardScanKeys{
						Corrupt: &c.Keys{
							UUID:    "test_uuid",
							MinPage: 0,
							MaxPage: 10,
						},
					},
				},
			}, shardReport)
		}
	}

	statusValue, err := env.QueryWorkflow(ShardStatusQuery, PaginatedShardQueryRequest{})
	s.NoError(err)
	var status *ShardStatusQueryResult
	s.NoError(statusValue.Get(&status))
	expected := make(map[int]ShardStatus)
	for i := 0; i < 30; i++ {
		if i == 0 || i == 1 || i == 2 || i == 15 || i == 16 || i == 17 {
			expected[i] = ShardStatusControlFlowFailure
		} else {
			expected[i] = ShardStatusSuccess
		}
	}
	s.Equal(ShardStatusResult(expected), status.Result)
	s.True(status.ShardQueryPaginationToken.IsDone)
	s.Nil(status.ShardQueryPaginationToken.NextShardID)

	// check for paginated query result
	statusValue, err = env.QueryWorkflow(ShardStatusQuery, PaginatedShardQueryRequest{
		StartingShardID: common.IntPtr(5),
		LimitShards:     common.IntPtr(10),
	})
	s.NoError(err)
	status = &ShardStatusQueryResult{}
	s.NoError(statusValue.Get(&status))
	expected = make(map[int]ShardStatus)
	for i := 5; i < 15; i++ {
		if i == 0 || i == 1 || i == 2 || i == 15 || i == 16 || i == 17 {
			expected[i] = ShardStatusControlFlowFailure
		} else {
			expected[i] = ShardStatusSuccess
		}
	}
	s.Equal(ShardStatusResult(expected), status.Result)
	s.False(status.ShardQueryPaginationToken.IsDone)
	s.Equal(15, *status.ShardQueryPaginationToken.NextShardID)

	corruptionKeysValue, err := env.QueryWorkflow(ShardCorruptKeysQuery, PaginatedShardQueryRequest{})
	s.NoError(err)
	var shardCorruptKeysResult *ShardCorruptKeysQueryResult
	s.NoError(corruptionKeysValue.Get(&shardCorruptKeysResult))
	expectedCorrupted := make(map[int]c.Keys)
	for i := 0; i < 30; i++ {
		if i != 0 && i != 1 && i != 2 && i != 15 && i != 16 && i != 17 {
			expectedCorrupted[i] = c.Keys{
				UUID:    "test_uuid",
				MinPage: 0,
				MaxPage: 10,
			}
		}
	}
	s.Equal(ShardCorruptKeysResult(expectedCorrupted), shardCorruptKeysResult.Result)
}

func (s *workflowsSuite) TestScannerWorkflow_Failure_ScanShard() {
	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(ScannerConfigActivityName, mock.Anything, mock.Anything).Return(ResolvedScannerWorkflowConfig{
		Enabled:           true,
		Concurrency:       3,
		ActivityBatchSize: 5,
	}, nil)
	shards := Shards{
		Range: &ShardRange{
			Min: 0,
			Max: 30,
		},
	}

	batches := [][]int{
		{0, 3, 6, 9, 12},
		{15, 18, 21, 24, 27},
		{1, 4, 7, 10, 13},
		{16, 19, 22, 25, 28},
		{2, 5, 8, 11, 14},
		{17, 20, 23, 26, 29},
	}

	for i, batch := range batches {
		var reports []c.ShardScanReport
		var err error
		if i == len(batches)-1 {
			reports = nil
			err = errors.New("scan shard activity got error")
		} else {
			err = nil
			for _, shard := range batch {
				reports = append(reports, c.ShardScanReport{
					ShardID: shard,
					Stats: c.ShardScanStats{
						ExecutionsCount: 10,
					},
					Result: c.ShardScanResult{
						ControlFlowFailure: &c.ControlFlowFailure{
							Info: "got control flow failure",
						},
					},
				})
			}
		}
		env.OnActivity(ScannerScanShardActivityName, mock.Anything, ScanShardActivityParams{
			Shards: batch,
		}).Return(reports, err)
	}
	env.ExecuteWorkflow(ScannerWorkflow, ScannerWorkflowParams{
		Shards: shards,
	})
	s.True(env.IsWorkflowCompleted())
	s.Equal("scan shard activity got error", env.GetWorkflowError().Error())
}

func (s *workflowsSuite) TestScannerWorkflow_Failure_CorruptedKeysActivity() {
	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(FixerCorruptedKeysActivityName, mock.Anything, mock.Anything).Return(nil, errors.New("got error getting corrupted keys"))
	env.ExecuteWorkflow(FixerWorkflow, FixerWorkflowParams{})
	s.True(env.IsWorkflowCompleted())
	s.Equal("got error getting corrupted keys", env.GetWorkflowError().Error())
}

func (s *workflowsSuite) TestFixerWorkflow_Success() {
	env := s.NewTestWorkflowEnvironment()
	corruptedKeys := make([]CorruptedKeysEntry, 30, 30)
	for i := 0; i < 30; i++ {
		corruptedKeys[i] = CorruptedKeysEntry{
			ShardID: i,
		}
	}
	env.OnActivity(FixerCorruptedKeysActivityName, mock.Anything, mock.Anything).Return(&FixerCorruptedKeysActivityResult{
		CorruptedKeys: corruptedKeys,
		MinShard:      common.IntPtr(0),
		MaxShard:      common.IntPtr(29),
		ShardQueryPaginationToken: ShardQueryPaginationToken{
			IsDone:      true,
			NextShardID: nil,
		},
	}, nil)

	fixerWorkflowConfigOverwrites := FixerWorkflowConfigOverwrites{
		Concurrency:             common.IntPtr(3),
		BlobstoreFlushThreshold: common.IntPtr(1000),
		ActivityBatchSize:       common.IntPtr(5),
		InvariantCollections: &InvariantCollections{
			InvariantCollectionHistory:      true,
			InvariantCollectionMutableState: true,
		},
	}
	resolvedFixerWorkflowConfig := ResolvedFixerWorkflowConfig{
		Concurrency:             3,
		ActivityBatchSize:       5,
		BlobstoreFlushThreshold: 1000,
		InvariantCollections: InvariantCollections{
			InvariantCollectionMutableState: true,
			InvariantCollectionHistory:      true,
		},
	}
	batches := [][]int{
		{0, 3, 6, 9, 12},
		{15, 18, 21, 24, 27},
		{1, 4, 7, 10, 13},
		{16, 19, 22, 25, 28},
		{2, 5, 8, 11, 14},
		{17, 20, 23, 26, 29},
	}

	for _, batch := range batches {
		var corruptedKeys []CorruptedKeysEntry
		for _, shard := range batch {
			corruptedKeys = append(corruptedKeys, CorruptedKeysEntry{
				ShardID: shard,
			})
		}
		var reports []c.ShardFixReport
		for i, shard := range batch {
			if i == 0 {
				reports = append(reports, c.ShardFixReport{
					ShardID: shard,
					Stats: c.ShardFixStats{
						ExecutionCount: 10,
					},
					Result: c.ShardFixResult{
						ControlFlowFailure: &c.ControlFlowFailure{
							Info: "got control flow failure",
						},
					},
				})
			} else {
				reports = append(reports, c.ShardFixReport{
					ShardID: shard,
					Stats: c.ShardFixStats{
						ExecutionCount: 10,
						FixedCount:     2,
						SkippedCount:   1,
						FailedCount:    1,
					},
					Result: c.ShardFixResult{
						ShardFixKeys: &c.ShardFixKeys{
							Skipped: &c.Keys{
								UUID: "skipped_keys",
							},
							Failed: &c.Keys{
								UUID: "failed_keys",
							},
							Fixed: &c.Keys{
								UUID: "fixed_keys",
							},
						},
					},
				})
			}
		}
		env.OnActivity(FixerFixShardActivityName, mock.Anything, FixShardActivityParams{
			CorruptedKeysEntries:        corruptedKeys,
			ResolvedFixerWorkflowConfig: resolvedFixerWorkflowConfig,
		}).Return(reports, nil)
	}

	env.ExecuteWorkflow(FixerWorkflow, FixerWorkflowParams{
		ScannerWorkflowWorkflowID:     "test_wid",
		ScannerWorkflowRunID:          "test_rid",
		FixerWorkflowConfigOverwrites: fixerWorkflowConfigOverwrites,
	})
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())

	aggValue, err := env.QueryWorkflow(AggregateReportQuery)
	s.NoError(err)
	var agg AggregateFixReportResult
	s.NoError(aggValue.Get(&agg))
	s.Equal(AggregateFixReportResult{
		ExecutionCount: 240,
		FixedCount:     48,
		FailedCount:    24,
		SkippedCount:   24,
	}, agg)

	for i := 0; i < 30; i++ {
		shardReportValue, err := env.QueryWorkflow(ShardReportQuery, i)
		s.NoError(err)
		var shardReport *c.ShardFixReport
		s.NoError(shardReportValue.Get(&shardReport))
		if i == 0 || i == 1 || i == 2 || i == 15 || i == 16 || i == 17 {
			s.Equal(&c.ShardFixReport{
				ShardID: i,
				Stats: c.ShardFixStats{
					ExecutionCount: 10,
				},
				Result: c.ShardFixResult{
					ControlFlowFailure: &c.ControlFlowFailure{
						Info: "got control flow failure",
					},
				},
			}, shardReport)
		} else {
			s.Equal(&c.ShardFixReport{
				ShardID: i,
				Stats: c.ShardFixStats{
					ExecutionCount: 10,
					FixedCount:     2,
					FailedCount:    1,
					SkippedCount:   1,
				},
				Result: c.ShardFixResult{
					ShardFixKeys: &c.ShardFixKeys{
						Skipped: &c.Keys{
							UUID: "skipped_keys",
						},
						Failed: &c.Keys{
							UUID: "failed_keys",
						},
						Fixed: &c.Keys{
							UUID: "fixed_keys",
						},
					},
				},
			}, shardReport)
		}
	}

	statusValue, err := env.QueryWorkflow(ShardStatusQuery, PaginatedShardQueryRequest{})
	s.NoError(err)
	var status *ShardStatusQueryResult
	s.NoError(statusValue.Get(&status))
	expected := make(map[int]ShardStatus)
	for i := 0; i < 30; i++ {
		if i == 0 || i == 1 || i == 2 || i == 15 || i == 16 || i == 17 {
			expected[i] = ShardStatusControlFlowFailure
		} else {
			expected[i] = ShardStatusSuccess
		}
	}
	s.Equal(ShardStatusResult(expected), status.Result)

	// check for paginated query result
	statusValue, err = env.QueryWorkflow(ShardStatusQuery, PaginatedShardQueryRequest{
		StartingShardID: common.IntPtr(5),
		LimitShards:     common.IntPtr(10),
	})
	s.NoError(err)
	status = &ShardStatusQueryResult{}
	s.NoError(statusValue.Get(&status))
	expected = make(map[int]ShardStatus)
	for i := 5; i < 15; i++ {
		if i == 0 || i == 1 || i == 2 || i == 15 || i == 16 || i == 17 {
			expected[i] = ShardStatusControlFlowFailure
		} else {
			expected[i] = ShardStatusSuccess
		}
	}
	s.Equal(ShardStatusResult(expected), status.Result)
	s.False(status.ShardQueryPaginationToken.IsDone)
	s.Equal(15, *status.ShardQueryPaginationToken.NextShardID)
}

func (s *workflowsSuite) TestGetCorruptedKeys_Success() {
	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(FixerCorruptedKeysActivityName, mock.Anything, FixerCorruptedKeysActivityParams{
		ScannerWorkflowWorkflowID: "test_wid",
		ScannerWorkflowRunID:      "test_rid",
		StartingShardID:           nil,
	}).Return(&FixerCorruptedKeysActivityResult{
		CorruptedKeys: []CorruptedKeysEntry{{ShardID: 1}, {ShardID: 5}, {ShardID: 10}},
		MinShard:      common.IntPtr(1),
		MaxShard:      common.IntPtr(10),
		ShardQueryPaginationToken: ShardQueryPaginationToken{
			NextShardID: common.IntPtr(11),
			IsDone:      false,
		},
	}, nil)
	env.OnActivity(FixerCorruptedKeysActivityName, mock.Anything, FixerCorruptedKeysActivityParams{
		ScannerWorkflowWorkflowID: "test_wid",
		ScannerWorkflowRunID:      "test_rid",
		StartingShardID:           common.IntPtr(11),
	}).Return(&FixerCorruptedKeysActivityResult{
		CorruptedKeys: []CorruptedKeysEntry{{ShardID: 11}, {ShardID: 12}},
		MinShard:      common.IntPtr(11),
		MaxShard:      common.IntPtr(12),
		ShardQueryPaginationToken: ShardQueryPaginationToken{
			NextShardID: common.IntPtr(13),
			IsDone:      false,
		},
	}, nil)
	env.OnActivity(FixerCorruptedKeysActivityName, mock.Anything, FixerCorruptedKeysActivityParams{
		ScannerWorkflowWorkflowID: "test_wid",
		ScannerWorkflowRunID:      "test_rid",
		StartingShardID:           common.IntPtr(13),
	}).Return(&FixerCorruptedKeysActivityResult{
		CorruptedKeys: []CorruptedKeysEntry{{ShardID: 20}, {ShardID: 41}},
		MinShard:      common.IntPtr(20),
		MaxShard:      common.IntPtr(41),
		ShardQueryPaginationToken: ShardQueryPaginationToken{
			NextShardID: common.IntPtr(42),
			IsDone:      false,
		},
	}, nil)
	env.OnActivity(FixerCorruptedKeysActivityName, mock.Anything, FixerCorruptedKeysActivityParams{
		ScannerWorkflowWorkflowID: "test_wid",
		ScannerWorkflowRunID:      "test_rid",
		StartingShardID:           common.IntPtr(42),
	}).Return(&FixerCorruptedKeysActivityResult{
		CorruptedKeys: []CorruptedKeysEntry{},
		MinShard:      nil,
		MaxShard:      nil,
		ShardQueryPaginationToken: ShardQueryPaginationToken{
			NextShardID: nil,
			IsDone:      true,
		},
	}, nil)

	env.ExecuteWorkflow(getCorruptedKeys, FixerWorkflowParams{
		ScannerWorkflowWorkflowID: "test_wid",
		ScannerWorkflowRunID:      "test_rid",
	})
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
	var result *FixerCorruptedKeysActivityResult
	s.NoError(env.GetWorkflowResult(&result))
	s.Equal(&FixerCorruptedKeysActivityResult{
		CorruptedKeys: []CorruptedKeysEntry{
			{ShardID: 1},
			{ShardID: 5},
			{ShardID: 10},
			{ShardID: 11},
			{ShardID: 12},
			{ShardID: 20},
			{ShardID: 41},
		},
		MinShard: common.IntPtr(1),
		MaxShard: common.IntPtr(41),
		ShardQueryPaginationToken: ShardQueryPaginationToken{
			NextShardID: nil,
			IsDone:      true,
		},
	}, result)
}

func (s *workflowsSuite) TestGetCorruptedKeys_Error() {
	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(FixerCorruptedKeysActivityName, mock.Anything, FixerCorruptedKeysActivityParams{
		ScannerWorkflowWorkflowID: "test_wid",
		ScannerWorkflowRunID:      "test_rid",
		StartingShardID:           nil,
	}).Return(&FixerCorruptedKeysActivityResult{
		CorruptedKeys: []CorruptedKeysEntry{{ShardID: 1}, {ShardID: 5}, {ShardID: 10}},
		MinShard:      common.IntPtr(1),
		MaxShard:      common.IntPtr(10),
		ShardQueryPaginationToken: ShardQueryPaginationToken{
			NextShardID: common.IntPtr(11),
			IsDone:      false,
		},
	}, nil)
	env.OnActivity(FixerCorruptedKeysActivityName, mock.Anything, FixerCorruptedKeysActivityParams{
		ScannerWorkflowWorkflowID: "test_wid",
		ScannerWorkflowRunID:      "test_rid",
		StartingShardID:           common.IntPtr(11),
	}).Return(nil, errors.New("got error"))
	env.ExecuteWorkflow(getCorruptedKeys, FixerWorkflowParams{
		ScannerWorkflowWorkflowID: "test_wid",
		ScannerWorkflowRunID:      "test_rid",
	})
	s.True(env.IsWorkflowCompleted())
	s.Error(env.GetWorkflowError())
}

func (s *workflowsSuite) TestFlattenShards() {
	testCases := []struct {
		input        Shards
		expectedList []int
		expectedMin  int
		expectedMax  int
		msg          string
	}{
		{
			input: Shards{
				List: []int{1, 2, 3},
			},
			expectedList: []int{1, 2, 3},
			expectedMin:  1,
			expectedMax:  3,
			msg:          "Shard list provided",
		},
		{
			input: Shards{
				Range: &ShardRange{
					Min: 5,
					Max: 10,
				},
			},
			expectedList: []int{5, 6, 7, 8, 9},
			expectedMin:  5,
			expectedMax:  9,
			msg:          "Shard range provided",
		},
		{
			input: Shards{
				List: []int{1, 90, 2, 3},
			},
			expectedList: []int{1, 90, 2, 3},
			expectedMin:  1,
			expectedMax:  90,
			msg:          "Unordered shard list provided",
		},
	}
	for _, tc := range testCases {
		shardList, min, max := tc.input.Flatten()
		s.Equal(tc.expectedList, shardList)
		s.Equal(tc.expectedMin, min)
		s.Equal(tc.expectedMax, max)
	}
}

func (s *workflowsSuite) TestValidateShards() {
	testCases := []struct {
		shards    Shards
		expectErr bool
	}{
		{
			shards:    Shards{},
			expectErr: true,
		},
		{
			shards: Shards{
				List:  []int{},
				Range: &ShardRange{},
			},
			expectErr: true,
		},
		{
			shards: Shards{
				List: []int{},
			},
			expectErr: true,
		},
		{
			shards: Shards{
				Range: &ShardRange{
					Min: 0,
					Max: 0,
				},
			},
			expectErr: true,
		},
		{
			shards: Shards{
				Range: &ShardRange{
					Min: 0,
					Max: 1,
				},
			},
			expectErr: false,
		},
	}

	for _, tc := range testCases {
		err := tc.shards.Validate()
		if tc.expectErr {
			s.Error(err)
		} else {
			s.NoError(err)
		}
	}
}
