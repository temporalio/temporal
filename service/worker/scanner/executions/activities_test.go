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
	"context"
	"encoding/json"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/testsuite"
	"go.uber.org/cadence/worker"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/resource"
	"github.com/uber/cadence/common/service/dynamicconfig"
	c "github.com/uber/cadence/service/worker/scanner/executions/common"
)

type activitiesSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	controller   *gomock.Controller
	mockResource *resource.Test
}

func TestActivitiesSuite(t *testing.T) {
	suite.Run(t, new(activitiesSuite))
}

func (s *activitiesSuite) SetupSuite() {
	activity.Register(ScannerConfigActivity)
	activity.Register(FixerCorruptedKeysActivity)
}

func (s *activitiesSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.mockResource = resource.NewTest(s.controller, metrics.Worker)
}

func (s *activitiesSuite) TestScannerConfigActivity() {
	testCases := []struct {
		scannerWorkflowDynamicConfig *ScannerWorkflowDynamicConfig
		params                       ScannerConfigActivityParams
		resolved                     ResolvedScannerWorkflowConfig
	}{
		{
			scannerWorkflowDynamicConfig: &ScannerWorkflowDynamicConfig{
				Enabled:                 dynamicconfig.GetBoolPropertyFn(true),
				Concurrency:             dynamicconfig.GetIntPropertyFn(10),
				ExecutionsPageSize:      dynamicconfig.GetIntPropertyFn(100),
				ActivityBatchSize:       dynamicconfig.GetIntPropertyFn(10),
				BlobstoreFlushThreshold: dynamicconfig.GetIntPropertyFn(1000),
				DynamicConfigInvariantCollections: DynamicConfigInvariantCollections{
					InvariantCollectionMutableState: dynamicconfig.GetBoolPropertyFn(true),
					InvariantCollectionHistory:      dynamicconfig.GetBoolPropertyFn(false),
				},
			},
			params: ScannerConfigActivityParams{
				Overwrites: ScannerWorkflowConfigOverwrites{},
			},
			resolved: ResolvedScannerWorkflowConfig{
				Enabled:                 true,
				Concurrency:             10,
				ActivityBatchSize:       10,
				ExecutionsPageSize:      100,
				BlobstoreFlushThreshold: 1000,
				InvariantCollections: InvariantCollections{
					InvariantCollectionHistory:      false,
					InvariantCollectionMutableState: true,
				},
			},
		},
		{
			scannerWorkflowDynamicConfig: &ScannerWorkflowDynamicConfig{
				Enabled:                 dynamicconfig.GetBoolPropertyFn(true),
				Concurrency:             dynamicconfig.GetIntPropertyFn(10),
				ActivityBatchSize:       dynamicconfig.GetIntPropertyFn(100),
				ExecutionsPageSize:      dynamicconfig.GetIntPropertyFn(100),
				BlobstoreFlushThreshold: dynamicconfig.GetIntPropertyFn(1000),
				DynamicConfigInvariantCollections: DynamicConfigInvariantCollections{
					InvariantCollectionMutableState: dynamicconfig.GetBoolPropertyFn(true),
					InvariantCollectionHistory:      dynamicconfig.GetBoolPropertyFn(false),
				},
			},
			params: ScannerConfigActivityParams{
				Overwrites: ScannerWorkflowConfigOverwrites{
					Enabled:           common.BoolPtr(false),
					ActivityBatchSize: common.IntPtr(1),
					InvariantCollections: &InvariantCollections{
						InvariantCollectionMutableState: false,
						InvariantCollectionHistory:      true,
					},
				},
			},
			resolved: ResolvedScannerWorkflowConfig{
				Enabled:                 false,
				Concurrency:             10,
				ActivityBatchSize:       1,
				ExecutionsPageSize:      100,
				BlobstoreFlushThreshold: 1000,
				InvariantCollections: InvariantCollections{
					InvariantCollectionHistory:      true,
					InvariantCollectionMutableState: false,
				},
			},
		},
	}

	for _, tc := range testCases {
		env := s.NewTestActivityEnvironment()
		env.SetWorkerOptions(worker.Options{
			BackgroundActivityContext: context.WithValue(context.Background(), ScannerContextKey, ScannerContext{
				ScannerWorkflowDynamicConfig: tc.scannerWorkflowDynamicConfig,
			}),
		})
		resolvedValue, err := env.ExecuteActivity(ScannerConfigActivity, tc.params)
		s.NoError(err)
		var resolved ResolvedScannerWorkflowConfig
		s.NoError(resolvedValue.Get(&resolved))
		s.Equal(tc.resolved, resolved)
	}
}

func (s *activitiesSuite) TestFixerCorruptedKeysActivity() {
	s.mockResource.SDKClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(&shared.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &shared.WorkflowExecutionInfo{
			CloseStatus: shared.WorkflowExecutionCloseStatusCompleted.Ptr(),
		},
	}, nil)
	queryResult := &ShardCorruptKeysQueryResult{
		Result: map[int]c.Keys{
			1: {
				UUID: "first",
			},
			2: {
				UUID: "second",
			},
			3: {
				UUID: "third",
			},
		},
		ShardQueryPaginationToken: ShardQueryPaginationToken{
			NextShardID: common.IntPtr(4),
			IsDone:      false,
		},
	}
	queryResultData, err := json.Marshal(queryResult)
	s.NoError(err)
	s.mockResource.SDKClient.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any()).Return(&shared.QueryWorkflowResponse{
		QueryResult: queryResultData,
	}, nil)
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), FixerContextKey, FixerContext{
			Resource: s.mockResource,
		}),
	})
	fixerResultValue, err := env.ExecuteActivity(FixerCorruptedKeysActivity, FixerCorruptedKeysActivityParams{})
	s.NoError(err)
	fixerResult := &FixerCorruptedKeysActivityResult{}
	s.NoError(fixerResultValue.Get(&fixerResult))
	s.Equal(1, *fixerResult.MinShard)
	s.Equal(3, *fixerResult.MaxShard)
	s.Equal(ShardQueryPaginationToken{
		NextShardID: common.IntPtr(4),
		IsDone:      false,
	}, fixerResult.ShardQueryPaginationToken)
	s.Contains(fixerResult.CorruptedKeys, CorruptedKeysEntry{
		ShardID: 1,
		CorruptedKeys: c.Keys{
			UUID: "first",
		},
	})
	s.Contains(fixerResult.CorruptedKeys, CorruptedKeysEntry{
		ShardID: 2,
		CorruptedKeys: c.Keys{
			UUID: "second",
		},
	})
	s.Contains(fixerResult.CorruptedKeys, CorruptedKeysEntry{
		ShardID: 3,
		CorruptedKeys: c.Keys{
			UUID: "third",
		},
	})
}
