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

package executions

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	historypb "go.temporal.io/api/history/v1"
	historyspb "go.temporal.io/server/api/history/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/quotas"
)

type (
	ShardValidatorTestSuite struct {
		suite.Suite
	}
	MockValidatorImpl struct {
		result executionValidatorResult
	}
)

func (m MockValidatorImpl) validatorTag() string {
	return "mockvalidator"
}

func (m MockValidatorImpl) validate(execData *executionData) executionValidatorResult {
	return m.result
}

var SuccessValidator executionValidator = &MockValidatorImpl{result: executionValidatorResult{isValid: true}}
var FailureValidator executionValidator = &MockValidatorImpl{result: executionValidatorResult{
	isValid:               false,
	failureReasonTag:      "mockvalidatorcheck",
	failureReasonDetailed: "just some failure reason",
	error:                 nil,
}}

func TestShardValidatorTestSuite(t *testing.T) {
	suite.Run(t, new(ShardValidatorTestSuite))
}

func (s *ShardValidatorTestSuite) TestNoExecutions() {
	mockCtrl := gomock.NewController(s.T())
	defer mockCtrl.Finish()

	historyDB := persistence.NewMockHistoryManager(mockCtrl)
	execMgrProvider := persistence.NewMockExecutionManagerFactory(mockCtrl)
	rateLimiter := quotas.NewMockRateLimiter(mockCtrl)
	var validators []executionValidator
	testObject := createShardValidator(execMgrProvider, rateLimiter, executionsPageSize, historyDB, validators)

	execMgr := persistence.NewMockExecutionManager(mockCtrl)
	execMgrProvider.EXPECT().NewExecutionManager(int32(42)).Return(execMgr, nil)
	rateLimiter.EXPECT().Wait(gomock.Any())

	execMgr.EXPECT().ListConcreteExecutions(gomock.Any()).Return(&persistence.ListConcreteExecutionsResponse{}, nil)

	shardID := int32(42)
	result := testObject.validate(shardID)
	s.Equal(shardID, result.ShardID)
	s.False(result.IsFailure)
}

func (s *ShardValidatorTestSuite) TestNoValidators() {
	mockCtrl := gomock.NewController(s.T())
	defer mockCtrl.Finish()

	historyDB := persistence.NewMockHistoryManager(mockCtrl)
	execMgrProvider := persistence.NewMockExecutionManagerFactory(mockCtrl)
	rateLimiter := quotas.NewMockRateLimiter(mockCtrl)
	var validators []executionValidator
	testObject := createShardValidator(execMgrProvider, rateLimiter, executionsPageSize, historyDB, validators)

	execMgr := persistence.NewMockExecutionManager(mockCtrl)
	execMgrProvider.EXPECT().NewExecutionManager(int32(42)).Return(execMgr, nil)
	rateLimiter.EXPECT().Wait(gomock.Any()).Times(2)
	historyBranchBlob, _ := serialization.HistoryBranchToBlob(&persistencespb.HistoryBranch{
		TreeId:    "treeid",
		BranchId:  "branchid",
		Ancestors: nil,
	})

	execMgr.EXPECT().ListConcreteExecutions(gomock.Any()).Return(&persistence.ListConcreteExecutionsResponse{
		States: []*persistencespb.WorkflowMutableState{&persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				VersionHistories: &historyspb.VersionHistories{
					CurrentVersionHistoryIndex: 0,
					Histories: []*historyspb.VersionHistory{&historyspb.VersionHistory{
						BranchToken: historyBranchBlob.Data,
						Items:       nil,
					}},
				},
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{},
		}},
	}, nil)

	historyDB.EXPECT().ReadHistoryBranch(gomock.Any()).Return(&persistence.ReadHistoryBranchResponse{
		HistoryEvents: []*historypb.HistoryEvent{&historypb.HistoryEvent{}},
	}, nil)

	shardID := int32(42)
	result := testObject.validate(shardID)
	s.Equal(shardID, result.ShardID)
	s.False(result.IsFailure)
}

func (s *ShardValidatorTestSuite) TestSucessValidator() {
	mockCtrl := gomock.NewController(s.T())
	defer mockCtrl.Finish()

	historyDB := persistence.NewMockHistoryManager(mockCtrl)
	execMgrProvider := persistence.NewMockExecutionManagerFactory(mockCtrl)
	rateLimiter := quotas.NewMockRateLimiter(mockCtrl)
	var validators = []executionValidator{SuccessValidator}
	testObject := createShardValidator(execMgrProvider, rateLimiter, executionsPageSize, historyDB, validators)

	execMgr := persistence.NewMockExecutionManager(mockCtrl)
	execMgrProvider.EXPECT().NewExecutionManager(int32(42)).Return(execMgr, nil)
	rateLimiter.EXPECT().Wait(gomock.Any()).Times(2)
	historyBranchBlob, _ := serialization.HistoryBranchToBlob(&persistencespb.HistoryBranch{
		TreeId:    "treeid",
		BranchId:  "branchid",
		Ancestors: nil,
	})

	execMgr.EXPECT().ListConcreteExecutions(gomock.Any()).Return(&persistence.ListConcreteExecutionsResponse{
		States: []*persistencespb.WorkflowMutableState{&persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				VersionHistories: &historyspb.VersionHistories{
					CurrentVersionHistoryIndex: 0,
					Histories: []*historyspb.VersionHistory{&historyspb.VersionHistory{
						BranchToken: historyBranchBlob.Data,
						Items:       nil,
					}},
				},
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{},
		}},
	}, nil)

	historyDB.EXPECT().ReadHistoryBranch(gomock.Any()).Return(&persistence.ReadHistoryBranchResponse{
		HistoryEvents: []*historypb.HistoryEvent{&historypb.HistoryEvent{}},
	}, nil)

	shardID := int32(42)
	result := testObject.validate(shardID)
	s.Equal(shardID, result.ShardID)
	s.False(result.IsFailure)
}

func (s *ShardValidatorTestSuite) TestSucessAndFailureValidator() {
	mockCtrl := gomock.NewController(s.T())
	defer mockCtrl.Finish()

	historyDB := persistence.NewMockHistoryManager(mockCtrl)
	execMgrProvider := persistence.NewMockExecutionManagerFactory(mockCtrl)
	rateLimiter := quotas.NewMockRateLimiter(mockCtrl)
	var validators = []executionValidator{SuccessValidator, FailureValidator}
	testObject := createShardValidator(execMgrProvider, rateLimiter, executionsPageSize, historyDB, validators)

	execMgr := persistence.NewMockExecutionManager(mockCtrl)
	execMgrProvider.EXPECT().NewExecutionManager(int32(42)).Return(execMgr, nil)
	rateLimiter.EXPECT().Wait(gomock.Any()).Times(2)
	historyBranchBlob, _ := serialization.HistoryBranchToBlob(&persistencespb.HistoryBranch{
		TreeId:    "treeid",
		BranchId:  "branchid",
		Ancestors: nil,
	})

	execMgr.EXPECT().ListConcreteExecutions(gomock.Any()).Return(&persistence.ListConcreteExecutionsResponse{
		States: []*persistencespb.WorkflowMutableState{&persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				VersionHistories: &historyspb.VersionHistories{
					CurrentVersionHistoryIndex: 0,
					Histories: []*historyspb.VersionHistory{&historyspb.VersionHistory{
						BranchToken: historyBranchBlob.Data,
						Items:       nil,
					}},
				},
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{},
		}},
	}, nil)

	historyDB.EXPECT().ReadHistoryBranch(gomock.Any()).Return(&persistence.ReadHistoryBranchResponse{
		HistoryEvents: []*historypb.HistoryEvent{&historypb.HistoryEvent{}},
	}, nil)

	shardID := int32(42)
	result := testObject.validate(shardID)
	s.Equal(shardID, result.ShardID)
	s.True(result.IsFailure)
	s.Equal(1, len(result.ExecutionFailures))
}
