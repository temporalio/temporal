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

package common

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/codec"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
)

const (
	domainID   = "test-domain-id"
	workflowID = "test-workflow-id"
	runID      = "test-run-id"
	treeID     = "test-tree-id"
	branchID   = "test-branch-id"
)

var (
	validBranchToken   = []byte{89, 11, 0, 10, 0, 0, 0, 12, 116, 101, 115, 116, 45, 116, 114, 101, 101, 45, 105, 100, 11, 0, 20, 0, 0, 0, 14, 116, 101, 115, 116, 45, 98, 114, 97, 110, 99, 104, 45, 105, 100, 0}
	invalidBranchToken = []byte("invalid")
)

type UtilSuite struct {
	*require.Assertions
	suite.Suite
}

func TestUtilSuite(t *testing.T) {
	suite.Run(t, new(UtilSuite))
}

func (s *UtilSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *UtilSuite) TestValidateExecution() {
	testCases := []struct {
		execution   *ConcreteExecution
		expectError bool
	}{
		{
			execution:   &ConcreteExecution{},
			expectError: true,
		},
		{
			execution: &ConcreteExecution{
				Execution: Execution{
					ShardID: -1,
				},
			},
			expectError: true,
		},
		{
			execution: &ConcreteExecution{
				Execution: Execution{
					ShardID: 0,
				},
			},
			expectError: true,
		},
		{
			execution: &ConcreteExecution{
				Execution: Execution{
					ShardID:  0,
					DomainID: domainID,
				},
			},
			expectError: true,
		},
		{
			execution: &ConcreteExecution{
				Execution: Execution{
					ShardID:    0,
					DomainID:   domainID,
					WorkflowID: workflowID,
				},
			},
			expectError: true,
		},
		{
			execution: &ConcreteExecution{
				Execution: Execution{
					ShardID:    0,
					DomainID:   domainID,
					WorkflowID: workflowID,
					RunID:      runID,
				},
			},
			expectError: true,
		},
		{
			execution: &ConcreteExecution{
				Execution: Execution{
					ShardID:    0,
					DomainID:   domainID,
					WorkflowID: workflowID,
					RunID:      runID,
				},
				BranchToken: []byte{1, 2, 3},
			},
			expectError: true,
		},
		{
			execution: &ConcreteExecution{
				Execution: Execution{
					ShardID:    0,
					DomainID:   domainID,
					WorkflowID: workflowID,
					RunID:      runID,
				},
				BranchToken: []byte{1, 2, 3},
				TreeID:      treeID,
			},
			expectError: true,
		},
		{
			execution: &ConcreteExecution{
				Execution: Execution{
					ShardID:    0,
					DomainID:   domainID,
					WorkflowID: workflowID,
					RunID:      runID,
					State:      persistence.WorkflowStateCreated - 1,
				},
				BranchToken: []byte{1, 2, 3},
				TreeID:      treeID,
				BranchID:    branchID,
			},
			expectError: true,
		},
		{
			execution: &ConcreteExecution{
				Execution: Execution{
					ShardID:    0,
					DomainID:   domainID,
					WorkflowID: workflowID,
					RunID:      runID,
					State:      persistence.WorkflowStateCorrupted + 1,
				},
				BranchToken: []byte{1, 2, 3},
				TreeID:      treeID,
				BranchID:    branchID,
			},
			expectError: true,
		},
		{
			execution: &ConcreteExecution{
				Execution: Execution{
					ShardID:    0,
					DomainID:   domainID,
					WorkflowID: workflowID,
					RunID:      runID,
					State:      persistence.WorkflowStateCreated,
				},
				BranchToken: []byte{1, 2, 3},
				TreeID:      treeID,
				BranchID:    branchID,
			},
			expectError: false,
		},
	}

	for _, tc := range testCases {
		err := ValidateConcreteExecution(tc.execution)
		if tc.expectError {
			s.Error(err)
		} else {
			s.NoError(err)
		}
	}
}

func (s *UtilSuite) TestGetBranchToken() {
	encoder := codec.NewThriftRWEncoder()
	testCases := []struct {
		entity      *persistence.ListConcreteExecutionsEntity
		expectError bool
		branchToken []byte
		treeID      string
		branchID    string
	}{
		{
			entity: &persistence.ListConcreteExecutionsEntity{
				ExecutionInfo: &persistence.WorkflowExecutionInfo{
					BranchToken: s.getValidBranchToken(encoder),
				},
			},
			expectError: false,
			branchToken: validBranchToken,
			treeID:      treeID,
			branchID:    branchID,
		},
		{
			entity: &persistence.ListConcreteExecutionsEntity{
				ExecutionInfo: &persistence.WorkflowExecutionInfo{
					BranchToken: invalidBranchToken,
				},
				VersionHistories: &persistence.VersionHistories{
					CurrentVersionHistoryIndex: 1,
					Histories: []*persistence.VersionHistory{
						{
							BranchToken: invalidBranchToken,
						},
						{
							BranchToken: validBranchToken,
						},
					},
				},
			},
			expectError: false,
			branchToken: validBranchToken,
			treeID:      treeID,
			branchID:    branchID,
		},
		{
			entity: &persistence.ListConcreteExecutionsEntity{
				ExecutionInfo: &persistence.WorkflowExecutionInfo{
					BranchToken: invalidBranchToken,
				},
				VersionHistories: &persistence.VersionHistories{
					CurrentVersionHistoryIndex: 1,
					Histories: []*persistence.VersionHistory{
						{
							BranchToken: validBranchToken,
						},
						{
							BranchToken: invalidBranchToken,
						},
					},
				},
			},
			expectError: true,
		},
		{
			entity: &persistence.ListConcreteExecutionsEntity{
				ExecutionInfo: &persistence.WorkflowExecutionInfo{
					BranchToken: invalidBranchToken,
				},
				VersionHistories: &persistence.VersionHistories{
					CurrentVersionHistoryIndex: 0,
					Histories:                  []*persistence.VersionHistory{},
				},
			},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		branchToken, treeID, branchID, err := GetBranchToken(tc.entity, encoder)
		if tc.expectError {
			s.Error(err)
			s.Nil(branchToken)
			s.Empty(treeID)
			s.Empty(branchID)
		} else {
			s.NoError(err)
			s.Equal(tc.branchToken, branchToken)
			s.Equal(tc.treeID, treeID)
			s.Equal(tc.branchID, branchID)
		}
	}
}

func (s *UtilSuite) TestExecutionStillOpen() {
	testCases := []struct {
		getExecResp *persistence.GetWorkflowExecutionResponse
		getExecErr  error
		expectError bool
		expectOpen  bool
	}{
		{
			getExecResp: nil,
			getExecErr:  &shared.EntityNotExistsError{},
			expectError: false,
			expectOpen:  false,
		},
		{
			getExecResp: nil,
			getExecErr:  errors.New("got error"),
			expectError: true,
			expectOpen:  false,
		},
		{
			getExecResp: &persistence.GetWorkflowExecutionResponse{
				State: &persistence.WorkflowMutableState{
					ExecutionInfo: &persistence.WorkflowExecutionInfo{
						State: persistence.WorkflowStateCompleted,
					},
				},
			},
			getExecErr:  nil,
			expectError: false,
			expectOpen:  false,
		},
		{
			getExecResp: &persistence.GetWorkflowExecutionResponse{
				State: &persistence.WorkflowMutableState{
					ExecutionInfo: &persistence.WorkflowExecutionInfo{
						State: persistence.WorkflowStateCreated,
					},
				},
			},
			getExecErr:  nil,
			expectError: false,
			expectOpen:  true,
		},
	}

	for _, tc := range testCases {
		execManager := &mocks.ExecutionManager{}
		execManager.On("GetWorkflowExecution", mock.Anything).Return(tc.getExecResp, tc.getExecErr)
		pr := NewPersistenceRetryer(execManager, nil)
		open, err := ExecutionStillOpen(&Execution{}, pr)
		if tc.expectError {
			s.Error(err)
		} else {
			s.NoError(err)
		}
		if tc.expectOpen {
			s.True(open)
		} else {
			s.False(open)
		}
	}
}

func (s *UtilSuite) TestExecutionStillExists() {
	testCases := []struct {
		getExecResp  *persistence.GetWorkflowExecutionResponse
		getExecErr   error
		expectError  bool
		expectExists bool
	}{
		{
			getExecResp:  &persistence.GetWorkflowExecutionResponse{},
			getExecErr:   nil,
			expectError:  false,
			expectExists: true,
		},
		{
			getExecResp:  nil,
			getExecErr:   &shared.EntityNotExistsError{},
			expectError:  false,
			expectExists: false,
		},
		{
			getExecResp:  nil,
			getExecErr:   errors.New("got error"),
			expectError:  true,
			expectExists: false,
		},
	}

	for _, tc := range testCases {
		execManager := &mocks.ExecutionManager{}
		execManager.On("GetWorkflowExecution", mock.Anything).Return(tc.getExecResp, tc.getExecErr)
		pr := NewPersistenceRetryer(execManager, nil)
		exists, err := ExecutionStillExists(&Execution{}, pr)
		if tc.expectError {
			s.Error(err)
		} else {
			s.NoError(err)
		}
		if tc.expectExists {
			s.True(exists)
		} else {
			s.False(exists)
		}
	}
}

func (s *UtilSuite) TestDeleteExecution() {
	testCases := []struct {
		deleteConcreteErr error
		deleteCurrentErr  error
		expectedFixResult *FixResult
	}{
		{
			deleteConcreteErr: errors.New("error deleting concrete execution"),
			expectedFixResult: &FixResult{
				FixResultType: FixResultTypeFailed,
				Info:          "failed to delete concrete workflow execution",
				InfoDetails:   "error deleting concrete execution",
			},
		},
		{
			deleteCurrentErr: errors.New("error deleting current execution"),
			expectedFixResult: &FixResult{
				FixResultType: FixResultTypeFailed,
				Info:          "failed to delete current workflow execution",
				InfoDetails:   "error deleting current execution",
			},
		},
		{
			expectedFixResult: &FixResult{
				FixResultType: FixResultTypeFixed,
			},
		},
	}

	for _, tc := range testCases {
		execManager := &mocks.ExecutionManager{}
		execManager.On("DeleteWorkflowExecution", mock.Anything).Return(tc.deleteConcreteErr).Once()
		if tc.deleteConcreteErr == nil {
			execManager.On("DeleteCurrentWorkflowExecution", mock.Anything).Return(tc.deleteCurrentErr).Once()
		}
		pr := NewPersistenceRetryer(execManager, nil)
		result := DeleteExecution(&ConcreteExecution{}, pr)
		s.Equal(tc.expectedFixResult, result)
	}
}

func (s *UtilSuite) getValidBranchToken(encoder *codec.ThriftRWEncoder) []byte {
	hb := &shared.HistoryBranch{
		TreeID:   common.StringPtr(treeID),
		BranchID: common.StringPtr(branchID),
	}
	bytes, err := encoder.Encode(hb)
	s.NoError(err)
	return bytes
}
