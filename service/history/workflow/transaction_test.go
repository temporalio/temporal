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

package workflow

import (
	"errors"

	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
)

type (
	transactionSuite struct {
		suite.Suite
		*require.Assertions

		controller         *gomock.Controller
		mockShard          *shard.MockContext
		mockEngine         *shard.MockEngine
		mockNamespaceCache *namespace.MockRegistry

		logger log.Logger

		transaction *TransactionImpl
	}
)

func TestTransactionSuite(t *testing.T) {
	s := new(transactionSuite)
	suite.Run(t, s)
}

func (s *transactionSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockShard = shard.NewMockContext(s.controller)
	s.mockEngine = shard.NewMockEngine(s.controller)
	s.mockNamespaceCache = namespace.NewMockRegistry(s.controller)
	s.logger = log.NewTestLogger()

	s.mockShard.EXPECT().GetShardID().Return(int32(1)).AnyTimes()
	s.mockShard.EXPECT().GetEngine().Return(s.mockEngine, nil).AnyTimes()
	s.mockShard.EXPECT().GetNamespaceRegistry().Return(s.mockNamespaceCache).AnyTimes()
	s.mockShard.EXPECT().GetLogger().Return(s.logger).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()

	s.transaction = NewTransaction(s.mockShard)
}

func (s *transactionSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *transactionSuite) TestOperationMayApplied() {
	testCases := []struct {
		err        error
		mayApplied bool
	}{
		{err: &persistence.CurrentWorkflowConditionFailedError{}, mayApplied: false},
		{err: &persistence.WorkflowConditionFailedError{}, mayApplied: false},
		{err: &persistence.ConditionFailedError{}, mayApplied: false},
		{err: &persistence.ShardOwnershipLostError{}, mayApplied: false},
		{err: &persistence.InvalidPersistenceRequestError{}, mayApplied: false},
		{err: &persistence.TransactionSizeLimitError{}, mayApplied: false},
		{err: &serviceerror.ResourceExhausted{}, mayApplied: false},
		{err: &serviceerror.NotFound{}, mayApplied: false},
		{err: nil, mayApplied: true},
		{err: &persistence.TimeoutError{}, mayApplied: true},
		{err: &serviceerror.Unavailable{}, mayApplied: true},
		{err: errors.New("some unknown error"), mayApplied: true},
	}

	for _, tc := range testCases {
		s.Equal(tc.mayApplied, operationMayApplied(tc.err))
	}
}

func (s *transactionSuite) TestCreateWorkflowExecution_NotifyTaskWhenFailed() {
	timeoutErr := &persistence.TimeoutError{}
	s.True(operationMayApplied(timeoutErr))

	s.mockShard.EXPECT().CreateWorkflowExecution(gomock.Any()).Return(nil, timeoutErr)
	s.setupMockForTaskNotification()

	_, err := s.transaction.CreateWorkflowExecution(
		persistence.CreateWorkflowModeBrandNew,
		&persistence.WorkflowSnapshot{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				NamespaceId: tests.NamespaceID.String(),
				WorkflowId:  tests.WorkflowID,
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId: tests.RunID,
			},
		},
		[]*persistence.WorkflowEvents{},
	)
	s.Equal(timeoutErr, err)
}

func (s *transactionSuite) TestUpdateWorkflowExecution_NotifyTaskWhenFailed() {
	timeoutErr := &persistence.TimeoutError{}
	s.True(operationMayApplied(timeoutErr))

	s.mockShard.EXPECT().UpdateWorkflowExecution(gomock.Any()).Return(nil, timeoutErr)
	s.setupMockForTaskNotification() // for current workflow mutation
	s.setupMockForTaskNotification() // for new workflow snapshot

	_, _, err := s.transaction.UpdateWorkflowExecution(
		persistence.UpdateWorkflowModeUpdateCurrent,
		&persistence.WorkflowMutation{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				NamespaceId: tests.NamespaceID.String(),
				WorkflowId:  tests.WorkflowID,
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId: tests.RunID,
			},
		},
		[]*persistence.WorkflowEvents{},
		&persistence.WorkflowSnapshot{},
		[]*persistence.WorkflowEvents{},
	)
	s.Equal(timeoutErr, err)
}

func (s *transactionSuite) TestConflictResolveWorkflowExecution_NotifyTaskWhenFailed() {
	timeoutErr := &persistence.TimeoutError{}
	s.True(operationMayApplied(timeoutErr))

	s.mockShard.EXPECT().ConflictResolveWorkflowExecution(gomock.Any()).Return(nil, timeoutErr)
	s.setupMockForTaskNotification() // for reset workflow snapshot
	s.setupMockForTaskNotification() // for new workflow snapshot
	s.setupMockForTaskNotification() // for current workflow mutation

	_, _, _, err := s.transaction.ConflictResolveWorkflowExecution(
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		&persistence.WorkflowSnapshot{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				NamespaceId: tests.NamespaceID.String(),
				WorkflowId:  tests.WorkflowID,
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId: tests.RunID,
			},
		},
		[]*persistence.WorkflowEvents{},
		&persistence.WorkflowSnapshot{},
		[]*persistence.WorkflowEvents{},
		&persistence.WorkflowMutation{},
		[]*persistence.WorkflowEvents{},
	)
	s.Equal(timeoutErr, err)
}

func (s *transactionSuite) setupMockForTaskNotification() {
	s.mockEngine.EXPECT().NotifyNewTransferTasks(gomock.Any(), gomock.Any()).Times(1)
	s.mockEngine.EXPECT().NotifyNewTimerTasks(gomock.Any(), gomock.Any()).Times(1)
	s.mockEngine.EXPECT().NotifyNewVisibilityTasks(gomock.Any()).Times(1)
	s.mockEngine.EXPECT().NotifyNewReplicationTasks(gomock.Any()).Times(1)
}
