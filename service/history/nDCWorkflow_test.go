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

package history

import (
	"context"
	"reflect"
	"runtime"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/persistenceblobs/v1"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/persistence"
)

type (
	nDCWorkflowSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockNamespaceCache  *cache.MockNamespaceCache
		mockContext         *MockworkflowExecutionContext
		mockMutableState    *MockmutableState
		mockClusterMetadata *cluster.MockMetadata

		namespaceID string
		workflowID  string
		runID       string
	}
)

func TestNDCWorkflowSuite(t *testing.T) {
	s := new(nDCWorkflowSuite)
	suite.Run(t, s)
}

func (s *nDCWorkflowSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockNamespaceCache = cache.NewMockNamespaceCache(s.controller)
	s.mockContext = NewMockworkflowExecutionContext(s.controller)
	s.mockMutableState = NewMockmutableState(s.controller)
	s.mockClusterMetadata = cluster.NewMockMetadata(s.controller)
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	s.namespaceID = uuid.New()
	s.workflowID = "some random workflow ID"
	s.runID = uuid.New()
}

func (s *nDCWorkflowSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *nDCWorkflowSuite) TestGetMethods() {
	lastEventTaskID := int64(144)
	lastEventVersion := int64(12)
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(lastEventVersion, nil).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		NamespaceId:     s.namespaceID,
		WorkflowId:      s.workflowID,
		ExecutionState:  &persistenceblobs.WorkflowExecutionState{RunId: s.runID},
		LastEventTaskId: lastEventTaskID,
	}).AnyTimes()

	nDCWorkflow := newNDCWorkflow(
		context.Background(),
		s.mockNamespaceCache,
		s.mockClusterMetadata,
		s.mockContext,
		s.mockMutableState,
		noopReleaseFn,
	)

	s.Equal(s.mockContext, nDCWorkflow.getContext())
	s.Equal(s.mockMutableState, nDCWorkflow.getMutableState())
	// NOTE golang does not seem to let people compare functions, easily
	//  link: https://github.com/stretchr/testify/issues/182
	// this is a hack to compare 2 functions, being the same
	expectedReleaseFn := runtime.FuncForPC(reflect.ValueOf(noopReleaseFn).Pointer()).Name()
	actualReleaseFn := runtime.FuncForPC(reflect.ValueOf(nDCWorkflow.getReleaseFn()).Pointer()).Name()
	s.Equal(expectedReleaseFn, actualReleaseFn)
	version, taskID, err := nDCWorkflow.getVectorClock()
	s.NoError(err)
	s.Equal(lastEventVersion, version)
	s.Equal(lastEventTaskID, taskID)
}

func (s *nDCWorkflowSuite) TestHappensAfter_LargerVersion() {
	thisLastWriteVersion := int64(0)
	thisLastEventTaskID := int64(100)
	thatLastWriteVersion := thisLastWriteVersion - 1
	thatLastEventTaskID := int64(123)

	s.True(workflowHappensAfter(
		thisLastWriteVersion,
		thisLastEventTaskID,
		thatLastWriteVersion,
		thatLastEventTaskID,
	))
}

func (s *nDCWorkflowSuite) TestHappensAfter_SmallerVersion() {
	thisLastWriteVersion := int64(0)
	thisLastEventTaskID := int64(100)
	thatLastWriteVersion := thisLastWriteVersion + 1
	thatLastEventTaskID := int64(23)

	s.False(workflowHappensAfter(
		thisLastWriteVersion,
		thisLastEventTaskID,
		thatLastWriteVersion,
		thatLastEventTaskID,
	))
}

func (s *nDCWorkflowSuite) TestHappensAfter_SameVersion_SmallerTaskID() {
	thisLastWriteVersion := int64(0)
	thisLastEventTaskID := int64(100)
	thatLastWriteVersion := thisLastWriteVersion
	thatLastEventTaskID := thisLastEventTaskID + 1

	s.False(workflowHappensAfter(
		thisLastWriteVersion,
		thisLastEventTaskID,
		thatLastWriteVersion,
		thatLastEventTaskID,
	))
}

func (s *nDCWorkflowSuite) TestHappensAfter_SameVersion_LatrgerTaskID() {
	thisLastWriteVersion := int64(0)
	thisLastEventTaskID := int64(100)
	thatLastWriteVersion := thisLastWriteVersion
	thatLastEventTaskID := thisLastEventTaskID - 1

	s.True(workflowHappensAfter(
		thisLastWriteVersion,
		thisLastEventTaskID,
		thatLastWriteVersion,
		thatLastEventTaskID,
	))
}

func (s *nDCWorkflowSuite) TestSuppressWorkflowBy_Error() {
	nDCWorkflow := newNDCWorkflow(
		context.Background(),
		s.mockNamespaceCache,
		s.mockClusterMetadata,
		s.mockContext,
		s.mockMutableState,
		noopReleaseFn,
	)

	incomingMockContext := NewMockworkflowExecutionContext(s.controller)
	incomingMockMutableState := NewMockmutableState(s.controller)
	incomingNDCWorkflow := newNDCWorkflow(
		context.Background(),
		s.mockNamespaceCache,
		s.mockClusterMetadata,
		incomingMockContext,
		incomingMockMutableState,
		noopReleaseFn,
	)

	// cannot suppress by older workflow
	lastEventTaskID := int64(144)
	lastEventVersion := int64(12)
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(lastEventVersion, nil).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		NamespaceId:     s.namespaceID,
		WorkflowId:      s.workflowID,
		ExecutionState:  &persistenceblobs.WorkflowExecutionState{RunId: s.runID},
		LastEventTaskId: lastEventTaskID,
	}).AnyTimes()

	incomingRunID := uuid.New()
	incomingLastEventTaskID := int64(144)
	incomingLastEventVersion := lastEventVersion - 1
	incomingMockMutableState.EXPECT().GetLastWriteVersion().Return(incomingLastEventVersion, nil).AnyTimes()
	incomingMockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		NamespaceId:     s.namespaceID,
		WorkflowId:      s.workflowID,
		ExecutionState:  &persistenceblobs.WorkflowExecutionState{RunId: incomingRunID},
		LastEventTaskId: incomingLastEventTaskID,
	}).AnyTimes()

	_, err := nDCWorkflow.suppressBy(incomingNDCWorkflow)
	s.Error(err)
}

func (s *nDCWorkflowSuite) TestSuppressWorkflowBy_Terminate() {
	lastEventID := int64(2)
	lastEventTaskID := int64(144)
	lastEventVersion := int64(12)
	s.mockMutableState.EXPECT().GetNextEventID().Return(lastEventID + 1).AnyTimes()
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(lastEventVersion, nil).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		NamespaceId:     s.namespaceID,
		WorkflowId:      s.workflowID,
		ExecutionState:  &persistenceblobs.WorkflowExecutionState{RunId: s.runID},
		LastEventTaskId: lastEventTaskID,
	}).AnyTimes()
	nDCWorkflow := newNDCWorkflow(
		context.Background(),
		s.mockNamespaceCache,
		s.mockClusterMetadata,
		s.mockContext,
		s.mockMutableState,
		noopReleaseFn,
	)

	incomingRunID := uuid.New()
	incomingLastEventTaskID := int64(144)
	incomingLastEventVersion := lastEventVersion + 1
	incomingMockContext := NewMockworkflowExecutionContext(s.controller)
	incomingMockMutableState := NewMockmutableState(s.controller)
	incomingNDCWorkflow := newNDCWorkflow(
		context.Background(),
		s.mockNamespaceCache,
		s.mockClusterMetadata,
		incomingMockContext,
		incomingMockMutableState,
		noopReleaseFn,
	)
	incomingMockMutableState.EXPECT().GetLastWriteVersion().Return(incomingLastEventVersion, nil).AnyTimes()
	incomingMockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		NamespaceId:     s.namespaceID,
		WorkflowId:      s.workflowID,
		ExecutionState:  &persistenceblobs.WorkflowExecutionState{RunId: incomingRunID},
		LastEventTaskId: incomingLastEventTaskID,
	}).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(lastEventVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	s.mockMutableState.EXPECT().UpdateCurrentVersion(lastEventVersion, true).Return(nil).AnyTimes()
	inFlightWorkflowTask := &workflowTaskInfo{
		Version:    1234,
		ScheduleID: 5678,
		StartedID:  9012,
	}
	s.mockMutableState.EXPECT().GetInFlightWorkflowTask().Return(inFlightWorkflowTask, true).Times(1)
	s.mockMutableState.EXPECT().AddWorkflowTaskFailedEvent(
		inFlightWorkflowTask.ScheduleID,
		inFlightWorkflowTask.StartedID,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_FAILOVER_CLOSE_COMMAND,
		nil,
		identityHistoryService,
		"",
		"",
		"",
		int64(0),
	).Return(&historypb.HistoryEvent{}, nil).Times(1)
	s.mockMutableState.EXPECT().FlushBufferedEvents().Return(nil).Times(1)

	s.mockMutableState.EXPECT().AddWorkflowExecutionTerminatedEvent(
		lastEventID+1, workflowTerminationReason, gomock.Any(), workflowTerminationIdentity,
	).Return(&historypb.HistoryEvent{}, nil).Times(1)

	// if workflow is in zombie or finished state, keep as is
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).Times(1)
	policy, err := nDCWorkflow.suppressBy(incomingNDCWorkflow)
	s.NoError(err)
	s.Equal(transactionPolicyPassive, policy)

	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).Times(1)
	policy, err = nDCWorkflow.suppressBy(incomingNDCWorkflow)
	s.NoError(err)
	s.Equal(transactionPolicyActive, policy)
}

func (s *nDCWorkflowSuite) TestSuppressWorkflowBy_Zombiefy() {
	lastEventTaskID := int64(144)
	lastEventVersion := int64(12)
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(lastEventVersion, nil).AnyTimes()
	executionInfo := &persistence.WorkflowExecutionInfo{
		NamespaceId:     s.namespaceID,
		WorkflowId:      s.workflowID,
		LastEventTaskId: lastEventTaskID,
		ExecutionState: &persistenceblobs.WorkflowExecutionState{
			RunId:  s.runID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
	}
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	nDCWorkflow := newNDCWorkflow(
		context.Background(),
		s.mockNamespaceCache,
		s.mockClusterMetadata,
		s.mockContext,
		s.mockMutableState,
		noopReleaseFn,
	)

	incomingRunID := uuid.New()
	incomingLastEventTaskID := int64(144)
	incomingLastEventVersion := lastEventVersion + 1
	incomingMockContext := NewMockworkflowExecutionContext(s.controller)
	incomingMockMutableState := NewMockmutableState(s.controller)
	incomingNDCWorkflow := newNDCWorkflow(
		context.Background(),
		s.mockNamespaceCache,
		s.mockClusterMetadata,
		incomingMockContext,
		incomingMockMutableState,
		noopReleaseFn,
	)
	incomingMockMutableState.EXPECT().GetLastWriteVersion().Return(incomingLastEventVersion, nil).AnyTimes()
	incomingMockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		NamespaceId:     s.namespaceID,
		WorkflowId:      s.workflowID,
		ExecutionState:  &persistenceblobs.WorkflowExecutionState{RunId: incomingRunID},
		LastEventTaskId: incomingLastEventTaskID,
	}).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(lastEventVersion).Return(cluster.TestAlternativeClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	// if workflow is in zombie or finished state, keep as is
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).Times(1)
	policy, err := nDCWorkflow.suppressBy(incomingNDCWorkflow)
	s.NoError(err)
	s.Equal(transactionPolicyPassive, policy)

	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).Times(1)
	policy, err = nDCWorkflow.suppressBy(incomingNDCWorkflow)
	s.NoError(err)
	s.Equal(transactionPolicyPassive, policy)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, executionInfo.ExecutionState.State)
	s.EqualValues(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, executionInfo.ExecutionState.Status)
}
