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
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/workflow"
)

type (
	nDCWorkflowSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockNamespaceCache  *namespace.MockRegistry
		mockContext         *workflow.MockContext
		mockMutableState    *workflow.MockMutableState
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
	s.mockNamespaceCache = namespace.NewMockRegistry(s.controller)
	s.mockContext = workflow.NewMockContext(s.controller)
	s.mockMutableState = workflow.NewMockMutableState(s.controller)
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
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:     s.namespaceID,
		WorkflowId:      s.workflowID,
		LastEventTaskId: lastEventTaskID,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: s.runID,
	}).AnyTimes()

	nDCWorkflow := newNDCWorkflow(
		context.Background(),
		s.mockNamespaceCache,
		s.mockClusterMetadata,
		s.mockContext,
		s.mockMutableState,
		workflow.NoopReleaseFn,
	)

	s.Equal(s.mockContext, nDCWorkflow.getContext())
	s.Equal(s.mockMutableState, nDCWorkflow.getMutableState())
	// NOTE golang does not seem to let people compare functions, easily
	//  link: https://github.com/stretchr/testify/issues/182
	// this is a hack to compare 2 functions, being the same
	expectedReleaseFn := runtime.FuncForPC(reflect.ValueOf(workflow.NoopReleaseFn).Pointer()).Name()
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

	s.True(WorkflowHappensAfter(
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

	s.False(WorkflowHappensAfter(
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

	s.False(WorkflowHappensAfter(
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

	s.True(WorkflowHappensAfter(
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
		workflow.NoopReleaseFn,
	)

	incomingMockContext := workflow.NewMockContext(s.controller)
	incomingMockMutableState := workflow.NewMockMutableState(s.controller)
	incomingNDCWorkflow := newNDCWorkflow(
		context.Background(),
		s.mockNamespaceCache,
		s.mockClusterMetadata,
		incomingMockContext,
		incomingMockMutableState,
		workflow.NoopReleaseFn,
	)

	// cannot suppress by older workflow
	lastEventTaskID := int64(144)
	lastEventVersion := int64(12)
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(lastEventVersion, nil).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:     s.namespaceID,
		WorkflowId:      s.workflowID,
		LastEventTaskId: lastEventTaskID,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: s.runID,
	}).AnyTimes()

	incomingRunID := uuid.New()
	incomingLastEventTaskID := int64(144)
	incomingLastEventVersion := lastEventVersion - 1
	incomingMockMutableState.EXPECT().GetLastWriteVersion().Return(incomingLastEventVersion, nil).AnyTimes()
	incomingMockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:     s.namespaceID,
		WorkflowId:      s.workflowID,
		LastEventTaskId: incomingLastEventTaskID,
	}).AnyTimes()
	incomingMockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: incomingRunID,
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
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:     s.namespaceID,
		WorkflowId:      s.workflowID,
		LastEventTaskId: lastEventTaskID,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: s.runID,
	}).AnyTimes()
	nDCWorkflow := newNDCWorkflow(
		context.Background(),
		s.mockNamespaceCache,
		s.mockClusterMetadata,
		s.mockContext,
		s.mockMutableState,
		workflow.NoopReleaseFn,
	)

	incomingRunID := uuid.New()
	incomingLastEventTaskID := int64(144)
	incomingLastEventVersion := lastEventVersion + 1
	incomingMockContext := workflow.NewMockContext(s.controller)
	incomingMockMutableState := workflow.NewMockMutableState(s.controller)
	incomingNDCWorkflow := newNDCWorkflow(
		context.Background(),
		s.mockNamespaceCache,
		s.mockClusterMetadata,
		incomingMockContext,
		incomingMockMutableState,
		workflow.NoopReleaseFn,
	)
	incomingMockMutableState.EXPECT().GetLastWriteVersion().Return(incomingLastEventVersion, nil).AnyTimes()
	incomingMockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:     s.namespaceID,
		WorkflowId:      s.workflowID,
		LastEventTaskId: incomingLastEventTaskID,
	}).AnyTimes()
	incomingMockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: incomingRunID,
	}).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, lastEventVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	s.mockMutableState.EXPECT().UpdateCurrentVersion(lastEventVersion, true).Return(nil).AnyTimes()
	inFlightWorkflowTask := &workflow.WorkflowTaskInfo{
		Version:          1234,
		ScheduledEventID: 5678,
		StartedEventID:   9012,
	}
	s.mockMutableState.EXPECT().GetInFlightWorkflowTask().Return(inFlightWorkflowTask, true)
	s.mockMutableState.EXPECT().AddWorkflowTaskFailedEvent(
		inFlightWorkflowTask.ScheduledEventID,
		inFlightWorkflowTask.StartedEventID,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_FAILOVER_CLOSE_COMMAND,
		nil,
		consts.IdentityHistoryService,
		"",
		"",
		"",
		int64(0),
	).Return(&historypb.HistoryEvent{}, nil)
	s.mockMutableState.EXPECT().FlushBufferedEvents()

	s.mockMutableState.EXPECT().AddWorkflowExecutionTerminatedEvent(
		lastEventID+1, workflowTerminationReason, gomock.Any(), workflowTerminationIdentity, false,
	).Return(&historypb.HistoryEvent{}, nil)

	// if workflow is in zombie or finished state, keep as is
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false)
	policy, err := nDCWorkflow.suppressBy(incomingNDCWorkflow)
	s.NoError(err)
	s.Equal(workflow.TransactionPolicyPassive, policy)

	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	policy, err = nDCWorkflow.suppressBy(incomingNDCWorkflow)
	s.NoError(err)
	s.Equal(workflow.TransactionPolicyActive, policy)
}

func (s *nDCWorkflowSuite) TestSuppressWorkflowBy_Zombiefy() {
	lastEventTaskID := int64(144)
	lastEventVersion := int64(12)
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(lastEventVersion, nil).AnyTimes()
	executionInfo := &persistencespb.WorkflowExecutionInfo{
		NamespaceId:     s.namespaceID,
		WorkflowId:      s.workflowID,
		LastEventTaskId: lastEventTaskID,
	}
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	executionState := &persistencespb.WorkflowExecutionState{
		RunId: s.runID,
	}
	s.mockMutableState.EXPECT().UpdateWorkflowStateStatus(enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING).
		DoAndReturn(func(state enumsspb.WorkflowExecutionState, status enumspb.WorkflowExecutionStatus) error {
			executionState.State, executionState.Status = state, status
			return nil
		}).AnyTimes()

	nDCWorkflow := newNDCWorkflow(
		context.Background(),
		s.mockNamespaceCache,
		s.mockClusterMetadata,
		s.mockContext,
		s.mockMutableState,
		workflow.NoopReleaseFn,
	)

	incomingRunID := uuid.New()
	incomingLastEventTaskID := int64(144)
	incomingLastEventVersion := lastEventVersion + 1
	incomingMockContext := workflow.NewMockContext(s.controller)
	incomingMockMutableState := workflow.NewMockMutableState(s.controller)
	incomingNDCWorkflow := newNDCWorkflow(
		context.Background(),
		s.mockNamespaceCache,
		s.mockClusterMetadata,
		incomingMockContext,
		incomingMockMutableState,
		workflow.NoopReleaseFn,
	)
	incomingMockMutableState.EXPECT().GetLastWriteVersion().Return(incomingLastEventVersion, nil).AnyTimes()
	incomingMockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:     s.namespaceID,
		WorkflowId:      s.workflowID,
		LastEventTaskId: incomingLastEventTaskID,
	}).AnyTimes()
	incomingMockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: incomingRunID,
	}).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, lastEventVersion).Return(cluster.TestAlternativeClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	// if workflow is in zombie or finished state, keep as is
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false)
	policy, err := nDCWorkflow.suppressBy(incomingNDCWorkflow)
	s.NoError(err)
	s.Equal(workflow.TransactionPolicyPassive, policy)

	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	policy, err = nDCWorkflow.suppressBy(incomingNDCWorkflow)
	s.NoError(err)
	s.Equal(workflow.TransactionPolicyPassive, policy)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, executionState.State)
	s.EqualValues(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, executionState.Status)
}
