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

package signalwithstart

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
)

type (
	signalWithStartWorkflowSuite struct {
		suite.Suite
		*require.Assertions

		controller   *gomock.Controller
		shardContext *shard.MockContext

		namespaceID string
		workflowID  string

		currentContext      *workflow.MockContext
		currentMutableState *workflow.MockMutableState
		currentRunID        string
	}
)

func TestSignalWithStartWorkflowSuite(t *testing.T) {
	s := new(signalWithStartWorkflowSuite)
	suite.Run(t, s)
}

func (s *signalWithStartWorkflowSuite) SetupSuite() {
	rand.Seed(time.Now().UnixNano())
}

func (s *signalWithStartWorkflowSuite) TearDownSuite() {
}

func (s *signalWithStartWorkflowSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.shardContext = shard.NewMockContext(s.controller)

	s.namespaceID = uuid.New().String()
	s.workflowID = uuid.New().String()

	s.currentContext = workflow.NewMockContext(s.controller)
	s.currentMutableState = workflow.NewMockMutableState(s.controller)
	s.currentRunID = uuid.New().String()

	s.shardContext.EXPECT().GetConfig().Return(tests.NewDynamicConfig()).AnyTimes()
	s.shardContext.EXPECT().GetLogger().Return(log.NewTestLogger()).AnyTimes()
	s.shardContext.EXPECT().GetThrottledLogger().Return(log.NewTestLogger()).AnyTimes()
	s.shardContext.EXPECT().GetTimeSource().Return(clock.NewRealTimeSource()).AnyTimes()

	s.currentMutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry).AnyTimes()
	s.currentMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		WorkflowId: s.workflowID,
	}).AnyTimes()
	s.currentMutableState.EXPECT().GetExecutionState().Return(&persistence.WorkflowExecutionState{
		RunId: s.currentRunID,
	}).AnyTimes()
}

func (s *signalWithStartWorkflowSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *signalWithStartWorkflowSuite) TestSignalWorkflow_Dedup() {
	ctx := context.Background()
	currentWorkflowContext := api.NewWorkflowContext(
		s.currentContext,
		workflow.NoopReleaseFn,
		s.currentMutableState,
	)
	request := s.randomRequest()

	s.currentMutableState.EXPECT().IsSignalRequested(request.GetRequestId()).Return(true)

	err := signalWorkflow(
		ctx,
		s.shardContext,
		currentWorkflowContext,
		request,
	)
	s.NoError(err)
}

func (s *signalWithStartWorkflowSuite) TestSignalWorkflow_NewWorkflowTask() {
	ctx := context.Background()
	currentWorkflowContext := api.NewWorkflowContext(
		s.currentContext,
		workflow.NoopReleaseFn,
		s.currentMutableState,
	)
	request := s.randomRequest()

	s.currentMutableState.EXPECT().IsSignalRequested(request.GetRequestId()).Return(false)
	s.currentMutableState.EXPECT().AddSignalRequested(request.GetRequestId())
	s.currentMutableState.EXPECT().AddWorkflowExecutionSignaled(
		request.GetSignalName(),
		request.GetSignalInput(),
		request.GetIdentity(),
		request.GetHeader(),
	).Return(&history.HistoryEvent{}, nil)
	s.currentMutableState.EXPECT().HasPendingWorkflowTask().Return(false)
	s.currentMutableState.EXPECT().AddWorkflowTaskScheduledEvent(false).Return(&workflow.WorkflowTaskInfo{}, nil)
	s.currentContext.EXPECT().UpdateWorkflowExecutionAsActive(ctx, gomock.Any()).Return(nil)

	err := signalWorkflow(
		ctx,
		s.shardContext,
		currentWorkflowContext,
		request,
	)
	s.NoError(err)
}

func (s *signalWithStartWorkflowSuite) TestSignalWorkflow_NoNewWorkflowTask() {
	ctx := context.Background()
	currentWorkflowContext := api.NewWorkflowContext(
		s.currentContext,
		workflow.NoopReleaseFn,
		s.currentMutableState,
	)
	request := s.randomRequest()

	s.currentMutableState.EXPECT().IsSignalRequested(request.GetRequestId()).Return(false)
	s.currentMutableState.EXPECT().AddSignalRequested(request.GetRequestId())
	s.currentMutableState.EXPECT().AddWorkflowExecutionSignaled(
		request.GetSignalName(),
		request.GetSignalInput(),
		request.GetIdentity(),
		request.GetHeader(),
	).Return(&history.HistoryEvent{}, nil)
	s.currentMutableState.EXPECT().HasPendingWorkflowTask().Return(true)
	s.currentContext.EXPECT().UpdateWorkflowExecutionAsActive(ctx, gomock.Any()).Return(nil)

	err := signalWorkflow(
		ctx,
		s.shardContext,
		currentWorkflowContext,
		request,
	)
	s.NoError(err)
}

func (s *signalWithStartWorkflowSuite) randomRequest() *workflowservice.SignalWithStartWorkflowExecutionRequest {
	var request workflowservice.SignalWithStartWorkflowExecutionRequest
	_ = gofakeit.Struct(&request)
	return &request
}
