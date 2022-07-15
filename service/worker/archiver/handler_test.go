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

package archiver

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

var (
	handlerTestMetrics *metrics.MockClient
	handlerTestLogger  *log.MockLogger
)

type handlerSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	controller *gomock.Controller
}

func TestHandlerSuite(t *testing.T) {
	suite.Run(t, new(handlerSuite))
}

func (s *handlerSuite) registerWorkflows(env *testsuite.TestWorkflowEnvironment) {
	env.RegisterWorkflow(handleHistoryRequestWorkflow)
	env.RegisterWorkflow(handleVisibilityRequestWorkflow)
	env.RegisterWorkflow(startAndFinishArchiverWorkflow)

	env.RegisterActivityWithOptions(uploadHistoryActivity, activity.RegisterOptions{Name: uploadHistoryActivityFnName})
	env.RegisterActivityWithOptions(deleteHistoryActivity, activity.RegisterOptions{Name: deleteHistoryActivityFnName})
	env.RegisterActivityWithOptions(archiveVisibilityActivity, activity.RegisterOptions{Name: archiveVisibilityActivityFnName})
}

func (s *handlerSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	handlerTestMetrics = metrics.NewMockClient(s.controller)
	handlerTestMetrics.EXPECT().StartTimer(gomock.Any(), gomock.Any()).Return(metrics.NoopStopwatch).AnyTimes()
	handlerTestLogger = log.NewMockLogger(s.controller)
}

func (s *handlerSuite) TearDownTest() {
}

func (s *handlerSuite) TestHandleHistoryRequest_UploadFails_NonRetryableError() {
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverUploadFailedAllRetriesCount)
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverDeleteSuccessCount)
	handlerTestLogger.EXPECT().Error(gomock.Any(), gomock.Any())

	env := s.NewTestWorkflowEnvironment()
	s.registerWorkflows(env)
	env.OnActivity(uploadHistoryActivityFnName, mock.Anything, mock.Anything).Return(errors.New("some random error"))
	env.OnActivity(deleteHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.ExecuteWorkflow(handleHistoryRequestWorkflow, ArchiveRequest{})

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *handlerSuite) TestHandleHistoryRequest_UploadFails_ExpireRetryTimeout() {
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverUploadFailedAllRetriesCount)
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverDeleteSuccessCount)
	handlerTestLogger.EXPECT().Error(gomock.Any(), gomock.Any())

	timeoutErr := temporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_START_TO_CLOSE, nil)
	env := s.NewTestWorkflowEnvironment()
	s.registerWorkflows(env)
	env.OnActivity(uploadHistoryActivityFnName, mock.Anything, mock.Anything).Return(timeoutErr)
	env.OnActivity(deleteHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.ExecuteWorkflow(handleHistoryRequestWorkflow, ArchiveRequest{})

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *handlerSuite) TestHandleHistoryRequest_UploadSuccess() {
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverUploadSuccessCount)
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverDeleteSuccessCount)

	env := s.NewTestWorkflowEnvironment()
	s.registerWorkflows(env)
	env.OnActivity(uploadHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(deleteHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.ExecuteWorkflow(handleHistoryRequestWorkflow, ArchiveRequest{})

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *handlerSuite) TestHandleHistoryRequest_DeleteFails_NonRetryableError() {
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverUploadSuccessCount)
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverDeleteFailedAllRetriesCount)
	handlerTestLogger.EXPECT().Error(gomock.Any(), gomock.Any())

	env := s.NewTestWorkflowEnvironment()
	s.registerWorkflows(env)
	env.OnActivity(uploadHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(deleteHistoryActivityFnName, mock.Anything, mock.Anything).Return(func(context.Context, ArchiveRequest) error {
		return temporal.NewNonRetryableApplicationError(errDeleteNonRetryable.Error(), "", nil)
	})
	env.ExecuteWorkflow(handleHistoryRequestWorkflow, ArchiveRequest{})

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *handlerSuite) TestHandleHistoryRequest_DeleteFailsThenSucceeds() {
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverUploadSuccessCount)
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverDeleteSuccessCount)

	env := s.NewTestWorkflowEnvironment()
	s.registerWorkflows(env)
	env.OnActivity(uploadHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	firstRun := true
	env.OnActivity(deleteHistoryActivityFnName, mock.Anything, mock.Anything).Return(func(context.Context, ArchiveRequest) error {
		if firstRun {
			firstRun = false
			return errors.New("some retryable error")
		}
		return nil
	})
	env.ExecuteWorkflow(handleHistoryRequestWorkflow, ArchiveRequest{})

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *handlerSuite) TestHandleVisibilityRequest_Fail() {
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverHandleVisibilityFailedAllRetiresCount)
	handlerTestLogger.EXPECT().Error(gomock.Any(), gomock.Any())

	env := s.NewTestWorkflowEnvironment()
	s.registerWorkflows(env)
	env.OnActivity(archiveVisibilityActivityFnName, mock.Anything, mock.Anything).Return(errors.New("some random error"))
	env.ExecuteWorkflow(handleVisibilityRequestWorkflow, ArchiveRequest{})

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *handlerSuite) TestHandleVisibilityRequest_Success() {
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverHandleVisibilitySuccessCount)

	env := s.NewTestWorkflowEnvironment()
	s.registerWorkflows(env)
	env.OnActivity(archiveVisibilityActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.ExecuteWorkflow(handleVisibilityRequestWorkflow, ArchiveRequest{})

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *handlerSuite) TestRunArchiver() {
	numRequests := 1000
	concurrency := 10
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverUploadSuccessCount).Times(numRequests)
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverDeleteSuccessCount).Times(numRequests)
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverHandleVisibilitySuccessCount).Times(numRequests)
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverStartedCount)
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverCoroutineStartedCount).Times(concurrency)
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverCoroutineStoppedCount).Times(concurrency)
	handlerTestMetrics.EXPECT().IncCounter(metrics.ArchiverScope, metrics.ArchiverStoppedCount)

	env := s.NewTestWorkflowEnvironment()
	s.registerWorkflows(env)
	env.OnActivity(uploadHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(deleteHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(archiveVisibilityActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.ExecuteWorkflow(startAndFinishArchiverWorkflow, concurrency, numRequests)

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func handleHistoryRequestWorkflow(ctx workflow.Context, request ArchiveRequest) error {
	handler := NewHandler(ctx, handlerTestLogger, handlerTestMetrics, 0, nil).(*handler)
	handler.handleHistoryRequest(ctx, &request)
	return nil
}

func handleVisibilityRequestWorkflow(ctx workflow.Context, request ArchiveRequest) error {
	handler := NewHandler(ctx, handlerTestLogger, handlerTestMetrics, 0, nil).(*handler)
	handler.handleVisibilityRequest(ctx, &request)
	return nil
}

func startAndFinishArchiverWorkflow(ctx workflow.Context, concurrency int, numRequests int) error {
	requestCh := workflow.NewBufferedChannel(ctx, numRequests)
	handler := NewHandler(ctx, handlerTestLogger, handlerTestMetrics, concurrency, requestCh)
	handler.Start()
	sentHashes := make([]uint64, numRequests)
	workflow.Go(ctx, func(ctx workflow.Context) {
		for i := 0; i < numRequests; i++ {
			ar, hash := randomArchiveRequest()
			requestCh.Send(ctx, ar)
			sentHashes[i] = hash
		}
		requestCh.Close()
	})
	handledHashes := handler.Finished()
	if !hashesEqual(handledHashes, sentHashes) {
		return errors.New("handled hashes does not equal sent hashes")
	}
	return nil
}

func randomArchiveRequest() (ArchiveRequest, uint64) {
	ar := ArchiveRequest{
		NamespaceID: fmt.Sprintf("%v", rand.Intn(1000)),
		WorkflowID:  fmt.Sprintf("%v", rand.Intn(1000)),
		RunID:       fmt.Sprintf("%v", rand.Intn(1000)),
		Targets:     []ArchivalTarget{ArchiveTargetHistory, ArchiveTargetVisibility},
	}
	return ar, hash(ar)
}
