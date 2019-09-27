// Copyright (c) 2017 Uber Technologies, Inc.
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

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	mmocks "github.com/uber/cadence/common/metrics/mocks"
	"go.uber.org/cadence"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/testsuite"
	"go.uber.org/cadence/workflow"
)

var (
	handlerTestMetrics *mmocks.Client
	handlerTestLogger  *log.MockLogger
)

type handlerSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
}

func TestHandlerSuite(t *testing.T) {
	suite.Run(t, new(handlerSuite))
}

func (s *handlerSuite) SetupSuite() {
	workflow.Register(handleHistoryRequestWorkflow)
	workflow.Register(handleVisibilityRequestWorkflow)
	workflow.Register(startAndFinishArchiverWorkflow)
}

func (s *handlerSuite) SetupTest() {
	handlerTestMetrics = &mmocks.Client{}
	handlerTestMetrics.On("StartTimer", mock.Anything, mock.Anything).Return(metrics.NopStopwatch())
	handlerTestLogger = &log.MockLogger{}
	handlerTestLogger.On("WithTags", mock.Anything).Return(handlerTestLogger)
}

func (s *handlerSuite) TearDownTest() {
	handlerTestMetrics.AssertExpectations(s.T())
	handlerTestLogger.AssertExpectations(s.T())
}

func (s *handlerSuite) TestHandleHistoryRequest_UploadFails_NonRetryableError() {
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverUploadFailedAllRetriesCount).Once()
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverDeleteSuccessCount).Once()
	handlerTestLogger.On("Error", mock.Anything, mock.Anything).Once()

	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(uploadHistoryActivityFnName, mock.Anything, mock.Anything).Return(errors.New("some random error"))
	env.OnActivity(deleteHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.ExecuteWorkflow(handleHistoryRequestWorkflow, ArchiveRequest{})

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *handlerSuite) TestHandleHistoryRequest_UploadFails_ExpireRetryTimeout() {
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverUploadFailedAllRetriesCount).Once()
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverDeleteSuccessCount).Once()
	handlerTestLogger.On("Error", mock.Anything, mock.Anything).Once()

	timeoutErr := workflow.NewTimeoutError(shared.TimeoutTypeStartToClose)
	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(uploadHistoryActivityFnName, mock.Anything, mock.Anything).Return(timeoutErr)
	env.OnActivity(deleteHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.ExecuteWorkflow(handleHistoryRequestWorkflow, ArchiveRequest{})

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *handlerSuite) TestHandleHistoryRequest_UploadSuccess() {
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverUploadSuccessCount).Once()
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverDeleteSuccessCount).Once()

	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(uploadHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(deleteHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.ExecuteWorkflow(handleHistoryRequestWorkflow, ArchiveRequest{})

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *handlerSuite) TestHandleHistoryRequest_DeleteFails_NonRetryableError() {
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverUploadSuccessCount).Once()
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverDeleteFailedAllRetriesCount).Once()
	handlerTestLogger.On("Error", mock.Anything, mock.Anything).Once()

	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(uploadHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(deleteHistoryActivityFnName, mock.Anything, mock.Anything).Return(func(context.Context, ArchiveRequest) error {
		return cadence.NewCustomError(errDeleteNonRetriable.Error())
	})
	env.ExecuteWorkflow(handleHistoryRequestWorkflow, ArchiveRequest{})

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *handlerSuite) TestHandleHistoryRequest_DeleteFailsThenSucceeds() {
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverUploadSuccessCount).Once()
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverDeleteSuccessCount).Once()

	env := s.NewTestWorkflowEnvironment()
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
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverHandleVisibilityFailedAllRetiresCount).Once()
	handlerTestLogger.On("Error", mock.Anything, mock.Anything).Once()

	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(archiveVisibilityActivityFnName, mock.Anything, mock.Anything).Return(errors.New("some random error"))
	env.ExecuteWorkflow(handleVisibilityRequestWorkflow, ArchiveRequest{})

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *handlerSuite) TestHandleVisibilityRequest_Success() {
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverHandleVisibilitySuccessCount).Once()

	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(archiveVisibilityActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.ExecuteWorkflow(handleVisibilityRequestWorkflow, ArchiveRequest{})

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *handlerSuite) TestRunArchiver() {
	numRequests := 1000
	concurrency := 10
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverUploadSuccessCount).Times(numRequests)
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverDeleteSuccessCount).Times(numRequests)
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverHandleVisibilitySuccessCount).Times(numRequests)
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverStartedCount).Once()
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverCoroutineStartedCount).Times(concurrency)
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverCoroutineStoppedCount).Times(concurrency)
	handlerTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverStoppedCount).Once()

	env := s.NewTestWorkflowEnvironment()
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
	sentHashes := make([]uint64, numRequests, numRequests)
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
		DomainID:   fmt.Sprintf("%v", rand.Intn(1000)),
		WorkflowID: fmt.Sprintf("%v", rand.Intn(1000)),
		RunID:      fmt.Sprintf("%v", rand.Intn(1000)),
		Targets:    []archivalTarget{ArchiveTargetHistory, ArchiveTargetVisibility},
	}
	return ar, hash(ar)
}
