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
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/common/metrics"
	mmocks "github.com/uber/cadence/common/metrics/mocks"
	"github.com/uber/cadence/common/mocks"
	"go.uber.org/cadence"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/testsuite"
	"go.uber.org/cadence/workflow"
)

var (
	archiverTestMetrics *mmocks.Client
	archiverTestLogger  *mocks.Logger
)

type archiverSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
}

func TestArchiverSuite(t *testing.T) {
	suite.Run(t, new(archiverSuite))
}

func (s *archiverSuite) SetupSuite() {
	workflow.Register(handleRequestWorkflow)
	workflow.Register(startAndFinishArchiverWorkflow)
}

func (s *archiverSuite) SetupTest() {
	archiverTestMetrics = &mmocks.Client{}
	archiverTestMetrics.On("StartTimer", mock.Anything, mock.Anything).Return(tally.NewStopwatch(time.Now(), &nopStopwatchRecorder{}))
	archiverTestLogger = &mocks.Logger{}
	archiverTestLogger.On("WithFields", mock.Anything).Return(archiverTestLogger)
	archiverTestLogger.On("WithField", mock.Anything, mock.Anything).Return(archiverTestLogger).Maybe()
}

func (s *archiverSuite) TearDownTest() {
	archiverTestMetrics.AssertExpectations(s.T())
	archiverTestLogger.AssertExpectations(s.T())
}

func (s *archiverSuite) TestHandleRequest_UploadFails_NonRetryableError() {
	archiverTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverUploadFailedAllRetriesCount).Once()
	archiverTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverDeleteLocalSuccessCount).Once()
	archiverTestLogger.On("Error", mock.Anything).Once()

	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(uploadHistoryActivityFnName, mock.Anything, mock.Anything).Return(cadence.NewCustomError(errGetDomainByID))
	env.OnActivity(deleteHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.ExecuteWorkflow(handleRequestWorkflow, ArchiveRequest{})

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *archiverSuite) TestHandleRequest_UploadFails_ExpireRetryTimeout() {
	archiverTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverUploadFailedAllRetriesCount).Once()
	archiverTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverDeleteLocalSuccessCount).Once()
	archiverTestLogger.On("Error", mock.Anything).Once()

	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(uploadHistoryActivityFnName, mock.Anything, mock.Anything).Return(workflow.NewTimeoutError(shared.TimeoutTypeStartToClose))
	env.OnActivity(deleteHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.ExecuteWorkflow(handleRequestWorkflow, ArchiveRequest{})

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *archiverSuite) TestHandleRequest_LocalDeleteFails_NonRetryableError() {
	archiverTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverUploadSuccessCount).Once()
	archiverTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverDeleteLocalFailedAllRetriesCount).Once()
	archiverTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverDeleteSuccessCount).Once()
	archiverTestLogger.On("Warn", mock.Anything).Once()

	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(uploadHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	var deleteSucceed bool
	env.OnActivity(deleteHistoryActivityFnName, mock.Anything, mock.Anything).Return(func(context.Context, ArchiveRequest) error {
		if !deleteSucceed {
			deleteSucceed = true
			return cadence.NewCustomError(errDeleteHistoryV1)
		}
		return nil
	})
	env.ExecuteWorkflow(handleRequestWorkflow, ArchiveRequest{})

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *archiverSuite) TestHandleRequest_LocalDeleteFailsThenSucceeds() {
	archiverTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverUploadSuccessCount).Once()
	archiverTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverDeleteLocalSuccessCount).Once()

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
	env.ExecuteWorkflow(handleRequestWorkflow, ArchiveRequest{})

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func (s *archiverSuite) TestRunArchiver() {
	numRequests := 1000
	concurrency := 10
	archiverTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverUploadSuccessCount).Times(numRequests)
	archiverTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverDeleteLocalSuccessCount).Times(numRequests)
	archiverTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverStartedCount).Once()
	archiverTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverCoroutineStartedCount).Times(concurrency)
	archiverTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverCoroutineStoppedCount).Times(concurrency)
	archiverTestMetrics.On("IncCounter", metrics.ArchiverScope, metrics.ArchiverStoppedCount).Once()

	env := s.NewTestWorkflowEnvironment()
	env.OnActivity(uploadHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(deleteHistoryActivityFnName, mock.Anything, mock.Anything).Return(nil)
	env.ExecuteWorkflow(startAndFinishArchiverWorkflow, concurrency, numRequests)

	env.AssertExpectations(s.T())
	s.True(env.IsWorkflowCompleted())
	s.NoError(env.GetWorkflowError())
}

func handleRequestWorkflow(ctx workflow.Context, request ArchiveRequest) error {
	handleRequest(ctx, archiverTestLogger, archiverTestMetrics, request)
	return nil
}

func startAndFinishArchiverWorkflow(ctx workflow.Context, concurrency int, numRequests int) error {
	requestCh := workflow.NewBufferedChannel(ctx, numRequests)
	archiver := NewArchiver(ctx, archiverTestLogger, archiverTestMetrics, concurrency, requestCh)
	archiver.Start()
	sentHashes := make([]uint64, numRequests, numRequests)
	workflow.Go(ctx, func(ctx workflow.Context) {
		for i := 0; i < numRequests; i++ {
			ar, hash := randomArchiveRequest()
			requestCh.Send(ctx, ar)
			sentHashes[i] = hash
		}
		requestCh.Close()
	})
	handledHashes := archiver.Finished()
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
	}
	return ar, hashArchiveRequest(ar)
}
