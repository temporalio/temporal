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
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/cadence/testsuite"
	"go.uber.org/cadence/worker"
	"go.uber.org/zap"

	"github.com/uber/cadence/common"
	carchiver "github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/archiver/provider"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
	mmocks "github.com/uber/cadence/common/metrics/mocks"
	"github.com/uber/cadence/common/mocks"
)

const (
	testDomainID             = "test-domain-id"
	testDomainName           = "test-domain-name"
	testWorkflowID           = "test-workflow-id"
	testRunID                = "test-run-id"
	testNextEventID          = 1800
	testCloseFailoverVersion = 100
	testScheme               = "testScheme"
	testArchivalURI          = testScheme + "://history/archival"
)

var (
	testBranchToken = []byte{1, 2, 3}

	errPersistenceNonRetryable = errors.New("persistence non-retryable error")
)

type activitiesSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	logger             log.Logger
	metricsClient      *mmocks.Client
	metricsScope       *mmocks.Scope
	archiverProvider   *provider.MockArchiverProvider
	historyArchiver    *carchiver.HistoryArchiverMock
	visibilityArchiver *carchiver.VisibilityArchiverMock
}

func TestActivitiesSuite(t *testing.T) {
	suite.Run(t, new(activitiesSuite))
}

func (s *activitiesSuite) SetupTest() {
	zapLogger := zap.NewNop()
	s.logger = loggerimpl.NewLogger(zapLogger)
	s.metricsClient = &mmocks.Client{}
	s.metricsScope = &mmocks.Scope{}
	s.archiverProvider = &provider.MockArchiverProvider{}
	s.historyArchiver = &carchiver.HistoryArchiverMock{}
	s.visibilityArchiver = &carchiver.VisibilityArchiverMock{}
	s.metricsScope.On("StartTimer", metrics.CadenceLatency).Return(metrics.NewTestStopwatch()).Maybe()
	s.metricsScope.On("RecordTimer", mock.Anything, mock.Anything).Maybe()
}

func (s *activitiesSuite) TearDownTest() {
	s.metricsClient.AssertExpectations(s.T())
	s.metricsScope.AssertExpectations(s.T())
	s.archiverProvider.AssertExpectations(s.T())
	s.historyArchiver.AssertExpectations(s.T())
	s.visibilityArchiver.AssertExpectations(s.T())
}

func (s *activitiesSuite) TestUploadHistory_Fail_InvalidURI() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverNonRetryableErrorCount).Once()
	container := &BootstrapContainer{
		Logger:        s.logger,
		MetricsClient: s.metricsClient,
	}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
		URI:                  "some invalid URI without scheme",
	}
	_, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.Equal(errUploadNonRetriable.Error(), err.Error())
}

func (s *activitiesSuite) TestUploadHistory_Fail_GetArchiverError() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverNonRetryableErrorCount).Once()
	s.archiverProvider.On("GetHistoryArchiver", mock.Anything, common.WorkerServiceName).Return(nil, errors.New("failed to get archiver"))
	container := &BootstrapContainer{
		Logger:           s.logger,
		MetricsClient:    s.metricsClient,
		ArchiverProvider: s.archiverProvider,
	}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
		URI:                  testArchivalURI,
	}
	_, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.Equal(errUploadNonRetriable.Error(), err.Error())
}

func (s *activitiesSuite) TestUploadHistory_Fail_ArchiveNonRetriableError() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverNonRetryableErrorCount).Once()
	s.historyArchiver.On("Archive", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errUploadNonRetriable)
	s.archiverProvider.On("GetHistoryArchiver", mock.Anything, common.WorkerServiceName).Return(s.historyArchiver, nil)
	container := &BootstrapContainer{
		Logger:           s.logger,
		MetricsClient:    s.metricsClient,
		ArchiverProvider: s.archiverProvider,
	}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
		URI:                  testArchivalURI,
	}
	_, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.Equal(errUploadNonRetriable.Error(), err.Error())
}

func (s *activitiesSuite) TestUploadHistory_Fail_ArchiveRetriableError() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	testArchiveErr := errors.New("some transient error")
	s.historyArchiver.On("Archive", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(testArchiveErr)
	s.archiverProvider.On("GetHistoryArchiver", mock.Anything, common.WorkerServiceName).Return(s.historyArchiver, nil)
	container := &BootstrapContainer{
		Logger:           s.logger,
		MetricsClient:    s.metricsClient,
		ArchiverProvider: s.archiverProvider,
	}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
		URI:                  testArchivalURI,
	}
	_, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.Equal(testArchiveErr.Error(), err.Error())
}

func (s *activitiesSuite) TestUploadHistory_Success() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.historyArchiver.On("Archive", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.archiverProvider.On("GetHistoryArchiver", mock.Anything, common.WorkerServiceName).Return(s.historyArchiver, nil)
	container := &BootstrapContainer{
		Logger:           s.logger,
		MetricsClient:    s.metricsClient,
		ArchiverProvider: s.archiverProvider,
	}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
		URI:                  testArchivalURI,
	}
	_, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.NoError(err)
}

func (s *activitiesSuite) TestDeleteHistoryActivity_Fail_DeleteFromV2NonRetryableError() {
	s.metricsClient.On("Scope", metrics.ArchiverDeleteHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverNonRetryableErrorCount).Once()
	mockHistoryV2Manager := &mocks.HistoryV2Manager{}
	mockHistoryV2Manager.On("DeleteHistoryBranch", mock.Anything).Return(errPersistenceNonRetryable)
	container := &BootstrapContainer{
		Logger:           s.logger,
		MetricsClient:    s.metricsClient,
		HistoryV2Manager: mockHistoryV2Manager,
	}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
		URI:                  testArchivalURI,
	}
	_, err := env.ExecuteActivity(deleteHistoryActivity, request)
	s.Equal(errDeleteNonRetriable.Error(), err.Error())
}

func (s *activitiesSuite) TestArchiveVisibilityActivity_Fail_InvalidURI() {
	s.metricsClient.On("Scope", metrics.ArchiverArchiveVisibilityActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverNonRetryableErrorCount).Once()
	container := &BootstrapContainer{
		Logger:        s.logger,
		MetricsClient: s.metricsClient,
	}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:      testDomainID,
		DomainName:    testDomainName,
		WorkflowID:    testWorkflowID,
		RunID:         testRunID,
		VisibilityURI: "some invalid URI without scheme",
	}
	_, err := env.ExecuteActivity(archiveVisibilityActivity, request)
	s.Equal(errArchiveVisibilityNonRetriable.Error(), err.Error())
}

func (s *activitiesSuite) TestArchiveVisibilityActivity_Fail_GetArchiverError() {
	s.metricsClient.On("Scope", metrics.ArchiverArchiveVisibilityActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverNonRetryableErrorCount).Once()
	s.archiverProvider.On("GetVisibilityArchiver", mock.Anything, common.WorkerServiceName).Return(nil, errors.New("failed to get archiver"))
	container := &BootstrapContainer{
		Logger:           s.logger,
		MetricsClient:    s.metricsClient,
		ArchiverProvider: s.archiverProvider,
	}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:      testDomainID,
		DomainName:    testDomainName,
		WorkflowID:    testWorkflowID,
		RunID:         testRunID,
		VisibilityURI: testArchivalURI,
	}
	_, err := env.ExecuteActivity(archiveVisibilityActivity, request)
	s.Equal(errArchiveVisibilityNonRetriable.Error(), err.Error())
}

func (s *activitiesSuite) TestArchiveVisibilityActivity_Fail_ArchiveNonRetriableError() {
	s.metricsClient.On("Scope", metrics.ArchiverArchiveVisibilityActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverNonRetryableErrorCount).Once()
	s.visibilityArchiver.On("Archive", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errArchiveVisibilityNonRetriable)
	s.archiverProvider.On("GetVisibilityArchiver", mock.Anything, common.WorkerServiceName).Return(s.visibilityArchiver, nil)
	container := &BootstrapContainer{
		Logger:           s.logger,
		MetricsClient:    s.metricsClient,
		ArchiverProvider: s.archiverProvider,
	}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:      testDomainID,
		DomainName:    testDomainName,
		WorkflowID:    testWorkflowID,
		RunID:         testRunID,
		VisibilityURI: testArchivalURI,
	}
	_, err := env.ExecuteActivity(archiveVisibilityActivity, request)
	s.Equal(errArchiveVisibilityNonRetriable.Error(), err.Error())
}

func (s *activitiesSuite) TestArchiveVisibilityActivity_Fail_ArchiveRetriableError() {
	s.metricsClient.On("Scope", metrics.ArchiverArchiveVisibilityActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	testArchiveErr := errors.New("some transient error")
	s.visibilityArchiver.On("Archive", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(testArchiveErr)
	s.archiverProvider.On("GetVisibilityArchiver", mock.Anything, common.WorkerServiceName).Return(s.visibilityArchiver, nil)
	container := &BootstrapContainer{
		Logger:           s.logger,
		MetricsClient:    s.metricsClient,
		ArchiverProvider: s.archiverProvider,
	}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:      testDomainID,
		DomainName:    testDomainName,
		WorkflowID:    testWorkflowID,
		RunID:         testRunID,
		VisibilityURI: testArchivalURI,
	}
	_, err := env.ExecuteActivity(archiveVisibilityActivity, request)
	s.Equal(testArchiveErr.Error(), err.Error())
}

func (s *activitiesSuite) TestArchiveVisibilityActivity_Success() {
	s.metricsClient.On("Scope", metrics.ArchiverArchiveVisibilityActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.visibilityArchiver.On("Archive", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.archiverProvider.On("GetVisibilityArchiver", mock.Anything, common.WorkerServiceName).Return(s.visibilityArchiver, nil)
	container := &BootstrapContainer{
		Logger:           s.logger,
		MetricsClient:    s.metricsClient,
		ArchiverProvider: s.archiverProvider,
	}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:      testDomainID,
		DomainName:    testDomainName,
		WorkflowID:    testWorkflowID,
		RunID:         testRunID,
		VisibilityURI: testArchivalURI,
	}
	_, err := env.ExecuteActivity(archiveVisibilityActivity, request)
	s.NoError(err)
}
