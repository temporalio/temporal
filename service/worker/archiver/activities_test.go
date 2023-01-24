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
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	carchiver "go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
)

const (
	testNamespaceID          = "test-namespace-id"
	testNamespace            = "test-namespace"
	testWorkflowID           = "test-workflow-id"
	testRunID                = "test-run-id"
	testNextEventID          = 1800
	testCloseFailoverVersion = 100
	testScheme               = "testScheme"
	testArchivalURI          = testScheme + "://history/archival"
)

var (
	testBranchToken = []byte{1, 2, 3}
)

type activitiesSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	controller       *gomock.Controller
	mockExecutionMgr *persistence.MockExecutionManager

	logger             log.Logger
	metricsHandler     *metrics.MockHandler
	archiverProvider   *provider.MockArchiverProvider
	historyArchiver    *carchiver.MockHistoryArchiver
	visibilityArchiver *carchiver.MockVisibilityArchiver
	historyClient      *historyservicemock.MockHistoryServiceClient
}

func TestActivitiesSuite(t *testing.T) {
	suite.Run(t, new(activitiesSuite))
}

func (s *activitiesSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.mockExecutionMgr = persistence.NewMockExecutionManager(s.controller)

	s.logger = log.NewNoopLogger()
	s.metricsHandler = metrics.NewMockHandler(s.controller)
	s.archiverProvider = provider.NewMockArchiverProvider(s.controller)
	s.historyArchiver = carchiver.NewMockHistoryArchiver(s.controller)
	s.visibilityArchiver = carchiver.NewMockVisibilityArchiver(s.controller)
	s.metricsHandler.EXPECT().Timer(metrics.ServiceLatency.GetMetricName()).Return(metrics.NoopTimerMetricFunc).MinTimes(0)
	s.historyClient = historyservicemock.NewMockHistoryServiceClient(s.controller)
}

func (s *activitiesSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *activitiesSuite) TestUploadHistory_Fail_InvalidURI() {
	s.metricsHandler.EXPECT().WithTags(
		metrics.OperationTag(metrics.ArchiverUploadHistoryActivityScope), []metrics.Tag{metrics.NamespaceTag(testNamespace)},
	).Return(s.metricsHandler)
	s.metricsHandler.EXPECT().Counter(metrics.ArchiverNonRetryableErrorCount.GetMetricName()).Return(metrics.NoopCounterMetricFunc)
	container := &BootstrapContainer{
		Logger:         s.logger,
		MetricsHandler: s.metricsHandler,
	}
	env := s.NewTestActivityEnvironment()
	s.registerWorkflows(env)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		NamespaceID:          testNamespaceID,
		Namespace:            testNamespace,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
		HistoryURI:           "some invalid URI without scheme",
	}
	_, err := env.ExecuteActivity(uploadHistoryActivity, request)
	applicationErr, ok := errors.Unwrap(err).(*temporal.ApplicationError)
	s.True(ok)
	s.True(applicationErr.NonRetryable())
	s.Equal(errUploadNonRetryable.Error(), applicationErr.Error())
}

func (s *activitiesSuite) TestUploadHistory_Fail_GetArchiverError() {
	s.metricsHandler.EXPECT().WithTags(
		metrics.OperationTag(metrics.ArchiverUploadHistoryActivityScope), []metrics.Tag{metrics.NamespaceTag(testNamespace)},
	).Return(s.metricsHandler)
	s.metricsHandler.EXPECT().Counter(metrics.ArchiverNonRetryableErrorCount.GetMetricName()).Return(metrics.NoopCounterMetricFunc)
	s.archiverProvider.EXPECT().GetHistoryArchiver(gomock.Any(), string(primitives.WorkerService)).Return(
		nil, errors.New("failed to get archiver"),
	)
	container := &BootstrapContainer{
		Logger:           s.logger,
		MetricsHandler:   s.metricsHandler,
		ArchiverProvider: s.archiverProvider,
	}
	env := s.NewTestActivityEnvironment()
	s.registerWorkflows(env)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		NamespaceID:          testNamespaceID,
		Namespace:            testNamespace,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
		HistoryURI:           testArchivalURI,
	}
	_, err := env.ExecuteActivity(uploadHistoryActivity, request)
	applicationErr, ok := errors.Unwrap(err).(*temporal.ApplicationError)
	s.True(ok)
	s.True(applicationErr.NonRetryable())
	s.Equal(errUploadNonRetryable.Error(), applicationErr.Error())
}

func (s *activitiesSuite) TestUploadHistory_Fail_ArchiveNonRetryableError() {
	s.metricsHandler.EXPECT().WithTags(metrics.OperationTag(metrics.ArchiverUploadHistoryActivityScope), []metrics.Tag{metrics.NamespaceTag(testNamespace)}).Return(s.metricsHandler)
	s.metricsHandler.EXPECT().Counter(metrics.ArchiverNonRetryableErrorCount.GetMetricName()).Return(metrics.NoopCounterMetricFunc)
	s.historyArchiver.EXPECT().Archive(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(errUploadNonRetryable)
	s.archiverProvider.EXPECT().GetHistoryArchiver(gomock.Any(), string(primitives.WorkerService)).Return(s.historyArchiver, nil)
	container := &BootstrapContainer{
		Logger:           s.logger,
		MetricsHandler:   s.metricsHandler,
		ArchiverProvider: s.archiverProvider,
	}
	env := s.NewTestActivityEnvironment()
	s.registerWorkflows(env)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		NamespaceID:          testNamespaceID,
		Namespace:            testNamespace,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
		HistoryURI:           testArchivalURI,
	}
	_, err := env.ExecuteActivity(uploadHistoryActivity, request)
	applicationErr, ok := errors.Unwrap(err).(*temporal.ApplicationError)
	s.True(ok)
	s.True(applicationErr.NonRetryable())
	s.Equal(errUploadNonRetryable.Error(), applicationErr.Error())
}

func (s *activitiesSuite) TestUploadHistory_Fail_ArchiveRetryableError() {
	s.metricsHandler.EXPECT().WithTags(metrics.OperationTag(metrics.ArchiverUploadHistoryActivityScope), []metrics.Tag{metrics.NamespaceTag(testNamespace)}).Return(s.metricsHandler)
	testArchiveErr := errors.New("some transient error")
	s.historyArchiver.EXPECT().Archive(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(testArchiveErr)
	s.archiverProvider.EXPECT().GetHistoryArchiver(gomock.Any(), string(primitives.WorkerService)).Return(s.historyArchiver, nil)
	container := &BootstrapContainer{
		Logger:           s.logger,
		MetricsHandler:   s.metricsHandler,
		ArchiverProvider: s.archiverProvider,
	}
	env := s.NewTestActivityEnvironment()
	s.registerWorkflows(env)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		NamespaceID:          testNamespaceID,
		Namespace:            testNamespace,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
		HistoryURI:           testArchivalURI,
	}
	_, err := env.ExecuteActivity(uploadHistoryActivity, request)
	applicationErr, ok := errors.Unwrap(err).(*temporal.ApplicationError)
	s.True(ok)
	s.False(applicationErr.NonRetryable())
	s.Equal(testArchiveErr.Error(), applicationErr.Error())
}

func (s *activitiesSuite) TestUploadHistory_Success() {
	s.metricsHandler.EXPECT().WithTags(metrics.OperationTag(metrics.ArchiverUploadHistoryActivityScope), []metrics.Tag{metrics.NamespaceTag(testNamespace)}).Return(s.metricsHandler)
	s.historyArchiver.EXPECT().Archive(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	s.archiverProvider.EXPECT().GetHistoryArchiver(gomock.Any(), string(primitives.WorkerService)).Return(s.historyArchiver, nil)
	container := &BootstrapContainer{
		Logger:           s.logger,
		MetricsHandler:   s.metricsHandler,
		ArchiverProvider: s.archiverProvider,
	}
	env := s.NewTestActivityEnvironment()
	s.registerWorkflows(env)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		NamespaceID:          testNamespaceID,
		Namespace:            testNamespace,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
		HistoryURI:           testArchivalURI,
	}
	_, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.NoError(err)
}

func (s *activitiesSuite) TestDeleteHistoryActivity_Fail_RetryableError() {
	s.metricsHandler.EXPECT().WithTags(metrics.OperationTag(metrics.ArchiverDeleteHistoryActivityScope), []metrics.Tag{metrics.NamespaceTag(testNamespace)}).Return(s.metricsHandler)
	container := &BootstrapContainer{
		Logger:           s.logger,
		MetricsHandler:   s.metricsHandler,
		HistoryV2Manager: s.mockExecutionMgr,
		HistoryClient:    s.historyClient,
	}
	env := s.NewTestActivityEnvironment()
	s.registerWorkflows(env)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})

	s.historyClient.EXPECT().DeleteWorkflowExecution(gomock.Any(), &historyservice.DeleteWorkflowExecutionRequest{
		NamespaceId: testNamespaceID,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: testWorkflowID,
			RunId:      testRunID,
		},
		WorkflowVersion:    testCloseFailoverVersion,
		ClosedWorkflowOnly: true,
	}).Return(nil, &serviceerror.WorkflowNotReady{})
	request := ArchiveRequest{
		NamespaceID:          testNamespaceID,
		Namespace:            testNamespace,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
		HistoryURI:           testArchivalURI,
	}
	_, err := env.ExecuteActivity(deleteHistoryActivity, request)
	applicationErr, ok := errors.Unwrap(err).(*temporal.ApplicationError)
	s.True(ok)
	s.False(applicationErr.NonRetryable())
}

func (s *activitiesSuite) TestDeleteHistoryActivity_Fail_NonRetryableError() {
	s.metricsHandler.EXPECT().WithTags(metrics.OperationTag(metrics.ArchiverDeleteHistoryActivityScope), []metrics.Tag{metrics.NamespaceTag(testNamespace)}).Return(s.metricsHandler)
	container := &BootstrapContainer{
		Logger:           s.logger,
		MetricsHandler:   s.metricsHandler,
		HistoryV2Manager: s.mockExecutionMgr,
		HistoryClient:    s.historyClient,
	}
	env := s.NewTestActivityEnvironment()
	s.registerWorkflows(env)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	s.historyClient.EXPECT().DeleteWorkflowExecution(gomock.Any(), &historyservice.DeleteWorkflowExecutionRequest{
		NamespaceId: testNamespaceID,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: testWorkflowID,
			RunId:      testRunID,
		},
		WorkflowVersion:    testCloseFailoverVersion,
		ClosedWorkflowOnly: true,
	}).Return(nil, &serviceerror.NotFound{})
	request := ArchiveRequest{
		NamespaceID:          testNamespaceID,
		Namespace:            testNamespace,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
		HistoryURI:           testArchivalURI,
	}
	_, err := env.ExecuteActivity(deleteHistoryActivity, request)
	applicationErr, ok := errors.Unwrap(err).(*temporal.ApplicationError)
	s.True(ok)
	s.True(applicationErr.NonRetryable())
	s.Equal(errDeleteNonRetryable.Error(), applicationErr.Error())
}

func (s *activitiesSuite) TestArchiveVisibilityActivity_Fail_InvalidURI() {
	s.metricsHandler.EXPECT().WithTags(metrics.OperationTag(metrics.ArchiverArchiveVisibilityActivityScope), []metrics.Tag{metrics.NamespaceTag(testNamespace)}).Return(s.metricsHandler)
	container := &BootstrapContainer{
		Logger:         s.logger,
		MetricsHandler: s.metricsHandler,
	}
	env := s.NewTestActivityEnvironment()
	s.registerWorkflows(env)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		NamespaceID:   testNamespaceID,
		Namespace:     testNamespace,
		WorkflowID:    testWorkflowID,
		RunID:         testRunID,
		VisibilityURI: "some invalid URI without scheme",
	}
	_, err := env.ExecuteActivity(archiveVisibilityActivity, request)
	applicationErr, ok := errors.Unwrap(err).(*temporal.ApplicationError)
	s.True(ok)
	s.True(applicationErr.NonRetryable())
	s.Equal(errArchiveVisibilityNonRetryable.Error(), applicationErr.Error())
}

func (s *activitiesSuite) TestArchiveVisibilityActivity_Fail_GetArchiverError() {
	s.metricsHandler.EXPECT().WithTags(metrics.OperationTag(metrics.ArchiverArchiveVisibilityActivityScope), []metrics.Tag{metrics.NamespaceTag(testNamespace)}).Return(s.metricsHandler)
	s.archiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any(), string(primitives.WorkerService)).Return(nil, errors.New("failed to get archiver"))
	container := &BootstrapContainer{
		Logger:           s.logger,
		MetricsHandler:   s.metricsHandler,
		ArchiverProvider: s.archiverProvider,
	}
	env := s.NewTestActivityEnvironment()
	s.registerWorkflows(env)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		NamespaceID:   testNamespaceID,
		Namespace:     testNamespace,
		WorkflowID:    testWorkflowID,
		RunID:         testRunID,
		VisibilityURI: testArchivalURI,
	}
	_, err := env.ExecuteActivity(archiveVisibilityActivity, request)
	applicationErr, ok := errors.Unwrap(err).(*temporal.ApplicationError)
	s.True(ok)
	s.True(applicationErr.NonRetryable())
	s.Equal(errArchiveVisibilityNonRetryable.Error(), applicationErr.Error())
}

func (s *activitiesSuite) TestArchiveVisibilityActivity_Fail_ArchiveNonRetryableError() {
	s.metricsHandler.EXPECT().WithTags(metrics.OperationTag(metrics.ArchiverArchiveVisibilityActivityScope), []metrics.Tag{metrics.NamespaceTag(testNamespace)}).Return(s.metricsHandler)
	s.visibilityArchiver.EXPECT().Archive(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(errArchiveVisibilityNonRetryable)
	s.archiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any(), string(primitives.WorkerService)).Return(s.visibilityArchiver, nil)
	container := &BootstrapContainer{
		Logger:           s.logger,
		MetricsHandler:   s.metricsHandler,
		ArchiverProvider: s.archiverProvider,
	}
	env := s.NewTestActivityEnvironment()
	s.registerWorkflows(env)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		NamespaceID:   testNamespaceID,
		Namespace:     testNamespace,
		WorkflowID:    testWorkflowID,
		RunID:         testRunID,
		VisibilityURI: testArchivalURI,
	}
	_, err := env.ExecuteActivity(archiveVisibilityActivity, request)
	applicationErr, ok := errors.Unwrap(err).(*temporal.ApplicationError)
	s.True(ok)
	s.True(applicationErr.NonRetryable())
	s.Equal(errArchiveVisibilityNonRetryable.Error(), applicationErr.Error())
}

func (s *activitiesSuite) TestArchiveVisibilityActivity_Fail_ArchiveRetryableError() {
	s.metricsHandler.EXPECT().WithTags(metrics.OperationTag(metrics.ArchiverArchiveVisibilityActivityScope), []metrics.Tag{metrics.NamespaceTag(testNamespace)}).Return(s.metricsHandler)
	testArchiveErr := errors.New("some transient error")
	s.visibilityArchiver.EXPECT().Archive(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(testArchiveErr)
	s.archiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any(), string(primitives.WorkerService)).Return(s.visibilityArchiver, nil)
	container := &BootstrapContainer{
		Logger:           s.logger,
		MetricsHandler:   s.metricsHandler,
		ArchiverProvider: s.archiverProvider,
	}
	env := s.NewTestActivityEnvironment()
	s.registerWorkflows(env)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		NamespaceID:   testNamespaceID,
		Namespace:     testNamespace,
		WorkflowID:    testWorkflowID,
		RunID:         testRunID,
		VisibilityURI: testArchivalURI,
	}
	_, err := env.ExecuteActivity(archiveVisibilityActivity, request)
	applicationErr, ok := errors.Unwrap(err).(*temporal.ApplicationError)
	s.True(ok)
	s.False(applicationErr.NonRetryable())
	s.Equal(testArchiveErr.Error(), applicationErr.Error())
}

func (s *activitiesSuite) TestArchiveVisibilityActivity_Success() {
	s.metricsHandler.EXPECT().WithTags(metrics.OperationTag(metrics.ArchiverArchiveVisibilityActivityScope), []metrics.Tag{metrics.NamespaceTag(testNamespace)}).Return(s.metricsHandler)
	s.visibilityArchiver.EXPECT().Archive(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	s.archiverProvider.EXPECT().GetVisibilityArchiver(gomock.Any(), string(primitives.WorkerService)).Return(s.visibilityArchiver, nil)
	container := &BootstrapContainer{
		Logger:           s.logger,
		MetricsHandler:   s.metricsHandler,
		ArchiverProvider: s.archiverProvider,
	}
	env := s.NewTestActivityEnvironment()
	s.registerWorkflows(env)
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		NamespaceID:   testNamespaceID,
		Namespace:     testNamespace,
		WorkflowID:    testWorkflowID,
		RunID:         testRunID,
		VisibilityURI: testArchivalURI,
	}
	_, err := env.ExecuteActivity(archiveVisibilityActivity, request)
	s.NoError(err)
}

func (s *activitiesSuite) registerWorkflows(env *testsuite.TestActivityEnvironment) {
	env.RegisterActivityWithOptions(uploadHistoryActivity, activity.RegisterOptions{Name: uploadHistoryActivityFnName})
	env.RegisterActivityWithOptions(deleteHistoryActivity, activity.RegisterOptions{Name: deleteHistoryActivityFnName})
	env.RegisterActivityWithOptions(archiveVisibilityActivity, activity.RegisterOptions{Name: archiveVisibilityActivityFnName})
}
