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
	"strconv"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/blobstore"
	"github.com/uber/cadence/common/blobstore/blob"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
	mmocks "github.com/uber/cadence/common/metrics/mocks"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"go.uber.org/cadence/testsuite"
	"go.uber.org/cadence/worker"
	"go.uber.org/zap"
)

const (
	testArchivalBucket     = "test-archival-bucket"
	testCurrentClusterName = "test-current-cluster-name"
)

var (
	errPersistenceNonRetryable = errors.New("persistence non-retryable error")
	errPersistenceRetryable    = &shared.InternalServiceError{}
)

type activitiesSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	logger        log.Logger
	metricsClient *mmocks.Client
	metricsScope  *mmocks.Scope
}

func TestActivitiesSuite(t *testing.T) {
	suite.Run(t, new(activitiesSuite))
}

func (s *activitiesSuite) SetupTest() {
	zapLogger := zap.NewNop()
	s.logger = loggerimpl.NewLogger(zapLogger)
	s.metricsClient = &mmocks.Client{}
	s.metricsScope = &mmocks.Scope{}
	s.metricsScope.On("StartTimer", metrics.CadenceLatency).Return(metrics.NewTestStopwatch()).Once()
}

func (s *activitiesSuite) TearDownTest() {
	s.metricsClient.AssertExpectations(s.T())
	s.metricsScope.AssertExpectations(s.T())
}

func (s *activitiesSuite) TestUploadHistoryActivity_Fail_DomainCacheNonRetryableError() {
	domainCache := &cache.DomainCacheMock{}
	domainCache.On("GetDomainByID", mock.Anything).Return(nil, errPersistenceNonRetryable).Once()
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverNonRetryableErrorCount).Once()
	container := &BootstrapContainer{
		Logger:        s.logger,
		MetricsClient: s.metricsClient,
		DomainCache:   domainCache,
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
		BucketName:           testArchivalBucket,
	}
	_, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.Equal(errGetDomainByID, err.Error())
}

func (s *activitiesSuite) TestUploadHistoryActivity_Fail_TimeoutGettingDomainCacheEntry() {
	domainCache := &cache.DomainCacheMock{}
	domainCache.On("GetDomainByID", mock.Anything).Return(nil, errPersistenceRetryable).Once()
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.CadenceErrContextTimeoutCounter).Once()
	container := &BootstrapContainer{
		Logger:        s.logger,
		MetricsClient: s.metricsClient,
		DomainCache:   domainCache,
	}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(getCanceledContext(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
		BucketName:           testArchivalBucket,
	}
	_, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.Equal(errContextTimeout.Error(), err.Error())
}

func (s *activitiesSuite) TestUploadHistoryActivity_Skip_ClusterArchivalNotEnabled() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverSkipUploadCount).Once()
	domainCache, mockClusterMetadata := s.archivalConfig(true, testArchivalBucket, false)
	container := &BootstrapContainer{
		Logger:          s.logger,
		MetricsClient:   s.metricsClient,
		DomainCache:     domainCache,
		ClusterMetadata: mockClusterMetadata,
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
		BucketName:           testArchivalBucket,
	}
	_, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.NoError(err)
}

func (s *activitiesSuite) TestUploadHistoryActivity_Skip_DomainArchivalNotEnabled() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverSkipUploadCount).Once()
	domainCache, mockClusterMetadata := s.archivalConfig(false, "", true)
	container := &BootstrapContainer{
		Logger:          s.logger,
		MetricsClient:   s.metricsClient,
		DomainCache:     domainCache,
		ClusterMetadata: mockClusterMetadata,
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
		BucketName:           testArchivalBucket,
	}
	_, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.NoError(err)
}

func (s *activitiesSuite) TestUploadHistoryActivity_Fail_DomainConfigMissingBucket() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverNonRetryableErrorCount).Once()
	domainCache, mockClusterMetadata := s.archivalConfig(true, "", true)
	container := &BootstrapContainer{
		Logger:          s.logger,
		MetricsClient:   s.metricsClient,
		DomainCache:     domainCache,
		ClusterMetadata: mockClusterMetadata,
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
		BucketName:           "",
	}
	_, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.Equal(errEmptyBucket, err.Error())
}

func (s *activitiesSuite) TestUploadHistoryActivity_Fail_ConstructBlobKeyError() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverNonRetryableErrorCount).Once()
	domainCache, mockClusterMetadata := s.archivalConfig(true, testArchivalBucket, true)
	container := &BootstrapContainer{
		Logger:          s.logger,
		MetricsClient:   s.metricsClient,
		DomainCache:     domainCache,
		ClusterMetadata: mockClusterMetadata,
	}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           "", // this causes an error when creating the blob key
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
		BucketName:           testArchivalBucket,
	}
	_, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.Equal(errConstructKey, err.Error())
}

func (s *activitiesSuite) TestUploadHistoryActivity_Fail_GetTagsNonRetryableError() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverNonRetryableErrorCount).Once()
	domainCache, mockClusterMetadata := s.archivalConfig(true, testArchivalBucket, true)
	mockBlobstore := &mocks.BlobstoreClient{}
	mockBlobstore.On("GetTags", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("some error"))
	mockBlobstore.On("IsRetryableError", mock.Anything).Return(false)
	container := &BootstrapContainer{
		Logger:          s.logger,
		MetricsClient:   s.metricsClient,
		DomainCache:     domainCache,
		ClusterMetadata: mockClusterMetadata,
		Blobstore:       mockBlobstore,
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
		BucketName:           testArchivalBucket,
	}
	_, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.Equal(errGetTags, err.Error())
}

func (s *activitiesSuite) TestUploadHistoryActivity_Fail_GetTagsTimeout() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.CadenceErrContextTimeoutCounter).Once()
	domainCache, mockClusterMetadata := s.archivalConfig(true, testArchivalBucket, true)
	mockBlobstore := &mocks.BlobstoreClient{}
	mockBlobstore.On("GetTags", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("some error"))
	mockBlobstore.On("IsRetryableError", mock.Anything).Return(true)
	container := &BootstrapContainer{
		Logger:          s.logger,
		MetricsClient:   s.metricsClient,
		DomainCache:     domainCache,
		ClusterMetadata: mockClusterMetadata,
		Blobstore:       mockBlobstore,
	}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(getCanceledContext(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
		BucketName:           testArchivalBucket,
	}
	_, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.Equal(errContextTimeout.Error(), err.Error())
}

func (s *activitiesSuite) TestUploadHistoryActivity_Success_BlobAlreadyExists() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	domainCache, mockClusterMetadata := s.archivalConfig(true, testArchivalBucket, true)
	mockBlobstore := &mocks.BlobstoreClient{}
	historyBlobKey, _ := NewHistoryBlobKey(testDomainID, testWorkflowID, testRunID, testCloseFailoverVersion, common.FirstBlobPageToken)
	mockBlobstore.On("GetTags", mock.Anything, mock.Anything, historyBlobKey).Return(map[string]string{"is_last": "true"}, nil).Once()
	historyIndexBlobKey, _ := NewHistoryIndexBlobKey(testDomainID, testWorkflowID, testRunID)
	mockBlobstore.On("GetTags", mock.Anything, mock.Anything, historyIndexBlobKey).Return(map[string]string{strconv.FormatInt(testCloseFailoverVersion, 10): ""}, nil).Once()
	container := &BootstrapContainer{
		Logger:          s.logger,
		MetricsClient:   s.metricsClient,
		DomainCache:     domainCache,
		ClusterMetadata: mockClusterMetadata,
		Blobstore:       mockBlobstore,
		Config:          getConfig(false),
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
		BucketName:           testArchivalBucket,
	}
	_, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.NoError(err)
}

func (s *activitiesSuite) TestUploadHistoryActivity_Success_MultipleBlobsAlreadyExist() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	domainCache, mockClusterMetadata := s.archivalConfig(true, testArchivalBucket, true)
	mockBlobstore := &mocks.BlobstoreClient{}
	firstKey, _ := NewHistoryBlobKey(testDomainID, testWorkflowID, testRunID, testCloseFailoverVersion, common.FirstBlobPageToken)
	mockBlobstore.On("GetTags", mock.Anything, mock.Anything, firstKey).Return(map[string]string{"is_last": "false"}, nil).Once()
	secondKey, _ := NewHistoryBlobKey(testDomainID, testWorkflowID, testRunID, testCloseFailoverVersion, common.FirstBlobPageToken+1)
	mockBlobstore.On("GetTags", mock.Anything, mock.Anything, secondKey).Return(map[string]string{"is_last": "true"}, nil).Once()
	historyIndexBlobKey, _ := NewHistoryIndexBlobKey(testDomainID, testWorkflowID, testRunID)
	mockBlobstore.On("GetTags", mock.Anything, mock.Anything, historyIndexBlobKey).Return(map[string]string{strconv.FormatInt(testCloseFailoverVersion, 10): ""}, nil).Once()
	container := &BootstrapContainer{
		Logger:          s.logger,
		MetricsClient:   s.metricsClient,
		DomainCache:     domainCache,
		ClusterMetadata: mockClusterMetadata,
		Blobstore:       mockBlobstore,
		Config:          getConfig(false),
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
		BucketName:           testArchivalBucket,
	}
	_, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.NoError(err)
}

func (s *activitiesSuite) TestUploadHistoryActivity_Fail_ReadBlobNonRetryableError() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverNonRetryableErrorCount).Once()
	domainCache, mockClusterMetadata := s.archivalConfig(true, testArchivalBucket, true)
	mockBlobstore := &mocks.BlobstoreClient{}
	mockBlobstore.On("GetTags", mock.Anything, mock.Anything, mock.Anything).Return(nil, blobstore.ErrBlobNotExists).Once()
	mockHistoryBlobReader := &HistoryBlobReaderMock{}
	mockHistoryBlobReader.On("GetBlob", mock.Anything).Return(nil, errPersistenceNonRetryable)
	container := &BootstrapContainer{
		Logger:            s.logger,
		MetricsClient:     s.metricsClient,
		DomainCache:       domainCache,
		ClusterMetadata:   mockClusterMetadata,
		Blobstore:         mockBlobstore,
		HistoryBlobReader: mockHistoryBlobReader,
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
		BucketName:           testArchivalBucket,
	}
	_, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.Equal(errReadBlob, err.Error())
}

func (s *activitiesSuite) TestUploadHistoryActivity_Fail_ReadBlobTimeout() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.CadenceErrContextTimeoutCounter).Once()
	domainCache, mockClusterMetadata := s.archivalConfig(true, testArchivalBucket, true)
	mockBlobstore := &mocks.BlobstoreClient{}
	mockBlobstore.On("GetTags", mock.Anything, mock.Anything, mock.Anything).Return(nil, blobstore.ErrBlobNotExists).Once()
	mockHistoryBlobReader := &HistoryBlobReaderMock{}
	mockHistoryBlobReader.On("GetBlob", mock.Anything).Return(nil, errPersistenceRetryable)
	container := &BootstrapContainer{
		Logger:            s.logger,
		MetricsClient:     s.metricsClient,
		DomainCache:       domainCache,
		ClusterMetadata:   mockClusterMetadata,
		Blobstore:         mockBlobstore,
		HistoryBlobReader: mockHistoryBlobReader,
	}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(getCanceledContext(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
		BucketName:           testArchivalBucket,
	}
	_, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.Equal(errContextTimeout.Error(), err.Error())
}

func (s *activitiesSuite) TestUploadHistoryActivity_Fail_CouldNotRunCheck() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverCouldNotRunDeterministicConstructionCheckCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverRunningDeterministicConstructionCheckCount).Once()
	domainCache, mockClusterMetadata := s.archivalConfig(true, testArchivalBucket, true)
	mockBlobstore := &mocks.BlobstoreClient{}
	historyBlobKey, _ := NewHistoryBlobKey(testDomainID, testWorkflowID, testRunID, testCloseFailoverVersion, common.FirstBlobPageToken)
	mockBlobstore.On("GetTags", mock.Anything, mock.Anything, historyBlobKey).Return(map[string]string{"is_last": "true"}, nil).Once()
	historyIndexBlobKey, _ := NewHistoryIndexBlobKey(testDomainID, testWorkflowID, testRunID)
	mockBlobstore.On("GetTags", mock.Anything, mock.Anything, historyIndexBlobKey).Return(map[string]string{strconv.FormatInt(testCloseFailoverVersion, 10): ""}, nil).Once()
	mockBlobstore.On("Download", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("some error"))
	mockBlobstore.On("IsRetryableError", mock.Anything).Return(false)
	mockHistoryBlobReader := &HistoryBlobReaderMock{}
	mockHistoryBlobReader.On("GetBlob", mock.Anything).Return(&HistoryBlob{
		Header: &HistoryBlobHeader{},
	}, nil)
	container := &BootstrapContainer{
		Logger:            s.logger,
		MetricsClient:     s.metricsClient,
		DomainCache:       domainCache,
		ClusterMetadata:   mockClusterMetadata,
		Blobstore:         mockBlobstore,
		Config:            getConfig(true),
		HistoryBlobReader: mockHistoryBlobReader,
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
		BucketName:           testArchivalBucket,
	}
	_, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.NoError(err)
}

func (s *activitiesSuite) TestUploadHistoryActivity_Fail_CheckFailed() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverDeterministicConstructionCheckFailedCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverRunningDeterministicConstructionCheckCount).Once()
	domainCache, mockClusterMetadata := s.archivalConfig(true, testArchivalBucket, true)
	mockBlobstore := &mocks.BlobstoreClient{}
	historyBlobKey, _ := NewHistoryBlobKey(testDomainID, testWorkflowID, testRunID, testCloseFailoverVersion, common.FirstBlobPageToken)
	mockBlobstore.On("GetTags", mock.Anything, mock.Anything, historyBlobKey).Return(map[string]string{"is_last": "true"}, nil).Once()
	historyIndexBlobKey, _ := NewHistoryIndexBlobKey(testDomainID, testWorkflowID, testRunID)
	mockBlobstore.On("GetTags", mock.Anything, mock.Anything, historyIndexBlobKey).Return(map[string]string{strconv.FormatInt(testCloseFailoverVersion, 10): ""}, nil).Once()
	mockBlobstore.On("Download", mock.Anything, mock.Anything, mock.Anything).Return(&blob.Blob{Body: []byte{1, 2, 3, 4}}, nil)
	mockHistoryBlobReader := &HistoryBlobReaderMock{}
	mockHistoryBlobReader.On("GetBlob", mock.Anything).Return(&HistoryBlob{
		Header: &HistoryBlobHeader{},
	}, nil)
	container := &BootstrapContainer{
		Logger:            s.logger,
		MetricsClient:     s.metricsClient,
		DomainCache:       domainCache,
		ClusterMetadata:   mockClusterMetadata,
		Blobstore:         mockBlobstore,
		Config:            getConfig(true),
		HistoryBlobReader: mockHistoryBlobReader,
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
		BucketName:           testArchivalBucket,
	}
	_, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.NoError(err)
}

func (s *activitiesSuite) TestUploadHistoryActivity_Fail_UploadBlobNonRetryableError() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverNonRetryableErrorCount).Once()
	domainCache, mockClusterMetadata := s.archivalConfig(true, testArchivalBucket, true)
	mockBlobstore := &mocks.BlobstoreClient{}
	mockBlobstore.On("GetTags", mock.Anything, mock.Anything, mock.Anything).Return(nil, blobstore.ErrBlobNotExists).Once()
	mockBlobstore.On("Upload", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("some error"))
	mockBlobstore.On("IsRetryableError", mock.Anything).Return(false)
	mockHistoryBlobReader := &HistoryBlobReaderMock{}
	mockHistoryBlobReader.On("GetBlob", mock.Anything).Return(&HistoryBlob{
		Header: &HistoryBlobHeader{},
	}, nil)
	container := &BootstrapContainer{
		Logger:            s.logger,
		MetricsClient:     s.metricsClient,
		DomainCache:       domainCache,
		ClusterMetadata:   mockClusterMetadata,
		Blobstore:         mockBlobstore,
		HistoryBlobReader: mockHistoryBlobReader,
		Config:            getConfig(false),
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
		BucketName:           testArchivalBucket,
	}
	_, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.Equal(errUploadBlob, err.Error())
}

func (s *activitiesSuite) TestUploadHistoryActivity_Fail_UploadBlobTimeout() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.CadenceErrContextTimeoutCounter).Once()
	domainCache, mockClusterMetadata := s.archivalConfig(true, testArchivalBucket, true)
	mockBlobstore := &mocks.BlobstoreClient{}
	mockBlobstore.On("GetTags", mock.Anything, mock.Anything, mock.Anything).Return(nil, blobstore.ErrBlobNotExists).Once()
	mockBlobstore.On("Upload", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("some error"))
	mockBlobstore.On("IsRetryableError", mock.Anything).Return(true)
	mockHistoryBlobReader := &HistoryBlobReaderMock{}
	mockHistoryBlobReader.On("GetBlob", mock.Anything).Return(&HistoryBlob{
		Header: &HistoryBlobHeader{},
	}, nil)
	container := &BootstrapContainer{
		Logger:            s.logger,
		MetricsClient:     s.metricsClient,
		DomainCache:       domainCache,
		ClusterMetadata:   mockClusterMetadata,
		Blobstore:         mockBlobstore,
		HistoryBlobReader: mockHistoryBlobReader,
		Config:            getConfig(false),
	}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(getCanceledContext(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
		BucketName:           testArchivalBucket,
	}
	_, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.Equal(errContextTimeout.Error(), err.Error())
}

func (s *activitiesSuite) TestUploadHistoryActivity_Success_BlobDoesNotAlreadyExist() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	domainCache, mockClusterMetadata := s.archivalConfig(true, testArchivalBucket, true)
	mockBlobstore := &mocks.BlobstoreClient{}
	mockBlobstore.On("GetTags", mock.Anything, mock.Anything, mock.Anything).Return(nil, blobstore.ErrBlobNotExists).Twice()
	mockBlobstore.On("Upload", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Twice()
	mockHistoryBlobReader := &HistoryBlobReaderMock{}
	mockHistoryBlobReader.On("GetBlob", mock.Anything).Return(&HistoryBlob{
		Header: &HistoryBlobHeader{
			IsLast: common.BoolPtr(true),
		},
	}, nil)
	container := &BootstrapContainer{
		Logger:            s.logger,
		MetricsClient:     s.metricsClient,
		DomainCache:       domainCache,
		ClusterMetadata:   mockClusterMetadata,
		Blobstore:         mockBlobstore,
		HistoryBlobReader: mockHistoryBlobReader,
		Config:            getConfig(false),
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
		BucketName:           testArchivalBucket,
	}
	_, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.NoError(err)
}

func (s *activitiesSuite) TestUploadHistoryActivity_Success_ConcurrentUploads() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	firstKey, _ := NewHistoryBlobKey(testDomainID, testWorkflowID, testRunID, testCloseFailoverVersion, common.FirstBlobPageToken)
	secondKey, _ := NewHistoryBlobKey(testDomainID, testWorkflowID, testRunID, testCloseFailoverVersion, common.FirstBlobPageToken+1)
	historyIndexBlobKey, _ := NewHistoryIndexBlobKey(testDomainID, testWorkflowID, testRunID)
	domainCache, mockClusterMetadata := s.archivalConfig(true, testArchivalBucket, true)
	mockBlobstore := &mocks.BlobstoreClient{}
	// first blob exists second blob does not exist
	mockBlobstore.On("GetTags", mock.Anything, mock.Anything, firstKey).Return(map[string]string{"is_last": "false"}, nil).Once()
	mockBlobstore.On("GetTags", mock.Anything, mock.Anything, secondKey).Return(nil, blobstore.ErrBlobNotExists).Once()
	mockBlobstore.On("Upload", mock.Anything, mock.Anything, secondKey, mock.Anything).Return(nil).Once()
	mockBlobstore.On("GetTags", mock.Anything, mock.Anything, historyIndexBlobKey).Return(nil, blobstore.ErrBlobNotExists).Once()
	mockBlobstore.On("Upload", mock.Anything, mock.Anything, historyIndexBlobKey, mock.Anything).Return(nil).Once()
	mockHistoryBlobReader := &HistoryBlobReaderMock{}
	mockHistoryBlobReader.On("GetBlob", common.FirstBlobPageToken+1).Return(&HistoryBlob{
		Header: &HistoryBlobHeader{
			IsLast: common.BoolPtr(true),
		},
	}, nil)
	container := &BootstrapContainer{
		Logger:            s.logger,
		MetricsClient:     s.metricsClient,
		DomainCache:       domainCache,
		ClusterMetadata:   mockClusterMetadata,
		Blobstore:         mockBlobstore,
		HistoryBlobReader: mockHistoryBlobReader,
		Config:            getConfig(false),
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
		BucketName:           testArchivalBucket,
	}
	_, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.NoError(err)
}

func (s *activitiesSuite) TestDeleteBlobActivity_Fail_ConstructBlobKeyError() {
	s.metricsClient.On("Scope", metrics.ArchiverDeleteBlobActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
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
		WorkflowID:           "", // this causes an error when creating the blob key
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
		BucketName:           testArchivalBucket,
	}
	_, err := env.ExecuteActivity(deleteBlobActivity, request)
	s.Equal(errConstructKey, err.Error())
}

func (s *activitiesSuite) TestDeleteBlobActivity_Fail_DeleteBlobError() {
	s.metricsClient.On("Scope", metrics.ArchiverDeleteBlobActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverNonRetryableErrorCount).Once()

	indexBlobKey, err := NewHistoryIndexBlobKey(testDomainID, testWorkflowID, testRunID)
	s.Nil(err)
	blobKey, err := NewHistoryBlobKey(testDomainID, testWorkflowID, testRunID, testCloseFailoverVersion, common.FirstBlobPageToken)
	s.Nil(err)

	mockBlobstore := &mocks.BlobstoreClient{}
	mockBlobstore.On("GetTags", mock.Anything, testArchivalBucket, indexBlobKey).Return(map[string]string{strconv.FormatInt(testCloseFailoverVersion, 10): ""}, nil).Once()
	mockBlobstore.On("Delete", mock.Anything, testArchivalBucket, indexBlobKey).Return(true, nil).Once()
	mockBlobstore.On("Delete", mock.Anything, testArchivalBucket, blobKey).Return(false, errors.New("some random error")).Once()
	mockBlobstore.On("IsRetryableError", mock.Anything).Return(false)

	container := &BootstrapContainer{
		Logger:        s.logger,
		MetricsClient: s.metricsClient,
		Blobstore:     mockBlobstore,
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
		BucketName:           testArchivalBucket,
	}
	_, err = env.ExecuteActivity(deleteBlobActivity, request)
	s.Equal(errDeleteBlob, err.Error())
}

func (s *activitiesSuite) TestDeleteBlobActivity_Success_NoHeartbeatDetails() {
	s.metricsClient.On("Scope", metrics.ArchiverDeleteBlobActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()

	pageToken := common.FirstBlobPageToken
	indexBlobKey, err := NewHistoryIndexBlobKey(testDomainID, testWorkflowID, testRunID)
	s.Nil(err)
	firstBlobKey, err := NewHistoryBlobKey(testDomainID, testWorkflowID, testRunID, testCloseFailoverVersion, pageToken)
	s.Nil(err)
	secondBlobKey, err := NewHistoryBlobKey(testDomainID, testWorkflowID, testRunID, testCloseFailoverVersion, pageToken+1)
	s.Nil(err)

	mockBlobstore := &mocks.BlobstoreClient{}
	mockBlobstore.On("GetTags", mock.Anything, testArchivalBucket, indexBlobKey).Return(map[string]string{strconv.FormatInt(testCloseFailoverVersion, 10): "", "some other version": ""}, nil).Once()
	mockBlobstore.On("Upload", mock.Anything, testArchivalBucket, indexBlobKey, mock.Anything).Return(nil).Once()
	mockBlobstore.On("Delete", mock.Anything, testArchivalBucket, firstBlobKey).Return(true, nil).Once()
	mockBlobstore.On("Delete", mock.Anything, testArchivalBucket, secondBlobKey).Return(false, nil).Once()
	mockBlobstore.On("Delete", mock.Anything, testArchivalBucket, indexBlobKey).Return(true, nil).Once()

	container := &BootstrapContainer{
		Logger:        s.logger,
		MetricsClient: s.metricsClient,
		Blobstore:     mockBlobstore,
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
		BucketName:           testArchivalBucket,
	}
	_, err = env.ExecuteActivity(deleteBlobActivity, request)
	s.NoError(err)
}

func (s *activitiesSuite) TestDeleteBlobActivity_Success_WithHeartbeatDetails() {
	s.metricsClient.On("Scope", metrics.ArchiverDeleteBlobActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()

	prevPageToken := common.FirstBlobPageToken + 1
	indexBlobKey, err := NewHistoryIndexBlobKey(testDomainID, testWorkflowID, testRunID)
	s.Nil(err)
	thirdBlobKey, err := NewHistoryBlobKey(testDomainID, testWorkflowID, testRunID, testCloseFailoverVersion, prevPageToken+1)
	s.Nil(err)
	fourthBlobKey, err := NewHistoryBlobKey(testDomainID, testWorkflowID, testRunID, testCloseFailoverVersion, prevPageToken+2)
	s.Nil(err)

	mockBlobstore := &mocks.BlobstoreClient{}
	mockBlobstore.On("GetTags", mock.Anything, testArchivalBucket, indexBlobKey).Return(map[string]string{"some other version": ""}, nil).Once()
	mockBlobstore.On("Delete", mock.Anything, testArchivalBucket, thirdBlobKey).Return(true, nil).Once()
	mockBlobstore.On("Delete", mock.Anything, testArchivalBucket, fourthBlobKey).Return(false, nil).Once()
	mockBlobstore.On("Delete", mock.Anything, testArchivalBucket, indexBlobKey).Return(true, nil).Once()

	container := &BootstrapContainer{
		Logger:        s.logger,
		MetricsClient: s.metricsClient,
		Blobstore:     mockBlobstore,
	}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	env.SetHeartbeatDetails(prevPageToken)
	request := ArchiveRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
		BucketName:           testArchivalBucket,
	}
	_, err = env.ExecuteActivity(deleteBlobActivity, request)
	s.NoError(err)
}

func (s *activitiesSuite) TestDeleteBlobActivity_Success_FirstBlobNotExist() {
	s.metricsClient.On("Scope", metrics.ArchiverDeleteBlobActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()

	prevPageToken := common.FirstBlobPageToken + 1
	indexBlobKey, err := NewHistoryIndexBlobKey(testDomainID, testWorkflowID, testRunID)
	s.Nil(err)
	thirdBlobKey, err := NewHistoryBlobKey(testDomainID, testWorkflowID, testRunID, testCloseFailoverVersion, prevPageToken+1)
	s.Nil(err)
	fourthBlobKey, err := NewHistoryBlobKey(testDomainID, testWorkflowID, testRunID, testCloseFailoverVersion, prevPageToken+2)
	s.Nil(err)
	fifthBlobKey, err := NewHistoryBlobKey(testDomainID, testWorkflowID, testRunID, testCloseFailoverVersion, prevPageToken+3)
	s.Nil(err)

	mockBlobstore := &mocks.BlobstoreClient{}
	mockBlobstore.On("GetTags", mock.Anything, testArchivalBucket, indexBlobKey).Return(nil, blobstore.ErrBlobNotExists).Once()
	mockBlobstore.On("Delete", mock.Anything, testArchivalBucket, thirdBlobKey).Return(false, blobstore.ErrBlobNotExists).Once()
	mockBlobstore.On("Delete", mock.Anything, testArchivalBucket, fourthBlobKey).Return(true, nil).Once()
	mockBlobstore.On("Delete", mock.Anything, testArchivalBucket, fifthBlobKey).Return(false, blobstore.ErrBlobNotExists).Once()
	mockBlobstore.On("Delete", mock.Anything, testArchivalBucket, indexBlobKey).Return(true, nil).Once()

	container := &BootstrapContainer{
		Logger:        s.logger,
		MetricsClient: s.metricsClient,
		Blobstore:     mockBlobstore,
	}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	env.SetHeartbeatDetails(prevPageToken)
	request := ArchiveRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
		BucketName:           testArchivalBucket,
	}
	_, err = env.ExecuteActivity(deleteBlobActivity, request)
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
		EventStoreVersion:    persistence.EventStoreVersionV2,
		BucketName:           testArchivalBucket,
	}
	_, err := env.ExecuteActivity(deleteHistoryActivity, request)
	s.Equal(errDeleteHistoryV2, err.Error())
}

func (s *activitiesSuite) TestDeleteHistoryActivity_Fail_TimeoutOnDeleteHistoryV2() {
	s.metricsClient.On("Scope", metrics.ArchiverDeleteHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.CadenceErrContextTimeoutCounter).Once()
	mockHistoryV2Manager := &mocks.HistoryV2Manager{}
	mockHistoryV2Manager.On("DeleteHistoryBranch", mock.Anything).Return(errPersistenceRetryable)
	container := &BootstrapContainer{
		Logger:           s.logger,
		MetricsClient:    s.metricsClient,
		HistoryV2Manager: mockHistoryV2Manager,
	}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(getCanceledContext(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
		EventStoreVersion:    persistence.EventStoreVersionV2,
		BucketName:           testArchivalBucket,
	}
	_, err := env.ExecuteActivity(deleteHistoryActivity, request)
	s.Equal(errContextTimeout.Error(), err.Error())
}

func (s *activitiesSuite) TestDeleteHistoryActivity_Fail_DeleteFromV1NonRetryableError() {
	s.metricsClient.On("Scope", metrics.ArchiverDeleteHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverNonRetryableErrorCount).Once()
	mockHistoryManager := &mocks.HistoryManager{}
	mockHistoryManager.On("DeleteWorkflowExecutionHistory", mock.Anything).Return(errPersistenceNonRetryable)
	container := &BootstrapContainer{
		Logger:         s.logger,
		MetricsClient:  s.metricsClient,
		HistoryManager: mockHistoryManager,
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
		BucketName:           testArchivalBucket,
	}
	_, err := env.ExecuteActivity(deleteHistoryActivity, request)
	s.Equal(errDeleteHistoryV1, err.Error())
}

func (s *activitiesSuite) TestDeleteHistoryActivity_Fail_TimeoutOnDeleteHistoryV1() {
	s.metricsClient.On("Scope", metrics.ArchiverDeleteHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.CadenceErrContextTimeoutCounter).Once()
	mockHistoryManager := &mocks.HistoryManager{}
	mockHistoryManager.On("DeleteWorkflowExecutionHistory", mock.Anything).Return(errPersistenceRetryable)
	container := &BootstrapContainer{
		Logger:         s.logger,
		MetricsClient:  s.metricsClient,
		HistoryManager: mockHistoryManager,
	}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(getCanceledContext(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
		BucketName:           testArchivalBucket,
	}
	_, err := env.ExecuteActivity(deleteHistoryActivity, request)
	s.Equal(errContextTimeout.Error(), err.Error())
}

func (s *activitiesSuite) TestDeleteHistoryActivity_Success() {
	s.metricsClient.On("Scope", metrics.ArchiverDeleteHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	mockHistoryManager := &mocks.HistoryManager{}
	mockHistoryManager.On("DeleteWorkflowExecutionHistory", mock.Anything).Return(nil)
	container := &BootstrapContainer{
		Logger:         s.logger,
		MetricsClient:  s.metricsClient,
		HistoryManager: mockHistoryManager,
	}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(getCanceledContext(), bootstrapContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
		BucketName:           testArchivalBucket,
	}
	_, err := env.ExecuteActivity(deleteHistoryActivity, request)
	s.NoError(err)
}

func (s *activitiesSuite) archivalConfig(
	domainEnablesArchival bool,
	domainArchivalBucket string,
	clusterEnablesArchival bool,
) (cache.DomainCache, cluster.Metadata) {
	domainArchivalStatus := shared.ArchivalStatusDisabled
	if domainEnablesArchival {
		domainArchivalStatus = shared.ArchivalStatusEnabled
	}
	clusterArchivalStatus := cluster.ArchivalDisabled
	clusterDefaultBucket := ""
	if clusterEnablesArchival {
		clusterDefaultBucket = "default-bucket"
		clusterArchivalStatus = cluster.ArchivalEnabled
	}
	mockMetadataMgr := &mocks.MetadataManager{}
	mockClusterMetadata := &mocks.ClusterMetadata{}
	mockClusterMetadata.On("ArchivalConfig").Return(cluster.NewArchivalConfig(clusterArchivalStatus, clusterDefaultBucket, clusterEnablesArchival))
	mockClusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	mockClusterMetadata.On("GetCurrentClusterName").Return(testCurrentClusterName)
	mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info: &persistence.DomainInfo{ID: testDomainID, Name: testDomain},
			Config: &persistence.DomainConfig{
				Retention:      1,
				ArchivalBucket: domainArchivalBucket,
				ArchivalStatus: domainArchivalStatus,
			},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			TableVersion: persistence.DomainTableVersionV1,
		},
		nil,
	)
	return cache.NewDomainCache(mockMetadataMgr, mockClusterMetadata, s.metricsClient, loggerimpl.NewNopLogger()), mockClusterMetadata
}

func getConfig(constCheck bool) *Config {
	probability := 0.0
	if constCheck {
		probability = 1.0
	}
	return &Config{
		DeterministicConstructionCheckProbability: dynamicconfig.GetFloatPropertyFn(probability),
		EnableArchivalCompression:                 dynamicconfig.GetBoolPropertyFnFilteredByDomain(true),
	}
}

func getCanceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
