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
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/blobstore"
	"github.com/uber/cadence/common/blobstore/blob"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/persistence"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
	mmocks "github.com/uber/cadence/common/metrics/mocks"
	"github.com/uber/cadence/common/mocks"
	"go.uber.org/cadence/encoded"
	"go.uber.org/cadence/testsuite"
	"go.uber.org/cadence/worker"
	"go.uber.org/zap"
)

type historyBlobUploaderSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	*require.Assertions

	logger        log.Logger
	metricsClient *mmocks.Client
	metricsScope  *mmocks.Scope
}

func TestHistoryBlobUploaderSuite(t *testing.T) {
	suite.Run(t, new(historyBlobUploaderSuite))
}

func (s *historyBlobUploaderSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	zapLogger := zap.NewNop()
	s.logger = loggerimpl.NewLogger(zapLogger)
	s.metricsClient = &mmocks.Client{}
	s.metricsScope = &mmocks.Scope{}
	s.metricsScope.On("StartTimer", metrics.CadenceLatency).Return(metrics.NewTestStopwatch()).Once()
	s.metricsScope.On("RecordTimer", mock.Anything, mock.Anything).Maybe()
}

func (s *historyBlobUploaderSuite) TearDownTest() {
	s.metricsClient.AssertExpectations(s.T())
	s.metricsScope.AssertExpectations(s.T())
}

func (s *historyBlobUploaderSuite) TestUploadHistoryActivity_Fail_DomainCacheNonRetryableError() {
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

func (s *historyBlobUploaderSuite) TestUploadHistoryActivity_Fail_TimeoutGettingDomainCacheEntry() {
	domainCache := &cache.DomainCacheMock{}
	domainCache.On("GetDomainByID", mock.Anything).Return(nil, errPersistenceRetryable).Once()
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
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
	encodedResult, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.NoError(err)

	result := s.getDecodedUploadResult(encodedResult)
	s.Empty(result.BlobsToDelete)
	s.Equal(errContextTimeout.Error(), result.ErrorWithDetails)
}

func (s *historyBlobUploaderSuite) TestUploadHistoryActivity_Skip_ClusterArchivalNotEnabled() {
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
	encodedResult, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.NoError(err)

	result := s.getDecodedUploadResult(encodedResult)
	s.Empty(result)
}

func (s *historyBlobUploaderSuite) TestUploadHistoryActivity_Skip_DomainArchivalNotEnabled() {
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
	encodedResult, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.NoError(err)

	result := s.getDecodedUploadResult(encodedResult)
	s.Empty(result)
}

func (s *historyBlobUploaderSuite) TestUploadHistoryActivity_Fail_DomainConfigMissingBucket() {
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
	s.Equal(errInvalidRequest, err.Error())
}

func (s *historyBlobUploaderSuite) TestUploadHistoryActivity_Fail_ConstructBlobKeyError() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverNonRetryableErrorCount).Once()
	domainCache, mockClusterMetadata := s.archivalConfig(true, testArchivalBucket, true)
	container := &BootstrapContainer{
		Logger:          s.logger,
		MetricsClient:   s.metricsClient,
		DomainCache:     domainCache,
		ClusterMetadata: mockClusterMetadata,
		Config:          getConfig(false, false),
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
	encodedResult, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.NoError(err)

	result := s.getDecodedUploadResult(encodedResult)
	s.Equal(fmt.Sprintf("%v: %v", errConstructKey, errInvalidKeyInput.Error()), result.ErrorWithDetails)
	s.Empty(result.BlobsToDelete)
}

func (s *historyBlobUploaderSuite) TestUploadHistoryActivity_Fail_GetTagsNonRetryableError() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverNonRetryableErrorCount).Once()
	domainCache, mockClusterMetadata := s.archivalConfig(true, testArchivalBucket, true)
	mockBlobstore := &mocks.BlobstoreClient{}
	mockBlobstore.On("GetTags", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New(testErrDetails))
	mockBlobstore.On("IsRetryableError", mock.Anything).Return(false)
	container := &BootstrapContainer{
		Logger:          s.logger,
		MetricsClient:   s.metricsClient,
		DomainCache:     domainCache,
		ClusterMetadata: mockClusterMetadata,
		Blobstore:       mockBlobstore,
		Config:          getConfig(false, false),
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
	encodedResult, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.NoError(err)

	result := s.getDecodedUploadResult(encodedResult)
	s.Equal(fmt.Sprintf("%v: %v", errGetTags, testErrDetails), result.ErrorWithDetails)
	s.Empty(result.BlobsToDelete)
}

func (s *historyBlobUploaderSuite) TestUploadHistoryActivity_Fail_GetTagsTimeout() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	domainCache, mockClusterMetadata := s.archivalConfig(true, testArchivalBucket, true)
	mockBlobstore := &mocks.BlobstoreClient{}
	mockBlobstore.On("GetTags", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New(testErrDetails))
	mockBlobstore.On("IsRetryableError", mock.Anything).Return(true)
	container := &BootstrapContainer{
		Logger:          s.logger,
		MetricsClient:   s.metricsClient,
		DomainCache:     domainCache,
		ClusterMetadata: mockClusterMetadata,
		Blobstore:       mockBlobstore,
		Config:          getConfig(false, false),
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
	encodedResult, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.NoError(err)

	result := s.getDecodedUploadResult(encodedResult)
	s.Empty(result.BlobsToDelete)
	s.Equal(errContextTimeout.Error(), result.ErrorWithDetails)
}

func (s *historyBlobUploaderSuite) TestUploadHistoryActivity_Success_BlobAlreadyExists() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverBlobExistsCount).Once()
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
		Config:          getConfig(false, false),
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
	encodedResult, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.NoError(err)

	result := s.getDecodedUploadResult(encodedResult)
	s.Empty(result)
}

func (s *historyBlobUploaderSuite) TestUploadHistoryActivity_Success_MultipleBlobsAlreadyExist() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverBlobExistsCount).Twice()
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
		Config:          getConfig(false, false),
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
	encodedResult, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.NoError(err)

	result := s.getDecodedUploadResult(encodedResult)
	s.Empty(result)
}

func (s *historyBlobUploaderSuite) TestUploadHistoryActivity_Fail_ReadBlobNonRetryableError() {
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
		Config:            getConfig(false, false),
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
	encodedResult, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.NoError(err)

	result := s.getDecodedUploadResult(encodedResult)
	s.Equal(fmt.Sprintf("%v: %v", errReadBlob, errPersistenceNonRetryable.Error()), result.ErrorWithDetails)
	s.Empty(result.BlobsToDelete)
}

func (s *historyBlobUploaderSuite) TestUploadHistoryActivity_Fail_ReadBlobTimeout() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
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
		Config:            getConfig(false, false),
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
	encodedResult, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.NoError(err)

	result := s.getDecodedUploadResult(encodedResult)
	s.Empty(result.BlobsToDelete)
	s.Equal(errContextTimeout.Error(), result.ErrorWithDetails)
}

func (s *historyBlobUploaderSuite) TestUploadHistoryActivity_Fail_CouldNotRunCheck() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverCouldNotRunDeterministicConstructionCheckCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverRunningDeterministicConstructionCheckCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverBlobExistsCount).Once()
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
		Config:            getConfig(true, false),
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
	encodedResult, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.NoError(err)

	result := s.getDecodedUploadResult(encodedResult)
	s.Empty(result)
}

func (s *historyBlobUploaderSuite) TestUploadHistoryActivity_Fail_CheckFailed() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverDeterministicConstructionCheckFailedCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverRunningDeterministicConstructionCheckCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverBlobExistsCount).Once()
	domainCache, mockClusterMetadata := s.archivalConfig(true, testArchivalBucket, true)
	mockBlobstore := &mocks.BlobstoreClient{}
	historyBlobKey, _ := NewHistoryBlobKey(testDomainID, testWorkflowID, testRunID, testCloseFailoverVersion, common.FirstBlobPageToken)
	mockBlobstore.On("GetTags", mock.Anything, mock.Anything, historyBlobKey).Return(map[string]string{"is_last": "true"}, nil).Once()
	historyIndexBlobKey, _ := NewHistoryIndexBlobKey(testDomainID, testWorkflowID, testRunID)
	mockBlobstore.On("GetTags", mock.Anything, mock.Anything, historyIndexBlobKey).Return(map[string]string{strconv.FormatInt(testCloseFailoverVersion, 10): ""}, nil).Once()
	mockBlobstore.On("Download", mock.Anything, mock.Anything, mock.Anything).Return(&blob.Blob{Body: []byte{1, 2, 3, 4}}, nil)
	mockBlobstore.On("Upload", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
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
		Config:            getConfig(true, false),
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
	encodedResult, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.NoError(err)

	result := s.getDecodedUploadResult(encodedResult)
	s.Empty(result)
}

func (s *historyBlobUploaderSuite) TestUploadHistoryActivity_Fail_UploadBlobNonRetryableError() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverNonRetryableErrorCount).Once()
	domainCache, mockClusterMetadata := s.archivalConfig(true, testArchivalBucket, true)
	mockBlobstore := &mocks.BlobstoreClient{}
	mockBlobstore.On("GetTags", mock.Anything, mock.Anything, mock.Anything).Return(nil, blobstore.ErrBlobNotExists).Once()
	mockBlobstore.On("Upload", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New(testErrDetails))
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
		Config:            getConfig(false, false),
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
	encodedResult, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.NoError(err)

	result := s.getDecodedUploadResult(encodedResult)
	s.Equal(fmt.Sprintf("%v: %v", errUploadBlob, testErrDetails), result.ErrorWithDetails)
	s.Empty(result.BlobsToDelete)
}

func (s *historyBlobUploaderSuite) TestUploadHistoryActivity_Fail_UploadBlobTimeout() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
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
		Config:            getConfig(false, false),
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
	encodedResult, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.NoError(err)

	result := s.getDecodedUploadResult(encodedResult)
	s.Empty(result.BlobsToDelete)
	s.Equal(errContextTimeout.Error(), result.ErrorWithDetails)
}

func (s *historyBlobUploaderSuite) TestUploadHistoryActivity_Success_BlobDoesNotAlreadyExist() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	domainCache, mockClusterMetadata := s.archivalConfig(true, testArchivalBucket, true)
	mockBlobstore := &mocks.BlobstoreClient{}
	mockBlobstore.On("GetTags", mock.Anything, mock.Anything, mock.Anything).Return(nil, blobstore.ErrBlobNotExists).Twice()
	mockBlobstore.On("Upload", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Twice()
	mockHistoryBlobReader := &HistoryBlobReaderMock{}
	mockHistoryBlobReader.On("GetBlob", mock.Anything).Return(&HistoryBlob{
		Header: &HistoryBlobHeader{
			LastFailoverVersion: common.Int64Ptr(testCloseFailoverVersion),
			LastEventID:         common.Int64Ptr(testNextEventID - 1),
			IsLast:              common.BoolPtr(true),
		},
	}, nil)
	container := &BootstrapContainer{
		Logger:            s.logger,
		MetricsClient:     s.metricsClient,
		DomainCache:       domainCache,
		ClusterMetadata:   mockClusterMetadata,
		Blobstore:         mockBlobstore,
		HistoryBlobReader: mockHistoryBlobReader,
		Config:            getConfig(false, false),
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
	encodedResult, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.NoError(err)

	result := s.getDecodedUploadResult(encodedResult)
	s.Empty(result)
}

func (s *historyBlobUploaderSuite) TestUploadHistoryActivity_Success_ConcurrentUploads() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverBlobExistsCount).Once()
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
			LastFailoverVersion: common.Int64Ptr(testCloseFailoverVersion),
			LastEventID:         common.Int64Ptr(testNextEventID - 1),
			IsLast:              common.BoolPtr(true),
		},
	}, nil)
	container := &BootstrapContainer{
		Logger:            s.logger,
		MetricsClient:     s.metricsClient,
		DomainCache:       domainCache,
		ClusterMetadata:   mockClusterMetadata,
		Blobstore:         mockBlobstore,
		HistoryBlobReader: mockHistoryBlobReader,
		Config:            getConfig(false, false),
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
	encodedResult, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.NoError(err)

	result := s.getDecodedUploadResult(encodedResult)
	s.Empty(result)
}

func (s *historyBlobUploaderSuite) TestUploadHistoryActivity_Fail_HistoryMutated() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverNonRetryableErrorCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverHistoryMutatedCount).Once()
	firstKey, _ := NewHistoryBlobKey(testDomainID, testWorkflowID, testRunID, testCloseFailoverVersion, common.FirstBlobPageToken)
	domainCache, mockClusterMetadata := s.archivalConfig(true, testArchivalBucket, true)
	mockBlobstore := &mocks.BlobstoreClient{}
	mockBlobstore.On("GetTags", mock.Anything, mock.Anything, firstKey).Return(nil, blobstore.ErrBlobNotExists).Once()
	mockHistoryBlobReader := &HistoryBlobReaderMock{}
	// Return a history blob with a larger failover version
	mockHistoryBlobReader.On("GetBlob", common.FirstBlobPageToken).Return(&HistoryBlob{
		Header: &HistoryBlobHeader{
			LastFailoverVersion: common.Int64Ptr(testCloseFailoverVersion + 1),
			IsLast:              common.BoolPtr(true),
		},
	}, nil)
	container := &BootstrapContainer{
		Logger:            s.logger,
		MetricsClient:     s.metricsClient,
		DomainCache:       domainCache,
		ClusterMetadata:   mockClusterMetadata,
		Blobstore:         mockBlobstore,
		HistoryBlobReader: mockHistoryBlobReader,
		Config:            getConfig(false, false),
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
	encodedResult, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.NoError(err)

	result := s.getDecodedUploadResult(encodedResult)
	s.Equal(errHistoryMutated, result.ErrorWithDetails)
	s.Empty(result.BlobsToDelete)
}

func (s *historyBlobUploaderSuite) TestUploadHistoryActivity_Fail_CouldNotRunBlobIntegrityCheck() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverRunningBlobIntegrityCheckCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverCouldNotRunBlobIntegrityCheckCount).Once()
	domainCache, mockClusterMetadata := s.archivalConfig(true, testArchivalBucket, true)
	mockBlobstore := &mocks.BlobstoreClient{}
	mockBlobstore.On("GetTags", mock.Anything, mock.Anything, mock.Anything).Return(nil, blobstore.ErrBlobNotExists).Twice()
	mockBlobstore.On("Upload", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Twice()
	mockHistoryBlobReader := &HistoryBlobReaderMock{}
	mockHistoryBlobReader.On("GetBlob", mock.Anything).Return(&HistoryBlob{
		Header: &HistoryBlobHeader{
			LastFailoverVersion: common.Int64Ptr(testCloseFailoverVersion),
			LastEventID:         common.Int64Ptr(testNextEventID - 1),
			IsLast:              common.BoolPtr(true),
		},
		Body: &shared.History{},
	}, nil)
	mockDownloader := &HistoryBlobDownloaderMock{}
	mockDownloader.On("DownloadBlob", mock.Anything, mock.Anything).Return(nil, errors.New("failed to download blob")).Once()
	container := &BootstrapContainer{
		Logger:                s.logger,
		MetricsClient:         s.metricsClient,
		DomainCache:           domainCache,
		ClusterMetadata:       mockClusterMetadata,
		Blobstore:             mockBlobstore,
		HistoryBlobReader:     mockHistoryBlobReader,
		Config:                getConfig(false, true),
		HistoryBlobDownloader: mockDownloader,
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
	encodedResult, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.NoError(err)

	result := s.getDecodedUploadResult(encodedResult)
	s.Empty(result)
}

func (s *historyBlobUploaderSuite) TestUploadHistoryActivity_Fail_BlobIntegrityCheckFailed() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverRunningBlobIntegrityCheckCount).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverBlobIntegrityCheckFailedCount).Once()
	domainCache, mockClusterMetadata := s.archivalConfig(true, testArchivalBucket, true)
	mockBlobstore := &mocks.BlobstoreClient{}
	mockBlobstore.On("GetTags", mock.Anything, mock.Anything, mock.Anything).Return(nil, blobstore.ErrBlobNotExists).Twice()
	mockBlobstore.On("Upload", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Twice()
	mockHistoryBlobReader := &HistoryBlobReaderMock{}
	uploadedBlob := &HistoryBlob{
		Header: &HistoryBlobHeader{
			LastFailoverVersion: common.Int64Ptr(testCloseFailoverVersion),
			LastEventID:         common.Int64Ptr(testNextEventID - 1),
			IsLast:              common.BoolPtr(true),
		},
		Body: &shared.History{
			Events: []*shared.HistoryEvent{
				{
					EventId:   common.Int64Ptr(7),
					EventType: eventTypePtr(shared.EventTypeChildWorkflowExecutionCanceled),
					ChildWorkflowExecutionCanceledEventAttributes: &shared.ChildWorkflowExecutionCanceledEventAttributes{
						Details: []byte{1, 2, 3, 4, 5},
					},
				},
			},
		},
	}
	mockHistoryBlobReader.On("GetBlob", mock.Anything).Return(uploadedBlob, nil)
	mockDownloader := &HistoryBlobDownloaderMock{}
	fetchedBlob := &HistoryBlob{
		Header: &HistoryBlobHeader{
			LastFailoverVersion: common.Int64Ptr(testCloseFailoverVersion),
			LastEventID:         common.Int64Ptr(testNextEventID - 1),
			IsLast:              common.BoolPtr(true),
		},
		Body: &shared.History{
			Events: []*shared.HistoryEvent{
				{
					EventId:   common.Int64Ptr(7),
					EventType: eventTypePtr(shared.EventTypeChildWorkflowExecutionCanceled),
					ChildWorkflowExecutionCanceledEventAttributes: &shared.ChildWorkflowExecutionCanceledEventAttributes{
						Details: []byte{1, 2, 3, 4},
					},
				},
			},
		},
	}
	mockDownloader.On("DownloadBlob", mock.Anything, mock.Anything).Return(&DownloadBlobResponse{
		NextPageToken: nil,
		HistoryBlob:   fetchedBlob,
	}, nil).Once()
	container := &BootstrapContainer{
		Logger:                s.logger,
		MetricsClient:         s.metricsClient,
		DomainCache:           domainCache,
		ClusterMetadata:       mockClusterMetadata,
		Blobstore:             mockBlobstore,
		HistoryBlobReader:     mockHistoryBlobReader,
		Config:                getConfig(false, true),
		HistoryBlobDownloader: mockDownloader,
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
	encodedResult, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.NoError(err)

	result := s.getDecodedUploadResult(encodedResult)
	s.Empty(result)
}

func (s *historyBlobUploaderSuite) TestUploadHistoryActivity_Success_BlobIntegrityCheckPassed() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverRunningBlobIntegrityCheckCount).Once()
	domainCache, mockClusterMetadata := s.archivalConfig(true, testArchivalBucket, true)
	mockBlobstore := &mocks.BlobstoreClient{}
	mockBlobstore.On("GetTags", mock.Anything, mock.Anything, mock.Anything).Return(nil, blobstore.ErrBlobNotExists)
	mockBlobstore.On("Upload", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockHistoryBlobReader := &HistoryBlobReaderMock{}
	firstUploadedBlob := &HistoryBlob{
		Header: &HistoryBlobHeader{
			LastFailoverVersion: common.Int64Ptr(testCloseFailoverVersion),
			LastEventID:         common.Int64Ptr(testNextEventID - 2),
			NextPageToken:       common.IntPtr(common.FirstBlobPageToken + 1),
			IsLast:              common.BoolPtr(false),
		},
		Body: &shared.History{
			Events: []*shared.HistoryEvent{
				{
					EventId:   common.Int64Ptr(6),
					EventType: eventTypePtr(shared.EventTypeChildWorkflowExecutionCanceled),
				},
			},
		},
	}
	secondUploadedBlob := &HistoryBlob{
		Header: &HistoryBlobHeader{
			LastFailoverVersion: common.Int64Ptr(testCloseFailoverVersion),
			LastEventID:         common.Int64Ptr(testNextEventID - 1),
			IsLast:              common.BoolPtr(true),
		},
		Body: &shared.History{
			Events: []*shared.HistoryEvent{
				{
					EventId:   common.Int64Ptr(7),
					EventType: eventTypePtr(shared.EventTypeChildWorkflowExecutionCanceled),
				},
			},
		},
	}
	mockHistoryBlobReader.On("GetBlob", common.FirstBlobPageToken).Return(firstUploadedBlob, nil).Once()
	mockHistoryBlobReader.On("GetBlob", common.FirstBlobPageToken+1).Return(secondUploadedBlob, nil).Once()
	firstFetchReq := &DownloadBlobRequest{
		DomainID:             testDomainID,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		ArchivalBucket:       testArchivalBucket,
		CloseFailoverVersion: common.Int64Ptr(testCloseFailoverVersion),
	}
	secondBlobPageToken, err := serializeArchivalToken(&archivalToken{
		BlobstorePageToken:   common.FirstBlobPageToken + 1,
		CloseFailoverVersion: testCloseFailoverVersion,
	})
	s.NoError(err)
	secondFetchReq := &DownloadBlobRequest{
		DomainID:             testDomainID,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		ArchivalBucket:       testArchivalBucket,
		CloseFailoverVersion: common.Int64Ptr(testCloseFailoverVersion),
		NextPageToken:        secondBlobPageToken,
	}
	firstFetchResp := &DownloadBlobResponse{
		NextPageToken: secondBlobPageToken,
		HistoryBlob: &HistoryBlob{
			Header: &HistoryBlobHeader{
				LastFailoverVersion: common.Int64Ptr(testCloseFailoverVersion),
				LastEventID:         common.Int64Ptr(testNextEventID - 2),
				IsLast:              common.BoolPtr(false),
			},
			Body: &shared.History{
				Events: []*shared.HistoryEvent{
					{
						EventId:   common.Int64Ptr(6),
						EventType: eventTypePtr(shared.EventTypeChildWorkflowExecutionCanceled),
					},
				},
			},
		},
	}
	secondFetchResp := &DownloadBlobResponse{
		HistoryBlob: &HistoryBlob{
			Header: &HistoryBlobHeader{
				LastFailoverVersion: common.Int64Ptr(testCloseFailoverVersion),
				LastEventID:         common.Int64Ptr(testNextEventID - 1),
				IsLast:              common.BoolPtr(true),
			},
			Body: &shared.History{
				Events: []*shared.HistoryEvent{
					{
						EventId:   common.Int64Ptr(7),
						EventType: eventTypePtr(shared.EventTypeChildWorkflowExecutionCanceled),
					},
				},
			},
		},
	}
	mockDownloader := &HistoryBlobDownloaderMock{}
	mockDownloader.On("DownloadBlob", mock.Anything, firstFetchReq).Return(firstFetchResp, nil).Once()
	mockDownloader.On("DownloadBlob", mock.Anything, secondFetchReq).Return(secondFetchResp, nil).Once()
	container := &BootstrapContainer{
		Logger:                s.logger,
		MetricsClient:         s.metricsClient,
		DomainCache:           domainCache,
		ClusterMetadata:       mockClusterMetadata,
		Blobstore:             mockBlobstore,
		HistoryBlobReader:     mockHistoryBlobReader,
		Config:                getConfig(false, true),
		HistoryBlobDownloader: mockDownloader,
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
	encodedResult, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.NoError(err)

	result := s.getDecodedUploadResult(encodedResult)
	s.Empty(result)
}

func (s *historyBlobUploaderSuite) TestUploadHistoryActivity_Fail_AfterUploadedSomeBlobs() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverNonRetryableErrorCount).Once()
	domainCache, mockClusterMetadata := s.archivalConfig(true, testArchivalBucket, true)

	pageToken := common.FirstBlobPageToken
	firstBlobKey, err := NewHistoryBlobKey(testDomainID, testWorkflowID, testRunID, testCloseFailoverVersion, pageToken)
	s.Nil(err)
	secondBlobKey, err := NewHistoryBlobKey(testDomainID, testWorkflowID, testRunID, testCloseFailoverVersion, pageToken+1)
	s.Nil(err)

	mockBlobstore := &mocks.BlobstoreClient{}
	mockBlobstore.On("GetTags", mock.Anything, mock.Anything, mock.Anything).Return(nil, blobstore.ErrBlobNotExists).Twice()
	mockBlobstore.On("Upload", mock.Anything, mock.Anything, firstBlobKey, mock.Anything).Return(nil)
	mockBlobstore.On("Upload", mock.Anything, mock.Anything, secondBlobKey, mock.Anything).Return(errors.New(testErrDetails))
	mockBlobstore.On("IsRetryableError", mock.Anything).Return(false)
	mockHistoryBlobReader := &HistoryBlobReaderMock{}
	mockHistoryBlobReader.On("GetBlob", mock.Anything).Return(&HistoryBlob{
		Header: &HistoryBlobHeader{IsLast: common.BoolPtr(false)},
	}, nil)
	container := &BootstrapContainer{
		Logger:            s.logger,
		MetricsClient:     s.metricsClient,
		DomainCache:       domainCache,
		ClusterMetadata:   mockClusterMetadata,
		Blobstore:         mockBlobstore,
		HistoryBlobReader: mockHistoryBlobReader,
		Config:            getConfig(false, false),
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
	encodedResult, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.NoError(err)

	result := s.getDecodedUploadResult(encodedResult)
	s.Equal(fmt.Sprintf("%v: %v", errUploadBlob, testErrDetails), result.ErrorWithDetails)
	s.Equal([]string{firstBlobKey.String()}, result.BlobsToDelete)
}

func (s *historyBlobUploaderSuite) TestUploadHistoryActivity_Fail_ContinueFromPreviousProgress() {
	s.metricsClient.On("Scope", metrics.ArchiverUploadHistoryActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverNonRetryableErrorCount).Once()
	domainCache, mockClusterMetadata := s.archivalConfig(true, testArchivalBucket, true)

	pageToken := common.FirstBlobPageToken
	thirdBlobKey, err := NewHistoryBlobKey(testDomainID, testWorkflowID, testRunID, testCloseFailoverVersion, pageToken+2)
	s.Nil(err)
	fourthBlobKey, err := NewHistoryBlobKey(testDomainID, testWorkflowID, testRunID, testCloseFailoverVersion, pageToken+3)
	s.Nil(err)

	mockBlobstore := &mocks.BlobstoreClient{}
	mockBlobstore.On("GetTags", mock.Anything, mock.Anything, mock.Anything).Return(nil, blobstore.ErrBlobNotExists).Twice()
	mockBlobstore.On("Upload", mock.Anything, mock.Anything, thirdBlobKey, mock.Anything).Return(nil)
	mockBlobstore.On("Upload", mock.Anything, mock.Anything, fourthBlobKey, mock.Anything).Return(errors.New(testErrDetails))
	mockBlobstore.On("IsRetryableError", mock.Anything).Return(false)
	mockHistoryBlobReader := &HistoryBlobReaderMock{}
	mockHistoryBlobReader.On("GetBlob", mock.Anything).Return(&HistoryBlob{
		Header: &HistoryBlobHeader{IsLast: common.BoolPtr(false)},
	}, nil)
	container := &BootstrapContainer{
		Logger:            s.logger,
		MetricsClient:     s.metricsClient,
		DomainCache:       domainCache,
		ClusterMetadata:   mockClusterMetadata,
		Blobstore:         mockBlobstore,
		HistoryBlobReader: mockHistoryBlobReader,
		Config:            getConfig(false, false),
	}
	env := s.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), bootstrapContainerKey, container),
	})
	uploadedBlobs := []string{"blob key 1", "blob key 2"}
	iterState, err := json.Marshal(historyBlobIteratorState{
		BlobPageToken:     common.FirstBlobPageToken + 1,
		FinishedIteration: false,
	})
	s.NoError(err)
	env.SetHeartbeatDetails(uploadProgress{
		UploadedBlobs:   uploadedBlobs,
		BlobPageToken:   common.FirstBlobPageToken + 1,
		HandledLastBlob: false,
		IteratorState:   iterState,
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
	encodedResult, err := env.ExecuteActivity(uploadHistoryActivity, request)
	s.NoError(err)

	result := s.getDecodedUploadResult(encodedResult)
	s.Equal(fmt.Sprintf("%v: %v", errUploadBlob, testErrDetails), result.ErrorWithDetails)
	uploadedBlobs = append(uploadedBlobs, thirdBlobKey.String())
	s.Equal(uploadedBlobs, result.BlobsToDelete)
}

func (s *historyBlobUploaderSuite) archivalConfig(
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

func (s *historyBlobUploaderSuite) getDecodedUploadResult(encodedResult encoded.Value) *uploadResult {
	if !encodedResult.HasValue() {
		return nil
	}
	result := &uploadResult{}
	err := encodedResult.Get(&result)
	s.NoError(err)
	return result
}
