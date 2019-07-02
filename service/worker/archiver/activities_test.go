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

	testErrDetails = "some error"
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
	s.metricsScope.On("RecordTimer", mock.Anything, mock.Anything).Maybe()
}

func (s *activitiesSuite) TearDownTest() {
	s.metricsClient.AssertExpectations(s.T())
	s.metricsScope.AssertExpectations(s.T())
}

func (s *activitiesSuite) TestDeleteBlobActivity_Fail_DeleteIndexBlobError() {
	s.metricsClient.On("Scope", metrics.ArchiverDeleteBlobActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()
	s.metricsScope.On("IncCounter", metrics.ArchiverNonRetryableErrorCount).Once()

	indexBlobKey, err := NewHistoryIndexBlobKey(testDomainID, testWorkflowID, testRunID)
	s.Nil(err)
	mockBlobstore := &mocks.BlobstoreClient{}
	mockBlobstore.On("GetTags", mock.Anything, testArchivalBucket, indexBlobKey).Return(nil, errors.New(testErrDetails)).Once()
	mockBlobstore.On("IsRetryableError", mock.Anything).Return(false).Once()
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
	_, err = env.ExecuteActivity(deleteBlobActivity, request, []string{})
	s.Equal(errGetTags, err.Error())
}

func (s *activitiesSuite) TestDeleteBlobActivity_Success_IndexBlobNotExist() {
	s.metricsClient.On("Scope", metrics.ArchiverDeleteBlobActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()

	indexBlobKey, err := NewHistoryIndexBlobKey(testDomainID, testWorkflowID, testRunID)
	s.Nil(err)
	blobKey, err := NewHistoryBlobKey(testDomainID, testWorkflowID, testRunID, testCloseFailoverVersion, common.FirstBlobPageToken)
	s.Nil(err)
	mockBlobstore := &mocks.BlobstoreClient{}
	mockBlobstore.On("GetTags", mock.Anything, testArchivalBucket, indexBlobKey).Return(nil, blobstore.ErrBlobNotExists).Once()
	mockBlobstore.On("Delete", mock.Anything, testArchivalBucket, blobKey).Return(true, nil).Once()
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
	_, err = env.ExecuteActivity(deleteBlobActivity, request, []string{blobKey.String()})
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
	_, err := env.ExecuteActivity(deleteBlobActivity, request, []string{})
	s.Equal(errConstructKey, err.Error())
}

func (s *activitiesSuite) TestDeleteBlobActivity_Success_DeleteBlobError() {
	s.metricsClient.On("Scope", metrics.ArchiverDeleteBlobActivityScope, []metrics.Tag{metrics.DomainTag(testDomainName)}).Return(s.metricsScope).Once()

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
	_, err = env.ExecuteActivity(deleteBlobActivity, request, []string{blobKey.String()})
	s.NoError(err)
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
	_, err = env.ExecuteActivity(deleteBlobActivity, request, []string{firstBlobKey.String(), secondBlobKey.String()})
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
	_, err = env.ExecuteActivity(deleteBlobActivity, request, []string{"random key 1", "random key 2", thirdBlobKey.String(), fourthBlobKey.String()})
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
	blobsToDelete := []string{"random key1", "random key2", thirdBlobKey.String(), fourthBlobKey.String(), fifthBlobKey.String()}
	_, err = env.ExecuteActivity(deleteBlobActivity, request, blobsToDelete)
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

func getConfig(constCheck, integrityCheck bool) *Config {
	constCheckProbability := 0.0
	if constCheck {
		constCheckProbability = 1.0
	}
	integrityCheckProbability := 0.0
	if integrityCheck {
		integrityCheckProbability = 1.0
	}
	return &Config{
		DeterministicConstructionCheckProbability: dynamicconfig.GetFloatPropertyFn(constCheckProbability),
		BlobIntegrityCheckProbability:             dynamicconfig.GetFloatPropertyFn(integrityCheckProbability),
		EnableArchivalCompression:                 dynamicconfig.GetBoolPropertyFnFilteredByDomain(true),
	}
}

func getCanceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func eventTypePtr(e shared.EventType) *shared.EventType {
	return &e
}
