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

package sysworkflow

import (
	"context"
	"errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	metricsMocks "github.com/uber/cadence/common/metrics/mocks"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"go.uber.org/cadence/testsuite"
	"go.uber.org/cadence/worker"
	"testing"
)

const (
	testArchivalBucket     = "test-archival-bucket"
	testCurrentClusterName = "test-current-cluster-name"
)

type ArchiveSystemWorkflowSuite struct {
	*require.Assertions
	suite.Suite
	logger        bark.Logger
	metricsClient *metricsMocks.Client
}

func TestSystemWorkflowSuite(t *testing.T) {
	suite.Run(t, new(ArchiveSystemWorkflowSuite))
}

func (s *ArchiveSystemWorkflowSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.logger = bark.NewNopLogger()
	s.metricsClient = &metricsMocks.Client{}
	s.metricsClient.On("IncCounter", mock.Anything, mock.Anything)
}

func (s *ArchiveSystemWorkflowSuite) TestArchivalUploadActivity_Fail_GetDomainByID() {
	domainCache := &cache.DomainCacheMock{}
	domainCache.On("GetDomainByID", mock.Anything).Return(nil, errors.New("failed to get domain cache entry"))
	container := &SysWorkerContainer{
		Logger:        s.logger,
		MetricsClient: s.metricsClient,
		DomainCache:   domainCache,
	}
	wts := testsuite.WorkflowTestSuite{}
	env := wts.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), sysWorkerContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:             testDomainID,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	_, err := env.ExecuteActivity("ArchivalUploadActivity", request)
	s.Error(err)
	s.Contains(err.Error(), "failed to get domain from domain cache")
}

func (s *ArchiveSystemWorkflowSuite) TestArchivalUploadActivity_Nop_ClusterNotEnablesArchival() {
	domainCache, mockClusterMetadata := s.domainCache(false, true)
	container := &SysWorkerContainer{
		Logger:          s.logger,
		MetricsClient:   s.metricsClient,
		DomainCache:     domainCache,
		ClusterMetadata: mockClusterMetadata,
	}
	wts := testsuite.WorkflowTestSuite{}
	env := wts.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), sysWorkerContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:             testDomainID,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	_, err := env.ExecuteActivity("ArchivalUploadActivity", request)

	// blobstore was not used at all meaning that operation was a nop
	s.NoError(err)
}

func (s *ArchiveSystemWorkflowSuite) TestArchivalUploadActivity_Nop_DomainNotEnablesArchival() {
	domainCache, mockClusterMetadata := s.domainCache(true, false)
	container := &SysWorkerContainer{
		Logger:          s.logger,
		MetricsClient:   s.metricsClient,
		DomainCache:     domainCache,
		ClusterMetadata: mockClusterMetadata,
	}
	wts := testsuite.WorkflowTestSuite{}
	env := wts.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), sysWorkerContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:             testDomainID,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	_, err := env.ExecuteActivity("ArchivalUploadActivity", request)

	// blobstore was not used at all meaning that operation was a nop
	s.NoError(err)
}

func (s *ArchiveSystemWorkflowSuite) TestArchiveUploadActivity_Fail_CannotGetNextHistoryBlob() {
	domainCache, mockClusterMetadata := s.domainCache(true, true)
	mockHistoryBlobIterator := &HistoryBlobIteratorMock{}
	mockHistoryBlobIterator.On("HasNext").Return(true)
	mockHistoryBlobIterator.On("Next").Return(nil, errors.New("non-retryable error getting history blob"))
	container := &SysWorkerContainer{
		Logger:              s.logger,
		MetricsClient:       s.metricsClient,
		DomainCache:         domainCache,
		ClusterMetadata:     mockClusterMetadata,
		HistoryBlobIterator: mockHistoryBlobIterator,
	}
	wts := testsuite.WorkflowTestSuite{}
	env := wts.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), sysWorkerContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:             testDomainID,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	_, err := env.ExecuteActivity("ArchivalUploadActivity", request)
	s.Error(err)
	s.Contains(err.Error(), "failed to get next blob from iterator")
}

func (s *ArchiveSystemWorkflowSuite) TestArchiveUploadActivity_Fail_CannotConstructBlobKey() {
	domainCache, mockClusterMetadata := s.domainCache(true, true)
	mockHistoryBlobIterator := &HistoryBlobIteratorMock{}
	mockHistoryBlobIterator.On("HasNext").Return(true)
	historyBlob := &HistoryBlob{
		Header: &HistoryBlobHeader{
			CurrentPageToken: common.IntPtr(1),
		},
	}
	mockHistoryBlobIterator.On("Next").Return(historyBlob, nil)
	container := &SysWorkerContainer{
		Logger:              s.logger,
		MetricsClient:       s.metricsClient,
		DomainCache:         domainCache,
		ClusterMetadata:     mockClusterMetadata,
		HistoryBlobIterator: mockHistoryBlobIterator,
	}
	wts := testsuite.WorkflowTestSuite{}
	env := wts.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), sysWorkerContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:             testDomainID,
		WorkflowID:           "", // this causes an error when creating the blob key
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	_, err := env.ExecuteActivity("ArchivalUploadActivity", request)
	s.Error(err)
	s.Contains(err.Error(), "failed to construct blob key")
}

func (s *ArchiveSystemWorkflowSuite) TestArchiveUploadActivity_Fail_CannotCheckBlobExists() {
	domainCache, mockClusterMetadata := s.domainCache(true, true)
	mockHistoryBlobIterator := &HistoryBlobIteratorMock{}
	mockHistoryBlobIterator.On("HasNext").Return(true)
	historyBlob := &HistoryBlob{
		Header: &HistoryBlobHeader{
			CurrentPageToken: common.IntPtr(1),
		},
	}
	mockHistoryBlobIterator.On("Next").Return(historyBlob, nil)
	mockBlobstore := &mocks.BlobstoreClient{}
	mockBlobstore.On("Exists", mock.Anything, mock.Anything, mock.Anything).Return(false, &shared.BadRequestError{Message: "non-retryable blobstore error"})
	container := &SysWorkerContainer{
		Logger:              s.logger,
		MetricsClient:       s.metricsClient,
		DomainCache:         domainCache,
		ClusterMetadata:     mockClusterMetadata,
		HistoryBlobIterator: mockHistoryBlobIterator,
		Blobstore:           mockBlobstore,
	}
	wts := testsuite.WorkflowTestSuite{}
	env := wts.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), sysWorkerContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:             testDomainID,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	_, err := env.ExecuteActivity("ArchivalUploadActivity", request)
	s.Error(err)
	s.Contains(err.Error(), "failed to check if blob exists already")
}

func (s *ArchiveSystemWorkflowSuite) TestArchiveUploadActivity_Nop_BlobAlreadyExists() {
	domainCache, mockClusterMetadata := s.domainCache(true, true)
	mockHistoryBlobIterator := &HistoryBlobIteratorMock{}
	mockHistoryBlobIterator.On("HasNext").Return(true).Once()
	mockHistoryBlobIterator.On("HasNext").Return(false).Once()
	historyBlob := &HistoryBlob{
		Header: &HistoryBlobHeader{
			CurrentPageToken: common.IntPtr(1),
		},
	}
	mockHistoryBlobIterator.On("Next").Return(historyBlob, nil)
	mockBlobstore := &mocks.BlobstoreClient{}
	mockBlobstore.On("Exists", mock.Anything, mock.Anything, mock.Anything).Return(true, nil)
	container := &SysWorkerContainer{
		Logger:              s.logger,
		MetricsClient:       s.metricsClient,
		DomainCache:         domainCache,
		ClusterMetadata:     mockClusterMetadata,
		HistoryBlobIterator: mockHistoryBlobIterator,
		Blobstore:           mockBlobstore,
	}
	wts := testsuite.WorkflowTestSuite{}
	env := wts.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), sysWorkerContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:             testDomainID,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	_, err := env.ExecuteActivity("ArchivalUploadActivity", request)
	s.NoError(err)
	mockBlobstore.AssertNotCalled(s.T(), "Upload", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func (s *ArchiveSystemWorkflowSuite) TestArchiveUploadActivity_Fail_CannotUploadBlob() {
	domainCache, mockClusterMetadata := s.domainCache(true, true)
	mockHistoryBlobIterator := &HistoryBlobIteratorMock{}
	mockHistoryBlobIterator.On("HasNext").Return(true).Once()
	mockHistoryBlobIterator.On("HasNext").Return(false).Once()
	historyBlob := &HistoryBlob{
		Header: &HistoryBlobHeader{
			CurrentPageToken: common.IntPtr(1),
		},
	}
	mockHistoryBlobIterator.On("Next").Return(historyBlob, nil)
	mockBlobstore := &mocks.BlobstoreClient{}
	mockBlobstore.On("Exists", mock.Anything, mock.Anything, mock.Anything).Return(false, nil)
	mockBlobstore.On("Upload", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&shared.BadRequestError{Message: "non-retryable error on upload"})
	container := &SysWorkerContainer{
		Logger:              s.logger,
		MetricsClient:       s.metricsClient,
		DomainCache:         domainCache,
		ClusterMetadata:     mockClusterMetadata,
		HistoryBlobIterator: mockHistoryBlobIterator,
		Blobstore:           mockBlobstore,
		Config:              constructConfig(testDefaultPersistencePageSize, testDefaultTargetArchivalBlobSize),
	}
	wts := testsuite.WorkflowTestSuite{}
	env := wts.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), sysWorkerContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:             testDomainID,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	_, err := env.ExecuteActivity("ArchivalUploadActivity", request)
	s.Error(err)
	s.Contains(err.Error(), "failed to upload blob")
}

func (s *ArchiveSystemWorkflowSuite) TestArchiveUploadActivity_Success() {
	domainCache, mockClusterMetadata := s.domainCache(true, true)
	mockHistoryBlobIterator := &HistoryBlobIteratorMock{}
	mockHistoryBlobIterator.On("HasNext").Return(true).Once()
	mockHistoryBlobIterator.On("HasNext").Return(false).Once()
	historyBlob := &HistoryBlob{
		Header: &HistoryBlobHeader{
			CurrentPageToken: common.IntPtr(1),
		},
	}
	mockHistoryBlobIterator.On("Next").Return(historyBlob, nil)
	mockBlobstore := &mocks.BlobstoreClient{}
	mockBlobstore.On("Exists", mock.Anything, mock.Anything, mock.Anything).Return(false, nil)
	mockBlobstore.On("Upload", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	container := &SysWorkerContainer{
		Logger:              s.logger,
		MetricsClient:       s.metricsClient,
		DomainCache:         domainCache,
		ClusterMetadata:     mockClusterMetadata,
		HistoryBlobIterator: mockHistoryBlobIterator,
		Blobstore:           mockBlobstore,
		Config:              constructConfig(testDefaultPersistencePageSize, testDefaultTargetArchivalBlobSize),
	}
	wts := testsuite.WorkflowTestSuite{}
	env := wts.NewTestActivityEnvironment()
	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.WithValue(context.Background(), sysWorkerContainerKey, container),
	})
	request := ArchiveRequest{
		DomainID:             testDomainID,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	_, err := env.ExecuteActivity("ArchivalUploadActivity", request)
	s.NoError(err)
	mockBlobstore.AssertCalled(s.T(), "Upload", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func (s *ArchiveSystemWorkflowSuite) domainCache(domainEnablesArchival, clusterEnablesArchival bool) (cache.DomainCache, cluster.Metadata) {
	domainArchivalStatus := shared.ArchivalStatusDisabled
	if domainEnablesArchival {
		domainArchivalStatus = shared.ArchivalStatusEnabled
	}
	mockMetadataMgr := &mocks.MetadataManager{}
	mockClusterMetadata := &mocks.ClusterMetadata{}
	mockClusterMetadata.On("IsArchivalEnabled").Return(clusterEnablesArchival)
	mockClusterMetadata.On("IsGlobalDomainEnabled").Return(false)
	mockClusterMetadata.On("GetCurrentClusterName").Return(testCurrentClusterName)
	mockMetadataMgr.On("GetDomain", mock.Anything).Return(
		&persistence.GetDomainResponse{
			Info: &persistence.DomainInfo{ID: testDomainID, Name: testDomain},
			Config: &persistence.DomainConfig{
				Retention:      1,
				ArchivalBucket: testArchivalBucket,
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
	return cache.NewDomainCache(mockMetadataMgr, mockClusterMetadata, s.metricsClient, s.logger), mockClusterMetadata
}
