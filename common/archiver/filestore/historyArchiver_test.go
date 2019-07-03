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

package filestore

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"go.uber.org/zap"
)

const (
	testDomainID             = "test-domain-id"
	testDomainName           = "test-domain-name"
	testWorkflowID           = "test-workflow-id"
	testRunID                = "test-run-id"
	testNextEventID          = 1800
	testCloseFailoverVersion = 100
	testPageSize             = 100
)

var (
	testBranchToken = []byte{1, 2, 3}
)

type historyArchiverSuite struct {
	*require.Assertions
	suite.Suite

	container          archiver.HistoryBootstrapContainer
	logger             log.Logger
	testGetDirectory   string
	historyBatchesV1   []*shared.History
	historyBatchesV100 []*shared.History
}

func TestHistoryArchiverSuite(t *testing.T) {
	suite.Run(t, new(historyArchiverSuite))
}

func (s *historyArchiverSuite) SetupSuite() {
	var err error
	s.testGetDirectory, err = ioutil.TempDir("", "TestGet")
	s.Require().NoError(err)
	s.setupHistoryDirectory()
}

func (s *historyArchiverSuite) TearDownSuite() {
	os.RemoveAll(s.testGetDirectory)
}

func (s *historyArchiverSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	zapLogger := zap.NewNop()
	s.container = archiver.HistoryBootstrapContainer{
		Logger: loggerimpl.NewLogger(zapLogger),
	}
}

func (s *historyArchiverSuite) TestValidateURI() {
	testCases := []struct {
		URI         string
		expectedErr error
	}{
		{
			URI:         "wrongscheme:///a/b/c",
			expectedErr: archiver.ErrInvalidURIScheme,
		},
		{
			URI:         "",
			expectedErr: archiver.ErrInvalidURIScheme,
		},
		{
			URI:         "file://",
			expectedErr: errEmptyDirectoryPath,
		},
		{
			URI:         "file:///a/b/c",
			expectedErr: nil,
		},
	}

	historyArchiver := newHistoryArchiver(s.container, nil, nil)
	for _, tc := range testCases {
		s.Equal(tc.expectedErr, historyArchiver.ValidateURI(tc.URI))
	}
}

func (s *historyArchiverSuite) TestArchive_Fail_InvalidURI() {
	historyArchiver := newHistoryArchiver(s.container, nil, nil)
	request := &archiver.ArchiveHistoryRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	err := historyArchiver.Archive(context.Background(), "wrongscheme://", request)
	s.Error(err)
}

func (s *historyArchiverSuite) TestArchive_Fail_InvalidRequest() {
	historyArchiver := newHistoryArchiver(s.container, nil, nil)
	request := &archiver.ArchiveHistoryRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           "", // an invalid request
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	err := historyArchiver.Archive(context.Background(), "file:///a/b/c", request)
	s.Error(err)
}

func (s *historyArchiverSuite) TestArchive_Fail_ErrorOnReadHistory() {
	mockCtrl := gomock.NewController(s.T())
	defer mockCtrl.Finish()
	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)
	gomock.InOrder(
		historyIterator.EXPECT().HasNext().Return(true),
		historyIterator.EXPECT().Next().Return(nil, errors.New("some random error")),
	)

	historyArchiver := newHistoryArchiver(s.container, nil, historyIterator)
	request := &archiver.ArchiveHistoryRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	err := historyArchiver.Archive(context.Background(), "file:///a/b/c", request)
	s.Error(err)
}

func (s *historyArchiverSuite) TestArchive_Fail_TimeoutWhenReadingHistory() {
	mockCtrl := gomock.NewController(s.T())
	defer mockCtrl.Finish()
	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)
	gomock.InOrder(
		historyIterator.EXPECT().HasNext().Return(true),
		historyIterator.EXPECT().Next().Return(nil, &shared.ServiceBusyError{}),
	)

	historyArchiver := newHistoryArchiver(s.container, nil, historyIterator)
	request := &archiver.ArchiveHistoryRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	err := historyArchiver.Archive(getCanceledContext(), "file:///a/b/c", request)
	s.Error(err)
}

func (s *historyArchiverSuite) TestArchive_Fail_HistoryMutated() {
	mockCtrl := gomock.NewController(s.T())
	defer mockCtrl.Finish()
	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)
	historyBatches := []*shared.History{
		&shared.History{
			Events: []*shared.HistoryEvent{
				&shared.HistoryEvent{
					EventId:   common.Int64Ptr(common.FirstEventID + 1),
					Timestamp: common.Int64Ptr(time.Now().UnixNano()),
					Version:   common.Int64Ptr(testCloseFailoverVersion + 1),
				},
			},
		},
	}
	historyBlob := &archiver.HistoryBlob{
		Header: &archiver.HistoryBlobHeader{
			IsLast: common.BoolPtr(true),
		},
		Body: historyBatches,
	}
	gomock.InOrder(
		historyIterator.EXPECT().HasNext().Return(true),
		historyIterator.EXPECT().Next().Return(historyBlob, nil),
	)

	historyArchiver := newHistoryArchiver(s.container, nil, historyIterator)
	request := &archiver.ArchiveHistoryRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	err := historyArchiver.Archive(context.Background(), "file:///a/b/c", request)
	s.Error(err)
}

func (s *historyArchiverSuite) TestArchive_Success() {
	mockCtrl := gomock.NewController(s.T())
	defer mockCtrl.Finish()
	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)
	historyBatches := []*shared.History{
		&shared.History{
			Events: []*shared.HistoryEvent{
				&shared.HistoryEvent{
					EventId:   common.Int64Ptr(common.FirstEventID + 1),
					Timestamp: common.Int64Ptr(time.Now().UnixNano()),
					Version:   common.Int64Ptr(testCloseFailoverVersion),
				},
				&shared.HistoryEvent{
					EventId:   common.Int64Ptr(common.FirstEventID + 2),
					Timestamp: common.Int64Ptr(time.Now().UnixNano()),
					Version:   common.Int64Ptr(testCloseFailoverVersion),
				},
			},
		},
		&shared.History{
			Events: []*shared.HistoryEvent{
				&shared.HistoryEvent{
					EventId:   common.Int64Ptr(testNextEventID - 1),
					Timestamp: common.Int64Ptr(time.Now().UnixNano()),
					Version:   common.Int64Ptr(testCloseFailoverVersion),
				},
			},
		},
	}
	historyBlob := &archiver.HistoryBlob{
		Header: &archiver.HistoryBlobHeader{
			IsLast: common.BoolPtr(true),
		},
		Body: historyBatches,
	}
	gomock.InOrder(
		historyIterator.EXPECT().HasNext().Return(true),
		historyIterator.EXPECT().Next().Return(historyBlob, nil),
		historyIterator.EXPECT().HasNext().Return(false),
	)

	dir, err := ioutil.TempDir("", "TestArchiveSingeRead")
	s.NoError(err)
	defer os.RemoveAll(dir)

	historyArchiver := newHistoryArchiver(s.container, nil, historyIterator)
	request := &archiver.ArchiveHistoryRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	err = historyArchiver.Archive(context.Background(), "file://"+dir, request)
	s.NoError(err)

	expectedFilename := constructFilename(testDomainID, testWorkflowID, testRunID, testCloseFailoverVersion)
	s.assertFileExists(path.Join(dir, expectedFilename))
}

func (s *historyArchiverSuite) TestGet_Fail_InvalidURI() {
	historyArchiver := newHistoryArchiver(s.container, nil, nil)
	request := &archiver.GetHistoryRequest{
		DomainID:   testDomainID,
		WorkflowID: testWorkflowID,
		RunID:      testRunID,
		PageSize:   100,
	}
	response, err := historyArchiver.Get(context.Background(), "wrongscheme://", request)
	s.Nil(response)
	s.Error(err)
}

func (s *historyArchiverSuite) TestGet_Fail_InvalidRequest() {
	historyArchiver := newHistoryArchiver(s.container, nil, nil)
	request := &archiver.GetHistoryRequest{
		DomainID:   testDomainID,
		WorkflowID: testWorkflowID,
		RunID:      testRunID,
		PageSize:   0, // pageSize shoulbe greater than 0
	}
	response, err := historyArchiver.Get(context.Background(), "file:///a/b/c", request)
	s.Nil(response)
	s.Equal(archiver.ErrInvalidGetHistoryRequest, err)
}

func (s *historyArchiverSuite) TestGet_Fail_DirectoryNotExist() {
	historyArchiver := newHistoryArchiver(s.container, nil, nil)
	request := &archiver.GetHistoryRequest{
		DomainID:   testDomainID,
		WorkflowID: testWorkflowID,
		RunID:      testRunID,
		PageSize:   testPageSize,
	}
	response, err := historyArchiver.Get(context.Background(), "file:///a/b/c", request)
	s.Nil(response)
	s.Equal(archiver.ErrHistoryNotExist, err)
}

func (s *historyArchiverSuite) TestGet_Fail_InvalidToken() {
	historyArchiver := newHistoryArchiver(s.container, nil, nil)
	request := &archiver.GetHistoryRequest{
		DomainID:      testDomainID,
		WorkflowID:    testWorkflowID,
		RunID:         testRunID,
		PageSize:      testPageSize,
		NextPageToken: []byte{'r', 'a', 'n', 'd', 'o', 'm'},
	}
	response, err := historyArchiver.Get(context.Background(), "file://.", request)
	s.Nil(response)
	s.Equal(archiver.ErrGetHistoryTokenCorrupted, err)
}

func (s *historyArchiverSuite) TestGet_Fail_FileNotExist() {
	historyArchiver := newHistoryArchiver(s.container, nil, nil)
	request := &archiver.GetHistoryRequest{
		DomainID:             testDomainID,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		PageSize:             testPageSize,
		CloseFailoverVersion: common.Int64Ptr(testCloseFailoverVersion),
	}
	response, err := historyArchiver.Get(context.Background(), "file://.", request)
	s.Nil(response)
	s.Equal(archiver.ErrHistoryNotExist, err)
}

func (s *historyArchiverSuite) TestGet_Success_PickHighestVersion() {
	historyArchiver := newHistoryArchiver(s.container, nil, nil)
	request := &archiver.GetHistoryRequest{
		DomainID:   testDomainID,
		WorkflowID: testWorkflowID,
		RunID:      testRunID,
		PageSize:   testPageSize,
	}
	response, err := historyArchiver.Get(context.Background(), "file://"+s.testGetDirectory, request)
	s.NoError(err)
	s.Nil(response.NextPageToken)
	s.Equal(s.historyBatchesV100, response.HistoryBatches)
}

func (s *historyArchiverSuite) TestGet_Success_UseProvidedVersion() {
	historyArchiver := newHistoryArchiver(s.container, nil, nil)
	request := &archiver.GetHistoryRequest{
		DomainID:             testDomainID,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		PageSize:             testPageSize,
		CloseFailoverVersion: common.Int64Ptr(1),
	}
	response, err := historyArchiver.Get(context.Background(), "file://"+s.testGetDirectory, request)
	s.NoError(err)
	s.Nil(response.NextPageToken)
	s.Equal(s.historyBatchesV1, response.HistoryBatches)
}

func (s *historyArchiverSuite) TestGet_Success_SmallPageSize() {
	historyArchiver := newHistoryArchiver(s.container, nil, nil)
	request := &archiver.GetHistoryRequest{
		DomainID:             testDomainID,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		PageSize:             1,
		CloseFailoverVersion: common.Int64Ptr(100),
	}
	combinedHistory := []*shared.History{}

	response, err := historyArchiver.Get(context.Background(), "file://"+s.testGetDirectory, request)
	s.NoError(err)
	s.NotNil(response)
	s.NotNil(response.NextPageToken)
	s.NotNil(response.HistoryBatches)
	s.Len(response.HistoryBatches, 1)
	combinedHistory = append(combinedHistory, response.HistoryBatches...)

	request.NextPageToken = response.NextPageToken
	response, err = historyArchiver.Get(context.Background(), "file://"+s.testGetDirectory, request)
	s.NoError(err)
	s.NotNil(response)
	s.Nil(response.NextPageToken)
	s.NotNil(response.HistoryBatches)
	s.Len(response.HistoryBatches, 1)
	combinedHistory = append(combinedHistory, response.HistoryBatches...)

	s.Equal(s.historyBatchesV100, combinedHistory)
}

func (s *historyArchiverSuite) TestArchiveAndGet() {
	mockCtrl := gomock.NewController(s.T())
	defer mockCtrl.Finish()
	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)
	historyBlob := &archiver.HistoryBlob{
		Header: &archiver.HistoryBlobHeader{
			IsLast: common.BoolPtr(true),
		},
		Body: s.historyBatchesV100,
	}
	gomock.InOrder(
		historyIterator.EXPECT().HasNext().Return(true),
		historyIterator.EXPECT().Next().Return(historyBlob, nil),
		historyIterator.EXPECT().HasNext().Return(false),
	)

	dir, err := ioutil.TempDir("", "TestArchiveAndGet")
	s.NoError(err)
	defer os.RemoveAll(dir)

	historyArchiver := newHistoryArchiver(s.container, nil, historyIterator)
	archiveRequest := &archiver.ArchiveHistoryRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	err = historyArchiver.Archive(context.Background(), "file://"+dir, archiveRequest)
	s.NoError(err)

	expectedFilename := constructFilename(testDomainID, testWorkflowID, testRunID, testCloseFailoverVersion)
	s.assertFileExists(path.Join(dir, expectedFilename))

	getRequest := &archiver.GetHistoryRequest{
		DomainID:   testDomainID,
		WorkflowID: testWorkflowID,
		RunID:      testRunID,
		PageSize:   testPageSize,
	}
	response, err := historyArchiver.Get(context.Background(), "file://"+dir, getRequest)
	s.NoError(err)
	s.NotNil(response)
	s.Nil(response.NextPageToken)
	s.Equal(s.historyBatchesV100, response.HistoryBatches)
}

func (s *historyArchiverSuite) setupHistoryDirectory() {
	s.historyBatchesV1 = []*shared.History{
		&shared.History{
			Events: []*shared.HistoryEvent{
				&shared.HistoryEvent{
					EventId:   common.Int64Ptr(testNextEventID - 1),
					Timestamp: common.Int64Ptr(time.Now().UnixNano()),
					Version:   common.Int64Ptr(1),
				},
			},
		},
	}

	s.historyBatchesV100 = []*shared.History{
		&shared.History{
			Events: []*shared.HistoryEvent{
				&shared.HistoryEvent{
					EventId:   common.Int64Ptr(common.FirstEventID + 1),
					Timestamp: common.Int64Ptr(time.Now().UnixNano()),
					Version:   common.Int64Ptr(testCloseFailoverVersion),
				},
				&shared.HistoryEvent{
					EventId:   common.Int64Ptr(common.FirstEventID + 1),
					Timestamp: common.Int64Ptr(time.Now().UnixNano()),
					Version:   common.Int64Ptr(testCloseFailoverVersion),
				},
			},
		},
		&shared.History{
			Events: []*shared.HistoryEvent{
				&shared.HistoryEvent{
					EventId:   common.Int64Ptr(testNextEventID - 1),
					Timestamp: common.Int64Ptr(time.Now().UnixNano()),
					Version:   common.Int64Ptr(testCloseFailoverVersion),
				},
			},
		},
	}

	s.writeHistoryBatchesForGetTest(s.historyBatchesV1, int64(1))
	s.writeHistoryBatchesForGetTest(s.historyBatchesV100, testCloseFailoverVersion)
}

func (s *historyArchiverSuite) writeHistoryBatchesForGetTest(historyBatches []*shared.History, version int64) {
	data, err := encodeHistoryBatches(historyBatches)
	s.Require().NoError(err)
	filename := constructFilename(testDomainID, testWorkflowID, testRunID, version)
	err = writeFile(path.Join(s.testGetDirectory, filename), data)
	s.Require().NoError(err)
}

func (s *historyArchiverSuite) assertFileExists(filepath string) {
	exists, err := fileExists(filepath)
	s.NoError(err)
	s.True(exists)
}

func getCanceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
