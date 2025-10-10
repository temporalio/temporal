package filestore

import (
	"context"
	"errors"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	archiverspb "go.temporal.io/server/api/archiver/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/tests/testutils"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	testNamespaceID          = "test-namespace-id"
	testNamespace            = "test-namespace"
	testWorkflowID           = "test-workflow-id"
	testRunID                = "test-run-id"
	testNextEventID          = 1800
	testCloseFailoverVersion = int64(100)
	testPageSize             = 100

	testFileModeStr = "0666"
	testDirModeStr  = "0766"
)

var (
	testBranchToken = []byte{1, 2, 3}
)

type historyArchiverSuite struct {
	suite.Suite

	logger             log.Logger
	executionManager   persistence.ExecutionManager
	metricsHandler     metrics.Handler
	testArchivalURI    archiver.URI
	testGetDirectory   string
	historyBatchesV1   []*historypb.History
	historyBatchesV100 []*historypb.History
}

func TestHistoryArchiverSuite(t *testing.T) {
	suite.Run(t, new(historyArchiverSuite))
}

func (s *historyArchiverSuite) SetupSuite() {
	var err error
	s.testGetDirectory, err = os.MkdirTemp("", "TestGet")
	s.Require().NoError(err)
	s.setupHistoryDirectory()
	s.testArchivalURI, err = archiver.NewURI("file:///a/b/c")
	s.Require().NoError(err)
}

func (s *historyArchiverSuite) TearDownSuite() {
	if err := os.RemoveAll(s.testGetDirectory); err != nil {
		require.Fail(s.T(), "Failed to remove test directory %v: %v", s.testGetDirectory, err)
	}
}

func (s *historyArchiverSuite) SetupTest() {
	s.logger = log.NewNoopLogger()
	s.metricsHandler = metrics.NoopMetricsHandler
}

func (s *historyArchiverSuite) TestValidateURI() {
	testCases := []struct {
		URI         string
		expectedErr error
	}{
		{
			URI:         "wrongscheme:///a/b/c",
			expectedErr: archiver.ErrURISchemeMismatch,
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

	historyArchiver := s.newTestHistoryArchiver(nil)
	for _, tc := range testCases {
		URI, err := archiver.NewURI(tc.URI)
		require.NoError(s.T(), err)
		require.Equal(s.T(), tc.expectedErr, historyArchiver.ValidateURI(URI))
	}
}

func (s *historyArchiverSuite) TestArchive_Fail_InvalidURI() {
	historyArchiver := s.newTestHistoryArchiver(nil)
	request := &archiver.ArchiveHistoryRequest{
		NamespaceID:          testNamespaceID,
		Namespace:            testNamespace,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	URI, err := archiver.NewURI("wrongscheme://")
	require.NoError(s.T(), err)
	err = historyArchiver.Archive(context.Background(), URI, request)
	require.Error(s.T(), err)
}

func (s *historyArchiverSuite) TestArchive_Fail_InvalidRequest() {
	historyArchiver := s.newTestHistoryArchiver(nil)
	request := &archiver.ArchiveHistoryRequest{
		NamespaceID:          testNamespaceID,
		Namespace:            testNamespace,
		WorkflowID:           "", // an invalid request
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	err := historyArchiver.Archive(context.Background(), s.testArchivalURI, request)
	require.Error(s.T(), err)
}

func (s *historyArchiverSuite) TestArchive_Fail_ErrorOnReadHistory() {
	mockCtrl := gomock.NewController(s.T())
	defer mockCtrl.Finish()
	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)
	gomock.InOrder(
		historyIterator.EXPECT().HasNext().Return(true),
		historyIterator.EXPECT().Next(gomock.Any()).Return(nil, errors.New("some random error")),
	)

	historyArchiver := s.newTestHistoryArchiver(historyIterator)
	request := &archiver.ArchiveHistoryRequest{
		NamespaceID:          testNamespaceID,
		Namespace:            testNamespace,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	err := historyArchiver.Archive(context.Background(), s.testArchivalURI, request)
	require.Error(s.T(), err)
}

func (s *historyArchiverSuite) TestArchive_Fail_TimeoutWhenReadingHistory() {
	mockCtrl := gomock.NewController(s.T())
	defer mockCtrl.Finish()
	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)
	gomock.InOrder(
		historyIterator.EXPECT().HasNext().Return(true),
		historyIterator.EXPECT().Next(gomock.Any()).Return(
			nil,
			&serviceerror.ResourceExhausted{
				Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
				Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
				Message: "",
			},
		),
	)

	historyArchiver := s.newTestHistoryArchiver(historyIterator)
	request := &archiver.ArchiveHistoryRequest{
		NamespaceID:          testNamespaceID,
		Namespace:            testNamespace,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	err := historyArchiver.Archive(getCanceledContext(), s.testArchivalURI, request)
	require.Error(s.T(), err)
}

func (s *historyArchiverSuite) TestArchive_Fail_HistoryMutated() {
	mockCtrl := gomock.NewController(s.T())
	defer mockCtrl.Finish()
	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)
	historyBatches := []*historypb.History{
		{
			Events: []*historypb.HistoryEvent{
				{
					EventId:   common.FirstEventID + 1,
					EventTime: timestamppb.New(time.Now().UTC()),
					Version:   testCloseFailoverVersion + 1,
				},
			},
		},
	}
	historyBlob := &archiverspb.HistoryBlob{
		Header: &archiverspb.HistoryBlobHeader{
			IsLast: true,
		},
		Body: historyBatches,
	}
	gomock.InOrder(
		historyIterator.EXPECT().HasNext().Return(true),
		historyIterator.EXPECT().Next(gomock.Any()).Return(historyBlob, nil),
	)

	historyArchiver := s.newTestHistoryArchiver(historyIterator)
	request := &archiver.ArchiveHistoryRequest{
		NamespaceID:          testNamespaceID,
		Namespace:            testNamespace,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	err := historyArchiver.Archive(context.Background(), s.testArchivalURI, request)
	require.Error(s.T(), err)
}

func (s *historyArchiverSuite) TestArchive_Fail_NonRetryableErrorOption() {
	mockCtrl := gomock.NewController(s.T())
	defer mockCtrl.Finish()
	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)
	gomock.InOrder(
		historyIterator.EXPECT().HasNext().Return(true),
		historyIterator.EXPECT().Next(gomock.Any()).Return(nil, errors.New("some random error")),
	)

	historyArchiver := s.newTestHistoryArchiver(historyIterator)
	request := &archiver.ArchiveHistoryRequest{
		NamespaceID:          testNamespaceID,
		Namespace:            testNamespace,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	nonRetryableErr := errors.New("some non-retryable error")
	err := historyArchiver.Archive(context.Background(), s.testArchivalURI, request, archiver.GetNonRetryableErrorOption(nonRetryableErr))
	require.Equal(s.T(), nonRetryableErr, err)
}

func (s *historyArchiverSuite) TestArchive_Skip() {
	mockCtrl := gomock.NewController(s.T())
	defer mockCtrl.Finish()
	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)
	historyBlob := &archiverspb.HistoryBlob{
		Header: &archiverspb.HistoryBlobHeader{
			IsLast: false,
		},
		Body: []*historypb.History{
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   common.FirstEventID,
						EventTime: timestamppb.New(time.Now().UTC()),
						Version:   testCloseFailoverVersion,
					},
				},
			},
		},
	}
	gomock.InOrder(
		historyIterator.EXPECT().HasNext().Return(true),
		historyIterator.EXPECT().Next(gomock.Any()).Return(historyBlob, nil),
		historyIterator.EXPECT().HasNext().Return(true),
		historyIterator.EXPECT().Next(gomock.Any()).Return(nil, serviceerror.NewNotFound("workflow not found")),
	)

	historyArchiver := s.newTestHistoryArchiver(historyIterator)
	request := &archiver.ArchiveHistoryRequest{
		NamespaceID:          testNamespaceID,
		Namespace:            testNamespace,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	err := historyArchiver.Archive(context.Background(), s.testArchivalURI, request)
	require.NoError(s.T(), err)
}

func (s *historyArchiverSuite) TestArchive_Success() {
	mockCtrl := gomock.NewController(s.T())
	defer mockCtrl.Finish()
	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)
	historyBatches := []*historypb.History{
		{
			Events: []*historypb.HistoryEvent{
				{
					EventId:   common.FirstEventID + 1,
					EventTime: timestamppb.New(time.Now().UTC()),
					Version:   testCloseFailoverVersion,
				},
				{
					EventId:   common.FirstEventID + 2,
					EventTime: timestamppb.New(time.Now().UTC()),
					Version:   testCloseFailoverVersion,
				},
			},
		},
		{
			Events: []*historypb.HistoryEvent{
				{
					EventId:   testNextEventID - 1,
					EventTime: timestamppb.New(time.Now().UTC()),
					Version:   testCloseFailoverVersion,
				},
			},
		},
	}
	historyBlob := &archiverspb.HistoryBlob{
		Header: &archiverspb.HistoryBlobHeader{
			IsLast: true,
		},
		Body: historyBatches,
	}
	gomock.InOrder(
		historyIterator.EXPECT().HasNext().Return(true),
		historyIterator.EXPECT().Next(gomock.Any()).Return(historyBlob, nil),
		historyIterator.EXPECT().HasNext().Return(false),
	)

	dir := testutils.MkdirTemp(s.T(), "", "TestArchiveSingleRead")

	historyArchiver := s.newTestHistoryArchiver(historyIterator)
	request := &archiver.ArchiveHistoryRequest{
		NamespaceID:          testNamespaceID,
		Namespace:            testNamespace,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	URI, err := archiver.NewURI("file://" + dir)
	require.NoError(s.T(), err)
	err = historyArchiver.Archive(context.Background(), URI, request)
	require.NoError(s.T(), err)

	expectedFilename := constructHistoryFilename(testNamespaceID, testWorkflowID, testRunID, testCloseFailoverVersion)
	s.assertFileExists(path.Join(dir, expectedFilename))
}

func (s *historyArchiverSuite) TestGet_Fail_InvalidURI() {
	historyArchiver := s.newTestHistoryArchiver(nil)
	request := &archiver.GetHistoryRequest{
		NamespaceID: testNamespaceID,
		WorkflowID:  testWorkflowID,
		RunID:       testRunID,
		PageSize:    100,
	}
	URI, err := archiver.NewURI("wrongscheme://")
	require.NoError(s.T(), err)
	response, err := historyArchiver.Get(context.Background(), URI, request)
	require.Nil(s.T(), response)
	require.Error(s.T(), err)
}

func (s *historyArchiverSuite) TestGet_Fail_InvalidRequest() {
	historyArchiver := s.newTestHistoryArchiver(nil)
	request := &archiver.GetHistoryRequest{
		NamespaceID: testNamespaceID,
		WorkflowID:  testWorkflowID,
		RunID:       testRunID,
		PageSize:    0, // pageSize should be greater than 0
	}
	response, err := historyArchiver.Get(context.Background(), s.testArchivalURI, request)
	require.Nil(s.T(), response)
	require.Error(s.T(), err)
	require.IsType(s.T(), &serviceerror.InvalidArgument{}, err)
}

func (s *historyArchiverSuite) TestGet_Fail_DirectoryNotExist() {
	historyArchiver := s.newTestHistoryArchiver(nil)
	request := &archiver.GetHistoryRequest{
		NamespaceID: testNamespaceID,
		WorkflowID:  testWorkflowID,
		RunID:       testRunID,
		PageSize:    testPageSize,
	}
	response, err := historyArchiver.Get(context.Background(), s.testArchivalURI, request)
	require.Nil(s.T(), response)
	require.Error(s.T(), err)
	require.IsType(s.T(), &serviceerror.NotFound{}, err)
}

func (s *historyArchiverSuite) TestGet_Fail_InvalidToken() {
	historyArchiver := s.newTestHistoryArchiver(nil)
	request := &archiver.GetHistoryRequest{
		NamespaceID:   testNamespaceID,
		WorkflowID:    testWorkflowID,
		RunID:         testRunID,
		PageSize:      testPageSize,
		NextPageToken: []byte{'r', 'a', 'n', 'd', 'o', 'm'},
	}
	URI, err := archiver.NewURI("file:///")
	require.NoError(s.T(), err)
	response, err := historyArchiver.Get(context.Background(), URI, request)
	require.Nil(s.T(), response)
	require.Error(s.T(), err)
	require.IsType(s.T(), &serviceerror.InvalidArgument{}, err)
}

func (s *historyArchiverSuite) TestGet_Fail_FileNotExist() {
	historyArchiver := s.newTestHistoryArchiver(nil)
	testCloseFailoverVersion := testCloseFailoverVersion
	request := &archiver.GetHistoryRequest{
		NamespaceID:          testNamespaceID,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		PageSize:             testPageSize,
		CloseFailoverVersion: &testCloseFailoverVersion,
	}
	URI, err := archiver.NewURI("file:///")
	require.NoError(s.T(), err)
	response, err := historyArchiver.Get(context.Background(), URI, request)
	require.Nil(s.T(), response)
	require.Error(s.T(), err)
	require.IsType(s.T(), &serviceerror.NotFound{}, err)
}

func (s *historyArchiverSuite) TestGet_Success_PickHighestVersion() {
	historyArchiver := s.newTestHistoryArchiver(nil)
	request := &archiver.GetHistoryRequest{
		NamespaceID: testNamespaceID,
		WorkflowID:  testWorkflowID,
		RunID:       testRunID,
		PageSize:    testPageSize,
	}
	URI, err := archiver.NewURI("file://" + s.testGetDirectory)
	require.NoError(s.T(), err)
	response, err := historyArchiver.Get(context.Background(), URI, request)
	require.NoError(s.T(), err)
	require.Nil(s.T(), response.NextPageToken)
	require.Equal(s.T(), s.historyBatchesV100, response.HistoryBatches)
}

func (s *historyArchiverSuite) TestGet_Success_UseProvidedVersion() {
	historyArchiver := s.newTestHistoryArchiver(nil)
	testCloseFailoverVersion := int64(1)
	request := &archiver.GetHistoryRequest{
		NamespaceID:          testNamespaceID,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		PageSize:             testPageSize,
		CloseFailoverVersion: &testCloseFailoverVersion,
	}
	URI, err := archiver.NewURI("file://" + s.testGetDirectory)
	require.NoError(s.T(), err)
	response, err := historyArchiver.Get(context.Background(), URI, request)
	require.NoError(s.T(), err)
	require.Nil(s.T(), response.NextPageToken)
	require.Equal(s.T(), s.historyBatchesV1, response.HistoryBatches)
}

func (s *historyArchiverSuite) TestGet_Success_SmallPageSize() {
	historyArchiver := s.newTestHistoryArchiver(nil)
	testCloseFailoverVersion := int64(100)
	request := &archiver.GetHistoryRequest{
		NamespaceID:          testNamespaceID,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		PageSize:             1,
		CloseFailoverVersion: &testCloseFailoverVersion,
	}
	var combinedHistory []*historypb.History

	URI, err := archiver.NewURI("file://" + s.testGetDirectory)
	require.NoError(s.T(), err)
	response, err := historyArchiver.Get(context.Background(), URI, request)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), response)
	require.NotNil(s.T(), response.NextPageToken)
	require.NotNil(s.T(), response.HistoryBatches)
	require.Len(s.T(), response.HistoryBatches, 1)
	combinedHistory = append(combinedHistory, response.HistoryBatches...)

	request.NextPageToken = response.NextPageToken
	response, err = historyArchiver.Get(context.Background(), URI, request)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), response)
	require.Nil(s.T(), response.NextPageToken)
	require.NotNil(s.T(), response.HistoryBatches)
	require.Len(s.T(), response.HistoryBatches, 1)
	combinedHistory = append(combinedHistory, response.HistoryBatches...)

	require.Equal(s.T(), s.historyBatchesV100, combinedHistory)
}

func (s *historyArchiverSuite) TestArchiveAndGet() {
	mockCtrl := gomock.NewController(s.T())
	defer mockCtrl.Finish()
	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)
	historyBlob := &archiverspb.HistoryBlob{
		Header: &archiverspb.HistoryBlobHeader{
			IsLast: true,
		},
		Body: s.historyBatchesV100,
	}
	gomock.InOrder(
		historyIterator.EXPECT().HasNext().Return(true),
		historyIterator.EXPECT().Next(gomock.Any()).Return(historyBlob, nil),
		historyIterator.EXPECT().HasNext().Return(false),
	)

	dir := testutils.MkdirTemp(s.T(), "", "TestArchiveAndGet")

	historyArchiver := s.newTestHistoryArchiver(historyIterator)
	archiveRequest := &archiver.ArchiveHistoryRequest{
		NamespaceID:          testNamespaceID,
		Namespace:            testNamespace,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	URI, err := archiver.NewURI("file://" + dir)
	require.NoError(s.T(), err)
	err = historyArchiver.Archive(context.Background(), URI, archiveRequest)
	require.NoError(s.T(), err)

	expectedFilename := constructHistoryFilename(testNamespaceID, testWorkflowID, testRunID, testCloseFailoverVersion)
	s.assertFileExists(path.Join(dir, expectedFilename))

	getRequest := &archiver.GetHistoryRequest{
		NamespaceID: testNamespaceID,
		WorkflowID:  testWorkflowID,
		RunID:       testRunID,
		PageSize:    testPageSize,
	}
	response, err := historyArchiver.Get(context.Background(), URI, getRequest)
	require.NoError(s.T(), err)
	require.NotNil(s.T(), response)
	require.Nil(s.T(), response.NextPageToken)
	require.Equal(s.T(), s.historyBatchesV100, response.HistoryBatches)
}

func (s *historyArchiverSuite) newTestHistoryArchiver(historyIterator archiver.HistoryIterator) *historyArchiver {
	config := &config.FilestoreArchiver{
		FileMode: testFileModeStr,
		DirMode:  testDirModeStr,
	}
	a, err := newHistoryArchiver(s.executionManager, s.logger, s.metricsHandler, config, historyIterator)
	require.NoError(s.T(), err)
	return a
}

func (s *historyArchiverSuite) setupHistoryDirectory() {
	now := timestamppb.New(time.Date(2020, 8, 22, 1, 2, 3, 4, time.UTC))
	s.historyBatchesV1 = []*historypb.History{
		{
			Events: []*historypb.HistoryEvent{
				{
					EventId:   testNextEventID - 1,
					EventTime: now,
					Version:   1,
				},
			},
		},
	}

	s.historyBatchesV100 = []*historypb.History{
		{
			Events: []*historypb.HistoryEvent{
				{
					EventId:   common.FirstEventID + 1,
					EventTime: now,
					Version:   testCloseFailoverVersion,
				},
				{
					EventId:   common.FirstEventID + 1,
					EventTime: now,
					Version:   testCloseFailoverVersion,
				},
			},
		},
		{
			Events: []*historypb.HistoryEvent{
				{
					EventId:   testNextEventID - 1,
					EventTime: now,
					Version:   testCloseFailoverVersion,
				},
			},
		},
	}

	s.writeHistoryBatchesForGetTest(s.historyBatchesV1, int64(1))
	s.writeHistoryBatchesForGetTest(s.historyBatchesV100, testCloseFailoverVersion)
}

func (s *historyArchiverSuite) writeHistoryBatchesForGetTest(historyBatches []*historypb.History, version int64) {
	data, err := encodeHistories(historyBatches)
	s.Require().NoError(err)
	filename := constructHistoryFilename(testNamespaceID, testWorkflowID, testRunID, version)
	err = writeFile(path.Join(s.testGetDirectory, filename), data, testFileMode)
	s.Require().NoError(err)
}

func (s *historyArchiverSuite) assertFileExists(filepath string) {
	exists, err := fileExists(filepath)
	require.NoError(s.T(), err)
	require.True(s.T(), exists)
}

func getCanceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
