// Copyright (c) 2019 Uber Technologies, Inc.
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
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/service/config"
	"go.uber.org/zap"
)

const (
	testWorkflowTypeName = "test-workflow-type"
)

type visibilityArchiverSuite struct {
	*require.Assertions
	suite.Suite

	container          *archiver.VisibilityBootstrapContainer
	logger             log.Logger
	testArchivalURI    archiver.URI
	testQueryDirectory string
	visibilityRecords  []*visibilityRecord
}

func TestVisibilityArchiverSuite(t *testing.T) {
	suite.Run(t, new(visibilityArchiverSuite))
}

func (s *visibilityArchiverSuite) SetupSuite() {
	var err error
	s.testQueryDirectory, err = ioutil.TempDir("", "TestQuery")
	s.Require().NoError(err)
	s.setupVisibilityDirectory()
	s.testArchivalURI, err = archiver.NewURI("file:///a/b/c")
	s.Require().NoError(err)
}

func (s *visibilityArchiverSuite) TearDownSuite() {
	os.RemoveAll(s.testQueryDirectory)
}

func (s *visibilityArchiverSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	zapLogger := zap.NewNop()
	s.container = &archiver.VisibilityBootstrapContainer{
		Logger: loggerimpl.NewLogger(zapLogger),
	}
}

func (s *visibilityArchiverSuite) TestValidateURI() {
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

	visibilityArchiver := s.newTestVisibilityArchiver()
	for _, tc := range testCases {
		URI, err := archiver.NewURI(tc.URI)
		s.NoError(err)
		s.Equal(tc.expectedErr, visibilityArchiver.ValidateURI(URI))
	}
}

func (s *visibilityArchiverSuite) TestArchive_Fail_InvalidURI() {
	visibilityArchiver := s.newTestVisibilityArchiver()
	URI, err := archiver.NewURI("wrongscheme://")
	s.NoError(err)
	request := &archiver.ArchiveVisibilityRequest{
		DomainID:           testDomainID,
		WorkflowID:         testWorkflowID,
		RunID:              testRunID,
		WorkflowTypeName:   testWorkflowTypeName,
		StartTimestamp:     int64(0), // workflow without backoff
		ExecutionTimestamp: time.Now().UnixNano(),
		CloseTimestamp:     time.Now().UnixNano(),
		CloseStatus:        shared.WorkflowExecutionCloseStatusFailed,
		HistoryLength:      int64(101),
	}
	err = visibilityArchiver.Archive(context.Background(), URI, request)
	s.Error(err)
}

func (s *visibilityArchiverSuite) TestArchive_Fail_InvalidRequest() {
	visibilityArchiver := s.newTestVisibilityArchiver()
	err := visibilityArchiver.Archive(context.Background(), s.testArchivalURI, &archiver.ArchiveVisibilityRequest{})
	s.Error(err)
}

func (s *visibilityArchiverSuite) TestArchive_Fail_NonRetriableErrorOption() {
	visibilityArchiver := s.newTestVisibilityArchiver()
	nonRetriableErr := errors.New("some non-retryable error")
	err := visibilityArchiver.Archive(
		context.Background(),
		s.testArchivalURI,
		&archiver.ArchiveVisibilityRequest{},
		archiver.GetNonRetriableErrorOption(nonRetriableErr),
	)
	s.Equal(nonRetriableErr, err)
}

func (s *visibilityArchiverSuite) TestArchive_Success() {
	dir, err := ioutil.TempDir("", "TestVisibilityArchive")
	s.NoError(err)
	defer os.RemoveAll(dir)

	visibilityArchiver := s.newTestVisibilityArchiver()
	closeTimestamp := time.Now()
	request := &archiver.ArchiveVisibilityRequest{
		DomainID:           testDomainID,
		WorkflowID:         testWorkflowID,
		RunID:              testRunID,
		WorkflowTypeName:   testWorkflowTypeName,
		StartTimestamp:     int64(0), // workflow without backoff
		ExecutionTimestamp: closeTimestamp.Add(-time.Hour).UnixNano(),
		CloseTimestamp:     closeTimestamp.UnixNano(),
		CloseStatus:        shared.WorkflowExecutionCloseStatusFailed,
		HistoryLength:      int64(101),
		Memo: &shared.Memo{
			Fields: map[string][]byte{
				"testFields": []byte{1, 2, 3},
			},
		},
		SearchAttributes: map[string][]byte{
			"testAttribute": []byte{4, 5, 6},
		},
	}
	URI, err := archiver.NewURI("file://" + dir)
	s.NoError(err)
	err = visibilityArchiver.Archive(context.Background(), URI, request)
	s.NoError(err)

	expectedFilename := constructVisibilityFilename(closeTimestamp.UnixNano(), testRunID)
	filepath := path.Join(dir, testDomainID, expectedFilename)
	s.assertFileExists(filepath)

	data, err := readFile(filepath)
	s.NoError(err)

	archivedRecord := &archiver.ArchiveVisibilityRequest{}
	err = json.Unmarshal(data, archivedRecord)
	s.NoError(err)
	s.Equal(request, archivedRecord)
}

func (s *visibilityArchiverSuite) TestMatchQuery() {
	testCases := []struct {
		queryRequest *archiver.QueryVisibilityRequest
		record       *visibilityRecord
		shouldMatch  bool
	}{
		{
			queryRequest: &archiver.QueryVisibilityRequest{
				EarliestCloseTime: int64(1000),
				LatestCloseTime:   int64(12345),
			},
			record: &visibilityRecord{
				CloseTimestamp: int64(1999),
			},
			shouldMatch: true,
		},
		{
			queryRequest: &archiver.QueryVisibilityRequest{
				EarliestCloseTime: int64(1000),
				LatestCloseTime:   int64(12345),
			},
			record: &visibilityRecord{
				CloseTimestamp: int64(999),
			},
			shouldMatch: false,
		},
		{
			queryRequest: &archiver.QueryVisibilityRequest{
				EarliestCloseTime: int64(1000),
				LatestCloseTime:   int64(12345),
				WorkflowID:        common.StringPtr("random workflowID"),
			},
			record: &visibilityRecord{
				CloseTimestamp: int64(2000),
			},
			shouldMatch: false,
		},
		{
			queryRequest: &archiver.QueryVisibilityRequest{
				EarliestCloseTime: int64(1000),
				LatestCloseTime:   int64(12345),
				WorkflowID:        common.StringPtr("random workflowID"),
				RunID:             common.StringPtr("random runID"),
			},
			record: &visibilityRecord{
				CloseTimestamp:   int64(12345),
				WorkflowID:       "random workflowID",
				RunID:            "random runID",
				WorkflowTypeName: "random type name",
			},
			shouldMatch: true,
		},
		{
			queryRequest: &archiver.QueryVisibilityRequest{
				EarliestCloseTime: int64(1000),
				LatestCloseTime:   int64(12345),
				WorkflowTypeName:  common.StringPtr("some random type name"),
			},
			record: &visibilityRecord{
				CloseTimestamp: int64(12345),
			},
			shouldMatch: false,
		},
		{
			queryRequest: &archiver.QueryVisibilityRequest{
				EarliestCloseTime: int64(1000),
				LatestCloseTime:   int64(12345),
				WorkflowTypeName:  common.StringPtr("some random type name"),
				CloseStatus:       shared.WorkflowExecutionCloseStatusContinuedAsNew.Ptr(),
			},
			record: &visibilityRecord{
				CloseTimestamp:   int64(12345),
				CloseStatus:      shared.WorkflowExecutionCloseStatusContinuedAsNew,
				WorkflowTypeName: "some random type name",
			},
			shouldMatch: true,
		},
	}

	for _, tc := range testCases {
		s.Equal(tc.shouldMatch, matchQuery(tc.record, tc.queryRequest))
	}
}

func (s *visibilityArchiverSuite) TestSortAndFilterFiles() {
	testCases := []struct {
		filenames      []string
		token          *queryVisibilityToken
		expectedResult []string
	}{
		{
			filenames:      []string{"9_12345.vis", "5_0.vis", "9_54321.vis", "1000_654.vis", "1000_78.vis"},
			expectedResult: []string{"1000_78.vis", "1000_654.vis", "9_54321.vis", "9_12345.vis", "5_0.vis"},
		},
		{
			filenames: []string{"9_12345.vis", "5_0.vis", "9_54321.vis", "1000_654.vis", "1000_78.vis"},
			token: &queryVisibilityToken{
				LastCloseTime: 3,
			},
			expectedResult: []string{},
		},
		{
			filenames: []string{"9_12345.vis", "5_0.vis", "9_54321.vis", "1000_654.vis", "1000_78.vis"},
			token: &queryVisibilityToken{
				LastCloseTime: 999,
			},
			expectedResult: []string{"9_54321.vis", "9_12345.vis", "5_0.vis"},
		},
		{
			filenames: []string{"9_12345.vis", "5_0.vis", "9_54321.vis", "1000_654.vis", "1000_78.vis"},
			token: &queryVisibilityToken{
				LastCloseTime: 5,
			},
			expectedResult: []string{"5_0.vis"},
		},
	}

	for _, tc := range testCases {
		result, err := sortAndFilterFiles(tc.filenames, tc.token)
		s.NoError(err)
		s.Equal(tc.expectedResult, result)
	}
}

func (s *visibilityArchiverSuite) TestQuery_Fail_InvalidURI() {
	visibilityArchiver := s.newTestVisibilityArchiver()
	URI, err := archiver.NewURI("wrongscheme://")
	s.NoError(err)
	request := &archiver.QueryVisibilityRequest{
		DomainID:          testDomainID,
		EarliestCloseTime: int64(1),
		LatestCloseTime:   int64(101),
		PageSize:          1,
	}
	response, err := visibilityArchiver.Query(context.Background(), URI, request)
	s.Error(err)
	s.Nil(response)
}

func (s *visibilityArchiverSuite) TestQuery_Fail_InvalidRequest() {
	visibilityArchiver := s.newTestVisibilityArchiver()
	response, err := visibilityArchiver.Query(context.Background(), s.testArchivalURI, &archiver.QueryVisibilityRequest{})
	s.Error(err)
	s.Nil(response)
}

func (s *visibilityArchiverSuite) TestQuery_Success_DirectoryNotExist() {
	visibilityArchiver := s.newTestVisibilityArchiver()
	request := &archiver.QueryVisibilityRequest{
		DomainID:          testDomainID,
		EarliestCloseTime: int64(1),
		LatestCloseTime:   int64(101),
		PageSize:          1,
	}
	response, err := visibilityArchiver.Query(context.Background(), s.testArchivalURI, request)
	s.NoError(err)
	s.NotNil(response)
	s.Empty(response.Executions)
	s.Empty(response.NextPageToken)
}

func (s *visibilityArchiverSuite) TestQuery_Fail_InvalidToken() {
	visibilityArchiver := s.newTestVisibilityArchiver()
	request := &archiver.QueryVisibilityRequest{
		DomainID:          testDomainID,
		EarliestCloseTime: int64(1),
		LatestCloseTime:   int64(101),
		PageSize:          1,
		NextPageToken:     []byte{1, 2, 3},
	}
	response, err := visibilityArchiver.Query(context.Background(), s.testArchivalURI, request)
	s.Error(err)
	s.Nil(response)
}

func (s *visibilityArchiverSuite) TestQuery_Success_NoNextPageToken() {
	visibilityArchiver := s.newTestVisibilityArchiver()
	request := &archiver.QueryVisibilityRequest{
		DomainID:          testDomainID,
		EarliestCloseTime: int64(1),
		LatestCloseTime:   int64(10001),
		PageSize:          10,
		WorkflowID:        common.StringPtr(testWorkflowID),
	}
	URI, err := archiver.NewURI("file://" + s.testQueryDirectory)
	s.NoError(err)
	response, err := visibilityArchiver.Query(context.Background(), URI, request)
	s.NoError(err)
	s.NotNil(response)
	s.Nil(response.NextPageToken)
	s.Len(response.Executions, 1)
	s.Equal(convertToExecutionInfo(s.visibilityRecords[0]), response.Executions[0])
}

func (s *visibilityArchiverSuite) TestQuery_Success_SmallPageSize() {
	visibilityArchiver := s.newTestVisibilityArchiver()
	request := &archiver.QueryVisibilityRequest{
		DomainID:          testDomainID,
		EarliestCloseTime: int64(1),
		LatestCloseTime:   int64(10001),
		PageSize:          2,
		CloseStatus:       shared.WorkflowExecutionCloseStatusFailed.Ptr(),
	}
	URI, err := archiver.NewURI("file://" + s.testQueryDirectory)
	s.NoError(err)
	response, err := visibilityArchiver.Query(context.Background(), URI, request)
	s.NoError(err)
	s.NotNil(response)
	s.NotNil(response.NextPageToken)
	s.Len(response.Executions, 2)
	s.Equal(convertToExecutionInfo(s.visibilityRecords[0]), response.Executions[0])
	s.Equal(convertToExecutionInfo(s.visibilityRecords[1]), response.Executions[1])

	request.NextPageToken = response.NextPageToken
	response, err = visibilityArchiver.Query(context.Background(), URI, request)
	s.NoError(err)
	s.NotNil(response)
	s.Nil(response.NextPageToken)
	s.Len(response.Executions, 1)
	s.Equal(convertToExecutionInfo(s.visibilityRecords[3]), response.Executions[0])
}

func (s *visibilityArchiverSuite) TestArchiveAndQuery() {
	dir, err := ioutil.TempDir("", "TestArchiveAndQuery")
	s.NoError(err)
	defer os.RemoveAll(dir)

	visibilityArchiver := s.newTestVisibilityArchiver()
	URI, err := archiver.NewURI("file://" + dir)
	s.NoError(err)
	for _, record := range s.visibilityRecords {
		err := visibilityArchiver.Archive(context.Background(), URI, (*archiver.ArchiveVisibilityRequest)(record))
		s.NoError(err)
	}

	request := &archiver.QueryVisibilityRequest{
		DomainID:          testDomainID,
		EarliestCloseTime: int64(10),
		LatestCloseTime:   int64(10001),
		PageSize:          1,
		CloseStatus:       shared.WorkflowExecutionCloseStatusFailed.Ptr(),
	}
	executions := []*shared.WorkflowExecutionInfo{}
	for len(executions) == 0 || request.NextPageToken != nil {
		response, err := visibilityArchiver.Query(context.Background(), URI, request)
		s.NoError(err)
		s.NotNil(response)
		executions = append(executions, response.Executions...)
		request.NextPageToken = response.NextPageToken
	}
	s.Len(executions, 2)
	s.Equal(convertToExecutionInfo(s.visibilityRecords[0]), executions[0])
	s.Equal(convertToExecutionInfo(s.visibilityRecords[1]), executions[1])
}

func (s *visibilityArchiverSuite) newTestVisibilityArchiver() *visibilityArchiver {
	config := &config.FilestoreArchiver{
		FileMode: testFileModeStr,
		DirMode:  testDirModeStr,
	}
	archiver, err := NewVisibilityArchiver(s.container, config)
	s.NoError(err)
	return archiver.(*visibilityArchiver)
}

func (s *visibilityArchiverSuite) setupVisibilityDirectory() {
	s.visibilityRecords = []*visibilityRecord{
		{
			DomainID:           testDomainID,
			WorkflowID:         testWorkflowID,
			RunID:              testRunID,
			WorkflowTypeName:   testWorkflowTypeName,
			ExecutionTimestamp: 1,
			CloseTimestamp:     10000,
			CloseStatus:        shared.WorkflowExecutionCloseStatusFailed,
			HistoryLength:      101,
		},
		{
			DomainID:           testDomainID,
			WorkflowID:         "some random workflow ID",
			RunID:              "some random run ID",
			WorkflowTypeName:   testWorkflowTypeName,
			StartTimestamp:     2,
			ExecutionTimestamp: 2,
			CloseTimestamp:     1000,
			CloseStatus:        shared.WorkflowExecutionCloseStatusFailed,
			HistoryLength:      123,
		},
		{
			DomainID:           testDomainID,
			WorkflowID:         "another workflow ID",
			RunID:              "another run ID",
			WorkflowTypeName:   testWorkflowTypeName,
			ExecutionTimestamp: 3,
			CloseTimestamp:     10,
			CloseStatus:        shared.WorkflowExecutionCloseStatusContinuedAsNew,
			HistoryLength:      456,
		},
		{
			DomainID:           testDomainID,
			WorkflowID:         "and another workflow ID",
			RunID:              "and another run ID",
			WorkflowTypeName:   testWorkflowTypeName,
			ExecutionTimestamp: 3,
			CloseTimestamp:     5,
			CloseStatus:        shared.WorkflowExecutionCloseStatusFailed,
			HistoryLength:      456,
		},
		{
			DomainID:           "some random domain ID",
			WorkflowID:         "another workflow ID",
			RunID:              "another run ID",
			WorkflowTypeName:   testWorkflowTypeName,
			ExecutionTimestamp: 3,
			CloseTimestamp:     10000,
			CloseStatus:        shared.WorkflowExecutionCloseStatusContinuedAsNew,
			HistoryLength:      456,
		},
	}

	for _, record := range s.visibilityRecords {
		s.writeVisibilityRecordForQueryTest(record)
	}
}

func (s *visibilityArchiverSuite) writeVisibilityRecordForQueryTest(record *visibilityRecord) {
	data, err := encode(record)
	s.Require().NoError(err)
	filename := constructVisibilityFilename(record.CloseTimestamp, record.RunID)
	s.Require().NoError(os.MkdirAll(path.Join(s.testQueryDirectory, record.DomainID), testDirMode))
	err = writeFile(path.Join(s.testQueryDirectory, record.DomainID, filename), data, testFileMode)
	s.Require().NoError(err)
}

func (s *visibilityArchiverSuite) assertFileExists(filepath string) {
	exists, err := fileExists(filepath)
	s.NoError(err)
	s.True(exists)
}
