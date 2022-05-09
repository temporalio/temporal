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

package filestore

import (
	"context"
	"errors"
	"os"
	"path"
	"testing"
	"time"

	"go.temporal.io/server/tests/testhelper"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"

	"go.temporal.io/server/common/searchattribute"

	archiverspb "go.temporal.io/server/api/archiver/v1"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/codec"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/primitives/timestamp"
)

const (
	testWorkflowTypeName = "test-workflow-type"
)

type visibilityArchiverSuite struct {
	*require.Assertions
	suite.Suite

	container          *archiver.VisibilityBootstrapContainer
	testArchivalURI    archiver.URI
	testQueryDirectory string
	visibilityRecords  []*archiverspb.VisibilityRecord

	controller *gomock.Controller
}

func TestVisibilityArchiverSuite(t *testing.T) {
	suite.Run(t, new(visibilityArchiverSuite))
}

func (s *visibilityArchiverSuite) SetupSuite() {
	var err error
	s.testQueryDirectory, err = os.MkdirTemp("", "TestQuery")
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
	s.container = &archiver.VisibilityBootstrapContainer{
		Logger: log.NewNoopLogger(),
	}
	s.controller = gomock.NewController(s.T())
}

func (s *visibilityArchiverSuite) TearDownTest() {
	s.controller.Finish()
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
	request := &archiverspb.VisibilityRecord{
		Namespace:        testNamespace,
		NamespaceId:      testNamespaceID,
		WorkflowId:       testWorkflowID,
		RunId:            testRunID,
		WorkflowTypeName: testWorkflowTypeName,
		StartTime:        timestamp.TimeNowPtrUtc(),
		ExecutionTime:    nil, // workflow without backoff
		CloseTime:        timestamp.TimeNowPtrUtc(),
		Status:           enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		HistoryLength:    int64(101),
	}
	err = visibilityArchiver.Archive(context.Background(), URI, request)
	s.Error(err)
}

func (s *visibilityArchiverSuite) TestArchive_Fail_InvalidRequest() {
	visibilityArchiver := s.newTestVisibilityArchiver()
	err := visibilityArchiver.Archive(context.Background(), s.testArchivalURI, &archiverspb.VisibilityRecord{})
	s.Error(err)
}

func (s *visibilityArchiverSuite) TestArchive_Fail_NonRetryableErrorOption() {
	visibilityArchiver := s.newTestVisibilityArchiver()
	nonRetryableErr := errors.New("some non-retryable error")
	err := visibilityArchiver.Archive(
		context.Background(),
		s.testArchivalURI,
		&archiverspb.VisibilityRecord{},
		archiver.GetNonRetryableErrorOption(nonRetryableErr),
	)
	s.Equal(nonRetryableErr, err)
}

func (s *visibilityArchiverSuite) TestArchive_Success() {
	dir := testhelper.MkdirTemp(s.T(), "", "TestVisibilityArchive")

	visibilityArchiver := s.newTestVisibilityArchiver()
	closeTimestamp := timestamp.TimeNowPtrUtc()
	request := &archiverspb.VisibilityRecord{
		NamespaceId:      testNamespaceID,
		Namespace:        testNamespace,
		WorkflowId:       testWorkflowID,
		RunId:            testRunID,
		WorkflowTypeName: testWorkflowTypeName,
		StartTime:        timestamp.TimePtr(closeTimestamp.Add(-time.Hour)),
		ExecutionTime:    nil, // workflow without backoff
		CloseTime:        closeTimestamp,
		Status:           enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		HistoryLength:    int64(101),
		Memo: &commonpb.Memo{
			Fields: map[string]*commonpb.Payload{
				"testFields": payload.EncodeBytes([]byte{1, 2, 3}),
			},
		},
		SearchAttributes: map[string]string{
			"testAttribute": "456",
		},
	}
	URI, err := archiver.NewURI("file://" + dir)
	s.NoError(err)
	err = visibilityArchiver.Archive(context.Background(), URI, request)
	s.NoError(err)

	expectedFilename := constructVisibilityFilename(closeTimestamp, testRunID)
	filepath := path.Join(dir, testNamespaceID, expectedFilename)
	s.assertFileExists(filepath)

	data, err := readFile(filepath)
	s.NoError(err)

	archivedRecord := &archiverspb.VisibilityRecord{}
	encoder := codec.NewJSONPBEncoder()
	err = encoder.Decode(data, archivedRecord)
	s.NoError(err)
	s.Equal(request, archivedRecord)
}

func (s *visibilityArchiverSuite) TestMatchQuery() {
	testCases := []struct {
		query       *parsedQuery
		record      *archiverspb.VisibilityRecord
		shouldMatch bool
	}{
		{
			query: &parsedQuery{
				earliestCloseTime: time.Unix(0, 1000),
				latestCloseTime:   time.Unix(0, 12345),
			},
			record: &archiverspb.VisibilityRecord{
				CloseTime: timestamp.UnixOrZeroTimePtr(1999),
			},
			shouldMatch: true,
		},
		{
			query: &parsedQuery{
				earliestCloseTime: time.Unix(0, 1000),
				latestCloseTime:   time.Unix(0, 12345),
			},
			record: &archiverspb.VisibilityRecord{
				CloseTime: timestamp.UnixOrZeroTimePtr(999),
			},
			shouldMatch: false,
		},
		{
			query: &parsedQuery{
				earliestCloseTime: time.Unix(0, 1000),
				latestCloseTime:   time.Unix(0, 12345),
				workflowID:        convert.StringPtr("random workflowID"),
			},
			record: &archiverspb.VisibilityRecord{
				CloseTime: timestamp.UnixOrZeroTimePtr(2000),
			},
			shouldMatch: false,
		},
		{
			query: &parsedQuery{
				earliestCloseTime: time.Unix(0, 1000),
				latestCloseTime:   time.Unix(0, 12345),
				workflowID:        convert.StringPtr("random workflowID"),
				runID:             convert.StringPtr("random runID"),
			},
			record: &archiverspb.VisibilityRecord{
				CloseTime:        timestamp.UnixOrZeroTimePtr(12345),
				WorkflowId:       "random workflowID",
				RunId:            "random runID",
				WorkflowTypeName: "random type name",
			},
			shouldMatch: true,
		},
		{
			query: &parsedQuery{
				earliestCloseTime: time.Unix(0, 1000),
				latestCloseTime:   time.Unix(0, 12345),
				workflowTypeName:  convert.StringPtr("some random type name"),
			},
			record: &archiverspb.VisibilityRecord{
				CloseTime: timestamp.UnixOrZeroTimePtr(12345),
			},
			shouldMatch: false,
		},
		{
			query: &parsedQuery{
				earliestCloseTime: time.Unix(0, 1000),
				latestCloseTime:   time.Unix(0, 12345),
				workflowTypeName:  convert.StringPtr("some random type name"),
				status:            toWorkflowExecutionStatusPtr(enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW),
			},
			record: &archiverspb.VisibilityRecord{
				CloseTime:        timestamp.UnixOrZeroTimePtr(12345),
				Status:           enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
				WorkflowTypeName: "some random type name",
			},
			shouldMatch: true,
		},
	}

	for _, tc := range testCases {
		s.Equal(tc.shouldMatch, matchQuery(tc.record, tc.query))
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
				LastCloseTime: time.Unix(0, 3),
			},
			expectedResult: []string{},
		},
		{
			filenames: []string{"9_12345.vis", "5_0.vis", "9_54321.vis", "1000_654.vis", "1000_78.vis"},
			token: &queryVisibilityToken{
				LastCloseTime: time.Unix(0, 999),
			},
			expectedResult: []string{"9_54321.vis", "9_12345.vis", "5_0.vis"},
		},
		{
			filenames: []string{"9_12345.vis", "5_0.vis", "9_54321.vis", "1000_654.vis", "1000_78.vis"},
			token: &queryVisibilityToken{
				LastCloseTime: time.Unix(0, 5).UTC(),
			},
			expectedResult: []string{"5_0.vis"},
		},
	}

	for i, tc := range testCases {
		result, err := sortAndFilterFiles(tc.filenames, tc.token)
		s.NoError(err, "case %d", i)
		s.Equal(tc.expectedResult, result, "case %d", i)
	}
}

func (s *visibilityArchiverSuite) TestQuery_Fail_InvalidURI() {
	visibilityArchiver := s.newTestVisibilityArchiver()
	URI, err := archiver.NewURI("wrongscheme://")
	s.NoError(err)
	request := &archiver.QueryVisibilityRequest{
		NamespaceID: testNamespaceID,
		PageSize:    1,
	}
	response, err := visibilityArchiver.Query(context.Background(), URI, request, searchattribute.TestNameTypeMap)
	s.Error(err)
	s.Nil(response)
}

func (s *visibilityArchiverSuite) TestQuery_Fail_InvalidRequest() {
	visibilityArchiver := s.newTestVisibilityArchiver()
	response, err := visibilityArchiver.Query(context.Background(), s.testArchivalURI, &archiver.QueryVisibilityRequest{}, searchattribute.TestNameTypeMap)
	s.Error(err)
	s.Nil(response)
}

func (s *visibilityArchiverSuite) TestQuery_Fail_InvalidQuery() {
	visibilityArchiver := s.newTestVisibilityArchiver()
	mockParser := NewMockQueryParser(s.controller)
	mockParser.EXPECT().Parse(gomock.Any()).Return(nil, errors.New("invalid query"))
	visibilityArchiver.queryParser = mockParser
	response, err := visibilityArchiver.Query(context.Background(), s.testArchivalURI, &archiver.QueryVisibilityRequest{
		NamespaceID: "some random namespaceID",
		PageSize:    10,
		Query:       "some invalid query",
	}, searchattribute.TestNameTypeMap)
	s.Error(err)
	s.Nil(response)
}

func (s *visibilityArchiverSuite) TestQuery_Success_DirectoryNotExist() {
	visibilityArchiver := s.newTestVisibilityArchiver()
	mockParser := NewMockQueryParser(s.controller)
	mockParser.EXPECT().Parse(gomock.Any()).Return(&parsedQuery{
		earliestCloseTime: time.Unix(0, 1),
		latestCloseTime:   time.Unix(0, 101),
	}, nil)
	visibilityArchiver.queryParser = mockParser
	request := &archiver.QueryVisibilityRequest{
		NamespaceID: testNamespaceID,
		Query:       "parsed by mockParser",
		PageSize:    1,
	}
	response, err := visibilityArchiver.Query(context.Background(), s.testArchivalURI, request, searchattribute.TestNameTypeMap)
	s.NoError(err)
	s.NotNil(response)
	s.Empty(response.Executions)
	s.Empty(response.NextPageToken)
}

func (s *visibilityArchiverSuite) TestQuery_Fail_InvalidToken() {
	visibilityArchiver := s.newTestVisibilityArchiver()
	mockParser := NewMockQueryParser(s.controller)
	mockParser.EXPECT().Parse(gomock.Any()).Return(&parsedQuery{
		earliestCloseTime: time.Unix(0, 1),
		latestCloseTime:   time.Unix(0, 101),
	}, nil)
	visibilityArchiver.queryParser = mockParser
	request := &archiver.QueryVisibilityRequest{
		NamespaceID:   testNamespaceID,
		Query:         "parsed by mockParser",
		PageSize:      1,
		NextPageToken: []byte{1, 2, 3},
	}
	response, err := visibilityArchiver.Query(context.Background(), s.testArchivalURI, request, searchattribute.TestNameTypeMap)
	s.Error(err)
	s.Nil(response)
}

func (s *visibilityArchiverSuite) TestQuery_Success_NoNextPageToken() {
	visibilityArchiver := s.newTestVisibilityArchiver()
	mockParser := NewMockQueryParser(s.controller)
	mockParser.EXPECT().Parse(gomock.Any()).Return(&parsedQuery{
		earliestCloseTime: time.Unix(0, 1),
		latestCloseTime:   time.Unix(0, 10001),
		workflowID:        convert.StringPtr(testWorkflowID),
	}, nil)
	visibilityArchiver.queryParser = mockParser
	request := &archiver.QueryVisibilityRequest{
		NamespaceID: testNamespaceID,
		PageSize:    10,
		Query:       "parsed by mockParser",
	}
	URI, err := archiver.NewURI("file://" + s.testQueryDirectory)
	s.NoError(err)
	response, err := visibilityArchiver.Query(context.Background(), URI, request, searchattribute.TestNameTypeMap)
	s.NoError(err)
	s.NotNil(response)
	s.Nil(response.NextPageToken)
	s.Len(response.Executions, 1)
	ei, err := convertToExecutionInfo(s.visibilityRecords[0], searchattribute.TestNameTypeMap)
	s.NoError(err)
	s.Equal(ei, response.Executions[0])
}

func (s *visibilityArchiverSuite) TestQuery_Success_SmallPageSize() {
	visibilityArchiver := s.newTestVisibilityArchiver()
	mockParser := NewMockQueryParser(s.controller)
	mockParser.EXPECT().Parse(gomock.Any()).Return(&parsedQuery{
		earliestCloseTime: time.Unix(0, 1),
		latestCloseTime:   time.Unix(0, 10001),
		status:            toWorkflowExecutionStatusPtr(enumspb.WORKFLOW_EXECUTION_STATUS_FAILED),
	}, nil).AnyTimes()
	visibilityArchiver.queryParser = mockParser
	request := &archiver.QueryVisibilityRequest{
		NamespaceID: testNamespaceID,
		PageSize:    2,
		Query:       "parsed by mockParser",
	}
	URI, err := archiver.NewURI("file://" + s.testQueryDirectory)
	s.NoError(err)
	response, err := visibilityArchiver.Query(context.Background(), URI, request, searchattribute.TestNameTypeMap)
	s.NoError(err)
	s.NotNil(response)
	s.NotNil(response.NextPageToken)
	s.Len(response.Executions, 2)
	ei, err := convertToExecutionInfo(s.visibilityRecords[0], searchattribute.TestNameTypeMap)
	s.NoError(err)
	s.Equal(ei, response.Executions[0])
	ei, err = convertToExecutionInfo(s.visibilityRecords[1], searchattribute.TestNameTypeMap)
	s.NoError(err)
	s.Equal(ei, response.Executions[1])

	request.NextPageToken = response.NextPageToken
	response, err = visibilityArchiver.Query(context.Background(), URI, request, searchattribute.TestNameTypeMap)
	s.NoError(err)
	s.NotNil(response)
	s.Nil(response.NextPageToken)
	s.Len(response.Executions, 1)
	ei, err = convertToExecutionInfo(s.visibilityRecords[3], searchattribute.TestNameTypeMap)
	s.NoError(err)
	s.Equal(ei, response.Executions[0])
}

func (s *visibilityArchiverSuite) TestArchiveAndQuery() {
	dir := testhelper.MkdirTemp(s.T(), "", "TestArchiveAndQuery")

	visibilityArchiver := s.newTestVisibilityArchiver()
	mockParser := NewMockQueryParser(s.controller)
	mockParser.EXPECT().Parse(gomock.Any()).Return(&parsedQuery{
		earliestCloseTime: time.Unix(0, 10),
		latestCloseTime:   time.Unix(0, 10001),
		status:            toWorkflowExecutionStatusPtr(enumspb.WORKFLOW_EXECUTION_STATUS_FAILED),
	}, nil).AnyTimes()
	visibilityArchiver.queryParser = mockParser
	URI, err := archiver.NewURI("file://" + dir)
	s.NoError(err)
	for _, record := range s.visibilityRecords {
		err := visibilityArchiver.Archive(context.Background(), URI, (*archiverspb.VisibilityRecord)(record))
		s.NoError(err)
	}

	request := &archiver.QueryVisibilityRequest{
		NamespaceID: testNamespaceID,
		PageSize:    1,
		Query:       "parsed by mockParser",
	}
	executions := []*workflowpb.WorkflowExecutionInfo{}
	for len(executions) == 0 || request.NextPageToken != nil {
		response, err := visibilityArchiver.Query(context.Background(), URI, request, searchattribute.TestNameTypeMap)
		s.NoError(err)
		s.NotNil(response)
		executions = append(executions, response.Executions...)
		request.NextPageToken = response.NextPageToken
	}
	s.Len(executions, 2)
	ei, err := convertToExecutionInfo(s.visibilityRecords[0], searchattribute.TestNameTypeMap)
	s.NoError(err)
	s.Equal(ei, executions[0])
	ei, err = convertToExecutionInfo(s.visibilityRecords[1], searchattribute.TestNameTypeMap)
	s.NoError(err)
	s.Equal(ei, executions[1])
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
	s.visibilityRecords = []*archiverspb.VisibilityRecord{
		{
			NamespaceId:      testNamespaceID,
			Namespace:        testNamespace,
			WorkflowId:       testWorkflowID,
			RunId:            testRunID,
			WorkflowTypeName: testWorkflowTypeName,
			StartTime:        timestamp.UnixOrZeroTimePtr(1),
			CloseTime:        timestamp.UnixOrZeroTimePtr(10000),
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
			HistoryLength:    101,
		},
		{
			NamespaceId:      testNamespaceID,
			Namespace:        testNamespace,
			WorkflowId:       "some random workflow ID",
			RunId:            "some random run ID",
			WorkflowTypeName: testWorkflowTypeName,
			StartTime:        timestamp.UnixOrZeroTimePtr(2),
			ExecutionTime:    nil,
			CloseTime:        timestamp.UnixOrZeroTimePtr(1000),
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
			HistoryLength:    123,
		},
		{
			NamespaceId:      testNamespaceID,
			Namespace:        testNamespace,
			WorkflowId:       "another workflow ID",
			RunId:            "another run ID",
			WorkflowTypeName: testWorkflowTypeName,
			StartTime:        timestamp.UnixOrZeroTimePtr(3),
			ExecutionTime:    nil,
			CloseTime:        timestamp.UnixOrZeroTimePtr(10),
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
			HistoryLength:    456,
		},
		{
			NamespaceId:      testNamespaceID,
			Namespace:        testNamespace,
			WorkflowId:       "and another workflow ID",
			RunId:            "and another run ID",
			WorkflowTypeName: testWorkflowTypeName,
			StartTime:        timestamp.UnixOrZeroTimePtr(3),
			ExecutionTime:    nil,
			CloseTime:        timestamp.UnixOrZeroTimePtr(5),
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
			HistoryLength:    456,
		},
		{
			NamespaceId:      "some random namespace ID",
			Namespace:        "some random namespace name",
			WorkflowId:       "another workflow ID",
			RunId:            "another run ID",
			WorkflowTypeName: testWorkflowTypeName,
			StartTime:        timestamp.UnixOrZeroTimePtr(3),
			ExecutionTime:    nil,
			CloseTime:        timestamp.UnixOrZeroTimePtr(10000),
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
			HistoryLength:    456,
		},
	}

	for _, record := range s.visibilityRecords {
		s.writeVisibilityRecordForQueryTest(record)
	}
}

func (s *visibilityArchiverSuite) writeVisibilityRecordForQueryTest(record *archiverspb.VisibilityRecord) {
	data, err := encode(record)
	s.Require().NoError(err)
	filename := constructVisibilityFilename(record.CloseTime, record.GetRunId())
	s.Require().NoError(os.MkdirAll(path.Join(s.testQueryDirectory, record.GetNamespaceId()), testDirMode))
	err = writeFile(path.Join(s.testQueryDirectory, record.GetNamespaceId(), filename), data, testFileMode)
	s.Require().NoError(err)
}

func (s *visibilityArchiverSuite) assertFileExists(filepath string) {
	exists, err := fileExists(filepath)
	s.NoError(err)
	s.True(exists)
}
