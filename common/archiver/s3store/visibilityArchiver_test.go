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

package s3store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/mock"

	"github.com/uber-go/tally"

	"github.com/uber/cadence/common/metrics"

	"go.uber.org/zap"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/archiver/s3store/mocks"
	"github.com/uber/cadence/common/log/loggerimpl"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/log"
)

type visibilityArchiverSuite struct {
	*require.Assertions
	suite.Suite
	s3cli *mocks.S3API

	container         *archiver.VisibilityBootstrapContainer
	logger            log.Logger
	visibilityRecords []*visibilityRecord

	controller      *gomock.Controller
	testArchivalURI archiver.URI
}

func TestVisibilityArchiverSuite(t *testing.T) {
	suite.Run(t, new(visibilityArchiverSuite))
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
			URI:         "s3://",
			expectedErr: errNoBucketSpecified,
		},
		{
			URI:         "s3:///test",
			expectedErr: errNoBucketSpecified,
		},
		{
			URI:         "s3://bucket/a/b/c",
			expectedErr: errBucketNotExists,
		},
		{
			URI:         testBucketURI,
			expectedErr: nil,
		},
	}

	s.s3cli.On("HeadBucketWithContext", mock.Anything, mock.MatchedBy(func(input *s3.HeadBucketInput) bool {
		return *input.Bucket != s.testArchivalURI.Hostname()
	})).Return(nil, awserr.New("NotFound", "", nil))
	s.s3cli.On("HeadBucketWithContext", mock.Anything, mock.Anything).Return(&s3.HeadBucketOutput{}, nil)

	visibilityArchiver := s.newTestVisibilityArchiver()
	for _, tc := range testCases {
		URI, err := archiver.NewURI(tc.URI)
		s.NoError(err)
		s.Equal(tc.expectedErr, visibilityArchiver.ValidateURI(URI))
	}
}

func (s *visibilityArchiverSuite) newTestVisibilityArchiver() *visibilityArchiver {
	archiver := &visibilityArchiver{
		container:   s.container,
		s3cli:       s.s3cli,
		queryParser: NewQueryParser(),
	}
	return archiver
}

const (
	testWorkflowTypeName = "test-workflow-type"
)

func (s *visibilityArchiverSuite) SetupSuite() {
	var err error
	scope := tally.NewTestScope("test", nil)
	s.s3cli = &mocks.S3API{}
	setupFsEmulation(s.s3cli)

	s.testArchivalURI, err = archiver.NewURI(testBucketURI)
	s.Require().NoError(err)

	zapLogger := zap.NewNop()
	s.container = &archiver.VisibilityBootstrapContainer{
		Logger:        loggerimpl.NewLogger(zapLogger),
		MetricsClient: metrics.NewClient(scope, metrics.VisibilityArchiverScope),
	}
	s.setupVisibilityDirectory()
}

func (s *visibilityArchiverSuite) TearDownSuite() {

}

func (s *visibilityArchiverSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
}

func (s *visibilityArchiverSuite) TearDownTest() {
	s.controller.Finish()
}
func (s *visibilityArchiverSuite) TestArchive_Fail_InvalidURI() {
	visibilityArchiver := s.newTestVisibilityArchiver()
	URI, err := archiver.NewURI("wrongscheme://")
	s.NoError(err)
	request := &archiver.ArchiveVisibilityRequest{
		DomainName:         testDomainName,
		DomainID:           testDomainID,
		WorkflowID:         testWorkflowID,
		RunID:              testRunID,
		WorkflowTypeName:   testWorkflowTypeName,
		StartTimestamp:     time.Now().UnixNano(),
		ExecutionTimestamp: 0, // workflow without backoff
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
		&archiver.ArchiveVisibilityRequest{
			DomainID: testDomainID,
		},
		archiver.GetNonRetriableErrorOption(nonRetriableErr),
	)
	s.Equal(nonRetriableErr, err)
}

func (s *visibilityArchiverSuite) TestArchive_Success() {
	visibilityArchiver := s.newTestVisibilityArchiver()
	closeTimestamp := time.Now()
	request := &archiver.ArchiveVisibilityRequest{
		DomainID:           testDomainID,
		DomainName:         testDomainName,
		WorkflowID:         testWorkflowID,
		RunID:              testRunID,
		WorkflowTypeName:   testWorkflowTypeName,
		StartTimestamp:     closeTimestamp.Add(-time.Hour).UnixNano(),
		ExecutionTimestamp: 0, // workflow without backoff
		CloseTimestamp:     closeTimestamp.UnixNano(),
		CloseStatus:        shared.WorkflowExecutionCloseStatusFailed,
		HistoryLength:      int64(101),
		Memo: &shared.Memo{
			Fields: map[string][]byte{
				"testFields": {1, 2, 3},
			},
		},
		SearchAttributes: map[string]string{
			"testAttribute": "456",
		},
	}
	URI, err := archiver.NewURI(testBucketURI + "/test-archive-success")
	s.NoError(err)
	err = visibilityArchiver.Archive(context.Background(), URI, request)
	s.NoError(err)

	expectedKey := constructTimestampIndex(URI.Path(), testDomainID, primaryIndexKeyWorkflowID, testWorkflowID, secondaryIndexKeyCloseTimeout, closeTimestamp.UnixNano(), testRunID)
	data, err := download(context.Background(), visibilityArchiver.s3cli, URI, expectedKey)
	s.NoError(err, expectedKey)

	archivedRecord := &archiver.ArchiveVisibilityRequest{}
	err = json.Unmarshal(data, archivedRecord)
	s.NoError(err)
	s.Equal(request, archivedRecord)
}

func (s *visibilityArchiverSuite) TestQuery_Fail_InvalidURI() {
	visibilityArchiver := s.newTestVisibilityArchiver()
	URI, err := archiver.NewURI("wrongscheme://")
	s.NoError(err)
	request := &archiver.QueryVisibilityRequest{
		DomainID: testDomainID,
		PageSize: 1,
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

func (s *visibilityArchiverSuite) TestQuery_Fail_InvalidQuery() {
	visibilityArchiver := s.newTestVisibilityArchiver()
	mockParser := NewMockQueryParser(s.controller)
	mockParser.EXPECT().Parse(gomock.Any()).Return(nil, errors.New("invalid query"))
	visibilityArchiver.queryParser = mockParser
	response, err := visibilityArchiver.Query(context.Background(), s.testArchivalURI, &archiver.QueryVisibilityRequest{
		DomainID: "some random domainID",
		PageSize: 10,
		Query:    "some invalid query",
	})
	s.Error(err)
	s.Nil(response)
}
func (s *visibilityArchiverSuite) TestQuery_Success_DirectoryNotExist() {
	visibilityArchiver := s.newTestVisibilityArchiver()
	mockParser := NewMockQueryParser(s.controller)
	mockParser.EXPECT().Parse(gomock.Any()).Return(&parsedQuery{
		workflowID:      common.StringPtr(testWorkflowID),
		closeTime:       common.Int64Ptr(0),
		searchPrecision: common.StringPtr(PrecisionSecond),
	}, nil)
	visibilityArchiver.queryParser = mockParser
	request := &archiver.QueryVisibilityRequest{
		DomainID: testDomainID,
		Query:    "parsed by mockParser",
		PageSize: 1,
	}
	response, err := visibilityArchiver.Query(context.Background(), s.testArchivalURI, request)
	s.NoError(err)
	s.NotNil(response)
	s.Empty(response.Executions)
	s.Empty(response.NextPageToken)
}

func (s *visibilityArchiverSuite) TestQuery_Success_NoNextPageToken() {
	visibilityArchiver := s.newTestVisibilityArchiver()
	mockParser := NewMockQueryParser(s.controller)
	mockParser.EXPECT().Parse(gomock.Any()).Return(&parsedQuery{
		closeTime:       common.Int64Ptr(int64(1 * time.Hour)),
		searchPrecision: common.StringPtr(PrecisionHour),
		workflowID:      common.StringPtr(testWorkflowID),
	}, nil)
	visibilityArchiver.queryParser = mockParser
	request := &archiver.QueryVisibilityRequest{
		DomainID: testDomainID,
		PageSize: 10,
		Query:    "parsed by mockParser",
	}
	URI, err := archiver.NewURI(testBucketURI)
	s.NoError(err)
	response, err := visibilityArchiver.Query(context.Background(), URI, request)
	s.NoError(err)
	s.NotNil(response)
	s.Nil(response.NextPageToken)
	s.Len(response.Executions, 2)
	s.Equal(convertToExecutionInfo(s.visibilityRecords[0]), response.Executions[0])
}

func (s *visibilityArchiverSuite) TestQuery_Success_SmallPageSize() {
	visibilityArchiver := s.newTestVisibilityArchiver()
	mockParser := NewMockQueryParser(s.controller)
	mockParser.EXPECT().Parse(gomock.Any()).Return(&parsedQuery{
		closeTime:       common.Int64Ptr(0),
		searchPrecision: common.StringPtr(PrecisionDay),
		workflowID:      common.StringPtr(testWorkflowID),
	}, nil).AnyTimes()
	visibilityArchiver.queryParser = mockParser
	request := &archiver.QueryVisibilityRequest{
		DomainID: testDomainID,
		PageSize: 2,
		Query:    "parsed by mockParser",
	}
	URI, err := archiver.NewURI(testBucketURI)
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
	s.Equal(convertToExecutionInfo(s.visibilityRecords[2]), response.Executions[0])
}

type precisionTest struct {
	day       int64
	hour      int64
	minute    int64
	second    int64
	precision string
}

func (s *visibilityArchiverSuite) TestArchiveAndQueryPrecisions() {
	precisionTests := []*precisionTest{
		{
			day:       1,
			hour:      0,
			minute:    0,
			second:    0,
			precision: PrecisionDay,
		},
		{
			day:       1,
			hour:      1,
			minute:    0,
			second:    0,
			precision: PrecisionDay,
		},
		{
			day:       2,
			hour:      1,
			minute:    0,
			second:    0,
			precision: PrecisionHour,
		},
		{
			day:       2,
			hour:      1,
			minute:    30,
			second:    0,
			precision: PrecisionHour,
		},
		{
			day:       3,
			hour:      2,
			minute:    1,
			second:    0,
			precision: PrecisionMinute,
		},
		{
			day:       3,
			hour:      2,
			minute:    1,
			second:    30,
			precision: PrecisionMinute,
		},
		{
			day:       4,
			hour:      3,
			minute:    2,
			second:    1,
			precision: PrecisionSecond,
		},
		{
			day:       4,
			hour:      3,
			minute:    2,
			second:    1,
			precision: PrecisionSecond,
		},
		{
			day:       4,
			hour:      3,
			minute:    2,
			second:    2,
			precision: PrecisionSecond,
		},
		{
			day:       4,
			hour:      3,
			minute:    2,
			second:    2,
			precision: PrecisionSecond,
		},
	}
	visibilityArchiver := s.newTestVisibilityArchiver()
	URI, err := archiver.NewURI(testBucketURI + "/archive-and-query-precision")
	s.NoError(err)

	for i, testData := range precisionTests {
		record := archiver.ArchiveVisibilityRequest{
			DomainID:         testDomainID,
			DomainName:       testDomainName,
			WorkflowID:       testWorkflowID,
			RunID:            fmt.Sprintf("%s-%d", testRunID, i),
			WorkflowTypeName: testWorkflowTypeName,
			StartTimestamp:   testData.day*int64(time.Hour)*24 + testData.hour*int64(time.Hour) + testData.minute*int64(time.Minute) + testData.second*int64(time.Second),
			CloseTimestamp:   (testData.day+30)*int64(time.Hour)*24 + testData.hour*int64(time.Hour) + testData.minute*int64(time.Minute) + testData.second*int64(time.Second),
			CloseStatus:      shared.WorkflowExecutionCloseStatusFailed,
			HistoryLength:    101,
		}
		err := visibilityArchiver.Archive(context.Background(), URI, &record)
		s.NoError(err)
	}

	request := &archiver.QueryVisibilityRequest{
		DomainID: testDomainID,
		PageSize: 100,
		Query:    "parsed by mockParser",
	}

	for i, testData := range precisionTests {
		mockParser := NewMockQueryParser(s.controller)
		mockParser.EXPECT().Parse(gomock.Any()).Return(&parsedQuery{
			closeTime:       common.Int64Ptr((testData.day+30)*int64(time.Hour)*24 + testData.hour*int64(time.Hour) + testData.minute*int64(time.Minute) + testData.second*int64(time.Second)),
			searchPrecision: common.StringPtr(testData.precision),
			workflowID:      common.StringPtr(testWorkflowID),
		}, nil).AnyTimes()
		visibilityArchiver.queryParser = mockParser

		response, err := visibilityArchiver.Query(context.Background(), URI, request)
		s.NoError(err)
		s.NotNil(response)
		s.Len(response.Executions, 2, "Iteration ", i)

		mockParser = NewMockQueryParser(s.controller)
		mockParser.EXPECT().Parse(gomock.Any()).Return(&parsedQuery{
			startTime:       common.Int64Ptr((testData.day)*int64(time.Hour)*24 + testData.hour*int64(time.Hour) + testData.minute*int64(time.Minute) + testData.second*int64(time.Second)),
			searchPrecision: common.StringPtr(testData.precision),
			workflowID:      common.StringPtr(testWorkflowID),
		}, nil).AnyTimes()
		visibilityArchiver.queryParser = mockParser

		response, err = visibilityArchiver.Query(context.Background(), URI, request)
		s.NoError(err)
		s.NotNil(response)
		s.Len(response.Executions, 2, "Iteration ", i)

		mockParser = NewMockQueryParser(s.controller)
		mockParser.EXPECT().Parse(gomock.Any()).Return(&parsedQuery{
			closeTime:        common.Int64Ptr((testData.day+30)*int64(time.Hour)*24 + testData.hour*int64(time.Hour) + testData.minute*int64(time.Minute) + testData.second*int64(time.Second)),
			searchPrecision:  common.StringPtr(testData.precision),
			workflowTypeName: common.StringPtr(testWorkflowTypeName),
		}, nil).AnyTimes()
		visibilityArchiver.queryParser = mockParser

		response, err = visibilityArchiver.Query(context.Background(), URI, request)
		s.NoError(err)
		s.NotNil(response)
		s.Len(response.Executions, 2, "Iteration ", i)

		mockParser = NewMockQueryParser(s.controller)
		mockParser.EXPECT().Parse(gomock.Any()).Return(&parsedQuery{
			startTime:        common.Int64Ptr((testData.day)*int64(time.Hour)*24 + testData.hour*int64(time.Hour) + testData.minute*int64(time.Minute) + testData.second*int64(time.Second)),
			searchPrecision:  common.StringPtr(testData.precision),
			workflowTypeName: common.StringPtr(testWorkflowTypeName),
		}, nil).AnyTimes()
		visibilityArchiver.queryParser = mockParser

		response, err = visibilityArchiver.Query(context.Background(), URI, request)
		s.NoError(err)
		s.NotNil(response)
		s.Len(response.Executions, 2, "Iteration ", i)
	}
}
func (s *visibilityArchiverSuite) TestArchiveAndQuery() {
	visibilityArchiver := s.newTestVisibilityArchiver()
	URI, err := archiver.NewURI(testBucketURI + "/archive-and-query")
	s.NoError(err)
	for _, record := range s.visibilityRecords {
		err := visibilityArchiver.Archive(context.Background(), URI, (*archiver.ArchiveVisibilityRequest)(record))
		s.NoError(err)
	}

	mockParser := NewMockQueryParser(s.controller)
	mockParser.EXPECT().Parse(gomock.Any()).Return(&parsedQuery{
		workflowID: common.StringPtr(testWorkflowID),
	}, nil).AnyTimes()
	visibilityArchiver.queryParser = mockParser
	request := &archiver.QueryVisibilityRequest{
		DomainID: testDomainID,
		PageSize: 1,
		Query:    "parsed by mockParser",
	}
	executions := []*shared.WorkflowExecutionInfo{}
	var first = true
	for first || request.NextPageToken != nil {
		response, err := visibilityArchiver.Query(context.Background(), URI, request)
		s.NoError(err)
		s.NotNil(response)
		executions = append(executions, response.Executions...)
		request.NextPageToken = response.NextPageToken
		first = false
	}
	s.Len(executions, 3)
	s.Equal(convertToExecutionInfo(s.visibilityRecords[0]), executions[0])
	s.Equal(convertToExecutionInfo(s.visibilityRecords[1]), executions[1])
	s.Equal(convertToExecutionInfo(s.visibilityRecords[2]), executions[2])

	mockParser = NewMockQueryParser(s.controller)
	mockParser.EXPECT().Parse(gomock.Any()).Return(&parsedQuery{
		workflowTypeName: common.StringPtr(testWorkflowTypeName),
	}, nil).AnyTimes()
	visibilityArchiver.queryParser = mockParser
	request = &archiver.QueryVisibilityRequest{
		DomainID: testDomainID,
		PageSize: 1,
		Query:    "parsed by mockParser",
	}
	executions = []*shared.WorkflowExecutionInfo{}
	first = true
	for first || request.NextPageToken != nil {
		response, err := visibilityArchiver.Query(context.Background(), URI, request)
		s.NoError(err)
		s.NotNil(response)
		executions = append(executions, response.Executions...)
		request.NextPageToken = response.NextPageToken
		first = false
	}
	s.Len(executions, 3)
	s.Equal(convertToExecutionInfo(s.visibilityRecords[0]), executions[0])
	s.Equal(convertToExecutionInfo(s.visibilityRecords[1]), executions[1])
	s.Equal(convertToExecutionInfo(s.visibilityRecords[2]), executions[2])
}

func (s *visibilityArchiverSuite) setupVisibilityDirectory() {
	s.visibilityRecords = []*visibilityRecord{
		{
			DomainID:         testDomainID,
			DomainName:       testDomainName,
			WorkflowID:       testWorkflowID,
			RunID:            testRunID,
			WorkflowTypeName: testWorkflowTypeName,
			StartTimestamp:   1,
			CloseTimestamp:   int64(1 * time.Hour),
			CloseStatus:      shared.WorkflowExecutionCloseStatusFailed,
			HistoryLength:    101,
		},
		{
			DomainID:         testDomainID,
			DomainName:       testDomainName,
			WorkflowID:       testWorkflowID,
			RunID:            testRunID + "1",
			WorkflowTypeName: testWorkflowTypeName,
			StartTimestamp:   1,
			CloseTimestamp:   int64(1*time.Hour + 30*time.Minute),
			CloseStatus:      shared.WorkflowExecutionCloseStatusFailed,
			HistoryLength:    101,
		},
		{
			DomainID:         testDomainID,
			DomainName:       testDomainName,
			WorkflowID:       testWorkflowID,
			RunID:            testRunID + "1",
			WorkflowTypeName: testWorkflowTypeName,
			StartTimestamp:   1,
			CloseTimestamp:   int64(3 * time.Hour),
			CloseStatus:      shared.WorkflowExecutionCloseStatusFailed,
			HistoryLength:    101,
		},
	}
	visibilityArchiver := s.newTestVisibilityArchiver()
	for _, record := range s.visibilityRecords {
		s.writeVisibilityRecordForQueryTest(visibilityArchiver, record)
	}
}

func (s *visibilityArchiverSuite) writeVisibilityRecordForQueryTest(visibilityArchiver *visibilityArchiver, record *visibilityRecord) {
	err := visibilityArchiver.Archive(context.Background(), s.testArchivalURI, (*archiver.ArchiveVisibilityRequest)(record))
	s.Require().NoError(err)
}
