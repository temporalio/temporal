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

package s3store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/searchattribute"

	archiverspb "go.temporal.io/server/api/archiver/v1"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/s3store/mocks"
	"go.temporal.io/server/common/codec"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/primitives/timestamp"

	commonpb "go.temporal.io/api/common/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
)

type visibilityArchiverSuite struct {
	*require.Assertions
	suite.Suite
	s3cli *mocks.MockS3API

	container         *archiver.VisibilityBootstrapContainer
	visibilityRecords []*archiverspb.VisibilityRecord

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

	s.s3cli.EXPECT().HeadBucketWithContext(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx aws.Context, input *s3.HeadBucketInput, options ...request.Option) (*s3.HeadBucketOutput, error) {
			if *input.Bucket != s.testArchivalURI.Hostname() {
				return nil, awserr.New("NotFound", "", nil)
			}

			return &s3.HeadBucketOutput{}, nil
		}).AnyTimes()

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

	s.testArchivalURI, err = archiver.NewURI(testBucketURI)
	s.Require().NoError(err)
	s.container = &archiver.VisibilityBootstrapContainer{
		Logger:        log.NewNoopLogger(),
		MetricsClient: metrics.NoopClient,
	}
}

func (s *visibilityArchiverSuite) TearDownSuite() {
}

func (s *visibilityArchiverSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())

	s.s3cli = mocks.NewMockS3API(s.controller)
	setupFsEmulation(s.s3cli)
	s.setupVisibilityDirectory()
}

func (s *visibilityArchiverSuite) TearDownTest() {
	s.controller.Finish()
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
		&archiverspb.VisibilityRecord{
			NamespaceId: testNamespaceID,
		},
		archiver.GetNonRetryableErrorOption(nonRetryableErr),
	)
	s.Equal(nonRetryableErr, err)
}

func (s *visibilityArchiverSuite) TestArchive_Success() {
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
	URI, err := archiver.NewURI(testBucketURI + "/test-archive-success")
	s.NoError(err)
	err = visibilityArchiver.Archive(context.Background(), URI, request)
	s.NoError(err)

	expectedKey := constructTimestampIndex(URI.Path(), testNamespaceID, primaryIndexKeyWorkflowID, testWorkflowID, secondaryIndexKeyCloseTimeout, timestamp.TimeValue(closeTimestamp), testRunID)
	data, err := download(context.Background(), visibilityArchiver.s3cli, URI, expectedKey)
	s.NoError(err, expectedKey)

	archivedRecord := &archiverspb.VisibilityRecord{}
	encoder := codec.NewJSONPBEncoder()
	err = encoder.Decode(data, archivedRecord)
	s.NoError(err)
	s.Equal(request, archivedRecord)
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
		workflowID:      convert.StringPtr(testWorkflowID),
		closeTime:       &time.Time{},
		searchPrecision: convert.StringPtr(PrecisionSecond),
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

func (s *visibilityArchiverSuite) TestQuery_Success_NoNextPageToken() {
	visibilityArchiver := s.newTestVisibilityArchiver()
	mockParser := NewMockQueryParser(s.controller)
	mockParser.EXPECT().Parse(gomock.Any()).Return(&parsedQuery{
		closeTime:       timestamp.TimePtr(time.Unix(0, int64(1*time.Hour)).UTC()),
		searchPrecision: convert.StringPtr(PrecisionHour),
		workflowID:      convert.StringPtr(testWorkflowID),
	}, nil)
	visibilityArchiver.queryParser = mockParser
	request := &archiver.QueryVisibilityRequest{
		NamespaceID: testNamespaceID,
		PageSize:    10,
		Query:       "parsed by mockParser",
	}
	URI, err := archiver.NewURI(testBucketURI)
	s.NoError(err)
	response, err := visibilityArchiver.Query(context.Background(), URI, request, searchattribute.TestNameTypeMap)
	s.NoError(err)
	s.NotNil(response)
	s.Nil(response.NextPageToken)
	s.Len(response.Executions, 2)
	ei, err := convertToExecutionInfo(s.visibilityRecords[0], searchattribute.TestNameTypeMap)
	s.NoError(err)
	s.Equal(response.Executions[0], ei)
}

func (s *visibilityArchiverSuite) TestQuery_Success_SmallPageSize() {
	visibilityArchiver := s.newTestVisibilityArchiver()
	mockParser := NewMockQueryParser(s.controller)
	mockParser.EXPECT().Parse(gomock.Any()).Return(&parsedQuery{
		closeTime:       timestamp.TimePtr(time.Unix(0, 0).UTC()),
		searchPrecision: convert.StringPtr(PrecisionDay),
		workflowID:      convert.StringPtr(testWorkflowID),
	}, nil).AnyTimes()
	visibilityArchiver.queryParser = mockParser
	request := &archiver.QueryVisibilityRequest{
		NamespaceID: testNamespaceID,
		PageSize:    2,
		Query:       "parsed by mockParser",
	}
	URI, err := archiver.NewURI(testBucketURI)
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
	ei, err = convertToExecutionInfo(s.visibilityRecords[2], searchattribute.TestNameTypeMap)
	s.NoError(err)
	s.Equal(ei, response.Executions[0])
}

type precisionTest struct {
	day       int
	hour      int
	minute    int
	second    int
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
		record := archiverspb.VisibilityRecord{
			NamespaceId:      testNamespaceID,
			Namespace:        testNamespace,
			WorkflowId:       testWorkflowID,
			RunId:            fmt.Sprintf("%s-%d", testRunID, i),
			WorkflowTypeName: testWorkflowTypeName,
			StartTime:        timestamp.TimePtr(time.Date(2000, 1, testData.day, testData.hour, testData.minute, testData.second, 0, time.UTC)),
			CloseTime:        timestamp.TimePtr(time.Date(2000, 1, testData.day, testData.hour, testData.minute, testData.second, 0, time.UTC)),
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
			HistoryLength:    101,
		}
		err := visibilityArchiver.Archive(context.Background(), URI, &record)
		s.NoError(err, "case %d", i)
	}

	request := &archiver.QueryVisibilityRequest{
		NamespaceID: testNamespaceID,
		PageSize:    100,
		Query:       "parsed by mockParser",
	}

	for i, testData := range precisionTests {
		mockParser := NewMockQueryParser(s.controller)
		mockParser.EXPECT().Parse(gomock.Any()).Return(&parsedQuery{
			closeTime:       timestamp.TimePtr(time.Date(2000, 1, testData.day, testData.hour, testData.minute, testData.second, 0, time.UTC)),
			searchPrecision: convert.StringPtr(testData.precision),
			workflowID:      convert.StringPtr(testWorkflowID),
		}, nil).AnyTimes()
		visibilityArchiver.queryParser = mockParser

		response, err := visibilityArchiver.Query(context.Background(), URI, request, searchattribute.TestNameTypeMap)
		s.NoError(err)
		s.NotNil(response)
		s.Len(response.Executions, 2, "Iteration ", i)

		mockParser = NewMockQueryParser(s.controller)
		mockParser.EXPECT().Parse(gomock.Any()).Return(&parsedQuery{
			startTime:       timestamp.TimePtr(time.Date(2000, 1, testData.day, testData.hour, testData.minute, testData.second, 0, time.UTC)),
			searchPrecision: convert.StringPtr(testData.precision),
			workflowID:      convert.StringPtr(testWorkflowID),
		}, nil).AnyTimes()
		visibilityArchiver.queryParser = mockParser

		response, err = visibilityArchiver.Query(context.Background(), URI, request, searchattribute.TestNameTypeMap)
		s.NoError(err)
		s.NotNil(response)
		s.Len(response.Executions, 2, "Iteration ", i)

		mockParser = NewMockQueryParser(s.controller)
		mockParser.EXPECT().Parse(gomock.Any()).Return(&parsedQuery{
			closeTime:        timestamp.TimePtr(time.Date(2000, 1, testData.day, testData.hour, testData.minute, testData.second, 0, time.UTC)),
			searchPrecision:  convert.StringPtr(testData.precision),
			workflowTypeName: convert.StringPtr(testWorkflowTypeName),
		}, nil).AnyTimes()
		visibilityArchiver.queryParser = mockParser

		response, err = visibilityArchiver.Query(context.Background(), URI, request, searchattribute.TestNameTypeMap)
		s.NoError(err)
		s.NotNil(response)
		s.Len(response.Executions, 2, "Iteration ", i)

		mockParser = NewMockQueryParser(s.controller)
		mockParser.EXPECT().Parse(gomock.Any()).Return(&parsedQuery{
			startTime:        timestamp.TimePtr(time.Date(2000, 1, testData.day, testData.hour, testData.minute, testData.second, 0, time.UTC)),
			searchPrecision:  convert.StringPtr(testData.precision),
			workflowTypeName: convert.StringPtr(testWorkflowTypeName),
		}, nil).AnyTimes()
		visibilityArchiver.queryParser = mockParser

		response, err = visibilityArchiver.Query(context.Background(), URI, request, searchattribute.TestNameTypeMap)
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
		err := visibilityArchiver.Archive(context.Background(), URI, (*archiverspb.VisibilityRecord)(record))
		s.NoError(err)
	}

	mockParser := NewMockQueryParser(s.controller)
	mockParser.EXPECT().Parse(gomock.Any()).Return(&parsedQuery{
		workflowID: convert.StringPtr(testWorkflowID),
	}, nil).AnyTimes()
	visibilityArchiver.queryParser = mockParser
	request := &archiver.QueryVisibilityRequest{
		NamespaceID: testNamespaceID,
		PageSize:    1,
		Query:       "parsed by mockParser",
	}
	executions := []*workflowpb.WorkflowExecutionInfo{}
	first := true
	for first || request.NextPageToken != nil {
		response, err := visibilityArchiver.Query(context.Background(), URI, request, searchattribute.TestNameTypeMap)
		s.NoError(err)
		s.NotNil(response)
		executions = append(executions, response.Executions...)
		request.NextPageToken = response.NextPageToken
		first = false
	}
	s.Len(executions, 3)
	ei, err := convertToExecutionInfo(s.visibilityRecords[0], searchattribute.TestNameTypeMap)
	s.NoError(err)
	s.Equal(ei, executions[0])
	ei, err = convertToExecutionInfo(s.visibilityRecords[1], searchattribute.TestNameTypeMap)
	s.NoError(err)
	s.Equal(ei, executions[1])
	ei, err = convertToExecutionInfo(s.visibilityRecords[2], searchattribute.TestNameTypeMap)
	s.NoError(err)
	s.Equal(ei, executions[2])

	mockParser = NewMockQueryParser(s.controller)
	mockParser.EXPECT().Parse(gomock.Any()).Return(&parsedQuery{
		workflowTypeName: convert.StringPtr(testWorkflowTypeName),
	}, nil).AnyTimes()
	visibilityArchiver.queryParser = mockParser
	request = &archiver.QueryVisibilityRequest{
		NamespaceID: testNamespaceID,
		PageSize:    1,
		Query:       "parsed by mockParser",
	}
	executions = []*workflowpb.WorkflowExecutionInfo{}
	first = true
	for first || request.NextPageToken != nil {
		response, err := visibilityArchiver.Query(context.Background(), URI, request, searchattribute.TestNameTypeMap)
		s.NoError(err)
		s.NotNil(response)
		executions = append(executions, response.Executions...)
		request.NextPageToken = response.NextPageToken
		first = false
	}
	s.Len(executions, 3)
	ei, err = convertToExecutionInfo(s.visibilityRecords[0], searchattribute.TestNameTypeMap)
	s.NoError(err)
	s.Equal(ei, executions[0])
	ei, err = convertToExecutionInfo(s.visibilityRecords[1], searchattribute.TestNameTypeMap)
	s.NoError(err)
	s.Equal(ei, executions[1])
	ei, err = convertToExecutionInfo(s.visibilityRecords[2], searchattribute.TestNameTypeMap)
	s.NoError(err)
	s.Equal(ei, executions[2])
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
			CloseTime:        timestamp.UnixOrZeroTimePtr(int64(time.Hour)),
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
			HistoryLength:    101,
		},
		{
			NamespaceId:      testNamespaceID,
			Namespace:        testNamespace,
			WorkflowId:       testWorkflowID,
			RunId:            testRunID + "1",
			WorkflowTypeName: testWorkflowTypeName,
			StartTime:        timestamp.UnixOrZeroTimePtr(1),
			CloseTime:        timestamp.UnixOrZeroTimePtr(int64(time.Hour + 30*time.Minute)),
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
			HistoryLength:    101,
		},
		{
			NamespaceId:      testNamespaceID,
			Namespace:        testNamespace,
			WorkflowId:       testWorkflowID,
			RunId:            testRunID + "1",
			WorkflowTypeName: testWorkflowTypeName,
			StartTime:        timestamp.UnixOrZeroTimePtr(1),
			CloseTime:        timestamp.UnixOrZeroTimePtr(int64(3 * time.Hour)),
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
			HistoryLength:    101,
		},
	}
	visibilityArchiver := s.newTestVisibilityArchiver()
	for _, record := range s.visibilityRecords {
		s.writeVisibilityRecordForQueryTest(visibilityArchiver, record)
	}
}

func (s *visibilityArchiverSuite) writeVisibilityRecordForQueryTest(visibilityArchiver *visibilityArchiver, record *archiverspb.VisibilityRecord) {
	err := visibilityArchiver.Archive(context.Background(), s.testArchivalURI, record)
	s.Require().NoError(err)
}
