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

package gcloud

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/zap"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/archiver/gcloud/connector/mocks"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
)

const (
	testWorkflowTypeName    = "test-workflow-type"
	exampleVisibilityRecord = `{"DomainID":"test-domain-id","DomainName":"test-domain-name","WorkflowID":"test-workflow-id","RunID":"test-run-id","WorkflowTypeName":"test-workflow-type","StartTimestamp":1580896574804475000,"ExecutionTimestamp":0,"CloseTimestamp":1580896575946478000,"CloseStatus":"COMPLETED","HistoryLength":36,"Memo":null,"SearchAttributes":{},"HistoryArchivalURI":"gs://my-bucket-cad/cadence_archival/development"}`
)

func (s *visibilityArchiverSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	zapLogger := zap.NewNop()
	s.container = &archiver.VisibilityBootstrapContainer{
		Logger:        loggerimpl.NewLogger(zapLogger),
		MetricsClient: metrics.NewClient(tally.NoopScope, metrics.History),
	}
	s.expectedVisibilityRecords = []*visibilityRecord{
		{
			DomainID:         testDomainID,
			DomainName:       testDomainName,
			WorkflowID:       testWorkflowID,
			RunID:            testRunID,
			WorkflowTypeName: testWorkflowTypeName,
			StartTimestamp:   1580896574804475000,
			CloseTimestamp:   1580896575946478000,
			CloseStatus:      shared.WorkflowExecutionCloseStatusCompleted,
			HistoryLength:    36,
		},
	}
}
func TestVisibilityArchiverSuiteSuite(t *testing.T) {
	suite.Run(t, new(visibilityArchiverSuite))
}

type visibilityArchiverSuite struct {
	*require.Assertions
	suite.Suite
	container                 *archiver.VisibilityBootstrapContainer
	expectedVisibilityRecords []*visibilityRecord
}

func (s *visibilityArchiverSuite) TestValidateVisibilityURI() {
	testCases := []struct {
		URI         string
		expectedErr error
	}{
		{
			URI:         "wrongscheme:///a/b/c",
			expectedErr: archiver.ErrURISchemeMismatch,
		},
		{
			URI:         "gs:my-bucket-cad/cadence_archival/visibility",
			expectedErr: archiver.ErrInvalidURI,
		},
		{
			URI:         "gs://",
			expectedErr: archiver.ErrInvalidURI,
		},
		{
			URI:         "gs://my-bucket-cad",
			expectedErr: archiver.ErrInvalidURI,
		},
		{
			URI:         "gs:/my-bucket-cad/cadence_archival/visibility",
			expectedErr: archiver.ErrInvalidURI,
		},
		{
			URI:         "gs://my-bucket-cad/cadence_archival/visibility",
			expectedErr: nil,
		},
	}

	storageWrapper := &mocks.Client{}
	storageWrapper.On("Exist", mock.Anything, mock.Anything, "").Return(false, nil)
	visibilityArchiver := new(visibilityArchiver)
	visibilityArchiver.gcloudStorage = storageWrapper
	for _, tc := range testCases {
		URI, err := archiver.NewURI(tc.URI)
		s.NoError(err)
		s.Equal(tc.expectedErr, visibilityArchiver.ValidateURI(URI))
	}
}

func (s *visibilityArchiverSuite) TestArchive_Fail_InvalidVisibilityURI() {
	ctx := context.Background()
	URI, err := archiver.NewURI("wrongscheme://")
	s.NoError(err)
	storageWrapper := &mocks.Client{}
	storageWrapper.On("Exist", mock.Anything, URI, "").Return(true, nil).Times(1)
	mockCtrl := gomock.NewController(s.T())
	defer mockCtrl.Finish()

	visibilityArchiver := newVisibilityArchiver(s.container, storageWrapper)
	s.NoError(err)
	request := &archiver.ArchiveVisibilityRequest{
		DomainID:   testDomainID,
		DomainName: testDomainName,
		WorkflowID: testWorkflowID,
		RunID:      testRunID,
	}

	err = visibilityArchiver.Archive(ctx, URI, request)
	s.Error(err)
}

func (s *visibilityArchiverSuite) TestQuery_Fail_InvalidVisibilityURI() {
	ctx := context.Background()
	URI, err := archiver.NewURI("wrongscheme://")
	s.NoError(err)
	storageWrapper := &mocks.Client{}
	storageWrapper.On("Exist", mock.Anything, URI, "").Return(true, nil).Times(1)
	mockCtrl := gomock.NewController(s.T())
	defer mockCtrl.Finish()

	visibilityArchiver := newVisibilityArchiver(s.container, storageWrapper)
	s.NoError(err)
	request := &archiver.QueryVisibilityRequest{
		DomainID: testDomainID,
		PageSize: 10,
		Query:    "WorkflowType='type::example' AND CloseTime='2020-02-05T11:00:00Z' AND SearchPrecision='Day'",
	}

	_, err = visibilityArchiver.Query(ctx, URI, request)
	s.Error(err)
}

func (s *visibilityArchiverSuite) TestVisibilityArchive() {
	ctx := context.Background()
	URI, err := archiver.NewURI("gs://my-bucket-cad/cadence_archival/visibility")
	s.NoError(err)
	storageWrapper := &mocks.Client{}
	storageWrapper.On("Exist", mock.Anything, URI, mock.Anything).Return(false, nil)
	storageWrapper.On("Upload", mock.Anything, URI, mock.Anything, mock.Anything).Return(nil)
	mockCtrl := gomock.NewController(s.T())
	defer mockCtrl.Finish()

	visibilityArchiver := newVisibilityArchiver(s.container, storageWrapper)
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

	err = visibilityArchiver.Archive(ctx, URI, request)
	s.NoError(err)
}

func (s *visibilityArchiverSuite) TestQuery_Fail_InvalidQuery() {
	ctx := context.Background()
	URI, err := archiver.NewURI("gs://my-bucket-cad/cadence_archival/visibility")
	s.NoError(err)
	storageWrapper := &mocks.Client{}
	storageWrapper.On("Exist", mock.Anything, URI, mock.Anything).Return(false, nil)
	visibilityArchiver := newVisibilityArchiver(s.container, storageWrapper)
	s.NoError(err)
	mockCtrl := gomock.NewController(s.T())
	defer mockCtrl.Finish()

	mockParser := NewMockQueryParser(mockCtrl)
	mockParser.EXPECT().Parse(gomock.Any()).Return(nil, errors.New("invalid query"))
	visibilityArchiver.queryParser = mockParser
	response, err := visibilityArchiver.Query(ctx, URI, &archiver.QueryVisibilityRequest{
		DomainID: "some random domainID",
		PageSize: 10,
		Query:    "some invalid query",
	})
	s.Error(err)
	s.Nil(response)
}

func (s *visibilityArchiverSuite) TestQuery_Fail_InvalidToken() {
	URI, err := archiver.NewURI("gs://my-bucket-cad/cadence_archival/visibility")
	s.NoError(err)
	storageWrapper := &mocks.Client{}
	storageWrapper.On("Exist", mock.Anything, URI, mock.Anything).Return(false, nil)
	visibilityArchiver := newVisibilityArchiver(s.container, storageWrapper)
	s.NoError(err)
	mockCtrl := gomock.NewController(s.T())
	defer mockCtrl.Finish()

	mockParser := NewMockQueryParser(mockCtrl)
	mockParser.EXPECT().Parse(gomock.Any()).Return(&parsedQuery{
		closeTime: int64(101),
		startTime: int64(1),
	}, nil)
	visibilityArchiver.queryParser = mockParser
	request := &archiver.QueryVisibilityRequest{
		DomainID:      testDomainID,
		Query:         "parsed by mockParser",
		PageSize:      1,
		NextPageToken: []byte{1, 2, 3},
	}
	response, err := visibilityArchiver.Query(context.Background(), URI, request)
	s.Error(err)
	s.Nil(response)
}

func (s *visibilityArchiverSuite) TestQuery_Success_NoNextPageToken() {
	ctx := context.Background()
	URI, err := archiver.NewURI("gs://my-bucket-cad/cadence_archival/visibility")
	s.NoError(err)
	storageWrapper := &mocks.Client{}
	storageWrapper.On("Exist", mock.Anything, URI, mock.Anything).Return(false, nil)
	storageWrapper.On("QueryWithFilters", mock.Anything, URI, mock.Anything, 10, 0, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]string{"closeTimeout_2020-02-05T09:56:14Z_test-workflow-id_MobileOnlyWorkflow::processMobileOnly_test-run-id.visibility"}, true, 1, nil).Times(1)
	storageWrapper.On("Get", mock.Anything, URI, "test-domain-id/closeTimeout_2020-02-05T09:56:14Z_test-workflow-id_MobileOnlyWorkflow::processMobileOnly_test-run-id.visibility").Return([]byte(exampleVisibilityRecord), nil)

	visibilityArchiver := newVisibilityArchiver(s.container, storageWrapper)
	s.NoError(err)
	mockCtrl := gomock.NewController(s.T())
	defer mockCtrl.Finish()

	mockParser := NewMockQueryParser(mockCtrl)
	dayPrecision := string("Day")
	mockParser.EXPECT().Parse(gomock.Any()).Return(&parsedQuery{
		closeTime:       int64(101),
		searchPrecision: &dayPrecision,
		workflowType:    common.StringPtr("MobileOnlyWorkflow::processMobileOnly"),
		workflowID:      common.StringPtr(testWorkflowID),
		runID:           common.StringPtr(testRunID),
	}, nil)
	visibilityArchiver.queryParser = mockParser
	request := &archiver.QueryVisibilityRequest{
		DomainID: testDomainID,
		PageSize: 10,
		Query:    "parsed by mockParser",
	}

	response, err := visibilityArchiver.Query(ctx, URI, request)
	s.NoError(err)
	s.NotNil(response)
	s.Nil(response.NextPageToken)
	s.Len(response.Executions, 1)
	s.Equal(convertToExecutionInfo(s.expectedVisibilityRecords[0]), response.Executions[0])
}

func (s *visibilityArchiverSuite) TestQuery_Success_SmallPageSize() {

	pageSize := 2
	ctx := context.Background()
	URI, err := archiver.NewURI("gs://my-bucket-cad/cadence_archival/visibility")
	s.NoError(err)
	storageWrapper := &mocks.Client{}
	storageWrapper.On("Exist", mock.Anything, URI, mock.Anything).Return(false, nil)
	storageWrapper.On("QueryWithFilters", mock.Anything, URI, mock.Anything, pageSize, 0, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]string{"closeTimeout_2020-02-05T09:56:14Z_test-workflow-id_MobileOnlyWorkflow::processMobileOnly_test-run-id.visibility", "closeTimeout_2020-02-05T09:56:15Z_test-workflow-id_MobileOnlyWorkflow::processMobileOnly_test-run-id.visibility"}, false, 1, nil).Times(2)
	storageWrapper.On("QueryWithFilters", mock.Anything, URI, mock.Anything, pageSize, 1, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]string{"closeTimeout_2020-02-05T09:56:16Z_test-workflow-id_MobileOnlyWorkflow::processMobileOnly_test-run-id.visibility"}, true, 2, nil).Times(2)
	storageWrapper.On("Get", mock.Anything, URI, "test-domain-id/closeTimeout_2020-02-05T09:56:14Z_test-workflow-id_MobileOnlyWorkflow::processMobileOnly_test-run-id.visibility").Return([]byte(exampleVisibilityRecord), nil)
	storageWrapper.On("Get", mock.Anything, URI, "test-domain-id/closeTimeout_2020-02-05T09:56:15Z_test-workflow-id_MobileOnlyWorkflow::processMobileOnly_test-run-id.visibility").Return([]byte(exampleVisibilityRecord), nil)
	storageWrapper.On("Get", mock.Anything, URI, "test-domain-id/closeTimeout_2020-02-05T09:56:16Z_test-workflow-id_MobileOnlyWorkflow::processMobileOnly_test-run-id.visibility").Return([]byte(exampleVisibilityRecord), nil)

	visibilityArchiver := newVisibilityArchiver(s.container, storageWrapper)
	s.NoError(err)
	mockCtrl := gomock.NewController(s.T())
	defer mockCtrl.Finish()

	mockParser := NewMockQueryParser(mockCtrl)
	dayPrecision := string("Day")
	mockParser.EXPECT().Parse(gomock.Any()).Return(&parsedQuery{
		closeTime:       int64(101),
		searchPrecision: &dayPrecision,
		workflowType:    common.StringPtr("MobileOnlyWorkflow::processMobileOnly"),
		workflowID:      common.StringPtr(testWorkflowID),
		runID:           common.StringPtr(testRunID),
	}, nil).AnyTimes()
	visibilityArchiver.queryParser = mockParser
	request := &archiver.QueryVisibilityRequest{
		DomainID: testDomainID,
		PageSize: pageSize,
		Query:    "parsed by mockParser",
	}

	response, err := visibilityArchiver.Query(ctx, URI, request)
	s.NoError(err)
	s.NotNil(response)
	s.NotNil(response.NextPageToken)
	s.Len(response.Executions, 2)
	s.Equal(convertToExecutionInfo(s.expectedVisibilityRecords[0]), response.Executions[0])
	s.Equal(convertToExecutionInfo(s.expectedVisibilityRecords[0]), response.Executions[1])

	request.NextPageToken = response.NextPageToken
	response, err = visibilityArchiver.Query(ctx, URI, request)
	s.NoError(err)
	s.NotNil(response)
	s.Nil(response.NextPageToken)
	s.Len(response.Executions, 1)
	s.Equal(convertToExecutionInfo(s.expectedVisibilityRecords[0]), response.Executions[0])
}
