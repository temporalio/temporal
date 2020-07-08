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
	historypb "go.temporal.io/temporal-proto/history/v1"
	"go.temporal.io/temporal-proto/serviceerror"
	"go.uber.org/zap"

	archiverproto "github.com/temporalio/temporal/api/archiver/v1"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/archiver"
	"github.com/temporalio/temporal/common/archiver/gcloud/connector"
	"github.com/temporalio/temporal/common/archiver/gcloud/connector/mocks"
	"github.com/temporalio/temporal/common/convert"
	"github.com/temporalio/temporal/common/log/loggerimpl"
	"github.com/temporalio/temporal/common/metrics"
)

const (
	testNamespaceID               = "test-namespace-id"
	testNamespace                 = "test-namespace"
	testWorkflowID                = "test-workflow-id"
	testRunID                     = "test-run-id"
	testNextEventID               = 1800
	testCloseFailoverVersion      = 100
	testPageSize                  = 100
	exampleHistoryRecord          = `[{"events":[{"eventId":1,"timestamp":1576800090315103000,"eventType":"WorkflowExecutionStarted","version":-24,"taskId":5242897,"workflowExecutionStartedEventAttributes":{"workflowType":{"name":"MobileOnlyWorkflow::processMobileOnly"},"taskQueue":{"name":"MobileOnly"},"input":null,"workflowExecutionTimeoutSeconds":300,"workflowTaskTimeoutSeconds":60,"originalExecutionRunId":"1fd5d4c8-1590-4a0a-8027-535e8729de8e","identity":"","firstExecutionRunId":"1fd5d4c8-1590-4a0a-8027-535e8729de8e","attempt":0,"firstDecisionTaskBackoffSeconds":0}}]}]`
	twoEventsExampleHistoryRecord = `[{"events":[{"eventId":1,"timestamp":1576800090315103000,"eventType":"WorkflowExecutionStarted","version":-24,"taskId":5242897,"workflowExecutionStartedEventAttributes":{"workflowType":{"name":"MobileOnlyWorkflow::processMobileOnly"},"taskQueue":{"name":"MobileOnly"},"input":null,"workflowExecutionTimeoutSeconds":300,"workflowTaskTimeoutSeconds":60,"originalExecutionRunId":"1fd5d4c8-1590-4a0a-8027-535e8729de8e","identity":"","firstExecutionRunId":"1fd5d4c8-1590-4a0a-8027-535e8729de8e","attempt":0,"firstDecisionTaskBackoffSeconds":0}},
	{"eventId":2,"timestamp":1576800090315103000,"eventType":"WorkflowExecutionStarted","version":-24,"taskId":5242897,"workflowExecutionStartedEventAttributes":{"workflowType":{"name":"MobileOnlyWorkflow::processMobileOnly"},"taskQueue":{"name":"MobileOnly"},"input":null,"workflowExecutionTimeoutSeconds":300,"workflowTaskTimeoutSeconds":60,"originalExecutionRunId":"1fd5d4c8-1590-4a0a-8027-535e8729de8e","identity":"","firstExecutionRunId":"1fd5d4c8-1590-4a0a-8027-535e8729de8e","attempt":0,"firstDecisionTaskBackoffSeconds":0}}]}]`
)

var (
	testBranchToken = []byte{1, 2, 3}
)

func (h *historyArchiverSuite) SetupTest() {
	zapLogger := zap.NewNop()
	h.Assertions = require.New(h.T())
	h.container = &archiver.HistoryBootstrapContainer{
		Logger:        loggerimpl.NewLogger(zapLogger),
		MetricsClient: metrics.NewClient(tally.NoopScope, metrics.History),
	}
	h.testArchivalURI, _ = archiver.NewURI("gs://my-bucket-cad/temporal_archival/development")
}

func TestHistoryArchiverSuite(t *testing.T) {
	suite.Run(t, new(historyArchiverSuite))
}

type historyArchiverSuite struct {
	*require.Assertions
	suite.Suite
	container       *archiver.HistoryBootstrapContainer
	testArchivalURI archiver.URI
}

func getCanceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func (h *historyArchiverSuite) TestValidateURI() {
	ctx := context.Background()
	testCases := []struct {
		URI         string
		expectedErr error
	}{
		{
			URI:         "wrongscheme:///a/b/c",
			expectedErr: archiver.ErrURISchemeMismatch,
		},
		{
			URI:         "gs:my-bucket-cad/temporal_archival/development",
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
			URI:         "gs:/my-bucket-cad/temporal_archival/development",
			expectedErr: archiver.ErrInvalidURI,
		},
		{
			URI:         "gs://my-bucket-cad/temporal_archival/development",
			expectedErr: nil,
		},
	}

	storageWrapper := &mocks.Client{}
	storageWrapper.On("Exist", ctx, mock.Anything, "").Return(false, nil)
	historyArchiver := new(historyArchiver)
	historyArchiver.gcloudStorage = storageWrapper
	for _, tc := range testCases {
		URI, err := archiver.NewURI(tc.URI)
		h.NoError(err)
		h.Equal(tc.expectedErr, historyArchiver.ValidateURI(URI))
	}
}

func (h *historyArchiverSuite) TestArchive_Fail_InvalidURI() {
	mockStorageClient := &mocks.GcloudStorageClient{}
	storageWrapper, _ := connector.NewClientWithParams(mockStorageClient)

	mockCtrl := gomock.NewController(h.T())
	defer mockCtrl.Finish()
	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)

	historyArchiver := newHistoryArchiver(h.container, historyIterator, storageWrapper)
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
	h.NoError(err)
	err = historyArchiver.Archive(context.Background(), URI, request)
	h.Error(err)
}

func (h *historyArchiverSuite) TestArchive_Fail_InvalidRequest() {
	ctx := context.Background()
	URI, err := archiver.NewURI("gs://my-bucket-cad/temporal_archival/development")
	h.NoError(err)
	storageWrapper := &mocks.Client{}
	storageWrapper.On("Exist", ctx, URI, "").Return(true, nil).Times(1)
	mockCtrl := gomock.NewController(h.T())

	defer mockCtrl.Finish()
	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)

	historyArchiver := newHistoryArchiver(h.container, historyIterator, storageWrapper)
	request := &archiver.ArchiveHistoryRequest{
		NamespaceID:          testNamespaceID,
		Namespace:            testNamespace,
		WorkflowID:           "",
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}

	err = historyArchiver.Archive(ctx, h.testArchivalURI, request)
	h.Error(err)
}

func (h *historyArchiverSuite) TestArchive_Fail_ErrorOnReadHistory() {
	ctx := context.Background()
	URI, err := archiver.NewURI("gs://my-bucket-cad/temporal_archival/development")
	h.NoError(err)
	storageWrapper := &mocks.Client{}
	storageWrapper.On("Exist", ctx, URI, "").Return(true, nil).Times(1)

	mockCtrl := gomock.NewController(h.T())
	defer mockCtrl.Finish()
	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)
	gomock.InOrder(
		historyIterator.EXPECT().HasNext().Return(true),
		historyIterator.EXPECT().Next().Return(nil, errors.New("some random error")),
	)

	historyArchiver := newHistoryArchiver(h.container, historyIterator, storageWrapper)
	request := &archiver.ArchiveHistoryRequest{
		NamespaceID:          testNamespaceID,
		Namespace:            testNamespace,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	err = historyArchiver.Archive(ctx, h.testArchivalURI, request)
	h.Error(err)
}

func (h *historyArchiverSuite) TestArchive_Fail_TimeoutWhenReadingHistory() {

	ctx := getCanceledContext()
	mockCtrl := gomock.NewController(h.T())
	storageWrapper := &mocks.Client{}
	storageWrapper.On("Exist", mock.Anything, mock.Anything, "").Return(true, nil).Times(1)

	defer mockCtrl.Finish()
	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)
	gomock.InOrder(
		historyIterator.EXPECT().HasNext().Return(true),
		historyIterator.EXPECT().Next().Return(nil, serviceerror.NewResourceExhausted("")),
	)

	historyArchiver := newHistoryArchiver(h.container, historyIterator, storageWrapper)
	request := &archiver.ArchiveHistoryRequest{
		NamespaceID:          testNamespaceID,
		Namespace:            testNamespace,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	err := historyArchiver.Archive(ctx, h.testArchivalURI, request)
	h.Error(err)
}

func (h *historyArchiverSuite) TestArchive_Fail_HistoryMutated() {
	ctx := context.Background()
	mockCtrl := gomock.NewController(h.T())
	URI, err := archiver.NewURI("gs://my-bucket-cad/temporal_archival/development")
	h.NoError(err)
	storageWrapper := &mocks.Client{}
	storageWrapper.On("Exist", ctx, URI, "").Return(true, nil).Times(1)

	defer mockCtrl.Finish()
	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)
	historyBatches := []*historypb.History{
		{
			Events: []*historypb.HistoryEvent{
				{
					EventId:   common.FirstEventID + 1,
					Timestamp: time.Now().UnixNano(),
					Version:   testCloseFailoverVersion + 1,
				},
			},
		},
	}
	historyBlob := &archiverproto.HistoryBlob{
		Header: &archiverproto.HistoryBlobHeader{
			IsLast: true,
		},
		Body: historyBatches,
	}
	gomock.InOrder(
		historyIterator.EXPECT().HasNext().Return(true),
		historyIterator.EXPECT().Next().Return(historyBlob, nil),
	)

	historyArchiver := newHistoryArchiver(h.container, historyIterator, storageWrapper)
	request := &archiver.ArchiveHistoryRequest{
		NamespaceID:          testNamespaceID,
		Namespace:            testNamespace,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	err = historyArchiver.Archive(ctx, h.testArchivalURI, request)
	h.Error(err)
}

func (h *historyArchiverSuite) TestArchive_Fail_NonRetryableErrorOption() {

	ctx := context.Background()
	mockCtrl := gomock.NewController(h.T())
	URI, err := archiver.NewURI("gs://my-bucket-cad/temporal_archival/development")
	h.NoError(err)
	storageWrapper := &mocks.Client{}
	storageWrapper.On("Exist", ctx, URI, "").Return(true, nil).Times(1)

	defer mockCtrl.Finish()
	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)
	gomock.InOrder(
		historyIterator.EXPECT().HasNext().Return(true),
		historyIterator.EXPECT().Next().Return(nil, errors.New("upload non-retryable error")),
	)

	historyArchiver := newHistoryArchiver(h.container, historyIterator, storageWrapper)
	request := &archiver.ArchiveHistoryRequest{
		NamespaceID:          testNamespaceID,
		Namespace:            testNamespace,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	err = historyArchiver.Archive(ctx, h.testArchivalURI, request, archiver.GetNonRetryableErrorOption(errUploadNonRetryable))
	h.Equal(errUploadNonRetryable, err)
}

func (h *historyArchiverSuite) TestArchive_Success() {

	ctx := context.Background()
	mockCtrl := gomock.NewController(h.T())
	URI, err := archiver.NewURI("gs://my-bucket-cad/temporal_archival/development")
	storageWrapper := &mocks.Client{}
	storageWrapper.On("Exist", ctx, URI, mock.Anything).Return(false, nil)
	storageWrapper.On("Upload", ctx, URI, mock.Anything, mock.Anything).Return(nil)

	defer mockCtrl.Finish()
	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)
	historyBatches := []*historypb.History{
		{
			Events: []*historypb.HistoryEvent{
				{
					EventId:   common.FirstEventID + 1,
					Timestamp: time.Now().UnixNano(),
					Version:   testCloseFailoverVersion,
				},
				{
					EventId:   common.FirstEventID + 2,
					Timestamp: time.Now().UnixNano(),
					Version:   testCloseFailoverVersion,
				},
			},
		},
		{
			Events: []*historypb.HistoryEvent{
				{
					EventId:   testNextEventID - 1,
					Timestamp: time.Now().UnixNano(),
					Version:   testCloseFailoverVersion,
				},
			},
		},
	}
	historyBlob := &archiverproto.HistoryBlob{
		Header: &archiverproto.HistoryBlobHeader{
			IsLast: true,
		},
		Body: historyBatches,
	}
	gomock.InOrder(
		historyIterator.EXPECT().HasNext().Return(true),
		historyIterator.EXPECT().Next().Return(historyBlob, nil),
		historyIterator.EXPECT().HasNext().Return(false),
	)

	historyArchiver := newHistoryArchiver(h.container, historyIterator, storageWrapper)

	request := &archiver.ArchiveHistoryRequest{
		NamespaceID:          testNamespaceID,
		Namespace:            testNamespace,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}

	h.NoError(err)
	err = historyArchiver.Archive(ctx, URI, request)

	h.NoError(err)

}

func (h *historyArchiverSuite) TestGet_Fail_InvalidURI() {
	ctx := context.Background()
	mockCtrl := gomock.NewController(h.T())
	mockStorageClient := &mocks.GcloudStorageClient{}
	storageWrapper, _ := connector.NewClientWithParams(mockStorageClient)
	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)
	historyArchiver := newHistoryArchiver(h.container, historyIterator, storageWrapper)

	request := &archiver.GetHistoryRequest{
		NamespaceID: testNamespaceID,
		WorkflowID:  testWorkflowID,
		RunID:       testRunID,
		PageSize:    100,
	}
	URI, err := archiver.NewURI("wrongscheme://")
	h.NoError(err)
	response, err := historyArchiver.Get(ctx, URI, request)
	h.Nil(response)
	h.Error(err)
}

func (h *historyArchiverSuite) TestGet_Fail_InvalidToken() {
	ctx := context.Background()
	mockCtrl := gomock.NewController(h.T())
	mockStorageClient := &mocks.GcloudStorageClient{}
	storageWrapper, _ := connector.NewClientWithParams(mockStorageClient)
	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)
	historyArchiver := newHistoryArchiver(h.container, historyIterator, storageWrapper)
	request := &archiver.GetHistoryRequest{
		NamespaceID:   testNamespaceID,
		WorkflowID:    testWorkflowID,
		RunID:         testRunID,
		PageSize:      testPageSize,
		NextPageToken: []byte{'r', 'a', 'n', 'd', 'o', 'm'},
	}
	URI, err := archiver.NewURI("gs:///")
	h.NoError(err)
	response, err := historyArchiver.Get(ctx, URI, request)
	h.Nil(response)
	h.Error(err)
	h.IsType(&serviceerror.InvalidArgument{}, err)
}

func (h *historyArchiverSuite) TestGet_Success_PickHighestVersion() {
	ctx := context.Background()
	mockCtrl := gomock.NewController(h.T())
	URI, err := archiver.NewURI("gs://my-bucket-cad/temporal_archival/development")
	storageWrapper := &mocks.Client{}
	storageWrapper.On("Exist", ctx, URI, "").Return(true, nil).Times(1)
	storageWrapper.On("Query", ctx, URI, mock.Anything).Return([]string{"905702227796330300141628222723188294514017512010591354159_-24_0.history", "905702227796330300141628222723188294514017512010591354159_-25_0.history"}, nil).Times(1)
	storageWrapper.On("Get", ctx, URI, "141323698701063509081739672280485489488911532452831150339470_-24_0.history").Return([]byte(exampleHistoryRecord), nil)
	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)
	historyArchiver := newHistoryArchiver(h.container, historyIterator, storageWrapper)
	request := &archiver.GetHistoryRequest{
		NamespaceID: testNamespaceID,
		WorkflowID:  testWorkflowID,
		RunID:       testRunID,
		PageSize:    testPageSize,
	}

	h.NoError(err)
	response, err := historyArchiver.Get(ctx, URI, request)
	h.NoError(err)
	h.Nil(response.NextPageToken)
}

func (h *historyArchiverSuite) TestGet_Success_UseProvidedVersion() {

	ctx := context.Background()
	mockCtrl := gomock.NewController(h.T())
	URI, err := archiver.NewURI("gs://my-bucket-cad/temporal_archival/development")
	storageWrapper := &mocks.Client{}
	storageWrapper.On("Exist", ctx, URI, "").Return(true, nil).Times(1)
	storageWrapper.On("Query", ctx, URI, "141323698701063509081739672280485489488911532452831150339470").Return([]string{"905702227796330300141628222723188294514017512010591354159_-24_0.history", "905702227796330300141628222723188294514017512010591354159_-25_0.history"}, nil).Times(1)
	storageWrapper.On("Get", ctx, URI, "141323698701063509081739672280485489488911532452831150339470_-25_0.history").Return([]byte(exampleHistoryRecord), nil)
	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)
	historyArchiver := newHistoryArchiver(h.container, historyIterator, storageWrapper)
	request := &archiver.GetHistoryRequest{
		NamespaceID:          testNamespaceID,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		PageSize:             testPageSize,
		CloseFailoverVersion: convert.Int64Ptr(-25),
	}

	h.NoError(err)
	response, err := historyArchiver.Get(ctx, URI, request)
	h.NoError(err)
	h.Nil(response.NextPageToken)
}

func (h *historyArchiverSuite) TestGet_Success_PageSize() {

	ctx := context.Background()
	mockCtrl := gomock.NewController(h.T())
	URI, err := archiver.NewURI("gs://my-bucket-cad/temporal_archival/development")
	storageWrapper := &mocks.Client{}
	storageWrapper.On("Exist", ctx, URI, "").Return(true, nil).Times(1)
	storageWrapper.On("Query", ctx, URI, "141323698701063509081739672280485489488911532452831150339470").Return([]string{"905702227796330300141628222723188294514017512010591354159_-24_0.history", "905702227796330300141628222723188294514017512010591354159_-24_1.history", "905702227796330300141628222723188294514017512010591354159_-24_2.history", "905702227796330300141628222723188294514017512010591354159_-24_3.history", "905702227796330300141628222723188294514017512010591354159_-25_0.history"}, nil).Times(1)
	storageWrapper.On("Get", ctx, URI, "141323698701063509081739672280485489488911532452831150339470_-24_0.history").Return([]byte(exampleHistoryRecord), nil)
	storageWrapper.On("Get", ctx, URI, "141323698701063509081739672280485489488911532452831150339470_-24_1.history").Return([]byte(exampleHistoryRecord), nil)
	storageWrapper.On("Get", ctx, URI, "141323698701063509081739672280485489488911532452831150339470_-24_2.history").Return([]byte(exampleHistoryRecord), nil)
	storageWrapper.On("Get", ctx, URI, "141323698701063509081739672280485489488911532452831150339470_-24_3.history").Return([]byte(exampleHistoryRecord), nil)

	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)
	historyArchiver := newHistoryArchiver(h.container, historyIterator, storageWrapper)
	request := &archiver.GetHistoryRequest{
		NamespaceID: testNamespaceID,
		WorkflowID:  testWorkflowID,
		RunID:       testRunID,
		PageSize:    2,
	}

	h.NoError(err)
	response, err := historyArchiver.Get(ctx, URI, request)
	h.NoError(err)
	h.NotNil(response.NextPageToken)
	h.EqualValues(len(response.HistoryBatches), 2)
}

func (h *historyArchiverSuite) TestGet_Success_FromToken() {

	ctx := context.Background()
	mockCtrl := gomock.NewController(h.T())
	URI, err := archiver.NewURI("gs://my-bucket-cad/temporal_archival/development")
	storageWrapper := &mocks.Client{}
	storageWrapper.On("Exist", ctx, URI, "").Return(true, nil).Times(1)
	storageWrapper.On("Query", ctx, URI, "141323698701063509081739672280485489488911532452831150339470").Return([]string{"905702227796330300141628222723188294514017512010591354159_-24_0.history", "905702227796330300141628222723188294514017512010591354159_-24_1.history", "905702227796330300141628222723188294514017512010591354159_-24_2.history", "905702227796330300141628222723188294514017512010591354159_-24_3.history", "905702227796330300141628222723188294514017512010591354159_-25_0.history"}, nil).Times(1)
	storageWrapper.On("Get", ctx, URI, "141323698701063509081739672280485489488911532452831150339470_-24_0.history").Return([]byte(exampleHistoryRecord), nil)
	storageWrapper.On("Get", ctx, URI, "141323698701063509081739672280485489488911532452831150339470_-24_1.history").Return([]byte(exampleHistoryRecord), nil)
	storageWrapper.On("Get", ctx, URI, "141323698701063509081739672280485489488911532452831150339470_-24_2.history").Return([]byte(exampleHistoryRecord), nil)
	storageWrapper.On("Get", ctx, URI, "141323698701063509081739672280485489488911532452831150339470_-24_3.history").Return([]byte(twoEventsExampleHistoryRecord), nil)
	storageWrapper.On("Get", ctx, URI, "141323698701063509081739672280485489488911532452831150339470_-24_4.history").Return([]byte(exampleHistoryRecord), nil)
	storageWrapper.On("Get", ctx, URI, "141323698701063509081739672280485489488911532452831150339470_-24_5.history").Return([]byte(exampleHistoryRecord), nil)

	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)
	historyArchiver := newHistoryArchiver(h.container, historyIterator, storageWrapper)

	token := &getHistoryToken{
		CloseFailoverVersion: -24,
		HighestPart:          5,
		CurrentPart:          2,
		BatchIdxOffset:       0,
	}

	nextPageToken, err := serializeToken(token)
	h.NoError(err)

	request := &archiver.GetHistoryRequest{
		NamespaceID:   testNamespaceID,
		WorkflowID:    testWorkflowID,
		RunID:         testRunID,
		PageSize:      4,
		NextPageToken: nextPageToken,
	}

	h.NoError(err)
	response, err := historyArchiver.Get(ctx, URI, request)
	h.NoError(err)
	h.NotNil(response.NextPageToken)

	token, err = deserializeGetHistoryToken(response.NextPageToken)
	h.NoError(err)

	h.EqualValues(5, token.HighestPart)
	h.EqualValues(5, token.CurrentPart)
	h.EqualValues(3, len(response.HistoryBatches))
	numOfEvents := 0
	for _, batch := range response.HistoryBatches {
		numOfEvents += len(batch.Events)
	}

	h.EqualValues(4, numOfEvents)
}
