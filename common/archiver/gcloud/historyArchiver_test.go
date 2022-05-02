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
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"

	archiverspb "go.temporal.io/server/api/archiver/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/gcloud/connector"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives/timestamp"
)

const (
	testNamespaceID               = "test-namespace-id"
	testNamespace                 = "test-namespace"
	testWorkflowID                = "test-workflow-id"
	testRunID                     = "test-run-id"
	testNextEventID               = 1800
	testCloseFailoverVersion      = 100
	testPageSize                  = 100
	exampleHistoryRecord          = `[{"events":[{"eventId":1,"eventTime": "2020-07-30T00:30:03.082421843Z","eventType":"WorkflowExecutionStarted","version":-24,"taskId":5242897,"workflowExecutionStartedEventAttributes":{"workflowType":{"name":"MobileOnlyWorkflow::processMobileOnly"},"taskQueue":{"name":"MobileOnly"},"input":null,"workflowExecutionTimeout":"300s","workflowTaskTimeout":"60s","originalExecutionRunId":"1fd5d4c8-1590-4a0a-8027-535e8729de8e","identity":"","firstExecutionRunId":"1fd5d4c8-1590-4a0a-8027-535e8729de8e","attempt":1,"firstWorkflowTaskBackoff":"0s"}}]}]`
	twoEventsExampleHistoryRecord = `[{"events":[{"eventId":1,"eventTime": "2020-07-30T00:30:03.082421843Z","eventType":"WorkflowExecutionStarted","version":-24,"taskId":5242897,"workflowExecutionStartedEventAttributes":{"workflowType":{"name":"MobileOnlyWorkflow::processMobileOnly"},"taskQueue":{"name":"MobileOnly"},"input":null,"workflowExecutionTimeout":"300s","workflowTaskTimeout":"60s","originalExecutionRunId":"1fd5d4c8-1590-4a0a-8027-535e8729de8e","identity":"","firstExecutionRunId":"1fd5d4c8-1590-4a0a-8027-535e8729de8e","attempt":1,"firstWorkflowTaskBackoff":"0s"}},{"eventId":2,"eventTime": "2020-07-30T00:30:03.082421843Z","eventType":"WorkflowExecutionStarted","version":-24,"taskId":5242897,"workflowExecutionStartedEventAttributes":{"workflowType":{"name":"MobileOnlyWorkflow::processMobileOnly"},"taskQueue":{"name":"MobileOnly"},"input":null,"workflowExecutionTimeout":"300s","workflowTaskTimeout":"60s","originalExecutionRunId":"1fd5d4c8-1590-4a0a-8027-535e8729de8e","identity":"","firstExecutionRunId":"1fd5d4c8-1590-4a0a-8027-535e8729de8e","attempt":1,"firstWorkflowTaskBackoff":"0s"}}]}]`
)

var (
	testBranchToken = []byte{1, 2, 3}
)

func (h *historyArchiverSuite) SetupTest() {
	h.Assertions = require.New(h.T())
	h.controller = gomock.NewController(h.T())
	h.container = &archiver.HistoryBootstrapContainer{
		Logger:        log.NewNoopLogger(),
		MetricsClient: metrics.NoopClient,
	}
	h.testArchivalURI, _ = archiver.NewURI("gs://my-bucket-cad/temporal_archival/development")
}

func (h *historyArchiverSuite) TearDownTest() {
	h.controller.Finish()
}

func TestHistoryArchiverSuite(t *testing.T) {
	suite.Run(t, new(historyArchiverSuite))
}

type historyArchiverSuite struct {
	*require.Assertions
	suite.Suite

	controller *gomock.Controller

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

	storageWrapper := connector.NewMockClient(h.controller)
	storageWrapper.EXPECT().Exist(ctx, gomock.Any(), "").Return(false, nil)
	historyArchiver := new(historyArchiver)
	historyArchiver.gcloudStorage = storageWrapper
	for _, tc := range testCases {
		URI, err := archiver.NewURI(tc.URI)
		h.NoError(err)
		h.Equal(tc.expectedErr, historyArchiver.ValidateURI(URI))
	}
}

func (h *historyArchiverSuite) TestArchive_Fail_InvalidURI() {
	mockStorageClient := connector.NewMockGcloudStorageClient(h.controller)
	storageWrapper, _ := connector.NewClientWithParams(mockStorageClient)

	historyIterator := archiver.NewMockHistoryIterator(h.controller)

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
	storageWrapper := connector.NewMockClient(h.controller)
	storageWrapper.EXPECT().Exist(ctx, h.testArchivalURI, "").Return(true, nil)

	historyIterator := archiver.NewMockHistoryIterator(h.controller)

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

	err := historyArchiver.Archive(ctx, h.testArchivalURI, request)
	h.Error(err)
}

func (h *historyArchiverSuite) TestArchive_Fail_ErrorOnReadHistory() {
	ctx := context.Background()
	storageWrapper := connector.NewMockClient(h.controller)
	storageWrapper.EXPECT().Exist(ctx, h.testArchivalURI, "").Return(true, nil)

	historyIterator := archiver.NewMockHistoryIterator(h.controller)
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
	err := historyArchiver.Archive(ctx, h.testArchivalURI, request)
	h.Error(err)
}

func (h *historyArchiverSuite) TestArchive_Fail_TimeoutWhenReadingHistory() {

	ctx := getCanceledContext()
	storageWrapper := connector.NewMockClient(h.controller)
	storageWrapper.EXPECT().Exist(gomock.Any(), gomock.Any(), "").Return(true, nil)

	historyIterator := archiver.NewMockHistoryIterator(h.controller)
	gomock.InOrder(
		historyIterator.EXPECT().HasNext().Return(true),
		historyIterator.EXPECT().Next().Return(nil, serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT, "")),
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
	storageWrapper := connector.NewMockClient(h.controller)
	storageWrapper.EXPECT().Exist(ctx, h.testArchivalURI, "").Return(true, nil)

	historyIterator := archiver.NewMockHistoryIterator(h.controller)
	historyBatches := []*historypb.History{
		{
			Events: []*historypb.HistoryEvent{
				{
					EventId:   common.FirstEventID + 1,
					EventTime: timestamp.TimePtr(time.Now().UTC()),
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
	err := historyArchiver.Archive(ctx, h.testArchivalURI, request)
	h.Error(err)
}

func (h *historyArchiverSuite) TestArchive_Fail_NonRetryableErrorOption() {

	ctx := context.Background()
	storageWrapper := connector.NewMockClient(h.controller)
	storageWrapper.EXPECT().Exist(ctx, h.testArchivalURI, "").Return(true, nil)

	historyIterator := archiver.NewMockHistoryIterator(h.controller)
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
	err := historyArchiver.Archive(ctx, h.testArchivalURI, request, archiver.GetNonRetryableErrorOption(errUploadNonRetryable))
	h.Equal(errUploadNonRetryable, err)
}

func (h *historyArchiverSuite) TestArchive_Skip() {
	ctx := context.Background()

	storageWrapper := connector.NewMockClient(h.controller)
	storageWrapper.EXPECT().Exist(ctx, h.testArchivalURI, "").Return(true, nil)
	storageWrapper.EXPECT().Exist(ctx, h.testArchivalURI, gomock.Any()).Return(false, nil)
	storageWrapper.EXPECT().Upload(ctx, h.testArchivalURI, gomock.Any(), gomock.Any()).Return(nil)

	historyIterator := archiver.NewMockHistoryIterator(h.controller)
	historyBlob := &archiverspb.HistoryBlob{
		Header: &archiverspb.HistoryBlobHeader{
			IsLast: false,
		},
		Body: []*historypb.History{
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId:   common.FirstEventID,
						EventTime: timestamp.TimePtr(time.Now().UTC()),
						Version:   testCloseFailoverVersion,
					},
				},
			},
		},
	}
	gomock.InOrder(
		historyIterator.EXPECT().HasNext().Return(true),
		historyIterator.EXPECT().Next().Return(historyBlob, nil),
		historyIterator.EXPECT().HasNext().Return(true),
		historyIterator.EXPECT().Next().Return(nil, serviceerror.NewNotFound("workflow not found")),
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
	h.NoError(err)
}

func (h *historyArchiverSuite) TestArchive_Success() {

	ctx := context.Background()

	storageWrapper := connector.NewMockClient(h.controller)
	storageWrapper.EXPECT().Exist(ctx, h.testArchivalURI, gomock.Any()).Return(false, nil).Times(2)
	storageWrapper.EXPECT().Upload(ctx, h.testArchivalURI, gomock.Any(), gomock.Any()).Return(nil)

	historyIterator := archiver.NewMockHistoryIterator(h.controller)
	historyBatches := []*historypb.History{
		{
			Events: []*historypb.HistoryEvent{
				{
					EventId:   common.FirstEventID + 1,
					EventTime: timestamp.TimePtr(time.Now().UTC()),
					Version:   testCloseFailoverVersion,
				},
				{
					EventId:   common.FirstEventID + 2,
					EventTime: timestamp.TimePtr(time.Now().UTC()),
					Version:   testCloseFailoverVersion,
				},
			},
		},
		{
			Events: []*historypb.HistoryEvent{
				{
					EventId:   testNextEventID - 1,
					EventTime: timestamp.TimePtr(time.Now().UTC()),
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

	err := historyArchiver.Archive(ctx, h.testArchivalURI, request)
	h.NoError(err)
}

func (h *historyArchiverSuite) TestGet_Fail_InvalidURI() {
	ctx := context.Background()
	mockStorageClient := connector.NewMockGcloudStorageClient(h.controller)
	storageWrapper, _ := connector.NewClientWithParams(mockStorageClient)
	historyIterator := archiver.NewMockHistoryIterator(h.controller)
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
	mockStorageClient := connector.NewMockGcloudStorageClient(h.controller)
	storageWrapper, _ := connector.NewClientWithParams(mockStorageClient)
	historyIterator := archiver.NewMockHistoryIterator(h.controller)
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
	storageWrapper := connector.NewMockClient(h.controller)
	storageWrapper.EXPECT().Exist(ctx, h.testArchivalURI, "").Return(true, nil)
	storageWrapper.EXPECT().Query(ctx, h.testArchivalURI, gomock.Any()).Return([]string{"905702227796330300141628222723188294514017512010591354159_-24_0.history", "905702227796330300141628222723188294514017512010591354159_-25_0.history"}, nil)
	storageWrapper.EXPECT().Get(ctx, h.testArchivalURI, "141323698701063509081739672280485489488911532452831150339470_-24_0.history").Return([]byte(exampleHistoryRecord), nil)
	historyIterator := archiver.NewMockHistoryIterator(h.controller)
	historyArchiver := newHistoryArchiver(h.container, historyIterator, storageWrapper)
	request := &archiver.GetHistoryRequest{
		NamespaceID: testNamespaceID,
		WorkflowID:  testWorkflowID,
		RunID:       testRunID,
		PageSize:    testPageSize,
	}

	response, err := historyArchiver.Get(ctx, h.testArchivalURI, request)
	h.NoError(err)
	h.Nil(response.NextPageToken)
}

func (h *historyArchiverSuite) TestGet_Success_UseProvidedVersion() {

	ctx := context.Background()
	storageWrapper := connector.NewMockClient(h.controller)
	storageWrapper.EXPECT().Exist(ctx, h.testArchivalURI, "").Return(true, nil)
	storageWrapper.EXPECT().Query(ctx, h.testArchivalURI, "141323698701063509081739672280485489488911532452831150339470").Return([]string{"905702227796330300141628222723188294514017512010591354159_-24_0.history", "905702227796330300141628222723188294514017512010591354159_-25_0.history"}, nil)
	storageWrapper.EXPECT().Get(ctx, h.testArchivalURI, "141323698701063509081739672280485489488911532452831150339470_-25_0.history").Return([]byte(exampleHistoryRecord), nil)
	historyIterator := archiver.NewMockHistoryIterator(h.controller)
	historyArchiver := newHistoryArchiver(h.container, historyIterator, storageWrapper)
	request := &archiver.GetHistoryRequest{
		NamespaceID:          testNamespaceID,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		PageSize:             testPageSize,
		CloseFailoverVersion: convert.Int64Ptr(-25),
	}

	response, err := historyArchiver.Get(ctx, h.testArchivalURI, request)
	h.NoError(err)
	h.Nil(response.NextPageToken)
}

func (h *historyArchiverSuite) TestGet_Success_PageSize() {

	ctx := context.Background()
	storageWrapper := connector.NewMockClient(h.controller)
	storageWrapper.EXPECT().Exist(ctx, h.testArchivalURI, "").Return(true, nil)
	storageWrapper.EXPECT().Query(ctx, h.testArchivalURI, "141323698701063509081739672280485489488911532452831150339470").Return([]string{"905702227796330300141628222723188294514017512010591354159_-24_0.history", "905702227796330300141628222723188294514017512010591354159_-24_1.history", "905702227796330300141628222723188294514017512010591354159_-24_2.history", "905702227796330300141628222723188294514017512010591354159_-24_3.history", "905702227796330300141628222723188294514017512010591354159_-25_0.history"}, nil)
	storageWrapper.EXPECT().Get(ctx, h.testArchivalURI, "141323698701063509081739672280485489488911532452831150339470_-24_0.history").Return([]byte(exampleHistoryRecord), nil)
	storageWrapper.EXPECT().Get(ctx, h.testArchivalURI, "141323698701063509081739672280485489488911532452831150339470_-24_1.history").Return([]byte(exampleHistoryRecord), nil)

	historyIterator := archiver.NewMockHistoryIterator(h.controller)
	historyArchiver := newHistoryArchiver(h.container, historyIterator, storageWrapper)
	request := &archiver.GetHistoryRequest{
		NamespaceID: testNamespaceID,
		WorkflowID:  testWorkflowID,
		RunID:       testRunID,
		PageSize:    2,
	}

	response, err := historyArchiver.Get(ctx, h.testArchivalURI, request)
	h.NoError(err)
	h.NotNil(response.NextPageToken)
	h.EqualValues(len(response.HistoryBatches), 2)
}

func (h *historyArchiverSuite) TestGet_Success_FromToken() {

	ctx := context.Background()
	storageWrapper := connector.NewMockClient(h.controller)
	storageWrapper.EXPECT().Exist(ctx, h.testArchivalURI, "").Return(true, nil)
	storageWrapper.EXPECT().Get(ctx, h.testArchivalURI, "141323698701063509081739672280485489488911532452831150339470_-24_2.history").Return([]byte(exampleHistoryRecord), nil)
	storageWrapper.EXPECT().Get(ctx, h.testArchivalURI, "141323698701063509081739672280485489488911532452831150339470_-24_3.history").Return([]byte(twoEventsExampleHistoryRecord), nil)
	storageWrapper.EXPECT().Get(ctx, h.testArchivalURI, "141323698701063509081739672280485489488911532452831150339470_-24_4.history").Return([]byte(exampleHistoryRecord), nil)

	historyIterator := archiver.NewMockHistoryIterator(h.controller)
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
	response, err := historyArchiver.Get(ctx, h.testArchivalURI, request)
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
