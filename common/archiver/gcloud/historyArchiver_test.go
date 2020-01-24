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
	"github.com/uber/cadence/common/archiver/gcloud/connector"
	"github.com/uber/cadence/common/archiver/gcloud/connector/mocks"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
)

const (
	testDomainID                  = "test-domain-id"
	testDomainName                = "test-domain-name"
	testWorkflowID                = "test-workflow-id"
	testRunID                     = "test-run-id"
	testNextEventID               = 1800
	testCloseFailoverVersion      = 100
	testPageSize                  = 100
	exampleHistoryRecord          = `[{"events":[{"eventId":1,"timestamp":1576800090315103000,"eventType":"WorkflowExecutionStarted","version":-24,"taskId":5242897,"workflowExecutionStartedEventAttributes":{"workflowType":{"name":"MobileOnlyWorkflow::processMobileOnly"},"taskList":{"name":"MobileOnly"},"input":"eyJkbmkiOiI0ODY5NGJmZi04MTU2LTRjZDEtYTYzZi0wZTM0ZDBlYzljMWEiLCJjYXRlZ29yeSI6InVwZGF0ZV9jbGllbnQiLCJhZ2UiOjE4LCJzY29yZSI6MCwic3RhdHVzIjoicGVuZGluZyIsImRlc2NyaXB0aW9uIjoiTG9yZW0gSXBzdW0gaXMgc2ltcGx5IGR1bW15IHRleHQgb2YgdGhlIHByaW50aW5nIGFuZCB0eXBlc2V0dGluZyBpbmR1c3RyeS4gTG9yZW0gSXBzdW0gaGFzIGJlZW4gdGhlIGluZHVzdHJ5XHUwMDI3cyBzdGFuZGFyZCBkdW1teSB0ZXh0IGV2ZXIgc2luY2UgdGhlIDE1MDBzLCB3aGVuIGFuIHVua25vd24gcHJpbnRlciB0b29rIGEgZ2FsbGV5IG9mIHR5cGUgYW5kIHNjcmFtYmxlZCBpdCB0byBtYWtlIGEgdHlwZSBzcGVjaW1lbiBib29rLkl0IGhhcyBzdXJ2aXZlZCBub3Qgb25seSBmaXZlIGNlbnR1cmllcywgYnV0IGFsc28gdGhlIGxlYXAgaW50byBlbGVjdHJvbmljIHR5cGVzZXR0aW5nLCByZW1haW5pbmcgZXNzZW50aWFsbHkgdW5jaGFuZ2VkLkl0IHdhcyBwb3B1bGFyaXNlZCBpbiB0aGUgMTk2MHMgd2l0aCB0aGUgcmVsZWFzZSBvZiBMZXRyYXNldCBzaGVldHMgY29udGFpbmluZyBMb3JlbSBJcHN1bSBwYXNzYWdlcywgYW5kIG1vcmUgcmVjZW50bHkgd2l0aCBkZXNrdG9wIHB1Ymxpc2hpbmcgc29mdHdhcmUgbGlrZSBBbGR1cyBQYWdlTWFrZXIgaW5jbHVkaW5nIHZlcnNpb25zIG9mIExvcmVtIElwc3VtLkl0IGlzIGEgbG9uZyBlc3RhYmxpc2hlZCBmYWN0IHRoYXQgYSByZWFkZXIgd2lsbCBiZSBkaXN0cmFjdGVkIGJ5IHRoZSByZWFkYWJsZSBjb250ZW50IG9mIGEgcGFnZSB3aGVuIGxvb2tpbmcgYXQgaXRzIGxheW91dC4gVGhlIHBvaW50IG9mIHVzaW5nIExvcmVtIElwc3VtIGlzIHRoYXQgaXQgaGFzIGEgbW9yZS1vci1sZXNzIG5vcm1hbCBkaXN0cmlidXRpb24gb2YgbGV0dGVycywgYXMgb3Bwb3NlZCB0byB1c2luZyBcdTAwMjdDb250ZW50IGhlcmUsIGNvbnRlbnQgaGVyZVx1MDAyNywgbWFraW5nIGl0IGxvb2sgbGlrZSByZWFkYWJsZSBFbmdsaXNoLiJ9","executionStartToCloseTimeoutSeconds":300,"taskStartToCloseTimeoutSeconds":60,"originalExecutionRunId":"1fd5d4c8-1590-4a0a-8027-535e8729de8e","identity":"","firstExecutionRunId":"1fd5d4c8-1590-4a0a-8027-535e8729de8e","attempt":0,"firstDecisionTaskBackoffSeconds":0}}]}]`
	twoEventsExampleHistoryRecord = `[{"events":[{"eventId":1,"timestamp":1576800090315103000,"eventType":"WorkflowExecutionStarted","version":-24,"taskId":5242897,"workflowExecutionStartedEventAttributes":{"workflowType":{"name":"MobileOnlyWorkflow::processMobileOnly"},"taskList":{"name":"MobileOnly"},"input":"eyJkbmkiOiI0ODY5NGJmZi04MTU2LTRjZDEtYTYzZi0wZTM0ZDBlYzljMWEiLCJjYXRlZ29yeSI6InVwZGF0ZV9jbGllbnQiLCJhZ2UiOjE4LCJzY29yZSI6MCwic3RhdHVzIjoicGVuZGluZyIsImRlc2NyaXB0aW9uIjoiTG9yZW0gSXBzdW0gaXMgc2ltcGx5IGR1bW15IHRleHQgb2YgdGhlIHByaW50aW5nIGFuZCB0eXBlc2V0dGluZyBpbmR1c3RyeS4gTG9yZW0gSXBzdW0gaGFzIGJlZW4gdGhlIGluZHVzdHJ5XHUwMDI3cyBzdGFuZGFyZCBkdW1teSB0ZXh0IGV2ZXIgc2luY2UgdGhlIDE1MDBzLCB3aGVuIGFuIHVua25vd24gcHJpbnRlciB0b29rIGEgZ2FsbGV5IG9mIHR5cGUgYW5kIHNjcmFtYmxlZCBpdCB0byBtYWtlIGEgdHlwZSBzcGVjaW1lbiBib29rLkl0IGhhcyBzdXJ2aXZlZCBub3Qgb25seSBmaXZlIGNlbnR1cmllcywgYnV0IGFsc28gdGhlIGxlYXAgaW50byBlbGVjdHJvbmljIHR5cGVzZXR0aW5nLCByZW1haW5pbmcgZXNzZW50aWFsbHkgdW5jaGFuZ2VkLkl0IHdhcyBwb3B1bGFyaXNlZCBpbiB0aGUgMTk2MHMgd2l0aCB0aGUgcmVsZWFzZSBvZiBMZXRyYXNldCBzaGVldHMgY29udGFpbmluZyBMb3JlbSBJcHN1bSBwYXNzYWdlcywgYW5kIG1vcmUgcmVjZW50bHkgd2l0aCBkZXNrdG9wIHB1Ymxpc2hpbmcgc29mdHdhcmUgbGlrZSBBbGR1cyBQYWdlTWFrZXIgaW5jbHVkaW5nIHZlcnNpb25zIG9mIExvcmVtIElwc3VtLkl0IGlzIGEgbG9uZyBlc3RhYmxpc2hlZCBmYWN0IHRoYXQgYSByZWFkZXIgd2lsbCBiZSBkaXN0cmFjdGVkIGJ5IHRoZSByZWFkYWJsZSBjb250ZW50IG9mIGEgcGFnZSB3aGVuIGxvb2tpbmcgYXQgaXRzIGxheW91dC4gVGhlIHBvaW50IG9mIHVzaW5nIExvcmVtIElwc3VtIGlzIHRoYXQgaXQgaGFzIGEgbW9yZS1vci1sZXNzIG5vcm1hbCBkaXN0cmlidXRpb24gb2YgbGV0dGVycywgYXMgb3Bwb3NlZCB0byB1c2luZyBcdTAwMjdDb250ZW50IGhlcmUsIGNvbnRlbnQgaGVyZVx1MDAyNywgbWFraW5nIGl0IGxvb2sgbGlrZSByZWFkYWJsZSBFbmdsaXNoLiJ9","executionStartToCloseTimeoutSeconds":300,"taskStartToCloseTimeoutSeconds":60,"originalExecutionRunId":"1fd5d4c8-1590-4a0a-8027-535e8729de8e","identity":"","firstExecutionRunId":"1fd5d4c8-1590-4a0a-8027-535e8729de8e","attempt":0,"firstDecisionTaskBackoffSeconds":0}}, 
	{"eventId":2,"timestamp":1576800090315103000,"eventType":"WorkflowExecutionStarted","version":-24,"taskId":5242897,"workflowExecutionStartedEventAttributes":{"workflowType":{"name":"MobileOnlyWorkflow::processMobileOnly"},"taskList":{"name":"MobileOnly"},"input":"eyJkbmkiOiI0ODY5NGJmZi04MTU2LTRjZDEtYTYzZi0wZTM0ZDBlYzljMWEiLCJjYXRlZ29yeSI6InVwZGF0ZV9jbGllbnQiLCJhZ2UiOjE4LCJzY29yZSI6MCwic3RhdHVzIjoicGVuZGluZyIsImRlc2NyaXB0aW9uIjoiTG9yZW0gSXBzdW0gaXMgc2ltcGx5IGR1bW15IHRleHQgb2YgdGhlIHByaW50aW5nIGFuZCB0eXBlc2V0dGluZyBpbmR1c3RyeS4gTG9yZW0gSXBzdW0gaGFzIGJlZW4gdGhlIGluZHVzdHJ5XHUwMDI3cyBzdGFuZGFyZCBkdW1teSB0ZXh0IGV2ZXIgc2luY2UgdGhlIDE1MDBzLCB3aGVuIGFuIHVua25vd24gcHJpbnRlciB0b29rIGEgZ2FsbGV5IG9mIHR5cGUgYW5kIHNjcmFtYmxlZCBpdCB0byBtYWtlIGEgdHlwZSBzcGVjaW1lbiBib29rLkl0IGhhcyBzdXJ2aXZlZCBub3Qgb25seSBmaXZlIGNlbnR1cmllcywgYnV0IGFsc28gdGhlIGxlYXAgaW50byBlbGVjdHJvbmljIHR5cGVzZXR0aW5nLCByZW1haW5pbmcgZXNzZW50aWFsbHkgdW5jaGFuZ2VkLkl0IHdhcyBwb3B1bGFyaXNlZCBpbiB0aGUgMTk2MHMgd2l0aCB0aGUgcmVsZWFzZSBvZiBMZXRyYXNldCBzaGVldHMgY29udGFpbmluZyBMb3JlbSBJcHN1bSBwYXNzYWdlcywgYW5kIG1vcmUgcmVjZW50bHkgd2l0aCBkZXNrdG9wIHB1Ymxpc2hpbmcgc29mdHdhcmUgbGlrZSBBbGR1cyBQYWdlTWFrZXIgaW5jbHVkaW5nIHZlcnNpb25zIG9mIExvcmVtIElwc3VtLkl0IGlzIGEgbG9uZyBlc3RhYmxpc2hlZCBmYWN0IHRoYXQgYSByZWFkZXIgd2lsbCBiZSBkaXN0cmFjdGVkIGJ5IHRoZSByZWFkYWJsZSBjb250ZW50IG9mIGEgcGFnZSB3aGVuIGxvb2tpbmcgYXQgaXRzIGxheW91dC4gVGhlIHBvaW50IG9mIHVzaW5nIExvcmVtIElwc3VtIGlzIHRoYXQgaXQgaGFzIGEgbW9yZS1vci1sZXNzIG5vcm1hbCBkaXN0cmlidXRpb24gb2YgbGV0dGVycywgYXMgb3Bwb3NlZCB0byB1c2luZyBcdTAwMjdDb250ZW50IGhlcmUsIGNvbnRlbnQgaGVyZVx1MDAyNywgbWFraW5nIGl0IGxvb2sgbGlrZSByZWFkYWJsZSBFbmdsaXNoLiJ9","executionStartToCloseTimeoutSeconds":300,"taskStartToCloseTimeoutSeconds":60,"originalExecutionRunId":"1fd5d4c8-1590-4a0a-8027-535e8729de8e","identity":"","firstExecutionRunId":"1fd5d4c8-1590-4a0a-8027-535e8729de8e","attempt":0,"firstDecisionTaskBackoffSeconds":0}}]}]`
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
	h.testArchivalURI, _ = archiver.NewURI("gs://my-bucket-cad/cadence_archival/development")
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
			URI:         "gs:my-bucket-cad/cadence_archival/development",
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
			URI:         "gs:/my-bucket-cad/cadence_archival/development",
			expectedErr: archiver.ErrInvalidURI,
		},
		{
			URI:         "gs://my-bucket-cad/cadence_archival/development",
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
		DomainID:             testDomainID,
		DomainName:           testDomainName,
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
	URI, err := archiver.NewURI("gs://my-bucket-cad/cadence_archival/development")
	h.NoError(err)
	storageWrapper := &mocks.Client{}
	storageWrapper.On("Exist", ctx, URI, "").Return(true, nil).Times(1)
	mockCtrl := gomock.NewController(h.T())

	defer mockCtrl.Finish()
	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)

	historyArchiver := newHistoryArchiver(h.container, historyIterator, storageWrapper)
	request := &archiver.ArchiveHistoryRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
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
	URI, err := archiver.NewURI("gs://my-bucket-cad/cadence_archival/development")
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
		DomainID:             testDomainID,
		DomainName:           testDomainName,
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
		historyIterator.EXPECT().Next().Return(nil, &shared.ServiceBusyError{}),
	)

	historyArchiver := newHistoryArchiver(h.container, historyIterator, storageWrapper)
	request := &archiver.ArchiveHistoryRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
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
	URI, err := archiver.NewURI("gs://my-bucket-cad/cadence_archival/development")
	h.NoError(err)
	storageWrapper := &mocks.Client{}
	storageWrapper.On("Exist", ctx, URI, "").Return(true, nil).Times(1)

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

	historyArchiver := newHistoryArchiver(h.container, historyIterator, storageWrapper)
	request := &archiver.ArchiveHistoryRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	err = historyArchiver.Archive(ctx, h.testArchivalURI, request)
	h.Error(err)
}

func (h *historyArchiverSuite) TestArchive_Fail_NonRetriableErrorOption() {

	ctx := context.Background()
	mockCtrl := gomock.NewController(h.T())
	URI, err := archiver.NewURI("gs://my-bucket-cad/cadence_archival/development")
	h.NoError(err)
	storageWrapper := &mocks.Client{}
	storageWrapper.On("Exist", ctx, URI, "").Return(true, nil).Times(1)

	defer mockCtrl.Finish()
	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)
	gomock.InOrder(
		historyIterator.EXPECT().HasNext().Return(true),
		historyIterator.EXPECT().Next().Return(nil, errors.New("upload non-retriable error")),
	)

	historyArchiver := newHistoryArchiver(h.container, historyIterator, storageWrapper)
	request := &archiver.ArchiveHistoryRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		BranchToken:          testBranchToken,
		NextEventID:          testNextEventID,
		CloseFailoverVersion: testCloseFailoverVersion,
	}
	err = historyArchiver.Archive(ctx, h.testArchivalURI, request, archiver.GetNonRetriableErrorOption(errUploadNonRetriable))
	h.Equal(errUploadNonRetriable, err)
}

func (h *historyArchiverSuite) TestArchive_Success() {

	ctx := context.Background()
	mockCtrl := gomock.NewController(h.T())
	URI, err := archiver.NewURI("gs://my-bucket-cad/cadence_archival/development")
	storageWrapper := &mocks.Client{}
	storageWrapper.On("Exist", ctx, URI, mock.Anything).Return(false, nil)
	storageWrapper.On("Upload", ctx, URI, mock.Anything, mock.Anything).Return(nil)

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

	historyArchiver := newHistoryArchiver(h.container, historyIterator, storageWrapper)

	request := &archiver.ArchiveHistoryRequest{
		DomainID:             testDomainID,
		DomainName:           testDomainName,
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
		DomainID:   testDomainID,
		WorkflowID: testWorkflowID,
		RunID:      testRunID,
		PageSize:   100,
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
		DomainID:      testDomainID,
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
	h.IsType(&shared.BadRequestError{}, err)
}

func (h *historyArchiverSuite) TestGet_Success_PickHighestVersion() {
	ctx := context.Background()
	mockCtrl := gomock.NewController(h.T())
	URI, err := archiver.NewURI("gs://my-bucket-cad/cadence_archival/development")
	storageWrapper := &mocks.Client{}
	storageWrapper.On("Exist", ctx, URI, "").Return(true, nil).Times(1)
	storageWrapper.On("Query", ctx, URI, mock.Anything).Return([]string{"905702227796330300141628222723188294514017512010591354159_-24_0.history", "905702227796330300141628222723188294514017512010591354159_-25_0.history"}, nil).Times(1)
	storageWrapper.On("Get", ctx, URI, "71817125141568232911739672280485489488911532452831150339470_-24_0.history").Return([]byte(exampleHistoryRecord), nil)
	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)
	historyArchiver := newHistoryArchiver(h.container, historyIterator, storageWrapper)
	request := &archiver.GetHistoryRequest{
		DomainID:   testDomainID,
		WorkflowID: testWorkflowID,
		RunID:      testRunID,
		PageSize:   testPageSize,
	}

	h.NoError(err)
	response, err := historyArchiver.Get(ctx, URI, request)
	h.NoError(err)
	h.Nil(response.NextPageToken)
}

func (h *historyArchiverSuite) TestGet_Success_UseProvidedVersion() {

	ctx := context.Background()
	mockCtrl := gomock.NewController(h.T())
	URI, err := archiver.NewURI("gs://my-bucket-cad/cadence_archival/development")
	storageWrapper := &mocks.Client{}
	storageWrapper.On("Exist", ctx, URI, "").Return(true, nil).Times(1)
	storageWrapper.On("Query", ctx, URI, "71817125141568232911739672280485489488911532452831150339470").Return([]string{"905702227796330300141628222723188294514017512010591354159_-24_0.history", "905702227796330300141628222723188294514017512010591354159_-25_0.history"}, nil).Times(1)
	storageWrapper.On("Get", ctx, URI, "71817125141568232911739672280485489488911532452831150339470_-25_0.history").Return([]byte(exampleHistoryRecord), nil)
	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)
	historyArchiver := newHistoryArchiver(h.container, historyIterator, storageWrapper)
	request := &archiver.GetHistoryRequest{
		DomainID:             testDomainID,
		WorkflowID:           testWorkflowID,
		RunID:                testRunID,
		PageSize:             testPageSize,
		CloseFailoverVersion: common.Int64Ptr(-25),
	}

	h.NoError(err)
	response, err := historyArchiver.Get(ctx, URI, request)
	h.NoError(err)
	h.Nil(response.NextPageToken)
}

func (h *historyArchiverSuite) TestGet_Success_PageSize() {

	ctx := context.Background()
	mockCtrl := gomock.NewController(h.T())
	URI, err := archiver.NewURI("gs://my-bucket-cad/cadence_archival/development")
	storageWrapper := &mocks.Client{}
	storageWrapper.On("Exist", ctx, URI, "").Return(true, nil).Times(1)
	storageWrapper.On("Query", ctx, URI, "71817125141568232911739672280485489488911532452831150339470").Return([]string{"905702227796330300141628222723188294514017512010591354159_-24_0.history", "905702227796330300141628222723188294514017512010591354159_-24_1.history", "905702227796330300141628222723188294514017512010591354159_-24_2.history", "905702227796330300141628222723188294514017512010591354159_-24_3.history", "905702227796330300141628222723188294514017512010591354159_-25_0.history"}, nil).Times(1)
	storageWrapper.On("Get", ctx, URI, "71817125141568232911739672280485489488911532452831150339470_-24_0.history").Return([]byte(exampleHistoryRecord), nil)
	storageWrapper.On("Get", ctx, URI, "71817125141568232911739672280485489488911532452831150339470_-24_1.history").Return([]byte(exampleHistoryRecord), nil)
	storageWrapper.On("Get", ctx, URI, "71817125141568232911739672280485489488911532452831150339470_-24_2.history").Return([]byte(exampleHistoryRecord), nil)
	storageWrapper.On("Get", ctx, URI, "71817125141568232911739672280485489488911532452831150339470_-24_3.history").Return([]byte(exampleHistoryRecord), nil)

	historyIterator := archiver.NewMockHistoryIterator(mockCtrl)
	historyArchiver := newHistoryArchiver(h.container, historyIterator, storageWrapper)
	request := &archiver.GetHistoryRequest{
		DomainID:   testDomainID,
		WorkflowID: testWorkflowID,
		RunID:      testRunID,
		PageSize:   2,
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
	URI, err := archiver.NewURI("gs://my-bucket-cad/cadence_archival/development")
	storageWrapper := &mocks.Client{}
	storageWrapper.On("Exist", ctx, URI, "").Return(true, nil).Times(1)
	storageWrapper.On("Query", ctx, URI, "71817125141568232911739672280485489488911532452831150339470").Return([]string{"905702227796330300141628222723188294514017512010591354159_-24_0.history", "905702227796330300141628222723188294514017512010591354159_-24_1.history", "905702227796330300141628222723188294514017512010591354159_-24_2.history", "905702227796330300141628222723188294514017512010591354159_-24_3.history", "905702227796330300141628222723188294514017512010591354159_-25_0.history"}, nil).Times(1)
	storageWrapper.On("Get", ctx, URI, "71817125141568232911739672280485489488911532452831150339470_-24_0.history").Return([]byte(exampleHistoryRecord), nil)
	storageWrapper.On("Get", ctx, URI, "71817125141568232911739672280485489488911532452831150339470_-24_1.history").Return([]byte(exampleHistoryRecord), nil)
	storageWrapper.On("Get", ctx, URI, "71817125141568232911739672280485489488911532452831150339470_-24_2.history").Return([]byte(exampleHistoryRecord), nil)
	storageWrapper.On("Get", ctx, URI, "71817125141568232911739672280485489488911532452831150339470_-24_3.history").Return([]byte(twoEventsExampleHistoryRecord), nil)
	storageWrapper.On("Get", ctx, URI, "71817125141568232911739672280485489488911532452831150339470_-24_4.history").Return([]byte(exampleHistoryRecord), nil)
	storageWrapper.On("Get", ctx, URI, "71817125141568232911739672280485489488911532452831150339470_-24_5.history").Return([]byte(exampleHistoryRecord), nil)

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
		DomainID:      testDomainID,
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
