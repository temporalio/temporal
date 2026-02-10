package azure_store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	archiverspb "go.temporal.io/server/api/archiver/v1"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/azure_store/connector"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/util"
	"go.uber.org/mock/gomock"
)

const (
	testNamespaceID         = "test-namespace-id"
	testNamespace           = "test-namespace"
	testWorkflowID          = "test-workflow-id"
	testRunID               = "test-run-id"
	testWorkflowTypeName    = "test-workflow-type"
	exampleVisibilityRecord = `{"namespaceId":"test-namespace-id","namespace":"test-namespace","workflowId":"test-workflow-id","runId":"test-run-id","workflowTypeName":"test-workflow-type","startTime":"2020-02-05T09:56:14.804475Z","closeTime":"2020-02-05T09:56:15.946478Z","status":"Completed","historyLength":36,"memo":null,"searchAttributes":null,"historyArchivalUri":"azblob://my-container/path"}`
)

type visibilityArchiverSuite struct {
	*require.Assertions
	protorequire.ProtoAssertions
	suite.Suite
	controller                *gomock.Controller
	logger                    log.Logger
	metricsHandler            metrics.Handler
	expectedVisibilityRecords []*archiverspb.VisibilityRecord
}

func TestVisibilityArchiverSuite(t *testing.T) {
	suite.Run(t, new(visibilityArchiverSuite))
}

func (s *visibilityArchiverSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.logger = log.NewNoopLogger()
	s.metricsHandler = metrics.NoopMetricsHandler
	s.expectedVisibilityRecords = []*archiverspb.VisibilityRecord{
		{
			NamespaceId:      testNamespaceID,
			Namespace:        testNamespace,
			WorkflowId:       testWorkflowID,
			RunId:            testRunID,
			WorkflowTypeName: testWorkflowTypeName,
			StartTime:        timestamp.UnixOrZeroTimePtr(1580896574804475000),
			CloseTime:        timestamp.UnixOrZeroTimePtr(1580896575946478000),
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			HistoryLength:    36,
		},
	}
}

func (s *visibilityArchiverSuite) TearDownTest() {
	s.controller.Finish()
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
			URI:         "azblob:my-container/path",
			expectedErr: archiver.ErrInvalidURI,
		},
		{
			URI:         "azblob://",
			expectedErr: archiver.ErrInvalidURI,
		},
		{
			URI:         "azblob://my-container",
			expectedErr: archiver.ErrInvalidURI,
		},
		{
			URI:         "azblob://my-container/path",
			expectedErr: nil,
		},
	}

	storageWrapper := connector.NewMockClient(s.controller)
	storageWrapper.EXPECT().Exist(gomock.Any(), gomock.Any(), "").Return(false, nil).AnyTimes()
	visibilityArchiver := new(visibilityArchiver)
	visibilityArchiver.azureStorage = storageWrapper
	for _, tc := range testCases {
		URI, err := archiver.NewURI(tc.URI)
		s.NoError(err)
		s.Equal(tc.expectedErr, visibilityArchiver.validateURI(URI))
	}
}

func (s *visibilityArchiverSuite) TestArchive() {
	ctx := context.Background()
	URI, err := archiver.NewURI("azblob://my-container/path")
	s.NoError(err)
	storageWrapper := connector.NewMockClient(s.controller)
	storageWrapper.EXPECT().Exist(gomock.Any(), URI, gomock.Any()).Return(false, nil)
	storageWrapper.EXPECT().Upload(gomock.Any(), URI, gomock.Any(), gomock.Any()).Return(nil).Times(2)

	visibilityArchiver := newVisibilityArchiver(s.logger, s.metricsHandler, storageWrapper)
	s.NoError(err)

	request := &archiverspb.VisibilityRecord{
		Namespace:        testNamespace,
		NamespaceId:      testNamespaceID,
		WorkflowId:       testWorkflowID,
		RunId:            testRunID,
		WorkflowTypeName: testWorkflowTypeName,
		StartTime:        timestamp.TimeNowPtrUtc(),
		CloseTime:        timestamp.TimeNowPtrUtc(),
		Status:           enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		HistoryLength:    int64(101),
	}

	err = visibilityArchiver.Archive(ctx, URI, request)
	s.NoError(err)
}

func (s *visibilityArchiverSuite) TestQuery_Fail_InvalidQuery() {
	ctx := context.Background()
	URI, err := archiver.NewURI("azblob://my-container/path")
	s.NoError(err)
	storageWrapper := connector.NewMockClient(s.controller)
	storageWrapper.EXPECT().Exist(gomock.Any(), URI, gomock.Any()).Return(false, nil)
	visibilityArchiver := newVisibilityArchiver(s.logger, s.metricsHandler, storageWrapper)
	s.NoError(err)

	mockParser := NewMockQueryParser(s.controller)
	mockParser.EXPECT().Parse(gomock.Any()).Return(nil, errors.New("invalid query"))
	visibilityArchiver.queryParser = mockParser
	response, err := visibilityArchiver.Query(ctx, URI, &archiver.QueryVisibilityRequest{
		NamespaceID: testNamespaceID,
		PageSize:    10,
		Query:       "some invalid query",
	}, searchattribute.TestNameTypeMap())
	s.Error(err)
	s.Nil(response)
}

func (s *visibilityArchiverSuite) TestQuery_Success() {
	ctx := context.Background()
	URI, err := archiver.NewURI("azblob://my-container/path")
	s.NoError(err)
	storageWrapper := connector.NewMockClient(s.controller)
	storageWrapper.EXPECT().Exist(gomock.Any(), URI, gomock.Any()).Return(false, nil)
	storageWrapper.EXPECT().QueryWithFilters(gomock.Any(), URI, gomock.Any(), 10, 0, gomock.Any()).Return([]string{"test-namespace-id/closeTimeout_2020-02-05T09:56:14Z_test-wf-id_wf-type_test-run-id.visibility"}, true, 1, nil)
	storageWrapper.EXPECT().Get(gomock.Any(), URI, "test-namespace-id/closeTimeout_2020-02-05T09:56:14Z_test-wf-id_wf-type_test-run-id.visibility").Return([]byte(exampleVisibilityRecord), nil)

	visibilityArchiver := newVisibilityArchiver(s.logger, s.metricsHandler, storageWrapper)
	s.NoError(err)

	mockParser := NewMockQueryParser(s.controller)
	dayPrecision := PrecisionDay
	closeTime, _ := time.Parse(time.RFC3339, "2020-02-05T09:56:14Z")
	mockParser.EXPECT().Parse(gomock.Any()).Return(&parsedQuery{
		closeTime:       closeTime,
		searchPrecision: &dayPrecision,
		workflowID:      util.Ptr(testWorkflowID),
	}, nil)
	visibilityArchiver.queryParser = mockParser
	request := &archiver.QueryVisibilityRequest{
		NamespaceID: testNamespaceID,
		PageSize:    10,
		Query:       "parsed by mockParser",
	}

	response, err := visibilityArchiver.Query(ctx, URI, request, searchattribute.TestNameTypeMap())
	s.NoError(err)
	s.NotNil(response)
	s.Len(response.Executions, 1)
}
