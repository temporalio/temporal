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

package frontend

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"go.temporal.io/server/common/clock"

	"google.golang.org/grpc/health"

	"go.temporal.io/server/api/adminservicemock/v1"
	"go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/testing/mocksdk"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/adminservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	clientmocks "go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/searchattribute"
)

type (
	adminHandlerSuite struct {
		suite.Suite
		*require.Assertions

		controller         *gomock.Controller
		mockResource       *resource.Test
		mockHistoryClient  *historyservicemock.MockHistoryServiceClient
		mockNamespaceCache *namespace.MockRegistry

		mockExecutionMgr           *persistence.MockExecutionManager
		mockVisibilityMgr          *manager.MockVisibilityManager
		mockClusterMetadataManager *persistence.MockClusterMetadataManager
		mockClientFactory          *clientmocks.MockFactory
		mockAdminClient            *adminservicemock.MockAdminServiceClient
		mockMetadata               *cluster.MockMetadata
		mockProducer               *persistence.MockNamespaceReplicationQueue

		namespace      namespace.Name
		namespaceID    namespace.ID
		namespaceEntry *namespace.Namespace

		handler *AdminHandler
	}
)

func TestAdminHandlerSuite(t *testing.T) {
	s := new(adminHandlerSuite)
	suite.Run(t, s)
}

func (s *adminHandlerSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.namespace = "some random namespace name"
	s.namespaceID = "deadd0d0-c001-face-d00d-000000000000"
	s.namespaceEntry = namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Name: s.namespace.String(),
			Id:   s.namespaceID.String(),
		},
		nil,
		false,
		nil,
		int64(100),
	)

	s.controller = gomock.NewController(s.T())
	s.mockResource = resource.NewTest(s.controller, metrics.Frontend)
	s.mockNamespaceCache = s.mockResource.NamespaceCache
	s.mockHistoryClient = s.mockResource.HistoryClient
	s.mockExecutionMgr = s.mockResource.ExecutionMgr
	s.mockClusterMetadataManager = s.mockResource.ClusterMetadataMgr
	s.mockClientFactory = s.mockResource.ClientFactory
	s.mockAdminClient = adminservicemock.NewMockAdminServiceClient(s.controller)
	s.mockMetadata = s.mockResource.ClusterMetadata
	s.mockVisibilityMgr = manager.NewMockVisibilityManager(s.controller)
	s.mockProducer = persistence.NewMockNamespaceReplicationQueue(s.controller)

	persistenceConfig := &config.Persistence{
		NumHistoryShards: 1,
	}

	cfg := &Config{}
	args := NewAdminHandlerArgs{
		persistenceConfig,
		cfg,
		s.mockResource.GetNamespaceReplicationQueue(),
		s.mockProducer,
		nil,
		s.mockResource.ESClient,
		s.mockVisibilityMgr,
		s.mockResource.Logger,
		s.mockResource.GetExecutionManager(),
		s.mockResource.GetTaskManager(),
		s.mockResource.GetClusterMetadataManager(),
		s.mockResource.GetMetadataManager(),
		s.mockResource.GetClientFactory(),
		s.mockResource.GetClientBean(),
		s.mockResource.GetHistoryClient(),
		s.mockResource.GetSDKClientFactory(),
		s.mockResource.GetMembershipMonitor(),
		s.mockResource.GetArchiverProvider(),
		s.mockResource.GetMetricsClient(),
		s.mockResource.GetNamespaceRegistry(),
		s.mockResource.GetSearchAttributesProvider(),
		s.mockResource.GetSearchAttributesManager(),
		s.mockMetadata,
		s.mockResource.GetArchivalMetadata(),
		health.NewServer(),
		serialization.NewSerializer(),
		clock.NewRealTimeSource(),
	}
	s.mockMetadata.EXPECT().GetCurrentClusterName().Return(uuid.New()).AnyTimes()
	s.handler = NewAdminHandler(args)
	s.handler.Start()
}

func (s *adminHandlerSuite) TearDownTest() {
	s.controller.Finish()
	s.handler.Stop()
}

func (s *adminHandlerSuite) Test_GetWorkflowExecutionRawHistoryV2_FailedOnInvalidWorkflowID() {
	ctx := context.Background()
	_, err := s.handler.GetWorkflowExecutionRawHistoryV2(ctx,
		&adminservice.GetWorkflowExecutionRawHistoryV2Request{
			Namespace: s.namespace.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: "",
				RunId:      uuid.New(),
			},
			StartEventId:      1,
			StartEventVersion: 100,
			EndEventId:        10,
			EndEventVersion:   100,
			MaximumPageSize:   1,
			NextPageToken:     nil,
		})
	s.Error(err)
}

func (s *adminHandlerSuite) Test_GetWorkflowExecutionRawHistoryV2_FailedOnInvalidRunID() {
	ctx := context.Background()
	_, err := s.handler.GetWorkflowExecutionRawHistoryV2(ctx,
		&adminservice.GetWorkflowExecutionRawHistoryV2Request{
			Namespace: s.namespace.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: "workflowID",
				RunId:      "runID",
			},
			StartEventId:      1,
			StartEventVersion: 100,
			EndEventId:        10,
			EndEventVersion:   100,
			MaximumPageSize:   1,
			NextPageToken:     nil,
		})
	s.Error(err)
}

func (s *adminHandlerSuite) Test_GetWorkflowExecutionRawHistoryV2_FailedOnInvalidSize() {
	ctx := context.Background()
	_, err := s.handler.GetWorkflowExecutionRawHistoryV2(ctx,
		&adminservice.GetWorkflowExecutionRawHistoryV2Request{
			Namespace: s.namespace.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: "workflowID",
				RunId:      uuid.New(),
			},
			StartEventId:      1,
			StartEventVersion: 100,
			EndEventId:        10,
			EndEventVersion:   100,
			MaximumPageSize:   -1,
			NextPageToken:     nil,
		})
	s.Error(err)
}

func (s *adminHandlerSuite) Test_GetWorkflowExecutionRawHistoryV2_FailedOnNamespaceCache() {
	ctx := context.Background()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(nil, fmt.Errorf("test"))
	_, err := s.handler.GetWorkflowExecutionRawHistoryV2(ctx,
		&adminservice.GetWorkflowExecutionRawHistoryV2Request{
			Namespace:   s.namespace.String(),
			NamespaceId: s.namespaceID.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: "workflowID",
				RunId:      uuid.New(),
			},
			StartEventId:      1,
			StartEventVersion: 100,
			EndEventId:        10,
			EndEventVersion:   100,
			MaximumPageSize:   1,
			NextPageToken:     nil,
		})
	s.Error(err)
}

func (s *adminHandlerSuite) Test_GetWorkflowExecutionRawHistoryV2() {
	ctx := context.Background()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(s.namespaceEntry, nil).AnyTimes()
	branchToken := []byte{1}
	versionHistory := versionhistory.NewVersionHistory(branchToken, []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(int64(10), int64(100)),
	})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	mState := &historyservice.GetMutableStateResponse{
		NextEventId:        11,
		CurrentBranchToken: branchToken,
		VersionHistories:   versionHistories,
	}
	s.mockHistoryClient.EXPECT().GetMutableState(gomock.Any(), gomock.Any()).Return(mState, nil).AnyTimes()

	s.mockExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{},
		NextPageToken:     []byte{},
		Size:              0,
	}, nil)
	_, err := s.handler.GetWorkflowExecutionRawHistoryV2(ctx,
		&adminservice.GetWorkflowExecutionRawHistoryV2Request{
			Namespace:   s.namespace.String(),
			NamespaceId: s.namespaceID.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: "workflowID",
				RunId:      uuid.New(),
			},
			StartEventId:      1,
			StartEventVersion: 100,
			EndEventId:        10,
			EndEventVersion:   100,
			MaximumPageSize:   10,
			NextPageToken:     nil,
		})
	s.NoError(err)
}

func (s *adminHandlerSuite) Test_GetWorkflowExecutionRawHistoryV2_SameStartIDAndEndID() {
	ctx := context.Background()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(s.namespaceEntry, nil).AnyTimes()
	branchToken := []byte{1}
	versionHistory := versionhistory.NewVersionHistory(branchToken, []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(int64(10), int64(100)),
	})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	mState := &historyservice.GetMutableStateResponse{
		NextEventId:        11,
		CurrentBranchToken: branchToken,
		VersionHistories:   versionHistories,
	}
	s.mockHistoryClient.EXPECT().GetMutableState(gomock.Any(), gomock.Any()).Return(mState, nil).AnyTimes()

	resp, err := s.handler.GetWorkflowExecutionRawHistoryV2(ctx,
		&adminservice.GetWorkflowExecutionRawHistoryV2Request{
			Namespace:   s.namespace.String(),
			NamespaceId: s.namespaceID.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: "workflowID",
				RunId:      uuid.New(),
			},
			StartEventId:      10,
			StartEventVersion: 100,
			EndEventId:        common.EmptyEventID,
			EndEventVersion:   common.EmptyVersion,
			MaximumPageSize:   1,
			NextPageToken:     nil,
		})
	s.Nil(resp.NextPageToken)
	s.NoError(err)
}

func (s *adminHandlerSuite) Test_SetRequestDefaultValueAndGetTargetVersionHistory_DefinedStartAndEnd() {
	inputStartEventID := int64(1)
	inputStartVersion := int64(10)
	inputEndEventID := int64(100)
	inputEndVersion := int64(11)
	firstItem := versionhistory.NewVersionHistoryItem(inputStartEventID, inputStartVersion)
	endItem := versionhistory.NewVersionHistoryItem(inputEndEventID, inputEndVersion)
	versionHistory := versionhistory.NewVersionHistory([]byte{}, []*historyspb.VersionHistoryItem{firstItem, endItem})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	request := &adminservice.GetWorkflowExecutionRawHistoryV2Request{
		Namespace: s.namespace.String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID",
			RunId:      uuid.New(),
		},
		StartEventId:      inputStartEventID,
		StartEventVersion: inputStartVersion,
		EndEventId:        inputEndEventID,
		EndEventVersion:   inputEndVersion,
		MaximumPageSize:   10,
		NextPageToken:     nil,
	}

	targetVersionHistory, err := s.handler.setRequestDefaultValueAndGetTargetVersionHistory(
		request,
		versionHistories,
	)
	s.Equal(request.GetStartEventId(), inputStartEventID)
	s.Equal(request.GetEndEventId(), inputEndEventID)
	s.Equal(targetVersionHistory, versionHistory)
	s.NoError(err)
}

func (s *adminHandlerSuite) Test_SetRequestDefaultValueAndGetTargetVersionHistory_DefinedEndEvent() {
	inputStartEventID := int64(1)
	inputEndEventID := int64(100)
	inputStartVersion := int64(10)
	inputEndVersion := int64(11)
	firstItem := versionhistory.NewVersionHistoryItem(inputStartEventID, inputStartVersion)
	targetItem := versionhistory.NewVersionHistoryItem(inputEndEventID, inputEndVersion)
	versionHistory := versionhistory.NewVersionHistory([]byte{}, []*historyspb.VersionHistoryItem{firstItem, targetItem})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	request := &adminservice.GetWorkflowExecutionRawHistoryV2Request{
		Namespace: s.namespace.String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID",
			RunId:      uuid.New(),
		},
		StartEventId:      common.EmptyEventID,
		StartEventVersion: common.EmptyVersion,
		EndEventId:        inputEndEventID,
		EndEventVersion:   inputEndVersion,
		MaximumPageSize:   10,
		NextPageToken:     nil,
	}

	targetVersionHistory, err := s.handler.setRequestDefaultValueAndGetTargetVersionHistory(
		request,
		versionHistories,
	)
	s.Equal(request.GetStartEventId(), inputStartEventID-1)
	s.Equal(request.GetEndEventId(), inputEndEventID)
	s.Equal(targetVersionHistory, versionHistory)
	s.NoError(err)
}

func (s *adminHandlerSuite) Test_SetRequestDefaultValueAndGetTargetVersionHistory_DefinedStartEvent() {
	inputStartEventID := int64(1)
	inputEndEventID := int64(100)
	inputStartVersion := int64(10)
	inputEndVersion := int64(11)
	firstItem := versionhistory.NewVersionHistoryItem(inputStartEventID, inputStartVersion)
	targetItem := versionhistory.NewVersionHistoryItem(inputEndEventID, inputEndVersion)
	versionHistory := versionhistory.NewVersionHistory([]byte{}, []*historyspb.VersionHistoryItem{firstItem, targetItem})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	request := &adminservice.GetWorkflowExecutionRawHistoryV2Request{
		Namespace: s.namespace.String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID",
			RunId:      uuid.New(),
		},
		StartEventId:      inputStartEventID,
		StartEventVersion: inputStartVersion,
		EndEventId:        common.EmptyEventID,
		EndEventVersion:   common.EmptyVersion,
		MaximumPageSize:   10,
		NextPageToken:     nil,
	}

	targetVersionHistory, err := s.handler.setRequestDefaultValueAndGetTargetVersionHistory(
		request,
		versionHistories,
	)
	s.Equal(request.GetStartEventId(), inputStartEventID)
	s.Equal(request.GetEndEventId(), inputEndEventID+1)
	s.Equal(targetVersionHistory, versionHistory)
	s.NoError(err)
}

func (s *adminHandlerSuite) Test_SetRequestDefaultValueAndGetTargetVersionHistory_NonCurrentBranch() {
	inputStartEventID := int64(1)
	inputEndEventID := int64(100)
	inputStartVersion := int64(10)
	inputEndVersion := int64(101)
	item1 := versionhistory.NewVersionHistoryItem(inputStartEventID, inputStartVersion)
	item2 := versionhistory.NewVersionHistoryItem(inputEndEventID, inputEndVersion)
	versionHistory1 := versionhistory.NewVersionHistory([]byte{}, []*historyspb.VersionHistoryItem{item1, item2})
	item3 := versionhistory.NewVersionHistoryItem(int64(10), int64(20))
	item4 := versionhistory.NewVersionHistoryItem(int64(20), int64(51))
	versionHistory2 := versionhistory.NewVersionHistory([]byte{}, []*historyspb.VersionHistoryItem{item1, item3, item4})
	versionHistories := versionhistory.NewVersionHistories(versionHistory1)
	_, _, err := versionhistory.AddVersionHistory(versionHistories, versionHistory2)
	s.NoError(err)
	request := &adminservice.GetWorkflowExecutionRawHistoryV2Request{
		Namespace: s.namespace.String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID",
			RunId:      uuid.New(),
		},
		StartEventId:      9,
		StartEventVersion: 20,
		EndEventId:        inputEndEventID,
		EndEventVersion:   inputEndVersion,
		MaximumPageSize:   10,
		NextPageToken:     nil,
	}

	targetVersionHistory, err := s.handler.setRequestDefaultValueAndGetTargetVersionHistory(
		request,
		versionHistories,
	)
	s.Equal(request.GetStartEventId(), inputStartEventID)
	s.Equal(request.GetEndEventId(), inputEndEventID)
	s.Equal(targetVersionHistory, versionHistory1)
	s.NoError(err)
}

func (s *adminHandlerSuite) Test_AddSearchAttributes() {
	handler := s.handler
	ctx := context.Background()

	type test struct {
		Name     string
		Request  *adminservice.AddSearchAttributesRequest
		Expected error
	}
	// request validation tests
	testCases1 := []test{
		{
			Name:     "nil request",
			Request:  nil,
			Expected: &serviceerror.InvalidArgument{Message: "Request is nil."},
		},
		{
			Name:     "empty request",
			Request:  &adminservice.AddSearchAttributesRequest{},
			Expected: &serviceerror.InvalidArgument{Message: "SearchAttributes are not set on request."},
		},
	}
	for _, testCase := range testCases1 {
		s.T().Run(testCase.Name, func(t *testing.T) {
			resp, err := handler.AddSearchAttributes(ctx, testCase.Request)
			s.Equal(testCase.Expected, err)
			s.Nil(resp)
		})
	}

	// Elasticsearch is not configured
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("", true).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()
	testCases3 := []test{
		{
			Name: "reserved key (empty index)",
			Request: &adminservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"WorkflowId": enumspb.INDEXED_VALUE_TYPE_TEXT,
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "Search attribute WorkflowId is reserved by system."},
		},
		{
			Name: "key already whitelisted (empty index)",
			Request: &adminservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomTextField": enumspb.INDEXED_VALUE_TYPE_TEXT,
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "Search attribute CustomTextField already exists."},
		},
	}
	for _, testCase := range testCases3 {
		s.T().Run(testCase.Name, func(t *testing.T) {
			resp, err := handler.AddSearchAttributes(ctx, testCase.Request)
			s.Equal(testCase.Expected, err)
			s.Nil(resp)
		})
	}

	// Configure Elasticsearch: add advanced visibility store config with index name.
	handler.ESConfig = &client.Config{
		Indices: map[string]string{
			"visibility": "random-index-name",
		},
	}

	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("random-index-name", true).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()
	testCases2 := []test{
		{
			Name: "reserved key (ES configured)",
			Request: &adminservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"WorkflowId": enumspb.INDEXED_VALUE_TYPE_TEXT,
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "Search attribute WorkflowId is reserved by system."},
		},
		{
			Name: "key already whitelisted (ES configured)",
			Request: &adminservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomTextField": enumspb.INDEXED_VALUE_TYPE_TEXT,
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "Search attribute CustomTextField already exists."},
		},
	}
	for _, testCase := range testCases2 {
		s.T().Run(testCase.Name, func(t *testing.T) {
			resp, err := handler.AddSearchAttributes(ctx, testCase.Request)
			s.Equal(testCase.Expected, err)
			s.Nil(resp)
		})
	}

	mockSdkClient := mocksdk.NewMockClient(s.controller)
	s.mockResource.SDKClientFactory.EXPECT().GetSystemClient(gomock.Any()).Return(mockSdkClient).AnyTimes()

	// Start workflow failed.
	mockSdkClient.EXPECT().ExecuteWorkflow(gomock.Any(), gomock.Any(), "temporal-sys-add-search-attributes-workflow", gomock.Any()).Return(nil, errors.New("start failed"))
	resp, err := handler.AddSearchAttributes(ctx, &adminservice.AddSearchAttributesRequest{
		SearchAttributes: map[string]enumspb.IndexedValueType{
			"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	})
	s.Error(err)
	s.Equal("Unable to start temporal-sys-add-search-attributes-workflow workflow: start failed.", err.Error())
	s.Nil(resp)

	// Workflow failed.
	mockRun := mocksdk.NewMockWorkflowRun(s.controller)
	mockRun.EXPECT().Get(gomock.Any(), nil).Return(errors.New("workflow failed"))
	mockSdkClient.EXPECT().ExecuteWorkflow(gomock.Any(), gomock.Any(), "temporal-sys-add-search-attributes-workflow", gomock.Any()).Return(mockRun, nil)
	resp, err = handler.AddSearchAttributes(ctx, &adminservice.AddSearchAttributesRequest{
		SearchAttributes: map[string]enumspb.IndexedValueType{
			"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	})
	s.Error(err)
	s.Equal("Workflow temporal-sys-add-search-attributes-workflow returned an error: workflow failed.", err.Error())
	s.Nil(resp)

	// Success case.
	mockRun.EXPECT().Get(gomock.Any(), nil).Return(nil)
	mockSdkClient.EXPECT().ExecuteWorkflow(gomock.Any(), gomock.Any(), "temporal-sys-add-search-attributes-workflow", gomock.Any()).Return(mockRun, nil)

	resp, err = handler.AddSearchAttributes(ctx, &adminservice.AddSearchAttributesRequest{
		SearchAttributes: map[string]enumspb.IndexedValueType{
			"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	})
	s.NoError(err)
	s.NotNil(resp)
}

func (s *adminHandlerSuite) Test_GetSearchAttributes() {
	handler := s.handler
	ctx := context.Background()

	resp, err := handler.GetSearchAttributes(ctx, nil)
	s.Error(err)
	s.Equal(&serviceerror.InvalidArgument{Message: "Request is nil."}, err)
	s.Nil(resp)

	mockSdkClient := mocksdk.NewMockClient(s.controller)
	s.mockResource.SDKClientFactory.EXPECT().GetSystemClient(gomock.Any()).Return(mockSdkClient).AnyTimes()

	// Elasticsearch is not configured
	mockSdkClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), "temporal-sys-add-search-attributes-workflow", "").Return(
		&workflowservice.DescribeWorkflowExecutionResponse{}, nil)
	s.mockResource.ESClient.EXPECT().GetMapping(gomock.Any(), "").Return(map[string]string{"col": "type"}, nil)
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("", true).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()

	resp, err = handler.GetSearchAttributes(ctx, &adminservice.GetSearchAttributesRequest{})
	s.NoError(err)
	s.NotNil(resp)

	// Configure Elasticsearch: add advanced visibility store config with index name.
	handler.ESConfig = &client.Config{
		Indices: map[string]string{
			"visibility": "random-index-name",
		},
	}

	mockSdkClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), "temporal-sys-add-search-attributes-workflow", "").Return(
		&workflowservice.DescribeWorkflowExecutionResponse{}, nil)
	s.mockResource.ESClient.EXPECT().GetMapping(gomock.Any(), "random-index-name").Return(map[string]string{"col": "type"}, nil)
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("random-index-name", true).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()
	resp, err = handler.GetSearchAttributes(ctx, &adminservice.GetSearchAttributesRequest{})
	s.NoError(err)
	s.NotNil(resp)

	mockSdkClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), "temporal-sys-add-search-attributes-workflow", "").Return(
		&workflowservice.DescribeWorkflowExecutionResponse{}, nil)
	s.mockResource.ESClient.EXPECT().GetMapping(gomock.Any(), "another-index-name").Return(map[string]string{"col": "type"}, nil)
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("another-index-name", true).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()
	resp, err = handler.GetSearchAttributes(ctx, &adminservice.GetSearchAttributesRequest{IndexName: "another-index-name"})
	s.NoError(err)
	s.NotNil(resp)

	mockSdkClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), "temporal-sys-add-search-attributes-workflow", "").Return(
		nil, errors.New("random error"))
	s.mockResource.ESClient.EXPECT().GetMapping(gomock.Any(), "random-index-name").Return(map[string]string{"col": "type"}, nil)
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("random-index-name", true).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()
	resp, err = handler.GetSearchAttributes(ctx, &adminservice.GetSearchAttributesRequest{})
	s.Error(err)
	s.Nil(resp)
}

func (s *adminHandlerSuite) Test_RemoveSearchAttributes() {
	handler := s.handler
	ctx := context.Background()

	type test struct {
		Name     string
		Request  *adminservice.RemoveSearchAttributesRequest
		Expected error
	}
	// request validation tests
	testCases1 := []test{
		{
			Name:     "nil request",
			Request:  nil,
			Expected: &serviceerror.InvalidArgument{Message: "Request is nil."},
		},
		{
			Name:     "empty request",
			Request:  &adminservice.RemoveSearchAttributesRequest{},
			Expected: &serviceerror.InvalidArgument{Message: "SearchAttributes are not set on request."},
		},
	}
	for _, testCase := range testCases1 {
		s.T().Run(testCase.Name, func(t *testing.T) {
			resp, err := handler.RemoveSearchAttributes(ctx, testCase.Request)
			s.Equal(testCase.Expected, err)
			s.Nil(resp)
		})
	}

	// Elasticsearch is not configured
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("", true).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()
	testCases3 := []test{
		{
			Name: "reserved search attribute (empty index)",
			Request: &adminservice.RemoveSearchAttributesRequest{
				SearchAttributes: []string{
					"WorkflowId",
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "Unable to remove non-custom search attributes: WorkflowId."},
		},
		{
			Name: "search attribute doesn't exist (empty index)",
			Request: &adminservice.RemoveSearchAttributesRequest{
				SearchAttributes: []string{
					"ProductId",
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "Search attribute ProductId doesn't exist."},
		},
	}
	for _, testCase := range testCases3 {
		s.T().Run(testCase.Name, func(t *testing.T) {
			resp, err := handler.RemoveSearchAttributes(ctx, testCase.Request)
			s.Equal(testCase.Expected, err)
			s.Nil(resp)
		})
	}

	// Configure Elasticsearch: add advanced visibility store config with index name.
	handler.ESConfig = &client.Config{
		Indices: map[string]string{
			"visibility": "random-index-name",
		},
	}

	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("random-index-name", true).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()
	testCases2 := []test{
		{
			Name: "reserved search attribute (ES configured)",
			Request: &adminservice.RemoveSearchAttributesRequest{
				SearchAttributes: []string{
					"WorkflowId",
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "Unable to remove non-custom search attributes: WorkflowId."},
		},
		{
			Name: "search attribute doesn't exist (ES configured)",
			Request: &adminservice.RemoveSearchAttributesRequest{
				SearchAttributes: []string{
					"ProductId",
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "Search attribute ProductId doesn't exist."},
		},
	}
	for _, testCase := range testCases2 {
		s.T().Run(testCase.Name, func(t *testing.T) {
			resp, err := handler.RemoveSearchAttributes(ctx, testCase.Request)
			s.Equal(testCase.Expected, err)
			s.Nil(resp)
		})
	}

	// Success case.
	s.mockResource.SearchAttributesManager.EXPECT().SaveSearchAttributes(gomock.Any(), "random-index-name", gomock.Any()).Return(nil)

	resp, err := handler.RemoveSearchAttributes(ctx, &adminservice.RemoveSearchAttributesRequest{
		SearchAttributes: []string{
			"CustomKeywordField",
		},
	})
	s.NoError(err)
	s.NotNil(resp)
}

func (s *adminHandlerSuite) Test_RemoveRemoteCluster_Success() {
	var clusterName = "cluster"
	s.mockClusterMetadataManager.EXPECT().DeleteClusterMetadata(
		gomock.Any(),
		&persistence.DeleteClusterMetadataRequest{ClusterName: clusterName},
	).Return(nil)

	_, err := s.handler.RemoveRemoteCluster(context.Background(), &adminservice.RemoveRemoteClusterRequest{ClusterName: clusterName})
	s.NoError(err)
}

func (s *adminHandlerSuite) Test_RemoveRemoteCluster_Error() {
	var clusterName = "cluster"
	s.mockClusterMetadataManager.EXPECT().DeleteClusterMetadata(
		gomock.Any(),
		&persistence.DeleteClusterMetadataRequest{ClusterName: clusterName},
	).Return(fmt.Errorf("test error"))

	_, err := s.handler.RemoveRemoteCluster(context.Background(), &adminservice.RemoveRemoteClusterRequest{ClusterName: clusterName})
	s.Error(err)
}

func (s *adminHandlerSuite) Test_AddOrUpdateRemoteCluster_RecordFound_Success() {
	var rpcAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()
	var recordVersion int64 = 5

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockClientFactory.EXPECT().NewAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        0,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		&persistence.GetClusterMetadataResponse{
			Version: recordVersion,
		}, nil)
	s.mockClusterMetadataManager.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        0,
			ClusterId:                clusterId,
			ClusterAddress:           rpcAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: recordVersion,
	}).Return(true, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.NoError(err)
}

func (s *adminHandlerSuite) Test_AddOrUpdateRemoteCluster_RecordNotFound_Success() {
	var rpcAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockClientFactory.EXPECT().NewAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        0,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		nil,
		serviceerror.NewNotFound("expected empty result"),
	)
	s.mockClusterMetadataManager.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        0,
			ClusterId:                clusterId,
			ClusterAddress:           rpcAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: 0,
	}).Return(true, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.NoError(err)
}

func (s *adminHandlerSuite) Test_AddOrUpdateRemoteCluster_ValidationError_ClusterNameConflict() {
	var rpcAddress = uuid.New()
	var clusterId = uuid.New()

	s.mockClientFactory.EXPECT().NewAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              s.mockMetadata.GetCurrentClusterName(),
			HistoryShardCount:        0,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *adminHandlerSuite) Test_AddOrUpdateRemoteCluster_ValidationError_FailoverVersionIncrementMismatch() {
	var rpcAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1))
	s.mockClientFactory.EXPECT().NewAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        0,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *adminHandlerSuite) Test_AddOrUpdateRemoteCluster_ValidationError_ShardCountMismatch() {
	var rpcAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockClientFactory.EXPECT().NewAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        1000,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *adminHandlerSuite) Test_AddOrUpdateRemoteCluster_ValidationError_GlobalNamespaceDisabled() {
	var rpcAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockClientFactory.EXPECT().NewAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        0,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: false,
		}, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *adminHandlerSuite) Test_AddOrUpdateRemoteCluster_ValidationError_InitialFailoverVersionConflict() {
	var rpcAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		uuid.New(): {InitialFailoverVersion: 0},
	})
	s.mockClientFactory.EXPECT().NewAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        0,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *adminHandlerSuite) Test_AddOrUpdateRemoteCluster_DescribeCluster_Error() {
	var rpcAddress = uuid.New()

	s.mockClientFactory.EXPECT().NewAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		nil,
		fmt.Errorf("test error"),
	)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
}

func (s *adminHandlerSuite) Test_AddOrUpdateRemoteCluster_GetClusterMetadata_Error() {
	var rpcAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockClientFactory.EXPECT().NewAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        0,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		nil,
		fmt.Errorf("test error"),
	)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
}

func (s *adminHandlerSuite) Test_AddOrUpdateRemoteCluster_SaveClusterMetadata_Error() {
	var rpcAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockClientFactory.EXPECT().NewAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        0,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		nil,
		serviceerror.NewNotFound("expected empty result"),
	)
	s.mockClusterMetadataManager.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        0,
			ClusterId:                clusterId,
			ClusterAddress:           rpcAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: 0,
	}).Return(false, fmt.Errorf("test error"))
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
}

func (s *adminHandlerSuite) Test_AddOrUpdateRemoteCluster_SaveClusterMetadata_NotApplied_Error() {
	var rpcAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockClientFactory.EXPECT().NewAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        0,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		nil,
		serviceerror.NewNotFound("expected empty result"),
	)
	s.mockClusterMetadataManager.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        0,
			ClusterId:                clusterId,
			ClusterAddress:           rpcAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: 0,
	}).Return(false, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *adminHandlerSuite) Test_DescribeCluster_CurrentCluster_Success() {
	var clusterId = uuid.New()
	clusterName := s.mockMetadata.GetCurrentClusterName()
	s.mockResource.MembershipMonitor.EXPECT().WhoAmI().Return(&membership.HostInfo{}, nil)
	s.mockResource.MembershipMonitor.EXPECT().GetReachableMembers().Return(nil, nil)
	s.mockResource.HistoryServiceResolver.EXPECT().Members().Return([]*membership.HostInfo{})
	s.mockResource.HistoryServiceResolver.EXPECT().MemberCount().Return(0)
	s.mockResource.FrontendServiceResolver.EXPECT().Members().Return([]*membership.HostInfo{})
	s.mockResource.FrontendServiceResolver.EXPECT().MemberCount().Return(0)
	s.mockResource.MatchingServiceResolver.EXPECT().Members().Return([]*membership.HostInfo{})
	s.mockResource.MatchingServiceResolver.EXPECT().MemberCount().Return(0)
	s.mockResource.WorkerServiceResolver.EXPECT().Members().Return([]*membership.HostInfo{})
	s.mockResource.WorkerServiceResolver.EXPECT().MemberCount().Return(0)
	s.mockResource.ExecutionMgr.EXPECT().GetName().Return("")
	s.mockVisibilityMgr.EXPECT().GetName().Return("")
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		&persistence.GetClusterMetadataResponse{
			ClusterMetadata: persistencespb.ClusterMetadata{
				ClusterName:              clusterName,
				HistoryShardCount:        0,
				ClusterId:                clusterId,
				FailoverVersionIncrement: 0,
				InitialFailoverVersion:   0,
				IsGlobalNamespaceEnabled: true,
			},
			Version: 1,
		}, nil)

	resp, err := s.handler.DescribeCluster(context.Background(), &adminservice.DescribeClusterRequest{})
	s.NoError(err)
	s.Equal(resp.GetClusterName(), clusterName)
	s.Equal(resp.GetClusterId(), clusterId)
	s.Equal(resp.GetHistoryShardCount(), int32(0))
	s.Equal(resp.GetFailoverVersionIncrement(), int64(0))
	s.Equal(resp.GetInitialFailoverVersion(), int64(0))
	s.True(resp.GetIsGlobalNamespaceEnabled())
}

func (s *adminHandlerSuite) Test_DescribeCluster_NonCurrentCluster_Success() {
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockResource.MembershipMonitor.EXPECT().WhoAmI().Return(&membership.HostInfo{}, nil)
	s.mockResource.MembershipMonitor.EXPECT().GetReachableMembers().Return(nil, nil)
	s.mockResource.HistoryServiceResolver.EXPECT().Members().Return([]*membership.HostInfo{})
	s.mockResource.HistoryServiceResolver.EXPECT().MemberCount().Return(0)
	s.mockResource.FrontendServiceResolver.EXPECT().Members().Return([]*membership.HostInfo{})
	s.mockResource.FrontendServiceResolver.EXPECT().MemberCount().Return(0)
	s.mockResource.MatchingServiceResolver.EXPECT().Members().Return([]*membership.HostInfo{})
	s.mockResource.MatchingServiceResolver.EXPECT().MemberCount().Return(0)
	s.mockResource.WorkerServiceResolver.EXPECT().Members().Return([]*membership.HostInfo{})
	s.mockResource.WorkerServiceResolver.EXPECT().MemberCount().Return(0)
	s.mockResource.ExecutionMgr.EXPECT().GetName().Return("")
	s.mockVisibilityMgr.EXPECT().GetName().Return("")
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		&persistence.GetClusterMetadataResponse{
			ClusterMetadata: persistencespb.ClusterMetadata{
				ClusterName:              clusterName,
				HistoryShardCount:        0,
				ClusterId:                clusterId,
				FailoverVersionIncrement: 0,
				InitialFailoverVersion:   0,
				IsGlobalNamespaceEnabled: true,
			},
			Version: 1,
		}, nil)

	resp, err := s.handler.DescribeCluster(context.Background(), &adminservice.DescribeClusterRequest{ClusterName: clusterName})
	s.NoError(err)
	s.Equal(resp.GetClusterName(), clusterName)
	s.Equal(resp.GetClusterId(), clusterId)
	s.Equal(resp.GetHistoryShardCount(), int32(0))
	s.Equal(resp.GetFailoverVersionIncrement(), int64(0))
	s.Equal(resp.GetInitialFailoverVersion(), int64(0))
	s.True(resp.GetIsGlobalNamespaceEnabled())
}

func (s *adminHandlerSuite) Test_ListClusters_Success() {
	var pageSize int32 = 1

	s.mockClusterMetadataManager.EXPECT().ListClusterMetadata(gomock.Any(), &persistence.ListClusterMetadataRequest{
		PageSize: int(pageSize),
	}).Return(
		&persistence.ListClusterMetadataResponse{
			ClusterMetadata: []*persistence.GetClusterMetadataResponse{
				{
					ClusterMetadata: persistencespb.ClusterMetadata{ClusterName: "test"},
				},
			}}, nil)

	resp, err := s.handler.ListClusters(context.Background(), &adminservice.ListClustersRequest{
		PageSize: pageSize,
	})
	s.NoError(err)
	s.Equal(1, len(resp.Clusters))
	s.Equal(0, len(resp.GetNextPageToken()))
}

func (s *adminHandlerSuite) TestDeleteWorkflowExecution_DeleteCurrentExecution() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "workflowID",
	}

	request := &adminservice.DeleteWorkflowExecutionRequest{
		Namespace: s.namespace.String(),
		Execution: &execution,
	}

	s.mockNamespaceCache.EXPECT().GetNamespaceID(s.namespace).Return(s.namespaceID, nil).AnyTimes()
	s.mockVisibilityMgr.EXPECT().GetName().Return("elasticsearch").AnyTimes()

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(nil, errors.New("some random error"))
	resp, err := s.handler.DeleteWorkflowExecution(context.Background(), request)
	s.Nil(resp)
	s.Error(err)

	mutableState := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			VersionHistories: &historyspb.VersionHistories{
				CurrentVersionHistoryIndex: 0,
				Histories: []*historyspb.VersionHistory{
					{BranchToken: []byte("branch1")},
					{BranchToken: []byte("branch2")},
					{BranchToken: []byte("branch3")},
				},
			},
		},
	}

	shardID := common.WorkflowIDToHistoryShard(
		s.namespaceID.String(),
		execution.GetWorkflowId(),
		s.handler.numberOfHistoryShards,
	)
	runID := uuid.New()
	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetCurrentExecutionResponse{
		StartRequestID: uuid.New(),
		RunID:          runID,
		State:          enums.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status:         enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	}, nil)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: s.namespaceID.String(),
		WorkflowID:  execution.WorkflowId,
		RunID:       runID,
	}).Return(&persistence.GetWorkflowExecutionResponse{State: mutableState}, nil)
	s.mockHistoryClient.EXPECT().DeleteWorkflowVisibilityRecord(gomock.Any(), &historyservice.DeleteWorkflowVisibilityRecordRequest{
		NamespaceId: s.namespaceID.String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: execution.WorkflowId,
			RunId:      runID,
		},
	}).Return(&historyservice.DeleteWorkflowVisibilityRecordResponse{}, nil)
	s.mockExecutionMgr.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any(), &persistence.DeleteCurrentWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: s.namespaceID.String(),
		WorkflowID:  execution.WorkflowId,
		RunID:       runID,
	}).Return(nil)
	s.mockExecutionMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), &persistence.DeleteWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: s.namespaceID.String(),
		WorkflowID:  execution.WorkflowId,
		RunID:       runID,
	}).Return(nil)
	s.mockExecutionMgr.EXPECT().DeleteHistoryBranch(gomock.Any(), gomock.Any()).Times(len(mutableState.ExecutionInfo.VersionHistories.Histories))

	_, err = s.handler.DeleteWorkflowExecution(context.Background(), request)
	s.NoError(err)
}

func (s *adminHandlerSuite) TestDeleteWorkflowExecution_LoadMutableStateFailed() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "workflowID",
		RunId:      uuid.New(),
	}

	request := &adminservice.DeleteWorkflowExecutionRequest{
		Namespace: s.namespace.String(),
		Execution: &execution,
	}

	s.mockNamespaceCache.EXPECT().GetNamespaceID(s.namespace).Return(s.namespaceID, nil).AnyTimes()
	s.mockVisibilityMgr.EXPECT().GetName().Return("elasticsearch").AnyTimes()

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, errors.New("some random error"))
	s.mockHistoryClient.EXPECT().DeleteWorkflowVisibilityRecord(gomock.Any(), gomock.Any()).Return(&historyservice.DeleteWorkflowVisibilityRecordResponse{}, nil)
	s.mockExecutionMgr.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	s.mockExecutionMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)

	_, err := s.handler.DeleteWorkflowExecution(context.Background(), request)
	s.NoError(err)
}

func (s *adminHandlerSuite) TestDeleteWorkflowExecution_CassandraVisibilityBackend() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "workflowID",
		RunId:      uuid.New(),
	}

	request := &adminservice.DeleteWorkflowExecutionRequest{
		Namespace: s.namespace.String(),
		Execution: &execution,
	}

	s.mockNamespaceCache.EXPECT().GetNamespaceID(s.namespace).Return(s.namespaceID, nil).AnyTimes()
	s.mockVisibilityMgr.EXPECT().GetName().Return("elasticsearch,cassandra").AnyTimes()

	// test delete open records
	branchToken := []byte("branchToken")
	version := int64(100)
	mutableState := &persistencespb.WorkflowMutableState{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			CreateRequestId: uuid.New(),
			RunId:           execution.RunId,
			State:           enums.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		NextEventId: 12,
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			CompletionEventBatchId: 10,
			StartTime:              timestamp.TimePtr(time.Now()),
			VersionHistories: &historyspb.VersionHistories{
				CurrentVersionHistoryIndex: 0,
				Histories: []*historyspb.VersionHistory{
					{
						BranchToken: branchToken,
						Items: []*historyspb.VersionHistoryItem{
							{EventId: 11, Version: version},
						},
					},
				},
			},
		},
	}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: mutableState}, nil)
	s.mockHistoryClient.EXPECT().DeleteWorkflowVisibilityRecord(gomock.Any(), &historyservice.DeleteWorkflowVisibilityRecordRequest{
		NamespaceId:       s.namespaceID.String(),
		Execution:         &execution,
		WorkflowStartTime: mutableState.ExecutionInfo.StartTime,
	}).Return(&historyservice.DeleteWorkflowVisibilityRecordResponse{}, nil)
	s.mockExecutionMgr.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	s.mockExecutionMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	s.mockExecutionMgr.EXPECT().DeleteHistoryBranch(gomock.Any(), gomock.Any()).Times(len(mutableState.ExecutionInfo.VersionHistories.Histories))

	_, err := s.handler.DeleteWorkflowExecution(context.Background(), request)
	s.NoError(err)

	// test delete close records
	mutableState.ExecutionState.State = enums.WORKFLOW_EXECUTION_STATE_COMPLETED
	mutableState.ExecutionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED

	shardID := common.WorkflowIDToHistoryShard(
		s.namespaceID.String(),
		execution.GetWorkflowId(),
		s.handler.numberOfHistoryShards,
	)
	closeTime := time.Now()
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: mutableState}, nil)
	s.mockExecutionMgr.EXPECT().ReadHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		ShardID:     shardID,
		BranchToken: branchToken,
		MinEventID:  mutableState.ExecutionInfo.CompletionEventBatchId,
		MaxEventID:  mutableState.NextEventId,
		PageSize:    1,
	}).Return(&persistence.ReadHistoryBranchResponse{
		HistoryEvents: []*historypb.HistoryEvent{
			{
				EventId:   10,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
				Version:   version,
				EventTime: timestamp.TimePtr(closeTime.Add(-time.Millisecond)),
			},
			{
				EventId:   11,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
				Version:   version,
				EventTime: timestamp.TimePtr(closeTime),
			},
		},
	}, nil)
	s.mockHistoryClient.EXPECT().DeleteWorkflowVisibilityRecord(gomock.Any(), &historyservice.DeleteWorkflowVisibilityRecordRequest{
		NamespaceId:       s.namespaceID.String(),
		Execution:         &execution,
		WorkflowCloseTime: timestamp.TimePtr(closeTime),
	}).Return(&historyservice.DeleteWorkflowVisibilityRecordResponse{}, nil)
	s.mockExecutionMgr.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	s.mockExecutionMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	s.mockExecutionMgr.EXPECT().DeleteHistoryBranch(gomock.Any(), gomock.Any()).Times(len(mutableState.ExecutionInfo.VersionHistories.Histories))

	_, err = s.handler.DeleteWorkflowExecution(context.Background(), request)
	s.NoError(err)
}
