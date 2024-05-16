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
	"math/rand"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/metadata"

	commonspb "go.temporal.io/server/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	clientmocks "go.temporal.io/server/client"
	historyclient "go.temporal.io/server/client/history"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/resourcetest"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/testing/mocksdk"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/worker/dlq"
)

type (
	adminHandlerSuite struct {
		suite.Suite
		*require.Assertions

		controller         *gomock.Controller
		mockResource       *resourcetest.Test
		mockHistoryClient  *historyservicemock.MockHistoryServiceClient
		mockNamespaceCache *namespace.MockRegistry

		// DEPRECATED
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
	s.mockResource = resourcetest.NewTest(s.controller, primitives.FrontendService)
	s.mockNamespaceCache = s.mockResource.NamespaceCache
	s.mockHistoryClient = s.mockResource.HistoryClient
	s.mockExecutionMgr = s.mockResource.ExecutionMgr
	s.mockClusterMetadataManager = s.mockResource.ClusterMetadataMgr
	s.mockClientFactory = s.mockResource.ClientFactory
	s.mockAdminClient = adminservicemock.NewMockAdminServiceClient(s.controller)
	s.mockMetadata = s.mockResource.ClusterMetadata
	s.mockVisibilityMgr = s.mockResource.VisibilityManager
	s.mockProducer = persistence.NewMockNamespaceReplicationQueue(s.controller)

	persistenceConfig := &config.Persistence{
		NumHistoryShards: 1,
	}

	cfg := &Config{
		NumHistoryShards:                 4,
		AccessHistoryFraction:            dynamicconfig.GetFloatPropertyFn(0.0),
		AdminDeleteAccessHistoryFraction: dynamicconfig.GetFloatPropertyFn(0.0),
	}
	args := NewAdminHandlerArgs{
		persistenceConfig,
		cfg,
		s.mockResource.GetNamespaceReplicationQueue(),
		s.mockProducer,
		s.mockResource.ESClient,
		s.mockResource.GetVisibilityManager(),
		s.mockResource.GetLogger(),
		s.mockResource.GetTaskManager(),
		s.mockResource.GetClusterMetadataManager(),
		s.mockResource.GetMetadataManager(),
		s.mockResource.GetClientFactory(),
		s.mockResource.GetClientBean(),
		s.mockResource.GetHistoryClient(),
		s.mockResource.GetSDKClientFactory(),
		s.mockResource.GetMembershipMonitor(),
		s.mockResource.GetHostInfoProvider(),
		s.mockResource.GetMetricsHandler(),
		s.mockResource.GetNamespaceRegistry(),
		s.mockResource.GetSearchAttributesProvider(),
		s.mockResource.GetSearchAttributesManager(),
		s.mockMetadata,
		health.NewServer(),
		serialization.NewSerializer(),
		clock.NewRealTimeSource(),
		s.mockResource.GetExecutionManager(),
		tasks.NewDefaultTaskCategoryRegistry(),
	}
	s.mockMetadata.EXPECT().GetCurrentClusterName().Return(uuid.New()).AnyTimes()
	s.handler = NewAdminHandler(args)
	s.handler.Start()
}

func (s *adminHandlerSuite) TearDownTest() {
	s.controller.Finish()
	s.handler.Stop()
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
	s.mockVisibilityMgr.EXPECT().GetIndexName().Return("").AnyTimes()
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
	s.mockVisibilityMgr.EXPECT().GetIndexName().Return("random-index-name").AnyTimes()
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
	s.mockResource.SDKClientFactory.EXPECT().GetSystemClient().Return(mockSdkClient).AnyTimes()
	s.mockVisibilityMgr.EXPECT().HasStoreName(elasticsearch.PersistenceName).Return(true).AnyTimes()

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

func (s *adminHandlerSuite) Test_GetSearchAttributes_EmptyIndexName() {
	handler := s.handler
	ctx := context.Background()

	resp, err := handler.GetSearchAttributes(ctx, nil)
	s.Error(err)
	s.Equal(&serviceerror.InvalidArgument{Message: "Request is nil."}, err)
	s.Nil(resp)

	mockSdkClient := mocksdk.NewMockClient(s.controller)
	s.mockResource.SDKClientFactory.EXPECT().GetSystemClient().Return(mockSdkClient).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(s.namespace).Return(s.namespaceEntry, nil).AnyTimes()

	// Elasticsearch is not configured
	s.mockVisibilityMgr.EXPECT().HasStoreName(elasticsearch.PersistenceName).Return(true).AnyTimes()
	s.mockVisibilityMgr.EXPECT().GetIndexName().Return("").AnyTimes()
	mockSdkClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), "temporal-sys-add-search-attributes-workflow", "").Return(
		&workflowservice.DescribeWorkflowExecutionResponse{}, nil)
	s.mockResource.ESClient.EXPECT().GetMapping(gomock.Any(), "").Return(map[string]string{"col": "type"}, nil)
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("", true).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()

	resp, err = handler.GetSearchAttributes(ctx, &adminservice.GetSearchAttributesRequest{Namespace: s.namespace.String()})
	s.NoError(err)
	s.NotNil(resp)
}

func (s *adminHandlerSuite) Test_GetSearchAttributes_NonEmptyIndexName() {
	handler := s.handler
	ctx := context.Background()

	mockSdkClient := mocksdk.NewMockClient(s.controller)
	s.mockResource.SDKClientFactory.EXPECT().GetSystemClient().Return(mockSdkClient).AnyTimes()

	// Configure Elasticsearch: add advanced visibility store config with index name.
	s.mockVisibilityMgr.EXPECT().HasStoreName(elasticsearch.PersistenceName).Return(true).AnyTimes()
	s.mockVisibilityMgr.EXPECT().GetIndexName().Return("random-index-name").AnyTimes()

	mockSdkClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), "temporal-sys-add-search-attributes-workflow", "").Return(
		&workflowservice.DescribeWorkflowExecutionResponse{}, nil)
	s.mockResource.ESClient.EXPECT().GetMapping(gomock.Any(), "random-index-name").Return(map[string]string{"col": "type"}, nil)
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("random-index-name", true).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()
	resp, err := handler.GetSearchAttributes(ctx, &adminservice.GetSearchAttributesRequest{})
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
	resp, err = handler.GetSearchAttributes(ctx, &adminservice.GetSearchAttributesRequest{Namespace: s.namespace.String()})
	s.Error(err)
	s.Nil(resp)
}

func (s *adminHandlerSuite) Test_RemoveSearchAttributes_EmptyIndexName() {
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
	s.mockVisibilityMgr.EXPECT().HasStoreName(elasticsearch.PersistenceName).Return(true).AnyTimes()
	s.mockVisibilityMgr.EXPECT().GetIndexName().Return("").AnyTimes()
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("", true).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()
	testCases2 := []test{
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
	for _, testCase := range testCases2 {
		s.T().Run(testCase.Name, func(t *testing.T) {
			resp, err := handler.RemoveSearchAttributes(ctx, testCase.Request)
			s.Equal(testCase.Expected, err)
			s.Nil(resp)
		})
	}
}

func (s *adminHandlerSuite) Test_RemoveSearchAttributes_NonEmptyIndexName() {
	handler := s.handler
	ctx := context.Background()

	type test struct {
		Name     string
		Request  *adminservice.RemoveSearchAttributesRequest
		Expected error
	}
	testCases := []test{
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

	// Configure Elasticsearch: add advanced visibility store config with index name.
	s.mockVisibilityMgr.EXPECT().HasStoreName(elasticsearch.PersistenceName).Return(true).AnyTimes()
	s.mockVisibilityMgr.EXPECT().GetIndexName().Return("random-index-name").AnyTimes()
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("random-index-name", true).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()
	for _, testCase := range testCases {
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
	var FrontendHttpAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()
	var recordVersion int64 = 5

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		&persistence.GetClusterMetadataResponse{
			Version: recordVersion,
		}, nil)
	s.mockClusterMetadataManager.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: &persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			ClusterId:                clusterId,
			ClusterAddress:           rpcAddress,
			HttpAddress:              FrontendHttpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: recordVersion,
	}).Return(true, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{
		FrontendAddress:     rpcAddress,
		FrontendHttpAddress: FrontendHttpAddress,
	})
	s.NoError(err)
}

func (s *adminHandlerSuite) Test_AddOrUpdateRemoteCluster_RecordNotFound_Success() {
	var rpcAddress = uuid.New()
	var FrontendHttpAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		nil,
		serviceerror.NewNotFound("expected empty result"),
	)
	s.mockClusterMetadataManager.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: &persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			ClusterId:                clusterId,
			ClusterAddress:           rpcAddress,
			HttpAddress:              FrontendHttpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: 0,
	}).Return(true, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{
		FrontendAddress:     rpcAddress,
		FrontendHttpAddress: FrontendHttpAddress,
	})
	s.NoError(err)
}

func (s *adminHandlerSuite) Test_AddOrUpdateRemoteCluster_ValidationError_ClusterNameConflict() {
	var rpcAddress = uuid.New()
	var clusterId = uuid.New()

	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
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
	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
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

func (s *adminHandlerSuite) Test_AddOrUpdateRemoteCluster_ValidationError_ShardCount_Invalid() {
	var rpcAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        5,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *adminHandlerSuite) Test_AddOrUpdateRemoteCluster_ShardCount_Multiple() {
	var rpcAddress = uuid.New()
	var FrontendHttpAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()
	var recordVersion int64 = 5

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        16,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		&persistence.GetClusterMetadataResponse{
			Version: recordVersion,
		}, nil)
	s.mockClusterMetadataManager.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: &persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        16,
			ClusterId:                clusterId,
			ClusterAddress:           rpcAddress,
			HttpAddress:              FrontendHttpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: recordVersion,
	}).Return(true, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{
		FrontendAddress:     rpcAddress,
		FrontendHttpAddress: FrontendHttpAddress,
	})
	s.NoError(err)
}

func (s *adminHandlerSuite) Test_AddOrUpdateRemoteCluster_ValidationError_GlobalNamespaceDisabled() {
	var rpcAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
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
	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
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

	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
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
	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
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
	var FrontendHttpAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		nil,
		serviceerror.NewNotFound("expected empty result"),
	)
	s.mockClusterMetadataManager.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: &persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			ClusterId:                clusterId,
			ClusterAddress:           rpcAddress,
			HttpAddress:              FrontendHttpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: 0,
	}).Return(false, fmt.Errorf("test error"))
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{
		FrontendAddress:     rpcAddress,
		FrontendHttpAddress: FrontendHttpAddress,
	})
	s.Error(err)
}

func (s *adminHandlerSuite) Test_AddOrUpdateRemoteCluster_SaveClusterMetadata_NotApplied_Error() {
	var rpcAddress = uuid.New()
	var FrontendHttpAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		nil,
		serviceerror.NewNotFound("expected empty result"),
	)
	s.mockClusterMetadataManager.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: &persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			ClusterId:                clusterId,
			ClusterAddress:           rpcAddress,
			HttpAddress:              FrontendHttpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: 0,
	}).Return(false, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &adminservice.AddOrUpdateRemoteClusterRequest{
		FrontendAddress:     rpcAddress,
		FrontendHttpAddress: FrontendHttpAddress,
	})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *adminHandlerSuite) Test_DescribeCluster_CurrentCluster_Success() {
	var clusterId = uuid.New()
	clusterName := s.mockMetadata.GetCurrentClusterName()
	s.mockResource.HostInfoProvider.EXPECT().HostInfo().Return(membership.NewHostInfoFromAddress("test"))
	s.mockResource.MembershipMonitor.EXPECT().GetReachableMembers().Return(nil, nil)
	s.mockResource.HistoryServiceResolver.EXPECT().Members().Return([]membership.HostInfo{})
	s.mockResource.HistoryServiceResolver.EXPECT().MemberCount().Return(0)
	s.mockResource.FrontendServiceResolver.EXPECT().Members().Return([]membership.HostInfo{})
	s.mockResource.FrontendServiceResolver.EXPECT().MemberCount().Return(0)
	s.mockResource.MatchingServiceResolver.EXPECT().Members().Return([]membership.HostInfo{})
	s.mockResource.MatchingServiceResolver.EXPECT().MemberCount().Return(0)
	s.mockResource.WorkerServiceResolver.EXPECT().Members().Return([]membership.HostInfo{})
	s.mockResource.WorkerServiceResolver.EXPECT().MemberCount().Return(0)
	s.mockResource.ExecutionMgr.EXPECT().GetName().Return("")
	s.mockVisibilityMgr.EXPECT().GetStoreNames().Return([]string{elasticsearch.PersistenceName})
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		&persistence.GetClusterMetadataResponse{
			ClusterMetadata: &persistencespb.ClusterMetadata{
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

	s.mockResource.HostInfoProvider.EXPECT().HostInfo().Return(membership.NewHostInfoFromAddress("test"))
	s.mockResource.MembershipMonitor.EXPECT().GetReachableMembers().Return(nil, nil)
	s.mockResource.HistoryServiceResolver.EXPECT().Members().Return([]membership.HostInfo{})
	s.mockResource.HistoryServiceResolver.EXPECT().MemberCount().Return(0)
	s.mockResource.FrontendServiceResolver.EXPECT().Members().Return([]membership.HostInfo{})
	s.mockResource.FrontendServiceResolver.EXPECT().MemberCount().Return(0)
	s.mockResource.MatchingServiceResolver.EXPECT().Members().Return([]membership.HostInfo{})
	s.mockResource.MatchingServiceResolver.EXPECT().MemberCount().Return(0)
	s.mockResource.WorkerServiceResolver.EXPECT().Members().Return([]membership.HostInfo{})
	s.mockResource.WorkerServiceResolver.EXPECT().MemberCount().Return(0)
	s.mockResource.ExecutionMgr.EXPECT().GetName().Return("")
	s.mockVisibilityMgr.EXPECT().GetStoreNames().Return([]string{elasticsearch.PersistenceName})
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		&persistence.GetClusterMetadataResponse{
			ClusterMetadata: &persistencespb.ClusterMetadata{
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
					ClusterMetadata: &persistencespb.ClusterMetadata{ClusterName: "test"},
				},
			}}, nil)

	resp, err := s.handler.ListClusters(context.Background(), &adminservice.ListClustersRequest{
		PageSize: pageSize,
	})
	s.NoError(err)
	s.Equal(1, len(resp.Clusters))
	s.Equal(0, len(resp.GetNextPageToken()))
}

func (s *adminHandlerSuite) TestStreamWorkflowReplicationMessages_ClientToServerBroken() {
	clientClusterShardID := historyclient.ClusterShardID{
		ClusterID: rand.Int31(),
		ShardID:   rand.Int31(),
	}
	serverClusterShardID := historyclient.ClusterShardID{
		ClusterID: rand.Int31(),
		ShardID:   rand.Int31(),
	}
	clusterShardMD := historyclient.EncodeClusterShardMD(
		clientClusterShardID,
		serverClusterShardID,
	)
	ctx := metadata.NewIncomingContext(context.Background(), clusterShardMD)
	clientCluster := adminservicemock.NewMockAdminService_StreamWorkflowReplicationMessagesServer(s.controller)
	clientCluster.EXPECT().Context().Return(ctx).AnyTimes()
	serverCluster := historyservicemock.NewMockHistoryService_StreamWorkflowReplicationMessagesClient(s.controller)
	s.mockHistoryClient.EXPECT().StreamWorkflowReplicationMessages(ctx).Return(serverCluster, nil)

	waitGroupStart := sync.WaitGroup{}
	waitGroupStart.Add(2)
	waitGroupEnd := sync.WaitGroup{}
	waitGroupEnd.Add(2)
	channel := make(chan struct{})

	clientCluster.EXPECT().Recv().DoAndReturn(func() (*adminservice.StreamWorkflowReplicationMessagesRequest, error) {
		waitGroupStart.Done()
		waitGroupStart.Wait()

		defer waitGroupEnd.Done()
		return nil, serviceerror.NewUnavailable("random error")
	})
	serverCluster.EXPECT().Recv().DoAndReturn(func() (*historyservice.StreamWorkflowReplicationMessagesResponse, error) {
		waitGroupStart.Done()
		waitGroupStart.Wait()

		defer waitGroupEnd.Done()
		<-channel
		return nil, serviceerror.NewUnavailable("random error")
	})
	_ = s.handler.StreamWorkflowReplicationMessages(clientCluster)
	close(channel)
	waitGroupEnd.Wait()
}

func (s *adminHandlerSuite) TestStreamWorkflowReplicationMessages_ServerToClientBroken() {
	clientClusterShardID := historyclient.ClusterShardID{
		ClusterID: rand.Int31(),
		ShardID:   rand.Int31(),
	}
	serverClusterShardID := historyclient.ClusterShardID{
		ClusterID: rand.Int31(),
		ShardID:   rand.Int31(),
	}
	clusterShardMD := historyclient.EncodeClusterShardMD(
		clientClusterShardID,
		serverClusterShardID,
	)
	ctx := metadata.NewIncomingContext(context.Background(), clusterShardMD)
	clientCluster := adminservicemock.NewMockAdminService_StreamWorkflowReplicationMessagesServer(s.controller)
	clientCluster.EXPECT().Context().Return(ctx).AnyTimes()
	serverCluster := historyservicemock.NewMockHistoryService_StreamWorkflowReplicationMessagesClient(s.controller)
	s.mockHistoryClient.EXPECT().StreamWorkflowReplicationMessages(ctx).Return(serverCluster, nil)

	waitGroupStart := sync.WaitGroup{}
	waitGroupStart.Add(2)
	waitGroupEnd := sync.WaitGroup{}
	waitGroupEnd.Add(2)
	channel := make(chan struct{})

	clientCluster.EXPECT().Recv().DoAndReturn(func() (*adminservice.StreamWorkflowReplicationMessagesRequest, error) {
		waitGroupStart.Done()
		waitGroupStart.Wait()

		defer waitGroupEnd.Done()
		<-channel
		return nil, serviceerror.NewUnavailable("random error")
	})
	serverCluster.EXPECT().Recv().DoAndReturn(func() (*historyservice.StreamWorkflowReplicationMessagesResponse, error) {
		waitGroupStart.Done()
		waitGroupStart.Wait()

		defer waitGroupEnd.Done()
		return nil, serviceerror.NewUnavailable("random error")
	})
	_ = s.handler.StreamWorkflowReplicationMessages(clientCluster)
	close(channel)
	waitGroupEnd.Wait()
}

func (s *adminHandlerSuite) TestGetNamespace_WithIDSuccess() {
	namespaceID := "someId"
	nsResponse := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			FailoverVersion: 1,
			Info: &persistencespb.NamespaceInfo{
				Id:    namespaceID,
				Name:  "another random namespace name",
				State: enumspb.NAMESPACE_STATE_DELETED,
				Data:  make(map[string]string)},
			Config: &persistencespb.NamespaceConfig{
				Retention: timestamp.DurationFromDays(2),
				BadBinaries: &namespacepb.BadBinaries{
					Binaries: map[string]*namespacepb.BadBinaryInfo{},
				}},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestAlternativeClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			FailoverNotificationVersion: 0,
		},
	}
	s.mockResource.MetadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		ID: namespaceID,
	}).Return(nsResponse, nil)
	resp, err := s.handler.GetNamespace(context.Background(), &adminservice.GetNamespaceRequest{
		Attributes: &adminservice.GetNamespaceRequest_Id{
			Id: namespaceID,
		},
	})
	s.NoError(err)
	s.Equal(namespaceID, resp.GetInfo().GetId())
}

func (s *adminHandlerSuite) TestGetNamespace_WithNameSuccess() {
	namespaceName := "some name"
	namespaceId := "some id"
	nsResponse := &persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			FailoverVersion: 1,
			Info: &persistencespb.NamespaceInfo{
				Id:    namespaceId,
				Name:  namespaceName,
				State: enumspb.NAMESPACE_STATE_DELETED,
				Data:  make(map[string]string)},
			Config: &persistencespb.NamespaceConfig{
				Retention: timestamp.DurationFromDays(2),
				BadBinaries: &namespacepb.BadBinaries{
					Binaries: map[string]*namespacepb.BadBinaryInfo{},
				}},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestAlternativeClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			FailoverNotificationVersion: 0,
		},
	}
	s.mockResource.MetadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		Name: namespaceName,
	}).Return(nsResponse, nil)
	resp, err := s.handler.GetNamespace(context.Background(), &adminservice.GetNamespaceRequest{
		Attributes: &adminservice.GetNamespaceRequest_Namespace{
			Namespace: namespaceName,
		},
	})
	s.NoError(err)
	s.Equal(namespaceId, resp.GetInfo().GetId())
	s.Equal(namespaceName, resp.GetInfo().GetName())
	s.Equal(cluster.TestAlternativeClusterName, resp.GetReplicationConfig().GetActiveClusterName())
}

func (s *adminHandlerSuite) TestGetNamespace_EmptyRequest() {
	v := &adminservice.GetNamespaceRequest{}
	_, err := s.handler.GetNamespace(context.Background(), v)
	s.Equal(errRequestNotSet, err)
}

func (s *adminHandlerSuite) TestGetDLQTasks() {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{
			name: "success",
			err:  nil,
		},
		{
			name: "failed to get dlq tasks",
			err:  serviceerror.NewNotFound("failed to get dlq tasks"),
		},
	} {
		s.Run(tc.name, func() {
			blob := &commonpb.DataBlob{}
			expectation := s.mockHistoryClient.EXPECT().GetDLQTasks(gomock.Any(), &historyservice.GetDLQTasksRequest{
				DlqKey: &commonspb.HistoryDLQKey{
					TaskCategory:  int32(tasks.CategoryTransfer.ID()),
					SourceCluster: "test-source-cluster",
					TargetCluster: "test-target-cluster",
				},
				PageSize:      1,
				NextPageToken: []byte{13},
			})
			if tc.err != nil {
				expectation.Return(nil, tc.err)
			} else {
				expectation.Return(&historyservice.GetDLQTasksResponse{
					DlqTasks: []*commonspb.HistoryDLQTask{
						{
							Metadata: &commonspb.HistoryDLQTaskMetadata{
								MessageId: 21,
							},
							Payload: &commonspb.HistoryTask{
								ShardId: 34,
								Blob:    blob,
							},
						},
					},
					NextPageToken: []byte{55},
				}, nil)
			}
			response, err := s.handler.GetDLQTasks(context.Background(), &adminservice.GetDLQTasksRequest{
				DlqKey: &commonspb.HistoryDLQKey{
					TaskCategory:  int32(tasks.CategoryTransfer.ID()),
					SourceCluster: "test-source-cluster",
					TargetCluster: "test-target-cluster",
				},
				PageSize:      1,
				NextPageToken: []byte{13},
			})
			if tc.err != nil {
				s.ErrorIs(err, tc.err)
				return
			}
			s.NoError(err)
			s.Equal(&adminservice.GetDLQTasksResponse{
				DlqTasks: []*commonspb.HistoryDLQTask{
					{
						Metadata: &commonspb.HistoryDLQTaskMetadata{
							MessageId: 21,
						},
						Payload: &commonspb.HistoryTask{
							ShardId: 34,
							Blob:    blob,
						},
					},
				},
				NextPageToken: []byte{55},
			}, response)
		})
	}
}

func (s *adminHandlerSuite) TestPurgeDLQTasks() {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{
			name: "Success",
			err:  nil,
		},
		{
			name: "WorkflowExecutionFailed",
			err:  serviceerror.NewNotFound("example sdk worfklow start failure"),
		},
	} {
		s.Run(tc.name, func() {
			mockSdkClient := mocksdk.NewMockClient(s.controller)
			s.mockResource.SDKClientFactory.EXPECT().GetSystemClient().Return(mockSdkClient)
			expectation := mockSdkClient.EXPECT().ExecuteWorkflow(
				gomock.Any(),
				gomock.Any(),
				dlq.WorkflowName,
				dlq.WorkflowParams{
					WorkflowType: dlq.WorkflowTypeDelete,
					DeleteParams: dlq.DeleteParams{
						Key: dlq.Key{
							TaskCategoryID: tasks.CategoryTransfer.ID(),
							SourceCluster:  "test-source-cluster",
							TargetCluster:  "test-target-cluster",
						},
						MaxMessageID: 42,
					},
				},
			)
			if tc.err != nil {
				expectation.Return(nil, tc.err)
			} else {
				run := mocksdk.NewMockWorkflowRun(s.controller)
				run.EXPECT().GetRunID().Return("test-run-id")
				expectation.Return(run, nil)
			}
			response, err := s.handler.PurgeDLQTasks(context.Background(), &adminservice.PurgeDLQTasksRequest{
				DlqKey: &commonspb.HistoryDLQKey{
					TaskCategory:  int32(tasks.CategoryTransfer.ID()),
					SourceCluster: "test-source-cluster",
					TargetCluster: "test-target-cluster",
				},
				InclusiveMaxTaskMetadata: &commonspb.HistoryDLQTaskMetadata{
					MessageId: 42,
				},
			})
			if tc.err != nil {
				s.ErrorIs(err, tc.err)
				return
			}
			s.NoError(err)
			s.NotNil(response)
			var token adminservice.DLQJobToken
			err = token.Unmarshal(response.JobToken)
			s.NoError(err)
			s.Equal("manage-dlq-tasks-1_test-source-cluster_test-target-cluster_aG2oua8T", token.WorkflowId)
			s.Equal("test-run-id", token.RunId)
		})
	}
}

func (s *adminHandlerSuite) TestPurgeDLQTasks_ClusterNotSet() {
	_, err := s.handler.PurgeDLQTasks(context.Background(), &adminservice.PurgeDLQTasksRequest{
		DlqKey: &commonspb.HistoryDLQKey{
			TaskCategory:  1,
			SourceCluster: "",
			TargetCluster: "test-target-cluster",
		},
		InclusiveMaxTaskMetadata: &commonspb.HistoryDLQTaskMetadata{
			MessageId: 42,
		},
	})
	s.Error(err)
	s.Equal(codes.InvalidArgument, serviceerror.ToStatus(err).Code())
	s.ErrorContains(err, errSourceClusterNotSet.Error())
}

func (s *adminHandlerSuite) TestDescribeDLQJob() {
	workflowID := "test-workflow-id"
	runID := "test-run-id"
	defaultMergeQueryResponse := dlq.ProgressQueryResponse{
		MaxMessageIDToProcess:  0,
		LastProcessedMessageID: 0,
		WorkflowType:           dlq.WorkflowTypeMerge,
		DlqKey: dlq.Key{
			TaskCategoryID: 1,
			SourceCluster:  "test-source-cluster",
			TargetCluster:  "test-target-cluster",
		},
	}
	defaultPurgeQueryResponse := dlq.ProgressQueryResponse{
		MaxMessageIDToProcess:  0,
		LastProcessedMessageID: 0,
		WorkflowType:           dlq.WorkflowTypeDelete,
		DlqKey: dlq.Key{
			TaskCategoryID: 1,
			SourceCluster:  "test-source-cluster",
			TargetCluster:  "test-target-cluster",
		},
	}
	defaultWorkflowExecution := workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
	}
	for _, tc := range []struct {
		name                  string
		err                   error
		progressQueryResponse dlq.ProgressQueryResponse
		workflowExecution     workflowservice.DescribeWorkflowExecutionResponse
		expectedResponse      adminservice.DescribeDLQJobResponse
	}{
		{
			name:                  "MergeRunning",
			err:                   nil,
			progressQueryResponse: defaultMergeQueryResponse,
			workflowExecution:     defaultWorkflowExecution,
			expectedResponse: adminservice.DescribeDLQJobResponse{
				DlqKey: &commonspb.HistoryDLQKey{
					TaskCategory:  1,
					SourceCluster: "test-source-cluster",
					TargetCluster: "test-target-cluster",
				},
				OperationType:          enumsspb.DLQ_OPERATION_TYPE_MERGE,
				OperationState:         enumsspb.DLQ_OPERATION_STATE_RUNNING,
				MaxMessageId:           0,
				LastProcessedMessageId: 0,
			},
		},
		{
			name:                  "MergeFinished",
			err:                   nil,
			progressQueryResponse: defaultMergeQueryResponse,
			workflowExecution: workflowservice.DescribeWorkflowExecutionResponse{
				WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
					Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
				},
			},
			expectedResponse: adminservice.DescribeDLQJobResponse{
				DlqKey: &commonspb.HistoryDLQKey{
					TaskCategory:  1,
					SourceCluster: "test-source-cluster",
					TargetCluster: "test-target-cluster",
				},
				OperationType:          enumsspb.DLQ_OPERATION_TYPE_MERGE,
				OperationState:         enumsspb.DLQ_OPERATION_STATE_COMPLETED,
				MaxMessageId:           0,
				LastProcessedMessageId: 0,
			},
		},
		{
			name:                  "MergeFailed",
			err:                   nil,
			progressQueryResponse: defaultMergeQueryResponse,
			workflowExecution: workflowservice.DescribeWorkflowExecutionResponse{
				WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
					Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
				},
			},
			expectedResponse: adminservice.DescribeDLQJobResponse{
				DlqKey: &commonspb.HistoryDLQKey{
					TaskCategory:  1,
					SourceCluster: "test-source-cluster",
					TargetCluster: "test-target-cluster",
				},
				OperationType:          enumsspb.DLQ_OPERATION_TYPE_MERGE,
				OperationState:         enumsspb.DLQ_OPERATION_STATE_FAILED,
				MaxMessageId:           0,
				LastProcessedMessageId: 0,
			},
		},
		{
			name:                  "DeleteRunning",
			err:                   nil,
			progressQueryResponse: defaultPurgeQueryResponse,
			workflowExecution:     defaultWorkflowExecution,
			expectedResponse: adminservice.DescribeDLQJobResponse{
				DlqKey: &commonspb.HistoryDLQKey{
					TaskCategory:  1,
					SourceCluster: "test-source-cluster",
					TargetCluster: "test-target-cluster",
				},
				OperationType:          enumsspb.DLQ_OPERATION_TYPE_PURGE,
				OperationState:         enumsspb.DLQ_OPERATION_STATE_RUNNING,
				MaxMessageId:           0,
				LastProcessedMessageId: 0,
			},
		},
	} {
		s.Run(tc.name, func() {
			jobToken := adminservice.DLQJobToken{
				WorkflowId: workflowID,
				RunId:      runID,
			}
			mockSdkClient := mocksdk.NewMockClient(s.controller)
			s.mockResource.SDKClientFactory.EXPECT().GetSystemClient().Return(mockSdkClient)
			describeExpectation := mockSdkClient.EXPECT().DescribeWorkflowExecution(
				gomock.Any(),
				workflowID,
				runID,
			)
			queryExpectation := mockSdkClient.EXPECT().QueryWorkflow(
				gomock.Any(),
				workflowID,
				runID,
				dlq.QueryTypeProgress,
			)
			mockValue := mocksdk.NewMockEncodedValue(s.controller)
			mockValue.EXPECT().Get(gomock.Any()).Do(func(result interface{}) {
				*(result.(*dlq.ProgressQueryResponse)) = tc.progressQueryResponse
			})
			queryExpectation.Return(mockValue, nil)
			if tc.err != nil {
				describeExpectation.Return(nil, tc.err)
			} else {
				describeExpectation.Return(&tc.workflowExecution, nil)
			}
			jobTokenBytes, _ := jobToken.Marshal()
			response, err := s.handler.DescribeDLQJob(context.Background(), &adminservice.DescribeDLQJobRequest{
				JobToken: jobTokenBytes,
			})
			if tc.err != nil {
				s.ErrorIs(err, tc.err)
				return
			}
			s.NoError(err)
			s.NotNil(response)
			s.EqualValues(tc.expectedResponse, *response)
		})
	}
}

func (s *adminHandlerSuite) TestDescribeDLQJob_InvalidJobToken() {
	_, err := s.handler.DescribeDLQJob(context.Background(), &adminservice.DescribeDLQJobRequest{JobToken: []byte("invalid_token")})
	s.Error(err)
	s.ErrorContains(err, "Invalid DLQ job token")

}

func (s *adminHandlerSuite) TestCancelDLQJob() {
	for _, tc := range []struct {
		name              string
		terminateErr      error
		describeErr       error
		workflowExecution workflowservice.DescribeWorkflowExecutionResponse
		terminateCalls    int
		expectedCancelled bool
	}{
		{
			name:         "SuccessForRunningWorkflow",
			terminateErr: nil,
			describeErr:  nil,
			workflowExecution: workflowservice.DescribeWorkflowExecutionResponse{
				WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
					Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				},
			},
			terminateCalls:    1,
			expectedCancelled: true,
		},
		{
			name:         "SuccessForCompletedWorkflow",
			terminateErr: nil,
			describeErr:  nil,
			workflowExecution: workflowservice.DescribeWorkflowExecutionResponse{
				WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
					Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
				},
			},
			terminateCalls:    0,
			expectedCancelled: false,
		},
		{
			name:         "TerminateWorkflowFailed",
			terminateErr: serviceerror.NewNotFound("example sdk terminate workflow failure"),
			describeErr:  nil,
			workflowExecution: workflowservice.DescribeWorkflowExecutionResponse{
				WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
					Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				},
			},
			terminateCalls:    1,
			expectedCancelled: false,
		},
		{
			name:         "DescribeWorkflowFailed",
			terminateErr: nil,
			describeErr:  serviceerror.NewNotFound("example sdk describe workflow failure"),
			workflowExecution: workflowservice.DescribeWorkflowExecutionResponse{
				WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
					Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				},
			},
			terminateCalls:    0,
			expectedCancelled: false,
		},
	} {
		s.Run(tc.name, func() {
			workflowID := "test-workflow-id"
			runID := "test-run-id"
			jobToken := adminservice.DLQJobToken{
				WorkflowId: workflowID,
				RunId:      runID,
			}
			mockSdkClient := mocksdk.NewMockClient(s.controller)
			s.mockResource.SDKClientFactory.EXPECT().GetSystemClient().Return(mockSdkClient)
			describeExpectation := mockSdkClient.EXPECT().DescribeWorkflowExecution(
				gomock.Any(),
				workflowID,
				runID,
			)
			terminateExpectation := mockSdkClient.EXPECT().TerminateWorkflow(
				gomock.Any(),
				workflowID,
				runID,
				"test-reason",
			)
			terminateExpectation.Return(tc.terminateErr).Times(tc.terminateCalls)
			if tc.describeErr != nil {
				describeExpectation.Return(nil, tc.describeErr)
			} else {
				describeExpectation.Return(&tc.workflowExecution, nil)
			}
			jobTokenBytes, _ := jobToken.Marshal()
			response, err := s.handler.CancelDLQJob(context.Background(), &adminservice.CancelDLQJobRequest{
				JobToken: jobTokenBytes,
				Reason:   "test-reason",
			})
			if tc.describeErr != nil {
				s.ErrorIs(err, tc.describeErr)
				return
			}
			if tc.terminateErr != nil {
				s.ErrorIs(err, tc.terminateErr)
				return
			}
			s.NoError(err)
			s.NotNil(response)
			s.Equal(tc.expectedCancelled, response.Canceled)
		})
	}
}

func (s *adminHandlerSuite) TestCancelDLQJob_InvalidJobToken() {
	_, err := s.handler.CancelDLQJob(context.Background(), &adminservice.CancelDLQJobRequest{JobToken: []byte("invalid_token"), Reason: "test-reason"})
	s.Error(err)
	s.ErrorContains(err, "Invalid DLQ job token")
}

func (s *adminHandlerSuite) TestAddDLQTasks_Ok() {
	s.mockHistoryClient.EXPECT().AddTasks(gomock.Any(), &historyservice.AddTasksRequest{
		ShardId: 13,
		Tasks: []*historyservice.AddTasksRequest_Task{
			{
				CategoryId: 21,
				Blob: &commonpb.DataBlob{
					EncodingType: enumspb.ENCODING_TYPE_PROTO3,
					Data:         []byte("test-data"),
				},
			},
		},
	}).Return(nil, nil)
	_, err := s.handler.AddTasks(context.Background(), &adminservice.AddTasksRequest{
		ShardId: 13,
		Tasks: []*adminservice.AddTasksRequest_Task{
			{
				CategoryId: 21,
				Blob: &commonpb.DataBlob{
					EncodingType: enumspb.ENCODING_TYPE_PROTO3,
					Data:         []byte("test-data"),
				},
			},
		},
	})
	s.NoError(err)
}

func (s *adminHandlerSuite) TestAddDLQTasks_Err() {
	s.mockHistoryClient.EXPECT().AddTasks(gomock.Any(), gomock.Any()).Return(nil, assert.AnError)
	_, err := s.handler.AddTasks(context.Background(), &adminservice.AddTasksRequest{})
	s.ErrorIs(err, assert.AnError)
}

func (s *adminHandlerSuite) TestListQueues_Ok() {
	s.mockHistoryClient.EXPECT().ListQueues(gomock.Any(), &historyservice.ListQueuesRequest{
		QueueType:     int32(persistence.QueueTypeHistoryDLQ),
		PageSize:      0,
		NextPageToken: nil,
	}).Return(&historyservice.ListQueuesResponse{
		Queues: []*historyservice.ListQueuesResponse_QueueInfo{
			{
				QueueName:    "testQueue",
				MessageCount: 100,
			},
		},
	}, nil)
	resp, err := s.handler.ListQueues(context.Background(), &adminservice.ListQueuesRequest{
		QueueType:     int32(persistence.QueueTypeHistoryDLQ),
		PageSize:      0,
		NextPageToken: nil,
	})
	s.NoError(err)
	s.Equal("testQueue", resp.Queues[0].QueueName)
	s.Equal(int64(100), resp.Queues[0].MessageCount)

}

func (s *adminHandlerSuite) TestListQueues_Err() {
	s.mockHistoryClient.EXPECT().ListQueues(gomock.Any(), gomock.Any()).Return(nil, assert.AnError)
	_, err := s.handler.ListQueues(context.Background(), &adminservice.ListQueuesRequest{})
	s.ErrorIs(err, assert.AnError)
}
