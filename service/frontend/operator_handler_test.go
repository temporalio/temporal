package frontend

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resourcetest"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/testing/mocksdk"
	"go.temporal.io/server/service/worker/deletenamespace"
	delnserrors "go.temporal.io/server/service/worker/deletenamespace/errors"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/health"
)

var (
	testNamespace = "test-namespace"
	testIndexName = "test-index-name"
)

type (
	operatorHandlerSuite struct {
		suite.Suite

		controller   *gomock.Controller
		mockResource *resourcetest.Test

		handler *OperatorHandlerImpl
	}
)

func TestOperatorHandlerSuite(t *testing.T) {
	s := new(operatorHandlerSuite)
	suite.Run(t, s)
}

func (s *operatorHandlerSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.mockResource = resourcetest.NewTest(s.controller, primitives.FrontendService)
	s.mockResource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(uuid.NewString()).AnyTimes()

	endpointClient := newNexusEndpointClient(
		newNexusEndpointClientConfig(dynamicconfig.NewNoopCollection()),
		s.mockResource.NamespaceCache,
		s.mockResource.MatchingClient,
		s.mockResource.NexusEndpointManager,
		s.mockResource.Logger,
	)

	args := NewOperatorHandlerImplArgs{
		config:                 &Config{NumHistoryShards: 4},
		Logger:                 s.mockResource.Logger,
		sdkClientFactory:       s.mockResource.GetSDKClientFactory(),
		MetricsHandler:         s.mockResource.GetMetricsHandler(),
		VisibilityMgr:          s.mockResource.GetVisibilityManager(),
		SaManager:              s.mockResource.GetSearchAttributesManager(),
		healthServer:           health.NewServer(),
		historyClient:          s.mockResource.GetHistoryClient(),
		clusterMetadataManager: s.mockResource.GetClusterMetadataManager(),
		clusterMetadata:        s.mockResource.GetClusterMetadata(),
		clientFactory:          s.mockResource.GetClientFactory(),
		namespaceRegistry:      s.mockResource.NamespaceCache,
		nexusEndpointClient:    endpointClient,
	}
	s.handler = NewOperatorHandlerImpl(args)
	s.handler.Start()
}

func (s *operatorHandlerSuite) TearDownTest() {
	s.controller.Finish()
	s.handler.Stop()
}

func (s *operatorHandlerSuite) Run(name string, subtest func()) bool {
	oldController := s.controller
	oldMockResource := s.mockResource
	oldHandler := s.handler

	return s.Suite.Run(name, func() {
		s.SetupTest()
		defer func() {
			s.TearDownTest()
			s.controller = oldController
			s.mockResource = oldMockResource
			s.handler = oldHandler
		}()

		subtest()
	})
}

func (s *operatorHandlerSuite) Test_AddSearchAttributes() {
	ctx := context.Background()
	testCases := []struct {
		name                      string
		request                   *operatorservice.AddSearchAttributesRequest
		storeNames                []string
		indexName                 string
		getSearchAttributesCalled bool
		getSearchAttributesErr    error
		addInternalSuccess        bool
		expectedErrMsg            string
	}{
		{
			name:           "fail: request is nil",
			request:        nil,
			expectedErrMsg: errRequestNotSet.Error(),
		},
		{
			name:           "fail: search attributes not set in request",
			request:        &operatorservice.AddSearchAttributesRequest{},
			expectedErrMsg: errSearchAttributesNotSet.Error(),
		},
		{
			name: "fail: cannot add reserved name search attribute",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"WorkflowId": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
			},
			expectedErrMsg: fmt.Sprintf(errSearchAttributeIsReservedMessage, "WorkflowId"),
		},
		{
			name: "fail: unknown search attribute type",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.IndexedValueType(-1),
				},
			},
			expectedErrMsg: fmt.Sprintf(errUnknownSearchAttributeTypeMessage, -1),
		},
		{
			name: "fail: cannot get existing search attributes",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
			},
			storeNames:                []string{elasticsearch.PersistenceName},
			getSearchAttributesCalled: true,
			getSearchAttributesErr:    errors.New("mock error get search attributes"),
			expectedErrMsg:            "mock error get search attributes",
		},
		{
			name: "success",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
				Namespace: testNamespace,
			},
			storeNames:                []string{elasticsearch.PersistenceName},
			indexName:                 testIndexName,
			getSearchAttributesCalled: true,
			addInternalSuccess:        true,
			expectedErrMsg:            "",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			s.mockResource.VisibilityManager.EXPECT().GetStoreNames().Return(tc.storeNames).AnyTimes()
			s.mockResource.VisibilityManager.EXPECT().GetIndexName().Return(tc.indexName).AnyTimes()

			saTypeMap := searchattribute.TestNameTypeMap()
			if len(tc.storeNames) > 0 && tc.storeNames[0] == elasticsearch.PersistenceName {
				saTypeMap = searchattribute.TestEsNameTypeMap()
			}

			if tc.getSearchAttributesCalled {
				s.mockResource.SearchAttributesManager.EXPECT().
					GetSearchAttributes(tc.indexName, true).
					Return(saTypeMap, tc.getSearchAttributesErr)
			}

			if tc.addInternalSuccess {
				// Mock the frontend client calls for getNamespaceInfo and updateNamespaceAliases
				s.mockResource.ClientFactory.EXPECT().
					NewLocalFrontendClientWithTimeout(gomock.Any(), gomock.Any()).
					Return(nil, s.mockResource.GetFrontendClient(), nil).
					AnyTimes()
				s.mockResource.FrontendClient.EXPECT().
					DescribeNamespace(
						gomock.Any(),
						&workflowservice.DescribeNamespaceRequest{Namespace: testNamespace},
					).
					Return(&workflowservice.DescribeNamespaceResponse{
						Config: &namespacepb.NamespaceConfig{
							CustomSearchAttributeAliases: searchattribute.TestAliases,
						},
					}, nil).
					AnyTimes()
				s.mockResource.FrontendClient.EXPECT().
					UpdateNamespace(gomock.Any(), gomock.Any()).
					Return(&workflowservice.UpdateNamespaceResponse{}, nil).
					AnyTimes()
			}

			_, err := s.handler.AddSearchAttributes(ctx, tc.request)
			if tc.expectedErrMsg == "" {
				s.NoError(err)
			} else {
				s.ErrorContains(err, tc.expectedErrMsg)
			}
		})
	}
}

func (s *operatorHandlerSuite) Test_AddSearchAttributes_DualVisibility() {
	ctx := context.Background()
	testCases := []struct {
		name              string
		request           *operatorservice.AddSearchAttributesRequest
		addVisManager1    bool
		addVisManager1Err error
		addVisManager2    bool
		addVisManager2Err error
		expectedErrMsg    string
	}{
		{
			name: "success",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
				Namespace: testNamespace,
			},
			addVisManager1: true,
			addVisManager2: true,
		},
		{
			name: "fail: failed to add search attributes to visibility manager 1",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
				Namespace: testNamespace,
			},
			addVisManager1:    true,
			addVisManager1Err: errors.New("mock error add vis manager 1"),
			expectedErrMsg:    "mock error add vis manager 1",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			mockVisManager1 := manager.NewMockVisibilityManager(s.controller)
			mockVisManager2 := manager.NewMockVisibilityManager(s.controller)
			mockManagerSelector := visibility.NewMockmanagerSelector(s.controller)
			mockDualVisManager := visibility.NewVisibilityManagerDual(
				mockVisManager1,
				mockVisManager2,
				mockManagerSelector,
				dynamicconfig.GetBoolPropertyFn(false),
			)
			s.handler.visibilityMgr = mockDualVisManager

			testIndexName1 := testIndexName + "-1"
			testIndexName2 := testIndexName + "-2"
			mockVisManager1.EXPECT().GetStoreNames().Return([]string{elasticsearch.PersistenceName}).AnyTimes()
			mockVisManager1.EXPECT().GetIndexName().Return(testIndexName1).AnyTimes()
			mockVisManager2.EXPECT().GetStoreNames().Return([]string{mysql.PluginName}).AnyTimes()
			mockVisManager2.EXPECT().GetIndexName().Return(testIndexName2).AnyTimes()

			s.mockResource.SearchAttributesManager.EXPECT().
				GetSearchAttributes(testIndexName1, true).
				Return(searchattribute.TestNameTypeMap(), nil).
				AnyTimes()

			s.mockResource.SearchAttributesManager.EXPECT().
				GetSearchAttributes(testIndexName2, true).
				Return(searchattribute.TestNameTypeMap(), nil).
				AnyTimes()

			if tc.addVisManager1 {
				// Mock the frontend client calls for getNamespaceInfo and updateNamespaceAliases
				s.mockResource.ClientFactory.EXPECT().
					NewLocalFrontendClientWithTimeout(gomock.Any(), gomock.Any()).
					Return(nil, s.mockResource.GetFrontendClient(), nil).
					AnyTimes()
				s.mockResource.FrontendClient.EXPECT().
					DescribeNamespace(
						gomock.Any(),
						&workflowservice.DescribeNamespaceRequest{Namespace: testNamespace},
					).
					Return(&workflowservice.DescribeNamespaceResponse{
						Config: &namespacepb.NamespaceConfig{
							CustomSearchAttributeAliases: searchattribute.TestAliases,
						},
					}, nil).
					AnyTimes()
				s.mockResource.FrontendClient.EXPECT().
					UpdateNamespace(gomock.Any(), gomock.Any()).
					Return(&workflowservice.UpdateNamespaceResponse{}, tc.addVisManager1Err).
					AnyTimes()
			}

			if tc.addVisManager2 && !tc.addVisManager1 {
				// Only set up frontend mocks if visManager1 didn't already set them up
				s.mockResource.ClientFactory.EXPECT().
					NewLocalFrontendClientWithTimeout(gomock.Any(), gomock.Any()).
					Return(nil, s.mockResource.GetFrontendClient(), nil).
					AnyTimes()
				s.mockResource.FrontendClient.EXPECT().
					DescribeNamespace(
						gomock.Any(),
						&workflowservice.DescribeNamespaceRequest{Namespace: testNamespace},
					).
					Return(
						&workflowservice.DescribeNamespaceResponse{
							Config: &namespacepb.NamespaceConfig{
								CustomSearchAttributeAliases: searchattribute.TestAliases,
							},
						},
						nil,
					).
					AnyTimes()
				s.mockResource.FrontendClient.EXPECT().
					UpdateNamespace(gomock.Any(), gomock.Any()).
					Return(&workflowservice.UpdateNamespaceResponse{}, tc.addVisManager2Err).
					AnyTimes()
			}

			_, err := s.handler.AddSearchAttributes(ctx, tc.request)
			if tc.expectedErrMsg == "" {
				s.NoError(err)
			} else {
				s.ErrorContains(err, tc.expectedErrMsg)
			}
		})
	}
}

func (s *operatorHandlerSuite) Test_AddSearchAttributesInternal() {
	ctx := context.Background()
	testCases := []struct {
		name                   string
		request                *operatorservice.AddSearchAttributesRequest
		storeName              string
		indexName              string
		getSearchAttributesErr error
		addCalled              bool
		addErr                 error
		expectedErrMsg         string
	}{
		{
			name: "fail: cannot get search attributes",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
			},
			storeName:              elasticsearch.PersistenceName,
			indexName:              testIndexName,
			getSearchAttributesErr: errors.New("mock error get search attributes"),
			expectedErrMsg:         "Unable to get search attributes: mock error get search attributes",
		},
		{
			name: "fail: cannot add search attributes to elasticsearch visibility",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
				Namespace: testNamespace,
			},
			storeName:      elasticsearch.PersistenceName,
			indexName:      testIndexName,
			addCalled:      true,
			addErr:         errors.New("mock error add es schema"),
			expectedErrMsg: "mock error add es schema",
		},
		{
			name: "success: add search attributes to elasticsearch visibility",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
				Namespace: testNamespace,
			},
			storeName:      elasticsearch.PersistenceName,
			indexName:      testIndexName,
			addCalled:      true,
			expectedErrMsg: "",
		},
		{
			name: "fail: cannot add search attributes to sql visibility",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
				Namespace: testNamespace,
			},
			storeName:      mysql.PluginName,
			indexName:      testIndexName,
			addCalled:      true,
			addErr:         errors.New("mock error add sql mapping"),
			expectedErrMsg: "mock error add sql mapping",
		},
		{
			name: "success: add search attributes to sql visibility",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
				Namespace: testNamespace,
			},
			storeName:      mysql.PluginName,
			indexName:      testIndexName,
			addCalled:      true,
			expectedErrMsg: "",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			saTypeMap := searchattribute.TestNameTypeMap()
			if tc.storeName == elasticsearch.PersistenceName {
				saTypeMap = searchattribute.TestEsNameTypeMap()
			}

			s.mockResource.SearchAttributesManager.EXPECT().
				GetSearchAttributes(tc.indexName, true).
				Return(saTypeMap, tc.getSearchAttributesErr)
			s.mockResource.VisibilityManager.EXPECT().GetIndexName().Return(tc.indexName)

			if tc.addCalled {
				// Mock the frontend client calls for getNamespaceInfo and updateNamespaceAliases
				s.mockResource.ClientFactory.EXPECT().
					NewLocalFrontendClientWithTimeout(gomock.Any(), gomock.Any()).
					Return(nil, s.mockResource.GetFrontendClient(), nil).
					AnyTimes()
				s.mockResource.FrontendClient.EXPECT().
					DescribeNamespace(
						gomock.Any(),
						&workflowservice.DescribeNamespaceRequest{Namespace: testNamespace},
					).
					Return(&workflowservice.DescribeNamespaceResponse{
						Config: &namespacepb.NamespaceConfig{
							CustomSearchAttributeAliases: searchattribute.TestAliases,
						},
					}, nil).
					AnyTimes()
				s.mockResource.FrontendClient.EXPECT().
					UpdateNamespace(gomock.Any(), gomock.Any()).
					Return(&workflowservice.UpdateNamespaceResponse{}, tc.addErr).
					AnyTimes()
			}

			err := s.handler.addSearchAttributesInternal(ctx, tc.request, s.mockResource.VisibilityManager)
			if tc.expectedErrMsg == "" {
				s.NoError(err)
			} else {
				s.ErrorContains(err, tc.expectedErrMsg)
			}
		})
	}
}


func (s *operatorHandlerSuite) Test_ListSearchAttributes() {
	handler := s.handler
	ctx := context.Background()

	s.mockResource.VisibilityManager.EXPECT().GetIndexName().Return(testIndexName).AnyTimes()
	s.mockResource.ClientFactory.EXPECT().
		NewLocalFrontendClientWithTimeout(gomock.Any(), gomock.Any()).
		Return(nil, s.mockResource.GetFrontendClient(), nil).
		AnyTimes()
	s.mockResource.FrontendClient.EXPECT().
		DescribeNamespace(gomock.Any(), &workflowservice.DescribeNamespaceRequest{Namespace: testNamespace}).
		Return(
			&workflowservice.DescribeNamespaceResponse{
				Config: &namespacepb.NamespaceConfig{CustomSearchAttributeAliases: searchattribute.TestAliases},
			},
			nil,
		).
		AnyTimes()

	s.mockResource.SearchAttributesManager.EXPECT().
		GetSearchAttributes(testIndexName, true).
		Return(searchattribute.TestNameTypeMap(), nil)
	resp, err := handler.ListSearchAttributes(
		ctx,
		&operatorservice.ListSearchAttributesRequest{Namespace: testNamespace},
	)
	s.NoError(err)
	s.NotNil(resp)

	s.mockResource.SearchAttributesManager.EXPECT().
		GetSearchAttributes(testIndexName, true).
		Return(searchattribute.NameTypeMap{}, errors.New("random error"))
	resp, err = handler.ListSearchAttributes(ctx, &operatorservice.ListSearchAttributesRequest{})
	s.Error(err)
	s.Nil(resp)
}

func (s *operatorHandlerSuite) Test_RemoveSearchAttributes() {
	ctx := context.Background()

	type test struct {
		Name       string
		Request    *operatorservice.RemoveSearchAttributesRequest
		SaveCalled bool
		Expected   error
	}
	testCases := []test{
		{
			Name: "success",
			Request: &operatorservice.RemoveSearchAttributesRequest{
				SearchAttributes: []string{
					"CustomKeywordField",
				},
				Namespace: testNamespace,
			},
			SaveCalled: true,
		},
		{
			Name: "reserved search attribute",
			Request: &operatorservice.RemoveSearchAttributesRequest{
				SearchAttributes: []string{
					"WorkflowId",
				},
				Namespace: testNamespace,
			},
			Expected: &serviceerror.InvalidArgument{Message: "Unable to remove non-custom search attributes: WorkflowId."},
		},
		{
			Name: "search attribute not found",
			Request: &operatorservice.RemoveSearchAttributesRequest{
				SearchAttributes: []string{
					"ProductId",
				},
				Namespace: testNamespace,
			},
		},
	}

	for _, testCase := range testCases {
		s.Run(testCase.Name, func() {
			s.mockResource.VisibilityManager.EXPECT().GetStoreNames().Return([]string{mysql.PluginName}).AnyTimes()
			s.mockResource.VisibilityManager.EXPECT().GetIndexName().Return(testIndexName).AnyTimes()
			s.mockResource.SearchAttributesManager.EXPECT().
				GetSearchAttributes(testIndexName, true).
				Return(searchattribute.TestNameTypeMap(), nil)
			s.mockResource.ClientFactory.EXPECT().
				NewLocalFrontendClientWithTimeout(gomock.Any(), gomock.Any()).
				Return(nil, s.mockResource.GetFrontendClient(), nil).
				AnyTimes()
			s.mockResource.FrontendClient.EXPECT().
				DescribeNamespace(
					gomock.Any(),
					&workflowservice.DescribeNamespaceRequest{Namespace: testNamespace},
				).
				Return(
					&workflowservice.DescribeNamespaceResponse{
						Config: &namespacepb.NamespaceConfig{
							CustomSearchAttributeAliases: searchattribute.TestAliases,
						},
					},
					nil,
				)

			if testCase.SaveCalled {
				s.mockResource.FrontendClient.EXPECT().
					UpdateNamespace(
						gomock.Any(),
						&workflowservice.UpdateNamespaceRequest{
							Namespace: testNamespace,
							Config: &namespacepb.NamespaceConfig{
								CustomSearchAttributeAliases: map[string]string{
									"Keyword01": "",
								},
							},
						},
					).
					Return(&workflowservice.UpdateNamespaceResponse{}, nil)
			}

			resp, err := s.handler.RemoveSearchAttributes(ctx, testCase.Request)
			s.Equal(testCase.Expected, err)
			if testCase.Expected != nil {
				s.Nil(resp)
			} else {
				s.NotNil(resp)
			}
		})
	}
}

func (s *operatorHandlerSuite) Test_DeleteNamespace() {
	handler := s.handler
	ctx := context.Background()

	// Nil request.
	resp, err := handler.DeleteNamespace(ctx, nil)
	s.Equal(&serviceerror.InvalidArgument{Message: "Request is nil."}, err)
	s.Nil(resp)

	mockSdkClient := mocksdk.NewMockClient(s.controller)
	s.mockResource.SDKClientFactory.EXPECT().GetSystemClient().Return(mockSdkClient).AnyTimes()

	handler.config = &Config{
		DeleteNamespaceDeleteActivityRPS:                    dynamicconfig.GetIntPropertyFn(22),
		DeleteNamespacePageSize:                             dynamicconfig.GetIntPropertyFn(8),
		DeleteNamespacePagesPerExecution:                    dynamicconfig.GetIntPropertyFn(78),
		DeleteNamespaceConcurrentDeleteExecutionsActivities: dynamicconfig.GetIntPropertyFn(3),
		DeleteNamespaceNamespaceDeleteDelay:                 dynamicconfig.GetDurationPropertyFn(22 * time.Hour),
	}

	// Start workflow failed.
	mockSdkClient.EXPECT().ExecuteWorkflow(gomock.Any(), gomock.Any(), "temporal-sys-delete-namespace-workflow", gomock.Any()).Return(nil, errors.New("start failed"))
	resp, err = handler.DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{
		Namespace: "test-namespace",
	})
	s.Error(err)
	s.Equal("Unable to start temporal-sys-delete-namespace-workflow workflow: start failed.", err.Error())
	s.Nil(resp)

	// Workflow failed.
	mockRun := mocksdk.NewMockWorkflowRun(s.controller)
	mockRun.EXPECT().Get(gomock.Any(), gomock.Any()).Return(errors.New("workflow failed"))
	const RunId = "9a9f668a-58b1-427e-bed6-bf1401049f7d"
	mockRun.EXPECT().GetRunID().Return(RunId)
	mockRun.EXPECT().GetID().Return("test-workflow-id")
	mockSdkClient.EXPECT().ExecuteWorkflow(gomock.Any(), gomock.Any(), "temporal-sys-delete-namespace-workflow", gomock.Any()).Return(mockRun, nil)
	resp, err = handler.DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{
		Namespace: "test-namespace",
	})
	s.Error(err)
	var sysWfErr *serviceerror.SystemWorkflow
	s.ErrorAs(err, &sysWfErr)
	s.Equal(RunId, sysWfErr.WorkflowExecution.RunId)
	s.Equal(fmt.Sprintf("System Workflow with WorkflowId test-workflow-id and RunId %s returned an error: workflow failed", RunId), err.Error())
	s.Nil(resp)

	// Workflow failed because of validation error (an attempt to delete system namespace).
	mockRun2 := mocksdk.NewMockWorkflowRun(s.controller)
	mockRun2.EXPECT().Get(gomock.Any(), gomock.Any()).Return(delnserrors.NewFailedPrecondition("unable to delete system namespace", nil))
	mockRun2.EXPECT().GetRunID().Return(RunId)
	mockRun2.EXPECT().GetID().Return("test-workflow-id")
	mockSdkClient.EXPECT().ExecuteWorkflow(gomock.Any(), gomock.Any(), "temporal-sys-delete-namespace-workflow", gomock.Any()).Return(mockRun2, nil)
	resp, err = handler.DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{
		Namespace: "temporal-system",
	})
	s.Error(err)
	var failedPreconditionErr *serviceerror.FailedPrecondition
	s.ErrorAs(err, &failedPreconditionErr)
	s.Equal("unable to delete system namespace", failedPreconditionErr.Error())
	s.Nil(resp)

	// Workflow failed because of validation error (an attempt to delete system namespace).
	mockRun3 := mocksdk.NewMockWorkflowRun(s.controller)
	mockRun3.EXPECT().Get(gomock.Any(), gomock.Any()).Return(delnserrors.NewInvalidArgument("only one of namespace or namespace ID must be set", nil))
	mockRun3.EXPECT().GetRunID().Return(RunId)
	mockRun3.EXPECT().GetID().Return("test-workflow-id")
	mockSdkClient.EXPECT().ExecuteWorkflow(gomock.Any(), gomock.Any(), "temporal-sys-delete-namespace-workflow", gomock.Any()).Return(mockRun3, nil)
	resp, err = handler.DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{
		Namespace:   "temporal-system",
		NamespaceId: "c13c01a7-3887-4eda-ba4b-9a07a6359e7e",
	})
	s.Error(err)
	var invalidArgErr *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgErr)
	s.Equal("only one of namespace or namespace ID must be set", invalidArgErr.Error())
	s.Nil(resp)

	// Success case.
	mockRun.EXPECT().Get(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, valuePtr interface{}) error {
		wfResult := valuePtr.(*deletenamespace.DeleteNamespaceWorkflowResult)
		wfResult.DeletedNamespace = "test-namespace-deleted-ka2te"
		wfResult.DeletedNamespaceID = "c13c01a7-3887-4eda-ba4b-9a07a6359e7e"
		return nil
	})
	mockSdkClient.EXPECT().ExecuteWorkflow(gomock.Any(), gomock.Any(), "temporal-sys-delete-namespace-workflow", gomock.Any()).Return(mockRun, nil)

	resp, err = handler.DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{
		Namespace: "test-namespace",
	})
	s.NoError(err)
	s.NotNil(resp)
	s.Equal("test-namespace-deleted-ka2te", resp.DeletedNamespace)

	// Success case with id.
	mockRun.EXPECT().Get(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, valuePtr interface{}) error {
		wfResult := valuePtr.(*deletenamespace.DeleteNamespaceWorkflowResult)
		wfResult.DeletedNamespace = "test-namespace-deleted-ka2te"
		wfResult.DeletedNamespaceID = "c13c01a7-3887-4eda-ba4b-9a07a6359e7e"
		return nil
	})
	mockSdkClient.EXPECT().ExecuteWorkflow(gomock.Any(), gomock.Any(), "temporal-sys-delete-namespace-workflow", gomock.Any()).Return(mockRun, nil)

	resp, err = handler.DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{
		NamespaceId: "c13c01a7-3887-4eda-ba4b-9a07a6359e7e",
	})
	s.NoError(err)
	s.NotNil(resp)
	s.Equal("test-namespace-deleted-ka2te", resp.DeletedNamespace)
}

func (s *operatorHandlerSuite) Test_RemoveRemoteCluster_Success() {
	var clusterName = "cluster"
	s.mockResource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{clusterName: {}})
	s.mockResource.ClusterMetadataMgr.EXPECT().DeleteClusterMetadata(
		gomock.Any(),
		&persistence.DeleteClusterMetadataRequest{ClusterName: clusterName},
	).Return(nil)

	_, err := s.handler.RemoveRemoteCluster(context.Background(), &operatorservice.RemoveRemoteClusterRequest{ClusterName: clusterName})
	s.NoError(err)
}

func (s *operatorHandlerSuite) Test_RemoveRemoteCluster_Error() {
	var clusterName = "cluster"
	s.mockResource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{clusterName: {}})
	s.mockResource.ClusterMetadataMgr.EXPECT().DeleteClusterMetadata(
		gomock.Any(),
		&persistence.DeleteClusterMetadataRequest{ClusterName: clusterName},
	).Return(fmt.Errorf("test error"))

	_, err := s.handler.RemoveRemoteCluster(context.Background(), &operatorservice.RemoveRemoteClusterRequest{ClusterName: clusterName})
	s.Error(err)
}

func (s *operatorHandlerSuite) Test_AddOrUpdateRemoteCluster_RecordFound_Success() {
	var rpcAddress = uuid.NewString()
	var httpAddress = uuid.NewString()
	var clusterName = uuid.NewString()
	var clusterID = uuid.NewString()
	var recordVersion int64 = 5

	s.mockResource.ClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockResource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterID,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			HttpAddress:              httpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockResource.ClusterMetadataMgr.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		&persistence.GetClusterMetadataResponse{
			Version: recordVersion,
		}, nil)
	s.mockResource.ClusterMetadataMgr.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: &persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			ClusterId:                clusterID,
			ClusterAddress:           rpcAddress,
			HttpAddress:              httpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: recordVersion,
	}).Return(true, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{
		FrontendAddress: rpcAddress,
	})
	s.NoError(err)
}

func (s *operatorHandlerSuite) Test_AddOrUpdateRemoteCluster_RecordNotFound_Success() {
	var rpcAddress = uuid.NewString()
	var httpAddress = uuid.NewString()
	var clusterName = uuid.NewString()
	var clusterID = uuid.NewString()

	s.mockResource.ClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockResource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterID,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			HttpAddress:              httpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockResource.ClusterMetadataMgr.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		nil,
		serviceerror.NewNotFound("expected empty result"),
	)
	s.mockResource.ClusterMetadataMgr.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: &persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			ClusterId:                clusterID,
			ClusterAddress:           rpcAddress,
			HttpAddress:              httpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: 0,
	}).Return(true, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{
		FrontendAddress: rpcAddress,
	})
	s.NoError(err)
}

func (s *operatorHandlerSuite) Test_AddOrUpdateRemoteCluster_ValidationError_ClusterNameConflict() {
	var rpcAddress = uuid.NewString()
	var clusterID = uuid.NewString()

	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterID,
			ClusterName:              s.mockResource.ClusterMetadata.GetCurrentClusterName(),
			HistoryShardCount:        0,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *operatorHandlerSuite) Test_AddOrUpdateRemoteCluster_ValidationError_FailoverVersionIncrementMismatch() {
	var rpcAddress = uuid.NewString()
	var clusterName = uuid.NewString()
	var clusterID = uuid.NewString()

	s.mockResource.ClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1))
	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterID,
			ClusterName:              clusterName,
			HistoryShardCount:        0,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *operatorHandlerSuite) Test_AddOrUpdateRemoteCluster_ValidationError_ShardCount_Invalid() {
	var rpcAddress = uuid.NewString()
	var clusterName = uuid.NewString()
	var clusterID = uuid.NewString()

	s.mockResource.ClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterID,
			ClusterName:              clusterName,
			HistoryShardCount:        5,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *operatorHandlerSuite) Test_AddOrUpdateRemoteCluster_ShardCount_Multiple() {
	var rpcAddress = uuid.NewString()
	var httpAddress = uuid.NewString()
	var clusterName = uuid.NewString()
	var clusterID = uuid.NewString()
	var recordVersion int64 = 5

	s.mockResource.ClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockResource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterID,
			ClusterName:              clusterName,
			HistoryShardCount:        16,
			HttpAddress:              httpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockResource.ClusterMetadataMgr.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		&persistence.GetClusterMetadataResponse{
			Version: recordVersion,
		}, nil)
	s.mockResource.ClusterMetadataMgr.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: &persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        16,
			ClusterId:                clusterID,
			ClusterAddress:           rpcAddress,
			HttpAddress:              httpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: recordVersion,
	}).Return(true, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{
		FrontendAddress: rpcAddress,
	})
	s.NoError(err)
}

func (s *operatorHandlerSuite) Test_AddOrUpdateRemoteCluster_ValidationError_GlobalNamespaceDisabled() {
	var rpcAddress = uuid.NewString()
	var clusterName = uuid.NewString()
	var clusterID = uuid.NewString()

	s.mockResource.ClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterID,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: false,
		}, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *operatorHandlerSuite) Test_AddOrUpdateRemoteCluster_ValidationError_InitialFailoverVersionConflict() {
	var rpcAddress = uuid.NewString()
	var clusterName = uuid.NewString()
	var clusterID = uuid.NewString()

	s.mockResource.ClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockResource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		uuid.NewString(): {InitialFailoverVersion: 0},
	})
	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterID,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

func (s *operatorHandlerSuite) Test_AddOrUpdateRemoteCluster_DescribeCluster_Error() {
	var rpcAddress = uuid.NewString()

	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		nil,
		fmt.Errorf("test error"),
	)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
}

func (s *operatorHandlerSuite) Test_AddOrUpdateRemoteCluster_GetClusterMetadata_Error() {
	var rpcAddress = uuid.NewString()
	var clusterName = uuid.NewString()
	var clusterID = uuid.NewString()

	s.mockResource.ClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockResource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterID,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockResource.ClusterMetadataMgr.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		nil,
		fmt.Errorf("test error"),
	)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
}

func (s *operatorHandlerSuite) Test_AddOrUpdateRemoteCluster_SaveClusterMetadata_Error() {
	var rpcAddress = uuid.NewString()
	var httpAddress = uuid.NewString()
	var clusterName = uuid.NewString()
	var clusterID = uuid.NewString()

	s.mockResource.ClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockResource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterID,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			HttpAddress:              httpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockResource.ClusterMetadataMgr.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		nil,
		serviceerror.NewNotFound("expected empty result"),
	)
	s.mockResource.ClusterMetadataMgr.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: &persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			ClusterId:                clusterID,
			ClusterAddress:           rpcAddress,
			HttpAddress:              httpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: 0,
	}).Return(false, fmt.Errorf("test error"))
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{
		FrontendAddress: rpcAddress,
	})
	s.Error(err)
}

func (s *operatorHandlerSuite) Test_AddOrUpdateRemoteCluster_SaveClusterMetadata_NotApplied_Error() {
	var rpcAddress = uuid.NewString()
	var httpAddress = uuid.NewString()
	var clusterName = uuid.NewString()
	var clusterID = uuid.NewString()

	s.mockResource.ClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockResource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterID,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			HttpAddress:              httpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		}, nil)
	s.mockResource.ClusterMetadataMgr.EXPECT().GetClusterMetadata(gomock.Any(), &persistence.GetClusterMetadataRequest{ClusterName: clusterName}).Return(
		nil,
		serviceerror.NewNotFound("expected empty result"),
	)
	s.mockResource.ClusterMetadataMgr.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: &persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			ClusterId:                clusterID,
			ClusterAddress:           rpcAddress,
			HttpAddress:              httpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: 0,
	}).Return(false, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{
		FrontendAddress: rpcAddress,
	})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}
