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

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc/health"

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
	"go.temporal.io/server/service/worker/addsearchattributes"
	"go.temporal.io/server/service/worker/deletenamespace"
)

var (
	testNamespace = "test-namespace"
	testIndexName = "test-index-name"
)

type (
	operatorHandlerSuite struct {
		suite.Suite
		*require.Assertions

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
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockResource = resourcetest.NewTest(s.controller, primitives.FrontendService)
	s.mockResource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(uuid.New()).AnyTimes()

	endpointClient := newNexusEndpointClient(
		newNexusEndpointClientConfig(dynamicconfig.NewNoopCollection()),
		s.mockResource.NamespaceCache,
		s.mockResource.MatchingClient,
		persistence.NewMockNexusEndpointManager(s.controller),
		s.mockResource.Logger,
	)

	args := NewOperatorHandlerImplArgs{
		&Config{NumHistoryShards: 4},
		s.mockResource.ESClient,
		s.mockResource.Logger,
		s.mockResource.GetSDKClientFactory(),
		s.mockResource.GetMetricsHandler(),
		s.mockResource.GetVisibilityManager(),
		s.mockResource.GetSearchAttributesManager(),
		health.NewServer(),
		s.mockResource.GetHistoryClient(),
		s.mockResource.GetClusterMetadataManager(),
		s.mockResource.GetClusterMetadata(),
		s.mockResource.GetClientFactory(),
		endpointClient,
	}
	s.handler = NewOperatorHandlerImpl(args)
	s.handler.Start()
}

func (s *operatorHandlerSuite) TearDownTest() {
	s.controller.Finish()
	s.handler.Stop()
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
			expectedErrMsg:            "Failed to add search attributes to store elasticsearch",
		},
		{
			name: "success",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
			},
			storeNames:                []string{elasticsearch.PersistenceName},
			indexName:                 testIndexName,
			getSearchAttributesCalled: true,
			addInternalSuccess:        true,
			expectedErrMsg:            "",
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			if tc.getSearchAttributesCalled {
				s.mockResource.VisibilityManager.EXPECT().GetStoreNames().Return(tc.storeNames)
				s.mockResource.VisibilityManager.EXPECT().GetIndexName().Return(tc.indexName)
				s.mockResource.SearchAttributesManager.EXPECT().
					GetSearchAttributes(tc.indexName, true).
					Return(searchattribute.TestNameTypeMap, tc.getSearchAttributesErr)
			}

			if tc.addInternalSuccess {
				mockSdkClient := mocksdk.NewMockClient(s.controller)
				s.mockResource.SDKClientFactory.EXPECT().GetSystemClient().Return(mockSdkClient)

				mockWfRun := mocksdk.NewMockWorkflowRun(s.controller)
				mockSdkClient.EXPECT().ExecuteWorkflow(
					gomock.Any(),
					sdkclient.StartWorkflowOptions{
						TaskQueue: primitives.DefaultWorkerTaskQueue,
						ID:        addsearchattributes.WorkflowName,
					},
					addsearchattributes.WorkflowName,
					addsearchattributes.WorkflowParams{
						CustomAttributesToAdd: tc.request.SearchAttributes,
						IndexName:             testIndexName,
						SkipSchemaUpdate:      false,
					},
				).Return(mockWfRun, nil)
				mockWfRun.EXPECT().Get(gomock.Any(), nil).Return(nil)
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
	mockVisManager1 := manager.NewMockVisibilityManager(s.controller)
	mockVisManager2 := manager.NewMockVisibilityManager(s.controller)
	mockManagerSelector := visibility.NewMockmanagerSelector(s.controller)
	mockDualVisManager := visibility.NewVisibilityManagerDual(
		mockVisManager1,
		mockVisManager2,
		mockManagerSelector,
	)
	s.handler.visibilityMgr = mockDualVisManager

	mockVisManager1.EXPECT().GetStoreNames().Return([]string{elasticsearch.PersistenceName}).AnyTimes()
	mockVisManager1.EXPECT().GetIndexName().Return(testIndexName).AnyTimes()
	mockVisManager2.EXPECT().GetStoreNames().Return([]string{mysql.PluginName}).AnyTimes()
	mockVisManager2.EXPECT().GetIndexName().Return(testIndexName).AnyTimes()

	s.mockResource.SearchAttributesManager.EXPECT().
		GetSearchAttributes(testIndexName, true).
		Return(searchattribute.TestNameTypeMap, nil).
		AnyTimes()

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
			expectedErrMsg: fmt.Sprintf(
				"Failed to add search attributes to store %s",
				elasticsearch.PersistenceName,
			),
		},
		{
			name: "fail: failed to add search attributes to visibility manager 2",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
				Namespace: testNamespace,
			},
			addVisManager1:    true,
			addVisManager2:    true,
			addVisManager2Err: errors.New("mock error add vis manager 2"),
			expectedErrMsg: fmt.Sprintf(
				"Failed to add search attributes to store %s",
				mysql.PluginName,
			),
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			if tc.addVisManager1 {
				mockSdkClient := mocksdk.NewMockClient(s.controller)
				s.mockResource.SDKClientFactory.EXPECT().GetSystemClient().Return(mockSdkClient)
				mockWfRun := mocksdk.NewMockWorkflowRun(s.controller)
				mockSdkClient.EXPECT().ExecuteWorkflow(
					gomock.Any(),
					sdkclient.StartWorkflowOptions{
						TaskQueue: primitives.DefaultWorkerTaskQueue,
						ID:        addsearchattributes.WorkflowName,
					},
					addsearchattributes.WorkflowName,
					addsearchattributes.WorkflowParams{
						CustomAttributesToAdd: tc.request.SearchAttributes,
						IndexName:             testIndexName,
						SkipSchemaUpdate:      false,
					},
				).Return(mockWfRun, nil)
				mockWfRun.EXPECT().Get(gomock.Any(), nil).Return(tc.addVisManager1Err)
				if tc.addVisManager1Err != nil {
					mockWfRun.EXPECT().GetRunID().Return("test-run-id")
				}
			}

			if tc.addVisManager2 {
				s.mockResource.ClientFactory.EXPECT().
					NewLocalFrontendClientWithTimeout(gomock.Any(), gomock.Any()).
					Return(nil, s.mockResource.GetFrontendClient(), nil)
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
				s.mockResource.FrontendClient.EXPECT().
					UpdateNamespace(gomock.Any(), gomock.Any()).
					Return(&workflowservice.UpdateNamespaceResponse{}, tc.addVisManager2Err)
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
		addEsCalled            bool
		addEsWfErr             error
		addSqlCalled           bool
		addSqlErr              error
		expectedErrMsg         string
	}{
		{
			name: "success: empty index name is noop",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
			},
			storeName:      mysql.PluginName,
			expectedErrMsg: "",
		},
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
			},
			storeName:      elasticsearch.PersistenceName,
			indexName:      testIndexName,
			addEsCalled:    true,
			addEsWfErr:     errors.New("mock error add es wf"),
			expectedErrMsg: "mock error add es wf",
		},
		{
			name: "success: add search attributes to elasticsearch visibility",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
			},
			storeName:      elasticsearch.PersistenceName,
			indexName:      testIndexName,
			addEsCalled:    true,
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
			addSqlCalled:   true,
			addSqlErr:      errors.New("mock error add sql wf"),
			expectedErrMsg: "mock error add sql wf",
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
			addSqlCalled:   true,
			expectedErrMsg: "",
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			s.mockResource.SearchAttributesManager.EXPECT().
				GetSearchAttributes(tc.indexName, true).
				Return(searchattribute.TestNameTypeMap, tc.getSearchAttributesErr)

			if tc.addEsCalled {
				mockSdkClient := mocksdk.NewMockClient(s.controller)
				s.mockResource.SDKClientFactory.EXPECT().GetSystemClient().Return(mockSdkClient)

				mockWfRun := mocksdk.NewMockWorkflowRun(s.controller)
				mockSdkClient.EXPECT().ExecuteWorkflow(
					gomock.Any(),
					sdkclient.StartWorkflowOptions{
						TaskQueue: primitives.DefaultWorkerTaskQueue,
						ID:        addsearchattributes.WorkflowName,
					},
					addsearchattributes.WorkflowName,
					addsearchattributes.WorkflowParams{
						CustomAttributesToAdd: tc.request.SearchAttributes,
						IndexName:             testIndexName,
						SkipSchemaUpdate:      false,
					},
				).Return(mockWfRun, nil)

				mockWfRun.EXPECT().Get(gomock.Any(), nil).Return(tc.addEsWfErr)
				if tc.addEsWfErr != nil {
					mockWfRun.EXPECT().GetRunID().Return("test-run-id")
				}
			}

			if tc.addSqlCalled {
				s.mockResource.ClientFactory.EXPECT().
					NewLocalFrontendClientWithTimeout(gomock.Any(), gomock.Any()).
					Return(nil, s.mockResource.GetFrontendClient(), nil)

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

				s.mockResource.FrontendClient.EXPECT().
					UpdateNamespace(gomock.Any(), gomock.Any()).
					Return(&workflowservice.UpdateNamespaceResponse{}, tc.addSqlErr)
			}

			err := s.handler.addSearchAttributesInternal(ctx, tc.request, tc.storeName, tc.indexName)
			if tc.expectedErrMsg == "" {
				s.NoError(err)
			} else {
				s.ErrorContains(err, tc.expectedErrMsg)
			}
		})
	}
}

func (s *operatorHandlerSuite) Test_AddSearchAttributesElasticsearch() {
	ctx := context.Background()
	testCases := []struct {
		name                  string
		request               *operatorservice.AddSearchAttributesRequest
		executeWorkflowCalled bool
		customAttributesToAdd map[string]enumspb.IndexedValueType
		executeWorkflowError  error
		wfRunGetCalled        bool
		wfRunGetError         error
		expectedErrMsg        string
	}{
		{
			name: "success",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
			},
			executeWorkflowCalled: true,
			customAttributesToAdd: map[string]enumspb.IndexedValueType{
				"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			},
			wfRunGetCalled: true,
			expectedErrMsg: "",
		},
		{
			name: "success: search attribute already exists",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomKeywordField": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
			},
			expectedErrMsg: "",
		},
		{
			name: "success: mix new and already exists search attributes",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr":         enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					"CustomKeywordField": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
			},
			executeWorkflowCalled: true,
			customAttributesToAdd: map[string]enumspb.IndexedValueType{
				"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			},
			wfRunGetCalled: true,
			expectedErrMsg: "",
		},

		{
			name: "fail: cannot execute workflow to add search attributes",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
			},
			executeWorkflowCalled: true,
			customAttributesToAdd: map[string]enumspb.IndexedValueType{
				"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			},
			executeWorkflowError: errors.New("mock error execute workflow"),
			expectedErrMsg: fmt.Sprintf(
				errUnableToStartWorkflowMessage,
				addsearchattributes.WorkflowName,
				errors.New("mock error execute workflow"),
			),
		},
		{
			name: "fail: add search attributes workflow failed",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
			},
			executeWorkflowCalled: true,
			customAttributesToAdd: map[string]enumspb.IndexedValueType{
				"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			},
			wfRunGetCalled: true,
			wfRunGetError:  errors.New("mock error workflow failed"),
			expectedErrMsg: fmt.Sprintf(
				"System Workflow with WorkflowId %s and RunId %s returned an error",
				addsearchattributes.WorkflowName,
				"test-run-id",
			),
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			if tc.executeWorkflowCalled {
				mockSdkClient := mocksdk.NewMockClient(s.controller)
				s.mockResource.SDKClientFactory.EXPECT().GetSystemClient().Return(mockSdkClient)

				mockWfRun := mocksdk.NewMockWorkflowRun(s.controller)
				mockSdkClient.EXPECT().ExecuteWorkflow(
					gomock.Any(),
					sdkclient.StartWorkflowOptions{
						TaskQueue: primitives.DefaultWorkerTaskQueue,
						ID:        addsearchattributes.WorkflowName,
					},
					addsearchattributes.WorkflowName,
					addsearchattributes.WorkflowParams{
						CustomAttributesToAdd: tc.customAttributesToAdd,
						IndexName:             testIndexName,
						SkipSchemaUpdate:      false,
					},
				).Return(mockWfRun, tc.executeWorkflowError)

				if tc.wfRunGetCalled {
					mockWfRun.EXPECT().Get(gomock.Any(), nil).Return(tc.wfRunGetError)
					if tc.wfRunGetError != nil {
						mockWfRun.EXPECT().GetRunID().Return("test-run-id")
					}
				}
			}

			err := s.handler.addSearchAttributesElasticsearch(
				ctx,
				tc.request,
				testIndexName,
				searchattribute.TestNameTypeMap,
			)
			if tc.expectedErrMsg == "" {
				s.NoError(err)
			} else {
				s.ErrorContains(err, tc.expectedErrMsg)
			}
		})
	}
}

func (s *operatorHandlerSuite) Test_AddSearchAttributesSQL() {
	ctx := context.Background()
	testCases := []struct {
		name                        string
		request                     *operatorservice.AddSearchAttributesRequest
		customSearchAttributesToAdd []string
		getFrontendClientErr        error
		describeNamespaceCalled     bool
		describeNamespaceErr        error
		updateNamespaceCalled       bool
		updateNamespaceErr          error
		expectedErrMsg              string
	}{
		{
			name: "success",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
				Namespace: testNamespace,
			},
			customSearchAttributesToAdd: []string{"CustomAttr"},
			describeNamespaceCalled:     true,
			updateNamespaceCalled:       true,
			expectedErrMsg:              "",
		},
		{
			name: "success: search attribute already exists",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomKeywordField": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
				Namespace: testNamespace,
			},
			describeNamespaceCalled: true,
			expectedErrMsg:          "",
		},
		{
			name: "success: mix new and already exists search attributes",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr":         enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					"CustomKeywordField": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
				Namespace: testNamespace,
			},
			customSearchAttributesToAdd: []string{"CustomAttr"},
			describeNamespaceCalled:     true,
			updateNamespaceCalled:       true,
			expectedErrMsg:              "",
		},

		{
			name: "fail: cannot get frontend client",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
				Namespace: testNamespace,
			},
			getFrontendClientErr: errors.New("mock error get frontend client"),
			expectedErrMsg:       "mock error get frontend client",
		},
		{
			name: "fail: namespace not set in request",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
			},
			expectedErrMsg: errNamespaceNotSet.Error(),
		},
		{
			name: "fail: cannot describe namespace",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
				Namespace: testNamespace,
			},
			describeNamespaceCalled: true,
			describeNamespaceErr:    errors.New("mock error describe namespace"),
			expectedErrMsg:          fmt.Sprintf(errUnableToGetNamespaceInfoMessage, testNamespace, "mock error describe namespace"),
		},
		{
			name: "fail: too many search attributes",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					// there is already one keyword search attribute defined in TestNameTypeMap
					"CustomAttr1": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					"CustomAttr2": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					"CustomAttr3": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
				Namespace: testNamespace,
			},
			describeNamespaceCalled: true,
			expectedErrMsg: fmt.Sprintf(
				errTooManySearchAttributesMessage,
				3,
				enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			),
		},
		{
			name: "fail: cannot update namespace",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
				Namespace: testNamespace,
			},
			customSearchAttributesToAdd: []string{"CustomAttr"},
			describeNamespaceCalled:     true,
			updateNamespaceCalled:       true,
			updateNamespaceErr:          errors.New("mock error update namespace"),
			expectedErrMsg: fmt.Sprintf(
				errUnableToSaveSearchAttributesMessage,
				"mock error update namespace",
			),
		},
		{
			name: "fail: update namespace race condition",
			request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
				Namespace: testNamespace,
			},
			customSearchAttributesToAdd: []string{"CustomAttr"},
			describeNamespaceCalled:     true,
			updateNamespaceCalled:       true,
			updateNamespaceErr:          errCustomSearchAttributeFieldAlreadyAllocated,
			expectedErrMsg:              errRaceConditionAddingSearchAttributes.Error(),
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			s.mockResource.ClientFactory.EXPECT().
				NewLocalFrontendClientWithTimeout(gomock.Any(), gomock.Any()).
				Return(nil, s.mockResource.GetFrontendClient(), tc.getFrontendClientErr)

			if tc.describeNamespaceCalled {
				s.mockResource.FrontendClient.EXPECT().
					DescribeNamespace(
						gomock.Any(),
						&workflowservice.DescribeNamespaceRequest{Namespace: testNamespace},
					).
					Return(
						&workflowservice.DescribeNamespaceResponse{
							Config: &namespacepb.NamespaceConfig{CustomSearchAttributeAliases: searchattribute.TestAliases},
						},
						tc.describeNamespaceErr,
					)
			}

			if tc.updateNamespaceCalled {
				s.mockResource.FrontendClient.EXPECT().
					UpdateNamespace(gomock.Any(), gomock.Any()).
					DoAndReturn(func(
						ctx context.Context,
						r *workflowservice.UpdateNamespaceRequest,
						opts ...any,
					) (*workflowservice.UpdateNamespaceResponse, error) {
						s.Len(r.Config.CustomSearchAttributeAliases, len(tc.customSearchAttributesToAdd))
						aliases := maps.Values(r.Config.CustomSearchAttributeAliases)
						for _, saName := range tc.customSearchAttributesToAdd {
							s.Contains(aliases, saName)
						}
						return &workflowservice.UpdateNamespaceResponse{}, tc.updateNamespaceErr
					})
			}

			err := s.handler.addSearchAttributesSQL(
				ctx,
				tc.request,
				searchattribute.TestNameTypeMap,
			)
			if tc.expectedErrMsg == "" {
				s.NoError(err)
			} else {
				s.ErrorContains(err, tc.expectedErrMsg)
			}
		})
	}
}

func (s *operatorHandlerSuite) Test_ListSearchAttributes_EmptyIndexName() {
	handler := s.handler
	ctx := context.Background()

	s.mockResource.VisibilityManager.EXPECT().HasStoreName(elasticsearch.PersistenceName).Return(true)
	s.mockResource.VisibilityManager.EXPECT().GetIndexName().Return("").AnyTimes()
	resp, err := handler.ListSearchAttributes(ctx, nil)
	s.Error(err)
	s.Equal(&serviceerror.InvalidArgument{Message: "Request is nil."}, err)
	s.Nil(resp)

	// Elasticsearch is not configured
	s.mockResource.ESClient.EXPECT().GetMapping(gomock.Any(), "").Return(map[string]string{"col": "type"}, nil)
	s.mockResource.SearchAttributesManager.EXPECT().GetSearchAttributes("", true).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()

	resp, err = handler.ListSearchAttributes(ctx, &operatorservice.ListSearchAttributesRequest{})
	s.NoError(err)
	s.NotNil(resp)
}

func (s *operatorHandlerSuite) Test_ListSearchAttributes_Elasticsearch() {
	handler := s.handler
	ctx := context.Background()

	// Configure Elasticsearch: add advanced visibility store config with index name.
	s.mockResource.VisibilityManager.EXPECT().HasStoreName(elasticsearch.PersistenceName).Return(true)
	s.mockResource.VisibilityManager.EXPECT().GetIndexName().Return(testIndexName).AnyTimes()
	s.mockResource.ESClient.EXPECT().GetMapping(gomock.Any(), testIndexName).Return(map[string]string{"col": "type"}, nil)
	s.mockResource.SearchAttributesManager.EXPECT().GetSearchAttributes(testIndexName, true).Return(searchattribute.TestNameTypeMap, nil)
	resp, err := handler.ListSearchAttributes(ctx, &operatorservice.ListSearchAttributesRequest{})
	s.NoError(err)
	s.NotNil(resp)

	s.mockResource.SearchAttributesManager.EXPECT().GetSearchAttributes(testIndexName, true).Return(searchattribute.NameTypeMap{}, errors.New("random error"))
	resp, err = handler.ListSearchAttributes(ctx, &operatorservice.ListSearchAttributesRequest{})
	s.Error(err)
	s.Nil(resp)
}

func (s *operatorHandlerSuite) Test_ListSearchAttributes_SQL() {
	handler := s.handler
	ctx := context.Background()

	s.mockResource.VisibilityManager.EXPECT().HasStoreName(elasticsearch.PersistenceName).Return(false)
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
		Return(searchattribute.TestNameTypeMap, nil)
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

func (s *operatorHandlerSuite) Test_RemoveSearchAttributes_EmptyIndexName() {
	handler := s.handler
	ctx := context.Background()

	type test struct {
		Name     string
		Request  *operatorservice.RemoveSearchAttributesRequest
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
			Request:  &operatorservice.RemoveSearchAttributesRequest{},
			Expected: &serviceerror.InvalidArgument{Message: "SearchAttributes are not set on request."},
		},
	}

	s.mockResource.VisibilityManager.EXPECT().HasStoreName(elasticsearch.PersistenceName).Return(true).AnyTimes()
	s.mockResource.VisibilityManager.EXPECT().GetIndexName().Return("").AnyTimes()
	for _, testCase := range testCases1 {
		s.T().Run(testCase.Name, func(t *testing.T) {
			resp, err := handler.RemoveSearchAttributes(ctx, testCase.Request)
			s.Equal(testCase.Expected, err)
			s.Nil(resp)
		})
	}

	// Elasticsearch is not configured
	s.mockResource.SearchAttributesManager.EXPECT().GetSearchAttributes("", true).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()
	testCases2 := []test{
		{
			Name: "reserved search attribute (empty index)",
			Request: &operatorservice.RemoveSearchAttributesRequest{
				SearchAttributes: []string{
					"WorkflowId",
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "Unable to remove non-custom search attributes: WorkflowId."},
		},
		{
			Name: "search attribute doesn't exist (empty index)",
			Request: &operatorservice.RemoveSearchAttributesRequest{
				SearchAttributes: []string{
					"ProductId",
				},
			},
			Expected: &serviceerror.NotFound{Message: "Search attribute ProductId doesn't exist."},
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

func (s *operatorHandlerSuite) Test_RemoveSearchAttributes_Elasticsearch() {
	handler := s.handler
	ctx := context.Background()

	type test struct {
		Name     string
		Request  *operatorservice.RemoveSearchAttributesRequest
		Expected error
	}
	testCases := []test{
		{
			Name: "reserved search attribute (ES configured)",
			Request: &operatorservice.RemoveSearchAttributesRequest{
				SearchAttributes: []string{
					"WorkflowId",
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "Unable to remove non-custom search attributes: WorkflowId."},
		},
		{
			Name: "search attribute doesn't exist (ES configured)",
			Request: &operatorservice.RemoveSearchAttributesRequest{
				SearchAttributes: []string{
					"ProductId",
				},
			},
			Expected: &serviceerror.NotFound{Message: "Search attribute ProductId doesn't exist."},
		},
	}

	// Configure Elasticsearch: add advanced visibility store config with index name.
	s.mockResource.VisibilityManager.EXPECT().HasStoreName(elasticsearch.PersistenceName).Return(true).AnyTimes()
	s.mockResource.VisibilityManager.EXPECT().GetIndexName().Return(testIndexName).AnyTimes()
	s.mockResource.SearchAttributesManager.EXPECT().GetSearchAttributes(testIndexName, true).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()
	for _, testCase := range testCases {
		s.T().Run(testCase.Name, func(t *testing.T) {
			resp, err := handler.RemoveSearchAttributes(ctx, testCase.Request)
			s.Equal(testCase.Expected, err)
			s.Nil(resp)
		})
	}

	// Success case.
	s.mockResource.SearchAttributesManager.EXPECT().SaveSearchAttributes(gomock.Any(), testIndexName, gomock.Any()).Return(nil)

	resp, err := handler.RemoveSearchAttributes(ctx, &operatorservice.RemoveSearchAttributesRequest{
		SearchAttributes: []string{
			"CustomKeywordField",
		},
	})
	s.NoError(err)
	s.NotNil(resp)
}

func (s *operatorHandlerSuite) Test_RemoveSearchAttributes_SQL() {
	handler := s.handler
	ctx := context.Background()

	s.mockResource.VisibilityManager.EXPECT().HasStoreName(elasticsearch.PersistenceName).Return(false).AnyTimes()
	s.mockResource.VisibilityManager.EXPECT().GetIndexName().Return(testIndexName).AnyTimes()
	s.mockResource.SearchAttributesManager.EXPECT().
		GetSearchAttributes(testIndexName, true).
		Return(searchattribute.TestNameTypeMap, nil).
		AnyTimes()
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

	type test struct {
		Name     string
		Request  *operatorservice.RemoveSearchAttributesRequest
		Expected error
	}
	testCases := []test{
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
			Name: "search attribute doesn't exist",
			Request: &operatorservice.RemoveSearchAttributesRequest{
				SearchAttributes: []string{
					"CustomKeywordField",
					"ProductId",
				},
				Namespace: testNamespace,
			},
			Expected: &serviceerror.NotFound{Message: "Search attribute ProductId doesn't exist."},
		},
	}

	for _, testCase := range testCases {
		s.T().Run(testCase.Name, func(t *testing.T) {
			resp, err := handler.RemoveSearchAttributes(ctx, testCase.Request)
			s.Equal(testCase.Expected, err)
			s.Nil(resp)
		})
	}

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
	resp, err := handler.RemoveSearchAttributes(
		ctx,
		&operatorservice.RemoveSearchAttributesRequest{
			SearchAttributes: []string{
				"CustomKeywordField",
			},
			Namespace: testNamespace,
		},
	)
	s.NoError(err)
	s.NotNil(resp)
}

func (s *operatorHandlerSuite) Test_DeleteNamespace() {
	handler := s.handler
	ctx := context.Background()

	type test struct {
		Name     string
		Request  *operatorservice.DeleteNamespaceRequest
		Expected error
	}
	// request validation tests
	testCases := []test{
		{
			Name:     "nil request",
			Request:  nil,
			Expected: &serviceerror.InvalidArgument{Message: "Request is nil."},
		},
		{
			Name:     "system namespace",
			Request:  &operatorservice.DeleteNamespaceRequest{Namespace: "temporal-system"},
			Expected: &serviceerror.InvalidArgument{Message: "Unable to delete system namespace."},
		},
		{
			Name:     "system namespace id",
			Request:  &operatorservice.DeleteNamespaceRequest{NamespaceId: "32049b68-7872-4094-8e63-d0dd59896a83"},
			Expected: &serviceerror.InvalidArgument{Message: "Unable to delete system namespace."},
		},
	}
	for _, testCase := range testCases {
		s.T().Run(testCase.Name, func(t *testing.T) {
			resp, err := handler.DeleteNamespace(ctx, testCase.Request)
			s.Equal(testCase.Expected, err)
			s.Nil(resp)
		})
	}

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
	resp, err := handler.DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{
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
	mockSdkClient.EXPECT().ExecuteWorkflow(gomock.Any(), gomock.Any(), "temporal-sys-delete-namespace-workflow", gomock.Any()).Return(mockRun, nil)
	resp, err = handler.DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{
		Namespace: "test-namespace",
	})
	s.Error(err)
	s.Equal(RunId, err.(*serviceerror.SystemWorkflow).WorkflowExecution.RunId)
	s.Equal(fmt.Sprintf("System Workflow with WorkflowId temporal-sys-delete-namespace-workflow and RunId %s returned an error: workflow failed", RunId), err.Error())
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
	var rpcAddress = uuid.New()
	var httpAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()
	var recordVersion int64 = 5

	s.mockResource.ClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockResource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
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
			ClusterId:                clusterId,
			ClusterAddress:           rpcAddress,
			HttpAddress:              httpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: recordVersion,
	}).Return(true, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{
		FrontendAddress:     rpcAddress,
		FrontendHttpAddress: httpAddress,
	})
	s.NoError(err)
}

func (s *operatorHandlerSuite) Test_AddOrUpdateRemoteCluster_RecordNotFound_Success() {
	var rpcAddress = uuid.New()
	var httpAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockResource.ClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockResource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
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
			ClusterId:                clusterId,
			ClusterAddress:           rpcAddress,
			HttpAddress:              httpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: 0,
	}).Return(true, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{
		FrontendAddress:     rpcAddress,
		FrontendHttpAddress: httpAddress,
	})
	s.NoError(err)
}

func (s *operatorHandlerSuite) Test_AddOrUpdateRemoteCluster_ValidationError_ClusterNameConflict() {
	var rpcAddress = uuid.New()
	var clusterId = uuid.New()

	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
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
	var rpcAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockResource.ClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(1))
	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
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
	var rpcAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockResource.ClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
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
	var rpcAddress = uuid.New()
	var httpAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()
	var recordVersion int64 = 5

	s.mockResource.ClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockResource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        16,
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
			ClusterId:                clusterId,
			ClusterAddress:           rpcAddress,
			HttpAddress:              httpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: recordVersion,
	}).Return(true, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{
		FrontendAddress:     rpcAddress,
		FrontendHttpAddress: httpAddress,
	})
	s.NoError(err)
}

func (s *operatorHandlerSuite) Test_AddOrUpdateRemoteCluster_ValidationError_GlobalNamespaceDisabled() {
	var rpcAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockResource.ClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
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
	var rpcAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockResource.ClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockResource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		uuid.New(): {InitialFailoverVersion: 0},
	})
	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
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
	var rpcAddress = uuid.New()

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
	var rpcAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockResource.ClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockResource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
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
	var rpcAddress = uuid.New()
	var httpAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockResource.ClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockResource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
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
			ClusterId:                clusterId,
			ClusterAddress:           rpcAddress,
			HttpAddress:              httpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: 0,
	}).Return(false, fmt.Errorf("test error"))
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{
		FrontendAddress:     rpcAddress,
		FrontendHttpAddress: httpAddress,
	})
	s.Error(err)
}

func (s *operatorHandlerSuite) Test_AddOrUpdateRemoteCluster_SaveClusterMetadata_NotApplied_Error() {
	var rpcAddress = uuid.New()
	var httpAddress = uuid.New()
	var clusterName = uuid.New()
	var clusterId = uuid.New()

	s.mockResource.ClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(0))
	s.mockResource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(make(map[string]cluster.ClusterInformation))
	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterId,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
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
			ClusterId:                clusterId,
			ClusterAddress:           rpcAddress,
			HttpAddress:              httpAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: 0,
	}).Return(false, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{
		FrontendAddress:     rpcAddress,
		FrontendHttpAddress: httpAddress,
	})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}
