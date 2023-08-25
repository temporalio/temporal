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
	"strings"
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
	"go.temporal.io/server/common/primitives"
	"google.golang.org/grpc/health"

	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch"
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

	args := NewOperatorHandlerImplArgs{
		&Config{NumHistoryShards: 4},
		s.mockResource.ESClient,
		s.mockResource.Logger,
		s.mockResource.GetSDKClientFactory(),
		s.mockResource.GetMetricsHandler(),
		s.mockResource.GetVisibilityManager(),
		s.mockResource.GetSearchAttributesProvider(),
		s.mockResource.GetSearchAttributesManager(),
		health.NewServer(),
		s.mockResource.GetHistoryClient(),
		s.mockResource.GetClusterMetadataManager(),
		s.mockResource.GetClusterMetadata(),
		s.mockResource.GetClientFactory(),
	}
	s.handler = NewOperatorHandlerImpl(args)
	s.handler.Start()
}

func (s *operatorHandlerSuite) TearDownTest() {
	s.controller.Finish()
	s.handler.Stop()
}

func (s *operatorHandlerSuite) Test_AddSearchAttributes_EmptyIndexName() {
	handler := s.handler
	ctx := context.Background()

	type test struct {
		Name     string
		Request  *operatorservice.AddSearchAttributesRequest
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
			Request:  &operatorservice.AddSearchAttributesRequest{},
			Expected: &serviceerror.InvalidArgument{Message: "SearchAttributes are not set on request."},
		},
	}

	s.mockResource.VisibilityManager.EXPECT().HasStoreName(elasticsearch.PersistenceName).Return(true)
	s.mockResource.VisibilityManager.EXPECT().GetIndexName().Return("").AnyTimes()
	for _, testCase := range testCases1 {
		s.T().Run(testCase.Name, func(t *testing.T) {
			resp, err := handler.AddSearchAttributes(ctx, testCase.Request)
			s.Equal(testCase.Expected, err)
			s.Nil(resp)
		})
	}

	// Elasticsearch is not configured
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("", true).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()
	testCases2 := []test{
		{
			Name: "reserved key (empty index)",
			Request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"WorkflowId": enumspb.INDEXED_VALUE_TYPE_TEXT,
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "Search attribute WorkflowId is reserved by system."},
		},
		{
			Name: "key already whitelisted (empty index)",
			Request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomTextField": enumspb.INDEXED_VALUE_TYPE_TEXT,
				},
			},
			Expected: &serviceerror.AlreadyExists{Message: "Search attribute CustomTextField already exists."},
		},
	}
	for _, testCase := range testCases2 {
		s.T().Run(testCase.Name, func(t *testing.T) {
			resp, err := handler.AddSearchAttributes(ctx, testCase.Request)
			s.Equal(testCase.Expected, err)
			s.Nil(resp)
		})
	}
}

func (s *operatorHandlerSuite) Test_AddSearchAttributes_Elasticsearch() {
	handler := s.handler
	ctx := context.Background()

	type test struct {
		Name     string
		Request  *operatorservice.AddSearchAttributesRequest
		Expected error
	}
	testCases := []test{
		{
			Name: "reserved key",
			Request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"WorkflowId": enumspb.INDEXED_VALUE_TYPE_TEXT,
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "Search attribute WorkflowId is reserved by system."},
		},
		{
			Name: "key already whitelisted",
			Request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomTextField": enumspb.INDEXED_VALUE_TYPE_TEXT,
				},
			},
			Expected: &serviceerror.AlreadyExists{Message: "Search attribute CustomTextField already exists."},
		},
		{
			Name: "mix new key and key already whitelisted",
			Request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr":      enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					"CustomTextField": enumspb.INDEXED_VALUE_TYPE_TEXT,
				},
			},
			Expected: &serviceerror.AlreadyExists{Message: "Search attribute CustomTextField already exists."},
		},
	}

	// Configure Elasticsearch: add advanced visibility store config with index name.
	s.mockResource.VisibilityManager.EXPECT().HasStoreName(elasticsearch.PersistenceName).Return(true).AnyTimes()
	s.mockResource.VisibilityManager.EXPECT().GetIndexName().Return(testIndexName).AnyTimes()
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes(testIndexName, true).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()
	for _, testCase := range testCases {
		s.T().Run(testCase.Name, func(t *testing.T) {
			resp, err := handler.AddSearchAttributes(ctx, testCase.Request)
			s.Equal(testCase.Expected, err)
			s.Nil(resp)
		})
	}

	mockSdkClient := mocksdk.NewMockClient(s.controller)
	s.mockResource.SDKClientFactory.EXPECT().GetSystemClient().Return(mockSdkClient).AnyTimes()

	// Start workflow failed.
	mockSdkClient.EXPECT().ExecuteWorkflow(gomock.Any(), gomock.Any(), "temporal-sys-add-search-attributes-workflow", gomock.Any()).Return(nil, errors.New("start failed"))
	resp, err := handler.AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{
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
	const RunId = "31d8ebd6-93a7-11ec-b909-0242ac120002"
	mockRun.EXPECT().GetRunID().Return(RunId)
	mockSdkClient.EXPECT().ExecuteWorkflow(gomock.Any(), gomock.Any(), "temporal-sys-add-search-attributes-workflow", gomock.Any()).Return(mockRun, nil)
	resp, err = handler.AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{
		SearchAttributes: map[string]enumspb.IndexedValueType{
			"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	})
	s.Error(err)
	s.Equal(RunId, err.(*serviceerror.SystemWorkflow).WorkflowExecution.RunId)
	s.Equal(fmt.Sprintf("System Workflow with WorkflowId temporal-sys-add-search-attributes-workflow and RunId %s returned an error: workflow failed", RunId), err.Error())
	s.Nil(resp)

	// Success case.
	mockRun.EXPECT().Get(gomock.Any(), nil).Return(nil)
	mockSdkClient.EXPECT().ExecuteWorkflow(
		gomock.Any(),
		gomock.Any(),
		"temporal-sys-add-search-attributes-workflow",
		addsearchattributes.WorkflowParams{
			CustomAttributesToAdd: map[string]enumspb.IndexedValueType{
				"CustomAttr1": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				"CustomAttr2": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			},
			IndexName:        testIndexName,
			SkipSchemaUpdate: false,
		},
	).Return(mockRun, nil)

	resp, err = handler.AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{
		SearchAttributes: map[string]enumspb.IndexedValueType{
			"CustomAttr1": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"CustomAttr2": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	})
	s.NoError(err)
	s.NotNil(resp)
}

func (s *operatorHandlerSuite) Test_AddSearchAttributes_SQL() {
	handler := s.handler
	ctx := context.Background()

	s.mockResource.VisibilityManager.EXPECT().HasStoreName(elasticsearch.PersistenceName).Return(false).AnyTimes()
	s.mockResource.VisibilityManager.EXPECT().GetIndexName().Return(testIndexName).AnyTimes()
	s.mockResource.SearchAttributesProvider.EXPECT().
		GetSearchAttributes(testIndexName, true).
		Return(searchattribute.TestNameTypeMap, nil).AnyTimes()
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
		Request  *operatorservice.AddSearchAttributesRequest
		Expected error
	}
	testCases := []test{
		{
			Name: "reserved key",
			Request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"WorkflowId": enumspb.INDEXED_VALUE_TYPE_TEXT,
				},
				Namespace: testNamespace,
			},
			Expected: &serviceerror.InvalidArgument{Message: "Search attribute WorkflowId is reserved by system."},
		},
		{
			Name: "key already whitelisted",
			Request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomTextField": enumspb.INDEXED_VALUE_TYPE_TEXT,
				},
				Namespace: testNamespace,
			},
			Expected: &serviceerror.AlreadyExists{Message: "Search attribute CustomTextField already exists."},
		},
		{
			Name: "mix new key and key already whitelisted",
			Request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr":      enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					"CustomTextField": enumspb.INDEXED_VALUE_TYPE_TEXT,
				},
				Namespace: testNamespace,
			},
			Expected: &serviceerror.AlreadyExists{Message: "Search attribute CustomTextField already exists."},
		},
		{
			Name: "too many search attributes",
			Request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"CustomAttr1": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					"CustomAttr2": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
					"CustomAttr3": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				},
				Namespace: testNamespace,
			},
			Expected: &serviceerror.InvalidArgument{Message: "Unable to create search attributes: cannot have more than 3 search attribute of type Keyword."},
		},
	}

	for _, testCase := range testCases {
		s.T().Run(testCase.Name, func(t *testing.T) {
			resp, err := handler.AddSearchAttributes(ctx, testCase.Request)
			s.Equal(testCase.Expected, err)
			s.Nil(resp)
		})
	}

	s.mockResource.FrontendClient.EXPECT().
		UpdateNamespace(gomock.Any(), gomock.Any()).
		Return(nil, errCustomSearchAttributeFieldAlreadyAllocated)
	resp, err := handler.AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{
		SearchAttributes: map[string]enumspb.IndexedValueType{
			"CustomAttr1": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"CustomAttr2": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"CustomAttr3": enumspb.INDEXED_VALUE_TYPE_INT,
		},
		Namespace: testNamespace,
	})
	s.Equal(errRaceConditionAddingSearchAttributes, err)
	s.Nil(resp)

	s.mockResource.FrontendClient.EXPECT().
		UpdateNamespace(
			gomock.Any(),
			newUpdateNamespaceRequestMatcher(
				func(request *workflowservice.UpdateNamespaceRequest) bool {
					if len(request.Config.CustomSearchAttributeAliases) != 3 {
						return false
					}
					cnt := 0
					for field, alias := range request.Config.CustomSearchAttributeAliases {
						if alias == "CustomAttr1" || alias == "CustomAttr2" {
							if !strings.HasPrefix(field, "Keyword") {
								return false
							}
							cnt++
						} else if alias == "CustomAttr3" {
							if !strings.HasPrefix(field, "Int") {
								return false
							}
							cnt++
						}
					}
					return cnt == 3
				},
			),
		).
		Return(&workflowservice.UpdateNamespaceResponse{}, nil)
	resp, err = handler.AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{
		SearchAttributes: map[string]enumspb.IndexedValueType{
			"CustomAttr1": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"CustomAttr2": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"CustomAttr3": enumspb.INDEXED_VALUE_TYPE_INT,
		},
		Namespace: testNamespace,
	})
	s.NoError(err)
	s.NotNil(resp)
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
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("", true).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()

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
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes(testIndexName, true).Return(searchattribute.TestNameTypeMap, nil)
	resp, err := handler.ListSearchAttributes(ctx, &operatorservice.ListSearchAttributesRequest{})
	s.NoError(err)
	s.NotNil(resp)

	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes(testIndexName, true).Return(searchattribute.NameTypeMap{}, errors.New("random error"))
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

	s.mockResource.SearchAttributesProvider.EXPECT().
		GetSearchAttributes(testIndexName, true).
		Return(searchattribute.TestNameTypeMap, nil)
	resp, err := handler.ListSearchAttributes(
		ctx,
		&operatorservice.ListSearchAttributesRequest{Namespace: testNamespace},
	)
	s.NoError(err)
	s.NotNil(resp)

	s.mockResource.SearchAttributesProvider.EXPECT().
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
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("", true).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()
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
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes(testIndexName, true).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()
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
	s.mockResource.SearchAttributesProvider.EXPECT().
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
		ClusterMetadata: persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			ClusterId:                clusterId,
			ClusterAddress:           rpcAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: recordVersion,
	}).Return(true, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.NoError(err)
}

func (s *operatorHandlerSuite) Test_AddOrUpdateRemoteCluster_RecordNotFound_Success() {
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
		serviceerror.NewNotFound("expected empty result"),
	)
	s.mockResource.ClusterMetadataMgr.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			ClusterId:                clusterId,
			ClusterAddress:           rpcAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: 0,
	}).Return(true, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
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
		ClusterMetadata: persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        16,
			ClusterId:                clusterId,
			ClusterAddress:           rpcAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: recordVersion,
	}).Return(true, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
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
		ClusterMetadata: persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			ClusterId:                clusterId,
			ClusterAddress:           rpcAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: 0,
	}).Return(false, fmt.Errorf("test error"))
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
}

func (s *operatorHandlerSuite) Test_AddOrUpdateRemoteCluster_SaveClusterMetadata_NotApplied_Error() {
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
		serviceerror.NewNotFound("expected empty result"),
	)
	s.mockResource.ClusterMetadataMgr.EXPECT().SaveClusterMetadata(gomock.Any(), &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: persistencespb.ClusterMetadata{
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			ClusterId:                clusterId,
			ClusterAddress:           rpcAddress,
			FailoverVersionIncrement: 0,
			InitialFailoverVersion:   0,
			IsGlobalNamespaceEnabled: true,
		},
		Version: 0,
	}).Return(false, nil)
	_, err := s.handler.AddOrUpdateRemoteCluster(context.Background(), &operatorservice.AddOrUpdateRemoteClusterRequest{FrontendAddress: rpcAddress})
	s.Error(err)
	s.IsType(&serviceerror.InvalidArgument{}, err)
}

type updateNamespaceRequestMatcher struct {
	f func(request *workflowservice.UpdateNamespaceRequest) bool
}

func newUpdateNamespaceRequestMatcher(f func(request *workflowservice.UpdateNamespaceRequest) bool) gomock.Matcher {
	return &updateNamespaceRequestMatcher{
		f: f,
	}
}

func (m *updateNamespaceRequestMatcher) Matches(x interface{}) bool {
	request, ok := x.(*workflowservice.UpdateNamespaceRequest)
	if !ok {
		return false
	}
	return m.f(request)
}

func (m *updateNamespaceRequestMatcher) String() string {
	return "UpdateNamespaceRequest match condition"
}
