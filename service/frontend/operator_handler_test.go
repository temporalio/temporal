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

	"go.temporal.io/api/operatorservice/v1"
	"google.golang.org/grpc/health"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	sdkmocks "go.temporal.io/sdk/mocks"

	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/searchattribute"
)

type (
	operatorHandlerSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockResource        *resource.Test
		mockSdkSystemClient *sdkmocks.Client

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
	s.mockResource = resource.NewTest(s.controller, metrics.Frontend)

	s.mockSdkSystemClient = &sdkmocks.Client{}

	args := NewOperatorHandlerImplArgs{
		nil,
		s.mockResource.ESClient,
		s.mockResource.Logger,
		s.mockSdkSystemClient,
		s.mockResource.GetMetricsClient(),
		s.mockResource.GetSearchAttributesProvider(),
		s.mockResource.GetSearchAttributesManager(),
		health.NewServer(),
	}
	s.handler = NewOperatorHandlerImpl(args)
	s.handler.Start()
}

func (s *operatorHandlerSuite) TearDownTest() {
	s.controller.Finish()
	s.handler.Stop()
}

func (s *operatorHandlerSuite) Test_AddSearchAttributes() {
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
	handler.esConfig = &client.Config{
		Indices: map[string]string{
			"visibility": "random-index-name",
		},
	}

	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("random-index-name", true).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()
	testCases2 := []test{
		{
			Name: "reserved key (ES configured)",
			Request: &operatorservice.AddSearchAttributesRequest{
				SearchAttributes: map[string]enumspb.IndexedValueType{
					"WorkflowId": enumspb.INDEXED_VALUE_TYPE_TEXT,
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "Search attribute WorkflowId is reserved by system."},
		},
		{
			Name: "key already whitelisted (ES configured)",
			Request: &operatorservice.AddSearchAttributesRequest{
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

	// Start workflow failed.
	s.mockSdkSystemClient.On("ExecuteWorkflow", mock.Anything, mock.Anything, "temporal-sys-add-search-attributes-workflow", mock.Anything).Return(nil, errors.New("start failed")).Once()
	resp, err := handler.AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{
		SearchAttributes: map[string]enumspb.IndexedValueType{
			"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	})
	s.Error(err)
	s.Equal("Unable to start temporal-sys-add-search-attributes-workflow workflow: start failed.", err.Error())
	s.Nil(resp)

	// Workflow failed.
	mockRun := &sdkmocks.WorkflowRun{}
	mockRun.On("Get", mock.Anything, nil).Return(errors.New("workflow failed")).Once()
	const RunId = "31d8ebd6-93a7-11ec-b909-0242ac120002"
	mockRun.On("GetRunID").Return(RunId).Once()
	s.mockSdkSystemClient.On("ExecuteWorkflow", mock.Anything, mock.Anything, "temporal-sys-add-search-attributes-workflow", mock.Anything).Return(mockRun, nil)
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
	mockRun.On("Get", mock.Anything, nil).Return(nil)

	resp, err = handler.AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{
		SearchAttributes: map[string]enumspb.IndexedValueType{
			"CustomAttr": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	})
	s.NoError(err)
	s.NotNil(resp)
	mockRun.AssertExpectations(s.T())
	s.mockSdkSystemClient.AssertExpectations(s.T())
}

func (s *operatorHandlerSuite) Test_ListSearchAttributes() {
	handler := s.handler
	ctx := context.Background()

	resp, err := handler.ListSearchAttributes(ctx, nil)
	s.Error(err)
	s.Equal(&serviceerror.InvalidArgument{Message: "Request is nil."}, err)
	s.Nil(resp)

	// Elasticsearch is not configured
	s.mockSdkSystemClient.On("DescribeWorkflowExecution", mock.Anything, "temporal-sys-add-search-attributes-workflow", "").Return(
		&workflowservice.DescribeWorkflowExecutionResponse{}, nil).Once()
	s.mockResource.ESClient.EXPECT().GetMapping(gomock.Any(), "").Return(map[string]string{"col": "type"}, nil)
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("", true).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()

	resp, err = handler.ListSearchAttributes(ctx, &operatorservice.ListSearchAttributesRequest{})
	s.NoError(err)
	s.NotNil(resp)

	// Configure Elasticsearch: add advanced visibility store config with index name.
	handler.esConfig = &client.Config{
		Indices: map[string]string{
			"visibility": "random-index-name",
		},
	}

	s.mockResource.ESClient.EXPECT().GetMapping(gomock.Any(), "random-index-name").Return(map[string]string{"col": "type"}, nil)
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("random-index-name", true).Return(searchattribute.TestNameTypeMap, nil)
	resp, err = handler.ListSearchAttributes(ctx, &operatorservice.ListSearchAttributesRequest{})
	s.NoError(err)
	s.NotNil(resp)

	s.mockResource.ESClient.EXPECT().GetMapping(gomock.Any(), "random-index-name").Return(map[string]string{"col": "type"}, nil)
	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("random-index-name", true).Return(searchattribute.NameTypeMap{}, errors.New("random error"))
	resp, err = handler.ListSearchAttributes(ctx, &operatorservice.ListSearchAttributesRequest{})
	s.Error(err)
	s.Nil(resp)
}

func (s *operatorHandlerSuite) Test_RemoveSearchAttributes() {
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
	handler.esConfig = &client.Config{
		Indices: map[string]string{
			"visibility": "random-index-name",
		},
	}

	s.mockResource.SearchAttributesProvider.EXPECT().GetSearchAttributes("random-index-name", true).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()
	testCases2 := []test{
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
	s.mockResource.SearchAttributesManager.EXPECT().SaveSearchAttributes("random-index-name", gomock.Any()).Return(nil)

	resp, err := handler.RemoveSearchAttributes(ctx, &operatorservice.RemoveSearchAttributesRequest{
		SearchAttributes: []string{
			"CustomKeywordField",
		},
	})
	s.NoError(err)
	s.NotNil(resp)
}
