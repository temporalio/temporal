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

	enumspb "go.temporal.io/api/enums/v1"

	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/common/persistence/versionhistory"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/elasticsearch"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/mocks"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/service/config"
	"go.temporal.io/server/common/service/dynamicconfig"
)

type (
	adminHandlerSuite struct {
		suite.Suite
		*require.Assertions

		controller         *gomock.Controller
		mockResource       *resource.Test
		mockHistoryClient  *historyservicemock.MockHistoryServiceClient
		mockNamespaceCache *cache.MockNamespaceCache

		mockHistoryV2Mgr *mocks.HistoryV2Manager

		namespace   string
		namespaceID string

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

	s.controller = gomock.NewController(s.T())
	s.mockResource = resource.NewTest(s.controller, metrics.Frontend)
	s.mockNamespaceCache = s.mockResource.NamespaceCache
	s.mockHistoryClient = s.mockResource.HistoryClient
	s.mockHistoryV2Mgr = s.mockResource.HistoryMgr

	params := &resource.BootstrapParams{
		PersistenceConfig: config.Persistence{
			NumHistoryShards: 1,
		},
	}
	config := &Config{
		EnableAdminProtection: dynamicconfig.GetBoolPropertyFn(false),
	}
	s.handler = NewAdminHandler(s.mockResource, params, config)
	s.handler.Start()
}

func (s *adminHandlerSuite) TearDownTest() {
	s.controller.Finish()
	s.mockResource.Finish(s.T())
	s.handler.Stop()
}

func (s *adminHandlerSuite) Test_ConvertIndexedValueTypeToESDataType() {
	tests := []struct {
		input    enumspb.IndexedValueType
		expected string
	}{
		{
			input:    enumspb.INDEXED_VALUE_TYPE_STRING,
			expected: "text",
		},
		{
			input:    enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			expected: "keyword",
		},
		{
			input:    enumspb.INDEXED_VALUE_TYPE_INT,
			expected: "long",
		},
		{
			input:    enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			expected: "double",
		},
		{
			input:    enumspb.INDEXED_VALUE_TYPE_BOOL,
			expected: "boolean",
		},
		{
			input:    enumspb.INDEXED_VALUE_TYPE_DATETIME,
			expected: "date",
		},
		{
			input:    enumspb.IndexedValueType(-1),
			expected: "",
		},
	}

	for _, test := range tests {
		s.Equal(test.expected, s.handler.convertIndexedValueTypeToESDataType(test.input))
	}
}

func (s *adminHandlerSuite) Test_GetWorkflowExecutionRawHistoryV2_FailedOnInvalidWorkflowID() {

	ctx := context.Background()
	_, err := s.handler.GetWorkflowExecutionRawHistoryV2(ctx,
		&adminservice.GetWorkflowExecutionRawHistoryV2Request{
			Namespace: s.namespace,
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
			Namespace: s.namespace,
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
			Namespace: s.namespace,
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
	s.mockNamespaceCache.EXPECT().GetNamespaceID(s.namespace).Return("", fmt.Errorf("test")).Times(1)
	_, err := s.handler.GetWorkflowExecutionRawHistoryV2(ctx,
		&adminservice.GetWorkflowExecutionRawHistoryV2Request{
			Namespace: s.namespace,
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
	s.mockNamespaceCache.EXPECT().GetNamespaceID(s.namespace).Return(s.namespaceID, nil).AnyTimes()
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

	s.mockHistoryV2Mgr.On("ReadRawHistoryBranch", mock.Anything).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{},
		NextPageToken:     []byte{},
		Size:              0,
	}, nil)
	_, err := s.handler.GetWorkflowExecutionRawHistoryV2(ctx,
		&adminservice.GetWorkflowExecutionRawHistoryV2Request{
			Namespace: s.namespace,
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
	s.mockNamespaceCache.EXPECT().GetNamespaceID(s.namespace).Return(s.namespaceID, nil).AnyTimes()
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
			Namespace: s.namespace,
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
		Namespace: s.namespace,
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
		Namespace: s.namespace,
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
		Namespace: s.namespace,
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
		Namespace: s.namespace,
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

func (s *adminHandlerSuite) Test_AddSearchAttribute_Validate() {
	handler := s.handler
	handler.params = &resource.BootstrapParams{}
	ctx := context.Background()

	type test struct {
		Name     string
		Request  *adminservice.AddSearchAttributeRequest
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
			Request:  &adminservice.AddSearchAttributeRequest{},
			Expected: &serviceerror.InvalidArgument{Message: "SearchAttributes are not set on request."},
		},
		{
			Name: "no advanced config",
			Request: &adminservice.AddSearchAttributeRequest{
				SearchAttribute: map[string]enumspb.IndexedValueType{
					"CustomKeywordField": 1,
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "AdvancedVisibilityStore is not configured for this cluster."},
		},
	}
	for _, testCase := range testCases1 {
		resp, err := handler.AddSearchAttribute(ctx, testCase.Request)
		s.Equal(testCase.Expected, err)
		s.Nil(resp)
	}

	dynamicConfig := dynamicconfig.NewMockClient(s.controller)
	handler.params.DynamicConfig = dynamicConfig
	// add advanced visibility store related config
	handler.params.ESConfig = &elasticsearch.Config{}
	esClient := elasticsearch.NewMockClient(s.controller)
	handler.params.ESClient = esClient

	mockValidAttr := map[string]interface{}{
		"testkey": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	}
	dynamicConfig.EXPECT().GetMapValue(dynamicconfig.ValidSearchAttributes, nil, definition.GetDefaultIndexedKeys()).
		Return(mockValidAttr, nil).AnyTimes()

	testCases2 := []test{
		{
			Name: "reserved key",
			Request: &adminservice.AddSearchAttributeRequest{
				SearchAttribute: map[string]enumspb.IndexedValueType{
					"WorkflowId": 1,
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "Key [WorkflowId] is reserved by system."},
		},
		{
			Name: "key already whitelisted",
			Request: &adminservice.AddSearchAttributeRequest{
				SearchAttribute: map[string]enumspb.IndexedValueType{
					"testkey": 1,
				},
			},
			Expected: &serviceerror.InvalidArgument{Message: "Key [testkey] is already whitelist."},
		},
	}
	for _, testCase := range testCases2 {
		resp, err := handler.AddSearchAttribute(ctx, testCase.Request)
		s.Equal(testCase.Expected, err)
		s.Nil(resp)
	}

	dcUpdateTest := test{
		Name: "dynamic config update failed",
		Request: &adminservice.AddSearchAttributeRequest{
			SearchAttribute: map[string]enumspb.IndexedValueType{
				"testkey2": 1,
			},
		},
		Expected: &serviceerror.Internal{Message: "Failed to update dynamic config, err: error."},
	}
	dynamicConfig.EXPECT().UpdateValue(dynamicconfig.ValidSearchAttributes, map[string]interface{}{
		"testkey":  enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"testkey2": 1,
	}).Return(errors.New("error"))

	resp, err := handler.AddSearchAttribute(ctx, dcUpdateTest.Request)
	s.Equal(dcUpdateTest.Expected, err)
	s.Nil(resp)

	// ES operations tests
	dynamicConfig.EXPECT().UpdateValue(gomock.Any(), gomock.Any()).Return(nil).Times(2)

	convertFailedTest := test{
		Name: "unknown value type",
		Request: &adminservice.AddSearchAttributeRequest{
			SearchAttribute: map[string]enumspb.IndexedValueType{
				"testkey3": -1,
			},
		},
		Expected: &serviceerror.InvalidArgument{Message: "Unknown value type, -1."},
	}
	resp, err = handler.AddSearchAttribute(ctx, convertFailedTest.Request)
	s.Equal(convertFailedTest.Expected, err)
	s.Nil(resp)

	putMappingErr := errors.New("error")
	esClient.EXPECT().PutMapping(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(putMappingErr)
	esClient.EXPECT().IsNotFoundError(putMappingErr).
		Return(false)
	esErrorTest := test{
		Name: "es error",
		Request: &adminservice.AddSearchAttributeRequest{
			SearchAttribute: map[string]enumspb.IndexedValueType{
				"testkey4": 1,
			},
		},
		Expected: &serviceerror.Internal{Message: "Failed to update ES mapping, err: error."},
	}
	resp, err = handler.AddSearchAttribute(ctx, esErrorTest.Request)
	s.Equal(esErrorTest.Expected, err)
	s.Nil(resp)
}

func (s *adminHandlerSuite) Test_AddSearchAttribute_Permission() {
	ctx := context.Background()
	handler := s.handler
	handler.config = &Config{
		EnableAdminProtection: dynamicconfig.GetBoolPropertyFn(true),
		AdminOperationToken:   dynamicconfig.GetStringPropertyFn(common.DefaultAdminOperationToken),
	}

	type test struct {
		Name     string
		Request  *adminservice.AddSearchAttributeRequest
		Expected error
	}
	testCases := []test{
		{
			Name: "unknown token",
			Request: &adminservice.AddSearchAttributeRequest{
				SecurityToken: "unknown",
			},
			Expected: errNoPermission,
		},
		{
			Name: "correct token",
			Request: &adminservice.AddSearchAttributeRequest{
				SecurityToken: common.DefaultAdminOperationToken,
			},
			Expected: &serviceerror.InvalidArgument{Message: "SearchAttributes are not set on request."},
		},
	}
	for _, testCase := range testCases {
		resp, err := handler.AddSearchAttribute(ctx, testCase.Request)
		s.Equal(testCase.Expected, err)
		s.Nil(resp)
	}
}
