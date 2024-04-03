// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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
	"testing"

	"github.com/golang/mock/gomock"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/schedule/v1"
	workflowpb "go.temporal.io/api/workflow/v1"

	schedspb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/searchattribute"
)

func TestDescribeScheduleAnnotatesScheduledWorkflowWithTypes(t *testing.T) {
	makeWorkflowHandler := func(
		visibilityManager *manager.MockVisibilityManager,
		searchAttrProvider *searchattribute.MockProvider,
	) WorkflowHandler {
		h := WorkflowHandler{
			saMapperProvider: searchattribute.NewTestMapperProvider(&searchattribute.TestMapper{Namespace: "ns"}),
			visibilityMgr:    visibilityManager,
			saProvider:       searchAttrProvider,
		}
		return h
	}
	controller := gomock.NewController(t)
	visibilityManager := makeVisibilityManagerStub(controller)
	searchAttrProvider := makeSearchAttributesProviderStub(
		controller,
		map[string]enumspb.IndexedValueType{
			"CustomKeywordField": enumspb.INDEXED_VALUE_TYPE_TEXT,
			"CustomBoolField":    enumspb.INDEXED_VALUE_TYPE_BOOL,
		},
	)
	response := makeResponseWithScheduledWorkflowAttributes(
		map[string]string{"CustomKeywordField": "keyword", "CustomBoolField": "true"})

	h := makeWorkflowHandler(visibilityManager, searchAttrProvider)

	err := h.annotateSearchAttributesOfScheduledWorkflow(response, "ns")

	if err != nil {
		t.Fatalf("error %v", err)
	}
	assertScheduledWorkflowSearchAttributeHasAssociatedTypeOf(
		t, response, "CustomKeywordField", enumspb.INDEXED_VALUE_TYPE_TEXT)
	assertScheduledWorkflowSearchAttributeHasAssociatedTypeOf(
		t, response, "CustomBoolField", enumspb.INDEXED_VALUE_TYPE_BOOL)
}

func assertScheduledWorkflowSearchAttributeHasAssociatedTypeOf(
	t *testing.T,
	response *schedspb.DescribeResponse,
	searchAttribute string,
	expectedType enumspb.IndexedValueType,
) {
	t.Helper()
	attributes := getScheduledWorkflowSearchAttributes(response)
	actualAttributeType := attributes.IndexedFields["AliasFor"+searchAttribute].Metadata[searchattribute.MetadataType]
	if string(actualAttributeType) != expectedType.String() {
		t.Errorf(
			"expected type %s for attribute %s, got %s",
			expectedType.String(),
			searchAttribute,
			string(actualAttributeType),
		)
	}
}

func getScheduledWorkflowSearchAttributes(response *schedspb.DescribeResponse) *commonpb.SearchAttributes {
	return response.Schedule.Action.Action.(*schedule.ScheduleAction_StartWorkflow).StartWorkflow.SearchAttributes
}

func makeSearchAttributesProviderStub(
	controller *gomock.Controller,
	custom map[string]enumspb.IndexedValueType,
) *searchattribute.MockProvider {
	searchAttrProvider := searchattribute.NewMockProvider(controller)
	nameTypeMapStub := searchattribute.NewNameTypeMapStub(custom)
	searchAttrProvider.EXPECT().GetSearchAttributes("index", false).Return(nameTypeMapStub, nil).AnyTimes()
	return searchAttrProvider
}

func makeVisibilityManagerStub(controller *gomock.Controller) *manager.MockVisibilityManager {
	visibilityManager := manager.NewMockVisibilityManager(controller)
	visibilityManager.EXPECT().GetIndexName().Return("index").AnyTimes()
	return visibilityManager
}

func makeResponseWithScheduledWorkflowAttributes(nameValueMap map[string]string) *schedspb.DescribeResponse {
	attributes := commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{},
	}
	for name, value := range nameValueMap {
		attributes.IndexedFields["AliasFor"+name] = payload.EncodeString(value)
	}

	response := schedspb.DescribeResponse{
		Schedule: &schedule.Schedule{
			Action: &schedule.ScheduleAction{
				Action: &schedule.ScheduleAction_StartWorkflow{
					StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
						SearchAttributes: &attributes,
					},
				},
			},
		},
	}
	return &response
}
