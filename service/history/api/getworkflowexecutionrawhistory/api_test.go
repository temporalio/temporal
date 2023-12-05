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

package getworkflowexecutionrawhistory

import (
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/api/adminservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence/versionhistory"
)

// DEPRECATED: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/history/historyEngine_test.go
func Test_SetRequestDefaultValueAndGetTargetVersionHistory_DefinedStartAndEnd(t *testing.T) {
	inputStartEventID := int64(1)
	inputStartVersion := int64(10)
	inputEndEventID := int64(100)
	inputEndVersion := int64(11)
	firstItem := versionhistory.NewVersionHistoryItem(inputStartEventID, inputStartVersion)
	endItem := versionhistory.NewVersionHistoryItem(inputEndEventID, inputEndVersion)
	versionHistory := versionhistory.NewVersionHistory([]byte{}, []*historyspb.VersionHistoryItem{firstItem, endItem})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	namespaceId := uuid.New()
	request := &adminservice.GetWorkflowExecutionRawHistoryRequest{
		NamespaceId: namespaceId,
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
	req := &historyservice.GetWorkflowExecutionRawHistoryRequest{
		Request: request,
	}

	targetVersionHistory, err := setRequestDefaultValueAndGetTargetVersionHistory(
		req,
		versionHistories,
	)
	assert.Equal(t, request.GetStartEventId(), inputStartEventID)
	assert.Equal(t, request.GetEndEventId(), inputEndEventID)
	assert.Equal(t, targetVersionHistory, versionHistory)
	assert.NoError(t, err)
}

func Test_SetRequestDefaultValueAndGetTargetVersionHistory_DefinedEndEvent(t *testing.T) {
	inputStartEventID := int64(1)
	inputEndEventID := int64(100)
	inputStartVersion := int64(10)
	inputEndVersion := int64(11)
	namespaceId := uuid.New()
	firstItem := versionhistory.NewVersionHistoryItem(inputStartEventID, inputStartVersion)
	targetItem := versionhistory.NewVersionHistoryItem(inputEndEventID, inputEndVersion)
	versionHistory := versionhistory.NewVersionHistory([]byte{}, []*historyspb.VersionHistoryItem{firstItem, targetItem})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	request := &adminservice.GetWorkflowExecutionRawHistoryRequest{
		NamespaceId: namespaceId,
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
	req := &historyservice.GetWorkflowExecutionRawHistoryRequest{
		Request: request,
	}
	targetVersionHistory, err := setRequestDefaultValueAndGetTargetVersionHistory(
		req,
		versionHistories,
	)
	assert.Equal(t, request.GetStartEventId(), inputStartEventID)
	assert.Equal(t, request.GetEndEventId(), inputEndEventID)
	assert.Equal(t, targetVersionHistory, versionHistory)
	assert.NoError(t, err)
}

func Test_SetRequestDefaultValueAndGetTargetVersionHistory_DefinedStartEvent(t *testing.T) {
	inputStartEventID := int64(1)
	inputEndEventID := int64(100)
	inputStartVersion := int64(10)
	inputEndVersion := int64(11)
	firstItem := versionhistory.NewVersionHistoryItem(inputStartEventID, inputStartVersion)
	targetItem := versionhistory.NewVersionHistoryItem(inputEndEventID, inputEndVersion)
	versionHistory := versionhistory.NewVersionHistory([]byte{}, []*historyspb.VersionHistoryItem{firstItem, targetItem})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	namespaceId := uuid.New()

	request := &adminservice.GetWorkflowExecutionRawHistoryRequest{
		NamespaceId: namespaceId,
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
	req := &historyservice.GetWorkflowExecutionRawHistoryRequest{
		Request: request,
	}
	targetVersionHistory, err := setRequestDefaultValueAndGetTargetVersionHistory(
		req,
		versionHistories,
	)
	assert.Equal(t, request.GetStartEventId(), inputStartEventID)
	assert.Equal(t, request.GetEndEventId(), inputEndEventID)
	assert.Equal(t, targetVersionHistory, versionHistory)
	assert.NoError(t, err)
}

func Test_SetRequestDefaultValueAndGetTargetVersionHistory_NonCurrentBranch(t *testing.T) {
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
	assert.NoError(t, err)
	request := &adminservice.GetWorkflowExecutionRawHistoryRequest{
		NamespaceId: uuid.New(),
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

	req := &historyservice.GetWorkflowExecutionRawHistoryRequest{
		Request: request,
	}
	targetVersionHistory, err := setRequestDefaultValueAndGetTargetVersionHistory(
		req,
		versionHistories,
	)
	assert.Equal(t, request.GetStartEventId(), inputStartEventID)
	assert.Equal(t, request.GetEndEventId(), inputEndEventID)
	assert.Equal(t, targetVersionHistory, versionHistory1)
	assert.NoError(t, err)
}
