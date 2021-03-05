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

package persistence

import (
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/service/dynamicconfig"
)

type (
	visibilityManagerImpl struct {
		serializer            PayloadSerializer
		persistence           VisibilityStore
		validSearchAttributes dynamicconfig.MapPropertyFn
		logger                log.Logger
	}
)

// VisibilityEncoding is default encoding for visibility data
const VisibilityEncoding = enumspb.ENCODING_TYPE_PROTO3

var _ VisibilityManager = (*visibilityManagerImpl)(nil)

// NewVisibilityManagerImpl returns new VisibilityManager
func NewVisibilityManagerImpl(persistence VisibilityStore, validSearchAttributes dynamicconfig.MapPropertyFn, logger log.Logger) VisibilityManager {
	return &visibilityManagerImpl{
		serializer:            NewPayloadSerializer(),
		persistence:           persistence,
		validSearchAttributes: validSearchAttributes,
		logger:                logger,
	}
}

func (v *visibilityManagerImpl) Close() {
	v.persistence.Close()
}

func (v *visibilityManagerImpl) GetName() string {
	return v.persistence.GetName()
}

func (v *visibilityManagerImpl) RecordWorkflowExecutionStarted(request *RecordWorkflowExecutionStartedRequest) error {
	req := &InternalRecordWorkflowExecutionStartedRequest{
		InternalVisibilityRequestBase: v.newInternalVisibilityRequestBase(request.VisibilityRequestBase),
		RunTimeout:                    request.RunTimeout,
	}
	return v.persistence.RecordWorkflowExecutionStarted(req)
}

func (v *visibilityManagerImpl) RecordWorkflowExecutionClosed(request *RecordWorkflowExecutionClosedRequest) error {
	req := &InternalRecordWorkflowExecutionClosedRequest{
		InternalVisibilityRequestBase: v.newInternalVisibilityRequestBase(request.VisibilityRequestBase),
		CloseTimestamp:                request.CloseTimestamp,
		HistoryLength:                 request.HistoryLength,
		RetentionSeconds:              request.RetentionSeconds,
	}
	return v.persistence.RecordWorkflowExecutionClosed(req)
}

func (v *visibilityManagerImpl) UpsertWorkflowExecution(request *UpsertWorkflowExecutionRequest) error {
	req := &InternalUpsertWorkflowExecutionRequest{
		InternalVisibilityRequestBase: v.newInternalVisibilityRequestBase(request.VisibilityRequestBase),
		WorkflowTimeout:               request.WorkflowTimeout,
	}
	return v.persistence.UpsertWorkflowExecution(req)
}

func (v *visibilityManagerImpl) newInternalVisibilityRequestBase(request *VisibilityRequestBase) *InternalVisibilityRequestBase {
	return &InternalVisibilityRequestBase{
		NamespaceID:        request.NamespaceID,
		WorkflowID:         request.Execution.GetWorkflowId(),
		RunID:              request.Execution.GetRunId(),
		WorkflowTypeName:   request.WorkflowTypeName,
		StartTimestamp:     request.StartTimestamp,
		Status:             request.Status,
		ExecutionTimestamp: request.ExecutionTimestamp,
		TaskID:             request.TaskID,
		ShardID:            request.ShardID,
		TaskQueue:          request.TaskQueue,
		Memo:               v.serializeMemo(request.Memo, request.NamespaceID, request.Execution.GetWorkflowId(), request.Execution.GetRunId()),
		SearchAttributes:   request.SearchAttributes,
	}
}

func (v *visibilityManagerImpl) ListOpenWorkflowExecutions(request *ListWorkflowExecutionsRequest) (*ListWorkflowExecutionsResponse, error) {
	internalResp, err := v.persistence.ListOpenWorkflowExecutions(request)
	if err != nil {
		return nil, err
	}
	return v.convertInternalListResponse(internalResp), nil
}

func (v *visibilityManagerImpl) ListClosedWorkflowExecutions(request *ListWorkflowExecutionsRequest) (*ListWorkflowExecutionsResponse, error) {
	internalResp, err := v.persistence.ListClosedWorkflowExecutions(request)
	if err != nil {
		return nil, err
	}
	return v.convertInternalListResponse(internalResp), nil
}

func (v *visibilityManagerImpl) ListOpenWorkflowExecutionsByType(request *ListWorkflowExecutionsByTypeRequest) (*ListWorkflowExecutionsResponse, error) {
	internalResp, err := v.persistence.ListOpenWorkflowExecutionsByType(request)
	if err != nil {
		return nil, err
	}
	return v.convertInternalListResponse(internalResp), nil
}

func (v *visibilityManagerImpl) ListClosedWorkflowExecutionsByType(request *ListWorkflowExecutionsByTypeRequest) (*ListWorkflowExecutionsResponse, error) {
	internalResp, err := v.persistence.ListClosedWorkflowExecutionsByType(request)
	if err != nil {
		return nil, err
	}
	return v.convertInternalListResponse(internalResp), nil
}

func (v *visibilityManagerImpl) ListOpenWorkflowExecutionsByWorkflowID(request *ListWorkflowExecutionsByWorkflowIDRequest) (*ListWorkflowExecutionsResponse, error) {
	internalResp, err := v.persistence.ListOpenWorkflowExecutionsByWorkflowID(request)
	if err != nil {
		return nil, err
	}
	return v.convertInternalListResponse(internalResp), nil
}

func (v *visibilityManagerImpl) ListClosedWorkflowExecutionsByWorkflowID(request *ListWorkflowExecutionsByWorkflowIDRequest) (*ListWorkflowExecutionsResponse, error) {
	internalResp, err := v.persistence.ListClosedWorkflowExecutionsByWorkflowID(request)
	if err != nil {
		return nil, err
	}
	return v.convertInternalListResponse(internalResp), nil
}

func (v *visibilityManagerImpl) ListClosedWorkflowExecutionsByStatus(request *ListClosedWorkflowExecutionsByStatusRequest) (*ListWorkflowExecutionsResponse, error) {
	internalResp, err := v.persistence.ListClosedWorkflowExecutionsByStatus(request)
	if err != nil {
		return nil, err
	}
	return v.convertInternalListResponse(internalResp), nil
}

func (v *visibilityManagerImpl) GetClosedWorkflowExecution(request *GetClosedWorkflowExecutionRequest) (*GetClosedWorkflowExecutionResponse, error) {
	internalResp, err := v.persistence.GetClosedWorkflowExecution(request)
	if err != nil {
		return nil, err
	}
	return v.convertInternalGetResponse(internalResp), nil
}

func (v *visibilityManagerImpl) DeleteWorkflowExecution(request *VisibilityDeleteWorkflowExecutionRequest) error {
	return v.persistence.DeleteWorkflowExecution(request)
}

func (v *visibilityManagerImpl) ListWorkflowExecutions(request *ListWorkflowExecutionsRequestV2) (*ListWorkflowExecutionsResponse, error) {
	internalResp, err := v.persistence.ListWorkflowExecutions(request)
	if err != nil {
		return nil, err
	}
	return v.convertInternalListResponse(internalResp), nil
}

func (v *visibilityManagerImpl) ScanWorkflowExecutions(request *ListWorkflowExecutionsRequestV2) (*ListWorkflowExecutionsResponse, error) {
	internalResp, err := v.persistence.ScanWorkflowExecutions(request)
	if err != nil {
		return nil, err
	}
	return v.convertInternalListResponse(internalResp), nil
}

func (v *visibilityManagerImpl) CountWorkflowExecutions(request *CountWorkflowExecutionsRequest) (*CountWorkflowExecutionsResponse, error) {
	return v.persistence.CountWorkflowExecutions(request)
}

func (v *visibilityManagerImpl) convertInternalGetResponse(internalResp *InternalGetClosedWorkflowExecutionResponse) *GetClosedWorkflowExecutionResponse {
	if internalResp == nil {
		return nil
	}

	resp := &GetClosedWorkflowExecutionResponse{}
	saTypeMap, err := searchattribute.BuildTypeMap(v.validSearchAttributes)
	if err != nil {
		v.logger.Error("Unable to build valid search attributes map", tag.Error(err))
	}
	resp.Execution = v.convertVisibilityWorkflowExecutionInfo(internalResp.Execution, saTypeMap)
	return resp
}

func (v *visibilityManagerImpl) convertInternalListResponse(internalResp *InternalListWorkflowExecutionsResponse) *ListWorkflowExecutionsResponse {
	if internalResp == nil {
		return nil
	}

	resp := &ListWorkflowExecutionsResponse{}
	resp.Executions = make([]*workflowpb.WorkflowExecutionInfo, len(internalResp.Executions))
	saTypeMap, err := searchattribute.BuildTypeMap(v.validSearchAttributes)
	if err != nil {
		v.logger.Error("Unable to build valid search attributes map", tag.Error(err))
	}
	for i, execution := range internalResp.Executions {
		resp.Executions[i] = v.convertVisibilityWorkflowExecutionInfo(execution, saTypeMap)
	}

	resp.NextPageToken = internalResp.NextPageToken
	return resp
}

func (v *visibilityManagerImpl) convertVisibilityWorkflowExecutionInfo(execution *VisibilityWorkflowExecutionInfo, saTypeMap map[string]enumspb.IndexedValueType) *workflowpb.WorkflowExecutionInfo {
	// special handling of ExecutionTime for cron or retry
	if execution.ExecutionTime.UnixNano() == 0 {
		execution.ExecutionTime = execution.StartTime
	}

	memo, err := v.serializer.DeserializeVisibilityMemo(execution.Memo)
	if err != nil {
		v.logger.Error("failed to deserialize memo",
			tag.WorkflowID(execution.WorkflowID),
			tag.WorkflowRunID(execution.RunID),
			tag.Error(err))
	}
	searchAttributes, err := searchattribute.Encode(execution.SearchAttributes, saTypeMap)
	if err != nil {
		v.logger.Error("failed to encode search attributes",
			tag.WorkflowID(execution.WorkflowID),
			tag.WorkflowRunID(execution.RunID),
			tag.Error(err))
	}

	convertedExecution := &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: execution.WorkflowID,
			RunId:      execution.RunID,
		},
		Type: &commonpb.WorkflowType{
			Name: execution.TypeName,
		},
		StartTime:        &execution.StartTime,
		ExecutionTime:    &execution.ExecutionTime,
		Memo:             memo,
		SearchAttributes: searchAttributes,
		TaskQueue:        execution.TaskQueue,
		Status:           execution.Status,
	}

	// for close records
	if execution.Status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		convertedExecution.CloseTime = &execution.CloseTime
		convertedExecution.HistoryLength = execution.HistoryLength
	}

	return convertedExecution
}

func (v *visibilityManagerImpl) serializeMemo(visibilityMemo *commonpb.Memo, namespaceID, wID, rID string) *commonpb.DataBlob {
	memo, err := v.serializer.SerializeVisibilityMemo(visibilityMemo, VisibilityEncoding)
	if err != nil {
		v.logger.WithTags(
			tag.WorkflowNamespaceID(namespaceID),
			tag.WorkflowID(wID),
			tag.WorkflowRunID(rID),
			tag.Error(err)).
			Error("Unable to encode visibility memo")
	}
	if memo == nil {
		return &commonpb.DataBlob{}
	}
	return memo
}
