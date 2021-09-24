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

package visibility

import (
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/quotas"
)

type (
	// visibilityManager is responsible for:
	//  - throttle persistence requests,
	//  - inject metrics,
	//  - convert request (serialized some fields),
	//  - call underlying store (standard or advanced),
	//  - convert response.
	visibilityManager struct {
		store                    store.VisibilityStore
		readRateLimiter          quotas.RateLimiter
		writeRateLimiter         quotas.RateLimiter
		metricClient             metrics.Client
		visibilityTypeMetricsTag metrics.Tag
		logger                   log.Logger
	}
)

const (
	// MemoEncoding is default encoding for visibility memo.
	MemoEncoding = enumspb.ENCODING_TYPE_PROTO3
)

var _ manager.VisibilityManager = (*visibilityManager)(nil)

func newVisibilityManager(
	store store.VisibilityStore,
	readMaxQPS dynamicconfig.IntPropertyFn,
	writeMaxQPS dynamicconfig.IntPropertyFn,
	metricClient metrics.Client,
	visibilityTypeMetricsTag metrics.Tag,
	logger log.Logger,
) *visibilityManager {

	readRateLimiter := quotas.NewDefaultOutgoingDynamicRateLimiter(
		func() float64 { return float64(readMaxQPS()) },
	)

	writeRateLimiter := quotas.NewDefaultOutgoingDynamicRateLimiter(
		func() float64 { return float64(writeMaxQPS()) },
	)

	return &visibilityManager{
		store:                    store,
		readRateLimiter:          readRateLimiter,
		writeRateLimiter:         writeRateLimiter,
		visibilityTypeMetricsTag: visibilityTypeMetricsTag,
		metricClient:             metricClient,
		logger:                   logger,
	}
}

func (p *visibilityManager) Close() {
	p.store.Close()
}

func (p *visibilityManager) GetName() string {
	return p.store.GetName()
}

func (p *visibilityManager) RecordWorkflowExecutionStarted(request *manager.RecordWorkflowExecutionStartedRequest) error {
	if ok := p.writeRateLimiter.Allow(); !ok {
		return persistence.ErrPersistenceLimitExceeded
	}

	requestBase, err := p.newInternalVisibilityRequestBase(request.VisibilityRequestBase)
	if err != nil {
		return err
	}
	req := &store.InternalRecordWorkflowExecutionStartedRequest{
		InternalVisibilityRequestBase: requestBase,
	}

	scope := p.metricClient.Scope(metrics.VisibilityPersistenceRecordWorkflowExecutionStartedScope, p.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	err = p.store.RecordWorkflowExecutionStarted(req)
	if err != nil {
		p.updateErrorMetric(scope, err)
		return err
	}
	return nil
}

func (p *visibilityManager) RecordWorkflowExecutionClosed(request *manager.RecordWorkflowExecutionClosedRequest) error {
	if ok := p.writeRateLimiter.Allow(); !ok {
		return persistence.ErrPersistenceLimitExceeded
	}

	requestBase, err := p.newInternalVisibilityRequestBase(request.VisibilityRequestBase)
	if err != nil {
		return err
	}
	req := &store.InternalRecordWorkflowExecutionClosedRequest{
		InternalVisibilityRequestBase: requestBase,
		CloseTime:                     request.CloseTime,
		HistoryLength:                 request.HistoryLength,
		Retention:                     request.Retention,
	}
	scope := p.metricClient.Scope(metrics.VisibilityPersistenceRecordWorkflowExecutionClosedScope, p.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	err = p.store.RecordWorkflowExecutionClosed(req)
	if err != nil {
		p.updateErrorMetric(scope, err)
		return err
	}
	return nil
}

func (p *visibilityManager) UpsertWorkflowExecution(request *manager.UpsertWorkflowExecutionRequest) error {
	if ok := p.writeRateLimiter.Allow(); !ok {
		return persistence.ErrPersistenceLimitExceeded
	}
	requestBase, err := p.newInternalVisibilityRequestBase(request.VisibilityRequestBase)
	if err != nil {
		return err
	}
	req := &store.InternalUpsertWorkflowExecutionRequest{
		InternalVisibilityRequestBase: requestBase,
	}
	scope := p.metricClient.Scope(metrics.VisibilityPersistenceUpsertWorkflowExecutionScope, p.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	err = p.store.UpsertWorkflowExecution(req)
	if err != nil {
		p.updateErrorMetric(scope, err)
		return err
	}
	return nil
}

func (p *visibilityManager) DeleteWorkflowExecution(request *manager.VisibilityDeleteWorkflowExecutionRequest) error {
	if ok := p.writeRateLimiter.Allow(); !ok {
		return persistence.ErrPersistenceLimitExceeded
	}
	scope := p.metricClient.Scope(metrics.VisibilityPersistenceDeleteWorkflowExecutionScope, p.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	err := p.store.DeleteWorkflowExecution(request)
	if err != nil {
		p.updateErrorMetric(scope, err)
		return err
	}
	return nil
}

func (p *visibilityManager) ListOpenWorkflowExecutions(request *manager.ListWorkflowExecutionsRequest) (*manager.ListWorkflowExecutionsResponse, error) {
	if ok := p.readRateLimiter.Allow(); !ok {
		return nil, persistence.ErrPersistenceLimitExceeded
	}

	scope := p.metricClient.Scope(metrics.VisibilityPersistenceListOpenWorkflowExecutionsScope, p.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	response, err := p.store.ListOpenWorkflowExecutions(request)
	if err != nil {
		p.updateErrorMetric(scope, err)
		return nil, err
	}

	return p.convertInternalListResponse(response, scope)
}

func (p *visibilityManager) ListClosedWorkflowExecutions(request *manager.ListWorkflowExecutionsRequest) (*manager.ListWorkflowExecutionsResponse, error) {
	if ok := p.readRateLimiter.Allow(); !ok {
		return nil, persistence.ErrPersistenceLimitExceeded
	}

	scope := p.metricClient.Scope(metrics.VisibilityPersistenceListClosedWorkflowExecutionsScope, p.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	response, err := p.store.ListClosedWorkflowExecutions(request)
	if err != nil {
		p.updateErrorMetric(scope, err)
		return nil, err
	}

	return p.convertInternalListResponse(response, scope)
}

func (p *visibilityManager) ListOpenWorkflowExecutionsByType(request *manager.ListWorkflowExecutionsByTypeRequest) (*manager.ListWorkflowExecutionsResponse, error) {
	if ok := p.readRateLimiter.Allow(); !ok {
		return nil, persistence.ErrPersistenceLimitExceeded
	}

	scope := p.metricClient.Scope(metrics.VisibilityPersistenceListOpenWorkflowExecutionsByTypeScope, p.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	response, err := p.store.ListOpenWorkflowExecutionsByType(request)
	if err != nil {
		p.updateErrorMetric(scope, err)
		return nil, err
	}

	return p.convertInternalListResponse(response, scope)
}

func (p *visibilityManager) ListClosedWorkflowExecutionsByType(request *manager.ListWorkflowExecutionsByTypeRequest) (*manager.ListWorkflowExecutionsResponse, error) {
	if ok := p.readRateLimiter.Allow(); !ok {
		return nil, persistence.ErrPersistenceLimitExceeded
	}

	scope := p.metricClient.Scope(metrics.VisibilityPersistenceListClosedWorkflowExecutionsByTypeScope, p.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	response, err := p.store.ListClosedWorkflowExecutionsByType(request)
	if err != nil {
		p.updateErrorMetric(scope, err)
		return nil, err
	}

	return p.convertInternalListResponse(response, scope)
}

func (p *visibilityManager) ListOpenWorkflowExecutionsByWorkflowID(request *manager.ListWorkflowExecutionsByWorkflowIDRequest) (*manager.ListWorkflowExecutionsResponse, error) {
	if ok := p.readRateLimiter.Allow(); !ok {
		return nil, persistence.ErrPersistenceLimitExceeded
	}

	scope := p.metricClient.Scope(metrics.VisibilityPersistenceListOpenWorkflowExecutionsByWorkflowIDScope, p.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	response, err := p.store.ListOpenWorkflowExecutionsByWorkflowID(request)
	if err != nil {
		p.updateErrorMetric(scope, err)
		return nil, err
	}

	return p.convertInternalListResponse(response, scope)
}

func (p *visibilityManager) ListClosedWorkflowExecutionsByWorkflowID(request *manager.ListWorkflowExecutionsByWorkflowIDRequest) (*manager.ListWorkflowExecutionsResponse, error) {
	if ok := p.readRateLimiter.Allow(); !ok {
		return nil, persistence.ErrPersistenceLimitExceeded
	}

	scope := p.metricClient.Scope(metrics.VisibilityPersistenceListClosedWorkflowExecutionsByWorkflowIDScope, p.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	response, err := p.store.ListClosedWorkflowExecutionsByWorkflowID(request)
	if err != nil {
		p.updateErrorMetric(scope, err)
		return nil, err
	}

	return p.convertInternalListResponse(response, scope)
}

func (p *visibilityManager) ListClosedWorkflowExecutionsByStatus(request *manager.ListClosedWorkflowExecutionsByStatusRequest) (*manager.ListWorkflowExecutionsResponse, error) {
	if ok := p.readRateLimiter.Allow(); !ok {
		return nil, persistence.ErrPersistenceLimitExceeded
	}

	scope := p.metricClient.Scope(metrics.VisibilityPersistenceListClosedWorkflowExecutionsByStatusScope, p.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	response, err := p.store.ListClosedWorkflowExecutionsByStatus(request)
	if err != nil {
		p.updateErrorMetric(scope, err)
		return nil, err
	}

	return p.convertInternalListResponse(response, scope)
}

func (p *visibilityManager) ListWorkflowExecutions(request *manager.ListWorkflowExecutionsRequestV2) (*manager.ListWorkflowExecutionsResponse, error) {
	if ok := p.readRateLimiter.Allow(); !ok {
		return nil, persistence.ErrPersistenceLimitExceeded
	}

	scope := p.metricClient.Scope(metrics.VisibilityPersistenceListWorkflowExecutionsScope, p.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	response, err := p.store.ListWorkflowExecutions(request)
	if err != nil {
		p.updateErrorMetric(scope, err)
		return nil, err
	}

	return p.convertInternalListResponse(response, scope)
}

func (p *visibilityManager) ScanWorkflowExecutions(request *manager.ListWorkflowExecutionsRequestV2) (*manager.ListWorkflowExecutionsResponse, error) {
	if ok := p.readRateLimiter.Allow(); !ok {
		return nil, persistence.ErrPersistenceLimitExceeded
	}

	scope := p.metricClient.Scope(metrics.VisibilityPersistenceScanWorkflowExecutionsScope, p.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	response, err := p.store.ScanWorkflowExecutions(request)
	if err != nil {
		p.updateErrorMetric(scope, err)
		return nil, err
	}

	return p.convertInternalListResponse(response, scope)
}

func (p *visibilityManager) CountWorkflowExecutions(request *manager.CountWorkflowExecutionsRequest) (*manager.CountWorkflowExecutionsResponse, error) {
	if ok := p.readRateLimiter.Allow(); !ok {
		return nil, persistence.ErrPersistenceLimitExceeded
	}

	scope := p.metricClient.Scope(metrics.VisibilityPersistenceCountWorkflowExecutionsScope, p.visibilityTypeMetricsTag)

	scope.IncCounter(metrics.VisibilityPersistenceRequests)
	sw := scope.StartTimer(metrics.VisibilityPersistenceLatency)
	defer sw.Stop()
	response, err := p.store.CountWorkflowExecutions(request)
	if err != nil {
		p.updateErrorMetric(scope, err)
		return nil, err
	}

	return response, err
}

func (p *visibilityManager) newInternalVisibilityRequestBase(request *manager.VisibilityRequestBase) (*store.InternalVisibilityRequestBase, error) {
	if request == nil {
		return nil, nil
	}
	memoBlob, err := p.serializeMemo(request.Memo)
	if err != nil {
		return nil, err
	}

	return &store.InternalVisibilityRequestBase{
		NamespaceID:          request.NamespaceID,
		WorkflowID:           request.Execution.GetWorkflowId(),
		RunID:                request.Execution.GetRunId(),
		WorkflowTypeName:     request.WorkflowTypeName,
		StartTime:            request.StartTime,
		Status:               request.Status,
		ExecutionTime:        request.ExecutionTime,
		StateTransitionCount: request.StateTransitionCount,
		TaskID:               request.TaskID,
		ShardID:              request.ShardID,
		TaskQueue:            request.TaskQueue,
		Memo:                 memoBlob,
		SearchAttributes:     request.SearchAttributes,
	}, nil
}

func (p *visibilityManager) convertInternalListResponse(internalResponse *store.InternalListWorkflowExecutionsResponse, scope metrics.Scope) (*manager.ListWorkflowExecutionsResponse, error) {
	if internalResponse == nil {
		return nil, nil
	}

	resp := &manager.ListWorkflowExecutionsResponse{}
	resp.Executions = make([]*workflowpb.WorkflowExecutionInfo, len(internalResponse.Executions))
	for i, execution := range internalResponse.Executions {
		var err error
		resp.Executions[i], err = p.convertInternalWorkflowExecutionInfo(execution)
		if err != nil {
			p.updateErrorMetric(scope, err)
			return nil, err
		}
	}

	resp.NextPageToken = internalResponse.NextPageToken
	return resp, nil
}

func (p *visibilityManager) convertInternalWorkflowExecutionInfo(internalExecution *store.InternalWorkflowExecutionInfo) (*workflowpb.WorkflowExecutionInfo, error) {
	if internalExecution == nil {
		return nil, nil
	}
	memo, err := p.deserializeMemo(internalExecution.Memo)
	if err != nil {
		return nil, err
	}

	executionInfo := &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: internalExecution.WorkflowID,
			RunId:      internalExecution.RunID,
		},
		Type: &commonpb.WorkflowType{
			Name: internalExecution.TypeName,
		},
		StartTime:            &internalExecution.StartTime,
		ExecutionTime:        &internalExecution.ExecutionTime,
		Memo:                 memo,
		SearchAttributes:     internalExecution.SearchAttributes,
		TaskQueue:            internalExecution.TaskQueue,
		Status:               internalExecution.Status,
		StateTransitionCount: internalExecution.StateTransitionCount,
	}

	// for close records
	if internalExecution.Status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		executionInfo.CloseTime = &internalExecution.CloseTime
		executionInfo.HistoryLength = internalExecution.HistoryLength
	}

	// Workflows created before 1.11 have ExecutionTime set to Unix epoch zero time (1/1/1970) for non-cron/non-retry case.
	// Use StartTime as ExecutionTime for this case (if there was a backoff it must be set).
	// Remove this "if" block when ExecutionTime field has actual correct value (added 6/9/21).
	// Affects only non-advanced visibility.
	if !executionInfo.ExecutionTime.After(time.Unix(0, 0)) {
		executionInfo.ExecutionTime = executionInfo.StartTime
	}

	return executionInfo, nil
}
func (p *visibilityManager) deserializeMemo(data *commonpb.DataBlob) (*commonpb.Memo, error) {
	if data == nil || len(data.Data) == 0 {
		return &commonpb.Memo{}, nil
	}

	var ()
	switch data.EncodingType {
	case enumspb.ENCODING_TYPE_PROTO3:
		memo := &commonpb.Memo{}
		err := proto.Unmarshal(data.Data, memo)
		if err != nil {
			return nil, serviceerror.NewInternal(fmt.Sprintf("Unable to deserialize memo from data blob: %v", err))
		}
		return memo, nil
	default:
		return nil, serviceerror.NewInternal(fmt.Sprintf("Invalid memo encoding in database: %s", data.GetEncodingType().String()))
	}
}

func (p *visibilityManager) serializeMemo(memo *commonpb.Memo) (*commonpb.DataBlob, error) {
	if memo == nil {
		memo = &commonpb.Memo{}
	}

	data, err := proto.Marshal(memo)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("Unable to serialize memo to data blob: %v", err))
	}

	return &commonpb.DataBlob{
		Data:         data,
		EncodingType: MemoEncoding,
	}, nil
}

func (p *visibilityManager) updateErrorMetric(scope metrics.Scope, err error) {
	switch err.(type) {
	case *serviceerror.InvalidArgument:
		scope.IncCounter(metrics.VisibilityPersistenceInvalidArgument)
		scope.IncCounter(metrics.VisibilityPersistenceFailures)
	case *persistence.TimeoutError:
		scope.IncCounter(metrics.VisibilityPersistenceTimeout)
		scope.IncCounter(metrics.VisibilityPersistenceFailures)
	case *serviceerror.ResourceExhausted:
		scope.IncCounter(metrics.VisibilityPersistenceResourceExhausted)
		scope.IncCounter(metrics.VisibilityPersistenceFailures)
	case *serviceerror.Internal:
		scope.IncCounter(metrics.VisibilityPersistenceInternal)
		scope.IncCounter(metrics.VisibilityPersistenceFailures)
	case *serviceerror.Unavailable:
		scope.IncCounter(metrics.VisibilityPersistenceUnavailable)
		scope.IncCounter(metrics.VisibilityPersistenceFailures)
	case *persistence.ConditionFailedError:
		scope.IncCounter(metrics.VisibilityPersistenceConditionFailed)
	case *serviceerror.NotFound:
		scope.IncCounter(metrics.VisibilityPersistenceNotFound)
	default:
		p.logger.Error("Operation failed with an error.", tag.Error(err))
		scope.IncCounter(metrics.VisibilityPersistenceFailures)
	}
}
