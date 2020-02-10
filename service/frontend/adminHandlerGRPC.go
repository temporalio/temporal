// Copyright (c) 2019 Temporal Technologies, Inc.
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
	"strconv"

	"github.com/gogo/status"
	"github.com/pborman/uuid"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"

	"github.com/temporalio/temporal/.gen/go/shared"
	"github.com/temporalio/temporal/.gen/proto/adminservice"
	"github.com/temporalio/temporal/.gen/proto/historyservice"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/adapter"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/resource"
	"github.com/temporalio/temporal/common/service"
)

var _ adminservice.AdminServiceServer = (*AdminHandlerGRPC)(nil)

const (
	getDomainReplicationMessageBatchSize = 100
	defaultLastMessageID                 = -1
)

type (
	// AdminHandlerGRPC - gRPC handler interface for workflow workflowservice
	AdminHandlerGRPC struct {
		resource.Resource

		adminHandlerThrift    *AdminHandler
		numberOfHistoryShards int
		params                *service.BootstrapParams
		config                *Config
	}
)

// NewAdminHandlerGRPC creates a thrift handler for the cadence workflowservice
func NewAdminHandlerGRPC(
	resource resource.Resource,
	adminHandlerThrift *AdminHandler,
	params *service.BootstrapParams,
	config *Config,
) *AdminHandlerGRPC {
	return &AdminHandlerGRPC{
		Resource:              resource,
		adminHandlerThrift:    adminHandlerThrift,
		numberOfHistoryShards: params.PersistenceConfig.NumHistoryShards,
		params:                params,
		config:                config,
	}
}

// Start starts the handler
func (adh *AdminHandlerGRPC) Start() {
	// Start domain replication queue cleanup
	adh.Resource.GetDomainReplicationQueue().Start()
}

// Stop stops the handler
func (adh *AdminHandlerGRPC) Stop() {
	adh.Resource.GetDomainReplicationQueue().Stop()
}

// DescribeWorkflowExecution returns information about the specified workflow execution.
func (adh *AdminHandlerGRPC) DescribeWorkflowExecution(ctx context.Context, request *adminservice.DescribeWorkflowExecutionRequest) (_ *adminservice.DescribeWorkflowExecutionResponse, retError error) {
	defer log.CapturePanicGRPC(adh.GetLogger(), &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminDescribeWorkflowExecutionScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}

	if err := adh.validateExecution(request.Execution); err != nil {
		return nil, adh.error(err, scope)
	}

	shardID := common.WorkflowIDToHistoryShard(request.Execution.WorkflowId, adh.numberOfHistoryShards)
	shardIDstr := string(shardID)
	shardIDForOutput := strconv.Itoa(shardID)

	historyHost, err := adh.GetMembershipMonitor().Lookup(common.HistoryServiceName, shardIDstr)
	if err != nil {
		return nil, adh.error(err, scope)
	}

	domainID, err := adh.GetDomainCache().GetDomainID(request.GetDomain())

	historyAddr := historyHost.GetAddress()
	resp2, err := adh.GetHistoryClientGRPC().DescribeMutableState(ctx, &historyservice.DescribeMutableStateRequest{
		DomainUUID: domainID,
		Execution:  request.Execution,
	})

	if err != nil {
		return &adminservice.DescribeWorkflowExecutionResponse{}, err
	}
	return &adminservice.DescribeWorkflowExecutionResponse{
		ShardId:                shardIDForOutput,
		HistoryAddr:            historyAddr,
		MutableStateInDatabase: resp2.MutableStateInDatabase,
		MutableStateInCache:    resp2.MutableStateInCache,
	}, err
}

// DescribeHistoryHost ...
func (adh *AdminHandlerGRPC) DescribeHistoryHost(ctx context.Context, request *adminservice.DescribeHistoryHostRequest) (_ *adminservice.DescribeHistoryHostResponse, retError error) {
	defer log.CapturePanicGRPC(adh.GetLogger(), &retError)

	resp, err := adh.adminHandlerThrift.DescribeHistoryHost(ctx, adapter.ToThriftDescribeHistoryHostRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoDescribeHistoryHostResponse(resp), nil
}

// CloseShard ...
func (adh *AdminHandlerGRPC) CloseShard(ctx context.Context, request *adminservice.CloseShardRequest) (_ *adminservice.CloseShardResponse, retError error) {
	defer log.CapturePanicGRPC(adh.GetLogger(), &retError)

	err := adh.adminHandlerThrift.CloseShard(ctx, adapter.ToThriftCloseShardRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &adminservice.CloseShardResponse{}, nil
}

// RemoveTask ...
func (adh *AdminHandlerGRPC) RemoveTask(ctx context.Context, request *adminservice.RemoveTaskRequest) (_ *adminservice.RemoveTaskResponse, retError error) {
	defer log.CapturePanicGRPC(adh.GetLogger(), &retError)

	err := adh.adminHandlerThrift.RemoveTask(ctx, adapter.ToThriftRemoveTaskRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &adminservice.RemoveTaskResponse{}, nil
}

// GetWorkflowExecutionRawHistory ...
func (adh *AdminHandlerGRPC) GetWorkflowExecutionRawHistory(ctx context.Context, request *adminservice.GetWorkflowExecutionRawHistoryRequest) (_ *adminservice.GetWorkflowExecutionRawHistoryResponse, retError error) {
	defer log.CapturePanicGRPC(adh.GetLogger(), &retError)

	resp, err := adh.adminHandlerThrift.GetWorkflowExecutionRawHistory(ctx, adapter.ToThriftAdminGetWorkflowExecutionRawHistoryRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoAdminGetWorkflowExecutionRawHistoryResponse(resp), nil
}

// GetWorkflowExecutionRawHistoryV2 ...
func (adh *AdminHandlerGRPC) GetWorkflowExecutionRawHistoryV2(ctx context.Context, request *adminservice.GetWorkflowExecutionRawHistoryV2Request) (_ *adminservice.GetWorkflowExecutionRawHistoryV2Response, retError error) {
	defer log.CapturePanicGRPC(adh.GetLogger(), &retError)

	resp, err := adh.adminHandlerThrift.GetWorkflowExecutionRawHistoryV2(ctx, adapter.ToThriftGetWorkflowExecutionRawHistoryV2Request(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoGetWorkflowExecutionRawHistoryV2Response(resp), nil
}

// AddSearchAttribute ...
func (adh *AdminHandlerGRPC) AddSearchAttribute(ctx context.Context, request *adminservice.AddSearchAttributeRequest) (_ *adminservice.AddSearchAttributeResponse, retError error) {
	defer log.CapturePanicGRPC(adh.GetLogger(), &retError)

	err := adh.adminHandlerThrift.AddSearchAttribute(ctx, adapter.ToThriftAddSearchAttributeRequest(request))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &adminservice.AddSearchAttributeResponse{}, nil
}

// DescribeCluster ...
func (adh *AdminHandlerGRPC) DescribeCluster(ctx context.Context, _ *adminservice.DescribeClusterRequest) (_ *adminservice.DescribeClusterResponse, retError error) {
	defer log.CapturePanicGRPC(adh.GetLogger(), &retError)

	resp, err := adh.adminHandlerThrift.DescribeCluster(ctx)
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return adapter.ToProtoDescribeClusterResponse(resp), nil
}

// GetReplicationMessages returns new replication tasks since the read level provided in the token.
func (adh *AdminHandlerGRPC) GetReplicationMessages(ctx context.Context, request *adminservice.GetReplicationMessagesRequest) (_ *adminservice.GetReplicationMessagesResponse, retError error) {
	defer log.CapturePanicGRPC(adh.GetLogger(), &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminGetReplicationMessagesScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}
	if request.GetClusterName() == "" {
		return nil, adh.error(errClusterNameNotSet, scope)
	}

	resp, err := adh.GetHistoryClientGRPC().GetReplicationMessages(ctx, &historyservice.GetReplicationMessagesRequest{
		Tokens:      request.GetTokens(),
		ClusterName: request.GetClusterName(),
	})
	if err != nil {
		return nil, adh.error(err, scope)
	}
	return &adminservice.GetReplicationMessagesResponse{MessagesByShard: resp.GetMessagesByShard()}, nil
}

// GetDomainReplicationMessages returns new domain replication tasks since last retrieved task ID.
func (adh *AdminHandlerGRPC) GetDomainReplicationMessages(ctx context.Context, request *adminservice.GetDomainReplicationMessagesRequest) (_ *adminservice.GetDomainReplicationMessagesResponse, retError error) {
	defer log.CapturePanicGRPC(adh.GetLogger(), &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminGetDomainReplicationMessagesScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}

	if adh.GetDomainReplicationQueue() == nil {
		return nil, adh.error(errors.New("domain replication queue not enabled for cluster"), scope)
	}

	lastMessageID := defaultLastMessageID
	if request.GetLastRetrievedMessageId() == defaultLastMessageID {
		clusterAckLevels, err := adh.GetDomainReplicationQueue().GetAckLevels()
		if err == nil {
			if ackLevel, ok := clusterAckLevels[request.GetClusterName()]; ok {
				lastMessageID = ackLevel
			}
		}
	}

	replicationTasks, lastMessageID, err := adh.GetDomainReplicationQueue().GetReplicationMessages(
		lastMessageID, getDomainReplicationMessageBatchSize)
	if err != nil {
		return nil, adh.error(err, scope)
	}

	if request.GetLastProcessedMessageId() != defaultLastMessageID {
		err := adh.GetDomainReplicationQueue().UpdateAckLevel(int(request.GetLastProcessedMessageId()), request.GetClusterName())
		if err != nil {
			adh.GetLogger().Warn("Failed to update domain replication queue ack level",
				tag.TaskID(request.GetLastProcessedMessageId()),
				tag.ClusterName(request.GetClusterName()))
		}
	}

	return &adminservice.GetDomainReplicationMessagesResponse{
		Messages: &commonproto.ReplicationMessages{
			ReplicationTasks:       adapter.ToProtoReplicationTasks(replicationTasks),
			LastRetrievedMessageId: int64(lastMessageID),
		},
	}, nil
}

// GetDLQReplicationMessages returns new replication tasks based on the dlq info.
func (adh *AdminHandlerGRPC) GetDLQReplicationMessages(ctx context.Context, request *adminservice.GetDLQReplicationMessagesRequest) (_ *adminservice.GetDLQReplicationMessagesResponse, retError error) {
	defer log.CapturePanicGRPC(adh.GetLogger(), &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminGetDLQReplicationMessagesScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}
	if len(request.GetTaskInfos()) == 0 {
		return nil, adh.error(errEmptyReplicationInfo, scope)
	}

	resp, err := adh.GetHistoryClientGRPC().GetDLQReplicationMessages(ctx, &historyservice.GetDLQReplicationMessagesRequest{TaskInfos: request.GetTaskInfos()})
	if err != nil {
		return nil, adh.error(err, scope)
	}
	return &adminservice.GetDLQReplicationMessagesResponse{ReplicationTasks: resp.GetReplicationTasks()}, nil
}

// ReapplyEvents applies stale events to the current workflow and the current run
func (adh *AdminHandlerGRPC) ReapplyEvents(ctx context.Context, request *adminservice.ReapplyEventsRequest) (_ *adminservice.ReapplyEventsResponse, retError error) {
	defer log.CapturePanicGRPC(adh.GetLogger(), &retError)
	scope, sw := adh.startRequestProfile(metrics.AdminReapplyEventsScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}
	if request.GetDomainName() == "" {
		return nil, adh.error(errDomainNotSet, scope)
	}
	if request.WorkflowExecution == nil {
		return nil, adh.error(errExecutionNotSet, scope)
	}
	if request.GetWorkflowExecution().GetWorkflowId() == "" {
		return nil, adh.error(errWorkflowIDNotSet, scope)
	}
	if request.GetEvents() == nil {
		return nil, adh.error(errWorkflowIDNotSet, scope)
	}
	domainEntry, err := adh.GetDomainCache().GetDomain(request.GetDomainName())
	if err != nil {
		return nil, adh.error(err, scope)
	}

	_, err = adh.GetHistoryClientGRPC().ReapplyEvents(ctx, &historyservice.ReapplyEventsRequest{
		DomainUUID: domainEntry.GetInfo().ID,
		Request:    request,
	})
	if err != nil {
		return nil, adh.error(err, scope)
	}
	return nil, nil
}

//===================================================================
func (adh *AdminHandlerGRPC) validateGetWorkflowExecutionRawHistoryV2Request(
	request *adminservice.GetWorkflowExecutionRawHistoryV2Request,
) error {

	execution := request.Execution
	if len(execution.GetWorkflowId()) == 0 {
		return &shared.BadRequestError{Message: "Invalid WorkflowID."}
	}
	// TODO currently, this API is only going to be used by re-send history events
	// to remote cluster if kafka is lossy again, in the future, this API can be used
	// by CLI and client, then empty runID (meaning the current workflow) should be allowed
	if len(execution.GetRunId()) == 0 || uuid.Parse(execution.GetRunId()) == nil {
		return &shared.BadRequestError{Message: "Invalid RunID."}
	}

	pageSize := int(request.GetMaximumPageSize())
	if pageSize <= 0 {
		return &shared.BadRequestError{Message: "Invalid PageSize."}
	}

	if request.GetStartEventId() == common.EmptyEventID &&
		request.GetStartEventVersion() == common.EmptyVersion &&
		request.GetEndEventId() == common.EmptyEventID &&
		request.GetEndEventVersion() == common.EmptyVersion {
		return &shared.BadRequestError{Message: "Invalid event query range."}
	}

	if (request.GetStartEventId() != common.EmptyEventID && request.GetStartEventVersion() == common.EmptyVersion) ||
		(request.GetStartEventId() == common.EmptyEventID && request.GetStartEventVersion() != common.EmptyVersion) {
		return &shared.BadRequestError{Message: "Invalid start event id and start event version combination."}
	}

	if (request.GetEndEventId() != common.EmptyEventID && request.GetEndEventVersion() == common.EmptyVersion) ||
		(request.GetEndEventId() == common.EmptyEventID && request.GetEndEventVersion() != common.EmptyVersion) {
		return &shared.BadRequestError{Message: "Invalid end event id and end event version combination."}
	}
	return nil
}

func (adh *AdminHandlerGRPC) validateConfigForAdvanceVisibility() error {
	if adh.params.ESConfig == nil || adh.params.ESClient == nil {
		return errors.New("ES related config not found")
	}
	return nil
}

func (adh *AdminHandlerGRPC) setRequestDefaultValueAndGetTargetVersionHistory(
	request *adminservice.GetWorkflowExecutionRawHistoryV2Request,
	versionHistories *persistence.VersionHistories,
) (*persistence.VersionHistory, error) {

	targetBranch, err := versionHistories.GetCurrentVersionHistory()
	if err != nil {
		return nil, err
	}
	firstItem, err := targetBranch.GetFirstItem()
	if err != nil {
		return nil, err
	}
	lastItem, err := targetBranch.GetLastItem()
	if err != nil {
		return nil, err
	}

	if request.GetStartEventId() == common.EmptyVersion || request.GetStartEventVersion() == common.EmptyVersion {
		// If start event is not set, get the events from the first event
		// As the API is exclusive-exclusive, use first event id - 1 here
		request.StartEventId = common.FirstEventID - 1
		request.StartEventVersion = firstItem.GetVersion()
	}
	if request.GetEndEventId() == common.EmptyEventID || request.GetEndEventVersion() == common.EmptyVersion {
		// If end event is not set, get the events until the end event
		// As the API is exclusive-exclusive, use end event id + 1 here
		request.EndEventId = lastItem.GetEventID() + 1
		request.EndEventVersion = lastItem.GetVersion()
	}

	if request.GetStartEventId() < 0 {
		return nil, &shared.BadRequestError{Message: "Invalid FirstEventID && NextEventID combination."}
	}

	// get branch based on the end event if end event is defined in the request
	if request.GetEndEventId() == lastItem.GetEventID()+1 &&
		request.GetEndEventVersion() == lastItem.GetVersion() {
		// this is a special case, target branch remains the same
	} else {
		endItem := persistence.NewVersionHistoryItem(request.GetEndEventId(), request.GetEndEventVersion())
		idx, err := versionHistories.FindFirstVersionHistoryIndexByItem(endItem)
		if err != nil {
			return nil, err
		}

		targetBranch, err = versionHistories.GetVersionHistory(idx)
		if err != nil {
			return nil, err
		}
	}

	startItem := persistence.NewVersionHistoryItem(request.GetStartEventId(), request.GetStartEventVersion())
	// If the request start event is defined. The start event may be on a different branch as current branch.
	// We need to find the LCA of the start event and the current branch.
	if request.GetStartEventId() == common.FirstEventID-1 &&
		request.GetStartEventVersion() == firstItem.GetVersion() {
		// this is a special case, start event is on the same branch as target branch
	} else {
		if !targetBranch.ContainsItem(startItem) {
			idx, err := versionHistories.FindFirstVersionHistoryIndexByItem(startItem)
			if err != nil {
				return nil, err
			}
			startBranch, err := versionHistories.GetVersionHistory(idx)
			if err != nil {
				return nil, err
			}
			startItem, err = targetBranch.FindLCAItem(startBranch)
			if err != nil {
				return nil, err
			}
			request.StartEventId = startItem.GetEventID()
			request.StartEventVersion = startItem.GetVersion()
		}
	}

	return targetBranch, nil
}

func (adh *AdminHandlerGRPC) generatePaginationToken(
	request *adminservice.GetWorkflowExecutionRawHistoryV2Request,
	versionHistories *persistence.VersionHistories,
) *getWorkflowRawHistoryV2Token {

	execution := request.Execution
	return &getWorkflowRawHistoryV2Token{
		DomainName:        request.GetDomain(),
		WorkflowID:        execution.GetWorkflowId(),
		RunID:             execution.GetRunId(),
		StartEventID:      request.GetStartEventId(),
		StartEventVersion: request.GetStartEventVersion(),
		EndEventID:        request.GetEndEventId(),
		EndEventVersion:   request.GetEndEventVersion(),
		VersionHistories:  versionHistories.ToThrift(),
		PersistenceToken:  nil, // this is the initialized value
	}
}

func (adh *AdminHandlerGRPC) validatePaginationToken(
	request *adminservice.GetWorkflowExecutionRawHistoryV2Request,
	token *getWorkflowRawHistoryV2Token,
) error {

	execution := request.Execution
	if request.GetDomain() != token.DomainName ||
		execution.GetWorkflowId() != token.WorkflowID ||
		execution.GetRunId() != token.RunID ||
		request.GetStartEventId() != token.StartEventID ||
		request.GetStartEventVersion() != token.StartEventVersion ||
		request.GetEndEventId() != token.EndEventID ||
		request.GetEndEventVersion() != token.EndEventVersion {
		return &shared.BadRequestError{Message: "Invalid pagination token."}
	}
	return nil
}

// startRequestProfile initiates recording of request metrics
func (adh *AdminHandlerGRPC) startRequestProfile(scope int) (metrics.Scope, metrics.Stopwatch) {
	metricsScope := adh.GetMetricsClient().Scope(scope)
	sw := metricsScope.StartTimer(metrics.CadenceLatency)
	metricsScope.IncCounter(metrics.CadenceRequests)
	return metricsScope, sw
}

func (adh *AdminHandlerGRPC) error(err error, scope metrics.Scope) error {
	if _, ok := status.FromError(err); ok {
		scope.IncCounter(metrics.CadenceFailures)
		return err
	}

	return adapter.ToProtoError(adh.errorThrift(err, scope))
}

func (adh *AdminHandlerGRPC) errorThrift(err error, scope metrics.Scope) error {
	switch err.(type) {
	case *shared.InternalServiceError:
		adh.GetLogger().Error("Internal service error", tag.Error(err))
		scope.IncCounter(metrics.CadenceFailures)
		return err
	case *shared.BadRequestError:
		scope.IncCounter(metrics.CadenceErrBadRequestCounter)
		return err
	case *shared.ServiceBusyError:
		scope.IncCounter(metrics.CadenceErrServiceBusyCounter)
		return err
	case *shared.EntityNotExistsError:
		return err
	default:
		adh.GetLogger().Error("Uncategorized error", tag.Error(err))
		scope.IncCounter(metrics.CadenceFailures)
		return &shared.InternalServiceError{Message: err.Error()}
	}
}

func (adh *AdminHandlerGRPC) convertIndexedValueTypeToESDataType(valueType enums.IndexedValueType) string {
	switch valueType {
	case enums.IndexedValueTypeString:
		return "text"
	case enums.IndexedValueTypeKeyword:
		return "keyword"
	case enums.IndexedValueTypeInt:
		return "long"
	case enums.IndexedValueTypeDouble:
		return "double"
	case enums.IndexedValueTypeBool:
		return "boolean"
	case enums.IndexedValueTypeDatetime:
		return "date"
	default:
		return ""
	}
}

// TODO: same func exists in workflowHandlerGRPC
func (adh *AdminHandlerGRPC) validateExecution(w *commonproto.WorkflowExecution) error {
	if w == nil {
		return errExecutionNotSet
	}
	if w.GetWorkflowId() == "" {
		return errWorkflowIDNotSet
	}
	if w.GetRunId() != "" && uuid.Parse(w.GetRunId()) == nil {
		return errInvalidRunID
	}
	return nil
}
