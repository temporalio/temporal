// Copyright (c) 2017 Uber Technologies, Inc.
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
	"time"

	"github.com/olivere/elastic"
	"github.com/pborman/uuid"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/.gen/proto/adminservice"
	"github.com/temporalio/temporal/.gen/proto/historyservice"
	"github.com/temporalio/temporal/.gen/proto/replication"
	"github.com/temporalio/temporal/.gen/proto/token"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/backoff"
	"github.com/temporalio/temporal/common/definition"
	"github.com/temporalio/temporal/common/domain"
	"github.com/temporalio/temporal/common/headers"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/resource"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
	"github.com/temporalio/temporal/service/history"
)

const (
	getDomainReplicationMessageBatchSize = 100
	defaultLastMessageID                 = -1
)

type (
	// AdminHandler - gRPC handler interface for adminservice
	AdminHandler struct {
		resource.Resource

		numberOfHistoryShards int
		params                *resource.BootstrapParams
		config                *Config
		domainDLQHandler      domain.DLQMessageHandler
	}
)

var (
	_ adminservice.AdminServiceServer = (*AdminHandler)(nil)

	adminServiceRetryPolicy = common.CreateAdminServiceRetryPolicy()
)

// NewAdminHandler creates a gRPC handler for the workflowservice
func NewAdminHandler(
	resource resource.Resource,
	params *resource.BootstrapParams,
	config *Config,
) *AdminHandler {

	domainReplicationTaskExecutor := domain.NewReplicationTaskExecutor(
		resource.GetMetadataManager(),
		resource.GetLogger(),
	)
	return &AdminHandler{
		Resource:              resource,
		numberOfHistoryShards: params.PersistenceConfig.NumHistoryShards,
		params:                params,
		config:                config,
		domainDLQHandler: domain.NewDLQMessageHandler(
			domainReplicationTaskExecutor,
			resource.GetDomainReplicationQueue(),
			resource.GetLogger(),
		),
	}
}

// Start starts the handler
func (adh *AdminHandler) Start() {
	// Start domain replication queue cleanup
	adh.Resource.GetDomainReplicationQueue().Start()
}

// Stop stops the handler
func (adh *AdminHandler) Stop() {
	adh.Resource.GetDomainReplicationQueue().Stop()
}

// AddSearchAttribute add search attribute to whitelist
func (adh *AdminHandler) AddSearchAttribute(ctx context.Context, request *adminservice.AddSearchAttributeRequest) (_ *adminservice.AddSearchAttributeResponse, retError error) {
	defer log.CapturePanicGRPC(adh.GetLogger(), &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminAddSearchAttributeScope)
	defer sw.Stop()

	// validate request
	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}
	if err := adh.checkPermission(adh.config, request.SecurityToken); err != nil {
		return nil, adh.error(errNoPermission, scope)
	}
	if len(request.GetSearchAttribute()) == 0 {
		return nil, adh.error(errSearchAttributesNotSet, scope)
	}
	if err := adh.validateConfigForAdvanceVisibility(); err != nil {
		return nil, adh.error(errAdvancedVisibilityStoreIsNotConfigured, scope)
	}

	searchAttr := request.GetSearchAttribute()
	currentValidAttr, _ := adh.params.DynamicConfig.GetMapValue(
		dynamicconfig.ValidSearchAttributes, nil, definition.GetDefaultIndexedKeys())
	for k, v := range searchAttr {
		if definition.IsSystemIndexedKey(k) {
			return nil, adh.error(errKeyIsReservedBySystem.MessageArgs(k), scope)
		}
		if _, exist := currentValidAttr[k]; exist {
			return nil, adh.error(errKeyIsAlreadyWhitelisted.MessageArgs(k), scope)
		}

		currentValidAttr[k] = int(v)
	}

	// update dynamic config
	err := adh.params.DynamicConfig.UpdateValue(dynamicconfig.ValidSearchAttributes, currentValidAttr)
	if err != nil {
		return nil, adh.error(errFailedUpdateDynamicConfig.MessageArgs(err), scope)
	}

	// update elasticsearch mapping, new added field will not be able to remove or update
	index := adh.params.ESConfig.GetVisibilityIndex()
	for k, v := range searchAttr {
		valueType := adh.convertIndexedValueTypeToESDataType(v)
		if len(valueType) == 0 {
			return nil, adh.error(errUnknownValueType.MessageArgs(v), scope)
		}
		err := adh.params.ESClient.PutMapping(ctx, index, definition.Attr, k, valueType)
		if elastic.IsNotFound(err) {
			err = adh.params.ESClient.CreateIndex(ctx, index)
			if err != nil {
				return nil, adh.error(errFailedToCreateESIndex.MessageArgs(err), scope)
			}
			err = adh.params.ESClient.PutMapping(ctx, index, definition.Attr, k, valueType)
		}
		if err != nil {
			return nil, adh.error(errFailedToUpdateESMapping.MessageArgs(err), scope)
		}
	}

	return &adminservice.AddSearchAttributeResponse{}, nil
}

// DescribeWorkflowExecution returns information about the specified workflow execution.
func (adh *AdminHandler) DescribeWorkflowExecution(ctx context.Context, request *adminservice.DescribeWorkflowExecutionRequest) (_ *adminservice.DescribeWorkflowExecutionResponse, retError error) {
	defer log.CapturePanicGRPC(adh.GetLogger(), &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminDescribeWorkflowExecutionScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}

	if err := validateExecution(request.Execution); err != nil {
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
	resp2, err := adh.GetHistoryClient().DescribeMutableState(ctx, &historyservice.DescribeMutableStateRequest{
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

// RemoveTask returns information about the internal states of a history host
func (adh *AdminHandler) RemoveTask(ctx context.Context, request *adminservice.RemoveTaskRequest) (_ *adminservice.RemoveTaskResponse, retError error) {
	defer log.CapturePanicGRPC(adh.GetLogger(), &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminRemoveTaskScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}
	_, err := adh.GetHistoryClient().RemoveTask(ctx, &historyservice.RemoveTaskRequest{
		ShardID: request.GetShardID(),
		Type:    request.GetType(),
		TaskID:  request.GetTaskID(),
	})
	return &adminservice.RemoveTaskResponse{}, err
}

// CloseShard returns information about the internal states of a history host
func (adh *AdminHandler) CloseShard(ctx context.Context, request *adminservice.CloseShardRequest) (_ *adminservice.CloseShardResponse, retError error) {
	defer log.CapturePanicGRPC(adh.GetLogger(), &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminCloseShardTaskScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}
	_, err := adh.GetHistoryClient().CloseShard(ctx, &historyservice.CloseShardRequest{ShardID: request.GetShardID()})
	return &adminservice.CloseShardResponse{}, err
}

// DescribeHistoryHost returns information about the internal states of a history host
func (adh *AdminHandler) DescribeHistoryHost(ctx context.Context, request *adminservice.DescribeHistoryHostRequest) (_ *adminservice.DescribeHistoryHostResponse, retError error) {
	defer log.CapturePanicGRPC(adh.GetLogger(), &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminDescribeHistoryHostScope)
	defer sw.Stop()

	if request == nil || request.ExecutionForHost == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}

	if request.ExecutionForHost != nil {
		if err := validateExecution(request.ExecutionForHost); err != nil {
			return nil, adh.error(err, scope)
		}
	}

	resp, err := adh.GetHistoryClient().DescribeHistoryHost(ctx, &historyservice.DescribeHistoryHostRequest{
		HostAddress:      request.GetHostAddress(),
		ShardIdForHost:   request.GetShardIdForHost(),
		ExecutionForHost: request.GetExecutionForHost(),
	})

	if resp == nil {
		return nil, err
	}

	return &adminservice.DescribeHistoryHostResponse{
		NumberOfShards:        resp.GetNumberOfShards(),
		ShardIDs:              resp.GetShardIDs(),
		DomainCache:           resp.GetDomainCache(),
		ShardControllerStatus: resp.GetShardControllerStatus(),
		Address:               resp.GetAddress(),
	}, err
}

// GetWorkflowExecutionRawHistory - retrieves the history of workflow execution
func (adh *AdminHandler) GetWorkflowExecutionRawHistory(ctx context.Context, request *adminservice.GetWorkflowExecutionRawHistoryRequest) (_ *adminservice.GetWorkflowExecutionRawHistoryResponse, retError error) {
	defer log.CapturePanicGRPC(adh.GetLogger(), &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminGetWorkflowExecutionRawHistoryScope)
	defer sw.Stop()

	var err error
	var size int

	domainID, err := adh.GetDomainCache().GetDomainID(request.GetDomain())
	if err != nil {
		return nil, adh.error(err, scope)
	}
	scope = scope.Tagged(metrics.DomainTag(request.GetDomain()))

	execution := request.Execution
	if execution.GetWorkflowId() == "" {
		return nil, errWorkflowIDNotSet
	}
	// TODO currently, this API is only going to be used by re-send history events
	// to remote cluster if kafka is lossy again, in the future, this API can be used
	// by CLI and client, then empty runID (meaning the current workflow) should be allowed
	if execution.GetRunId() == "" || uuid.Parse(execution.GetRunId()) == nil {
		return nil, errInvalidRunID
	}

	pageSize := int(request.GetMaximumPageSize())
	if pageSize <= 0 {
		return nil, errInvalidPageSize
	}

	var continuationToken *token.HistoryContinuation
	// initialize or validate the token
	// token will be used as a source of truth
	if request.NextPageToken != nil {
		continuationToken, err = deserializeHistoryToken(request.NextPageToken)
		if err != nil {
			return nil, err
		}

		if execution.GetRunId() != continuationToken.GetRunId() ||
			// we guarantee to use the first event ID provided in the request
			request.GetFirstEventId() != continuationToken.GetFirstEventId() ||
			// the next event ID in the request must be <= next event ID from mutable state, when initialized
			// so as long as customer do not change next event ID during pagination,
			// next event ID in the token <= next event ID in the request.
			request.GetNextEventId() < continuationToken.GetNextEventId() {
			return nil, errInvalidPaginationToken
		}

		// for the rest variables in the token, since we do not do hmac,
		// the only thing can be done is to trust the token:
		// IsWorkflowRunning: not used
		// TransientDecision: not used
		// PersistenceToken: trust
		// ReplicationInfo: trust

	} else {
		firstEventID := request.GetFirstEventId()
		nextEventID := request.GetNextEventId()
		if firstEventID < 0 || firstEventID > nextEventID {
			return nil, errInvalidFirstNextEventCombination
		}

		response, err := adh.GetHistoryClient().GetMutableState(ctx, &historyservice.GetMutableStateRequest{
			DomainUUID: domainID,
			Execution:  execution,
		})
		if err != nil {
			return nil, err
		}

		// check if the input next event ID is > actual next event ID in the mutable state
		// since we should not leak invalid events
		if nextEventID > response.GetNextEventId() {
			nextEventID = response.GetNextEventId()
		}
		continuationToken = &token.HistoryContinuation{
			RunId:            execution.GetRunId(),
			BranchToken:      response.CurrentBranchToken,
			FirstEventId:     firstEventID,
			NextEventId:      nextEventID,
			PersistenceToken: nil, // this is the initialized value
			ReplicationInfo:  response.ReplicationInfo,
		}
	}

	if continuationToken.GetFirstEventId() >= continuationToken.GetNextEventId() {
		return &adminservice.GetWorkflowExecutionRawHistoryResponse{
			HistoryBatches:  []*commonproto.DataBlob{},
			ReplicationInfo: continuationToken.ReplicationInfo,
			NextPageToken:   nil, // no further pagination
		}, nil
	}

	// TODO need to deal with transient decision if to be used by client getting history
	var historyBatches []*commonproto.History
	shardID := common.WorkflowIDToHistoryShard(execution.GetWorkflowId(), adh.numberOfHistoryShards)
	_, historyBatches, continuationToken.PersistenceToken, size, err = history.PaginateHistory(
		adh.GetHistoryManager(),
		true, // this means that we are getting history by batch
		continuationToken.GetBranchToken(),
		continuationToken.GetFirstEventId(),
		continuationToken.GetNextEventId(),
		continuationToken.GetPersistenceToken(),
		pageSize,
		&shardID,
	)
	if err != nil {
		if _, ok := err.(*serviceerror.NotFound); ok {
			// when no events can be returned from DB, DB layer will return
			// EntityNotExistsError, this API shall return empty response
			return &adminservice.GetWorkflowExecutionRawHistoryResponse{
				HistoryBatches:  []*commonproto.DataBlob{},
				ReplicationInfo: continuationToken.ReplicationInfo,
				NextPageToken:   nil, // no further pagination
			}, nil
		}
		return nil, err
	}

	// N.B. - Dual emit is required here so that we can see aggregate timer stats across all
	// domains along with the individual domains stats
	adh.GetMetricsClient().RecordTimer(metrics.AdminGetWorkflowExecutionRawHistoryScope, metrics.HistorySize, time.Duration(size))
	scope.RecordTimer(metrics.HistorySize, time.Duration(size))

	var blobs []*commonproto.DataBlob
	for _, historyBatch := range historyBatches {
		blob, err := adh.GetPayloadSerializer().SerializeBatchEvents(historyBatch.Events, common.EncodingTypeThriftRW)
		if err != nil {
			return nil, err
		}
		blobs = append(blobs, &commonproto.DataBlob{
			EncodingType: enums.EncodingTypeThriftRW,
			Data:         blob.Data,
		})
	}

	result := &adminservice.GetWorkflowExecutionRawHistoryResponse{
		HistoryBatches:  blobs,
		ReplicationInfo: continuationToken.ReplicationInfo,
	}
	if len(continuationToken.PersistenceToken) == 0 {
		result.NextPageToken = nil
	} else {
		result.NextPageToken, err = serializeHistoryToken(continuationToken)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// GetWorkflowExecutionRawHistoryV2 - retrieves the history of workflow execution
func (adh *AdminHandler) GetWorkflowExecutionRawHistoryV2(ctx context.Context, request *adminservice.GetWorkflowExecutionRawHistoryV2Request) (_ *adminservice.GetWorkflowExecutionRawHistoryV2Response, retError error) {
	defer log.CapturePanicGRPC(adh.GetLogger(), &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminGetWorkflowExecutionRawHistoryV2Scope)
	defer sw.Stop()

	if err := adh.validateGetWorkflowExecutionRawHistoryV2Request(
		request,
	); err != nil {
		return nil, adh.error(err, scope)
	}
	domainID, err := adh.GetDomainCache().GetDomainID(request.GetDomain())
	if err != nil {
		return nil, adh.error(err, scope)
	}
	scope = scope.Tagged(metrics.DomainTag(request.GetDomain()))

	execution := request.Execution
	var pageToken *token.RawHistoryContinuation
	var targetVersionHistory *persistence.VersionHistory
	if request.NextPageToken == nil {
		response, err := adh.GetHistoryClient().GetMutableState(ctx, &historyservice.GetMutableStateRequest{
			DomainUUID: domainID,
			Execution:  execution,
		})
		if err != nil {
			return nil, adh.error(err, scope)
		}

		versionHistories := persistence.NewVersionHistoriesFromProto(
			response.GetVersionHistories(),
		)
		targetVersionHistory, err = adh.setRequestDefaultValueAndGetTargetVersionHistory(
			request,
			versionHistories,
		)
		if err != nil {
			return nil, adh.error(err, scope)
		}

		pageToken = generatePaginationToken(request, versionHistories)
	} else {
		pageToken, err = deserializeRawHistoryToken(request.NextPageToken)
		if err != nil {
			return nil, adh.error(err, scope)
		}
		versionHistories := pageToken.GetVersionHistories()
		if versionHistories == nil {
			return nil, adh.error(errInvalidVersionHistories, scope)
		}
		targetVersionHistory, err = adh.setRequestDefaultValueAndGetTargetVersionHistory(
			request,
			persistence.NewVersionHistoriesFromProto(versionHistories),
		)
		if err != nil {
			return nil, adh.error(err, scope)
		}
	}

	if err := validatePaginationToken(
		request,
		pageToken,
	); err != nil {
		return nil, adh.error(err, scope)
	}

	if pageToken.GetStartEventId()+1 == pageToken.GetEndEventId() {
		// API is exclusive-exclusive. Return empty response here.
		return &adminservice.GetWorkflowExecutionRawHistoryV2Response{
			HistoryBatches: []*commonproto.DataBlob{},
			NextPageToken:  nil, // no further pagination
			VersionHistory: targetVersionHistory.ToProto(),
		}, nil
	}
	pageSize := int(request.GetMaximumPageSize())
	shardID := common.WorkflowIDToHistoryShard(
		execution.GetWorkflowId(),
		adh.numberOfHistoryShards,
	)
	rawHistoryResponse, err := adh.GetHistoryManager().ReadRawHistoryBranch(&persistence.ReadHistoryBranchRequest{
		BranchToken: targetVersionHistory.GetBranchToken(),
		// GetWorkflowExecutionRawHistoryV2 is exclusive exclusive.
		// ReadRawHistoryBranch is inclusive exclusive.
		MinEventID:    pageToken.GetStartEventId() + 1,
		MaxEventID:    pageToken.GetEndEventId(),
		PageSize:      pageSize,
		NextPageToken: pageToken.PersistenceToken,
		ShardID:       &shardID,
	})
	if err != nil {
		if _, ok := err.(*serviceerror.NotFound); ok {
			// when no events can be returned from DB, DB layer will return
			// EntityNotExistsError, this API shall return empty response
			return &adminservice.GetWorkflowExecutionRawHistoryV2Response{
				HistoryBatches: []*commonproto.DataBlob{},
				NextPageToken:  nil, // no further pagination
				VersionHistory: targetVersionHistory.ToProto(),
			}, nil
		}
		return nil, err
	}

	pageToken.PersistenceToken = rawHistoryResponse.NextPageToken
	size := rawHistoryResponse.Size
	// N.B. - Dual emit is required here so that we can see aggregate timer stats across all
	// domains along with the individual domains stats
	adh.GetMetricsClient().RecordTimer(metrics.AdminGetWorkflowExecutionRawHistoryScope, metrics.HistorySize, time.Duration(size))
	scope.RecordTimer(metrics.HistorySize, time.Duration(size))

	rawBlobs := rawHistoryResponse.HistoryEventBlobs
	var blobs []*commonproto.DataBlob
	for _, blob := range rawBlobs {
		blobs = append(blobs, blob.ToProto())
	}

	result := &adminservice.GetWorkflowExecutionRawHistoryV2Response{
		HistoryBatches: blobs,
		VersionHistory: targetVersionHistory.ToProto(),
	}
	if len(pageToken.PersistenceToken) == 0 {
		result.NextPageToken = nil
	} else {
		result.NextPageToken, err = serializeRawHistoryToken(pageToken)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// DescribeCluster return information about cadence deployment
func (adh *AdminHandler) DescribeCluster(ctx context.Context, _ *adminservice.DescribeClusterRequest) (_ *adminservice.DescribeClusterResponse, retError error) {
	defer log.CapturePanicGRPC(adh.GetLogger(), &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminGetWorkflowExecutionRawHistoryV2Scope)
	defer sw.Stop()

	membershipInfo := &commonproto.MembershipInfo{}
	if monitor := adh.GetMembershipMonitor(); monitor != nil {
		currentHost, err := monitor.WhoAmI()
		if err != nil {
			return nil, adh.error(err, scope)
		}

		membershipInfo.CurrentHost = &commonproto.HostInfo{
			Identity: currentHost.Identity(),
		}

		members, err := monitor.GetReachableMembers()
		if err != nil {
			return nil, adh.error(err, scope)
		}

		membershipInfo.ReachableMembers = members

		var rings []*commonproto.RingInfo
		for _, role := range []string{common.FrontendServiceName, common.HistoryServiceName, common.MatchingServiceName, common.WorkerServiceName} {
			resolver, err := monitor.GetResolver(role)
			if err != nil {
				return nil, adh.error(err, scope)
			}

			var servers []*commonproto.HostInfo
			for _, server := range resolver.Members() {
				servers = append(servers, &commonproto.HostInfo{
					Identity: server.Identity(),
				})
			}

			rings = append(rings, &commonproto.RingInfo{
				Role:        role,
				MemberCount: int32(resolver.MemberCount()),
				Members:     servers,
			})
		}
		membershipInfo.Rings = rings
	}

	return &adminservice.DescribeClusterResponse{
		SupportedClientVersions: &commonproto.SupportedClientVersions{
			GoSdk:   headers.SupportedGoSDKVersion,
			JavaSdk: headers.SupportedJavaSDKVersion,
		},
		MembershipInfo: membershipInfo,
	}, nil
}

// GetReplicationMessages returns new replication tasks since the read level provided in the token.
func (adh *AdminHandler) GetReplicationMessages(ctx context.Context, request *adminservice.GetReplicationMessagesRequest) (_ *adminservice.GetReplicationMessagesResponse, retError error) {
	defer log.CapturePanicGRPC(adh.GetLogger(), &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminGetReplicationMessagesScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}
	if request.GetClusterName() == "" {
		return nil, adh.error(errClusterNameNotSet, scope)
	}

	resp, err := adh.GetHistoryClient().GetReplicationMessages(ctx, &historyservice.GetReplicationMessagesRequest{
		Tokens:      request.GetTokens(),
		ClusterName: request.GetClusterName(),
	})
	if err != nil {
		return nil, adh.error(err, scope)
	}
	return &adminservice.GetReplicationMessagesResponse{MessagesByShard: resp.GetMessagesByShard()}, nil
}

// GetDomainReplicationMessages returns new domain replication tasks since last retrieved task ID.
func (adh *AdminHandler) GetDomainReplicationMessages(ctx context.Context, request *adminservice.GetDomainReplicationMessagesRequest) (_ *adminservice.GetDomainReplicationMessagesResponse, retError error) {
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
		Messages: &replication.ReplicationMessages{
			ReplicationTasks:       replicationTasks,
			LastRetrievedMessageId: int64(lastMessageID),
		},
	}, nil
}

// GetDLQReplicationMessages returns new replication tasks based on the dlq info.
func (adh *AdminHandler) GetDLQReplicationMessages(ctx context.Context, request *adminservice.GetDLQReplicationMessagesRequest) (_ *adminservice.GetDLQReplicationMessagesResponse, retError error) {
	defer log.CapturePanicGRPC(adh.GetLogger(), &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminGetDLQReplicationMessagesScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}
	if len(request.GetTaskInfos()) == 0 {
		return nil, adh.error(errEmptyReplicationInfo, scope)
	}

	resp, err := adh.GetHistoryClient().GetDLQReplicationMessages(ctx, &historyservice.GetDLQReplicationMessagesRequest{TaskInfos: request.GetTaskInfos()})
	if err != nil {
		return nil, adh.error(err, scope)
	}
	return &adminservice.GetDLQReplicationMessagesResponse{ReplicationTasks: resp.GetReplicationTasks()}, nil
}

// ReapplyEvents applies stale events to the current workflow and the current run
func (adh *AdminHandler) ReapplyEvents(ctx context.Context, request *adminservice.ReapplyEventsRequest) (_ *adminservice.ReapplyEventsResponse, retError error) {
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

	_, err = adh.GetHistoryClient().ReapplyEvents(ctx, &historyservice.ReapplyEventsRequest{
		DomainUUID: domainEntry.GetInfo().ID,
		Request:    request,
	})
	if err != nil {
		return nil, adh.error(err, scope)
	}
	return nil, nil
}

// ReadDLQMessages reads messages from DLQ
func (adh *AdminHandler) ReadDLQMessages(
	ctx context.Context,
	request *adminservice.ReadDLQMessagesRequest,
) (resp *adminservice.ReadDLQMessagesResponse, retErr error) {

	defer log.CapturePanicGRPC(adh.GetLogger(), &retErr)
	scope, sw := adh.startRequestProfile(metrics.AdminReadDLQMessagesScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}

	if request.GetMaximumPageSize() <= 0 {
		request.MaximumPageSize = common.ReadDLQMessagesPageSize
	}

	if request.InclusiveEndMessageID <= 0 {
		request.InclusiveEndMessageID = common.EndMessageID
	}

	var tasks []*replication.ReplicationTask
	var token []byte
	var op func() error
	switch request.GetType() {
	case enums.DLQTypeReplication:
		resp, err := adh.GetHistoryClient().ReadDLQMessages(ctx, &historyservice.ReadDLQMessagesRequest{
			Type:                  request.GetType(),
			ShardID:               request.GetShardID(),
			SourceCluster:         request.GetSourceCluster(),
			InclusiveEndMessageID: request.GetInclusiveEndMessageID(),
			MaximumPageSize:       request.GetMaximumPageSize(),
			NextPageToken:         request.GetNextPageToken(),
		})

		if resp == nil {
			return nil, err
		}

		return &adminservice.ReadDLQMessagesResponse{
			Type:             resp.GetType(),
			ReplicationTasks: resp.GetReplicationTasks(),
			NextPageToken:    resp.GetNextPageToken(),
		}, err
	case enums.DLQTypeDomain:
		op = func() error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				var err error
				tasks, token, err = adh.domainDLQHandler.Read(
					int(request.GetInclusiveEndMessageID()),
					int(request.GetMaximumPageSize()),
					request.GetNextPageToken())
				return err
			}
		}
	default:
		return nil, adh.error(errDLQTypeIsNotSupported, scope)
	}
	retErr = backoff.Retry(op, adminServiceRetryPolicy, common.IsServiceTransientError)
	if retErr != nil {
		return nil, adh.error(retErr, scope)
	}

	return &adminservice.ReadDLQMessagesResponse{
		ReplicationTasks: tasks,
		NextPageToken:    token,
	}, nil
}

// PurgeDLQMessages purge messages from DLQ
func (adh *AdminHandler) PurgeDLQMessages(
	ctx context.Context,
	request *adminservice.PurgeDLQMessagesRequest,
) (_ *adminservice.PurgeDLQMessagesResponse, err error) {

	defer log.CapturePanicGRPC(adh.GetLogger(), &err)
	scope, sw := adh.startRequestProfile(metrics.AdminPurgeDLQMessagesScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}

	if request.InclusiveEndMessageID <= 0 {
		request.InclusiveEndMessageID = common.EndMessageID
	}

	var op func() error
	switch request.GetType() {
	case enums.DLQTypeReplication:
		resp, err := adh.GetHistoryClient().PurgeDLQMessages(ctx, &historyservice.PurgeDLQMessagesRequest{
			Type:                  request.GetType(),
			ShardID:               request.GetShardID(),
			SourceCluster:         request.GetSourceCluster(),
			InclusiveEndMessageID: request.GetInclusiveEndMessageID(),
		})

		if resp == nil {
			return nil, adh.error(err, scope)
		}

		return &adminservice.PurgeDLQMessagesResponse{}, err
	case enums.DLQTypeDomain:
		op = func() error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				return adh.domainDLQHandler.Purge(int(request.GetInclusiveEndMessageID()))
			}
		}
	default:
		return nil, adh.error(errDLQTypeIsNotSupported, scope)
	}
	err = backoff.Retry(op, adminServiceRetryPolicy, common.IsServiceTransientError)
	if err != nil {
		return nil, adh.error(err, scope)
	}

	return &adminservice.PurgeDLQMessagesResponse{}, err
}

// MergeDLQMessages merges DLQ messages
func (adh *AdminHandler) MergeDLQMessages(
	ctx context.Context,
	request *adminservice.MergeDLQMessagesRequest,
) (resp *adminservice.MergeDLQMessagesResponse, err error) {

	defer log.CapturePanicGRPC(adh.GetLogger(), &err)
	scope, sw := adh.startRequestProfile(metrics.AdminMergeDLQMessagesScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}

	if request.InclusiveEndMessageID <= 0 {
		request.InclusiveEndMessageID = common.EndMessageID
	}

	var token []byte
	var op func() error
	switch request.GetType() {
	case enums.DLQTypeReplication:
		resp, err := adh.GetHistoryClient().MergeDLQMessages(ctx, &historyservice.MergeDLQMessagesRequest{
			Type:                  request.GetType(),
			ShardID:               request.GetShardID(),
			SourceCluster:         request.GetSourceCluster(),
			InclusiveEndMessageID: request.GetInclusiveEndMessageID(),
			MaximumPageSize:       request.GetMaximumPageSize(),
			NextPageToken:         request.GetNextPageToken(),
		})
		if resp == nil {
			return nil, adh.error(err, scope)
		}

		return &adminservice.MergeDLQMessagesResponse{
			NextPageToken: request.GetNextPageToken(),
		}, nil
	case enums.DLQTypeDomain:

		op = func() error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				var err error
				token, err = adh.domainDLQHandler.Merge(
					int(request.GetInclusiveEndMessageID()),
					int(request.GetMaximumPageSize()),
					request.GetNextPageToken(),
				)
				return err
			}
		}
	default:
		return nil, adh.error(errDLQTypeIsNotSupported, scope)
	}
	err = backoff.Retry(op, adminServiceRetryPolicy, common.IsServiceTransientError)
	if err != nil {
		return nil, adh.error(err, scope)
	}

	return &adminservice.MergeDLQMessagesResponse{
		NextPageToken: token,
	}, nil
}

// RefreshWorkflowTasks re-generates the workflow tasks
func (adh *AdminHandler) RefreshWorkflowTasks(
	ctx context.Context,
	request *adminservice.RefreshWorkflowTasksRequest,
) (_ *adminservice.RefreshWorkflowTasksResponse, err error) {
	defer log.CapturePanicGRPC(adh.GetLogger(), &err)
	scope, sw := adh.startRequestProfile(metrics.AdminRefreshWorkflowTasksScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}
	if err := validateExecution(request.Execution); err != nil {
		return nil, adh.error(err, scope)
	}
	domainEntry, err := adh.GetDomainCache().GetDomain(request.GetDomain())
	if err != nil {
		return nil, adh.error(err, scope)
	}

	_, err = adh.GetHistoryClient().RefreshWorkflowTasks(ctx, &historyservice.RefreshWorkflowTasksRequest{
		DomainUUID: domainEntry.GetInfo().ID,
		Request:    request,
	})
	if err != nil {
		return nil, adh.error(err, scope)
	}
	return &adminservice.RefreshWorkflowTasksResponse{}, nil
}

func (adh *AdminHandler) validateGetWorkflowExecutionRawHistoryV2Request(
	request *adminservice.GetWorkflowExecutionRawHistoryV2Request,
) error {

	execution := request.Execution
	if execution.GetWorkflowId() == "" {
		return errWorkflowIDNotSet
	}
	// TODO currently, this API is only going to be used by re-send history events
	// to remote cluster if kafka is lossy again, in the future, this API can be used
	// by CLI and client, then empty runID (meaning the current workflow) should be allowed
	if execution.GetRunId() == "" || uuid.Parse(execution.GetRunId()) == nil {
		return errInvalidRunID
	}

	pageSize := int(request.GetMaximumPageSize())
	if pageSize <= 0 {
		return errInvalidPageSize
	}

	if request.GetStartEventId() == common.EmptyEventID &&
		request.GetStartEventVersion() == common.EmptyVersion &&
		request.GetEndEventId() == common.EmptyEventID &&
		request.GetEndEventVersion() == common.EmptyVersion {
		return errInvalidEventQueryRange
	}

	if (request.GetStartEventId() != common.EmptyEventID && request.GetStartEventVersion() == common.EmptyVersion) ||
		(request.GetStartEventId() == common.EmptyEventID && request.GetStartEventVersion() != common.EmptyVersion) {
		return errInvalidStartEventCombination
	}

	if (request.GetEndEventId() != common.EmptyEventID && request.GetEndEventVersion() == common.EmptyVersion) ||
		(request.GetEndEventId() == common.EmptyEventID && request.GetEndEventVersion() != common.EmptyVersion) {
		return errInvalidEndEventCombination
	}
	return nil
}

func (adh *AdminHandler) validateConfigForAdvanceVisibility() error {
	if adh.params.ESConfig == nil || adh.params.ESClient == nil {
		return errors.New("ES related config not found")
	}
	return nil
}

func (adh *AdminHandler) setRequestDefaultValueAndGetTargetVersionHistory(
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
		return nil, errInvalidFirstNextEventCombination
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

// startRequestProfile initiates recording of request metrics
func (adh *AdminHandler) startRequestProfile(scope int) (metrics.Scope, metrics.Stopwatch) {
	metricsScope := adh.GetMetricsClient().Scope(scope)
	sw := metricsScope.StartTimer(metrics.ServiceLatency)
	metricsScope.IncCounter(metrics.ServiceRequests)
	return metricsScope, sw
}

func (adh *AdminHandler) error(err error, scope metrics.Scope) error {
	switch err.(type) {
	case *serviceerror.Internal:
		adh.GetLogger().Error("Internal service error", tag.Error(err))
		scope.IncCounter(metrics.ServiceFailures)
		return err
	case *serviceerror.InvalidArgument:
		scope.IncCounter(metrics.ServiceErrInvalidArgumentCounter)
		return err
	case *serviceerror.ResourceExhausted:
		scope.IncCounter(metrics.ServiceErrResourceExhaustedCounter)
		return err
	case *serviceerror.NotFound:
		return err
	}

	adh.GetLogger().Error("Unknown error", tag.Error(err))
	scope.IncCounter(metrics.ServiceFailures)

	return err
}

func (adh *AdminHandler) convertIndexedValueTypeToESDataType(valueType enums.IndexedValueType) string {
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

func (adh *AdminHandler) checkPermission(
	config *Config,
	securityToken string,
) error {
	if config.EnableAdminProtection() {
		if securityToken == "" {
			return errNoPermission
		}
		requiredToken := config.AdminOperationToken()
		if securityToken != requiredToken {
			return errNoPermission
		}
	}
	return nil
}
