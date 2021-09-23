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
	"sync/atomic"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/sdk/client"

	"go.temporal.io/server/api/adminservice/v1"
	clusterspb "go.temporal.io/server/api/cluster/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/worker/addsearchattributes"
)

const (
	getNamespaceReplicationMessageBatchSize = 100
	defaultLastMessageID                    = -1
)

type (
	// AdminHandler - gRPC handler interface for adminservice
	AdminHandler struct {
		resource.Resource
		status int32

		numberOfHistoryShards int32
		ESConfig              *esclient.Config
		ESClient              esclient.Client
		config                *Config
		namespaceDLQHandler   namespace.DLQMessageHandler
		eventSerializer       serialization.Serializer
	}
)

var (
	_ adminservice.AdminServiceServer = (*AdminHandler)(nil)

	adminServiceRetryPolicy = common.CreateAdminServiceRetryPolicy()
	resendStartEventID      = int64(0)
)

// NewAdminHandler creates a gRPC handler for the adminservice
func NewAdminHandler(
	resource resource.Resource,
	params *resource.BootstrapParams,
	config *Config,
) *AdminHandler {

	namespaceReplicationTaskExecutor := namespace.NewReplicationTaskExecutor(
		resource.GetMetadataManager(),
		resource.GetLogger(),
	)
	return &AdminHandler{
		Resource:              resource,
		status:                common.DaemonStatusInitialized,
		numberOfHistoryShards: params.PersistenceConfig.NumHistoryShards,
		config:                config,
		namespaceDLQHandler: namespace.NewDLQMessageHandler(
			namespaceReplicationTaskExecutor,
			resource.GetNamespaceReplicationQueue(),
			resource.GetLogger(),
		),
		eventSerializer: serialization.NewSerializer(),
		ESConfig:        params.ESConfig,
		ESClient:        params.ESClient,
	}
}

// Start starts the handler
func (adh *AdminHandler) Start() {
	if !atomic.CompareAndSwapInt32(
		&adh.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	// Start namespace replication queue cleanup
	// If the queue does not start, we can still call stop()
	adh.Resource.GetNamespaceReplicationQueue().Start()
}

// Stop stops the handler
func (adh *AdminHandler) Stop() {
	if !atomic.CompareAndSwapInt32(
		&adh.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	// Calling stop if the queue does not start is ok
	adh.Resource.GetNamespaceReplicationQueue().Stop()
}

// AddSearchAttributes add search attribute to the cluster.
func (adh *AdminHandler) AddSearchAttributes(ctx context.Context, request *adminservice.AddSearchAttributesRequest) (_ *adminservice.AddSearchAttributesResponse, retError error) {
	defer log.CapturePanic(adh.GetLogger(), &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminAddSearchAttributesScope)
	defer sw.Stop()

	// validate request
	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}

	if len(request.GetSearchAttributes()) == 0 {
		return nil, adh.error(errSearchAttributesNotSet, scope)
	}

	indexName := request.GetIndexName()
	if indexName == "" {
		indexName = adh.ESConfig.GetVisibilityIndex()
	}

	currentSearchAttributes, err := adh.Resource.GetSearchAttributesProvider().GetSearchAttributes(indexName, true)
	if err != nil {
		return nil, adh.error(serviceerror.NewUnavailable(fmt.Sprintf(errUnableToGetSearchAttributesMessage, err)), scope)
	}

	for saName, saType := range request.GetSearchAttributes() {
		if searchattribute.IsReserved(saName) {
			return nil, adh.error(serviceerror.NewInvalidArgument(fmt.Sprintf(errSearchAttributeIsReservedMessage, saName)), scope)
		}
		if currentSearchAttributes.IsDefined(saName) {
			return nil, adh.error(serviceerror.NewInvalidArgument(fmt.Sprintf(errSearchAttributeAlreadyExistsMessage, saName)), scope)
		}
		if _, ok := enumspb.IndexedValueType_name[int32(saType)]; !ok {
			return nil, adh.error(serviceerror.NewInvalidArgument(fmt.Sprintf(errUnknownSearchAttributeTypeMessage, saType)), scope)
		}
	}

	// Execute workflow.
	wfParams := addsearchattributes.WorkflowParams{
		CustomAttributesToAdd: request.GetSearchAttributes(),
		IndexName:             indexName,
		SkipSchemaUpdate:      request.GetSkipSchemaUpdate(),
	}

	run, err := adh.GetSDKClient().ExecuteWorkflow(
		ctx,
		client.StartWorkflowOptions{
			TaskQueue: addsearchattributes.TaskQueueName,
			ID:        addsearchattributes.WorkflowName,
		},
		addsearchattributes.WorkflowName,
		wfParams,
	)
	if err != nil {
		return nil, adh.error(serviceerror.NewUnavailable(fmt.Sprintf(errUnableToStartWorkflowMessage, addsearchattributes.WorkflowName, err)), scope)
	}

	// Wait for workflow to complete.
	err = run.Get(ctx, nil)
	if err != nil {
		scope.IncCounter(metrics.AddSearchAttributesWorkflowFailuresCount)
		return nil, adh.error(serviceerror.NewUnavailable(fmt.Sprintf(errWorkflowReturnedErrorMessage, addsearchattributes.WorkflowName, err)), scope)
	}
	scope.IncCounter(metrics.AddSearchAttributesWorkflowSuccessCount)

	return &adminservice.AddSearchAttributesResponse{}, nil
}

// RemoveSearchAttributes remove search attribute from the cluster.
func (adh *AdminHandler) RemoveSearchAttributes(ctx context.Context, request *adminservice.RemoveSearchAttributesRequest) (_ *adminservice.RemoveSearchAttributesResponse, retError error) {
	defer log.CapturePanic(adh.GetLogger(), &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminRemoveSearchAttributesScope)
	defer sw.Stop()

	// validate request
	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}

	if len(request.GetSearchAttributes()) == 0 {
		return nil, adh.error(errSearchAttributesNotSet, scope)
	}

	indexName := request.GetIndexName()
	if indexName == "" {
		indexName = adh.ESConfig.GetVisibilityIndex()
	}

	currentSearchAttributes, err := adh.Resource.GetSearchAttributesProvider().GetSearchAttributes(indexName, true)
	if err != nil {
		return nil, adh.error(serviceerror.NewUnavailable(fmt.Sprintf(errUnableToGetSearchAttributesMessage, err)), scope)
	}

	newCustomSearchAttributes := map[string]enumspb.IndexedValueType{}
	for saName, saType := range currentSearchAttributes.Custom() {
		newCustomSearchAttributes[saName] = saType
	}

	for _, saName := range request.GetSearchAttributes() {
		if !currentSearchAttributes.IsDefined(saName) {
			return nil, adh.error(serviceerror.NewInvalidArgument(fmt.Sprintf(errSearchAttributeDoesntExistMessage, saName)), scope)
		}
		if _, ok := newCustomSearchAttributes[saName]; !ok {
			return nil, adh.error(serviceerror.NewInvalidArgument(fmt.Sprintf(errUnableToRemoveNonCustomSearchAttributesMessage, saName)), scope)
		}
		delete(newCustomSearchAttributes, saName)
	}

	err = adh.Resource.GetSearchAttributesManager().SaveSearchAttributes(indexName, newCustomSearchAttributes)
	if err != nil {
		return nil, adh.error(serviceerror.NewUnavailable(fmt.Sprintf(errUnableToSaveSearchAttributesMessage, err)), scope)
	}

	return &adminservice.RemoveSearchAttributesResponse{}, nil
}

func (adh *AdminHandler) GetSearchAttributes(ctx context.Context, request *adminservice.GetSearchAttributesRequest) (_ *adminservice.GetSearchAttributesResponse, retError error) {
	defer log.CapturePanic(adh.GetLogger(), &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminGetSearchAttributesScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}

	indexName := request.GetIndexName()
	if indexName == "" {
		indexName = adh.ESConfig.GetVisibilityIndex()
	}

	resp, err := adh.getSearchAttributes(ctx, indexName, "")
	if err != nil {
		return nil, adh.error(err, scope)
	}

	return resp, nil
}

func (adh *AdminHandler) getSearchAttributes(ctx context.Context, indexName string, runID string) (*adminservice.GetSearchAttributesResponse, error) {
	var lastErr error
	descResp, err := adh.GetSDKClient().DescribeWorkflowExecution(ctx, addsearchattributes.WorkflowName, runID)
	var wfInfo *workflowpb.WorkflowExecutionInfo
	if err != nil {
		// NotFound can happen when no search attributes were added and the workflow has never been executed.
		if _, notFound := err.(*serviceerror.NotFound); !notFound {
			lastErr = serviceerror.NewUnavailable(fmt.Sprintf("unable to get %s workflow state: %v", addsearchattributes.WorkflowName, err))
			adh.GetLogger().Error("getSearchAttributes error", tag.Error(lastErr))
		}
	} else {
		wfInfo = descResp.GetWorkflowExecutionInfo()
	}

	var esMapping map[string]string
	if adh.ESClient != nil {
		esMapping, err = adh.ESClient.GetMapping(ctx, indexName)
		if err != nil {
			lastErr = serviceerror.NewUnavailable(fmt.Sprintf("unable to get mapping from Elasticsearch: %v", err))
			adh.GetLogger().Error("getSearchAttributes error", tag.Error(lastErr))
		}
	}

	searchAttributes, err := adh.Resource.GetSearchAttributesProvider().GetSearchAttributes(indexName, true)
	if err != nil {
		lastErr = serviceerror.NewUnavailable(fmt.Sprintf("unable to read custom search attributes: %v", err))
		adh.GetLogger().Error("getSearchAttributes error", tag.Error(lastErr))
	}

	return &adminservice.GetSearchAttributesResponse{
		CustomAttributes:         searchAttributes.Custom(),
		SystemAttributes:         searchAttributes.System(),
		Mapping:                  esMapping,
		AddWorkflowExecutionInfo: wfInfo,
	}, lastErr
}

// DescribeMutableState returns information about the specified workflow execution.
func (adh *AdminHandler) DescribeMutableState(ctx context.Context, request *adminservice.DescribeMutableStateRequest) (_ *adminservice.DescribeMutableStateResponse, retError error) {
	defer log.CapturePanic(adh.GetLogger(), &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminDescribeWorkflowExecutionScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}

	if err := validateExecution(request.Execution); err != nil {
		return nil, adh.error(err, scope)
	}

	namespaceID, err := adh.GetNamespaceCache().GetNamespaceID(request.GetNamespace())

	shardID := common.WorkflowIDToHistoryShard(namespaceID, request.Execution.WorkflowId, adh.numberOfHistoryShards)
	shardIDStr := convert.Int32ToString(shardID)

	historyHost, err := adh.GetMembershipMonitor().Lookup(common.HistoryServiceName, shardIDStr)
	if err != nil {
		return nil, adh.error(err, scope)
	}

	historyAddr := historyHost.GetAddress()
	historyResponse, err := adh.GetHistoryClient().DescribeMutableState(ctx, &historyservice.DescribeMutableStateRequest{
		NamespaceId: namespaceID,
		Execution:   request.Execution,
	})

	if err != nil {
		return nil, err
	}
	return &adminservice.DescribeMutableStateResponse{
		ShardId:              shardIDStr,
		HistoryAddr:          historyAddr,
		DatabaseMutableState: historyResponse.GetDatabaseMutableState(),
		CacheMutableState:    historyResponse.GetCacheMutableState(),
	}, err
}

// RemoveTask returns information about the internal states of a history host
func (adh *AdminHandler) RemoveTask(ctx context.Context, request *adminservice.RemoveTaskRequest) (_ *adminservice.RemoveTaskResponse, retError error) {
	defer log.CapturePanic(adh.GetLogger(), &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminRemoveTaskScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}
	_, err := adh.GetHistoryClient().RemoveTask(ctx, &historyservice.RemoveTaskRequest{
		ShardId:        request.GetShardId(),
		Category:       request.GetCategory(),
		TaskId:         request.GetTaskId(),
		VisibilityTime: request.GetVisibilityTime(),
	})
	return &adminservice.RemoveTaskResponse{}, err
}

// CloseShard returns information about the internal states of a history host
func (adh *AdminHandler) CloseShard(ctx context.Context, request *adminservice.CloseShardRequest) (_ *adminservice.CloseShardResponse, retError error) {
	defer log.CapturePanic(adh.GetLogger(), &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminCloseShardTaskScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}
	_, err := adh.GetHistoryClient().CloseShard(ctx, &historyservice.CloseShardRequest{ShardId: request.GetShardId()})
	return &adminservice.CloseShardResponse{}, err
}

// DescribeHistoryHost returns information about the internal states of a history host
func (adh *AdminHandler) DescribeHistoryHost(ctx context.Context, request *adminservice.DescribeHistoryHostRequest) (_ *adminservice.DescribeHistoryHostResponse, retError error) {
	defer log.CapturePanic(adh.GetLogger(), &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminDescribeHistoryHostScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}

	flagsCount := 0
	if request.ShardId != 0 {
		flagsCount++
	}
	if len(request.Namespace) != 0 && request.WorkflowExecution != nil {
		flagsCount++
	}
	if len(request.GetHostAddress()) > 0 {
		flagsCount++
	}
	if flagsCount != 1 {
		return nil, serviceerror.NewInvalidArgument("must provide one and only one: shard id or namespace & workflow id or host address")
	}

	var err error
	namespaceID := ""
	if request.WorkflowExecution != nil {
		namespaceID, err = adh.GetNamespaceCache().GetNamespaceID(request.Namespace)
		if err != nil {
			return nil, adh.error(err, scope)
		}

		if err := validateExecution(request.WorkflowExecution); err != nil {
			return nil, adh.error(err, scope)
		}
	}

	resp, err := adh.GetHistoryClient().DescribeHistoryHost(ctx, &historyservice.DescribeHistoryHostRequest{
		HostAddress:       request.GetHostAddress(),
		ShardId:           request.GetShardId(),
		NamespaceId:       namespaceID,
		WorkflowExecution: request.GetWorkflowExecution(),
	})

	if resp == nil {
		return nil, err
	}

	return &adminservice.DescribeHistoryHostResponse{
		ShardsNumber:          resp.GetShardsNumber(),
		ShardIds:              resp.GetShardIds(),
		NamespaceCache:        resp.GetNamespaceCache(),
		ShardControllerStatus: resp.GetShardControllerStatus(),
		Address:               resp.GetAddress(),
	}, err
}

// GetWorkflowExecutionRawHistoryV2 - retrieves the history of workflow execution
func (adh *AdminHandler) GetWorkflowExecutionRawHistoryV2(ctx context.Context, request *adminservice.GetWorkflowExecutionRawHistoryV2Request) (_ *adminservice.GetWorkflowExecutionRawHistoryV2Response, retError error) {
	defer log.CapturePanic(adh.GetLogger(), &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminGetWorkflowExecutionRawHistoryV2Scope)
	defer sw.Stop()

	if err := adh.validateGetWorkflowExecutionRawHistoryV2Request(
		request,
	); err != nil {
		return nil, adh.error(err, scope)
	}
	namespaceID, err := adh.GetNamespaceCache().GetNamespaceID(request.GetNamespace())
	if err != nil {
		return nil, adh.error(err, scope)
	}
	scope = scope.Tagged(metrics.NamespaceTag(request.GetNamespace()))

	execution := request.Execution
	var pageToken *tokenspb.RawHistoryContinuation
	var targetVersionHistory *historyspb.VersionHistory
	if request.NextPageToken == nil {
		response, err := adh.GetHistoryClient().GetMutableState(ctx, &historyservice.GetMutableStateRequest{
			NamespaceId: namespaceID,
			Execution:   execution,
		})
		if err != nil {
			return nil, adh.error(err, scope)
		}

		targetVersionHistory, err = adh.setRequestDefaultValueAndGetTargetVersionHistory(
			request,
			response.GetVersionHistories(),
		)
		if err != nil {
			return nil, adh.error(err, scope)
		}

		pageToken = generatePaginationToken(request, response.GetVersionHistories())
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
			versionHistories,
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
			HistoryBatches: []*commonpb.DataBlob{},
			NextPageToken:  nil, // no further pagination
			VersionHistory: targetVersionHistory,
		}, nil
	}
	pageSize := int(request.GetMaximumPageSize())
	shardID := common.WorkflowIDToHistoryShard(
		namespaceID,
		execution.GetWorkflowId(),
		adh.numberOfHistoryShards,
	)
	rawHistoryResponse, err := adh.GetExecutionManager().ReadRawHistoryBranch(&persistence.ReadHistoryBranchRequest{
		BranchToken: targetVersionHistory.GetBranchToken(),
		// GetWorkflowExecutionRawHistoryV2 is exclusive exclusive.
		// ReadRawHistoryBranch is inclusive exclusive.
		MinEventID:    pageToken.GetStartEventId() + 1,
		MaxEventID:    pageToken.GetEndEventId(),
		PageSize:      pageSize,
		NextPageToken: pageToken.PersistenceToken,
		ShardID:       shardID,
	})
	if err != nil {
		if _, ok := err.(*serviceerror.NotFound); ok {
			// when no events can be returned from DB, DB layer will return
			// EntityNotExistsError, this API shall return empty response
			return &adminservice.GetWorkflowExecutionRawHistoryV2Response{
				HistoryBatches: []*commonpb.DataBlob{},
				NextPageToken:  nil, // no further pagination
				VersionHistory: targetVersionHistory,
			}, nil
		}
		return nil, err
	}

	pageToken.PersistenceToken = rawHistoryResponse.NextPageToken
	size := rawHistoryResponse.Size
	// N.B. - Dual emit is required here so that we can see aggregate timer stats across all
	// namespaces along with the individual namespaces stats
	adh.GetMetricsClient().
		Scope(metrics.AdminGetWorkflowExecutionRawHistoryScope, metrics.StatsTypeTag(metrics.SizeStatsTypeTagValue)).
		RecordDistribution(metrics.HistorySize, size)
	scope.Tagged(metrics.StatsTypeTag(metrics.SizeStatsTypeTagValue)).
		RecordDistribution(metrics.HistorySize, size)

	result := &adminservice.GetWorkflowExecutionRawHistoryV2Response{
		HistoryBatches: rawHistoryResponse.HistoryEventBlobs,
		VersionHistory: targetVersionHistory,
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

// DescribeCluster return information about temporal deployment
func (adh *AdminHandler) DescribeCluster(ctx context.Context, _ *adminservice.DescribeClusterRequest) (_ *adminservice.DescribeClusterResponse, retError error) {
	defer log.CapturePanic(adh.GetLogger(), &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminGetWorkflowExecutionRawHistoryV2Scope)
	defer sw.Stop()

	membershipInfo := &clusterspb.MembershipInfo{}
	if monitor := adh.GetMembershipMonitor(); monitor != nil {
		currentHost, err := monitor.WhoAmI()
		if err != nil {
			return nil, adh.error(err, scope)
		}

		membershipInfo.CurrentHost = &clusterspb.HostInfo{
			Identity: currentHost.Identity(),
		}

		members, err := monitor.GetReachableMembers()
		if err != nil {
			return nil, adh.error(err, scope)
		}

		membershipInfo.ReachableMembers = members

		var rings []*clusterspb.RingInfo
		for _, role := range []string{common.FrontendServiceName, common.HistoryServiceName, common.MatchingServiceName, common.WorkerServiceName} {
			resolver, err := monitor.GetResolver(role)
			if err != nil {
				return nil, adh.error(err, scope)
			}

			var servers []*clusterspb.HostInfo
			for _, server := range resolver.Members() {
				servers = append(servers, &clusterspb.HostInfo{
					Identity: server.Identity(),
				})
			}

			rings = append(rings, &clusterspb.RingInfo{
				Role:        role,
				MemberCount: int32(resolver.MemberCount()),
				Members:     servers,
			})
		}
		membershipInfo.Rings = rings
	}

	return &adminservice.DescribeClusterResponse{
		SupportedClients: headers.SupportedClients,
		ServerVersion:    headers.ServerVersion,
		MembershipInfo:   membershipInfo,
	}, nil
}

// GetReplicationMessages returns new replication tasks since the read level provided in the token.
func (adh *AdminHandler) GetReplicationMessages(ctx context.Context, request *adminservice.GetReplicationMessagesRequest) (_ *adminservice.GetReplicationMessagesResponse, retError error) {
	defer log.CapturePanic(adh.GetLogger(), &retError)

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
	return &adminservice.GetReplicationMessagesResponse{ShardMessages: resp.GetShardMessages()}, nil
}

// GetNamespaceReplicationMessages returns new namespace replication tasks since last retrieved task ID.
func (adh *AdminHandler) GetNamespaceReplicationMessages(ctx context.Context, request *adminservice.GetNamespaceReplicationMessagesRequest) (_ *adminservice.GetNamespaceReplicationMessagesResponse, retError error) {
	defer log.CapturePanic(adh.GetLogger(), &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminGetNamespaceReplicationMessagesScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}

	if adh.GetNamespaceReplicationQueue() == nil {
		return nil, adh.error(errors.New("namespace replication queue not enabled for cluster"), scope)
	}

	lastMessageID := request.GetLastRetrievedMessageId()
	if request.GetLastRetrievedMessageId() == defaultLastMessageID {
		if clusterAckLevels, err := adh.GetNamespaceReplicationQueue().GetAckLevels(); err == nil {
			if ackLevel, ok := clusterAckLevels[request.GetClusterName()]; ok {
				lastMessageID = ackLevel
			}
		}
	}

	replicationTasks, lastMessageID, err := adh.GetNamespaceReplicationQueue().GetReplicationMessages(
		lastMessageID,
		getNamespaceReplicationMessageBatchSize,
	)
	if err != nil {
		return nil, adh.error(err, scope)
	}

	if request.GetLastProcessedMessageId() != defaultLastMessageID {
		if err := adh.GetNamespaceReplicationQueue().UpdateAckLevel(
			request.GetLastProcessedMessageId(),
			request.GetClusterName(),
		); err != nil {
			adh.GetLogger().Warn("Failed to update namespace replication queue ack level",
				tag.TaskID(request.GetLastProcessedMessageId()),
				tag.ClusterName(request.GetClusterName()))
		}
	}

	return &adminservice.GetNamespaceReplicationMessagesResponse{
		Messages: &replicationspb.ReplicationMessages{
			ReplicationTasks:       replicationTasks,
			LastRetrievedMessageId: int64(lastMessageID),
		},
	}, nil
}

// GetDLQReplicationMessages returns new replication tasks based on the dlq info.
func (adh *AdminHandler) GetDLQReplicationMessages(ctx context.Context, request *adminservice.GetDLQReplicationMessagesRequest) (_ *adminservice.GetDLQReplicationMessagesResponse, retError error) {
	defer log.CapturePanic(adh.GetLogger(), &retError)

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
	defer log.CapturePanic(adh.GetLogger(), &retError)
	scope, sw := adh.startRequestProfile(metrics.AdminReapplyEventsScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}
	if request.GetNamespace() == "" {
		return nil, adh.error(errNamespaceNotSet, scope)
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
	namespaceEntry, err := adh.GetNamespaceCache().GetNamespace(request.GetNamespace())
	if err != nil {
		return nil, adh.error(err, scope)
	}

	_, err = adh.GetHistoryClient().ReapplyEvents(ctx, &historyservice.ReapplyEventsRequest{
		NamespaceId: namespaceEntry.GetInfo().Id,
		Request:     request,
	})
	if err != nil {
		return nil, adh.error(err, scope)
	}
	return &adminservice.ReapplyEventsResponse{}, nil
}

// GetDLQMessages reads messages from DLQ
func (adh *AdminHandler) GetDLQMessages(
	ctx context.Context,
	request *adminservice.GetDLQMessagesRequest,
) (resp *adminservice.GetDLQMessagesResponse, retErr error) {

	defer log.CapturePanic(adh.GetLogger(), &retErr)
	scope, sw := adh.startRequestProfile(metrics.AdminReadDLQMessagesScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}

	if request.GetMaximumPageSize() <= 0 {
		request.MaximumPageSize = common.ReadDLQMessagesPageSize
	}

	if request.GetInclusiveEndMessageId() <= 0 {
		request.InclusiveEndMessageId = common.EndMessageID
	}

	var tasks []*replicationspb.ReplicationTask
	var token []byte
	var op func() error
	switch request.GetType() {
	case enumsspb.DEAD_LETTER_QUEUE_TYPE_REPLICATION:
		resp, err := adh.GetHistoryClient().GetDLQMessages(ctx, &historyservice.GetDLQMessagesRequest{
			Type:                  request.GetType(),
			ShardId:               request.GetShardId(),
			SourceCluster:         request.GetSourceCluster(),
			InclusiveEndMessageId: request.GetInclusiveEndMessageId(),
			MaximumPageSize:       request.GetMaximumPageSize(),
			NextPageToken:         request.GetNextPageToken(),
		})

		if resp == nil {
			return nil, err
		}

		return &adminservice.GetDLQMessagesResponse{
			Type:             resp.GetType(),
			ReplicationTasks: resp.GetReplicationTasks(),
			NextPageToken:    resp.GetNextPageToken(),
		}, err
	case enumsspb.DEAD_LETTER_QUEUE_TYPE_NAMESPACE:
		op = func() error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				var err error
				tasks, token, err = adh.namespaceDLQHandler.Read(
					request.GetInclusiveEndMessageId(),
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

	return &adminservice.GetDLQMessagesResponse{
		ReplicationTasks: tasks,
		NextPageToken:    token,
	}, nil
}

// PurgeDLQMessages purge messages from DLQ
func (adh *AdminHandler) PurgeDLQMessages(
	ctx context.Context,
	request *adminservice.PurgeDLQMessagesRequest,
) (_ *adminservice.PurgeDLQMessagesResponse, err error) {

	defer log.CapturePanic(adh.GetLogger(), &err)
	scope, sw := adh.startRequestProfile(metrics.AdminPurgeDLQMessagesScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}

	if request.GetInclusiveEndMessageId() <= 0 {
		request.InclusiveEndMessageId = common.EndMessageID
	}

	var op func() error
	switch request.GetType() {
	case enumsspb.DEAD_LETTER_QUEUE_TYPE_REPLICATION:
		resp, err := adh.GetHistoryClient().PurgeDLQMessages(ctx, &historyservice.PurgeDLQMessagesRequest{
			Type:                  request.GetType(),
			ShardId:               request.GetShardId(),
			SourceCluster:         request.GetSourceCluster(),
			InclusiveEndMessageId: request.GetInclusiveEndMessageId(),
		})

		if resp == nil {
			return nil, adh.error(err, scope)
		}

		return &adminservice.PurgeDLQMessagesResponse{}, err
	case enumsspb.DEAD_LETTER_QUEUE_TYPE_NAMESPACE:
		op = func() error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				return adh.namespaceDLQHandler.Purge(request.GetInclusiveEndMessageId())
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

	defer log.CapturePanic(adh.GetLogger(), &err)
	scope, sw := adh.startRequestProfile(metrics.AdminMergeDLQMessagesScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}

	if request.GetInclusiveEndMessageId() <= 0 {
		request.InclusiveEndMessageId = common.EndMessageID
	}

	var token []byte
	var op func() error
	switch request.GetType() {
	case enumsspb.DEAD_LETTER_QUEUE_TYPE_REPLICATION:
		resp, err := adh.GetHistoryClient().MergeDLQMessages(ctx, &historyservice.MergeDLQMessagesRequest{
			Type:                  request.GetType(),
			ShardId:               request.GetShardId(),
			SourceCluster:         request.GetSourceCluster(),
			InclusiveEndMessageId: request.GetInclusiveEndMessageId(),
			MaximumPageSize:       request.GetMaximumPageSize(),
			NextPageToken:         request.GetNextPageToken(),
		})
		if resp == nil {
			return nil, adh.error(err, scope)
		}

		return &adminservice.MergeDLQMessagesResponse{
			NextPageToken: request.GetNextPageToken(),
		}, nil
	case enumsspb.DEAD_LETTER_QUEUE_TYPE_NAMESPACE:

		op = func() error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				var err error
				token, err = adh.namespaceDLQHandler.Merge(
					request.GetInclusiveEndMessageId(),
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
	defer log.CapturePanic(adh.GetLogger(), &err)
	scope, sw := adh.startRequestProfile(metrics.AdminRefreshWorkflowTasksScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}
	if err := validateExecution(request.Execution); err != nil {
		return nil, adh.error(err, scope)
	}
	namespaceEntry, err := adh.GetNamespaceCache().GetNamespace(request.GetNamespace())
	if err != nil {
		return nil, adh.error(err, scope)
	}

	_, err = adh.GetHistoryClient().RefreshWorkflowTasks(ctx, &historyservice.RefreshWorkflowTasksRequest{
		NamespaceId: namespaceEntry.GetInfo().Id,
		Request:     request,
	})
	if err != nil {
		return nil, adh.error(err, scope)
	}
	return &adminservice.RefreshWorkflowTasksResponse{}, nil
}

// ResendReplicationTasks requests replication task from remote cluster
func (adh *AdminHandler) ResendReplicationTasks(
	ctx context.Context,
	request *adminservice.ResendReplicationTasksRequest,
) (_ *adminservice.ResendReplicationTasksResponse, err error) {
	defer log.CapturePanic(adh.GetLogger(), &err)
	scope, sw := adh.startRequestProfile(metrics.AdminResendReplicationTasksScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}

	resender := xdc.NewNDCHistoryResender(
		adh.GetNamespaceCache(),
		adh.GetRemoteAdminClient(request.GetRemoteCluster()),
		func(ctx context.Context, request *historyservice.ReplicateEventsV2Request) error {
			_, err1 := adh.GetHistoryClient().ReplicateEventsV2(ctx, request)
			return err1
		},
		adh.eventSerializer,
		nil,
		adh.GetLogger(),
	)
	if err := resender.SendSingleWorkflowHistory(
		request.GetNamespaceId(),
		request.GetWorkflowId(),
		request.GetRunId(),
		resendStartEventID,
		request.StartVersion,
		common.EmptyEventID,
		common.EmptyVersion,
	); err != nil {
		return nil, err
	}
	return &adminservice.ResendReplicationTasksResponse{}, nil
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

func (adh *AdminHandler) setRequestDefaultValueAndGetTargetVersionHistory(
	request *adminservice.GetWorkflowExecutionRawHistoryV2Request,
	versionHistories *historyspb.VersionHistories,
) (*historyspb.VersionHistory, error) {

	targetBranch, err := versionhistory.GetCurrentVersionHistory(versionHistories)
	if err != nil {
		return nil, err
	}
	firstItem, err := versionhistory.GetFirstVersionHistoryItem(targetBranch)
	if err != nil {
		return nil, err
	}
	lastItem, err := versionhistory.GetLastVersionHistoryItem(targetBranch)
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
		request.EndEventId = lastItem.GetEventId() + 1
		request.EndEventVersion = lastItem.GetVersion()
	}

	if request.GetStartEventId() < 0 {
		return nil, errInvalidFirstNextEventCombination
	}

	// get branch based on the end event if end event is defined in the request
	if request.GetEndEventId() == lastItem.GetEventId()+1 &&
		request.GetEndEventVersion() == lastItem.GetVersion() {
		// this is a special case, target branch remains the same
	} else {
		endItem := versionhistory.NewVersionHistoryItem(request.GetEndEventId(), request.GetEndEventVersion())
		idx, err := versionhistory.FindFirstVersionHistoryIndexByVersionHistoryItem(versionHistories, endItem)
		if err != nil {
			return nil, err
		}

		targetBranch, err = versionhistory.GetVersionHistory(versionHistories, idx)
		if err != nil {
			return nil, err
		}
	}

	startItem := versionhistory.NewVersionHistoryItem(request.GetStartEventId(), request.GetStartEventVersion())
	// If the request start event is defined. The start event may be on a different branch as current branch.
	// We need to find the LCA of the start event and the current branch.
	if request.GetStartEventId() == common.FirstEventID-1 &&
		request.GetStartEventVersion() == firstItem.GetVersion() {
		// this is a special case, start event is on the same branch as target branch
	} else {
		if !versionhistory.ContainsVersionHistoryItem(targetBranch, startItem) {
			idx, err := versionhistory.FindFirstVersionHistoryIndexByVersionHistoryItem(versionHistories, startItem)
			if err != nil {
				return nil, err
			}
			startBranch, err := versionhistory.GetVersionHistory(versionHistories, idx)
			if err != nil {
				return nil, err
			}
			startItem, err = versionhistory.FindLCAVersionHistoryItem(targetBranch, startBranch)
			if err != nil {
				return nil, err
			}
			request.StartEventId = startItem.GetEventId()
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
	case *serviceerror.Unavailable:
		adh.GetLogger().Error("unavailable error", tag.Error(err))
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
