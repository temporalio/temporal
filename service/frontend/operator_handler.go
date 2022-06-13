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
	"fmt"
  "net"
	"sync/atomic"
	"time"

	"github.com/pborman/uuid"
	clusterpb "go.temporal.io/api/cluster/v1"
	"go.temporal.io/server/api/adminservice/v1"
  "go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	serverClient "go.temporal.io/server/client"
	"go.temporal.io/server/client/admin"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	sdkclient "go.temporal.io/sdk/client"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	
	"go.temporal.io/server/common"
  clustermetadata "go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/worker"
	"go.temporal.io/server/service/worker/addsearchattributes"
	"go.temporal.io/server/service/worker/deletenamespace"
	"go.temporal.io/server/service/worker/deletenamespace/deleteexecutions"
)

var _ OperatorHandler = (*OperatorHandlerImpl)(nil)

type (
	// OperatorHandlerImpl - gRPC handler interface for operatorservice
	OperatorHandlerImpl struct {
		status int32

		healthStatus           int32
		logger                 log.Logger
		config                 *Config
		esConfig               *esclient.Config
		esClient               esclient.Client
		sdkClientFactory       sdk.ClientFactory
		metricsClient          metrics.Client
		saProvider             searchattribute.Provider
		saManager              searchattribute.Manager
		healthServer           *health.Server
		historyClient          historyservice.HistoryServiceClient
		namespaceRegistry      namespace.Registry
		clusterMetadataManager persistence.ClusterMetadataManager
		clusterMetadata        clustermetadata.Metadata
		clientFactory          serverClient.Factory
		adminClient            adminservice.AdminServiceClient
	}

	NewOperatorHandlerImplArgs struct {
		config                 *Config
		EsConfig               *esclient.Config
		EsClient               esclient.Client
		Logger                 log.Logger
		sdkClientFactory       sdk.ClientFactory
		MetricsClient          metrics.Client
		SaProvider             searchattribute.Provider
		SaManager              searchattribute.Manager
		healthServer           *health.Server
		historyClient          historyservice.HistoryServiceClient
		namespaceRegistry      namespace.Registry
		clusterMetadataManager persistence.ClusterMetadataManager
		clusterMetadata        clustermetadata.Metadata
		clientFactory          serverClient.Factory
		adminClient            adminservice.AdminServiceClient
	}
)

// NewOperatorHandlerImpl creates a gRPC handler for operatorservice
func NewOperatorHandlerImpl(
	args NewOperatorHandlerImplArgs,
) *OperatorHandlerImpl {

	handler := &OperatorHandlerImpl{
		logger:                 args.Logger,
		status:                 common.DaemonStatusInitialized,
		config:                 args.config,
		esConfig:               args.EsConfig,
		esClient:               args.EsClient,
		sdkClientFactory:       args.sdkClientFactory,
		metricsClient:          args.MetricsClient,
		saProvider:             args.SaProvider,
		saManager:              args.SaManager,
		healthServer:           args.healthServer,
		historyClient:          args.historyClient,
		namespaceRegistry:      args.namespaceRegistry,
		clusterMetadataManager: args.clusterMetadataManager,
		clusterMetadata:        args.clusterMetadata,
		clientFactory:          args.clientFactory,
		adminClient:            args.adminClient,
	}

	return handler
}

// Start starts the handler
func (h *OperatorHandlerImpl) Start() {
	if atomic.CompareAndSwapInt32(
		&h.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		h.healthServer.SetServingStatus(OperatorServiceName, healthpb.HealthCheckResponse_SERVING)
	}
}

// Stop stops the handler
func (h *OperatorHandlerImpl) Stop() {
	if atomic.CompareAndSwapInt32(
		&h.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		h.healthServer.SetServingStatus(OperatorServiceName, healthpb.HealthCheckResponse_NOT_SERVING)
	}
}

func (h *OperatorHandlerImpl) AddSearchAttributes(ctx context.Context, request *operatorservice.AddSearchAttributesRequest) (_ *operatorservice.AddSearchAttributesResponse, retError error) {
	const endpointName = "AddSearchAttributes"

	defer log.CapturePanic(h.logger, &retError)

	scope, sw := h.startRequestProfile(metrics.OperatorAddSearchAttributesScope)
	defer sw.Stop()

	// validate request
	if request == nil {
		return nil, h.error(errRequestNotSet, scope, endpointName)
	}

	if len(request.GetSearchAttributes()) == 0 {
		return nil, h.error(errSearchAttributesNotSet, scope, endpointName)
	}

	indexName := h.esConfig.GetVisibilityIndex()

	currentSearchAttributes, err := h.saProvider.GetSearchAttributes(indexName, true)
	if err != nil {
		return nil, h.error(serviceerror.NewUnavailable(fmt.Sprintf(errUnableToGetSearchAttributesMessage, err)), scope, endpointName)
	}

	for saName, saType := range request.GetSearchAttributes() {
		if searchattribute.IsReserved(saName) {
			return nil, h.error(serviceerror.NewInvalidArgument(fmt.Sprintf(errSearchAttributeIsReservedMessage, saName)), scope, endpointName)
		}
		if currentSearchAttributes.IsDefined(saName) {
			return nil, h.error(serviceerror.NewAlreadyExist(fmt.Sprintf(errSearchAttributeAlreadyExistsMessage, saName)), scope, endpointName)
		}
		if _, ok := enumspb.IndexedValueType_name[int32(saType)]; !ok {
			return nil, h.error(serviceerror.NewInvalidArgument(fmt.Sprintf(errUnknownSearchAttributeTypeMessage, saType)), scope, endpointName)
		}
	}

	// Execute workflow.
	wfParams := addsearchattributes.WorkflowParams{
		CustomAttributesToAdd: request.GetSearchAttributes(),
		IndexName:             indexName,
		SkipSchemaUpdate:      false,
	}

	sdkClient := h.sdkClientFactory.GetSystemClient(h.logger)
	run, err := sdkClient.ExecuteWorkflow(
		ctx,
		sdkclient.StartWorkflowOptions{
			TaskQueue: worker.DefaultWorkerTaskQueue,
			ID:        addsearchattributes.WorkflowName,
		},
		addsearchattributes.WorkflowName,
		wfParams,
	)
	if err != nil {
		return nil, h.error(serviceerror.NewUnavailable(fmt.Sprintf(errUnableToStartWorkflowMessage, addsearchattributes.WorkflowName, err)), scope, endpointName)
	}

	// Wait for workflow to complete.
	err = run.Get(ctx, nil)
	if err != nil {
		scope.IncCounter(metrics.AddSearchAttributesWorkflowFailuresCount)
		execution := &commonpb.WorkflowExecution{WorkflowId: addsearchattributes.WorkflowName, RunId: run.GetRunID()}
		return nil, h.error(serviceerror.NewSystemWorkflow(execution, err), scope, endpointName)
	}
	scope.IncCounter(metrics.AddSearchAttributesWorkflowSuccessCount)

	return &operatorservice.AddSearchAttributesResponse{}, nil
}

func (h *OperatorHandlerImpl) RemoveSearchAttributes(ctx context.Context, request *operatorservice.RemoveSearchAttributesRequest) (_ *operatorservice.RemoveSearchAttributesResponse, retError error) {
	const endpointName = "RemoveSearchAttributes"

	defer log.CapturePanic(h.logger, &retError)

	scope, sw := h.startRequestProfile(metrics.OperatorRemoveSearchAttributesScope)
	defer sw.Stop()

	// validate request
	if request == nil {
		return nil, h.error(errRequestNotSet, scope, endpointName)
	}

	if len(request.GetSearchAttributes()) == 0 {
		return nil, h.error(errSearchAttributesNotSet, scope, endpointName)
	}

	indexName := h.esConfig.GetVisibilityIndex()

	currentSearchAttributes, err := h.saProvider.GetSearchAttributes(indexName, true)
	if err != nil {
		return nil, h.error(serviceerror.NewUnavailable(fmt.Sprintf(errUnableToGetSearchAttributesMessage, err)), scope, endpointName)
	}

	newCustomSearchAttributes := map[string]enumspb.IndexedValueType{}
	for saName, saType := range currentSearchAttributes.Custom() {
		newCustomSearchAttributes[saName] = saType
	}

	for _, saName := range request.GetSearchAttributes() {
		if !currentSearchAttributes.IsDefined(saName) {
			return nil, h.error(serviceerror.NewNotFound(fmt.Sprintf(errSearchAttributeDoesntExistMessage, saName)), scope, endpointName)
		}
		if _, ok := newCustomSearchAttributes[saName]; !ok {
			return nil, h.error(serviceerror.NewInvalidArgument(fmt.Sprintf(errUnableToRemoveNonCustomSearchAttributesMessage, saName)), scope, endpointName)
		}
		delete(newCustomSearchAttributes, saName)
	}

	err = h.saManager.SaveSearchAttributes(ctx, indexName, newCustomSearchAttributes)
	if err != nil {
		return nil, h.error(serviceerror.NewUnavailable(fmt.Sprintf(errUnableToSaveSearchAttributesMessage, err)), scope, endpointName)
	}

	return &operatorservice.RemoveSearchAttributesResponse{}, nil
}

func (h *OperatorHandlerImpl) ListSearchAttributes(ctx context.Context, request *operatorservice.ListSearchAttributesRequest) (_ *operatorservice.ListSearchAttributesResponse, retError error) {
	const endpointName = "ListSearchAttributes"

	defer log.CapturePanic(h.logger, &retError)

	scope, sw := h.startRequestProfile(metrics.OperatorListSearchAttributesScope)
	defer sw.Stop()

	if request == nil {
		return nil, h.error(errRequestNotSet, scope, endpointName)
	}

	indexName := h.esConfig.GetVisibilityIndex()

	var lastErr error
	var esMapping map[string]string = nil
	if h.esClient != nil {
		esMapping, lastErr = h.esClient.GetMapping(ctx, indexName)
		if lastErr != nil {
			lastErr = h.error(serviceerror.NewUnavailable(fmt.Sprintf("unable to get mapping from Elasticsearch: %v", lastErr)), scope, endpointName)
		}
	}

	searchAttributes, err := h.saProvider.GetSearchAttributes(indexName, true)
	if err != nil {
		lastErr = h.error(serviceerror.NewUnavailable(fmt.Sprintf("unable to read custom search attributes: %v", err)), scope, endpointName)
	}

	if lastErr != nil {
		return nil, lastErr
	}

	return &operatorservice.ListSearchAttributesResponse{
		CustomAttributes: searchAttributes.Custom(),
		SystemAttributes: searchAttributes.System(),
		StorageSchema:    esMapping,
	}, nil
}

func (h *OperatorHandlerImpl) DeleteNamespace(ctx context.Context, request *operatorservice.DeleteNamespaceRequest) (_ *operatorservice.DeleteNamespaceResponse, retError error) {
	const endpointName = "DeleteNamespace"

	defer log.CapturePanic(h.logger, &retError)

	scope, sw := h.startRequestProfile(metrics.OperatorDeleteNamespaceScope)
	defer sw.Stop()

	// validate request
	if request == nil {
		return nil, h.error(errRequestNotSet, scope, endpointName)
	}

	// Execute workflow.
	wfParams := deletenamespace.DeleteNamespaceWorkflowParams{
		Namespace: namespace.Name(request.GetNamespace()),
		DeleteExecutionsConfig: deleteexecutions.DeleteExecutionsConfig{
			DeleteActivityRPS:                    h.config.DeleteNamespaceDeleteActivityRPS(),
			ConcurrentDeleteExecutionsActivities: h.config.DeleteNamespaceConcurrentDeleteExecutionsActivities(),
		},
	}

	sdkClient := h.sdkClientFactory.GetSystemClient(h.logger)
	run, err := sdkClient.ExecuteWorkflow(
		ctx,
		sdkclient.StartWorkflowOptions{
			TaskQueue: worker.DefaultWorkerTaskQueue,
			ID:        fmt.Sprintf("%s/%s", deletenamespace.WorkflowName, request.GetNamespace()),
		},
		deletenamespace.WorkflowName,
		wfParams,
	)
	if err != nil {
		return nil, h.error(serviceerror.NewUnavailable(fmt.Sprintf(errUnableToStartWorkflowMessage, deletenamespace.WorkflowName, err)), scope, endpointName)
	}

	// Wait for workflow to complete.
	var wfResult deletenamespace.DeleteNamespaceWorkflowResult
	err = run.Get(ctx, &wfResult)
	if err != nil {
		scope.IncCounter(metrics.DeleteNamespaceWorkflowFailuresCount)
		execution := &commonpb.WorkflowExecution{WorkflowId: deletenamespace.WorkflowName, RunId: run.GetRunID()}
		return nil, h.error(serviceerror.NewSystemWorkflow(execution, err), scope, endpointName)
	}
	scope.IncCounter(metrics.DeleteNamespaceWorkflowSuccessCount)

	return &operatorservice.DeleteNamespaceResponse{
		DeletedNamespace: wfResult.DeletedNamespace.String(),
	}, nil
}

// DeleteWorkflowExecution deletes a closed workflow execution asynchronously (workflow must be completed or terminated before).
// This method is EXPERIMENTAL and may be changed or removed in a later release.
func (h *OperatorHandlerImpl) DeleteWorkflowExecution(ctx context.Context, request *operatorservice.DeleteWorkflowExecutionRequest) (_ *operatorservice.DeleteWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	if err := validateExecution(request.WorkflowExecution); err != nil {
		return nil, err
	}

	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	_, err = h.historyClient.DeleteWorkflowExecution(ctx, &historyservice.DeleteWorkflowExecutionRequest{
		NamespaceId:       namespaceID.String(),
		WorkflowExecution: request.GetWorkflowExecution(),
	})
	if err != nil {
		return nil, err
	}

	return &operatorservice.DeleteWorkflowExecutionResponse{}, nil
}

// AddOrUpdateRemoteCluster adds or updates the connection config to a remote cluster.
func (h *OperatorHandlerImpl) AddOrUpdateRemoteCluster(
	ctx context.Context,
	request *operatorservice.AddOrUpdateRemoteClusterRequest,
) (_ *operatorservice.AddOrUpdateRemoteClusterResponse, retError error) {
	const endpointName = "AddOrUpdateRemoteCluster"
	defer log.CapturePanic(h.logger, &retError)

	scope, sw := h.startRequestProfile(metrics.OperatorAddOrUpdateRemoteClusterScope)
	defer sw.Stop()

	adminClient := h.clientFactory.NewAdminClientWithTimeout(
		request.GetFrontendAddress(),
		admin.DefaultTimeout,
		admin.DefaultLargeTimeout,
	)

	// Fetch cluster metadata from remote cluster
	resp, err := adminClient.DescribeCluster(ctx, &adminservice.DescribeClusterRequest{})
	if err != nil {
		return nil, h.error(err, scope, endpointName)
	}

	err = h.validateRemoteClusterMetadata(resp)
	if err != nil {
		return nil, h.error(err, scope, endpointName)
	}

	var updateRequestVersion int64 = 0
	clusterMetadataMgr := h.clusterMetadataManager
	clusterData, err := clusterMetadataMgr.GetClusterMetadata(
		ctx,
		&persistence.GetClusterMetadataRequest{ClusterName: resp.GetClusterName()},
	)
	switch err.(type) {
	case nil:
		updateRequestVersion = clusterData.Version
	case *serviceerror.NotFound:
		updateRequestVersion = 0
	default:
		return nil, h.error(err, scope, endpointName)
	}

	applied, err := clusterMetadataMgr.SaveClusterMetadata(ctx, &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: persistencespb.ClusterMetadata{
			ClusterName:              resp.GetClusterName(),
			HistoryShardCount:        resp.GetHistoryShardCount(),
			ClusterId:                resp.GetClusterId(),
			ClusterAddress:           request.GetFrontendAddress(),
			FailoverVersionIncrement: resp.GetFailoverVersionIncrement(),
			InitialFailoverVersion:   resp.GetInitialFailoverVersion(),
			IsGlobalNamespaceEnabled: resp.GetIsGlobalNamespaceEnabled(),
			IsConnectionEnabled:      request.GetEnableRemoteClusterConnection(),
		},
		Version: updateRequestVersion,
	})
	if err != nil {
		return nil, h.error(err, scope, endpointName)
	}
	if !applied {
		return nil, h.error(serviceerror.NewInvalidArgument(
			"Cannot update remote cluster due to update immutable fields"),
			scope,
			endpointName,
		)
	}
	return &operatorservice.AddOrUpdateRemoteClusterResponse{}, nil
}

func (h *OperatorHandlerImpl) RemoveRemoteCluster(
	ctx context.Context,
	request *operatorservice.RemoveRemoteClusterRequest,
) (_ *operatorservice.RemoveRemoteClusterResponse, retError error) {
	const endpointName = "RemoveRemoteCluster"
	defer log.CapturePanic(h.logger, &retError)

	scope, sw := h.startRequestProfile(metrics.OperatorRemoveRemoteClusterScope)
	defer sw.Stop()

	if err := h.clusterMetadataManager.DeleteClusterMetadata(
		ctx,
		&persistence.DeleteClusterMetadataRequest{ClusterName: request.GetClusterName()},
	); err != nil {
		return nil, h.error(err, scope, endpointName)
	}
	return &operatorservice.RemoveRemoteClusterResponse{}, nil
}

func (h *OperatorHandlerImpl) DescribeCluster(
	ctx context.Context,
	request *operatorservice.DescribeClusterRequest,
) (_ *operatorservice.DescribeClusterResponse, retError error) {
	const endpointName = "DescribeCluster"
	defer log.CapturePanic(h.logger, &retError)

	scope, sw := h.startRequestProfile(metrics.OperatorDescribeClusterScope)
	defer sw.Stop()

	resp, err := h.adminClient.DescribeCluster(ctx, &adminservice.DescribeClusterRequest{
		ClusterName: request.GetClusterName(),
	})
	if err != nil {
		return nil, h.error(err, scope, endpointName)
	}

	var membership *clusterpb.MembershipInfo
	if resp.GetMembershipInfo() != nil {
		membership = &clusterpb.MembershipInfo{
			CurrentHost: &clusterpb.HostInfo{
				Identity: resp.GetMembershipInfo().GetCurrentHost().Identity,
			},
			ReachableMembers: resp.GetMembershipInfo().GetReachableMembers(),
			Rings:            []*clusterpb.RingInfo{},
		}
		for _, ringInfo := range resp.GetMembershipInfo().GetRings() {
			var members []*clusterpb.HostInfo
			for _, member := range ringInfo.GetMembers() {
				members = append(members, &clusterpb.HostInfo{
					Identity: member.GetIdentity(),
				})
			}
			membership.Rings = append(membership.Rings, &clusterpb.RingInfo{
				Role:        ringInfo.GetRole(),
				MemberCount: ringInfo.GetMemberCount(),
				Members:     members,
			})
		}
	}
	return &operatorservice.DescribeClusterResponse{
		SupportedClients:         resp.GetSupportedClients(),
		ServerVersion:            resp.GetServerVersion(),
		MembershipInfo:           membership,
		ClusterId:                resp.GetClusterId(),
		ClusterName:              resp.GetClusterName(),
		HistoryShardCount:        resp.GetHistoryShardCount(),
		PersistenceStore:         resp.GetPersistenceStore(),
		VisibilityStore:          resp.GetVisibilityStore(),
		VersionInfo:              resp.GetVersionInfo(),
		FailoverVersionIncrement: resp.GetFailoverVersionIncrement(),
		InitialFailoverVersion:   resp.GetInitialFailoverVersion(),
		IsGlobalNamespaceEnabled: resp.GetIsGlobalNamespaceEnabled(),
	}, nil
}

func (h *OperatorHandlerImpl) ListClusters(
	ctx context.Context,
	request *operatorservice.ListClustersRequest,
) (_ *operatorservice.ListClustersResponse, retError error) {
	const endpointName = "ListClusters"
	defer log.CapturePanic(h.logger, &retError)

	scope, sw := h.startRequestProfile(metrics.OperatorListClustersScope)
	defer sw.Stop()

	resp, err := h.clusterMetadataManager.ListClusterMetadata(ctx, &persistence.ListClusterMetadataRequest{
		PageSize:      int(request.GetPageSize()),
		NextPageToken: request.GetNextPageToken(),
	})
	if err != nil {
		return nil, h.error(err, scope, endpointName)
	}

	var clusterMetadataList []*clusterpb.ClusterMetadata
	indexSearchAttributes := make(map[string]*clusterpb.IndexSearchAttributes)
	for _, clusterResp := range resp.ClusterMetadata {
		if clusterResp.GetIndexSearchAttributes() != nil {
			for name, attr := range clusterResp.GetIndexSearchAttributes() {
				indexSearchAttributes[name] = &clusterpb.IndexSearchAttributes{
					CustomSearchAttributes: attr.GetCustomSearchAttributes(),
				}
			}
		}
		clusterMetadataList = append(clusterMetadataList, &clusterpb.ClusterMetadata{
			Cluster:                  clusterResp.GetClusterName(),
			HistoryShardCount:        clusterResp.GetHistoryShardCount(),
			ClusterId:                clusterResp.GetClusterId(),
			VersionInfo:              clusterResp.GetVersionInfo(),
			IndexSearchAttributes:    indexSearchAttributes,
			ClusterAddress:           clusterResp.GetClusterAddress(),
			FailoverVersionIncrement: clusterResp.GetFailoverVersionIncrement(),
			InitialFailoverVersion:   clusterResp.GetInitialFailoverVersion(),
			IsGlobalNamespaceEnabled: clusterResp.GetIsGlobalNamespaceEnabled(),
			IsConnectionEnabled:      clusterResp.GetIsConnectionEnabled(),
		})
	}
	return &operatorservice.ListClustersResponse{
		Clusters:      clusterMetadataList,
		NextPageToken: resp.NextPageToken,
	}, nil
}

func (h *OperatorHandlerImpl) ListClusterMembers(
	ctx context.Context,
	request *operatorservice.ListClusterMembersRequest,
) (_ *operatorservice.ListClusterMembersResponse, retError error) {
	const endpointName = "ListClusterMembers"
	defer log.CapturePanic(h.logger, &retError)

	scope, sw := h.startRequestProfile(metrics.OperatorListClusterMembersScope)
	defer sw.Stop()

	if request == nil {
		return nil, h.error(errRequestNotSet, scope, endpointName)
	}

	heartbeatRef := request.GetLastHeartbeatWithin()
	var heartbeat time.Duration
	if heartbeatRef != nil {
		heartbeat = *heartbeatRef
	}
	startedTimeRef := request.GetSessionStartedAfterTime()
	var startedTime time.Time
	if startedTimeRef != nil {
		startedTime = *startedTimeRef
	}

	resp, err := h.clusterMetadataManager.GetClusterMembers(ctx, &persistence.GetClusterMembersRequest{
		LastHeartbeatWithin: heartbeat,
		RPCAddressEquals:    net.ParseIP(request.GetRpcAddress()),
		HostIDEquals:        uuid.Parse(request.GetHostId()),
		RoleEquals:          persistence.ServiceType(request.GetRole()),
		SessionStartedAfter: startedTime,
		PageSize:            int(request.GetPageSize()),
		NextPageToken:       request.GetNextPageToken(),
	})
	if err != nil {
		return nil, h.error(err, scope, endpointName)
	}

	var activeMembers []*clusterpb.ClusterMember
	for _, member := range resp.ActiveMembers {
		activeMembers = append(activeMembers, &clusterpb.ClusterMember{
			Role:             enumspb.ClusterMemberRole(member.Role),
			HostId:           member.HostID.String(),
			RpcAddress:       member.RPCAddress.String(),
			RpcPort:          int32(member.RPCPort),
			SessionStartTime: timestamp.TimePtr(member.SessionStart),
			LastHeartbitTime: timestamp.TimePtr(member.LastHeartbeat),
			RecordExpiryTime: timestamp.TimePtr(member.RecordExpiry),
		})
	}
	return &operatorservice.ListClusterMembersResponse{
		ActiveMembers: activeMembers,
		NextPageToken: resp.NextPageToken,
	}, nil
}

func (h *OperatorHandlerImpl) validateRemoteClusterMetadata(metadata *adminservice.DescribeClusterResponse) error {
	// Verify remote cluster config
	currentClusterInfo := h.clusterMetadata
	if metadata.GetClusterName() == currentClusterInfo.GetCurrentClusterName() {
		// cluster name conflict
		return serviceerror.NewInvalidArgument("Cannot update current cluster metadata from rpc calls")
	}
	if metadata.GetFailoverVersionIncrement() != currentClusterInfo.GetFailoverVersionIncrement() {
		// failover version increment is mismatch with current cluster config
		return serviceerror.NewInvalidArgument("Cannot add remote cluster due to failover version increment mismatch")
	}
	if metadata.GetHistoryShardCount() != h.config.NumHistoryShards {
		// cluster shard number not equal
		// TODO: remove this check once we support different shard numbers
		return serviceerror.NewInvalidArgument("Cannot add remote cluster due to history shard number mismatch")
	}
	if !metadata.IsGlobalNamespaceEnabled {
		// remote cluster doesn't support global namespace
		return serviceerror.NewInvalidArgument("Cannot add remote cluster as global namespace is not supported")
	}
	for clusterName, cluster := range currentClusterInfo.GetAllClusterInfo() {
		if clusterName != metadata.ClusterName && cluster.InitialFailoverVersion == metadata.GetInitialFailoverVersion() {
			// initial failover version conflict
			// best effort: race condition if a concurrent write to db with the same version.
			return serviceerror.NewInvalidArgument("Cannot add remote cluster due to initial failover version conflict")
		}
	}
	return nil
}

// startRequestProfile initiates recording of request metrics
func (h *OperatorHandlerImpl) startRequestProfile(scope int) (metrics.Scope, metrics.Stopwatch) {
	metricsScope := h.metricsClient.Scope(scope)
	sw := metricsScope.StartTimer(metrics.ServiceLatency)
	metricsScope.IncCounter(metrics.ServiceRequests)
	return metricsScope, sw
}

func (h *OperatorHandlerImpl) error(err error, scope metrics.Scope, endpointName string) error {
	scope.Tagged(metrics.ServiceErrorTypeTag(err)).IncCounter(metrics.ServiceFailuresWithType)

	switch err := err.(type) {
	case *serviceerror.Unavailable:
		h.logger.Error("Unavailable error.", tag.Error(err), tag.Endpoint(endpointName))
		scope.IncCounter(metrics.ServiceFailures)
	case *serviceerror.InvalidArgument:
		scope.IncCounter(metrics.ServiceErrInvalidArgumentCounter)
	case *serviceerror.ResourceExhausted:
		scope.Tagged(metrics.ResourceExhaustedCauseTag(err.Cause)).IncCounter(metrics.ServiceErrResourceExhaustedCounter)
	case *serviceerror.NotFound, *serviceerror.NamespaceNotFound:
	default:
		h.logger.Error("Unknown error.", tag.Error(err), tag.Endpoint(endpointName))
		scope.IncCounter(metrics.ServiceFailures)
	}

	return err
}
