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
	"net"
	"strings"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/clock"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	sdkclient "go.temporal.io/sdk/client"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"go.temporal.io/server/api/adminservice/v1"
	clusterspb "go.temporal.io/server/api/cluster/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	serverClient "go.temporal.io/server/client"
	"go.temporal.io/server/client/admin"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/persistence/visibility/manager"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/persistence/visibility/store/standard/cassandra"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/worker"
	"go.temporal.io/server/service/worker/addsearchattributes"
)

const (
	getNamespaceReplicationMessageBatchSize = 100
	defaultLastMessageID                    = -1
	listClustersPageSize                    = 100
)

type (
	// AdminHandler - gRPC handler interface for adminservice
	AdminHandler struct {
		status int32

		logger                      log.Logger
		numberOfHistoryShards       int32
		ESConfig                    *esclient.Config
		ESClient                    esclient.Client
		config                      *Config
		namespaceHandler            namespace.Handler
		namespaceDLQHandler         namespace.DLQMessageHandler
		eventSerializer             serialization.Serializer
		visibilityMgr               manager.VisibilityManager
		persistenceExecutionManager persistence.ExecutionManager
		namespaceReplicationQueue   persistence.NamespaceReplicationQueue
		taskManager                 persistence.TaskManager
		clusterMetadataManager      persistence.ClusterMetadataManager
		persistenceMetadataManager  persistence.MetadataManager
		clientFactory               serverClient.Factory
		clientBean                  serverClient.Bean
		historyClient               historyservice.HistoryServiceClient
		sdkClientFactory            sdk.ClientFactory
		membershipMonitor           membership.Monitor
		metricsClient               metrics.Client
		namespaceRegistry           namespace.Registry
		saProvider                  searchattribute.Provider
		saManager                   searchattribute.Manager
		clusterMetadata             cluster.Metadata
		healthServer                *health.Server
	}

	NewAdminHandlerArgs struct {
		PersistenceConfig                   *config.Persistence
		Config                              *Config
		NamespaceReplicationQueue           persistence.NamespaceReplicationQueue
		ReplicatorNamespaceReplicationQueue persistence.NamespaceReplicationQueue
		EsConfig                            *esclient.Config
		EsClient                            esclient.Client
		VisibilityMrg                       manager.VisibilityManager
		Logger                              log.Logger
		PersistenceExecutionManager         persistence.ExecutionManager
		TaskManager                         persistence.TaskManager
		ClusterMetadataManager              persistence.ClusterMetadataManager
		PersistenceMetadataManager          persistence.MetadataManager
		ClientFactory                       serverClient.Factory
		ClientBean                          serverClient.Bean
		HistoryClient                       historyservice.HistoryServiceClient
		sdkClientFactory                    sdk.ClientFactory
		MembershipMonitor                   membership.Monitor
		ArchiverProvider                    provider.ArchiverProvider
		MetricsClient                       metrics.Client
		NamespaceRegistry                   namespace.Registry
		SaProvider                          searchattribute.Provider
		SaManager                           searchattribute.Manager
		ClusterMetadata                     cluster.Metadata
		ArchivalMetadata                    archiver.ArchivalMetadata
		HealthServer                        *health.Server
		EventSerializer                     serialization.Serializer
		TimeSource                          clock.TimeSource
	}
)

var (
	_ adminservice.AdminServiceServer = (*AdminHandler)(nil)

	adminServiceRetryPolicy = common.CreateAdminServiceRetryPolicy()
	resendStartEventID      = int64(0)
)

// NewAdminHandler creates a gRPC handler for the adminservice
func NewAdminHandler(
	args NewAdminHandlerArgs,
) *AdminHandler {
	namespaceReplicationTaskExecutor := namespace.NewReplicationTaskExecutor(
		args.ClusterMetadata.GetCurrentClusterName(),
		args.PersistenceMetadataManager,
		args.Logger,
	)

	return &AdminHandler{
		logger:                args.Logger,
		status:                common.DaemonStatusInitialized,
		numberOfHistoryShards: args.PersistenceConfig.NumHistoryShards,
		config:                args.Config,
		namespaceHandler: namespace.NewHandler(
			args.Config.MaxBadBinaries,
			args.Logger,
			args.PersistenceMetadataManager,
			args.ClusterMetadata,
			namespace.NewNamespaceReplicator(args.ReplicatorNamespaceReplicationQueue, args.Logger),
			args.ArchivalMetadata,
			args.ArchiverProvider,
			args.Config.EnableSchedules,
			args.TimeSource,
		),
		namespaceDLQHandler: namespace.NewDLQMessageHandler(
			namespaceReplicationTaskExecutor,
			args.NamespaceReplicationQueue,
			args.Logger,
		),
		eventSerializer:             args.EventSerializer,
		visibilityMgr:               args.VisibilityMrg,
		ESConfig:                    args.EsConfig,
		ESClient:                    args.EsClient,
		persistenceExecutionManager: args.PersistenceExecutionManager,
		namespaceReplicationQueue:   args.NamespaceReplicationQueue,
		taskManager:                 args.TaskManager,
		clusterMetadataManager:      args.ClusterMetadataManager,
		persistenceMetadataManager:  args.PersistenceMetadataManager,
		clientFactory:               args.ClientFactory,
		clientBean:                  args.ClientBean,
		historyClient:               args.HistoryClient,
		sdkClientFactory:            args.sdkClientFactory,
		membershipMonitor:           args.MembershipMonitor,
		metricsClient:               args.MetricsClient,
		namespaceRegistry:           args.NamespaceRegistry,
		saProvider:                  args.SaProvider,
		saManager:                   args.SaManager,
		clusterMetadata:             args.ClusterMetadata,
		healthServer:                args.HealthServer,
	}
}

// Start starts the handler
func (adh *AdminHandler) Start() {
	if atomic.CompareAndSwapInt32(
		&adh.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		adh.healthServer.SetServingStatus(AdminServiceName, healthpb.HealthCheckResponse_SERVING)
	}

	// Start namespace replication queue cleanup
	// If the queue does not start, we can still call stop()
	adh.namespaceReplicationQueue.Start()
}

// Stop stops the handler
func (adh *AdminHandler) Stop() {
	if atomic.CompareAndSwapInt32(
		&adh.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		adh.healthServer.SetServingStatus(AdminServiceName, healthpb.HealthCheckResponse_NOT_SERVING)
	}

	// Calling stop if the queue does not start is ok
	adh.namespaceReplicationQueue.Stop()
}

// AddSearchAttributes add search attribute to the cluster.
func (adh *AdminHandler) AddSearchAttributes(ctx context.Context, request *adminservice.AddSearchAttributesRequest) (_ *adminservice.AddSearchAttributesResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)

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

	currentSearchAttributes, err := adh.saProvider.GetSearchAttributes(indexName, true)
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

	sdkClient := adh.sdkClientFactory.GetSystemClient(adh.logger)
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
	defer log.CapturePanic(adh.logger, &retError)

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

	currentSearchAttributes, err := adh.saProvider.GetSearchAttributes(indexName, true)
	if err != nil {
		return nil, adh.error(serviceerror.NewUnavailable(fmt.Sprintf(errUnableToGetSearchAttributesMessage, err)), scope)
	}

	newCustomSearchAttributes := maps.Clone(currentSearchAttributes.Custom())

	for _, saName := range request.GetSearchAttributes() {
		if !currentSearchAttributes.IsDefined(saName) {
			return nil, adh.error(serviceerror.NewInvalidArgument(fmt.Sprintf(errSearchAttributeDoesntExistMessage, saName)), scope)
		}
		if _, ok := newCustomSearchAttributes[saName]; !ok {
			return nil, adh.error(serviceerror.NewInvalidArgument(fmt.Sprintf(errUnableToRemoveNonCustomSearchAttributesMessage, saName)), scope)
		}
		delete(newCustomSearchAttributes, saName)
	}

	err = adh.saManager.SaveSearchAttributes(ctx, indexName, newCustomSearchAttributes)
	if err != nil {
		return nil, adh.error(serviceerror.NewUnavailable(fmt.Sprintf(errUnableToSaveSearchAttributesMessage, err)), scope)
	}

	return &adminservice.RemoveSearchAttributesResponse{}, nil
}

func (adh *AdminHandler) GetSearchAttributes(ctx context.Context, request *adminservice.GetSearchAttributesRequest) (_ *adminservice.GetSearchAttributesResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)

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

	sdkClient := adh.sdkClientFactory.GetSystemClient(adh.logger)
	descResp, err := sdkClient.DescribeWorkflowExecution(ctx, addsearchattributes.WorkflowName, runID)
	var wfInfo *workflowpb.WorkflowExecutionInfo
	if err != nil {
		// NotFound can happen when no search attributes were added and the workflow has never been executed.
		if _, isNotFound := err.(*serviceerror.NotFound); !isNotFound {
			lastErr = serviceerror.NewUnavailable(fmt.Sprintf("unable to get %s workflow state: %v", addsearchattributes.WorkflowName, err))
			adh.logger.Error("getSearchAttributes error", tag.Error(lastErr))
		}
	} else {
		wfInfo = descResp.GetWorkflowExecutionInfo()
	}

	var esMapping map[string]string
	if adh.ESClient != nil {
		esMapping, err = adh.ESClient.GetMapping(ctx, indexName)
		if err != nil {
			lastErr = serviceerror.NewUnavailable(fmt.Sprintf("unable to get mapping from Elasticsearch: %v", err))
			adh.logger.Error("getSearchAttributes error", tag.Error(lastErr))
		}
	}

	searchAttributes, err := adh.saProvider.GetSearchAttributes(indexName, true)
	if err != nil {
		lastErr = serviceerror.NewUnavailable(fmt.Sprintf("unable to read custom search attributes: %v", err))
		adh.logger.Error("getSearchAttributes error", tag.Error(lastErr))
	}

	return &adminservice.GetSearchAttributesResponse{
		CustomAttributes:         searchAttributes.Custom(),
		SystemAttributes:         searchAttributes.System(),
		Mapping:                  esMapping,
		AddWorkflowExecutionInfo: wfInfo,
	}, lastErr
}

func (adh *AdminHandler) RebuildMutableState(ctx context.Context, request *adminservice.RebuildMutableStateRequest) (_ *adminservice.RebuildMutableStateResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminRebuildMutableStateScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}

	if err := validateExecution(request.Execution); err != nil {
		return nil, adh.error(err, scope)
	}

	namespaceID, err := adh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, adh.error(err, scope)
	}

	if _, err := adh.historyClient.RebuildMutableState(ctx, &historyservice.RebuildMutableStateRequest{
		NamespaceId: namespaceID.String(),
		Execution:   request.Execution,
	}); err != nil {
		return nil, err
	}
	return &adminservice.RebuildMutableStateResponse{}, nil
}

// DescribeMutableState returns information about the specified workflow execution.
func (adh *AdminHandler) DescribeMutableState(ctx context.Context, request *adminservice.DescribeMutableStateRequest) (_ *adminservice.DescribeMutableStateResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminDescribeWorkflowExecutionScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}

	if err := validateExecution(request.Execution); err != nil {
		return nil, adh.error(err, scope)
	}

	namespaceID, err := adh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, adh.error(err, scope)
	}

	shardID := common.WorkflowIDToHistoryShard(namespaceID.String(), request.Execution.WorkflowId, adh.numberOfHistoryShards)
	shardIDStr := convert.Int32ToString(shardID)

	historyHost, err := adh.membershipMonitor.Lookup(common.HistoryServiceName, shardIDStr)
	if err != nil {
		return nil, adh.error(err, scope)
	}

	historyAddr := historyHost.GetAddress()
	historyResponse, err := adh.historyClient.DescribeMutableState(ctx, &historyservice.DescribeMutableStateRequest{
		NamespaceId: namespaceID.String(),
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
	}, nil
}

// RemoveTask returns information about the internal states of a history host
func (adh *AdminHandler) RemoveTask(ctx context.Context, request *adminservice.RemoveTaskRequest) (_ *adminservice.RemoveTaskResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminRemoveTaskScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}
	_, err := adh.historyClient.RemoveTask(ctx, &historyservice.RemoveTaskRequest{
		ShardId:        request.GetShardId(),
		Category:       request.GetCategory(),
		TaskId:         request.GetTaskId(),
		VisibilityTime: request.GetVisibilityTime(),
	})
	return &adminservice.RemoveTaskResponse{}, err
}

// GetShard returns information about the internal states of a shard
func (adh *AdminHandler) GetShard(ctx context.Context, request *adminservice.GetShardRequest) (_ *adminservice.GetShardResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminGetShardScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}
	resp, err := adh.historyClient.GetShard(ctx, &historyservice.GetShardRequest{ShardId: request.GetShardId()})
	if err != nil {
		return nil, err
	}
	return &adminservice.GetShardResponse{ShardInfo: resp.ShardInfo}, nil
}

// CloseShard returns information about the internal states of a history host
func (adh *AdminHandler) CloseShard(ctx context.Context, request *adminservice.CloseShardRequest) (_ *adminservice.CloseShardResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminCloseShardScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}
	_, err := adh.historyClient.CloseShard(ctx, &historyservice.CloseShardRequest{ShardId: request.GetShardId()})
	return &adminservice.CloseShardResponse{}, err
}

func (adh *AdminHandler) ListHistoryTasks(
	ctx context.Context,
	request *adminservice.ListHistoryTasksRequest,
) (_ *adminservice.ListHistoryTasksResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminListHistoryTasksScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}
	taskRange := request.GetTaskRange()
	if taskRange == nil {
		return nil, adh.error(errTaskRangeNotSet, scope)
	}

	taskCategory, ok := tasks.GetCategoryByID(int32(request.Category))
	if !ok {
		return nil, adh.error(&serviceerror.InvalidArgument{
			Message: fmt.Sprintf("unknown task category: %v", request.Category),
		}, scope)
	}

	var minTaskKey, maxTaskKey tasks.Key
	if taskRange.InclusiveMinTaskKey != nil {
		minTaskKey = tasks.NewKey(
			timestamp.TimeValue(taskRange.InclusiveMinTaskKey.FireTime),
			taskRange.InclusiveMinTaskKey.TaskId,
		)
		if err := tasks.ValidateKey(minTaskKey); err != nil {
			return nil, adh.error(&serviceerror.InvalidArgument{
				Message: fmt.Sprintf("invalid minTaskKey: %v", err.Error()),
			}, scope)
		}
	}
	if taskRange.ExclusiveMaxTaskKey != nil {
		maxTaskKey = tasks.NewKey(
			timestamp.TimeValue(taskRange.ExclusiveMaxTaskKey.FireTime),
			taskRange.ExclusiveMaxTaskKey.TaskId,
		)
		if err := tasks.ValidateKey(maxTaskKey); err != nil {
			return nil, adh.error(&serviceerror.InvalidArgument{
				Message: fmt.Sprintf("invalid maxTaskKey: %v", err.Error()),
			}, scope)
		}
	}
	resp, err := adh.persistenceExecutionManager.GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
		ShardID:             request.ShardId,
		TaskCategory:        taskCategory,
		InclusiveMinTaskKey: minTaskKey,
		ExclusiveMaxTaskKey: maxTaskKey,
		BatchSize:           int(request.BatchSize),
		NextPageToken:       request.NextPageToken,
	})
	if err != nil {
		return nil, adh.error(err, scope)
	}

	return &adminservice.ListHistoryTasksResponse{
		Tasks:         toAdminTask(resp.Tasks),
		NextPageToken: resp.NextPageToken,
	}, nil
}

func toAdminTask(tasks []tasks.Task) []*adminservice.Task {
	var adminTasks []*adminservice.Task
	for _, task := range tasks {
		adminTasks = append(adminTasks, &adminservice.Task{
			NamespaceId: task.GetNamespaceID(),
			WorkflowId:  task.GetWorkflowID(),
			RunId:       task.GetRunID(),
			TaskId:      task.GetTaskID(),
			TaskType:    task.GetType(),
			FireTime:    timestamp.TimePtr(task.GetKey().FireTime),
			Version:     task.GetVersion(),
		})
	}
	return adminTasks
}

// DescribeHistoryHost returns information about the internal states of a history host
func (adh *AdminHandler) DescribeHistoryHost(ctx context.Context, request *adminservice.DescribeHistoryHostRequest) (_ *adminservice.DescribeHistoryHostResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)

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
	var namespaceID namespace.ID
	if request.WorkflowExecution != nil {
		namespaceID, err = adh.namespaceRegistry.GetNamespaceID(namespace.Name(request.Namespace))
		if err != nil {
			return nil, adh.error(err, scope)
		}

		if err := validateExecution(request.WorkflowExecution); err != nil {
			return nil, adh.error(err, scope)
		}
	}

	resp, err := adh.historyClient.DescribeHistoryHost(ctx, &historyservice.DescribeHistoryHostRequest{
		HostAddress:       request.GetHostAddress(),
		ShardId:           request.GetShardId(),
		NamespaceId:       namespaceID.String(),
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
	defer log.CapturePanic(adh.logger, &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminGetWorkflowExecutionRawHistoryV2Scope)
	defer sw.Stop()

	if err := adh.validateGetWorkflowExecutionRawHistoryV2Request(
		request,
	); err != nil {
		return nil, adh.error(err, scope)
	}

	ns, err := adh.getNamespaceFromRequest(request)
	if err != nil {
		return nil, adh.error(err, scope)
	}
	scope = scope.Tagged(metrics.NamespaceTag(ns.Name().String()))

	execution := request.Execution
	var pageToken *tokenspb.RawHistoryContinuation
	var targetVersionHistory *historyspb.VersionHistory
	if request.NextPageToken == nil {
		response, err := adh.historyClient.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
			NamespaceId: ns.ID().String(),
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
		ns.ID().String(),
		execution.GetWorkflowId(),
		adh.numberOfHistoryShards,
	)
	rawHistoryResponse, err := adh.persistenceExecutionManager.ReadRawHistoryBranch(ctx, &persistence.ReadHistoryBranchRequest{
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
		if _, isNotFound := err.(*serviceerror.NotFound); isNotFound {
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
	adh.metricsClient.
		Scope(metrics.AdminGetWorkflowExecutionRawHistoryScope).
		RecordDistribution(metrics.HistorySize, size)
	scope.RecordDistribution(metrics.HistorySize, size)

	result := &adminservice.GetWorkflowExecutionRawHistoryV2Response{
		HistoryBatches: rawHistoryResponse.HistoryEventBlobs,
		VersionHistory: targetVersionHistory,
		HistoryNodeIds: rawHistoryResponse.NodeIDs,
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

// DescribeCluster return information about a temporal cluster
func (adh *AdminHandler) DescribeCluster(
	ctx context.Context,
	request *adminservice.DescribeClusterRequest,
) (_ *adminservice.DescribeClusterResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminDescribeClusterScope)
	defer sw.Stop()

	membershipInfo := &clusterspb.MembershipInfo{}
	if monitor := adh.membershipMonitor; monitor != nil {
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

	if len(request.ClusterName) == 0 {
		request.ClusterName = adh.clusterMetadata.GetCurrentClusterName()
	}
	metadata, err := adh.clusterMetadataManager.GetClusterMetadata(
		ctx,
		&persistence.GetClusterMetadataRequest{ClusterName: request.GetClusterName()},
	)
	if err != nil {
		return nil, adh.error(err, scope)
	}

	return &adminservice.DescribeClusterResponse{
		SupportedClients:         headers.SupportedClients,
		ServerVersion:            headers.ServerVersion,
		MembershipInfo:           membershipInfo,
		ClusterId:                metadata.ClusterId,
		ClusterName:              metadata.ClusterName,
		HistoryShardCount:        metadata.HistoryShardCount,
		PersistenceStore:         adh.persistenceExecutionManager.GetName(),
		VisibilityStore:          adh.visibilityMgr.GetName(),
		VersionInfo:              metadata.VersionInfo,
		FailoverVersionIncrement: metadata.FailoverVersionIncrement,
		InitialFailoverVersion:   metadata.InitialFailoverVersion,
		IsGlobalNamespaceEnabled: metadata.IsGlobalNamespaceEnabled,
	}, nil
}

// ListClusters return information about temporal clusters
func (adh *AdminHandler) ListClusters(
	ctx context.Context,
	request *adminservice.ListClustersRequest,
) (_ *adminservice.ListClustersResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminListClustersScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}
	if request.GetPageSize() <= 0 {
		request.PageSize = listClustersPageSize
	}

	resp, err := adh.clusterMetadataManager.ListClusterMetadata(ctx, &persistence.ListClusterMetadataRequest{
		PageSize:      int(request.GetPageSize()),
		NextPageToken: request.GetNextPageToken(),
	})
	if err != nil {
		return nil, adh.error(err, scope)
	}

	var clusterMetadataList []*persistencespb.ClusterMetadata
	for _, clusterResp := range resp.ClusterMetadata {
		clusterMetadataList = append(clusterMetadataList, &clusterResp.ClusterMetadata)
	}
	return &adminservice.ListClustersResponse{
		Clusters:      clusterMetadataList,
		NextPageToken: resp.NextPageToken,
	}, nil
}

func (adh *AdminHandler) ListClusterMembers(
	ctx context.Context,
	request *adminservice.ListClusterMembersRequest,
) (_ *adminservice.ListClusterMembersResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminListClusterMembersScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}

	metadataMgr := adh.clusterMetadataManager

	heartbitRef := request.GetLastHeartbeatWithin()
	var heartbit time.Duration
	if heartbitRef != nil {
		heartbit = *heartbitRef
	}
	startedTimeRef := request.GetSessionStartedAfterTime()
	var startedTime time.Time
	if startedTimeRef != nil {
		startedTime = *startedTimeRef
	}

	resp, err := metadataMgr.GetClusterMembers(ctx, &persistence.GetClusterMembersRequest{
		LastHeartbeatWithin: heartbit,
		RPCAddressEquals:    net.ParseIP(request.GetRpcAddress()),
		HostIDEquals:        uuid.Parse(request.GetHostId()),
		RoleEquals:          persistence.ServiceType(request.GetRole()),
		SessionStartedAfter: startedTime,
		PageSize:            int(request.GetPageSize()),
		NextPageToken:       request.GetNextPageToken(),
	})
	if err != nil {
		return nil, adh.error(err, scope)
	}

	var activeMembers []*clusterspb.ClusterMember
	for _, member := range resp.ActiveMembers {
		activeMembers = append(activeMembers, &clusterspb.ClusterMember{
			Role:             enumsspb.ClusterMemberRole(member.Role),
			HostId:           member.HostID.String(),
			RpcAddress:       member.RPCAddress.String(),
			RpcPort:          int32(member.RPCPort),
			SessionStartTime: &member.SessionStart,
			LastHeartbitTime: &member.LastHeartbeat,
			RecordExpiryTime: &member.RecordExpiry,
		})
	}

	return &adminservice.ListClusterMembersResponse{
		ActiveMembers: activeMembers,
		NextPageToken: resp.NextPageToken,
	}, nil
}

func (adh *AdminHandler) AddOrUpdateRemoteCluster(
	ctx context.Context,
	request *adminservice.AddOrUpdateRemoteClusterRequest,
) (_ *adminservice.AddOrUpdateRemoteClusterResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminAddOrUpdateRemoteClusterScope)
	defer sw.Stop()

	adminClient := adh.clientFactory.NewAdminClientWithTimeout(
		request.GetFrontendAddress(),
		admin.DefaultTimeout,
		admin.DefaultLargeTimeout,
	)

	// Fetch cluster metadata from remote cluster
	resp, err := adminClient.DescribeCluster(ctx, &adminservice.DescribeClusterRequest{})
	if err != nil {
		return nil, adh.error(err, scope)
	}

	err = adh.validateRemoteClusterMetadata(resp)
	if err != nil {
		return nil, adh.error(err, scope)
	}

	var updateRequestVersion int64 = 0
	clusterMetadataMrg := adh.clusterMetadataManager
	clusterData, err := clusterMetadataMrg.GetClusterMetadata(
		ctx,
		&persistence.GetClusterMetadataRequest{ClusterName: resp.GetClusterName()},
	)
	switch err.(type) {
	case nil:
		updateRequestVersion = clusterData.Version
	case *serviceerror.NotFound:
		updateRequestVersion = 0
	default:
		return nil, adh.error(err, scope)
	}

	applied, err := clusterMetadataMrg.SaveClusterMetadata(ctx, &persistence.SaveClusterMetadataRequest{
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
		return nil, adh.error(err, scope)
	}
	if !applied {
		return nil, adh.error(serviceerror.NewInvalidArgument(
			"Cannot update remote cluster due to update immutable fields"),
			scope,
		)
	}
	return &adminservice.AddOrUpdateRemoteClusterResponse{}, nil
}

func (adh *AdminHandler) RemoveRemoteCluster(
	ctx context.Context,
	request *adminservice.RemoveRemoteClusterRequest,
) (_ *adminservice.RemoveRemoteClusterResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminRemoveRemoteClusterScope)
	defer sw.Stop()

	if err := adh.clusterMetadataManager.DeleteClusterMetadata(
		ctx,
		&persistence.DeleteClusterMetadataRequest{ClusterName: request.GetClusterName()},
	); err != nil {
		return nil, adh.error(err, scope)
	}
	return &adminservice.RemoveRemoteClusterResponse{}, nil
}

// GetReplicationMessages returns new replication tasks since the read level provided in the token.
func (adh *AdminHandler) GetReplicationMessages(ctx context.Context, request *adminservice.GetReplicationMessagesRequest) (_ *adminservice.GetReplicationMessagesResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminGetReplicationMessagesScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}
	if request.GetClusterName() == "" {
		return nil, adh.error(errClusterNameNotSet, scope)
	}

	resp, err := adh.historyClient.GetReplicationMessages(ctx, &historyservice.GetReplicationMessagesRequest{
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
	defer log.CapturePanic(adh.logger, &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminGetNamespaceReplicationMessagesScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}

	if adh.namespaceReplicationQueue == nil {
		return nil, adh.error(errors.New("namespace replication queue not enabled for cluster"), scope)
	}

	lastMessageID := request.GetLastRetrievedMessageId()
	if request.GetLastRetrievedMessageId() == defaultLastMessageID {
		if clusterAckLevels, err := adh.namespaceReplicationQueue.GetAckLevels(ctx); err == nil {
			if ackLevel, ok := clusterAckLevels[request.GetClusterName()]; ok {
				lastMessageID = ackLevel
			}
		}
	}

	replicationTasks, lastMessageID, err := adh.namespaceReplicationQueue.GetReplicationMessages(
		ctx,
		lastMessageID,
		getNamespaceReplicationMessageBatchSize,
	)
	if err != nil {
		return nil, adh.error(err, scope)
	}

	if request.GetLastProcessedMessageId() != defaultLastMessageID {
		if err := adh.namespaceReplicationQueue.UpdateAckLevel(
			ctx,
			request.GetLastProcessedMessageId(),
			request.GetClusterName(),
		); err != nil {
			adh.logger.Warn("Failed to update namespace replication queue ack level",
				tag.TaskID(request.GetLastProcessedMessageId()),
				tag.ClusterName(request.GetClusterName()))
		}
	}

	return &adminservice.GetNamespaceReplicationMessagesResponse{
		Messages: &replicationspb.ReplicationMessages{
			ReplicationTasks:       replicationTasks,
			LastRetrievedMessageId: lastMessageID,
		},
	}, nil
}

// GetDLQReplicationMessages returns new replication tasks based on the dlq info.
func (adh *AdminHandler) GetDLQReplicationMessages(ctx context.Context, request *adminservice.GetDLQReplicationMessagesRequest) (_ *adminservice.GetDLQReplicationMessagesResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)

	scope, sw := adh.startRequestProfile(metrics.AdminGetDLQReplicationMessagesScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}
	if len(request.GetTaskInfos()) == 0 {
		return nil, adh.error(errEmptyReplicationInfo, scope)
	}

	resp, err := adh.historyClient.GetDLQReplicationMessages(ctx, &historyservice.GetDLQReplicationMessagesRequest{TaskInfos: request.GetTaskInfos()})
	if err != nil {
		return nil, adh.error(err, scope)
	}
	return &adminservice.GetDLQReplicationMessagesResponse{ReplicationTasks: resp.GetReplicationTasks()}, nil
}

// ReapplyEvents applies stale events to the current workflow and the current run
func (adh *AdminHandler) ReapplyEvents(ctx context.Context, request *adminservice.ReapplyEventsRequest) (_ *adminservice.ReapplyEventsResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)
	scope, sw := adh.startRequestProfile(metrics.AdminReapplyEventsScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
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
	namespaceEntry, err := adh.getNamespaceFromRequest(request)
	if err != nil {
		return nil, adh.error(err, scope)
	}

	_, err = adh.historyClient.ReapplyEvents(ctx, &historyservice.ReapplyEventsRequest{
		NamespaceId: namespaceEntry.ID().String(),
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

	defer log.CapturePanic(adh.logger, &retErr)
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
	var op func(ctx context.Context) error
	switch request.GetType() {
	case enumsspb.DEAD_LETTER_QUEUE_TYPE_REPLICATION:
		resp, err := adh.historyClient.GetDLQMessages(ctx, &historyservice.GetDLQMessagesRequest{
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
		op = func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				var err error
				tasks, token, err = adh.namespaceDLQHandler.Read(
					ctx,
					request.GetInclusiveEndMessageId(),
					int(request.GetMaximumPageSize()),
					request.GetNextPageToken())
				return err
			}
		}
	default:
		return nil, adh.error(errDLQTypeIsNotSupported, scope)
	}
	retErr = backoff.ThrottleRetryContext(ctx, op, adminServiceRetryPolicy, common.IsServiceTransientError)
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

	defer log.CapturePanic(adh.logger, &err)
	scope, sw := adh.startRequestProfile(metrics.AdminPurgeDLQMessagesScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}

	if request.GetInclusiveEndMessageId() <= 0 {
		request.InclusiveEndMessageId = common.EndMessageID
	}

	var op func(ctx context.Context) error
	switch request.GetType() {
	case enumsspb.DEAD_LETTER_QUEUE_TYPE_REPLICATION:
		resp, err := adh.historyClient.PurgeDLQMessages(ctx, &historyservice.PurgeDLQMessagesRequest{
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
		op = func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				return adh.namespaceDLQHandler.Purge(ctx, request.GetInclusiveEndMessageId())
			}
		}
	default:
		return nil, adh.error(errDLQTypeIsNotSupported, scope)
	}
	err = backoff.ThrottleRetryContext(ctx, op, adminServiceRetryPolicy, common.IsServiceTransientError)
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

	defer log.CapturePanic(adh.logger, &err)
	scope, sw := adh.startRequestProfile(metrics.AdminMergeDLQMessagesScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}

	if request.GetInclusiveEndMessageId() <= 0 {
		request.InclusiveEndMessageId = common.EndMessageID
	}

	var token []byte
	var op func(ctx context.Context) error
	switch request.GetType() {
	case enumsspb.DEAD_LETTER_QUEUE_TYPE_REPLICATION:
		resp, err := adh.historyClient.MergeDLQMessages(ctx, &historyservice.MergeDLQMessagesRequest{
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
		op = func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				var err error
				token, err = adh.namespaceDLQHandler.Merge(
					ctx,
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
	err = backoff.ThrottleRetryContext(ctx, op, adminServiceRetryPolicy, common.IsServiceTransientError)
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
	defer log.CapturePanic(adh.logger, &err)
	scope, sw := adh.startRequestProfile(metrics.AdminRefreshWorkflowTasksScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}
	if err := validateExecution(request.Execution); err != nil {
		return nil, adh.error(err, scope)
	}
	namespaceEntry, err := adh.getNamespaceFromRequest(request)
	if err != nil {
		return nil, adh.error(err, scope)
	}

	_, err = adh.historyClient.RefreshWorkflowTasks(ctx, &historyservice.RefreshWorkflowTasksRequest{
		NamespaceId: namespaceEntry.ID().String(),
		Request:     request,
	})
	if err != nil {
		return nil, adh.error(err, scope)
	}
	return &adminservice.RefreshWorkflowTasksResponse{}, nil
}

// ResendReplicationTasks requests replication task from remote cluster
func (adh *AdminHandler) ResendReplicationTasks(
	_ context.Context,
	request *adminservice.ResendReplicationTasksRequest,
) (_ *adminservice.ResendReplicationTasksResponse, err error) {
	defer log.CapturePanic(adh.logger, &err)
	scope, sw := adh.startRequestProfile(metrics.AdminResendReplicationTasksScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}
	resender := xdc.NewNDCHistoryResender(
		adh.namespaceRegistry,
		adh.clientBean,
		func(ctx context.Context, request *historyservice.ReplicateEventsV2Request) error {
			_, err1 := adh.historyClient.ReplicateEventsV2(ctx, request)
			return err1
		},
		adh.eventSerializer,
		nil,
		adh.logger,
	)
	if err := resender.SendSingleWorkflowHistory(
		request.GetRemoteCluster(),
		namespace.ID(request.GetNamespaceId()),
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

// GetTaskQueueTasks returns tasks from task queue
func (adh *AdminHandler) GetTaskQueueTasks(
	ctx context.Context,
	request *adminservice.GetTaskQueueTasksRequest,
) (_ *adminservice.GetTaskQueueTasksResponse, err error) {
	defer log.CapturePanic(adh.logger, &err)
	scope, sw := adh.startRequestProfile(metrics.AdminGetTaskQueueTasksScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}

	namespaceID, err := adh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, adh.error(err, scope)
	}

	resp, err := adh.taskManager.GetTasks(ctx, &persistence.GetTasksRequest{
		NamespaceID:        namespaceID.String(),
		TaskQueue:          request.GetTaskQueue(),
		TaskType:           request.GetTaskQueueType(),
		InclusiveMinTaskID: request.GetMinTaskId(),
		ExclusiveMaxTaskID: request.GetMaxTaskId(),
		PageSize:           int(request.GetBatchSize()),
		NextPageToken:      request.NextPageToken,
	})
	if err != nil {
		return nil, adh.error(err, scope)
	}

	return &adminservice.GetTaskQueueTasksResponse{
		Tasks:         resp.Tasks,
		NextPageToken: resp.NextPageToken,
	}, nil
}

func (adh *AdminHandler) DeleteWorkflowExecution(
	ctx context.Context,
	request *adminservice.DeleteWorkflowExecutionRequest,
) (_ *adminservice.DeleteWorkflowExecutionResponse, err error) {
	defer log.CapturePanic(adh.logger, &err)
	scope, sw := adh.startRequestProfile(metrics.AdminDeleteWorkflowExecutionScope)
	defer sw.Stop()

	if request == nil {
		return nil, adh.error(errRequestNotSet, scope)
	}

	if err := validateExecution(request.Execution); err != nil {
		return nil, adh.error(err, scope)
	}

	namespaceID, err := adh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, adh.error(err, scope)
	}
	execution := request.Execution

	shardID := common.WorkflowIDToHistoryShard(
		namespaceID.String(),
		execution.GetWorkflowId(),
		adh.numberOfHistoryShards,
	)
	logger := log.With(adh.logger,
		tag.WorkflowNamespace(request.Namespace),
		tag.WorkflowID(execution.WorkflowId),
		tag.WorkflowRunID(execution.RunId),
	)

	if execution.RunId == "" {
		resp, err := adh.persistenceExecutionManager.GetCurrentExecution(ctx, &persistence.GetCurrentExecutionRequest{
			ShardID:     shardID,
			NamespaceID: namespaceID.String(),
			WorkflowID:  execution.WorkflowId,
		})
		if err != nil {
			return nil, adh.error(err, scope)
		}
		execution.RunId = resp.RunID
	}

	var warnings []string
	var branchTokens [][]byte
	var startTime, closeTime *time.Time
	cassVisBackend := strings.Contains(adh.visibilityMgr.GetName(), cassandra.CassandraPersistenceName)

	resp, err := adh.persistenceExecutionManager.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: namespaceID.String(),
		WorkflowID:  execution.WorkflowId,
		RunID:       execution.RunId,
	})
	if err != nil {
		if common.IsContextCanceledErr(err) || common.IsContextDeadlineExceededErr(err) {
			return nil, adh.error(err, scope)
		}
		// continue to deletion
		warnMsg := "Unable to load mutable state when deleting workflow execution, " +
			"will skip deleting workflow history and cassandra visibility record"
		logger.Warn(warnMsg, tag.Error(err))
		warnings = append(warnings, fmt.Sprintf("%s. Error: %v", warnMsg, err.Error()))
	} else {
		// load necessary information from mutable state
		executionInfo := resp.State.GetExecutionInfo()
		histories := executionInfo.GetVersionHistories().GetHistories()
		branchTokens = make([][]byte, 0, len(histories))
		for _, historyItem := range histories {
			branchTokens = append(branchTokens, historyItem.GetBranchToken())
		}

		if cassVisBackend {
			if resp.State.ExecutionState.State != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
				startTime = executionInfo.GetStartTime()
			} else if executionInfo.GetCloseTime() != nil {
				closeTime = executionInfo.GetCloseTime()
			} else {
				completionEvent, err := adh.getWorkflowCompletionEvent(ctx, shardID, resp.State)
				if err != nil {
					warnMsg := "Unable to load workflow completion event, will skip deleting visibility record"
					adh.logger.Warn(warnMsg, tag.Error(err))
					warnings = append(warnings, fmt.Sprintf("%s. Error: %v", warnMsg, err.Error()))
				} else {
					closeTime = completionEvent.GetEventTime()
				}
			}
		}
	}

	if !cassVisBackend || (startTime != nil || closeTime != nil) {
		// if using cass visibility, then either start or close time should be non-nil
		// NOTE: the deletion is best effort, for sql and cassandra visibility implementation,
		// we can't guarantee there's no update or record close request for this workflow since
		// visibility queue processing is async. Operator can call this api again to delete visibility
		// record again if this happens.
		if _, err := adh.historyClient.DeleteWorkflowVisibilityRecord(ctx, &historyservice.DeleteWorkflowVisibilityRecordRequest{
			NamespaceId:       namespaceID.String(),
			Execution:         execution,
			WorkflowStartTime: startTime,
			WorkflowCloseTime: closeTime,
		}); err != nil {
			return nil, adh.error(err, scope)
		}
	}

	if err := adh.persistenceExecutionManager.DeleteCurrentWorkflowExecution(ctx, &persistence.DeleteCurrentWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: namespaceID.String(),
		WorkflowID:  execution.WorkflowId,
		RunID:       execution.RunId,
	}); err != nil {
		return nil, adh.error(err, scope)
	}

	if err := adh.persistenceExecutionManager.DeleteWorkflowExecution(ctx, &persistence.DeleteWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: namespaceID.String(),
		WorkflowID:  execution.WorkflowId,
		RunID:       execution.RunId,
	}); err != nil {
		return nil, adh.error(err, scope)
	}

	for _, branchToken := range branchTokens {
		if err := adh.persistenceExecutionManager.DeleteHistoryBranch(ctx, &persistence.DeleteHistoryBranchRequest{
			ShardID:     shardID,
			BranchToken: branchToken,
		}); err != nil {
			warnMsg := "Failed to delete history branch, skip"
			adh.logger.Warn(warnMsg, tag.WorkflowBranchID(string(branchToken)), tag.Error(err))
			warnings = append(warnings, fmt.Sprintf("%s. BranchToken: %v, Error: %v", warnMsg, branchToken, err.Error()))
		}
	}

	return &adminservice.DeleteWorkflowExecutionResponse{
		Warnings: warnings,
	}, nil
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

	return nil
}

func (adh *AdminHandler) validateRemoteClusterMetadata(metadata *adminservice.DescribeClusterResponse) error {
	// Verify remote cluster config
	currentClusterInfo := adh.clusterMetadata
	if metadata.GetClusterName() == currentClusterInfo.GetCurrentClusterName() {
		// cluster name conflict
		return serviceerror.NewInvalidArgument("Cannot update current cluster metadata from rpc calls")
	}
	if metadata.GetFailoverVersionIncrement() != currentClusterInfo.GetFailoverVersionIncrement() {
		// failover version increment is mismatch with current cluster config
		return serviceerror.NewInvalidArgument("Cannot add remote cluster due to failover version increment mismatch")
	}
	if metadata.GetHistoryShardCount() != adh.config.NumHistoryShards {
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
	metricsScope := adh.metricsClient.Scope(scope)
	sw := metricsScope.StartTimer(metrics.ServiceLatency)
	metricsScope.IncCounter(metrics.ServiceRequests)
	return metricsScope, sw
}

func (adh *AdminHandler) error(err error, scope metrics.Scope) error {
	scope.Tagged(metrics.ServiceErrorTypeTag(err)).IncCounter(metrics.ServiceErrorWithType)

	switch err := err.(type) {
	case *serviceerror.Unavailable:
		adh.logger.Error("unavailable error", tag.Error(err))
		scope.IncCounter(metrics.ServiceFailures)
		return err
	case *serviceerror.InvalidArgument:
		scope.IncCounter(metrics.ServiceErrInvalidArgumentCounter)
		return err
	case *serviceerror.ResourceExhausted:
		scope.Tagged(metrics.ResourceExhaustedCauseTag(err.Cause)).IncCounter(metrics.ServiceErrResourceExhaustedCounter)
		return err
	case *serviceerror.NotFound, *serviceerror.NamespaceNotFound:
		return err
	}

	adh.logger.Error("Unknown error", tag.Error(err))
	scope.IncCounter(metrics.ServiceFailures)

	return err
}

// TODO (alex): remove this func in 1.18+ together with `namespace` field in corresponding protos and namespace_validator.go.
func (adh *AdminHandler) getNamespaceFromRequest(request interface {
	GetNamespace() string
	GetNamespaceId() string
}) (*namespace.Namespace, error) {

	if request.GetNamespaceId() != "" {
		ns, err := adh.namespaceRegistry.GetNamespaceByID(namespace.ID(request.GetNamespaceId()))
		if err != nil {
			return nil, err
		}
		return ns, nil
	}

	ns, err := adh.namespaceRegistry.GetNamespace(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}
	return ns, nil
}

func (adh *AdminHandler) getWorkflowCompletionEvent(
	ctx context.Context,
	shardID int32,
	mutableState *persistencespb.WorkflowMutableState,
) (*historypb.HistoryEvent, error) {
	executionInfo := mutableState.GetExecutionInfo()
	completionEventID := mutableState.GetNextEventId() - 1

	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(executionInfo.VersionHistories)
	if err != nil {
		return nil, err
	}
	version, err := versionhistory.GetVersionHistoryEventVersion(currentVersionHistory, completionEventID)
	if err != nil {
		return nil, err
	}

	resp, err := adh.persistenceExecutionManager.ReadHistoryBranch(ctx, &persistence.ReadHistoryBranchRequest{
		ShardID:     shardID,
		BranchToken: currentVersionHistory.GetBranchToken(),
		MinEventID:  executionInfo.CompletionEventBatchId,
		MaxEventID:  completionEventID + 1,
		PageSize:    1,
	})
	if err != nil {
		return nil, err
	}

	// find history event from batch and return back single event to caller
	for _, e := range resp.HistoryEvents {
		if e.EventId == completionEventID && e.Version == version {
			return e, nil
		}
	}

	return nil, serviceerror.NewInternal("Unable to find closed event for workflow")
}
