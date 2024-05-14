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
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.temporal.io/server/api/adminservice/v1"
	clusterspb "go.temporal.io/server/api/cluster/v1"
	commonspb "go.temporal.io/server/api/common/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	serverClient "go.temporal.io/server/client"
	"go.temporal.io/server/client/admin"
	"go.temporal.io/server/client/frontend"
	"go.temporal.io/server/client/history"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/channel"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/utf8validator"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/worker/addsearchattributes"
	"go.temporal.io/server/service/worker/dlq"
)

const (
	getNamespaceReplicationMessageBatchSize = 100
	defaultLastMessageID                    = -1
	listClustersPageSize                    = 100
)

type (
	// AdminHandler - gRPC handler interface for adminservice
	AdminHandler struct {
		adminservice.UnsafeAdminServiceServer

		status int32

		logger                     log.Logger
		numberOfHistoryShards      int32
		ESClient                   esclient.Client
		config                     *Config
		namespaceDLQHandler        namespace.DLQMessageHandler
		eventSerializer            serialization.Serializer
		visibilityMgr              manager.VisibilityManager
		namespaceReplicationQueue  persistence.NamespaceReplicationQueue
		taskManager                persistence.TaskManager
		clusterMetadataManager     persistence.ClusterMetadataManager
		persistenceMetadataManager persistence.MetadataManager
		clientFactory              serverClient.Factory
		clientBean                 serverClient.Bean
		historyClient              historyservice.HistoryServiceClient
		sdkClientFactory           sdk.ClientFactory
		membershipMonitor          membership.Monitor
		hostInfoProvider           membership.HostInfoProvider
		metricsHandler             metrics.Handler
		namespaceRegistry          namespace.Registry
		saProvider                 searchattribute.Provider
		saManager                  searchattribute.Manager
		clusterMetadata            cluster.Metadata
		healthServer               *health.Server

		// DEPRECATED: only history service on server side is supposed to
		// use the following components.
		persistenceExecutionManager persistence.ExecutionManager
		taskCategoryRegistry        tasks.TaskCategoryRegistry
	}

	NewAdminHandlerArgs struct {
		PersistenceConfig                   *config.Persistence
		Config                              *Config
		NamespaceReplicationQueue           persistence.NamespaceReplicationQueue
		ReplicatorNamespaceReplicationQueue persistence.NamespaceReplicationQueue
		EsClient                            esclient.Client
		visibilityMgr                       manager.VisibilityManager
		Logger                              log.Logger
		TaskManager                         persistence.TaskManager
		ClusterMetadataManager              persistence.ClusterMetadataManager
		PersistenceMetadataManager          persistence.MetadataManager
		ClientFactory                       serverClient.Factory
		ClientBean                          serverClient.Bean
		HistoryClient                       historyservice.HistoryServiceClient
		sdkClientFactory                    sdk.ClientFactory
		MembershipMonitor                   membership.Monitor
		HostInfoProvider                    membership.HostInfoProvider
		MetricsHandler                      metrics.Handler
		NamespaceRegistry                   namespace.Registry
		SaProvider                          searchattribute.Provider
		SaManager                           searchattribute.Manager
		ClusterMetadata                     cluster.Metadata
		HealthServer                        *health.Server
		EventSerializer                     serialization.Serializer
		TimeSource                          clock.TimeSource

		// DEPRECATED: only history service on server side is supposed to
		// use the following components.
		PersistenceExecutionManager persistence.ExecutionManager
		CategoryRegistry            tasks.TaskCategoryRegistry
	}
)

var (
	_ adminservice.AdminServiceServer = (*AdminHandler)(nil)

	resendStartEventID = int64(0)
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
		namespaceDLQHandler: namespace.NewDLQMessageHandler(
			namespaceReplicationTaskExecutor,
			args.NamespaceReplicationQueue,
			args.Logger,
		),
		eventSerializer:             args.EventSerializer,
		visibilityMgr:               args.visibilityMgr,
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
		hostInfoProvider:            args.HostInfoProvider,
		metricsHandler:              args.MetricsHandler,
		namespaceRegistry:           args.NamespaceRegistry,
		saProvider:                  args.SaProvider,
		saManager:                   args.SaManager,
		clusterMetadata:             args.ClusterMetadata,
		healthServer:                args.HealthServer,
		taskCategoryRegistry:        args.CategoryRegistry,
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
func (adh *AdminHandler) AddSearchAttributes(
	ctx context.Context,
	request *adminservice.AddSearchAttributesRequest,
) (_ *adminservice.AddSearchAttributesResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)

	// validate request
	if request == nil {
		return nil, errRequestNotSet
	}

	if len(request.GetSearchAttributes()) == 0 {
		return nil, errSearchAttributesNotSet
	}

	indexName := request.GetIndexName()
	if indexName == "" {
		indexName = adh.visibilityMgr.GetIndexName()
	}

	currentSearchAttributes, err := adh.saProvider.GetSearchAttributes(indexName, true)
	if err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf(errUnableToGetSearchAttributesMessage, err))
	}

	for saName, saType := range request.GetSearchAttributes() {
		if searchattribute.IsReserved(saName) {
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(errSearchAttributeIsReservedMessage, saName))
		}
		if currentSearchAttributes.IsDefined(saName) {
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(errSearchAttributeAlreadyExistsMessage, saName))
		}
		if _, ok := enumspb.IndexedValueType_name[int32(saType)]; !ok {
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(errUnknownSearchAttributeTypeMessage, saType))
		}
	}

	// TODO (rodrigozhou): Remove condition `indexName == ""`.
	// If indexName == "", then calling addSearchAttributesElasticsearch will
	// register the search attributes in the cluster metadata if ES is up or if
	// `skip-schema-update` is set. This is for backward compatibility using
	// standard visibility.
	if adh.visibilityMgr.HasStoreName(elasticsearch.PersistenceName) || indexName == "" {
		err = adh.addSearchAttributesElasticsearch(ctx, request, indexName)
	} else {
		err = adh.addSearchAttributesSQL(ctx, request, currentSearchAttributes)
	}

	if err != nil {
		return nil, err
	}
	return &adminservice.AddSearchAttributesResponse{}, nil
}

func (adh *AdminHandler) addSearchAttributesElasticsearch(
	ctx context.Context,
	request *adminservice.AddSearchAttributesRequest,
	indexName string,
) error {
	// Execute workflow.
	wfParams := addsearchattributes.WorkflowParams{
		CustomAttributesToAdd: request.GetSearchAttributes(),
		IndexName:             indexName,
		SkipSchemaUpdate:      request.GetSkipSchemaUpdate(),
	}

	sdkClient := adh.sdkClientFactory.GetSystemClient()
	run, err := sdkClient.ExecuteWorkflow(
		ctx,
		sdkclient.StartWorkflowOptions{
			TaskQueue: primitives.DefaultWorkerTaskQueue,
			ID:        addsearchattributes.WorkflowName,
		},
		addsearchattributes.WorkflowName,
		wfParams,
	)
	if err != nil {
		return serviceerror.NewUnavailable(
			fmt.Sprintf(errUnableToStartWorkflowMessage, addsearchattributes.WorkflowName, err),
		)
	}

	// Wait for workflow to complete.
	err = run.Get(ctx, nil)
	if err != nil {
		return serviceerror.NewUnavailable(
			fmt.Sprintf(errWorkflowReturnedErrorMessage, addsearchattributes.WorkflowName, err),
		)
	}
	return nil
}

func (adh *AdminHandler) addSearchAttributesSQL(
	ctx context.Context,
	request *adminservice.AddSearchAttributesRequest,
	currentSearchAttributes searchattribute.NameTypeMap,
) error {
	_, client, err := adh.clientFactory.NewLocalFrontendClientWithTimeout(
		frontend.DefaultTimeout,
		frontend.DefaultLongPollTimeout,
	)
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf(errUnableToCreateFrontendClientMessage, err))
	}

	nsName := request.GetNamespace()
	if nsName == "" {
		return errNamespaceNotSet
	}
	resp, err := client.DescribeNamespace(
		ctx,
		&workflowservice.DescribeNamespaceRequest{Namespace: nsName},
	)
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf(errUnableToGetNamespaceInfoMessage, nsName, err))
	}

	dbCustomSearchAttributes := searchattribute.GetSqlDbIndexSearchAttributes().CustomSearchAttributes
	cmCustomSearchAttributes := currentSearchAttributes.Custom()
	upsertFieldToAliasMap := make(map[string]string)
	fieldToAliasMap := resp.Config.CustomSearchAttributeAliases
	aliasToFieldMap := util.InverseMap(fieldToAliasMap)
	for saName, saType := range request.GetSearchAttributes() {
		// check if alias is already in use
		if _, ok := aliasToFieldMap[saName]; ok {
			return serviceerror.NewAlreadyExist(
				fmt.Sprintf(errSearchAttributeAlreadyExistsMessage, saName),
			)
		}
		// find the first available field for the given type
		targetFieldName := ""
		cntUsed := 0
		for fieldName, fieldType := range dbCustomSearchAttributes {
			if fieldType != saType {
				continue
			}
			// make sure the pre-allocated custom search attributes are created in cluster metadata
			if _, ok := cmCustomSearchAttributes[fieldName]; !ok {
				continue
			}
			if _, ok := fieldToAliasMap[fieldName]; ok {
				cntUsed++
			} else if _, ok := upsertFieldToAliasMap[fieldName]; ok {
				cntUsed++
			} else {
				targetFieldName = fieldName
				break
			}
		}
		if targetFieldName == "" {
			return serviceerror.NewInvalidArgument(
				fmt.Sprintf(errTooManySearchAttributesMessage, cntUsed, saType.String()),
			)
		}
		upsertFieldToAliasMap[targetFieldName] = saName
	}

	_, err = client.UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: nsName,
		Config: &namespacepb.NamespaceConfig{
			CustomSearchAttributeAliases: upsertFieldToAliasMap,
		},
	})
	if err != nil && err.Error() == errCustomSearchAttributeFieldAlreadyAllocated.Error() {
		return errRaceConditionAddingSearchAttributes
	}
	return err
}

// RemoveSearchAttributes remove search attribute from the cluster.
func (adh *AdminHandler) RemoveSearchAttributes(
	ctx context.Context,
	request *adminservice.RemoveSearchAttributesRequest,
) (_ *adminservice.RemoveSearchAttributesResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)

	// validate request
	if request == nil {
		return nil, errRequestNotSet
	}

	if len(request.GetSearchAttributes()) == 0 {
		return nil, errSearchAttributesNotSet
	}

	indexName := request.GetIndexName()
	if indexName == "" {
		indexName = adh.visibilityMgr.GetIndexName()
	}

	currentSearchAttributes, err := adh.saProvider.GetSearchAttributes(indexName, true)
	if err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf(errUnableToGetSearchAttributesMessage, err))
	}

	// TODO (rodrigozhou): Remove condition `indexName == ""`.
	// If indexName == "", then calling addSearchAttributesElasticsearch will
	// register the search attributes in the cluster metadata if ES is up or if
	// `skip-schema-update` is set. This is for backward compatibility using
	// standard visibility.
	if adh.visibilityMgr.HasStoreName(elasticsearch.PersistenceName) || indexName == "" {
		err = adh.removeSearchAttributesElasticsearch(ctx, request, indexName, currentSearchAttributes)
	} else {
		err = adh.removeSearchAttributesSQL(ctx, request, currentSearchAttributes)
	}

	if err != nil {
		return nil, err
	}
	return &adminservice.RemoveSearchAttributesResponse{}, nil
}

func (adh *AdminHandler) removeSearchAttributesElasticsearch(
	ctx context.Context,
	request *adminservice.RemoveSearchAttributesRequest,
	indexName string,
	currentSearchAttributes searchattribute.NameTypeMap,
) error {
	newCustomSearchAttributes := maps.Clone(currentSearchAttributes.Custom())
	for _, saName := range request.GetSearchAttributes() {
		if !currentSearchAttributes.IsDefined(saName) {
			return serviceerror.NewInvalidArgument(fmt.Sprintf(errSearchAttributeDoesntExistMessage, saName))
		}
		if _, ok := newCustomSearchAttributes[saName]; !ok {
			return serviceerror.NewInvalidArgument(fmt.Sprintf(errUnableToRemoveNonCustomSearchAttributesMessage, saName))
		}
		delete(newCustomSearchAttributes, saName)
	}

	err := adh.saManager.SaveSearchAttributes(ctx, indexName, newCustomSearchAttributes)
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf(errUnableToSaveSearchAttributesMessage, err))
	}
	return nil
}

func (adh *AdminHandler) removeSearchAttributesSQL(
	ctx context.Context,
	request *adminservice.RemoveSearchAttributesRequest,
	currentSearchAttributes searchattribute.NameTypeMap,
) error {
	_, client, err := adh.clientFactory.NewLocalFrontendClientWithTimeout(
		frontend.DefaultTimeout,
		frontend.DefaultLongPollTimeout,
	)
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf(errUnableToCreateFrontendClientMessage, err))
	}

	nsName := request.GetNamespace()
	if nsName == "" {
		return errNamespaceNotSet
	}
	resp, err := client.DescribeNamespace(
		ctx,
		&workflowservice.DescribeNamespaceRequest{Namespace: nsName},
	)
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf(errUnableToGetNamespaceInfoMessage, nsName, err))
	}

	upsertFieldToAliasMap := make(map[string]string)
	aliasToFieldMap := util.InverseMap(resp.Config.CustomSearchAttributeAliases)
	for _, saName := range request.GetSearchAttributes() {
		if fieldName, ok := aliasToFieldMap[saName]; ok {
			upsertFieldToAliasMap[fieldName] = ""
			continue
		}
		if currentSearchAttributes.IsDefined(saName) {
			return serviceerror.NewInvalidArgument(
				fmt.Sprintf(errUnableToRemoveNonCustomSearchAttributesMessage, saName),
			)
		}
		return serviceerror.NewNotFound(fmt.Sprintf(errSearchAttributeDoesntExistMessage, saName))
	}

	_, err = client.UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: nsName,
		Config: &namespacepb.NamespaceConfig{
			CustomSearchAttributeAliases: upsertFieldToAliasMap,
		},
	})
	return err
}

func (adh *AdminHandler) GetSearchAttributes(
	ctx context.Context,
	request *adminservice.GetSearchAttributesRequest,
) (_ *adminservice.GetSearchAttributesResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	indexName := request.GetIndexName()
	if indexName == "" {
		indexName = adh.visibilityMgr.GetIndexName()
	}

	searchAttributes, err := adh.saProvider.GetSearchAttributes(indexName, true)
	if err != nil {
		adh.logger.Error("getSearchAttributes error", tag.Error(err))
		return nil, serviceerror.NewUnavailable(fmt.Sprintf(errUnableToGetSearchAttributesMessage, err))
	}

	// TODO (rodrigozhou): Remove condition `indexName == ""`.
	// If indexName == "", then calling addSearchAttributesElasticsearch will
	// register the search attributes in the cluster metadata if ES is up or if
	// `skip-schema-update` is set. This is for backward compatibility using
	// standard visibility.
	if adh.visibilityMgr.HasStoreName(elasticsearch.PersistenceName) || indexName == "" {
		return adh.getSearchAttributesElasticsearch(ctx, indexName, searchAttributes)
	}
	return adh.getSearchAttributesSQL(ctx, request, searchAttributes)
}

func (adh *AdminHandler) getSearchAttributesElasticsearch(
	ctx context.Context,
	indexName string,
	searchAttributes searchattribute.NameTypeMap,
) (*adminservice.GetSearchAttributesResponse, error) {
	var lastErr error

	sdkClient := adh.sdkClientFactory.GetSystemClient()
	descResp, err := sdkClient.DescribeWorkflowExecution(ctx, addsearchattributes.WorkflowName, "")
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

	if lastErr != nil {
		return nil, lastErr
	}
	return &adminservice.GetSearchAttributesResponse{
		CustomAttributes:         searchAttributes.Custom(),
		SystemAttributes:         searchAttributes.System(),
		Mapping:                  esMapping,
		AddWorkflowExecutionInfo: wfInfo,
	}, nil
}

func (adh *AdminHandler) getSearchAttributesSQL(
	ctx context.Context,
	request *adminservice.GetSearchAttributesRequest,
	searchAttributes searchattribute.NameTypeMap,
) (*adminservice.GetSearchAttributesResponse, error) {
	_, client, err := adh.clientFactory.NewLocalFrontendClientWithTimeout(
		frontend.DefaultTimeout,
		frontend.DefaultLongPollTimeout,
	)
	if err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf(errUnableToCreateFrontendClientMessage, err))
	}

	nsName := request.GetNamespace()
	if nsName == "" {
		return nil, errNamespaceNotSet
	}
	resp, err := client.DescribeNamespace(
		ctx,
		&workflowservice.DescribeNamespaceRequest{Namespace: nsName},
	)
	if err != nil {
		return nil, serviceerror.NewUnavailable(
			fmt.Sprintf(errUnableToGetNamespaceInfoMessage, nsName, err),
		)
	}

	fieldToAliasMap := resp.Config.CustomSearchAttributeAliases
	customSearchAttributes := make(map[string]enumspb.IndexedValueType)
	for field, tp := range searchAttributes.Custom() {
		if alias, ok := fieldToAliasMap[field]; ok {
			customSearchAttributes[alias] = tp
		}
	}
	return &adminservice.GetSearchAttributesResponse{
		CustomAttributes: customSearchAttributes,
		SystemAttributes: searchAttributes.System(),
	}, nil
}

func (adh *AdminHandler) RebuildMutableState(
	ctx context.Context,
	request *adminservice.RebuildMutableStateRequest,
) (_ *adminservice.RebuildMutableStateResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	if err := validateExecution(request.Execution); err != nil {
		return nil, err
	}

	namespaceID, err := adh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	if _, err := adh.historyClient.RebuildMutableState(ctx, &historyservice.RebuildMutableStateRequest{
		NamespaceId: namespaceID.String(),
		Execution:   request.Execution,
	}); err != nil {
		return nil, err
	}
	return &adminservice.RebuildMutableStateResponse{}, nil
}

func (adh *AdminHandler) ImportWorkflowExecution(
	ctx context.Context,
	request *adminservice.ImportWorkflowExecutionRequest,
) (_ *adminservice.ImportWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	if err := validateExecution(request.Execution); err != nil {
		return nil, err
	}

	namespaceID, err := adh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	resp, err := adh.historyClient.ImportWorkflowExecution(ctx, &historyservice.ImportWorkflowExecutionRequest{
		NamespaceId:    namespaceID.String(),
		Execution:      request.Execution,
		HistoryBatches: request.HistoryBatches,
		VersionHistory: request.VersionHistory,
		Token:          request.Token,
	})
	if err != nil {
		return nil, err
	}
	return &adminservice.ImportWorkflowExecutionResponse{
		Token: resp.Token,
	}, nil
}

// DescribeMutableState returns information about the specified workflow execution.
func (adh *AdminHandler) DescribeMutableState(ctx context.Context, request *adminservice.DescribeMutableStateRequest) (_ *adminservice.DescribeMutableStateResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	if err := validateExecution(request.Execution); err != nil {
		return nil, err
	}

	namespaceID, err := adh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	shardID := common.WorkflowIDToHistoryShard(namespaceID.String(), request.Execution.WorkflowId, adh.numberOfHistoryShards)
	shardIDStr := convert.Int32ToString(shardID)

	resolver, err := adh.membershipMonitor.GetResolver(primitives.HistoryService)
	if err != nil {
		return nil, err
	}
	historyHost, err := resolver.Lookup(shardIDStr)
	if err != nil {
		return nil, err
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

	if request == nil {
		return nil, errRequestNotSet
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
	if request == nil {
		return nil, errRequestNotSet
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

	if request == nil {
		return nil, errRequestNotSet
	}
	_, err := adh.historyClient.CloseShard(ctx, &historyservice.CloseShardRequest{ShardId: request.GetShardId()})
	return &adminservice.CloseShardResponse{}, err
}

func (adh *AdminHandler) ListHistoryTasks(
	ctx context.Context,
	request *adminservice.ListHistoryTasksRequest,
) (_ *adminservice.ListHistoryTasksResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	if request.GetTaskRange() == nil {
		return nil, errTaskRangeNotSet
	}

	if !adh.config.AdminEnableListHistoryTasks() {
		return nil, errListHistoryTasksNotAllowed
	}

	resp, err := adh.historyClient.ListTasks(
		ctx, &historyservice.ListTasksRequest{
			Request: request,
		},
	)
	if err != nil {
		return nil, err
	}
	return resp.Response, nil
}

// DescribeHistoryHost returns information about the internal states of a history host
func (adh *AdminHandler) DescribeHistoryHost(ctx context.Context, request *adminservice.DescribeHistoryHostRequest) (_ *adminservice.DescribeHistoryHostResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
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
			return nil, err
		}

		if err := validateExecution(request.WorkflowExecution); err != nil {
			return nil, err
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
		ShardsNumber:   resp.GetShardsNumber(),
		ShardIds:       resp.GetShardIds(),
		NamespaceCache: resp.GetNamespaceCache(),
		Address:        resp.GetAddress(),
	}, err
}

func (adh *AdminHandler) GetWorkflowExecutionRawHistory(
	ctx context.Context,
	request *adminservice.GetWorkflowExecutionRawHistoryRequest,
) (_ *adminservice.GetWorkflowExecutionRawHistoryResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)
	response, err := adh.historyClient.GetWorkflowExecutionRawHistory(ctx,
		&historyservice.GetWorkflowExecutionRawHistoryRequest{
			NamespaceId: request.NamespaceId,
			Request:     request,
		})
	if err != nil {
		return nil, err
	}
	return response.Response, nil
}

// GetWorkflowExecutionRawHistoryV2 - retrieves the history of workflow execution
func (adh *AdminHandler) GetWorkflowExecutionRawHistoryV2(ctx context.Context, request *adminservice.GetWorkflowExecutionRawHistoryV2Request) (_ *adminservice.GetWorkflowExecutionRawHistoryV2Response, retError error) {
	defer log.CapturePanic(adh.logger, &retError)

	if err := adh.validateGetWorkflowExecutionRawHistoryV2Request(
		request,
	); err != nil {
		return nil, err
	}

	if dynamicconfig.AccessHistory(
		adh.config.AccessHistoryFraction,
		adh.metricsHandler.WithTags(metrics.OperationTag(metrics.AdminGetWorkflowExecutionRawHistoryV2Tag)),
	) {
		response, err := adh.historyClient.GetWorkflowExecutionRawHistoryV2(ctx,
			&historyservice.GetWorkflowExecutionRawHistoryV2Request{
				NamespaceId: request.NamespaceId,
				Request:     request,
			})
		if err != nil {
			return nil, err
		}
		return response.Response, nil
	}
	return adh.getWorkflowExecutionRawHistoryV2(ctx, request)
}

// DescribeCluster return information about a temporal cluster
func (adh *AdminHandler) DescribeCluster(
	ctx context.Context,
	request *adminservice.DescribeClusterRequest,
) (_ *adminservice.DescribeClusterResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)

	membershipInfo := &clusterspb.MembershipInfo{}
	if monitor := adh.membershipMonitor; monitor != nil {
		membershipInfo.CurrentHost = &clusterspb.HostInfo{
			Identity: adh.hostInfoProvider.HostInfo().Identity(),
		}

		members, err := monitor.GetReachableMembers()
		if err != nil {
			return nil, err
		}

		membershipInfo.ReachableMembers = members

		var rings []*clusterspb.RingInfo
		for _, role := range []primitives.ServiceName{
			primitives.FrontendService,
			primitives.InternalFrontendService,
			primitives.HistoryService,
			primitives.MatchingService,
			primitives.WorkerService,
		} {
			resolver, err := monitor.GetResolver(role)
			if err != nil {
				if role == primitives.InternalFrontendService {
					continue // this one is optional
				}
				return nil, err
			}

			var servers []*clusterspb.HostInfo
			for _, server := range resolver.Members() {
				servers = append(servers, &clusterspb.HostInfo{
					Identity: server.Identity(),
				})
			}

			rings = append(rings, &clusterspb.RingInfo{
				Role:        string(role),
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
		return nil, err
	}

	return &adminservice.DescribeClusterResponse{
		SupportedClients:         headers.SupportedClients,
		ServerVersion:            headers.ServerVersion,
		MembershipInfo:           membershipInfo,
		ClusterId:                metadata.GetClusterId(),
		ClusterName:              metadata.GetClusterName(),
		HistoryShardCount:        metadata.GetHistoryShardCount(),
		PersistenceStore:         adh.persistenceExecutionManager.GetName(),
		VisibilityStore:          strings.Join(adh.visibilityMgr.GetStoreNames(), ","),
		VersionInfo:              metadata.GetVersionInfo(),
		FailoverVersionIncrement: metadata.GetFailoverVersionIncrement(),
		InitialFailoverVersion:   metadata.GetInitialFailoverVersion(),
		IsGlobalNamespaceEnabled: metadata.GetIsGlobalNamespaceEnabled(),
		Tags:                     metadata.GetTags(),
	}, nil
}

// ListClusters return information about temporal clusters
// TODO: Remove this API after migrate tctl to use operator handler
func (adh *AdminHandler) ListClusters(
	ctx context.Context,
	request *adminservice.ListClustersRequest,
) (_ *adminservice.ListClustersResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}
	if request.GetPageSize() <= 0 {
		request.PageSize = listClustersPageSize
	}

	resp, err := adh.clusterMetadataManager.ListClusterMetadata(ctx, &persistence.ListClusterMetadataRequest{
		PageSize:      int(request.GetPageSize()),
		NextPageToken: request.GetNextPageToken(),
	})
	if err != nil {
		return nil, err
	}

	var clusterMetadataList []*persistencespb.ClusterMetadata
	for _, clusterResp := range resp.ClusterMetadata {
		clusterMetadataList = append(clusterMetadataList, clusterResp.ClusterMetadata)
	}
	return &adminservice.ListClustersResponse{
		Clusters:      clusterMetadataList,
		NextPageToken: resp.NextPageToken,
	}, nil
}

// ListClusterMembers
// TODO: Remove this API after migrate tctl to use operator handler
func (adh *AdminHandler) ListClusterMembers(
	ctx context.Context,
	request *adminservice.ListClusterMembersRequest,
) (_ *adminservice.ListClusterMembersResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	metadataMgr := adh.clusterMetadataManager

	heartbitRef := request.GetLastHeartbeatWithin()
	var heartbit time.Duration
	if heartbitRef != nil {
		heartbit = heartbitRef.AsDuration()
	}
	startedTimeRef := request.GetSessionStartedAfterTime()
	var startedTime time.Time
	if startedTimeRef != nil {
		startedTime = startedTimeRef.AsTime()
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
		return nil, err
	}

	var activeMembers []*clusterspb.ClusterMember
	for _, member := range resp.ActiveMembers {
		activeMembers = append(activeMembers, &clusterspb.ClusterMember{
			Role:             enumsspb.ClusterMemberRole(member.Role),
			HostId:           member.HostID.String(),
			RpcAddress:       member.RPCAddress.String(),
			RpcPort:          int32(member.RPCPort),
			SessionStartTime: timestamppb.New(member.SessionStart),
			LastHeartbitTime: timestamppb.New(member.LastHeartbeat),
			RecordExpiryTime: timestamppb.New(member.RecordExpiry),
		})
	}

	return &adminservice.ListClusterMembersResponse{
		ActiveMembers: activeMembers,
		NextPageToken: resp.NextPageToken,
	}, nil
}

// AddOrUpdateRemoteCluster
// TODO: Remove this API after migrate tctl to use operator handler
func (adh *AdminHandler) AddOrUpdateRemoteCluster(
	ctx context.Context,
	request *adminservice.AddOrUpdateRemoteClusterRequest,
) (_ *adminservice.AddOrUpdateRemoteClusterResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)

	adminClient := adh.clientFactory.NewRemoteAdminClientWithTimeout(
		request.GetFrontendAddress(),
		admin.DefaultTimeout,
		admin.DefaultLargeTimeout,
	)

	// Fetch cluster metadata from remote cluster
	resp, err := adminClient.DescribeCluster(ctx, &adminservice.DescribeClusterRequest{})
	if err != nil {
		return nil, err
	}

	err = adh.validateRemoteClusterMetadata(resp)
	if err != nil {
		return nil, err
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
		return nil, err
	}

	applied, err := clusterMetadataMrg.SaveClusterMetadata(ctx, &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: &persistencespb.ClusterMetadata{
			ClusterName:              resp.GetClusterName(),
			HistoryShardCount:        resp.GetHistoryShardCount(),
			ClusterId:                resp.GetClusterId(),
			ClusterAddress:           request.GetFrontendAddress(),
			HttpAddress:              request.GetFrontendHttpAddress(),
			FailoverVersionIncrement: resp.GetFailoverVersionIncrement(),
			InitialFailoverVersion:   resp.GetInitialFailoverVersion(),
			IsGlobalNamespaceEnabled: resp.GetIsGlobalNamespaceEnabled(),
			IsConnectionEnabled:      request.GetEnableRemoteClusterConnection(),
			Tags:                     resp.GetTags(),
		},
		Version: updateRequestVersion,
	})
	if err != nil {
		return nil, err
	}
	if !applied {
		return nil, serviceerror.NewInvalidArgument(
			"Cannot update remote cluster due to update immutable fields")
	}
	return &adminservice.AddOrUpdateRemoteClusterResponse{}, nil
}

// RemoveRemoteCluster
// TODO: Remove this API after migrate tctl to use operator handler
func (adh *AdminHandler) RemoveRemoteCluster(
	ctx context.Context,
	request *adminservice.RemoveRemoteClusterRequest,
) (_ *adminservice.RemoveRemoteClusterResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)

	if err := adh.clusterMetadataManager.DeleteClusterMetadata(
		ctx,
		&persistence.DeleteClusterMetadataRequest{ClusterName: request.GetClusterName()},
	); err != nil {
		return nil, err
	}
	return &adminservice.RemoveRemoteClusterResponse{}, nil
}

// GetReplicationMessages returns new replication tasks since the read level provided in the token.
func (adh *AdminHandler) GetReplicationMessages(ctx context.Context, request *adminservice.GetReplicationMessagesRequest) (_ *adminservice.GetReplicationMessagesResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}
	if request.GetClusterName() == "" {
		return nil, errClusterNameNotSet
	}

	resp, err := adh.historyClient.GetReplicationMessages(ctx, &historyservice.GetReplicationMessagesRequest{
		Tokens:      request.GetTokens(),
		ClusterName: request.GetClusterName(),
	})
	if err != nil {
		return nil, err
	}
	return &adminservice.GetReplicationMessagesResponse{ShardMessages: resp.GetShardMessages()}, nil
}

// GetNamespaceReplicationMessages returns new namespace replication tasks since last retrieved task ID.
func (adh *AdminHandler) GetNamespaceReplicationMessages(ctx context.Context, request *adminservice.GetNamespaceReplicationMessagesRequest) (_ *adminservice.GetNamespaceReplicationMessagesResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	if adh.namespaceReplicationQueue == nil {
		return nil, errors.New("namespace replication queue not enabled for cluster")
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
		return nil, err
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

	if request == nil {
		return nil, errRequestNotSet
	}
	if len(request.GetTaskInfos()) == 0 {
		return nil, errEmptyReplicationInfo
	}

	resp, err := adh.historyClient.GetDLQReplicationMessages(ctx, &historyservice.GetDLQReplicationMessagesRequest{TaskInfos: request.GetTaskInfos()})
	if err != nil {
		return nil, err
	}
	return &adminservice.GetDLQReplicationMessagesResponse{ReplicationTasks: resp.GetReplicationTasks()}, nil
}

// ReapplyEvents applies stale events to the current workflow and the current run
func (adh *AdminHandler) ReapplyEvents(ctx context.Context, request *adminservice.ReapplyEventsRequest) (_ *adminservice.ReapplyEventsResponse, retError error) {
	defer log.CapturePanic(adh.logger, &retError)
	if request == nil {
		return nil, errRequestNotSet
	}
	if request.WorkflowExecution == nil {
		return nil, errExecutionNotSet
	}
	if request.GetWorkflowExecution().GetWorkflowId() == "" {
		return nil, errWorkflowIDNotSet
	}
	if request.GetEvents() == nil {
		return nil, errWorkflowIDNotSet
	}
	namespaceEntry, err := adh.namespaceRegistry.GetNamespaceByID(namespace.ID(request.GetNamespaceId()))
	if err != nil {
		return nil, err
	}

	_, err = adh.historyClient.ReapplyEvents(ctx, &historyservice.ReapplyEventsRequest{
		NamespaceId: namespaceEntry.ID().String(),
		Request:     request,
	})
	if err != nil {
		return nil, err
	}
	return &adminservice.ReapplyEventsResponse{}, nil
}

// GetDLQMessages reads messages from DLQ
func (adh *AdminHandler) GetDLQMessages(
	ctx context.Context,
	request *adminservice.GetDLQMessagesRequest,
) (resp *adminservice.GetDLQMessagesResponse, retErr error) {
	defer log.CapturePanic(adh.logger, &retErr)
	if request == nil {
		return nil, errRequestNotSet
	}

	if request.GetMaximumPageSize() <= 0 {
		request.MaximumPageSize = primitives.ReadDLQMessagesPageSize
	}

	if request.GetInclusiveEndMessageId() <= 0 {
		request.InclusiveEndMessageId = common.EndMessageID
	}

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
			Type:                 resp.GetType(),
			ReplicationTasks:     resp.GetReplicationTasks(),
			ReplicationTasksInfo: resp.GetReplicationTasksInfo(),
			NextPageToken:        resp.GetNextPageToken(),
		}, err
	case enumsspb.DEAD_LETTER_QUEUE_TYPE_NAMESPACE:
		tasks, token, err := adh.namespaceDLQHandler.Read(
			ctx,
			request.GetInclusiveEndMessageId(),
			int(request.GetMaximumPageSize()),
			request.GetNextPageToken())
		if err != nil {
			return nil, err
		}

		return &adminservice.GetDLQMessagesResponse{
			ReplicationTasks: tasks,
			NextPageToken:    token,
		}, nil
	default:
		return nil, errDLQTypeIsNotSupported
	}
}

// PurgeDLQMessages purge messages from DLQ
func (adh *AdminHandler) PurgeDLQMessages(
	ctx context.Context,
	request *adminservice.PurgeDLQMessagesRequest,
) (_ *adminservice.PurgeDLQMessagesResponse, err error) {
	defer log.CapturePanic(adh.logger, &err)
	if request == nil {
		return nil, errRequestNotSet
	}

	if request.GetInclusiveEndMessageId() <= 0 {
		request.InclusiveEndMessageId = common.EndMessageID
	}

	switch request.GetType() {
	case enumsspb.DEAD_LETTER_QUEUE_TYPE_REPLICATION:
		resp, err := adh.historyClient.PurgeDLQMessages(ctx, &historyservice.PurgeDLQMessagesRequest{
			Type:                  request.GetType(),
			ShardId:               request.GetShardId(),
			SourceCluster:         request.GetSourceCluster(),
			InclusiveEndMessageId: request.GetInclusiveEndMessageId(),
		})

		if resp == nil {
			return nil, err
		}

		return &adminservice.PurgeDLQMessagesResponse{}, err
	case enumsspb.DEAD_LETTER_QUEUE_TYPE_NAMESPACE:
		err := adh.namespaceDLQHandler.Purge(ctx, request.GetInclusiveEndMessageId())
		if err != nil {
			return nil, err
		}

		return &adminservice.PurgeDLQMessagesResponse{}, err
	default:
		return nil, errDLQTypeIsNotSupported
	}
}

// MergeDLQMessages merges DLQ messages
func (adh *AdminHandler) MergeDLQMessages(
	ctx context.Context,
	request *adminservice.MergeDLQMessagesRequest,
) (resp *adminservice.MergeDLQMessagesResponse, err error) {
	defer log.CapturePanic(adh.logger, &err)
	if request == nil {
		return nil, errRequestNotSet
	}

	if request.GetInclusiveEndMessageId() <= 0 {
		request.InclusiveEndMessageId = common.EndMessageID
	}

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
			return nil, err
		}

		return &adminservice.MergeDLQMessagesResponse{
			NextPageToken: request.GetNextPageToken(),
		}, nil
	case enumsspb.DEAD_LETTER_QUEUE_TYPE_NAMESPACE:
		token, err := adh.namespaceDLQHandler.Merge(
			ctx,
			request.GetInclusiveEndMessageId(),
			int(request.GetMaximumPageSize()),
			request.GetNextPageToken(),
		)
		if err != nil {
			return nil, err
		}

		return &adminservice.MergeDLQMessagesResponse{
			NextPageToken: token,
		}, nil
	default:
		return nil, errDLQTypeIsNotSupported
	}
}

// RefreshWorkflowTasks re-generates the workflow tasks
func (adh *AdminHandler) RefreshWorkflowTasks(
	ctx context.Context,
	request *adminservice.RefreshWorkflowTasksRequest,
) (_ *adminservice.RefreshWorkflowTasksResponse, err error) {
	defer log.CapturePanic(adh.logger, &err)

	if request == nil {
		return nil, errRequestNotSet
	}
	if err := validateExecution(request.Execution); err != nil {
		return nil, err
	}
	namespaceEntry, err := adh.namespaceRegistry.GetNamespaceByID(namespace.ID(request.GetNamespaceId()))
	if err != nil {
		return nil, err
	}

	_, err = adh.historyClient.RefreshWorkflowTasks(ctx, &historyservice.RefreshWorkflowTasksRequest{
		NamespaceId: namespaceEntry.ID().String(),
		Request:     request,
	})
	if err != nil {
		return nil, err
	}
	return &adminservice.RefreshWorkflowTasksResponse{}, nil
}

// ResendReplicationTasks requests replication task from remote cluster
func (adh *AdminHandler) ResendReplicationTasks(
	ctx context.Context,
	request *adminservice.ResendReplicationTasksRequest,
) (_ *adminservice.ResendReplicationTasksResponse, err error) {
	defer log.CapturePanic(adh.logger, &err)

	if request == nil {
		return nil, errRequestNotSet
	}
	resender := xdc.NewNDCHistoryResender(
		adh.namespaceRegistry,
		adh.clientBean,
		func(
			ctx context.Context,
			sourceClusterName string,
			namespaceId namespace.ID,
			workflowId string,
			runId string,
			events []*historypb.HistoryEvent,
			versionHistory []*historyspb.VersionHistoryItem,
		) error {
			historyBlob, err1 := adh.eventSerializer.SerializeEvents(events, enumspb.ENCODING_TYPE_PROTO3)
			if err1 != nil {
				return err1
			}
			replicateRequest := &historyservice.ReplicateEventsV2Request{
				NamespaceId: namespaceId.String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: workflowId,
					RunId:      runId,
				},
				Events:              historyBlob,
				VersionHistoryItems: versionHistory,
			}
			_, err1 = adh.historyClient.ReplicateEventsV2(ctx, replicateRequest)
			return err1
		},
		adh.eventSerializer,
		nil,
		adh.logger,
	)
	if err := resender.SendSingleWorkflowHistory(
		ctx,
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

	if request == nil {
		return nil, errRequestNotSet
	}

	namespaceID, err := adh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
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
		return nil, err
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

	if request == nil {
		return nil, errRequestNotSet
	}

	if err := validateExecution(request.Execution); err != nil {
		return nil, err
	}

	namespaceID, err := adh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	if dynamicconfig.AccessHistory(
		adh.config.AdminDeleteAccessHistoryFraction,
		adh.metricsHandler.WithTags(metrics.OperationTag(metrics.AdminDeleteWorkflowExecutionTag)),
	) {
		response, err := adh.historyClient.ForceDeleteWorkflowExecution(ctx,
			&historyservice.ForceDeleteWorkflowExecutionRequest{
				NamespaceId: namespaceID.String(),
				Request:     request,
			})
		if err != nil {
			return nil, err
		}
		return response.Response, nil
	}
	return adh.deleteWorkflowExecution(ctx, request)
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
		remoteShardCount := metadata.GetHistoryShardCount()
		large := remoteShardCount
		small := adh.config.NumHistoryShards
		if large < small {
			small, large = large, small
		}
		if large%small != 0 {
			return serviceerror.NewInvalidArgument("Remote cluster shard number and local cluster shard number are not multiples.")
		}
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

func (adh *AdminHandler) StreamWorkflowReplicationMessages(
	clientCluster adminservice.AdminService_StreamWorkflowReplicationMessagesServer,
) (retError error) {
	defer log.CapturePanic(adh.logger, &retError)

	ctxMetadata, ok := metadata.FromIncomingContext(clientCluster.Context())
	if !ok {
		return serviceerror.NewInvalidArgument("missing cluster & shard ID metadata")
	}
	_, serverClusterShardID, err := history.DecodeClusterShardMD(ctxMetadata)
	if err != nil {
		return err
	}

	logger := log.With(adh.logger, tag.ShardID(serverClusterShardID.ShardID))
	logger.Info("AdminStreamReplicationMessages started.")
	defer logger.Info("AdminStreamReplicationMessages stopped.")

	ctx := clientCluster.Context()
	serverCluster, err := adh.historyClient.StreamWorkflowReplicationMessages(ctx)
	if err != nil {
		return err
	}

	shutdownChan := channel.NewShutdownOnce()
	go func() {
		defer shutdownChan.Shutdown()

		for !shutdownChan.IsShutdown() {
			req, err := clientCluster.Recv()
			if err != nil {
				logger.Info("AdminStreamReplicationMessages client -> server encountered error", tag.Error(err))
				return
			}
			switch attr := req.GetAttributes().(type) {
			case *adminservice.StreamWorkflowReplicationMessagesRequest_SyncReplicationState:
				if err = serverCluster.Send(&historyservice.StreamWorkflowReplicationMessagesRequest{
					Attributes: &historyservice.StreamWorkflowReplicationMessagesRequest_SyncReplicationState{
						SyncReplicationState: attr.SyncReplicationState,
					},
				}); err != nil {
					logger.Info("AdminStreamReplicationMessages client -> server encountered error", tag.Error(err))
					return
				}
			default:
				logger.Info("AdminStreamReplicationMessages client -> server encountered error", tag.Error(serviceerror.NewInternal(fmt.Sprintf(
					"StreamWorkflowReplicationMessages encountered unknown type: %T %v", attr, attr,
				))))
				return
			}
		}
	}()
	go func() {
		defer shutdownChan.Shutdown()

		for !shutdownChan.IsShutdown() {
			resp, err := serverCluster.Recv()
			if err != nil {
				logger.Info("AdminStreamReplicationMessages server -> client encountered error", tag.Error(err))
				return
			}
			switch attr := resp.GetAttributes().(type) {
			case *historyservice.StreamWorkflowReplicationMessagesResponse_Messages:
				if err = clientCluster.Send(&adminservice.StreamWorkflowReplicationMessagesResponse{
					Attributes: &adminservice.StreamWorkflowReplicationMessagesResponse_Messages{
						Messages: attr.Messages,
					},
				}); err != nil {
					if err != io.EOF {
						logger.Info("AdminStreamReplicationMessages server -> client encountered error", tag.Error(err))

					}
					return
				}
			default:
				logger.Info("AdminStreamReplicationMessages server -> client encountered error", tag.Error(serviceerror.NewInternal(fmt.Sprintf(
					"StreamWorkflowReplicationMessages encountered unknown type: %T %v", attr, attr,
				))))
				return
			}
		}
	}()
	<-shutdownChan.Channel()
	return nil
}

func (adh *AdminHandler) GetNamespace(ctx context.Context, request *adminservice.GetNamespaceRequest) (_ *adminservice.GetNamespaceResponse, err error) {
	defer log.CapturePanic(adh.logger, &err)
	if request == nil || (len(request.GetId()) == 0 && len(request.GetNamespace()) == 0) {
		return nil, errRequestNotSet
	}
	req := &persistence.GetNamespaceRequest{
		Name: request.GetNamespace(),
		ID:   request.GetId(),
	}
	resp, err := adh.persistenceMetadataManager.GetNamespace(ctx, req)
	if err != nil {
		return nil, err
	}
	info := resp.Namespace.GetInfo()
	nsConfig := resp.Namespace.GetConfig()
	replicationConfig := resp.Namespace.GetReplicationConfig()

	nsResponse := &adminservice.GetNamespaceResponse{
		Info: &namespacepb.NamespaceInfo{
			Name:        info.Name,
			State:       info.State,
			Description: info.Description,
			OwnerEmail:  info.Owner,
			Data:        info.Data,
			Id:          info.Id,
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: nsConfig.Retention,
			HistoryArchivalState:          nsConfig.HistoryArchivalState,
			HistoryArchivalUri:            nsConfig.HistoryArchivalUri,
			VisibilityArchivalState:       nsConfig.VisibilityArchivalState,
			VisibilityArchivalUri:         nsConfig.VisibilityArchivalUri,
			BadBinaries:                   nsConfig.BadBinaries,
			CustomSearchAttributeAliases:  nsConfig.CustomSearchAttributeAliases,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: replicationConfig.ActiveClusterName,
			Clusters:          convertClusterReplicationConfigToProto(replicationConfig.Clusters),
			State:             replicationConfig.GetState(),
		},
		ConfigVersion:     resp.Namespace.GetConfigVersion(),
		FailoverVersion:   resp.Namespace.GetFailoverVersion(),
		IsGlobalNamespace: resp.IsGlobalNamespace,
		FailoverHistory:   convertFailoverHistoryToReplicationProto(resp.Namespace.GetReplicationConfig().GetFailoverHistory()),
	}
	return nsResponse, nil
}

func (adh *AdminHandler) GetDLQTasks(
	ctx context.Context,
	request *adminservice.GetDLQTasksRequest,
) (*adminservice.GetDLQTasksResponse, error) {
	response, err := adh.historyClient.GetDLQTasks(ctx, &historyservice.GetDLQTasksRequest{
		DlqKey:        request.DlqKey,
		PageSize:      request.PageSize,
		NextPageToken: request.NextPageToken,
	})
	if err != nil {
		return nil, err
	}
	return &adminservice.GetDLQTasksResponse{
		DlqTasks:      response.DlqTasks,
		NextPageToken: response.NextPageToken,
	}, nil
}

func (adh *AdminHandler) PurgeDLQTasks(
	ctx context.Context,
	request *adminservice.PurgeDLQTasksRequest,
) (*adminservice.PurgeDLQTasksResponse, error) {
	if err := validateHistoryDLQKey(request.DlqKey); err != nil {
		return nil, err
	}

	workflowID := adh.getDLQWorkflowID(request.DlqKey)
	client := adh.sdkClientFactory.GetSystemClient()
	run, err := client.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: primitives.DefaultWorkerTaskQueue,
	}, dlq.WorkflowName, dlq.WorkflowParams{
		WorkflowType: dlq.WorkflowTypeDelete,
		DeleteParams: dlq.DeleteParams{
			Key: dlq.Key{
				TaskCategoryID: int(request.DlqKey.TaskCategory),
				SourceCluster:  request.DlqKey.SourceCluster,
				TargetCluster:  request.DlqKey.TargetCluster,
			},
			MaxMessageID: request.InclusiveMaxTaskMetadata.MessageId,
		},
	})
	if err != nil {
		return nil, err
	}
	runID := run.GetRunID()
	jobToken := adminservice.DLQJobToken{
		WorkflowId: workflowID,
		RunId:      runID,
	}
	if err := utf8validator.Validate(&jobToken, utf8validator.SourceRPCResponse); err != nil {
		return nil, err
	}
	jobTokenBytes, _ := jobToken.Marshal()
	return &adminservice.PurgeDLQTasksResponse{
		JobToken: jobTokenBytes,
	}, nil
}

func (adh *AdminHandler) MergeDLQTasks(ctx context.Context, request *adminservice.MergeDLQTasksRequest) (*adminservice.MergeDLQTasksResponse, error) {
	if err := validateHistoryDLQKey(request.DlqKey); err != nil {
		return nil, err
	}

	workflowID := adh.getDLQWorkflowID(request.DlqKey)
	client := adh.sdkClientFactory.GetSystemClient()
	run, err := client.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: primitives.DefaultWorkerTaskQueue,
	}, dlq.WorkflowName, dlq.WorkflowParams{
		WorkflowType: dlq.WorkflowTypeMerge,
		MergeParams: dlq.MergeParams{
			Key: dlq.Key{
				TaskCategoryID: int(request.DlqKey.TaskCategory),
				SourceCluster:  request.DlqKey.SourceCluster,
				TargetCluster:  request.DlqKey.TargetCluster,
			},
			MaxMessageID: request.InclusiveMaxTaskMetadata.MessageId,
			BatchSize:    int(request.BatchSize), // Let the workflow code validate and set the default value if needed.
		},
	})
	if err != nil {
		return nil, err
	}
	runID := run.GetRunID()
	jobToken := adminservice.DLQJobToken{
		WorkflowId: workflowID,
		RunId:      runID,
	}
	if err := utf8validator.Validate(&jobToken, utf8validator.SourceRPCResponse); err != nil {
		return nil, err
	}
	jobTokenBytes, _ := jobToken.Marshal()
	return &adminservice.MergeDLQTasksResponse{
		JobToken: jobTokenBytes,
	}, nil
}

func (adh *AdminHandler) DescribeDLQJob(ctx context.Context, request *adminservice.DescribeDLQJobRequest) (*adminservice.DescribeDLQJobResponse, error) {
	jt := adminservice.DLQJobToken{}
	err := jt.Unmarshal([]byte(request.JobToken))
	if err == nil {
		err = utf8validator.Validate(&jt, utf8validator.SourceRPCRequest)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errInvalidDLQJobToken, err)
	}
	client := adh.sdkClientFactory.GetSystemClient()
	execution, err := client.DescribeWorkflowExecution(ctx, jt.WorkflowId, jt.RunId)
	if err != nil {
		return nil, err
	}
	response, err := client.QueryWorkflow(ctx, jt.WorkflowId, jt.RunId, dlq.QueryTypeProgress)
	if err != nil {
		return nil, err
	}
	var queryResponse dlq.ProgressQueryResponse
	if err = response.Get(&queryResponse); err != nil {
		return nil, err
	}
	var state enumsspb.DLQOperationState
	switch execution.WorkflowExecutionInfo.Status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING:
		state = enumsspb.DLQ_OPERATION_STATE_RUNNING
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		state = enumsspb.DLQ_OPERATION_STATE_COMPLETED
	default:
		state = enumsspb.DLQ_OPERATION_STATE_FAILED
	}
	var opType enumsspb.DLQOperationType
	switch queryResponse.WorkflowType {
	case dlq.WorkflowTypeDelete:
		opType = enumsspb.DLQ_OPERATION_TYPE_PURGE
	case dlq.WorkflowTypeMerge:
		opType = enumsspb.DLQ_OPERATION_TYPE_MERGE
	default:
		return nil, serviceerror.NewInternal(fmt.Sprintf("Invalid DLQ workflow type: %v", opType))
	}
	return &adminservice.DescribeDLQJobResponse{
		DlqKey: &commonspb.HistoryDLQKey{
			TaskCategory:  int32(queryResponse.DlqKey.TaskCategoryID),
			SourceCluster: queryResponse.DlqKey.SourceCluster,
			TargetCluster: queryResponse.DlqKey.TargetCluster,
		},
		OperationType:          opType,
		OperationState:         state,
		MaxMessageId:           queryResponse.MaxMessageIDToProcess,
		LastProcessedMessageId: queryResponse.LastProcessedMessageID,
		MessagesProcessed:      queryResponse.NumberOfMessagesProcessed,
		StartTime:              execution.WorkflowExecutionInfo.StartTime,
		EndTime:                execution.WorkflowExecutionInfo.CloseTime,
	}, nil
}

func (adh *AdminHandler) CancelDLQJob(ctx context.Context, request *adminservice.CancelDLQJobRequest) (*adminservice.CancelDLQJobResponse, error) {
	jt := adminservice.DLQJobToken{}
	err := jt.Unmarshal([]byte(request.JobToken))
	if err == nil {
		err = utf8validator.Validate(&jt, utf8validator.SourceRPCRequest)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errInvalidDLQJobToken, err)
	}
	client := adh.sdkClientFactory.GetSystemClient()
	execution, err := client.DescribeWorkflowExecution(ctx, jt.WorkflowId, jt.RunId)
	if err != nil {
		return nil, err
	}
	if execution.WorkflowExecutionInfo.Status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return &adminservice.CancelDLQJobResponse{Canceled: false}, nil
	}
	err = client.TerminateWorkflow(ctx, jt.WorkflowId, jt.RunId, request.Reason)
	if err != nil {
		return nil, err
	}
	return &adminservice.CancelDLQJobResponse{Canceled: true}, nil
}

// AddTasks just translates the admin service's request proto into a history service request proto and then sends it.
func (adh *AdminHandler) AddTasks(
	ctx context.Context,
	request *adminservice.AddTasksRequest,
) (*adminservice.AddTasksResponse, error) {
	historyTasks := make([]*historyservice.AddTasksRequest_Task, len(request.Tasks))
	for i, task := range request.Tasks {
		historyTasks[i] = &historyservice.AddTasksRequest_Task{
			CategoryId: task.CategoryId,
			Blob:       task.Blob,
		}
	}
	historyServiceRequest := &historyservice.AddTasksRequest{
		ShardId: request.ShardId,
		Tasks:   historyTasks,
	}
	_, err := adh.historyClient.AddTasks(ctx, historyServiceRequest)
	if err != nil {
		return nil, err
	}
	return &adminservice.AddTasksResponse{}, nil
}

func (adh *AdminHandler) ListQueues(
	ctx context.Context,
	request *adminservice.ListQueuesRequest,
) (*adminservice.ListQueuesResponse, error) {
	historyServiceRequest := &historyservice.ListQueuesRequest{
		QueueType:     request.QueueType,
		PageSize:      request.PageSize,
		NextPageToken: request.NextPageToken,
	}
	resp, err := adh.historyClient.ListQueues(ctx, historyServiceRequest)
	if err != nil {
		return nil, err
	}
	queues := make([]*adminservice.ListQueuesResponse_QueueInfo, len(resp.Queues))
	for i, queue := range resp.Queues {
		queues[i] = &adminservice.ListQueuesResponse_QueueInfo{
			QueueName:    queue.QueueName,
			MessageCount: queue.MessageCount,
		}
	}
	return &adminservice.ListQueuesResponse{
		Queues:        queues,
		NextPageToken: resp.NextPageToken,
	}, nil
}

func (adh *AdminHandler) getDLQWorkflowID(
	key *commonspb.HistoryDLQKey,
) string {
	return fmt.Sprintf(
		"manage-dlq-tasks-%s",
		persistence.GetHistoryTaskQueueName(
			int(key.TaskCategory),
			key.SourceCluster,
			key.TargetCluster,
		),
	)
}

func validateHistoryDLQKey(
	key *commonspb.HistoryDLQKey,
) error {
	if len(key.SourceCluster) == 0 {
		return errSourceClusterNotSet
	}

	if len(key.TargetCluster) == 0 {
		return errTargetClusterNotSet
	}

	// history service is responsible for validating
	// categoryID using task category registry

	return nil
}

func convertClusterReplicationConfigToProto(
	input []string,
) []*replicationpb.ClusterReplicationConfig {
	output := make([]*replicationpb.ClusterReplicationConfig, 0, len(input))
	for _, clusterName := range input {
		output = append(output, &replicationpb.ClusterReplicationConfig{ClusterName: clusterName})
	}
	return output
}

func convertFailoverHistoryToReplicationProto(
	failoverHistoy []*persistencespb.FailoverStatus,
) []*replicationpb.FailoverStatus {
	var replicationProto []*replicationpb.FailoverStatus
	for _, failoverStatus := range failoverHistoy {
		replicationProto = append(replicationProto, &replicationpb.FailoverStatus{
			FailoverTime:    failoverStatus.GetFailoverTime(),
			FailoverVersion: failoverStatus.GetFailoverVersion(),
		})
	}

	return replicationProto
}
