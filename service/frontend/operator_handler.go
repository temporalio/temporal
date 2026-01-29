package frontend

import (
	"context"
	"fmt"
	"maps"
	"sync/atomic"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	svc "go.temporal.io/server/client"
	"go.temporal.io/server/client/admin"
	"go.temporal.io/server/client/frontend"
	"go.temporal.io/server/common"
	clustermetadata "go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/worker/deletenamespace"
	"go.temporal.io/server/service/worker/deletenamespace/deleteexecutions"
	delnserrors "go.temporal.io/server/service/worker/deletenamespace/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

var _ OperatorHandler = (*OperatorHandlerImpl)(nil)

type (
	// OperatorHandlerImpl - gRPC handler interface for operator service
	OperatorHandlerImpl struct {
		operatorservice.UnimplementedOperatorServiceServer

		status int32

		logger                 log.Logger
		config                 *Config
		sdkClientFactory       sdk.ClientFactory
		metricsHandler         metrics.Handler
		visibilityMgr          manager.VisibilityManager
		saManager              searchattribute.Manager
		healthServer           *health.Server
		historyClient          resource.HistoryClient
		clusterMetadataManager persistence.ClusterMetadataManager
		clusterMetadata        clustermetadata.Metadata
		clientFactory          svc.Factory
		namespaceRegistry      namespace.Registry
		nexusEndpointClient    *NexusEndpointClient
	}

	NewOperatorHandlerImplArgs struct {
		config                 *Config
		Logger                 log.Logger
		sdkClientFactory       sdk.ClientFactory
		MetricsHandler         metrics.Handler
		VisibilityMgr          manager.VisibilityManager
		SaManager              searchattribute.Manager
		healthServer           *health.Server
		historyClient          resource.HistoryClient
		clusterMetadataManager persistence.ClusterMetadataManager
		clusterMetadata        clustermetadata.Metadata
		clientFactory          svc.Factory
		namespaceRegistry      namespace.Registry
		nexusEndpointClient    *NexusEndpointClient
	}
)

const (
	namespaceTagName                 = "namespace"
	visibilityIndexNameTagName       = "visibility-index-name"
	visibilitySearchAttributeTagName = "visibility-search-attribute"
)

// NewOperatorHandlerImpl creates a gRPC handler for operatorservice
func NewOperatorHandlerImpl(
	args NewOperatorHandlerImplArgs,
) *OperatorHandlerImpl {

	handler := &OperatorHandlerImpl{
		logger:                 args.Logger,
		status:                 common.DaemonStatusInitialized,
		config:                 args.config,
		sdkClientFactory:       args.sdkClientFactory,
		metricsHandler:         args.MetricsHandler,
		visibilityMgr:          args.VisibilityMgr,
		saManager:              args.SaManager,
		healthServer:           args.healthServer,
		historyClient:          args.historyClient,
		clusterMetadataManager: args.clusterMetadataManager,
		clusterMetadata:        args.clusterMetadata,
		clientFactory:          args.clientFactory,
		namespaceRegistry:      args.namespaceRegistry,
		nexusEndpointClient:    args.nexusEndpointClient,
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

func (h *OperatorHandlerImpl) AddSearchAttributes(
	ctx context.Context,
	request *operatorservice.AddSearchAttributesRequest,
) (_ *operatorservice.AddSearchAttributesResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)

	// validate request
	if request == nil {
		return nil, errRequestNotSet
	}

	if len(request.GetSearchAttributes()) == 0 {
		return nil, errSearchAttributesNotSet
	}

	for saName, saType := range request.GetSearchAttributes() {
		if sadefs.IsReserved(saName) {
			return nil, serviceerror.NewInvalidArgumentf(errSearchAttributeIsReservedMessage, saName)
		}
		if _, ok := enumspb.IndexedValueType_name[int32(saType)]; !ok {
			return nil, serviceerror.NewInvalidArgumentf(errUnknownSearchAttributeTypeMessage, saType)
		}
	}

	var visManagers []manager.VisibilityManager
	if visManagerDual, ok := h.visibilityMgr.(*visibility.VisibilityManagerDual); ok {
		visManagers = append(
			visManagers,
			visManagerDual.GetPrimaryVisibility(),
			visManagerDual.GetSecondaryVisibility(),
		)
	} else {
		visManagers = append(visManagers, h.visibilityMgr)
	}

	for _, visManager := range visManagers {
		if err := h.addSearchAttributesInternal(ctx, request, visManager); err != nil {
			return nil, err
		}
	}

	return &operatorservice.AddSearchAttributesResponse{}, nil
}

func (h *OperatorHandlerImpl) addSearchAttributesInternal(
	ctx context.Context,
	request *operatorservice.AddSearchAttributesRequest,
	visManager manager.VisibilityManager,
) error {
	storeName := visManager.GetStoreNames()[0]
	if storeName == elasticsearch.PersistenceName {
		return h.addSearchAttributesElasticsearch(ctx, request, visManager)
	}
	return h.addSearchAttributesSQL(ctx, request, visManager)
}

func (h *OperatorHandlerImpl) addSearchAttributesElasticsearch(
	ctx context.Context,
	request *operatorservice.AddSearchAttributesRequest,
	visManager manager.VisibilityManager,
) error {
	indexName := visManager.GetIndexName()
	currentSearchAttributes, err := h.saManager.GetSearchAttributes(indexName, true)
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf(errUnableToGetSearchAttributesMessage, err))
	}

	// Check if custom search attribute already exists in cluster metadata.
	// This check is not needed in SQL DB because all custom search attributes
	// are pre-allocated, and only aliases are created.
	customAttributesToAdd := map[string]enumspb.IndexedValueType{}
	for saName, saType := range request.GetSearchAttributes() {
		if !currentSearchAttributes.IsDefined(saName) {
			customAttributesToAdd[saName] = saType
		} else {
			h.logger.Warn(
				fmt.Sprintf(errSearchAttributeAlreadyExistsMessage, saName),
				tag.NewStringTag(visibilityIndexNameTagName, indexName),
				tag.NewStringTag(visibilitySearchAttributeTagName, saName),
			)
		}
	}

	// If the map is empty, then all custom search attributes already exists.
	if len(customAttributesToAdd) == 0 {
		return nil
	}

	if err := visManager.AddSearchAttributes(
		ctx,
		&manager.AddSearchAttributesRequest{SearchAttributes: customAttributesToAdd},
	); err != nil {
		return serviceerror.NewUnavailablef(errUnableToSaveSearchAttributesMessage, err)
	}

	newCustomSearchAttributes := util.CloneMapNonNil(currentSearchAttributes.Custom())
	maps.Copy(newCustomSearchAttributes, customAttributesToAdd)
	err = h.saManager.SaveSearchAttributes(ctx, indexName, newCustomSearchAttributes)
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf(errUnableToSaveSearchAttributesMessage, err))
	}
	return nil
}

func (h *OperatorHandlerImpl) addSearchAttributesSQL(
	ctx context.Context,
	request *operatorservice.AddSearchAttributesRequest,
	visManager manager.VisibilityManager,
) error {
	indexName := visManager.GetIndexName()
	currentSearchAttributes, err := h.saManager.GetSearchAttributes(indexName, true)
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf(errUnableToGetSearchAttributesMessage, err))
	}

	_, client, err := h.clientFactory.NewLocalFrontendClientWithTimeout(
		frontend.DefaultTimeout,
		frontend.DefaultLongPollTimeout,
	)
	if err != nil {
		return serviceerror.NewUnavailablef(errUnableToCreateFrontendClientMessage, err)
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
		return serviceerror.NewUnavailablef(errUnableToGetNamespaceInfoMessage, nsName, err)
	}

	cmCustomSearchAttributes := currentSearchAttributes.Custom()
	upsertFieldToAliasMap := make(map[string]string)
	fieldToAliasMap := resp.Config.CustomSearchAttributeAliases
	aliasToFieldMap := util.InverseMap(fieldToAliasMap)
	for saName, saType := range request.GetSearchAttributes() {
		// check if alias is already in use
		if _, ok := aliasToFieldMap[saName]; ok {
			h.logger.Warn(
				fmt.Sprintf(errSearchAttributeAlreadyExistsMessage, saName),
				tag.NewStringTag(namespaceTagName, nsName),
				tag.NewStringTag(visibilitySearchAttributeTagName, saName),
			)
			continue
		}
		// find the first available field for the given type
		targetFieldName := ""
		cntUsed := 0
		for fieldName, fieldType := range cmCustomSearchAttributes {
			if fieldType != saType || !sadefs.IsPreallocatedCSAFieldName(fieldName, fieldType) {
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
			return serviceerror.NewInvalidArgumentf(errTooManySearchAttributesMessage, cntUsed, saType)
		}
		upsertFieldToAliasMap[targetFieldName] = saName
	}

	// If the map is empty, then all custom search attributes already exists.
	if len(upsertFieldToAliasMap) == 0 {
		return nil
	}

	_, err = client.UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: nsName,
		Config: &namespacepb.NamespaceConfig{
			CustomSearchAttributeAliases: upsertFieldToAliasMap,
		},
	})
	if err != nil {
		if err.Error() == errCustomSearchAttributeFieldAlreadyAllocated.Error() {
			return errRaceConditionAddingSearchAttributes
		}
		return serviceerror.NewUnavailablef(errUnableToSaveSearchAttributesMessage, err)
	}
	return nil
}

func (h *OperatorHandlerImpl) RemoveSearchAttributes(
	ctx context.Context,
	request *operatorservice.RemoveSearchAttributesRequest,
) (_ *operatorservice.RemoveSearchAttributesResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)

	// validate request
	if request == nil {
		return nil, errRequestNotSet
	}

	if len(request.GetSearchAttributes()) == 0 {
		return nil, errSearchAttributesNotSet
	}

	var visManagers []manager.VisibilityManager
	if visManagerDual, ok := h.visibilityMgr.(*visibility.VisibilityManagerDual); ok {
		visManagers = append(
			visManagers,
			visManagerDual.GetPrimaryVisibility(),
			visManagerDual.GetSecondaryVisibility(),
		)
	} else {
		visManagers = append(visManagers, h.visibilityMgr)
	}

	for _, visManager := range visManagers {
		if err := h.removeSearchAttributesInternal(ctx, request, visManager); err != nil {
			return nil, err
		}
	}

	return &operatorservice.RemoveSearchAttributesResponse{}, nil
}

func (h *OperatorHandlerImpl) removeSearchAttributesInternal(
	ctx context.Context,
	request *operatorservice.RemoveSearchAttributesRequest,
	visManager manager.VisibilityManager,
) error {
	storeName := visManager.GetStoreNames()[0]
	if storeName == elasticsearch.PersistenceName {
		return h.removeSearchAttributesElasticsearch(ctx, request, visManager)
	}
	return h.removeSearchAttributesSQL(ctx, request, visManager)
}

func (h *OperatorHandlerImpl) removeSearchAttributesElasticsearch(
	ctx context.Context,
	request *operatorservice.RemoveSearchAttributesRequest,
	visManager manager.VisibilityManager,
) error {
	indexName := h.visibilityMgr.GetIndexName()
	currentSearchAttributes, err := h.saManager.GetSearchAttributes(indexName, true)
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf(errUnableToGetSearchAttributesMessage, err))
	}

	newCustomSearchAttributes := maps.Clone(currentSearchAttributes.Custom())
	for _, saName := range request.GetSearchAttributes() {
		if !currentSearchAttributes.IsDefined(saName) {
			// Custom search attribute not found, skip it.
			continue
		}
		if _, ok := newCustomSearchAttributes[saName]; !ok {
			return serviceerror.NewInvalidArgumentf(
				errUnableToRemoveNonCustomSearchAttributesMessage, saName,
			)
		}
		delete(newCustomSearchAttributes, saName)
	}

	if len(newCustomSearchAttributes) == len(currentSearchAttributes.Custom()) {
		return nil
	}

	err = h.saManager.SaveSearchAttributes(ctx, indexName, newCustomSearchAttributes)
	if err != nil {
		return serviceerror.NewUnavailablef(errUnableToSaveSearchAttributesMessage, err)
	}
	return nil
}

func (h *OperatorHandlerImpl) removeSearchAttributesSQL(
	ctx context.Context,
	request *operatorservice.RemoveSearchAttributesRequest,
	visManager manager.VisibilityManager,
) error {
	indexName := h.visibilityMgr.GetIndexName()
	currentSearchAttributes, err := h.saManager.GetSearchAttributes(indexName, true)
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf(errUnableToGetSearchAttributesMessage, err))
	}

	_, client, err := h.clientFactory.NewLocalFrontendClientWithTimeout(
		frontend.DefaultTimeout,
		frontend.DefaultLongPollTimeout,
	)
	if err != nil {
		return serviceerror.NewUnavailablef(errUnableToCreateFrontendClientMessage, err)
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
		return serviceerror.NewUnavailablef(errUnableToGetNamespaceInfoMessage, nsName, err)
	}

	upsertFieldToAliasMap := make(map[string]string)
	aliasToFieldMap := util.InverseMap(resp.Config.CustomSearchAttributeAliases)
	for _, saName := range request.GetSearchAttributes() {
		if fieldName, ok := aliasToFieldMap[saName]; ok {
			upsertFieldToAliasMap[fieldName] = ""
			continue
		}
		if currentSearchAttributes.IsDefined(saName) {
			return serviceerror.NewInvalidArgumentf(
				errUnableToRemoveNonCustomSearchAttributesMessage, saName,
			)
		}
	}

	if len(upsertFieldToAliasMap) == 0 {
		return nil
	}

	_, err = client.UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: nsName,
		Config: &namespacepb.NamespaceConfig{
			CustomSearchAttributeAliases: upsertFieldToAliasMap,
		},
	})
	return err
}

func (h *OperatorHandlerImpl) ListSearchAttributes(
	ctx context.Context,
	request *operatorservice.ListSearchAttributesRequest,
) (_ *operatorservice.ListSearchAttributesResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	indexName := h.visibilityMgr.GetIndexName()
	searchAttributes, err := h.saManager.GetSearchAttributes(indexName, true)
	if err != nil {
		return nil, serviceerror.NewUnavailablef(
			"unable to read custom search attributes: %v", err,
		)
	}

	if h.visibilityMgr.HasStoreName(elasticsearch.PersistenceName) {
		return h.listSearchAttributesElasticsearch(ctx, indexName, searchAttributes)
	}
	return h.listSearchAttributesSQL(ctx, request, searchAttributes)
}

func (h *OperatorHandlerImpl) listSearchAttributesElasticsearch(
	ctx context.Context,
	indexName string,
	searchAttributes searchattribute.NameTypeMap,
) (*operatorservice.ListSearchAttributesResponse, error) {
	return &operatorservice.ListSearchAttributesResponse{
		CustomAttributes: searchAttributes.Custom(),
		SystemAttributes: searchAttributes.System(),
	}, nil
}

func (h *OperatorHandlerImpl) listSearchAttributesSQL(
	ctx context.Context,
	request *operatorservice.ListSearchAttributesRequest,
	searchAttributes searchattribute.NameTypeMap,
) (*operatorservice.ListSearchAttributesResponse, error) {
	_, client, err := h.clientFactory.NewLocalFrontendClientWithTimeout(
		frontend.DefaultTimeout,
		frontend.DefaultLongPollTimeout,
	)
	if err != nil {
		return nil, serviceerror.NewUnavailablef(errUnableToCreateFrontendClientMessage, err)
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
		return nil, serviceerror.NewUnavailablef(
			errUnableToGetNamespaceInfoMessage, nsName, err,
		)
	}

	fieldToAliasMap := resp.Config.CustomSearchAttributeAliases
	customSearchAttributes := make(map[string]enumspb.IndexedValueType)
	for field, tp := range searchAttributes.Custom() {
		if alias, ok := fieldToAliasMap[field]; ok {
			customSearchAttributes[alias] = tp
		}
	}
	return &operatorservice.ListSearchAttributesResponse{
		CustomAttributes: customSearchAttributes,
		SystemAttributes: searchAttributes.System(),
		StorageSchema:    nil,
	}, nil
}

func (h *OperatorHandlerImpl) DeleteNamespace(
	ctx context.Context,
	request *operatorservice.DeleteNamespaceRequest,
) (_ *operatorservice.DeleteNamespaceResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	// If NamespaceDeleteDelay is not provided, the default delay configured in the cluster should be used.
	var namespaceDeleteDelay time.Duration
	if request.NamespaceDeleteDelay == nil {
		namespaceDeleteDelay = h.config.DeleteNamespaceNamespaceDeleteDelay()
	} else {
		namespaceDeleteDelay = request.NamespaceDeleteDelay.AsDuration()
	}

	// Execute workflow.
	wfParams := deletenamespace.DeleteNamespaceWorkflowParams{
		Namespace:   namespace.Name(request.GetNamespace()),
		NamespaceID: namespace.ID(request.GetNamespaceId()),
		DeleteExecutionsConfig: deleteexecutions.DeleteExecutionsConfig{
			DeleteActivityRPS:                    h.config.DeleteNamespaceDeleteActivityRPS(),
			PageSize:                             h.config.DeleteNamespacePageSize(),
			PagesPerExecution:                    h.config.DeleteNamespacePagesPerExecution(),
			ConcurrentDeleteExecutionsActivities: h.config.DeleteNamespaceConcurrentDeleteExecutionsActivities(),
		},
		NamespaceDeleteDelay: namespaceDeleteDelay,
	}

	sdkClient := h.sdkClientFactory.GetSystemClient()
	run, err := sdkClient.ExecuteWorkflow(
		ctx,
		sdkclient.StartWorkflowOptions{
			TaskQueue: primitives.DefaultWorkerTaskQueue,
			ID:        fmt.Sprintf("%s/%s", deletenamespace.WorkflowName, request.GetNamespace()),
		},
		deletenamespace.WorkflowName,
		wfParams,
	)
	if err != nil {
		return nil, serviceerror.NewUnavailablef(errUnableToStartWorkflowMessage, deletenamespace.WorkflowName, err)
	}

	// Wait for the workflow to complete.
	var wfResult deletenamespace.DeleteNamespaceWorkflowResult
	err = run.Get(ctx, &wfResult)
	if err != nil {
		return nil, delnserrors.ToServiceError(err, run.GetID(), run.GetRunID())
	}

	return &operatorservice.DeleteNamespaceResponse{
		DeletedNamespace: wfResult.DeletedNamespace.String(),
	}, nil
}

// AddOrUpdateRemoteCluster adds or updates the connection config to a remote cluster.
func (h *OperatorHandlerImpl) AddOrUpdateRemoteCluster(
	ctx context.Context,
	request *operatorservice.AddOrUpdateRemoteClusterRequest,
) (_ *operatorservice.AddOrUpdateRemoteClusterResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)

	adminClient := h.clientFactory.NewRemoteAdminClientWithTimeout(
		request.GetFrontendAddress(),
		admin.DefaultTimeout,
		admin.DefaultLargeTimeout,
	)

	// Fetch cluster metadata from remote cluster
	resp, err := adminClient.DescribeCluster(ctx, &adminservice.DescribeClusterRequest{})
	if err != nil {
		return nil, serviceerror.NewUnavailablef(
			errUnableConnectRemoteClusterMessage,
			request.GetFrontendAddress(),
			err,
		)
	}

	err = h.validateRemoteClusterMetadata(resp)
	if err != nil {
		return nil, serviceerror.NewInvalidArgumentf(errInvalidRemoteClusterInfo, err)
	}

	var updateRequestVersion int64 = 0
	clusterData, err := h.clusterMetadataManager.GetClusterMetadata(
		ctx,
		&persistence.GetClusterMetadataRequest{ClusterName: resp.GetClusterName()},
	)
	switch err.(type) {
	case nil:
		updateRequestVersion = clusterData.Version
	case *serviceerror.NotFound:
		updateRequestVersion = 0
	default:
		return nil, serviceerror.NewInternalf(errUnableToStoreClusterInfo, err)
	}

	applied, err := h.clusterMetadataManager.SaveClusterMetadata(ctx, &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: &persistencespb.ClusterMetadata{
			ClusterName:              resp.GetClusterName(),
			HistoryShardCount:        resp.GetHistoryShardCount(),
			ClusterId:                resp.GetClusterId(),
			ClusterAddress:           request.GetFrontendAddress(),
			HttpAddress:              resp.GetHttpAddress(),
			FailoverVersionIncrement: resp.GetFailoverVersionIncrement(),
			InitialFailoverVersion:   resp.GetInitialFailoverVersion(),
			IsGlobalNamespaceEnabled: resp.GetIsGlobalNamespaceEnabled(),
			IsConnectionEnabled:      request.GetEnableRemoteClusterConnection(),
			IsReplicationEnabled:     request.GetEnableReplication(),
			Tags:                     resp.GetTags(),
		},
		Version: updateRequestVersion,
	})
	if err != nil {
		return nil, serviceerror.NewInternalf(errUnableToStoreClusterInfo, err)
	}
	if !applied {
		return nil, serviceerror.NewInvalidArgumentf(errUnableToStoreClusterInfo, err)
	}
	return &operatorservice.AddOrUpdateRemoteClusterResponse{}, nil
}

func (h *OperatorHandlerImpl) RemoveRemoteCluster(
	ctx context.Context,
	request *operatorservice.RemoveRemoteClusterRequest,
) (_ *operatorservice.RemoveRemoteClusterResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)

	var isClusterNameExist bool
	for clusterName := range h.clusterMetadata.GetAllClusterInfo() {
		if clusterName == request.GetClusterName() {
			isClusterNameExist = true
			break
		}
	}
	if !isClusterNameExist {
		return nil, serviceerror.NewNotFound("The cluster to be deleted cannot be found in clusters cache.")
	}

	if err := h.clusterMetadataManager.DeleteClusterMetadata(
		ctx,
		&persistence.DeleteClusterMetadataRequest{ClusterName: request.GetClusterName()},
	); err != nil {
		return nil, serviceerror.NewInternalf(errUnableToDeleteClusterInfo, err)
	}
	return &operatorservice.RemoveRemoteClusterResponse{}, nil
}

func (h *OperatorHandlerImpl) ListClusters(
	ctx context.Context,
	request *operatorservice.ListClustersRequest,
) (_ *operatorservice.ListClustersResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)
	if request == nil {
		return nil, errRequestNotSet
	}
	if request.GetPageSize() <= 0 {
		request.PageSize = listClustersPageSize
	}

	resp, err := h.clusterMetadataManager.ListClusterMetadata(ctx, &persistence.ListClusterMetadataRequest{
		PageSize:      int(request.GetPageSize()),
		NextPageToken: request.GetNextPageToken(),
	})
	if err != nil {
		return nil, err
	}

	var clusterMetadataList []*operatorservice.ClusterMetadata
	for _, clusterResp := range resp.ClusterMetadata {
		clusterMetadataList = append(clusterMetadataList, &operatorservice.ClusterMetadata{
			ClusterName:            clusterResp.GetClusterName(),
			ClusterId:              clusterResp.GetClusterId(),
			Address:                clusterResp.GetClusterAddress(),
			HttpAddress:            clusterResp.GetHttpAddress(),
			InitialFailoverVersion: clusterResp.GetInitialFailoverVersion(),
			HistoryShardCount:      clusterResp.GetHistoryShardCount(),
			IsConnectionEnabled:    clusterResp.GetIsConnectionEnabled(),
			IsReplicationEnabled:   clusterResp.GetIsReplicationEnabled(),
		})
	}
	return &operatorservice.ListClustersResponse{
		Clusters:      clusterMetadataList,
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
		remoteShardCount := metadata.GetHistoryShardCount()
		large := remoteShardCount
		small := h.config.NumHistoryShards
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

func (h *OperatorHandlerImpl) CreateNexusEndpoint(
	ctx context.Context,
	request *operatorservice.CreateNexusEndpointRequest,
) (_ *operatorservice.CreateNexusEndpointResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)
	if !h.config.EnableNexusAPIs() {
		return nil, status.Error(codes.NotFound, "Nexus APIs are disabled")
	}
	return h.nexusEndpointClient.Create(ctx, request)
}

func (h *OperatorHandlerImpl) UpdateNexusEndpoint(
	ctx context.Context,
	request *operatorservice.UpdateNexusEndpointRequest,
) (_ *operatorservice.UpdateNexusEndpointResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)
	if !h.config.EnableNexusAPIs() {
		return nil, status.Error(codes.NotFound, "Nexus APIs are disabled")
	}
	return h.nexusEndpointClient.Update(ctx, request)
}

func (h *OperatorHandlerImpl) DeleteNexusEndpoint(
	ctx context.Context,
	request *operatorservice.DeleteNexusEndpointRequest,
) (_ *operatorservice.DeleteNexusEndpointResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)
	if !h.config.EnableNexusAPIs() {
		return nil, status.Error(codes.NotFound, "Nexus APIs are disabled")
	}
	return h.nexusEndpointClient.Delete(ctx, request)
}

func (h *OperatorHandlerImpl) GetNexusEndpoint(
	ctx context.Context,
	request *operatorservice.GetNexusEndpointRequest,
) (_ *operatorservice.GetNexusEndpointResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)
	if !h.config.EnableNexusAPIs() {
		return nil, status.Error(codes.NotFound, "Nexus APIs are disabled")
	}
	return h.nexusEndpointClient.Get(ctx, request)
}

func (h *OperatorHandlerImpl) ListNexusEndpoints(
	ctx context.Context,
	request *operatorservice.ListNexusEndpointsRequest,
) (_ *operatorservice.ListNexusEndpointsResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)
	if !h.config.EnableNexusAPIs() {
		return nil, status.Error(codes.NotFound, "Nexus APIs are disabled")
	}
	return h.nexusEndpointClient.List(ctx, request)
}
