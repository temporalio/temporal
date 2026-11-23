package frontend

import (
	"context"
	"fmt"
	"maps"
	"sync/atomic"
	"time"

	otellog "go.opentelemetry.io/otel/log"
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
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/esconfig"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/worker/deletenamespace"
	"go.temporal.io/server/service/worker/deletenamespace/deleteexecutions"
	delnserrors "go.temporal.io/server/service/worker/deletenamespace/errors"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

var _ OperatorHandler = (*OperatorHandlerImpl)(nil)

type (
	// OperatorHandlerImpl - gRPC handler interface for operator service
	OperatorHandlerImpl struct {
		operatorservice.UnimplementedOperatorServiceServer

		status int32

		logger                 log.Logger
		eventLogger            otellog.Logger
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
		EventLogger            otellog.Logger
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
		eventLogger:            args.EventLogger,
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
	if storeName == esconfig.PersistenceName && !h.useElasticsearchNamespaceAliases() {
		return h.addSearchAttributesElasticsearch(ctx, request, visManager)
	}
	return h.addSearchAttributeAliases(ctx, request, visManager, storeName == esconfig.PersistenceName)
}

// useElasticsearchNamespaceAliases returns true if custom search attributes of an Elasticsearch
// visibility store are registered as namespace aliases of pre-allocated fields, like SQL ones,
// instead of being added to the index mapping and cluster metadata (legacy).
func (h *OperatorHandlerImpl) useElasticsearchNamespaceAliases() bool {
	return h.config.EnableElasticsearchNamespaceSearchAttributeAliases != nil &&
		h.config.EnableElasticsearchNamespaceSearchAttributeAliases()
}

// validateNamespaceIsActive rejects namespace alias changes for a global namespace on a cluster
// that is not active for it. Namespace aliases are replicated with the namespace, and changes made
// concurrently on different clusters can be dropped by namespace replication.
func (h *OperatorHandlerImpl) validateNamespaceIsActive(resp *workflowservice.DescribeNamespaceResponse) error {
	if !resp.GetIsGlobalNamespace() {
		return nil
	}
	currentCluster := h.clusterMetadata.GetCurrentClusterName()
	activeCluster := resp.GetReplicationConfig().GetActiveClusterName()
	if activeCluster == "" || activeCluster == currentCluster {
		return nil
	}
	return serviceerror.NewNamespaceNotActive(resp.GetNamespaceInfo().GetName(), currentCluster, activeCluster)
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
				tag.String(visibilityIndexNameTagName, indexName),
				tag.String(visibilitySearchAttributeTagName, saName),
			)
		}
	}

	// If the map is empty, then all custom search attributes already exists.
	if len(customAttributesToAdd) == 0 {
		return nil
	}

	err = visManager.AddSearchAttributes(
		ctx,
		&manager.AddSearchAttributesRequest{SearchAttributes: customAttributesToAdd},
	)
	if err != nil {
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

// addSearchAttributeAliases registers custom search attributes as namespace aliases of
// pre-allocated fields. It's used by SQL visibility stores, and by Elasticsearch ones when
// useElasticsearchNamespaceAliases is enabled.
func (h *OperatorHandlerImpl) addSearchAttributeAliases(
	ctx context.Context,
	request *operatorservice.AddSearchAttributesRequest,
	visManager manager.VisibilityManager,
	isElasticsearch bool,
) error {
	if isElasticsearch {
		if err := validateNoPreallocatedFieldNames(request.GetSearchAttributes()); err != nil {
			return err
		}
	}

	indexName := visManager.GetIndexName()
	currentSearchAttributes, err := h.saManager.GetSearchAttributes(indexName, true)
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf(errUnableToGetSearchAttributesMessage, err))
	}

	nsName := request.GetNamespace()
	resp, err := h.getNamespaceInfo(ctx, nsName)
	if err != nil {
		return err
	}
	if err := h.validateNamespaceIsActive(resp); err != nil {
		return err
	}

	cmCustomSearchAttributes := currentSearchAttributes.Custom()
	upsertFieldToAliasMap := make(map[string]string)
	fieldToAliasMap := resp.Config.CustomSearchAttributeAliases
	aliasToFieldMap := util.InverseMap(fieldToAliasMap)
	for saName, saType := range request.GetSearchAttributes() {
		// Check if the name is already in use, either as an alias or as a custom search attribute
		// registered in cluster metadata (eg: by an Elasticsearch visibility store before namespace
		// aliases were enabled), which every namespace can use.
		if _, ok := aliasToFieldMap[saName]; ok || isClusterLevelCustomSearchAttribute(cmCustomSearchAttributes, saName) {
			h.logger.Warn(
				fmt.Sprintf(errSearchAttributeAlreadyExistsMessage, saName),
				tag.String(namespaceTagName, nsName),
				tag.String(visibilitySearchAttributeTagName, saName),
			)
			continue
		}
		targetFieldName, cntUsed := findFreePreallocatedField(
			cmCustomSearchAttributes, saType, fieldToAliasMap, upsertFieldToAliasMap,
		)
		if targetFieldName == "" {
			return serviceerror.NewInvalidArgumentf(errTooManySearchAttributesMessage, cntUsed, saType)
		}
		upsertFieldToAliasMap[targetFieldName] = saName
	}

	// If the map is empty, then all custom search attributes already exist.
	if len(upsertFieldToAliasMap) == 0 {
		return nil
	}

	return h.updateNamespaceAliases(ctx, nsName, upsertFieldToAliasMap)
}

// findFreePreallocatedField returns the first pre-allocated field of the given type that is not
// used by an existing or a pending alias (or an empty string if there is none), and the number of
// used fields of that type.
func findFreePreallocatedField(
	customSearchAttributes map[string]enumspb.IndexedValueType,
	saType enumspb.IndexedValueType,
	fieldToAliasMap map[string]string,
	pendingFieldToAliasMap map[string]string,
) (string, int) {
	cntUsed := 0
	for fieldName, fieldType := range customSearchAttributes {
		if fieldType != saType || !sadefs.IsPreallocatedCSAFieldName(fieldName, fieldType) {
			continue
		}
		if _, ok := fieldToAliasMap[fieldName]; ok {
			cntUsed++
		} else if _, ok := pendingFieldToAliasMap[fieldName]; ok {
			cntUsed++
		} else {
			return fieldName, cntUsed
		}
	}
	return "", cntUsed
}

// validateNoPreallocatedFieldNames rejects pre-allocated field names (eg: Keyword01) as aliases of
// an Elasticsearch visibility store: its schema maps them with a fixed type, and cluster metadata
// already lists them.
func validateNoPreallocatedFieldNames(searchAttributes map[string]enumspb.IndexedValueType) error {
	for saName := range searchAttributes {
		if sadefs.IsPreallocatedCSAFieldNameOfAnyType(saName) {
			return serviceerror.NewInvalidArgumentf(errSearchAttributeIsPreallocatedFieldMessage, saName)
		}
	}
	return nil
}

// isClusterLevelCustomSearchAttribute returns true if name is a custom search attribute registered
// in cluster metadata that is not a pre-allocated field, ie: it's usable by every namespace under its
// own name.
func isClusterLevelCustomSearchAttribute(
	customSearchAttributes map[string]enumspb.IndexedValueType,
	name string,
) bool {
	saType, ok := customSearchAttributes[name]
	return ok && !sadefs.IsPreallocatedCSAFieldName(name, saType)
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
	if storeName != esconfig.PersistenceName {
		return h.removeSearchAttributeAliases(ctx, request.GetNamespace(), request.GetSearchAttributes(), visManager)
	}
	if !h.useElasticsearchNamespaceAliases() {
		return h.removeSearchAttributesElasticsearch(ctx, request, visManager)
	}

	// Custom search attributes registered in cluster metadata are removed from cluster metadata,
	// and the rest are removed from the namespace aliases.
	currentSearchAttributes, err := h.saManager.GetSearchAttributes(visManager.GetIndexName(), true)
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf(errUnableToGetSearchAttributesMessage, err))
	}
	var clusterLevel, aliases []string
	for _, saName := range request.GetSearchAttributes() {
		if isClusterLevelCustomSearchAttribute(currentSearchAttributes.Custom(), saName) {
			clusterLevel = append(clusterLevel, saName)
		} else {
			aliases = append(aliases, saName)
		}
	}
	if len(aliases) > 0 {
		if err := h.removeSearchAttributeAliases(ctx, request.GetNamespace(), aliases, visManager); err != nil {
			return err
		}
	}
	if len(clusterLevel) > 0 {
		return h.removeSearchAttributesElasticsearch(
			ctx,
			&operatorservice.RemoveSearchAttributesRequest{
				SearchAttributes: clusterLevel,
				Namespace:        request.GetNamespace(),
			},
			visManager,
		)
	}
	return nil
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

// removeSearchAttributeAliases removes custom search attributes registered as namespace aliases.
func (h *OperatorHandlerImpl) removeSearchAttributeAliases(
	ctx context.Context,
	nsName string,
	searchAttributes []string,
	visManager manager.VisibilityManager,
) error {
	indexName := visManager.GetIndexName()
	currentSearchAttributes, err := h.saManager.GetSearchAttributes(indexName, true)
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf(errUnableToGetSearchAttributesMessage, err))
	}

	resp, err := h.getNamespaceInfo(ctx, nsName)
	if err != nil {
		return err
	}
	if err := h.validateNamespaceIsActive(resp); err != nil {
		return err
	}

	upsertFieldToAliasMap := make(map[string]string)
	aliasToFieldMap := util.InverseMap(resp.Config.CustomSearchAttributeAliases)
	for _, saName := range searchAttributes {
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

	return h.updateNamespaceAliases(ctx, nsName, upsertFieldToAliasMap)
}

func (h *OperatorHandlerImpl) getNamespaceInfo(
	ctx context.Context,
	namespaceName string,
) (*workflowservice.DescribeNamespaceResponse, error) {
	if namespaceName == "" {
		return nil, errNamespaceNotSet
	}

	_, client, err := h.clientFactory.NewLocalFrontendClientWithTimeout(
		frontend.DefaultTimeout,
		frontend.DefaultLongPollTimeout,
	)
	if err != nil {
		return nil, serviceerror.NewUnavailablef(errUnableToCreateFrontendClientMessage, err)
	}

	resp, err := client.DescribeNamespace(
		ctx,
		&workflowservice.DescribeNamespaceRequest{Namespace: namespaceName},
	)
	if err != nil {
		return nil, serviceerror.NewUnavailablef(errUnableToGetNamespaceInfoMessage, namespaceName, err)
	}

	return resp, nil
}

func (h *OperatorHandlerImpl) updateNamespaceAliases(
	ctx context.Context,
	namespaceName string,
	fieldToAliasMap map[string]string,
) error {
	_, client, err := h.clientFactory.NewLocalFrontendClientWithTimeout(
		frontend.DefaultTimeout,
		frontend.DefaultLongPollTimeout,
	)
	if err != nil {
		return serviceerror.NewUnavailablef(errUnableToCreateFrontendClientMessage, err)
	}

	_, err = client.UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: namespaceName,
		Config: &namespacepb.NamespaceConfig{
			CustomSearchAttributeAliases: fieldToAliasMap,
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

	if h.visibilityMgr.HasStoreName(esconfig.PersistenceName) {
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
	var (
		lifecycleEvent     *remoteClusterLifecycleEvent
		remoteResponse     *adminservice.DescribeClusterResponse
		persistedBefore    *persistence.GetClusterMetadataResponse
		persistenceRequest *persistence.SaveClusterMetadataRequest
	)
	// Bracket deferred emission with panic capture: the inner capture converts handler panics
	// into retError for the event, while the outer capture recovers panics from emission itself.
	defer log.CapturePanic(h.logger, &retError)
	defer func() {
		lifecycleEvent.emitUpsertFailure(retError, remoteResponse, persistedBefore, persistenceRequest)
	}()
	defer log.CapturePanic(h.logger, &retError)
	lifecycleEvent = newRemoteClusterUpsertLifecycleEvent(
		ctx,
		h.eventLogger,
		h.clusterMetadata,
		h.config,
		remoteClusterAPIOperator,
		remoteClusterUpsertRequestFields{
			FrontendAddress:               request.GetFrontendAddress(),
			FrontendHTTPAddress:           request.GetFrontendHttpAddress(),
			EnableRemoteClusterConnection: request.GetEnableRemoteClusterConnection(),
			EnableReplication:             request.GetEnableReplication(),
		},
	)

	adminClient := h.clientFactory.NewRemoteAdminClientWithTimeout(
		request.GetFrontendAddress(),
		admin.DefaultTimeout,
		admin.DefaultLargeTimeout,
	)

	// Fetch cluster metadata from remote cluster
	resp, err := adminClient.DescribeCluster(ctx, &adminservice.DescribeClusterRequest{})
	remoteResponse = resp
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
	// Validate the same invariants the in-memory cluster metadata enforces,
	// against the values we are about to persist. Without this, a bad row
	// would crash the metadata refresher on every host on the next tick.
	if err := clustermetadata.ValidateClusterInformation(
		resp.GetClusterName(),
		clustermetadata.ClusterInformation{
			Enabled:                request.GetEnableRemoteClusterConnection(),
			InitialFailoverVersion: resp.GetInitialFailoverVersion(),
			RPCAddress:             request.GetFrontendAddress(),
		},
		h.clusterMetadata.GetFailoverVersionIncrement(),
	); err != nil {
		return nil, serviceerror.NewInvalidArgumentf(errInvalidRemoteClusterInfo, err)
	}

	var updateRequestVersion int64 = 0
	clusterData, err := h.clusterMetadataManager.GetClusterMetadata(
		ctx,
		&persistence.GetClusterMetadataRequest{ClusterName: resp.GetClusterName()},
	)
	switch err.(type) {
	case nil:
		persistedBefore = clusterData
		updateRequestVersion = clusterData.Version
	case *serviceerror.NotFound:
		updateRequestVersion = 0
	default:
		return nil, serviceerror.NewInternalf(errUnableToStoreClusterInfo, err)
	}
	saveRequest := &persistence.SaveClusterMetadataRequest{
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
	}
	persistenceRequest = saveRequest
	applied, err := h.clusterMetadataManager.SaveClusterMetadata(ctx, saveRequest)
	if err != nil {
		return nil, serviceerror.NewInternalf(errUnableToStoreClusterInfo, err)
	}
	if !applied {
		return nil, serviceerror.NewInvalidArgumentf(errUnableToStoreClusterInfo, err)
	}
	lifecycleEvent.emitUpsertSuccess(persistedBefore, persistenceRequest)
	return &operatorservice.AddOrUpdateRemoteClusterResponse{}, nil
}

func (h *OperatorHandlerImpl) RemoveRemoteCluster(
	ctx context.Context,
	request *operatorservice.RemoveRemoteClusterRequest,
) (_ *operatorservice.RemoveRemoteClusterResponse, retError error) {
	var (
		lifecycleEvent     *remoteClusterLifecycleEvent
		cachedBefore       cachedRemoteClusterLookup
		persistenceRequest *persistence.DeleteClusterMetadataRequest
	)
	// Bracket deferred emission with panic capture: the inner capture converts handler panics
	// into retError for the event, while the outer capture recovers panics from emission itself.
	defer log.CapturePanic(h.logger, &retError)
	defer func() {
		lifecycleEvent.emitRemoveFailure(retError, cachedBefore, persistenceRequest)
	}()
	defer log.CapturePanic(h.logger, &retError)
	clusterName := request.GetClusterName()
	lifecycleEvent = newRemoteClusterRemoveLifecycleEvent(
		ctx,
		h.eventLogger,
		h.clusterMetadata,
		h.config,
		remoteClusterAPIOperator,
		remoteClusterRemoveRequestFields{ClusterName: clusterName},
	)

	cachedBefore = lookupCachedRemoteCluster(h.clusterMetadata, clusterName)
	if !cachedBefore.found {
		return nil, serviceerror.NewNotFound("The cluster to be deleted cannot be found in clusters cache.")
	}

	if err := validateClusterNotInUseByNamespaces(h.namespaceRegistry, h.clusterMetadata.GetCurrentClusterName(), clusterName); err != nil {
		return nil, err
	}

	deleteRequest := &persistence.DeleteClusterMetadataRequest{ClusterName: clusterName}
	persistenceRequest = deleteRequest
	if err := h.clusterMetadataManager.DeleteClusterMetadata(ctx, deleteRequest); err != nil {
		return nil, serviceerror.NewInternalf(errUnableToDeleteClusterInfo, err)
	}
	lifecycleEvent.emitRemoveSuccess(cachedBefore, persistenceRequest)
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
	if metadata.GetHistoryShardCount() <= 0 {
		// Guards the modulo below against divide-by-zero and rejects nonsense shard counts.
		return serviceerror.NewInvalidArgument("Remote cluster HistoryShardCount must be positive")
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
	return h.nexusEndpointClient.Create(ctx, request)
}

func (h *OperatorHandlerImpl) UpdateNexusEndpoint(
	ctx context.Context,
	request *operatorservice.UpdateNexusEndpointRequest,
) (_ *operatorservice.UpdateNexusEndpointResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)
	return h.nexusEndpointClient.Update(ctx, request)
}

func (h *OperatorHandlerImpl) DeleteNexusEndpoint(
	ctx context.Context,
	request *operatorservice.DeleteNexusEndpointRequest,
) (_ *operatorservice.DeleteNexusEndpointResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)
	return h.nexusEndpointClient.Delete(ctx, request)
}

func (h *OperatorHandlerImpl) GetNexusEndpoint(
	ctx context.Context,
	request *operatorservice.GetNexusEndpointRequest,
) (_ *operatorservice.GetNexusEndpointResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)
	return h.nexusEndpointClient.Get(ctx, request)
}

func (h *OperatorHandlerImpl) ListNexusEndpoints(
	ctx context.Context,
	request *operatorservice.ListNexusEndpointsRequest,
) (_ *operatorservice.ListNexusEndpointsResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)
	return h.nexusEndpointClient.List(ctx, request)
}
