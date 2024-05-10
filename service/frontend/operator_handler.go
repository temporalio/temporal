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
	"sync/atomic"

	"golang.org/x/exp/maps"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	commonpb "go.temporal.io/api/common/v1"
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
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/worker/addsearchattributes"
	"go.temporal.io/server/service/worker/deletenamespace"
	"go.temporal.io/server/service/worker/deletenamespace/deleteexecutions"
)

var _ OperatorHandler = (*OperatorHandlerImpl)(nil)

type (
	// OperatorHandlerImpl - gRPC handler interface for operator service
	OperatorHandlerImpl struct {
		operatorservice.UnsafeOperatorServiceServer

		status int32

		logger                 log.Logger
		config                 *Config
		esClient               esclient.Client
		sdkClientFactory       sdk.ClientFactory
		metricsHandler         metrics.Handler
		visibilityMgr          manager.VisibilityManager
		saManager              searchattribute.Manager
		healthServer           *health.Server
		historyClient          resource.HistoryClient
		clusterMetadataManager persistence.ClusterMetadataManager
		clusterMetadata        clustermetadata.Metadata
		clientFactory          svc.Factory
		nexusEndpointClient    *NexusEndpointClient
	}

	NewOperatorHandlerImplArgs struct {
		config                 *Config
		EsClient               esclient.Client
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
		esClient:               args.EsClient,
		sdkClientFactory:       args.sdkClientFactory,
		metricsHandler:         args.MetricsHandler,
		visibilityMgr:          args.VisibilityMgr,
		saManager:              args.SaManager,
		healthServer:           args.healthServer,
		historyClient:          args.historyClient,
		clusterMetadataManager: args.clusterMetadataManager,
		clusterMetadata:        args.clusterMetadata,
		clientFactory:          args.clientFactory,
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
		if searchattribute.IsReserved(saName) {
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(errSearchAttributeIsReservedMessage, saName))
		}
		if _, ok := enumspb.IndexedValueType_name[int32(saType)]; !ok {
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(errUnknownSearchAttributeTypeMessage, saType))
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
		var (
			storeName = visManager.GetStoreNames()[0]
			indexName = visManager.GetIndexName()
		)
		if err := h.addSearchAttributesInternal(ctx, request, storeName, indexName); err != nil {
			return nil, fmt.Errorf("Failed to add search attributes to store %s: %w", storeName, err)
		}
	}

	return &operatorservice.AddSearchAttributesResponse{}, nil
}

func (h *OperatorHandlerImpl) addSearchAttributesInternal(
	ctx context.Context,
	request *operatorservice.AddSearchAttributesRequest,
	storeName string,
	indexName string,
) error {
	currentSearchAttributes, err := h.saManager.GetSearchAttributes(indexName, true)
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf(errUnableToGetSearchAttributesMessage, err))
	}

	if indexName == "" {
		h.logger.Error(
			"Cannot add search attributes in standard visibility.",
			tag.NewStringTag("pluginName", storeName),
		)
	} else if storeName == elasticsearch.PersistenceName {
		scope := h.metricsHandler.WithTags(metrics.OperationTag(metrics.OperatorAddSearchAttributesScope))
		err = h.addSearchAttributesElasticsearch(ctx, request, indexName, currentSearchAttributes)
		if err != nil {
			if _, isWorkflowErr := err.(*serviceerror.SystemWorkflow); isWorkflowErr {
				metrics.AddSearchAttributesWorkflowFailuresCount.With(scope).Record(1)
			}
		} else {
			metrics.AddSearchAttributesWorkflowSuccessCount.With(scope).Record(1)
		}
	} else {
		err = h.addSearchAttributesSQL(ctx, request, currentSearchAttributes)
	}
	return err
}

func (h *OperatorHandlerImpl) addSearchAttributesElasticsearch(
	ctx context.Context,
	request *operatorservice.AddSearchAttributesRequest,
	indexName string,
	currentSearchAttributes searchattribute.NameTypeMap,
) error {
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

	// Execute workflow.
	wfParams := addsearchattributes.WorkflowParams{
		CustomAttributesToAdd: customAttributesToAdd,
		IndexName:             indexName,
		SkipSchemaUpdate:      false,
	}

	sdkClient := h.sdkClientFactory.GetSystemClient()
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
		execution := &commonpb.WorkflowExecution{
			WorkflowId: addsearchattributes.WorkflowName,
			RunId:      run.GetRunID(),
		}
		return serviceerror.NewSystemWorkflow(execution, err)
	}
	return nil
}

func (h *OperatorHandlerImpl) addSearchAttributesSQL(
	ctx context.Context,
	request *operatorservice.AddSearchAttributesRequest,
	currentSearchAttributes searchattribute.NameTypeMap,
) error {
	_, client, err := h.clientFactory.NewLocalFrontendClientWithTimeout(
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
				fmt.Sprintf(errTooManySearchAttributesMessage, cntUsed, saType),
			)
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
		return serviceerror.NewUnavailable(fmt.Sprintf(errUnableToSaveSearchAttributesMessage, err))
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

	indexName := h.visibilityMgr.GetIndexName()
	currentSearchAttributes, err := h.saManager.GetSearchAttributes(indexName, true)
	if err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf(errUnableToGetSearchAttributesMessage, err))
	}

	// TODO (rodrigozhou): Remove condition `indexName == ""`.
	// If indexName == "", then calling addSearchAttributesElasticsearch will
	// register the search attributes in the cluster metadata if ES is up or if
	// `skip-schema-update` is set. This is for backward compatibility using
	// standard visibility.
	if h.visibilityMgr.HasStoreName(elasticsearch.PersistenceName) || indexName == "" {
		err = h.removeSearchAttributesElasticsearch(ctx, request, indexName, currentSearchAttributes)
	} else {
		err = h.removeSearchAttributesSQL(ctx, request, currentSearchAttributes)
	}

	if err != nil {
		return nil, err
	}
	return &operatorservice.RemoveSearchAttributesResponse{}, nil
}

func (h *OperatorHandlerImpl) removeSearchAttributesElasticsearch(
	ctx context.Context,
	request *operatorservice.RemoveSearchAttributesRequest,
	indexName string,
	currentSearchAttributes searchattribute.NameTypeMap,
) error {
	newCustomSearchAttributes := maps.Clone(currentSearchAttributes.Custom())
	for _, saName := range request.GetSearchAttributes() {
		if !currentSearchAttributes.IsDefined(saName) {
			return serviceerror.NewNotFound(fmt.Sprintf(errSearchAttributeDoesntExistMessage, saName))
		}
		if _, ok := newCustomSearchAttributes[saName]; !ok {
			return serviceerror.NewInvalidArgument(
				fmt.Sprintf(errUnableToRemoveNonCustomSearchAttributesMessage, saName),
			)
		}
		delete(newCustomSearchAttributes, saName)
	}

	err := h.saManager.SaveSearchAttributes(ctx, indexName, newCustomSearchAttributes)
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf(errUnableToSaveSearchAttributesMessage, err))
	}
	return nil
}

func (h *OperatorHandlerImpl) removeSearchAttributesSQL(
	ctx context.Context,
	request *operatorservice.RemoveSearchAttributesRequest,
	currentSearchAttributes searchattribute.NameTypeMap,
) error {
	_, client, err := h.clientFactory.NewLocalFrontendClientWithTimeout(
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
		return nil, serviceerror.NewUnavailable(
			fmt.Sprintf("unable to read custom search attributes: %v", err),
		)
	}

	// TODO (rodrigozhou): Remove condition `indexName == ""`.
	// If indexName == "", then calling addSearchAttributesElasticsearch will
	// register the search attributes in the cluster metadata if ES is up or if
	// `skip-schema-update` is set. This is for backward compatibility using
	// standard visibility.
	if h.visibilityMgr.HasStoreName(elasticsearch.PersistenceName) || indexName == "" {
		return h.listSearchAttributesElasticsearch(ctx, indexName, searchAttributes)
	}
	return h.listSearchAttributesSQL(ctx, request, searchAttributes)
}

func (h *OperatorHandlerImpl) listSearchAttributesElasticsearch(
	ctx context.Context,
	indexName string,
	searchAttributes searchattribute.NameTypeMap,
) (*operatorservice.ListSearchAttributesResponse, error) {
	var storageSchema map[string]string
	if h.esClient != nil {
		var err error
		storageSchema, err = h.esClient.GetMapping(ctx, indexName)
		if err != nil {
			return nil, serviceerror.NewUnavailable(
				fmt.Sprintf("unable to get mapping from Elasticsearch: %v", err),
			)
		}
	}
	return &operatorservice.ListSearchAttributesResponse{
		CustomAttributes: searchAttributes.Custom(),
		SystemAttributes: searchAttributes.System(),
		StorageSchema:    storageSchema,
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

	// validate request
	if request == nil {
		return nil, errRequestNotSet
	}

	if request.GetNamespace() == primitives.SystemLocalNamespace || request.GetNamespaceId() == primitives.SystemNamespaceID {
		return nil, errUnableDeleteSystemNamespace
	}

	namespaceDeleteDelay := h.config.DeleteNamespaceNamespaceDeleteDelay()
	if request.NamespaceDeleteDelay != nil {
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
		return nil, serviceerror.NewUnavailable(fmt.Sprintf(errUnableToStartWorkflowMessage, deletenamespace.WorkflowName, err))
	}

	scope := h.metricsHandler.WithTags(metrics.OperationTag(metrics.OperatorDeleteNamespaceScope))

	// Wait for workflow to complete.
	var wfResult deletenamespace.DeleteNamespaceWorkflowResult
	err = run.Get(ctx, &wfResult)
	if err != nil {
		metrics.DeleteNamespaceWorkflowFailuresCount.With(scope).Record(1)
		execution := &commonpb.WorkflowExecution{WorkflowId: deletenamespace.WorkflowName, RunId: run.GetRunID()}
		return nil, serviceerror.NewSystemWorkflow(execution, err)
	}
	metrics.DeleteNamespaceWorkflowSuccessCount.With(scope).Record(1)

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
		return nil, serviceerror.NewUnavailable(fmt.Sprintf(
			errUnableConnectRemoteClusterMessage,
			request.GetFrontendAddress(),
			err,
		))
	}

	err = h.validateRemoteClusterMetadata(resp)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(errInvalidRemoteClusterInfo, err))
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
		return nil, serviceerror.NewInternal(fmt.Sprintf(errUnableToStoreClusterInfo, err))
	}

	applied, err := h.clusterMetadataManager.SaveClusterMetadata(ctx, &persistence.SaveClusterMetadataRequest{
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
		return nil, serviceerror.NewInternal(fmt.Sprintf(errUnableToStoreClusterInfo, err))
	}
	if !applied {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(errUnableToStoreClusterInfo, err))
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
		return nil, serviceerror.NewInternal(fmt.Sprintf(errUnableToDeleteClusterInfo, err))
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
			InitialFailoverVersion: clusterResp.GetInitialFailoverVersion(),
			HistoryShardCount:      clusterResp.GetHistoryShardCount(),
			IsConnectionEnabled:    clusterResp.GetIsConnectionEnabled(),
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
