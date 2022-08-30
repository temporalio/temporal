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
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	sdkclient "go.temporal.io/sdk/client"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/primitives"
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

		logger            log.Logger
		config            *Config
		esConfig          *esclient.Config
		esClient          esclient.Client
		sdkClientFactory  sdk.ClientFactory
		metricsHandler    metrics.Handler
		saProvider        searchattribute.Provider
		saManager         searchattribute.Manager
		healthServer      *health.Server
		historyClient     historyservice.HistoryServiceClient
		namespaceRegistry namespace.Registry
	}

	NewOperatorHandlerImplArgs struct {
		config            *Config
		EsConfig          *esclient.Config
		EsClient          esclient.Client
		Logger            log.Logger
		sdkClientFactory  sdk.ClientFactory
		MetricsHandler    metrics.Handler
		SaProvider        searchattribute.Provider
		SaManager         searchattribute.Manager
		healthServer      *health.Server
		historyClient     historyservice.HistoryServiceClient
		namespaceRegistry namespace.Registry
	}
)

// NewOperatorHandlerImpl creates a gRPC handler for operatorservice
func NewOperatorHandlerImpl(
	args NewOperatorHandlerImplArgs,
) *OperatorHandlerImpl {

	handler := &OperatorHandlerImpl{
		logger:            args.Logger,
		status:            common.DaemonStatusInitialized,
		config:            args.config,
		esConfig:          args.EsConfig,
		esClient:          args.EsClient,
		sdkClientFactory:  args.sdkClientFactory,
		metricsHandler:    args.MetricsHandler,
		saProvider:        args.SaProvider,
		saManager:         args.SaManager,
		healthServer:      args.healthServer,
		historyClient:     args.historyClient,
		namespaceRegistry: args.namespaceRegistry,
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
	defer log.CapturePanic(h.logger, &retError)

	metricsHandler, startTime := h.startRequestProfile(metrics.OperatorAddSearchAttributesOperation)
	defer func() {
		metricsHandler.Timer(metrics.ServiceLatency.MetricName.String()).Record(time.Since(startTime))
	}()

	// validate request
	if request == nil {
		return nil, errRequestNotSet
	}

	if len(request.GetSearchAttributes()) == 0 {
		return nil, errSearchAttributesNotSet
	}

	indexName := h.esConfig.GetVisibilityIndex()

	currentSearchAttributes, err := h.saProvider.GetSearchAttributes(indexName, true)
	if err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf(errUnableToGetSearchAttributesMessage, err))
	}

	for saName, saType := range request.GetSearchAttributes() {
		if searchattribute.IsReserved(saName) {
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(errSearchAttributeIsReservedMessage, saName))
		}
		if currentSearchAttributes.IsDefined(saName) {
			return nil, serviceerror.NewAlreadyExist(fmt.Sprintf(errSearchAttributeAlreadyExistsMessage, saName))
		}
		if _, ok := enumspb.IndexedValueType_name[int32(saType)]; !ok {
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(errUnknownSearchAttributeTypeMessage, saType))
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
		return nil, serviceerror.NewUnavailable(fmt.Sprintf(errUnableToStartWorkflowMessage, addsearchattributes.WorkflowName, err))
	}

	// Wait for workflow to complete.
	err = run.Get(ctx, nil)
	if err != nil {
		metricsHandler.Counter(metrics.AddSearchAttributesWorkflowFailuresCount.MetricName.String()).Record(1)
		execution := &commonpb.WorkflowExecution{WorkflowId: addsearchattributes.WorkflowName, RunId: run.GetRunID()}
		return nil, serviceerror.NewSystemWorkflow(execution, err)
	}
	metricsHandler.Counter(metrics.AddSearchAttributesWorkflowSuccessCount.MetricName.String()).Record(1)

	return &operatorservice.AddSearchAttributesResponse{}, nil
}

func (h *OperatorHandlerImpl) RemoveSearchAttributes(ctx context.Context, request *operatorservice.RemoveSearchAttributesRequest) (_ *operatorservice.RemoveSearchAttributesResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)

	// validate request
	if request == nil {
		return nil, errRequestNotSet
	}

	if len(request.GetSearchAttributes()) == 0 {
		return nil, errSearchAttributesNotSet
	}

	indexName := h.esConfig.GetVisibilityIndex()

	currentSearchAttributes, err := h.saProvider.GetSearchAttributes(indexName, true)
	if err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf(errUnableToGetSearchAttributesMessage, err))
	}

	newCustomSearchAttributes := maps.Clone(currentSearchAttributes.Custom())

	for _, saName := range request.GetSearchAttributes() {
		if !currentSearchAttributes.IsDefined(saName) {
			return nil, serviceerror.NewNotFound(fmt.Sprintf(errSearchAttributeDoesntExistMessage, saName))
		}
		if _, ok := newCustomSearchAttributes[saName]; !ok {
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(errUnableToRemoveNonCustomSearchAttributesMessage, saName))
		}
		delete(newCustomSearchAttributes, saName)
	}

	err = h.saManager.SaveSearchAttributes(ctx, indexName, newCustomSearchAttributes)
	if err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf(errUnableToSaveSearchAttributesMessage, err))
	}

	return &operatorservice.RemoveSearchAttributesResponse{}, nil
}

func (h *OperatorHandlerImpl) ListSearchAttributes(ctx context.Context, request *operatorservice.ListSearchAttributesRequest) (_ *operatorservice.ListSearchAttributesResponse, retError error) {
	defer log.CapturePanic(h.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	indexName := h.esConfig.GetVisibilityIndex()

	var lastErr error
	var esMapping map[string]string = nil
	if h.esClient != nil {
		esMapping, lastErr = h.esClient.GetMapping(ctx, indexName)
		if lastErr != nil {
			lastErr = serviceerror.NewUnavailable(fmt.Sprintf("unable to get mapping from Elasticsearch: %v", lastErr))
		}
	}

	searchAttributes, err := h.saProvider.GetSearchAttributes(indexName, true)
	if err != nil {
		lastErr = serviceerror.NewUnavailable(fmt.Sprintf("unable to read custom search attributes: %v", err))
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
	defer log.CapturePanic(h.logger, &retError)

	metricsHandler, startTime := h.startRequestProfile(metrics.OperatorDeleteNamespaceOperation)
	defer func() {
		metricsHandler.Timer(metrics.ServiceLatency.MetricName.String()).Record(time.Since(startTime))
	}()

	// validate request
	if request == nil {
		return nil, errRequestNotSet
	}

	if request.GetNamespace() == primitives.SystemLocalNamespace {
		return nil, errUnableDeleteSystemNamespace
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
		return nil, serviceerror.NewUnavailable(fmt.Sprintf(errUnableToStartWorkflowMessage, deletenamespace.WorkflowName, err))
	}

	// Wait for workflow to complete.
	var wfResult deletenamespace.DeleteNamespaceWorkflowResult
	err = run.Get(ctx, &wfResult)
	if err != nil {
		metricsHandler.Counter(metrics.DeleteNamespaceWorkflowFailuresCount.MetricName.String()).Record(1)
		execution := &commonpb.WorkflowExecution{WorkflowId: deletenamespace.WorkflowName, RunId: run.GetRunID()}
		return nil, serviceerror.NewSystemWorkflow(execution, err)
	}
	metricsHandler.Counter(metrics.DeleteNamespaceWorkflowSuccessCount.MetricName.String()).Record(1)

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
		NamespaceId:        namespaceID.String(),
		WorkflowExecution:  request.GetWorkflowExecution(),
		WorkflowVersion:    common.EmptyVersion,
		ClosedWorkflowOnly: false,
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
	return nil, serviceerror.NewUnimplemented("TODO: Need to get from another PR")
}

func (h *OperatorHandlerImpl) RemoveRemoteCluster(
	ctx context.Context,
	request *operatorservice.RemoveRemoteClusterRequest,
) (_ *operatorservice.RemoveRemoteClusterResponse, retError error) {
	return nil, serviceerror.NewUnimplemented("TODO: Need to get from another PR")
}

func (h *OperatorHandlerImpl) DescribeCluster(
	ctx context.Context,
	request *operatorservice.DescribeClusterRequest,
) (_ *operatorservice.DescribeClusterResponse, retError error) {
	return nil, serviceerror.NewUnimplemented("TODO: Need to get from another PR")
}

func (h *OperatorHandlerImpl) ListClusters(
	ctx context.Context,
	request *operatorservice.ListClustersRequest,
) (_ *operatorservice.ListClustersResponse, retError error) {
	return nil, serviceerror.NewUnimplemented("TODO: Need to get from another PR")
}

func (h *OperatorHandlerImpl) ListClusterMembers(
	ctx context.Context,
	request *operatorservice.ListClusterMembersRequest,
) (_ *operatorservice.ListClusterMembersResponse, retError error) {
	return nil, serviceerror.NewUnimplemented("TODO: Need to get from another PR")
}

// startRequestProfile initiates recording of request metrics
func (h *OperatorHandlerImpl) startRequestProfile(operation string) (metrics.Handler, time.Time) {
	metricsHandler := h.metricsHandler.WithTags(metrics.OperationTag(operation))
	metricsHandler.Counter(metrics.ServiceRequests.MetricName.String()).Record(1)
	return metricsHandler, time.Now()
}
