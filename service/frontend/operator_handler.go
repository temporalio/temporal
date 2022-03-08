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

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	sdkclient "go.temporal.io/sdk/client"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/service/worker"
	"go.temporal.io/server/service/worker/addsearchattributes"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/searchattribute"
)

var _ OperatorHandler = (*OperatorHandlerImpl)(nil)

type (
	// OperatorHandlerImpl - gRPC handler interface for operatorservice
	OperatorHandlerImpl struct {
		status int32

		healthStatus  int32
		logger        log.Logger
		esConfig      *esclient.Config
		esClient      esclient.Client
		sdkClient     sdkclient.Client
		metricsClient metrics.Client
		saProvider    searchattribute.Provider
		saManager     searchattribute.Manager
		healthServer  *health.Server
	}

	NewOperatorHandlerImplArgs struct {
		EsConfig        *esclient.Config
		EsClient        esclient.Client
		Logger          log.Logger
		SdkSystemClient sdkclient.Client
		MetricsClient   metrics.Client
		SaProvider      searchattribute.Provider
		SaManager       searchattribute.Manager
		healthServer    *health.Server
	}
)

// NewOperatorHandlerImpl creates a gRPC handler for operatorservice
func NewOperatorHandlerImpl(
	args NewOperatorHandlerImplArgs,
) *OperatorHandlerImpl {

	handler := &OperatorHandlerImpl{
		logger:        args.Logger,
		status:        common.DaemonStatusInitialized,
		esConfig:      args.EsConfig,
		esClient:      args.EsClient,
		sdkClient:     args.SdkSystemClient,
		metricsClient: args.MetricsClient,
		saProvider:    args.SaProvider,
		saManager:     args.SaManager,
		healthServer:  args.healthServer,
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
			return nil, h.error(serviceerror.NewInvalidArgument(fmt.Sprintf(errSearchAttributeAlreadyExistsMessage, saName)), scope, endpointName)
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

	run, err := h.sdkClient.ExecuteWorkflow(
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
			return nil, h.error(serviceerror.NewInvalidArgument(fmt.Sprintf(errSearchAttributeDoesntExistMessage, saName)), scope, endpointName)
		}
		if _, ok := newCustomSearchAttributes[saName]; !ok {
			return nil, h.error(serviceerror.NewInvalidArgument(fmt.Sprintf(errUnableToRemoveNonCustomSearchAttributesMessage, saName)), scope, endpointName)
		}
		delete(newCustomSearchAttributes, saName)
	}

	err = h.saManager.SaveSearchAttributes(indexName, newCustomSearchAttributes)
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

// startRequestProfile initiates recording of request metrics
func (h *OperatorHandlerImpl) startRequestProfile(scope int) (metrics.Scope, metrics.Stopwatch) {
	metricsScope := h.metricsClient.Scope(scope)
	sw := metricsScope.StartTimer(metrics.ServiceLatency)
	metricsScope.IncCounter(metrics.ServiceRequests)
	return metricsScope, sw
}

func (h *OperatorHandlerImpl) error(err error, scope metrics.Scope, endpointName string) error {
	switch err := err.(type) {
	case *serviceerror.Unavailable:
		h.logger.Error("["+endpointName+"] Unavailable error", tag.Error(err))
		scope.IncCounter(metrics.ServiceFailures)
		return err
	case *serviceerror.InvalidArgument:
		scope.IncCounter(metrics.ServiceErrInvalidArgumentCounter)
		return err
	case *serviceerror.ResourceExhausted:
		scope.Tagged(metrics.ResourceExhaustedCauseTag(err.Cause)).IncCounter(metrics.ServiceErrResourceExhaustedCounter)
		return err
	case *serviceerror.NotFound:
		return err
	}

	h.logger.Error("["+endpointName+"] Unknown error", tag.Error(err))
	scope.IncCounter(metrics.ServiceFailures)

	return err
}
