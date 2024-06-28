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
	"encoding/binary"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pborman/uuid"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	filterpb "go.temporal.io/api/filter/v1"
	historypb "go.temporal.io/api/history/v1"
	querypb "go.temporal.io/api/query/v1"
	schedpb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	schedspb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/client/frontend"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/enums"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/retrypolicy"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/timer"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/common/utf8validator"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/worker/batcher"
	"go.temporal.io/server/service/worker/scheduler"
)

var _ Handler = (*WorkflowHandler)(nil)

var (
	minTime = time.Unix(0, 0).UTC()
	maxTime = time.Date(2100, 1, 1, 1, 0, 0, 0, time.UTC)

	// Tail room for context deadline to bail out from retry for long poll.
	longPollTailRoom = time.Second

	errWaitForRefresh = serviceerror.NewDeadlineExceeded("waiting for schedule to refresh status of completed workflows")
)

type (
	// WorkflowHandler - gRPC handler interface for workflowservice
	WorkflowHandler struct {
		workflowservice.UnsafeWorkflowServiceServer
		status int32

		tokenSerializer                 common.TaskTokenSerializer
		config                          *Config
		versionChecker                  headers.VersionChecker
		namespaceHandler                *namespaceHandler
		getDefaultWorkflowRetrySettings dynamicconfig.TypedPropertyFnWithNamespaceFilter[retrypolicy.DefaultRetrySettings]
		visibilityMgr                   manager.VisibilityManager
		logger                          log.Logger
		throttledLogger                 log.Logger
		persistenceExecutionName        string
		clusterMetadataManager          persistence.ClusterMetadataManager
		historyClient                   historyservice.HistoryServiceClient
		matchingClient                  matchingservice.MatchingServiceClient
		archiverProvider                provider.ArchiverProvider
		payloadSerializer               serialization.Serializer
		namespaceRegistry               namespace.Registry
		saMapperProvider                searchattribute.MapperProvider
		saProvider                      searchattribute.Provider
		saValidator                     *searchattribute.Validator
		archivalMetadata                archiver.ArchivalMetadata
		healthServer                    *health.Server
		overrides                       *Overrides
		membershipMonitor               membership.Monitor
		healthInterceptor               *interceptor.HealthInterceptor
		scheduleSpecBuilder             *scheduler.SpecBuilder
	}
)

// NewWorkflowHandler creates a gRPC handler for workflowservice
func NewWorkflowHandler(
	config *Config,
	namespaceReplicationQueue persistence.NamespaceReplicationQueue,
	visibilityMgr manager.VisibilityManager,
	logger log.Logger,
	throttledLogger log.Logger,
	persistenceExecutionName string,
	clusterMetadataManager persistence.ClusterMetadataManager,
	persistenceMetadataManager persistence.MetadataManager,
	historyClient historyservice.HistoryServiceClient,
	matchingClient matchingservice.MatchingServiceClient,
	archiverProvider provider.ArchiverProvider,
	payloadSerializer serialization.Serializer,
	namespaceRegistry namespace.Registry,
	saMapperProvider searchattribute.MapperProvider,
	saProvider searchattribute.Provider,
	clusterMetadata cluster.Metadata,
	archivalMetadata archiver.ArchivalMetadata,
	healthServer *health.Server,
	timeSource clock.TimeSource,
	membershipMonitor membership.Monitor,
	healthInterceptor *interceptor.HealthInterceptor,
	scheduleSpecBuilder *scheduler.SpecBuilder,
) *WorkflowHandler {

	handler := &WorkflowHandler{
		status:          common.DaemonStatusInitialized,
		config:          config,
		tokenSerializer: common.NewProtoTaskTokenSerializer(),
		versionChecker:  headers.NewDefaultVersionChecker(),
		namespaceHandler: newNamespaceHandler(
			config.MaxBadBinaries,
			logger,
			persistenceMetadataManager,
			clusterMetadata,
			namespace.NewNamespaceReplicator(namespaceReplicationQueue, logger),
			archivalMetadata,
			archiverProvider,
			config.EnableSchedules,
			timeSource,
		),
		getDefaultWorkflowRetrySettings: config.DefaultWorkflowRetryPolicy,
		visibilityMgr:                   visibilityMgr,
		logger:                          logger,
		throttledLogger:                 throttledLogger,
		persistenceExecutionName:        persistenceExecutionName,
		clusterMetadataManager:          clusterMetadataManager,
		historyClient:                   historyClient,
		matchingClient:                  matchingClient,
		archiverProvider:                archiverProvider,
		payloadSerializer:               payloadSerializer,
		namespaceRegistry:               namespaceRegistry,
		saProvider:                      saProvider,
		saMapperProvider:                saMapperProvider,
		saValidator: searchattribute.NewValidator(
			saProvider,
			saMapperProvider,
			config.SearchAttributesNumberOfKeysLimit,
			config.SearchAttributesSizeOfValueLimit,
			config.SearchAttributesTotalSizeLimit,
			visibilityMgr,
			visibility.AllowListForValidation(
				visibilityMgr.GetStoreNames(),
				config.VisibilityAllowList,
			),
			config.SuppressErrorSetSystemSearchAttribute,
		),
		archivalMetadata:    archivalMetadata,
		healthServer:        healthServer,
		overrides:           NewOverrides(),
		membershipMonitor:   membershipMonitor,
		healthInterceptor:   healthInterceptor,
		scheduleSpecBuilder: scheduleSpecBuilder,
	}

	return handler
}

// Start starts the handler
func (wh *WorkflowHandler) Start() {
	if atomic.CompareAndSwapInt32(
		&wh.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		// Start in NOT_SERVING state and switch to SERVING after membership is ready
		wh.healthServer.SetServingStatus(WorkflowServiceName, healthpb.HealthCheckResponse_NOT_SERVING)
		go func() {
			_ = wh.membershipMonitor.WaitUntilInitialized(context.Background())
			wh.healthServer.SetServingStatus(WorkflowServiceName, healthpb.HealthCheckResponse_SERVING)
			wh.healthInterceptor.SetHealthy(true)
			wh.logger.Info("Frontend is now healthy")
		}()
	}
}

// Stop stops the handler
func (wh *WorkflowHandler) Stop() {
	if atomic.CompareAndSwapInt32(
		&wh.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		wh.healthServer.SetServingStatus(WorkflowServiceName, healthpb.HealthCheckResponse_NOT_SERVING)
		wh.healthInterceptor.SetHealthy(false)
	}
}

// GetConfig return config
func (wh *WorkflowHandler) GetConfig() *Config {
	return wh.config
}

// RegisterNamespace creates a new namespace which can be used as a container for all resources.  Namespace is a top level
// entity within Temporal, used as a container for all resources like workflow executions, task queues, etc.  Namespace
// acts as a sandbox and provides isolation for all resources within the namespace.  All resources belong to exactly one
// namespace.
func (wh *WorkflowHandler) RegisterNamespace(ctx context.Context, request *workflowservice.RegisterNamespaceRequest) (_ *workflowservice.RegisterNamespaceResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	if err := wh.validateNamespace(request.GetNamespace()); err != nil {
		return nil, err
	}

	resp, err := wh.namespaceHandler.RegisterNamespace(ctx, request)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// DescribeNamespace returns the information and configuration for a registered namespace.
func (wh *WorkflowHandler) DescribeNamespace(ctx context.Context, request *workflowservice.DescribeNamespaceRequest) (_ *workflowservice.DescribeNamespaceResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	resp, err := wh.namespaceHandler.DescribeNamespace(ctx, request)
	if err != nil {
		return resp, err
	}
	return resp, err
}

// ListNamespaces returns the information and configuration for all namespaces.
func (wh *WorkflowHandler) ListNamespaces(ctx context.Context, request *workflowservice.ListNamespacesRequest) (_ *workflowservice.ListNamespacesResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	resp, err := wh.namespaceHandler.ListNamespaces(ctx, request)
	if err != nil {
		return resp, err
	}
	return resp, err
}

// UpdateNamespace is used to update the information and configuration for a registered namespace.
func (wh *WorkflowHandler) UpdateNamespace(ctx context.Context, request *workflowservice.UpdateNamespaceRequest) (_ *workflowservice.UpdateNamespaceResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	resp, err := wh.namespaceHandler.UpdateNamespace(ctx, request)
	if err != nil {
		return resp, err
	}
	return resp, err
}

// DeprecateNamespace us used to update status of a registered namespace to DEPRECATED.  Once the namespace is deprecated
// it cannot be used to start new workflow executions.  Existing workflow executions will continue to run on
// deprecated namespaces.
// Deprecated.
func (wh *WorkflowHandler) DeprecateNamespace(ctx context.Context, request *workflowservice.DeprecateNamespaceRequest) (_ *workflowservice.DeprecateNamespaceResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	resp, err := wh.namespaceHandler.DeprecateNamespace(ctx, request)
	if err != nil {
		return nil, err
	}
	return resp, err
}

// StartWorkflowExecution starts a new long running workflow instance.  It will create the instance with
// 'WorkflowExecutionStarted' event in history and also schedule the first WorkflowTask for the worker to make the
// first workflow task for this instance.  It will return 'WorkflowExecutionAlreadyStartedError', if an instance already
// exists with same workflowId.
func (wh *WorkflowHandler) StartWorkflowExecution(
	ctx context.Context,
	request *workflowservice.StartWorkflowExecutionRequest,
) (_ *workflowservice.StartWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	var err error
	if request, err = wh.prepareStartWorkflowRequest(request); err != nil {
		return nil, err
	}

	wh.logger.Debug("Received StartWorkflowExecution.", tag.WorkflowID(request.GetWorkflowId()))

	namespaceName := namespace.Name(request.GetNamespace())

	wh.logger.Debug("Start workflow execution request namespace.", tag.WorkflowNamespace(namespaceName.String()))
	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespaceName)
	if err != nil {
		return nil, err
	}
	wh.logger.Debug("Start workflow execution request namespaceID.", tag.WorkflowNamespaceID(namespaceID.String()))

	resp, err := wh.historyClient.StartWorkflowExecution(
		ctx,
		common.CreateHistoryStartWorkflowRequest(
			namespaceID.String(),
			request,
			nil,
			nil,
			time.Now().UTC(),
		),
	)
	if err != nil {
		return nil, err
	}
	return &workflowservice.StartWorkflowExecutionResponse{
		RunId:             resp.GetRunId(),
		Started:           resp.Started,
		EagerWorkflowTask: resp.GetEagerWorkflowTask(),
	}, nil
}

// Validates the request and sets default values where they are missing.
func (wh *WorkflowHandler) prepareStartWorkflowRequest(
	request *workflowservice.StartWorkflowExecutionRequest,
) (*workflowservice.StartWorkflowExecutionRequest, error) {
	if request == nil {
		return nil, errRequestNotSet
	}

	if err := wh.validateWorkflowID(request.GetWorkflowId()); err != nil {
		return nil, err
	}

	namespaceName := namespace.Name(request.GetNamespace())
	if err := wh.validateRetryPolicy(namespaceName, request.RetryPolicy); err != nil {
		return nil, err
	}

	if err := wh.validateWorkflowStartDelay(request.GetCronSchedule(), request.WorkflowStartDelay); err != nil {
		return nil, err
	}

	if err := backoff.ValidateSchedule(request.GetCronSchedule()); err != nil {
		return nil, err
	}

	if request.WorkflowType == nil || request.WorkflowType.GetName() == "" {
		return nil, errWorkflowTypeNotSet
	}

	if len(request.WorkflowType.GetName()) > wh.config.MaxIDLengthLimit() {
		return nil, errWorkflowTypeTooLong
	}

	if err := common.ValidateUTF8String("WorkflowType", request.WorkflowType.GetName()); err != nil {
		return nil, err
	}

	if err := wh.validateTaskQueue(request.TaskQueue); err != nil {
		return nil, err
	}

	if err := wh.validateStartWorkflowTimeouts(request); err != nil {
		return nil, err
	}

	if err := validateRequestId(&request.RequestId, wh.config.MaxIDLengthLimit()); err != nil {
		return nil, err
	}

	if err := wh.validateWorkflowIdReusePolicy(request.WorkflowIdReusePolicy, request.WorkflowIdConflictPolicy); err != nil {
		return nil, err
	}

	enums.SetDefaultWorkflowIdReusePolicy(&request.WorkflowIdReusePolicy)
	enums.SetDefaultWorkflowIdConflictPolicy(&request.WorkflowIdConflictPolicy, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL)

	sa, err := wh.unaliasedSearchAttributesFrom(request.GetSearchAttributes(), namespaceName)
	if err != nil {
		return nil, err
	}
	if sa != request.SearchAttributes {
		// cloning here so in case of retry the field is set to the current search attributes
		request = common.CloneProto(request)
		request.SearchAttributes = sa
	}

	if err := wh.validateWorkflowCompletionCallbacks(namespaceName, request.GetCompletionCallbacks()); err != nil {
		return nil, err
	}

	return request, nil
}

func (wh *WorkflowHandler) unaliasedSearchAttributesFrom(
	attributes *commonpb.SearchAttributes,
	namespaceName namespace.Name,
) (*commonpb.SearchAttributes, error) {
	sa, err := searchattribute.UnaliasFields(wh.saMapperProvider, attributes, namespaceName.String())
	if err != nil {
		return nil, err
	}

	if err = wh.validateSearchAttributes(sa, namespaceName); err != nil {
		return nil, err
	}
	return sa, nil
}

func (wh *WorkflowHandler) ExecuteMultiOperation(
	ctx context.Context,
	request *workflowservice.ExecuteMultiOperationRequest,
) (_ *workflowservice.ExecuteMultiOperationResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	namespaceName := namespace.Name(request.Namespace)
	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespaceName)
	if err != nil {
		return nil, err
	}

	if !wh.config.EnableExecuteMultiOperation(request.Namespace) {
		return nil, errMultiOperationAPINotAllowed
	}

	// as a temporary limitation, the only allowed list of operations is exactly [Start, Update]
	if len(request.Operations) != 2 {
		return nil, errMultiOpNotStartAndUpdate
	}
	if request.Operations[0].GetStartWorkflow() == nil {
		return nil, errMultiOpNotStartAndUpdate
	}
	if request.Operations[1].GetUpdateWorkflow() == nil {
		return nil, errMultiOpNotStartAndUpdate
	}

	historyReq, err := wh.convertToHistoryMultiOperationRequest(namespaceID, request)
	if err != nil {
		return nil, err
	}

	historyResp, err := wh.historyClient.ExecuteMultiOperation(ctx, historyReq)
	if err != nil {
		return nil, err
	}

	response, err := convertToMultiOperationResponse(historyResp)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (wh *WorkflowHandler) convertToHistoryMultiOperationRequest(
	namespaceID namespace.ID,
	request *workflowservice.ExecuteMultiOperationRequest,
) (*historyservice.ExecuteMultiOperationRequest, error) {
	var lastWorkflowID string
	ops := make([]*historyservice.ExecuteMultiOperationRequest_Operation, len(request.Operations))

	var hasError bool
	errs := make([]error, len(request.Operations))

	for i, op := range request.Operations {
		convertedOp, opWorkflowID, err := wh.convertToHistoryMultiOperationItem(namespaceID, op)
		if err != nil {
			hasError = true
		} else {
			// set to default in case the whole MultOp request
			err = errMultiOpAborted

			switch {
			case lastWorkflowID == "":
				lastWorkflowID = opWorkflowID
			case lastWorkflowID != opWorkflowID:
				err = errMultiOpWorkflowIdInconsistent
				hasError = true
			}
		}
		errs[i] = err
		ops[i] = convertedOp
	}

	if hasError {
		return nil, serviceerror.NewMultiOperationExecution("MultiOperation could not be executed.", errs)
	}

	return &historyservice.ExecuteMultiOperationRequest{
		NamespaceId: namespaceID.String(),
		WorkflowId:  lastWorkflowID,
		Operations:  ops,
	}, nil
}

func (wh *WorkflowHandler) convertToHistoryMultiOperationItem(
	namespaceID namespace.ID,
	op *workflowservice.ExecuteMultiOperationRequest_Operation,
) (*historyservice.ExecuteMultiOperationRequest_Operation, string, error) {
	var workflowId string
	var opReq *historyservice.ExecuteMultiOperationRequest_Operation

	if startReq := op.GetStartWorkflow(); startReq != nil {
		var err error
		if startReq, err = wh.prepareStartWorkflowRequest(startReq); err != nil {
			return nil, "", err
		}
		if len(startReq.CronSchedule) > 0 {
			return nil, "", errMultiOpStartCronSchedule
		}
		if startReq.RequestEagerExecution {
			return nil, "", errMultiOpEagerWorkflow
		}

		workflowId = startReq.WorkflowId
		opReq = &historyservice.ExecuteMultiOperationRequest_Operation{
			Operation: &historyservice.ExecuteMultiOperationRequest_Operation_StartWorkflow{
				StartWorkflow: common.CreateHistoryStartWorkflowRequest(
					namespaceID.String(),
					startReq,
					nil,
					nil,
					time.Now().UTC(),
				),
			},
		}
	} else if updateReq := op.GetUpdateWorkflow(); updateReq != nil {
		if err := wh.prepareUpdateWorkflowRequest(updateReq); err != nil {
			return nil, "", err
		}

		workflowId = updateReq.WorkflowExecution.WorkflowId
		opReq = &historyservice.ExecuteMultiOperationRequest_Operation{
			Operation: &historyservice.ExecuteMultiOperationRequest_Operation_UpdateWorkflow{
				UpdateWorkflow: &historyservice.UpdateWorkflowExecutionRequest{
					NamespaceId: namespaceID.String(),
					Request:     updateReq,
				},
			},
		}
	} else {
		return nil, "", serviceerror.NewInternal(fmt.Sprintf("unsupported operation: %T", op.Operation))
	}

	return opReq, workflowId, nil
}

func convertToMultiOperationResponse(
	historyResp *historyservice.ExecuteMultiOperationResponse,
) (*workflowservice.ExecuteMultiOperationResponse, error) {
	resp := &workflowservice.ExecuteMultiOperationResponse{
		Responses: make([]*workflowservice.ExecuteMultiOperationResponse_Response, len(historyResp.Responses)),
	}
	for i, op := range historyResp.Responses {
		var opResp *workflowservice.ExecuteMultiOperationResponse_Response
		if startResp := op.GetStartWorkflow(); startResp != nil {
			opResp = &workflowservice.ExecuteMultiOperationResponse_Response{
				Response: &workflowservice.ExecuteMultiOperationResponse_Response_StartWorkflow{
					StartWorkflow: &workflowservice.StartWorkflowExecutionResponse{
						RunId: startResp.RunId,
					},
				},
			}
		} else if updateResp := op.GetUpdateWorkflow(); updateResp != nil {
			opResp = &workflowservice.ExecuteMultiOperationResponse_Response{
				Response: &workflowservice.ExecuteMultiOperationResponse_Response_UpdateWorkflow{
					UpdateWorkflow: &workflowservice.UpdateWorkflowExecutionResponse{
						UpdateRef: updateResp.Response.UpdateRef,
						Outcome:   updateResp.Response.Outcome,
						Stage:     updateResp.Response.Stage,
					},
				},
			}
		} else {
			return nil, serviceerror.NewInternal(fmt.Sprintf("unexpected operation result: %T", op.Response))
		}
		resp.Responses[i] = opResp
	}
	return resp, nil
}

// GetWorkflowExecutionHistory returns the history of specified workflow execution.  It fails with 'EntityNotExistError' if specified workflow
// execution in unknown to the service.
func (wh *WorkflowHandler) GetWorkflowExecutionHistory(ctx context.Context, request *workflowservice.GetWorkflowExecutionHistoryRequest) (_ *workflowservice.GetWorkflowExecutionHistoryResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	if err := validateExecution(request.Execution); err != nil {
		return nil, err
	}

	if request.GetMaximumPageSize() <= 0 {
		request.MaximumPageSize = int32(wh.config.HistoryMaxPageSize(request.GetNamespace()))
	}

	enums.SetDefaultHistoryEventFilterType(&request.HistoryEventFilterType)

	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	// force limit page size if exceed
	if request.GetMaximumPageSize() > primitives.GetHistoryMaxPageSize {
		wh.throttledLogger.Warn("GetHistory page size is larger than threshold",
			tag.WorkflowID(request.Execution.GetWorkflowId()),
			tag.WorkflowRunID(request.Execution.GetRunId()),
			tag.WorkflowNamespaceID(namespaceID.String()), tag.WorkflowSize(int64(request.GetMaximumPageSize())))
		request.MaximumPageSize = primitives.GetHistoryMaxPageSize
	}

	if !request.GetSkipArchival() {
		enableArchivalRead := wh.archivalMetadata.GetHistoryConfig().ReadEnabled()
		historyArchived := wh.historyArchived(ctx, request, namespaceID)
		if enableArchivalRead && historyArchived {
			return wh.getArchivedHistory(ctx, request, namespaceID)
		}
	}

	response, err := wh.historyClient.GetWorkflowExecutionHistory(ctx,
		&historyservice.GetWorkflowExecutionHistoryRequest{
			NamespaceId: namespaceID.String(),
			Request:     request,
		})
	if err != nil {
		return nil, err
	}
	return response.Response, nil
}

// GetWorkflowExecutionHistory returns the history of specified workflow execution.  It fails with 'EntityNotExistError' if specified workflow
// execution in unknown to the service.
func (wh *WorkflowHandler) GetWorkflowExecutionHistoryReverse(ctx context.Context, request *workflowservice.GetWorkflowExecutionHistoryReverseRequest) (_ *workflowservice.GetWorkflowExecutionHistoryReverseResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	if err := validateExecution(request.Execution); err != nil {
		return nil, err
	}

	if request.GetMaximumPageSize() <= 0 {
		request.MaximumPageSize = int32(wh.config.HistoryMaxPageSize(request.GetNamespace()))
	}

	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	// force limit page size if exceed
	if request.GetMaximumPageSize() > primitives.GetHistoryMaxPageSize {
		wh.throttledLogger.Warn("GetHistory page size is larger than threshold",
			tag.WorkflowID(request.Execution.GetWorkflowId()),
			tag.WorkflowRunID(request.Execution.GetRunId()),
			tag.WorkflowNamespaceID(namespaceID.String()), tag.WorkflowSize(int64(request.GetMaximumPageSize())))
		request.MaximumPageSize = primitives.GetHistoryMaxPageSize
	}

	response, err := wh.historyClient.GetWorkflowExecutionHistoryReverse(ctx,
		&historyservice.GetWorkflowExecutionHistoryReverseRequest{
			NamespaceId: namespaceID.String(),
			Request:     request,
		})
	if err != nil {
		return nil, err
	}
	return response.Response, nil
}

// PollWorkflowTaskQueue is called by application worker to process WorkflowTask from a specific task queue.  A
// WorkflowTask is dispatched to callers for active workflow executions, with pending workflow tasks.
// Application is then expected to call 'RespondWorkflowTaskCompleted' API when it is done processing the WorkflowTask.
// It will also create a 'WorkflowTaskStarted' event in the history for that session before handing off WorkflowTask to
// application worker.
func (wh *WorkflowHandler) PollWorkflowTaskQueue(ctx context.Context, request *workflowservice.PollWorkflowTaskQueueRequest) (_ *workflowservice.PollWorkflowTaskQueueResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)
	if request == nil {
		return nil, errRequestNotSet
	}

	wh.logger.Debug("Received PollWorkflowTaskQueue")
	if err := common.ValidateLongPollContextTimeout(
		ctx,
		"PollWorkflowTaskQueue",
		wh.throttledLogger,
	); err != nil {
		return nil, err
	}

	if len(request.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return nil, errIdentityTooLong
	}

	if err := wh.validateVersioningInfo(request.Namespace, request.WorkerVersionCapabilities, request.TaskQueue); err != nil {
		return nil, err
	}

	if request.TaskQueue.GetKind() == enumspb.TASK_QUEUE_KIND_UNSPECIFIED {
		wh.logger.Warn("Unspecified task queue kind",
			tag.WorkflowTaskQueueName(request.TaskQueue.GetName()),
			tag.WorkflowNamespace(namespace.Name(request.GetNamespace()).String()),
		)
	}

	if err := wh.validateTaskQueue(request.TaskQueue); err != nil {
		return nil, err
	}

	callTime := time.Now().UTC()

	namespaceEntry, err := wh.namespaceRegistry.GetNamespace(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}
	namespaceID := namespaceEntry.ID()

	wh.logger.Debug("Poll workflow task queue.", tag.WorkflowNamespace(namespaceEntry.Name().String()), tag.WorkflowNamespaceID(namespaceID.String()))
	if err := wh.checkBadBinary(namespaceEntry, request.GetBinaryChecksum()); err != nil {
		return nil, err
	}

	if contextNearDeadline(ctx, longPollTailRoom) {
		return &workflowservice.PollWorkflowTaskQueueResponse{}, nil
	}

	pollerID := uuid.New()
	matchingResp, err := wh.matchingClient.PollWorkflowTaskQueue(ctx, &matchingservice.PollWorkflowTaskQueueRequest{
		NamespaceId: namespaceID.String(),
		PollerId:    pollerID,
		PollRequest: request,
	})
	if err != nil {
		contextWasCanceled := wh.cancelOutstandingPoll(ctx, namespaceID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, request.TaskQueue, pollerID)
		if contextWasCanceled {
			// Clear error as we don't want to report context cancellation error to count against our SLA.
			// It doesn't matter what to return here, client has already gone. But (nil,nil) is invalid gogo return pair.
			return &workflowservice.PollWorkflowTaskQueueResponse{}, nil
		}

		// These errors are expected from some versioning situations. We should not log them, it'd be too noisy.
		var newerBuild *serviceerror.NewerBuildExists      // expected when versioned poller is superceded
		var failedPrecond *serviceerror.FailedPrecondition // expected when user data is disabled
		if errors.As(err, &newerBuild) || errors.As(err, &failedPrecond) {
			return nil, err
		}

		// For all other errors log an error and return it back to client.
		ctxTimeout := "not-set"
		ctxDeadline, ok := ctx.Deadline()
		if ok {
			ctxTimeout = ctxDeadline.Sub(callTime).String()
		}
		wh.logger.Error("Unable to call matching.PollWorkflowTaskQueue.",
			tag.WorkflowTaskQueueName(request.GetTaskQueue().GetName()),
			tag.Timeout(ctxTimeout),
			tag.Error(err))
		return nil, err
	}

	return &workflowservice.PollWorkflowTaskQueueResponse{
		TaskToken:                  matchingResp.TaskToken,
		WorkflowExecution:          matchingResp.WorkflowExecution,
		WorkflowType:               matchingResp.WorkflowType,
		PreviousStartedEventId:     matchingResp.PreviousStartedEventId,
		StartedEventId:             matchingResp.StartedEventId,
		Query:                      matchingResp.Query,
		BacklogCountHint:           matchingResp.BacklogCountHint,
		Attempt:                    matchingResp.Attempt,
		History:                    matchingResp.History,
		NextPageToken:              matchingResp.NextPageToken,
		WorkflowExecutionTaskQueue: matchingResp.WorkflowExecutionTaskQueue,
		ScheduledTime:              matchingResp.ScheduledTime,
		StartedTime:                matchingResp.StartedTime,
		Queries:                    matchingResp.Queries,
		Messages:                   matchingResp.Messages,
	}, nil
}

func contextNearDeadline(ctx context.Context, tailroom time.Duration) bool {
	if ctxDeadline, ok := ctx.Deadline(); ok {
		return time.Now().Add(tailroom).After(ctxDeadline)
	}
	return false
}

// RespondWorkflowTaskCompleted is called by application worker to complete a WorkflowTask handed as a result of
// 'PollWorkflowTaskQueue' API call.  Completing a WorkflowTask will result in new events for the workflow execution and
// potentially new ActivityTask being created for corresponding commands.  It will also create a WorkflowTaskCompleted
// event in the history for that session.  Use the 'taskToken' provided as response of PollWorkflowTaskQueue API call
// for completing the WorkflowTask.
// The response could contain a new workflow task if there is one or if the request asking for one.
func (wh *WorkflowHandler) RespondWorkflowTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondWorkflowTaskCompletedRequest,
) (_ *workflowservice.RespondWorkflowTaskCompletedResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	if len(request.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return nil, errIdentityTooLong
	}

	if err := wh.validateVersioningInfo(
		request.Namespace,
		request.WorkerVersionStamp,
		request.StickyAttributes.GetWorkerTaskQueue(),
	); err != nil {
		return nil, err
	}

	wh.overrides.DisableEagerActivityDispatchForBuggyClients(ctx, request)

	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	response, err := wh.historyClient.RespondWorkflowTaskCompleted(ctx,
		&historyservice.RespondWorkflowTaskCompletedRequest{
			NamespaceId:     namespaceID.String(),
			CompleteRequest: request,
		},
	)
	if err != nil {
		return nil, err
	}

	return &workflowservice.RespondWorkflowTaskCompletedResponse{
		WorkflowTask:        response.NewWorkflowTask,
		ActivityTasks:       response.ActivityTasks,
		ResetHistoryEventId: response.ResetHistoryEventId,
	}, nil
}

// RespondWorkflowTaskFailed is called by application worker to indicate failure.  This results in
// WorkflowTaskFailedEvent written to the history and a new WorkflowTask created.  This API can be used by client to
// either clear sticky taskqueue or report any panics during WorkflowTask processing.  Temporal will only append first
// WorkflowTaskFailed event to the history of workflow execution for consecutive failures.
func (wh *WorkflowHandler) RespondWorkflowTaskFailed(
	ctx context.Context,
	request *workflowservice.RespondWorkflowTaskFailedRequest,
) (_ *workflowservice.RespondWorkflowTaskFailedResponse, retError error) {

	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	taskToken, err := wh.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return nil, err
	}
	namespaceId := namespace.ID(taskToken.GetNamespaceId())
	namespaceEntry, err := wh.namespaceRegistry.GetNamespaceByID(namespaceId)
	if err != nil {
		return nil, err
	}

	if len(request.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return nil, errIdentityTooLong
	}

	sizeLimitError := wh.config.BlobSizeLimitError(namespaceEntry.Name().String())
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(namespaceEntry.Name().String())

	if err := common.CheckEventBlobSizeLimit(
		request.GetFailure().Size(),
		sizeLimitWarn,
		sizeLimitError,
		namespaceId.String(),
		taskToken.GetWorkflowId(),
		taskToken.GetRunId(),
		wh.metricsScope(ctx).WithTags(metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UNSPECIFIED.String())),
		wh.throttledLogger,
		tag.BlobSizeViolationOperation("RespondWorkflowTaskFailed"),
	); err != nil {
		serverFailure := failure.NewServerFailure(common.FailureReasonFailureExceedsLimit, true)
		serverFailure.Cause = failure.Truncate(request.Failure, sizeLimitWarn)
		request.Failure = serverFailure
	}

	if request.GetCause() == enumspb.WORKFLOW_TASK_FAILED_CAUSE_NON_DETERMINISTIC_ERROR {
		wh.logger.Info("Non-Deterministic Error",
			tag.WorkflowNamespaceID(taskToken.GetNamespaceId()),
			tag.WorkflowID(taskToken.GetWorkflowId()),
			tag.WorkflowRunID(taskToken.GetRunId()),
		)
		metrics.ServiceErrNonDeterministicCounter.With(wh.metricsScope(ctx)).Record(1)
	}

	_, err = wh.historyClient.RespondWorkflowTaskFailed(ctx, &historyservice.RespondWorkflowTaskFailedRequest{
		NamespaceId:   namespaceId.String(),
		FailedRequest: request,
	})
	if err != nil {
		return nil, err
	}

	return &workflowservice.RespondWorkflowTaskFailedResponse{}, nil
}

// PollActivityTaskQueue is called by application worker to process ActivityTask from a specific task queue.  ActivityTask
// is dispatched to callers whenever a ScheduleTask command is made for a workflow execution.
// Application is expected to call 'RespondActivityTaskCompleted' or 'RespondActivityTaskFailed' once it is done
// processing the task.
// Application also needs to call 'RecordActivityTaskHeartbeat' API within 'heartbeatTimeoutSeconds' interval to
// prevent the task from getting timed out.  An event 'ActivityTaskStarted' event is also written to workflow execution
// history before the ActivityTask is dispatched to application worker.
func (wh *WorkflowHandler) PollActivityTaskQueue(ctx context.Context, request *workflowservice.PollActivityTaskQueueRequest) (_ *workflowservice.PollActivityTaskQueueResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	callTime := time.Now().UTC()

	if request == nil {
		return nil, errRequestNotSet
	}

	wh.logger.Debug("Received PollActivityTaskQueue")
	if err := common.ValidateLongPollContextTimeout(
		ctx,
		"PollActivityTaskQueue",
		wh.throttledLogger,
	); err != nil {
		return nil, err
	}

	namespaceName := namespace.Name(request.GetNamespace())
	if err := wh.validateTaskQueue(request.TaskQueue); err != nil {
		return nil, err
	}
	if len(request.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return nil, errIdentityTooLong
	}

	if err := wh.validateVersioningInfo(request.Namespace, request.WorkerVersionCapabilities, request.TaskQueue); err != nil {
		return nil, err
	}

	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespaceName)
	if err != nil {
		return nil, err
	}

	if contextNearDeadline(ctx, longPollTailRoom) {
		return &workflowservice.PollActivityTaskQueueResponse{}, nil
	}

	pollerID := uuid.New()
	matchingResponse, err := wh.matchingClient.PollActivityTaskQueue(ctx, &matchingservice.PollActivityTaskQueueRequest{
		NamespaceId: namespaceID.String(),
		PollerId:    pollerID,
		PollRequest: request,
	})
	if err != nil {
		contextWasCanceled := wh.cancelOutstandingPoll(ctx, namespaceID, enumspb.TASK_QUEUE_TYPE_ACTIVITY, request.TaskQueue, pollerID)
		if contextWasCanceled {
			// Clear error as we don't want to report context cancellation error to count against our SLA.
			// It doesn't matter what to return here, client has already gone. But (nil,nil) is invalid gogo return pair.
			return &workflowservice.PollActivityTaskQueueResponse{}, nil
		}

		// These errors are expected from some versioning situations. We should not log them, it'd be too noisy.
		var newerBuild *serviceerror.NewerBuildExists      // expected when versioned poller is superceded
		var failedPrecond *serviceerror.FailedPrecondition // expected when user data is disabled
		if errors.As(err, &newerBuild) || errors.As(err, &failedPrecond) {
			return nil, err
		}

		// For all other errors log an error and return it back to client.
		ctxTimeout := "not-set"
		ctxDeadline, ok := ctx.Deadline()
		if ok {
			ctxTimeout = ctxDeadline.Sub(callTime).String()
		}
		wh.logger.Error("Unable to call matching.PollActivityTaskQueue.",
			tag.WorkflowTaskQueueName(request.GetTaskQueue().GetName()),
			tag.Timeout(ctxTimeout),
			tag.Error(err))

		return nil, err
	}

	return &workflowservice.PollActivityTaskQueueResponse{
		TaskToken:                   matchingResponse.TaskToken,
		WorkflowExecution:           matchingResponse.WorkflowExecution,
		ActivityId:                  matchingResponse.ActivityId,
		ActivityType:                matchingResponse.ActivityType,
		Input:                       matchingResponse.Input,
		ScheduledTime:               matchingResponse.ScheduledTime,
		ScheduleToCloseTimeout:      matchingResponse.ScheduleToCloseTimeout,
		StartedTime:                 matchingResponse.StartedTime,
		StartToCloseTimeout:         matchingResponse.StartToCloseTimeout,
		HeartbeatTimeout:            matchingResponse.HeartbeatTimeout,
		Attempt:                     matchingResponse.Attempt,
		CurrentAttemptScheduledTime: matchingResponse.CurrentAttemptScheduledTime,
		HeartbeatDetails:            matchingResponse.HeartbeatDetails,
		WorkflowType:                matchingResponse.WorkflowType,
		WorkflowNamespace:           matchingResponse.WorkflowNamespace,
		Header:                      matchingResponse.Header,
	}, nil
}

// RecordActivityTaskHeartbeat is called by application worker while it is processing an ActivityTask.  If worker fails
// to heartbeat within 'heartbeatTimeoutSeconds' interval for the ActivityTask, then it will be marked as timedout and
// 'ActivityTaskTimedOut' event will be written to the workflow history.  Calling 'RecordActivityTaskHeartbeat' will
// fail with 'EntityNotExistsError' in such situations.  Use the 'taskToken' provided as response of
// PollActivityTaskQueue API call for heartbeating.
func (wh *WorkflowHandler) RecordActivityTaskHeartbeat(ctx context.Context, request *workflowservice.RecordActivityTaskHeartbeatRequest) (_ *workflowservice.RecordActivityTaskHeartbeatResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	wh.logger.Debug("Received RecordActivityTaskHeartbeat")
	taskToken, err := wh.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return nil, err
	}
	namespaceId := namespace.ID(taskToken.GetNamespaceId())
	namespaceEntry, err := wh.namespaceRegistry.GetNamespaceByID(namespaceId)
	if err != nil {
		return nil, err
	}

	sizeLimitError := wh.config.BlobSizeLimitError(namespaceEntry.Name().String())
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(namespaceEntry.Name().String())

	if err := common.CheckEventBlobSizeLimit(
		request.GetDetails().Size(),
		sizeLimitWarn,
		sizeLimitError,
		namespaceId.String(),
		taskToken.GetWorkflowId(),
		taskToken.GetRunId(),
		wh.metricsScope(ctx).WithTags(metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UNSPECIFIED.String())),
		wh.throttledLogger,
		tag.BlobSizeViolationOperation("RecordActivityTaskHeartbeat"),
	); err != nil {
		// heartbeat details exceed size limit, we would fail the activity immediately with explicit error reason
		failRequest := &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: request.TaskToken,
			Failure:   failure.NewServerFailure(common.FailureReasonHeartbeatExceedsLimit, true),
			Identity:  request.Identity,
		}
		_, err = wh.historyClient.RespondActivityTaskFailed(ctx, &historyservice.RespondActivityTaskFailedRequest{
			NamespaceId:   namespaceId.String(),
			FailedRequest: failRequest,
		})
		if err != nil {
			return nil, err
		}
		return &workflowservice.RecordActivityTaskHeartbeatResponse{CancelRequested: true}, nil
	}

	resp, err := wh.historyClient.RecordActivityTaskHeartbeat(ctx, &historyservice.RecordActivityTaskHeartbeatRequest{
		NamespaceId:      namespaceId.String(),
		HeartbeatRequest: request,
	})
	if err != nil {
		return nil, err
	}

	return &workflowservice.RecordActivityTaskHeartbeatResponse{CancelRequested: resp.GetCancelRequested()}, nil
}

// RecordActivityTaskHeartbeatById is called by application worker while it is processing an ActivityTask.  If worker fails
// to heartbeat within 'heartbeatTimeoutSeconds' interval for the ActivityTask, then it will be marked as timedout and
// 'ActivityTaskTimedOut' event will be written to the workflow history.  Calling 'RecordActivityTaskHeartbeatById' will
// fail with 'EntityNotExistsError' in such situations.  Instead of using 'taskToken' like in RecordActivityTaskHeartbeat,
// use Namespace, WorkflowID and ActivityID
func (wh *WorkflowHandler) RecordActivityTaskHeartbeatById(ctx context.Context, request *workflowservice.RecordActivityTaskHeartbeatByIdRequest) (_ *workflowservice.RecordActivityTaskHeartbeatByIdResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	wh.logger.Debug("Received RecordActivityTaskHeartbeatById")
	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}
	workflowID := request.GetWorkflowId()
	runID := request.GetRunId() // runID is optional so can be empty
	activityID := request.GetActivityId()

	if workflowID == "" {
		return nil, errWorkflowIDNotSet
	}
	if activityID == "" {
		return nil, errActivityIDNotSet
	}

	taskToken := tasktoken.NewActivityTaskToken(
		namespaceID.String(),
		workflowID,
		runID,
		common.EmptyEventID,
		activityID,
		"",
		1,
		nil,
		common.EmptyVersion,
	)
	token, err := wh.tokenSerializer.Serialize(taskToken)
	if err != nil {
		return nil, err
	}

	namespaceEntry, err := wh.namespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, err
	}

	sizeLimitError := wh.config.BlobSizeLimitError(namespaceEntry.Name().String())
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(namespaceEntry.Name().String())

	if err := common.CheckEventBlobSizeLimit(
		request.GetDetails().Size(),
		sizeLimitWarn,
		sizeLimitError,
		namespaceID.String(),
		taskToken.GetWorkflowId(),
		taskToken.GetRunId(),
		wh.metricsScope(ctx).WithTags(metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UNSPECIFIED.String())),
		wh.throttledLogger,
		tag.BlobSizeViolationOperation("RecordActivityTaskHeartbeatById"),
	); err != nil {
		// heartbeat details exceed size limit, we would fail the activity immediately with explicit error reason
		failRequest := &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: token,
			Failure:   failure.NewServerFailure(common.FailureReasonHeartbeatExceedsLimit, true),
			Identity:  request.Identity,
		}
		_, err = wh.historyClient.RespondActivityTaskFailed(ctx, &historyservice.RespondActivityTaskFailedRequest{
			NamespaceId:   namespaceID.String(),
			FailedRequest: failRequest,
		})
		if err != nil {
			return nil, err
		}
		return &workflowservice.RecordActivityTaskHeartbeatByIdResponse{CancelRequested: true}, nil
	}

	req := &workflowservice.RecordActivityTaskHeartbeatRequest{
		TaskToken: token,
		Details:   request.Details,
		Identity:  request.Identity,
	}

	resp, err := wh.historyClient.RecordActivityTaskHeartbeat(ctx, &historyservice.RecordActivityTaskHeartbeatRequest{
		NamespaceId:      namespaceID.String(),
		HeartbeatRequest: req,
	})
	if err != nil {
		return nil, err
	}
	return &workflowservice.RecordActivityTaskHeartbeatByIdResponse{CancelRequested: resp.GetCancelRequested()}, nil
}

// RespondActivityTaskCompleted is called by application worker when it is done processing an ActivityTask.  It will
// result in a new 'ActivityTaskCompleted' event being written to the workflow history and a new WorkflowTask
// created for the workflow so new commands could be made.  Use the 'taskToken' provided as response of
// PollActivityTaskQueue API call for completion. It fails with 'NotFoundFailure' if the taskToken is not valid
// anymore due to activity timeout.
func (wh *WorkflowHandler) RespondActivityTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskCompletedRequest,
) (_ *workflowservice.RespondActivityTaskCompletedResponse, retError error) {

	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}
	taskToken, err := wh.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return nil, err
	}
	namespaceId := namespace.ID(taskToken.GetNamespaceId())
	namespaceEntry, err := wh.namespaceRegistry.GetNamespaceByID(namespaceId)
	if err != nil {
		return nil, err
	}

	if len(request.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return nil, errIdentityTooLong
	}

	sizeLimitError := wh.config.BlobSizeLimitError(namespaceEntry.Name().String())
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(namespaceEntry.Name().String())

	if err := common.CheckEventBlobSizeLimit(
		request.GetResult().Size(),
		sizeLimitWarn,
		sizeLimitError,
		namespaceId.String(),
		taskToken.GetWorkflowId(),
		taskToken.GetRunId(),
		wh.metricsScope(ctx).WithTags(metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UNSPECIFIED.String())),
		wh.throttledLogger,
		tag.BlobSizeViolationOperation("RespondActivityTaskCompleted"),
	); err != nil {
		// result exceeds blob size limit, we would record it as failure
		failRequest := &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: request.TaskToken,
			Failure:   failure.NewServerFailure(common.FailureReasonCompleteResultExceedsLimit, true),
			Identity:  request.Identity,
		}
		_, err = wh.historyClient.RespondActivityTaskFailed(ctx, &historyservice.RespondActivityTaskFailedRequest{
			NamespaceId:   namespaceId.String(),
			FailedRequest: failRequest,
		})
		if err != nil {
			return nil, err
		}
	} else {
		_, err = wh.historyClient.RespondActivityTaskCompleted(ctx, &historyservice.RespondActivityTaskCompletedRequest{
			NamespaceId:     namespaceId.String(),
			CompleteRequest: request,
		})
		if err != nil {
			return nil, err
		}
	}

	return &workflowservice.RespondActivityTaskCompletedResponse{}, nil
}

// RespondActivityTaskCompletedById is called by application worker when it is done processing an ActivityTask.
// It will result in a new 'ActivityTaskCompleted' event being written to the workflow history and a new WorkflowTask
// created for the workflow so new commands could be made.  Similar to RespondActivityTaskCompleted but use Namespace,
// WorkflowId and ActivityId instead of 'taskToken' for completion. It fails with 'NotFoundFailure'
// if the these Ids are not valid anymore due to activity timeout.
func (wh *WorkflowHandler) RespondActivityTaskCompletedById(ctx context.Context, request *workflowservice.RespondActivityTaskCompletedByIdRequest) (_ *workflowservice.RespondActivityTaskCompletedByIdResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}
	workflowID := request.GetWorkflowId()
	runID := request.GetRunId() // runID is optional so can be empty
	activityID := request.GetActivityId()

	if workflowID == "" {
		return nil, errWorkflowIDNotSet
	}
	if activityID == "" {
		return nil, errActivityIDNotSet
	}

	if len(request.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return nil, errIdentityTooLong
	}

	taskToken := tasktoken.NewActivityTaskToken(
		namespaceID.String(),
		workflowID,
		runID,
		common.EmptyEventID,
		activityID,
		"",
		1,
		nil,
		common.EmptyVersion,
	)
	token, err := wh.tokenSerializer.Serialize(taskToken)
	if err != nil {
		return nil, err
	}

	namespaceEntry, err := wh.namespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, err
	}

	sizeLimitError := wh.config.BlobSizeLimitError(namespaceEntry.Name().String())
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(namespaceEntry.Name().String())

	if err := common.CheckEventBlobSizeLimit(
		request.GetResult().Size(),
		sizeLimitWarn,
		sizeLimitError,
		namespaceID.String(),
		taskToken.GetWorkflowId(),
		runID,
		wh.metricsScope(ctx).WithTags(metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UNSPECIFIED.String())),
		wh.throttledLogger,
		tag.BlobSizeViolationOperation("RespondActivityTaskCompletedById"),
	); err != nil {
		// result exceeds blob size limit, we would record it as failure
		failRequest := &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: token,
			Failure:   failure.NewServerFailure(common.FailureReasonCompleteResultExceedsLimit, true),
			Identity:  request.Identity,
		}
		_, err = wh.historyClient.RespondActivityTaskFailed(ctx, &historyservice.RespondActivityTaskFailedRequest{
			NamespaceId:   namespaceID.String(),
			FailedRequest: failRequest,
		})
		if err != nil {
			return nil, err
		}
	} else {
		req := &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: token,
			Result:    request.Result,
			Identity:  request.Identity,
		}

		_, err = wh.historyClient.RespondActivityTaskCompleted(ctx, &historyservice.RespondActivityTaskCompletedRequest{
			NamespaceId:     namespaceID.String(),
			CompleteRequest: req,
		})
		if err != nil {
			return nil, err
		}
	}

	return &workflowservice.RespondActivityTaskCompletedByIdResponse{}, nil
}

// RespondActivityTaskFailed is called by application worker when it is done processing an ActivityTask.  It will
// result in a new 'ActivityTaskFailed' event being written to the workflow history and a new WorkflowTask
// created for the workflow instance so new commands could be made.  Use the 'taskToken' provided as response of
// PollActivityTaskQueue API call for completion. It fails with 'EntityNotExistsError' if the taskToken is not valid
// anymore due to activity timeout.
func (wh *WorkflowHandler) RespondActivityTaskFailed(
	ctx context.Context,
	request *workflowservice.RespondActivityTaskFailedRequest,
) (_ *workflowservice.RespondActivityTaskFailedResponse, retError error) {

	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	taskToken, err := wh.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return nil, err
	}
	namespaceID := namespace.ID(taskToken.GetNamespaceId())
	namespaceEntry, err := wh.namespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, err
	}

	if request.GetFailure() != nil && request.GetFailure().GetApplicationFailureInfo() == nil {
		return nil, errFailureMustHaveApplicationFailureInfo
	}

	if len(request.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return nil, errIdentityTooLong
	}

	sizeLimitError := wh.config.BlobSizeLimitError(namespaceEntry.Name().String())
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(namespaceEntry.Name().String())

	response := workflowservice.RespondActivityTaskFailedResponse{}

	if request.GetLastHeartbeatDetails() != nil {
		if err := common.CheckEventBlobSizeLimit(
			request.GetLastHeartbeatDetails().Size(),
			sizeLimitWarn,
			sizeLimitError,
			namespaceID.String(),
			taskToken.GetWorkflowId(),
			taskToken.GetRunId(),
			wh.metricsScope(ctx).WithTags(metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UNSPECIFIED.String())),
			wh.throttledLogger,
			tag.BlobSizeViolationOperation("RespondActivityTaskFailed"),
		); err != nil {
			// heartbeat details exceed size limit, we would fail the activity immediately with explicit error reason
			response.Failures = append(response.Failures, failure.NewServerFailure(common.FailureReasonHeartbeatExceedsLimit, true))

			// do not send heartbeat to history service
			request.LastHeartbeatDetails = nil
		}
	}

	if err := common.CheckEventBlobSizeLimit(
		request.GetFailure().Size(),
		sizeLimitWarn,
		sizeLimitError,
		namespaceID.String(),
		taskToken.GetWorkflowId(),
		taskToken.GetRunId(),
		wh.metricsScope(ctx).WithTags(metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UNSPECIFIED.String())),
		wh.throttledLogger,
		tag.BlobSizeViolationOperation("RespondActivityTaskFailed"),
	); err != nil {
		serverFailure := failure.NewServerFailure(common.FailureReasonFailureExceedsLimit, true)
		serverFailure.Cause = failure.Truncate(request.Failure, sizeLimitWarn)
		request.Failure = serverFailure

		response.Failures = append(response.Failures, serverFailure)
	}

	_, err = wh.historyClient.RespondActivityTaskFailed(ctx, &historyservice.RespondActivityTaskFailedRequest{
		NamespaceId:   namespaceID.String(),
		FailedRequest: request,
	})
	if err != nil {
		return nil, err
	}
	return &response, nil
}

// RespondActivityTaskFailedById is called by application worker when it is done processing an ActivityTask.
// It will result in a new 'ActivityTaskFailed' event being written to the workflow history and a new WorkflowTask
// created for the workflow instance so new commands could be made.  Similar to RespondActivityTaskFailed but use
// Namespace, WorkflowID and ActivityID instead of 'taskToken' for completion. It fails with 'EntityNotExistsError'
// if the these IDs are not valid anymore due to activity timeout.
func (wh *WorkflowHandler) RespondActivityTaskFailedById(ctx context.Context, request *workflowservice.RespondActivityTaskFailedByIdRequest) (_ *workflowservice.RespondActivityTaskFailedByIdResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}
	workflowID := request.GetWorkflowId()
	runID := request.GetRunId() // runID is optional so can be empty
	activityID := request.GetActivityId()

	if workflowID == "" {
		return nil, errWorkflowIDNotSet
	}
	if activityID == "" {
		return nil, errActivityIDNotSet
	}
	if len(request.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return nil, errIdentityTooLong
	}

	taskToken := tasktoken.NewActivityTaskToken(
		namespaceID.String(),
		workflowID,
		runID,
		common.EmptyEventID,
		activityID,
		"",
		1,
		nil,
		common.EmptyVersion,
	)
	token, err := wh.tokenSerializer.Serialize(taskToken)
	if err != nil {
		return nil, err
	}

	namespaceEntry, err := wh.namespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, err
	}

	sizeLimitError := wh.config.BlobSizeLimitError(namespaceEntry.Name().String())
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(namespaceEntry.Name().String())

	response := workflowservice.RespondActivityTaskFailedByIdResponse{}

	if request.GetLastHeartbeatDetails() != nil {
		if err := common.CheckEventBlobSizeLimit(
			request.GetLastHeartbeatDetails().Size(),
			sizeLimitWarn,
			sizeLimitError,
			namespaceID.String(),
			taskToken.GetWorkflowId(),
			runID,
			wh.metricsScope(ctx).WithTags(metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UNSPECIFIED.String())),
			wh.throttledLogger,
			tag.BlobSizeViolationOperation("RespondActivityTaskFailedById"),
		); err != nil {
			// heartbeat details exceed size limit, we would fail the activity immediately with explicit error reason
			response.Failures = append(response.Failures, failure.NewServerFailure(common.FailureReasonHeartbeatExceedsLimit, true))

			// do not send heartbeat to history service
			request.LastHeartbeatDetails = nil
		}
	}

	if err := common.CheckEventBlobSizeLimit(
		request.GetFailure().Size(),
		sizeLimitWarn,
		sizeLimitError,
		namespaceID.String(),
		taskToken.GetWorkflowId(),
		runID,
		wh.metricsScope(ctx).WithTags(metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UNSPECIFIED.String())),
		wh.throttledLogger,
		tag.BlobSizeViolationOperation("RespondActivityTaskFailedById"),
	); err != nil {
		serverFailure := failure.NewServerFailure(common.FailureReasonFailureExceedsLimit, true)
		serverFailure.Cause = failure.Truncate(request.Failure, sizeLimitWarn)
		request.Failure = serverFailure

		response.Failures = append(response.Failures, serverFailure)
	}

	req := &workflowservice.RespondActivityTaskFailedRequest{
		TaskToken: token,
		Failure:   request.GetFailure(),
		Identity:  request.Identity,
	}

	_, err = wh.historyClient.RespondActivityTaskFailed(ctx, &historyservice.RespondActivityTaskFailedRequest{
		NamespaceId:   namespaceID.String(),
		FailedRequest: req,
	})
	if err != nil {
		return nil, err
	}
	return &response, nil
}

// RespondActivityTaskCanceled is called by application worker when it is successfully canceled an ActivityTask.  It will
// result in a new 'ActivityTaskCanceled' event being written to the workflow history and a new WorkflowTask
// created for the workflow instance so new commands could be made.  Use the 'taskToken' provided as response of
// PollActivityTaskQueue API call for completion. It fails with 'EntityNotExistsError' if the taskToken is not valid
// anymore due to activity timeout.
func (wh *WorkflowHandler) RespondActivityTaskCanceled(ctx context.Context, request *workflowservice.RespondActivityTaskCanceledRequest) (_ *workflowservice.RespondActivityTaskCanceledResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	taskToken, err := wh.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return nil, err
	}
	namespaceID := namespace.ID(taskToken.GetNamespaceId())
	namespaceEntry, err := wh.namespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, err
	}

	if len(request.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return nil, errIdentityTooLong
	}

	sizeLimitError := wh.config.BlobSizeLimitError(namespaceEntry.Name().String())
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(namespaceEntry.Name().String())

	if err := common.CheckEventBlobSizeLimit(
		request.GetDetails().Size(),
		sizeLimitWarn,
		sizeLimitError,
		namespaceID.String(),
		taskToken.GetWorkflowId(),
		taskToken.GetRunId(),
		wh.metricsScope(ctx).WithTags(metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UNSPECIFIED.String())),
		wh.throttledLogger,
		tag.BlobSizeViolationOperation("RespondActivityTaskCanceled"),
	); err != nil {
		// details exceeds blob size limit, we would record it as failure
		failRequest := &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: request.TaskToken,
			Failure:   failure.NewServerFailure(common.FailureReasonCancelDetailsExceedsLimit, true),
			Identity:  request.Identity,
		}
		_, err = wh.historyClient.RespondActivityTaskFailed(ctx, &historyservice.RespondActivityTaskFailedRequest{
			NamespaceId:   taskToken.GetNamespaceId(),
			FailedRequest: failRequest,
		})
		if err != nil {
			return nil, err
		}
	} else {
		_, err = wh.historyClient.RespondActivityTaskCanceled(ctx, &historyservice.RespondActivityTaskCanceledRequest{
			NamespaceId:   taskToken.GetNamespaceId(),
			CancelRequest: request,
		})
		if err != nil {
			return nil, err
		}
	}

	return &workflowservice.RespondActivityTaskCanceledResponse{}, nil
}

// RespondActivityTaskCanceledById is called by application worker when it is successfully canceled an ActivityTask.
// It will result in a new 'ActivityTaskCanceled' event being written to the workflow history and a new WorkflowTask
// created for the workflow instance so new commands could be made.  Similar to RespondActivityTaskCanceled but use
// Namespace, WorkflowID and ActivityID instead of 'taskToken' for completion. It fails with 'EntityNotExistsError'
// if the these IDs are not valid anymore due to activity timeout.
func (wh *WorkflowHandler) RespondActivityTaskCanceledById(ctx context.Context, request *workflowservice.RespondActivityTaskCanceledByIdRequest) (_ *workflowservice.RespondActivityTaskCanceledByIdResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}
	workflowID := request.GetWorkflowId()
	runID := request.GetRunId() // runID is optional so can be empty
	activityID := request.GetActivityId()

	if workflowID == "" {
		return nil, errWorkflowIDNotSet
	}
	if activityID == "" {
		return nil, errActivityIDNotSet
	}
	if len(request.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return nil, errIdentityTooLong
	}

	taskToken := tasktoken.NewActivityTaskToken(
		namespaceID.String(),
		workflowID,
		runID,
		common.EmptyEventID,
		activityID,
		"",
		1,
		nil,
		common.EmptyVersion,
	)
	token, err := wh.tokenSerializer.Serialize(taskToken)
	if err != nil {
		return nil, err
	}

	namespaceEntry, err := wh.namespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, err
	}

	sizeLimitError := wh.config.BlobSizeLimitError(namespaceEntry.Name().String())
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(namespaceEntry.Name().String())

	if err := common.CheckEventBlobSizeLimit(
		request.GetDetails().Size(),
		sizeLimitWarn,
		sizeLimitError,
		namespaceID.String(),
		taskToken.GetWorkflowId(),
		runID,
		wh.metricsScope(ctx).WithTags(metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UNSPECIFIED.String())),
		wh.throttledLogger,
		tag.BlobSizeViolationOperation("RespondActivityTaskCanceledById"),
	); err != nil {
		// details exceeds blob size limit, we would record it as failure
		failRequest := &workflowservice.RespondActivityTaskFailedRequest{
			TaskToken: token,
			Failure:   failure.NewServerFailure(common.FailureReasonCancelDetailsExceedsLimit, true),
			Identity:  request.Identity,
		}
		_, err = wh.historyClient.RespondActivityTaskFailed(ctx, &historyservice.RespondActivityTaskFailedRequest{
			NamespaceId:   namespaceID.String(),
			FailedRequest: failRequest,
		})
		if err != nil {
			return nil, err
		}
	} else {
		req := &workflowservice.RespondActivityTaskCanceledRequest{
			TaskToken: token,
			Details:   request.Details,
			Identity:  request.Identity,
		}

		_, err = wh.historyClient.RespondActivityTaskCanceled(ctx, &historyservice.RespondActivityTaskCanceledRequest{
			NamespaceId:   namespaceID.String(),
			CancelRequest: req,
		})
		if err != nil {
			return nil, err
		}
	}

	return &workflowservice.RespondActivityTaskCanceledByIdResponse{}, nil
}

// RequestCancelWorkflowExecution is called by application worker when it wants to request cancellation of a workflow instance.
// It will result in a new 'WorkflowExecutionCancelRequested' event being written to the workflow history and a new WorkflowTask
// created for the workflow instance so new commands could be made. It returns success if requested workflow already closed.
// It fails with 'NotFound' if the requested workflow doesn't exist.
func (wh *WorkflowHandler) RequestCancelWorkflowExecution(ctx context.Context, request *workflowservice.RequestCancelWorkflowExecutionRequest) (_ *workflowservice.RequestCancelWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	if err := validateExecution(request.WorkflowExecution); err != nil {
		return nil, err
	}

	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	_, err = wh.historyClient.RequestCancelWorkflowExecution(ctx, &historyservice.RequestCancelWorkflowExecutionRequest{
		NamespaceId:   namespaceID.String(),
		CancelRequest: request,
	})
	if err != nil {
		return nil, err
	}

	return &workflowservice.RequestCancelWorkflowExecutionResponse{}, nil
}

// SignalWorkflowExecution is used to send a signal event to running workflow execution. This results in
// a WorkflowExecutionSignaled event recorded in the history and a workflow task being created for the execution.
func (wh *WorkflowHandler) SignalWorkflowExecution(ctx context.Context, request *workflowservice.SignalWorkflowExecutionRequest) (_ *workflowservice.SignalWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	if err := validateExecution(request.WorkflowExecution); err != nil {
		return nil, err
	}

	if request.GetSignalName() == "" {
		return nil, errSignalNameNotSet
	}

	if len(request.GetSignalName()) > wh.config.MaxIDLengthLimit() {
		return nil, errSignalNameTooLong
	}

	if len(request.GetRequestId()) > wh.config.MaxIDLengthLimit() {
		return nil, errRequestIDTooLong
	}

	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	sizeLimitError := wh.config.BlobSizeLimitError(request.GetNamespace())
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(request.GetNamespace())
	if err := common.CheckEventBlobSizeLimit(
		request.GetInput().Size(),
		sizeLimitWarn,
		sizeLimitError,
		namespaceID.String(),
		request.GetWorkflowExecution().GetWorkflowId(),
		request.GetWorkflowExecution().GetRunId(),
		wh.metricsScope(ctx).WithTags(metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UNSPECIFIED.String())),
		wh.throttledLogger,
		tag.BlobSizeViolationOperation("SignalWorkflowExecution"),
	); err != nil {
		return nil, err
	}

	_, err = wh.historyClient.SignalWorkflowExecution(ctx, &historyservice.SignalWorkflowExecutionRequest{
		NamespaceId:   namespaceID.String(),
		SignalRequest: request,
	})
	if err != nil {
		return nil, err
	}

	return &workflowservice.SignalWorkflowExecutionResponse{}, nil
}

// SignalWithStartWorkflowExecution is used to ensure sending signal to a workflow.
// If the workflow is running, this results in WorkflowExecutionSignaled event being recorded in the history
// and a workflow task being created for the execution.
// If the workflow is not running or not found, this results in WorkflowExecutionStarted and WorkflowExecutionSignaled
// events being recorded in history, and a workflow task being created for the execution
func (wh *WorkflowHandler) SignalWithStartWorkflowExecution(ctx context.Context, request *workflowservice.SignalWithStartWorkflowExecutionRequest) (_ *workflowservice.SignalWithStartWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	if err := wh.validateWorkflowID(request.GetWorkflowId()); err != nil {
		return nil, err
	}

	if request.GetSignalName() == "" {
		return nil, errSignalNameNotSet
	}

	if len(request.GetSignalName()) > wh.config.MaxIDLengthLimit() {
		return nil, errSignalNameTooLong
	}

	if request.WorkflowType == nil || request.WorkflowType.GetName() == "" {
		return nil, errWorkflowTypeNotSet
	}

	if len(request.WorkflowType.GetName()) > wh.config.MaxIDLengthLimit() {
		return nil, errWorkflowTypeTooLong
	}

	if err := common.ValidateUTF8String("WorkflowType", request.WorkflowType.GetName()); err != nil {
		return nil, err
	}
	namespaceName := namespace.Name(request.GetNamespace())
	if err := wh.validateTaskQueue(request.TaskQueue); err != nil {
		return nil, err
	}

	if err := validateRequestId(&request.RequestId, wh.config.MaxIDLengthLimit()); err != nil {
		return nil, err
	}

	if err := wh.validateSignalWithStartWorkflowTimeouts(request); err != nil {
		return nil, err
	}

	if err := wh.validateRetryPolicy(namespaceName, request.RetryPolicy); err != nil {
		return nil, err
	}

	if err := wh.validateWorkflowStartDelay(request.GetCronSchedule(), request.WorkflowStartDelay); err != nil {
		return nil, err
	}

	if err := wh.validateWorkflowIdReusePolicy(
		request.WorkflowIdReusePolicy,
		request.WorkflowIdConflictPolicy,
	); err != nil {
		return nil, err
	}

	if request.WorkflowIdConflictPolicy == enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL {
		// Signal-with-*Required*-Start is not supported
		name := enumspb.WorkflowIdConflictPolicy_name[int32(request.WorkflowIdConflictPolicy.Number())]
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(errUnsupportedIDConflictPolicy, name))
	}

	enums.SetDefaultWorkflowIdReusePolicy(&request.WorkflowIdReusePolicy)
	enums.SetDefaultWorkflowIdConflictPolicy(&request.WorkflowIdConflictPolicy, enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING)

	if err := backoff.ValidateSchedule(request.GetCronSchedule()); err != nil {
		return nil, err
	}

	sa, err := wh.unaliasedSearchAttributesFrom(request.GetSearchAttributes(), namespaceName)
	if err != nil {
		return nil, err
	}
	if sa != request.GetSearchAttributes() {
		// cloning here so in case of retry the field is set to the current search attributes
		request = common.CloneProto(request)
		request.SearchAttributes = sa
	}

	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespaceName)
	if err != nil {
		return nil, err
	}

	resp, err := wh.historyClient.SignalWithStartWorkflowExecution(ctx, &historyservice.SignalWithStartWorkflowExecutionRequest{
		NamespaceId:            namespaceID.String(),
		SignalWithStartRequest: request,
	})

	if err != nil {
		return nil, err
	}

	return &workflowservice.SignalWithStartWorkflowExecutionResponse{
		RunId:   resp.GetRunId(),
		Started: resp.Started,
	}, nil
}

// ResetWorkflowExecution reset an existing workflow execution to WorkflowTaskCompleted event(exclusive).
// And it will immediately terminating the current execution instance.
func (wh *WorkflowHandler) ResetWorkflowExecution(ctx context.Context, request *workflowservice.ResetWorkflowExecutionRequest) (_ *workflowservice.ResetWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}
	if request.GetRequestId() == "" {
		return nil, errRequestIDNotSet
	}
	if len(request.GetRequestId()) > wh.config.MaxIDLengthLimit() {
		return nil, errRequestIDTooLong
	}

	if err := validateExecution(request.WorkflowExecution); err != nil {
		return nil, err
	}

	enums.SetDefaultResetReapplyType(&request.ResetReapplyType)
	if _, validType := enumspb.ResetReapplyType_name[int32(request.GetResetReapplyType())]; !validType {
		return nil, serviceerror.NewInternal(fmt.Sprintf("unknown reset reapply type: %v", request.GetResetReapplyType()))
	}

	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	resp, err := wh.historyClient.ResetWorkflowExecution(ctx, &historyservice.ResetWorkflowExecutionRequest{
		NamespaceId:  namespaceID.String(),
		ResetRequest: request,
	})
	if err != nil {
		return nil, err
	}

	return &workflowservice.ResetWorkflowExecutionResponse{RunId: resp.GetRunId()}, nil
}

// TerminateWorkflowExecution terminates an existing workflow execution by recording WorkflowExecutionTerminated event
// in the history and immediately terminating the execution instance.
func (wh *WorkflowHandler) TerminateWorkflowExecution(ctx context.Context, request *workflowservice.TerminateWorkflowExecutionRequest) (_ *workflowservice.TerminateWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	if err := validateExecution(request.WorkflowExecution); err != nil {
		return nil, err
	}

	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	_, err = wh.historyClient.TerminateWorkflowExecution(ctx, &historyservice.TerminateWorkflowExecutionRequest{
		NamespaceId:      namespaceID.String(),
		TerminateRequest: request,
	})
	if err != nil {
		return nil, err
	}

	return &workflowservice.TerminateWorkflowExecutionResponse{}, nil
}

// DeleteWorkflowExecution deletes a closed workflow execution asynchronously (workflow must be completed or terminated before).
// This method is EXPERIMENTAL and may be changed or removed in a later release.
func (wh *WorkflowHandler) DeleteWorkflowExecution(ctx context.Context, request *workflowservice.DeleteWorkflowExecutionRequest) (_ *workflowservice.DeleteWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	if err := validateExecution(request.WorkflowExecution); err != nil {
		return nil, err
	}

	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	_, err = wh.historyClient.DeleteWorkflowExecution(ctx, &historyservice.DeleteWorkflowExecutionRequest{
		NamespaceId:        namespaceID.String(),
		WorkflowExecution:  request.GetWorkflowExecution(),
		ClosedWorkflowOnly: false,
	})
	if err != nil {
		return nil, err
	}

	return &workflowservice.DeleteWorkflowExecutionResponse{}, nil
}

// ListOpenWorkflowExecutions is a visibility API to list the open executions in a specific namespace.
func (wh *WorkflowHandler) ListOpenWorkflowExecutions(ctx context.Context, request *workflowservice.ListOpenWorkflowExecutionsRequest) (_ *workflowservice.ListOpenWorkflowExecutionsResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	maxPageSize := int32(wh.config.VisibilityMaxPageSize(request.GetNamespace()))
	if request.GetMaximumPageSize() <= 0 || request.GetMaximumPageSize() > maxPageSize {
		request.MaximumPageSize = maxPageSize
	}

	namespaceName := namespace.Name(request.GetNamespace())
	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespaceName)
	if err != nil {
		return nil, err
	}

	if request.StartTimeFilter == nil {
		request.StartTimeFilter = &filterpb.StartTimeFilter{}
	}

	earliestTime := request.StartTimeFilter.GetEarliestTime()
	latestTime := request.StartTimeFilter.GetLatestTime()
	query := []string{}

	query = append(query, fmt.Sprintf(
		"%s = '%s'",
		searchattribute.ExecutionStatus,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	))

	if earliestTime != nil && !earliestTime.AsTime().IsZero() &&
		latestTime != nil && !latestTime.AsTime().IsZero() {
		if earliestTime.AsTime().After(latestTime.AsTime()) {
			return nil, errEarliestTimeIsGreaterThanLatestTime
		}
		query = append(query, fmt.Sprintf(
			"%s BETWEEN '%s' AND '%s'",
			searchattribute.StartTime,
			earliestTime.AsTime().Format(time.RFC3339Nano),
			latestTime.AsTime().Format(time.RFC3339Nano),
		))
	} else if earliestTime != nil && !earliestTime.AsTime().IsZero() {
		query = append(query, fmt.Sprintf(
			"%s >= '%s'",
			searchattribute.StartTime,
			earliestTime.AsTime().Format(time.RFC3339Nano),
		))
	} else if latestTime != nil && !latestTime.AsTime().IsZero() {
		query = append(query, fmt.Sprintf(
			"%s <= '%s'",
			searchattribute.StartTime,
			latestTime.AsTime().Format(time.RFC3339Nano),
		))
	}

	if request.GetExecutionFilter() != nil {
		if wh.config.DisableListVisibilityByFilter(namespaceName.String()) {
			return nil, errListNotAllowed
		}
		query = append(query, fmt.Sprintf(
			"%s = '%s'",
			searchattribute.WorkflowID,
			request.GetExecutionFilter().GetWorkflowId()))

		wh.logger.Debug("List open workflow with filter",
			tag.WorkflowNamespace(request.GetNamespace()), tag.WorkflowListWorkflowFilterByID)
	} else if request.GetTypeFilter() != nil {
		if wh.config.DisableListVisibilityByFilter(namespaceName.String()) {
			return nil, errListNotAllowed
		}
		query = append(query, fmt.Sprintf(
			"%s = '%s'",
			searchattribute.WorkflowType,
			request.GetTypeFilter().GetName()))

		wh.logger.Debug("List open workflow with filter",
			tag.WorkflowNamespace(request.GetNamespace()), tag.WorkflowListWorkflowFilterByType)
	}

	baseReq := &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID:   namespaceID,
		Namespace:     namespaceName,
		PageSize:      int(request.GetMaximumPageSize()),
		NextPageToken: request.NextPageToken,
		Query:         strings.Join(query, " AND "),
	}
	persistenceResp, err := wh.visibilityMgr.ListWorkflowExecutions(ctx, baseReq)

	if err != nil {
		return nil, err
	}

	return &workflowservice.ListOpenWorkflowExecutionsResponse{
		Executions:    persistenceResp.Executions,
		NextPageToken: persistenceResp.NextPageToken,
	}, nil
}

// ListClosedWorkflowExecutions is a visibility API to list the closed executions in a specific namespace.
func (wh *WorkflowHandler) ListClosedWorkflowExecutions(ctx context.Context, request *workflowservice.ListClosedWorkflowExecutionsRequest) (_ *workflowservice.ListClosedWorkflowExecutionsResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	maxPageSize := int32(wh.config.VisibilityMaxPageSize(request.GetNamespace()))
	if request.GetMaximumPageSize() <= 0 || request.GetMaximumPageSize() > maxPageSize {
		request.MaximumPageSize = maxPageSize
	}

	namespaceName := namespace.Name(request.GetNamespace())
	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespaceName)
	if err != nil {
		return nil, err
	}

	if request.StartTimeFilter == nil {
		request.StartTimeFilter = &filterpb.StartTimeFilter{}
	}

	earliestTime := request.StartTimeFilter.GetEarliestTime()
	latestTime := request.StartTimeFilter.GetLatestTime()
	query := []string{}

	query = append(query, fmt.Sprintf(
		"%s != '%s'",
		searchattribute.ExecutionStatus,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	))

	if earliestTime != nil && !earliestTime.AsTime().IsZero() &&
		latestTime != nil && !latestTime.AsTime().IsZero() {
		if earliestTime.AsTime().After(latestTime.AsTime()) {
			return nil, errEarliestTimeIsGreaterThanLatestTime
		}
		query = append(query, fmt.Sprintf(
			"%s BETWEEN '%s' AND '%s'",
			searchattribute.CloseTime,
			earliestTime.AsTime().Format(time.RFC3339Nano),
			latestTime.AsTime().Format(time.RFC3339Nano),
		))
	} else if earliestTime != nil && !earliestTime.AsTime().IsZero() {
		query = append(query, fmt.Sprintf(
			"%s >= '%s'",
			searchattribute.CloseTime,
			earliestTime.AsTime().Format(time.RFC3339Nano),
		))
	} else if latestTime != nil && !latestTime.AsTime().IsZero() {
		query = append(query, fmt.Sprintf(
			"%s <= '%s'",
			searchattribute.CloseTime,
			latestTime.AsTime().Format(time.RFC3339Nano),
		))
	}

	if request.GetExecutionFilter() != nil {
		if wh.config.DisableListVisibilityByFilter(namespaceName.String()) {
			return nil, errListNotAllowed
		}
		query = append(query, fmt.Sprintf(
			"%s = '%s'",
			searchattribute.WorkflowID,
			request.GetExecutionFilter().GetWorkflowId()))

		wh.logger.Debug("List closed workflow with filter",
			tag.WorkflowNamespace(request.GetNamespace()), tag.WorkflowListWorkflowFilterByID)
	} else if request.GetTypeFilter() != nil {
		if wh.config.DisableListVisibilityByFilter(namespaceName.String()) {
			return nil, errListNotAllowed
		}
		query = append(query, fmt.Sprintf(
			"%s = '%s'",
			searchattribute.WorkflowType,
			request.GetTypeFilter().GetName()))

		wh.logger.Debug("List closed workflow with filter",
			tag.WorkflowNamespace(request.GetNamespace()), tag.WorkflowListWorkflowFilterByType)
	} else if request.GetStatusFilter() != nil {
		if wh.config.DisableListVisibilityByFilter(namespaceName.String()) {
			return nil, errListNotAllowed
		}
		if request.GetStatusFilter().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED || request.GetStatusFilter().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			return nil, errStatusFilterMustBeNotRunning
		}
		query = append(query, fmt.Sprintf(
			"%s = '%s'",
			searchattribute.ExecutionStatus,
			request.GetStatusFilter().GetStatus()))

		wh.logger.Debug("List closed workflow with filter",
			tag.WorkflowNamespace(request.GetNamespace()), tag.WorkflowListWorkflowFilterByStatus)
	}

	baseReq := &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID:   namespaceID,
		Namespace:     namespaceName,
		PageSize:      int(request.GetMaximumPageSize()),
		NextPageToken: request.NextPageToken,
		Query:         strings.Join(query, " AND "),
	}
	persistenceResp, err := wh.visibilityMgr.ListWorkflowExecutions(ctx, baseReq)

	if err != nil {
		return nil, err
	}

	return &workflowservice.ListClosedWorkflowExecutionsResponse{
		Executions:    persistenceResp.Executions,
		NextPageToken: persistenceResp.NextPageToken,
	}, nil
}

// ListWorkflowExecutions is a visibility API to list workflow executions in a specific namespace.
func (wh *WorkflowHandler) ListWorkflowExecutions(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (_ *workflowservice.ListWorkflowExecutionsResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	maxPageSize := int32(wh.config.VisibilityMaxPageSize(request.GetNamespace()))
	if request.GetPageSize() <= 0 || request.GetPageSize() > maxPageSize {
		request.PageSize = maxPageSize
	}

	namespaceName := namespace.Name(request.GetNamespace())
	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespaceName)
	if err != nil {
		return nil, err
	}

	req := &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID:   namespaceID,
		Namespace:     namespaceName,
		PageSize:      int(request.GetPageSize()),
		NextPageToken: request.NextPageToken,
		Query:         request.GetQuery(),
	}
	persistenceResp, err := wh.visibilityMgr.ListWorkflowExecutions(ctx, req)
	if err != nil {
		return nil, err
	}

	return &workflowservice.ListWorkflowExecutionsResponse{
		Executions:    persistenceResp.Executions,
		NextPageToken: persistenceResp.NextPageToken,
	}, nil
}

// ListArchivedWorkflowExecutions is a visibility API to list archived workflow executions in a specific namespace.
func (wh *WorkflowHandler) ListArchivedWorkflowExecutions(ctx context.Context, request *workflowservice.ListArchivedWorkflowExecutionsRequest) (_ *workflowservice.ListArchivedWorkflowExecutionsResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	maxPageSize := int32(wh.config.VisibilityArchivalQueryMaxPageSize())
	if request.GetPageSize() <= 0 {
		request.PageSize = maxPageSize
	} else if request.GetPageSize() > maxPageSize {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(errPageSizeTooBigMessage, maxPageSize))
	}

	if !wh.archivalMetadata.GetVisibilityConfig().ClusterConfiguredForArchival() {
		return nil, errClusterIsNotConfiguredForVisibilityArchival
	}

	if !wh.archivalMetadata.GetVisibilityConfig().ReadEnabled() {
		return nil, errClusterIsNotConfiguredForReadingArchivalVisibility
	}

	entry, err := wh.namespaceRegistry.GetNamespace(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	if entry.VisibilityArchivalState().State != enumspb.ARCHIVAL_STATE_ENABLED {
		return nil, errNamespaceIsNotConfiguredForVisibilityArchival
	}

	URI, err := archiver.NewURI(entry.VisibilityArchivalState().URI)
	if err != nil {
		return nil, err
	}

	visibilityArchiver, err := wh.archiverProvider.GetVisibilityArchiver(URI.Scheme(), string(primitives.FrontendService))
	if err != nil {
		return nil, err
	}

	archiverRequest := &archiver.QueryVisibilityRequest{
		NamespaceID:   entry.ID().String(),
		PageSize:      int(request.GetPageSize()),
		NextPageToken: request.NextPageToken,
		Query:         request.GetQuery(),
	}

	searchAttributes, err := wh.saProvider.GetSearchAttributes(wh.visibilityMgr.GetIndexName(), false)
	if err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf(errUnableToGetSearchAttributesMessage, err))
	}

	archiverResponse, err := visibilityArchiver.Query(
		ctx,
		URI,
		archiverRequest,
		searchAttributes)
	if err != nil {
		return nil, err
	}

	// special handling of ExecutionTime for cron or retry
	for _, execution := range archiverResponse.Executions {
		if execution.ExecutionTime == nil || execution.ExecutionTime.AsTime().IsZero() {
			execution.ExecutionTime = execution.GetStartTime()
		}
	}

	return &workflowservice.ListArchivedWorkflowExecutionsResponse{
		Executions:    archiverResponse.Executions,
		NextPageToken: archiverResponse.NextPageToken,
	}, nil
}

// ScanWorkflowExecutions is a visibility API to list large amount of workflow executions in a specific namespace without order.
func (wh *WorkflowHandler) ScanWorkflowExecutions(ctx context.Context, request *workflowservice.ScanWorkflowExecutionsRequest) (_ *workflowservice.ScanWorkflowExecutionsResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	maxPageSize := int32(wh.config.VisibilityMaxPageSize(request.GetNamespace()))
	if request.GetPageSize() <= 0 || request.GetPageSize() > maxPageSize {
		request.PageSize = maxPageSize
	}

	namespaceName := namespace.Name(request.GetNamespace())
	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespaceName)
	if err != nil {
		return nil, err
	}

	req := &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID:   namespaceID,
		Namespace:     namespaceName,
		PageSize:      int(request.GetPageSize()),
		NextPageToken: request.NextPageToken,
		Query:         request.GetQuery(),
	}
	persistenceResp, err := wh.visibilityMgr.ScanWorkflowExecutions(ctx, req)
	if err != nil {
		return nil, err
	}

	resp := &workflowservice.ScanWorkflowExecutionsResponse{
		Executions:    persistenceResp.Executions,
		NextPageToken: persistenceResp.NextPageToken,
	}
	return resp, nil
}

// CountWorkflowExecutions is a visibility API to count of workflow executions in a specific namespace.
func (wh *WorkflowHandler) CountWorkflowExecutions(ctx context.Context, request *workflowservice.CountWorkflowExecutionsRequest) (_ *workflowservice.CountWorkflowExecutionsResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	namespaceName := namespace.Name(request.GetNamespace())
	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespaceName)
	if err != nil {
		return nil, err
	}

	req := &manager.CountWorkflowExecutionsRequest{
		NamespaceID: namespaceID,
		Namespace:   namespaceName,
		Query:       request.GetQuery(),
	}
	persistenceResp, err := wh.visibilityMgr.CountWorkflowExecutions(ctx, req)
	if err != nil {
		return nil, err
	}

	resp := &workflowservice.CountWorkflowExecutionsResponse{
		Count:  persistenceResp.Count,
		Groups: persistenceResp.Groups,
	}
	return resp, nil
}

// GetSearchAttributes is a visibility API to get all legal keys that could be used in list APIs
func (wh *WorkflowHandler) GetSearchAttributes(ctx context.Context, _ *workflowservice.GetSearchAttributesRequest) (_ *workflowservice.GetSearchAttributesResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	searchAttributes, err := wh.saProvider.GetSearchAttributes(wh.visibilityMgr.GetIndexName(), false)
	if err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf(errUnableToGetSearchAttributesMessage, err))
	}
	resp := &workflowservice.GetSearchAttributesResponse{
		Keys: searchAttributes.All(),
	}
	return resp, nil
}

// RespondQueryTaskCompleted is called by application worker to complete a QueryTask (which is a WorkflowTask for query)
// as a result of 'PollWorkflowTaskQueue' API call. Completing a QueryTask will unblock the client call to 'QueryWorkflow'
// API and return the query result to client as a response to 'QueryWorkflow' API call.
func (wh *WorkflowHandler) RespondQueryTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondQueryTaskCompletedRequest,
) (_ *workflowservice.RespondQueryTaskCompletedResponse, retError error) {

	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	queryTaskToken, err := wh.tokenSerializer.DeserializeQueryTaskToken(request.TaskToken)
	if err != nil {
		return nil, err
	}
	if queryTaskToken.GetTaskQueue() == "" || queryTaskToken.GetTaskId() == "" {
		return nil, errInvalidTaskToken
	}
	namespaceId := namespace.ID(queryTaskToken.GetNamespaceId())
	namespaceEntry, err := wh.namespaceRegistry.GetNamespaceByID(namespaceId)
	if err != nil {
		return nil, err
	}

	sizeLimitError := wh.config.BlobSizeLimitError(namespaceEntry.Name().String())
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(namespaceEntry.Name().String())

	if err := common.CheckEventBlobSizeLimit(
		request.GetQueryResult().Size(),
		sizeLimitWarn,
		sizeLimitError,
		namespaceId.String(),
		"",
		"",
		wh.metricsScope(ctx).WithTags(metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UNSPECIFIED.String())),
		wh.throttledLogger,
		tag.BlobSizeViolationOperation("RespondQueryTaskCompleted"),
	); err != nil {
		request = &workflowservice.RespondQueryTaskCompletedRequest{
			TaskToken:     request.TaskToken,
			CompletedType: enumspb.QUERY_RESULT_TYPE_FAILED,
			QueryResult:   nil,
			ErrorMessage:  err.Error(),
		}
	}

	matchingRequest := &matchingservice.RespondQueryTaskCompletedRequest{
		NamespaceId: namespaceId.String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: queryTaskToken.GetTaskQueue(),
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		TaskId:           queryTaskToken.GetTaskId(),
		CompletedRequest: request,
	}

	_, err = wh.matchingClient.RespondQueryTaskCompleted(ctx, matchingRequest)
	if err != nil {
		return nil, err
	}
	return &workflowservice.RespondQueryTaskCompletedResponse{}, nil
}

// ResetStickyTaskQueue resets the sticky taskqueue related information in mutable state of a given workflow.
// Things cleared are:
// 1. StickyTaskQueue
// 2. StickyScheduleToStartTimeout
func (wh *WorkflowHandler) ResetStickyTaskQueue(ctx context.Context, request *workflowservice.ResetStickyTaskQueueRequest) (_ *workflowservice.ResetStickyTaskQueueResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	if err := validateExecution(request.Execution); err != nil {
		return nil, err
	}

	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	_, err = wh.historyClient.ResetStickyTaskQueue(ctx, &historyservice.ResetStickyTaskQueueRequest{
		NamespaceId: namespaceID.String(),
		Execution:   request.Execution,
	})
	if err != nil {
		return nil, err
	}
	return &workflowservice.ResetStickyTaskQueueResponse{}, nil
}

// QueryWorkflow returns query result for a specified workflow execution
func (wh *WorkflowHandler) QueryWorkflow(ctx context.Context, request *workflowservice.QueryWorkflowRequest) (_ *workflowservice.QueryWorkflowResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if wh.config.DisallowQuery(request.GetNamespace()) {
		return nil, errQueryDisallowedForNamespace
	}

	if request == nil {
		return nil, errRequestNotSet
	}

	if err := validateExecution(request.Execution); err != nil {
		return nil, err
	}

	if request.Query == nil {
		return nil, errQueryNotSet
	}

	if request.Query.GetQueryType() == "" {
		return nil, errQueryTypeNotSet
	}

	enums.SetDefaultQueryRejectCondition(&request.QueryRejectCondition)

	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	sizeLimitError := wh.config.BlobSizeLimitError(request.GetNamespace())
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(request.GetNamespace())

	if err := common.CheckEventBlobSizeLimit(
		request.GetQuery().GetQueryArgs().Size(),
		sizeLimitWarn,
		sizeLimitError,
		namespaceID.String(),
		request.GetExecution().GetWorkflowId(),
		request.GetExecution().GetRunId(),
		wh.metricsScope(ctx).WithTags(metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UNSPECIFIED.String())),
		wh.throttledLogger,
		tag.BlobSizeViolationOperation("QueryWorkflow")); err != nil {
		return nil, err
	}

	req := &historyservice.QueryWorkflowRequest{
		NamespaceId: namespaceID.String(),
		Request:     request,
	}
	hResponse, err := wh.historyClient.QueryWorkflow(ctx, req)
	if err != nil {
		if common.IsContextDeadlineExceededErr(err) {
			return nil, serviceerror.NewDeadlineExceeded("query timed out before a worker could process it")
		}
		return nil, err
	}
	return hResponse.GetResponse(), nil
}

// DescribeWorkflowExecution returns information about the specified workflow execution.
func (wh *WorkflowHandler) DescribeWorkflowExecution(ctx context.Context, request *workflowservice.DescribeWorkflowExecutionRequest) (_ *workflowservice.DescribeWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	if err := validateExecution(request.Execution); err != nil {
		return nil, err
	}

	response, err := wh.historyClient.DescribeWorkflowExecution(ctx, &historyservice.DescribeWorkflowExecutionRequest{
		NamespaceId: namespaceID.String(),
		Request:     request,
	})

	if err != nil {
		return nil, err
	}

	if response.GetWorkflowExecutionInfo().GetSearchAttributes() != nil {
		saTypeMap, err := wh.saProvider.GetSearchAttributes(wh.visibilityMgr.GetIndexName(), false)
		if err != nil {
			return nil, serviceerror.NewUnavailable(fmt.Sprintf(errUnableToGetSearchAttributesMessage, err))
		}
		searchattribute.ApplyTypeMap(response.GetWorkflowExecutionInfo().GetSearchAttributes(), saTypeMap)
		aliasedSas, err := searchattribute.AliasFields(wh.saMapperProvider, response.GetWorkflowExecutionInfo().GetSearchAttributes(), request.GetNamespace())
		if err != nil {
			return nil, err
		}
		if aliasedSas != response.GetWorkflowExecutionInfo().GetSearchAttributes() {
			response.GetWorkflowExecutionInfo().SearchAttributes = aliasedSas
		}
	}

	return &workflowservice.DescribeWorkflowExecutionResponse{
		ExecutionConfig:        response.GetExecutionConfig(),
		WorkflowExecutionInfo:  response.GetWorkflowExecutionInfo(),
		PendingActivities:      response.GetPendingActivities(),
		PendingChildren:        response.GetPendingChildren(),
		PendingWorkflowTask:    response.GetPendingWorkflowTask(),
		Callbacks:              response.GetCallbacks(),
		PendingNexusOperations: response.GetPendingNexusOperations(),
	}, nil
}

// DescribeTaskQueue returns information about the target taskqueue, right now this API returns the
// pollers which polled this taskqueue in last few minutes.
func (wh *WorkflowHandler) DescribeTaskQueue(ctx context.Context, request *workflowservice.DescribeTaskQueueRequest) (_ *workflowservice.DescribeTaskQueueResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	namespaceName := namespace.Name(request.GetNamespace())
	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespaceName)
	if err != nil {
		return nil, err
	}

	if err := wh.validateTaskQueue(request.TaskQueue); err != nil {
		return nil, err
	}

	if request.TaskQueueType == enumspb.TASK_QUEUE_TYPE_UNSPECIFIED || request.ApiMode == enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED {
		request.TaskQueueType = enumspb.TASK_QUEUE_TYPE_WORKFLOW
	}

	if len(request.TaskQueueTypes) == 0 {
		request.TaskQueueTypes = []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW, enumspb.TASK_QUEUE_TYPE_ACTIVITY}
	}

	if request.GetReportTaskReachability() &&
		len(request.GetVersions().GetBuildIds()) > wh.config.ReachabilityQueryBuildIdLimit() {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(
			"Too many build ids queried at once with ReportTaskReachability==true, limit: %d", wh.config.ReachabilityQueryBuildIdLimit()))
	}

	if request.ApiMode == enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED {
		if request.TaskQueue.Kind == enumspb.TASK_QUEUE_KIND_STICKY {
			return nil, errUseEnhancedDescribeOnStickyQueue
		}
		if partition, err := tqid.PartitionFromProto(request.TaskQueue, namespaceID.String(), enumspb.TASK_QUEUE_TYPE_WORKFLOW); err != nil {
			return nil, errTaskQueuePartitionInvalid
		} else if !partition.IsRoot() {
			return nil, errUseEnhancedDescribeOnNonRootQueue
		}
	}

	matchingResponse, err := wh.matchingClient.DescribeTaskQueue(ctx, &matchingservice.DescribeTaskQueueRequest{
		NamespaceId: namespaceID.String(),
		DescRequest: request,
	})
	if err != nil {
		return nil, err
	}

	resp := matchingResponse.DescResponse
	// Manually parse unknown fields to handle proto incompatibility.
	// TODO: remove this after 1.24.0-m3
	if resp == nil {
		resp = &workflowservice.DescribeTaskQueueResponse{}
		unknown := []byte(matchingResponse.ProtoReflect().GetUnknown())
		for len(unknown) > 0 {
			num, typ, n := protowire.ConsumeTag(unknown)
			if n < 0 {
				break
			}
			unknown = unknown[n:]
			if typ != protowire.BytesType {
				break
			}
			msg, n := protowire.ConsumeBytes(unknown)
			if n < 0 {
				break
			}
			unknown = unknown[n:]
			switch num {
			case 1:
				// msg is either a temporal.api.workflowservice.v1.DescribeTaskQueueResponse (new) or repeated temporal.api.taskqueue.v1.PollerInfo (old)
				// try DescribeTaskQueueResponse first
				var dtqr workflowservice.DescribeTaskQueueResponse
				var pi taskqueuepb.PollerInfo
				if err := proto.Unmarshal(msg, &dtqr); err == nil {
					// merge this into the response, to avoid losing data in case this was a spurious success
					proto.Merge(resp, &dtqr)
				} else if err := proto.Unmarshal(msg, &pi); err == nil {
					resp.Pollers = append(resp.Pollers, &pi)
				}
			case 2:
				// msg should be a temporal.api.taskqueue.v1.TaskQueueStatus
				var tqstatus taskqueuepb.TaskQueueStatus
				if err := proto.Unmarshal(msg, &tqstatus); err == nil {
					resp.TaskQueueStatus = &tqstatus
				}
			}
		}
	}
	return resp, nil
}

// GetClusterInfo return information about Temporal deployment.
func (wh *WorkflowHandler) GetClusterInfo(ctx context.Context, _ *workflowservice.GetClusterInfoRequest) (_ *workflowservice.GetClusterInfoResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	metadata, err := wh.clusterMetadataManager.GetCurrentClusterMetadata(ctx)
	if err != nil {
		return nil, err
	}

	return &workflowservice.GetClusterInfoResponse{
		SupportedClients:  headers.SupportedClients,
		ServerVersion:     headers.ServerVersion,
		ClusterId:         metadata.ClusterId,
		VersionInfo:       metadata.VersionInfo,
		ClusterName:       metadata.ClusterName,
		HistoryShardCount: metadata.HistoryShardCount,
		PersistenceStore:  wh.persistenceExecutionName,
		VisibilityStore:   strings.Join(wh.visibilityMgr.GetStoreNames(), ","),
	}, nil
}

// GetSystemInfo returns information about the Temporal system.
func (wh *WorkflowHandler) GetSystemInfo(ctx context.Context, request *workflowservice.GetSystemInfoRequest) (_ *workflowservice.GetSystemInfoResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	return &workflowservice.GetSystemInfoResponse{
		ServerVersion: headers.ServerVersion,
		// Capabilities should be added as needed. In many cases, capabilities are
		// hardcoded boolean true values since older servers will respond with a
		// form of this message without the field which is implied false.
		Capabilities: &workflowservice.GetSystemInfoResponse_Capabilities{
			SignalAndQueryHeader:            true,
			InternalErrorDifferentiation:    true,
			ActivityFailureIncludeHeartbeat: true,
			SupportsSchedules:               true,
			EncodedFailureAttributes:        true,
			UpsertMemo:                      true,
			EagerWorkflowStart:              true,
			SdkMetadata:                     true,
			BuildIdBasedVersioning:          true,
			CountGroupByExecutionStatus:     true,
			Nexus:                           wh.config.EnableNexusAPIs(),
		},
	}, nil
}

// ListTaskQueuePartitions returns all the partition and host for a task queue.
func (wh *WorkflowHandler) ListTaskQueuePartitions(ctx context.Context, request *workflowservice.ListTaskQueuePartitionsRequest) (_ *workflowservice.ListTaskQueuePartitionsResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	namespaceName := namespace.Name(request.GetNamespace())
	if err := wh.validateTaskQueue(request.TaskQueue); err != nil {
		return nil, err
	}

	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespaceName)
	if err != nil {
		return nil, err
	}

	matchingResponse, err := wh.matchingClient.ListTaskQueuePartitions(ctx, &matchingservice.ListTaskQueuePartitionsRequest{
		NamespaceId: namespaceID.String(),
		Namespace:   request.GetNamespace(),
		TaskQueue:   request.TaskQueue,
	})

	if matchingResponse == nil {
		return nil, err
	}

	return &workflowservice.ListTaskQueuePartitionsResponse{
		ActivityTaskQueuePartitions: matchingResponse.ActivityTaskQueuePartitions,
		WorkflowTaskQueuePartitions: matchingResponse.WorkflowTaskQueuePartitions,
	}, err
}

// Creates a new schedule.
func (wh *WorkflowHandler) CreateSchedule(ctx context.Context, request *workflowservice.CreateScheduleRequest) (_ *workflowservice.CreateScheduleResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	if !wh.config.EnableSchedules(request.Namespace) {
		return nil, errSchedulesNotAllowed
	}

	workflowID := scheduler.WorkflowIDPrefix + request.ScheduleId

	if err := wh.validateWorkflowID(workflowID); err != nil {
		return nil, err
	}

	wh.logger.Debug("Received CreateSchedule", tag.ScheduleID(request.ScheduleId))

	if request.GetRequestId() == "" {
		return nil, errRequestIDNotSet
	}

	if len(request.GetRequestId()) > wh.config.MaxIDLengthLimit() {
		return nil, errRequestIDTooLong
	}

	namespaceName := namespace.Name(request.Namespace)
	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespaceName)
	if err != nil {
		return nil, err
	}

	if request.Schedule == nil {
		request.Schedule = &schedpb.Schedule{}
	}
	err = wh.canonicalizeScheduleSpec(request.Schedule)
	if err != nil {
		return nil, err
	}

	// Add namespace division before unaliasing search attributes.
	searchattribute.AddSearchAttribute(&request.SearchAttributes, searchattribute.TemporalNamespaceDivision, payload.EncodeString(scheduler.NamespaceDivision))

	sa, err := wh.unaliasedSearchAttributesFrom(request.GetSearchAttributes(), namespaceName)
	if err != nil {
		return nil, err
	}

	if err = wh.validateStartWorkflowArgsForSchedule(namespaceName, request.GetSchedule().GetAction().GetStartWorkflow()); err != nil {
		return nil, err
	}

	// size limits will be validated on history. note that the start workflow request is
	// embedded in the schedule, which is in the scheduler input. so if the scheduler itself
	// doesn't exceed the limit, the started workflows should be safe as well.

	// Set up input to scheduler workflow
	input := &schedspb.StartScheduleArgs{
		Schedule:     request.Schedule,
		InitialPatch: request.InitialPatch,
		State: &schedspb.InternalState{
			Namespace:     namespaceName.String(),
			NamespaceId:   namespaceID.String(),
			ScheduleId:    request.ScheduleId,
			ConflictToken: scheduler.InitialConflictToken,
		},
	}
	inputPayloads, err := sdk.PreferProtoDataConverter.ToPayloads(input)
	if err != nil {
		return nil, err
	}
	// Add initial memo for list schedules
	wh.addInitialScheduleMemo(request, input)
	// Create StartWorkflowExecutionRequest
	startReq := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:                request.Namespace,
		WorkflowId:               workflowID,
		WorkflowType:             &commonpb.WorkflowType{Name: scheduler.WorkflowType},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
		Input:                    inputPayloads,
		Identity:                 request.Identity,
		RequestId:                request.RequestId,
		WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
		Memo:                     request.Memo,
		SearchAttributes:         sa,
	}
	_, err = wh.historyClient.StartWorkflowExecution(
		ctx,
		common.CreateHistoryStartWorkflowRequest(
			namespaceID.String(),
			startReq,
			nil,
			nil,
			time.Now().UTC(),
		),
	)

	if err != nil {
		return nil, err
	}
	token := make([]byte, 8)
	binary.BigEndian.PutUint64(token, scheduler.InitialConflictToken)
	return &workflowservice.CreateScheduleResponse{
		ConflictToken: token,
	}, nil
}

// Validates inner start workflow request. Note that this can mutate search attributes if present.
func (wh *WorkflowHandler) validateStartWorkflowArgsForSchedule(
	namespaceName namespace.Name,
	startWorkflow *workflowpb.NewWorkflowExecutionInfo,
) error {
	if startWorkflow == nil {
		return nil
	}

	if err := wh.validateWorkflowID(startWorkflow.WorkflowId + scheduler.AppendedTimestampForValidation); err != nil {
		return err
	}

	if startWorkflow.WorkflowType == nil || startWorkflow.WorkflowType.GetName() == "" {
		return errWorkflowTypeNotSet
	}

	if len(startWorkflow.WorkflowType.GetName()) > wh.config.MaxIDLengthLimit() {
		return errWorkflowTypeTooLong
	}

	if err := wh.validateTaskQueue(startWorkflow.TaskQueue); err != nil {
		return err
	}

	if err := wh.validateStartWorkflowTimeouts(&workflowservice.StartWorkflowExecutionRequest{
		WorkflowExecutionTimeout: startWorkflow.WorkflowExecutionTimeout,
		WorkflowRunTimeout:       startWorkflow.WorkflowRunTimeout,
		WorkflowTaskTimeout:      startWorkflow.WorkflowTaskTimeout,
	}); err != nil {
		return err
	}

	if len(startWorkflow.CronSchedule) > 0 {
		return errCronNotAllowed
	}

	if startWorkflow.WorkflowIdReusePolicy != enumspb.WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED &&
		startWorkflow.WorkflowIdReusePolicy != enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE {
		return errIDReusePolicyNotAllowed
	}

	// Unalias startWorkflow search attributes only for validation.
	// Keep aliases in the request, because the request will be
	// sent back to frontend to start workflows, which will unalias at that point.
	unaliasedStartWorkflowSas, err := searchattribute.UnaliasFields(wh.saMapperProvider, startWorkflow.GetSearchAttributes(), namespaceName.String())
	if err != nil {
		return err
	}
	return wh.validateSearchAttributes(unaliasedStartWorkflowSas, namespaceName)
}

// Returns the schedule description and current state of an existing schedule.
func (wh *WorkflowHandler) DescribeSchedule(ctx context.Context, request *workflowservice.DescribeScheduleRequest) (_ *workflowservice.DescribeScheduleResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	if !wh.config.EnableSchedules(request.Namespace) {
		return nil, errSchedulesNotAllowed
	}

	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	workflowID := scheduler.WorkflowIDPrefix + request.ScheduleId
	execution := &commonpb.WorkflowExecution{WorkflowId: workflowID}

	// first describe to get memo and search attributes
	describeResponse, err := wh.historyClient.DescribeWorkflowExecution(ctx, &historyservice.DescribeWorkflowExecutionRequest{
		NamespaceId: namespaceID.String(),
		Request: &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: request.Namespace,
			Execution: execution,
		},
	})
	if err != nil {
		// TODO: rewrite "workflow" in error messages to "schedule"
		return nil, err
	}

	executionInfo := describeResponse.GetWorkflowExecutionInfo()
	if executionInfo.GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		// only treat running schedules as existing
		return nil, serviceerror.NewNotFound("schedule not found")
	}

	// map search attributes
	if sas := executionInfo.GetSearchAttributes(); sas != nil {
		saTypeMap, err := wh.saProvider.GetSearchAttributes(wh.visibilityMgr.GetIndexName(), false)
		if err != nil {
			return nil, serviceerror.NewUnavailable(fmt.Sprintf(errUnableToGetSearchAttributesMessage, err))
		}
		searchattribute.ApplyTypeMap(sas, saTypeMap)
		aliasedSas, err := searchattribute.AliasFields(wh.saMapperProvider, sas, request.GetNamespace())
		if err != nil {
			return nil, err
		}
		if aliasedSas != sas {
			executionInfo.SearchAttributes = aliasedSas
		}
	}

	// then query to get current state from the workflow itself
	req := &historyservice.QueryWorkflowRequest{
		NamespaceId: namespaceID.String(),
		Request: &workflowservice.QueryWorkflowRequest{
			Namespace: request.Namespace,
			Execution: execution,
			Query:     &querypb.WorkflowQuery{QueryType: scheduler.QueryNameDescribe},
		},
	}
	res, err := wh.historyClient.QueryWorkflow(ctx, req)
	if err != nil {
		return nil, err
	}

	var queryResponse schedspb.DescribeResponse
	err = payloads.Decode(res.GetResponse().GetQueryResult(), &queryResponse)
	if err != nil {
		return nil, err
	}

	err = wh.annotateSearchAttributesOfScheduledWorkflow(&queryResponse, request.GetNamespace())
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("describe schedule: %v", err))
	}
	// Search attributes in the Action are already in external ("aliased") form. Do not alias them here.

	// for all running workflows started by the schedule, we should check that they're still running
	origLen := len(queryResponse.Info.RunningWorkflows)
	queryResponse.Info.RunningWorkflows = util.FilterSlice(queryResponse.Info.RunningWorkflows, func(ex *commonpb.WorkflowExecution) bool {
		// we'll usually have just zero or one of these so we can just do them sequentially
		msResponse, err := wh.historyClient.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
			NamespaceId: namespaceID.String(),
			// Note: do not send runid here so that we always get the latest one
			Execution: &commonpb.WorkflowExecution{WorkflowId: ex.WorkflowId},
		})
		if err != nil {
			// if it's not found, it's certainly not running, so return false. if we got
			// another error, we don't know the state so assume it's still running.
			return !common.IsNotFoundError(err)
		}
		// return true if it is still running and is part of the chain the schedule started
		return msResponse.WorkflowStatus == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING &&
			msResponse.FirstExecutionRunId == ex.RunId
	})

	if len(queryResponse.Info.RunningWorkflows) < origLen {
		// we noticed some "running workflows" aren't running anymore. poke the workflow to
		// refresh, but don't wait for the state to change. ignore errors.
		go func() {
			disconnectedCtx := headers.SetCallerInfo(context.Background(), headers.NewBackgroundCallerInfo(request.Namespace))
			_, _ = wh.historyClient.SignalWorkflowExecution(disconnectedCtx, &historyservice.SignalWorkflowExecutionRequest{
				NamespaceId: namespaceID.String(),
				SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
					Namespace:         request.Namespace,
					WorkflowExecution: execution,
					SignalName:        scheduler.SignalNameRefresh,
					Identity:          "internal refresh from describe request",
					RequestId:         uuid.New(),
				},
			})
		}()
	}

	token := make([]byte, 8)
	binary.BigEndian.PutUint64(token, uint64(queryResponse.ConflictToken))

	searchAttributes := describeResponse.GetWorkflowExecutionInfo().GetSearchAttributes()
	searchAttributes = wh.cleanScheduleSearchAttributes(searchAttributes)

	memo := describeResponse.GetWorkflowExecutionInfo().GetMemo()
	memo = wh.cleanScheduleMemo(memo)

	scheduler.CleanSpec(queryResponse.Schedule.Spec)

	return &workflowservice.DescribeScheduleResponse{
		Schedule:         queryResponse.Schedule,
		Info:             queryResponse.Info,
		Memo:             memo,
		SearchAttributes: searchAttributes,
		ConflictToken:    token,
	}, nil
}

func (wh *WorkflowHandler) annotateSearchAttributesOfScheduledWorkflow(
	queryResponse *schedspb.DescribeResponse,
	nsName string,
) error {
	ei := wh.getScheduledWorkflowExecutionInfoFrom(queryResponse)
	if ei == nil {
		return nil
	}
	annotatedAttributes, err := wh.annotateSearchAttributes(ei.GetSearchAttributes(), nsName)
	if err != nil {
		return fmt.Errorf("annotate search attributes: %w", err)
	}
	ei.SearchAttributes = annotatedAttributes
	return nil
}

func (wh *WorkflowHandler) getScheduledWorkflowExecutionInfoFrom(
	queryResponse *schedspb.DescribeResponse,
) *workflowpb.NewWorkflowExecutionInfo {
	action := queryResponse.GetSchedule().GetAction().GetAction()
	startWorkflowAction, ok := action.(*schedpb.ScheduleAction_StartWorkflow)
	if !ok {
		return nil
	}
	return startWorkflowAction.StartWorkflow
}

func (wh *WorkflowHandler) annotateSearchAttributes(
	searchAttributes *commonpb.SearchAttributes,
	nsName string,
) (*commonpb.SearchAttributes, error) {
	unaliasedSearchAttrs, err := searchattribute.UnaliasFields(
		wh.saMapperProvider,
		searchAttributes,
		nsName,
	)
	if err != nil {
		return nil, fmt.Errorf("create annotations: %w", err)
	}
	saTypeMap, err := wh.saProvider.GetSearchAttributes(wh.visibilityMgr.GetIndexName(), false)
	if err != nil {
		return nil, fmt.Errorf("create annotations: %w", err)
	}
	searchattribute.ApplyTypeMap(unaliasedSearchAttrs, saTypeMap)
	annotatedAttributes, err := searchattribute.AliasFields(
		wh.saMapperProvider,
		unaliasedSearchAttrs,
		nsName,
	)
	if err != nil {
		return nil, fmt.Errorf("create annotations: %w", err)
	}
	return annotatedAttributes, nil
}

// Changes the configuration or state of an existing schedule.
func (wh *WorkflowHandler) UpdateSchedule(
	ctx context.Context,
	request *workflowservice.UpdateScheduleRequest,
) (_ *workflowservice.UpdateScheduleResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	if !wh.config.EnableSchedules(request.Namespace) {
		return nil, errSchedulesNotAllowed
	}

	if len(request.GetRequestId()) > wh.config.MaxIDLengthLimit() {
		return nil, errRequestIDTooLong
	}

	workflowID := scheduler.WorkflowIDPrefix + request.ScheduleId

	namespaceName := namespace.Name(request.Namespace)
	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	if request.Schedule == nil {
		request.Schedule = &schedpb.Schedule{}
	}
	err = wh.canonicalizeScheduleSpec(request.Schedule)
	if err != nil {
		return nil, err
	}

	// Need to validate the custom search attributes, but need to pass the original
	// custom search attributes map to FullUpdateRequest because it needs to call
	// UpsertSearchAttributes which expects aliased names.
	_, err = wh.unaliasedSearchAttributesFrom(request.GetSearchAttributes(), namespaceName)
	if err != nil {
		return nil, err
	}

	if err = wh.validateStartWorkflowArgsForSchedule(
		namespaceName,
		request.GetSchedule().GetAction().GetStartWorkflow(),
	); err != nil {
		return nil, err
	}

	input := &schedspb.FullUpdateRequest{
		Schedule:         request.Schedule,
		SearchAttributes: request.SearchAttributes,
	}
	if len(request.ConflictToken) >= 8 {
		input.ConflictToken = int64(binary.BigEndian.Uint64(request.ConflictToken))
	}
	inputPayloads, err := sdk.PreferProtoDataConverter.ToPayloads(input)
	if err != nil {
		return nil, err
	}

	sizeLimitError := wh.config.BlobSizeLimitError(request.GetNamespace())
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(request.GetNamespace())
	if err := common.CheckEventBlobSizeLimit(
		inputPayloads.Size(),
		sizeLimitWarn,
		sizeLimitError,
		namespaceID.String(),
		workflowID,
		"", // don't have runid yet
		wh.metricsScope(ctx).WithTags(metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UNSPECIFIED.String())),
		wh.throttledLogger,
		tag.BlobSizeViolationOperation("UpdateSchedule"),
	); err != nil {
		return nil, err
	}

	_, err = wh.historyClient.SignalWorkflowExecution(ctx, &historyservice.SignalWorkflowExecutionRequest{
		NamespaceId: namespaceID.String(),
		SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         request.Namespace,
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
			SignalName:        scheduler.SignalNameUpdate,
			Input:             inputPayloads,
			Identity:          request.Identity,
			RequestId:         request.RequestId,
		},
	})
	if err != nil {
		return nil, err
	}

	return &workflowservice.UpdateScheduleResponse{}, nil
}

// Makes a specific change to a schedule or triggers an immediate action.
func (wh *WorkflowHandler) PatchSchedule(ctx context.Context, request *workflowservice.PatchScheduleRequest) (_ *workflowservice.PatchScheduleResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	if !wh.config.EnableSchedules(request.Namespace) {
		return nil, errSchedulesNotAllowed
	}

	if len(request.GetRequestId()) > wh.config.MaxIDLengthLimit() {
		return nil, errRequestIDTooLong
	}

	workflowID := scheduler.WorkflowIDPrefix + request.ScheduleId

	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	if len(request.Patch.Pause) > common.ScheduleNotesSizeLimit ||
		len(request.Patch.Unpause) > common.ScheduleNotesSizeLimit {
		return nil, errNotesTooLong
	}

	inputPayloads, err := sdk.PreferProtoDataConverter.ToPayloads(request.Patch)
	if err != nil {
		return nil, err
	}

	sizeLimitError := wh.config.BlobSizeLimitError(request.GetNamespace())
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(request.GetNamespace())
	if err := common.CheckEventBlobSizeLimit(
		inputPayloads.Size(),
		sizeLimitWarn,
		sizeLimitError,
		namespaceID.String(),
		workflowID,
		"", // don't have runid yet
		wh.metricsScope(ctx).WithTags(metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UNSPECIFIED.String())),
		wh.throttledLogger,
		tag.BlobSizeViolationOperation("PatchSchedule"),
	); err != nil {
		return nil, err
	}

	_, err = wh.historyClient.SignalWorkflowExecution(ctx, &historyservice.SignalWorkflowExecutionRequest{
		NamespaceId: namespaceID.String(),
		SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         request.Namespace,
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
			SignalName:        scheduler.SignalNamePatch,
			Input:             inputPayloads,
			Identity:          request.Identity,
			RequestId:         request.RequestId,
		},
	})
	if err != nil {
		return nil, err
	}

	return &workflowservice.PatchScheduleResponse{}, nil
}

// Lists matching times within a range.
func (wh *WorkflowHandler) ListScheduleMatchingTimes(ctx context.Context, request *workflowservice.ListScheduleMatchingTimesRequest) (_ *workflowservice.ListScheduleMatchingTimesResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	if !wh.config.EnableSchedules(request.Namespace) {
		return nil, errSchedulesNotAllowed
	}

	workflowID := scheduler.WorkflowIDPrefix + request.ScheduleId

	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	queryPayload, err := sdk.PreferProtoDataConverter.ToPayloads(request)
	if err != nil {
		return nil, err
	}

	sizeLimitError := wh.config.BlobSizeLimitError(request.GetNamespace())
	sizeLimitWarn := wh.config.BlobSizeLimitWarn(request.GetNamespace())
	if err := common.CheckEventBlobSizeLimit(
		queryPayload.Size(),
		sizeLimitWarn,
		sizeLimitError,
		namespaceID.String(),
		workflowID,
		"",
		wh.metricsScope(ctx).WithTags(metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UNSPECIFIED.String())),
		wh.throttledLogger,
		tag.BlobSizeViolationOperation("ListScheduleMatchingTimes")); err != nil {
		return nil, err
	}

	req := &historyservice.QueryWorkflowRequest{
		NamespaceId: namespaceID.String(),
		Request: &workflowservice.QueryWorkflowRequest{
			Namespace: request.Namespace,
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
			Query: &querypb.WorkflowQuery{
				QueryType: scheduler.QueryNameListMatchingTimes,
				QueryArgs: queryPayload,
			},
		},
	}
	res, err := wh.historyClient.QueryWorkflow(ctx, req)
	if err != nil {
		return nil, err
	}

	var response workflowservice.ListScheduleMatchingTimesResponse
	err = payloads.Decode(res.GetResponse().GetQueryResult(), &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

// Deletes a schedule, removing it from the system.
func (wh *WorkflowHandler) DeleteSchedule(ctx context.Context, request *workflowservice.DeleteScheduleRequest) (_ *workflowservice.DeleteScheduleResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	if !wh.config.EnableSchedules(request.Namespace) {
		return nil, errSchedulesNotAllowed
	}

	workflowID := scheduler.WorkflowIDPrefix + request.ScheduleId

	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	_, err = wh.historyClient.TerminateWorkflowExecution(ctx, &historyservice.TerminateWorkflowExecutionRequest{
		NamespaceId: namespaceID.String(),
		TerminateRequest: &workflowservice.TerminateWorkflowExecutionRequest{
			Namespace:         request.Namespace,
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
			Reason:            "terminated by DeleteSchedule",
			Identity:          request.Identity,
		},
	})
	if err != nil {
		return nil, err
	}

	return &workflowservice.DeleteScheduleResponse{}, nil
}

// List all schedules in a namespace.
func (wh *WorkflowHandler) ListSchedules(
	ctx context.Context,
	request *workflowservice.ListSchedulesRequest,
) (_ *workflowservice.ListSchedulesResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	if !wh.config.EnableSchedules(request.Namespace) {
		return nil, errSchedulesNotAllowed
	}

	maxPageSize := int32(wh.config.VisibilityMaxPageSize(request.GetNamespace()))
	if request.GetMaximumPageSize() <= 0 || request.GetMaximumPageSize() > maxPageSize {
		request.MaximumPageSize = maxPageSize
	}

	namespaceName := namespace.Name(request.GetNamespace())
	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespaceName)
	if err != nil {
		return nil, err
	}

	if wh.config.DisableListVisibilityByFilter(namespaceName.String()) {
		return nil, errListNotAllowed
	}

	query := ""
	if strings.TrimSpace(request.Query) != "" {
		saNameType, err := wh.saProvider.GetSearchAttributes(wh.visibilityMgr.GetIndexName(), false)
		if err != nil {
			return nil, serviceerror.NewUnavailable(fmt.Sprintf(errUnableToGetSearchAttributesMessage, err))
		}
		if err := scheduler.ValidateVisibilityQuery(request.Query, saNameType); err != nil {
			return nil, err
		}
		query = fmt.Sprintf("%s AND (%s)", scheduler.VisibilityBaseListQuery, request.Query)
	} else {
		query = scheduler.VisibilityBaseListQuery
	}

	persistenceResp, err := wh.visibilityMgr.ListWorkflowExecutions(
		ctx,
		&manager.ListWorkflowExecutionsRequestV2{
			NamespaceID:   namespaceID,
			Namespace:     namespaceName,
			PageSize:      int(request.GetMaximumPageSize()),
			NextPageToken: request.NextPageToken,
			Query:         query,
		},
	)
	if err != nil {
		return nil, err
	}

	schedules := make([]*schedpb.ScheduleListEntry, len(persistenceResp.Executions))
	for i, ex := range persistenceResp.Executions {
		memo := ex.GetMemo()
		info := wh.decodeScheduleListInfo(memo)
		memo = wh.cleanScheduleMemo(memo)
		workflowID := ex.GetExecution().GetWorkflowId()
		scheduleID := strings.TrimPrefix(workflowID, scheduler.WorkflowIDPrefix)
		schedules[i] = &schedpb.ScheduleListEntry{
			ScheduleId:       scheduleID,
			Memo:             memo,
			SearchAttributes: wh.cleanScheduleSearchAttributes(ex.GetSearchAttributes()),
			Info:             info,
		}
	}

	return &workflowservice.ListSchedulesResponse{
		Schedules:     schedules,
		NextPageToken: persistenceResp.NextPageToken,
	}, nil
}

func (wh *WorkflowHandler) UpdateWorkflowExecution(
	ctx context.Context,
	request *workflowservice.UpdateWorkflowExecutionRequest,
) (_ *workflowservice.UpdateWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if err := wh.prepareUpdateWorkflowRequest(request); err != nil {
		return nil, err
	}

	nsID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	switch request.WaitPolicy.LifecycleStage { // nolint:exhaustive
	case enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED:
		metrics.WorkflowExecutionUpdateWaitStageAccepted.With(wh.metricsScope(ctx)).Record(1)
	case enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED:
		metrics.WorkflowExecutionUpdateWaitStageCompleted.With(wh.metricsScope(ctx)).Record(1)
	}

	histResp, err := wh.historyClient.UpdateWorkflowExecution(ctx, &historyservice.UpdateWorkflowExecutionRequest{
		NamespaceId: nsID.String(),
		Request:     request,
	})

	return histResp.GetResponse(), err
}

func (wh *WorkflowHandler) prepareUpdateWorkflowRequest(
	request *workflowservice.UpdateWorkflowExecutionRequest,
) error {
	if request == nil {
		return errRequestNotSet
	}

	if err := validateExecution(request.GetWorkflowExecution()); err != nil {
		return err
	}

	if request.GetRequest().GetMeta() == nil {
		return errUpdateMetaNotSet
	}

	if len(request.GetRequest().GetMeta().GetUpdateId()) > wh.config.MaxIDLengthLimit() {
		return errUpdateIDTooLong
	}

	if request.GetRequest().GetMeta().GetUpdateId() == "" {
		request.GetRequest().GetMeta().UpdateId = uuid.New()
	}

	if request.GetRequest().GetInput() == nil {
		return errUpdateInputNotSet
	}

	if request.GetRequest().GetInput().GetName() == "" {
		return errUpdateNameNotSet
	}

	if request.GetWaitPolicy() == nil {
		request.WaitPolicy = &updatepb.WaitPolicy{}
	}

	if !wh.config.EnableUpdateWorkflowExecution(request.Namespace) {
		return errUpdateWorkflowExecutionAPINotAllowed
	}

	enums.SetDefaultUpdateWorkflowExecutionLifecycleStage(&request.GetWaitPolicy().LifecycleStage)

	if request.WaitPolicy.LifecycleStage == enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED {
		return errUpdateWorkflowExecutionAsyncAdmittedNotAllowed
	}

	if request.WaitPolicy.LifecycleStage == enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED &&
		!wh.config.EnableUpdateWorkflowExecutionAsyncAccepted(request.Namespace) {
		return errUpdateWorkflowExecutionAsyncAcceptedNotAllowed
	}

	return nil
}

func (wh *WorkflowHandler) PollWorkflowExecutionUpdate(
	ctx context.Context,
	request *workflowservice.PollWorkflowExecutionUpdateRequest,
) (_ *workflowservice.PollWorkflowExecutionUpdateResponse, retError error) {
	if request == nil {
		return nil, errRequestNotSet
	}

	if request.GetUpdateRef() == nil {
		return nil, errUpdateRefNotSet
	}

	if request.GetWaitPolicy() == nil {
		request.WaitPolicy = &updatepb.WaitPolicy{}
	}

	nsID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	if !wh.config.EnableUpdateWorkflowExecution(request.Namespace) {
		return nil, errUpdateWorkflowExecutionAPINotAllowed
	}

	ctx, cancel := context.WithTimeout(ctx, frontend.DefaultLongPollTimeout)
	defer cancel()

	histResp, err := wh.historyClient.PollWorkflowExecutionUpdate(
		ctx,
		&historyservice.PollWorkflowExecutionUpdateRequest{
			NamespaceId: nsID.String(),
			Request:     request,
		},
	)
	if err != nil {
		return nil, err
	}
	return histResp.GetResponse(), nil
}

func (wh *WorkflowHandler) UpdateWorkerBuildIdCompatibility(ctx context.Context, request *workflowservice.UpdateWorkerBuildIdCompatibilityRequest) (_ *workflowservice.UpdateWorkerBuildIdCompatibilityResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	if !wh.config.EnableWorkerVersioningData(request.Namespace) {
		return nil, errWorkerVersioningNotAllowed
	}

	if err := wh.validateBuildIdCompatibilityUpdate(request); err != nil {
		return nil, err
	}

	taskQueue := &taskqueuepb.TaskQueue{Name: request.GetTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	if err := wh.validateTaskQueue(taskQueue); err != nil {
		return nil, err
	}

	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	matchingResponse, err := wh.matchingClient.UpdateWorkerBuildIdCompatibility(ctx, &matchingservice.UpdateWorkerBuildIdCompatibilityRequest{
		NamespaceId: namespaceID.String(),
		TaskQueue:   request.GetTaskQueue(),
		Operation: &matchingservice.UpdateWorkerBuildIdCompatibilityRequest_ApplyPublicRequest_{
			ApplyPublicRequest: &matchingservice.UpdateWorkerBuildIdCompatibilityRequest_ApplyPublicRequest{
				Request: request,
			},
		},
	})

	if matchingResponse == nil {
		return nil, err
	}

	return &workflowservice.UpdateWorkerBuildIdCompatibilityResponse{}, err
}

func (wh *WorkflowHandler) GetWorkerBuildIdCompatibility(ctx context.Context, request *workflowservice.GetWorkerBuildIdCompatibilityRequest) (_ *workflowservice.GetWorkerBuildIdCompatibilityResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	if !wh.config.EnableWorkerVersioningData(request.Namespace) {
		return nil, errWorkerVersioningNotAllowed
	}

	taskQueue := &taskqueuepb.TaskQueue{Name: request.GetTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	if err := wh.validateTaskQueue(taskQueue); err != nil {
		return nil, err
	}

	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	matchingResponse, err := wh.matchingClient.GetWorkerBuildIdCompatibility(ctx, &matchingservice.GetWorkerBuildIdCompatibilityRequest{
		NamespaceId: namespaceID.String(),
		Request:     request,
	})

	if matchingResponse == nil {
		return nil, err
	}

	return matchingResponse.Response, err
}

func (wh *WorkflowHandler) UpdateWorkerVersioningRules(ctx context.Context, request *workflowservice.UpdateWorkerVersioningRulesRequest) (_ *workflowservice.UpdateWorkerVersioningRulesResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	if !wh.config.EnableWorkerVersioningRules(request.Namespace) {
		return nil, errWorkerVersioningNotAllowed
	}

	taskQueue := &taskqueuepb.TaskQueue{Name: request.GetTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	if err := wh.validateTaskQueue(taskQueue); err != nil {
		return nil, err
	}

	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	matchingResponse, err := wh.matchingClient.UpdateWorkerVersioningRules(ctx, &matchingservice.UpdateWorkerVersioningRulesRequest{
		NamespaceId: namespaceID.String(),
		TaskQueue:   request.GetTaskQueue(),
		Command: &matchingservice.UpdateWorkerVersioningRulesRequest_Request{
			Request: request,
		},
	})

	if matchingResponse == nil {
		return nil, err
	}

	return matchingResponse.Response, err
}

func (wh *WorkflowHandler) GetWorkerVersioningRules(ctx context.Context, request *workflowservice.GetWorkerVersioningRulesRequest) (_ *workflowservice.GetWorkerVersioningRulesResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	if !wh.config.EnableWorkerVersioningRules(request.Namespace) {
		return nil, errWorkerVersioningNotAllowed
	}

	taskQueue := &taskqueuepb.TaskQueue{Name: request.GetTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	if err := wh.validateTaskQueue(taskQueue); err != nil {
		return nil, err
	}

	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	matchingResponse, err := wh.matchingClient.GetWorkerVersioningRules(ctx, &matchingservice.GetWorkerVersioningRulesRequest{
		NamespaceId: namespaceID.String(),
		TaskQueue:   request.GetTaskQueue(),
		Command: &matchingservice.GetWorkerVersioningRulesRequest_Request{
			Request: &workflowservice.GetWorkerVersioningRulesRequest{
				Namespace: request.GetNamespace(),
				TaskQueue: request.GetTaskQueue(),
			},
		},
	})

	if matchingResponse == nil {
		return nil, err
	}

	return matchingResponse.Response, err
}

func (wh *WorkflowHandler) GetWorkerTaskReachability(ctx context.Context, request *workflowservice.GetWorkerTaskReachabilityRequest) (_ *workflowservice.GetWorkerTaskReachabilityResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	if !wh.config.EnableWorkerVersioningData(request.Namespace) {
		return nil, errWorkerVersioningNotAllowed
	}

	if len(request.GetBuildIds()) == 0 {
		return nil, serviceerror.NewInvalidArgument("Must query at least one build ID (or empty string for unversioned worker)")
	}
	if len(request.GetBuildIds()) > wh.config.ReachabilityQueryBuildIdLimit() {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("Too many build ids queried at once, limit: %d", wh.config.ReachabilityQueryBuildIdLimit()))
	}
	gotUnversionedRequest := false
	for _, buildId := range request.GetBuildIds() {
		if buildId == "" {
			gotUnversionedRequest = true
		}
		if len(buildId) > wh.config.WorkerBuildIdSizeLimit() {
			return nil, errBuildIdTooLong
		}
	}
	if gotUnversionedRequest && len(request.GetTaskQueues()) == 0 {
		return nil, serviceerror.NewInvalidArgument("Cannot get reachability of an unversioned worker without specifying at least one task queue (empty build ID is interpereted as unversioned)")
	}

	for _, taskQueue := range request.GetTaskQueues() {
		taskQueue := &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
		if err := wh.validateTaskQueue(taskQueue); err != nil {
			return nil, err
		}
	}

	ns, err := wh.namespaceRegistry.GetNamespace(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	response, err := wh.getWorkerTaskReachabilityValidated(ctx, ns, request)
	if err != nil {
		var invalidArgument *serviceerror.InvalidArgument
		if errors.As(err, &invalidArgument) {
			return nil, err
		}
		// Intentionally treat all errors as internal errors
		wh.logger.Error("Failed getting worker task reachability", tag.Error(err))
		return nil, serviceerror.NewInternal("Internal error")
	}
	return response, nil
}

func (wh *WorkflowHandler) StartBatchOperation(
	ctx context.Context,
	request *workflowservice.StartBatchOperationRequest,
) (_ *workflowservice.StartBatchOperationResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if err := wh.versionChecker.ClientSupported(ctx); err != nil {
		return nil, err
	}

	if request == nil {
		return nil, errRequestNotSet
	}

	if len(request.GetJobId()) == 0 {
		return nil, errBatchJobIDNotSet
	}
	if len(request.Namespace) == 0 {
		return nil, errNamespaceNotSet
	}
	if len(request.VisibilityQuery) == 0 && len(request.Executions) == 0 {
		return nil, errBatchOpsWorkflowFilterNotSet
	}
	if len(request.VisibilityQuery) != 0 && len(request.Executions) != 0 {
		return nil, errBatchOpsWorkflowFiltersNotAllowed
	}
	if len(request.Executions) > wh.config.MaxExecutionCountBatchOperation(request.Namespace) {
		return nil, errBatchOpsMaxWorkflowExecutionCount
	}
	if len(request.Reason) == 0 {
		return nil, errReasonNotSet
	}
	if request.Operation == nil {
		return nil, errBatchOperationNotSet
	}

	if !wh.config.EnableBatcher(request.Namespace) {
		return nil, errBatchAPINotAllowed
	}

	// Validate concurrent batch operation
	maxConcurrentBatchOperation := wh.config.MaxConcurrentBatchOperation(request.GetNamespace())
	countResp, err := wh.CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{
		Namespace: request.GetNamespace(),
		Query:     batcher.OpenBatchOperationQuery,
	})
	openBatchOperationCount := 0
	if err == nil {
		openBatchOperationCount = int(countResp.GetCount())
	} else {
		if !errors.Is(err, store.OperationNotSupportedErr) {
			return nil, err
		}
		// Some std visibility stores don't yet support CountWorkflowExecutions, even though some
		// batch operations are still possible on those store (eg. by specyfing a list of Executions
		// rather than a VisibilityQuery). Fallback to ListOpenWorkflowExecutions in these cases.
		// TODO: Remove this once all std visibility stores support CountWorkflowExecutions.
		nextPageToken := []byte{}
		for nextPageToken != nil && openBatchOperationCount < maxConcurrentBatchOperation {
			listResp, err := wh.ListOpenWorkflowExecutions(ctx, &workflowservice.ListOpenWorkflowExecutionsRequest{
				Namespace: request.GetNamespace(),
				Filters: &workflowservice.ListOpenWorkflowExecutionsRequest_TypeFilter{
					TypeFilter: &filterpb.WorkflowTypeFilter{
						Name: batcher.BatchWFTypeName,
					},
				},
				MaximumPageSize: int32(maxConcurrentBatchOperation - openBatchOperationCount),
				NextPageToken:   nextPageToken,
			})
			if err != nil {
				return nil, err
			}
			openBatchOperationCount += len(listResp.Executions)
			nextPageToken = listResp.NextPageToken
		}
	}
	if openBatchOperationCount >= maxConcurrentBatchOperation {
		return nil, &serviceerror.ResourceExhausted{
			Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
			Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
			Message: "Max concurrent batch operations is reached",
		}
	}

	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}
	var identity string
	var operationType string
	var signalParams batcher.SignalParams
	var resetParams batcher.ResetParams
	switch op := request.Operation.(type) {
	case *workflowservice.StartBatchOperationRequest_TerminationOperation:
		identity = op.TerminationOperation.GetIdentity()
		operationType = batcher.BatchTypeTerminate
	case *workflowservice.StartBatchOperationRequest_SignalOperation:
		identity = op.SignalOperation.GetIdentity()
		operationType = batcher.BatchTypeSignal
		signalParams.SignalName = op.SignalOperation.GetSignal()
		signalParams.Input = op.SignalOperation.GetInput()
	case *workflowservice.StartBatchOperationRequest_CancellationOperation:
		identity = op.CancellationOperation.GetIdentity()
		operationType = batcher.BatchTypeCancel
	case *workflowservice.StartBatchOperationRequest_DeletionOperation:
		identity = op.DeletionOperation.GetIdentity()
		operationType = batcher.BatchTypeDelete
	case *workflowservice.StartBatchOperationRequest_ResetOperation:
		identity = op.ResetOperation.GetIdentity()
		operationType = batcher.BatchTypeReset
		if op.ResetOperation.Options != nil {
			if op.ResetOperation.Options.Target == nil {
				return nil, serviceerror.NewInvalidArgument("batch reset missing target")
			}
			encoded, err := op.ResetOperation.Options.Marshal()
			if err != nil {
				return nil, err
			}
			resetParams.ResetOptions = encoded
		} else {
			// TODO: remove support for old fields later
			resetType := op.ResetOperation.GetResetType()
			if _, ok := enumspb.ResetType_name[int32(resetType)]; !ok || resetType == enumspb.RESET_TYPE_UNSPECIFIED {
				return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("unknown batch reset type %v", resetType))
			}
			resetParams.ResetType = resetType
			resetParams.ResetReapplyType = op.ResetOperation.GetResetReapplyType()
		}

	default:
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("The operation type %T is not supported", op))
	}

	input := &batcher.BatchParams{
		Namespace:       request.GetNamespace(),
		Query:           request.GetVisibilityQuery(),
		Executions:      request.GetExecutions(),
		Reason:          request.GetReason(),
		BatchType:       operationType,
		RPS:             float64(request.GetMaxOperationsPerSecond()),
		TerminateParams: batcher.TerminateParams{},
		CancelParams:    batcher.CancelParams{},
		SignalParams:    signalParams,
		DeleteParams:    batcher.DeleteParams{},
		ResetParams:     resetParams,
	}
	inputPayload, err := sdk.PreferProtoDataConverter.ToPayloads(input)
	if err != nil {
		return nil, err
	}

	memo := &commonpb.Memo{
		Fields: map[string]*commonpb.Payload{
			batcher.BatchOperationTypeMemo: payload.EncodeString(operationType),
			batcher.BatchReasonMemo:        payload.EncodeString(request.GetReason()),
		},
	}

	// Add pre-define search attributes
	var searchAttributes *commonpb.SearchAttributes
	searchattribute.AddSearchAttribute(&searchAttributes, searchattribute.BatcherUser, payload.EncodeString(identity))
	searchattribute.AddSearchAttribute(&searchAttributes, searchattribute.TemporalNamespaceDivision, payload.EncodeString(batcher.NamespaceDivision))

	startReq := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:                request.Namespace,
		WorkflowId:               request.GetJobId(),
		WorkflowType:             &commonpb.WorkflowType{Name: batcher.BatchWFTypeName},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
		Input:                    inputPayload,
		Identity:                 identity,
		RequestId:                uuid.New(),
		WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
		WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		Memo:                     memo,
		SearchAttributes:         searchAttributes,
	}

	_, err = wh.historyClient.StartWorkflowExecution(
		ctx,
		common.CreateHistoryStartWorkflowRequest(
			namespaceID.String(),
			startReq,
			nil,
			nil,
			time.Now().UTC(),
		),
	)
	if err != nil {
		return nil, err
	}
	return &workflowservice.StartBatchOperationResponse{}, nil
}

func (wh *WorkflowHandler) StopBatchOperation(
	ctx context.Context,
	request *workflowservice.StopBatchOperationRequest,
) (_ *workflowservice.StopBatchOperationResponse, retError error) {

	defer log.CapturePanic(wh.logger, &retError)

	if err := wh.versionChecker.ClientSupported(ctx); err != nil {
		return nil, err
	}

	if request == nil {
		return nil, errRequestNotSet
	}

	if len(request.GetJobId()) == 0 {
		return nil, errBatchJobIDNotSet
	}
	if len(request.Namespace) == 0 {
		return nil, errNamespaceNotSet
	}
	if len(request.Reason) == 0 {
		return nil, errReasonNotSet
	}

	if !wh.config.EnableBatcher(request.Namespace) {
		return nil, errBatchAPINotAllowed
	}

	terminateReq := &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: request.GetNamespace(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: request.GetJobId(),
		},
		Reason:   request.GetReason(),
		Identity: request.GetIdentity(),
	}
	_, err := wh.TerminateWorkflowExecution(ctx, terminateReq)
	if err != nil {
		return nil, err
	}
	return &workflowservice.StopBatchOperationResponse{}, nil
}

func (wh *WorkflowHandler) DescribeBatchOperation(
	ctx context.Context,
	request *workflowservice.DescribeBatchOperationRequest,
) (_ *workflowservice.DescribeBatchOperationResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if err := wh.versionChecker.ClientSupported(ctx); err != nil {
		return nil, err
	}

	if request == nil {
		return nil, errRequestNotSet
	}

	if len(request.GetJobId()) == 0 {
		return nil, errBatchJobIDNotSet
	}
	if len(request.Namespace) == 0 {
		return nil, errNamespaceNotSet
	}

	if !wh.config.EnableBatcher(request.Namespace) {
		return nil, errBatchAPINotAllowed
	}

	execution := &commonpb.WorkflowExecution{
		WorkflowId: request.GetJobId(),
		RunId:      "",
	}
	resp, err := wh.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: request.GetNamespace(),
		Execution: execution,
	})
	if err != nil {
		return nil, err
	}

	executionInfo := resp.GetWorkflowExecutionInfo()
	operationState := getBatchOperationState(executionInfo.GetStatus())
	memo := executionInfo.GetMemo().GetFields()
	typePayload := memo[batcher.BatchOperationTypeMemo]
	operationReason := memo[batcher.BatchReasonMemo]
	var reason string
	err = payload.Decode(operationReason, &reason)
	if err != nil {
		return nil, err
	}
	var identity string
	encodedBatcherIdentity := executionInfo.GetSearchAttributes().GetIndexedFields()[searchattribute.BatcherUser]
	err = payload.Decode(encodedBatcherIdentity, &identity)
	if err != nil {
		return nil, err
	}
	var operationTypeString string
	err = payload.Decode(typePayload, &operationTypeString)
	if err != nil {
		return nil, err
	}
	var operationType enumspb.BatchOperationType
	switch operationTypeString {
	case batcher.BatchTypeCancel:
		operationType = enumspb.BATCH_OPERATION_TYPE_CANCEL
	case batcher.BatchTypeSignal:
		operationType = enumspb.BATCH_OPERATION_TYPE_SIGNAL
	case batcher.BatchTypeTerminate:
		operationType = enumspb.BATCH_OPERATION_TYPE_TERMINATE
	case batcher.BatchTypeDelete:
		operationType = enumspb.BATCH_OPERATION_TYPE_DELETE
	case batcher.BatchTypeReset:
		operationType = enumspb.BATCH_OPERATION_TYPE_RESET
	default:
		operationType = enumspb.BATCH_OPERATION_TYPE_UNSPECIFIED
		wh.throttledLogger.Warn("Unknown batch operation type", tag.NewStringTag("batch-operation-type", operationTypeString))
	}

	batchOperationResp := &workflowservice.DescribeBatchOperationResponse{
		OperationType: operationType,
		JobId:         executionInfo.Execution.GetWorkflowId(),
		State:         operationState,
		StartTime:     executionInfo.StartTime,
		CloseTime:     executionInfo.CloseTime,
		Identity:      identity,
		Reason:        reason,
	}
	if executionInfo.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
		stats, err := wh.getCompletedBatchOperationStats(memo)
		if err != nil {
			return nil, err
		}
		batchOperationResp.TotalOperationCount = int64(stats.NumSuccess + stats.NumFailure)
		batchOperationResp.FailureOperationCount = int64(stats.NumFailure)
		batchOperationResp.CompleteOperationCount = int64(stats.NumSuccess)
	} else {
		if len(resp.GetPendingActivities()) > 0 {
			hbdPayload := resp.GetPendingActivities()[0].HeartbeatDetails
			var hbd batcher.HeartBeatDetails
			err = payloads.Decode(hbdPayload, &hbd)
			if err != nil {
				return nil, err
			}
			batchOperationResp.TotalOperationCount = hbd.TotalEstimate
			batchOperationResp.CompleteOperationCount = int64(hbd.SuccessCount)
			batchOperationResp.FailureOperationCount = int64(hbd.ErrorCount)
		}
	}
	return batchOperationResp, nil
}

func (wh *WorkflowHandler) getCompletedBatchOperationStats(memo map[string]*commonpb.Payload) (stats batcher.BatchOperationStats, err error) {
	statsPayload, ok := memo[batcher.BatchOperationStatsMemo]
	if !ok {
		return stats, errors.New("batch operation stats are not present in the memo")
	}
	err = payload.Decode(statsPayload, &stats)
	return stats, err
}

func (wh *WorkflowHandler) ListBatchOperations(
	ctx context.Context,
	request *workflowservice.ListBatchOperationsRequest,
) (_ *workflowservice.ListBatchOperationsResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if err := wh.versionChecker.ClientSupported(ctx); err != nil {
		return nil, err
	}

	if request == nil {
		return nil, errRequestNotSet
	}

	if len(request.Namespace) == 0 {
		return nil, errNamespaceNotSet
	}

	if !wh.config.EnableBatcher(request.Namespace) {
		return nil, errBatchAPINotAllowed
	}

	maxPageSize := int32(wh.config.VisibilityMaxPageSize(request.GetNamespace()))
	if request.GetPageSize() <= 0 || request.GetPageSize() > maxPageSize {
		request.PageSize = maxPageSize
	}

	resp, err := wh.ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace:     request.GetNamespace(),
		PageSize:      request.PageSize,
		NextPageToken: request.GetNextPageToken(),
		Query: fmt.Sprintf("%s = '%s' and %s = '%s'",
			searchattribute.WorkflowType,
			batcher.BatchWFTypeName,
			searchattribute.TemporalNamespaceDivision,
			batcher.NamespaceDivision,
		),
	})
	if err != nil {
		return nil, err
	}

	var operations []*batchpb.BatchOperationInfo
	for _, execution := range resp.GetExecutions() {
		operations = append(operations, &batchpb.BatchOperationInfo{
			JobId:     execution.GetExecution().GetWorkflowId(),
			State:     getBatchOperationState(execution.GetStatus()),
			StartTime: execution.GetStartTime(),
			CloseTime: execution.GetCloseTime(),
		})
	}
	return &workflowservice.ListBatchOperationsResponse{
		OperationInfo: operations,
		NextPageToken: resp.NextPageToken,
	}, nil
}

func (wh *WorkflowHandler) PollNexusTaskQueue(ctx context.Context, request *workflowservice.PollNexusTaskQueueRequest) (_ *workflowservice.PollNexusTaskQueueResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	callTime := time.Now().UTC()

	if request == nil {
		return nil, errRequestNotSet
	}

	wh.logger.Debug("Received PollNexusTaskQueue")
	if err := common.ValidateLongPollContextTimeout(ctx, "PollNexusTaskQueue", wh.throttledLogger); err != nil {
		return nil, err
	}

	namespaceName := namespace.Name(request.GetNamespace())
	if err := wh.validateTaskQueue(request.TaskQueue); err != nil {
		return nil, err
	}
	if len(request.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return nil, errIdentityTooLong
	}

	if err := wh.validateVersioningInfo(request.Namespace, request.WorkerVersionCapabilities, request.TaskQueue); err != nil {
		return nil, err
	}

	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespaceName)
	if err != nil {
		return nil, err
	}

	if contextNearDeadline(ctx, longPollTailRoom) {
		return &workflowservice.PollNexusTaskQueueResponse{}, nil
	}

	pollerID := uuid.New()
	matchingResponse, err := wh.matchingClient.PollNexusTaskQueue(ctx, &matchingservice.PollNexusTaskQueueRequest{
		NamespaceId: namespaceID.String(),
		PollerId:    pollerID,
		Request:     request,
	})
	if err != nil {
		contextWasCanceled := wh.cancelOutstandingPoll(ctx, namespaceID, enumspb.TASK_QUEUE_TYPE_NEXUS, request.TaskQueue, pollerID)
		if contextWasCanceled {
			// Clear error as we don't want to report context cancellation error to count against our SLA.
			return &workflowservice.PollNexusTaskQueueResponse{}, nil
		}

		// These errors are expected from some versioning situations. We should not log them, it'd be too noisy.
		var newerBuild *serviceerror.NewerBuildExists      // expected when versioned poller is superceded
		var failedPrecond *serviceerror.FailedPrecondition // expected when user data is disabled
		if errors.As(err, &newerBuild) || errors.As(err, &failedPrecond) {
			return nil, err
		}

		// For all other errors log an error and return it back to client.
		ctxTimeout := "not-set"
		ctxDeadline, ok := ctx.Deadline()
		if ok {
			ctxTimeout = ctxDeadline.Sub(callTime).String()
		}
		wh.logger.Error("Unable to call matching.PollNexusTaskQueue.",
			tag.WorkflowTaskQueueName(request.GetTaskQueue().GetName()),
			tag.Timeout(ctxTimeout),
			tag.Error(err))

		return nil, err
	}

	return matchingResponse.GetResponse(), nil
}

func (wh *WorkflowHandler) RespondNexusTaskCompleted(ctx context.Context, request *workflowservice.RespondNexusTaskCompletedRequest) (_ *workflowservice.RespondNexusTaskCompletedResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	// Both the task token and the request have a reference to a namespace. We prefer using the namespace ID from
	// the token as it is a more stable identifier.
	// There's no need to validate that the namespace in the token and the request match,
	// NamespaceValidatorInterceptor does this for us.
	tt, err := wh.tokenSerializer.DeserializeNexusTaskToken(request.GetTaskToken())
	if err != nil {
		return nil, err
	}
	if tt.GetTaskQueue() == "" || tt.GetTaskId() == "" {
		return nil, errInvalidTaskToken
	}
	namespaceId := namespace.ID(tt.GetNamespaceId())

	// NOTE: Not checking blob size limit here as we already enforce the 4 MB gRPC request limit and since this
	// doesn't go into workflow history, and the Nexus request caller is unknown, there doesn't seem like there's a
	// good reason to fail at this point.

	matchingRequest := &matchingservice.RespondNexusTaskCompletedRequest{
		NamespaceId: namespaceId.String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: tt.GetTaskQueue(),
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		TaskId:  tt.GetTaskId(),
		Request: request,
	}

	_, err = wh.matchingClient.RespondNexusTaskCompleted(ctx, matchingRequest)
	if err != nil {
		return nil, err
	}
	return &workflowservice.RespondNexusTaskCompletedResponse{}, nil
}

func (wh *WorkflowHandler) RespondNexusTaskFailed(ctx context.Context, request *workflowservice.RespondNexusTaskFailedRequest) (_ *workflowservice.RespondNexusTaskFailedResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	// Both the task token and the request have a reference to a namespace. We prefer using the namespace ID from
	// the token as it is a more stable identifier.
	// There's no need to validate that the namespace in the token and the request match,
	// NamespaceValidatorInterceptor does this for us.
	tt, err := wh.tokenSerializer.DeserializeNexusTaskToken(request.GetTaskToken())
	if err != nil {
		return nil, err
	}
	if tt.GetTaskQueue() == "" || tt.GetTaskId() == "" {
		return nil, errInvalidTaskToken
	}
	namespaceId := namespace.ID(tt.GetNamespaceId())

	// NOTE: Not checking blob size limit here as we already enforce the 4 MB gRPC request limit and since this
	// doesn't go into workflow history, and the Nexus request caller is unknown, there doesn't seem like there's a
	// good reason to fail at this point.

	matchingRequest := &matchingservice.RespondNexusTaskFailedRequest{
		NamespaceId: namespaceId.String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: tt.GetTaskQueue(),
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		TaskId:  tt.GetTaskId(),
		Request: request,
	}

	_, err = wh.matchingClient.RespondNexusTaskFailed(ctx, matchingRequest)
	if err != nil {
		return nil, err
	}
	return &workflowservice.RespondNexusTaskFailedResponse{}, nil
}

func (wh *WorkflowHandler) validateSearchAttributes(searchAttributes *commonpb.SearchAttributes, namespaceName namespace.Name) error {
	if err := wh.saValidator.Validate(searchAttributes, namespaceName.String()); err != nil {
		return err
	}
	return wh.saValidator.ValidateSize(searchAttributes, namespaceName.String())
}

func (wh *WorkflowHandler) validateTaskQueue(t *taskqueuepb.TaskQueue) error {
	if t == nil {
		return errTaskQueueNotSet
	}
	if err := validateTaskQueueName(t.GetName(), wh.config.MaxIDLengthLimit()); err != nil {
		return err
	}

	if t.GetKind() == enumspb.TASK_QUEUE_KIND_STICKY {
		if err := common.ValidateUTF8String("TaskQueue", t.GetNormalName()); err != nil {
			return err
		}
	}

	enums.SetDefaultTaskQueueKind(&t.Kind)
	return nil
}

func (wh *WorkflowHandler) validateWorkflowIdReusePolicy(
	reusePolicy enumspb.WorkflowIdReusePolicy,
	conflictPolicy enumspb.WorkflowIdConflictPolicy,
) error {
	if conflictPolicy != enumspb.WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED &&
		reusePolicy == enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING {
		return errIncompatibleIDReusePolicy
	}
	return nil
}

func (wh *WorkflowHandler) validateWorkflowCompletionCallbacks(
	ns namespace.Name,
	callbacks []*commonpb.Callback,
) error {
	if len(callbacks) > 0 && !wh.config.EnableNexusAPIs() {
		return status.Error(
			codes.InvalidArgument,
			"attaching workflow callbacks is disabled for this namespace",
		)
	}

	if len(callbacks) > wh.config.MaxCallbacksPerWorkflow(ns.String()) {
		return status.Error(
			codes.InvalidArgument,
			fmt.Sprintf(
				"cannot attach more than %d callbacks to a workflow",
				wh.config.MaxCallbacksPerWorkflow(ns.String()),
			),
		)
	}

	for _, callback := range callbacks {
		switch cb := callback.GetVariant().(type) {
		case *commonpb.Callback_Nexus_:
			if err := wh.validateCallbackURL(ns, cb.Nexus.GetUrl()); err != nil {
				return err
			}

			headerSize := 0
			for k, v := range cb.Nexus.GetHeader() {
				headerSize += len(k) + len(v)
			}
			if headerSize > wh.config.CallbackHeaderMaxSize(ns.String()) {
				return status.Error(
					codes.InvalidArgument,
					fmt.Sprintf(
						"invalid header: header size longer than max allowed size of %d",
						wh.config.CallbackHeaderMaxSize(ns.String()),
					),
				)
			}

		default:
			return status.Error(codes.Unimplemented, fmt.Sprintf("unknown callback variant: %T", cb))
		}
	}
	return nil
}

func (wh *WorkflowHandler) validateCallbackURL(ns namespace.Name, rawURL string) error {
	if len(rawURL) > wh.config.CallbackURLMaxLength(ns.String()) {
		return status.Errorf(codes.InvalidArgument, "invalid url: url length longer than max length allowed of %d", wh.config.CallbackURLMaxLength(ns.String()))
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return err
	}
	if !(u.Scheme == "http" || u.Scheme == "https") {
		return status.Errorf(codes.InvalidArgument, "invalid url: unknown scheme: %v", u)
	}
	for _, cfg := range wh.config.CallbackEndpointConfigs(ns.String()) {
		if cfg.Regexp.MatchString(u.Host) {
			if u.Scheme == "http" && !cfg.AllowInsecure {
				return status.Errorf(codes.InvalidArgument, "invalid url: callback address does not allow insecure connections: %v", u)
			}
			return nil
		}
	}
	return status.Errorf(codes.InvalidArgument, "invalid url: url does not match any configured callback address: %v", u)
}

type buildIdAndFlag interface {
	GetBuildId() string
	GetUseVersioning() bool
}

func (wh *WorkflowHandler) validateVersioningInfo(nsName string, id buildIdAndFlag, tq *taskqueuepb.TaskQueue) error {
	if id.GetUseVersioning() && !wh.config.EnableWorkerVersioningWorkflow(nsName) {
		return errWorkerVersioningNotAllowed
	}
	if id.GetUseVersioning() && tq.GetKind() == enumspb.TASK_QUEUE_KIND_STICKY && len(tq.GetNormalName()) == 0 {
		return errUseVersioningWithoutNormalName
	}
	if id.GetUseVersioning() && len(id.GetBuildId()) == 0 {
		return errUseVersioningWithoutBuildId
	}
	if len(id.GetBuildId()) > wh.config.WorkerBuildIdSizeLimit() {
		return errBuildIdTooLong
	}
	return nil
}

//nolint:revive // cyclomatic complexity
func (wh *WorkflowHandler) validateBuildIdCompatibilityUpdate(
	req *workflowservice.UpdateWorkerBuildIdCompatibilityRequest,
) error {
	errDeets := []string{"request to update worker build ID compatability requires: "}

	checkIdLen := func(id string) {
		if len(id) > wh.config.WorkerBuildIdSizeLimit() {
			errDeets = append(errDeets, fmt.Sprintf(" Worker build IDs to be no larger than %v characters",
				wh.config.WorkerBuildIdSizeLimit()))
		}
		if err := common.ValidateUTF8String("BuildId", id); err != nil {
			errDeets = append(errDeets, err.Error())
		}
	}

	if req.GetNamespace() == "" {
		errDeets = append(errDeets, "`namespace` to be set")
	}
	if req.GetTaskQueue() == "" {
		errDeets = append(errDeets, "`task_queue` to be set")
	}
	if req.GetOperation() == nil {
		errDeets = append(errDeets, "an operation to be specified")
	}
	if op, ok := req.GetOperation().(*workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewCompatibleBuildId); ok {
		if op.AddNewCompatibleBuildId.GetNewBuildId() == "" {
			errDeets = append(errDeets, "`add_new_compatible_version` to be set")
		} else {
			checkIdLen(op.AddNewCompatibleBuildId.GetNewBuildId())
		}
		if op.AddNewCompatibleBuildId.GetExistingCompatibleBuildId() == "" {
			errDeets = append(errDeets, "`existing_compatible_version` to be set")
		}
	} else if op, ok := req.GetOperation().(*workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet); ok {
		if op.AddNewBuildIdInNewDefaultSet == "" {
			errDeets = append(errDeets, "`add_new_version_id_in_new_default_set` to be set")
		} else {
			checkIdLen(op.AddNewBuildIdInNewDefaultSet)
		}
	} else if op, ok := req.GetOperation().(*workflowservice.UpdateWorkerBuildIdCompatibilityRequest_PromoteSetByBuildId); ok {
		if op.PromoteSetByBuildId == "" {
			errDeets = append(errDeets, "`promote_set_by_version_id` to be set")
		} else {
			checkIdLen(op.PromoteSetByBuildId)
		}
	} else if op, ok := req.GetOperation().(*workflowservice.UpdateWorkerBuildIdCompatibilityRequest_PromoteBuildIdWithinSet); ok {
		if op.PromoteBuildIdWithinSet == "" {
			errDeets = append(errDeets, "`promote_version_id_within_set` to be set")
		} else {
			checkIdLen(op.PromoteBuildIdWithinSet)
		}
	}
	if len(errDeets) > 1 {
		return serviceerror.NewInvalidArgument(strings.Join(errDeets, ", "))
	}
	return nil
}

func (wh *WorkflowHandler) historyArchived(ctx context.Context, request *workflowservice.GetWorkflowExecutionHistoryRequest, namespaceID namespace.ID) bool {
	if request.GetExecution() == nil || request.GetExecution().GetRunId() == "" {
		return false
	}
	getMutableStateRequest := &historyservice.GetMutableStateRequest{
		NamespaceId: namespaceID.String(),
		Execution:   request.Execution,
	}
	_, err := wh.historyClient.GetMutableState(ctx, getMutableStateRequest)
	if err == nil {
		return false
	}
	switch err.(type) {
	case *serviceerror.NotFound:
		// the only case in which history is assumed to be archived is if getting mutable state returns entity not found error
		return true
	}

	return false
}

func (wh *WorkflowHandler) getArchivedHistory(
	ctx context.Context,
	request *workflowservice.GetWorkflowExecutionHistoryRequest,
	namespaceID namespace.ID,
) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	entry, err := wh.namespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, err
	}

	URIString := entry.HistoryArchivalState().URI
	if URIString == "" {
		// if URI is empty, it means the namespace has never enabled for archival.
		// the error is not "workflow has passed retention period", because
		// we have no way to tell if the requested workflow exists or not.
		return nil, errHistoryNotFound
	}

	URI, err := archiver.NewURI(URIString)
	if err != nil {
		return nil, err
	}

	historyArchiver, err := wh.archiverProvider.GetHistoryArchiver(URI.Scheme(), string(primitives.FrontendService))
	if err != nil {
		return nil, err
	}

	resp, err := historyArchiver.Get(ctx, URI, &archiver.GetHistoryRequest{
		NamespaceID:   namespaceID.String(),
		WorkflowID:    request.GetExecution().GetWorkflowId(),
		RunID:         request.GetExecution().GetRunId(),
		NextPageToken: request.GetNextPageToken(),
		PageSize:      int(request.GetMaximumPageSize()),
	})
	if err != nil {
		return nil, err
	}

	history := &historypb.History{}
	for _, batch := range resp.HistoryBatches {
		history.Events = append(history.Events, batch.Events...)
	}
	return &workflowservice.GetWorkflowExecutionHistoryResponse{
		History:       history,
		NextPageToken: resp.NextPageToken,
		Archived:      true,
	}, nil
}

// cancelOutstandingPoll cancel outstanding poll if context was canceled and returns true. Otherwise returns false.
func (wh *WorkflowHandler) cancelOutstandingPoll(ctx context.Context, namespaceID namespace.ID, taskQueueType enumspb.TaskQueueType,
	taskQueue *taskqueuepb.TaskQueue, pollerID string) bool {
	// First check if this err is due to context cancellation.  This means client connection to frontend is closed.
	if ctx.Err() != context.Canceled {
		return false
	}
	// Our rpc stack does not propagates context cancellation to the other service.  Lets make an explicit
	// call to matching to notify this poller is gone to prevent any tasks being dispatched to zombie pollers.
	// TODO: specify a reasonable timeout for CancelOutstandingPoll.
	_, err := wh.matchingClient.CancelOutstandingPoll(
		rpc.CopyContextValues(context.TODO(), ctx),
		&matchingservice.CancelOutstandingPollRequest{
			NamespaceId:   namespaceID.String(),
			TaskQueueType: taskQueueType,
			TaskQueue:     taskQueue,
			PollerId:      pollerID,
		},
	)
	// We can not do much if this call fails.  Just log the error and move on.
	if err != nil {
		wh.logger.Warn("Failed to cancel outstanding poller.",
			tag.WorkflowTaskQueueName(taskQueue.GetName()), tag.Error(err))
	}

	return true
}

func (wh *WorkflowHandler) checkBadBinary(namespaceEntry *namespace.Namespace, binaryChecksum string) error {
	if err := namespaceEntry.VerifyBinaryChecksum(binaryChecksum); err != nil {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("Binary %v already marked as bad deployment.", binaryChecksum))
	}
	return nil
}

func (wh *WorkflowHandler) validateRetryPolicy(namespaceName namespace.Name, retryPolicy *commonpb.RetryPolicy) error {
	if retryPolicy == nil {
		// By default, if the user does not explicitly set a retry policy for a Workflow, do not perform any retries.
		return nil
	}

	defaultWorkflowRetrySettings := wh.getDefaultWorkflowRetrySettings(namespaceName.String())
	retrypolicy.EnsureDefaults(retryPolicy, defaultWorkflowRetrySettings)
	return retrypolicy.Validate(retryPolicy)
}

func validateRequestId(requestID *string, lenLimit int) error {
	if requestID == nil {
		// should never happen, but just in case.
		return serviceerror.NewInvalidArgument("RequestId is nil")
	}
	if *requestID == "" {
		// For easy direct API use, we default the request ID here but expect all
		// SDKs and other auto-retrying clients to set it
		*requestID = uuid.New()
	}

	if len(*requestID) > lenLimit {
		return errRequestIDTooLong
	}

	return common.ValidateUTF8String("RequestId", *requestID)
}

func (wh *WorkflowHandler) validateStartWorkflowTimeouts(
	request *workflowservice.StartWorkflowExecutionRequest,
) error {
	if err := timer.ValidateAndCapTimer(request.GetWorkflowExecutionTimeout()); err != nil {
		return errInvalidWorkflowExecutionTimeoutSeconds
	}

	if err := timer.ValidateAndCapTimer(request.GetWorkflowRunTimeout()); err != nil {
		return errInvalidWorkflowRunTimeoutSeconds
	}

	if err := timer.ValidateAndCapTimer(request.GetWorkflowTaskTimeout()); err != nil {
		return errInvalidWorkflowTaskTimeoutSeconds
	}

	return nil
}

func (wh *WorkflowHandler) validateSignalWithStartWorkflowTimeouts(
	request *workflowservice.SignalWithStartWorkflowExecutionRequest,
) error {
	if err := timer.ValidateAndCapTimer(request.WorkflowTaskTimeout); err != nil {
		return errInvalidWorkflowExecutionTimeoutSeconds
	}

	if err := timer.ValidateAndCapTimer(request.WorkflowRunTimeout); err != nil {
		return errInvalidWorkflowRunTimeoutSeconds
	}

	if err := timer.ValidateAndCapTimer(request.WorkflowTaskTimeout); err != nil {
		return errInvalidWorkflowTaskTimeoutSeconds
	}

	return nil
}

func (wh *WorkflowHandler) validateWorkflowStartDelay(
	cronSchedule string,
	startDelay *durationpb.Duration,
) error {
	if len(cronSchedule) > 0 && startDelay != nil {
		return errCronAndStartDelaySet
	}

	if err := timer.ValidateAndCapTimer(startDelay); err != nil {
		return errInvalidWorkflowStartDelaySeconds
	}

	return nil
}

func (wh *WorkflowHandler) metricsScope(ctx context.Context) metrics.Handler {
	return interceptor.GetMetricsHandlerFromContext(ctx, wh.logger)
}

func (wh *WorkflowHandler) validateNamespace(
	namespace string,
) error {
	if err := common.ValidateUTF8String("Namespace", namespace); err != nil {
		return err
	}
	if len(namespace) > wh.config.MaxIDLengthLimit() {
		return errNamespaceTooLong
	}
	return nil
}

func (wh *WorkflowHandler) validateWorkflowID(
	workflowID string,
) error {
	if workflowID == "" {
		return errWorkflowIDNotSet
	}
	if err := common.ValidateUTF8String("WorkflowId", workflowID); err != nil {
		return err
	}
	if len(workflowID) > wh.config.MaxIDLengthLimit() {
		return errWorkflowIDTooLong
	}
	return nil
}

func (wh *WorkflowHandler) canonicalizeScheduleSpec(schedule *schedpb.Schedule) error {
	if schedule.Spec == nil {
		schedule.Spec = &schedpb.ScheduleSpec{}
	}
	compiledSpec, err := wh.scheduleSpecBuilder.NewCompiledSpec(schedule.Spec)
	if err != nil {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("Invalid schedule spec: %v", err))
	}
	// This mutates a part of the request message, but it's safe even in the presence of
	// retries (reusing the same message) because canonicalization is idempotent.
	schedule.Spec = compiledSpec.CanonicalForm()
	return nil
}

func (wh *WorkflowHandler) decodeScheduleListInfo(memo *commonpb.Memo) *schedpb.ScheduleListInfo {
	var listInfo schedpb.ScheduleListInfo
	var listInfoBytes []byte
	if p := memo.GetFields()[scheduler.MemoFieldInfo]; p == nil {
		return nil
	} else if err := payload.Decode(p, &listInfoBytes); err != nil {
		wh.logger.Error("decoding schedule list info from payload", tag.Error(err))
		return nil
	} else if err := listInfo.Unmarshal(listInfoBytes); err != nil {
		wh.logger.Error("decoding schedule list info from payload", tag.Error(err))
		return nil
	} else if err := utf8validator.Validate(&listInfo, utf8validator.SourcePersistence); err != nil {
		wh.logger.Error("decoding schedule list info from payload", tag.Error(err))
		return nil
	}
	scheduler.CleanSpec(listInfo.Spec)
	return &listInfo
}

// This mutates searchAttributes
func (wh *WorkflowHandler) cleanScheduleSearchAttributes(searchAttributes *commonpb.SearchAttributes) *commonpb.SearchAttributes {
	fields := searchAttributes.GetIndexedFields()
	if len(fields) == 0 {
		return nil
	}

	delete(fields, searchattribute.TemporalSchedulePaused)
	delete(fields, "TemporalScheduleInfoJSON") // used by older version, clean this up if present
	// these aren't schedule-related but they aren't relevant to the user for
	// scheduler workflows since it's the server worker
	delete(fields, searchattribute.BinaryChecksums)
	delete(fields, searchattribute.BuildIds)
	// all schedule workflows should be in this namespace division so there's no need to include it
	delete(fields, searchattribute.TemporalNamespaceDivision)

	if len(fields) == 0 {
		return nil
	}
	return searchAttributes
}

// This mutates memo
func (wh *WorkflowHandler) cleanScheduleMemo(memo *commonpb.Memo) *commonpb.Memo {
	fields := memo.GetFields()
	if len(fields) == 0 {
		return nil
	}
	delete(fields, scheduler.MemoFieldInfo)
	if len(fields) == 0 {
		return nil
	}
	return memo
}

// This mutates request (but idempotent so safe for retries)
func (wh *WorkflowHandler) addInitialScheduleMemo(request *workflowservice.CreateScheduleRequest, args *schedspb.StartScheduleArgs) {
	info := scheduler.GetListInfoFromStartArgs(args, time.Now().UTC(), wh.scheduleSpecBuilder)
	// utf8validator: don't bother validating strings in info here because they all came from an rpc request
	infoBytes, err := info.Marshal()
	if err != nil {
		wh.logger.Error("encoding initial schedule memo failed", tag.Error(err))
		return
	}
	p, err := sdk.PreferProtoDataConverter.ToPayload(infoBytes)
	if err != nil {
		wh.logger.Error("encoding initial schedule memo failed", tag.Error(err))
		return
	}
	if request.Memo == nil {
		request.Memo = &commonpb.Memo{}
	}
	if request.Memo.Fields == nil {
		request.Memo.Fields = make(map[string]*commonpb.Payload)
	}
	request.Memo.Fields[scheduler.MemoFieldInfo] = p
}

func getBatchOperationState(workflowState enumspb.WorkflowExecutionStatus) enumspb.BatchOperationState {
	var operationState enumspb.BatchOperationState
	switch workflowState {
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING:
		operationState = enumspb.BATCH_OPERATION_STATE_RUNNING
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		operationState = enumspb.BATCH_OPERATION_STATE_COMPLETED
	default:
		operationState = enumspb.BATCH_OPERATION_STATE_FAILED
	}
	return operationState
}
