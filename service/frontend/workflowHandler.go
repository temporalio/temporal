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
	"unicode/utf8"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	filterpb "go.temporal.io/api/filter/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/enums"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/searchattribute"
)

const (
	// HealthStatusOK is used when this node is healthy and rpc requests are allowed
	HealthStatusOK HealthStatus = iota + 1
	// HealthStatusShuttingDown is used when the rpc handler is shutting down
	HealthStatusShuttingDown
)

const (
	serviceName = "temporal.api.workflowservice.v1.WorkflowService"
)

var _ Handler = (*WorkflowHandler)(nil)

var (
	minTime = time.Unix(0, 0).UTC()
	maxTime = time.Date(2100, 1, 1, 1, 0, 0, 0, time.UTC)
)

type (
	// WorkflowHandler - gRPC handler interface for workflowservice
	WorkflowHandler struct {
		status int32

		healthStatus                    int32
		tokenSerializer                 common.TaskTokenSerializer
		config                          *Config
		versionChecker                  headers.VersionChecker
		namespaceHandler                namespace.Handler
		getDefaultWorkflowRetrySettings dynamicconfig.MapPropertyFnWithNamespaceFilter
		visibilityMrg                   manager.VisibilityManager
		logger                          log.Logger
		throttledLogger                 log.Logger
		persistenceExecutionManager     persistence.ExecutionManager
		clusterMetadataManager          persistence.ClusterMetadataManager
		historyClient                   historyservice.HistoryServiceClient
		matchingClient                  matchingservice.MatchingServiceClient
		archiverProvider                provider.ArchiverProvider
		payloadSerializer               serialization.Serializer
		namespaceRegistry               namespace.Registry
		saMapper                        searchattribute.Mapper
		saProvider                      searchattribute.Provider
		archivalMetadata                archiver.ArchivalMetadata
	}

	// HealthStatus is an enum that refers to the rpc handler health status
	HealthStatus int32
)

var (
	frontendServiceRetryPolicy = common.CreateFrontendServiceRetryPolicy()
)

// NewWorkflowHandler creates a gRPC handler for workflowservice
func NewWorkflowHandler(
	config *Config,
	namespaceReplicationQueue persistence.NamespaceReplicationQueue,
	visibilityMrg manager.VisibilityManager,
	logger log.Logger,
	throttledLogger log.Logger,
	persistenceExecutionManager persistence.ExecutionManager,
	clusterMetadataManager persistence.ClusterMetadataManager,
	persistenceMetadataManager persistence.MetadataManager,
	historyClient historyservice.HistoryServiceClient,
	matchingClient matchingservice.MatchingServiceClient,
	archiverProvider provider.ArchiverProvider,
	payloadSerializer serialization.Serializer,
	namespaceRegistry namespace.Registry,
	saMapper searchattribute.Mapper,
	saProvider searchattribute.Provider,
	clusterMetadata cluster.Metadata,
	archivalMetadata archiver.ArchivalMetadata,
) *WorkflowHandler {

	handler := &WorkflowHandler{
		status:          common.DaemonStatusInitialized,
		config:          config,
		healthStatus:    int32(HealthStatusOK),
		tokenSerializer: common.NewProtoTaskTokenSerializer(),
		versionChecker:  headers.NewDefaultVersionChecker(),
		namespaceHandler: namespace.NewHandler(
			config.MaxBadBinaries,
			logger,
			persistenceMetadataManager,
			clusterMetadata,
			namespace.NewNamespaceReplicator(namespaceReplicationQueue, logger),
			archivalMetadata,
			archiverProvider,
		),
		getDefaultWorkflowRetrySettings: config.DefaultWorkflowRetryPolicy,
		visibilityMrg:                   visibilityMrg,
		logger:                          logger,
		throttledLogger:                 throttledLogger,
		persistenceExecutionManager:     persistenceExecutionManager,
		clusterMetadataManager:          clusterMetadataManager,
		historyClient:                   historyClient,
		matchingClient:                  matchingClient,
		archiverProvider:                archiverProvider,
		payloadSerializer:               payloadSerializer,
		namespaceRegistry:               namespaceRegistry,
		saProvider:                      saProvider,
		saMapper:                        saMapper,
		archivalMetadata:                archivalMetadata,
	}

	return handler
}

// Start starts the handler
func (wh *WorkflowHandler) Start() {
	if !atomic.CompareAndSwapInt32(
		&wh.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}
}

// Stop stops the handler
func (wh *WorkflowHandler) Stop() {
	if !atomic.CompareAndSwapInt32(
		&wh.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}
}

// UpdateHealthStatus sets the health status for this rpc handler.
// This health status will be used within the rpc health check handler
func (wh *WorkflowHandler) UpdateHealthStatus(status HealthStatus) {
	atomic.StoreInt32(&wh.healthStatus, int32(status))
}

func (wh *WorkflowHandler) isStopped() bool {
	return atomic.LoadInt32(&wh.status) == common.DaemonStatusStopped
}

// GetConfig return config
func (wh *WorkflowHandler) GetConfig() *Config {
	return wh.config
}

// Check is from: https://github.com/grpc/grpc/blob/master/doc/health-checking.md
func (wh *WorkflowHandler) Check(_ context.Context, request *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	wh.logger.Debug("Frontend service health check endpoint (gRPC) reached.")

	if request.Service != serviceName {
		return &healthpb.HealthCheckResponse{
			Status: healthpb.HealthCheckResponse_SERVICE_UNKNOWN,
		}, nil
	}

	status := HealthStatus(atomic.LoadInt32(&wh.healthStatus))
	if status == HealthStatusOK {
		return &healthpb.HealthCheckResponse{
			Status: healthpb.HealthCheckResponse_SERVING,
		}, nil
	}

	return &healthpb.HealthCheckResponse{
		Status: healthpb.HealthCheckResponse_NOT_SERVING,
	}, nil
}

func (wh *WorkflowHandler) Watch(*healthpb.HealthCheckRequest, healthpb.Health_WatchServer) error {
	return serviceerror.NewUnimplemented("Watch is not implemented.")
}

// RegisterNamespace creates a new namespace which can be used as a container for all resources.  Namespace is a top level
// entity within Temporal, used as a container for all resources like workflow executions, task queues, etc.  Namespace
// acts as a sandbox and provides isolation for all resources within the namespace.  All resources belong to exactly one
// namespace.
func (wh *WorkflowHandler) RegisterNamespace(ctx context.Context, request *workflowservice.RegisterNamespaceRequest) (_ *workflowservice.RegisterNamespaceResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

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

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

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

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

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

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

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

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

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
func (wh *WorkflowHandler) StartWorkflowExecution(ctx context.Context, request *workflowservice.StartWorkflowExecutionRequest) (_ *workflowservice.StartWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

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

	if err := backoff.ValidateSchedule(request.GetCronSchedule()); err != nil {
		return nil, err
	}

	wh.logger.Debug("Received StartWorkflowExecution.", tag.WorkflowID(request.GetWorkflowId()))

	if request.WorkflowType == nil || request.WorkflowType.GetName() == "" {
		return nil, errWorkflowTypeNotSet
	}

	if len(request.WorkflowType.GetName()) > wh.config.MaxIDLengthLimit() {
		return nil, errWorkflowTypeTooLong
	}

	if err := wh.validateTaskQueue(request.TaskQueue); err != nil {
		return nil, err
	}

	if err := wh.validateStartWorkflowTimeouts(request); err != nil {
		return nil, err
	}

	if request.GetRequestId() == "" {
		return nil, errRequestIDNotSet
	}

	if len(request.GetRequestId()) > wh.config.MaxIDLengthLimit() {
		return nil, errRequestIDTooLong
	}

	enums.SetDefaultWorkflowIdReusePolicy(&request.WorkflowIdReusePolicy)

	wh.logger.Debug("Start workflow execution request namespace.", tag.WorkflowNamespace(namespaceName.String()))
	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespaceName)
	if err != nil {
		return nil, err
	}
	wh.logger.Debug("Start workflow execution request namespaceID.", tag.WorkflowNamespaceID(namespaceID.String()))

	resp, err := wh.historyClient.StartWorkflowExecution(ctx, common.CreateHistoryStartWorkflowRequest(namespaceID.String(), request, nil, time.Now().UTC()))

	if err != nil {
		return nil, err
	}
	return &workflowservice.StartWorkflowExecutionResponse{RunId: resp.GetRunId()}, nil
}

// GetWorkflowExecutionHistory returns the history of specified workflow execution.  It fails with 'EntityNotExistError' if specified workflow
// execution in unknown to the service.
func (wh *WorkflowHandler) GetWorkflowExecutionHistory(ctx context.Context, request *workflowservice.GetWorkflowExecutionHistoryRequest) (_ *workflowservice.GetWorkflowExecutionHistoryResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if request == nil {
		return nil, errRequestNotSet
	}

	if err := wh.validateExecution(request.Execution); err != nil {
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
	if request.GetMaximumPageSize() > common.GetHistoryMaxPageSize {
		wh.throttledLogger.Warn("GetHistory page size is larger than threshold",
			tag.WorkflowID(request.Execution.GetWorkflowId()),
			tag.WorkflowRunID(request.Execution.GetRunId()),
			tag.WorkflowNamespaceID(namespaceID.String()), tag.WorkflowSize(int64(request.GetMaximumPageSize())))
		request.MaximumPageSize = common.GetHistoryMaxPageSize
	}

	if !request.GetSkipArchival() {
		enableArchivalRead := wh.archivalMetadata.GetHistoryConfig().ReadEnabled()
		historyArchived := wh.historyArchived(ctx, request, namespaceID)
		if enableArchivalRead && historyArchived {
			return wh.getArchivedHistory(ctx, request, namespaceID)
		}
	}

	// this function returns the following 7 things,
	// 1. the current branch token (to use to retrieve history events)
	// 2. the workflow run ID
	// 3. the last first event ID (the event ID of the last batch of events in the history)
	// 4. the last first event transaction id
	// 5. the next event ID
	// 6. whether the workflow is running
	// 7. error if any
	queryHistory := func(
		namespaceUUID namespace.ID,
		execution *commonpb.WorkflowExecution,
		expectedNextEventID int64,
		currentBranchToken []byte,
	) ([]byte, string, int64, int64, int64, bool, error) {
		response, err := wh.historyClient.PollMutableState(ctx, &historyservice.PollMutableStateRequest{
			NamespaceId:         namespaceUUID.String(),
			Execution:           execution,
			ExpectedNextEventId: expectedNextEventID,
			CurrentBranchToken:  currentBranchToken,
		})

		if err != nil {
			return nil, "", 0, 0, 0, false, err
		}
		isWorkflowRunning := response.GetWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING

		return response.CurrentBranchToken,
			response.Execution.GetRunId(),
			response.GetLastFirstEventId(),
			response.GetLastFirstEventTxnId(),
			response.GetNextEventId(),
			isWorkflowRunning,
			nil
	}

	isLongPoll := request.GetWaitNewEvent()
	isCloseEventOnly := request.GetHistoryEventFilterType() == enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT
	execution := request.Execution
	var continuationToken *tokenspb.HistoryContinuation

	var runID string
	lastFirstEventID := common.FirstEventID
	lastFirstEventTxnID := int64(0)
	var nextEventID int64
	var isWorkflowRunning bool

	// process the token for paging
	queryNextEventID := common.EndEventID
	if request.NextPageToken != nil {
		continuationToken, err = deserializeHistoryToken(request.NextPageToken)
		if err != nil {
			return nil, errInvalidNextPageToken
		}
		if execution.GetRunId() != "" && execution.GetRunId() != continuationToken.GetRunId() {
			return nil, errNextPageTokenRunIDMismatch
		}

		execution.RunId = continuationToken.GetRunId()

		// we need to update the current next event ID and whether workflow is running
		if len(continuationToken.PersistenceToken) == 0 && isLongPoll && continuationToken.IsWorkflowRunning {
			if !isCloseEventOnly {
				queryNextEventID = continuationToken.GetNextEventId()
			}
			continuationToken.BranchToken, _, lastFirstEventID, lastFirstEventTxnID, nextEventID, isWorkflowRunning, err =
				queryHistory(namespaceID, execution, queryNextEventID, continuationToken.BranchToken)
			if err != nil {
				return nil, err
			}
			continuationToken.FirstEventId = continuationToken.GetNextEventId()
			continuationToken.NextEventId = nextEventID
			continuationToken.IsWorkflowRunning = isWorkflowRunning
		}
	} else {
		continuationToken = &tokenspb.HistoryContinuation{}
		if !isCloseEventOnly {
			queryNextEventID = common.FirstEventID
		}
		continuationToken.BranchToken, runID, lastFirstEventID, lastFirstEventTxnID, nextEventID, isWorkflowRunning, err =
			queryHistory(namespaceID, execution, queryNextEventID, nil)
		if err != nil {
			return nil, err
		}

		execution.RunId = runID

		continuationToken.RunId = runID
		continuationToken.FirstEventId = common.FirstEventID
		continuationToken.NextEventId = nextEventID
		continuationToken.IsWorkflowRunning = isWorkflowRunning
		continuationToken.PersistenceToken = nil
	}

	// TODO below is a temporal solution to guard against invalid event batch
	//  when data inconsistency occurs
	//  long term solution should check event batch pointing backwards within history store
	defer func() {
		// lastFirstEventTxnID != 0 exists due to forward / backward compatibility
		if _, ok := retError.(*serviceerror.DataLoss); ok && lastFirstEventTxnID != 0 {
			_, _ = wh.persistenceExecutionManager.TrimHistoryBranch(&persistence.TrimHistoryBranchRequest{
				ShardID:       common.WorkflowIDToHistoryShard(namespaceID.String(), execution.GetWorkflowId(), wh.config.NumHistoryShards),
				BranchToken:   continuationToken.BranchToken,
				NodeID:        lastFirstEventID,
				TransactionID: lastFirstEventTxnID,
			})
		}
	}()

	rawHistoryQueryEnabled := wh.config.SendRawWorkflowHistory(request.GetNamespace())

	history := &historypb.History{}
	history.Events = []*historypb.HistoryEvent{}
	var historyBlob []*commonpb.DataBlob
	if isCloseEventOnly {
		if !isWorkflowRunning {
			if rawHistoryQueryEnabled {
				historyBlob, _, err = wh.getRawHistory(
					wh.metricsScope(ctx),
					namespaceID,
					*execution,
					lastFirstEventID,
					nextEventID,
					request.GetMaximumPageSize(),
					nil,
					continuationToken.TransientWorkflowTask,
					continuationToken.BranchToken,
				)
				if err != nil {
					return nil, err
				}

				// since getHistory func will not return empty history, so the below is safe
				historyBlob = historyBlob[len(historyBlob)-1:]
			} else {
				history, _, err = wh.getHistory(
					wh.metricsScope(ctx),
					namespaceID,
					namespace.Name(request.GetNamespace()),
					*execution,
					lastFirstEventID,
					nextEventID,
					request.GetMaximumPageSize(),
					nil,
					continuationToken.TransientWorkflowTask,
					continuationToken.BranchToken,
				)
				if err != nil {
					return nil, err
				}
				// since getHistory func will not return empty history, so the below is safe
				history.Events = history.Events[len(history.Events)-1 : len(history.Events)]
			}
			continuationToken = nil
		} else if isLongPoll {
			// set the persistence token to be nil so next time we will query history for updates
			continuationToken.PersistenceToken = nil
		} else {
			continuationToken = nil
		}
	} else {
		// return all events
		if continuationToken.FirstEventId >= continuationToken.NextEventId {
			// currently there is no new event
			history.Events = []*historypb.HistoryEvent{}
			if !isWorkflowRunning {
				continuationToken = nil
			}
		} else {
			if rawHistoryQueryEnabled {
				historyBlob, continuationToken.PersistenceToken, err = wh.getRawHistory(
					wh.metricsScope(ctx),
					namespaceID,
					*execution,
					continuationToken.FirstEventId,
					continuationToken.NextEventId,
					request.GetMaximumPageSize(),
					continuationToken.PersistenceToken,
					continuationToken.TransientWorkflowTask,
					continuationToken.BranchToken,
				)
			} else {
				history, continuationToken.PersistenceToken, err = wh.getHistory(
					wh.metricsScope(ctx),
					namespaceID,
					namespace.Name(request.GetNamespace()),
					*execution,
					continuationToken.FirstEventId,
					continuationToken.NextEventId,
					request.GetMaximumPageSize(),
					continuationToken.PersistenceToken,
					continuationToken.TransientWorkflowTask,
					continuationToken.BranchToken,
				)
			}

			if err != nil {
				return nil, err
			}

			// here, for long pull on history events, we need to intercept the paging token from cassandra
			// and do something clever
			if len(continuationToken.PersistenceToken) == 0 && (!continuationToken.IsWorkflowRunning || !isLongPoll) {
				// meaning, there is no more history to be returned
				continuationToken = nil
			}
		}
	}

	nextToken, err := serializeHistoryToken(continuationToken)
	if err != nil {
		return nil, err
	}

	// Backwards-compatibility fix for retry events after #1866: older SDKs don't know how to "follow"
	// subsequent runs linked in WorkflowExecutionFailed or TimedOut events, so they'll get the wrong result
	// when trying to "get" the result of a workflow run. (This applies to cron runs also but "get" on a cron
	// workflow isn't really sensible.)
	//
	// To handle this in a backwards-compatible way, we'll pretend the completion event is actually
	// ContinuedAsNew, if it's Failed or TimedOut. We want to do this only when the client is looking for a
	// completion event, and not when it's getting the history to display for other purposes. The best signal
	// for that purpose is `isCloseEventOnly`. (We can't use `isLongPoll` also because in some cases, older
	// versions of the Java SDK don't set that flag.)
	//
	// TODO: We can remove this once we no longer support SDK versions prior to around September 2021.
	// Revisit this once we have an SDK deprecation policy.
	if isCloseEventOnly &&
		!wh.versionChecker.ClientSupportsFeature(ctx, headers.FeatureFollowsNextRunID) &&
		len(history.Events) > 0 {
		lastEvent := history.Events[len(history.Events)-1]
		fakeEvent, err := wh.makeFakeContinuedAsNewEvent(ctx, lastEvent)
		if err != nil {
			return nil, err
		}
		if fakeEvent != nil {
			history.Events[len(history.Events)-1] = fakeEvent
		}
	}

	return &workflowservice.GetWorkflowExecutionHistoryResponse{
		History:       history,
		RawHistory:    historyBlob,
		NextPageToken: nextToken,
		Archived:      false,
	}, nil
}

// PollWorkflowTaskQueue is called by application worker to process WorkflowTask from a specific task queue.  A
// WorkflowTask is dispatched to callers for active workflow executions, with pending workflow tasks.
// Application is then expected to call 'RespondWorkflowTaskCompleted' API when it is done processing the WorkflowTask.
// It will also create a 'WorkflowTaskStarted' event in the history for that session before handing off WorkflowTask to
// application worker.
func (wh *WorkflowHandler) PollWorkflowTaskQueue(ctx context.Context, request *workflowservice.PollWorkflowTaskQueueRequest) (_ *workflowservice.PollWorkflowTaskQueueResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	callTime := time.Now().UTC()

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

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

	if err := wh.validateTaskQueue(request.TaskQueue); err != nil {
		return nil, err
	}

	namespaceEntry, err := wh.namespaceRegistry.GetNamespace(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}
	namespaceID := namespaceEntry.ID()

	wh.logger.Debug("Poll workflow task queue.", tag.WorkflowNamespace(namespaceEntry.Name().String()), tag.WorkflowNamespaceID(namespaceID.String()))
	if err := wh.checkBadBinary(namespaceEntry, request.GetBinaryChecksum()); err != nil {
		return nil, err
	}

	pollerID := uuid.New()
	var matchingResp *matchingservice.PollWorkflowTaskQueueResponse
	op := func() error {
		var err error
		matchingResp, err = wh.matchingClient.PollWorkflowTaskQueue(ctx, &matchingservice.PollWorkflowTaskQueueRequest{
			NamespaceId: namespaceID.String(),
			PollerId:    pollerID,
			PollRequest: request,
		})
		return err
	}

	err = backoff.Retry(op, frontendServiceRetryPolicy, common.IsServiceTransientError)
	if err != nil {
		contextWasCanceled := wh.cancelOutstandingPoll(ctx, namespaceID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, request.TaskQueue, pollerID)
		if contextWasCanceled {
			// Clear error as we don't want to report context cancellation error to count against our SLA.
			// It doesn't matter what to return here, client has already gone. But (nil,nil) is invalid gogo return pair.
			return &workflowservice.PollWorkflowTaskQueueResponse{}, nil
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

	resp, err := wh.createPollWorkflowTaskQueueResponse(ctx, namespaceID, matchingResp, matchingResp.GetBranchToken())
	if err != nil {
		return nil, err
	}
	return resp, nil
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

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if request == nil {
		return nil, errRequestNotSet
	}

	taskToken, err := wh.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return nil, err
	}
	namespaceId := namespace.ID(taskToken.GetNamespaceId())

	histResp, err := wh.historyClient.RespondWorkflowTaskCompleted(ctx, &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId:     namespaceId.String(),
		CompleteRequest: request},
	)
	if err != nil {
		return nil, err
	}

	if len(request.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return nil, errIdentityTooLong
	}

	completedResp := &workflowservice.RespondWorkflowTaskCompletedResponse{}
	if request.GetReturnNewWorkflowTask() && histResp != nil && histResp.StartedResponse != nil {
		taskToken := &tokenspb.Task{
			NamespaceId:     taskToken.GetNamespaceId(),
			WorkflowId:      taskToken.GetWorkflowId(),
			RunId:           taskToken.GetRunId(),
			ScheduleId:      histResp.StartedResponse.GetScheduledEventId(),
			ScheduleAttempt: histResp.StartedResponse.GetAttempt(),
		}
		token, _ := wh.tokenSerializer.Serialize(taskToken)
		workflowExecution := &commonpb.WorkflowExecution{
			WorkflowId: taskToken.GetWorkflowId(),
			RunId:      taskToken.GetRunId(),
		}
		matchingResp := common.CreateMatchingPollWorkflowTaskQueueResponse(histResp.StartedResponse, workflowExecution, token)

		newWorkflowTask, err := wh.createPollWorkflowTaskQueueResponse(ctx, namespaceId, matchingResp, matchingResp.GetBranchToken())
		if err != nil {
			return nil, err
		}
		completedResp.WorkflowTask = newWorkflowTask
	}

	return completedResp, nil
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

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

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
		wh.metricsScope(ctx).Tagged(metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UNSPECIFIED.String())),
		wh.throttledLogger,
		tag.BlobSizeViolationOperation("RespondWorkflowTaskFailed"),
	); err != nil {
		serverFailure := failure.NewServerFailure(common.FailureReasonFailureExceedsLimit, false)
		serverFailure.Cause = failure.Truncate(request.Failure, sizeLimitWarn)
		request.Failure = serverFailure
	}

	if request.GetCause() == enumspb.WORKFLOW_TASK_FAILED_CAUSE_NON_DETERMINISTIC_ERROR {
		wh.logger.Info("Non-Deterministic Error",
			tag.WorkflowNamespaceID(taskToken.GetNamespaceId()),
			tag.WorkflowID(taskToken.GetWorkflowId()),
			tag.WorkflowRunID(taskToken.GetRunId()),
		)
		wh.metricsScope(ctx).IncCounter(metrics.ServiceErrNonDeterministicCounter)
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

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

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

	if err := wh.validateTaskQueue(request.TaskQueue); err != nil {
		return nil, err
	}
	if len(request.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return nil, errIdentityTooLong
	}

	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	pollerID := uuid.New()
	var matchingResponse *matchingservice.PollActivityTaskQueueResponse
	op := func() error {
		var err error
		matchingResponse, err = wh.matchingClient.PollActivityTaskQueue(ctx, &matchingservice.PollActivityTaskQueueRequest{
			NamespaceId: namespaceID.String(),
			PollerId:    pollerID,
			PollRequest: request,
		})
		return err
	}

	err = backoff.Retry(op, frontendServiceRetryPolicy, common.IsServiceTransientError)
	if err != nil {
		contextWasCanceled := wh.cancelOutstandingPoll(ctx, namespaceID, enumspb.TASK_QUEUE_TYPE_ACTIVITY, request.TaskQueue, pollerID)
		if contextWasCanceled {
			// Clear error as we don't want to report context cancellation error to count against our SLA.
			// It doesn't matter what to return here, client has already gone. But (nil,nil) is invalid gogo return pair.
			return &workflowservice.PollActivityTaskQueueResponse{}, nil
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

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

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
		wh.metricsScope(ctx).Tagged(metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UNSPECIFIED.String())),
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

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

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

	taskToken := &tokenspb.Task{
		NamespaceId:     namespaceID.String(),
		RunId:           runID,
		WorkflowId:      workflowID,
		ScheduleId:      common.EmptyEventID,
		ActivityId:      activityID,
		ScheduleAttempt: 1,
	}
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
		wh.metricsScope(ctx).Tagged(metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UNSPECIFIED.String())),
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

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

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
		wh.metricsScope(ctx).Tagged(metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UNSPECIFIED.String())),
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

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

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

	taskToken := &tokenspb.Task{
		NamespaceId:     namespaceID.String(),
		RunId:           runID,
		WorkflowId:      workflowID,
		ScheduleId:      common.EmptyEventID,
		ActivityId:      activityID,
		ScheduleAttempt: 1,
	}
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
		wh.metricsScope(ctx).Tagged(metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UNSPECIFIED.String())),
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

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

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

	if err := common.CheckEventBlobSizeLimit(
		request.GetFailure().Size(),
		sizeLimitWarn,
		sizeLimitError,
		namespaceID.String(),
		taskToken.GetWorkflowId(),
		taskToken.GetRunId(),
		wh.metricsScope(ctx).Tagged(metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UNSPECIFIED.String())),
		wh.throttledLogger,
		tag.BlobSizeViolationOperation("RespondActivityTaskFailed"),
	); err != nil {
		serverFailure := failure.NewServerFailure(common.FailureReasonFailureExceedsLimit, false)
		serverFailure.Cause = failure.Truncate(request.Failure, sizeLimitWarn)
		request.Failure = serverFailure
	}

	_, err = wh.historyClient.RespondActivityTaskFailed(ctx, &historyservice.RespondActivityTaskFailedRequest{
		NamespaceId:   namespaceID.String(),
		FailedRequest: request,
	})
	if err != nil {
		return nil, err
	}
	return &workflowservice.RespondActivityTaskFailedResponse{}, nil
}

// RespondActivityTaskFailedById is called by application worker when it is done processing an ActivityTask.
// It will result in a new 'ActivityTaskFailed' event being written to the workflow history and a new WorkflowTask
// created for the workflow instance so new commands could be made.  Similar to RespondActivityTaskFailed but use
// Namespace, WorkflowID and ActivityID instead of 'taskToken' for completion. It fails with 'EntityNotExistsError'
// if the these IDs are not valid anymore due to activity timeout.
func (wh *WorkflowHandler) RespondActivityTaskFailedById(ctx context.Context, request *workflowservice.RespondActivityTaskFailedByIdRequest) (_ *workflowservice.RespondActivityTaskFailedByIdResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

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

	taskToken := &tokenspb.Task{
		NamespaceId:     namespaceID.String(),
		RunId:           runID,
		WorkflowId:      workflowID,
		ScheduleId:      common.EmptyEventID,
		ActivityId:      activityID,
		ScheduleAttempt: 1,
	}
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
		request.GetFailure().Size(),
		sizeLimitWarn,
		sizeLimitError,
		namespaceID.String(),
		taskToken.GetWorkflowId(),
		runID,
		wh.metricsScope(ctx).Tagged(metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UNSPECIFIED.String())),
		wh.throttledLogger,
		tag.BlobSizeViolationOperation("RespondActivityTaskFailedById"),
	); err != nil {
		serverFailure := failure.NewServerFailure(common.FailureReasonFailureExceedsLimit, false)
		serverFailure.Cause = failure.Truncate(request.Failure, sizeLimitWarn)
		request.Failure = serverFailure
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
	return &workflowservice.RespondActivityTaskFailedByIdResponse{}, nil
}

// RespondActivityTaskCanceled is called by application worker when it is successfully canceled an ActivityTask.  It will
// result in a new 'ActivityTaskCanceled' event being written to the workflow history and a new WorkflowTask
// created for the workflow instance so new commands could be made.  Use the 'taskToken' provided as response of
// PollActivityTaskQueue API call for completion. It fails with 'EntityNotExistsError' if the taskToken is not valid
// anymore due to activity timeout.
func (wh *WorkflowHandler) RespondActivityTaskCanceled(ctx context.Context, request *workflowservice.RespondActivityTaskCanceledRequest) (_ *workflowservice.RespondActivityTaskCanceledResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

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
		wh.metricsScope(ctx).Tagged(metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UNSPECIFIED.String())),
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

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

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

	taskToken := &tokenspb.Task{
		NamespaceId:     namespaceID.String(),
		RunId:           runID,
		WorkflowId:      workflowID,
		ScheduleId:      common.EmptyEventID,
		ActivityId:      activityID,
		ScheduleAttempt: 1,
	}
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
		wh.metricsScope(ctx).Tagged(metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UNSPECIFIED.String())),
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
// created for the workflow instance so new commands could be made. It fails with 'EntityNotExistsError' if the workflow is not valid
// anymore due to completion or doesn't exist.
func (wh *WorkflowHandler) RequestCancelWorkflowExecution(ctx context.Context, request *workflowservice.RequestCancelWorkflowExecutionRequest) (_ *workflowservice.RequestCancelWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if request == nil {
		return nil, errRequestNotSet
	}

	if err := wh.validateExecution(request.WorkflowExecution); err != nil {
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

// SignalWorkflowExecution is used to send a signal event to running workflow execution.  This results in
// WorkflowExecutionSignaled event recorded in the history and a workflow task being created for the execution.
func (wh *WorkflowHandler) SignalWorkflowExecution(ctx context.Context, request *workflowservice.SignalWorkflowExecutionRequest) (_ *workflowservice.SignalWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if request == nil {
		return nil, errRequestNotSet
	}

	if err := wh.validateExecution(request.WorkflowExecution); err != nil {
		return nil, err
	}

	if request.GetSignalName() == "" {
		return nil, errSignalNameTooLong
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
		wh.metricsScope(ctx).Tagged(metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UNSPECIFIED.String())),
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

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

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

	if err := wh.validateTaskQueue(request.TaskQueue); err != nil {
		return nil, err
	}

	if len(request.GetRequestId()) > wh.config.MaxIDLengthLimit() {
		return nil, errRequestIDTooLong
	}

	if err := wh.validateSignalWithStartWorkflowTimeouts(request); err != nil {
		return nil, err
	}

	namespaceName := namespace.Name(request.GetNamespace())
	if err := wh.validateRetryPolicy(namespaceName, request.RetryPolicy); err != nil {
		return nil, err
	}

	if err := backoff.ValidateSchedule(request.GetCronSchedule()); err != nil {
		return nil, err
	}

	enums.SetDefaultWorkflowIdReusePolicy(&request.WorkflowIdReusePolicy)

	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespaceName)
	if err != nil {
		return nil, err
	}

	var runId string
	op := func() error {
		var err error
		resp, err := wh.historyClient.SignalWithStartWorkflowExecution(ctx, &historyservice.SignalWithStartWorkflowExecutionRequest{
			NamespaceId:            namespaceID.String(),
			SignalWithStartRequest: request,
		})
		if err != nil {
			return err
		}
		runId = resp.GetRunId()
		return nil
	}

	err = backoff.Retry(op, frontendServiceRetryPolicy, common.IsServiceTransientError)
	if err != nil {
		return nil, err
	}

	return &workflowservice.SignalWithStartWorkflowExecutionResponse{RunId: runId}, nil
}

// ResetWorkflowExecution reset an existing workflow execution to WorkflowTaskCompleted event(exclusive).
// And it will immediately terminating the current execution instance.
func (wh *WorkflowHandler) ResetWorkflowExecution(ctx context.Context, request *workflowservice.ResetWorkflowExecutionRequest) (_ *workflowservice.ResetWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if request == nil {
		return nil, errRequestNotSet
	}
	if request.GetRequestId() == "" {
		return nil, errRequestIDNotSet
	}
	if len(request.GetRequestId()) > wh.config.MaxIDLengthLimit() {
		return nil, errRequestIDTooLong
	}

	if err := wh.validateExecution(request.WorkflowExecution); err != nil {
		return nil, err
	}

	switch request.GetResetReapplyType() {
	case enumspb.RESET_REAPPLY_TYPE_UNSPECIFIED:
		request.ResetReapplyType = enumspb.RESET_REAPPLY_TYPE_SIGNAL
	case enumspb.RESET_REAPPLY_TYPE_SIGNAL:
		// noop
	case enumspb.RESET_REAPPLY_TYPE_NONE:
		// noop
	default:
		return nil, serviceerror.NewInternal("unknown reset type")
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

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if request == nil {
		return nil, errRequestNotSet
	}

	if err := wh.validateExecution(request.WorkflowExecution); err != nil {
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

// ListOpenWorkflowExecutions is a visibility API to list the open executions in a specific namespace.
func (wh *WorkflowHandler) ListOpenWorkflowExecutions(ctx context.Context, request *workflowservice.ListOpenWorkflowExecutionsRequest) (_ *workflowservice.ListOpenWorkflowExecutionsResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if request == nil {
		return nil, errRequestNotSet
	}

	if request.StartTimeFilter == nil {
		request.StartTimeFilter = &filterpb.StartTimeFilter{}
	}

	if timestamp.TimeValue(request.GetStartTimeFilter().GetEarliestTime()).IsZero() {
		request.GetStartTimeFilter().EarliestTime = &minTime
	}

	if timestamp.TimeValue(request.GetStartTimeFilter().GetLatestTime()).IsZero() {
		request.GetStartTimeFilter().LatestTime = &maxTime
	}

	if timestamp.TimeValue(request.StartTimeFilter.GetEarliestTime()).After(timestamp.TimeValue(request.StartTimeFilter.GetLatestTime())) {
		return nil, errEarliestTimeIsGreaterThanLatestTime
	}

	if request.GetMaximumPageSize() <= 0 {
		request.MaximumPageSize = int32(wh.config.VisibilityMaxPageSize(request.GetNamespace()))
	}

	if wh.isListRequestPageSizeTooLarge(request.GetMaximumPageSize(), request.GetNamespace()) {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(errPageSizeTooBigMessage, wh.config.ESIndexMaxResultWindow()))
	}

	namespaceName := namespace.Name(request.GetNamespace())
	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespaceName)
	if err != nil {
		return nil, err
	}

	baseReq := &manager.ListWorkflowExecutionsRequest{
		NamespaceID:       namespaceID,
		Namespace:         namespaceName,
		PageSize:          int(request.GetMaximumPageSize()),
		NextPageToken:     request.NextPageToken,
		EarliestStartTime: timestamp.TimeValue(request.StartTimeFilter.GetEarliestTime()),
		LatestStartTime:   timestamp.TimeValue(request.StartTimeFilter.GetLatestTime()),
	}

	var persistenceResp *manager.ListWorkflowExecutionsResponse
	if request.GetExecutionFilter() != nil {
		if wh.config.DisableListVisibilityByFilter(namespaceName.String()) {
			err = errNoPermission
		} else {
			persistenceResp, err = wh.visibilityMrg.ListOpenWorkflowExecutionsByWorkflowID(
				&manager.ListWorkflowExecutionsByWorkflowIDRequest{
					ListWorkflowExecutionsRequest: baseReq,
					WorkflowID:                    request.GetExecutionFilter().GetWorkflowId(),
				})
		}
		wh.logger.Debug("List open workflow with filter",
			tag.WorkflowNamespace(request.GetNamespace()), tag.WorkflowListWorkflowFilterByID)
	} else if request.GetTypeFilter() != nil {
		if wh.config.DisableListVisibilityByFilter(namespaceName.String()) {
			err = errNoPermission
		} else {
			persistenceResp, err = wh.visibilityMrg.ListOpenWorkflowExecutionsByType(&manager.ListWorkflowExecutionsByTypeRequest{
				ListWorkflowExecutionsRequest: baseReq,
				WorkflowTypeName:              request.GetTypeFilter().GetName(),
			})
		}
		wh.logger.Debug("List open workflow with filter",
			tag.WorkflowNamespace(request.GetNamespace()), tag.WorkflowListWorkflowFilterByType)
	} else {
		persistenceResp, err = wh.visibilityMrg.ListOpenWorkflowExecutions(baseReq)
	}

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

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if request == nil {
		return nil, errRequestNotSet
	}

	if request.StartTimeFilter == nil {
		request.StartTimeFilter = &filterpb.StartTimeFilter{}
	}

	if timestamp.TimeValue(request.GetStartTimeFilter().GetEarliestTime()).IsZero() {
		request.GetStartTimeFilter().EarliestTime = &minTime
	}

	if timestamp.TimeValue(request.GetStartTimeFilter().GetLatestTime()).IsZero() {
		request.GetStartTimeFilter().LatestTime = &maxTime
	}

	if timestamp.TimeValue(request.StartTimeFilter.GetEarliestTime()).After(timestamp.TimeValue(request.StartTimeFilter.GetLatestTime())) {
		return nil, errEarliestTimeIsGreaterThanLatestTime
	}

	if request.GetMaximumPageSize() <= 0 {
		request.MaximumPageSize = int32(wh.config.VisibilityMaxPageSize(request.GetNamespace()))
	}

	if wh.isListRequestPageSizeTooLarge(request.GetMaximumPageSize(), request.GetNamespace()) {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(errPageSizeTooBigMessage, wh.config.ESIndexMaxResultWindow()))
	}

	namespaceName := namespace.Name(request.GetNamespace())
	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespaceName)
	if err != nil {
		return nil, err
	}

	baseReq := &manager.ListWorkflowExecutionsRequest{
		NamespaceID:       namespaceID,
		Namespace:         namespaceName,
		PageSize:          int(request.GetMaximumPageSize()),
		NextPageToken:     request.NextPageToken,
		EarliestStartTime: timestamp.TimeValue(request.StartTimeFilter.GetEarliestTime()),
		LatestStartTime:   timestamp.TimeValue(request.StartTimeFilter.GetLatestTime()),
	}

	var persistenceResp *manager.ListWorkflowExecutionsResponse
	if request.GetExecutionFilter() != nil {
		if wh.config.DisableListVisibilityByFilter(namespaceName.String()) {
			err = errNoPermission
		} else {
			persistenceResp, err = wh.visibilityMrg.ListClosedWorkflowExecutionsByWorkflowID(
				&manager.ListWorkflowExecutionsByWorkflowIDRequest{
					ListWorkflowExecutionsRequest: baseReq,
					WorkflowID:                    request.GetExecutionFilter().GetWorkflowId(),
				})
		}
		wh.logger.Debug("List closed workflow with filter",
			tag.WorkflowNamespace(request.GetNamespace()), tag.WorkflowListWorkflowFilterByID)
	} else if request.GetTypeFilter() != nil {
		if wh.config.DisableListVisibilityByFilter(namespaceName.String()) {
			err = errNoPermission
		} else {
			persistenceResp, err = wh.visibilityMrg.ListClosedWorkflowExecutionsByType(&manager.ListWorkflowExecutionsByTypeRequest{
				ListWorkflowExecutionsRequest: baseReq,
				WorkflowTypeName:              request.GetTypeFilter().GetName(),
			})
		}
		wh.logger.Debug("List closed workflow with filter",
			tag.WorkflowNamespace(request.GetNamespace()), tag.WorkflowListWorkflowFilterByType)
	} else if request.GetStatusFilter() != nil {
		if wh.config.DisableListVisibilityByFilter(namespaceName.String()) {
			err = errNoPermission
		} else {
			if request.GetStatusFilter().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED || request.GetStatusFilter().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
				err = errStatusFilterMustBeNotRunning
			} else {
				persistenceResp, err = wh.visibilityMrg.ListClosedWorkflowExecutionsByStatus(&manager.ListClosedWorkflowExecutionsByStatusRequest{
					ListWorkflowExecutionsRequest: baseReq,
					Status:                        request.GetStatusFilter().GetStatus(),
				})
			}
		}
		wh.logger.Debug("List closed workflow with filter",
			tag.WorkflowNamespace(request.GetNamespace()), tag.WorkflowListWorkflowFilterByStatus)
	} else {
		persistenceResp, err = wh.visibilityMrg.ListClosedWorkflowExecutions(baseReq)
	}

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

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if request == nil {
		return nil, errRequestNotSet
	}

	if request.GetPageSize() <= 0 {
		request.PageSize = int32(wh.config.VisibilityMaxPageSize(request.GetNamespace()))
	}

	if wh.isListRequestPageSizeTooLarge(request.GetPageSize(), request.GetNamespace()) {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(errPageSizeTooBigMessage, wh.config.ESIndexMaxResultWindow()))
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
	persistenceResp, err := wh.visibilityMrg.ListWorkflowExecutions(req)
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

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if request == nil {
		return nil, errRequestNotSet
	}

	if request.GetPageSize() <= 0 {
		request.PageSize = int32(wh.config.VisibilityMaxPageSize(request.GetNamespace()))
	}

	maxPageSize := wh.config.VisibilityArchivalQueryMaxPageSize()
	if int(request.GetPageSize()) > maxPageSize {
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

	visibilityArchiver, err := wh.archiverProvider.GetVisibilityArchiver(URI.Scheme(), common.FrontendServiceName)
	if err != nil {
		return nil, err
	}

	archiverRequest := &archiver.QueryVisibilityRequest{
		NamespaceID:   entry.ID().String(),
		PageSize:      int(request.GetPageSize()),
		NextPageToken: request.NextPageToken,
		Query:         request.GetQuery(),
	}

	searchAttributes, err := wh.saProvider.GetSearchAttributes(wh.config.ESIndexName, false)
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
		if timestamp.TimeValue(execution.GetExecutionTime()).IsZero() {
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

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if request == nil {
		return nil, errRequestNotSet
	}

	if request.GetPageSize() <= 0 {
		request.PageSize = int32(wh.config.VisibilityMaxPageSize(request.GetNamespace()))
	}

	if wh.isListRequestPageSizeTooLarge(request.GetPageSize(), request.GetNamespace()) {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(errPageSizeTooBigMessage, wh.config.ESIndexMaxResultWindow()))
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
	persistenceResp, err := wh.visibilityMrg.ScanWorkflowExecutions(req)
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

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

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
	persistenceResp, err := wh.visibilityMrg.CountWorkflowExecutions(req)
	if err != nil {
		return nil, err
	}

	resp := &workflowservice.CountWorkflowExecutionsResponse{
		Count: persistenceResp.Count,
	}
	return resp, nil
}

// GetSearchAttributes is a visibility API to get all legal keys that could be used in list APIs
func (wh *WorkflowHandler) GetSearchAttributes(ctx context.Context, _ *workflowservice.GetSearchAttributesRequest) (_ *workflowservice.GetSearchAttributesResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	searchAttributes, err := wh.saProvider.GetSearchAttributes(wh.config.ESIndexName, false)
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

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

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
		wh.metricsScope(ctx).Tagged(metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UNSPECIFIED.String())),
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

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if request == nil {
		return nil, errRequestNotSet
	}

	if err := wh.validateExecution(request.Execution); err != nil {
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

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if wh.config.DisallowQuery(request.GetNamespace()) {
		return nil, errQueryDisallowedForNamespace
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if request == nil {
		return nil, errRequestNotSet
	}

	if err := wh.validateExecution(request.Execution); err != nil {
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
		wh.metricsScope(ctx).Tagged(metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UNSPECIFIED.String())),
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
		return nil, err
	}
	return hResponse.GetResponse(), nil
}

// DescribeWorkflowExecution returns information about the specified workflow execution.
func (wh *WorkflowHandler) DescribeWorkflowExecution(ctx context.Context, request *workflowservice.DescribeWorkflowExecutionRequest) (_ *workflowservice.DescribeWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if request == nil {
		return nil, errRequestNotSet
	}

	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	if err := wh.validateExecution(request.Execution); err != nil {
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
		saTypeMap, err := wh.saProvider.GetSearchAttributes(wh.config.ESIndexName, false)
		if err != nil {
			return nil, serviceerror.NewUnavailable(fmt.Sprintf(errUnableToGetSearchAttributesMessage, err))
		}
		searchattribute.ApplyTypeMap(response.GetWorkflowExecutionInfo().GetSearchAttributes(), saTypeMap)
		err = searchattribute.ApplyAliases(wh.saMapper, response.GetWorkflowExecutionInfo().GetSearchAttributes(), request.GetNamespace())
		if err != nil {
			return nil, err
		}
	}

	return &workflowservice.DescribeWorkflowExecutionResponse{
		ExecutionConfig:       response.GetExecutionConfig(),
		WorkflowExecutionInfo: response.GetWorkflowExecutionInfo(),
		PendingActivities:     response.GetPendingActivities(),
		PendingChildren:       response.GetPendingChildren(),
	}, nil
}

// DescribeTaskQueue returns information about the target taskqueue, right now this API returns the
// pollers which polled this taskqueue in last few minutes.
func (wh *WorkflowHandler) DescribeTaskQueue(ctx context.Context, request *workflowservice.DescribeTaskQueueRequest) (_ *workflowservice.DescribeTaskQueueResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if request == nil {
		return nil, errRequestNotSet
	}

	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	if err := wh.validateTaskQueue(request.TaskQueue); err != nil {
		return nil, err
	}

	var matchingResponse *matchingservice.DescribeTaskQueueResponse
	op := func() error {
		var err error
		matchingResponse, err = wh.matchingClient.DescribeTaskQueue(ctx, &matchingservice.DescribeTaskQueueRequest{
			NamespaceId: namespaceID.String(),
			DescRequest: request,
		})
		return err
	}

	err = backoff.Retry(op, frontendServiceRetryPolicy, common.IsServiceTransientError)
	if err != nil {
		return nil, err
	}

	return &workflowservice.DescribeTaskQueueResponse{
		Pollers:         matchingResponse.Pollers,
		TaskQueueStatus: matchingResponse.TaskQueueStatus,
	}, nil
}

// GetClusterInfo return information about Temporal deployment.
func (wh *WorkflowHandler) GetClusterInfo(_ context.Context, _ *workflowservice.GetClusterInfoRequest) (_ *workflowservice.GetClusterInfoResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	metadata, err := wh.clusterMetadataManager.GetCurrentClusterMetadata()
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
		PersistenceStore:  wh.persistenceExecutionManager.GetName(),
		VisibilityStore:   wh.visibilityMrg.GetName(),
	}, nil
}

// GetSystemInfo returns information about the Temporal system.
func (wh *WorkflowHandler) GetSystemInfo(ctx context.Context, request *workflowservice.GetSystemInfoRequest) (_ *workflowservice.GetSystemInfoResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if err := wh.versionChecker.ClientSupported(ctx, wh.config.EnableClientVersionCheck()); err != nil {
		return nil, err
	}

	if request == nil {
		return nil, errRequestNotSet
	}

	_, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	return &workflowservice.GetSystemInfoResponse{
		ServerVersion: headers.ServerVersion,
		// Capabilities should be added as needed. In many cases, capabilities are
		// hardcoded boolean true values since older servers will respond with a
		// form of this message without the field which is implied false.
		Capabilities: &workflowservice.GetSystemInfoResponse_Capabilities{
			SignalAndQueryHeader:         true,
			InternalErrorDifferentiation: true,
		},
	}, nil
}

// ListTaskQueuePartitions returns all the partition and host for a task queue.
func (wh *WorkflowHandler) ListTaskQueuePartitions(ctx context.Context, request *workflowservice.ListTaskQueuePartitionsRequest) (_ *workflowservice.ListTaskQueuePartitionsResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if wh.isStopped() {
		return nil, errShuttingDown
	}

	if request == nil {
		return nil, errRequestNotSet
	}

	if err := wh.validateTaskQueue(request.TaskQueue); err != nil {
		return nil, err
	}

	matchingResponse, err := wh.matchingClient.ListTaskQueuePartitions(ctx, &matchingservice.ListTaskQueuePartitionsRequest{
		Namespace: request.GetNamespace(),
		TaskQueue: request.TaskQueue,
	})

	if matchingResponse == nil {
		return nil, err
	}

	return &workflowservice.ListTaskQueuePartitionsResponse{
		ActivityTaskQueuePartitions: matchingResponse.ActivityTaskQueuePartitions,
		WorkflowTaskQueuePartitions: matchingResponse.WorkflowTaskQueuePartitions,
	}, err
}

func (wh *WorkflowHandler) getRawHistory(
	scope metrics.Scope,
	namespaceID namespace.ID,
	execution commonpb.WorkflowExecution,
	firstEventID int64,
	nextEventID int64,
	pageSize int32,
	nextPageToken []byte,
	transientWorkflowTaskInfo *historyspb.TransientWorkflowTaskInfo,
	branchToken []byte,
) ([]*commonpb.DataBlob, []byte, error) {
	var rawHistory []*commonpb.DataBlob
	shardID := common.WorkflowIDToHistoryShard(namespaceID.String(), execution.GetWorkflowId(), wh.config.NumHistoryShards)

	resp, err := wh.persistenceExecutionManager.ReadRawHistoryBranch(&persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      int(pageSize),
		NextPageToken: nextPageToken,
		ShardID:       shardID,
	})
	if err != nil {
		return nil, nil, err
	}

	for _, data := range resp.HistoryEventBlobs {
		rawHistory = append(rawHistory, &commonpb.DataBlob{
			EncodingType: data.EncodingType,
			Data:         data.Data,
		})
	}

	if len(nextPageToken) == 0 && transientWorkflowTaskInfo != nil {
		if err := wh.validateTransientWorkflowTaskEvents(nextEventID, transientWorkflowTaskInfo); err != nil {
			scope.IncCounter(metrics.ServiceErrIncompleteHistoryCounter)
			wh.logger.Error("getHistory error",
				tag.WorkflowNamespaceID(namespaceID.String()),
				tag.WorkflowID(execution.GetWorkflowId()),
				tag.WorkflowRunID(execution.GetRunId()),
				tag.Error(err))
		}

		blob, err := wh.payloadSerializer.SerializeEvent(transientWorkflowTaskInfo.ScheduledEvent, enumspb.ENCODING_TYPE_PROTO3)
		if err != nil {
			return nil, nil, err
		}
		rawHistory = append(rawHistory, &commonpb.DataBlob{
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			Data:         blob.Data,
		})

		blob, err = wh.payloadSerializer.SerializeEvent(transientWorkflowTaskInfo.StartedEvent, enumspb.ENCODING_TYPE_PROTO3)
		if err != nil {
			return nil, nil, err
		}
		rawHistory = append(rawHistory, &commonpb.DataBlob{
			EncodingType: enumspb.ENCODING_TYPE_PROTO3,
			Data:         blob.Data,
		})
	}

	return rawHistory, resp.NextPageToken, nil
}

func (wh *WorkflowHandler) getHistory(
	scope metrics.Scope,
	namespaceID namespace.ID,
	namespace namespace.Name,
	execution commonpb.WorkflowExecution,
	firstEventID int64,
	nextEventID int64,
	pageSize int32,
	nextPageToken []byte,
	transientWorkflowTaskInfo *historyspb.TransientWorkflowTaskInfo,
	branchToken []byte,
) (*historypb.History, []byte, error) {

	var size int
	isFirstPage := len(nextPageToken) == 0
	shardID := common.WorkflowIDToHistoryShard(namespaceID.String(), execution.GetWorkflowId(), wh.config.NumHistoryShards)
	var err error
	var historyEvents []*historypb.HistoryEvent
	historyEvents, size, nextPageToken, err = persistence.ReadFullPageEvents(wh.persistenceExecutionManager, &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      int(pageSize),
		NextPageToken: nextPageToken,
		ShardID:       shardID,
	})
	switch err.(type) {
	case nil:
		// noop
	case *serviceerror.DataLoss:
		// log event
		wh.logger.Error("encountered data loss event", tag.WorkflowNamespaceID(namespaceID.String()), tag.WorkflowID(execution.GetWorkflowId()), tag.WorkflowRunID(execution.GetRunId()))
		return nil, nil, err
	default:
		return nil, nil, err
	}

	scope.Tagged(metrics.StatsTypeTag(metrics.SizeStatsTypeTagValue)).RecordDistribution(metrics.HistorySize, size)

	isLastPage := len(nextPageToken) == 0
	if err := wh.verifyHistoryIsComplete(
		historyEvents,
		firstEventID,
		nextEventID-1,
		isFirstPage,
		isLastPage,
		int(pageSize)); err != nil {
		scope.IncCounter(metrics.ServiceErrIncompleteHistoryCounter)
		wh.logger.Error("getHistory: incomplete history",
			tag.WorkflowNamespaceID(namespaceID.String()),
			tag.WorkflowID(execution.GetWorkflowId()),
			tag.WorkflowRunID(execution.GetRunId()),
			tag.Error(err))
		return nil, nil, err
	}

	if len(nextPageToken) == 0 && transientWorkflowTaskInfo != nil {
		if err := wh.validateTransientWorkflowTaskEvents(nextEventID, transientWorkflowTaskInfo); err != nil {
			scope.IncCounter(metrics.ServiceErrIncompleteHistoryCounter)
			wh.logger.Error("getHistory error",
				tag.WorkflowNamespaceID(namespaceID.String()),
				tag.WorkflowID(execution.GetWorkflowId()),
				tag.WorkflowRunID(execution.GetRunId()),
				tag.Error(err))
		}
		// Append the transient workflow task events once we are done enumerating everything from the events table
		historyEvents = append(historyEvents, transientWorkflowTaskInfo.ScheduledEvent, transientWorkflowTaskInfo.StartedEvent)
	}

	if err := wh.processSearchAttributes(historyEvents, namespace); err != nil {
		return nil, nil, err
	}

	executionHistory := &historypb.History{
		Events: historyEvents,
	}
	return executionHistory, nextPageToken, nil
}

func (wh *WorkflowHandler) processSearchAttributes(events []*historypb.HistoryEvent, namespace namespace.Name) error {
	saTypeMap, err := wh.saProvider.GetSearchAttributes(wh.config.ESIndexName, false)
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf(errUnableToGetSearchAttributesMessage, err))
	}
	for _, event := range events {
		var searchAttributes *commonpb.SearchAttributes
		switch event.EventType {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
			searchAttributes = event.GetWorkflowExecutionStartedEventAttributes().GetSearchAttributes()
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
			searchAttributes = event.GetWorkflowExecutionContinuedAsNewEventAttributes().GetSearchAttributes()
		case enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED:
			searchAttributes = event.GetStartChildWorkflowExecutionInitiatedEventAttributes().GetSearchAttributes()
		case enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:
			searchAttributes = event.GetUpsertWorkflowSearchAttributesEventAttributes().GetSearchAttributes()
		}
		if searchAttributes != nil {
			searchattribute.ApplyTypeMap(searchAttributes, saTypeMap)
			err = searchattribute.ApplyAliases(wh.saMapper, searchAttributes, namespace.String())
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (wh *WorkflowHandler) validateTransientWorkflowTaskEvents(
	expectedNextEventID int64,
	transientWorkflowTaskInfo *historyspb.TransientWorkflowTaskInfo,
) error {

	if transientWorkflowTaskInfo.ScheduledEvent.GetEventId() == expectedNextEventID &&
		transientWorkflowTaskInfo.StartedEvent.GetEventId() == expectedNextEventID+1 {
		return nil
	}

	return fmt.Errorf("invalid transient workflow task: expectedScheduledEventID=%v expectedStartedEventID=%v but have scheduledEventID=%v startedEventID=%v",
		expectedNextEventID,
		expectedNextEventID+1,
		transientWorkflowTaskInfo.ScheduledEvent.GetEventId(),
		transientWorkflowTaskInfo.StartedEvent.GetEventId())
}

func (wh *WorkflowHandler) validateTaskQueue(t *taskqueuepb.TaskQueue) error {
	if t == nil || t.GetName() == "" {
		return errTaskQueueNotSet
	}
	if len(t.GetName()) > wh.config.MaxIDLengthLimit() {
		return errTaskQueueTooLong
	}

	enums.SetDefaultTaskQueueKind(&t.Kind)
	return nil
}

func (wh *WorkflowHandler) validateExecution(w *commonpb.WorkflowExecution) error {
	err := validateExecution(w)
	if err != nil {
		return err
	}
	return nil
}

func (wh *WorkflowHandler) createPollWorkflowTaskQueueResponse(
	ctx context.Context,
	namespaceID namespace.ID,
	matchingResp *matchingservice.PollWorkflowTaskQueueResponse,
	branchToken []byte,
) (*workflowservice.PollWorkflowTaskQueueResponse, error) {

	if matchingResp.WorkflowExecution == nil {
		// this will happen if there is no workflow task to be send to worker / caller
		return &workflowservice.PollWorkflowTaskQueueResponse{}, nil
	}

	var history *historypb.History
	var continuation []byte
	var err error

	if matchingResp.GetStickyExecutionEnabled() && matchingResp.Query != nil {
		// meaning sticky query, we should not return any events to worker
		// since query task only check the current status
		history = &historypb.History{
			Events: []*historypb.HistoryEvent{},
		}
	} else {
		// here we have 3 cases:
		// 1. sticky && non query task
		// 2. non sticky &&  non query task
		// 3. non sticky && query task
		// for 1, partial history have to be send back
		// for 2 and 3, full history have to be send back

		var persistenceToken []byte

		firstEventID := common.FirstEventID
		nextEventID := matchingResp.GetNextEventId()
		if matchingResp.GetStickyExecutionEnabled() {
			firstEventID = matchingResp.GetPreviousStartedEventId() + 1
		}
		namespaceEntry, dErr := wh.namespaceRegistry.GetNamespaceByID(namespaceID)
		if dErr != nil {
			return nil, dErr
		}
		history, persistenceToken, err = wh.getHistory(
			wh.metricsScope(ctx),
			namespaceID,
			namespaceEntry.Name(),
			*matchingResp.GetWorkflowExecution(),
			firstEventID,
			nextEventID,
			int32(wh.config.HistoryMaxPageSize(namespaceEntry.Name().String())),
			nil,
			matchingResp.GetWorkflowTaskInfo(),
			branchToken,
		)
		if err != nil {
			return nil, err
		}

		if len(persistenceToken) != 0 {
			continuation, err = serializeHistoryToken(&tokenspb.HistoryContinuation{
				RunId:                 matchingResp.WorkflowExecution.GetRunId(),
				FirstEventId:          firstEventID,
				NextEventId:           nextEventID,
				PersistenceToken:      persistenceToken,
				TransientWorkflowTask: matchingResp.GetWorkflowTaskInfo(),
				BranchToken:           branchToken,
			})
			if err != nil {
				return nil, err
			}
		}
	}

	resp := &workflowservice.PollWorkflowTaskQueueResponse{
		TaskToken:                  matchingResp.TaskToken,
		WorkflowExecution:          matchingResp.WorkflowExecution,
		WorkflowType:               matchingResp.WorkflowType,
		PreviousStartedEventId:     matchingResp.PreviousStartedEventId,
		StartedEventId:             matchingResp.StartedEventId,
		Query:                      matchingResp.Query,
		BacklogCountHint:           matchingResp.BacklogCountHint,
		Attempt:                    matchingResp.Attempt,
		History:                    history,
		NextPageToken:              continuation,
		WorkflowExecutionTaskQueue: matchingResp.WorkflowExecutionTaskQueue,
		ScheduledTime:              matchingResp.ScheduledTime,
		StartedTime:                matchingResp.StartedTime,
		Queries:                    matchingResp.Queries,
	}

	return resp, nil
}

func (wh *WorkflowHandler) verifyHistoryIsComplete(
	events []*historypb.HistoryEvent,
	expectedFirstEventID int64,
	expectedLastEventID int64,
	isFirstPage bool,
	isLastPage bool,
	pageSize int,
) error {

	nEvents := len(events)
	if nEvents == 0 {
		if isLastPage {
			// we seem to be returning a non-nil pageToken on the lastPage which
			// in turn cases the client to call getHistory again - only to find
			// there are no more events to consume - bail out if this is the case here
			return nil
		}
		return serviceerror.NewDataLoss("History contains zero events.")
	}

	firstEventID := events[0].GetEventId()
	lastEventID := events[nEvents-1].GetEventId()

	if !isFirstPage { // at least one page of history has been read previously
		if firstEventID <= expectedFirstEventID {
			// not first page and no events have been read in the previous pages - not possible
			return serviceerror.NewDataLoss(fmt.Sprintf("Invalid history: expected first eventID to be > %v but got %v", expectedFirstEventID, firstEventID))
		}
		expectedFirstEventID = firstEventID
	}

	if !isLastPage {
		// estimate lastEventID based on pageSize. This is a lower bound
		// since the persistence layer counts "batch of events" as a single page
		expectedLastEventID = expectedFirstEventID + int64(pageSize) - 1
	}

	nExpectedEvents := expectedLastEventID - expectedFirstEventID + 1

	if firstEventID == expectedFirstEventID &&
		((isLastPage && lastEventID == expectedLastEventID && int64(nEvents) == nExpectedEvents) ||
			(!isLastPage && lastEventID >= expectedLastEventID && int64(nEvents) >= nExpectedEvents)) {
		return nil
	}

	return serviceerror.NewDataLoss(fmt.Sprintf("Incomplete history: expected events [%v-%v] but got events [%v-%v] of length %v: isFirstPage=%v,isLastPage=%v,pageSize=%v",
		expectedFirstEventID,
		expectedLastEventID,
		firstEventID,
		lastEventID,
		nEvents,
		isFirstPage,
		isLastPage,
		pageSize))
}

func (wh *WorkflowHandler) isFailoverRequest(updateRequest *workflowservice.UpdateNamespaceRequest) bool {
	return updateRequest.ReplicationConfig != nil && updateRequest.ReplicationConfig.GetActiveClusterName() != ""
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

	historyArchiver, err := wh.archiverProvider.GetHistoryArchiver(URI.Scheme(), common.FrontendServiceName)
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

func (wh *WorkflowHandler) isListRequestPageSizeTooLarge(pageSize int32, namespace string) bool {
	return wh.config.EnableReadVisibilityFromES(namespace) &&
		pageSize > int32(wh.config.ESIndexMaxResultWindow())
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
	_, err := wh.matchingClient.CancelOutstandingPoll(context.Background(), &matchingservice.CancelOutstandingPollRequest{
		NamespaceId:   namespaceID.String(),
		TaskQueueType: taskQueueType,
		TaskQueue:     taskQueue,
		PollerId:      pollerID,
	})
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

func (hs HealthStatus) String() string {
	switch hs {
	case HealthStatusOK:
		return "OK"
	case HealthStatusShuttingDown:
		return "ShuttingDown"
	default:
		return "unknown"
	}
}

func (wh *WorkflowHandler) validateRetryPolicy(namespaceName namespace.Name, retryPolicy *commonpb.RetryPolicy) error {
	if retryPolicy == nil {
		// By default, if the user does not explicitly set a retry policy for a Workflow, do not perform any retries.
		return nil
	}

	defaultWorkflowRetrySettings := common.FromConfigToDefaultRetrySettings(wh.getDefaultWorkflowRetrySettings(namespaceName.String()))
	common.EnsureRetryPolicyDefaults(retryPolicy, defaultWorkflowRetrySettings)
	return common.ValidateRetryPolicy(retryPolicy)
}

func (wh *WorkflowHandler) validateStartWorkflowTimeouts(
	request *workflowservice.StartWorkflowExecutionRequest,
) error {
	if timestamp.DurationValue(request.GetWorkflowExecutionTimeout()) < 0 {
		return errInvalidWorkflowExecutionTimeoutSeconds
	}

	if timestamp.DurationValue(request.GetWorkflowRunTimeout()) < 0 {
		return errInvalidWorkflowRunTimeoutSeconds
	}

	if timestamp.DurationValue(request.GetWorkflowTaskTimeout()) < 0 {
		return errInvalidWorkflowTaskTimeoutSeconds
	}

	return nil
}

func (wh *WorkflowHandler) validateSignalWithStartWorkflowTimeouts(
	request *workflowservice.SignalWithStartWorkflowExecutionRequest,
) error {
	if timestamp.DurationValue(request.GetWorkflowExecutionTimeout()) < 0 {
		return errInvalidWorkflowExecutionTimeoutSeconds
	}

	if timestamp.DurationValue(request.GetWorkflowRunTimeout()) < 0 {
		return errInvalidWorkflowRunTimeoutSeconds
	}

	if timestamp.DurationValue(request.GetWorkflowTaskTimeout()) < 0 {
		return errInvalidWorkflowTaskTimeoutSeconds
	}

	return nil
}

func (wh *WorkflowHandler) metricsScope(ctx context.Context) metrics.Scope {
	return interceptor.MetricsScope(ctx, wh.logger)
}

func (wh *WorkflowHandler) makeFakeContinuedAsNewEvent(
	_ context.Context,
	lastEvent *historypb.HistoryEvent,
) (*historypb.HistoryEvent, error) {
	switch lastEvent.EventType {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		if lastEvent.GetWorkflowExecutionCompletedEventAttributes().GetNewExecutionRunId() == "" {
			return nil, nil
		}
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		if lastEvent.GetWorkflowExecutionFailedEventAttributes().GetNewExecutionRunId() == "" {
			return nil, nil
		}
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		if lastEvent.GetWorkflowExecutionTimedOutEventAttributes().GetNewExecutionRunId() == "" {
			return nil, nil
		}
	default:
		return nil, nil
	}

	// We need to replace the last event with a continued-as-new event that has at least the
	// NewExecutionRunId field. We don't actually need any other fields, since that's the only one
	// the client looks at in this case, but copy the last result or failure from the real completed
	// event just so it's clear what the result was.
	newAttrs := &historypb.WorkflowExecutionContinuedAsNewEventAttributes{}
	switch lastEvent.EventType {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		attrs := lastEvent.GetWorkflowExecutionCompletedEventAttributes()
		newAttrs.NewExecutionRunId = attrs.NewExecutionRunId
		newAttrs.LastCompletionResult = attrs.Result
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		attrs := lastEvent.GetWorkflowExecutionFailedEventAttributes()
		newAttrs.NewExecutionRunId = attrs.NewExecutionRunId
		newAttrs.Failure = attrs.Failure
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		attrs := lastEvent.GetWorkflowExecutionTimedOutEventAttributes()
		newAttrs.NewExecutionRunId = attrs.NewExecutionRunId
		newAttrs.Failure = failure.NewTimeoutFailure("workflow timeout", enumspb.TIMEOUT_TYPE_START_TO_CLOSE)
	}

	return &historypb.HistoryEvent{
		EventId:   lastEvent.EventId,
		EventTime: lastEvent.EventTime,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		Version:   lastEvent.Version,
		TaskId:    lastEvent.TaskId,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{
			WorkflowExecutionContinuedAsNewEventAttributes: newAttrs,
		},
	}, nil
}

func (wh *WorkflowHandler) validateNamespace(
	namespace string,
) error {
	if namespace == "" {
		return errNamespaceNotSet
	}
	if err := wh.validateUTF8String(namespace); err != nil {
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
	if err := wh.validateUTF8String(workflowID); err != nil {
		return err
	}
	if len(workflowID) > wh.config.MaxIDLengthLimit() {
		return errWorkflowIDTooLong
	}
	return nil
}

func (wh *WorkflowHandler) validateUTF8String(
	str string,
) error {
	if !utf8.ValidString(str) {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("%v is not a valid UTF-8 string", str))
	}
	return nil
}
