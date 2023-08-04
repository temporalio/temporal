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
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

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
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	schedspb "go.temporal.io/server/api/schedule/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
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
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/tasktoken"
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
		status int32

		tokenSerializer                 common.TaskTokenSerializer
		config                          *Config
		versionChecker                  headers.VersionChecker
		namespaceHandler                NamespaceHandler
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
		saMapperProvider                searchattribute.MapperProvider
		saProvider                      searchattribute.Provider
		saValidator                     *searchattribute.Validator
		archivalMetadata                archiver.ArchivalMetadata
		healthServer                    *health.Server
		overrides                       *Overrides
		membershipMonitor               membership.Monitor
	}
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
	saMapperProvider searchattribute.MapperProvider,
	saProvider searchattribute.Provider,
	clusterMetadata cluster.Metadata,
	archivalMetadata archiver.ArchivalMetadata,
	healthServer *health.Server,
	timeSource clock.TimeSource,
	membershipMonitor membership.Monitor,
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
		saMapperProvider:                saMapperProvider,
		saValidator: searchattribute.NewValidator(
			saProvider,
			saMapperProvider,
			config.SearchAttributesNumberOfKeysLimit,
			config.SearchAttributesSizeOfValueLimit,
			config.SearchAttributesTotalSizeLimit,
			visibilityMrg,
			visibility.AllowListForValidation(visibilityMrg.GetStoreNames()),
		),
		archivalMetadata:  archivalMetadata,
		healthServer:      healthServer,
		overrides:         NewOverrides(),
		membershipMonitor: membershipMonitor,
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
func (wh *WorkflowHandler) StartWorkflowExecution(ctx context.Context, request *workflowservice.StartWorkflowExecutionRequest) (_ *workflowservice.StartWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

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

	if err := wh.validateWorkflowStartDelay(request.GetCronSchedule(), request.GetWorkflowStartDelay()); err != nil {
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

	request, err := wh.unaliasStartWorkflowExecutionRequestSearchAttributes(request, namespaceName)
	if err != nil {
		return nil, err
	}

	if err = wh.validateSearchAttributes(request.GetSearchAttributes(), namespaceName); err != nil {
		return nil, err
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
	return &workflowservice.StartWorkflowExecutionResponse{RunId: resp.GetRunId(), EagerWorkflowTask: resp.GetEagerWorkflowTask()}, nil
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
	) ([]byte, string, int64, int64, bool, error) {
		response, err := wh.historyClient.PollMutableState(ctx, &historyservice.PollMutableStateRequest{
			NamespaceId:         namespaceUUID.String(),
			Execution:           execution,
			ExpectedNextEventId: expectedNextEventID,
			CurrentBranchToken:  currentBranchToken,
		})

		if err != nil {
			return nil, "", 0, 0, false, err
		}
		isWorkflowRunning := response.GetWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING

		return response.CurrentBranchToken,
			response.Execution.GetRunId(),
			response.GetLastFirstEventId(),
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
			continuationToken.BranchToken, _, lastFirstEventID, nextEventID, isWorkflowRunning, err =
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
		continuationToken.BranchToken, runID, lastFirstEventID, nextEventID, isWorkflowRunning, err =
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
		if _, ok := retError.(*serviceerror.DataLoss); ok {
			wh.trimHistoryNode(ctx, namespaceID.String(), execution.GetWorkflowId(), execution.GetRunId())
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
					ctx,
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
					ctx,
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
					ctx,
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
					ctx,
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
	if request.GetMaximumPageSize() > common.GetHistoryMaxPageSize {
		wh.throttledLogger.Warn("GetHistory page size is larger than threshold",
			tag.WorkflowID(request.Execution.GetWorkflowId()),
			tag.WorkflowRunID(request.Execution.GetRunId()),
			tag.WorkflowNamespaceID(namespaceID.String()), tag.WorkflowSize(int64(request.GetMaximumPageSize())))
		request.MaximumPageSize = common.GetHistoryMaxPageSize
	}

	queryMutableState := func(
		namespaceUUID namespace.ID,
		execution *commonpb.WorkflowExecution,
		expectedNextEventID int64,
		currentBranchToken []byte,
	) ([]byte, string, int64, error) {
		response, err := wh.historyClient.PollMutableState(ctx, &historyservice.PollMutableStateRequest{
			NamespaceId:         namespaceUUID.String(),
			Execution:           execution,
			ExpectedNextEventId: expectedNextEventID,
			CurrentBranchToken:  currentBranchToken,
		})

		if err != nil {
			return nil, "", 0, err
		}

		return response.CurrentBranchToken,
			response.Execution.GetRunId(),
			response.GetLastFirstEventTxnId(),
			nil
	}

	execution := request.Execution
	var continuationToken *tokenspb.HistoryContinuation

	var runID string
	var lastFirstTxnID int64

	if request.NextPageToken == nil {
		continuationToken = &tokenspb.HistoryContinuation{}
		continuationToken.BranchToken, runID, lastFirstTxnID, err =
			queryMutableState(namespaceID, execution, common.FirstEventID, nil)
		if err != nil {
			return nil, err
		}

		execution.RunId = runID
		continuationToken.RunId = runID
		continuationToken.FirstEventId = common.FirstEventID
		continuationToken.NextEventId = common.EmptyEventID
		continuationToken.PersistenceToken = nil
	} else {
		continuationToken, err = deserializeHistoryToken(request.NextPageToken)
		if err != nil {
			return nil, errInvalidNextPageToken
		}
		if execution.GetRunId() != "" && execution.GetRunId() != continuationToken.GetRunId() {
			return nil, errNextPageTokenRunIDMismatch
		}

		execution.RunId = continuationToken.GetRunId()
	}

	// TODO below is a temporal solution to guard against invalid event batch
	//  when data inconsistency occurs
	//  long term solution should check event batch pointing backwards within history store
	defer func() {
		if _, ok := retError.(*serviceerror.DataLoss); ok {
			wh.trimHistoryNode(ctx, namespaceID.String(), execution.GetWorkflowId(), execution.GetRunId())
		}
	}()

	history := &historypb.History{}
	history.Events = []*historypb.HistoryEvent{}
	// return all events
	history, continuationToken.PersistenceToken, continuationToken.NextEventId, err = wh.getHistoryReverse(
		ctx,
		wh.metricsScope(ctx),
		namespaceID,
		namespace.Name(request.GetNamespace()),
		*execution,
		continuationToken.NextEventId,
		lastFirstTxnID,
		request.GetMaximumPageSize(),
		continuationToken.PersistenceToken,
		continuationToken.BranchToken,
	)

	if err != nil {
		return nil, err
	}

	if continuationToken.NextEventId < continuationToken.FirstEventId {
		continuationToken = nil
	}

	nextToken, err := serializeHistoryToken(continuationToken)
	if err != nil {
		return nil, err
	}

	return &workflowservice.GetWorkflowExecutionHistoryReverseResponse{
		History:       history,
		NextPageToken: nextToken,
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

	resp, err := wh.createPollWorkflowTaskQueueResponse(ctx, namespaceID, matchingResp, matchingResp.GetBranchToken())
	if err != nil {
		return nil, err
	}
	return resp, nil
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

	taskToken, err := wh.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return nil, err
	}
	namespaceId := namespace.ID(taskToken.GetNamespaceId())

	wh.overrides.DisableEagerActivityDispatchForBuggyClients(ctx, request)

	histResp, err := wh.historyClient.RespondWorkflowTaskCompleted(ctx, &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId:     namespaceId.String(),
		CompleteRequest: request},
	)
	if err != nil {
		return nil, err
	}

	completedResp := &workflowservice.RespondWorkflowTaskCompletedResponse{
		ActivityTasks:       histResp.ActivityTasks,
		ResetHistoryEventId: histResp.ResetHistoryEventId,
	}
	if request.GetReturnNewWorkflowTask() && histResp != nil && histResp.StartedResponse != nil {
		taskToken := tasktoken.NewWorkflowTaskToken(
			taskToken.GetNamespaceId(),
			taskToken.GetWorkflowId(),
			taskToken.GetRunId(),
			histResp.StartedResponse.GetScheduledEventId(),
			histResp.StartedResponse.GetStartedEventId(),
			histResp.StartedResponse.GetStartedTime(),
			histResp.StartedResponse.GetAttempt(),
			histResp.StartedResponse.GetClock(),
			histResp.StartedResponse.GetVersion(),
		)
		token, err := wh.tokenSerializer.Serialize(taskToken)
		if err != nil {
			return nil, err
		}
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
		wh.metricsScope(ctx).Counter(metrics.ServiceErrNonDeterministicCounter.GetMetricName()).Record(1)
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

	if err := wh.validateTaskQueue(request.TaskQueue); err != nil {
		return nil, err
	}
	if len(request.GetIdentity()) > wh.config.MaxIDLengthLimit() {
		return nil, errIdentityTooLong
	}

	if err := wh.validateVersioningInfo(request.Namespace, request.WorkerVersionCapabilities, request.TaskQueue); err != nil {
		return nil, err
	}

	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
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

// SignalWorkflowExecution is used to send a signal event to running workflow execution.  This results in
// WorkflowExecutionSignaled event recorded in the history and a workflow task being created for the execution.
func (wh *WorkflowHandler) SignalWorkflowExecution(ctx context.Context, request *workflowservice.SignalWorkflowExecutionRequest) (_ *workflowservice.SignalWorkflowExecutionResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	if err := validateExecution(request.WorkflowExecution); err != nil {
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

	if err := wh.validateWorkflowStartDelay(request.GetCronSchedule(), request.GetWorkflowStartDelay()); err != nil {
		return nil, err
	}

	if err := backoff.ValidateSchedule(request.GetCronSchedule()); err != nil {
		return nil, err
	}

	request, err := wh.unaliasSignalWithStartWorkflowExecutionRequestSearchAttributes(request, namespaceName)
	if err != nil {
		return nil, err
	}

	if err = wh.validateSearchAttributes(request.GetSearchAttributes(), namespaceName); err != nil {
		return nil, err
	}

	enums.SetDefaultWorkflowIdReusePolicy(&request.WorkflowIdReusePolicy)

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

	return &workflowservice.SignalWithStartWorkflowExecutionResponse{RunId: resp.GetRunId()}, nil
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
		WorkflowVersion:    common.EmptyVersion,
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

	maxPageSize := int32(wh.config.VisibilityMaxPageSize(request.GetNamespace()))
	if request.GetMaximumPageSize() <= 0 || request.GetMaximumPageSize() > maxPageSize {
		request.MaximumPageSize = maxPageSize
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
			err = errListNotAllowed
		} else {
			persistenceResp, err = wh.visibilityMrg.ListOpenWorkflowExecutionsByWorkflowID(
				ctx,
				&manager.ListWorkflowExecutionsByWorkflowIDRequest{
					ListWorkflowExecutionsRequest: baseReq,
					WorkflowID:                    request.GetExecutionFilter().GetWorkflowId(),
				})
		}
		wh.logger.Debug("List open workflow with filter",
			tag.WorkflowNamespace(request.GetNamespace()), tag.WorkflowListWorkflowFilterByID)
	} else if request.GetTypeFilter() != nil {
		if wh.config.DisableListVisibilityByFilter(namespaceName.String()) {
			err = errListNotAllowed
		} else {
			persistenceResp, err = wh.visibilityMrg.ListOpenWorkflowExecutionsByType(ctx, &manager.ListWorkflowExecutionsByTypeRequest{
				ListWorkflowExecutionsRequest: baseReq,
				WorkflowTypeName:              request.GetTypeFilter().GetName(),
			})
		}
		wh.logger.Debug("List open workflow with filter",
			tag.WorkflowNamespace(request.GetNamespace()), tag.WorkflowListWorkflowFilterByType)
	} else {
		persistenceResp, err = wh.visibilityMrg.ListOpenWorkflowExecutions(ctx, baseReq)
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

	maxPageSize := int32(wh.config.VisibilityMaxPageSize(request.GetNamespace()))
	if request.GetMaximumPageSize() <= 0 || request.GetMaximumPageSize() > maxPageSize {
		request.MaximumPageSize = maxPageSize
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
			err = errListNotAllowed
		} else {
			persistenceResp, err = wh.visibilityMrg.ListClosedWorkflowExecutionsByWorkflowID(
				ctx,
				&manager.ListWorkflowExecutionsByWorkflowIDRequest{
					ListWorkflowExecutionsRequest: baseReq,
					WorkflowID:                    request.GetExecutionFilter().GetWorkflowId(),
				})
		}
		wh.logger.Debug("List closed workflow with filter",
			tag.WorkflowNamespace(request.GetNamespace()), tag.WorkflowListWorkflowFilterByID)
	} else if request.GetTypeFilter() != nil {
		if wh.config.DisableListVisibilityByFilter(namespaceName.String()) {
			err = errListNotAllowed
		} else {
			persistenceResp, err = wh.visibilityMrg.ListClosedWorkflowExecutionsByType(ctx, &manager.ListWorkflowExecutionsByTypeRequest{
				ListWorkflowExecutionsRequest: baseReq,
				WorkflowTypeName:              request.GetTypeFilter().GetName(),
			})
		}
		wh.logger.Debug("List closed workflow with filter",
			tag.WorkflowNamespace(request.GetNamespace()), tag.WorkflowListWorkflowFilterByType)
	} else if request.GetStatusFilter() != nil {
		if wh.config.DisableListVisibilityByFilter(namespaceName.String()) {
			err = errListNotAllowed
		} else {
			if request.GetStatusFilter().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED || request.GetStatusFilter().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
				err = errStatusFilterMustBeNotRunning
			} else {
				persistenceResp, err = wh.visibilityMrg.ListClosedWorkflowExecutionsByStatus(ctx, &manager.ListClosedWorkflowExecutionsByStatusRequest{
					ListWorkflowExecutionsRequest: baseReq,
					Status:                        request.GetStatusFilter().GetStatus(),
				})
			}
		}
		wh.logger.Debug("List closed workflow with filter",
			tag.WorkflowNamespace(request.GetNamespace()), tag.WorkflowListWorkflowFilterByStatus)
	} else {
		persistenceResp, err = wh.visibilityMrg.ListClosedWorkflowExecutions(ctx, baseReq)
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
	persistenceResp, err := wh.visibilityMrg.ListWorkflowExecutions(ctx, req)
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

	searchAttributes, err := wh.saProvider.GetSearchAttributes(wh.visibilityMrg.GetIndexName(), false)
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
	persistenceResp, err := wh.visibilityMrg.ScanWorkflowExecutions(ctx, req)
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
	persistenceResp, err := wh.visibilityMrg.CountWorkflowExecutions(ctx, req)
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

	searchAttributes, err := wh.saProvider.GetSearchAttributes(wh.visibilityMrg.GetIndexName(), false)
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
		saTypeMap, err := wh.saProvider.GetSearchAttributes(wh.visibilityMrg.GetIndexName(), false)
		if err != nil {
			return nil, serviceerror.NewUnavailable(fmt.Sprintf(errUnableToGetSearchAttributesMessage, err))
		}
		searchattribute.ApplyTypeMap(response.GetWorkflowExecutionInfo().GetSearchAttributes(), saTypeMap)
		aliasedSas, err := searchattribute.AliasFields(wh.saMapperProvider, response.GetWorkflowExecutionInfo().GetSearchAttributes(), request.GetNamespace())
		if err != nil {
			return nil, err
		}
		if aliasedSas != nil {
			response.GetWorkflowExecutionInfo().SearchAttributes = aliasedSas
		}
	}

	return &workflowservice.DescribeWorkflowExecutionResponse{
		ExecutionConfig:       response.GetExecutionConfig(),
		WorkflowExecutionInfo: response.GetWorkflowExecutionInfo(),
		PendingActivities:     response.GetPendingActivities(),
		PendingChildren:       response.GetPendingChildren(),
		PendingWorkflowTask:   response.GetPendingWorkflowTask(),
	}, nil
}

// DescribeTaskQueue returns information about the target taskqueue, right now this API returns the
// pollers which polled this taskqueue in last few minutes.
func (wh *WorkflowHandler) DescribeTaskQueue(ctx context.Context, request *workflowservice.DescribeTaskQueueRequest) (_ *workflowservice.DescribeTaskQueueResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

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

	matchingResponse, err := wh.matchingClient.DescribeTaskQueue(ctx, &matchingservice.DescribeTaskQueueRequest{
		NamespaceId: namespaceID.String(),
		DescRequest: request,
	})
	if err != nil {
		return nil, err
	}

	return &workflowservice.DescribeTaskQueueResponse{
		Pollers:         matchingResponse.Pollers,
		TaskQueueStatus: matchingResponse.TaskQueueStatus,
	}, nil
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
		PersistenceStore:  wh.persistenceExecutionManager.GetName(),
		VisibilityStore:   strings.Join(wh.visibilityMrg.GetStoreNames(), ","),
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
		},
	}, nil
}

// ListTaskQueuePartitions returns all the partition and host for a task queue.
func (wh *WorkflowHandler) ListTaskQueuePartitions(ctx context.Context, request *workflowservice.ListTaskQueuePartitionsRequest) (_ *workflowservice.ListTaskQueuePartitionsResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	if err := wh.validateTaskQueue(request.TaskQueue); err != nil {
		return nil, err
	}

	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
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

	request, err = wh.unaliasCreateScheduleRequestSearchAttributes(request, namespaceName)
	if err != nil {
		return nil, err
	}

	err = wh.validateSearchAttributes(request.GetSearchAttributes(), namespaceName)
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
		Namespace:             request.Namespace,
		WorkflowId:            workflowID,
		WorkflowType:          &commonpb.WorkflowType{Name: scheduler.WorkflowType},
		TaskQueue:             &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
		Input:                 inputPayloads,
		Identity:              request.Identity,
		RequestId:             request.RequestId,
		WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		Memo:                  request.Memo,
		SearchAttributes:      request.SearchAttributes,
	}
	_, err = wh.historyClient.StartWorkflowExecution(ctx, common.CreateHistoryStartWorkflowRequest(namespaceID.String(), startReq, nil, time.Now().UTC()))

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
		saTypeMap, err := wh.saProvider.GetSearchAttributes(wh.visibilityMrg.GetIndexName(), false)
		if err != nil {
			return nil, serviceerror.NewUnavailable(fmt.Sprintf(errUnableToGetSearchAttributesMessage, err))
		}
		searchattribute.ApplyTypeMap(sas, saTypeMap)
		aliasedSas, err := searchattribute.AliasFields(wh.saMapperProvider, sas, request.GetNamespace())
		if err != nil {
			return nil, err
		}
		if aliasedSas != nil {
			executionInfo.SearchAttributes = aliasedSas
		}
	}

	// then query to get current state from the workflow itself
	// TODO: turn the refresh path into a synchronous update so we don't have to retry in a loop
	sentRefresh := make(map[commonpb.WorkflowExecution]struct{})
	// limit how many signals we send, separate from the retry policy (which is used to retry
	// the query if the signal was not received or processed yet)
	signalsLeft := 1
	var queryResponse schedspb.DescribeResponse

	op := func(ctx context.Context) error {
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
			return err
		}

		queryResponse.Reset()
		err = payloads.Decode(res.GetResponse().GetQueryResult(), &queryResponse)
		if err != nil {
			return err
		}

		// Search attributes in the Action are already in external ("aliased") form. Do not alias them here.

		// for all running workflows started by the schedule, we should check that they're
		// still running, and if not, poke the schedule to refresh
		needRefresh := false
		for _, ex := range queryResponse.GetInfo().GetRunningWorkflows() {
			if _, ok := sentRefresh[*ex]; ok {
				// we asked the schedule to refresh this one because it wasn't running, but
				// it's still reporting it as running
				return errWaitForRefresh
			}

			// we'll usually have just zero or one of these so we can just do them sequentially
			if msResponse, err := wh.historyClient.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
				NamespaceId: namespaceID.String(),
				// Note: do not send runid here so that we always get the latest one
				Execution: &commonpb.WorkflowExecution{WorkflowId: ex.WorkflowId},
			}); err != nil {
				switch err.(type) {
				case *serviceerror.NotFound:
					// if it doesn't exist (past retention period?) it's certainly not running
					needRefresh = true
					sentRefresh[*ex] = struct{}{}
				default:
					return err
				}
			} else if msResponse.WorkflowStatus != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING ||
				msResponse.FirstExecutionRunId != ex.RunId {
				// there is no running execution of this workflow id, or there is a running
				// execution, but it's not part of the chain that we started.
				// either way, the workflow that we started is not running.
				needRefresh = true
				sentRefresh[*ex] = struct{}{}
			}
		}

		if !needRefresh || signalsLeft == 0 {
			return nil
		}
		signalsLeft--

		// poke to refresh
		_, err = wh.historyClient.SignalWorkflowExecution(ctx, &historyservice.SignalWorkflowExecutionRequest{
			NamespaceId: namespaceID.String(),
			SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
				Namespace:         request.Namespace,
				WorkflowExecution: execution,
				SignalName:        scheduler.SignalNameRefresh,
				Identity:          "internal refresh from describe request",
				RequestId:         uuid.New(),
			},
		})
		if err != nil {
			return err
		}

		return errWaitForRefresh
	}

	// wait up to 4 seconds or rpc deadline minus 1 second, but at least 1 second
	expiration := 4 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline) - 1*time.Second
		expiration = util.Min(expiration, remaining)
	}
	expiration = util.Max(expiration, 1*time.Second)
	policy := backoff.NewExponentialRetryPolicy(200 * time.Millisecond).
		WithExpirationInterval(expiration)
	isWaitErr := func(e error) bool { return e == errWaitForRefresh }

	err = backoff.ThrottleRetryContext(ctx, op, policy, isWaitErr)
	// if we still got errWaitForRefresh that means we used up our retries, just return
	// whatever we have
	if err != nil && err != errWaitForRefresh {
		return nil, err
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

// Changes the configuration or state of an existing schedule.
func (wh *WorkflowHandler) UpdateSchedule(ctx context.Context, request *workflowservice.UpdateScheduleRequest) (_ *workflowservice.UpdateScheduleResponse, retError error) {
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

	if err = wh.validateStartWorkflowArgsForSchedule(namespaceName, request.GetSchedule().GetAction().GetStartWorkflow()); err != nil {
		return nil, err
	}

	input := &schedspb.FullUpdateRequest{
		Schedule: request.Schedule,
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
func (wh *WorkflowHandler) ListSchedules(ctx context.Context, request *workflowservice.ListSchedulesRequest) (_ *workflowservice.ListSchedulesResponse, retError error) {
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

	persistenceResp, err := wh.visibilityMrg.ListOpenWorkflowExecutionsByType(ctx, &manager.ListWorkflowExecutionsByTypeRequest{
		ListWorkflowExecutionsRequest: &manager.ListWorkflowExecutionsRequest{
			NamespaceID:       namespaceID,
			Namespace:         namespaceName,
			NamespaceDivision: scheduler.NamespaceDivision,
			PageSize:          int(request.GetMaximumPageSize()),
			NextPageToken:     request.NextPageToken,
			EarliestStartTime: minTime,
			LatestStartTime:   maxTime,
		},
		WorkflowTypeName: scheduler.WorkflowType,
	})
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

	if request == nil {
		return nil, errRequestNotSet
	}

	if err := validateExecution(request.GetWorkflowExecution()); err != nil {
		return nil, err
	}

	if request.GetRequest().GetMeta() == nil {
		return nil, errUpdateMetaNotSet
	}

	if len(request.GetRequest().GetMeta().GetUpdateId()) > wh.config.MaxIDLengthLimit() {
		return nil, errUpdateIDTooLong
	}

	if request.GetRequest().GetMeta().GetUpdateId() == "" {
		request.GetRequest().GetMeta().UpdateId = uuid.New()
	}

	if request.GetRequest().GetInput() == nil {
		return nil, errUpdateInputNotSet
	}

	if request.GetRequest().GetInput().GetName() == "" {
		return nil, errUpdateNameNotSet
	}

	if request.GetWaitPolicy() == nil {
		request.WaitPolicy = &updatepb.WaitPolicy{}
	}
	enums.SetDefaultUpdateWorkflowExecutionLifecycleStage(&request.GetWaitPolicy().LifecycleStage)

	nsID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	if !wh.config.EnableUpdateWorkflowExecution(request.Namespace) {
		return nil, errUpdateWorkflowExecutionAPINotAllowed
	}

	if request.WaitPolicy.LifecycleStage == enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED &&
		!wh.config.EnableUpdateWorkflowExecutionAsyncAccepted(request.Namespace) {
		return nil, errUpdateWorkflowExecutionAsyncAcceptedNotAllowed
	}

	histResp, err := wh.historyClient.UpdateWorkflowExecution(ctx, &historyservice.UpdateWorkflowExecutionRequest{
		NamespaceId: nsID.String(),
		Request:     request,
	})

	return histResp.GetResponse(), err
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
	enums.SetDefaultUpdateWorkflowExecutionLifecycleStage(&request.GetWaitPolicy().LifecycleStage)

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

	if err := wh.validateTaskQueue(&taskqueuepb.TaskQueue{Name: request.GetTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL}); err != nil {
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

	if err := wh.validateTaskQueue(&taskqueuepb.TaskQueue{Name: request.GetTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL}); err != nil {
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

func (wh *WorkflowHandler) GetWorkerTaskReachability(ctx context.Context, request *workflowservice.GetWorkerTaskReachabilityRequest) (_ *workflowservice.GetWorkerTaskReachabilityResponse, retError error) {
	defer log.CapturePanic(wh.logger, &retError)

	if request == nil {
		return nil, errRequestNotSet
	}

	if !wh.config.EnableWorkerVersioningData(request.Namespace) {
		return nil, errWorkerVersioningNotAllowed
	}

	if len(request.GetBuildIds()) == 0 {
		return nil, serviceerror.NewInvalidArgument("Must query at least one build id (or empty string for unversioned worker)")
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
		return nil, serviceerror.NewInvalidArgument("Cannot get reachability of an unversioned worker without specifying at least one task queue (empty build id is interpereted as unversioned)")
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
		return nil, serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT, "Max concurrent batch operations is reached")
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
		resetParams.ResetType = op.ResetOperation.GetResetType()
		resetParams.ResetReapplyType = op.ResetOperation.GetResetReapplyType()
	default:
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("The operation type %T is not supported", op))
	}

	input := &batcher.BatchParams{
		Namespace:       request.GetNamespace(),
		Query:           request.GetVisibilityQuery(),
		Executions:      request.GetExecutions(),
		Reason:          request.GetReason(),
		BatchType:       operationType,
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
		Namespace:             request.Namespace,
		WorkflowId:            request.GetJobId(),
		WorkflowType:          &commonpb.WorkflowType{Name: batcher.BatchWFTypeName},
		TaskQueue:             &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
		Input:                 inputPayload,
		Identity:              identity,
		RequestId:             uuid.New(),
		WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		Memo:                  memo,
		SearchAttributes:      searchAttributes,
	}

	_, err = wh.historyClient.StartWorkflowExecution(ctx, common.CreateHistoryStartWorkflowRequest(namespaceID.String(), startReq, nil, time.Now().UTC()))
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
	default:
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("The operation type %s is not supported", operationTypeString))
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

	resp, err := wh.ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace:     request.GetNamespace(),
		PageSize:      int32(wh.config.VisibilityMaxPageSize(request.GetNamespace())),
		NextPageToken: request.GetNextPageToken(),
		Query: fmt.Sprintf("%s = '%s' and %s='%s'",
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

func (wh *WorkflowHandler) getRawHistory(
	ctx context.Context,
	metricsHandler metrics.Handler,
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

	resp, err := wh.persistenceExecutionManager.ReadRawHistoryBranch(ctx, &persistence.ReadHistoryBranchRequest{
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

	if len(resp.NextPageToken) == 0 && transientWorkflowTaskInfo != nil {
		if err := wh.validateTransientWorkflowTaskEvents(nextEventID, transientWorkflowTaskInfo); err != nil {
			metricsHandler.Counter(metrics.ServiceErrIncompleteHistoryCounter.GetMetricName()).Record(1)
			wh.logger.Error("getHistory error",
				tag.WorkflowNamespaceID(namespaceID.String()),
				tag.WorkflowID(execution.GetWorkflowId()),
				tag.WorkflowRunID(execution.GetRunId()),
				tag.Error(err))
			return nil, nil, err
		}

		for _, event := range transientWorkflowTaskInfo.HistorySuffix {
			blob, err := wh.payloadSerializer.SerializeEvent(event, enumspb.ENCODING_TYPE_PROTO3)
			if err != nil {
				return nil, nil, err
			}
			rawHistory = append(rawHistory, &commonpb.DataBlob{
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				Data:         blob.Data,
			})
		}
	}

	return rawHistory, resp.NextPageToken, nil
}

func (wh *WorkflowHandler) getHistory(
	ctx context.Context,
	metricsHandler metrics.Handler,
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
	historyEvents, size, nextPageToken, err = persistence.ReadFullPageEvents(ctx, wh.persistenceExecutionManager, &persistence.ReadHistoryBranchRequest{
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
		wh.logger.Error("encountered data loss event",
			tag.WorkflowNamespaceID(namespaceID.String()),
			tag.WorkflowID(execution.GetWorkflowId()),
			tag.WorkflowRunID(execution.GetRunId()),
			tag.Error(err),
		)
		return nil, nil, err
	default:
		return nil, nil, err
	}

	metricsHandler.Histogram(metrics.HistorySize.GetMetricName(), metrics.HistorySize.GetMetricUnit()).Record(int64(size))

	isLastPage := len(nextPageToken) == 0
	if err := wh.verifyHistoryIsComplete(
		historyEvents,
		firstEventID,
		nextEventID-1,
		isFirstPage,
		isLastPage,
		int(pageSize)); err != nil {
		metricsHandler.Counter(metrics.ServiceErrIncompleteHistoryCounter.GetMetricName()).Record(1)
		wh.logger.Error("getHistory: incomplete history",
			tag.WorkflowNamespaceID(namespaceID.String()),
			tag.WorkflowID(execution.GetWorkflowId()),
			tag.WorkflowRunID(execution.GetRunId()),
			tag.Error(err))
	}

	if len(nextPageToken) == 0 && transientWorkflowTaskInfo != nil {
		if err := wh.validateTransientWorkflowTaskEvents(nextEventID, transientWorkflowTaskInfo); err != nil {
			metricsHandler.Counter(metrics.ServiceErrIncompleteHistoryCounter.GetMetricName()).Record(1)
			wh.logger.Error("getHistory error",
				tag.WorkflowNamespaceID(namespaceID.String()),
				tag.WorkflowID(execution.GetWorkflowId()),
				tag.WorkflowRunID(execution.GetRunId()),
				tag.Error(err))
		}
		// Append the transient workflow task events once we are done enumerating everything from the events table
		historyEvents = append(historyEvents, transientWorkflowTaskInfo.HistorySuffix...)
	}

	if err := wh.processOutgoingSearchAttributes(historyEvents, namespace); err != nil {
		return nil, nil, err
	}

	executionHistory := &historypb.History{
		Events: historyEvents,
	}
	return executionHistory, nextPageToken, nil
}

func (wh *WorkflowHandler) getHistoryReverse(
	ctx context.Context,
	metricsHandler metrics.Handler,
	namespaceID namespace.ID,
	namespace namespace.Name,
	execution commonpb.WorkflowExecution,
	nextEventID int64,
	lastFirstTxnID int64,
	pageSize int32,
	nextPageToken []byte,
	branchToken []byte,
) (*historypb.History, []byte, int64, error) {
	var size int
	shardID := common.WorkflowIDToHistoryShard(namespaceID.String(), execution.GetWorkflowId(), wh.config.NumHistoryShards)
	var err error
	var historyEvents []*historypb.HistoryEvent

	historyEvents, size, nextPageToken, err = persistence.ReadFullPageEventsReverse(ctx, wh.persistenceExecutionManager, &persistence.ReadHistoryBranchReverseRequest{
		BranchToken:            branchToken,
		MaxEventID:             nextEventID,
		LastFirstTransactionID: lastFirstTxnID,
		PageSize:               int(pageSize),
		NextPageToken:          nextPageToken,
		ShardID:                shardID,
	})

	switch err.(type) {
	case nil:
		// noop
	case *serviceerror.DataLoss:
		// log event
		wh.logger.Error("encountered data loss event", tag.WorkflowNamespaceID(namespaceID.String()), tag.WorkflowID(execution.GetWorkflowId()), tag.WorkflowRunID(execution.GetRunId()))
		return nil, nil, 0, err
	default:
		return nil, nil, 0, err
	}

	metricsHandler.Histogram(metrics.HistorySize.GetMetricName(), metrics.HistorySize.GetMetricUnit()).Record(int64(size))

	if err := wh.processOutgoingSearchAttributes(historyEvents, namespace); err != nil {
		return nil, nil, 0, err
	}

	executionHistory := &historypb.History{
		Events: historyEvents,
	}

	var newNextEventID int64
	if len(historyEvents) > 0 {
		newNextEventID = historyEvents[len(historyEvents)-1].EventId - 1
	} else {
		newNextEventID = nextEventID
	}

	return executionHistory, nextPageToken, newNextEventID, nil
}

func (wh *WorkflowHandler) processOutgoingSearchAttributes(events []*historypb.HistoryEvent, namespace namespace.Name) error {
	saTypeMap, err := wh.saProvider.GetSearchAttributes(wh.visibilityMrg.GetIndexName(), false)
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
			aliasedSas, err := searchattribute.AliasFields(wh.saMapperProvider, searchAttributes, namespace.String())
			if err != nil {
				return err
			}
			if aliasedSas != nil {
				searchAttributes.IndexedFields = aliasedSas.IndexedFields
			}
		}
	}

	return nil
}

func (wh *WorkflowHandler) validateSearchAttributes(searchAttributes *commonpb.SearchAttributes, namespaceName namespace.Name) error {
	if err := wh.saValidator.Validate(searchAttributes, namespaceName.String()); err != nil {
		return err
	}
	return wh.saValidator.ValidateSize(searchAttributes, namespaceName.String())
}

func (wh *WorkflowHandler) validateTransientWorkflowTaskEvents(
	eventIDOffset int64,
	transientWorkflowTaskInfo *historyspb.TransientWorkflowTaskInfo,
) error {
	for i, event := range transientWorkflowTaskInfo.HistorySuffix {
		expectedEventID := eventIDOffset + int64(i)
		if event.GetEventId() != expectedEventID {
			return serviceerror.NewInternal(
				fmt.Sprintf(
					"invalid transient workflow task at position %v; expected event ID %v, found event ID %v",
					i,
					expectedEventID,
					event.GetEventId()))
		}
	}

	return nil
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
	errDeets := []string{"request to update worker build id compatability requires: "}

	checkIdLen := func(id string) {
		if len(id) > wh.config.WorkerBuildIdSizeLimit() {
			errDeets = append(errDeets, fmt.Sprintf(" Worker build IDs to be no larger than %v characters",
				wh.config.WorkerBuildIdSizeLimit()))
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

func (wh *WorkflowHandler) createPollWorkflowTaskQueueResponse(
	ctx context.Context,
	namespaceID namespace.ID,
	matchingResp *matchingservice.PollWorkflowTaskQueueResponse,
	branchToken []byte,
) (_ *workflowservice.PollWorkflowTaskQueueResponse, retError error) {

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

		// TODO below is a temporal solution to guard against invalid event batch
		//  when data inconsistency occurs
		//  long term solution should check event batch pointing backwards within history store
		defer func() {
			if _, ok := retError.(*serviceerror.DataLoss); ok {
				wh.trimHistoryNode(ctx, namespaceID.String(), matchingResp.WorkflowExecution.GetWorkflowId(), matchingResp.WorkflowExecution.GetRunId())
			}
		}()
		history, persistenceToken, err = wh.getHistory(
			ctx,
			wh.metricsScope(ctx),
			namespaceID,
			namespaceEntry.Name(),
			*matchingResp.GetWorkflowExecution(),
			firstEventID,
			nextEventID,
			int32(wh.config.HistoryMaxPageSize(namespaceEntry.Name().String())),
			nil,
			matchingResp.GetTransientWorkflowTask(),
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
				TransientWorkflowTask: matchingResp.GetTransientWorkflowTask(),
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
		Messages:                   matchingResp.Messages,
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

func (wh *WorkflowHandler) validateWorkflowStartDelay(
	cronSchedule string,
	startDelay *time.Duration,
) error {
	if len(cronSchedule) > 0 && startDelay != nil {
		return errCronAndStartDelaySet
	}

	if timestamp.DurationValue(startDelay) < 0 {
		return errInvalidWorkflowStartDelaySeconds
	}

	return nil
}

func (wh *WorkflowHandler) metricsScope(ctx context.Context) metrics.Handler {
	return interceptor.GetMetricsHandlerFromContext(ctx, wh.logger)
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

func (wh *WorkflowHandler) trimHistoryNode(
	ctx context.Context,
	namespaceID string,
	workflowID string,
	runID string,
) {
	response, err := wh.historyClient.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
		NamespaceId: namespaceID,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	})
	if err != nil {
		return // abort
	}

	_, err = wh.persistenceExecutionManager.TrimHistoryBranch(ctx, &persistence.TrimHistoryBranchRequest{
		ShardID:       common.WorkflowIDToHistoryShard(namespaceID, workflowID, wh.config.NumHistoryShards),
		BranchToken:   response.CurrentBranchToken,
		NodeID:        response.GetLastFirstEventId(),
		TransactionID: response.GetLastFirstEventTxnId(),
	})
	if err != nil {
		// best effort
		wh.logger.Error("unable to trim history branch",
			tag.WorkflowNamespaceID(namespaceID),
			tag.WorkflowID(workflowID),
			tag.WorkflowRunID(runID),
			tag.Error(err),
		)
	}
}

func (wh *WorkflowHandler) canonicalizeScheduleSpec(schedule *schedpb.Schedule) error {
	if schedule.Spec == nil {
		schedule.Spec = &schedpb.ScheduleSpec{}
	}
	compiledSpec, err := scheduler.NewCompiledSpec(schedule.Spec)
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
	info := scheduler.GetListInfoFromStartArgs(args, time.Now().UTC())
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

func (wh *WorkflowHandler) unaliasStartWorkflowExecutionRequestSearchAttributes(request *workflowservice.StartWorkflowExecutionRequest, namespaceName namespace.Name) (*workflowservice.StartWorkflowExecutionRequest, error) {
	unaliasedSas, err := searchattribute.UnaliasFields(wh.saMapperProvider, request.GetSearchAttributes(), namespaceName.String())
	if err != nil {
		return nil, err
	}
	if unaliasedSas == nil {
		return request, nil
	}

	// Shallow copy request and replace SearchAttributes fields only.
	newRequest := *request
	newRequest.SearchAttributes = unaliasedSas
	return &newRequest, nil
}

func (wh *WorkflowHandler) unaliasSignalWithStartWorkflowExecutionRequestSearchAttributes(request *workflowservice.SignalWithStartWorkflowExecutionRequest, namespaceName namespace.Name) (*workflowservice.SignalWithStartWorkflowExecutionRequest, error) {
	unaliasedSas, err := searchattribute.UnaliasFields(wh.saMapperProvider, request.GetSearchAttributes(), namespaceName.String())
	if err != nil {
		return nil, err
	}
	if unaliasedSas == nil {
		return request, nil
	}

	// Shallow copy request and replace SearchAttributes fields only.
	newRequest := *request
	newRequest.SearchAttributes = unaliasedSas
	return &newRequest, nil
}

func (wh *WorkflowHandler) unaliasCreateScheduleRequestSearchAttributes(request *workflowservice.CreateScheduleRequest, namespaceName namespace.Name) (*workflowservice.CreateScheduleRequest, error) {
	unaliasedSas, err := searchattribute.UnaliasFields(wh.saMapperProvider, request.GetSearchAttributes(), namespaceName.String())
	if err != nil {
		return nil, err
	}

	if unaliasedSas == nil {
		return request, nil
	}

	// Shallow copy request and replace SearchAttributes fields only.
	newRequest := *request

	if unaliasedSas != nil {
		newRequest.SearchAttributes = unaliasedSas
	}

	return &newRequest, nil
}
