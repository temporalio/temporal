package history

import (
	"context"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/trace"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/fxutil"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/api/addtasks"
	"go.temporal.io/server/service/history/api/deleteworkflow"
	"go.temporal.io/server/service/history/api/describemutablestate"
	"go.temporal.io/server/service/history/api/describeworkflow"
	"go.temporal.io/server/service/history/api/getworkflowexecutionhistory"
	"go.temporal.io/server/service/history/api/getworkflowexecutionhistoryreverse"
	"go.temporal.io/server/service/history/api/getworkflowexecutionrawhistory"
	"go.temporal.io/server/service/history/api/getworkflowexecutionrawhistoryv2"
	"go.temporal.io/server/service/history/api/isactivitytaskvalid"
	"go.temporal.io/server/service/history/api/isworkflowtaskvalid"
	"go.temporal.io/server/service/history/api/listtasks"
	"go.temporal.io/server/service/history/api/multioperation"
	"go.temporal.io/server/service/history/api/pauseactivity"
	"go.temporal.io/server/service/history/api/pollupdate"
	"go.temporal.io/server/service/history/api/queryworkflow"
	"go.temporal.io/server/service/history/api/reapplyevents"
	"go.temporal.io/server/service/history/api/recordactivitytaskheartbeat"
	"go.temporal.io/server/service/history/api/recordactivitytaskstarted"
	"go.temporal.io/server/service/history/api/recordchildworkflowcompleted"
	"go.temporal.io/server/service/history/api/recordworkflowtaskstarted"
	"go.temporal.io/server/service/history/api/refreshworkflow"
	"go.temporal.io/server/service/history/api/removesignalmutablestate"
	replicationapi "go.temporal.io/server/service/history/api/replication"
	"go.temporal.io/server/service/history/api/replicationadmin"
	"go.temporal.io/server/service/history/api/requestcancelworkflow"
	"go.temporal.io/server/service/history/api/resetactivity"
	"go.temporal.io/server/service/history/api/resetstickytaskqueue"
	"go.temporal.io/server/service/history/api/resetworkflow"
	"go.temporal.io/server/service/history/api/respondactivitytaskcanceled"
	"go.temporal.io/server/service/history/api/respondactivitytaskcompleted"
	"go.temporal.io/server/service/history/api/respondactivitytaskfailed"
	"go.temporal.io/server/service/history/api/respondworkflowtaskcompleted"
	"go.temporal.io/server/service/history/api/respondworkflowtaskfailed"
	"go.temporal.io/server/service/history/api/scheduleworkflowtask"
	"go.temporal.io/server/service/history/api/signalwithstartworkflow"
	"go.temporal.io/server/service/history/api/signalworkflow"
	"go.temporal.io/server/service/history/api/startworkflow"
	"go.temporal.io/server/service/history/api/terminateworkflow"
	"go.temporal.io/server/service/history/api/unpauseactivity"
	"go.temporal.io/server/service/history/api/updateactivityoptions"
	"go.temporal.io/server/service/history/api/updateworkflow"
	"go.temporal.io/server/service/history/api/updateworkflowoptions"
	"go.temporal.io/server/service/history/api/verifychildworkflowcompletionrecorded"
	"go.temporal.io/server/service/history/api/verifyfirstworkflowtaskscheduled"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/deletemanager"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/ndc"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/replication"
	"go.temporal.io/server/service/history/tasks"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/fx"
)

type (
	// These are fields of historyEngineImpl that are supplied from shard context or constructed by fx.
	engineImplDeps struct {
		fx.In

		ShardContext               historyi.ShardContext
		NamespaceRegistry          namespace.Registry
		ExecutionManager           persistence.ExecutionManager
		ClusterMetadata            cluster.Metadata
		Config                     *configs.Config
		TimeSource                 clock.TimeSource
		Logger                     log.Logger
		ThrottledLogger            log.ThrottledLogger
		MetricsHandler             metrics.Handler
		TokenSerializer            *tasktoken.Serializer
		EventSerializer            serialization.Serializer
		VersionChecker             headers.VersionChecker
		TaskCategoryRegistry       tasks.TaskCategoryRegistry
		Tracer                     trace.Tracer
		WorkflowDeleteManager      deletemanager.DeleteManager
		WorkflowConsistencyChecker api.WorkflowConsistencyChecker
		SearchAttributesValidator  *searchattribute.Validator
		SyncStateRetriever         replication.SyncStateRetriever
		WorkflowRebuilder          workflowRebuilder
		WorkflowResetter           ndc.WorkflowResetter
		EventsReapplier            ndc.EventsReapplier
		ReplicationDLQHandler      replication.DLQHandler
		ReplicationProcessorMgr    replication.TaskProcessor
		QueueProcessors            map[tasks.Category]queues.Queue

		// Only for global namespace:
		ReplicationAckMgr          replication.AckManager      `optional:"true"`
		NDCHistoryReplicator       ndc.HistoryReplicator       `optional:"true"`
		NDCHistoryImporter         ndc.HistoryImporter         `optional:"true"`
		NDCActivityStateReplicator ndc.ActivityStateReplicator `optional:"true"`
		NDCWorkflowStateReplicator ndc.WorkflowStateReplicator `optional:"true"`
		NDCHSMStateReplicator      ndc.HSMStateReplicator      `optional:"true"`

		// API handler deps
		// TODO: make these able to be pointers so indirect deps can be shared instead of
		// expanding recursively
		GetOrPollMutableState              api.GetOrPollMutableStateDeps
		QueryWorkflow                      queryworkflow.Deps
		DescribeMutableState               describemutablestate.Deps
		DescribeWorkflow                   describeworkflow.Deps
		RecordWorkflowTaskStarted          recordworkflowtaskstarted.Deps
		RespondWorkflowTaskCompleted       respondworkflowtaskcompleted.Deps
		GetWorkflowExecutionHistory        getworkflowexecutionhistory.Deps
		GetWorkflowExecutionHistoryReverse getworkflowexecutionhistoryreverse.Deps
		GetWorkflowExecutionRawHistory     getworkflowexecutionrawhistory.Deps
		GetWorkflowExecutionRawHistoryV2   getworkflowexecutionrawhistoryv2.Deps
	}

	historyEngineImpl struct {
		status             int32
		currentClusterName string

		// We can't embed these at top-level because that would make historyEngineImpl itself
		// an 'fx.In'. We can use a struct to separate them without any indirection in memory.
		deps struct {
			HistoryEngineFactoryParams
			engineImplDeps
		}
	}

	// proxy struct used to create cycle between historyEngineImpl and replication.taskProcessorManagerImpl
	engineProxy struct {
		historyi.Engine
	}
)

var engineFx = fx.Options(
	fxutil.LogAdapter,
	fx.Provide(fx.Annotate(deletemanager.NewDeleteManager, fx.As(new(deletemanager.DeleteManager)))),
	fx.Provide(fx.Annotate(replication.NewSyncStateRetriever, fx.As(new(replication.SyncStateRetriever)))),
	fx.Provide(fx.Annotate(ndc.NewEventsReapplier, fx.As(new(ndc.EventsReapplier)))),
	fx.Provide(fx.Annotate(NewWorkflowRebuilder, fx.As(new(workflowRebuilder)))),
	fx.Provide(fx.Annotate(ndc.NewWorkflowResetter, fx.As(new(ndc.WorkflowResetter)))),
	fx.Provide(fx.Annotate(api.NewWorkflowConsistencyChecker, fx.As(new(api.WorkflowConsistencyChecker)))),
	fx.Provide(newSearchAttributeValidator),
	fx.Provide(replication.NewLazyDLQHandler),
	fx.Provide(tasktoken.NewSerializer),
	fx.Provide(api.NewCommandAttrValidator), // used to construct respondworkflowtaskcompleted.Deps
	fx.Provide(fx.Annotate(headers.NewDefaultVersionChecker, fx.As(new(headers.VersionChecker)))),
	fx.Provide(func(tp trace.TracerProvider) trace.Tracer { return tp.Tracer(consts.LibraryName) }),
	fx.Provide(func(shard historyi.ShardContext, factories []QueueFactory, cache wcache.Cache) map[tasks.Category]queues.Queue {
		queueProcessors := make(map[tasks.Category]queues.Queue)
		for _, factory := range factories {
			processor := factory.CreateQueue(shard, cache)
			queueProcessors[processor.Category()] = processor
		}
		return queueProcessors
	}),
	// TODO: doesn't work since they're already decorated. do we really need this?
	// fx.Decorate(func(logger log.Logger) log.Logger {
	// 	return log.With(logger, tag.ComponentHistoryEngine)
	// }),
	// fx.Decorate(func(logger log.ThrottledLogger) log.ThrottledLogger {
	// 	return log.With(logger, tag.ComponentHistoryEngine)
	// }),
	fx.Provide(func(
		cm cluster.Metadata,
		params HistoryEngineFactoryParams,
		deps engineImplDeps,
	) *historyEngineImpl {
		return &historyEngineImpl{
			status:             common.DaemonStatusInitialized,
			currentClusterName: cm.GetCurrentClusterName(),
			deps: struct {
				HistoryEngineFactoryParams
				engineImplDeps
			}{
				HistoryEngineFactoryParams: params,
				engineImplDeps:             deps,
			},
		}
	}),
	// TODO: this is unfortunate but it's an easy way to create a cycle
	fx.Provide(fx.Annotate(func() *engineProxy { return new(engineProxy) }, fx.As(new(historyi.Engine)), fx.As(fx.Self()))),
	fx.Provide(fx.Annotate(replication.NewTaskProcessorManager, fx.As(new(replication.TaskProcessor)))),
	fx.Invoke(func(e *historyEngineImpl, p *engineProxy) { p.Engine = e }),
)

var globalEngineFx = fx.Options(
	fx.Provide(fx.Annotate(replication.NewAckManager, fx.As(new(replication.AckManager)))),
	fx.Provide(fx.Annotate(ndc.NewHistoryReplicator, fx.As(new(ndc.HistoryReplicator)))),
	fx.Provide(fx.Annotate(ndc.NewHistoryImporter, fx.As(new(ndc.HistoryImporter)))),
	fx.Provide(fx.Annotate(ndc.NewActivityStateReplicator, fx.As(new(ndc.ActivityStateReplicator)))),
	fx.Provide(fx.Annotate(ndc.NewWorkflowStateReplicator, fx.As(new(ndc.WorkflowStateReplicator)))),
	fx.Provide(fx.Annotate(ndc.NewHSMStateReplicator, fx.As(new(ndc.HSMStateReplicator)))),
)

func newSearchAttributeValidator(
	p searchattribute.Provider,
	mp searchattribute.MapperProvider,
	vm manager.VisibilityManager,
	config *configs.Config,
) *searchattribute.Validator {
	return searchattribute.NewValidator(
		p,
		mp,
		config.SearchAttributesNumberOfKeysLimit,
		config.SearchAttributesSizeOfValueLimit,
		config.SearchAttributesTotalSizeLimit,
		vm,
		visibility.AllowListForValidation(
			vm.GetStoreNames(),
			config.VisibilityAllowList,
		),
		config.SuppressErrorSetSystemSearchAttribute,
	)
}

// Start will spin up all the components needed to start serving this shard.
// Make sure all the components are loaded lazily so start can return immediately.  This is important because
// ShardController calls start sequentially for all the shards for a given host during startup.
func (e *historyEngineImpl) Start() {
	if !atomic.CompareAndSwapInt32(
		&e.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	e.deps.Logger.Info("", tag.LifeCycleStarting)
	defer e.deps.Logger.Info("", tag.LifeCycleStarted)

	e.registerNamespaceStateChangeCallback()

	for _, queueProcessor := range e.deps.QueueProcessors {
		queueProcessor.Start()
	}
	e.deps.ReplicationProcessorMgr.Start()
}

// Stop the service.
func (e *historyEngineImpl) Stop() {
	if !atomic.CompareAndSwapInt32(
		&e.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	e.deps.Logger.Info("", tag.LifeCycleStopping)
	defer e.deps.Logger.Info("", tag.LifeCycleStopped)

	for _, queueProcessor := range e.deps.QueueProcessors {
		queueProcessor.Stop()
	}
	e.deps.ReplicationProcessorMgr.Stop()
	// unset the failover callback
	e.deps.NamespaceRegistry.UnregisterStateChangeCallback(e)
}

func (e *historyEngineImpl) registerNamespaceStateChangeCallback() {

	e.deps.NamespaceRegistry.RegisterStateChangeCallback(e, func(ns *namespace.Namespace, deletedFromDb bool) {
		if e.deps.ClusterMetadata.IsGlobalNamespaceEnabled() {
			e.deps.ShardContext.UpdateHandoverNamespace(ns, deletedFromDb)
		}

		if deletedFromDb {
			return
		}

		if ns.IsGlobalNamespace() &&
			ns.ReplicationPolicy() == namespace.ReplicationPolicyMultiCluster &&
			ns.ActiveClusterName() == e.currentClusterName {

			for _, queueProcessor := range e.deps.QueueProcessors {
				queueProcessor.FailoverNamespace(ns.ID().String())
			}
		}
	})
}

// StartWorkflowExecution starts a workflow execution
// Consistency guarantee: always write
func (e *historyEngineImpl) StartWorkflowExecution(
	ctx context.Context,
	startRequest *historyservice.StartWorkflowExecutionRequest,
) (*historyservice.StartWorkflowExecutionResponse, error) {
	starter, err := startworkflow.NewStarter(
		e.deps.ShardContext,
		e.deps.WorkflowConsistencyChecker,
		e.deps.TokenSerializer,
		e.deps.PersistenceVisibilityMgr,
		startRequest,
		api.NewWorkflowLeaseAndContext,
	)
	if err != nil {
		return nil, err
	}

	resp, _, err := starter.Invoke(ctx)
	return resp, err
}

func (e *historyEngineImpl) ExecuteMultiOperation(
	ctx context.Context,
	request *historyservice.ExecuteMultiOperationRequest,
) (*historyservice.ExecuteMultiOperationResponse, error) {
	return multioperation.Invoke(
		ctx,
		request,
		e.deps.ShardContext,
		e.deps.WorkflowConsistencyChecker,
		e.deps.TokenSerializer,
		e.deps.PersistenceVisibilityMgr,
		e.deps.MatchingClient,
		e.deps.TestHooks,
	)
}

// GetMutableState retrieves the mutable state of the workflow execution
func (e *historyEngineImpl) GetMutableState(
	ctx context.Context,
	request *historyservice.GetMutableStateRequest,
) (*historyservice.GetMutableStateResponse, error) {
	return e.deps.GetOrPollMutableState.Invoke(ctx, request)
}

// PollMutableState retrieves the mutable state of the workflow execution with long polling
func (e *historyEngineImpl) PollMutableState(
	ctx context.Context,
	request *historyservice.PollMutableStateRequest,
) (*historyservice.PollMutableStateResponse, error) {

	response, err := e.deps.GetOrPollMutableState.Invoke(
		ctx,
		&historyservice.GetMutableStateRequest{
			NamespaceId:         request.GetNamespaceId(),
			Execution:           request.Execution,
			ExpectedNextEventId: request.ExpectedNextEventId,
			CurrentBranchToken:  request.CurrentBranchToken,
			VersionHistoryItem:  request.GetVersionHistoryItem(),
		},
	)
	if err != nil {
		return nil, err
	}

	return &historyservice.PollMutableStateResponse{
		Execution:                             response.Execution,
		WorkflowType:                          response.WorkflowType,
		NextEventId:                           response.NextEventId,
		PreviousStartedEventId:                response.PreviousStartedEventId,
		LastFirstEventId:                      response.LastFirstEventId,
		LastFirstEventTxnId:                   response.LastFirstEventTxnId,
		TaskQueue:                             response.TaskQueue,
		StickyTaskQueue:                       response.StickyTaskQueue,
		StickyTaskQueueScheduleToStartTimeout: response.StickyTaskQueueScheduleToStartTimeout,
		CurrentBranchToken:                    response.CurrentBranchToken,
		VersionHistories:                      response.VersionHistories,
		WorkflowState:                         response.WorkflowState,
		WorkflowStatus:                        response.WorkflowStatus,
		FirstExecutionRunId:                   response.FirstExecutionRunId,
	}, nil
}

func (e *historyEngineImpl) QueryWorkflow(
	ctx context.Context,
	request *historyservice.QueryWorkflowRequest,
) (*historyservice.QueryWorkflowResponse, error) {
	return e.deps.QueryWorkflow.Invoke(ctx, request)
}

func (e *historyEngineImpl) DescribeMutableState(
	ctx context.Context,
	request *historyservice.DescribeMutableStateRequest,
) (response *historyservice.DescribeMutableStateResponse, retError error) {
	return e.deps.DescribeMutableState.Invoke(ctx, request)
}

// ResetStickyTaskQueue reset the volatile information in mutable state of a given workflow.
// Volatile information are the information related to client, such as:
// 1. StickyTaskQueue
// 2. StickyScheduleToStartTimeout
func (e *historyEngineImpl) ResetStickyTaskQueue(
	ctx context.Context,
	resetRequest *historyservice.ResetStickyTaskQueueRequest,
) (*historyservice.ResetStickyTaskQueueResponse, error) {
	return resetstickytaskqueue.Invoke(ctx, resetRequest, e.deps.ShardContext, e.deps.WorkflowConsistencyChecker)
}

// DescribeWorkflowExecution returns information about the specified workflow execution.
func (e *historyEngineImpl) DescribeWorkflowExecution(
	ctx context.Context,
	request *historyservice.DescribeWorkflowExecutionRequest,
) (_ *historyservice.DescribeWorkflowExecutionResponse, retError error) {
	return e.deps.DescribeWorkflow.Invoke(ctx, request)
}

func (e *historyEngineImpl) RecordActivityTaskStarted(
	ctx context.Context,
	request *historyservice.RecordActivityTaskStartedRequest,
) (*historyservice.RecordActivityTaskStartedResponse, error) {
	return recordactivitytaskstarted.Invoke(ctx, request, e.deps.ShardContext, e.deps.WorkflowConsistencyChecker, e.deps.MatchingClient)
}

// ScheduleWorkflowTask schedules a workflow task if no outstanding workflow task found
func (e *historyEngineImpl) ScheduleWorkflowTask(
	ctx context.Context,
	req *historyservice.ScheduleWorkflowTaskRequest,
) error {
	return scheduleworkflowtask.Invoke(ctx, req, e.deps.ShardContext, e.deps.WorkflowConsistencyChecker)
}

func (e *historyEngineImpl) VerifyFirstWorkflowTaskScheduled(
	ctx context.Context,
	request *historyservice.VerifyFirstWorkflowTaskScheduledRequest,
) (retError error) {
	return verifyfirstworkflowtaskscheduled.Invoke(ctx, request, e.deps.WorkflowConsistencyChecker)
}

// RecordWorkflowTaskStarted starts a workflow task
func (e *historyEngineImpl) RecordWorkflowTaskStarted(
	ctx context.Context,
	request *historyservice.RecordWorkflowTaskStartedRequest,
) (*historyservice.RecordWorkflowTaskStartedResponseWithRawHistory, error) {
	return e.deps.RecordWorkflowTaskStarted.Invoke(
		ctx,
		request,
	)
}

// RespondWorkflowTaskCompleted completes a workflow task
func (e *historyEngineImpl) RespondWorkflowTaskCompleted(
	ctx context.Context,
	req *historyservice.RespondWorkflowTaskCompletedRequest,
) (*historyservice.RespondWorkflowTaskCompletedResponse, error) {
	return e.deps.RespondWorkflowTaskCompleted.Invoke(ctx, req)
}

// RespondWorkflowTaskFailed fails a workflow task
func (e *historyEngineImpl) RespondWorkflowTaskFailed(
	ctx context.Context,
	req *historyservice.RespondWorkflowTaskFailedRequest,
) error {
	return respondworkflowtaskfailed.Invoke(ctx, req, e.deps.ShardContext, e.deps.TokenSerializer, e.deps.WorkflowConsistencyChecker)
}

// RespondActivityTaskCompleted completes an activity task.
func (e *historyEngineImpl) RespondActivityTaskCompleted(
	ctx context.Context,
	req *historyservice.RespondActivityTaskCompletedRequest,
) (*historyservice.RespondActivityTaskCompletedResponse, error) {
	return respondactivitytaskcompleted.Invoke(ctx, req, e.deps.ShardContext, e.deps.WorkflowConsistencyChecker)
}

// RespondActivityTaskFailed completes an activity task failure.
func (e *historyEngineImpl) RespondActivityTaskFailed(
	ctx context.Context,
	req *historyservice.RespondActivityTaskFailedRequest,
) (*historyservice.RespondActivityTaskFailedResponse, error) {
	return respondactivitytaskfailed.Invoke(ctx, req, e.deps.ShardContext, e.deps.WorkflowConsistencyChecker)
}

// RespondActivityTaskCanceled completes an activity task failure.
func (e *historyEngineImpl) RespondActivityTaskCanceled(
	ctx context.Context,
	req *historyservice.RespondActivityTaskCanceledRequest,
) (*historyservice.RespondActivityTaskCanceledResponse, error) {
	return respondactivitytaskcanceled.Invoke(ctx, req, e.deps.ShardContext, e.deps.WorkflowConsistencyChecker)
}

// RecordActivityTaskHeartbeat records an hearbeat for a task.
// This method can be used for two purposes.
// - For reporting liveness of the activity.
// - For reporting progress of the activity, this can be done even if the liveness is not configured.
func (e *historyEngineImpl) RecordActivityTaskHeartbeat(
	ctx context.Context,
	req *historyservice.RecordActivityTaskHeartbeatRequest,
) (*historyservice.RecordActivityTaskHeartbeatResponse, error) {
	return recordactivitytaskheartbeat.Invoke(ctx, req, e.deps.ShardContext, e.deps.WorkflowConsistencyChecker)
}

// RequestCancelWorkflowExecution records request cancellation event for workflow execution
func (e *historyEngineImpl) RequestCancelWorkflowExecution(
	ctx context.Context,
	req *historyservice.RequestCancelWorkflowExecutionRequest,
) (resp *historyservice.RequestCancelWorkflowExecutionResponse, retError error) {
	return requestcancelworkflow.Invoke(ctx, req, e.deps.ShardContext, e.deps.WorkflowConsistencyChecker)
}

func (e *historyEngineImpl) SignalWorkflowExecution(
	ctx context.Context,
	req *historyservice.SignalWorkflowExecutionRequest,
) (resp *historyservice.SignalWorkflowExecutionResponse, retError error) {
	return signalworkflow.Invoke(ctx, req, e.deps.ShardContext, e.deps.WorkflowConsistencyChecker)
}

// SignalWithStartWorkflowExecution signals current workflow (if running) or creates & signals a new workflow
// Consistency guarantee: always write
func (e *historyEngineImpl) SignalWithStartWorkflowExecution(
	ctx context.Context,
	req *historyservice.SignalWithStartWorkflowExecutionRequest,
) (_ *historyservice.SignalWithStartWorkflowExecutionResponse, retError error) {
	return signalwithstartworkflow.Invoke(ctx, req, e.deps.ShardContext, e.deps.WorkflowConsistencyChecker)
}

func (e *historyEngineImpl) UpdateWorkflowExecution(
	ctx context.Context,
	req *historyservice.UpdateWorkflowExecutionRequest,
) (*historyservice.UpdateWorkflowExecutionResponse, error) {
	updater := updateworkflow.NewUpdater(
		e.deps.ShardContext,
		e.deps.WorkflowConsistencyChecker,
		e.deps.MatchingClient,
		req,
	)
	return updater.Invoke(ctx)
}

func (e *historyEngineImpl) PollWorkflowExecutionUpdate(
	ctx context.Context,
	req *historyservice.PollWorkflowExecutionUpdateRequest,
) (*historyservice.PollWorkflowExecutionUpdateResponse, error) {
	return pollupdate.Invoke(ctx, req, e.deps.ShardContext, e.deps.WorkflowConsistencyChecker)
}

// RemoveSignalMutableState remove the signal request id in signal_requested for deduplicate
func (e *historyEngineImpl) RemoveSignalMutableState(
	ctx context.Context,
	req *historyservice.RemoveSignalMutableStateRequest,
) (*historyservice.RemoveSignalMutableStateResponse, error) {
	return removesignalmutablestate.Invoke(ctx, req, e.deps.ShardContext, e.deps.WorkflowConsistencyChecker)
}

func (e *historyEngineImpl) TerminateWorkflowExecution(
	ctx context.Context,
	req *historyservice.TerminateWorkflowExecutionRequest,
) (*historyservice.TerminateWorkflowExecutionResponse, error) {
	return terminateworkflow.Invoke(ctx, req, e.deps.ShardContext, e.deps.WorkflowConsistencyChecker)
}

func (e *historyEngineImpl) DeleteWorkflowExecution(
	ctx context.Context,
	request *historyservice.DeleteWorkflowExecutionRequest,
) (*historyservice.DeleteWorkflowExecutionResponse, error) {
	return deleteworkflow.Invoke(ctx, request, e.deps.ShardContext, e.deps.WorkflowConsistencyChecker, e.deps.WorkflowDeleteManager)
}

// RecordChildExecutionCompleted records the completion of child execution into parent execution history
func (e *historyEngineImpl) RecordChildExecutionCompleted(
	ctx context.Context,
	req *historyservice.RecordChildExecutionCompletedRequest,
) (*historyservice.RecordChildExecutionCompletedResponse, error) {
	return recordchildworkflowcompleted.Invoke(ctx, req, e.deps.ShardContext, e.deps.WorkflowConsistencyChecker)
}

// IsActivityTaskValid - whether activity task is still valid
func (e *historyEngineImpl) IsActivityTaskValid(
	ctx context.Context,
	req *historyservice.IsActivityTaskValidRequest,
) (*historyservice.IsActivityTaskValidResponse, error) {
	return isactivitytaskvalid.Invoke(ctx, req, e.deps.ShardContext, e.deps.WorkflowConsistencyChecker)
}

// IsWorkflowTaskValid - whether workflow task is still valid
func (e *historyEngineImpl) IsWorkflowTaskValid(
	ctx context.Context,
	req *historyservice.IsWorkflowTaskValidRequest,
) (*historyservice.IsWorkflowTaskValidResponse, error) {
	return isworkflowtaskvalid.Invoke(ctx, req, e.deps.ShardContext, e.deps.WorkflowConsistencyChecker)
}

func (e *historyEngineImpl) VerifyChildExecutionCompletionRecorded(
	ctx context.Context,
	req *historyservice.VerifyChildExecutionCompletionRecordedRequest,
) (*historyservice.VerifyChildExecutionCompletionRecordedResponse, error) {
	return verifychildworkflowcompletionrecorded.Invoke(ctx, req, e.deps.WorkflowConsistencyChecker, e.deps.ShardContext)
}

func (e *historyEngineImpl) ReplicateEventsV2(
	ctx context.Context,
	replicateRequest *historyservice.ReplicateEventsV2Request,
) error {
	return e.deps.NDCHistoryReplicator.ApplyEvents(ctx, replicateRequest)
}

func (e *historyEngineImpl) ReplicateHistoryEvents(
	ctx context.Context,
	workflowKey definition.WorkflowKey,
	baseExecutionInfo *workflowspb.BaseExecutionInfo,
	versionHistoryItems []*historyspb.VersionHistoryItem,
	historyEvents [][]*historypb.HistoryEvent,
	newEvents []*historypb.HistoryEvent,
	newRunID string,
) error {
	return e.deps.NDCHistoryReplicator.ReplicateHistoryEvents(
		ctx,
		workflowKey,
		baseExecutionInfo,
		versionHistoryItems,
		historyEvents,
		newEvents,
		newRunID,
	)
}

func (e *historyEngineImpl) SyncActivity(
	ctx context.Context,
	request *historyservice.SyncActivityRequest,
) error {
	return e.deps.NDCActivityStateReplicator.SyncActivityState(ctx, request)
}

func (e *historyEngineImpl) SyncActivities(
	ctx context.Context,
	request *historyservice.SyncActivitiesRequest,
) error {
	return e.deps.NDCActivityStateReplicator.SyncActivitiesState(ctx, request)
}

func (e *historyEngineImpl) SyncHSM(
	ctx context.Context,
	request *historyi.SyncHSMRequest,
) error {
	return e.deps.NDCHSMStateReplicator.SyncHSMState(ctx, request)
}

func (e *historyEngineImpl) BackfillHistoryEvents(
	ctx context.Context,
	request *historyi.BackfillHistoryEventsRequest,
) error {
	return e.deps.NDCHistoryReplicator.BackfillHistoryEvents(ctx, request)
}

// ReplicateWorkflowState is an experimental method to replicate workflow state. This should not expose outside of history service role.
func (e *historyEngineImpl) ReplicateWorkflowState(
	ctx context.Context,
	request *historyservice.ReplicateWorkflowStateRequest,
) error {
	return e.deps.NDCWorkflowStateReplicator.SyncWorkflowState(ctx, request)
}

func (e *historyEngineImpl) ReplicateVersionedTransition(ctx context.Context, artifact *replicationspb.VersionedTransitionArtifact, sourceClusterName string) error {
	return e.deps.NDCWorkflowStateReplicator.ReplicateVersionedTransition(ctx, artifact, sourceClusterName)
}

func (e *historyEngineImpl) ImportWorkflowExecution(
	ctx context.Context,
	request *historyservice.ImportWorkflowExecutionRequest,
) (*historyservice.ImportWorkflowExecutionResponse, error) {
	historyEvents, err := ndc.DeserializeBlobs(e.deps.EventSerializer, request.HistoryBatches)
	if err != nil {
		return nil, err
	}
	token, eventsApplied, err := e.deps.NDCHistoryImporter.ImportWorkflow(
		ctx,
		definition.NewWorkflowKey(
			request.NamespaceId,
			request.Execution.GetWorkflowId(),
			request.Execution.GetRunId(),
		),
		request.VersionHistory.Items,
		historyEvents,
		request.Token,
	)
	if err != nil {
		return nil, err
	}
	return &historyservice.ImportWorkflowExecutionResponse{
		Token:         token,
		EventsApplied: eventsApplied,
	}, nil
}

func (e *historyEngineImpl) SyncShardStatus(
	ctx context.Context,
	request *historyservice.SyncShardStatusRequest,
) error {

	clusterName := request.GetSourceCluster()
	now := timestamp.TimeValue(request.GetStatusTime())

	// here there are 3 main things
	// 1. update the view of remote cluster's shard time
	// 2. notify the timer gate in the timer queue standby processor
	// 3, notify the transfer (essentially a no op, just put it here so it looks symmetric)
	e.deps.ShardContext.SetCurrentTime(clusterName, now)
	for _, processor := range e.deps.QueueProcessors {
		processor.NotifyNewTasks([]tasks.Task{})
	}
	return nil
}

// ResetWorkflowExecution terminates current workflow (if running) and replay & create new workflow
// Consistency guarantee: always write
func (e *historyEngineImpl) ResetWorkflowExecution(
	ctx context.Context,
	req *historyservice.ResetWorkflowExecutionRequest,
) (*historyservice.ResetWorkflowExecutionResponse, error) {
	return resetworkflow.Invoke(ctx, req, e.deps.ShardContext, e.deps.WorkflowConsistencyChecker)
}

// UpdateWorkflowExecutionOptions updates the options of a specific workflow execution.
// Can be used to set and unset versioning behavior override.
func (e *historyEngineImpl) UpdateWorkflowExecutionOptions(
	ctx context.Context,
	req *historyservice.UpdateWorkflowExecutionOptionsRequest,
) (*historyservice.UpdateWorkflowExecutionOptionsResponse, error) {
	return updateworkflowoptions.Invoke(ctx, req, e.deps.ShardContext, e.deps.WorkflowConsistencyChecker)
}

func (e *historyEngineImpl) NotifyNewHistoryEvent(
	notification *events.Notification,
) {

	e.deps.EventNotifier.NotifyNewHistoryEvent(notification)
}

func (e *historyEngineImpl) NotifyNewTasks(
	newTasks map[tasks.Category][]tasks.Task,
) {
	for category, tasksByCategory := range newTasks {
		// TODO: make replicatorProcessor part of queueProcessors list
		// and get rid of the special case here.
		if category == tasks.CategoryReplication {
			if e.deps.ReplicationAckMgr != nil {
				e.deps.ReplicationAckMgr.NotifyNewTasks(tasksByCategory)
			}
			continue
		}

		if len(tasksByCategory) > 0 {
			proc, ok := e.deps.QueueProcessors[category]
			if !ok {
				// On shard reload it sends fake tasks to wake up the queue processors. Only log if there are "real"
				// tasks that can't be processed.
				if _, ok := tasksByCategory[0].(*tasks.FakeTask); !ok {
					e.deps.Logger.Error("Skipping notification for new tasks, processor not registered", tag.TaskCategoryID(category.ID()))
				}
				continue
			}
			proc.NotifyNewTasks(tasksByCategory)
		}
	}
}

func (e *historyEngineImpl) GetReplicationMessages(
	ctx context.Context,
	pollingCluster string,
	ackMessageID int64,
	ackTimestamp time.Time,
	queryMessageID int64,
) (*replicationspb.ReplicationMessages, error) {
	return replicationapi.GetTasks(ctx, e.deps.ShardContext, e.deps.ReplicationAckMgr, pollingCluster, ackMessageID, ackTimestamp, queryMessageID)
}

func (e *historyEngineImpl) SubscribeReplicationNotification(
	clusterName string,
) (notifyCh <-chan struct{}, subscribeId string) {
	return e.deps.ReplicationAckMgr.SubscribeNotification(clusterName)
}

func (e *historyEngineImpl) UnsubscribeReplicationNotification(subscriberID string) {
	e.deps.ReplicationAckMgr.UnsubscribeNotification(subscriberID)
}

func (e *historyEngineImpl) ConvertReplicationTask(
	ctx context.Context,
	task tasks.Task,
	clusterID int32,
) (*replicationspb.ReplicationTask, error) {
	return e.deps.ReplicationAckMgr.ConvertTaskByCluster(ctx, task, clusterID)
}

func (e *historyEngineImpl) GetReplicationTasksIter(
	ctx context.Context,
	pollingCluster string,
	minInclusiveTaskID int64,
	maxExclusiveTaskID int64,
) (collection.Iterator[tasks.Task], error) {
	return e.deps.ReplicationAckMgr.GetReplicationTasksIter(ctx, pollingCluster, minInclusiveTaskID, maxExclusiveTaskID)
}

func (e *historyEngineImpl) GetMaxReplicationTaskInfo() (int64, time.Time) {
	return e.deps.ReplicationAckMgr.GetMaxTaskInfo()
}

func (e *historyEngineImpl) GetDLQReplicationMessages(
	ctx context.Context,
	taskInfos []*replicationspb.ReplicationTaskInfo,
) ([]*replicationspb.ReplicationTask, error) {
	return replicationapi.GetDLQTasks(ctx, e.deps.ShardContext, e.deps.ReplicationAckMgr, taskInfos)
}

func (e *historyEngineImpl) ReapplyEvents(
	ctx context.Context,
	namespaceUUID namespace.ID,
	workflowID string,
	runID string,
	reapplyEvents []*historypb.HistoryEvent,
) error {
	return reapplyevents.Invoke(ctx, namespaceUUID, workflowID, runID, reapplyEvents, e.deps.ShardContext, e.deps.WorkflowConsistencyChecker, e.deps.WorkflowResetter, e.deps.EventsReapplier)
}

func (e *historyEngineImpl) GetDLQMessages(
	ctx context.Context,
	request *historyservice.GetDLQMessagesRequest,
) (*historyservice.GetDLQMessagesResponse, error) {
	return replicationadmin.GetDLQ(ctx, request, e.deps.ShardContext, e.deps.ReplicationDLQHandler)
}

func (e *historyEngineImpl) PurgeDLQMessages(
	ctx context.Context,
	request *historyservice.PurgeDLQMessagesRequest,
) (*historyservice.PurgeDLQMessagesResponse, error) {
	return replicationadmin.PurgeDLQ(ctx, request, e.deps.ShardContext, e.deps.ReplicationDLQHandler)
}

func (e *historyEngineImpl) MergeDLQMessages(
	ctx context.Context,
	request *historyservice.MergeDLQMessagesRequest,
) (*historyservice.MergeDLQMessagesResponse, error) {
	return replicationadmin.MergeDLQ(ctx, request, e.deps.ShardContext, e.deps.ReplicationDLQHandler)
}

func (e *historyEngineImpl) RebuildMutableState(
	ctx context.Context,
	namespaceUUID namespace.ID,
	execution *commonpb.WorkflowExecution,
) error {
	return e.deps.WorkflowRebuilder.rebuild(
		ctx,
		definition.NewWorkflowKey(
			namespaceUUID.String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
	)
}

func (e *historyEngineImpl) RefreshWorkflowTasks(
	ctx context.Context,
	namespaceUUID namespace.ID,
	execution *commonpb.WorkflowExecution,
) (retError error) {
	return refreshworkflow.Invoke(
		ctx,
		definition.NewWorkflowKey(namespaceUUID.String(), execution.WorkflowId, execution.RunId),
		e.deps.ShardContext,
		e.deps.WorkflowConsistencyChecker,
	)
}

func (e *historyEngineImpl) GenerateLastHistoryReplicationTasks(
	ctx context.Context,
	request *historyservice.GenerateLastHistoryReplicationTasksRequest,
) (_ *historyservice.GenerateLastHistoryReplicationTasksResponse, retError error) {
	return replicationapi.GenerateTask(ctx, request, e.deps.ShardContext, e.deps.WorkflowConsistencyChecker)
}

func (e *historyEngineImpl) GetReplicationStatus(
	ctx context.Context,
	request *historyservice.GetReplicationStatusRequest,
) (_ *historyservice.ShardReplicationStatus, retError error) {
	return replicationapi.GetStatus(ctx, request, e.deps.ShardContext, e.deps.ReplicationAckMgr)
}

func (e *historyEngineImpl) GetWorkflowExecutionHistory(
	ctx context.Context,
	request *historyservice.GetWorkflowExecutionHistoryRequest,
) (_ *historyservice.GetWorkflowExecutionHistoryResponseWithRaw, retError error) {
	return e.deps.GetWorkflowExecutionHistory.Invoke(ctx, request)
}

func (e *historyEngineImpl) GetWorkflowExecutionHistoryReverse(
	ctx context.Context,
	request *historyservice.GetWorkflowExecutionHistoryReverseRequest,
) (_ *historyservice.GetWorkflowExecutionHistoryReverseResponse, retError error) {
	return e.deps.GetWorkflowExecutionHistoryReverse.Invoke(ctx, request)
}

func (e *historyEngineImpl) GetWorkflowExecutionRawHistory(ctx context.Context, request *historyservice.GetWorkflowExecutionRawHistoryRequest) (*historyservice.GetWorkflowExecutionRawHistoryResponse, error) {
	return e.deps.GetWorkflowExecutionRawHistory.Invoke(ctx, request)
}

func (e *historyEngineImpl) GetWorkflowExecutionRawHistoryV2(
	ctx context.Context,
	request *historyservice.GetWorkflowExecutionRawHistoryV2Request,
) (_ *historyservice.GetWorkflowExecutionRawHistoryV2Response, retError error) {
	return e.deps.GetWorkflowExecutionRawHistoryV2.Invoke(ctx, request)
}

func (e *historyEngineImpl) AddTasks(
	ctx context.Context,
	request *historyservice.AddTasksRequest,
) (_ *historyservice.AddTasksResponse, retError error) {
	return addtasks.Invoke(
		ctx,
		e.deps.ShardContext,
		e.deps.EventSerializer,
		int(e.deps.Config.NumberOfShards),
		request,
		e.deps.TaskCategoryRegistry,
	)
}

func (e *historyEngineImpl) ListTasks(
	ctx context.Context,
	request *historyservice.ListTasksRequest,
) (_ *historyservice.ListTasksResponse, retError error) {
	return listtasks.Invoke(
		ctx,
		e.deps.TaskCategoryRegistry,
		e.deps.ExecutionManager,
		request,
	)
}

// StateMachineEnvironment implements shard.Engine.
func (e *historyEngineImpl) StateMachineEnvironment(
	operationTag metrics.Tag,
) hsm.Environment {
	return &stateMachineEnvironment{
		shardContext:   e.deps.ShardContext,
		cache:          e.deps.WorkflowCache,
		metricsHandler: e.deps.MetricsHandler.WithTags(operationTag),
		logger:         e.deps.Logger,
	}
}

func (e *historyEngineImpl) SyncWorkflowState(ctx context.Context, request *historyservice.SyncWorkflowStateRequest) (_ *historyservice.SyncWorkflowStateResponse, retErr error) {
	return replicationapi.SyncWorkflowState(ctx, request, e.deps.ReplicationProgressCache, e.deps.SyncStateRetriever, e.deps.Logger)
}

func (e *historyEngineImpl) UpdateActivityOptions(
	ctx context.Context,
	request *historyservice.UpdateActivityOptionsRequest,
) (*historyservice.UpdateActivityOptionsResponse, error) {
	return updateactivityoptions.Invoke(ctx, request, e.deps.ShardContext, e.deps.WorkflowConsistencyChecker)
}

func (e *historyEngineImpl) PauseActivity(
	ctx context.Context,
	request *historyservice.PauseActivityRequest,
) (*historyservice.PauseActivityResponse, error) {
	return pauseactivity.Invoke(ctx, request, e.deps.ShardContext, e.deps.WorkflowConsistencyChecker)
}

func (e *historyEngineImpl) UnpauseActivity(
	ctx context.Context,
	request *historyservice.UnpauseActivityRequest,
) (*historyservice.UnpauseActivityResponse, error) {
	return unpauseactivity.Invoke(ctx, request, e.deps.ShardContext, e.deps.WorkflowConsistencyChecker)
}

func (e *historyEngineImpl) ResetActivity(
	ctx context.Context,
	request *historyservice.ResetActivityRequest,
) (*historyservice.ResetActivityResponse, error) {
	return resetactivity.Invoke(ctx, request, e.deps.ShardContext, e.deps.WorkflowConsistencyChecker)
}
