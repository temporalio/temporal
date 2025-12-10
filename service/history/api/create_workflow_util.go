package api

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/retrypolicy"
	"go.temporal.io/server/common/rpc/interceptor"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	// maxWorkflowTaskStartToCloseTimeout sets the Max Workflow Task start to close timeout for a Workflow
	maxWorkflowTaskStartToCloseTimeout = 120 * time.Second
)

type (
	VersionedRunID struct {
		RunID            string
		LastWriteVersion int64
	}
	CreateOrUpdateLeaseFunc func(
		WorkflowLease,
		historyi.ShardContext,
		historyi.MutableState,
	) (WorkflowLease, error)
)

func NewWorkflowWithSignal(
	shard historyi.ShardContext,
	namespaceEntry *namespace.Namespace,
	workflowID string,
	runID string,
	startRequest *historyservice.StartWorkflowExecutionRequest,
	signalWithStartRequest *workflowservice.SignalWithStartWorkflowExecutionRequest,
) (historyi.MutableState, error) {
	newMutableState, err := CreateMutableState(
		shard,
		namespaceEntry,
		startRequest.StartRequest.WorkflowExecutionTimeout,
		startRequest.StartRequest.WorkflowRunTimeout,
		workflowID,
		runID,
	)
	if err != nil {
		return nil, err
	}

	startEvent, err := newMutableState.AddWorkflowExecutionStartedEvent(
		&commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		startRequest,
	)
	if err != nil {
		return nil, err
	}

	if signalWithStartRequest != nil {
		if signalWithStartRequest.GetRequestId() != "" {
			newMutableState.AddSignalRequested(signalWithStartRequest.GetRequestId())
		}
		if _, err := newMutableState.AddWorkflowExecutionSignaled(
			signalWithStartRequest.GetSignalName(),
			signalWithStartRequest.GetSignalInput(),
			signalWithStartRequest.GetIdentity(),
			signalWithStartRequest.GetHeader(),
			signalWithStartRequest.GetLinks(),
		); err != nil {
			return nil, err
		}
	}
	requestEagerExecution := startRequest.StartRequest.GetRequestEagerExecution()

	var scheduledEventID int64
	// Generate first workflow task event if not child WF and no first workflow task backoff
	scheduledEventID, err = GenerateFirstWorkflowTask(
		newMutableState,
		startRequest.ParentExecutionInfo,
		startEvent,
		requestEagerExecution,
	)
	if err != nil {
		return nil, err
	}

	// If first workflow task should back off (e.g. cron or workflow retry) a workflow task will not be scheduled.
	if requestEagerExecution && newMutableState.HasPendingWorkflowTask() {
		// TODO: get build ID from Starter so eager workflows can be versioned
		_, _, err = newMutableState.AddWorkflowTaskStartedEvent(
			scheduledEventID,
			startRequest.StartRequest.RequestId,
			startRequest.StartRequest.TaskQueue,
			startRequest.StartRequest.Identity,
			nil,
			nil,
			nil,
			false,
			nil,
		)
		if err != nil {
			// Unable to add WorkflowTaskStarted event to history
			return nil, err
		}
	}

	return newMutableState, nil
}

// NOTE: must implement CreateOrUpdateLeaseFunc.
func NewWorkflowLeaseAndContext(
	existingLease WorkflowLease,
	shardCtx historyi.ShardContext,
	ms historyi.MutableState,
) (WorkflowLease, error) {
	// TODO(stephanos): remove this hack
	if existingLease != nil {
		return existingLease, nil
	}
	return NewWorkflowLease(
		workflow.NewContext(
			shardCtx.GetConfig(),
			definition.NewWorkflowKey(
				ms.GetNamespaceEntry().ID().String(),
				ms.GetExecutionInfo().WorkflowId,
				ms.GetExecutionState().RunId,
			),
			chasm.WorkflowArchetypeID,
			shardCtx.GetLogger(),
			shardCtx.GetThrottledLogger(),
			shardCtx.GetMetricsHandler(),
		),
		wcache.NoopReleaseFn,
		ms,
	), nil
}

func CreateMutableState(
	shard historyi.ShardContext,
	namespaceEntry *namespace.Namespace,
	executionTimeout *durationpb.Duration,
	runTimeout *durationpb.Duration,
	workflowID string,
	runID string,
) (historyi.MutableState, error) {
	newMutableState := workflow.NewMutableState(
		shard,
		shard.GetEventsCache(),
		shard.GetLogger(),
		namespaceEntry,
		workflowID,
		runID,
		shard.GetTimeSource().Now(),
	)
	if err := newMutableState.SetHistoryTree(executionTimeout, runTimeout, runID); err != nil {
		return nil, err
	}
	return newMutableState, nil
}

func GenerateFirstWorkflowTask(
	mutableState historyi.MutableState,
	parentInfo *workflowspb.ParentExecutionInfo,
	startEvent *historypb.HistoryEvent,
	bypassTaskGeneration bool,
) (int64, error) {
	if parentInfo == nil {
		// WorkflowTask is only created when it is not a Child Workflow and no backoff is needed
		return mutableState.AddFirstWorkflowTaskScheduled(nil, startEvent, bypassTaskGeneration)
	}
	return 0, nil
}

func NewWorkflowVersionCheck(
	shard historyi.ShardContext,
	prevLastWriteVersion int64,
	newMutableState historyi.MutableState,
) error {
	if prevLastWriteVersion == common.EmptyVersion {
		return nil
	}

	if prevLastWriteVersion > newMutableState.GetCurrentVersion() {
		clusterMetadata := shard.GetClusterMetadata()
		namespaceEntry := newMutableState.GetNamespaceEntry()
		clusterName := clusterMetadata.ClusterNameForFailoverVersion(namespaceEntry.IsGlobalNamespace(), prevLastWriteVersion)
		return serviceerror.NewNamespaceNotActive(
			namespaceEntry.Name().String(),
			clusterMetadata.GetCurrentClusterName(),
			clusterName,
		)
	}
	return nil
}

func ValidateStart(
	ctx context.Context,
	shard historyi.ShardContext,
	namespaceEntry *namespace.Namespace,
	workflowID string,
	workflowInputSize int,
	workflowMemoSize int,
	workflowHeaderSize int,
	operation string,
) error {
	config := shard.GetConfig()
	logger := shard.GetLogger()
	throttledLogger := shard.GetThrottledLogger()
	namespaceName := namespaceEntry.Name().String()

	metricsHandler := interceptor.GetMetricsHandlerFromContext(ctx, logger)
	metrics.HeaderSize.With(metricsHandler.WithTags(metrics.HeaderCallsiteTag(operation))).Record(int64(workflowHeaderSize))
	handlerWithCommandTag := metricsHandler.WithTags(metrics.CommandTypeTag(operation))
	if err := common.CheckEventBlobSizeLimit(
		workflowInputSize,
		config.BlobSizeLimitWarn(namespaceName),
		config.BlobSizeLimitError(namespaceName),
		namespaceName,
		workflowID,
		"",
		handlerWithCommandTag,
		throttledLogger,
		tag.BlobSizeViolationOperation(operation),
	); err != nil {
		return err
	}

	metrics.MemoSize.With(handlerWithCommandTag).Record(int64(workflowMemoSize))
	if err := common.CheckEventBlobSizeLimit(
		workflowMemoSize,
		config.MemoSizeLimitWarn(namespaceName),
		config.MemoSizeLimitError(namespaceName),
		namespaceName,
		workflowID,
		"",
		handlerWithCommandTag,
		throttledLogger,
		tag.BlobSizeViolationOperation(operation),
	); err != nil {
		return common.ErrMemoSizeExceedsLimit
	}

	return nil
}

func ValidateStartWorkflowExecutionRequest(
	ctx context.Context,
	request *workflowservice.StartWorkflowExecutionRequest,
	shard historyi.ShardContext,
	namespaceEntry *namespace.Namespace,
	operation string,
) error {

	workflowID := request.GetWorkflowId()
	maxIDLengthLimit := shard.GetConfig().MaxIDLengthLimit()

	if len(request.GetRequestId()) == 0 {
		return serviceerror.NewInvalidArgument("Missing request ID.")
	}
	if err := timestamp.ValidateAndCapProtoDuration(request.GetWorkflowExecutionTimeout()); err != nil {
		return serviceerror.NewInvalidArgumentf("invalid WorkflowExecutionTimeoutSeconds: %s", err.Error())
	}
	if err := timestamp.ValidateAndCapProtoDuration(request.GetWorkflowRunTimeout()); err != nil {
		return serviceerror.NewInvalidArgumentf("invalid WorkflowRunTimeoutSeconds: %s", err.Error())
	}
	if err := timestamp.ValidateAndCapProtoDuration(request.GetWorkflowTaskTimeout()); err != nil {
		return serviceerror.NewInvalidArgumentf("invalid WorkflowTaskTimeoutSeconds: %s", err.Error())
	}
	if request.TaskQueue == nil || request.TaskQueue.GetName() == "" {
		return serviceerror.NewInvalidArgument("Missing Taskqueue.")
	}
	if request.WorkflowType == nil || request.WorkflowType.GetName() == "" {
		return serviceerror.NewInvalidArgument("Missing WorkflowType.")
	}
	if len(request.GetNamespace()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("Namespace exceeds length limit.")
	}
	if len(request.GetWorkflowId()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("WorkflowId exceeds length limit.")
	}
	if len(request.TaskQueue.GetName()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("TaskQueue exceeds length limit.")
	}
	if len(request.WorkflowType.GetName()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("WorkflowType exceeds length limit.")
	}

	if err := retrypolicy.Validate(request.RetryPolicy); err != nil {
		return err
	}
	return ValidateStart(
		ctx,
		shard,
		namespaceEntry,
		workflowID,
		request.GetInput().Size(),
		request.GetMemo().Size(),
		request.GetHeader().Size(),
		operation,
	)
}

func OverrideStartWorkflowExecutionRequest(
	request *workflowservice.StartWorkflowExecutionRequest,
	operation string,
	shard historyi.ShardContext,
	metricsHandler metrics.Handler,
) {
	// workflow execution timeout is left as is
	//  if workflow execution timeout == 0 -> infinity

	ns := namespace.Name(request.GetNamespace())

	workflowRunTimeout := overrideWorkflowRunTimeout(
		timestamp.DurationValue(request.GetWorkflowRunTimeout()),
		timestamp.DurationValue(request.GetWorkflowExecutionTimeout()),
	)
	if workflowRunTimeout != timestamp.DurationValue(request.GetWorkflowRunTimeout()) {
		request.WorkflowRunTimeout = durationpb.New(workflowRunTimeout)
		metrics.WorkflowRunTimeoutOverrideCount.With(metricsHandler).Record(
			1,
			metrics.OperationTag(operation),
			metrics.NamespaceTag(ns.String()),
		)
	}

	workflowTaskStartToCloseTimeout := overrideWorkflowTaskTimeout(
		ns,
		timestamp.DurationValue(request.GetWorkflowTaskTimeout()),
		timestamp.DurationValue(request.GetWorkflowRunTimeout()),
		shard.GetConfig().DefaultWorkflowTaskTimeout,
	)
	if workflowTaskStartToCloseTimeout != timestamp.DurationValue(request.GetWorkflowTaskTimeout()) {
		request.WorkflowTaskTimeout = durationpb.New(workflowTaskStartToCloseTimeout)
		metrics.WorkflowTaskTimeoutOverrideCount.With(metricsHandler).Record(
			1,
			metrics.OperationTag(operation),
			metrics.NamespaceTag(ns.String()),
		)
	}
}

// overrideWorkflowRunTimeout override the run timeout according to execution timeout
func overrideWorkflowRunTimeout(
	workflowRunTimeout time.Duration,
	workflowExecutionTimeout time.Duration,
) time.Duration {

	if workflowExecutionTimeout == 0 {
		return workflowRunTimeout
	} else if workflowRunTimeout == 0 {
		return workflowExecutionTimeout
	}
	return min(workflowRunTimeout, workflowExecutionTimeout)
}

// overrideWorkflowTaskTimeout override the workflow task timeout according to default timeout or max timeout
func overrideWorkflowTaskTimeout(
	ns namespace.Name,
	taskStartToCloseTimeout time.Duration,
	workflowRunTimeout time.Duration,
	getDefaultTimeoutFunc func(namespaceName string) time.Duration,
) time.Duration {

	if taskStartToCloseTimeout == 0 {
		taskStartToCloseTimeout = getDefaultTimeoutFunc(ns.String())
	}

	taskStartToCloseTimeout = min(taskStartToCloseTimeout, maxWorkflowTaskStartToCloseTimeout)

	if workflowRunTimeout == 0 {
		return taskStartToCloseTimeout
	}

	return min(taskStartToCloseTimeout, workflowRunTimeout)
}
