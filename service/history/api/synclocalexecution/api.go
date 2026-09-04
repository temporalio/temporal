package synclocalexecution

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const protocolVersion = 1

type syncCursor struct {
	EventID int64
	Version int64
}

func Invoke(
	ctx context.Context,
	request *historyservice.SyncLocalExecutionRequest,
	shardContext historyi.ShardContext,
	config *configs.Config,
	eventSerializer serialization.Serializer,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (_ *historyservice.SyncLocalExecutionResponse, retError error) {
	if request == nil || request.GetRequest() == nil {
		return nil, serviceerror.NewInvalidArgument("request is required")
	}
	syncRequest := request.GetRequest()
	if syncRequest.GetExecution().GetWorkflowId() == "" || syncRequest.GetExecution().GetRunId() == "" {
		return nil, serviceerror.NewInvalidArgument("workflow ID and run ID are required")
	}
	if len(request.GetSyncRequestHash()) != sha256.Size {
		return nil, serviceerror.NewInvalidArgument("sync request hash must be a SHA-256 digest")
	}
	if err := validateSyncEnvelope(syncRequest); err != nil {
		return nil, err
	}

	namespaceEntry, err := api.GetActiveNamespace(
		shardContext,
		namespace.ID(request.GetNamespaceId()),
		syncRequest.GetExecution().GetWorkflowId(),
	)
	if err != nil {
		return nil, err
	}
	if !config.EnableLocalExecution(namespaceEntry.Name().String()) {
		return nil, serviceerror.NewFailedPrecondition("local execution is not enabled for the namespace")
	}

	workflowLease, err := workflowConsistencyChecker.GetWorkflowLease(
		ctx,
		nil,
		definition.NewWorkflowKey(
			request.GetNamespaceId(),
			syncRequest.GetExecution().GetWorkflowId(),
			syncRequest.GetExecution().GetRunId(),
		),
		locks.PriorityHigh,
	)
	if err != nil {
		return nil, err
	}
	defer func() { workflowLease.GetReleaseFn()(retError) }()

	mutableState := workflowLease.GetMutableState()
	if response, handled, err := repeatedSync(
		mutableState.GetExecutionInfo().GetLocalExecutionInfo(),
		syncRequest,
		request.GetSyncRequestHash(),
		shardContext.GetTimeSource().Now(),
	); handled || err != nil {
		return response, err
	}
	if err := validateState(mutableState, syncRequest, shardContext.GetTimeSource().Now()); err != nil {
		return nil, err
	}
	history, err := decodeHistoryBatches(
		syncRequest,
		eventSerializer,
		config.LocalExecutionSyncMaxBytes(namespaceEntry.Name().String()),
		config.LocalExecutionSyncMaxEvents(namespaceEntry.Name().String()),
		config.LocalExecutionSyncMaxBatches(namespaceEntry.Name().String()),
	)
	if err != nil {
		return nil, err
	}
	leaseExpiration, err := applyAndCommitSync(
		ctx,
		shardContext,
		namespaceEntry.ID(),
		workflowLease,
		syncRequest,
		request.GetSyncRequestHash(),
		history,
	)
	if err != nil {
		return nil, err
	}

	return newResponse(syncRequest, leaseExpiration), nil
}

func applyAndCommitSync(
	ctx context.Context,
	shardContext historyi.ShardContext,
	namespaceID namespace.ID,
	workflowLease api.WorkflowLease,
	request *adminservice.SyncLocalExecutionRequest,
	requestHash []byte,
	history [][]*historypb.HistoryEvent,
) (*timestamppb.Timestamp, error) {
	mutableState := workflowLease.GetMutableState()
	if len(history) != 0 {
		newMutableState, err := workflow.NewMutableStateRebuilder(
			shardContext,
			shardContext.GetLogger(),
			mutableState,
		).ApplyEvents(ctx, namespaceID, request.GetSyncId(), request.GetExecution(), history, nil, "")
		if err != nil {
			return nil, err
		}
		if newMutableState != nil {
			return nil, serviceerror.NewInvalidArgument("protocol version 1 cannot synchronize a successor workflow run")
		}
		if err := forkHistoryForSync(ctx, shardContext, mutableState, request); err != nil {
			return nil, err
		}
	}

	// The rebuilder traverses events as if they were applied normally. Discard
	// intermediate tasks and persist only tasks appropriate at this sync point.
	mutableState.PopTasks()
	leaseExpiration, err := applySyncMetadata(
		mutableState,
		request,
		requestHash,
		shardContext.GetTimeSource().Now(),
	)
	if err != nil {
		return nil, err
	}
	if request.GetRelease() {
		if err := workflow.NewTaskRefresher(shardContext).Refresh(ctx, mutableState, false); err != nil {
			return nil, err
		}
	}

	workflowContext := workflowLease.GetContext()
	if request.GetRelease() {
		err = workflowContext.UpdateWorkflowExecutionAsActive(ctx, shardContext)
	} else {
		err = workflowContext.UpdateWorkflowExecutionAsActiveWithTaskFilter(ctx, shardContext, retainSyncTask)
	}
	return leaseExpiration, err
}

func retainSyncTask(task tasks.Task) bool {
	category := task.GetCategory()
	return category == tasks.CategoryReplication || category == tasks.CategoryVisibility
}

func forkHistoryForSync(
	ctx context.Context,
	shardContext historyi.ShardContext,
	mutableState historyi.MutableState,
	request *adminservice.SyncLocalExecutionRequest,
) error {
	branchToken, err := mutableState.GetCurrentBranchToken()
	if err != nil {
		return err
	}
	response, err := shardContext.GetExecutionManager().ForkHistoryBranch(
		ctx,
		&persistence.ForkHistoryBranchRequest{
			ForkBranchToken: branchToken,
			ForkNodeID:      request.GetPreviousEventId() + 1,
			Info: persistence.BuildHistoryGarbageCleanupInfo(
				mutableState.GetExecutionInfo().GetNamespaceId(),
				request.GetExecution().GetWorkflowId(),
				request.GetExecution().GetRunId(),
			),
			ShardID:     shardContext.GetShardID(),
			NamespaceID: mutableState.GetExecutionInfo().GetNamespaceId(),
			NewRunID:    request.GetExecution().GetRunId(),
		},
	)
	if err != nil {
		return err
	}
	return mutableState.SetCurrentBranchToken(response.NewBranchToken)
}

func decodeAndValidateHistory(
	request *adminservice.SyncLocalExecutionRequest,
	eventSerializer serialization.Serializer,
	maxBytes int,
	maxEvents int,
	maxBatches int,
) ([][]*historypb.HistoryEvent, error) {
	if err := validateSyncEnvelope(request); err != nil {
		return nil, err
	}
	return decodeHistoryBatches(request, eventSerializer, maxBytes, maxEvents, maxBatches)
}

func validateSyncEnvelope(request *adminservice.SyncLocalExecutionRequest) error {
	if request.GetProtocolVersion() != protocolVersion {
		return serviceerror.NewInvalidArgumentf(
			"unsupported local execution protocol version: %d",
			request.GetProtocolVersion(),
		)
	}
	if request.GetLocalServerId() == "" || request.GetSyncId() == "" {
		return serviceerror.NewInvalidArgument("local server ID and sync ID are required")
	}
	if request.GetPreviousEventId() < common.FirstEventID ||
		request.GetNewEventId() < request.GetPreviousEventId() {
		return serviceerror.NewInvalidArgument("invalid local execution synchronization cursor")
	}
	if request.GetVersionHistory() == nil {
		return serviceerror.NewInvalidArgument("version history is required")
	}

	previous := &historyspb.VersionHistoryItem{
		EventId: request.GetPreviousEventId(),
		Version: request.GetPreviousEventVersion(),
	}
	if !versionhistory.ContainsVersionHistoryItem(request.GetVersionHistory(), previous) {
		return serviceerror.NewInvalidArgument("version history does not contain the previous cursor")
	}
	lastItem, err := versionhistory.GetLastVersionHistoryItem(request.GetVersionHistory())
	if err != nil {
		return serviceerror.NewInvalidArgument(err.Error())
	}
	if lastItem.GetEventId() != request.GetNewEventId() || lastItem.GetVersion() != request.GetNewEventVersion() {
		return serviceerror.NewInvalidArgument("version history does not end at the new cursor")
	}
	return nil
}

func decodeHistoryBatches(
	request *adminservice.SyncLocalExecutionRequest,
	eventSerializer serialization.Serializer,
	maxBytes int,
	maxEvents int,
	maxBatches int,
) ([][]*historypb.HistoryEvent, error) {
	if maxBatches >= 0 && len(request.GetHistoryBatches()) > maxBatches {
		return nil, syncLimitError("history batches", len(request.GetHistoryBatches()), maxBatches)
	}

	history := make([][]*historypb.HistoryEvent, 0, len(request.GetHistoryBatches()))
	expectedEventID := request.GetPreviousEventId() + 1
	totalBytes := 0
	totalEvents := 0
	for _, batch := range request.GetHistoryBatches() {
		totalBytes += len(batch.GetData())
		if maxBytes >= 0 && totalBytes > maxBytes {
			return nil, syncLimitError("serialized history bytes", totalBytes, maxBytes)
		}
		events, err := eventSerializer.DeserializeEvents(batch)
		if err != nil {
			return nil, serviceerror.NewInvalidArgumentf("invalid history batch: %v", err)
		}
		if len(events) == 0 {
			return nil, serviceerror.NewInvalidArgument("history batches must not be empty")
		}
		totalEvents += len(events)
		if maxEvents >= 0 && totalEvents > maxEvents {
			return nil, syncLimitError("history events", totalEvents, maxEvents)
		}
		expectedEventID, err = validateHistoryEvents(request.GetVersionHistory(), events, expectedEventID)
		if err != nil {
			return nil, err
		}
		history = append(history, events)
	}
	if expectedEventID != request.GetNewEventId()+1 {
		return nil, serviceerror.NewInvalidArgumentf(
			"history ends at event %d, expected %d",
			expectedEventID-1,
			request.GetNewEventId(),
		)
	}
	return history, nil
}

func validateHistoryEvents(
	versionHistory *historyspb.VersionHistory,
	events []*historypb.HistoryEvent,
	expectedEventID int64,
) (int64, error) {
	for _, event := range events {
		if event.GetEventId() != expectedEventID {
			return 0, serviceerror.NewInvalidArgumentf(
				"history is not contiguous: expected event %d, got %d",
				expectedEventID,
				event.GetEventId(),
			)
		}
		expectedVersion, err := versionhistory.GetVersionHistoryEventVersion(versionHistory, event.GetEventId())
		if err != nil || event.GetVersion() != expectedVersion {
			return 0, serviceerror.NewInvalidArgumentf(
				"event %d version does not match version history",
				event.GetEventId(),
			)
		}
		if err := validateSingleRunEvent(event); err != nil {
			return 0, err
		}
		expectedEventID++
	}
	return expectedEventID, nil
}

func syncLimitError(kind string, actual int, limit int) error {
	return serviceerror.NewResourceExhausted(
		enumspb.RESOURCE_EXHAUSTED_CAUSE_UNSPECIFIED,
		fmt.Sprintf("local execution synchronization %s %d exceeds limit %d", kind, actual, limit),
	)
}

func validateSingleRunEvent(event *historypb.HistoryEvent) error {
	switch event.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
		return serviceerror.NewInvalidArgument("protocol version 1 cannot synchronize continue-as-new")
	case enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED,
		enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED:
		return serviceerror.NewInvalidArgument("protocol version 1 cannot synchronize locally created child workflow history")
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		if event.GetWorkflowExecutionCompletedEventAttributes().GetNewExecutionRunId() != "" {
			return serviceerror.NewInvalidArgument("protocol version 1 cannot synchronize cron successor runs")
		}
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		if event.GetWorkflowExecutionFailedEventAttributes().GetNewExecutionRunId() != "" {
			return serviceerror.NewInvalidArgument("protocol version 1 cannot synchronize retry successor runs")
		}
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		if event.GetWorkflowExecutionTimedOutEventAttributes().GetNewExecutionRunId() != "" {
			return serviceerror.NewInvalidArgument("protocol version 1 cannot synchronize retry or cron successor runs")
		}
	default:
	}
	return nil
}

func repeatedSync(
	localInfo *persistencespb.LocalExecutionInfo,
	request *adminservice.SyncLocalExecutionRequest,
	requestHash []byte,
	now time.Time,
) (*historyservice.SyncLocalExecutionResponse, bool, error) {
	if localInfo.GetLastSyncId() != request.GetSyncId() {
		return nil, false, nil
	}
	requested := syncCursor{EventID: request.GetNewEventId(), Version: request.GetNewEventVersion()}
	stored := syncCursor{
		EventID: localInfo.GetLastSynchronizedEventId(),
		Version: localInfo.GetLastSynchronizedEventVersion(),
	}
	if requested != stored || !bytes.Equal(localInfo.GetLastSyncRequestHash(), requestHash) {
		return nil, true, serviceerror.NewFailedPrecondition("sync ID was already used for a different request")
	}
	if request.GetRelease() && localInfo.GetState() == persistencespb.LocalExecutionInfo_STATE_UNOWNED {
		return newResponse(request, nil), true, nil
	}
	if err := validateOwner(localInfo, request, now); err != nil {
		return nil, true, err
	}
	return newResponse(request, localInfo.GetLeaseExpirationTime()), true, nil
}

func validateState(
	mutableState historyi.MutableState,
	request *adminservice.SyncLocalExecutionRequest,
	now time.Time,
) error {
	localInfo := mutableState.GetExecutionInfo().GetLocalExecutionInfo()
	if err := validateOwner(localInfo, request, now); err != nil {
		return err
	}
	previous := syncCursor{EventID: request.GetPreviousEventId(), Version: request.GetPreviousEventVersion()}
	stored := syncCursor{
		EventID: localInfo.GetLastSynchronizedEventId(),
		Version: localInfo.GetLastSynchronizedEventVersion(),
	}
	if previous != stored {
		return serviceerror.NewFailedPreconditionf(
			"local execution cursor mismatch: stored cursor is %d/%d, request starts at %d/%d",
			stored.EventID,
			stored.Version,
			previous.EventID,
			previous.Version,
		)
	}
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(
		mutableState.GetExecutionInfo().GetVersionHistories(),
	)
	if err != nil {
		return err
	}
	current, err := versionhistory.GetLastVersionHistoryItem(currentVersionHistory)
	if err != nil {
		return err
	}
	if current.GetEventId() != previous.EventID || current.GetVersion() != previous.Version {
		return serviceerror.NewFailedPreconditionf(
			"upstream cursor is %d/%d, request starts at %d/%d",
			current.GetEventId(),
			current.GetVersion(),
			previous.EventID,
			previous.Version,
		)
	}
	lca, err := versionhistory.FindLCAVersionHistoryItem(currentVersionHistory, request.GetVersionHistory())
	if err != nil || lca == nil || lca.GetEventId() != previous.EventID || lca.GetVersion() != previous.Version {
		return serviceerror.NewFailedPrecondition("incoming version history does not extend the current upstream branch")
	}
	return nil
}

func validateOwner(
	localInfo *persistencespb.LocalExecutionInfo,
	request *adminservice.SyncLocalExecutionRequest,
	now time.Time,
) error {
	if localInfo.GetState() != persistencespb.LocalExecutionInfo_STATE_OWNED {
		return serviceerror.NewFailedPrecondition("workflow execution has no active local execution owner")
	}
	if localInfo.GetLocalServerId() != request.GetLocalServerId() ||
		localInfo.GetFencingEpoch() != request.GetFencingEpoch() {
		return serviceerror.NewFailedPrecondition("local execution owner is fenced")
	}
	tokenHash := sha256.Sum256(request.GetOwnershipToken())
	if subtle.ConstantTimeCompare(tokenHash[:], localInfo.GetOwnershipTokenHash()) != 1 {
		return serviceerror.NewFailedPrecondition("local execution ownership token is invalid")
	}
	if err := localInfo.GetLeaseExpirationTime().CheckValid(); err != nil {
		return serviceerror.NewInternal("local execution owner has an invalid lease expiration")
	}
	if !now.Before(localInfo.GetLeaseExpirationTime().AsTime()) {
		return serviceerror.NewFailedPrecondition("local execution ownership lease has expired")
	}
	return nil
}

func applySyncMetadata(
	mutableState historyi.MutableState,
	request *adminservice.SyncLocalExecutionRequest,
	requestHash []byte,
	now time.Time,
) (*timestamppb.Timestamp, error) {
	localInfo := mutableState.GetExecutionInfo().GetLocalExecutionInfo()
	localInfo.LastSynchronizedEventId = request.GetNewEventId()
	localInfo.LastSynchronizedEventVersion = request.GetNewEventVersion()
	localInfo.LastSyncId = request.GetSyncId()
	localInfo.LastSyncRequestHash = append(localInfo.LastSyncRequestHash[:0], requestHash...)
	if request.GetRelease() {
		localInfo.State = persistencespb.LocalExecutionInfo_STATE_UNOWNED
		localInfo.FencingEpoch++
		localInfo.OwnershipTokenHash = nil
		localInfo.LeaseExpirationTime = nil
		return nil, nil
	}
	leaseDuration := localInfo.GetLeaseDuration()
	if leaseDuration == nil || leaseDuration.CheckValid() != nil || leaseDuration.AsDuration() <= 0 {
		return nil, serviceerror.NewInternal("local execution owner has an invalid lease duration")
	}
	leaseExpiration := timestamppb.New(now.Add(leaseDuration.AsDuration()))
	localInfo.LeaseExpirationTime = leaseExpiration
	return leaseExpiration, nil
}

func newResponse(
	request *adminservice.SyncLocalExecutionRequest,
	leaseExpiration *timestamppb.Timestamp,
) *historyservice.SyncLocalExecutionResponse {
	return &historyservice.SyncLocalExecutionResponse{
		Response: &adminservice.SyncLocalExecutionResponse{
			SyncId:                   request.GetSyncId(),
			AcknowledgedEventId:      request.GetNewEventId(),
			AcknowledgedEventVersion: request.GetNewEventVersion(),
			LeaseExpirationTime:      leaseExpiration,
		},
	}
}
