package renewlocalexecutionlease

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func Invoke(
	ctx context.Context,
	request *historyservice.RenewLocalExecutionLeaseRequest,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (*historyservice.RenewLocalExecutionLeaseResponse, error) {
	if request.GetExecution().GetWorkflowId() == "" || request.GetExecution().GetRunId() == "" {
		return nil, serviceerror.NewInvalidArgument("workflow ID and run ID are required")
	}
	if _, err := api.GetActiveNamespace(
		shardContext,
		namespace.ID(request.GetNamespaceId()),
		request.GetExecution().GetWorkflowId(),
	); err != nil {
		return nil, err
	}

	var response *historyservice.RenewLocalExecutionLeaseResponse
	err := api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		definition.NewWorkflowKey(
			request.GetNamespaceId(),
			request.GetExecution().GetWorkflowId(),
			request.GetExecution().GetRunId(),
		),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			var updateAction *api.UpdateWorkflowAction
			var updateErr error
			response, updateAction, updateErr = applyRenewal(
				workflowLease.GetMutableState(),
				request,
				shardContext.GetTimeSource().Now(),
			)
			return updateAction, updateErr
		},
		nil,
		shardContext,
		workflowConsistencyChecker,
	)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func applyRenewal(
	mutableState historyi.MutableState,
	request *historyservice.RenewLocalExecutionLeaseRequest,
	now time.Time,
) (*historyservice.RenewLocalExecutionLeaseResponse, *api.UpdateWorkflowAction, error) {
	localInfo := mutableState.GetExecutionInfo().GetLocalExecutionInfo()
	if err := validateOwner(localInfo, request, now); err != nil {
		return nil, nil, err
	}
	requestedCursor := syncCursor{EventID: request.GetNewEventId(), Version: request.GetNewEventVersion()}
	if response, handled, err := repeatedRenewal(localInfo, request, requestedCursor); handled || err != nil {
		return response, &api.UpdateWorkflowAction{Noop: true}, err
	}
	if err := validateCursors(mutableState, localInfo, request, requestedCursor); err != nil {
		return nil, nil, err
	}

	localInfo.LastSynchronizedEventId = requestedCursor.EventID
	localInfo.LastSynchronizedEventVersion = requestedCursor.Version
	localInfo.LastSyncId = request.GetSyncId()
	localInfo.LastSyncRequestHash = append(localInfo.LastSyncRequestHash[:0], request.GetSyncRequestHash()...)
	if request.GetRelease() {
		localInfo.State = persistencespb.LocalExecutionInfo_STATE_UNOWNED
		localInfo.FencingEpoch++
		localInfo.OwnershipTokenHash = nil
		localInfo.LeaseExpirationTime = nil
		return &historyservice.RenewLocalExecutionLeaseResponse{}, &api.UpdateWorkflowAction{}, nil
	}

	leaseDuration := localInfo.GetLeaseDuration()
	if leaseDuration == nil || leaseDuration.CheckValid() != nil || leaseDuration.AsDuration() <= 0 {
		return nil, nil, serviceerror.NewInternal("local execution owner has an invalid lease duration")
	}
	leaseExpiration := timestamppb.New(now.Add(leaseDuration.AsDuration()))
	localInfo.LeaseExpirationTime = leaseExpiration
	return &historyservice.RenewLocalExecutionLeaseResponse{
		LeaseExpirationTime: leaseExpiration,
	}, &api.UpdateWorkflowAction{}, nil
}

func validateOwner(
	localInfo *persistencespb.LocalExecutionInfo,
	request *historyservice.RenewLocalExecutionLeaseRequest,
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

func repeatedRenewal(
	localInfo *persistencespb.LocalExecutionInfo,
	request *historyservice.RenewLocalExecutionLeaseRequest,
	requestedCursor syncCursor,
) (*historyservice.RenewLocalExecutionLeaseResponse, bool, error) {
	if localInfo.GetLastSyncId() != request.GetSyncId() ||
		localInfo.GetLastSynchronizedEventId() != requestedCursor.EventID ||
		localInfo.GetLastSynchronizedEventVersion() != requestedCursor.Version {
		return nil, false, nil
	}
	if !bytes.Equal(localInfo.GetLastSyncRequestHash(), request.GetSyncRequestHash()) {
		return nil, true, serviceerror.NewFailedPrecondition("sync ID was already used for a different request")
	}
	return &historyservice.RenewLocalExecutionLeaseResponse{
		LeaseExpirationTime: localInfo.GetLeaseExpirationTime(),
	}, true, nil
}

func validateCursors(
	mutableState historyi.MutableState,
	localInfo *persistencespb.LocalExecutionInfo,
	request *historyservice.RenewLocalExecutionLeaseRequest,
	requestedCursor syncCursor,
) error {
	previousCursor := syncCursor{EventID: request.GetPreviousEventId(), Version: request.GetPreviousEventVersion()}
	storedCursor := syncCursor{
		EventID: localInfo.GetLastSynchronizedEventId(),
		Version: localInfo.GetLastSynchronizedEventVersion(),
	}
	if storedCursor != previousCursor {
		return serviceerror.NewFailedPreconditionf(
			"local execution lease cursor mismatch: stored cursor is %d/%d, request starts at %d/%d",
			storedCursor.EventID,
			storedCursor.Version,
			previousCursor.EventID,
			previousCursor.Version,
		)
	}
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(
		mutableState.GetExecutionInfo().GetVersionHistories(),
	)
	if err != nil {
		return err
	}
	currentItem, err := versionhistory.GetLastVersionHistoryItem(currentVersionHistory)
	if err != nil {
		return err
	}
	currentCursor := syncCursor{EventID: currentItem.GetEventId(), Version: currentItem.GetVersion()}
	if currentCursor != requestedCursor {
		return serviceerror.NewFailedPreconditionf(
			"upstream cursor is %d/%d, expected synchronized cursor %d/%d",
			currentCursor.EventID,
			currentCursor.Version,
			requestedCursor.EventID,
			requestedCursor.Version,
		)
	}
	return nil
}

type syncCursor struct {
	EventID int64
	Version int64
}
