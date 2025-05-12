package workflow

import (
	"fmt"

	checksumspb "go.temporal.io/server/api/checksum/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/checksum"
	"go.temporal.io/server/common/util"
	historyi "go.temporal.io/server/service/history/interfaces"
	expmaps "golang.org/x/exp/maps"
)

const (
	mutableStateChecksumPayloadV1 = int32(1)
)

func generateMutableStateChecksum(ms historyi.MutableState) (*persistencespb.Checksum, error) {
	payload := newMutableStateChecksumPayload(ms)
	csum, err := checksum.GenerateCRC32(payload, mutableStateChecksumPayloadV1)
	if err != nil {
		return nil, err
	}
	return csum, nil
}

func verifyMutableStateChecksum(
	ms historyi.MutableState,
	csum *persistencespb.Checksum,
) error {
	if csum.Version != mutableStateChecksumPayloadV1 {
		return fmt.Errorf("invalid checksum payload version %v", csum.Version)
	}
	payload := newMutableStateChecksumPayload(ms)
	return checksum.Verify(payload, csum)
}

func newMutableStateChecksumPayload(ms historyi.MutableState) *checksumspb.MutableStateChecksumPayload {
	executionInfo := ms.GetExecutionInfo()
	executionState := ms.GetExecutionState()
	payload := &checksumspb.MutableStateChecksumPayload{
		CancelRequested:              executionInfo.CancelRequested,
		State:                        executionState.State,
		LastFirstEventId:             executionInfo.LastFirstEventId,
		NextEventId:                  ms.GetNextEventID(),
		LastProcessedEventId:         executionInfo.LastCompletedWorkflowTaskStartedEventId,
		ActivityCount:                executionInfo.ActivityCount,
		ChildExecutionCount:          executionInfo.ChildExecutionCount,
		UserTimerCount:               executionInfo.UserTimerCount,
		RequestCancelExternalCount:   executionInfo.RequestCancelExternalCount,
		SignalExternalCount:          executionInfo.SignalExternalCount,
		SignalCount:                  executionInfo.SignalCount,
		WorkflowTaskAttempt:          executionInfo.WorkflowTaskAttempt,
		WorkflowTaskScheduledEventId: executionInfo.WorkflowTaskScheduledEventId,
		WorkflowTaskStartedEventId:   executionInfo.WorkflowTaskStartedEventId,
		WorkflowTaskVersion:          executionInfo.WorkflowTaskVersion,
		StickyTaskQueueName:          executionInfo.StickyTaskQueue,
		VersionHistories:             executionInfo.VersionHistories,
	}

	// for each of the pendingXXX ids below, sorting is needed to guarantee that
	// same serialized bytes can be generated during verification
	pendingTimerIDs := make([]int64, 0, len(ms.GetPendingTimerInfos()))
	for _, ti := range ms.GetPendingTimerInfos() {
		pendingTimerIDs = append(pendingTimerIDs, ti.GetStartedEventId())
	}
	util.SortSlice(pendingTimerIDs)
	payload.PendingTimerStartedEventIds = pendingTimerIDs

	pendingActivityIDs := expmaps.Keys(ms.GetPendingActivityInfos())
	util.SortSlice(pendingActivityIDs)
	payload.PendingActivityScheduledEventIds = pendingActivityIDs

	pendingChildIDs := expmaps.Keys(ms.GetPendingChildExecutionInfos())
	util.SortSlice(pendingChildIDs)
	payload.PendingChildInitiatedEventIds = pendingChildIDs

	signalIDs := expmaps.Keys(ms.GetPendingSignalExternalInfos())
	util.SortSlice(signalIDs)
	payload.PendingSignalInitiatedEventIds = signalIDs

	requestCancelIDs := expmaps.Keys(ms.GetPendingRequestCancelExternalInfos())
	util.SortSlice(requestCancelIDs)
	payload.PendingReqCancelInitiatedEventIds = requestCancelIDs

	chasmNodePaths := expmaps.Keys(ms.ChasmTree().Snapshot(nil).Nodes)
	util.SortSlice(chasmNodePaths)
	payload.PendingChasmNodePaths = chasmNodePaths

	return payload
}
