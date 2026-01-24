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
	if csum.GetVersion() != mutableStateChecksumPayloadV1 {
		return fmt.Errorf("invalid checksum payload version %v", csum.GetVersion())
	}
	payload := newMutableStateChecksumPayload(ms)
	return checksum.Verify(payload, csum)
}

func newMutableStateChecksumPayload(ms historyi.MutableState) *checksumspb.MutableStateChecksumPayload {
	executionInfo := ms.GetExecutionInfo()
	executionState := ms.GetExecutionState()
	payload := checksumspb.MutableStateChecksumPayload_builder{
		CancelRequested:              executionInfo.GetCancelRequested(),
		State:                        executionState.GetState(),
		LastFirstEventId:             executionInfo.GetLastFirstEventId(),
		NextEventId:                  ms.GetNextEventID(),
		LastProcessedEventId:         executionInfo.GetLastCompletedWorkflowTaskStartedEventId(),
		ActivityCount:                executionInfo.GetActivityCount(),
		ChildExecutionCount:          executionInfo.GetChildExecutionCount(),
		UserTimerCount:               executionInfo.GetUserTimerCount(),
		RequestCancelExternalCount:   executionInfo.GetRequestCancelExternalCount(),
		SignalExternalCount:          executionInfo.GetSignalExternalCount(),
		SignalCount:                  executionInfo.GetSignalCount(),
		WorkflowTaskAttempt:          executionInfo.GetWorkflowTaskAttempt(),
		WorkflowTaskScheduledEventId: executionInfo.GetWorkflowTaskScheduledEventId(),
		WorkflowTaskStartedEventId:   executionInfo.GetWorkflowTaskStartedEventId(),
		WorkflowTaskVersion:          executionInfo.GetWorkflowTaskVersion(),
		StickyTaskQueueName:          executionInfo.GetStickyTaskQueue(),
		VersionHistories:             executionInfo.GetVersionHistories(),
	}.Build()

	// for each of the pendingXXX ids below, sorting is needed to guarantee that
	// same serialized bytes can be generated during verification
	pendingTimerIDs := make([]int64, 0, len(ms.GetPendingTimerInfos()))
	for _, ti := range ms.GetPendingTimerInfos() {
		pendingTimerIDs = append(pendingTimerIDs, ti.GetStartedEventId())
	}
	util.SortSlice(pendingTimerIDs)
	payload.SetPendingTimerStartedEventIds(pendingTimerIDs)

	pendingActivityIDs := expmaps.Keys(ms.GetPendingActivityInfos())
	util.SortSlice(pendingActivityIDs)
	payload.SetPendingActivityScheduledEventIds(pendingActivityIDs)

	pendingChildIDs := expmaps.Keys(ms.GetPendingChildExecutionInfos())
	util.SortSlice(pendingChildIDs)
	payload.SetPendingChildInitiatedEventIds(pendingChildIDs)

	signalIDs := expmaps.Keys(ms.GetPendingSignalExternalInfos())
	util.SortSlice(signalIDs)
	payload.SetPendingSignalInitiatedEventIds(signalIDs)

	requestCancelIDs := expmaps.Keys(ms.GetPendingRequestCancelExternalInfos())
	util.SortSlice(requestCancelIDs)
	payload.SetPendingReqCancelInitiatedEventIds(requestCancelIDs)

	chasmNodePaths := expmaps.Keys(ms.ChasmTree().Snapshot(nil).Nodes)
	util.SortSlice(chasmNodePaths)
	payload.SetPendingChasmNodePaths(chasmNodePaths)

	return payload
}
