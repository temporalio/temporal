package history

import (
	"fmt"

	checksumproto "github.com/temporalio/temporal/.gen/proto/checksum"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/checksum"
)

const (
	mutableStateChecksumPayloadV1 = 1
)

func generateMutableStateChecksum(ms mutableState) (checksum.Checksum, error) {
	payload := newMutableStateChecksumPayload(ms)
	csum, err := checksum.GenerateCRC32(payload, mutableStateChecksumPayloadV1)
	if err != nil {
		return checksum.Checksum{}, err
	}
	return csum, nil
}

func verifyMutableStateChecksum(
	ms mutableState,
	csum checksum.Checksum,
) error {
	if csum.Version != mutableStateChecksumPayloadV1 {
		return fmt.Errorf("invalid checksum payload version %v", csum.Version)
	}
	payload := newMutableStateChecksumPayload(ms)
	return checksum.Verify(payload, csum)
}

func newMutableStateChecksumPayload(ms mutableState) *checksumproto.MutableStateChecksumPayload {
	executionInfo := ms.GetExecutionInfo()
	replicationState := ms.GetReplicationState()
	payload := &checksumproto.MutableStateChecksumPayload{
		CancelRequested:      executionInfo.CancelRequested,
		State:                int32(executionInfo.State),
		LastFirstEventId:     executionInfo.LastFirstEventID,
		NextEventId:          executionInfo.NextEventID,
		LastProcessedEventId: executionInfo.LastProcessedEvent,
		SignalCount:          int64(executionInfo.SignalCount),
		DecisionAttempt:      int32(executionInfo.DecisionAttempt),
		DecisionScheduledId:  executionInfo.DecisionScheduleID,
		DecisionStartedId:    executionInfo.DecisionStartedID,
		DecisionVersion:      executionInfo.DecisionVersion,
		StickyTaskListName:   executionInfo.StickyTaskList,
	}

	if replicationState != nil {
		payload.LastWriteVersion = replicationState.LastWriteVersion
		payload.LastWriteEventId = replicationState.LastWriteEventID
	}

	versionHistories := ms.GetVersionHistories()
	if versionHistories != nil {
		payload.VersionHistories = versionHistories.ToProto()
	}

	// for each of the pendingXXX ids below, sorting is needed to guarantee that
	// same serialized bytes can be generated during verification
	pendingTimerIDs := make([]int64, 0, len(ms.GetPendingTimerInfos()))
	for _, ti := range ms.GetPendingTimerInfos() {
		pendingTimerIDs = append(pendingTimerIDs, ti.GetStartedId())
	}
	common.SortInt64Slice(pendingTimerIDs)
	payload.PendingTimerStartedIds = pendingTimerIDs

	pendingActivityIDs := make([]int64, 0, len(ms.GetPendingActivityInfos()))
	for id := range ms.GetPendingActivityInfos() {
		pendingActivityIDs = append(pendingActivityIDs, id)
	}
	common.SortInt64Slice(pendingActivityIDs)
	payload.PendingActivityScheduledIds = pendingActivityIDs

	pendingChildIDs := make([]int64, 0, len(ms.GetPendingChildExecutionInfos()))
	for id := range ms.GetPendingChildExecutionInfos() {
		pendingChildIDs = append(pendingChildIDs, id)
	}
	common.SortInt64Slice(pendingChildIDs)
	payload.PendingChildInitiatedIds = pendingChildIDs

	signalIDs := make([]int64, 0, len(ms.GetPendingSignalExternalInfos()))
	for id := range ms.GetPendingSignalExternalInfos() {
		signalIDs = append(signalIDs, id)
	}
	common.SortInt64Slice(signalIDs)
	payload.PendingSignalInitiatedIds = signalIDs

	requestCancelIDs := make([]int64, 0, len(ms.GetPendingRequestCancelExternalInfos()))
	for id := range ms.GetPendingRequestCancelExternalInfos() {
		requestCancelIDs = append(requestCancelIDs, id)
	}
	common.SortInt64Slice(requestCancelIDs)
	payload.PendingReqCancelInitiatedIds = requestCancelIDs
	return payload
}
