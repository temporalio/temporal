// Copyright (c) 2019 Uber Technologies, Inc.
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

package history

import (
	"fmt"

	checksumgen "github.com/uber/cadence/.gen/go/checksum"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/checksum"
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

func newMutableStateChecksumPayload(ms mutableState) *checksumgen.MutableStateChecksumPayload {
	executionInfo := ms.GetExecutionInfo()
	replicationState := ms.GetReplicationState()
	payload := &checksumgen.MutableStateChecksumPayload{
		CancelRequested:      common.BoolPtr(executionInfo.CancelRequested),
		State:                common.Int16Ptr(int16(executionInfo.State)),
		LastFirstEventID:     common.Int64Ptr(executionInfo.LastFirstEventID),
		NextEventID:          common.Int64Ptr(executionInfo.NextEventID),
		LastProcessedEventID: common.Int64Ptr(executionInfo.LastProcessedEvent),
		SignalCount:          common.Int64Ptr(int64(executionInfo.SignalCount)),
		DecisionAttempt:      common.Int32Ptr(int32(executionInfo.DecisionAttempt)),
		DecisionScheduledID:  common.Int64Ptr(executionInfo.DecisionScheduleID),
		DecisionStartedID:    common.Int64Ptr(executionInfo.DecisionStartedID),
		DecisionVersion:      common.Int64Ptr(executionInfo.DecisionVersion),
		StickyTaskListName:   common.StringPtr(executionInfo.StickyTaskList),
	}

	if replicationState != nil {
		payload.LastWriteVersion = common.Int64Ptr(replicationState.LastWriteVersion)
		payload.LastWriteEventID = common.Int64Ptr(replicationState.LastWriteEventID)
	}

	versionHistories := ms.GetVersionHistories()
	if versionHistories != nil {
		payload.VersionHistories = versionHistories.ToThrift()
	}

	// for each of the pendingXXX ids below, sorting is needed to guarantee that
	// same serialized bytes can be generated during verification
	pendingTimerIDs := make([]int64, 0, len(ms.GetPendingTimerInfos()))
	for _, ti := range ms.GetPendingTimerInfos() {
		pendingTimerIDs = append(pendingTimerIDs, ti.StartedID)
	}
	common.SortInt64Slice(pendingTimerIDs)
	payload.PendingTimerStartedIDs = pendingTimerIDs

	pendingActivityIDs := make([]int64, 0, len(ms.GetPendingActivityInfos()))
	for id := range ms.GetPendingActivityInfos() {
		pendingActivityIDs = append(pendingActivityIDs, id)
	}
	common.SortInt64Slice(pendingActivityIDs)
	payload.PendingActivityScheduledIDs = pendingActivityIDs

	pendingChildIDs := make([]int64, 0, len(ms.GetPendingChildExecutionInfos()))
	for id := range ms.GetPendingChildExecutionInfos() {
		pendingChildIDs = append(pendingChildIDs, id)
	}
	common.SortInt64Slice(pendingChildIDs)
	payload.PendingChildInitiatedIDs = pendingChildIDs

	signalIDs := make([]int64, 0, len(ms.GetPendingSignalExternalInfos()))
	for id := range ms.GetPendingSignalExternalInfos() {
		signalIDs = append(signalIDs, id)
	}
	common.SortInt64Slice(signalIDs)
	payload.PendingSignalInitiatedIDs = signalIDs

	requestCancelIDs := make([]int64, 0, len(ms.GetPendingRequestCancelExternalInfos()))
	for id := range ms.GetPendingRequestCancelExternalInfos() {
		requestCancelIDs = append(requestCancelIDs, id)
	}
	common.SortInt64Slice(requestCancelIDs)
	payload.PendingReqCancelInitiatedIDs = requestCancelIDs
	return payload
}
