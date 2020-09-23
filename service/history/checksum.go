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

package history

import (
	"fmt"

	checksumspb "go.temporal.io/server/api/checksum/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/checksum"
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

func newMutableStateChecksumPayload(ms mutableState) *checksumspb.MutableStateChecksumPayload {
	executionInfo := ms.GetExecutionInfo()
	payload := &checksumspb.MutableStateChecksumPayload{
		CancelRequested:         executionInfo.CancelRequested,
		State:                   executionInfo.ExecutionState.State,
		LastFirstEventId:        executionInfo.LastFirstEventId,
		NextEventId:             executionInfo.NextEventId,
		LastProcessedEventId:    executionInfo.LastProcessedEvent,
		SignalCount:             int64(executionInfo.SignalCount),
		WorkflowTaskAttempt:     int32(executionInfo.WorkflowTaskAttempt),
		WorkflowTaskScheduledId: executionInfo.WorkflowTaskScheduleId,
		WorkflowTaskStartedId:   executionInfo.WorkflowTaskStartedId,
		WorkflowTaskVersion:     executionInfo.WorkflowTaskVersion,
		StickyTaskQueueName:     executionInfo.StickyTaskQueue,
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
