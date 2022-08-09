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

package workflow

import (
	"fmt"

	checksumspb "go.temporal.io/server/api/checksum/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/checksum"
	"go.temporal.io/server/common/util"
	"golang.org/x/exp/maps"
)

const (
	mutableStateChecksumPayloadV1 = int32(1)
)

func generateMutableStateChecksum(ms MutableState) (*persistencespb.Checksum, error) {
	payload := newMutableStateChecksumPayload(ms)
	csum, err := checksum.GenerateCRC32(payload, mutableStateChecksumPayloadV1)
	if err != nil {
		return nil, err
	}
	return csum, nil
}

func verifyMutableStateChecksum(
	ms MutableState,
	csum *persistencespb.Checksum,
) error {
	if csum.Version != mutableStateChecksumPayloadV1 {
		return fmt.Errorf("invalid checksum payload version %v", csum.Version)
	}
	payload := newMutableStateChecksumPayload(ms)
	return checksum.Verify(payload, csum)
}

func newMutableStateChecksumPayload(ms MutableState) *checksumspb.MutableStateChecksumPayload {
	executionInfo := ms.GetExecutionInfo()
	executionState := ms.GetExecutionState()
	payload := &checksumspb.MutableStateChecksumPayload{
		CancelRequested:              executionInfo.CancelRequested,
		State:                        executionState.State,
		LastFirstEventId:             executionInfo.LastFirstEventId,
		NextEventId:                  ms.GetNextEventID(),
		LastProcessedEventId:         executionInfo.LastWorkflowTaskStartedEventId,
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

	pendingActivityIDs := maps.Keys(ms.GetPendingActivityInfos())
	util.SortSlice(pendingActivityIDs)
	payload.PendingActivityScheduledEventIds = pendingActivityIDs

	pendingChildIDs := maps.Keys(ms.GetPendingChildExecutionInfos())
	util.SortSlice(pendingChildIDs)
	payload.PendingChildInitiatedEventIds = pendingChildIDs

	signalIDs := maps.Keys(ms.GetPendingSignalExternalInfos())
	util.SortSlice(signalIDs)
	payload.PendingSignalInitiatedEventIds = signalIDs

	requestCancelIDs := maps.Keys(ms.GetPendingRequestCancelExternalInfos())
	util.SortSlice(requestCancelIDs)
	payload.PendingReqCancelInitiatedEventIds = requestCancelIDs
	return payload
}
