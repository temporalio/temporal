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

package persistence

import (
	commonpb "go.temporal.io/api/common/v1"
)

func sizeOfExecutionInfo(
	executionInfo *commonpb.DataBlob,
) int {
	return executionInfo.Size()
}

func sizeOfExecutionState(
	executionState *commonpb.DataBlob,
) int {
	return executionState.Size()
}

func sizeOfActivityInfoIDs(
	activityInfoIDs map[int64]struct{},
) int {
	return sizeOfInt64Set(activityInfoIDs)
}

func sizeOfActivityInfos(
	activityInfos map[int64]*commonpb.DataBlob,
) int {
	return sizeOfInt64BlobMap(activityInfos)
}

func sizeOfTimerInfoIDs(
	timerInfoIDs map[string]struct{},
) int {
	size := 0
	for id := range timerInfoIDs {
		size += len(id)
	}
	return size
}

func sizeOfTimerInfos(
	timerInfos map[string]*commonpb.DataBlob,
) int {
	size := 0
	for id, blob := range timerInfos {
		size += len(id) + blob.Size()
	}
	return size
}

func sizeOfChildWorkflowInfoIDs(
	childWorkflowInfoIDs map[int64]struct{},
) int {
	return sizeOfInt64Set(childWorkflowInfoIDs)
}

func sizeOfChildWorkflowInfos(
	childWorkflowInfos map[int64]*commonpb.DataBlob,
) int {
	return sizeOfInt64BlobMap(childWorkflowInfos)
}

func sizeOfRequestCancelInfoIDs(
	requestCancelInfoIDs map[int64]struct{},
) int {
	return sizeOfInt64Set(requestCancelInfoIDs)
}

func sizeOfRequestCancelInfos(
	requestCancelInfos map[int64]*commonpb.DataBlob,
) int {
	return sizeOfInt64BlobMap(requestCancelInfos)
}

func sizeOfSignalInfoIDs(
	signalInfoIDs map[int64]struct{},
) int {
	return sizeOfInt64Set(signalInfoIDs)
}

func sizeOfSignalInfos(
	signalInfos map[int64]*commonpb.DataBlob,
) int {
	return sizeOfInt64BlobMap(signalInfos)
}

func sizeOfSignalRequestIDs(
	signalRequestIDs map[string]struct{},
) int {
	return sizeOfStringSet(signalRequestIDs)
}

func sizeOfInt64Set(
	int64Set map[int64]struct{},
) int {
	// 8 == 64 bit / 8 bit per byte
	return 8 * len(int64Set)
}

func sizeOfStringSet(
	stringSet map[string]struct{},
) int {
	size := 0
	for requestID := range stringSet {
		size += len(requestID)
	}
	return size
}

func sizeOfInt64BlobMap(
	kvBlob map[int64]*commonpb.DataBlob,
) int {
	size := 0
	for _, blob := range kvBlob {
		// 8 == 64 bit / 8 bit per byte
		size += 8 + blob.Size()
	}
	return size
}

func sizeOfStringSlice(
	stringSlice []string,
) int {
	size := 0
	for _, str := range stringSlice {
		size += len(str)
	}
	return size
}

func sizeOfBlobSlice(
	blobSlice []*commonpb.DataBlob,
) int {
	size := 0
	for _, blob := range blobSlice {
		size += blob.Size()
	}
	return size
}
