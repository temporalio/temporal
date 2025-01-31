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
	"context"
	"fmt"
	"sort"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// ReadFullPageEvents reads a full page of history events from ExecutionManager. Due to storage format of V2 History
// it is not guaranteed that pageSize amount of data is returned. Function returns the list of history events, the size
// of data read, the next page token, and an error if present.
func ReadFullPageEvents(
	ctx context.Context,
	executionMgr ExecutionManager,
	req *ReadHistoryBranchRequest,
) ([]*historypb.HistoryEvent, int, []byte, error) {
	var historyEvents []*historypb.HistoryEvent
	size := 0
	for {
		response, err := executionMgr.ReadHistoryBranch(ctx, req)
		if err != nil {
			return nil, 0, nil, err
		}
		historyEvents = append(historyEvents, response.HistoryEvents...)
		size += response.Size
		if len(historyEvents) >= req.PageSize || len(response.NextPageToken) == 0 {
			return historyEvents, size, response.NextPageToken, nil
		}
		req.NextPageToken = response.NextPageToken
	}
}

// ReadFullPageEventsByBatch reads a full page of history events by batch from ExecutionManager. Due to storage format of V2 History
// it is not guaranteed that pageSize amount of data is returned. Function returns the list of history batches, the size
// of data read, the next page token, and an error if present.
func ReadFullPageEventsByBatch(
	ctx context.Context,
	executionMgr ExecutionManager,
	req *ReadHistoryBranchRequest,
) ([]*historypb.History, int, []byte, error) {
	var historyBatches []*historypb.History
	eventsRead := 0
	size := 0
	for {
		response, err := executionMgr.ReadHistoryBranchByBatch(ctx, req)
		if err != nil {
			return nil, 0, nil, err
		}
		historyBatches = append(historyBatches, response.History...)
		for _, batch := range response.History {
			eventsRead += len(batch.Events)
		}
		size += response.Size
		if eventsRead >= req.PageSize || len(response.NextPageToken) == 0 {
			return historyBatches, size, response.NextPageToken, nil
		}
		req.NextPageToken = response.NextPageToken
	}
}

// ReadFullPageEventsReverse reads a full page of history events from ExecutionManager in reverse orcer. Due to storage
// format of V2 History it is not guaranteed that pageSize amount of data is returned. Function returns the list of
// history events, the size of data read, the next page token, and an error if present.
func ReadFullPageEventsReverse(
	ctx context.Context,
	executionMgr ExecutionManager,
	req *ReadHistoryBranchReverseRequest,
) ([]*historypb.HistoryEvent, int, []byte, error) {
	var historyEvents []*historypb.HistoryEvent
	size := 0
	for {
		response, err := executionMgr.ReadHistoryBranchReverse(ctx, req)
		if err != nil {
			return nil, 0, nil, err
		}
		historyEvents = append(historyEvents, response.HistoryEvents...)
		size += response.Size
		if len(historyEvents) >= req.PageSize || len(response.NextPageToken) == 0 {
			return historyEvents, size, response.NextPageToken, nil
		}
		req.NextPageToken = response.NextPageToken
	}
}

// GetBeginNodeID gets node id from last ancestor
func GetBeginNodeID(bi *persistencespb.HistoryBranch) int64 {
	if len(bi.Ancestors) == 0 {
		// root branch
		return 1
	}
	idx := len(bi.Ancestors) - 1
	return bi.Ancestors[idx].GetEndNodeId()
}

func sortAncestors(ans []*persistencespb.HistoryBranchRange) {
	if len(ans) > 0 {
		// sort ans based onf EndNodeID so that we can set BeginNodeID
		sort.Slice(ans, func(i, j int) bool { return (ans)[i].GetEndNodeId() < (ans)[j].GetEndNodeId() })
		(ans)[0].BeginNodeId = int64(1)
		for i := 1; i < len(ans); i++ {
			(ans)[i].BeginNodeId = (ans)[i-1].GetEndNodeId()
		}
	}
}

func ValidateBatch(
	batch []*historyspb.StrippedHistoryEvent,
	branchToken []byte,
	token *historyPagingToken,
	logger log.Logger,
) error {
	var firstEvent, lastEvent *historyspb.StrippedHistoryEvent
	var eventCount int
	dataLossTags := func(cause string) []tag.Tag {
		return []tag.Tag{
			tag.Cause(cause),
			tag.WorkflowBranchToken(branchToken),
			tag.WorkflowFirstEventID(firstEvent.GetEventId()),
			tag.FirstEventVersion(firstEvent.GetVersion()),
			tag.WorkflowNextEventID(lastEvent.GetEventId()),
			tag.LastEventVersion(lastEvent.GetVersion()),
			tag.Counter(eventCount),
			tag.TokenLastEventID(token.LastEventID),
		}
	}
	firstEvent = batch[0]
	eventCount = len(batch)
	lastEvent = batch[eventCount-1]

	if firstEvent.GetVersion() != lastEvent.GetVersion() || firstEvent.GetEventId()+int64(eventCount-1) != lastEvent.GetEventId() {
		// in a single batch, version should be the same, and ID should be contiguous
		logger.Error(dataLossMsg, dataLossTags(errWrongVersion)...)
		return serviceerror.NewDataLoss(errWrongVersion)
	}
	if firstEvent.GetEventId() != token.LastEventID+1 {
		logger.Error(dataLossMsg, dataLossTags(errNonContiguousEventID)...)
		return serviceerror.NewDataLoss(errNonContiguousEventID)
	}
	token.LastEventID = lastEvent.GetEventId()
	return nil
}

func VerifyHistoryIsComplete(
	events []*historyspb.StrippedHistoryEvent,
	expectedFirstEventID int64,
	expectedLastEventID int64,
	isFirstPage bool,
	isLastPage bool,
	pageSize int,
) error {

	nEvents := len(events)
	if nEvents == 0 {
		if isLastPage {
			// we seem to be returning a non-nil pageToken on the lastPage which
			// in turn cases the client to call getHistory again - only to find
			// there are no more events to consume - bail out if this is the case here
			return nil
		}
		return serviceerror.NewDataLoss("History contains zero events.")
	}

	firstEventID := events[0].GetEventId()
	lastEventID := events[nEvents-1].GetEventId()

	if !isFirstPage { // at least one page of history has been read previously
		if firstEventID <= expectedFirstEventID {
			// not first page and no events have been read in the previous pages - not possible
			return serviceerror.NewDataLoss(fmt.Sprintf("Invalid history: expected first eventID to be > %v but got %v", expectedFirstEventID, firstEventID))
		}
		expectedFirstEventID = firstEventID
	}

	if !isLastPage {
		// estimate lastEventID based on pageSize. This is a lower bound
		// since the persistence layer counts "batch of events" as a single page
		expectedLastEventID = expectedFirstEventID + int64(pageSize) - 1
	}

	nExpectedEvents := expectedLastEventID - expectedFirstEventID + 1

	if firstEventID == expectedFirstEventID &&
		((isLastPage && lastEventID == expectedLastEventID && int64(nEvents) == nExpectedEvents) ||
			(!isLastPage && lastEventID >= expectedLastEventID && int64(nEvents) >= nExpectedEvents)) {
		return nil
	}

	return serviceerror.NewDataLoss(fmt.Sprintf("Incomplete history: expected events [%v-%v] but got events [%v-%v] of length %v: isFirstPage=%v,isLastPage=%v,pageSize=%v",
		expectedFirstEventID,
		expectedLastEventID,
		firstEventID,
		lastEventID,
		nEvents,
		isFirstPage,
		isLastPage,
		pageSize))
}
