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
	"sort"

	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence/serialization"
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

//// DeserializeAndValidateNodes deserializes and validates history nodes
//func DeserializeAndValidateNodes(
//	request *ReadHistoryBranchRequest,
//	dataBlobs []*commonpb.DataBlob,
//	token []byte,
//	byBatch bool,
//	serializer serialization.Serializer,
//	logger log.Logger,
//) ([]*historypb.HistoryEvent, []*historypb.History, error) {
//	token := serializer.Dese
//	return deserializeAndValidateNodes(request, dataBlobs, token, byBatch, serializer, logger)
//}

// DeserializeAndValidateNodes deserializes and validates history nodes
func DeserializeAndValidateNodes(
	dataBlobs []*commonpb.DataBlob,
	pageSize int,
	branchToken []byte,
	token *historyPagingToken,
	byBatch bool,
	serializer serialization.Serializer,
	logger log.Logger,
) ([]*historypb.HistoryEvent, []*historypb.History, error) {

	historyEvents := make([]*historypb.HistoryEvent, 0, pageSize)
	historyEventBatches := make([]*historypb.History, 0, pageSize)

	var firstEvent, lastEvent *historypb.HistoryEvent
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

	for _, batch := range dataBlobs {
		events, err := serializer.DeserializeEvents(batch)
		if err != nil {
			return nil, nil, err
		}
		if len(events) == 0 {
			logger.Error(dataLossMsg, dataLossTags(errEmptyEvents)...)
			return nil, nil, serviceerror.NewDataLoss(errEmptyEvents)
		}

		firstEvent = events[0]
		eventCount = len(events)
		lastEvent = events[eventCount-1]

		if firstEvent.GetVersion() != lastEvent.GetVersion() || firstEvent.GetEventId()+int64(eventCount-1) != lastEvent.GetEventId() {
			// in a single batch, version should be the same, and ID should be contiguous
			logger.Error(dataLossMsg, dataLossTags(errWrongVersion)...)
			return historyEvents, historyEventBatches, serviceerror.NewDataLoss(errWrongVersion)
		}
		if firstEvent.GetEventId() != token.LastEventID+1 {
			logger.Error(dataLossMsg, dataLossTags(errNonContiguousEventID)...)
			return historyEvents, historyEventBatches, serviceerror.NewDataLoss(errNonContiguousEventID)
		}

		if byBatch {
			historyEventBatches = append(historyEventBatches, &historypb.History{Events: events})
		} else {
			historyEvents = append(historyEvents, events...)
		}
		token.LastEventID = lastEvent.GetEventId()
	}
	return historyEvents, historyEventBatches, nil
}
