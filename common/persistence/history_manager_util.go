package persistence

import (
	"context"
	"sort"

	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/softassert"
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

// ReadFullPageRawEvents reads a full page of raw history events from ExecutionManager. Due to storage format of V2 History
// it is not guaranteed that pageSize amount of data is returned. Function returns the list of history blobs, the size
// of data read, the next page token, and an error if present.
func ReadFullPageRawEvents(
	ctx context.Context,
	executionMgr ExecutionManager,
	req *ReadHistoryBranchRequest,
) ([]*commonpb.DataBlob, int, []byte, error) {
	var blobs []*commonpb.DataBlob
	size := 0
	for {
		response, err := executionMgr.ReadRawHistoryBranch(ctx, req)
		if err != nil {
			return nil, 0, nil, err
		}
		blobs = append(blobs, response.HistoryEventBlobs...)
		size += response.Size
		if len(blobs) >= req.PageSize || len(response.NextPageToken) == 0 {
			return blobs, size, response.NextPageToken, nil
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
	lastEventID int64,
	logger log.Logger,
) error {
	var firstEvent, lastEvent *historyspb.StrippedHistoryEvent
	var eventCount int
	dataLossTags := func(cause error) []tag.Tag {
		return []tag.Tag{
			tag.Cause(cause.Error()),
			tag.WorkflowBranchToken(branchToken),
			tag.WorkflowFirstEventID(firstEvent.GetEventId()),
			tag.FirstEventVersion(firstEvent.GetVersion()),
			tag.WorkflowNextEventID(lastEvent.GetEventId()),
			tag.LastEventVersion(lastEvent.GetVersion()),
			tag.Counter(eventCount),
			tag.TokenLastEventID(lastEventID),
		}
	}
	firstEvent = batch[0]
	eventCount = len(batch)
	lastEvent = batch[eventCount-1]

	if firstEvent.GetVersion() != lastEvent.GetVersion() || firstEvent.GetEventId()+int64(eventCount-1) != lastEvent.GetEventId() {
		// in a single batch, version should be the same, and ID should be contiguous
		return softassert.UnexpectedDataLoss(logger, dataLossMsg, errWrongVersion, dataLossTags(errWrongVersion)...)
	}
	// If it is the first batch in the response, we cannot check the first event id here. That information is in the historyPagingToken.
	// TODO: PPV refactor to move this check to ExecutionManager so that we can include that check as well.
	if lastEventID != 0 && firstEvent.GetEventId() != lastEventID+1 {
		return softassert.UnexpectedDataLoss(logger, dataLossMsg, errNonContiguousEventID, dataLossTags(errNonContiguousEventID)...)
	}
	return nil
}
