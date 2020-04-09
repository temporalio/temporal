package persistence

import (
	"fmt"

	eventpb "go.temporal.io/temporal-proto/event"

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
)

// ReadFullPageV2Events reads a full page of history events from HistoryManager. Due to storage format of V2 History
// it is not guaranteed that pageSize amount of data is returned. Function returns the list of history events, the size
// of data read, the next page token, and an error if present.
func ReadFullPageV2Events(historyV2Mgr HistoryManager, req *ReadHistoryBranchRequest) ([]*eventpb.HistoryEvent, int, []byte, error) {
	var historyEvents []*eventpb.HistoryEvent
	size := int(0)
	for {
		response, err := historyV2Mgr.ReadHistoryBranch(req)
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

// ReadFullPageV2EventsByBatch reads a full page of history events by batch from HistoryManager. Due to storage format of V2 History
// it is not guaranteed that pageSize amount of data is returned. Function returns the list of history batches, the size
// of data read, the next page token, and an error if present.
func ReadFullPageV2EventsByBatch(historyV2Mgr HistoryManager, req *ReadHistoryBranchRequest) ([]*eventpb.History, int, []byte, error) {
	historyBatches := []*eventpb.History{}
	eventsRead := 0
	size := 0
	for {
		response, err := historyV2Mgr.ReadHistoryBranchByBatch(req)
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

// GetBeginNodeID gets node id from last ancestor
func GetBeginNodeID(bi *persistenceblobs.HistoryBranch) int64 {
	if len(bi.Ancestors) == 0 {
		// root branch
		return 1
	}
	idx := len(bi.Ancestors) - 1
	return bi.Ancestors[idx].GetEndNodeId()
}

func getShardID(shardID *int) (int, error) {
	if shardID == nil {
		return 0, fmt.Errorf("shardID is not set for persistence operation")
	}
	return *shardID, nil
}
