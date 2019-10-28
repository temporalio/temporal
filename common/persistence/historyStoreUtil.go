// Copyright (c) 2017 Uber Technologies, Inc.
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
	"fmt"

	"github.com/uber/cadence/.gen/go/shared"
)

// ReadFullPageV2Events reads a full page of history events from HistoryManager. Due to storage format of V2 History
// it is not guaranteed that pageSize amount of data is returned. Function returns the list of history events, the size
// of data read, the next page token, and an error if present.
func ReadFullPageV2Events(historyV2Mgr HistoryManager, req *ReadHistoryBranchRequest) ([]*shared.HistoryEvent, int, []byte, error) {
	historyEvents := []*shared.HistoryEvent{}
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
func ReadFullPageV2EventsByBatch(historyV2Mgr HistoryManager, req *ReadHistoryBranchRequest) ([]*shared.History, int, []byte, error) {
	historyBatches := []*shared.History{}
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
func GetBeginNodeID(bi shared.HistoryBranch) int64 {
	if len(bi.Ancestors) == 0 {
		// root branch
		return 1
	}
	idx := len(bi.Ancestors) - 1
	return *bi.Ancestors[idx].EndNodeID
}

func getShardID(shardID *int) (int, error) {
	if shardID == nil {
		return 0, fmt.Errorf("shardID is not set for persistence operation")
	}
	return *shardID, nil
}
