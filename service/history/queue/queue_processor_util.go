// Copyright (c) 2017-2020 Uber Technologies Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package queue

import (
	"time"

	h "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/common"
)

func convertToPersistenceTransferProcessingQueueStates(
	states []ProcessingQueueState,
) []*h.ProcessingQueueState {
	pStates := make([]*h.ProcessingQueueState, 0, len(states))
	for _, state := range states {
		pStates = append(pStates, &h.ProcessingQueueState{
			Level:        common.Int32Ptr(int32(state.Level())),
			AckLevel:     common.Int64Ptr(state.AckLevel().(transferTaskKey).taskID),
			MaxLevel:     common.Int64Ptr(state.MaxLevel().(transferTaskKey).taskID),
			DomainFilter: convertToPersistenceDomainFilter(state.DomainFilter()),
		})
	}

	return pStates
}

func convertFromPersistenceTransferProcessingQueueStates(
	pStates []*h.ProcessingQueueState,
) []ProcessingQueueState {
	states := make([]ProcessingQueueState, 0, len(pStates))
	for _, pState := range pStates {
		states = append(states, NewProcessingQueueState(
			int(pState.GetLevel()),
			newTransferTaskKey(pState.GetAckLevel()),
			newTransferTaskKey(pState.GetMaxLevel()),
			convertFromPersistenceDomainFilter(pState.DomainFilter),
		))
	}

	return states
}

func convertToPersistenceTimerProcessingQueueStates(
	states []ProcessingQueueState,
) []*h.ProcessingQueueState {
	pStates := make([]*h.ProcessingQueueState, 0, len(states))
	for _, state := range states {
		pStates = append(pStates, &h.ProcessingQueueState{
			Level:        common.Int32Ptr(int32(state.Level())),
			AckLevel:     common.Int64Ptr(state.AckLevel().(timerTaskKey).visibilityTimestamp.UnixNano()),
			MaxLevel:     common.Int64Ptr(state.MaxLevel().(timerTaskKey).visibilityTimestamp.UnixNano()),
			DomainFilter: convertToPersistenceDomainFilter(state.DomainFilter()),
		})
	}

	return pStates
}

func convertFromPersistenceTimerProcessingQueueStates(
	pStates []*h.ProcessingQueueState,
) []ProcessingQueueState {
	states := make([]ProcessingQueueState, 0, len(pStates))
	for _, pState := range pStates {
		states = append(states, NewProcessingQueueState(
			int(pState.GetLevel()),
			newTimerTaskKey(time.Unix(0, pState.GetAckLevel()), 0),
			newTimerTaskKey(time.Unix(0, pState.GetMaxLevel()), 0),
			convertFromPersistenceDomainFilter(pState.DomainFilter),
		))
	}

	return states
}

func convertToPersistenceDomainFilter(
	domainFilter DomainFilter,
) *h.DomainFilter {
	domainIDs := make([]string, 0, len(domainFilter.DomainIDs))
	for domainID := range domainFilter.DomainIDs {
		domainIDs = append(domainIDs, domainID)
	}

	return &h.DomainFilter{
		DomainIDs:    domainIDs,
		ReverseMatch: common.BoolPtr(domainFilter.ReverseMatch),
	}
}

func convertFromPersistenceDomainFilter(
	domainFilter *h.DomainFilter,
) DomainFilter {
	domainIDs := make(map[string]struct{})
	for _, domainID := range domainFilter.DomainIDs {
		domainIDs[domainID] = struct{}{}
	}

	return NewDomainFilter(domainIDs, domainFilter.GetReverseMatch())
}

func validateProcessingQueueStates(
	pStates []*h.ProcessingQueueState,
	ackLevel interface{},
) bool {
	if len(pStates) == 0 {
		return false
	}

	minAckLevel := pStates[0].GetAckLevel()
	for _, pState := range pStates {
		minAckLevel = common.MinInt64(minAckLevel, pState.GetAckLevel())
	}

	switch ackLevel := ackLevel.(type) {
	case int64:
		return minAckLevel == ackLevel
	case time.Time:
		return minAckLevel == ackLevel.UnixNano()
	default:
		return false
	}
}
