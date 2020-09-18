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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	h "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/common"
)

type (
	queueProcessorUtilSuite struct {
		suite.Suite
		*require.Assertions
	}
)

func TestQueueProcessorUtilSuite(t *testing.T) {
	s := new(queueProcessorUtilSuite)
	suite.Run(t, s)
}

func (s *queueProcessorUtilSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *queueProcessorUtilSuite) TestConvertToPersistenceTransferProcessingQueueStates() {
	levels := []int{0, 1}
	ackLevels := []int64{123, 456}
	maxLevels := []int64{456, 789}
	domainFilters := []DomainFilter{
		NewDomainFilter(nil, true),
		NewDomainFilter(map[string]struct{}{"domain 1": {}, "domain 2": {}}, false),
	}
	states := []ProcessingQueueState{}
	for i := 0; i != len(levels); i++ {
		states = append(states, NewProcessingQueueState(
			levels[i],
			newTransferTaskKey(ackLevels[i]),
			newTransferTaskKey(maxLevels[i]),
			domainFilters[i],
		))
	}

	pStates := convertToPersistenceTransferProcessingQueueStates(states)
	s.Equal(len(states), len(pStates))
	for i := 0; i != len(states); i++ {
		s.assertProcessingQueueStateEqual(states[i], pStates[i])
	}
}

func (s *queueProcessorUtilSuite) TestConvertFromPersistenceTransferProcessingQueueStates() {
	levels := []int32{0, 1}
	ackLevels := []int64{123, 456}
	maxLevels := []int64{456, 789}
	domainFilters := []*h.DomainFilter{
		{
			DomainIDs:    nil,
			ReverseMatch: common.BoolPtr(true),
		},
		{
			DomainIDs:    []string{"domain 1", "domain 2"},
			ReverseMatch: common.BoolPtr(false),
		},
	}
	pStates := []*h.ProcessingQueueState{}
	for i := 0; i != len(levels); i++ {
		pStates = append(pStates, &h.ProcessingQueueState{
			Level:        common.Int32Ptr(levels[i]),
			AckLevel:     common.Int64Ptr(ackLevels[i]),
			MaxLevel:     common.Int64Ptr(maxLevels[i]),
			DomainFilter: domainFilters[i],
		})
	}

	states := convertFromPersistenceTransferProcessingQueueStates(pStates)
	s.Equal(len(pStates), len(states))
	for i := 0; i != len(pStates); i++ {
		s.assertProcessingQueueStateEqual(states[i], pStates[i])
	}
}

func (s *queueProcessorUtilSuite) TestConvertToPersistenceTimerProcessingQueueStates() {
	levels := []int{0, 1}
	ackLevels := []time.Time{time.Now(), time.Now().Add(-time.Minute)}
	maxLevels := []time.Time{time.Now().Add(time.Hour), time.Now().Add(time.Nanosecond)}
	domainFilters := []DomainFilter{
		NewDomainFilter(nil, true),
		NewDomainFilter(map[string]struct{}{"domain 1": {}, "domain 2": {}}, false),
	}
	states := []ProcessingQueueState{}
	for i := 0; i != len(levels); i++ {
		states = append(states, NewProcessingQueueState(
			levels[i],
			newTimerTaskKey(ackLevels[i], 0),
			newTimerTaskKey(maxLevels[i], 0),
			domainFilters[i],
		))
	}

	pStates := convertToPersistenceTimerProcessingQueueStates(states)
	s.Equal(len(states), len(pStates))
	for i := 0; i != len(states); i++ {
		s.assertProcessingQueueStateEqual(states[i], pStates[i])
	}
}

func (s *queueProcessorUtilSuite) TestConvertFromPersistenceTimerProcessingQueueStates() {
	levels := []int32{0, 1}
	ackLevels := []time.Time{time.Now(), time.Now().Add(-time.Minute)}
	maxLevels := []time.Time{time.Now().Add(time.Hour), time.Now().Add(time.Nanosecond)}
	domainFilters := []*h.DomainFilter{
		{
			DomainIDs:    nil,
			ReverseMatch: common.BoolPtr(true),
		},
		{
			DomainIDs:    []string{"domain 1", "domain 2"},
			ReverseMatch: common.BoolPtr(false),
		},
	}
	pStates := []*h.ProcessingQueueState{}
	for i := 0; i != len(levels); i++ {
		pStates = append(pStates, &h.ProcessingQueueState{
			Level:        common.Int32Ptr(levels[i]),
			AckLevel:     common.Int64Ptr(ackLevels[i].UnixNano()),
			MaxLevel:     common.Int64Ptr(maxLevels[i].UnixNano()),
			DomainFilter: domainFilters[i],
		})
	}

	states := convertFromPersistenceTimerProcessingQueueStates(pStates)
	s.Equal(len(pStates), len(states))
	for i := 0; i != len(pStates); i++ {
		s.assertProcessingQueueStateEqual(states[i], pStates[i])
	}
}

func (s *queueProcessorUtilSuite) assertProcessingQueueStateEqual(
	state ProcessingQueueState,
	pState *h.ProcessingQueueState,
) {
	var ackLevel, maxLevel int64
	switch taskKey := state.AckLevel().(type) {
	case transferTaskKey:
		ackLevel = taskKey.taskID
	case timerTaskKey:
		ackLevel = taskKey.visibilityTimestamp.UnixNano()
		s.Zero(taskKey.taskID)
	}
	switch taskKey := state.MaxLevel().(type) {
	case transferTaskKey:
		maxLevel = taskKey.taskID
	case timerTaskKey:
		maxLevel = taskKey.visibilityTimestamp.UnixNano()
		s.Zero(taskKey.taskID)
	}
	s.Equal(state.Level(), int(pState.GetLevel()))
	s.Equal(ackLevel, pState.GetAckLevel())
	s.Equal(maxLevel, pState.GetMaxLevel())
	s.assertDomainFilterEqual(state.DomainFilter(), pState.GetDomainFilter())
}

func (s *queueProcessorUtilSuite) TestValidateProcessingQueueStates_Fail() {
	ackLevels := []int64{123, 456}
	pStates := []*h.ProcessingQueueState{}
	for i := 0; i != len(ackLevels); i++ {
		pStates = append(pStates, &h.ProcessingQueueState{
			Level:        nil,
			AckLevel:     common.Int64Ptr(ackLevels[i]),
			MaxLevel:     nil,
			DomainFilter: nil,
		})
	}

	isValid := validateProcessingQueueStates(pStates, 789)
	s.False(isValid)
}

func (s *queueProcessorUtilSuite) TestValidateProcessingQueueStates_Success() {
	ackLevels := []time.Time{time.Now(), time.Now().Add(-time.Minute)}
	pStates := []*h.ProcessingQueueState{}
	for i := 0; i != len(ackLevels); i++ {
		pStates = append(pStates, &h.ProcessingQueueState{
			Level:        nil,
			AckLevel:     common.Int64Ptr(ackLevels[i].UnixNano()),
			MaxLevel:     nil,
			DomainFilter: nil,
		})
	}

	isValid := validateProcessingQueueStates(pStates, ackLevels[1])
	s.True(isValid)
}

func (s *queueProcessorUtilSuite) assertDomainFilterEqual(
	domainFilter DomainFilter,
	pDomainFilter *h.DomainFilter,
) {
	s.Equal(domainFilter.ReverseMatch, pDomainFilter.GetReverseMatch())
	s.Equal(len(domainFilter.DomainIDs), len(pDomainFilter.GetDomainIDs()))
	for _, domainID := range pDomainFilter.GetDomainIDs() {
		_, ok := domainFilter.DomainIDs[domainID]
		s.True(ok)
	}
}
