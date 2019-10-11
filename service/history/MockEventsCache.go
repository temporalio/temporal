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

package history

import (
	"github.com/stretchr/testify/mock"
	"github.com/uber/cadence/.gen/go/shared"
)

// MockEventsCache is used as mock implementation for EventsCache
type MockEventsCache struct {
	mock.Mock
}

var _ eventsCache = (*MockEventsCache)(nil)

// getEvent is mock implementation for getEvent of EventsCache
func (_m *MockEventsCache) getEvent(domainID, workflowID, runID string, firstEventID, eventID int64,
	branchToken []byte) (*shared.HistoryEvent, error) {
	ret := _m.Called(domainID, workflowID, runID, firstEventID, eventID, branchToken)

	var r0 *shared.HistoryEvent
	if rf, ok := ret.Get(0).(func(string, string, string, int64, int64, []byte) *shared.HistoryEvent); ok {
		r0 = rf(domainID, workflowID, runID, firstEventID, eventID, branchToken)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*shared.HistoryEvent)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, string, string, int64, []byte) error); ok {
		r1 = rf(domainID, workflowID, runID, eventID, branchToken)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// putEvent is mock implementation for putEvent of EventsCache
func (_m *MockEventsCache) putEvent(domainID, workflowID, runID string, eventID int64, event *shared.HistoryEvent) {
	_m.Called(domainID, workflowID, runID, eventID, event)
}

// deleteEvent is mock implementation for deleteEvent of EventsCache
func (_m *MockEventsCache) deleteEvent(domainID, workflowID, runID string, eventID int64) {
	_m.Called(domainID, workflowID, runID, eventID)
}
