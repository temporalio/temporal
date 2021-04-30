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

package clock

import (
	"sync/atomic"
	"time"

	// clockwork is not currently used but it is useful to have the option to use this in testing code
	// this comment is needed to stop lint from complaining about this _ import
	_ "github.com/jonboulle/clockwork"
)

type (
	// TimeSource is an interface for any
	// entity that provides the current
	// time. Its primarily used to mock
	// out timesources in unit test
	TimeSource interface {
		Now() time.Time
	}
	// RealTimeSource serves real wall-clock time
	RealTimeSource struct{}

	// EventTimeSource serves fake controlled time
	EventTimeSource struct {
		now int64
	}
)

// NewRealTimeSource returns a time source that servers
// real wall clock time
func NewRealTimeSource() *RealTimeSource {
	return &RealTimeSource{}
}

// Now return the real current time
func (ts *RealTimeSource) Now() time.Time {
	return time.Now().UTC()
}

// NewEventTimeSource returns a time source that servers
// fake controlled time
func NewEventTimeSource() *EventTimeSource {
	return &EventTimeSource{}
}

// Now return the fake current time
func (ts *EventTimeSource) Now() time.Time {
	return time.Unix(0, atomic.LoadInt64(&ts.now)).UTC()
}

// Update update the fake current time
func (ts *EventTimeSource) Update(now time.Time) *EventTimeSource {
	atomic.StoreInt64(&ts.now, now.UnixNano())
	return ts
}
