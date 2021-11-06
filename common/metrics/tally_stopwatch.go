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

package metrics

import (
	"time"

	"github.com/uber-go/tally/v4"
)

// TallyStopwatch is a helper for simpler tracking of elapsed time, use the
// Stop() method to report time elapsed since its created back to the
// timer or histogram.
type TallyStopwatch struct {
	start       time.Time
	toSubstract time.Duration
	timers      []tally.Timer
}

// NewStopwatch creates a new immutable stopwatch for recording the start
// time to a stopwatch reporter.
func NewStopwatch(timers ...tally.Timer) Stopwatch {
	return &TallyStopwatch{time.Now().UTC(), 0, timers}
}

// NewTestStopwatch returns a new test stopwatch
func NewTestStopwatch() Stopwatch {
	return &TallyStopwatch{time.Now().UTC(), 0, []tally.Timer{}}
}

// Stop reports time elapsed since the stopwatch start to the recorder.
func (sw *TallyStopwatch) Stop() {
	d := time.Since(sw.start)
	d = d - sw.toSubstract
	for _, timer := range sw.timers {
		timer.Record(d)
	}
}

// Substract records time to substract from resulting value.
func (sw *TallyStopwatch) Subtract(toSubstract time.Duration) {
	sw.toSubstract += toSubstract
}
