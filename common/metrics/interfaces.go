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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination interfaces_mock.go

package metrics

import (
	"time"

	"github.com/uber-go/tally/v4"
)

type (
	// Stopwatch is an interface tracking of elapsed time, use the
	// Stop() method to report time elapsed since its created back to the
	// timer or histogram.
	// Deprecated
	Stopwatch interface {
		// Stop records time elapsed from time of creation.
		Stop()
		// Subtract adds value to subtract from recorded duration.
		Subtract(d time.Duration)
	}

	// Client is the interface used to report metrics tally.
	// Deprecated
	Client interface {
		// IncCounter increments a counter metric
		IncCounter(scope int, counter int)
		// AddCounter adds delta to the counter metric
		AddCounter(scope int, counter int, delta int64)
		// StartTimer starts a timer for the given
		// metric name. Time will be recorded when stopwatch is stopped.
		StartTimer(scope int, timer int) Stopwatch
		// RecordTimer starts a timer for the given
		// metric name
		RecordTimer(scope int, timer int, d time.Duration)
		// RecordDistribution records and emits a distribution (wrapper on top of timer) for the given
		// metric name
		RecordDistribution(scope int, timer int, d int)
		// UpdateGauge reports Gauge type absolute value metric
		UpdateGauge(scope int, gauge int, value float64)
		// Scope returns an internal scope that can be used to add additional
		// information to metrics
		Scope(scope int, tags ...Tag) Scope
	}

	// Scope is an interface for metric.
	// Deprecated
	Scope interface {
		// IncCounter increments a counter metric
		IncCounter(counter int)
		// AddCounter adds delta to the counter metric
		AddCounter(counter int, delta int64)
		// StartTimer starts a timer for the given metric name.
		// Time will be recorded when stopwatch is stopped.
		StartTimer(timer int) Stopwatch
		// RecordTimer records a timer for the given metric name
		RecordTimer(timer int, d time.Duration)
		// RecordDistribution records a distribution (wrapper on top of timer) for the given
		// metric name
		RecordDistribution(id int, d int)
		// UpdateGauge reports Gauge type absolute value metric
		UpdateGauge(gauge int, value float64)
		// Tagged returns an internal scope that can be used to add additional
		// information to metrics
		Tagged(tags ...Tag) Scope
	}
)

var sanitizer = tally.NewSanitizer(tally.SanitizeOptions{
	NameCharacters:       tally.ValidCharacters{Ranges: tally.AlphanumericRange, Characters: tally.UnderscoreCharacters},
	KeyCharacters:        tally.ValidCharacters{Ranges: tally.AlphanumericRange, Characters: tally.UnderscoreCharacters},
	ValueCharacters:      tally.ValidCharacters{Ranges: tally.AlphanumericRange, Characters: tally.UnderscoreCharacters},
	ReplacementCharacter: '_',
})
