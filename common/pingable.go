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

package common

import "time"

type (
	// Pingable is interface to check for liveness of a component, to detect deadlocks.
	// This call should not block.
	Pingable interface {
		GetPingChecks() []PingCheck
	}

	PingCheck struct {
		// Name of this component.
		Name string
		// The longest time that Ping can take. If it doesn't return in that much time, that's
		// considered a deadlock and the deadlock detector may take actions to recover, like
		// killing the process.
		Timeout time.Duration
		// Perform the check. The typical implementation will just be Lock() and then Unlock()
		// on a mutex, returning nil. Ping can also return more Pingables for sub-components
		// that will be checked independently. These should form a tree and not lead to cycles.
		Ping func() []Pingable

		// Metrics recording:
		// Timer id within DeadlockDetectorScope (or zero for no metrics)
		MetricsName string
	}
)
