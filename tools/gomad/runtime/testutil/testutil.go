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

package testutil

import (
	"time"

	SIM "go.temporal.io/server/tools/gomad/runtime"
)

const TestRuns = 50

func StressRun(
	f func(int64),
	opts ...SIM.InitOption,
) {
	for i := int64(0); i < TestRuns; i++ {
		SingleRun(f, i, opts...)
	}
}

func SingleRun(
	f func(int64),
	seed int64,
	opts ...SIM.InitOption,
) {
	done := make(chan struct{})

	go func() {
		defer close(done)

		// start new simulator
		allOpts := []SIM.InitOption{SIM.Seed(seed), SIM.DebugMode, SIM.StopFirstIfRunning}
		allOpts = append(allOpts, opts...)
		SIM.Start(allOpts...)

		// start test case
		f(seed)

		// ensure simulator completed
		SIM.Join()
	}()

	// check for timeout
	select {
	case <-done:
	case <-time.After(1 * time.Second * SIM.TimeoutMultiplier):
		panic("test timed out")
	}
}
