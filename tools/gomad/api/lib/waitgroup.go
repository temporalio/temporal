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

package lib

import (
	"fmt"

	SIMLANG "go.temporal.io/server/tools/gomad/api/lang"
	SIM "go.temporal.io/server/tools/gomad/runtime"
	"go.temporal.io/server/tools/gomad/util/verify"
)

type WaitGroup struct {
	todo int
	done chan struct{}
}

func (wg *WaitGroup) Add(delta int) {
	SIM.Dbg("🚥", fmt.Sprintf("add %+d", delta))

	if wg.todo == 0 {
		wg.done = make(chan struct{})
	}

	wg.todo += delta
	verify.T(wg.todo >= 0, "negative counter in WaitGroup")

	if wg.todo == 0 {
		SIMLANG.ChanClose(wg.done) // unblocks all Wait calls
	}
}

func (wg *WaitGroup) Done() {
	wg.Add(-1)
}

func (wg *WaitGroup) Wait() {
	if wg.done != nil {
		_ = SIMLANG.ChanRcv(wg.done) // will not block if already closed
	}
	SIM.Dbg("🚥", "wait done")
}

func (wg *WaitGroup) Counter() int {
	return wg.todo
}

func (wg *WaitGroup) Go(fn func()) {
	wg.Add(1)
	SIMLANG.Go(func() {
		defer wg.Done()
		fn()
	})
}
