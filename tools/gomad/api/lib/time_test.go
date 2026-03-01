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

package lib_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	SIMLANG "go.temporal.io/server/tools/gomad/api/lang"
	SIMLIB "go.temporal.io/server/tools/gomad/api/lib"
	SIM "go.temporal.io/server/tools/gomad/runtime"
	"go.temporal.io/server/tools/gomad/runtime/testutil"
)

func TestSleep(t *testing.T) {
	testutil.StressRun(func(seed int64) {
		var res []int64

		sleep := func(d time.Duration) {
			before := SIMLIB.NowMs()
			SIMLIB.Sleep(d)
			elapsed := SIMLIB.NowMs() - before
			res = append(res, elapsed)
		}

		SIMLANG.Go(func() { sleep(time.Hour) })
		SIMLANG.Go(func() { sleep(time.Minute) })
		SIMLANG.Go(func() { sleep(time.Second) })

		SIM.Join()

		require.Equal(t, []int64{
			time.Second.Milliseconds(),
			time.Minute.Milliseconds(),
			time.Hour.Milliseconds()}, res)
	})
}
