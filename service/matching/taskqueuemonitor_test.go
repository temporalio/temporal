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

package matching

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
)

type fakeTQMAccess struct {
	tqmonAccess
	getTaskQueuesFun   func(int) []taskQueueManager
	unloadTaskQueueFun func(*taskQueueID)
}

func (a fakeTQMAccess) getTaskQueues(i int) []taskQueueManager {
	return a.getTaskQueuesFun(i)
}

func (a fakeTQMAccess) unloadTaskQueue(id *taskQueueID) {
	a.unloadTaskQueueFun(id)
}

type fakeTQM struct {
	taskQueueManager
	livenessFun func() (bool, *taskQueueID)
}

func (m fakeTQM) Liveness() (bool, *taskQueueID) {
	return m.livenessFun()
}

func mustTQID(t *testing.T, n string) *taskQueueID {
	t.Helper()
	id, err := newTaskQueueID(t.Name(), n, enumspb.TASK_QUEUE_TYPE_UNSPECIFIED)
	require.NoError(t, err)
	return id
}

func TestUsurp(t *testing.T) {
	want := mustTQID(t, "q0")
	var got *taskQueueID
	a := fakeTQMAccess{
		unloadTaskQueueFun: func(id *taskQueueID) { got = id },
	}
	m := newTaskQueueMonitor()
	go m.Run(a, defaultTestConfig())
	m.Signal(tqmonUsurped{id: want})
	m.Stop(context.TODO())
	require.Equal(t, want, got)
}

func TestGC(t *testing.T) {
	makeTQM := func(alive bool, id *taskQueueID) taskQueueManager {
		return fakeTQM{
			livenessFun: func() (bool, *taskQueueID) { return alive, id },
		}
	}
	wants := []*taskQueueID{mustTQID(t, "x1"), mustTQID(t, "x2")}
	unloads := 0
	dead, alive := false, true
	a := fakeTQMAccess{
		unloadTaskQueueFun: func(id *taskQueueID) {
			require.Contains(t, wants, id)
			unloads++
		},
		getTaskQueuesFun: func(int) []taskQueueManager {
			return []taskQueueManager{ //2 dead out of 5
				makeTQM(alive, nil),
				makeTQM(dead, wants[0]),
				makeTQM(alive, nil),
				makeTQM(dead, wants[1]),
				makeTQM(alive, nil),
			}
		},
	}
	m := newTaskQueueMonitor()
	go m.Run(a, defaultTestConfig())
	m.Signal(tqmonGC{})
	m.Stop(context.TODO())
	require.Equal(t, 2, unloads, "Expected 2 of the 5 TQMs to be unloaded")
}

func TestUnsupportedSignalPanics(t *testing.T) {
	m := newTaskQueueMonitor()
	go func() {
		require.Panics(t, func() {
			m.Run(fakeTQMAccess{}, defaultTestConfig())
		})
	}()
	m.Signal("string is not a supported signal type")
}
