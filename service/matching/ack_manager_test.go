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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
)

func TestAckManager_AddingTasksIncreasesBacklogCounter(t *testing.T) {
	t.Parallel()
	mgr := newAckManager(log.NewTestLogger())
	mgr.addTask(1)
	require.Equal(t, mgr.getBacklogCountHint(), int64(1))
	mgr.addTask(12)
	require.Equal(t, mgr.getBacklogCountHint(), int64(2))
}

func TestAckManager_CompleteTaskMovesAckLevelUpToGap(t *testing.T) {
	t.Parallel()
	mgr := newAckManager(log.NewTestLogger())
	mgr.addTask(1)
	require.Equal(t, int64(-1), mgr.getAckLevel(), "should only move ack level on completion")
	require.Equal(t, int64(1), mgr.completeTask(1), "should move ack level on completion")

	mgr.addTask(2)
	mgr.addTask(3)
	mgr.addTask(12)

	require.Equal(t, int64(1), mgr.completeTask(3), "task 2 is not complete, we should not move ack level")
	require.Equal(t, int64(3), mgr.completeTask(2), "both tasks 2 and 3 are complete")
}

func BenchmarkAckManager_AddTask(b *testing.B) {
	tasks := make([]int, 1000)
	for i := 0; i < len(tasks); i++ {
		tasks[i] = i
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Add 1000 tasks in order and complete them in a random order.
		// This will cause our ack level to jump as we complete them
		b.StopTimer()
		mgr := newAckManager(log.NewTestLogger())
		rand.Shuffle(len(tasks), func(i, j int) {
			tasks[i], tasks[j] = tasks[j], tasks[i]
		})
		b.StartTimer()
		for i := 0; i < len(tasks); i++ {
			tasks[i] = i
			mgr.addTask(int64(i))
		}
	}
}

func BenchmarkAckManager_CompleteTask(b *testing.B) {
	tasks := make([]int, 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Add 1000 tasks in order and complete them in a random order.
		// This will cause our ack level to jump as we complete them
		b.StopTimer()
		mgr := newAckManager(log.NewTestLogger())
		for i := 0; i < len(tasks); i++ {
			tasks[i] = i
			mgr.addTask(int64(i))
		}
		rand.Shuffle(len(tasks), func(i, j int) {
			tasks[i], tasks[j] = tasks[j], tasks[i]
		})
		b.StartTimer()

		for i := 0; i < len(tasks); i++ {
			mgr.completeTask(int64(i))
		}
	}
}
