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

	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/require"
)

func TestAckManager_AddingTasksIncreasesBacklogCounter(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()
	backlogMgr := newBacklogMgr(controller)

	t.Parallel()
	backlogMgr.taskAckManager.addTask(1)
	require.Equal(t, backlogMgr.taskAckManager.getBacklogCountHint(), int64(1))
	backlogMgr.taskAckManager.addTask(12)
	require.Equal(t, backlogMgr.taskAckManager.getBacklogCountHint(), int64(2))
}

func TestAckManager_CompleteTaskMovesAckLevelUpToGap(t *testing.T) {
	t.Parallel()
	controller := gomock.NewController(t)
	defer controller.Finish()
	backlogMgr := newBacklogMgr(controller)

	backlogMgr.taskAckManager.addTask(1)

	// Incrementing the backlog as otherwise we would get an error that it is under-counting;
	// this happens since we decrease the counter on completion of a task
	backlogMgr.db.updateApproximateBacklogCount(1)
	require.Equal(t, int64(-1), backlogMgr.taskAckManager.getAckLevel(), "should only move ack level on completion")
	require.Equal(t, int64(1), backlogMgr.taskAckManager.completeTask(1), "should move ack level on completion")

	backlogMgr.taskAckManager.addTask(2)
	backlogMgr.taskAckManager.addTask(3)
	backlogMgr.taskAckManager.addTask(12)
	backlogMgr.db.updateApproximateBacklogCount(3)

	require.Equal(t, int64(1), backlogMgr.taskAckManager.completeTask(3), "task 2 is not complete, we should not move ack level")
	require.Equal(t, int64(3), backlogMgr.taskAckManager.completeTask(2), "both tasks 2 and 3 are complete")
}

func BenchmarkAckManager_AddTask(b *testing.B) {
	controller := gomock.NewController(b)
	defer controller.Finish()

	tasks := make([]int, 1000)
	for i := 0; i < len(tasks); i++ {
		tasks[i] = i
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Add 1000 tasks in order and complete them in a random order.
		// This will cause our ack level to jump as we complete them
		b.StopTimer()
		backlogMgr := newBacklogMgr(controller)
		rand.Shuffle(len(tasks), func(i, j int) {
			tasks[i], tasks[j] = tasks[j], tasks[i]
		})
		b.StartTimer()
		for i := 0; i < len(tasks); i++ {
			tasks[i] = i
			backlogMgr.taskAckManager.addTask(int64(i))
		}
	}
}

func BenchmarkAckManager_CompleteTask(b *testing.B) {
	controller := gomock.NewController(b)
	defer controller.Finish()

	tasks := make([]int, 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Add 1000 tasks in order and complete them in a random order.
		// This will cause our ack level to jump as we complete them
		b.StopTimer()
		backlogMgr := newBacklogMgr(controller)
		for i := 0; i < len(tasks); i++ {
			tasks[i] = i
			backlogMgr.taskAckManager.addTask(int64(i))
		}
		rand.Shuffle(len(tasks), func(i, j int) {
			tasks[i], tasks[j] = tasks[j], tasks[i]
		})
		b.StartTimer()

		for i := 0; i < len(tasks); i++ {
			backlogMgr.taskAckManager.completeTask(int64(i))
		}
	}
}
