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

package queues

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
)

func TestIsTaskAcked(t *testing.T) {
	scopes := NewRandomScopes(5)
	exclusiveReaderHighWatermark := scopes[len(scopes)-1].Range.ExclusiveMax.Next()
	persistenceQueueState := ToPersistenceQueueState(&queueState{
		readerScopes: map[int64][]Scope{
			DefaultReaderId: scopes,
		},
		exclusiveReaderHighWatermark: exclusiveReaderHighWatermark,
	})

	workflowKey := definition.NewWorkflowKey(tests.NamespaceID.String(), tests.WorkflowID, tests.RunID)

	testKey := exclusiveReaderHighWatermark
	testTask := tasks.NewFakeTask(
		workflowKey,
		tasks.CategoryTimer,
		testKey.FireTime,
	)
	testTask.SetTaskID(testKey.TaskID)
	assert.False(t, IsTaskAcked(testTask, persistenceQueueState))

	testKey = NewRandomKeyInRange(scopes[rand.Intn(len(scopes))].Range)
	testTask.SetVisibilityTime(testKey.FireTime)
	testTask.SetTaskID(testKey.TaskID)
	assert.False(t, IsTaskAcked(testTask, persistenceQueueState))

	testKey = NewRandomKeyInRange(NewRange(
		scopes[2].Range.ExclusiveMax,
		scopes[3].Range.InclusiveMin,
	))
	testTask.SetVisibilityTime(testKey.FireTime)
	testTask.SetTaskID(testKey.TaskID)
	assert.True(t, IsTaskAcked(testTask, persistenceQueueState))

	testKey = scopes[0].Range.InclusiveMin.Prev()
	testTask.SetVisibilityTime(testKey.FireTime)
	testTask.SetTaskID(testKey.TaskID)
	assert.True(t, IsTaskAcked(testTask, persistenceQueueState))
}
