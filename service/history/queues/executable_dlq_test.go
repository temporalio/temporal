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

package queues_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/queues/queuestest"
	"go.temporal.io/server/service/history/tasks"
)

var (
	// testTask is an arbitrary task that we use to verify that the correct task is enqueued to the DLQ
	testTask tasks.Task = &tasks.WorkflowTask{}
)

func TestDLQExecutable_TerminalErrors(t *testing.T) {
	// Verify that if Executable.Execute returns a terminal error, the task is sent to the DLQ

	t.Parallel()

	for _, tc := range []struct {
		name string
		err  error
	}{
		{
			name: "Deserialization Error",
			err:  new(serialization.DeserializationError),
		},
		{
			name: "Unknown Encoding Type Error",
			err:  new(serialization.UnknownEncodingTypeError),
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dlq := &queuestest.FakeDLQ{}
			executable := newExecutable(tc.err)

			dlqExecutable := newExecutableDLQ(executable, dlq)
			err := dlqExecutable.Execute()
			assert.ErrorIs(t, err, queues.ErrTerminalTaskFailure)
			assert.ErrorContains(t, err, tc.err.Error())

			err = dlqExecutable.Execute()
			assert.Equal(t, 1, executable.GetCalls())
			assert.NoError(t, err)

			require.Len(t, dlq.Requests, 1)
			assert.Equal(t, &persistence.EnqueueTaskRequest{
				QueueType:     persistence.QueueTypeHistoryDLQ,
				SourceCluster: "test-cluster-name",
				TargetCluster: "test-cluster-name",
				Task:          testTask,
			}, dlq.Requests[0])
		})
	}
}

func TestDLQExecutable_NilError(t *testing.T) {
	// Verify that successful calls to Execute do not send the task to the DLQ

	t.Parallel()
	dlq := &queuestest.FakeDLQ{}
	executable := newExecutable(nil)
	dlqExecutable := newExecutableDLQ(executable, dlq)
	err := dlqExecutable.Execute()
	assert.NoError(t, err)
	assert.Empty(t, dlq.Requests, "Nil error should not be sent to DLQ")
}

func TestDLQExecutable_RandomErr(t *testing.T) {
	// Verify that non-terminal errors are not sent to the DLQ

	t.Parallel()

	originalErr := errors.New("some non-terminal error")
	dlq := &queuestest.FakeDLQ{}
	executable := newExecutable(originalErr)
	dlqExecutable := newExecutableDLQ(executable, dlq)
	for i := 0; i < 2; i++ {
		// simulate the client retrying the task
		err := dlqExecutable.Execute()
		assert.ErrorIs(t, err, originalErr)
	}
	assert.Empty(t, dlq.Requests, "Non-terminal error should not be sent to DLQ")
}

func TestDLQExecutable_DLQEnqueueErr(t *testing.T) {
	// Verify that if the DLQ returns an error, we return ErrSendTaskToDLQ

	t.Parallel()

	dlqErr := errors.New("error writing task to queue")
	originalErr := new(serialization.DeserializationError)
	dlq := &queuestest.FakeDLQ{EnqueueTaskErr: dlqErr}
	executable := newExecutable(originalErr)
	dlqExecutable := newExecutableDLQ(executable, dlq)

	err := dlqExecutable.Execute()
	assert.ErrorIs(t, err, queues.ErrTerminalTaskFailure, "First call to Execute for a terminal error should return"+
		" ErrTerminalTaskFailure")
	assert.ErrorContains(t, err, originalErr.Error())

	for i := 0; i < 2; i++ {
		err := dlqExecutable.Execute()
		assert.ErrorIs(t, err, queues.ErrSendTaskToDLQ)
		assert.ErrorContains(t, err, dlqErr.Error(), "Subsequent calls to Execute should return"+
			" ErrSendTaskToDLQ if the DLQ is still returning an error")
	}

	require.Len(t, dlq.Requests, 2, "The DLQ should have received two requests: one for each call"+
		" to Execute after the first")
	for _, r := range dlq.Requests {
		assert.Equal(t, &persistence.EnqueueTaskRequest{
			QueueType:     persistence.QueueTypeHistoryDLQ,
			SourceCluster: "test-cluster-name",
			TargetCluster: "test-cluster-name",
			Task:          testTask,
		}, r, "The queue should have received a request to enqueue the failing task")
	}
}

func TestDLQExecutable_DLQCreateErr(t *testing.T) {
	// Verify that if the DLQ returns an error, we return ErrSendTaskToDLQ

	t.Parallel()

	dlqErr := errors.New("error creating queue")
	originalErr := new(serialization.DeserializationError)
	dlq := &queuestest.FakeDLQ{
		CreateQueueErr: dlqErr,
	}
	executable := newExecutable(originalErr)
	dlqExecutable := newExecutableDLQ(executable, dlq)

	err := dlqExecutable.Execute()
	assert.ErrorIs(t, err, queues.ErrTerminalTaskFailure)
	assert.ErrorContains(t, err, originalErr.Error())

	err = dlqExecutable.Execute()
	assert.ErrorIs(t, err, queues.ErrCreateDLQ)
	assert.ErrorContains(t, err, dlqErr.Error())
}

func newExecutableDLQ(executable *queuestest.FakeExecutable, dlq queues.DLQ) *queues.ExecutableDLQ {
	return queues.NewExecutableDLQ(executable, dlq, clock.NewEventTimeSource(), "test-cluster-name")
}

func newExecutable(err error) *queuestest.FakeExecutable {
	return queuestest.NewFakeExecutable(testTask, err)
}
