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

package queuestest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
)

var errRetryable = errors.New("retryable error")

// TestExecutable is a library test function which tests the behavior of queues.ExecutableDLQ against a real database.
func TestExecutable(t *testing.T, tqm persistence.HistoryTaskQueueManager) {
	for _, tc := range []struct {
		name      string
		err       error
		shouldDLQ bool
	}{
		{
			name:      "Deserialization Error",
			err:       new(serialization.DeserializationError),
			shouldDLQ: true,
		},
		{
			name:      "Unknown Encoding Type Error",
			err:       new(serialization.UnknownEncodingTypeError),
			shouldDLQ: true,
		},
		{
			name:      "Retryable Error",
			err:       errRetryable,
			shouldDLQ: false,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()

			task := &tasks.WorkflowTask{}
			clusterName := "test-cluster-" + t.Name()

			var executable queues.Executable = NewFakeExecutable(task, tc.err)
			ts := clock.NewEventTimeSource()
			// MySQL connector compare the deadline in the ctx with time.Now().
			// Updating ts to avoid deadline exceeded error.
			ts.Update(time.Now())
			executable = queues.NewExecutableDLQ(executable, tqm, ts, clusterName)
			err := executable.Execute()
			assert.ErrorContains(t, err, tc.err.Error())
			if tc.shouldDLQ {
				assert.ErrorIs(t, err, queues.ErrTerminalTaskFailure)
				err = executable.Execute()
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, tc.err)
			}
			response, err := tqm.ReadTasks(ctx, &persistence.ReadTasksRequest{
				QueueKey: persistence.QueueKey{
					QueueType:     persistence.QueueTypeHistoryDLQ,
					Category:      tasks.CategoryTransfer,
					SourceCluster: clusterName,
					TargetCluster: clusterName,
				},
				PageSize:      2, // make it 2 even though we want just 1 to verify that there's no extra tasks
				NextPageToken: nil,
			})
			if tc.shouldDLQ {
				require.NoError(t, err)
				if assert.Len(t, response.Tasks, 1) {
					assert.Equal(t, task, response.Tasks[0].Task)
				}
			} else {
				assert.ErrorAs(t, err, new(*serviceerror.NotFound))
			}
		})
	}
}
