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
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/service/history/tasks"
)

func IsTaskAcked(
	task tasks.Task,
	persistenceQueueState *persistencespb.QueueState,
) bool {
	queueState := FromPersistenceQueueState(persistenceQueueState)
	taskKey := task.GetKey()
	if taskKey.CompareTo(queueState.exclusiveReaderHighWatermark) >= 0 {
		return false
	}

	for _, scopes := range queueState.readerScopes {
		for _, scope := range scopes {
			if taskKey.CompareTo(scope.Range.InclusiveMin) < 0 {
				// scopes are ordered for each reader, so if task is less the current
				// range's min, it can not be contained by this or later scopes of this
				// reader.
				break
			}

			if scope.Contains(task) {
				return false
			}
		}
	}

	return true
}
