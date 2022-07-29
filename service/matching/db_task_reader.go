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
	"sync"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/util"
	"golang.org/x/exp/maps"
)

const (
	dbTaskReaderPageSize = 100
)

// temporarily disable mock gen until mock gen support go generics
// //go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination db_task_reader_mock.go

type (
	dbTaskReader interface {
		taskIterator(ctx context.Context, maxTaskID int64) collection.Iterator[*persistencespb.AllocatedTaskInfo]
		ackTask(taskID int64)
		moveAckedTaskID() int64
	}

	dbTaskReaderImpl struct {
		taskQueueKey persistence.TaskQueueKey
		store        persistence.TaskManager
		logger       log.Logger

		sync.Mutex
		tasks        map[int64]bool // task ID -> true: acked or false not acked
		ackedTaskID  int64          // acked task ID
		loadedTaskID int64          // loaded into memory task ID
	}
)

func newDBTaskReader(
	taskQueueKey persistence.TaskQueueKey,
	store persistence.TaskManager,
	ackedTaskID int64,
	logger log.Logger,
) *dbTaskReaderImpl {
	return &dbTaskReaderImpl{
		taskQueueKey: taskQueueKey,
		store:        store,
		logger:       logger,

		tasks:        make(map[int64]bool),
		ackedTaskID:  ackedTaskID,
		loadedTaskID: ackedTaskID,
	}
}

func (t *dbTaskReaderImpl) taskIterator(
	ctx context.Context,
	maxTaskID int64,
) collection.Iterator[*persistencespb.AllocatedTaskInfo] {
	return collection.NewPagingIterator[*persistencespb.AllocatedTaskInfo](
		t.getPaginationFn(ctx, maxTaskID),
	)
}

func (t *dbTaskReaderImpl) ackTask(taskID int64) {
	t.Lock()
	defer t.Unlock()

	_, ok := t.tasks[taskID]
	if !ok {
		// trying to ack a task ID which is not present in tracking map
		return
	}
	t.tasks[taskID] = true
}

// moveAckedTaskID tries to advance the acked task ID
// e.g. assuming task ID & whether the task is completed
//  10 -> true
//  12 -> true
//  15 -> false
// the acked task ID can be set to 12, meaning task with ID <= 12 are finished
func (t *dbTaskReaderImpl) moveAckedTaskID() int64 {
	t.Lock()
	defer t.Unlock()

	taskIDs := maps.Keys(t.tasks)
	util.SortSlice(taskIDs)

	for _, taskID := range taskIDs {
		if !t.tasks[taskID] {
			break
		}
		t.ackedTaskID = taskID
		delete(t.tasks, taskID)
	}
	return t.ackedTaskID
}

func (t *dbTaskReaderImpl) getPaginationFn(
	ctx context.Context,
	maxTaskID int64,
) collection.PaginationFn[*persistencespb.AllocatedTaskInfo] {
	t.Lock()
	defer t.Unlock()
	minTaskID := t.loadedTaskID

	return func(paginationToken []byte) ([]*persistencespb.AllocatedTaskInfo, []byte, error) {
		response, err := t.store.GetTasks(ctx, &persistence.GetTasksRequest{
			NamespaceID:        t.taskQueueKey.NamespaceID,
			TaskQueue:          t.taskQueueKey.TaskQueueName,
			TaskType:           t.taskQueueKey.TaskQueueType,
			InclusiveMinTaskID: minTaskID + 1,
			ExclusiveMaxTaskID: maxTaskID + 1,
			PageSize:           dbTaskReaderPageSize,
			NextPageToken:      paginationToken,
		})
		if err != nil {
			return nil, nil, err
		}
		paginateItems := response.Tasks
		token := response.NextPageToken

		t.Lock()
		defer t.Unlock()
		for _, task := range response.Tasks {
			t.loadedTaskID = task.GetTaskId()
			t.tasks[task.GetTaskId()] = false
		}
		// special handling max task ID
		// if there is a task with max task ID
		//  then t.tasks with maxTaskID is set
		// if there is no task with max task ID
		//  then we simply set t.tasks[maxTaskID] = true
		//  indicating that maxTaskID is already finished
		//  this will greatly simplify the acked task ID logic
		if len(token) == 0 {
			t.loadedTaskID = maxTaskID
			if _, ok := t.tasks[maxTaskID]; !ok {
				t.tasks[maxTaskID] = true
			}
		}
		return paginateItems, token, nil
	}
}
