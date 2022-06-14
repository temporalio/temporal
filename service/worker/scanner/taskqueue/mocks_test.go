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

package taskqueue

import (
	"encoding/binary"
	"sync"
	"time"

	"github.com/pborman/uuid"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
)

type (
	mockTaskTable struct {
		namespaceID string
		workflowID  string
		runID       string
		nextTaskID  int64
		tasks       []*persistencespb.AllocatedTaskInfo
	}
	mockTaskQueueTable struct {
		sync.Mutex
		info []*p.PersistedTaskQueueInfo
	}
)

func newMockTaskTable() *mockTaskTable {
	return &mockTaskTable{
		namespaceID: uuid.New(),
		workflowID:  uuid.New(),
		runID:       uuid.New(),
	}
}

func (tbl *mockTaskQueueTable) generate(name string, idle bool) {
	tq := p.PersistedTaskQueueInfo{
		Data: &persistencespb.TaskQueueInfo{
			NamespaceId:    uuid.New(),
			Name:           name,
			LastUpdateTime: timestamp.TimeNowPtrUtc(),
		},
		RangeID: 22,
	}
	if idle {
		tq.Data.LastUpdateTime = timestamp.TimePtr(time.Unix(1000, 1000).UTC())
	}
	tbl.info = append(tbl.info, &tq)
}

func (tbl *mockTaskQueueTable) list(token []byte, count int) ([]*p.PersistedTaskQueueInfo, []byte) {
	tbl.Lock()
	defer tbl.Unlock()
	if tbl.info == nil {
		return nil, nil
	}
	var off int
	if token != nil {
		off = int(binary.BigEndian.Uint32(token))
	}
	rem := len(tbl.info) - count + 1
	if rem < count {
		return tbl.info[off:], nil
	}
	token = make([]byte, 4)
	binary.BigEndian.PutUint32(token, uint32(off+count))
	return tbl.info[off : off+count], token
}

func (tbl *mockTaskQueueTable) delete(name string) {
	tbl.Lock()
	defer tbl.Unlock()
	var newInfo []*p.PersistedTaskQueueInfo
	for _, tl := range tbl.info {
		if tl.Data.Name != name {
			newInfo = append(newInfo, tl)
		}
	}
	tbl.info = newInfo
}

func (tbl *mockTaskQueueTable) get(name string) *p.PersistedTaskQueueInfo {
	tbl.Lock()
	defer tbl.Unlock()
	for _, tl := range tbl.info {
		if tl.Data.Name == name {
			return tl
		}
	}
	return nil
}

func (tbl *mockTaskTable) generate(count int, expired bool) {
	for i := 0; i < count; i++ {
		exp := time.Now().UTC().Add(time.Hour)
		ti := &persistencespb.AllocatedTaskInfo{
			Data: &persistencespb.TaskInfo{
				NamespaceId:      tbl.namespaceID,
				WorkflowId:       tbl.workflowID,
				RunId:            tbl.runID,
				ScheduledEventId: 3,
				ExpiryTime:       &exp,
			},
			TaskId: tbl.nextTaskID,
		}
		if expired {
			ti.Data.ExpiryTime = timestamp.TimePtr(time.Unix(0, time.Now().UTC().UnixNano()-int64(time.Second*33)).UTC())
		}
		tbl.tasks = append(tbl.tasks, ti)
		tbl.nextTaskID++
	}
}

func (tbl *mockTaskTable) get(count int) []*persistencespb.AllocatedTaskInfo {
	if len(tbl.tasks) >= count {
		return tbl.tasks[:count]
	}
	return tbl.tasks[:]
}

func (tbl *mockTaskTable) deleteLessThan(id int64, limit int) int {
	count := 0
	for _, t := range tbl.tasks {
		if t.GetTaskId() < id && count < limit {
			count++
			continue
		}
		break
	}
	switch {
	case count == 0:
	case count == len(tbl.tasks):
		tbl.tasks = nil
	default:
		tbl.tasks = tbl.tasks[count:]
	}
	return count
}
