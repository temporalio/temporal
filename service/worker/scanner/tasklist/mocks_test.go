// Copyright (c) 2017 Uber Technologies, Inc.
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

package tasklist

import (
	"encoding/binary"
	"sync"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/pborman/uuid"

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
)

type (
	mockTaskTable struct {
		domainID   []byte
		workflowID string
		runID      []byte
		nextTaskID int64
		tasks      []*persistenceblobs.PersistedTaskInfo
	}
	mockTaskListTable struct {
		sync.Mutex
		info []*persistenceblobs.PersistedTaskListInfo
	}
)

func newMockTaskTable() *mockTaskTable {
	return &mockTaskTable{
		domainID:   uuid.NewRandom(),
		workflowID: uuid.New(),
		runID:      uuid.NewRandom(),
	}
}

func (tbl *mockTaskListTable) generate(name string, idle bool) {
	tl := persistenceblobs.PersistedTaskListInfo{
		ListData: &persistenceblobs.TaskListInfo{
			DomainID: uuid.NewRandom(),
			Name:     name,
		},
		RangeID:     22,
		LastUpdated: types.TimestampNow(),
	}
	if idle {
		tl.LastUpdated, _ = types.TimestampProto(time.Unix(1000, 1000))
	}
	tbl.info = append(tbl.info, &tl)
}

func (tbl *mockTaskListTable) list(token []byte, count int) ([]*persistenceblobs.PersistedTaskListInfo, []byte) {
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

func (tbl *mockTaskListTable) delete(name string) {
	tbl.Lock()
	defer tbl.Unlock()
	var newInfo []*persistenceblobs.PersistedTaskListInfo
	for _, tl := range tbl.info {
		if tl.ListData.Name != name {
			newInfo = append(newInfo, tl)
		}
	}
	tbl.info = newInfo
}

func (tbl *mockTaskListTable) get(name string) *persistenceblobs.PersistedTaskListInfo {
	tbl.Lock()
	defer tbl.Unlock()
	for _, tl := range tbl.info {
		if tl.ListData.Name == name {
			return tl
		}
	}
	return nil
}

func (tbl *mockTaskTable) generate(count int, expired bool) {
	for i := 0; i < count; i++ {
		exp, _ := types.TimestampProto(time.Now().Add(time.Hour))
		ti := &persistenceblobs.PersistedTaskInfo{
			TaskData: &persistenceblobs.TaskInfo{
				DomainID:   tbl.domainID,
				WorkflowID: tbl.workflowID,
				RunID:      tbl.runID,
				ScheduleID: 3,
				Expiry: exp,
			},
			TaskID: tbl.nextTaskID,

		}
		if expired {
			ti.TaskData.Expiry, _ = types.TimestampProto(time.Unix(0, time.Now().UnixNano()-int64(time.Second*33)))
		}
		tbl.tasks = append(tbl.tasks, ti)
		tbl.nextTaskID++
	}
}

func (tbl *mockTaskTable) get(count int) []*persistenceblobs.PersistedTaskInfo {
	if len(tbl.tasks) >= count {
		return tbl.tasks[:count]
	}
	return tbl.tasks[:]
}

func (tbl *mockTaskTable) deleteLessThan(id int64, limit int) int {
	count := 0
	for _, t := range tbl.tasks {
		if t.TaskID <= id && count < limit {
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
