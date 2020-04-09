package tasklist

import (
	"encoding/binary"
	"sync"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/pborman/uuid"

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	p "github.com/temporalio/temporal/common/persistence"
)

type (
	mockTaskTable struct {
		namespaceID []byte
		workflowID  string
		runID       []byte
		nextTaskID  int64
		tasks       []*persistenceblobs.AllocatedTaskInfo
	}
	mockTaskListTable struct {
		sync.Mutex
		info []*p.PersistedTaskListInfo
	}
)

func newMockTaskTable() *mockTaskTable {
	return &mockTaskTable{
		namespaceID: uuid.NewRandom(),
		workflowID:  uuid.New(),
		runID:       uuid.NewRandom(),
	}
}

func (tbl *mockTaskListTable) generate(name string, idle bool) {
	tl := p.PersistedTaskListInfo{
		Data: &persistenceblobs.TaskListInfo{
			NamespaceId: uuid.NewRandom(),
			Name:        name,
			LastUpdated: types.TimestampNow(),
		},
		RangeID: 22,
	}
	if idle {
		tl.Data.LastUpdated, _ = types.TimestampProto(time.Unix(1000, 1000))
	}
	tbl.info = append(tbl.info, &tl)
}

func (tbl *mockTaskListTable) list(token []byte, count int) ([]*p.PersistedTaskListInfo, []byte) {
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
	var newInfo []*p.PersistedTaskListInfo
	for _, tl := range tbl.info {
		if tl.Data.Name != name {
			newInfo = append(newInfo, tl)
		}
	}
	tbl.info = newInfo
}

func (tbl *mockTaskListTable) get(name string) *p.PersistedTaskListInfo {
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
		exp, _ := types.TimestampProto(time.Now().Add(time.Hour))
		ti := &persistenceblobs.AllocatedTaskInfo{
			Data: &persistenceblobs.TaskInfo{
				NamespaceId: tbl.namespaceID,
				WorkflowId:  tbl.workflowID,
				RunId:       tbl.runID,
				ScheduleId:  3,
				Expiry:      exp,
			},
			TaskId: tbl.nextTaskID,
		}
		if expired {
			ti.Data.Expiry, _ = types.TimestampProto(time.Unix(0, time.Now().UnixNano()-int64(time.Second*33)))
		}
		tbl.tasks = append(tbl.tasks, ti)
		tbl.nextTaskID++
	}
}

func (tbl *mockTaskTable) get(count int) []*persistenceblobs.AllocatedTaskInfo {
	if len(tbl.tasks) >= count {
		return tbl.tasks[:count]
	}
	return tbl.tasks[:]
}

func (tbl *mockTaskTable) deleteLessThan(id int64, limit int) int {
	count := 0
	for _, t := range tbl.tasks {
		if t.GetTaskId() <= id && count < limit {
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
