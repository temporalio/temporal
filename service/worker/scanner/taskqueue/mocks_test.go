package taskqueue

import (
	"encoding/binary"
	"sync"
	"time"

	"github.com/google/uuid"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"google.golang.org/protobuf/types/known/timestamppb"
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
		namespaceID: uuid.NewString(),
		workflowID:  uuid.NewString(),
		runID:       uuid.NewString(),
	}
}

func (tbl *mockTaskQueueTable) generate(name string, idle bool) {
	tq := p.PersistedTaskQueueInfo{
		Data: &persistencespb.TaskQueueInfo{
			NamespaceId:    uuid.NewString(),
			Name:           name,
			LastUpdateTime: timestamp.TimeNowPtrUtc(),
		},
		RangeID: 22,
	}
	if idle {
		tq.Data.LastUpdateTime = timestamppb.New(time.Unix(1000, 1000).UTC())
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
				CreateTime:       timestamp.TimePtr(time.Now().UTC()),
				ScheduledEventId: 3,
				ExpiryTime:       timestamppb.New(exp),
			},
			TaskId: tbl.nextTaskID,
		}
		if expired {
			ti.Data.ExpiryTime = timestamppb.New(time.Unix(0, time.Now().UTC().UnixNano()-int64(time.Second*33)).UTC())
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
