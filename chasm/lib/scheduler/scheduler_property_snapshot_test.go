package scheduler_test

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/service/history/tasks"
	"pgregory.net/rapid"
)

type schedulerTaskSnapshot struct {
	Type           string
	Category       string
	VisibilityTime time.Time
}

type schedulerSnapshot struct {
	Now                time.Time
	Closed             bool
	Paused             bool
	Notes              string
	ConflictToken      int64
	ActionCount        int64
	BufferSize         int64
	Running            int
	Recent             int
	GeneratorWatermark time.Time
	InvokerWatermark   time.Time
	BufferedStarts     int
	Tasks              []schedulerTaskSnapshot
	StartCalls         int
}

func (e *schedulerPropertyEnv) snapshot(t schedulerPropertyTestingT) schedulerSnapshot {
	t.Helper()
	description := e.describe(t)
	snapshot := schedulerSnapshot{
		Now: e.timeSource.Now(), Paused: description.GetSchedule().GetState().GetPaused(), Notes: description.GetSchedule().GetState().GetNotes(),
		ActionCount: description.GetInfo().GetActionCount(), BufferSize: description.GetInfo().GetBufferSize(), Running: len(description.GetInfo().GetRunningWorkflows()),
		Recent: len(description.GetInfo().GetRecentActions()), StartCalls: len(e.services.StartCalls()), ConflictToken: schedulerConflictToken(description.GetConflictToken()),
	}
	err := e.engine.ReadComponent(e.engineCtx, e.ref, func(ctx chasm.Context, component chasm.Component) error {
		schedule := component.(*scheduler.Scheduler)
		snapshot.Closed = schedule.Closed
		snapshot.GeneratorWatermark = schedule.Generator.Get(ctx).GetLastProcessedTime().AsTime()
		invoker := schedule.Invoker.Get(ctx)
		snapshot.InvokerWatermark = invoker.GetLastProcessedTime().AsTime()
		snapshot.BufferedStarts = len(invoker.GetBufferedStarts())
		return nil
	})
	require.NoError(t, err)
	queued, err := e.engine.Tasks(e.ref)
	require.NoError(t, err)
	for category, categoryTasks := range queued {
		if category == tasks.CategoryVisibility {
			continue
		}
		for _, task := range categoryTasks {
			snapshot.Tasks = append(snapshot.Tasks, schedulerTaskSnapshot{Type: fmt.Sprintf("%T", task), Category: category.Name(), VisibilityTime: task.GetVisibilityTime()})
		}
	}
	slices.SortFunc(snapshot.Tasks, func(a, b schedulerTaskSnapshot) int {
		if order := a.VisibilityTime.Compare(b.VisibilityTime); order != 0 {
			return order
		}
		if a.Category < b.Category {
			return -1
		}
		if a.Category > b.Category {
			return 1
		}
		if a.Type < b.Type {
			return -1
		}
		if a.Type > b.Type {
			return 1
		}
		return 0
	})
	return snapshot
}

func validateSchedulerSnapshot(snapshot schedulerSnapshot, maxBufferSize int) error {
	if snapshot.BufferSize < 0 || snapshot.ActionCount < 0 {
		return errors.New("scheduler counters must not be negative")
	}
	if snapshot.Running > snapshot.Recent {
		return errors.New("running workflows must be represented in recent actions")
	}
	if snapshot.BufferedStarts < int(snapshot.BufferSize)+snapshot.Recent {
		return errors.New("internal buffered starts do not cover API views")
	}
	internalLimit := maxBufferSize + scheduler.RecentActionCount
	if maxBufferSize > 0 && snapshot.BufferedStarts > internalLimit {
		return fmt.Errorf("buffered starts %d exceed internal limit %d", snapshot.BufferedStarts, internalLimit)
	}
	if snapshot.GeneratorWatermark.After(snapshot.Now) || snapshot.InvokerWatermark.After(snapshot.Now) {
		return errors.New("scheduler watermark is ahead of logical time")
	}
	return nil
}

func TestValidateSchedulerSnapshot(t *testing.T) {
	t.Parallel()
	valid := schedulerSnapshot{Now: schedulerPropertyStartTime, Recent: 1, Running: 1, BufferedStarts: 1, GeneratorWatermark: schedulerPropertyStartTime}
	require.NoError(t, validateSchedulerSnapshot(valid, 2))
	for _, test := range []struct {
		name     string
		mutate   func(*schedulerSnapshot)
		contains string
	}{
		{name: "negative counter", mutate: func(s *schedulerSnapshot) { s.BufferSize = -1 }, contains: "negative"},
		{name: "running absent from recent", mutate: func(s *schedulerSnapshot) { s.Recent = 0 }, contains: "recent"},
		{name: "API exceeds internal", mutate: func(s *schedulerSnapshot) { s.BufferSize = 1 }, contains: "API views"},
		{name: "buffer limit", mutate: func(s *schedulerSnapshot) { s.BufferedStarts = 2 + scheduler.RecentActionCount + 1 }, contains: "limit"},
		{name: "future watermark", mutate: func(s *schedulerSnapshot) { s.GeneratorWatermark = s.Now.Add(time.Second) }, contains: "logical time"},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot := valid
			test.mutate(&snapshot)
			require.ErrorContains(t, validateSchedulerSnapshot(snapshot, 2), test.contains)
		})
	}
}

func TestSchedulerSnapshotProperty(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		env := newSchedulerPropertyEnv(t, rapid.Bool().Draw(t, "initially paused"))
		env.drain(t, schedulerConformanceDrainLimit)
		env.trigger(t)
		env.drain(t, schedulerConformanceDrainLimit)
		require.NoError(t, validateSchedulerSnapshot(env.snapshot(t), schedulerPropertyMaxBufferSize))
		env.reload(t)
		require.NoError(t, validateSchedulerSnapshot(env.snapshot(t), schedulerPropertyMaxBufferSize))
	})
}
