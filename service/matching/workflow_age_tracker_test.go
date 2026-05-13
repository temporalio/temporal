package matching

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

func TestWorkflowAgeTracker_FirstObserveReturnsZero(t *testing.T) {
	tr := newWorkflowAgeTracker(func() int { return 10 })
	now := time.Now()
	require.Equal(t, time.Duration(0), tr.observe("run-1", now))
}

func TestWorkflowAgeTracker_RepeatObserveReturnsAge(t *testing.T) {
	tr := newWorkflowAgeTracker(func() int { return 10 })
	t0 := time.Now()
	tr.observe("run-1", t0)
	got := tr.observe("run-1", t0.Add(5*time.Minute))
	require.Equal(t, 5*time.Minute, got)
}

func TestWorkflowAgeTracker_EmptyRunIDIsNoop(t *testing.T) {
	tr := newWorkflowAgeTracker(func() int { return 10 })
	require.Equal(t, time.Duration(0), tr.observe("", time.Now()))
	require.Equal(t, 0, tr.size())
}

func TestWorkflowAgeTracker_NegativeClockSkewClampsToZero(t *testing.T) {
	tr := newWorkflowAgeTracker(func() int { return 10 })
	t0 := time.Now()
	tr.observe("run-1", t0)
	require.Equal(t, time.Duration(0), tr.observe("run-1", t0.Add(-time.Second)))
}

func TestWorkflowAgeTracker_EvictsOldestWhenFull(t *testing.T) {
	tr := newWorkflowAgeTracker(func() int { return 3 })
	t0 := time.Now()
	tr.observe("run-1", t0)
	tr.observe("run-2", t0.Add(time.Second))
	tr.observe("run-3", t0.Add(2*time.Second))
	tr.observe("run-4", t0.Add(3*time.Second))

	require.Equal(t, 3, tr.size())
	// run-2 should still be present and report its real age.
	require.Equal(t, 3*time.Second, tr.observe("run-2", t0.Add(4*time.Second)))
	// run-1 should have been evicted; re-observe returns 0 (treated as new).
	require.Equal(t, time.Duration(0), tr.observe("run-1", t0.Add(5*time.Second)))
}

func TestWorkflowAgeTracker_ReObservingRefreshesRecency(t *testing.T) {
	tr := newWorkflowAgeTracker(func() int { return 3 })
	t0 := time.Now()
	tr.observe("run-1", t0)
	tr.observe("run-2", t0.Add(time.Second))
	tr.observe("run-3", t0.Add(2*time.Second))
	// touching run-1 should move it to front; next insert should evict run-2 instead.
	tr.observe("run-1", t0.Add(3*time.Second))
	tr.observe("run-4", t0.Add(4*time.Second))

	require.Equal(t, 3, tr.size())
	// run-1 still tracked (and still reports original firstSeen)
	require.Equal(t, 5*time.Second, tr.observe("run-1", t0.Add(5*time.Second)))
	// run-2 evicted
	require.Equal(t, time.Duration(0), tr.observe("run-2", t0.Add(6*time.Second)))
}

func TestWorkflowAgeTracker_ZeroSizeFallsBackToOne(t *testing.T) {
	tr := newWorkflowAgeTracker(func() int { return 0 })
	t0 := time.Now()
	tr.observe("run-1", t0)
	tr.observe("run-2", t0.Add(time.Second))
	require.Equal(t, 1, tr.size())
}

func TestWorkflowAgeTracker_ShrinkingMaxSizeEvictsOnNextInsert(t *testing.T) {
	maxSize := 5
	tr := newWorkflowAgeTracker(func() int { return maxSize })
	t0 := time.Now()
	for i := 0; i < 5; i++ {
		tr.observe("run-"+strconv.Itoa(i), t0.Add(time.Duration(i)*time.Second))
	}
	require.Equal(t, 5, tr.size())

	maxSize = 2
	tr.observe("run-new", t0.Add(10*time.Second))
	require.Equal(t, 2, tr.size())
}

func TestWorkflowAgeBoost_ZeroForZeroAge(t *testing.T) {
	require.Equal(t, priorityKey(0), workflowAgeBoost(0, time.Minute))
}

func TestWorkflowAgeBoost_ZeroForZeroFullBoost(t *testing.T) {
	require.Equal(t, priorityKey(0), workflowAgeBoost(time.Hour, 0))
}

func TestWorkflowAgeBoost_SaturatesAtFullBoost(t *testing.T) {
	want := priorityKey(effectivePriorityFactor - 1)
	require.Equal(t, want, workflowAgeBoost(time.Hour, time.Minute))
	require.Equal(t, want, workflowAgeBoost(time.Minute, time.Minute))
}

func TestWorkflowAgeBoost_MonotonicallyIncreases(t *testing.T) {
	full := 10 * time.Minute
	var prev priorityKey
	for i := 1; i <= 10; i++ {
		got := workflowAgeBoost(time.Duration(i)*time.Minute, full)
		require.GreaterOrEqual(t, got, prev, "boost should be monotonic")
		prev = got
	}
}

func TestApplyWorkflowAgeBoost_DisabledIsNoop(t *testing.T) {
	cfg := &taskQueueConfig{
		WorkflowAgingEnabled:        func() bool { return false },
		WorkflowAgingFullBoostAfter: func() time.Duration { return 10 * time.Minute },
	}
	tr := newWorkflowAgeTracker(func() int { return 10 })
	task := newInternalTaskFromBacklog(&persistencespb.AllocatedTaskInfo{
		Data: &persistencespb.TaskInfo{RunId: "run-1"},
	}, nil)
	task.effectivePriority = effectivePriorityFactor * 1
	before := task.effectivePriority
	applyWorkflowAgeBoost(cfg, tr, task)
	require.Equal(t, before, task.effectivePriority)
	require.Equal(t, 0, tr.size(), "tracker should not record observations when disabled")
}

func TestApplyWorkflowAgeBoost_StaysWithinLevel(t *testing.T) {
	// A level-1 task (effectivePriority = 10) must never end up at <= 0 after boost,
	// or it would leapfrog level 0 / unbounded space reserved for draining tasks.
	cfg := &taskQueueConfig{
		WorkflowAgingEnabled:        func() bool { return true },
		WorkflowAgingFullBoostAfter: func() time.Duration { return time.Millisecond },
	}
	tr := newWorkflowAgeTracker(func() int { return 10 })
	task := newInternalTaskFromBacklog(&persistencespb.AllocatedTaskInfo{
		Data: &persistencespb.TaskInfo{RunId: "run-1"},
	}, nil)
	task.effectivePriority = effectivePriorityFactor * 1

	// First observation: age=0, no boost.
	applyWorkflowAgeBoost(cfg, tr, task)
	require.Equal(t, priorityKey(effectivePriorityFactor*1), task.effectivePriority)

	// Second observation: age is well past fullBoostAfter, max boost applied.
	time.Sleep(5 * time.Millisecond)
	applyWorkflowAgeBoost(cfg, tr, task)
	require.Equal(t, priorityKey(effectivePriorityFactor*1-(effectivePriorityFactor-1)), task.effectivePriority)
	require.Greater(t, task.effectivePriority, priorityKey(0),
		"boost must not let a level-1 task cross into level 0")
}

func TestApplyWorkflowAgeBoost_NoEventIsNoop(t *testing.T) {
	cfg := &taskQueueConfig{
		WorkflowAgingEnabled:        func() bool { return true },
		WorkflowAgingFullBoostAfter: func() time.Duration { return time.Minute },
	}
	tr := newWorkflowAgeTracker(func() int { return 10 })
	task := &internalTask{effectivePriority: effectivePriorityFactor * 2}
	applyWorkflowAgeBoost(cfg, tr, task)
	require.Equal(t, priorityKey(effectivePriorityFactor*2), task.effectivePriority)
}

func TestWorkflowAgeBoost_NeverCrossesPriorityLevel(t *testing.T) {
	// effectivePriority for priority level 1 = 10. Subtracting any boost from
	// a level-1 task must stay strictly above 0 (i.e. >= 1), guaranteeing it
	// never reaches the effective priority of level 0 (which doesn't exist)
	// or lands in negative territory that would let it leapfrog a draining task.
	for age := time.Duration(0); age <= 2*time.Hour; age += time.Minute {
		boost := workflowAgeBoost(age, time.Hour)
		require.Less(t, boost, priorityKey(effectivePriorityFactor),
			"boost %d at age %s must be < effectivePriorityFactor", boost, age)
	}
}
