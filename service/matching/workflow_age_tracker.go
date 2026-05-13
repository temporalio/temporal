package matching

import (
	"container/list"
	"sync"
	"time"
)

// workflowAgeTracker records the first time a given RunId was observed enqueueing
// a task in this backlog manager. It is used to compute a workflow-age priority
// boost so that long-running workflows are dispatched ahead of fresh ones.
//
// The tracker is bounded: when more than sizeFn() distinct RunIds are present the
// oldest (least recently observed) entries are evicted. Eviction is acceptable —
// a workflow that returns after eviction simply starts fresh from age 0.
type workflowAgeTracker struct {
	sizeFn func() int

	mu      sync.Mutex
	byKey   map[string]*list.Element
	byOrder *list.List // front = most recently observed
}

type workflowAgeEntry struct {
	runID     string
	firstSeen time.Time
}

func newWorkflowAgeTracker(sizeFn func() int) *workflowAgeTracker {
	return &workflowAgeTracker{
		sizeFn:  sizeFn,
		byKey:   make(map[string]*list.Element),
		byOrder: list.New(),
	}
}

// observe records (or refreshes the recency of) a RunId and returns now-firstSeen.
// If the RunId has not been seen before, firstSeen is set to now and 0 is returned.
func (t *workflowAgeTracker) observe(runID string, now time.Time) time.Duration {
	if runID == "" {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	if elt, ok := t.byKey[runID]; ok {
		entry := elt.Value.(*workflowAgeEntry)
		t.byOrder.MoveToFront(elt)
		age := now.Sub(entry.firstSeen)
		if age < 0 {
			return 0
		}
		return age
	}

	entry := &workflowAgeEntry{runID: runID, firstSeen: now}
	elt := t.byOrder.PushFront(entry)
	t.byKey[runID] = elt

	maxSize := t.sizeFn()
	if maxSize <= 0 {
		maxSize = 1
	}
	for t.byOrder.Len() > maxSize {
		oldest := t.byOrder.Back()
		if oldest == nil {
			break
		}
		t.byOrder.Remove(oldest)
		delete(t.byKey, oldest.Value.(*workflowAgeEntry).runID)
	}
	return 0
}

// size returns the current number of tracked RunIds. For tests/metrics.
func (t *workflowAgeTracker) size() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.byOrder.Len()
}

// applyWorkflowAgeBoost subtracts a sub-level workflow-age boost from
// task.effectivePriority when enabled. The boost is bounded so it cannot
// promote a task across a configured priority level.
func applyWorkflowAgeBoost(cfg *taskQueueConfig, tracker *workflowAgeTracker, task *internalTask) {
	if cfg.WorkflowAgingEnabled == nil || !cfg.WorkflowAgingEnabled() {
		return
	}
	if tracker == nil || task.event == nil || task.event.Data == nil {
		return
	}
	runID := task.event.Data.GetRunId()
	if runID == "" {
		return
	}
	age := tracker.observe(runID, time.Now())
	task.effectivePriority -= workflowAgeBoost(age, cfg.WorkflowAgingFullBoostAfter())
}

// workflowAgeBoost translates a workflow age into a sub-level priority boost.
// The return value is in [0, effectivePriorityFactor-1] so that subtracting it
// from effectivePriority can never cross a configured priority level.
func workflowAgeBoost(age time.Duration, fullBoostAfter time.Duration) priorityKey {
	if age <= 0 || fullBoostAfter <= 0 {
		return 0
	}
	const maxBoost = effectivePriorityFactor - 1
	if age >= fullBoostAfter {
		return maxBoost
	}
	boost := int64(age) * maxBoost / int64(fullBoostAfter)
	if boost < 0 {
		return 0
	}
	if boost > maxBoost {
		return maxBoost
	}
	return priorityKey(boost)
}
