package testrunner2

import "sync"

type workTracker struct {
	mu        sync.Mutex
	total     int
	completed int
	roots     map[string]*workSlot
}

type workSlot struct {
	running    int
	completed  bool
	successful bool
}

type progressUpdate struct {
	completed int
	total     int
	done      bool
}

func (t *workTracker) reset() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.total = 0
	t.completed = 0
	t.roots = make(map[string]*workSlot)
}

func (t *workTracker) addRoots(n int) {
	if n <= 0 {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.total += n
}

func (t *workTracker) beginAttempt(root string) {
	if root == "" {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.roots == nil {
		t.roots = make(map[string]*workSlot)
	}
	slot := t.roots[root]
	if slot == nil {
		slot = &workSlot{}
		t.roots[root] = slot
	}
	if slot.completed {
		slot.completed = false
		slot.successful = false
		if t.completed > 0 {
			t.completed--
		}
	}
	slot.running++
}

func (t *workTracker) finishAttempt(root string, successful bool) progressUpdate {
	if root == "" {
		return progressUpdate{}
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	slot := t.roots[root]
	if slot == nil {
		return progressUpdate{completed: t.completed, total: t.total}
	}
	if slot.running > 0 {
		slot.running--
	}
	if successful {
		slot.successful = true
	}
	if slot.successful && !slot.completed && slot.running == 0 {
		slot.completed = true
		t.completed++
		return progressUpdate{completed: t.completed, total: t.total, done: true}
	}
	return progressUpdate{completed: t.completed, total: t.total}
}
