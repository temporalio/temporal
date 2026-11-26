package fc

import "time"

type testTask struct {
	limiters []Limiter
}

func (t *testTask) Limiters() []Limiter { return t.limiters }

func (*testTask) PriorityAndAge() (int32, time.Time) { return 1, time.Time{} }
