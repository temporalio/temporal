package tests

import (
	"testing"
	"time"

	chasmscheduler "go.temporal.io/server/chasm/lib/scheduler"
)

func TestScheduleVisibilityLifecycleV2Coalesced(t *testing.T) {
	tweakables := chasmscheduler.DefaultTweakables
	tweakables.EnableVisibilityCoalescing = true
	tweakables.VisibilityCoalesceInterval = 2 * time.Second
	runScheduleVisibilityLifecycleTests(t, tweakables)
}
