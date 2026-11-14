package worker

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// startWorker passes DeadlockDetectionTimeout through unchanged, so it is 0 when not
// overridden (asserted in TestPerNsWorkerManager's TestOptions). This pins what 0 means to
// the SDK: its 1s default, so per-namespace workers keep their existing deadlock threshold
// unless explicitly overridden.
func TestDeadlockDetectionTimeoutDefault(t *testing.T) {
	if os.Getenv("TEMPORAL_DEBUG") != "" {
		t.Skip("TEMPORAL_DEBUG disables the SDK's default deadlock detection")
	}

	for _, tc := range []struct {
		name         string
		override     time.Duration
		hold         time.Duration
		wantDeadlock bool
	}{
		{name: "default allows sub-second task", hold: 200 * time.Millisecond},
		{name: "default detects task over 1s", hold: 1500 * time.Millisecond, wantDeadlock: true},
		{name: "override allows task over 1s", override: 5 * time.Second, hold: 1500 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var ts testsuite.WorkflowTestSuite
			env := ts.NewTestWorkflowEnvironment()
			env.SetWorkerOptions(sdkworker.Options{DeadlockDetectionTimeout: tc.override})

			env.ExecuteWorkflow(func(workflow.Context) error {
				// Hold the workflow goroutine without yielding.
				for start := time.Now(); time.Since(start) < tc.hold; {
				}
				return nil
			})

			require.True(t, env.IsWorkflowCompleted())
			if tc.wantDeadlock {
				require.ErrorContains(t, env.GetWorkflowError(), "TMPRL1101")
				return
			}
			require.NoError(t, env.GetWorkflowError())
		})
	}
}
