package passivepath

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/testhooks"
)

// TestPassivePathServer runs a long-lived single-cluster server with the passive-path
// diversion installed, so external load generators (bench-go, omes, the temporal CLI)
// can be pointed at it. It is not an assertion-style test; it is a server.
//
// Skipped unless PASSIVEPATH_SERVER=1, so it never runs in a normal package run.
//
//	PASSIVEPATH_SERVER=1 PASSIVEPATH_DURATION=10m \
//	  go test -tags test_dep -count=1 -timeout 0 -v \
//	  -run TestPassivePathServer ./tests/passivepath/...
//
// Env:
//
//	PASSIVEPATH_SERVER    must be "1"
//	PASSIVEPATH_DURATION  how long to stay up (default 10m)
//	PASSIVEPATH_NAMESPACE global namespace to pre-create (default "default")
//	PASSIVEPATH_ADDR_FILE file to write the frontend address to (default
//	                      /tmp/passivepath_frontend_addr)
//
// The namespace is pre-created as *global* because the NDC stack -- and therefore
// ReplicateVersionedTransition -- only exists on a cluster with global namespaces
// enabled. bench-go's own RegisterNamespace call tolerates AlreadyExists, so it will
// simply reuse this one.
func TestPassivePathServer(t *testing.T) {
	if os.Getenv("PASSIVEPATH_SERVER") != "1" {
		t.Skip("set PASSIVEPATH_SERVER=1 to run the passive-path server")
	}

	duration := 10 * time.Minute
	if v := os.Getenv("PASSIVEPATH_DURATION"); v != "" {
		d, err := time.ParseDuration(v)
		require.NoError(t, err)
		duration = d
	}
	ns := os.Getenv("PASSIVEPATH_NAMESPACE")
	if ns == "" {
		ns = "default"
	}
	addrFile := os.Getenv("PASSIVEPATH_ADDR_FILE")
	if addrFile == "" {
		addrFile = "/tmp/passivepath_frontend_addr"
	}
	logger := log.NewNoopLogger() // server logs would drown the stats output
	harness := NewHarness(logger)
	// Active execution can return the next workflow task inline and deliberately skip
	// persisting its transfer task. Passive apply cannot use that active-side delivery,
	// so TaskRefresher must persist the transfer task instead.
	harness.AllowPassiveOnlyTaskTypes("transfer/TransferWorkflowTask")
	tc := newSingleClusterWithGlobalNamespace(t, logger)
	namespaceID := registerGlobalNamespace(t, tc, ns)
	t.Cleanup(tc.InjectHook(t, testhooks.NewHook[testhooks.HistoryPassiveReplicationTestHook](
		testhooks.HistoryPassiveReplicationTest,
		harness,
	), namespaceID))

	addr := tc.Host().FrontendGRPCAddress()
	require.NoError(t, os.WriteFile(addrFile, []byte(addr), 0o644))
	t.Cleanup(func() { _ = os.Remove(addrFile) })

	fmt.Printf("PASSIVEPATH_READY addr=%s namespace=%s duration=%s\n", addr, ns, duration)

	deadline := time.Now().Add(duration)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var lastApplied int
	lastAt := time.Now()
	for range ticker.C {
		applied := harness.Applied()
		now := time.Now()
		rate := float64(applied-lastApplied) / now.Sub(lastAt).Seconds()
		lastApplied, lastAt = applied, now
		activeAttempts := harness.ActiveAttempts()
		diversionPercent := 0.0
		if activeAttempts != 0 {
			diversionPercent = 100 * float64(harness.Diverted()) / float64(activeAttempts)
		}

		fmt.Printf("PASSIVEPATH_STATS intercepted=%d activeAttempts=%d diverted=%d diversion=%.1f%% applied=%d applies/s=%.1f "+
			"standbyExecutions=%d bailouts=%v applyErrs=%d\n",
			harness.Intercepted(), activeAttempts, harness.Diverted(), diversionPercent, applied, rate,
			harness.StandbyExecutions(),
			harness.Bailouts(), len(harness.ApplyErrors()))
		if applyErrs := harness.ApplyErrors(); len(applyErrs) != 0 {
			t.Fatalf("passivepath: passive apply failed: %v", applyErrs[0])
		}

		if now.After(deadline) {
			break
		}
	}

	fmt.Printf("PASSIVEPATH_FINAL intercepted=%d activeAttempts=%d diverted=%d applied=%d standbyExecutions=%d bailouts=%v allBailouts=%v applyErrs=%d\n",
		harness.Intercepted(), harness.ActiveAttempts(), harness.Diverted(), harness.Applied(),
		harness.StandbyExecutions(),
		harness.Bailouts(), harness.AllBailouts(), len(harness.ApplyErrors()))
	for i, err := range harness.ApplyErrors() {
		if i >= 10 {
			fmt.Printf("PASSIVEPATH_ERR ... and %d more\n", len(harness.ApplyErrors())-10)
			break
		}
		fmt.Printf("PASSIVEPATH_ERR %v\n", err)
	}
}
