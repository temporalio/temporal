package mixedbrain

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func testDuration() time.Duration {
	if v := os.Getenv("MIXED_BRAIN_TEST_DURATION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			panic(fmt.Sprintf("invalid MIXED_BRAIN_TEST_DURATION %q: %v", v, err))
		}
		return d
	}
	return 30 * time.Second // locally we want only a smoke test to ensure it works
}

func logDir(t *testing.T) string {
	t.Helper()
	dir := os.Getenv("TEST_OUTPUT_ROOT")
	if dir == "" {
		dir = filepath.Join(os.TempDir(), "temporal-test-output")
	}
	require.NoError(t, os.MkdirAll(dir, 0755))
	return dir
}

// nexusEndpoint is created in the throughput_stress namespace before Omes runs.
const nexusEndpoint = "mixed-brain-nexus"

// omesScenario describes a single Omes scenario invocation for the mixed brain test.
type omesScenario struct {
	name      string
	namespace string
	// options are extra "key=value" pairs passed as --option flags.
	options []string
}

// scenarios are the Omes scenarios run concurrently against the mixed cluster,
// each in its own namespace (and, at runtime, its own run ID / task queue) so
// their load stays isolated while both exercise the mixed-version cluster.
var scenarios = []omesScenario{
	{
		name:      "throughput_stress",
		namespace: "throughput-stress",
		options: []string{
			"internal-iterations=10",
			"nexus-endpoint=" + nexusEndpoint,
		},
	},
	{
		// scheduler_stress needs no Nexus endpoint or search attributes;
		// chasm-scheduler is left at its scenario default (on).
		name:      "scheduler_stress",
		namespace: "scheduler-stress",
	},
}

// TestMixedBrain starts two servers in parallel, one using the current branch's binary
// and the other using the latest release binary. It then runs the Omes
// throughput_stress and scheduler_stress scenarios to ensure that the mixed
// brain works correctly.
// Uses SQLite locally; and a dedicated database in CI for better concurrency.
func TestMixedBrain(t *testing.T) {
	tmpDir := t.TempDir()
	logRoot := logDir(t)

	currentBinary := filepath.Join(tmpDir, "temporal-server-current")
	releaseBinary := filepath.Join(tmpDir, "temporal-server-release")
	omesBinary := filepath.Join(tmpDir, "omes-bin")

	currentLog := filepath.Join(logRoot, "mixedbrain_process-current.log")
	releaseLog := filepath.Join(logRoot, "mixedbrain_process-release.log")

	t.Run("setup", func(t *testing.T) {
		t.Run("build current server", func(t *testing.T) {
			t.Parallel()
			buildServer(t, sourceRoot(), currentBinary)
		})
		t.Run("download and build release server", func(t *testing.T) {
			t.Parallel()
			downloadAndBuildReleaseServer(t, releaseBinary)
		})
		t.Run("download and build Omes", func(t *testing.T) {
			t.Parallel()
			downloadAndBuildOmes(t, tmpDir)
		})
	})
	if t.Failed() {
		return
	}

	var portsCurrent, portsRelease portSet
	if os.Getenv("CI") != "" {
		portsCurrent = portSetA
		portsRelease = portSetB
	} else {
		portsCurrent = newRandPortSet()
		portsRelease = newRandPortSet()
	}

	configCurrent := generateConfig(t, tmpDir, portsCurrent, portsCurrent)
	configRelease := generateConfig(t, tmpDir, portsRelease, portsCurrent)

	var procCurrent, procRelease *serverProcess
	var conn *grpc.ClientConn
	var proxy *frontendProxy
	runID := fmt.Sprintf("mixed-brain-%d", time.Now().Unix())
	chaosInterval := processChaosInterval(t)

	t.Run("start current server", func(st *testing.T) {
		// Server processes use the parent t so their context survives this sub-test.
		procCurrent = startServerProcess(t, "current", currentBinary, configCurrent, currentLog)

		var err error
		conn, err = grpc.NewClient(portsCurrent.frontendAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(st, err)

		// This ensures the current server is fully booted before starting the release
		// server. Registering every scenario's namespace here gives them the full
		// cluster-formation window to propagate to all services before Omes connects.
		for _, s := range scenarios {
			registerNamespace(st, conn, s.namespace)
		}
	})
	if t.Failed() {
		return
	}
	t.Cleanup(procCurrent.stop)
	defer func() { _ = conn.Close() }()

	t.Run("start release server", func(_ *testing.T) {
		procRelease = startServerProcess(t, "release", releaseBinary, configRelease, releaseLog)
	})
	if t.Failed() {
		return
	}
	t.Cleanup(procRelease.stop)

	t.Run("form cluster", func(st *testing.T) {
		waitForClusterFormation(st, conn, 90*time.Second, portsCurrent, portsRelease)
	})
	if t.Failed() {
		return
	}

	t.Run("run omes", func(st *testing.T) {
		// The scenario's run ID determines its task queue (omes-<run ID>).
		// throughput_stress owns the Nexus endpoint, so target its task queue.
		throughput := scenarios[0]
		createNexusEndpoint(st, conn, nexusEndpoint, throughput.namespace, "omes-"+runID+"-"+throughput.name)

		proxy = startFrontendProxy(st, portsCurrent.frontendAddr(), portsRelease.frontendAddr())
		startProcessChaos(
			st,
			conn,
			chaosInterval,
			[]portSet{portsCurrent, portsRelease},
			processChaosTarget{name: "current", proc: procCurrent},
			processChaosTarget{name: "release", proc: procRelease},
		)

		for _, scenario := range scenarios {
			st.Run(scenario.name, func(sst *testing.T) {
				sst.Parallel()
				logPath := filepath.Join(logRoot, "mixedbrain_omes_"+scenario.name+".log")
				runOmes(sst, omesBinary, proxy.addr(), logPath, testDuration(), scenario, runID+"-"+scenario.name)
			})
		}
	})
	if t.Failed() {
		return
	}
	t.Cleanup(proxy.stop)

	t.Run("verify", func(st *testing.T) {
		procCurrent.requireAlive(st)
		procRelease.requireAlive(st)

		for i, backend := range []string{"current", "release"} {
			count := proxy.connCount[i].Load()
			st.Logf("Proxy connections to %s: %d", backend, count)
			require.Positive(st, count, "expected proxy to route traffic to %s server", backend)
		}
	})

	// Stop the servers so their logs are fully flushed, then scan them for
	// panics, soft-assertion failures, and other problems that don't surface as
	// a process exit. Runs regardless of whether "verify" failed, since a crashed
	// server's log is exactly what we want to inspect.
	procCurrent.stop()
	procRelease.stop()

	t.Run("scan server logs", func(st *testing.T) {
		problems, err := scanServerLogs(serverLogValidators, currentLog, releaseLog)
		require.NoError(st, err)
		for _, p := range problems {
			st.Error(p)
		}
	})
}

// runOmes runs the given Omes scenario against serverAddr under the given run ID.
// Retries if Omes fails due to search attribute not being ready yet.
// Deducts elapsed time from duration on retry so total wall time stays bounded.
func runOmes(t *testing.T, binary, serverAddr, logPath string, duration time.Duration, scenario omesScenario, runID string) {
	t.Helper()
	t.Logf("Running Omes %s for %v against %s", scenario.name, duration, serverAddr)

	started := time.Now()
	for {
		remaining := duration - time.Since(started)
		require.Greater(t, remaining, 10*time.Second, "Omes never started successfully, check %s", logPath)

		logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		require.NoError(t, err)

		args := []string{
			"run-scenario-with-worker",
			"--scenario", scenario.name,
			"--language", "go",
			"--server-address", serverAddr,
			"--namespace", scenario.namespace,
			"--duration", remaining.String(),
			"--timeout", (remaining + 2*time.Minute).String(), // with grace period to complete
			"--run-id", runID,
			"--max-concurrent", "5",
		}
		for _, opt := range scenario.options {
			args = append(args, "--option", opt)
		}

		var buf bytes.Buffer
		cmd := exec.CommandContext(t.Context(), binary, args...)
		cmd.Stdout = logFile
		cmd.Stderr = io.MultiWriter(logFile, &buf)
		cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
		cmd.WaitDelay = 15 * time.Second

		err = cmd.Run()
		_ = logFile.Close()
		if err != nil && strings.Contains(buf.String(), "no mapping defined for search attribute") {
			t.Log("Omes failed due to search attributes not ready, retrying...")
			continue
		}
		require.NoError(t, err, "Omes scenario failed, check %s", logPath)
		return
	}
}
