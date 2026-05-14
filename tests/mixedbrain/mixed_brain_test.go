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
	"github.com/temporalio/omes/devserver"
	"go.temporal.io/server/common/headers"
	"go.uber.org/zap"
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

func persistenceFromEnv() devserver.PersistenceOptions {
	driver := os.Getenv("PERSISTENCE_DRIVER")
	if driver == "" || driver == "sqlite" {
		return devserver.PersistenceOptions{} // default sqlite
	}
	return devserver.PersistenceOptions{
		Driver:      driver,
		ConnectAddr: "127.0.0.1:5432",
		User:        "temporal",
		Password:    "temporal",
	}
}

func serverLogger(t *testing.T, name, logRoot string) (*zap.SugaredLogger, *os.File, string) {
	t.Helper()
	logPath := filepath.Join(logRoot, fmt.Sprintf("mixedbrain_process-%s.log", name))
	f, err := os.Create(logPath)
	require.NoError(t, err)
	return zap.NewNop().Sugar().With("server", name), f, logPath
}

// TestMixedBrainOSS runs the mixed-brain scenario against the latest OSS
// release tag of the previous minor (e.g. v1.30.x when current is 1.31.x).
func TestMixedBrainOSS(t *testing.T) {
	runMixedBrain(t, fetchPreviousMinorTag)
}

// TestMixedBrainCloud runs the mixed-brain scenario against the latest
// non-rc cloud release tag (e.g. v1.32.0-155.3). Cloud releases may be a
// newer codebase than the current branch — this exercises the opposite
// direction of version skew compared to TestMixedBrainOSS.
func TestMixedBrainCloud(t *testing.T) {
	runMixedBrain(t, fetchLastCloudReleaseTag)
}

// runMixedBrain starts two servers in parallel — one built from the current
// branch's source tree and the other from the tag returned by resolveTag —
// joins them into a single logical cluster, and runs the Omes
// throughput_stress scenario through a round-robin TCP proxy to exercise both.
// Server lifecycle (clone + build + config + process) is delegated to
// github.com/temporalio/omes/devserver.
func runMixedBrain(t *testing.T, resolveTag func(*testing.T) string) {
	tmpDir := t.TempDir()
	logRoot := logDir(t)

	omesBinary := filepath.Join(tmpDir, "omes-bin")

	var releaseTag string
	t.Run("setup", func(t *testing.T) {
		t.Run("resolve release tag", func(t *testing.T) {
			t.Parallel()
			releaseTag = resolveTag(t)
			t.Logf("Release tag: %s (current server version: %s)", releaseTag, headers.ServerVersion)
		})
		t.Run("build omes binary", func(t *testing.T) {
			t.Parallel()
			downloadAndBuildOmes(t, tmpDir, omesBinary)
		})
	})
	if t.Failed() {
		return
	}

	persistence := persistenceFromEnv()
	// postgres' cluster_membership.rpc_port is SMALLINT (max 32767); ephemeral
	// ports overflow. Pin two non-overlapping low bases when not on sqlite.
	currentBase, releaseBase := 0, 0
	if persistence.Driver != "" && persistence.Driver != "sqlite" {
		currentBase, releaseBase = 7230, 7240
	}

	// Keep repeated-task failure reporting inside the mixed-brain smoke-test
	// window. Defaults are tuned for production (e.g. 70 history-task DLQ
	// attempts is roughly an hour), which can let a bad task hide until after
	// this test has already ended.
	commonDC := map[string]any{
		"history.TaskDLQUnexpectedErrorAttempts":                            5,
		"history.workflowTaskCriticalAttempt":                               2,
		"system.numConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute": 2,
	}

	// Standalone Nexus is a current-only feature (off by default in releases
	// pre-1.31). Enabling it on the current server lets omes exercise the
	// StartNexusOperationExecution RPC; requests that the proxy routes to
	// the release server come back with Unimplemented, which the omes
	// scenario tolerates as a no-op.
	currentDC := map[string]any{
		"nexusoperation.enableStandalone": true,
	}
	for k, v := range commonDC {
		currentDC[k] = v
	}

	// Start the current-source server first so the release server can target
	// its frontend in cluster metadata.
	logMonitor := devserver.NewLogMonitor(t)

	currentLogger, currentLog, currentLogPath := serverLogger(t, "current", logRoot)
	defer currentLog.Close()
	currentSrv, err := devserver.Start(t.Context(), devserver.Options{
		SourceDir:           sourceRoot(),
		PortBase:            currentBase,
		Persistence:         persistence,
		DynamicConfigValues: currentDC,
		Stdout:              currentLog,
		Stderr:              currentLog,
		Logger:              currentLogger,
	})
	require.NoError(t, err, "start current server")
	t.Cleanup(func() { _ = currentSrv.Stop() })

	conn, err := grpc.NewClient(currentSrv.FrontendHostPort(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	// devserver registers "default" itself, but the release server will need
	// to see it too once it joins — the helper waits for AlreadyExists.
	registerDefaultNamespace(t, conn)

	releaseLogger, releaseLog, _ := serverLogger(t, "release", logRoot)
	defer releaseLog.Close()
	releaseSrv, err := devserver.Start(t.Context(), devserver.Options{
		Ref:                 releaseTag,
		PortBase:            releaseBase,
		Persistence:         persistence,
		DynamicConfigValues: commonDC,
		ClusterEndpoint: devserver.ClusterEndpoint{
			RPCAddress: currentSrv.FrontendHostPort(),
		},
		Stdout: releaseLog,
		Stderr: releaseLog,
		Logger: releaseLogger,
	})
	require.NoError(t, err, "start release server")
	t.Cleanup(func() { _ = releaseSrv.Stop() })

	// Current branch must not emit soft-asserts during the run. Release tag
	// is what it is, so we don't police its soft-asserts. Cross-version
	// task-failure probes (e.g. DLQ markers) belong on the *processing*
	// side: in the OSS direction current is the newer build and never
	// schedules tasks release would have to drop, so there's no probe to
	// fire here — the equivalent probe lives in TestMixedBrainCloud where
	// the direction reverses and current may DLQ tasks from a newer cloud.
	currentWatcher := logMonitor.Watch(t, "current", currentLogPath).
		MustNotMatch("failed assertion: ")

	runID := fmt.Sprintf("mixed-brain-%d", time.Now().Unix())
	nexusEndpoint := "mixed-brain-nexus"

	t.Run("form cluster", func(st *testing.T) {
		// Two physical servers, each running frontend/history/matching/worker.
		waitForClusterFormation(st, conn, 90*time.Second, 2)
	})
	if t.Failed() {
		return
	}

	t.Run("run omes", func(st *testing.T) {
		createNexusEndpoint(st, conn, nexusEndpoint, "default", "omes-"+runID)

		// TCP proxy round-robins each new connection across the two
		// backends so omes' frontend traffic touches both servers.
		proxy := startFrontendProxy(st, currentSrv.FrontendHostPort(), releaseSrv.FrontendHostPort())
		st.Cleanup(proxy.stop)

		runOmes(st, omesBinary, proxy.addr(), filepath.Join(logRoot, "mixedbrain_omes.log"), testDuration(), runID, nexusEndpoint)

		for i, backend := range []string{"current", "release"} {
			count := proxy.connCount[i].Load()
			st.Logf("Proxy connections to %s: %d", backend, count)
			require.Positive(st, count, "expected proxy to route traffic to %s server", backend)
		}
		logMonitor.AssertAll(st, currentWatcher)
	})
}

// runOmes runs Omes throughput stress scenario.
// Retries if Omes fails due to search attribute not being ready yet.
// Deducts elapsed time from duration on retry so total wall time stays bounded.
func runOmes(t *testing.T, binary, serverAddr, logPath string, duration time.Duration, runID, nexusEndpoint string) {
	t.Helper()
	t.Logf("Running Omes throughput_stress for %v against %s", duration, serverAddr)

	started := time.Now()
	for {
		remaining := duration - time.Since(started)
		require.Greater(t, remaining, 10*time.Second, "Omes never started successfully, check %s", logPath)

		logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		require.NoError(t, err)

		var buf bytes.Buffer
		cmd := exec.CommandContext(t.Context(), binary,
			"run-scenario-with-worker",
			"--scenario", "throughput_stress",
			"--language", "go",
			"--server-address", serverAddr,
			"--duration", remaining.String(),
			"--timeout", (remaining + 2*time.Minute).String(), // with grace period to complete
			"--run-id", runID,
			"--max-concurrent", "5",
			"--option", "internal-iterations=10",
			"--option", "nexus-endpoint="+nexusEndpoint,
			"--option", "include-standalone-nexus=true",
		)
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
