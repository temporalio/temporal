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

// TestMixedBrain starts two servers in parallel — one built from the current
// branch's source tree and the other from the latest release tag of the
// previous minor — joined into a single logical cluster, and runs the Omes
// throughput_stress scenario through a round-robin TCP proxy to exercise both.
// Server lifecycle (clone + build + config + process) is delegated to
// github.com/temporalio/omes/devserver.
func TestMixedBrain(t *testing.T) {
	tmpDir := t.TempDir()
	logRoot := logDir(t)

	omesBinary := filepath.Join(tmpDir, "omes-bin")

	var releaseTag string
	t.Run("setup", func(t *testing.T) {
		t.Run("resolve release tag", func(t *testing.T) {
			t.Parallel()
			releaseTag = fetchPreviousMinorTag(t)
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

	// Start the current-source server first so the release server can target
	// its frontend in cluster metadata.
	currentLogger, currentLog := serverLogger(t, "current", logRoot)
	defer currentLog.Close()
	currentSrv, err := devserver.Start(t.Context(), devserver.Options{
		SourceDir:   sourceRoot(),
		Persistence: persistence,
		Output:      currentLog,
		Logger:      currentLogger,
	})
	require.NoError(t, err, "start current server")
	t.Cleanup(func() { _ = currentSrv.Stop() })

	conn, err := grpc.NewClient(currentSrv.FrontendHostPort(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	// devserver registers "default" itself, but the release server will need
	// to see it too once it joins — the helper waits for AlreadyExists.
	registerDefaultNamespace(t, conn)

	releaseLogger, releaseLog := serverLogger(t, "release", logRoot)
	defer releaseLog.Close()
	releaseSrv, err := devserver.Start(t.Context(), devserver.Options{
		Ref:         releaseTag,
		Persistence: persistence,
		ClusterEndpoint: devserver.ClusterEndpoint{
			RPCAddress: currentSrv.FrontendHostPort(),
		},
		Output: releaseLog,
		Logger: releaseLogger,
	})
	require.NoError(t, err, "start release server")
	t.Cleanup(func() { _ = releaseSrv.Stop() })

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

		proxy := startFrontendProxy(st, currentSrv.FrontendHostPort(), releaseSrv.FrontendHostPort())
		st.Cleanup(proxy.stop)

		runOmes(st, omesBinary, proxy.addr(), filepath.Join(logRoot, "mixedbrain_omes.log"), testDuration(), runID, nexusEndpoint)

		for i, backend := range []string{"current", "release"} {
			count := proxy.connCount[i].Load()
			st.Logf("Proxy connections to %s: %d", backend, count)
			require.Positive(st, count, "expected proxy to route traffic to %s server", backend)
		}
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

func persistenceFromEnv() devserver.PersistenceOptions {
	driver := os.Getenv("PERSISTENCE_DRIVER")
	if driver == "" || driver == "sqlite" {
		return devserver.PersistenceOptions{} // default sqlite
	}
	return devserver.PersistenceOptions{
		Driver: driver,
	}
}

func serverLogger(t *testing.T, name, logRoot string) (*zap.SugaredLogger, *os.File) {
	t.Helper()
	logPath := filepath.Join(logRoot, fmt.Sprintf("mixedbrain_process-%s.log", name))
	f, err := os.Create(logPath)
	require.NoError(t, err)
	return zap.NewNop().Sugar().With("server", name), f
}
