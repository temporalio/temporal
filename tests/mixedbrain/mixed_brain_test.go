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
	"go.temporal.io/api/workflowservice/v1"
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

func serverLogger(t *testing.T, name, logRoot string) (*zap.SugaredLogger, *os.File) {
	t.Helper()
	logPath := filepath.Join(logRoot, fmt.Sprintf("mixedbrain_process-%s.log", name))
	f, err := os.Create(logPath)
	require.NoError(t, err)
	return zap.NewNop().Sugar().With("server", name), f
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
	// postgres' cluster_membership.rpc_port is SMALLINT (max 32767); ephemeral
	// ports overflow. Pin two non-overlapping low bases when not on sqlite.
	currentBase, releaseBase := 0, 0
	if persistence.Driver != "" && persistence.Driver != "sqlite" {
		currentBase, releaseBase = 7230, 7240
	}

	// Standalone Nexus is a current-only feature (off by default in releases
	// pre-1.31). Enabling it on the current server lets omes exercise the
	// StartNexusOperationExecution RPC; requests that the proxy routes to
	// the release server come back with Unimplemented, which the omes
	// scenario tolerates as a no-op.
	currentDC := map[string]any{
		"nexusoperation.enableStandalone": true,
	}

	// Start the current-source server first so the release server can target
	// its frontend in cluster metadata.
	currentLogger, currentLog := serverLogger(t, "current", logRoot)
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

	releaseLogger, releaseLog := serverLogger(t, "release", logRoot)
	defer releaseLog.Close()
	releaseSrv, err := devserver.Start(t.Context(), devserver.Options{
		Ref:         releaseTag,
		PortBase:    releaseBase,
		Persistence: persistence,
		ClusterEndpoint: devserver.ClusterEndpoint{
			RPCAddress: currentSrv.FrontendHostPort(),
		},
		Stdout: releaseLog,
		Stderr: releaseLog,
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

		// TCP proxy round-robins each new connection across the two
		// backends. gRPC's pick-first multiplexing keeps every RPC on the
		// first connection it picks, so omes' standalone-Nexus calls all
		// land on whichever backend the worker's gRPC client landed on
		// (typically the release server here — which returns Unimplemented
		// and exercises the graceful-degradation path in omes#339). The
		// success path is verified separately below against currentSrv.
		proxy := startFrontendProxy(st, currentSrv.FrontendHostPort(), releaseSrv.FrontendHostPort())
		st.Cleanup(proxy.stop)

		runOmes(st, omesBinary, proxy.addr(), filepath.Join(logRoot, "mixedbrain_omes.log"), testDuration(), runID, nexusEndpoint)

		for i, backend := range []string{"current", "release"} {
			count := proxy.connCount[i].Load()
			st.Logf("Proxy connections to %s: %d", backend, count)
			require.Positive(st, count, "expected proxy to route traffic to %s server", backend)
		}
	})

	t.Run("verify standalone nexus succeeds on current", func(st *testing.T) {
		// Direct call to currentSrv (no proxy) to prove the success path
		// works in the same run that demonstrates Unimplemented tolerance
		// against the release server. The omes scenario can't easily target
		// a specific backend (HTTP/2 multiplexing pins streams), so we
		// verify here.
		verifyStandaloneNexus(st, currentSrv.FrontendHostPort(), nexusEndpoint, runID)
	})
}

// verifyStandaloneNexus issues one StartNexusOperationExecution against the
// given frontend and asserts it succeeds. The endpoint must already exist.
func verifyStandaloneNexus(t *testing.T, frontend, endpoint, runID string) {
	t.Helper()
	cc, err := grpc.NewClient(frontend, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer cc.Close()

	wf := workflowservice.NewWorkflowServiceClient(cc)
	opID := fmt.Sprintf("verify-standalone-nexus-%s", runID)
	_, err = wf.StartNexusOperationExecution(t.Context(), &workflowservice.StartNexusOperationExecutionRequest{
		Namespace:   "default",
		OperationId: opID,
		Endpoint:    endpoint,
		Service:     "kitchen-sink",
		Operation:   "echo-sync",
	})
	require.NoError(t, err, "standalone Nexus must succeed against current server")
	t.Logf("Direct standalone-Nexus call against current server succeeded (opID=%s)", opID)
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
			// Schedule standalone-Nexus actions through omes. With the TCP
			// proxy these typically all pin to one backend (HTTP/2
			// multiplexing); the direct verification subtest below covers
			// the success path explicitly against the current server.
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
