package mixedbrain

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/omes/devserver"
	"go.temporal.io/server/common/headers"
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
	return 2 * time.Minute // locally we want only a smoke test to ensure it works
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

func setDefaultEnv(t *testing.T, key, value string) {
	t.Helper()
	if os.Getenv(key) == "" {
		t.Setenv(key, value)
	}
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

// TestMixedBrain starts two current and two release servers, then replaces each
// server once while Omes exercises the mixed-version cluster.
// Uses PostgreSQL because older release config templates do not support SQLite.
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
		t.Run("download and build Omes", func(t *testing.T) {
			t.Parallel()
			downloadAndBuildOmes(t, tmpDir, omesBinary)
		})
	})
	if t.Failed() {
		return
	}

	persistenceDriver := os.Getenv("PERSISTENCE_DRIVER")
	if persistenceDriver == "" {
		persistenceDriver = "postgres12"
	}
	switch persistenceDriver {
	case "postgres12", "postgres12_pgx":
		setDefaultEnv(t, "POSTGRES_SEEDS", "127.0.0.1")
		setDefaultEnv(t, "POSTGRES_USER", "temporal")
		setDefaultEnv(t, "POSTGRES_PWD", "temporal")
	default:
		t.Fatalf("mixedbrain requires PostgreSQL because older release config templates do not support SQLite")
	}
	persistence := devserver.PersistenceOptions{Driver: persistenceDriver}
	instances := []*serverInstance{
		{
			name:    "current-0",
			version: "current",
			logPath: filepath.Join(logRoot, "mixedbrain_process-current-0.log"),
			options: devserver.Options{SourceDir: sourceRoot(), Persistence: persistence},
		},
		{
			name:    "current-1",
			version: "current",
			logPath: filepath.Join(logRoot, "mixedbrain_process-current-1.log"),
			options: devserver.Options{SourceDir: sourceRoot(), Persistence: persistence},
		},
		{
			name:    "release-0",
			version: "release",
			logPath: filepath.Join(logRoot, "mixedbrain_process-release-0.log"),
			options: devserver.Options{Ref: releaseTag, Persistence: persistence},
		},
		{
			name:    "release-1",
			version: "release",
			logPath: filepath.Join(logRoot, "mixedbrain_process-release-1.log"),
			options: devserver.Options{Ref: releaseTag, Persistence: persistence},
		},
	}
	for _, instance := range instances {
		if err := os.Remove(instance.logPath); !os.IsNotExist(err) {
			require.NoError(t, err)
		}
	}
	t.Cleanup(func() {
		for _, instance := range instances {
			if err := instance.stop(); err != nil {
				t.Errorf("stop %s during cleanup: %v", instance.name, err)
			}
		}
	})
	runID := fmt.Sprintf("mixed-brain-%d", time.Now().Unix())

	t.Run("start servers", func(st *testing.T) {
		require.NoError(st, instances[0].start(t.Context(), ""))
		conn, err := grpc.NewClient(instances[0].frontendAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(st, err)
		defer func() { _ = conn.Close() }()

		for _, s := range scenarios {
			registerNamespace(st, conn, s.namespace)
		}
		for _, instance := range instances[1:] {
			require.NoError(st, instance.start(t.Context(), instances[0].frontendAddress()))
		}
	})
	if t.Failed() {
		return
	}

	t.Run("form cluster", func(st *testing.T) {
		conn, err := grpc.NewClient(instances[0].frontendAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(st, err)
		defer func() { _ = conn.Close() }()
		waitForClusterFormation(st, conn, 90*time.Second, instancePorts(instances)...)
	})
	if t.Failed() {
		return
	}

	proxy := startFrontendProxy(t)
	t.Cleanup(proxy.stop)
	for _, instance := range instances {
		require.NoError(t, proxy.AddBackend(instance.name, instance.version, instance.frontendAddress()))
	}

	t.Run("run omes and roll servers", func(st *testing.T) {
		conn, err := grpc.NewClient(instances[0].frontendAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(st, err)
		defer func() { _ = conn.Close() }()

		// The scenario's run ID determines its task queue (omes-<run ID>).
		// throughput_stress owns the Nexus endpoint, so target its task queue.
		throughput := scenarios[0]
		createNexusEndpoint(st, conn, nexusEndpoint, throughput.namespace, "omes-"+runID+"-"+throughput.name)

		duration := testDuration()
		workloadDeadline := time.Now().Add(duration)
		finalSoak := duration / 5
		omesCtx, cancelOmes := context.WithCancel(st.Context())
		results := make(chan omesResult, len(scenarios))
		var omesWG sync.WaitGroup
		defer omesWG.Wait()
		defer cancelOmes()
		for _, scenario := range scenarios {
			omesWG.Go(func() {
				logPath := filepath.Join(logRoot, "mixedbrain_omes_"+scenario.name+".log")
				err := runOmes(omesCtx, st, omesBinary, proxy.addr(), logPath, duration, scenario, runID+"-"+scenario.name)
				results <- omesResult{name: scenario.name, err: err}
			})
		}

		workloadCtx, cancelWorkload := context.WithDeadline(st.Context(), workloadDeadline)
		defer cancelWorkload()
		require.NoError(st, waitForProxyTraffic(workloadCtx, proxy))

		rollOrder := []*serverInstance{instances[0], instances[2], instances[1], instances[3]}
		for i, instance := range rollOrder {
			remainingRolls := len(rollOrder) - i
			dwell := (time.Until(workloadDeadline) - finalSoak) / time.Duration(remainingRolls)
			require.Positive(st, dwell, "workload deadline elapsed before replacing %s", instance.name)
			require.NoError(st, waitForRollDwell(workloadCtx, dwell, results))
			st.Logf("Replacing %s with %v remaining", instance.name, time.Until(workloadDeadline))
			require.NoError(st, replaceServer(t.Context(), workloadCtx, proxy, instance, instances))
		}

		for range scenarios {
			select {
			case result := <-results:
				require.NoError(st, result.err, "Omes %s failed", result.name)
			case <-st.Context().Done():
				st.Fatalf("Omes did not complete: %v", context.Cause(st.Context()))
			}
		}
	})
	if !t.Failed() {
		t.Run("verify", func(st *testing.T) {
			for _, instance := range instances {
				requireServerAlive(st, instance.name, instance.frontendAddress())
				count, ok := proxy.BackendCallCount(instance.name)
				require.True(st, ok)
				st.Logf("Proxy RPCs to %s: %d", instance.name, count)
				require.Positive(st, count, "expected proxy to route RPCs to %s", instance.name)
			}
			for _, version := range []string{"current", "release"} {
				require.Positive(st, proxy.VersionCallCount(version), "expected proxy traffic to %s servers", version)
			}
		})
	}

	// Stop the servers so their logs are fully flushed, then scan them for
	// panics, soft-assertion failures, and other problems that don't surface as
	// a process exit. Runs regardless of whether "verify" failed, since a crashed
	// server's log is exactly what we want to inspect.
	for _, instance := range instances {
		if err := instance.stop(); err != nil {
			t.Errorf("stop %s: %v", instance.name, err)
		}
	}

	t.Run("scan server logs", func(st *testing.T) {
		logPaths := make([]string, 0, len(instances))
		for _, instance := range instances {
			logPaths = append(logPaths, instance.logPath)
		}
		problems, err := scanServerLogs(serverLogValidators, logPaths...)
		require.NoError(st, err)
		for _, p := range problems {
			st.Error(p)
		}
	})
}

type omesResult struct {
	name string
	err  error
}

func instancePorts(instances []*serverInstance) []devserver.Ports {
	ports := make([]devserver.Ports, 0, len(instances))
	for _, instance := range instances {
		if instance.server != nil {
			ports = append(ports, instance.ports)
		}
	}
	return ports
}

func waitForProxyTraffic(ctx context.Context, proxy *frontendProxy) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if proxy.VersionCallCount("current")+proxy.VersionCallCount("release") > 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for Omes proxy traffic: %w", context.Cause(ctx))
		case <-ticker.C:
		}
	}
}

func waitForRollDwell(ctx context.Context, dwell time.Duration, results <-chan omesResult) error {
	timer := time.NewTimer(dwell)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case result := <-results:
		if result.err != nil {
			return fmt.Errorf("Omes %s failed before server roll completed: %w", result.name, result.err)
		}
		return fmt.Errorf("Omes %s completed before server roll completed", result.name)
	case <-ctx.Done():
		return fmt.Errorf("wait before server roll: %w", context.Cause(ctx))
	}
}

func replaceServer(
	startCtx context.Context,
	membershipCtx context.Context,
	proxy *frontendProxy,
	instance *serverInstance,
	instances []*serverInstance,
) error {
	if err := proxy.RemoveBackend(instance.name); err != nil {
		return fmt.Errorf("remove %s from proxy: %w", instance.name, err)
	}

	oldPorts := instance.ports
	if err := instance.stop(); err != nil {
		return fmt.Errorf("stop %s: %w", instance.name, err)
	}
	var clusterEndpoint string
	for _, candidate := range instances {
		if candidate.server != nil {
			clusterEndpoint = candidate.frontendAddress()
			break
		}
	}
	if clusterEndpoint == "" {
		return fmt.Errorf("replace %s: no surviving frontend", instance.name)
	}
	if err := waitForClusterMembership(membershipCtx, instances, []devserver.Ports{oldPorts}); err != nil {
		return fmt.Errorf("wait for %s to leave membership: %w", instance.name, err)
	}
	if err := instance.start(startCtx, clusterEndpoint); err != nil {
		return err
	}
	if err := waitForClusterMembership(membershipCtx, instances, nil); err != nil {
		return fmt.Errorf("wait for replacement %s to join membership: %w", instance.name, err)
	}
	if err := proxy.AddBackend(instance.name, instance.version, instance.frontendAddress()); err != nil {
		return fmt.Errorf("re-add %s to proxy: %w", instance.name, err)
	}
	return nil
}

func waitForClusterMembership(ctx context.Context, instances []*serverInstance, absent []devserver.Ports) error {
	present := instancePorts(instances)
	for _, instance := range instances {
		if instance.server == nil {
			continue
		}
		if err := waitForMembership(ctx, instance.frontendAddress(), present, absent); err != nil {
			return fmt.Errorf("wait for %s membership: %w", instance.name, err)
		}
	}
	return nil
}

// runOmes runs the given Omes scenario against serverAddr under the given run ID.
// Retries if Omes fails due to search attribute not being ready yet.
// Deducts elapsed time from duration on retry so total wall time stays bounded.
func runOmes(
	ctx context.Context,
	t *testing.T,
	binary string,
	serverAddr string,
	logPath string,
	duration time.Duration,
	scenario omesScenario,
	runID string,
) error {
	t.Helper()
	t.Logf("Running Omes %s for %v against %s", scenario.name, duration, serverAddr)

	started := time.Now()
	for {
		remaining := duration - time.Since(started)
		if remaining <= 10*time.Second {
			return fmt.Errorf("Omes never started successfully, check %s", logPath)
		}

		logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return err
		}

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
		cmd := exec.CommandContext(ctx, binary, args...)
		cmd.Env = append(os.Environ(), "GOTOOLCHAIN=auto") // Omes workers may require a newer toolchain
		cmd.Stdout = logFile
		cmd.Stderr = io.MultiWriter(logFile, &buf)
		cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
		cmd.WaitDelay = 15 * time.Second

		err = cmd.Run()
		closeErr := logFile.Close()
		if err != nil && strings.Contains(buf.String(), "no mapping defined for search attribute") {
			if closeErr != nil {
				return fmt.Errorf("close Omes log %s: %w", logPath, closeErr)
			}
			t.Log("Omes failed due to search attributes not ready, retrying...")
			continue
		}
		if err != nil {
			return fmt.Errorf("Omes scenario failed, check %s: %w", logPath, err)
		}
		if closeErr != nil {
			return fmt.Errorf("close Omes log %s: %w", logPath, closeErr)
		}
		return nil
	}
}
