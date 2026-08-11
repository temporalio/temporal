package testcore

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testlogger"
)

// This file is the measurement baseline for functional test cluster startup.
// See FASTBOOT.md at the repo root for the recorded numbers and the analysis
// derived from them.
//
// Wall-clock numbers are machine dependent. The allocation, object, goroutine
// and stack numbers are not, so prefer those when comparing across machines.

// BenchmarkClusterBoot measures what a functional test pays to get a usable
// cluster: everything setupCluster does, split into phases. Every number is per
// cluster creation; the cluster pool only amortizes it over the tests that
// manage to share a cluster.
//
// Reported metrics (per boot):
//
//	persistence_ms      create the test DB and build the persistence managers
//	cluster-metadata_ms write cluster metadata rows, reconcile them into config
//	fx-graph_ms         temporal.NewServer: server graph + one graph per service
//	service-start_ms    fx lifecycle start hooks for all services
//	namespaces_ms       seed the two per-cluster namespaces (variants only)
//	boot_MB             heap bytes allocated during boot
//	boot_Mallocs        heap objects allocated during boot
//	goroutines          live goroutines added by the booted cluster
//
// Teardown is excluded from ns/op; TestClusterBootScaling covers it.
//
//	go test -tags test_dep ./tests/testcore -run '^$' -bench BenchmarkClusterBoot -benchtime 5x
//
// Add -cpuprofile/-memprofile to attribute a phase. Note that the default test
// log level is debug, which puts server logging on the profile; set
// TEMPORAL_TEST_LOG_LEVEL=error to measure without it.
func BenchmarkClusterBoot(b *testing.B) {
	for _, variant := range []struct {
		name string
		// withWorkerService starts the system worker service, which most tests
		// do not need but which a dedicated cluster may request.
		withWorkerService bool
		// withNamespaces seeds the two namespaces that setupCluster creates
		// directly into persistence before the server starts.
		withNamespaces bool
	}{
		{name: "Core"},
		{name: "CoreAndNamespaces", withNamespaces: true},
		{name: "WorkerService", withWorkerService: true},
		{name: "WorkerServiceAndNamespaces", withWorkerService: true, withNamespaces: true},
	} {
		b.Run(variant.name, func(b *testing.B) {
			benchmarkClusterBoot(b, variant.withWorkerService, variant.withNamespaces)
		})
	}
}

func benchmarkClusterBoot(b *testing.B, withWorkerService, withNamespaces bool) {
	phases := observeBootPhases(b)

	var (
		allocBytes uint64
		mallocs    uint64
		goroutines int64
	)

	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		//nolint:revive // The heap delta below is only meaningful from a collected baseline.
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		goroutinesBefore := runtime.NumGoroutine()
		b.StartTimer()

		cluster, _ := newBenchCluster(b, bootOptions{
			withWorkerService: withWorkerService,
			withNamespaces:    withNamespaces,
		})

		b.StopTimer()
		// Sample goroutines and heap growth while the cluster is still up.
		goroutines += int64(runtime.NumGoroutine() - goroutinesBefore)
		runtime.ReadMemStats(&after)
		allocBytes += after.TotalAlloc - before.TotalAlloc
		mallocs += after.Mallocs - before.Mallocs
		require.NoError(b, cluster.TearDownCluster())
		b.StartTimer()
	}

	iterations := float64(max(b.N, 1))
	for _, phase := range []string{
		bootPhasePersistence,
		bootPhaseClusterMetadata,
		bootPhaseFxGraph,
		bootPhaseServiceStart,
	} {
		b.ReportMetric(float64(phases.total(phase).Milliseconds())/iterations, phase+"_ms")
	}
	if withNamespaces {
		b.ReportMetric(float64(phases.total(bootPhaseNamespaces).Milliseconds())/iterations, "namespaces_ms")
	}
	b.ReportMetric(float64(allocBytes)/iterations/(1<<20), "boot_MB")
	b.ReportMetric(float64(mallocs)/iterations, "boot_Mallocs")
	b.ReportMetric(float64(goroutines)/iterations, "goroutines")
}

// TestClusterBootBaseline records one cluster boot as a test rather than a
// benchmark, so the phase and resource breakdown shows up in a plain `go test -v`
// run without a separate benchmark invocation.
func TestClusterBootBaseline(t *testing.T) {
	phases := observeBootPhases(t)

	//nolint:revive // The heap delta below is only meaningful from a collected baseline.
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	goroutinesBefore := runtime.NumGoroutine()

	cluster, _ := newBenchCluster(t, bootOptions{withNamespaces: true})

	goroutines := runtime.NumGoroutine() - goroutinesBefore
	runtime.ReadMemStats(&after)

	teardownStart := time.Now()
	require.NoError(t, cluster.TearDownCluster())

	for _, phase := range []string{
		bootPhasePersistence,
		bootPhaseClusterMetadata,
		bootPhaseFxGraph,
		bootPhaseServiceStart,
		bootPhaseTotal,
	} {
		t.Logf("%-18s %6d ms", phase, phases.total(phase).Milliseconds())
	}
	t.Logf("%-18s %6d ms (2 namespaces)", "namespaces", phases.total(bootPhaseNamespaces).Milliseconds())
	t.Logf("%-18s %6d ms", "teardown", time.Since(teardownStart).Milliseconds())
	t.Logf("%-18s %6d MB", "boot alloc", (after.TotalAlloc-before.TotalAlloc)/(1<<20))
	t.Logf("%-18s %6d k", "boot objects", (after.Mallocs-before.Mallocs)/1000)
	t.Logf("%-18s %6d MB", "stacks", (after.StackSys-before.StackSys)/(1<<20))
	t.Logf("%-18s %6d", "goroutines", goroutines)
	t.Logf("%-18s %6d", "GOMAXPROCS", runtime.GOMAXPROCS(0))
}

// TestClusterBootScaling measures the cost of holding several clusters at once,
// which is what the cluster pools actually do: the shared pool keeps
// GOMAXPROCS/2 clusters and the dedicated pool up to GOMAXPROCS. Per-cluster
// goroutines and stack memory are multiplied by that factor for the whole test
// binary, so this is the number that bounds how much parallelism a test run can
// afford.
func TestClusterBootScaling(t *testing.T) {
	const clusterCount = 8

	//nolint:revive // The heap delta below is only meaningful from a collected baseline.
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	goroutinesBefore := runtime.NumGoroutine()

	clusters := make([]*TestCluster, clusterCount)
	start := time.Now()
	var wg sync.WaitGroup
	for i := range clusters {
		wg.Go(func() {
			clusters[i], _ = newBenchCluster(t, bootOptions{})
		})
	}
	wg.Wait()
	bootTime := time.Since(start)

	goroutines := runtime.NumGoroutine() - goroutinesBefore
	runtime.ReadMemStats(&after)

	teardownStart := time.Now()
	var teardownWG sync.WaitGroup
	for _, cluster := range clusters {
		teardownWG.Go(func() { require.NoError(t, cluster.TearDownCluster()) })
	}
	teardownWG.Wait()

	t.Logf("%d clusters: boot=%dms teardown=%dms goroutines=%d stacks=%dMB heapInUse=%dMB",
		clusterCount,
		bootTime.Milliseconds(),
		time.Since(teardownStart).Milliseconds(),
		goroutines,
		(after.StackSys-before.StackSys)/(1<<20),
		(after.HeapInuse-before.HeapInuse)/(1<<20),
	)
}

type bootOptions struct {
	withWorkerService bool
	withNamespaces    bool
}

// newBenchCluster boots a cluster through the same path as setupCluster and
// returns the logger it was given so callers can reuse it for post-boot work.
func newBenchCluster(t testing.TB, opts bootOptions) (*TestCluster, log.Logger) {
	logger := testlogger.NewTestLogger(&sharedClusterT{name: t.Name()}, testlogger.FailOnExpectedErrorOnly)
	logger.Expect(testlogger.Error, ".*", tag.FailedAssertion)

	clusterConfig := &TestClusterConfig{
		HistoryConfig:        HistoryConfig{NumHistoryShards: 4},
		EnableMetricsCapture: true,
		WorkerConfig:         WorkerConfig{DisableWorker: !opts.withWorkerService},
	}
	if opts.withNamespaces {
		clusterConfig.preseededNamespaces = []preseededNamespace{
			newPreseededNamespace(namespace.Name(RandomizeStr("namespace"))),
			newPreseededNamespace(namespace.Name(RandomizeStr("external-namespace"))),
		}
	}
	cluster, err := NewTestClusterFactory().NewCluster(t, clusterConfig, logger)
	require.NoError(t, err)
	return cluster, logger
}

// bootPhaseTotals accumulates per-phase durations reported by bootPhaseObserver.
type bootPhaseTotals struct {
	mu     sync.Mutex
	phases map[string]time.Duration
}

func (b *bootPhaseTotals) total(phase string) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.phases[phase]
}

// observeBootPhases installs a boot phase observer for the duration of t.
func observeBootPhases(t testing.TB) *bootPhaseTotals {
	totals := &bootPhaseTotals{phases: make(map[string]time.Duration)}
	observer := func(phase string, d time.Duration) {
		totals.mu.Lock()
		defer totals.mu.Unlock()
		totals.phases[phase] += d
	}
	bootPhaseObserver.Store(&observer)
	t.Cleanup(func() { bootPhaseObserver.Store(nil) })
	return totals
}
