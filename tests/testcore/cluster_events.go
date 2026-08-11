package testcore

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"maps"
	"os"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"time"

	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
)

const (
	clusterEventTypeRunStarted  = "run_started"
	clusterEventTypeRunFinished = "run_finished"
	clusterEventTypeCreated     = "cluster_created"
	clusterEventTypeAcquired    = "cluster_acquired"
	clusterEventTypeDestroyed   = "cluster_destroyed"
	clusterEventTypeNamespace   = "namespace_registered"
	clusterEventTypeRuntime     = "runtime"
)

type clusterEvent struct {
	Type           string               `json:"type"`
	Timestamp      time.Time            `json:"timestamp"`
	ClusterID      int64                `json:"cluster_id,omitempty"`
	Suite          string               `json:"suite,omitempty"`
	Test           string               `json:"test,omitempty"`
	Kind           string               `json:"kind,omitempty"`
	Reason         string               `json:"reason,omitempty"`
	Worker         bool                 `json:"worker,omitempty"`
	DurationMS     float64              `json:"duration_ms,omitempty"`
	AcquireMS      float64              `json:"acquire_ms,omitempty"`
	AcquireSource  clusterAcquireSource `json:"acquire_source,omitempty"`
	LiveClusters   int                  `json:"live_clusters"`
	Namespaces     int                  `json:"namespaces,omitempty"`
	Namespace      string               `json:"namespace,omitempty"`
	PhasesMS       map[string]float64   `json:"phases_ms,omitempty"`
	Goroutines     int                  `json:"goroutines,omitempty"`
	HeapInUseBytes uint64               `json:"heap_in_use_bytes,omitempty"`
	SysBytes       uint64               `json:"sys_bytes,omitempty"`
	RSSBytes       uint64               `json:"rss_bytes,omitempty"`
	ExitCode       int                  `json:"exit_code,omitempty"`
}

type clusterCreationEvent struct {
	suite      string
	test       string
	kind       string
	reason     string
	worker     bool
	duration   time.Duration
	namespaces int
	phases     map[string]time.Duration
}

type runtimeSample struct {
	Goroutines   int
	HeapInUse    uint64
	Sys          uint64
	RSS          uint64
	LiveClusters int
}

type clusterEventRecorder struct {
	mu sync.Mutex

	writer          io.Writer
	now             func() time.Time
	samplerInterval time.Duration
	nextID          int64
	liveClusters    int
	liveIDs         map[int64]struct{}
}

type bootPhaseDurations struct {
	mu     sync.Mutex
	phases map[string]time.Duration
}

func newClusterEventRecorder(writer io.Writer) *clusterEventRecorder {
	return &clusterEventRecorder{
		writer:          writer,
		now:             time.Now,
		samplerInterval: time.Second,
		liveIDs:         make(map[int64]struct{}),
	}
}

func newBootPhaseDurations() *bootPhaseDurations {
	return &bootPhaseDurations{phases: make(map[string]time.Duration)}
}

func (d *bootPhaseDurations) record(phase string, duration time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.phases[phase] += duration
}

func (d *bootPhaseDurations) snapshot() map[string]time.Duration {
	d.mu.Lock()
	defer d.mu.Unlock()
	result := make(map[string]time.Duration, len(d.phases))
	maps.Copy(result, d.phases)
	return result
}

func (r *clusterEventRecorder) nextClusterID() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextID++
	return r.nextID
}

func (r *clusterEventRecorder) recordRunStarted() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writeLocked(clusterEvent{
		Type:      clusterEventTypeRunStarted,
		Timestamp: r.now(),
	})
}

func (r *clusterEventRecorder) recordRunFinished(exitCode int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	liveIDs := make([]int64, 0, len(r.liveIDs))
	for clusterID := range r.liveIDs {
		liveIDs = append(liveIDs, clusterID)
	}
	slices.Sort(liveIDs)
	for _, clusterID := range liveIDs {
		delete(r.liveIDs, clusterID)
		r.liveClusters--
		r.writeLocked(clusterEvent{
			Type:         clusterEventTypeDestroyed,
			Timestamp:    r.now(),
			ClusterID:    clusterID,
			LiveClusters: r.liveClusters,
		})
	}
	r.writeLocked(clusterEvent{
		Type:         clusterEventTypeRunFinished,
		Timestamp:    r.now(),
		LiveClusters: r.liveClusters,
		ExitCode:     exitCode,
	})
}

// RunTests records process-level boundaries around a test run when cluster event collection is enabled.
func RunTests(run func() int) int {
	recorder := testClusterRouter.events
	if recorder == nil {
		recorder = newClusterEventRecorderFromEnvironment()
		testClusterRouter.events = recorder
	}
	stopRuntimeSampler := func() {}
	if recorder != nil {
		recorder.recordRunStarted()
		recorder.sampleRuntime()
		stopRuntimeSampler = recorder.startRuntimeSampler(recorder.samplerInterval)
	}
	if testClusterRouter.perTest != nil {
		testParallelFlag := flag.Lookup("test.parallel")
		testParallelism, err := strconv.Atoi(testParallelFlag.Value.String())
		if err != nil {
			panic("invalid -test.parallel value")
		}
		testParallelism = testClusterRouter.perTest.testParallelism(testParallelism)
		if err := testParallelFlag.Value.Set(strconv.Itoa(testParallelism)); err != nil {
			panic("cannot set -test.parallel value")
		}
		_ = testClusterRouter.perTest.startAndWait()
	}
	exitCode := run()
	if testClusterRouter.perTest != nil {
		testClusterRouter.perTest.close()
	}
	if err := persistencetests.CloseReusableCassandraDatabases(); err != nil {
		log.Printf("failed to close reusable Cassandra databases: %v", err)
		exitCode = 1
	}
	if recorder != nil {
		stopRuntimeSampler()
		recorder.sampleRuntime()
		recorder.recordRunFinished(exitCode)
		recorder.close()
	}
	return exitCode
}

func newClusterEventRecorderFromEnvironment() *clusterEventRecorder {
	path := os.Getenv("TEMPORAL_TEST_CLUSTER_EVENTS_FILE")
	if path == "" {
		return nil
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Printf("cluster events disabled: cannot open %q: %v", path, err)
		return nil
	}
	return newClusterEventRecorder(f)
}

func (r *clusterEventRecorder) recordClusterCreated(clusterID int64, creation clusterCreationEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.liveClusters++
	r.liveIDs[clusterID] = struct{}{}
	r.writeLocked(clusterEvent{
		Type:         clusterEventTypeCreated,
		Timestamp:    r.now(),
		ClusterID:    clusterID,
		Suite:        creation.suite,
		Test:         creation.test,
		Kind:         creation.kind,
		Reason:       creation.reason,
		Worker:       creation.worker,
		DurationMS:   durationMilliseconds(creation.duration),
		LiveClusters: r.liveClusters,
		Namespaces:   creation.namespaces,
		PhasesMS:     phaseMilliseconds(creation.phases),
	})
}

func (r *clusterEventRecorder) recordClusterAcquire(
	clusterID int64,
	test string,
	wait time.Duration,
	source clusterAcquireSource,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writeLocked(clusterEvent{
		Type:          clusterEventTypeAcquired,
		Timestamp:     r.now(),
		ClusterID:     clusterID,
		Test:          test,
		AcquireMS:     durationMilliseconds(wait),
		AcquireSource: source,
		LiveClusters:  r.liveClusters,
	})
}

func (r *clusterEventRecorder) recordClusterDestroyed(clusterID int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.liveIDs[clusterID]; ok {
		delete(r.liveIDs, clusterID)
		r.liveClusters--
	}
	r.writeLocked(clusterEvent{
		Type:         clusterEventTypeDestroyed,
		Timestamp:    r.now(),
		ClusterID:    clusterID,
		LiveClusters: r.liveClusters,
	})
}

func (r *clusterEventRecorder) recordNamespaceRegistered(clusterID int64, namespace string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writeLocked(clusterEvent{
		Type:         clusterEventTypeNamespace,
		Timestamp:    r.now(),
		ClusterID:    clusterID,
		Namespace:    namespace,
		LiveClusters: r.liveClusters,
	})
}

func (r *clusterEventRecorder) recordRuntimeSample(sample runtimeSample) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writeLocked(clusterEvent{
		Type:           clusterEventTypeRuntime,
		Timestamp:      r.now(),
		LiveClusters:   sample.LiveClusters,
		Goroutines:     sample.Goroutines,
		HeapInUseBytes: sample.HeapInUse,
		SysBytes:       sample.Sys,
		RSSBytes:       sample.RSS,
	})
}

func (r *clusterEventRecorder) startRuntimeSampler(interval time.Duration) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				r.sampleRuntime()
			case <-stop:
				return
			}
		}
	}()
	var stopOnce sync.Once
	return func() {
		stopOnce.Do(func() {
			close(stop)
			<-done
		})
	}
}

func (r *clusterEventRecorder) sampleRuntime() {
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	r.mu.Lock()
	liveClusters := r.liveClusters
	r.mu.Unlock()
	r.recordRuntimeSample(runtimeSample{
		Goroutines:   runtime.NumGoroutine(),
		HeapInUse:    memory.HeapInuse,
		Sys:          memory.Sys,
		RSS:          processRSSBytes(),
		LiveClusters: liveClusters,
	})
}

func (r *clusterEventRecorder) writeLocked(event clusterEvent) {
	line, err := json.Marshal(event)
	if err != nil {
		return
	}
	_, _ = r.writer.Write(append(line, '\n'))
}

func (r *clusterEventRecorder) close() {
	if closer, ok := r.writer.(io.Closer); ok {
		_ = closer.Close()
	}
}

func durationMilliseconds(duration time.Duration) float64 {
	return float64(duration) / float64(time.Millisecond)
}

func phaseMilliseconds(phases map[string]time.Duration) map[string]float64 {
	result := make(map[string]float64, len(phases))
	for phase, duration := range phases {
		result[phase] = durationMilliseconds(duration)
	}
	return result
}
