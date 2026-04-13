package testcore

import (
	"fmt"
	"sync"

	"go.temporal.io/server/common/metrics/metricstest"
)

const collectMetricNilKeepPanic = "CollectMetric keep func must not be nil; use Metric(name) to collect all readings"

type GlobalMetricCapture struct {
	capture *metricstest.Capture

	mu             sync.Mutex
	queriedMetrics map[string]struct{}
}

func newGlobalMetricCapture(capture *metricstest.Capture) *GlobalMetricCapture {
	return &GlobalMetricCapture{
		capture:        capture,
		queriedMetrics: make(map[string]struct{}),
	}
}

func (c *GlobalMetricCapture) Metric(name string) []*metricstest.CapturedRecording {
	return c.collectMetric(name, nil)
}

// CollectMetric returns the recordings for the named metric that the caller chooses to keep.
func (c *GlobalMetricCapture) CollectMetric(name string, keep func(*metricstest.CapturedRecording) bool) []*metricstest.CapturedRecording {
	if keep == nil {
		panic(collectMetricNilKeepPanic)
	}
	return c.collectMetric(name, keep)
}

func (c *GlobalMetricCapture) collectMetric(name string, keep func(*metricstest.CapturedRecording) bool) []*metricstest.CapturedRecording {
	c.mu.Lock()
	c.queriedMetrics[name] = struct{}{}
	c.mu.Unlock()

	keepAll := keep == nil
	var collected []*metricstest.CapturedRecording
	for _, rec := range c.capture.Snapshot()[name] {
		if keepAll || keep(rec) {
			collected = append(collected, rec)
		}
	}
	return collected
}

// checkForNamespaceCaptureMisuse panics when all queried metrics that produced
// recordings were namespace-scoped. Metrics with no recordings are treated as
// inconclusive and do not trigger misuse detection.
func (c *GlobalMetricCapture) checkForNamespaceCaptureMisuse() {
	queriedMetrics := c.queriedMetricNames()

	if len(queriedMetrics) == 0 {
		return
	}

	if allQueriedMetricsAreNamespaceScoped(c.capture.Snapshot(), queriedMetrics) {
		panic("GlobalMetricCapture was used, but all queried metrics were namespace-scoped; use NamespaceMetricCapture instead")
	}
}

func (c *GlobalMetricCapture) queriedMetricNames() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	queriedMetrics := make([]string, 0, len(c.queriedMetrics))
	for name := range c.queriedMetrics {
		queriedMetrics = append(queriedMetrics, name)
	}
	return queriedMetrics
}

func allQueriedMetricsAreNamespaceScoped(snap metricstest.CaptureSnapshot, queriedMetrics []string) bool {
	for _, name := range queriedMetrics {
		recordings := snap[name]
		if len(recordings) == 0 {
			return false
		}
		for _, rec := range recordings {
			if _, ok := rec.Tags["namespace"]; !ok {
				return false
			}
		}
	}
	return true
}

type NamespaceMetricCapture struct {
	capture   *metricstest.Capture
	namespace string
}

func newNamespaceMetricCapture(capture *metricstest.Capture, namespace string) *NamespaceMetricCapture {
	return &NamespaceMetricCapture{
		capture:   capture,
		namespace: namespace,
	}
}

// ForNamespace returns a view of the same capture filtered to the given namespace.
func (c *NamespaceMetricCapture) ForNamespace(namespace string) *NamespaceMetricCapture {
	return &NamespaceMetricCapture{
		capture:   c.capture,
		namespace: namespace,
	}
}

func (c *NamespaceMetricCapture) Metric(name string) []*metricstest.CapturedRecording {
	return c.collectMetric(name, nil)
}

// CollectMetric returns the recordings for the named metric that belong to the test namespace
// and that the caller chooses to keep. It panics if the requested metric is not namespace-scoped.
func (c *NamespaceMetricCapture) CollectMetric(name string, keep func(*metricstest.CapturedRecording) bool) []*metricstest.CapturedRecording {
	if keep == nil {
		panic(collectMetricNilKeepPanic)
	}
	return c.collectMetric(name, keep)
}

func (c *NamespaceMetricCapture) collectMetric(name string, keep func(*metricstest.CapturedRecording) bool) []*metricstest.CapturedRecording {
	keepAll := keep == nil
	var collected []*metricstest.CapturedRecording
	for _, rec := range c.capture.Snapshot()[name] {
		namespace, ok := rec.Tags["namespace"]
		if !ok {
			panic(fmt.Sprintf("metric %q is not namespace-scoped; use GlobalMetricCapture instead", name))
		}
		if namespace == c.namespace && (keepAll || keep(rec)) {
			collected = append(collected, rec)
		}
	}
	return collected
}
