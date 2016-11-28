package common

import (
	"sync/atomic"
	"time"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/go-common.git/x/metrics"
)

type (
	// SimpleReporter is the reporter used to dump metric to console for stress runs
	SimpleReporter struct {
		scope metrics.Scope
		tags  map[string]string

		startTime                time.Time
		workflowsStartCount      int64
		activitiesTotalCount     int64
		decisionsTotalCount      int64
		workflowsCompletionCount int64
		workflowsEndToEndLatency int64

		previousReportTime               time.Time
		previousWorkflowsStartCount      int64
		previousActivitiesTotalCount     int64
		previousDecisionsTotalCount      int64
		previousWorkflowsCompletionCount int64
		previousWorkflowsEndToEndLatency int64
	}

	simpleStopWatch struct {
		metricName string
		reporter   *SimpleReporter
		startTime  time.Time
		elasped    time.Duration
	}
)

// Workflow Creation metrics
const (
	WorkflowsStartTotalCounter      = "workflows-start-total"
	ActivitiesTotalCounter          = "activities-total"
	DecisionsTotalCounter           = "decisions-total"
	WorkflowEndToEndLatency         = "workflows-endtoend-latency"
	WorkflowsCompletionTotalCounter = "workflows-completion-total"
)

// NewSimpleReporter create an instance of Reporter which can be used for driver to emit metric to console
func NewSimpleReporter(scope metrics.Scope, tags map[string]string) Reporter {
	reporter := &SimpleReporter{
		scope: scope,
		tags:  make(map[string]string),
	}

	if tags != nil {
		copyMap(tags, reporter.tags)
	}

	// Initialize metric
	reporter.workflowsStartCount = 0
	reporter.activitiesTotalCount = 0
	reporter.decisionsTotalCount = 0
	reporter.workflowsCompletionCount = 0
	reporter.workflowsEndToEndLatency = 0
	reporter.startTime = time.Now()

	return reporter
}

// InitMetrics is used to initialize the metrics map with the respective type
func (r *SimpleReporter) InitMetrics(metricMap map[MetricName]MetricType) {
	// This is a no-op for simple reporter as it is already have a static list of metric to work with
}

// GetChildReporter creates the child reporter for this parent reporter
func (r *SimpleReporter) GetChildReporter(tags map[string]string) Reporter {
	sr := NewSimpleReporter(r.GetScope(), tags)

	// copy the parent tags as well
	copyMap(r.GetTags(), sr.GetTags())

	return sr
}

// GetTags returns the tags for this reporter object
func (r *SimpleReporter) GetTags() map[string]string {
	return r.tags
}

// GetScope returns the metrics scope for this reporter
func (r *SimpleReporter) GetScope() metrics.Scope {
	return r.scope
}

// IncCounter reports Counter metric to M3
func (r *SimpleReporter) IncCounter(name string, tags map[string]string, delta int64) {
	switch name {
	case WorkflowsStartTotalCounter:
		atomic.AddInt64(&r.workflowsStartCount, delta)
	case ActivitiesTotalCounter:
		atomic.AddInt64(&r.activitiesTotalCount, delta)
	case DecisionsTotalCounter:
		atomic.AddInt64(&r.decisionsTotalCount, delta)
	case WorkflowsCompletionTotalCounter:
		atomic.AddInt64(&r.workflowsCompletionCount, delta)
	case WorkflowEndToEndLatency:
		atomic.AddInt64(&r.workflowsEndToEndLatency, delta)
	default:
		log.WithField(`name`, name).Error(`Unknown metric`)
	}
}

// UpdateGauge reports Gauge type metric
func (r *SimpleReporter) UpdateGauge(name string, tags map[string]string, value int64) {
	// Not implemented
}

// StartTimer returns a Stopwatch which when stopped will report the metric to M3
func (r *SimpleReporter) StartTimer(name string, tags map[string]string) Stopwatch {
	w := newSimpleStopWatch(name, r)
	w.Start()

	return w
}

// RecordTimer should be used for measuring latency when you cannot start the stop watch.
func (r *SimpleReporter) RecordTimer(name string, tags map[string]string, d time.Duration) {
	// Record the time as counter of time in milliseconds
	timeToRecord := int64(d / time.Millisecond)
	r.IncCounter(name, tags, timeToRecord)
}

// PrintStressMetric is used by stress host to dump metric to logs
func (r *SimpleReporter) PrintStressMetric() {

	currentTime := time.Now()
	elapsed := time.Duration(0)
	if !r.previousReportTime.IsZero() {
		elapsed = currentTime.Sub(r.previousReportTime) / time.Second
	}

	totalWorkflowStarted := atomic.LoadInt64(&r.workflowsStartCount)
	creationThroughput := int64(0)
	if elapsed > 0 && totalWorkflowStarted > r.previousWorkflowsStartCount {
		creationThroughput = (totalWorkflowStarted - r.previousWorkflowsStartCount) / int64(elapsed)
	}

	totalActivitiesCount := atomic.LoadInt64(&r.activitiesTotalCount)
	activitiesThroughput := int64(0)
	if elapsed > 0 && totalActivitiesCount > r.previousActivitiesTotalCount {
		activitiesThroughput = (totalActivitiesCount - r.previousActivitiesTotalCount) / int64(elapsed)
	}

	totalDecisionsCount := atomic.LoadInt64(&r.decisionsTotalCount)
	decisionsThroughput := int64(0)
	if elapsed > 0 && totalDecisionsCount > r.previousDecisionsTotalCount {
		decisionsThroughput = (totalDecisionsCount - r.previousDecisionsTotalCount) / int64(elapsed)
	}

	totalWorkflowsCompleted := atomic.LoadInt64(&r.workflowsCompletionCount)
	completionThroughput := int64(0)
	if elapsed > 0 && totalWorkflowsCompleted > r.previousWorkflowsCompletionCount {
		completionThroughput = (totalWorkflowsCompleted - r.previousWorkflowsCompletionCount) / int64(elapsed)
	}

	var latency int64
	workflowsLatency := atomic.LoadInt64(&r.workflowsEndToEndLatency)
	if totalWorkflowsCompleted > 0 && workflowsLatency > r.previousWorkflowsEndToEndLatency {
		currentLatency := workflowsLatency - r.previousWorkflowsEndToEndLatency
		latency = currentLatency / totalWorkflowsCompleted
	}

	log.Infof("Workflows Started(Count=%v, Throughput=%v)", totalWorkflowStarted, creationThroughput)
	log.Infof("Workflows Completed(Count=%v, Throughput=%v, Average Latency: %v)", totalWorkflowsCompleted, completionThroughput, latency)
	log.Infof("Activites(Count=%v, Throughput=%v)", totalActivitiesCount, activitiesThroughput)
	log.Infof("Decisions(Count=%v, Throughput=%v)", totalDecisionsCount, decisionsThroughput)

	r.previousWorkflowsStartCount = totalWorkflowStarted
	r.previousActivitiesTotalCount = totalActivitiesCount
	r.previousDecisionsTotalCount = totalDecisionsCount
	r.previousWorkflowsCompletionCount = totalWorkflowsCompleted
	r.previousWorkflowsEndToEndLatency = workflowsLatency
	r.previousReportTime = currentTime
}

// PrintFinalMetric prints the workflows metrics
func (r *SimpleReporter) PrintFinalMetric() {
	workflowsCount := atomic.LoadInt64(&r.workflowsStartCount)
	workflowsCompletedCount := atomic.LoadInt64(&r.workflowsCompletionCount)
	totalLatency := atomic.LoadInt64(&r.workflowsEndToEndLatency)
	activitiesCount := atomic.LoadInt64(&r.activitiesTotalCount)
	decisionsCount := atomic.LoadInt64(&r.decisionsTotalCount)

	var throughput int64
	var latency int64
	if workflowsCompletedCount > 0 {
		elapsed := time.Since(r.startTime) / time.Second
		throughput = workflowsCompletedCount / int64(elapsed)
		latency = totalLatency / workflowsCompletedCount
	}

	log.Infof("Total workflows processed:(Started=%v, Completed=%v), Throughput: %v, Average Latency: %v",
		workflowsCount, workflowsCompletedCount, throughput, time.Duration(latency))
	log.Infof("Total activites processed: %v, decisions processed: %v", activitiesCount, decisionsCount)
}

// IsProcessComplete  indicates if we have completed processing.
func (r *SimpleReporter) IsProcessComplete() bool {
	totalWorkflowStarted := atomic.LoadInt64(&r.workflowsStartCount)
	totalWorkflowsCompleted := atomic.LoadInt64(&r.workflowsCompletionCount)
	return totalWorkflowStarted > 0 && totalWorkflowStarted == totalWorkflowsCompleted
}

// ResetMetric resets the metric values to zero
func (r *SimpleReporter) ResetMetric() {
	// Reset workflow metric
	atomic.StoreInt64(&r.workflowsStartCount, 0)
	atomic.StoreInt64(&r.workflowsCompletionCount, 0)
	atomic.StoreInt64(&r.activitiesTotalCount, 0)
	atomic.StoreInt64(&r.decisionsTotalCount, 0)
	atomic.StoreInt64(&r.workflowsEndToEndLatency, 0)
}

func newSimpleStopWatch(metricName string, reporter *SimpleReporter) *simpleStopWatch {
	watch := &simpleStopWatch{
		metricName: metricName,
		reporter:   reporter,
	}

	return watch
}

func (w *simpleStopWatch) Start() {
	w.startTime = time.Now()
}

func (w *simpleStopWatch) Stop() time.Duration {
	w.elasped = time.Since(w.startTime)
	w.reporter.IncCounter(w.metricName, nil, w.milliseconds())

	return w.elasped
}

func (w *simpleStopWatch) milliseconds() int64 {
	return int64(w.elasped / time.Millisecond)
}

func (w *simpleStopWatch) microseconds() int64 {
	return int64(w.elasped / time.Microsecond)
}
