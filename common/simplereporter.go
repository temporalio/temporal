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

		messagesSentCount   int64
		messagesErrorCount  int64
		messagesSentLatency int64

		messagesReceivedCount          int64
		messagesReceivedDuplicateCount int64
		messagesEndToEndLatency        int64
		messagesCorruptedCounter       int64

		previousReportTime   time.Time
		previousPublishCount int64
		previousConsumeCount int64
		previousLatency      int64
	}

	simpleStopWatch struct {
		metricName string
		reporter   *SimpleReporter
		startTime  time.Time
		elasped    time.Duration
	}
)

// Generator metric
const (
	MessagesSentTotalCounter = "messages-sent-total"
	MessagesSentErrorCounter = "messages-sent-error"
	MessagesSentLatency      = "messages-sent-latency"
)

// Processor metric
const (
	MessagesReceivedTotalCounter     = "messages-received-total"
	MessagesReceivedDuplicateCounter = "messages-received-duplicate"
	MessagesEndToEndLatency          = "messages-endtoend-latency"
	MessagesCorruptedCounter         = "messages-corrupted-total"
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

	// Initialize generator metric
	reporter.messagesSentCount = 0
	reporter.messagesErrorCount = 0
	reporter.messagesSentLatency = 0

	// Initialize processor metric
	reporter.messagesReceivedCount = 0
	reporter.messagesReceivedDuplicateCount = 0
	reporter.messagesEndToEndLatency = 0
	reporter.messagesCorruptedCounter = 0

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
	case MessagesSentTotalCounter:
		atomic.AddInt64(&r.messagesSentCount, delta)
	case MessagesSentErrorCounter:
		atomic.AddInt64(&r.messagesErrorCount, delta)
	case MessagesSentLatency:
		atomic.AddInt64(&r.messagesSentLatency, delta)
	case MessagesReceivedTotalCounter:
		atomic.AddInt64(&r.messagesReceivedCount, delta)
	case MessagesReceivedDuplicateCounter:
		atomic.AddInt64(&r.messagesReceivedDuplicateCount, delta)
	case MessagesEndToEndLatency:
		atomic.AddInt64(&r.messagesEndToEndLatency, delta)
	case MessagesCorruptedCounter:
		atomic.AddInt64(&r.messagesCorruptedCounter, delta)
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

	total := atomic.LoadInt64(&r.messagesSentCount)
	errors := atomic.LoadInt64(&r.messagesErrorCount)
	latency := atomic.LoadInt64(&r.messagesSentLatency)
	publishThroughput := int64(0)
	if elapsed > 0 && total > r.previousPublishCount {
		publishThroughput = (total - r.previousPublishCount) / int64(elapsed)
	}

	messageCount := atomic.LoadInt64(&r.messagesReceivedCount)
	messageDupCount := atomic.LoadInt64(&r.messagesReceivedDuplicateCount)
	totalLatency := atomic.LoadInt64(&r.messagesEndToEndLatency)
	receiveThroughput := int64(0)
	endToEndLatency := int64(0)
	if elapsed > 0 && messageCount > r.previousConsumeCount {
		currentMessageCount := messageCount - r.previousConsumeCount
		receiveThroughput = currentMessageCount / int64(elapsed)
		currentLatency := totalLatency - r.previousLatency
		endToEndLatency = currentLatency / currentMessageCount
	}

	if total > 0 {
		log.Infof("Publish(S=%v, L=%v, E=%v, T=%v)-Consume(R=%v, L=%v, D=%v, T=%v)",
			total, latency/total, errors, publishThroughput,
			messageCount, endToEndLatency, messageDupCount, receiveThroughput)
	}

	r.previousPublishCount = total
	r.previousConsumeCount = messageCount
	r.previousLatency = totalLatency
	r.previousReportTime = currentTime
}

// PrintGeneratorMetric prints the metrics for generator
func (r *SimpleReporter) PrintGeneratorMetric() {
	total := atomic.LoadInt64(&r.messagesSentCount)
	errors := atomic.LoadInt64(&r.messagesErrorCount)
	latency := atomic.LoadInt64(&r.messagesSentLatency)

	if total > 0 {
		log.Infof("Total Messages Sent: %v, Average Latency: %v, Errors: %v",
			total, latency/total, errors)
	}
}

// PrintProcessorMetric prints the processor metrics
func (r *SimpleReporter) PrintProcessorMetric(startTime time.Time, totalMessageCount uint64) {
	messageCount := atomic.LoadInt64(&r.messagesReceivedCount)
	messageDupCount := atomic.LoadInt64(&r.messagesReceivedDuplicateCount)
	totalLatency := atomic.LoadInt64(&r.messagesEndToEndLatency)
	corruptedCount := atomic.LoadInt64(&r.messagesCorruptedCounter)

	if messageCount > 0 {
		elapsed := time.Since(startTime) / time.Second
		throughput := messageCount / int64(elapsed)
		latency := totalLatency / messageCount

		log.Infof("Total messages processed: %v, duplicates: %v, Throughput: %v, Average Latency: %v, Corrupted: %v",
			totalMessageCount, messageDupCount, throughput, time.Duration(latency), corruptedCount)
	}
}

// ResetMetric resets the metric values to zero
func (r *SimpleReporter) ResetMetric() {
	// Reset generator metric
	atomic.StoreInt64(&r.messagesSentCount, 0)
	atomic.StoreInt64(&r.messagesErrorCount, 0)
	atomic.StoreInt64(&r.messagesSentLatency, 0)

	// Reset processor metric
	atomic.StoreInt64(&r.messagesReceivedCount, 0)
	atomic.StoreInt64(&r.messagesEndToEndLatency, 0)
	atomic.StoreInt64(&r.messagesCorruptedCounter, 0)
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
