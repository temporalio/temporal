// The MIT License

//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package metrics

// types used/defined by the package
type (
	// MetricName is the name of the metric
	MetricName string

	// MetricType is the type of the metric
	MetricType int

	MetricUnit string

	// metricDefinition contains the definition for a metric
	metricDefinition struct {
		metricType       MetricType // metric type
		metricName       MetricName // metric name
		metricRollupName MetricName // optional. if non-empty, this name must be used for rolled-up version of this metric
		unit             MetricUnit
	}

	// scopeDefinition holds the tag definitions for a scope
	scopeDefinition struct {
		operation string            // 'operation' tag for scope
		tags      map[string]string // additional tags for scope
	}

	// ServiceIdx is an index that uniquely identifies the service
	ServiceIdx int
)

// MetricUnit supported values
// Values are pulled from https://pkg.go.dev/golang.org/x/exp/event#Unit
const (
	Dimensionless = "1"
	Milliseconds  = "ms"
	Bytes         = "By"
)

// MetricTypes which are supported
const (
	Counter MetricType = iota
	Timer
	Gauge
	Histogram
)

// Service names for all services that emit metrics.
const (
	Common ServiceIdx = iota
	Frontend
	History
	Matching
	Worker
	Server
	UnitTestService
	NumServices
)

// Values used for metrics propagation
const (
	HistoryWorkflowExecutionCacheLatency = "history_workflow_execution_cache_latency"
)

// Common tags for all services
const (
	OperationTagName      = "operation"
	ServiceRoleTagName    = "service_role"
	CacheTypeTagName      = "cache_type"
	FailureTagName        = "failure"
	TaskCategoryTagName   = "task_category"
	TaskTypeTagName       = "task_type"
	TaskPriorityTagName   = "task_priority"
	QueueReaderIDTagName  = "queue_reader_id"
	QueueAlertTypeTagName = "queue_alert_type"
	QueueTypeTagName      = "queue_type"
	visibilityTypeTagName = "visibility_type"
	ErrorTypeTagName      = "error_type"
	httpStatusTagName     = "http_status"
	resourceExhaustedTag  = "resource_exhausted_cause"
)

// This package should hold all the metrics and tags for temporal
const (
	HistoryRoleTagValue       = "history"
	MatchingRoleTagValue      = "matching"
	FrontendRoleTagValue      = "frontend"
	AdminRoleTagValue         = "admin"
	DCRedirectionRoleTagValue = "dc_redirection"
	BlobstoreRoleTagValue     = "blobstore"

	MutableStateCacheTypeTagValue = "mutablestate"
	EventsCacheTypeTagValue       = "events"

	standardVisibilityTagValue = "standard_visibility"
	advancedVisibilityTagValue = "advanced_visibility"
)

// Common service base metrics
const (
	RestartCount         = "restarts"
	NumGoRoutinesGauge   = "num_goroutines"
	GoMaxProcsGauge      = "gomaxprocs"
	MemoryAllocatedGauge = "memory_allocated"
	MemoryHeapGauge      = "memory_heap"
	MemoryHeapIdleGauge  = "memory_heapidle"
	MemoryHeapInuseGauge = "memory_heapinuse"
	MemoryStackGauge     = "memory_stack"
	NumGCCounter         = "memory_num_gc"
	GcPauseMsTimer       = "memory_gc_pause_ms"
)

// ServiceMetrics are types for common service base metrics
var ServiceMetrics = map[MetricName]MetricType{
	RestartCount: Counter,
}

// GoRuntimeMetrics represent the runtime stats from go runtime
var GoRuntimeMetrics = map[MetricName]MetricType{
	NumGoRoutinesGauge:   Gauge,
	GoMaxProcsGauge:      Gauge,
	MemoryAllocatedGauge: Gauge,
	MemoryHeapGauge:      Gauge,
	MemoryHeapIdleGauge:  Gauge,
	MemoryHeapInuseGauge: Gauge,
	MemoryStackGauge:     Gauge,
	NumGCCounter:         Counter,
	GcPauseMsTimer:       Timer,
}

// Scopes enum
const (
	UnknownScope = iota

	// -- Common Operation scopes --

	// MessagingClientPublishScope tracks Publish calls made by service to messaging layer
	MessagingClientPublishScope
	// MessagingClientPublishBatchScope tracks Publish calls made by service to messaging layer
	MessagingClientPublishBatchScope

	// ClusterMetadataArchivalConfigScope tracks ArchivalConfig calls to ClusterMetadata
	ClusterMetadataArchivalConfigScope

	// SequentialTaskProcessingScope is used by sequential task processing logic
	SequentialTaskProcessingScope
	// ParallelTaskProcessingScope is used by parallel task processing logic
	ParallelTaskProcessingScope
	// TaskSchedulerScope is used by task scheduler logic
	TaskSchedulerScope

	// The following metrics are only used by internal archiver implemention.
	// TODO: move them to internal repo once temporal plugin model is in place.

	// BlobstoreClientUploadScope tracks Upload calls to blobstore
	BlobstoreClientUploadScope
	// BlobstoreClientDownloadScope tracks Download calls to blobstore
	BlobstoreClientDownloadScope
	// BlobstoreClientGetMetadataScope tracks GetMetadata calls to blobstore
	BlobstoreClientGetMetadataScope
	// BlobstoreClientExistsScope tracks Exists calls to blobstore
	BlobstoreClientExistsScope
	// BlobstoreClientDeleteScope tracks Delete calls to blobstore
	BlobstoreClientDeleteScope
	// BlobstoreClientDirectoryExistsScope tracks DirectoryExists calls to blobstore
	BlobstoreClientDirectoryExistsScope

	DynamicConfigScope

	NumCommonScopes
)

// -- Scopes for UnitTestService --
const (
	TestScope1 = iota
	TestScope2

	NumUnitTestServiceScopes
)

// ScopeDefs record the scopes for all services
var ScopeDefs = map[ServiceIdx]map[int]scopeDefinition{
	// common scope Names
	Common: {
		UnknownScope:                       {operation: "Unknown"},
		ClusterMetadataArchivalConfigScope: {operation: "ArchivalConfig"},
		MessagingClientPublishScope:        {operation: "MessagingClientPublish"},
		MessagingClientPublishBatchScope:   {operation: "MessagingClientPublishBatch"},

		SequentialTaskProcessingScope: {operation: "SequentialTaskProcessing"},
		ParallelTaskProcessingScope:   {operation: "ParallelTaskProcessing"},
		TaskSchedulerScope:            {operation: "TaskScheduler"},

		BlobstoreClientUploadScope:          {operation: "BlobstoreClientUpload", tags: map[string]string{ServiceRoleTagName: BlobstoreRoleTagValue}},
		BlobstoreClientDownloadScope:        {operation: "BlobstoreClientDownload", tags: map[string]string{ServiceRoleTagName: BlobstoreRoleTagValue}},
		BlobstoreClientGetMetadataScope:     {operation: "BlobstoreClientGetMetadata", tags: map[string]string{ServiceRoleTagName: BlobstoreRoleTagValue}},
		BlobstoreClientExistsScope:          {operation: "BlobstoreClientExists", tags: map[string]string{ServiceRoleTagName: BlobstoreRoleTagValue}},
		BlobstoreClientDeleteScope:          {operation: "BlobstoreClientDelete", tags: map[string]string{ServiceRoleTagName: BlobstoreRoleTagValue}},
		BlobstoreClientDirectoryExistsScope: {operation: "BlobstoreClientDirectoryExists", tags: map[string]string{ServiceRoleTagName: BlobstoreRoleTagValue}},

		DynamicConfigScope: {operation: "DynamicConfig"},
	},
	// Frontend Scope Names

	UnitTestService: {
		TestScope1: {operation: "test_scope_1_operation"},
		TestScope2: {operation: "test_scope_2_operation"},
	},
}

// Common Metrics enum
const (
	ArchivalConfigFailures = iota

	SequentialTaskSubmitRequest
	SequentialTaskSubmitRequestTaskQueueExist
	SequentialTaskSubmitRequestTaskQueueMissing
	SequentialTaskSubmitLatency
	SequentialTaskQueueSize
	SequentialTaskQueueProcessingLatency
	SequentialTaskTaskProcessingLatency

	ParallelTaskSubmitRequest
	ParallelTaskSubmitLatency
	ParallelTaskTaskProcessingLatency

	PriorityTaskSubmitRequest
	PriorityTaskSubmitLatency

	// common metrics that are emitted per task queue

	ServiceRequestsPerTaskQueue
	ServiceFailuresPerTaskQueue
	ServiceLatencyPerTaskQueue
	ServiceErrInvalidArgumentPerTaskQueueCounter
	ServiceErrNamespaceNotActivePerTaskQueueCounter
	ServiceErrResourceExhaustedPerTaskQueueCounter
	ServiceErrNotFoundPerTaskQueueCounter
	ServiceErrExecutionAlreadyStartedPerTaskQueueCounter
	ServiceErrNamespaceAlreadyExistsPerTaskQueueCounter
	ServiceErrCancellationAlreadyRequestedPerTaskQueueCounter
	ServiceErrQueryFailedPerTaskQueueCounter
	ServiceErrContextTimeoutPerTaskQueueCounter
	ServiceErrRetryTaskPerTaskQueueCounter
	ServiceErrBadBinaryPerTaskQueueCounter
	ServiceErrClientVersionNotSupportedPerTaskQueueCounter
	ServiceErrIncompleteHistoryPerTaskQueueCounter
	ServiceErrNonDeterministicPerTaskQueueCounter
	ServiceErrUnauthorizedPerTaskQueueCounter
	ServiceErrAuthorizeFailedPerTaskQueueCounter

	NoopImplementationIsUsed

	NumCommonMetrics // Needs to be last on this list for iota numbering
)

// Server metrics enum

// UnitTestService metrics enum
const (
	TestCounterMetric1 = iota
	TestCounterMetric2
	TestCounterRollupMetric1
	TestTimerMetric1
	TestTimerMetric2
	TestGaugeMetric1
	TestGaugeMetric2
	TestBytesHistogramMetric1
	TestBytesHistogramMetric2
	TestDimensionlessHistogramMetric1
	TestDimensionlessHistogramMetric2

	NumUnitTestServiceMetrics
)

// MetricDefs record the metrics for all services
var MetricDefs = map[ServiceIdx]map[int]metricDefinition{
	Common: {

		ArchivalConfigFailures: NewCounterDef("archivalconfig_failures"),

		SequentialTaskSubmitRequest:                 NewCounterDef("sequentialtask_submit_request"),
		SequentialTaskSubmitRequestTaskQueueExist:   NewCounterDef("sequentialtask_submit_request_taskqueue_exist"),
		SequentialTaskSubmitRequestTaskQueueMissing: NewCounterDef("sequentialtask_submit_request_taskqueue_missing"),
		SequentialTaskSubmitLatency:                 NewTimerDef("sequentialtask_submit_latency"),
		SequentialTaskQueueSize:                     NewBytesHistogramDef("sequentialtask_queue_size"),
		SequentialTaskQueueProcessingLatency:        NewTimerDef("sequentialtask_queue_processing_latency"),
		SequentialTaskTaskProcessingLatency:         NewTimerDef("sequentialtask_task_processing_latency"),
		ParallelTaskSubmitRequest:                   NewCounterDef("paralleltask_submit_request"),
		ParallelTaskSubmitLatency:                   NewTimerDef("paralleltask_submit_latency"),
		ParallelTaskTaskProcessingLatency:           NewTimerDef("paralleltask_task_processing_latency"),
		PriorityTaskSubmitRequest:                   NewCounterDef("prioritytask_submit_request"),
		PriorityTaskSubmitLatency:                   NewTimerDef("prioritytask_submit_latency"),

		// per task queue common metrics

		ServiceRequestsPerTaskQueue:                               NewRollupCounterDef("service_requests_per_tl", "service_requests"),
		ServiceFailuresPerTaskQueue:                               NewRollupCounterDef("service_errors_per_tl", "service_errors"),
		ServiceLatencyPerTaskQueue:                                NewRollupTimerDef("service_latency_per_tl", "service_latency"),
		ServiceErrInvalidArgumentPerTaskQueueCounter:              NewRollupCounterDef("service_errors_invalid_argument_per_tl", "service_errors_invalid_argument"),
		ServiceErrNamespaceNotActivePerTaskQueueCounter:           NewRollupCounterDef("service_errors_namespace_not_active_per_tl", "service_errors_namespace_not_active"),
		ServiceErrResourceExhaustedPerTaskQueueCounter:            NewRollupCounterDef("service_errors_resource_exhausted_per_tl", "service_errors_resource_exhausted"),
		ServiceErrNotFoundPerTaskQueueCounter:                     NewRollupCounterDef("service_errors_entity_not_found_per_tl", "service_errors_entity_not_found"),
		ServiceErrExecutionAlreadyStartedPerTaskQueueCounter:      NewRollupCounterDef("service_errors_execution_already_started_per_tl", "service_errors_execution_already_started"),
		ServiceErrNamespaceAlreadyExistsPerTaskQueueCounter:       NewRollupCounterDef("service_errors_namespace_already_exists_per_tl", "service_errors_namespace_already_exists"),
		ServiceErrCancellationAlreadyRequestedPerTaskQueueCounter: NewRollupCounterDef("service_errors_cancellation_already_requested_per_tl", "service_errors_cancellation_already_requested"),
		ServiceErrQueryFailedPerTaskQueueCounter:                  NewRollupCounterDef("service_errors_query_failed_per_tl", "service_errors_query_failed"),
		ServiceErrContextTimeoutPerTaskQueueCounter:               NewRollupCounterDef("service_errors_context_timeout_per_tl", "service_errors_context_timeout"),
		ServiceErrRetryTaskPerTaskQueueCounter:                    NewRollupCounterDef("service_errors_retry_task_per_tl", "service_errors_retry_task"),
		ServiceErrBadBinaryPerTaskQueueCounter:                    NewRollupCounterDef("service_errors_bad_binary_per_tl", "service_errors_bad_binary"),
		ServiceErrClientVersionNotSupportedPerTaskQueueCounter:    NewRollupCounterDef("service_errors_client_version_not_supported_per_tl", "service_errors_client_version_not_supported"),
		ServiceErrIncompleteHistoryPerTaskQueueCounter:            NewRollupCounterDef("service_errors_incomplete_history_per_tl", "service_errors_incomplete_history"),
		ServiceErrNonDeterministicPerTaskQueueCounter:             NewRollupCounterDef("service_errors_nondeterministic_per_tl", "service_errors_nondeterministic"),
		ServiceErrUnauthorizedPerTaskQueueCounter:                 NewRollupCounterDef("service_errors_unauthorized_per_tl", "service_errors_unauthorized"),
		ServiceErrAuthorizeFailedPerTaskQueueCounter:              NewRollupCounterDef("service_errors_authorize_failed_per_tl", "service_errors_authorize_failed"),

		NoopImplementationIsUsed: NewCounterDef("noop_implementation_is_used"),
	},
	Matching: {},
	UnitTestService: {
		TestCounterMetric1:                NewCounterDef("test_counter_metric_a"),
		TestCounterMetric2:                NewCounterDef("test_counter_metric_b"),
		TestCounterRollupMetric1:          NewRollupCounterDef("test_counter_rollup_metric_a", "test_counter_rollup_metric_a_rollup"),
		TestTimerMetric1:                  NewTimerDef("test_timer_metric_a"),
		TestTimerMetric2:                  NewTimerDef("test_timer_metric_b"),
		TestGaugeMetric1:                  NewGaugeDef("test_gauge_metric_a"),
		TestGaugeMetric2:                  NewGaugeDef("test_gauge_metric_b"),
		TestBytesHistogramMetric1:         NewBytesHistogramDef("test_bytes_histogram_metric_a"),
		TestBytesHistogramMetric2:         NewBytesHistogramDef("test_bytes_histogram_metric_b"),
		TestDimensionlessHistogramMetric1: NewDimensionlessHistogramDef("test_bytes_histogram_metric_a"),
		TestDimensionlessHistogramMetric2: NewDimensionlessHistogramDef("test_bytes_histogram_metric_b"),
	},
}

// ErrorClass is an enum to help with classifying SLA vs. non-SLA errors (SLA = "service level agreement")
type ErrorClass uint8

const (
	// NoError indicates that there is no error (error should be nil)
	NoError = ErrorClass(iota)
	// UserError indicates that this is NOT an SLA-reportable error
	UserError
	// InternalError indicates that this is an SLA-reportable error
	InternalError
)

// Empty returns true if the metricName is an empty string
func (mn MetricName) Empty() bool {
	return mn == ""
}

// String returns string representation of this metric name
func (mn MetricName) String() string {
	return string(mn)
}

func (md metricDefinition) GetMetricType() MetricType {
	return md.metricType
}

func (md metricDefinition) GetMetricName() string {
	return md.metricName.String()
}

func (md metricDefinition) GetMetricRollupName() string {
	return md.metricRollupName.String()
}

func (md metricDefinition) GetMetricUnit() MetricUnit {
	return md.unit
}

func NewTimerDef(name string) metricDefinition {
	return metricDefinition{metricName: MetricName(name), metricType: Timer, unit: Milliseconds}
}

func NewRollupTimerDef(name string, rollupName string) metricDefinition {
	return metricDefinition{metricName: MetricName(name), metricRollupName: MetricName(rollupName), metricType: Timer, unit: Milliseconds}
}

func NewBytesHistogramDef(name string) metricDefinition {
	return metricDefinition{metricName: MetricName(name), metricType: Histogram, unit: Bytes}
}

func NewDimensionlessHistogramDef(name string) metricDefinition {
	return metricDefinition{metricName: MetricName(name), metricType: Histogram, unit: Dimensionless}
}

func NewCounterDef(name string) metricDefinition {
	return metricDefinition{metricName: MetricName(name), metricType: Counter}
}

// Rollup counter name is used to report aggregated metric excluding namespace tag.
func NewRollupCounterDef(name string, rollupName string) metricDefinition {
	return metricDefinition{metricName: MetricName(name), metricRollupName: MetricName(rollupName), metricType: Counter}
}

func NewGaugeDef(name string) metricDefinition {
	return metricDefinition{metricName: MetricName(name), metricType: Gauge}
}
