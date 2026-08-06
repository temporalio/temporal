package tests

// SAA vs WFA metrics parity tests.

import (
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/await"
)

// TestScheduleToStartMetric asserts that SAA and WFA record task_schedule_to_start_latency
// identically: one sample per accepted worker start, with the same tags. Workflow-task starts share
// the metric name, so only recordings tagged with the activity-task start operation and task type
// are collected. Both settings of MetricsBreakdownByTaskQueue are covered, since it decides whether
// "taskqueue" carries the real name or the "__omitted__" cardinality placeholder.
func (s *activityParityTestSuite) TestScheduleToStartMetric() {
	type scenario struct {
		name           string
		trace          []model.Event
		expectedStarts int
	}
	type captureResult struct {
		recordings []*metricstest.CapturedRecording
		namespace  string
		taskQueue  string
	}

	scenarios := []scenario{
		{name: "FirstAttempt", trace: []model.Event{model.Poll}, expectedStarts: 1},
		{
			name:           "RetryAttempt",
			trace:          []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll},
			expectedStarts: 2,
		},
	}
	capture := func(t *testing.T, standalone, breakdownByTaskQueue bool, trace []model.Event) captureResult {
		env := newActivityParityEnv(t)
		env.GetTestCluster().OverrideDynamicConfig(t, dynamicconfig.MetricsBreakdownByTaskQueue,
			[]dynamicconfig.ConstrainedValue{{
				Constraints: dynamicconfig.Constraints{Namespace: env.Namespace().String()},
				Value:       breakdownByTaskQueue,
			}})
		metricCapture := env.StartNamespaceMetricCapture()

		var taskQueue string
		if standalone {
			taskQueue = newSAADriver(t, env, activityConfig{}).driveTrace(t, trace).taskQueue
		} else {
			taskQueue = newWFADriver(t, env, activityConfig{}).driveTrace(t, trace).taskQueue
		}
		recordings := metricCapture.CollectMetric(metrics.TaskScheduleToStartLatency.Name(), func(rec *metricstest.CapturedRecording) bool {
			return rec.Tags["operation"] == metrics.HistoryRecordActivityTaskStartedScope &&
				rec.Tags["task_type"] == enumspb.TASK_QUEUE_TYPE_ACTIVITY.String()
		})
		return captureResult{recordings: recordings, namespace: env.Namespace().String(), taskQueue: taskQueue}
	}
	check := func(t *testing.T, implementation string, result captureResult, breakdownByTaskQueue bool, expectedStarts int) {
		require.Len(t, result.recordings, expectedStarts,
			"%s must record schedule-to-start latency once for every accepted worker start", implementation)
		taskQueue := "__omitted__"
		if breakdownByTaskQueue {
			taskQueue = result.taskQueue
		}
		expectedTags := map[string]string{
			"namespace":    result.namespace,
			"operation":    metrics.HistoryRecordActivityTaskStartedScope,
			"partition":    "__normal__",
			"service_name": string(primitives.HistoryService),
			"task_type":    enumspb.TASK_QUEUE_TYPE_ACTIVITY.String(),
			"taskqueue":    taskQueue,
		}
		for _, recording := range result.recordings {
			require.Equal(t, expectedTags, recording.Tags, "%s metric tags must match the activity-task start scope", implementation)
		}
	}

	for _, breakdownByTaskQueue := range []bool{true, false} {
		breakdownName := "TaskQueueBreakdownEnabled"
		if !breakdownByTaskQueue {
			breakdownName = "TaskQueueBreakdownDisabled"
		}
		s.Run(breakdownName, func(s *activityParityTestSuite) {
			for _, sc := range scenarios {
				s.Run(sc.name, func(s *activityParityTestSuite) {
					t := s.T()
					wfa := capture(t, false, breakdownByTaskQueue, sc.trace)
					saa := capture(t, true, breakdownByTaskQueue, sc.trace)
					check(t, "WFA", wfa, breakdownByTaskQueue, sc.expectedStarts)
					check(t, "SAA", saa, breakdownByTaskQueue, sc.expectedStarts)
				})
			}
		})
	}
}

func (s *activityParityTestSuite) TestMetrics() {
	type activityMetric struct {
		name             string
		compared         bool
		counter          bool
		baseHandler      bool
		legacyWFAHandler bool
		recordingTagKeys []string
	}
	type scenario struct {
		name    string
		trace   []model.Event
		cfg     activityConfig
		saaOnly bool
		anchor  string
	}
	type recordings map[string][]*metricstest.CapturedRecording

	perActivityTagKeys := []string{
		"activityType",
		"namespace",
		"operation",
		"service_name",
		"taskqueue",
		"versioning_behavior",
		"workflowType",
	}
	legacyWFATagKeys := []string{
		"activity_targeting_method",
		"namespace",
		"service_name",
	}
	catalog := []activityMetric{
		{name: metrics.ActivitySuccess.Name(), compared: true, counter: true},
		{name: metrics.ActivityFail.Name(), compared: true, counter: true},
		{name: metrics.ActivityTaskFail.Name(), compared: true, counter: true},
		{name: metrics.ActivityCancel.Name(), compared: true, counter: true},
		{name: metrics.ActivityTerminate.Name()},
		{name: metrics.ActivityTimeout.Name(), compared: true, counter: true, recordingTagKeys: []string{"timeout_type"}},
		{name: metrics.ActivityTaskTimeout.Name(), compared: true, counter: true, recordingTagKeys: []string{"timeout_type"}},
		{name: metrics.ActivityStartToCloseLatency.Name(), compared: true},
		{name: metrics.ActivityScheduleToCloseLatency.Name(), compared: true},
		{name: metrics.ActivityPause.Name(), compared: true, counter: true, legacyWFAHandler: true},
		{name: metrics.ActivityUnpause.Name(), compared: true, counter: true, legacyWFAHandler: true},
		{name: metrics.ActivityReset.Name(), compared: true, counter: true, legacyWFAHandler: true},
		{name: metrics.ActivityUpdateOptions.Name(), compared: true, counter: true, legacyWFAHandler: true},
		{name: metrics.ActivityHeartbeatCount.Name(), compared: true, counter: true, baseHandler: true},
		{name: metrics.ActivityPayloadSize.Name(), compared: true, counter: true, baseHandler: true},
	}
	scenarios := []scenario{
		{name: "Success", trace: []model.Event{model.Poll, model.Complete}, cfg: activityConfig{MaxAttempts: 1}},
		{name: "TerminalFailure", trace: []model.Event{model.Poll, model.FailNonRetryably}, cfg: activityConfig{MaxAttempts: 1}},
		{name: "Cancel", trace: []model.Event{model.Poll, model.RequestCancel, model.RespondCanceled}, cfg: activityConfig{MaxAttempts: 1}},
		{name: "TerminalTimeout", trace: []model.Event{model.Poll, model.StartToCloseElapses}, cfg: activityConfig{MaxAttempts: 1, StartToClose: activityShortTimeout}, anchor: metrics.ActivityTimeout.Name()},
		{name: "RetryableTimeout", trace: []model.Event{model.Poll, model.StartToCloseElapses}, cfg: activityConfig{MaxAttempts: 2, StartToClose: activityShortTimeout}, anchor: metrics.ActivityTaskTimeout.Name()},
		{name: "RetryableTaskFailure", trace: []model.Event{model.Poll, model.FailRetryably}, cfg: activityConfig{MaxAttempts: 2}},
		{name: "RetryableTaskFailureWithHeartbeatDetails", trace: []model.Event{model.Poll, {Type: model.RespondFailedType, Failure: &model.Failure{Retryable: true}, HasHeartbeatDetails: true}}, cfg: activityConfig{MaxAttempts: 2}},
		{name: "Heartbeat", trace: []model.Event{model.Poll, model.Heartbeat}, cfg: activityConfig{MaxAttempts: 1}},
		{name: "Pause", trace: []model.Event{model.Poll, model.Pause}, cfg: activityConfig{MaxAttempts: 1}},
		{name: "Unpause", trace: []model.Event{model.Poll, model.Pause, model.Unpause}, cfg: activityConfig{MaxAttempts: 1}},
		{name: "Reset", trace: []model.Event{model.Poll, model.Reset}, cfg: activityConfig{MaxAttempts: 1}},
		{name: "UpdateOptions", trace: []model.Event{model.Poll, model.UpdateOptions}, cfg: activityConfig{MaxAttempts: 1}},
		{name: "Terminate", trace: []model.Event{model.Poll, model.Terminate}, cfg: activityConfig{MaxAttempts: 1}, saaOnly: true},
	}
	expectedTimeoutType := func(trace []model.Event) string {
		for _, event := range trace {
			switch event.Type {
			case model.StartToCloseElapsesType:
				return enumspb.TIMEOUT_TYPE_START_TO_CLOSE.String()
			case model.ScheduleToCloseElapsesType:
				return enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE.String()
			case model.ScheduleToStartElapsesType:
				return enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START.String()
			case model.HeartbeatElapsesType:
				return enumspb.TIMEOUT_TYPE_HEARTBEAT.String()
			default:
			}
		}
		return ""
	}
	captureMetrics := func(t *testing.T, sc scenario, standalone bool) (recordings, string) {
		env := newActivityParityEnv(t)
		capture := env.StartNamespaceMetricCapture()
		if standalone {
			newSAADriver(t, env, sc.cfg).driveTrace(t, sc.trace)
		} else {
			newWFADriver(t, env, sc.cfg).driveTrace(t, sc.trace)
		}

		if sc.anchor != "" {
			await.RequireTrue(t, func() bool {
				return len(capture.Metric(sc.anchor)) > 0
			}, 15*time.Second, 100*time.Millisecond)
		}

		result := make(recordings, len(catalog))
		for _, metric := range catalog {
			if recs := capture.Metric(metric.name); len(recs) > 0 {
				result[metric.name] = recs
			}
		}
		return result, env.Namespace().String()
	}
	seriesTagKeys := func(recs []*metricstest.CapturedRecording) []string {
		seen := make(map[string]struct{})
		for _, rec := range recs {
			for key := range rec.Tags {
				seen[key] = struct{}{}
			}
		}
		keys := make([]string, 0, len(seen))
		for key := range seen {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		return keys
	}
	metricSeries := func(
		t *testing.T,
		implementation string,
		recs []*metricstest.CapturedRecording,
		tagKeys []string,
		counter bool,
	) map[string]int64 {
		series := make(map[string]int64)
		for _, rec := range recs {
			tags := make([]string, 0, len(tagKeys))
			for _, key := range tagKeys {
				value, ok := rec.Tags[key]
				require.True(t, ok, "%s metric recording must have tag %q", implementation, key)
				// These identify the distinct test executions rather than metric behavior.
				switch key {
				case "activityType", "namespace", "taskqueue", "workflowType":
					value = ""
				default:
				}
				tags = append(tags, key+"="+value)
			}
			key := strings.Join(tags, "\x00")
			if counter {
				value, ok := rec.Value.(int64)
				require.True(t, ok, "%s counter recording must have an int64 value", implementation)
				series[key] += value
			} else {
				series[key]++
			}
		}
		return series
	}

	for _, sc := range scenarios {
		s.Run(sc.name, func(s *activityParityTestSuite) {
			t := s.T()
			saa, saaNS := captureMetrics(t, sc, true)
			checkTags := func(implementation string, emitted recordings, namespace string) {
				for name, recs := range emitted {
					for _, rec := range recs {
						require.Equal(t, namespace, rec.Tags["namespace"],
							"%s %s must be tagged with the namespace it was driven in", implementation, name)
					}
				}
				if timeoutType := expectedTimeoutType(sc.trace); timeoutType != "" {
					for _, name := range []string{metrics.ActivityTaskTimeout.Name(), metrics.ActivityTimeout.Name()} {
						for _, rec := range emitted[name] {
							require.Equal(t, timeoutType, rec.Tags["timeout_type"],
								"%s %s must carry the timeout_type that fired", implementation, name)
						}
					}
				}
			}
			checkTags("SAA", saa, saaNS)

			if sc.saaOnly {
				require.NotEmpty(t, saa[metrics.ActivityTerminate.Name()],
					"terminating a standalone activity must emit activity_terminate")
				return
			}

			wfa, wfaNS := captureMetrics(t, sc, false)
			checkTags("WFA", wfa, wfaNS)
			for _, metric := range catalog {
				if !metric.compared {
					continue
				}
				s.Run(metric.name, func(s *activityParityTestSuite) {
					t := s.T()
					wfaTagKeys := seriesTagKeys(wfa[metric.name])
					saaTagKeys := seriesTagKeys(saa[metric.name])
					comparisonTagKeys := wfaTagKeys
					if metric.legacyWFAHandler && len(wfa[metric.name]) > 0 {
						require.Equal(t, legacyWFATagKeys, wfaTagKeys,
							"WFA currently emits this metric from its legacy activity handler")
						for _, rec := range wfa[metric.name] {
							require.Equal(t, "id", rec.Tags["activity_targeting_method"])
						}
						require.Equal(t, perActivityTagKeys, saaTagKeys,
							"SAA must use the standard per-activity tag keys")
						comparisonTagKeys = []string{"namespace", "service_name"}
					} else {
						if !metric.baseHandler && len(wfa[metric.name]) > 0 {
							expectedTagKeys := append([]string{}, perActivityTagKeys...)
							expectedTagKeys = append(expectedTagKeys, metric.recordingTagKeys...)
							sort.Strings(expectedTagKeys)
							require.Equal(t, expectedTagKeys, wfaTagKeys,
								"WFA must use the standard per-activity tag keys")
						}
						require.Equal(t, wfaTagKeys, saaTagKeys, "WFA and SAA tag keys must match")
					}
					wfaSeries := metricSeries(t, "WFA", wfa[metric.name], comparisonTagKeys, metric.counter)
					saaSeries := metricSeries(t, "SAA", saa[metric.name], comparisonTagKeys, metric.counter)
					if metric.counter {
						require.Equal(t, wfaSeries, saaSeries, "WFA and SAA counter values must match")
					} else {
						require.Equal(t, wfaSeries, saaSeries, "WFA and SAA timer sample counts must match")
					}
				})
			}
		})
	}
}
