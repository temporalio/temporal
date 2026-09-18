package tests

// SAA vs WFA metrics parity tests.

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
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
			"namespace":              result.namespace,
			"operation":              metrics.HistoryRecordActivityTaskStartedScope,
			"partition":              "__normal__",
			"service_name":           string(primitives.HistoryService),
			"task_type":              enumspb.TASK_QUEUE_TYPE_ACTIVITY.String(),
			"taskqueue":              taskQueue,
			"worker_build_id":        "",
			"worker_deployment_name": "",
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
		name                 string
		compared             bool
		counter              bool
		baseHandler          bool
		workerDeploymentTags bool
		recordingTagKeys     []string
	}
	type scenario struct {
		name           string
		trace          []model.Event
		cfg            activityConfig
		saaOnly        bool
		anchor         string
		requiredMetric string
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
	workerDeploymentTagKeys := []string{
		"worker_build_id",
		"worker_deployment_name",
	}
	catalog := []activityMetric{
		{name: metrics.ActivitySuccess.Name(), compared: true, counter: true, workerDeploymentTags: true},
		{name: metrics.ActivityFail.Name(), compared: true, counter: true, workerDeploymentTags: true},
		{name: metrics.ActivityTaskFail.Name(), compared: true, counter: true, workerDeploymentTags: true},
		{name: metrics.ActivityCancel.Name(), compared: true, counter: true, workerDeploymentTags: true},
		{name: metrics.ActivityTerminate.Name()},
		{name: metrics.ActivityTimeout.Name(), compared: true, counter: true, workerDeploymentTags: true, recordingTagKeys: []string{"timeout_type"}},
		{name: metrics.ActivityTaskTimeout.Name(), compared: true, counter: true, workerDeploymentTags: true, recordingTagKeys: []string{"timeout_type"}},
		{name: metrics.ActivityStartToCloseLatency.Name(), compared: true, workerDeploymentTags: true},
		{name: metrics.ActivityScheduleToCloseLatency.Name(), compared: true, workerDeploymentTags: true},
		{name: metrics.ActivityPause.Name(), compared: true, counter: true},
		{name: metrics.ActivityUnpause.Name(), compared: true, counter: true},
		{name: metrics.ActivityReset.Name(), compared: true, counter: true},
		{name: metrics.ActivityUpdateOptions.Name(), compared: true, counter: true},
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
		{name: "Pause", trace: []model.Event{model.Poll, model.Pause}, cfg: activityConfig{MaxAttempts: 1}, requiredMetric: metrics.ActivityPause.Name()},
		{name: "Unpause", trace: []model.Event{model.Poll, model.Pause, model.Unpause}, cfg: activityConfig{MaxAttempts: 1}, requiredMetric: metrics.ActivityUnpause.Name()},
		{name: "Reset", trace: []model.Event{model.Poll, model.Reset}, cfg: activityConfig{MaxAttempts: 1}, requiredMetric: metrics.ActivityReset.Name()},
		{name: "UpdateOptions", trace: []model.Event{model.Poll, model.UpdateOptions}, cfg: activityConfig{MaxAttempts: 1}, requiredMetric: metrics.ActivityUpdateOptions.Name()},
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
		slices.Sort(keys)
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
			if sc.requiredMetric != "" {
				require.NotEmpty(t, saa[sc.requiredMetric], "SAA must emit %s", sc.requiredMetric)
			}
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
			if sc.requiredMetric != "" {
				require.NotEmpty(t, wfa[sc.requiredMetric], "WFA must emit %s", sc.requiredMetric)
			}
			checkTags("WFA", wfa, wfaNS)
			for _, metric := range catalog {
				if !metric.compared {
					continue
				}
				s.Run(metric.name, func(s *activityParityTestSuite) {
					t := s.T()
					wfaTagKeys := seriesTagKeys(wfa[metric.name])
					saaTagKeys := seriesTagKeys(saa[metric.name])
					if !metric.baseHandler && len(wfa[metric.name]) > 0 {
						expectedTagKeys := append([]string{}, perActivityTagKeys...)
						if metric.workerDeploymentTags {
							expectedTagKeys = append(expectedTagKeys, workerDeploymentTagKeys...)
						}
						expectedTagKeys = append(expectedTagKeys, metric.recordingTagKeys...)
						slices.Sort(expectedTagKeys)
						require.Equal(t, expectedTagKeys, wfaTagKeys,
							"WFA must use the standard per-activity tag keys")
					}
					require.Equal(t, wfaTagKeys, saaTagKeys, "WFA and SAA tag keys must match")
					wfaSeries := metricSeries(t, "WFA", wfa[metric.name], wfaTagKeys, metric.counter)
					saaSeries := metricSeries(t, "SAA", saa[metric.name], wfaTagKeys, metric.counter)
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

// TestOperatorMetricsPerActivityParity verifies that one WFA operator
// request targeting multiple activities in a workflow emits the same per-activity
// metrics as equivalent SAA per-ID requests.
func (s *activityParityTestSuite) TestOperatorMetricsPerActivityParity() {
	type scenario struct {
		name      string
		metric    string
		operation string
		event     model.Event
	}
	scenarios := []scenario{
		{name: "Pause", metric: metrics.ActivityPause.Name(), operation: metrics.ActivityPausedScope, event: model.Pause},
		{name: "Unpause", metric: metrics.ActivityUnpause.Name(), operation: metrics.ActivityUnpausedScope, event: model.Unpause},
		{name: "Reset", metric: metrics.ActivityReset.Name(), operation: metrics.ActivityResetScope, event: model.Reset},
		{name: "UpdateOptions", metric: metrics.ActivityUpdateOptions.Name(), operation: metrics.ActivityUpdateOptionsScope, event: model.UpdateOptions},
	}

	captureWFA := func(t *testing.T, sc scenario) ([]*metricstest.CapturedRecording, string) {
		env := newActivityParityEnv(t)
		ctx := testcontext.For(t)
		workflowTaskQueue := testcore.RandomizeStr("operator-metrics-wfa-workflow")
		activityTaskQueue := testcore.RandomizeStr("operator-metrics-wfa-activity")
		worker := sdkworker.New(env.SdkClient(), workflowTaskQueue, sdkworker.Options{})
		worker.RegisterWorkflow(wfaOperatorMetricsWorkflow)
		require.NoError(t, worker.Start())
		t.Cleanup(worker.Stop)

		run, err := env.SdkClient().ExecuteWorkflow(
			ctx,
			sdkclient.StartWorkflowOptions{
				ID:        testcore.RandomizeStr("operator-metrics-wfa-run"),
				TaskQueue: workflowTaskQueue,
			},
			wfaOperatorMetricsWorkflow,
			activityTaskQueue,
		)
		require.NoError(t, err)
		await.Require(ctx, t, func(t *await.T) {
			description, err := env.SdkClient().DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
			t.Require().NoError(err)
			t.Require().Len(description.GetPendingActivities(), 2)
		}, activityDriverTimeout, activityDriverPollInterval)

		execution := &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()}
		if sc.event.Type == model.UnpauseType {
			_, err = env.FrontendClient().PauseActivity(ctx, &workflowservice.PauseActivityRequest{
				Namespace: env.Namespace().String(),
				Execution: execution,
				Activity:  &workflowservice.PauseActivityRequest_Type{Type: wfaOperatorMetricsActivityType},
			})
			require.NoError(t, err)
		}

		capture := env.StartNamespaceMetricCapture()
		switch sc.event.Type {
		case model.PauseType:
			_, err = env.FrontendClient().PauseActivity(ctx, &workflowservice.PauseActivityRequest{
				Namespace: env.Namespace().String(),
				Execution: execution,
				Activity:  &workflowservice.PauseActivityRequest_Type{Type: wfaOperatorMetricsActivityType},
			})
		case model.UnpauseType:
			_, err = env.FrontendClient().UnpauseActivity(ctx, &workflowservice.UnpauseActivityRequest{
				Namespace: env.Namespace().String(),
				Execution: execution,
				Activity:  &workflowservice.UnpauseActivityRequest_UnpauseAll{UnpauseAll: true},
			})
		case model.ResetType:
			_, err = env.FrontendClient().ResetActivity(ctx, &workflowservice.ResetActivityRequest{
				Namespace: env.Namespace().String(),
				Execution: execution,
				Activity:  &workflowservice.ResetActivityRequest_MatchAll{MatchAll: true},
			})
		case model.UpdateOptionsType:
			_, err = env.FrontendClient().UpdateActivityOptions(ctx, &workflowservice.UpdateActivityOptionsRequest{
				Namespace: env.Namespace().String(),
				Execution: execution,
				Activity:  &workflowservice.UpdateActivityOptionsRequest_MatchAll{MatchAll: true},
				ActivityOptions: &activitypb.ActivityOptions{
					HeartbeatTimeout: durationpb.New(time.Hour),
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"heartbeat_timeout"}},
			})
		default:
			t.Fatalf("unsupported operator event %v", sc.event.Type)
		}
		require.NoError(t, err)
		return capture.Metric(sc.metric), activityTaskQueue
	}

	captureSAA := func(t *testing.T, sc scenario) []*metricstest.CapturedRecording {
		env := newActivityParityEnv(t)
		driver := newSAADriver(t, env, activityConfig{MaxAttempts: 1})
		activities := []*saaHandle{
			driver.start(t, activityConfig{MaxAttempts: 1}),
			driver.start(t, activityConfig{MaxAttempts: 1}),
		}
		if sc.event.Type == model.UnpauseType {
			for _, activity := range activities {
				activity.driveEvent(t, model.Pause)
			}
		}

		capture := env.StartNamespaceMetricCapture()
		for _, activity := range activities {
			activity.driveEvent(t, sc.event)
		}
		return capture.Metric(sc.metric)
	}

	normalizedSeries := func(t *testing.T, recordings []*metricstest.CapturedRecording) map[string]int64 {
		series := make(map[string]int64)
		for _, recording := range recordings {
			tags := make([]string, 0, len(recording.Tags))
			for key, value := range recording.Tags {
				switch key {
				case "activityType", "namespace", "taskqueue", "workflowType":
					value = ""
				default:
				}
				tags = append(tags, key+"="+value)
			}
			slices.Sort(tags)
			value, ok := recording.Value.(int64)
			require.True(t, ok)
			series[strings.Join(tags, "\x00")] += value
		}
		return series
	}

	for _, sc := range scenarios {
		s.Run(sc.name, func(s *activityParityTestSuite) {
			t := s.T()
			wfa, activityTaskQueue := captureWFA(t, sc)
			saa := captureSAA(t, sc)
			require.Len(t, wfa, 2, "one WFA request affecting two activities must emit two recordings")
			require.Len(t, saa, 2, "two SAA requests affecting one activity each must emit two recordings")
			for _, recording := range wfa {
				require.Equal(t, activityTaskQueue, recording.Tags["taskqueue"],
					"WFA must tag the activity task queue")
			}
			for _, recordings := range [][]*metricstest.CapturedRecording{wfa, saa} {
				for _, recording := range recordings {
					require.Equal(t, sc.operation, recording.Tags["operation"])
					require.Equal(t, int64(1), recording.Value)
				}
			}
			require.Equal(t, normalizedSeries(t, wfa), normalizedSeries(t, saa))
		})
	}
}

const wfaOperatorMetricsActivityType = "operatorMetricsActivity"

func wfaOperatorMetricsWorkflow(ctx workflow.Context, activityTaskQueue string) error {
	futures := make([]workflow.Future, 0, 2)
	for i := range 2 {
		activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:          fmt.Sprintf("activity-%d", i),
			TaskQueue:           activityTaskQueue,
			StartToCloseTimeout: activityLongDuration,
		})
		futures = append(futures, workflow.ExecuteActivity(activityCtx, wfaOperatorMetricsActivityType))
	}
	for _, future := range futures {
		if err := future.Get(ctx, nil); err != nil {
			return err
		}
	}
	return nil
}
