package scheduler

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/sdk"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Shape of the production schedules that hit the catch-up death spiral: an
// every-second calendar plus a far-future "keep-alive" calendar, SKIP, and a
// one-year catchup window.
const catchupSpecJSON = `{
  "structuredCalendar": [
    {"second": [{"start": 59, "end": 59}], "minute": [{"start": 59, "end": 59}], "hour": [{"start": 23, "end": 23}],
     "dayOfMonth": [{"start": 31, "end": 31}], "month": [{"start": 12, "end": 12}], "year": [{"start": 2100, "end": 2100}],
     "dayOfWeek": [{"end": 6, "step": 1}], "comment": "keep-alive"},
    {"second": [{"end": 59}], "minute": [{"end": 59}], "hour": [{"end": 23}],
     "dayOfMonth": [{"start": 1, "end": 31}], "month": [{"start": 1, "end": 12}], "dayOfWeek": [{"end": 6}]}
  ],
  "startTime": "1970-01-01T00:00:00Z",
  "endTime": "2124-11-03T06:25:23.869Z",
  "timezoneName": "Etc/GMT"
}`

// newCatchupArgs builds StartScheduleArgs shaped like the production ones (as
// after continue-as-new), with lastProcessedTime gap before baseStartTime.
func newCatchupArgs(tb testing.TB, gap time.Duration) *schedulespb.StartScheduleArgs {
	spec := &schedulepb.ScheduleSpec{}
	require.NoError(tb, protojson.Unmarshal([]byte(catchupSpecJSON), spec))
	jsonPayload := func(s string) *commonpb.Payload {
		return &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: []byte(s)}
	}
	recent := make([]*schedulepb.ScheduleActionResult, 10)
	for i := range recent {
		t := baseStartTime.Add(-gap - time.Duration(10-i)*2*time.Minute)
		recent[i] = &schedulepb.ScheduleActionResult{
			ScheduleTime: timestamppb.New(t), ActualTime: timestamppb.New(t),
			StartWorkflowResult: &commonpb.WorkflowExecution{WorkflowId: "myid-" + t.Format(time.RFC3339), RunId: "r"},
		}
	}
	return &schedulespb.StartScheduleArgs{
		Schedule: &schedulepb.Schedule{
			Spec: spec,
			Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
				WorkflowId:   "myid",
				WorkflowType: &commonpb.WorkflowType{Name: "mywf"},
				TaskQueue:    &taskqueuepb.TaskQueue{Name: "mytq"},
				Input: &commonpb.Payloads{Payloads: []*commonpb.Payload{
					jsonPayload(`{"definition":"` + strings.Repeat("x", 8000) + `"}`),
					{Metadata: map[string][]byte{"encoding": []byte("binary/null")}},
					jsonPayload(`"` + strings.Repeat("y", 36) + `"`),
					jsonPayload(`"` + strings.Repeat("z", 93) + `"`),
				}},
				WorkflowRunTimeout: durationpb.New(720 * time.Hour),
				RetryPolicy:        &commonpb.RetryPolicy{MaximumAttempts: 1},
			}}},
			Policies: &schedulepb.SchedulePolicies{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
				CatchupWindow: durationpb.New(365 * 24 * time.Hour),
			},
		},
		Info: &schedulepb.ScheduleInfo{
			ActionCount:    450000,
			OverlapSkipped: 55000000,
			RecentActions:  recent,
			CreateTime:     timestamppb.New(baseStartTime.Add(-365 * 24 * time.Hour)),
		},
		State: &schedulespb.InternalState{
			Namespace:         "myns",
			NamespaceId:       "mynsid",
			ScheduleId:        "myschedule",
			ConflictToken:     InitialConflictToken,
			LastProcessedTime: timestamppb.New(baseStartTime.Add(-gap)),
		},
	}
}

// runCatchupFirstTask runs only the first task after continue-as-new: catch up
// the gap, start the action, continue-as-new.
func runCatchupFirstTask(tb testing.TB, ts *testsuite.WorkflowTestSuite, gap time.Duration) {
	env := ts.NewTestWorkflowEnvironment()
	env.SetStartTime(baseStartTime)
	mockEverySecondActivities(env, &everySecondCounters{runEndTimes: map[string]time.Time{}})
	env.ExecuteWorkflow(SchedulerWorkflow, newCatchupArgs(tb, gap))
	require.True(tb, env.IsWorkflowCompleted())
	require.True(tb, workflow.IsContinueAsNewError(env.GetWorkflowError()), "%v", env.GetWorkflowError())
}

// prodLogger is the per-namespace worker's SDK logger: the server's zap JSON
// logger (no sampling, caller info on) behind log.NewSdkLogger. Output goes to
// a file, a lower bound on a pod's stdout pipe.
func prodLogger(tb testing.TB) sdklog.Logger {
	path := filepath.Join(tb.TempDir(), "worker.log")
	return log.NewSdkLogger(log.NewZapLogger(log.BuildZapLogger(log.Config{Level: "info", Format: "json", OutputFile: path})))
}

// prodMetrics is the per-namespace worker's SDK metrics handler: the server's
// OpenTelemetry/Prometheus handler behind sdk.NewMetricsHandler.
func prodMetrics(tb testing.TB) sdkclient.MetricsHandler {
	h, err := metrics.MetricsHandlerFromConfig(log.NewNoopLogger(), &metrics.Config{
		Prometheus: &metrics.PrometheusConfig{Framework: metrics.FrameworkOpentelemetry, ListenAddress: "127.0.0.1:0"},
	})
	require.NoError(tb, err)
	tb.Cleanup(func() { h.Stop(log.NewNoopLogger()) })
	return sdk.NewMetricsHandler(h)
}

func withOneIteration(tb testing.TB) {
	prev := CurrentTweakablePolicies.IterationsBeforeContinueAsNew
	CurrentTweakablePolicies.IterationsBeforeContinueAsNew = 1
	tb.Cleanup(func() { CurrentTweakablePolicies.IterationsBeforeContinueAsNew = prev })
}

// BenchmarkCatchupFirstTask attributes the cost of the first task after a stall
// to the production logger and metrics handler. Items are seconds of gap; all
// but the first MaxBufferSize take the "Buffer too large" log+metric path.
//
//	go test ./service/worker/scheduler -run x -bench CatchupFirstTask -benchtime 5x
func BenchmarkCatchupFirstTask(b *testing.B) {
	withOneIteration(b)
	const gap = 4*time.Hour + 24*time.Minute // first production failure
	for _, tc := range []struct {
		name          string
		prodLogging   bool
		prodMetricsOn bool
	}{
		{name: "noop-log/noop-metrics"},
		{name: "prod-log/noop-metrics", prodLogging: true},
		{name: "noop-log/prod-metrics", prodMetricsOn: true},
		{name: "prod-log/prod-metrics", prodLogging: true, prodMetricsOn: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			var ts testsuite.WorkflowTestSuite
			if tc.prodLogging {
				ts.SetLogger(prodLogger(b))
			} else {
				ts.SetLogger(log.NewSdkLogger(log.NewNoopLogger()))
			}
			if tc.prodMetricsOn {
				ts.SetMetricsHandler(prodMetrics(b))
			}
			for b.Loop() {
				runCatchupFirstTask(b, &ts, gap)
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/gap.Seconds(), "ns/item")
		})
	}
}
