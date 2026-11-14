package scheduler

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/testsuite"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/sdk"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// catchupCase is one first-task-after-a-stall scenario.
type catchupCase struct {
	name             string
	gap              time.Duration // now - LastProcessedTime
	overlap          enumspb.ScheduleOverlapPolicy
	running          bool                           // a previous action is still running
	catchupWindow    time.Duration                  // 0: production's 365d
	maxBuffer        int                            // 0: tweakable default (1000)
	deadlockTimeout  time.Duration                  // 0: completingDeadlockTimeout
	chunked          int                            // >0: run at ChunkedCatchup with this MaxCatchupMatchesPerTask
	args             *schedulespb.StartScheduleArgs // overrides the args built from gap
	dcMatchesPerTask *int                           // worker.schedulerV1CatchupMatchesPerTask, injected as fx.go does
}

// catchupResult is what the first task did.
type catchupResult struct {
	elapsed       time.Duration
	err           error                          // workflow error other than continue-as-new
	next          *schedulespb.StartScheduleArgs // continue-as-new input, nil on error
	walked        int                            // matches enumerated by processTimeRange
	bufferDropped int                            // "Buffer too large" warnings
	missedWindow  int                            // "Schedule missed catchup window" warnings
	sideEffects   int                            // SideEffect calls (one marker each)
	startedIDs    []string                       // workflow IDs the action started
	metrics       metricstest.CaptureSnapshot    // scheduler metrics emitted
	logger        *warnCounter                   // log message counts
}

// TestCatchupRepro reproduces the V1 first workflow task after a stall:
// processTimeRange enumerates every match since LastProcessedTime in one task.
// Set a breakpoint in processTimeRange (workflow.go) or addStart to inspect it.
//
// To debug, set TEMPORAL_DEBUG=1 in the debugger's environment: it disables the
// SDK deadlock detector (1s) and idle test timeout (3s) that a breakpoint trips.
//
// Run your own scenario with the "custom" case:
//
//	CATCHUP_GAP=4h24m CATCHUP_OVERLAP=skip CATCHUP_RUNNING=false CATCHUP_WINDOW=8760h \
//	CATCHUP_MAX_BUFFER=1000 CATCHUP_DEADLOCK_TIMEOUT=1s \
//	go test ./service/worker/scheduler -run 'TestCatchupRepro/custom' -v -count 1
func TestCatchupRepro(t *testing.T) {
	const buf = 1000
	skip := enumspb.SCHEDULE_OVERLAP_POLICY_SKIP

	t.Run("skip/nothing-running", func(t *testing.T) {
		gap := time.Hour
		r := runCatchupRepro(t, catchupCase{gap: gap, overlap: skip})
		g := int(gap.Seconds())
		require.NoError(t, r.err)
		// Every second of the gap is enumerated, but only the first `buf` are kept.
		require.Equal(t, g, r.walked)
		require.Equal(t, g-buf, r.bufferDropped)
		require.Equal(t, g-buf, int(r.next.Info.BufferDropped))
		// One SideEffect per NextTimeCacheV2Size matches.
		require.InDelta(t, g/CurrentTweakablePolicies.NextTimeCacheV2Size, r.sideEffects, 3)
		// Skip starts the EARLIEST buffered match, LastProcessedTime+1s, and skips the rest.
		require.Equal(t, []string{"myid-" + baseStartTime.Add(-gap+time.Second).UTC().Format(time.RFC3339)}, r.startedIDs)
		require.EqualValues(t, 55000000+buf-1, r.next.Info.OverlapSkipped)
		require.Empty(t, r.next.State.BufferedStarts)
		require.Equal(t, baseStartTime, r.next.State.LastProcessedTime.AsTime())
	})

	t.Run("skip/running", func(t *testing.T) {
		r := runCatchupRepro(t, catchupCase{gap: time.Hour, overlap: skip, running: true})
		require.NoError(t, r.err)
		require.Equal(t, 3600, r.walked)
		require.Empty(t, r.startedIDs)
		require.EqualValues(t, 55000000+buf, r.next.Info.OverlapSkipped)
	})

	t.Run("buffer-one/nothing-running", func(t *testing.T) {
		r := runCatchupRepro(t, catchupCase{gap: time.Hour, overlap: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE})
		require.NoError(t, r.err)
		require.Equal(t, 3600, r.walked)
		require.Len(t, r.startedIDs, 1)
		// The second-earliest match waits in the buffer.
		require.Len(t, r.next.State.BufferedStarts, 1)
		require.Equal(t, baseStartTime.Add(-time.Hour+2*time.Second), r.next.State.BufferedStarts[0].NominalTime.AsTime())
	})

	t.Run("catchup-window-shorter-than-gap", func(t *testing.T) {
		// Matches older than the window are still enumerated, just counted as missed.
		r := runCatchupRepro(t, catchupCase{gap: 2 * time.Hour, overlap: skip, catchupWindow: time.Hour})
		require.NoError(t, r.err)
		require.Equal(t, 7200, r.walked)
		// The window is inclusive: a match exactly one window old is not missed.
		require.Equal(t, 3599, r.missedWindow)
		require.EqualValues(t, 3599, r.next.Info.MissedCatchupWindow)
		require.Equal(t, []string{"myid-" + baseStartTime.Add(-time.Hour).UTC().Format(time.RFC3339)}, r.startedIDs)
	})

	t.Run("smaller-buffer", func(t *testing.T) {
		r := runCatchupRepro(t, catchupCase{gap: time.Hour, overlap: skip, maxBuffer: 10})
		require.NoError(t, r.err)
		require.Equal(t, 3600, r.walked)
		require.Equal(t, 3600-10, r.bufferDropped)
	})

	t.Run("prod-first-failure-gap", func(t *testing.T) {
		// 4h24m: where production first tripped the 1s detector on a busy pod.
		r := runCatchupRepro(t, catchupCase{gap: 4*time.Hour + 24*time.Minute, overlap: skip})
		require.NoError(t, r.err)
		require.Equal(t, 15840, r.walked)
	})

	t.Run("deadlock", func(t *testing.T) {
		if debugging() {
			t.Skip("TEMPORAL_DEBUG disables the deadlock detector")
		}
		// Large enough to exceed the 1s detector locally. Nothing is persisted:
		// the retry would start from the same LastProcessedTime with a larger gap.
		r := runCatchupRepro(t, catchupCase{gap: 30 * 24 * time.Hour, overlap: skip, deadlockTimeout: time.Second})
		require.ErrorContains(t, r.err, "TMPRL1101")
		require.Nil(t, r.next)
	})

	t.Run("custom", func(t *testing.T) {
		c, ok := catchupCaseFromEnv(t)
		if !ok {
			t.Skip("set CATCHUP_GAP (and optionally CATCHUP_OVERLAP, CATCHUP_RUNNING, CATCHUP_WINDOW, CATCHUP_MAX_BUFFER, CATCHUP_DEADLOCK_TIMEOUT, CATCHUP_CHUNKED)")
		}
		runCatchupRepro(t, c)
	})
}

func runCatchupRepro(t *testing.T, c catchupCase) catchupResult {
	withOneIteration(t)
	if c.maxBuffer > 0 {
		prev := CurrentTweakablePolicies.MaxBufferSize
		CurrentTweakablePolicies.MaxBufferSize = c.maxBuffer
		t.Cleanup(func() { CurrentTweakablePolicies.MaxBufferSize = prev })
	}
	if c.chunked > 0 {
		prev := CurrentTweakablePolicies
		CurrentTweakablePolicies.Version = ChunkedCatchup
		CurrentTweakablePolicies.MaxCatchupMatchesPerTask = c.chunked
		t.Cleanup(func() { CurrentTweakablePolicies = prev })
	}

	args := c.args
	if args == nil {
		args = newCatchupArgs(t, c.gap)
	}
	if c.overlap != enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
		args.Schedule.Policies.OverlapPolicy = c.overlap
	}
	if c.catchupWindow > 0 {
		args.Schedule.Policies.CatchupWindow = durationpb.New(c.catchupWindow)
	}
	const runningID = "myid-still-running"
	if c.running {
		args.Info.RunningWorkflows = append(args.Info.RunningWorkflows, &commonpb.WorkflowExecution{WorkflowId: runningID, RunId: "r"})
	}

	var r catchupResult
	logger := &warnCounter{}
	var ts testsuite.WorkflowTestSuite
	ts.SetLogger(logger)
	mh := metricstest.NewCaptureHandler()
	capture := mh.StartCapture()
	ts.SetMetricsHandler(sdk.NewMetricsHandler(mh))
	env := ts.NewTestWorkflowEnvironment()
	env.SetStartTime(baseStartTime)
	deadlockTimeout := c.deadlockTimeout
	if deadlockTimeout == 0 {
		deadlockTimeout = completingDeadlockTimeout
	}
	if debugging() {
		// SetWorkerOptions replaces the SDK's TEMPORAL_DEBUG defaults; restore them so
		// breakpoints don't trip the deadlock detector or the 3s idle test timeout.
		if c.deadlockTimeout == 0 {
			deadlockTimeout = 24 * time.Hour
		}
		env.SetTestTimeout(24 * time.Hour)
	}
	env.SetWorkerOptions(sdkworker.Options{
		DeadlockDetectionTimeout: deadlockTimeout,
		Interceptors:             []interceptor.WorkerInterceptor{&sideEffectCounter{n: &r.sideEffects}},
	})
	env.OnActivity(new(activities).StartWorkflow, mock.Anything, mock.Anything).Maybe().Return(
		func(_ context.Context, req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
			r.startedIDs = append(r.startedIDs, req.Request.WorkflowId)
			return &schedulespb.StartWorkflowResponse{RunId: uuid.NewString(), RealStartTime: timestamppb.New(env.Now())}, nil
		})
	env.OnActivity(new(activities).WatchWorkflow, mock.Anything, mock.Anything).Maybe().Return(
		func(_ context.Context, req *schedulespb.WatchWorkflowRequest) (*schedulespb.WatchWorkflowResponse, error) {
			if req.Execution.WorkflowId == runningID && !req.LongPoll {
				return &schedulespb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}, nil
			}
			if req.LongPoll {
				return nil, context.DeadlineExceeded // keep long polls from resolving within the task
			}
			return &schedulespb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}, nil
		})

	start := time.Now()
	if c.dcMatchesPerTask != nil {
		dc := dynamicconfig.NewNoopCollection()
		specBuilder := NewSpecBuilder(dynamicconfig.SchedulerSpecWarnIterations.Get(dc), dynamicconfig.SchedulerSpecMaxIterations.Get(dc))
		env.ExecuteWorkflow(func(ctx workflow.Context, a *schedulespb.StartScheduleArgs) error {
			return schedulerWorkflowWithSpecBuilder(ctx, a, specBuilder, schedulerDynamicConfig{
				catchupMatchesPerTask: func() int { return *c.dcMatchesPerTask },
			})
		}, args)
	} else {
		env.ExecuteWorkflow(SchedulerWorkflow, args)
	}
	r.elapsed = time.Since(start)

	require.True(t, env.IsWorkflowCompleted())
	var canErr *workflow.ContinueAsNewError
	if err := env.GetWorkflowError(); !errors.As(err, &canErr) {
		r.err = err
	} else {
		require.NoError(t, payloads.Decode(canErr.Input, &r.next))
	}
	r.metrics = capture.Snapshot()
	r.logger = logger
	r.bufferDropped = logger.count("Buffer too large")
	r.missedWindow = logger.count("Schedule missed catchup window")
	// Every enumerated match is either missed (no addStart) or passed to addStart.
	r.walked = r.missedWindow + logger.count("addStart")

	t.Logf("gap=%s overlap=%s running=%v window=%s buffer=%d", c.gap, args.Schedule.Policies.OverlapPolicy, c.running,
		args.Schedule.Policies.CatchupWindow.AsDuration(), CurrentTweakablePolicies.MaxBufferSize)
	t.Logf("  elapsed=%s (%.0f ns/item)  walked=%d  missedWindow=%d  bufferDropped=%d  sideEffects=%d",
		r.elapsed, float64(r.elapsed.Nanoseconds())/float64(max(r.walked, 1)), r.walked, r.missedWindow, r.bufferDropped, r.sideEffects)
	t.Logf("  started=%v", r.startedIDs)
	if r.err != nil {
		t.Logf("  error: %s", firstLine(r.err.Error()))
	} else {
		t.Logf("  after: LastProcessedTime=%s overlapSkipped=%d bufferedStarts=%d infoBufferDropped=%d",
			r.next.State.LastProcessedTime.AsTime().Format(time.RFC3339), r.next.Info.OverlapSkipped,
			len(r.next.State.BufferedStarts), r.next.Info.BufferDropped)
	}
	return r
}

// warnCounter is an SDK logger that counts messages by text. Debug is counted
// too: addStart logs one per call.
type warnCounter struct {
	mu     sync.Mutex
	counts map[string]int
}

func (l *warnCounter) add(level, msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.counts == nil {
		l.counts = map[string]int{}
	}
	l.counts[msg]++
	l.counts[level+" "+msg]++
}

func (l *warnCounter) count(msg string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.counts[msg]
}

// countAt counts msg logged at level (DEBUG, INFO, WARN, ERROR).
func (l *warnCounter) countAt(level, msg string) int { return l.count(level + " " + msg) }

func (l *warnCounter) Debug(msg string, _ ...any) { l.add("DEBUG", msg) }
func (l *warnCounter) Info(msg string, _ ...any)  { l.add("INFO", msg) }
func (l *warnCounter) Warn(msg string, _ ...any)  { l.add("WARN", msg) }
func (l *warnCounter) Error(msg string, _ ...any) { l.add("ERROR", msg) }

// sideEffectCounter counts workflow.SideEffect calls; each records a marker.
type sideEffectCounter struct {
	interceptor.WorkerInterceptorBase
	n *int
}

func (c *sideEffectCounter) InterceptWorkflow(_ workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &sideEffectInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, n: c.n}
}

type sideEffectInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	n *int
}

func (i *sideEffectInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	return i.Next.Init(&sideEffectOutbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: outbound}, n: i.n})
}

type sideEffectOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	n *int
}

func (o *sideEffectOutbound) SideEffect(ctx workflow.Context, f func(workflow.Context) any) converter.EncodedValue {
	*o.n++
	return o.Next.SideEffect(ctx, f)
}

func catchupCaseFromEnv(t *testing.T) (catchupCase, bool) {
	gap := os.Getenv("CATCHUP_GAP")
	if gap == "" {
		return catchupCase{}, false
	}
	dur := func(key string) time.Duration {
		v := os.Getenv(key)
		if v == "" {
			return 0
		}
		d, err := time.ParseDuration(v)
		require.NoError(t, err, key)
		return d
	}
	c := catchupCase{
		gap:             dur("CATCHUP_GAP"),
		catchupWindow:   dur("CATCHUP_WINDOW"),
		deadlockTimeout: dur("CATCHUP_DEADLOCK_TIMEOUT"),
	}
	if v := os.Getenv("CATCHUP_OVERLAP"); v != "" {
		p, ok := enumspb.ScheduleOverlapPolicy_value["SCHEDULE_OVERLAP_POLICY_"+strings.ToUpper(strings.ReplaceAll(v, "-", "_"))]
		require.Truef(t, ok, "CATCHUP_OVERLAP=%s: want skip, buffer_one, buffer_all, cancel_other, terminate_other or allow_all", v)
		c.overlap = enumspb.ScheduleOverlapPolicy(p)
	}
	if v := os.Getenv("CATCHUP_RUNNING"); v != "" {
		b, err := strconv.ParseBool(v)
		require.NoError(t, err, "CATCHUP_RUNNING")
		c.running = b
	}
	if v := os.Getenv("CATCHUP_CHUNKED"); v != "" {
		n, err := strconv.Atoi(v)
		require.NoError(t, err, "CATCHUP_CHUNKED")
		c.chunked = n
	}
	if v := os.Getenv("CATCHUP_MAX_BUFFER"); v != "" {
		n, err := strconv.Atoi(v)
		require.NoError(t, err, "CATCHUP_MAX_BUFFER")
		c.maxBuffer = n
	}
	return c, true
}

func firstLine(s string) string {
	if before, _, ok := strings.Cut(s, "\n"); ok {
		return before
	}
	return s
}

// completingDeadlockTimeout replaces the SDK's 1s deadlock detector in tests that walk a
// pre-ChunkedCatchup catch-up and are expected to complete: CI runs that loop ~50x slower
// than a laptop (~70us per match), so 1s makes them flaky.
const completingDeadlockTimeout = time.Minute

// allowSlowWorkflowTask applies completingDeadlockTimeout, or under TEMPORAL_DEBUG the
// SDK's debug defaults, which SetWorkerOptions would otherwise replace.
func allowSlowWorkflowTask(env *testsuite.TestWorkflowEnvironment) {
	if debugging() {
		env.SetTestTimeout(24 * time.Hour)
		env.SetWorkerOptions(sdkworker.Options{DeadlockDetectionTimeout: 24 * time.Hour})
		return
	}
	env.SetWorkerOptions(sdkworker.Options{DeadlockDetectionTimeout: completingDeadlockTimeout})
}

// debugging reports whether the SDK's debug mode (TEMPORAL_DEBUG) is on.
func debugging() bool { return os.Getenv("TEMPORAL_DEBUG") != "" }
