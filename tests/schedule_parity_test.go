package tests

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protodiff"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TestScheduleV1V2Parity drives a V1 and a V2 schedule through the same scripted
// sequence of workflow outcomes and diffs what each exposes: the started event a
// worker receives for every run, DescribeSchedule after every step, and the
// ListSchedules entry at the end. Comparison is presence-aware (nil != empty);
// only the fields in the normalize* functions are excluded, each with a reason.
func TestScheduleV1V2Parity(t *testing.T) {
	steps := []parityOutcome{
		parityValue, parityNoResult, parityFail, parityNoResult, parityValue,
		parityTimeout, parityValue, parityTerminate, parityCancel, parityNoResult,
	}
	for _, policy := range []enumspb.ScheduleOverlapPolicy{
		enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	} {
		t.Run(policy.String(), func(t *testing.T) {
			t.Parallel()
			runScheduleParity(t, policy, steps)
		})
	}
}

type parityOutcome string

const (
	parityValue     parityOutcome = "value"
	parityNoResult  parityOutcome = "no-result"
	parityFail      parityOutcome = "fail"
	parityTimeout   parityOutcome = "timeout"
	parityTerminate parityOutcome = "terminate"
	parityCancel    parityOutcome = "cancel"
)

type paritySchedule struct {
	name       string
	scheduleID string
	workflowID string
	newContext contextFactory
}

func runScheduleParity(t *testing.T, policy enumspb.ScheduleOverlapPolicy, steps []parityOutcome) {
	// Must precede env creation, which otherwise fixes the default timeout.
	ctx := testcontext.For(t, testcontext.WithTimeout(5*time.Minute))
	s := newScheduleEnv(t, append(scheduleCommonOpts(t), testcore.WithWorkerService("V1 scheduler for parity"))...)

	v1 := paritySchedule{name: "v1", newContext: v1ContextFactory,
		scheduleID: testcore.RandomizeStr("parity-v1"), workflowID: testcore.RandomizeStr("parity-v1-wf")}
	v2 := paritySchedule{name: "v2", newContext: chasmContextFactory,
		scheduleID: testcore.RandomizeStr("parity-v2"), workflowID: testcore.RandomizeStr("parity-v2-wf")}
	wt := testcore.RandomizeStr("parity-wt")

	// Current step per schedule, keyed by workflow ID prefix.
	type parityStep struct {
		Index   int
		Outcome parityOutcome
	}
	var outcomes sync.Map
	s.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) (*string, error) {
		var step parityStep
		_ = workflow.SideEffect(ctx, func(workflow.Context) any {
			id := workflow.GetInfo(ctx).WorkflowExecution.ID
			for _, sched := range []paritySchedule{v1, v2} {
				if strings.HasPrefix(id, sched.workflowID) {
					o, _ := outcomes.Load(sched.workflowID)
					return o
				}
			}
			return parityStep{Outcome: parityValue}
		}).Get(&step)
		switch step.Outcome {
		case parityNoResult:
			return nil, nil
		case parityFail:
			return nil, temporal.NewNonRetryableApplicationError("scripted failure", "parity", nil)
		case parityTimeout, parityTerminate, parityCancel:
			err := workflow.Await(ctx, func() bool { return false })
			return nil, err
		default:
			// Identical across V1 and V2 so the next run's last completion result compares equal.
			result := fmt.Sprintf("result-%d", step.Index)
			return &result, nil
		}
	}, workflow.RegisterOptions{Name: wt})

	for _, sched := range []paritySchedule{v1, v2} {
		action := startWorkflowAction(s, sched.workflowID, wt)
		action.GetStartWorkflow().WorkflowRunTimeout = durationpb.New(4 * time.Second)
		_, err := s.FrontendClient().CreateSchedule(sched.newContext(ctx), &workflowservice.CreateScheduleRequest{
			Namespace:  s.Namespace().String(),
			ScheduleId: sched.scheduleID,
			Schedule: &schedulepb.Schedule{
				Spec:     &schedulepb.ScheduleSpec{},
				Action:   action,
				Policies: &schedulepb.SchedulePolicies{OverlapPolicy: policy},
				State:    &schedulepb.ScheduleState{Paused: true, Notes: "parity"},
			},
			Identity:  "test",
			RequestId: uuid.NewString(),
		})
		require.NoError(t, err, sched.name)
	}

	for i, outcome := range steps {
		step := fmt.Sprintf("step %d (%s)", i, outcome)
		started := map[string]*historypb.WorkflowExecutionStartedEventAttributes{}
		closed := map[string]enumspb.EventType{}
		for _, sched := range []paritySchedule{v1, v2} {
			outcomes.Store(sched.workflowID, parityStep{Index: i, Outcome: outcome})
			execution := triggerParityStep(t, s, ctx, sched, policy, i+1)
			switch outcome {
			case parityTerminate:
				require.NoError(t, s.SdkClient().TerminateWorkflow(ctx, execution.GetWorkflowId(), execution.GetRunId(), "parity"))
			case parityCancel:
				require.NoError(t, s.SdkClient().CancelWorkflow(ctx, execution.GetWorkflowId(), execution.GetRunId()))
			case parityValue, parityNoResult, parityFail, parityTimeout:
			}
			started[sched.name], closed[sched.name] = parityRunEvents(t, s, ctx, execution)
		}

		v1Started, v2Started := normalizeParityStarted(started["v1"]), normalizeParityStarted(started["v2"])
		logParityGaps(t, step+": started event", reconcileStartedGaps(v1Started, v2Started))
		if diff := protodiff.Diff(v1Started, v2Started); diff != "" {
			t.Errorf("%s: started event differs (-v1 +v2):\n%s", step, diff)
		}
		if closed["v1"] != closed["v2"] {
			t.Errorf("%s: close event differs: v1=%s v2=%s", step, closed["v1"], closed["v2"])
		}
		// V1 refreshes completion state asynchronously, so compare once converged.
		awaitParity(ctx, t, step+": DescribeSchedule", func(at *await.T) (proto.Message, proto.Message, []string) {
			v1Desc := normalizeParityDescribe(describeParity(at, s, ctx, v1), v1)
			v2Desc := normalizeParityDescribe(describeParity(at, s, ctx, v2), v2)
			return v1Desc, v2Desc, reconcileDescribeGaps(v1Desc, v2Desc)
		})
	}

	awaitParity(ctx, t, "ListSchedules", func(at *await.T) (proto.Message, proto.Message, []string) {
		v1Entry := normalizeParityListEntry(listParity(at, s, ctx, v1))
		v2Entry := normalizeParityListEntry(listParity(at, s, ctx, v2))
		return v1Entry, v2Entry, reconcileListGaps(v1Entry, v2Entry)
	})
}

// triggerParityStep fires one manual action and returns the run it started.
func triggerParityStep(
	t *testing.T,
	s *testcore.TestEnv,
	ctx context.Context,
	sched paritySchedule,
	policy enumspb.ScheduleOverlapPolicy,
	wantActions int,
) *commonpb.WorkflowExecution {
	t.Helper()
	// Generated workflow IDs have second resolution; keep triggers in distinct
	// seconds. Nothing observable to await on, so sleep to the next boundary.
	time.Sleep(time.Until(time.Now().Truncate(time.Second).Add(time.Second))) //nolint:forbidigo
	_, err := s.FrontendClient().PatchSchedule(sched.newContext(ctx), &workflowservice.PatchScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sched.scheduleID,
		Patch:      triggerPatch(policy),
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	require.NoError(t, err, sched.name)

	var execution *commonpb.WorkflowExecution
	await.Requiref(ctx, t, func(at *await.T) {
		actions := describeParity(at, s, ctx, sched).GetInfo().GetRecentActions()
		require.GreaterOrEqual(at, len(actions), wantActions)
		execution = actions[len(actions)-1].GetStartWorkflowResult()
	}, 20*time.Second, 100*time.Millisecond, "%s: action %d never started", sched.name, wantActions)
	return execution
}

// parityRunEvents waits for the run to close and returns its first and last events.
func parityRunEvents(
	t *testing.T,
	s *testcore.TestEnv,
	ctx context.Context,
	execution *commonpb.WorkflowExecution,
) (*historypb.WorkflowExecutionStartedEventAttributes, enumspb.EventType) {
	t.Helper()
	await.Requiref(ctx, t, func(at *await.T) {
		desc, err := s.SdkClient().DescribeWorkflowExecution(at.Context(), execution.GetWorkflowId(), execution.GetRunId())
		require.NoError(at, err)
		require.NotEqual(at, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, desc.GetWorkflowExecutionInfo().GetStatus())
	}, 20*time.Second, 100*time.Millisecond, "run %s never closed", execution.GetWorkflowId())

	var events []*historypb.HistoryEvent
	iter := s.SdkClient().GetWorkflowHistory(ctx, execution.GetWorkflowId(), execution.GetRunId(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		require.NoError(t, err)
		events = append(events, event)
	}
	require.NotEmpty(t, events)
	return events[0].GetWorkflowExecutionStartedEventAttributes(), events[len(events)-1].GetEventType()
}

func describeParity(t require.TestingT, s *testcore.TestEnv, ctx context.Context, sched paritySchedule) *workflowservice.DescribeScheduleResponse {
	resp, err := s.FrontendClient().DescribeSchedule(sched.newContext(ctx), &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sched.scheduleID,
	})
	require.NoError(t, err, sched.name)
	return resp
}

func listParity(t require.TestingT, s *testcore.TestEnv, ctx context.Context, sched paritySchedule) *schedulepb.ScheduleListEntry {
	resp, err := s.FrontendClient().ListSchedules(sched.newContext(ctx), &workflowservice.ListSchedulesRequest{
		Namespace:       s.Namespace().String(),
		MaximumPageSize: 100,
		Query:           fmt.Sprintf("ScheduleId = '%s'", sched.scheduleID),
	})
	require.NoError(t, err, sched.name)
	for _, entry := range resp.GetSchedules() {
		if entry.GetScheduleId() == sched.scheduleID {
			return entry
		}
	}
	return nil
}

// awaitParity retries until both sides match, then reports the last diff.
func awaitParity(
	ctx context.Context,
	t *testing.T,
	what string,
	get func(*await.T) (proto.Message, proto.Message, []string),
) {
	t.Helper()
	var gaps []string
	await.Require(ctx, t, func(at *await.T) {
		v1, v2, matched := get(at)
		if diff := protodiff.Diff(v1, v2); diff != "" {
			at.Errorf("%s differs (-v1 +v2):\n%s", what, diff)
			return
		}
		gaps = matched
	}, 15*time.Second, 200*time.Millisecond)
	logParityGaps(t, what, gaps)
}

// normalizeParityStarted removes per-execution identity and wall-clock fields.
func normalizeParityStarted(attrs *historypb.WorkflowExecutionStartedEventAttributes) *historypb.WorkflowExecutionStartedEventAttributes {
	if attrs == nil {
		return nil
	}
	n := proto.CloneOf(attrs)
	n.WorkflowId = ""
	n.OriginalExecutionRunId = ""
	n.FirstExecutionRunId = ""
	n.WorkflowExecutionExpirationTime = nil
	n.RootWorkflowExecution = nil
	// Implementation identity: V1 starts from the scheduler workflow's worker, V2 from the history service.
	n.Identity = ""
	// V2 attaches a completion callback to every start; V1 watches with an activity.
	n.CompletionCallbacks = nil
	// Per-schedule identity and the trigger wall-clock time.
	normalizeParitySearchAttributes(n.GetSearchAttributes())
	return n
}

func normalizeParityDescribe(resp *workflowservice.DescribeScheduleResponse, sched paritySchedule) *workflowservice.DescribeScheduleResponse {
	n := proto.CloneOf(resp)
	// Opaque, implementation-defined token.
	n.ConflictToken = nil
	// V2-only addition; V1 never reports state size.
	if info := n.GetInfo(); info != nil {
		info.StateSizeBytes = 0
	}
	n.GetSchedule().GetAction().GetStartWorkflow().WorkflowId = ""
	if info := n.GetInfo(); info != nil {
		info.CreateTime = nil
		info.UpdateTime = nil
		normalizeParityActions(info.RecentActions)
		for _, running := range info.RunningWorkflows {
			running.WorkflowId = strings.TrimPrefix(running.GetWorkflowId(), sched.workflowID)
			running.RunId = ""
		}
	}
	normalizeParitySearchAttributes(n.GetSearchAttributes())
	return n
}

func normalizeParityListEntry(entry *schedulepb.ScheduleListEntry) *schedulepb.ScheduleListEntry {
	if entry == nil {
		return nil
	}
	n := proto.CloneOf(entry)
	n.ScheduleId = ""
	if info := n.GetInfo(); info != nil {
		normalizeParityActions(info.RecentActions)
	}
	normalizeParitySearchAttributes(n.GetSearchAttributes())
	return n
}

// normalizeParityActions keeps action order, status and shape but not times or IDs.
func normalizeParityActions(actions []*schedulepb.ScheduleActionResult) {
	for _, action := range actions {
		action.ScheduleTime = nil
		action.ActualTime = nil
		if result := action.GetStartWorkflowResult(); result != nil {
			result.WorkflowId = ""
			result.RunId = ""
		}
	}
}

func normalizeParitySearchAttributes(sas *commonpb.SearchAttributes) {
	if sas == nil {
		return
	}
	for _, name := range []string{sadefs.TemporalScheduledById, sadefs.TemporalScheduledStartTime} {
		if _, ok := sas.GetIndexedFields()[name]; ok {
			sas.IndexedFields[name] = &commonpb.Payload{}
		}
	}
}

// Known V1/V2 divergences. Each reconcile* erases only the exact known shape on
// the normalized copies and reports it, so new divergences still fail the test.
// Remove an entry once the divergence is fixed.
const (
	// V1's watcher reports a failure only for FAILED; V2 records a synthesized
	// Nexus failure for CANCELED, TERMINATED and TIMED_OUT, so GetLastError differs.
	gapContinuedFailureFromNonFailedClose = "v2 continued_failure from canceled/terminated/timed-out run"
	// V2 returns empty-but-present memo/search_attributes where V1 leaves them unset.
	gapDescribeEmptyMemoAndSearchAttributes = "v2 describe returns empty memo/search_attributes instead of unset"
	// V1's list entry carries an empty-but-present memo where V2 leaves it unset.
	gapListEmptyMemo = "v1 list entry returns empty memo instead of unset"
)

func reconcileStartedGaps(v1, v2 *historypb.WorkflowExecutionStartedEventAttributes) []string {
	failure := v2.GetContinuedFailure()
	if failure == nil || proto.Equal(failure, v1.GetContinuedFailure()) {
		return nil
	}
	if failure.GetCanceledFailureInfo() == nil && failure.GetTerminatedFailureInfo() == nil && failure.GetTimeoutFailureInfo() == nil {
		return nil
	}
	v2.ContinuedFailure = v1.GetContinuedFailure()
	return []string{gapContinuedFailureFromNonFailedClose}
}

func reconcileDescribeGaps(v1, v2 *workflowservice.DescribeScheduleResponse) []string {
	var gaps []string
	if v1.Memo == nil && v2.Memo != nil && len(v2.GetMemo().GetFields()) == 0 {
		v2.Memo = nil
		gaps = append(gaps, gapDescribeEmptyMemoAndSearchAttributes)
	}
	if v1.SearchAttributes == nil && v2.SearchAttributes != nil && len(v2.GetSearchAttributes().GetIndexedFields()) == 0 {
		v2.SearchAttributes = nil
		gaps = append(gaps, gapDescribeEmptyMemoAndSearchAttributes)
	}
	return gaps
}

func reconcileListGaps(v1, v2 *schedulepb.ScheduleListEntry) []string {
	if v1 == nil || v2 == nil {
		return nil
	}
	if v2.Memo == nil && v1.Memo != nil && len(v1.GetMemo().GetFields()) == 0 {
		v1.Memo = nil
		return []string{gapListEmptyMemo}
	}
	return nil
}

func logParityGaps(t *testing.T, what string, gaps []string) {
	t.Helper()
	for _, gap := range slices.Compact(gaps) {
		t.Logf("%s: known V1/V2 gap: %s", what, gap)
	}
}
