package scheduler_test

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/chasm/lib/scheduler/migration"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.temporal.io/server/common/testing/testlogger"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fakeRun struct {
	status     enumspb.WorkflowExecutionStatus
	firstRunID string
	// next is the NewExecutionRunId recorded on this run's close event.
	next string
}

// fakeWorkflowID models the runs sharing one workflow ID.
type fakeWorkflowID struct {
	current string
	runs    map[string]*fakeRun
	// beforeAttach runs once, on the first attach, to race a close/CAN against it.
	beforeAttach func(*fakeWorkflowID)
	attachErr    error
	attached     []string
}

func (w *fakeWorkflowID) open() bool {
	run := w.runs[w.current]
	return run != nil && (run.status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING ||
		run.status == enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED)
}

// qualify prefixes run IDs with the workflow ID so migrated starts (whose
// request IDs derive from run IDs) stay distinct across workflows.
func (w *fakeWorkflowID) qualify(workflowID string) {
	q := func(runID string) string {
		if runID == "" {
			return ""
		}
		return workflowID + "/" + runID
	}
	runs := make(map[string]*fakeRun, len(w.runs))
	for runID, run := range w.runs {
		run.firstRunID, run.next = q(run.firstRunID), q(run.next)
		runs[q(runID)] = run
	}
	w.runs, w.current = runs, q(w.current)
	if hook := w.beforeAttach; hook != nil {
		w.beforeAttach = func(w *fakeWorkflowID) {
			w.unqualify(workflowID)
			hook(w)
			w.qualify(workflowID)
		}
	}
}

func (w *fakeWorkflowID) unqualify(workflowID string) {
	q := func(runID string) string { return strings.TrimPrefix(runID, workflowID+"/") }
	runs := make(map[string]*fakeRun, len(w.runs))
	for runID, run := range w.runs {
		run.firstRunID, run.next = q(run.firstRunID), q(run.next)
		runs[q(runID)] = run
	}
	w.runs, w.current = runs, q(w.current)
}

func chain(statuses ...enumspb.WorkflowExecutionStatus) *fakeWorkflowID {
	w := &fakeWorkflowID{runs: map[string]*fakeRun{}}
	for i, status := range statuses {
		runID := runName(i)
		run := &fakeRun{status: status, firstRunID: runName(0)}
		if i+1 < len(statuses) {
			run.next = runName(i + 1)
		}
		w.runs[runID] = run
		w.current = runID
	}
	return w
}

func runName(i int) string {
	return "run-" + string(rune('a'+i))
}

type fakeWorkflows struct {
	t         *testing.T
	closeTime *timestamppb.Timestamp
	ids       map[string]*fakeWorkflowID
}

func (f *fakeWorkflows) describe(
	_ context.Context,
	req *historyservice.DescribeWorkflowExecutionRequest,
	_ ...grpc.CallOption,
) (*historyservice.DescribeWorkflowExecutionResponse, error) {
	exec := req.GetRequest().GetExecution()
	w := f.ids[exec.GetWorkflowId()]
	if w == nil {
		return nil, serviceerror.NewNotFound("workflow not found")
	}
	runID := exec.GetRunId()
	if runID == "" {
		runID = w.current
	}
	run := w.runs[runID]
	if run == nil {
		return nil, serviceerror.NewNotFound("run not found")
	}
	info := &workflowpb.WorkflowExecutionInfo{
		Execution:  &commonpb.WorkflowExecution{WorkflowId: exec.GetWorkflowId(), RunId: runID},
		Status:     run.status,
		FirstRunId: run.firstRunID,
	}
	if run.status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING && run.status != enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED {
		info.CloseTime = f.closeTime
	}
	return &historyservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: info}, nil
}

func (f *fakeWorkflows) closeEvent(
	_ context.Context,
	req *workflowservice.GetWorkflowExecutionHistoryRequest,
	_ ...grpc.CallOption,
) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	require.Equal(f.t, enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT, req.GetHistoryEventFilterType())
	exec := req.GetExecution()
	w := f.ids[exec.GetWorkflowId()]
	if w == nil || w.runs[exec.GetRunId()] == nil {
		return nil, serviceerror.NewNotFound("run not found")
	}
	run := w.runs[exec.GetRunId()]
	event := &historypb.HistoryEvent{EventTime: f.closeTime}
	switch run.status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
		event.EventType = enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW
		event.Attributes = &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{
			WorkflowExecutionContinuedAsNewEventAttributes: &historypb.WorkflowExecutionContinuedAsNewEventAttributes{NewExecutionRunId: run.next},
		}
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		event.EventType = enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
		event.Attributes = &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
			WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{NewExecutionRunId: run.next},
		}
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
		event.EventType = enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED
		event.Attributes = &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
			WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{NewExecutionRunId: run.next},
		}
	case enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		event.EventType = enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT
		event.Attributes = &historypb.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{
			WorkflowExecutionTimedOutEventAttributes: &historypb.WorkflowExecutionTimedOutEventAttributes{NewExecutionRunId: run.next},
		}
	case enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		event.EventType = enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		event.EventType = enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED
	default:
		f.t.Fatalf("close event requested for open run %s", exec.GetRunId())
	}
	return &workflowservice.GetWorkflowExecutionHistoryResponse{
		History: &historypb.History{Events: []*historypb.HistoryEvent{event}},
	}, nil
}

func (f *fakeWorkflows) attach(
	_ context.Context,
	req *workflowservice.StartWorkflowExecutionRequest,
	_ ...grpc.CallOption,
) (*workflowservice.StartWorkflowExecutionResponse, error) {
	require.Equal(f.t, enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING, req.GetWorkflowIdConflictPolicy())
	require.Equal(f.t, enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE, req.GetWorkflowIdReusePolicy())
	require.Len(f.t, req.GetCompletionCallbacks(), 1)
	w := f.ids[req.GetWorkflowId()]
	require.NotNil(f.t, w, "attach must not start a workflow that doesn't exist")
	if w.beforeAttach != nil {
		hook := w.beforeAttach
		w.beforeAttach = nil
		hook(w)
	}
	if w.attachErr != nil {
		return nil, w.attachErr
	}
	if !w.open() {
		return nil, serviceerror.NewWorkflowExecutionAlreadyStarted("closed", req.GetRequestId(), w.current)
	}
	w.attached = append(w.attached, w.current)
	return &workflowservice.StartWorkflowExecutionResponse{RunId: w.current, Started: false}, nil
}

// Reattachment after V1->V2 migration must resolve the imported run's whole
// execution chain (as V1's watcher does) instead of re-describing a
// continued-as-new predecessor forever, and must start the generator afterward.
func TestSchedulerCallbacksTask_FollowsExecutionChain(t *testing.T) {
	const (
		running  = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
		can      = enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW
		done     = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
		failed   = enumspb.WORKFLOW_EXECUTION_STATUS_FAILED
		timedOut = enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT
		term     = enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED
	)
	type want struct {
		// status is UNSPECIFIED when the start should be left running with a callback.
		status   enumspb.WorkflowExecutionStatus
		attached []string
	}

	cases := []struct {
		name           string
		pauseOnFailure bool
		ids            func() map[string]*fakeWorkflowID
		want           map[string]want
		wantPaused     bool
		wantErr        bool
	}{
		{
			name: "imported run still running",
			ids:  func() map[string]*fakeWorkflowID { return map[string]*fakeWorkflowID{"wf-1": chain(running)} },
			want: map[string]want{"wf-1": {attached: []string{"run-a"}}},
		},
		{
			name: "CAN successor running attaches to successor",
			ids:  func() map[string]*fakeWorkflowID { return map[string]*fakeWorkflowID{"wf-1": chain(can, running)} },
			want: map[string]want{"wf-1": {attached: []string{"run-b"}}},
		},
		{
			name: "CAN successor completed",
			ids:  func() map[string]*fakeWorkflowID { return map[string]*fakeWorkflowID{"wf-1": chain(can, done)} },
			want: map[string]want{"wf-1": {status: done}},
		},
		{
			name:           "CAN successor failed pauses on failure",
			pauseOnFailure: true,
			ids:            func() map[string]*fakeWorkflowID { return map[string]*fakeWorkflowID{"wf-1": chain(can, failed)} },
			want:           map[string]want{"wf-1": {status: failed}},
			wantPaused:     true,
		},
		{
			name:           "multiple CANs ending timed out pauses on failure",
			pauseOnFailure: true,
			ids: func() map[string]*fakeWorkflowID {
				return map[string]*fakeWorkflowID{"wf-1": chain(can, can, can, timedOut)}
			},
			want:       map[string]want{"wf-1": {status: timedOut}},
			wantPaused: true,
		},
		{
			name: "multiple CANs ending running",
			ids: func() map[string]*fakeWorkflowID {
				return map[string]*fakeWorkflowID{"wf-1": chain(can, can, running)}
			},
			want: map[string]want{"wf-1": {attached: []string{"run-c"}}},
		},
		{
			name:           "successor fails between describe and attach",
			pauseOnFailure: true,
			ids: func() map[string]*fakeWorkflowID {
				w := chain(can, running)
				w.beforeAttach = func(w *fakeWorkflowID) { w.runs["run-b"].status = failed }
				return map[string]*fakeWorkflowID{"wf-1": w}
			},
			want:       map[string]want{"wf-1": {status: failed}},
			wantPaused: true,
		},
		{
			name: "successor continues-as-new between describe and attach",
			ids: func() map[string]*fakeWorkflowID {
				w := chain(can, running)
				w.beforeAttach = func(w *fakeWorkflowID) {
					w.runs["run-b"].status = can
					w.runs["run-b"].next = "run-c"
					w.runs["run-c"] = &fakeRun{status: running, firstRunID: "run-a"}
					w.current = "run-c"
				}
				return map[string]*fakeWorkflowID{"wf-1": w}
			},
			// The first attach lands on run-c, which isn't the described run, so it's re-resolved.
			want: map[string]want{"wf-1": {attached: []string{"run-c", "run-c"}}},
		},
		{
			name:           "whole chain gone from retention synthesizes TERMINATED without pausing",
			pauseOnFailure: true,
			ids:            func() map[string]*fakeWorkflowID { return map[string]*fakeWorkflowID{} },
			want:           map[string]want{"wf-1": {status: term}},
		},
		{
			name: "predecessor gone but chain tail still current",
			ids: func() map[string]*fakeWorkflowID {
				w := chain(can, running)
				delete(w.runs, "run-a")
				return map[string]*fakeWorkflowID{"wf-1": w}
			},
			want: map[string]want{"wf-1": {attached: []string{"run-b"}}},
		},
		{
			name:           "unrelated chain is current: follow ours by close events, never attach",
			pauseOnFailure: true,
			ids: func() map[string]*fakeWorkflowID {
				w := chain(can, can, failed)
				w.runs["other"] = &fakeRun{status: running, firstRunID: "other"}
				w.current = "other"
				return map[string]*fakeWorkflowID{"wf-1": w}
			},
			want:       map[string]want{"wf-1": {status: failed}},
			wantPaused: true,
		},
		{
			name:           "closed chain too long to follow synthesizes TERMINATED",
			pauseOnFailure: true,
			ids: func() map[string]*fakeWorkflowID {
				statuses := slices.Repeat([]enumspb.WorkflowExecutionStatus{can}, 150)
				w := chain(append(statuses, failed)...)
				w.runs["other"] = &fakeRun{status: running, firstRunID: "other"}
				w.current = "other"
				return map[string]*fakeWorkflowID{"wf-1": w}
			},
			want: map[string]want{"wf-1": {status: term}},
		},
		{
			name: "one start failing doesn't block the others or the generator",
			ids: func() map[string]*fakeWorkflowID {
				bad := chain(running)
				bad.attachErr = serviceerror.NewUnavailable("frontend down")
				return map[string]*fakeWorkflowID{
					"wf-1": bad,
					"wf-2": chain(can, done),
					"wf-3": chain(can, running),
				}
			},
			want: map[string]want{
				"wf-1": {},
				"wf-2": {status: done},
				"wf-3": {attached: []string{"run-b"}},
			},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now().UTC()
			ids := tc.ids()
			for workflowID, w := range ids {
				w.qualify(workflowID)
			}
			fake := &fakeWorkflows{t: t, closeTime: timestamppb.New(now.Add(-time.Minute)), ids: ids}

			v1Schedule := defaultSchedule()
			v1Schedule.Policies.PauseOnFailure = tc.pauseOnFailure
			v1Info := &schedulepb.ScheduleInfo{}
			for workflowID := range tc.want {
				v1Info.RunningWorkflows = append(v1Info.RunningWorkflows,
					&commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: workflowID + "/run-a"})
			}
			req := migration.LegacyToCreateFromMigrationStateRequest(v1Schedule, v1Info, &schedulespb.InternalState{
				Namespace:         namespace,
				NamespaceId:       namespaceID,
				ScheduleId:        scheduleID,
				ConflictToken:     1,
				LastProcessedTime: timestamppb.New(now),
			}, nil, nil, now)

			logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
			engine, engineCtx := newTestEngineContext(t, logger)
			_, err := scheduler.NewTestHandler(logger).TestCreateFromMigrationState(engineCtx, req)
			require.NoError(t, err)
			rootRef := chasm.NewComponentRef[*scheduler.Scheduler](chasm.ExecutionKey{
				NamespaceID: namespaceID,
				BusinessID:  scheduleID,
			})

			var root *scheduler.Scheduler
			_, err = chasm.ReadComponent(engineCtx, rootRef,
				func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
					root = s
					require.Zero(t, generatorKicks(s, ctx), "migration must defer generation to the callbacks task")
					return struct{}{}, nil
				}, struct{}{})
			require.NoError(t, err)

			ctrl := gomock.NewController(t)
			historyClient := historyservicemock.NewMockHistoryServiceClient(ctrl)
			historyClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(fake.describe).AnyTimes()
			frontendClient := workflowservicemock.NewMockWorkflowServiceClient(ctrl)
			frontendClient.EXPECT().GetWorkflowExecutionHistory(gomock.Any(), gomock.Any()).DoAndReturn(fake.closeEvent).AnyTimes()
			frontendClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(fake.attach).AnyTimes()
			handler := scheduler.NewSchedulerCallbacksTaskHandler(scheduler.SchedulerCallbacksTaskHandlerOptions{
				Config:         defaultConfig(),
				HistoryClient:  historyClient,
				FrontendClient: frontendClient,
				MetricsHandler: metrics.NoopMetricsHandler,
			})

			dropped, err := chasmtest.ExecuteSideEffectTask(
				engineCtx, engine, root, handler, chasm.TaskAttributes{}, &schedulerpb.SchedulerCallbacksTask{})
			require.False(t, dropped)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			_, err = chasm.ReadComponent(engineCtx, rootRef,
				func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
					require.Positive(t, generatorKicks(s, ctx), "generator must be started after reattachment")
					require.Equal(t, tc.wantPaused, s.Schedule.State.Paused)

					starts := map[string]*schedulespb.BufferedStart{}
					for _, start := range s.Invoker.Get(ctx).GetBufferedStarts() {
						starts[start.GetWorkflowId()] = start
					}
					for workflowID, w := range tc.want {
						start := starts[workflowID]
						require.NotNil(t, start, workflowID)
						if w.status == enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED {
							require.Nil(t, start.GetCompleted(), workflowID)
							require.Equal(t, len(w.attached) > 0, start.GetHasCallback(), workflowID)
						} else {
							require.Equal(t, w.status, start.GetCompleted().GetStatus(), workflowID)
							require.True(t, start.GetHasCallback(), workflowID)
						}
						if fakeID := ids[workflowID]; fakeID != nil {
							var wantAttached []string
							for _, runID := range w.attached {
								wantAttached = append(wantAttached, workflowID+"/"+runID)
							}
							require.Equal(t, wantAttached, fakeID.attached, workflowID)
						}
					}
					return struct{}{}, nil
				}, struct{}{})
			require.NoError(t, err)
		})
	}
}

func generatorKicks(s *scheduler.Scheduler, ctx chasm.Context) int {
	eventLog, ok := s.Generator.Get(ctx).EventLog.TryGet(ctx)
	if !ok {
		return 0
	}
	var n int
	for _, event := range eventLog.GetEvents() {
		if strings.HasPrefix(event.GetMessage(), "scheduled generatorTask") {
			n++
		}
	}
	return n
}
