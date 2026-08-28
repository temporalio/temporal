package scheduleinvariants

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	chasmspb "go.temporal.io/server/api/chasm/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/api/visibilityservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.temporal.io/server/common/testing/mocksdk"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const testClusterName = "test-cluster"

// testNow anchors the injected clock; the overdue re-check reads timeSource.Now().
var testNow = time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)

type testDeps struct {
	ctrl              *gomock.Controller
	visibilityManager *manager.MockVisibilityManager
	namespaceRegistry *namespace.MockRegistry
	sdkClientFactory  *sdk.MockClientFactory
	sdkClient         *mocksdk.MockClient
	frontendClient    *workflowservicemock.MockWorkflowServiceClient
	timeSource        *clock.EventTimeSource
	metricsHandler    metrics.Handler
}

func newTestDeps(t *testing.T) *testDeps {
	t.Helper()
	ctrl := gomock.NewController(t)
	d := &testDeps{
		ctrl:              ctrl,
		visibilityManager: manager.NewMockVisibilityManager(ctrl),
		namespaceRegistry: namespace.NewMockRegistry(ctrl),
		sdkClientFactory:  sdk.NewMockClientFactory(ctrl),
		sdkClient:         mocksdk.NewMockClient(ctrl),
		frontendClient:    workflowservicemock.NewMockWorkflowServiceClient(ctrl),
		timeSource:        clock.NewEventTimeSource(),
		metricsHandler:    metrics.NoopMetricsHandler,
	}
	d.timeSource.Update(testNow)
	// The DescribeSchedule path always goes via system client → frontend stub.
	d.sdkClientFactory.EXPECT().GetSystemClient().Return(d.sdkClient).AnyTimes()
	d.sdkClient.EXPECT().WorkflowService().Return(d.frontendClient).AnyTimes()
	return d
}

func (d *testDeps) newActivities() *Activities {
	return d.newActivitiesWithParams(dynamicconfig.DefaultScheduleInvariantsScannerParams)
}

func (d *testDeps) newActivitiesWithParams(params dynamicconfig.ScheduleInvariantsScannerParams) *Activities {
	// A very high RPS rate-limiter so Wait() never blocks under test.
	rl := quotas.NewDefaultOutgoingRateLimiter(quotas.RateFn(dynamicconfig.GetFloatPropertyFn(10000.0)))
	return &Activities{
		logger:             log.NewNoopLogger(),
		metricsHandler:     d.metricsHandler,
		visibilityManager:  d.visibilityManager,
		namespaceRegistry:  d.namespaceRegistry,
		sdkClientFactory:   d.sdkClientFactory,
		currentClusterName: testClusterName,
		timeSource:         d.timeSource,
		opts:               dynamicconfig.GetTypedPropertyFn(params),
		rateLimiter:        rl,
	}
}

func localNS(id, name, activeCluster string) *namespace.Namespace {
	return namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: id, Name: name},
		nil,
		activeCluster,
	)
}

// deletedNS builds a local namespace in the DELETED state, which ListAllNamespaces skips.
func deletedNS(id, name, activeCluster string) *namespace.Namespace {
	return namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: id, Name: name, State: enumspb.NAMESPACE_STATE_DELETED},
		nil,
		activeCluster,
	)
}

// globalNS builds a global (replicated) namespace whose active cluster is
// activeCluster. Only global namespaces return false from ActiveInCluster when the
// active cluster doesn't match; local namespaces are always "active" in every cluster.
func globalNS(id, name, activeCluster string) *namespace.Namespace {
	return namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: id, Name: name},
		nil,
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: activeCluster,
			Clusters:          []string{activeCluster, "other-cluster"},
		},
		0,
	)
}

func TestListAllNamespaces_FiltersInactiveAndDeleted(t *testing.T) {
	d := newTestDeps(t)

	d.namespaceRegistry.EXPECT().GetAllNamespaces().Return([]*namespace.Namespace{
		localNS("id-1", "ns-1", testClusterName),
		globalNS("id-2", "ns-2", "other-cluster"),  // inactive in this cluster
		globalNS("id-3", "ns-3", testClusterName),  // active here
		deletedNS("id-4", "ns-4", testClusterName), // deleted
	})

	names := d.newActivities().ListAllNamespaces()
	require.ElementsMatch(t, []string{"ns-1", "ns-3"}, names,
		"ns-2 is active in another cluster: evaluating its invariants here would read a "+
			"standby replica's stale visibility records")
}

func TestForEachNamespace_InvokesCallbackWithCount(t *testing.T) {
	d := newTestDeps(t)

	d.namespaceRegistry.EXPECT().GetNamespaceID(namespace.Name("ns-1")).Return(namespace.ID("id-1"), nil)
	d.visibilityManager.EXPECT().CountChasmExecutions(gomock.Any(), &visibilityservice.CountChasmExecutionsRequest{
		ArchetypeId: chasm.SchedulerArchetypeID,
		NamespaceId: "id-1",
		Namespace:   "ns-1",
		Query:       "some-query",
	}).Return(&visibilityservice.CountChasmExecutionsResponse{Count: 7}, nil)

	var got int64
	err := d.newActivities().forEachNamespace(context.Background(), "ns-1", "some-query", func(count int64) {
		got = count
	})
	require.NoError(t, err)
	require.Equal(t, int64(7), got)
}

func TestForEachNamespace_PropagatesVisibilityError(t *testing.T) {
	d := newTestDeps(t)

	d.namespaceRegistry.EXPECT().GetNamespaceID(namespace.Name("ns-1")).Return(namespace.ID("id-1"), nil)
	d.visibilityManager.EXPECT().CountChasmExecutions(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("count failed"))

	called := false
	err := d.newActivities().forEachNamespace(context.Background(), "ns-1", "q", func(count int64) {
		called = true
	})
	require.Error(t, err)
	require.False(t, called, "callback should not fire on error")
}

func chasmExec(id string) *chasmspb.VisibilityExecutionInfo {
	return &chasmspb.VisibilityExecutionInfo{BusinessId: id}
}

func TestSchedulesInNamespace_PaginatesAndYieldsEachSchedule(t *testing.T) {
	d := newTestDeps(t)

	d.namespaceRegistry.EXPECT().GetNamespaceID(namespace.Name("ns-1")).Return(namespace.ID("id-1"), nil)

	d.visibilityManager.EXPECT().ListChasmExecutions(gomock.Any(), &visibilityservice.ListChasmExecutionsRequest{
		ArchetypeId:   chasm.SchedulerArchetypeID,
		NamespaceId:   "id-1",
		Namespace:     "ns-1",
		Query:         "q",
		PageSize:      scheduleListPageSize,
		NextPageToken: nil,
	}).Return(&visibilityservice.ListChasmExecutionsResponse{
		Executions:    []*chasmspb.VisibilityExecutionInfo{chasmExec("sched-1"), chasmExec("sched-2")},
		NextPageToken: []byte("p2"),
	}, nil)
	d.visibilityManager.EXPECT().ListChasmExecutions(gomock.Any(), &visibilityservice.ListChasmExecutionsRequest{
		ArchetypeId:   chasm.SchedulerArchetypeID,
		NamespaceId:   "id-1",
		Namespace:     "ns-1",
		Query:         "q",
		PageSize:      scheduleListPageSize,
		NextPageToken: []byte("p2"),
	}).Return(&visibilityservice.ListChasmExecutionsResponse{
		Executions:    []*chasmspb.VisibilityExecutionInfo{chasmExec("sched-3")},
		NextPageToken: nil,
	}, nil)

	var visited []string
	var iterErr error
	for scheduleID, err := range d.newActivities().schedulesInNamespace(context.Background(), "ns-1", "q") {
		if err != nil {
			iterErr = err
			break
		}
		visited = append(visited, scheduleID)
	}
	require.NoError(t, iterErr)
	require.Equal(t, []string{"sched-1", "sched-2", "sched-3"}, visited)
}

func TestSchedulesInNamespace_YieldsErrorAndStops(t *testing.T) {
	d := newTestDeps(t)

	d.namespaceRegistry.EXPECT().GetNamespaceID(namespace.Name("ns-1")).Return(namespace.ID("id-1"), nil)
	d.visibilityManager.EXPECT().ListChasmExecutions(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("list failed"))

	var iterErr error
	for scheduleID, err := range d.newActivities().schedulesInNamespace(context.Background(), "ns-1", "q") {
		if err != nil {
			iterErr = err
			continue
		}
		t.Fatalf("should not visit any schedule, got %q", scheduleID)
	}
	require.Error(t, iterErr)
}

var overdueTolerance = dynamicconfig.DefaultScheduleInvariantsScannerParams.OverdueNextActionTimeTolerance

// overdueActionTime confirms the invariant; pendingActionTime clears it on re-check.
func overdueActionTime() time.Time {
	return testNow.Add(-overdueTolerance).Add(-time.Hour)
}

func pendingActionTime() time.Time {
	return testNow.Add(time.Hour)
}

// describeResp builds a DescribeSchedule response. Passing no futureActionTimes models
// a schedule with no upcoming action.
func describeResp(
	paused bool,
	overlap enumspb.ScheduleOverlapPolicy,
	runningCount int,
	futureActionTimes ...time.Time,
) *workflowservice.DescribeScheduleResponse {
	resp := &workflowservice.DescribeScheduleResponse{
		Schedule: &schedulepb.Schedule{
			State:    &schedulepb.ScheduleState{Paused: paused},
			Policies: &schedulepb.SchedulePolicies{OverlapPolicy: overlap},
		},
		Info: &schedulepb.ScheduleInfo{},
	}
	for range runningCount {
		resp.Info.RunningWorkflows = append(resp.Info.RunningWorkflows, &commonpb.WorkflowExecution{WorkflowId: "running"})
	}
	for _, t := range futureActionTimes {
		resp.Info.FutureActionTimes = append(resp.Info.FutureActionTimes, timestamppb.New(t))
	}
	return resp
}

func TestScheduleIsExpectedNotToFire(t *testing.T) {
	cases := []struct {
		name string
		resp *workflowservice.DescribeScheduleResponse
		err  error
		want bool
	}{
		{
			name: "paused",
			resp: describeResp(true, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, 0),
			want: true,
		},
		{
			name: "buffer_one_with_running_workflow",
			resp: describeResp(false, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, 1),
			want: true,
		},
		{
			name: "buffer_all_with_running_workflow",
			resp: describeResp(false, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, 2),
			want: true,
		},
		{
			name: "buffer_one_no_running_workflow",
			resp: describeResp(false, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, 0, overdueActionTime()),
			want: false,
		},
		{
			name: "skip_policy_with_running_workflow_still_overdue",
			resp: describeResp(false, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, 1, overdueActionTime()),
			want: false,
		},
		{
			name: "cancel_other_policy",
			resp: describeResp(false, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, 1, overdueActionTime()),
			want: false,
		},
		{
			name: "describe_error",
			err:  errors.New("describe failed"),
			want: false,
		},
		{
			// Stale index entry: a standby's frozen record, or a SKIP schedule whose
			// action overran while the Generator kept ticking.
			name: "next_action_time_still_pending",
			resp: describeResp(false, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, 1, pendingActionTime()),
			want: true,
		},
		{
			// Nothing pending can be late.
			name: "no_future_action_times",
			resp: describeResp(false, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, 0),
			want: true,
		},
		{
			name: "next_action_time_exactly_at_threshold",
			resp: describeResp(false, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, 0, testNow.Add(-overdueTolerance)),
			want: true,
		},
		{
			name: "next_action_time_just_past_threshold",
			resp: describeResp(false, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, 0,
				testNow.Add(-overdueTolerance).Add(-time.Nanosecond)),
			want: false,
		},
		{
			// The stalled-generator shape: visibility indexes FutureActionTimes[0],
			// which is overdue, while the rest of the cached horizon is still future.
			// Requiring every entry to be overdue would delay detection by the full
			// cache depth.
			name: "only_earliest_entry_overdue",
			resp: func() *workflowservice.DescribeScheduleResponse {
				times := []time.Time{overdueActionTime()}
				for i := range 9 {
					times = append(times, testNow.Add(time.Duration(i+1)*time.Hour))
				}
				return describeResp(false, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, 0, times...)
			}(),
			want: false,
		},
		{
			// Ordering isn't guaranteed: the earliest entry decides, wherever it sits.
			name: "unordered_earliest_is_overdue",
			resp: describeResp(false, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, 0,
				pendingActionTime(), overdueActionTime()),
			want: false,
		},
		{
			// Stale index entry: every cached time is still in the future.
			name: "all_entries_pending",
			resp: describeResp(false, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, 0,
				pendingActionTime(), pendingActionTime().Add(time.Hour)),
			want: true,
		},
		{
			// A nil entry must not read as the zero time, which would look overdue.
			name: "nil_entry_among_pending_times",
			resp: func() *workflowservice.DescribeScheduleResponse {
				r := describeResp(false, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, 0, pendingActionTime())
				r.Info.FutureActionTimes = append([]*timestamppb.Timestamp{nil}, r.Info.FutureActionTimes...)
				return r
			}(),
			want: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := newTestDeps(t)
			d.frontendClient.EXPECT().DescribeSchedule(gomock.Any(), &workflowservice.DescribeScheduleRequest{
				Namespace:  "ns-1",
				ScheduleId: "sched-1",
			}).Return(tc.resp, tc.err)

			got := d.newActivities().scheduleIsExpectedNotToFire(context.Background(), "ns-1", "sched-1")
			require.Equal(t, tc.want, got)
		})
	}
}

func TestRunOverdueScan_FiltersExpectedNotToFireSchedulesAndCountsRest(t *testing.T) {
	d := newTestDeps(t)

	d.namespaceRegistry.EXPECT().GetAllNamespaces().Return([]*namespace.Namespace{localNS("id-1", "ns-1", testClusterName)})
	d.namespaceRegistry.EXPECT().GetNamespaceID(namespace.Name("ns-1")).Return(namespace.ID("id-1"), nil)

	d.visibilityManager.EXPECT().ListChasmExecutions(gomock.Any(), gomock.Any()).Return(&visibilityservice.ListChasmExecutionsResponse{
		Executions: []*chasmspb.VisibilityExecutionInfo{
			chasmExec("sched-paused"),
			chasmExec("sched-buffer-waiting"),
			chasmExec("sched-actually-overdue"),
		},
		NextPageToken: nil,
	}, nil)

	d.frontendClient.EXPECT().DescribeSchedule(gomock.Any(), &workflowservice.DescribeScheduleRequest{
		Namespace: "ns-1", ScheduleId: "sched-paused",
	}).Return(describeResp(true, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, 0), nil)
	d.frontendClient.EXPECT().DescribeSchedule(gomock.Any(), &workflowservice.DescribeScheduleRequest{
		Namespace: "ns-1", ScheduleId: "sched-buffer-waiting",
	}).Return(describeResp(false, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, 1), nil)
	d.frontendClient.EXPECT().DescribeSchedule(gomock.Any(), &workflowservice.DescribeScheduleRequest{
		Namespace: "ns-1", ScheduleId: "sched-actually-overdue",
	}).Return(describeResp(false, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, 0, overdueActionTime()), nil)

	rec := metricstest.NewCaptureHandler()
	d.metricsHandler = rec
	capture := rec.StartCapture()
	defer rec.StopCapture(capture)

	err := d.newActivities().runOverdueScan(context.Background(), "q")
	require.NoError(t, err)

	snapshot := capture.Snapshot()
	anomalies := snapshot[metrics.ScheduleInvariantsScannerOverdueNextActionTimeCount.Name()]
	require.Len(t, anomalies, 1)
	require.Equal(t, int64(1), anomalies[0].Value, "only sched-actually-overdue should count")
	require.Empty(t, snapshot[metrics.ScheduleInvariantsScannerOverdueNextActionTimeStaleCandidateCount.Name()],
		"paused and buffer-waiting are exemptions, not stale candidates")
}

// Asserts by absence: with no expectations registered, any call for ns-passive fails.
func TestRunOverdueScan_SkipsNamespaceActiveInAnotherCluster(t *testing.T) {
	d := newTestDeps(t)

	d.namespaceRegistry.EXPECT().GetAllNamespaces().Return([]*namespace.Namespace{
		globalNS("id-passive", "ns-passive", "other-cluster"),
	})

	err := d.newActivities().runOverdueScan(context.Background(), "q")
	require.NoError(t, err)
}

// Same gate for the count-only scanners, which have no confirmation step at all.
func TestRunScan_SkipsNamespaceActiveInAnotherCluster(t *testing.T) {
	d := newTestDeps(t)

	d.namespaceRegistry.EXPECT().GetAllNamespaces().Return([]*namespace.Namespace{
		globalNS("id-passive", "ns-passive", "other-cluster"),
		localNS("id-local", "ns-local", testClusterName),
	})
	// Only the local namespace is queried.
	d.namespaceRegistry.EXPECT().GetNamespaceID(namespace.Name("ns-local")).Return(namespace.ID("id-local"), nil)
	d.visibilityManager.EXPECT().CountChasmExecutions(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *visibilityservice.CountChasmExecutionsRequest) (*visibilityservice.CountChasmExecutionsResponse, error) {
			require.Equal(t, "ns-local", req.Namespace)
			return &visibilityservice.CountChasmExecutionsResponse{Count: 3}, nil
		})

	err := d.newActivities().runScan(context.Background(), "stuck_open", "q", "some_metric")
	require.NoError(t, err)
}

// A stale candidate is not an anomaly, but must still be counted so the suppression
// is observable.
func TestRunOverdueScan_StaleCandidateIsCountedSeparatelyNotAsAnomaly(t *testing.T) {
	d := newTestDeps(t)

	d.namespaceRegistry.EXPECT().GetAllNamespaces().Return([]*namespace.Namespace{localNS("id-1", "ns-1", testClusterName)})
	d.namespaceRegistry.EXPECT().GetNamespaceID(namespace.Name("ns-1")).Return(namespace.ID("id-1"), nil)
	d.visibilityManager.EXPECT().ListChasmExecutions(gomock.Any(), gomock.Any()).Return(&visibilityservice.ListChasmExecutionsResponse{
		Executions: []*chasmspb.VisibilityExecutionInfo{chasmExec("sched-stale")},
	}, nil)
	d.frontendClient.EXPECT().DescribeSchedule(gomock.Any(), gomock.Any()).
		Return(describeResp(false, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, 1, pendingActionTime()), nil)

	rec := metricstest.NewCaptureHandler()
	d.metricsHandler = rec
	capture := rec.StartCapture()
	defer rec.StopCapture(capture)

	require.NoError(t, d.newActivities().runOverdueScan(context.Background(), "q"))

	snapshot := capture.Snapshot()
	require.Empty(t, snapshot[metrics.ScheduleInvariantsScannerOverdueNextActionTimeCount.Name()],
		"a stale visibility entry is not an anomaly")
	stale := snapshot[metrics.ScheduleInvariantsScannerOverdueNextActionTimeStaleCandidateCount.Name()]
	require.Len(t, stale, 1)
	require.Equal(t, "ns-1", stale[0].Tags["namespace"])
}

func TestRunOverdueScan_ContinuesPastPerNamespaceErrors(t *testing.T) {
	d := newTestDeps(t)

	d.namespaceRegistry.EXPECT().GetAllNamespaces().Return([]*namespace.Namespace{
		localNS("id-broken", "ns-broken", testClusterName),
		localNS("id-ok", "ns-ok", testClusterName),
	})

	d.namespaceRegistry.EXPECT().GetNamespaceID(namespace.Name("ns-broken")).Return(namespace.ID("id-broken"), nil)
	d.visibilityManager.EXPECT().ListChasmExecutions(gomock.Any(), gomock.AssignableToTypeOf(&visibilityservice.ListChasmExecutionsRequest{})).
		DoAndReturn(func(_ context.Context, req *visibilityservice.ListChasmExecutionsRequest) (*visibilityservice.ListChasmExecutionsResponse, error) {
			if req.Namespace == "ns-broken" {
				return nil, errors.New("list failed")
			}
			return &visibilityservice.ListChasmExecutionsResponse{
				Executions:    []*chasmspb.VisibilityExecutionInfo{chasmExec("sched-1")},
				NextPageToken: nil,
			}, nil
		}).AnyTimes()

	d.namespaceRegistry.EXPECT().GetNamespaceID(namespace.Name("ns-ok")).Return(namespace.ID("id-ok"), nil)
	d.frontendClient.EXPECT().DescribeSchedule(gomock.Any(), gomock.Any()).
		Return(describeResp(false, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, 0), nil)

	err := d.newActivities().runOverdueScan(context.Background(), "q")
	require.NoError(t, err)
}

func TestRunOverdueScan_StopsAtPerNamespaceCap(t *testing.T) {
	d := newTestDeps(t)

	d.namespaceRegistry.EXPECT().GetAllNamespaces().Return([]*namespace.Namespace{localNS("id-1", "ns-1", testClusterName)})
	d.namespaceRegistry.EXPECT().GetNamespaceID(namespace.Name("ns-1")).Return(namespace.ID("id-1"), nil)

	// Five overdue schedules, but the cap is 2: only the first two should be checked.
	d.visibilityManager.EXPECT().ListChasmExecutions(gomock.Any(), gomock.Any()).Return(&visibilityservice.ListChasmExecutionsResponse{
		Executions: []*chasmspb.VisibilityExecutionInfo{
			chasmExec("sched-1"), chasmExec("sched-2"), chasmExec("sched-3"),
			chasmExec("sched-4"), chasmExec("sched-5"),
		},
		NextPageToken: nil,
	}, nil)

	// Exactly two DescribeSchedule calls; gomock fails the test on a third.
	d.frontendClient.EXPECT().DescribeSchedule(gomock.Any(), gomock.Any()).
		Return(describeResp(false, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, 0), nil).Times(2)

	params := dynamicconfig.DefaultScheduleInvariantsScannerParams
	params.OverdueNextActionTimeMaxChecksPerNamespace = 2
	err := d.newActivitiesWithParams(params).runOverdueScan(context.Background(), "q")
	require.NoError(t, err)
}

func TestRunScan_EmitsPerNamespaceCounts(t *testing.T) {
	d := newTestDeps(t)

	d.namespaceRegistry.EXPECT().GetAllNamespaces().Return([]*namespace.Namespace{
		localNS("id-1", "ns-1", testClusterName),
		localNS("id-2", "ns-2", testClusterName),
	})

	d.namespaceRegistry.EXPECT().GetNamespaceID(namespace.Name("ns-1")).Return(namespace.ID("id-1"), nil)
	d.namespaceRegistry.EXPECT().GetNamespaceID(namespace.Name("ns-2")).Return(namespace.ID("id-2"), nil)
	d.visibilityManager.EXPECT().CountChasmExecutions(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *visibilityservice.CountChasmExecutionsRequest) (*visibilityservice.CountChasmExecutionsResponse, error) {
			switch req.Namespace {
			case "ns-1":
				return &visibilityservice.CountChasmExecutionsResponse{Count: 3}, nil
			case "ns-2":
				return &visibilityservice.CountChasmExecutionsResponse{Count: 0}, nil
			}
			return &visibilityservice.CountChasmExecutionsResponse{Count: 0}, nil
		}).Times(2)

	err := d.newActivities().runScan(context.Background(), "stuck_open", "q", metrics.ScheduleInvariantsScannerStuckOpenCount.Name())
	require.NoError(t, err)
}

func TestEmitCount_IgnoresZeroAndNegative(t *testing.T) {
	d := newTestDeps(t)
	a := d.newActivities()
	// emitCount is a no-op for count <= 0; mainly we verify it doesn't panic.
	a.emitCount("metric", "ns", 0)
	a.emitCount("metric", "ns", -1)
	a.emitCount("metric", "ns", 5) // exercise positive path
}
