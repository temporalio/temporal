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
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
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
			// The Generator advances FutureActionTimes while starts are buffered, so
			// an overdue time under BUFFER_* means it stalled.
			name: "buffer_one_with_running_workflow_still_overdue",
			resp: describeResp(false, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, 1, overdueActionTime()),
			want: false,
		},
		{
			name: "buffer_all_with_running_workflow_still_overdue",
			resp: describeResp(false, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, 2, overdueActionTime()),
			want: false,
		},
		{
			name: "buffer_one_with_running_workflow_pending",
			resp: describeResp(false, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, 1, pendingActionTime()),
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
			chasmExec("sched-buffer-stalled"),
			chasmExec("sched-actually-overdue"),
		},
		NextPageToken: nil,
	}, nil)

	d.frontendClient.EXPECT().DescribeSchedule(gomock.Any(), &workflowservice.DescribeScheduleRequest{
		Namespace: "ns-1", ScheduleId: "sched-paused",
	}).Return(describeResp(true, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, 0), nil)
	d.frontendClient.EXPECT().DescribeSchedule(gomock.Any(), &workflowservice.DescribeScheduleRequest{
		Namespace: "ns-1", ScheduleId: "sched-buffer-stalled",
	}).Return(describeResp(false, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, 1, overdueActionTime()), nil)
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
	require.Equal(t, int64(2), anomalies[0].Value, "sched-buffer-stalled and sched-actually-overdue should count")
	require.Empty(t, snapshot[metrics.ScheduleInvariantsScannerOverdueNextActionTimeStaleCandidateCount.Name()],
		"paused is an exemption, not a stale candidate")
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

var staleCloseTolerance = dynamicconfig.DefaultScheduleInvariantsScannerParams.StaleRunningWorkflowsCloseTimeTolerance

func wfExec(id string) *commonpb.WorkflowExecution {
	return &commonpb.WorkflowExecution{WorkflowId: id, RunId: id + "-run"}
}

// describeRunningResp builds a DescribeSchedule response listing the given running workflows.
func describeRunningResp(paused bool, running ...*commonpb.WorkflowExecution) *workflowservice.DescribeScheduleResponse {
	return &workflowservice.DescribeScheduleResponse{
		Schedule: &schedulepb.Schedule{
			State:    &schedulepb.ScheduleState{Paused: paused},
			Policies: &schedulepb.SchedulePolicies{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP},
		},
		Info: &schedulepb.ScheduleInfo{RunningWorkflows: running},
	}
}

func runningWFResp() *workflowservice.DescribeWorkflowExecutionResponse {
	return &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
	}
}

func closedWFResp(status enumspb.WorkflowExecutionStatus, closeTime time.Time) *workflowservice.DescribeWorkflowExecutionResponse {
	return &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Status: status, CloseTime: timestamppb.New(closeTime)},
	}
}

// expectDescribeWF registers a DescribeWorkflowExecution expectation for exactly wf.
func (d *testDeps) expectDescribeWF(wf *commonpb.WorkflowExecution, resp *workflowservice.DescribeWorkflowExecutionResponse, err error) {
	d.frontendClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: "ns-1",
		Execution: wf,
	}).Return(resp, err)
}

func (d *testDeps) expectDescribeSchedule(scheduleID string, resp *workflowservice.DescribeScheduleResponse, err error) {
	d.frontendClient.EXPECT().DescribeSchedule(gomock.Any(), &workflowservice.DescribeScheduleRequest{
		Namespace: "ns-1", ScheduleId: scheduleID,
	}).Return(resp, err)
}

// expectSingleNamespaceListing sets up ns-1 as the only namespace, listing scheduleIDs.
func (d *testDeps) expectSingleNamespaceListing(scheduleIDs ...string) {
	d.namespaceRegistry.EXPECT().GetAllNamespaces().Return([]*namespace.Namespace{localNS("id-1", "ns-1", testClusterName)})
	d.namespaceRegistry.EXPECT().GetNamespaceID(namespace.Name("ns-1")).Return(namespace.ID("id-1"), nil)
	var execs []*chasmspb.VisibilityExecutionInfo
	for _, id := range scheduleIDs {
		execs = append(execs, chasmExec(id))
	}
	d.visibilityManager.EXPECT().ListChasmExecutions(gomock.Any(), gomock.Any()).
		Return(&visibilityservice.ListChasmExecutionsResponse{Executions: execs}, nil)
}

func (d *testDeps) captureMetrics(t *testing.T) *metricstest.Capture {
	t.Helper()
	rec := metricstest.NewCaptureHandler()
	d.metricsHandler = rec
	capture := rec.StartCapture()
	t.Cleanup(func() { rec.StopCapture(capture) })
	return capture
}

func TestRunningWorkflowStaleReason(t *testing.T) {
	cases := []struct {
		name       string
		resp       *workflowservice.DescribeWorkflowExecutionResponse
		err        error
		wantReason string
		wantErr    bool
	}{
		{
			// Closed and purged after retention: the production failure mode.
			name:       "not_found",
			err:        serviceerror.NewNotFound("workflow not found"),
			wantReason: staleReasonNotFound,
		},
		{
			name: "running",
			resp: runningWFResp(),
		},
		{
			name:       "closed_past_grace_period",
			resp:       closedWFResp(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, testNow.Add(-staleCloseTolerance).Add(-time.Minute)),
			wantReason: staleReasonClosed,
		},
		{
			// The scheduler may not have processed the completion yet.
			name: "closed_within_grace_period",
			resp: closedWFResp(enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, testNow.Add(-time.Minute)),
		},
		{
			name: "closed_exactly_at_grace_threshold",
			resp: closedWFResp(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, testNow.Add(-staleCloseTolerance)),
		},
		{
			name: "closed_without_close_time",
			resp: &workflowservice.DescribeWorkflowExecutionResponse{
				WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED},
			},
			wantReason: staleReasonClosed,
		},
		{
			name:    "other_error",
			err:     serviceerror.NewUnavailable("frontend unavailable"),
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := newTestDeps(t)
			wf := wfExec("wf-1")
			d.expectDescribeWF(wf, tc.resp, tc.err)

			reason, _, err := d.newActivities().runningWorkflowStaleReason(context.Background(), "ns-1", wf)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.wantReason, reason)
		})
	}
}

func TestRunStaleRunningWorkflowsScan_CountsSchedulesWithStaleEntries(t *testing.T) {
	d := newTestDeps(t)
	d.expectSingleNamespaceListing(
		"sched-not-found",
		"sched-closed-long-ago",
		"sched-closed-recently",
		"sched-running",
		"sched-no-running",
		"sched-deleted",
		"sched-two-stale",
	)

	d.expectDescribeSchedule("sched-not-found", describeRunningResp(false, wfExec("wf-a")), nil)
	d.expectDescribeWF(wfExec("wf-a"), nil, serviceerror.NewNotFound("gone"))

	d.expectDescribeSchedule("sched-closed-long-ago", describeRunningResp(false, wfExec("wf-b")), nil)
	d.expectDescribeWF(wfExec("wf-b"), closedWFResp(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, testNow.Add(-24*time.Hour)), nil)

	d.expectDescribeSchedule("sched-closed-recently", describeRunningResp(false, wfExec("wf-c")), nil)
	d.expectDescribeWF(wfExec("wf-c"), closedWFResp(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, testNow.Add(-time.Minute)), nil)

	d.expectDescribeSchedule("sched-running", describeRunningResp(false, wfExec("wf-d")), nil)
	d.expectDescribeWF(wfExec("wf-d"), runningWFResp(), nil)

	// Listed by visibility but nothing running by the time of the describe.
	d.expectDescribeSchedule("sched-no-running", describeRunningResp(false), nil)

	// Deleted between the listing and the describe: neither an anomaly nor an error.
	d.expectDescribeSchedule("sched-deleted", nil, serviceerror.NewNotFound("schedule not found"))

	// Two stale entries on one schedule count as one anomalous schedule.
	d.expectDescribeSchedule("sched-two-stale", describeRunningResp(false, wfExec("wf-e"), wfExec("wf-f")), nil)
	d.expectDescribeWF(wfExec("wf-e"), nil, serviceerror.NewNotFound("gone"))
	d.expectDescribeWF(wfExec("wf-f"), nil, serviceerror.NewNotFound("gone"))

	capture := d.captureMetrics(t)
	require.NoError(t, d.newActivities().runStaleRunningWorkflowsScan(context.Background(), "q"))

	snapshot := capture.Snapshot()
	anomalies := snapshot[metrics.ScheduleInvariantsScannerStaleRunningWorkflowsCount.Name()]
	require.Len(t, anomalies, 1)
	require.Equal(t, int64(3), anomalies[0].Value,
		"sched-not-found, sched-closed-long-ago and sched-two-stale should count")
	require.Equal(t, "ns-1", anomalies[0].Tags["namespace"])
	require.Empty(t, snapshot[metrics.ScheduleInvariantsScannerErrorCount.Name()])
}

func TestRunStaleRunningWorkflowsScan_NoStaleEntriesEmitsNoAnomaly(t *testing.T) {
	d := newTestDeps(t)
	d.expectSingleNamespaceListing("sched-running", "sched-no-running")
	d.expectDescribeSchedule("sched-running", describeRunningResp(false, wfExec("wf-a")), nil)
	d.expectDescribeWF(wfExec("wf-a"), runningWFResp(), nil)
	d.expectDescribeSchedule("sched-no-running", describeRunningResp(false), nil)

	capture := d.captureMetrics(t)
	require.NoError(t, d.newActivities().runStaleRunningWorkflowsScan(context.Background(), "q"))

	snapshot := capture.Snapshot()
	require.Empty(t, snapshot[metrics.ScheduleInvariantsScannerStaleRunningWorkflowsCount.Name()])
	require.Empty(t, snapshot[metrics.ScheduleInvariantsScannerErrorCount.Name()])
}

// Pausing doesn't clear the running list, so a paused schedule is still flagged.
func TestRunStaleRunningWorkflowsScan_FlagsPausedSchedule(t *testing.T) {
	d := newTestDeps(t)
	d.expectSingleNamespaceListing("sched-paused")
	d.expectDescribeSchedule("sched-paused", describeRunningResp(true, wfExec("wf-a")), nil)
	d.expectDescribeWF(wfExec("wf-a"), nil, serviceerror.NewNotFound("gone"))

	capture := d.captureMetrics(t)
	require.NoError(t, d.newActivities().runStaleRunningWorkflowsScan(context.Background(), "q"))

	anomalies := capture.Snapshot()[metrics.ScheduleInvariantsScannerStaleRunningWorkflowsCount.Name()]
	require.Len(t, anomalies, 1)
	require.Equal(t, int64(1), anomalies[0].Value)
}

// Unlike the overdue scan, a failed describe is an error, never an anomaly.
func TestRunStaleRunningWorkflowsScan_DescribeErrorsAreRecordedNotCounted(t *testing.T) {
	d := newTestDeps(t)
	d.expectSingleNamespaceListing("sched-describe-fails", "sched-wf-describe-fails")
	d.expectDescribeSchedule("sched-describe-fails", nil, errors.New("describe schedule failed"))
	d.expectDescribeSchedule("sched-wf-describe-fails", describeRunningResp(false, wfExec("wf-a")), nil)
	d.expectDescribeWF(wfExec("wf-a"), nil, serviceerror.NewUnavailable("frontend unavailable"))

	capture := d.captureMetrics(t)
	require.NoError(t, d.newActivities().runStaleRunningWorkflowsScan(context.Background(), "q"))

	snapshot := capture.Snapshot()
	require.Empty(t, snapshot[metrics.ScheduleInvariantsScannerStaleRunningWorkflowsCount.Name()])
	scanErrors := snapshot[metrics.ScheduleInvariantsScannerErrorCount.Name()]
	require.Len(t, scanErrors, 2)
	for _, e := range scanErrors {
		require.Equal(t, "ns-1", e.Tags["namespace"])
		require.Equal(t, "stale_running_workflows", e.Tags["sub_scanner"])
	}
}

// A failure on one entry doesn't hide a stale sibling: both are reported.
func TestRunStaleRunningWorkflowsScan_StaleEntryCountsDespiteSiblingError(t *testing.T) {
	d := newTestDeps(t)
	d.expectSingleNamespaceListing("sched-1")
	d.expectDescribeSchedule("sched-1", describeRunningResp(false, wfExec("wf-a"), wfExec("wf-b")), nil)
	d.expectDescribeWF(wfExec("wf-a"), nil, serviceerror.NewUnavailable("frontend unavailable"))
	d.expectDescribeWF(wfExec("wf-b"), nil, serviceerror.NewNotFound("gone"))

	capture := d.captureMetrics(t)
	require.NoError(t, d.newActivities().runStaleRunningWorkflowsScan(context.Background(), "q"))

	snapshot := capture.Snapshot()
	anomalies := snapshot[metrics.ScheduleInvariantsScannerStaleRunningWorkflowsCount.Name()]
	require.Len(t, anomalies, 1)
	require.Equal(t, int64(1), anomalies[0].Value)
	require.Len(t, snapshot[metrics.ScheduleInvariantsScannerErrorCount.Name()], 1)
}

func TestRunStaleRunningWorkflowsScan_StopsAtPerNamespaceCap(t *testing.T) {
	d := newTestDeps(t)
	d.expectSingleNamespaceListing("sched-1", "sched-2", "sched-3", "sched-4", "sched-5")

	// Exactly two schedules are checked; gomock fails the test on a third describe.
	d.frontendClient.EXPECT().DescribeSchedule(gomock.Any(), gomock.Any()).
		Return(describeRunningResp(false, wfExec("wf-a")), nil).Times(2)
	d.frontendClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(nil, serviceerror.NewNotFound("gone")).Times(2)

	capture := d.captureMetrics(t)
	params := dynamicconfig.DefaultScheduleInvariantsScannerParams
	params.StaleRunningWorkflowsMaxChecksPerNamespace = 2
	require.NoError(t, d.newActivitiesWithParams(params).runStaleRunningWorkflowsScan(context.Background(), "q"))

	snapshot := capture.Snapshot()
	capHits := snapshot[metrics.ScheduleInvariantsScannerStaleRunningWorkflowsCapHitCount.Name()]
	require.Len(t, capHits, 1)
	require.Equal(t, "ns-1", capHits[0].Tags["namespace"])
	anomalies := snapshot[metrics.ScheduleInvariantsScannerStaleRunningWorkflowsCount.Name()]
	require.Len(t, anomalies, 1)
	require.Equal(t, int64(2), anomalies[0].Value, "anomalies found before the cap are still emitted")
}

func TestRunStaleRunningWorkflowsScan_BoundsChecksPerSchedule(t *testing.T) {
	d := newTestDeps(t)
	d.expectSingleNamespaceListing("sched-1")
	var running []*commonpb.WorkflowExecution
	for range maxRunningWorkflowChecksPerSchedule + 5 {
		running = append(running, wfExec("wf"))
	}
	d.expectDescribeSchedule("sched-1", describeRunningResp(false, running...), nil)
	d.frontendClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(runningWFResp(), nil).Times(maxRunningWorkflowChecksPerSchedule)

	require.NoError(t, d.newActivities().runStaleRunningWorkflowsScan(context.Background(), "q"))
}

func TestRunStaleRunningWorkflowsScan_ListErrorIsRecordedPerNamespace(t *testing.T) {
	d := newTestDeps(t)
	d.namespaceRegistry.EXPECT().GetAllNamespaces().Return([]*namespace.Namespace{localNS("id-1", "ns-1", testClusterName)})
	d.namespaceRegistry.EXPECT().GetNamespaceID(namespace.Name("ns-1")).Return(namespace.ID("id-1"), nil)
	d.visibilityManager.EXPECT().ListChasmExecutions(gomock.Any(), gomock.Any()).Return(nil, errors.New("list failed"))

	capture := d.captureMetrics(t)
	require.NoError(t, d.newActivities().runStaleRunningWorkflowsScan(context.Background(), "q"))
	require.Len(t, capture.Snapshot()[metrics.ScheduleInvariantsScannerErrorCount.Name()], 1)
}

// The activity queries schedules with at least one running workflow, paused or not.
func TestScanStaleRunningWorkflows_Query(t *testing.T) {
	d := newTestDeps(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d.namespaceRegistry.EXPECT().GetAllNamespaces().Return([]*namespace.Namespace{localNS("id-1", "ns-1", testClusterName)})
	d.namespaceRegistry.EXPECT().GetNamespaceID(namespace.Name("ns-1")).Return(namespace.ID("id-1"), nil)
	d.visibilityManager.EXPECT().ListChasmExecutions(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *visibilityservice.ListChasmExecutionsRequest) (*visibilityservice.ListChasmExecutionsResponse, error) {
			require.Equal(t, chasm.SchedulerArchetypeID, req.ArchetypeId)
			require.Equal(t, `ScheduleRunningWorkflowCount > 0 AND ExecutionStatus = "Running"`, req.Query)
			// End the activity after its first pass.
			cancel()
			return &visibilityservice.ListChasmExecutionsResponse{}, nil
		})

	err := d.newActivities().ScanStaleRunningWorkflows(ctx)
	require.ErrorIs(t, err, context.Canceled)
}
