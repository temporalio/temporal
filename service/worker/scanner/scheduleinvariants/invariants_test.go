package scheduleinvariants

import (
	"context"
	"errors"
	"testing"

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
)

const testClusterName = "test-cluster"

type testDeps struct {
	ctrl              *gomock.Controller
	visibilityManager *manager.MockVisibilityManager
	namespaceRegistry *namespace.MockRegistry
	sdkClientFactory  *sdk.MockClientFactory
	sdkClient         *mocksdk.MockClient
	frontendClient    *workflowservicemock.MockWorkflowServiceClient
	timeSource        *clock.EventTimeSource
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
	}
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
		metricsHandler:     metrics.NoopMetricsHandler,
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
		&persistencespb.NamespaceInfo{Id: id, Name: name, State: enumspb.NAMESPACE_STATE_REGISTERED},
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

// deprecatedNS builds a local namespace in the DEPRECATED state, which ListAllNamespaces skips.
func deprecatedNS(id, name, activeCluster string) *namespace.Namespace {
	return namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: id, Name: name, State: enumspb.NAMESPACE_STATE_DEPRECATED},
		nil,
		activeCluster,
	)
}

// globalNS builds a global (replicated) namespace whose active cluster is
// activeCluster. Only global namespaces return false from ActiveInCluster when the
// active cluster doesn't match; local namespaces are always "active" in every cluster.
func globalNS(id, name, activeCluster string) *namespace.Namespace {
	return namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: id, Name: name, State: enumspb.NAMESPACE_STATE_REGISTERED},
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
		globalNS("id-2", "ns-2", "other-cluster"),     // inactive in this cluster
		globalNS("id-3", "ns-3", testClusterName),     // active here
		deletedNS("id-4", "ns-4", testClusterName),    // deleted
		deprecatedNS("id-5", "ns-5", testClusterName), // deprecated
	})

	names := d.newActivities().ListAllNamespaces()
	require.ElementsMatch(t, []string{"ns-1", "ns-2", "ns-3"}, names)
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

func describeResp(paused bool, overlap enumspb.ScheduleOverlapPolicy, runningCount int) *workflowservice.DescribeScheduleResponse {
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
			resp: describeResp(false, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE, 0),
			want: false,
		},
		{
			name: "skip_policy_with_running_workflow",
			resp: describeResp(false, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, 1),
			want: false,
		},
		{
			name: "cancel_other_policy",
			resp: describeResp(false, enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER, 1),
			want: false,
		},
		{
			name: "describe_error",
			err:  errors.New("describe failed"),
			want: false,
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
	}).Return(describeResp(false, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, 0), nil)

	err := d.newActivities().runOverdueScan(context.Background(), "q")
	require.NoError(t, err)
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

func TestEmitCount_AlwaysEmitsIncludingZero(t *testing.T) {
	d := newTestDeps(t)
	a := d.newActivities()
	capture := metricstest.NewCaptureHandler()
	a.metricsHandler = capture
	c := capture.StartCapture()
	defer capture.StopCapture(c)

	// A healthy namespace (count 0) must still produce a series, so dashboards see a
	// zero baseline rather than a gap.
	a.emitCount("test_metric", "ns-healthy", 0)
	a.emitCount("test_metric", "ns-anomalous", 5)

	recs := c.Snapshot()["test_metric"]
	require.Len(t, recs, 2)
	require.Equal(t, int64(0), recs[0].Value)
	require.Equal(t, "ns-healthy", recs[0].Tags["namespace"])
	require.Equal(t, int64(5), recs[1].Value)
	require.Equal(t, "ns-anomalous", recs[1].Tags["namespace"])
}

// TestRunScan_ClassifiesInternalVsScanErrors verifies a namespace-registry lookup failure
// is counted under the internal-error metric while a visibility-query failure is counted
// under the scan-error metric.
func TestRunScan_ClassifiesInternalVsScanErrors(t *testing.T) {
	d := newTestDeps(t)

	d.namespaceRegistry.EXPECT().GetAllNamespaces().Return([]*namespace.Namespace{
		localNS("id-lookup", "ns-lookup", testClusterName),
		localNS("id-vis", "ns-vis", testClusterName),
	})

	// ns-lookup: the namespace-ID lookup itself fails -> internal error.
	d.namespaceRegistry.EXPECT().GetNamespaceID(namespace.Name("ns-lookup")).Return(namespace.ID(""), errors.New("registry down"))
	// ns-vis: lookup succeeds but the visibility query fails -> scan error.
	d.namespaceRegistry.EXPECT().GetNamespaceID(namespace.Name("ns-vis")).Return(namespace.ID("id-vis"), nil)
	d.visibilityManager.EXPECT().CountChasmExecutions(gomock.Any(), gomock.Any()).Return(nil, errors.New("es down"))

	a := d.newActivities()
	capture := metricstest.NewCaptureHandler()
	a.metricsHandler = capture
	c := capture.StartCapture()
	defer capture.StopCapture(c)

	err := a.runScan(context.Background(), "stuck_open", "q", metrics.ScheduleInvariantsScannerStuckOpenCount.Name())
	require.NoError(t, err)

	snap := c.Snapshot()
	internal := snap[metrics.ScheduleInvariantsScannerInternalErrorCount.Name()]
	require.Len(t, internal, 1)
	require.Equal(t, "ns-lookup", internal[0].Tags["namespace"])
	require.Equal(t, "stuck_open", internal[0].Tags["sub_scanner"])

	scan := snap[metrics.ScheduleInvariantsScannerErrorCount.Name()]
	require.Len(t, scan, 1)
	require.Equal(t, "ns-vis", scan[0].Tags["namespace"])
}
