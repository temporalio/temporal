package restorenamespace

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

const (
	testNamespaceID   = "ns-id"
	testRestoreID     = "restore-1"
	testActive        = "active"
	testStandby       = "standby"
	testConfigVersion = int64(10)
)

var testInput = RestoreJobInput{NamespaceID: testNamespaceID, RestoreID: testRestoreID, RestoreVersion: testConfigVersion}

type activityTestEnv struct {
	activities *Activities
	registry   *namespace.MockRegistry
	visibility *manager.MockVisibilityManager
	clients    map[string]*adminservicemock.MockAdminServiceClient
	env        *testsuite.TestActivityEnvironment
}

func newActivityTestEnv(t *testing.T, currentCluster string) *activityTestEnv {
	ctrl := gomock.NewController(t)
	te := &activityTestEnv{
		registry:   namespace.NewMockRegistry(ctrl),
		visibility: manager.NewMockVisibilityManager(ctrl),
		clients: map[string]*adminservicemock.MockAdminServiceClient{
			testActive:  adminservicemock.NewMockAdminServiceClient(ctrl),
			testStandby: adminservicemock.NewMockAdminServiceClient(ctrl),
		},
	}
	bean := client.NewMockBean(ctrl)
	bean.EXPECT().GetRemoteAdminClient(gomock.Any()).DoAndReturn(func(cluster string) (adminservice.AdminServiceClient, error) {
		return te.clients[cluster], nil
	}).AnyTimes()
	te.activities = NewActivities(
		te.visibility,
		te.registry,
		bean,
		currentCluster,
		dynamicconfig.GetIntPropertyFnFilteredByNamespace(1000),
		dynamicconfig.GetIntPropertyFn(4),
		metrics.NoopMetricsHandler,
		log.NewNoopLogger(),
		log.NewNoopLogger(),
	)
	te.activities.minRetryInterval = time.Millisecond
	te.activities.maxRetryInterval = time.Millisecond
	te.activities.heartbeatInterval = 10 * time.Millisecond
	te.activities.registryWait = 0

	var suite testsuite.WorkflowTestSuite
	te.env = suite.NewTestActivityEnvironment()
	te.env.RegisterActivityWithOptions(te.activities.RefreshExecutions, activity.RegisterOptions{Name: RefreshExecutionsActivityName})
	te.env.RegisterActivityWithOptions(te.activities.CompleteRestore, activity.RegisterOptions{Name: CompleteRestoreActivityName})
	return te
}

// serveNamespace makes the registry return whatever ns holds when it is read.
func (te *activityTestEnv) serveNamespace(ns *atomic.Pointer[namespace.Namespace]) {
	te.registry.EXPECT().GetNamespaceByID(namespace.ID(testNamespaceID)).DoAndReturn(
		func(namespace.ID) (*namespace.Namespace, error) { return ns.Load(), nil }).AnyTimes()
}

func testNamespace(t *testing.T, configVersion int64, restore *persistencespb.NamespaceRestoreInfo, clusters ...string) *namespace.Namespace {
	if len(clusters) == 0 {
		// Standby first, so the test sees the job refresh the active cluster first anyway.
		clusters = []string{testStandby, testActive}
	}
	detail := &persistencespb.NamespaceDetail{
		Info:   &persistencespb.NamespaceInfo{Id: testNamespaceID, Name: "ns", State: enumspb.NAMESPACE_STATE_REGISTERED, RestoreInfo: restore},
		Config: &persistencespb.NamespaceConfig{},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: testActive,
			Clusters:          clusters,
		},
		ConfigVersion:   configVersion,
		FailoverVersion: 1,
	}
	ns, err := namespace.FromPersistentState(detail, namespace.NewDefaultReplicationResolverFactory()(detail), namespace.WithGlobalFlag(true))
	require.NoError(t, err)
	return ns
}

func runningRestore() *persistencespb.NamespaceRestoreInfo {
	return &persistencespb.NamespaceRestoreInfo{RestoreId: testRestoreID, HostCluster: testActive, State: enumspb.NAMESPACE_RESTORE_STATE_RUNNING}
}

func restoreIn(state enumspb.NamespaceRestoreState) *persistencespb.NamespaceRestoreInfo {
	restore := runningRestore()
	restore.State = state
	return restore
}

func executionInfo(workflowID string) *workflowpb.WorkflowExecutionInfo {
	return &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: workflowID + "-run"}}
}

func listOnePage(te *activityTestEnv, workflowIDs ...string) {
	executions := make([]*workflowpb.WorkflowExecutionInfo, len(workflowIDs))
	for i, workflowID := range workflowIDs {
		executions[i] = executionInfo(workflowID)
	}
	te.visibility.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).Return(
		&manager.CountWorkflowExecutionsResponse{Count: int64(len(workflowIDs))}, nil)
	te.visibility.EXPECT().ListWorkflowExecutions(gomock.Any(), gomock.Any()).Return(
		&manager.ListWorkflowExecutionsResponse{Executions: executions}, nil)
}

// refreshLog records, per workflow ID, the clusters refreshed in order.
type refreshLog struct {
	sync.Mutex
	calls map[string][]string
}

func (l *refreshLog) get(workflowID string) []string {
	l.Lock()
	defer l.Unlock()
	return l.calls[workflowID]
}

func (l *refreshLog) expect(t *testing.T, te *activityTestEnv, cluster string, result func(workflowID string, attempt int) error) {
	te.clients[cluster].EXPECT().RefreshWorkflowTasks(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *adminservice.RefreshWorkflowTasksRequest, _ ...grpc.CallOption) (*adminservice.RefreshWorkflowTasksResponse, error) {
			require.Equal(t, testNamespaceID, request.GetNamespaceId())
			require.Equal(t, testConfigVersion, request.GetMinNamespaceConfigVersion())
			require.Equal(t, chasm.WorkflowArchetypeID, request.GetArchetypeId())
			workflowID := request.GetExecution().GetWorkflowId()
			l.Lock()
			if l.calls == nil {
				l.calls = map[string][]string{}
			}
			l.calls[workflowID] = append(l.calls[workflowID], cluster)
			attempt := 0
			for _, c := range l.calls[workflowID] {
				if c == cluster {
					attempt++
				}
			}
			l.Unlock()
			return &adminservice.RefreshWorkflowTasksResponse{}, result(workflowID, attempt)
		}).AnyTimes()
}

func TestRefreshExecutions(t *testing.T) {
	t.Parallel()
	te := newActivityTestEnv(t, testActive)
	te.registry.EXPECT().GetNamespaceByID(namespace.ID(testNamespaceID)).Return(testNamespace(t, testConfigVersion, runningRestore()), nil).AnyTimes()
	te.visibility.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).Return(&manager.CountWorkflowExecutionsResponse{Count: 5}, nil)
	gomock.InOrder(
		te.visibility.EXPECT().ListWorkflowExecutions(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, request *manager.ListWorkflowExecutionsRequestV2) (*manager.ListWorkflowExecutionsResponse, error) {
				require.Empty(t, request.NextPageToken)
				return &manager.ListWorkflowExecutionsResponse{
					Executions:    []*workflowpb.WorkflowExecutionInfo{executionInfo("refreshed"), executionInfo("gone-on-standby")},
					NextPageToken: []byte("page-2"),
				}, nil
			}),
		te.visibility.EXPECT().ListWorkflowExecutions(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, request *manager.ListWorkflowExecutionsRequestV2) (*manager.ListWorkflowExecutionsResponse, error) {
				require.Equal(t, []byte("page-2"), request.NextPageToken)
				return &manager.ListWorkflowExecutionsResponse{
					Executions: []*workflowpb.WorkflowExecutionInfo{executionInfo("data-loss"), executionInfo("rejected"), executionInfo("flaky-standby")},
				}, nil
			}),
	)

	// The rate is read for the restored namespace, so a restore can be slowed on its own.
	var rateNamespaces sync.Map
	te.activities.refreshRPS = func(namespaceName string) int {
		rateNamespaces.Store(namespaceName, true)
		return 1000
	}

	var calls refreshLog
	calls.expect(t, te, testActive, func(string, int) error { return nil })
	calls.expect(t, te, testStandby, func(workflowID string, attempt int) error {
		switch workflowID {
		case "gone-on-standby":
			return serviceerror.NewNotFound("gone")
		case "data-loss":
			return serviceerror.NewDataLoss("corrupt")
		case "rejected":
			return serviceerror.NewInvalidArgument("unknown archetype")
		case "flaky-standby":
			if attempt <= 3 {
				return serviceerror.NewUnavailable("registry behind")
			}
		default:
		}
		return nil
	})

	result, err := te.env.ExecuteActivity(RefreshExecutionsActivityName, testInput)
	require.NoError(t, err)
	var stats RefreshStats
	require.NoError(t, result.Get(&stats))
	require.Equal(t, int64(5), stats.TotalEstimate)
	require.Equal(t, int64(5), stats.ProcessedCount)
	require.Equal(t, int64(2), stats.SkippedCount)
	require.False(t, stats.RefreshStartTime.IsZero())
	var namespaceNames []any
	rateNamespaces.Range(func(key, _ any) bool {
		namespaceNames = append(namespaceNames, key)
		return true
	})
	require.Equal(t, []any{"ns"}, namespaceNames)

	require.Equal(t, []string{testActive, testStandby}, calls.get("refreshed"))
	require.Equal(t, []string{testActive, testStandby}, calls.get("gone-on-standby"))
	require.Equal(t, []string{testActive, testStandby}, calls.get("data-loss"), "DataLoss isn't retried")
	require.Equal(t, []string{testActive, testStandby}, calls.get("rejected"), "InvalidArgument isn't retried")
	require.Equal(t, []string{testActive, testStandby, testStandby, testStandby, testStandby}, calls.get("flaky-standby"),
		"retries the standby until it succeeds, without refreshing the active cluster again")
}

func TestRefreshExecutions_RemovingClusterUnblocks(t *testing.T) {
	t.Parallel()
	te := newActivityTestEnv(t, testActive)
	var ns atomic.Pointer[namespace.Namespace]
	ns.Store(testNamespace(t, testConfigVersion, runningRestore()))
	te.serveNamespace(&ns)
	listOnePage(te, "wf")

	var calls refreshLog
	calls.expect(t, te, testActive, func(string, int) error { return nil })
	calls.expect(t, te, testStandby, func(_ string, attempt int) error {
		if attempt == 3 {
			// An operator removes the unreachable standby from the namespace.
			ns.Store(testNamespace(t, testConfigVersion+1, runningRestore(), testActive))
		}
		return serviceerror.NewUnavailable("down")
	})

	result, err := te.env.ExecuteActivity(RefreshExecutionsActivityName, testInput)
	require.NoError(t, err)
	var stats RefreshStats
	require.NoError(t, result.Get(&stats))
	require.Equal(t, int64(1), stats.ProcessedCount)
	require.Zero(t, stats.SkippedCount)
	require.Equal(t, []string{testActive, testStandby, testStandby, testStandby}, calls.get("wf"))
}

func TestRefreshExecutions_StopsWhileRetrying(t *testing.T) {
	t.Parallel()
	te := newActivityTestEnv(t, testActive)
	var ns atomic.Pointer[namespace.Namespace]
	ns.Store(testNamespace(t, testConfigVersion, runningRestore()))
	te.serveNamespace(&ns)
	listOnePage(te, "wf")

	var calls refreshLog
	calls.expect(t, te, testActive, func(string, int) error { return nil })
	calls.expect(t, te, testStandby, func(_ string, attempt int) error {
		if attempt == 3 {
			// A soft delete abandons the restore while the standby is down.
			ns.Store(testNamespace(t, testConfigVersion+1, restoreIn(enumspb.NAMESPACE_RESTORE_STATE_FAILED)))
		}
		return serviceerror.NewUnavailable("down")
	})

	_, err := te.env.ExecuteActivity(RefreshExecutionsActivityName, testInput)
	var appErr *temporal.ApplicationError
	require.ErrorAs(t, err, &appErr)
	require.True(t, appErr.NonRetryable(), err.Error())
}

func TestRefreshExecutions_Stops(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name           string
		currentCluster string
		ns             func(t *testing.T) *namespace.Namespace
		expectClose    bool
		wantRetryable  bool
	}{
		{
			name:           "registry behind the restore",
			currentCluster: testActive,
			ns:             func(t *testing.T) *namespace.Namespace { return testNamespace(t, testConfigVersion-1, nil) },
			wantRetryable:  true,
		},
		{
			name:           "superseded",
			currentCluster: testActive,
			ns: func(t *testing.T) *namespace.Namespace {
				restore := runningRestore()
				restore.RestoreId = "restore-2"
				return testNamespace(t, testConfigVersion+1, restore)
			},
		},
		{
			name:           "abandoned by soft delete",
			currentCluster: testActive,
			ns: func(t *testing.T) *namespace.Namespace {
				return testNamespace(t, testConfigVersion+1, restoreIn(enumspb.NAMESPACE_RESTORE_STATE_FAILED))
			},
		},
		{
			name:           "failed over",
			currentCluster: testStandby,
			ns:             func(t *testing.T) *namespace.Namespace { return testNamespace(t, testConfigVersion, runningRestore()) },
			expectClose:    true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			te := newActivityTestEnv(t, tc.currentCluster)
			te.registry.EXPECT().GetNamespaceByID(namespace.ID(testNamespaceID)).Return(tc.ns(t), nil)
			if tc.expectClose {
				// Written on the cluster the namespace failed over to.
				te.clients[testActive].EXPECT().CloseNamespaceRestore(gomock.Any(), &adminservice.CloseNamespaceRestoreRequest{
					NamespaceId: testNamespaceID,
					RestoreId:   testRestoreID,
					State:       enumspb.NAMESPACE_RESTORE_STATE_FAILED,
				}).Return(&adminservice.CloseNamespaceRestoreResponse{}, nil)
			}

			_, err := te.env.ExecuteActivity(RefreshExecutionsActivityName, testInput)
			require.Error(t, err)
			var appErr *temporal.ApplicationError
			require.ErrorAs(t, err, &appErr)
			require.Equal(t, !tc.wantRetryable, appErr.NonRetryable(), err.Error())
		})
	}
}

func TestCompleteRestore(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name           string
		currentCluster string
		restore        *persistencespb.NamespaceRestoreInfo
		closeCluster   string
		closeState     enumspb.NamespaceRestoreState
		closeErr       error
		wantErr        bool
		wantRetryable  bool
	}{
		{
			name:           "completed",
			currentCluster: testActive,
			restore:        runningRestore(),
			closeCluster:   testActive,
			closeState:     enumspb.NAMESPACE_RESTORE_STATE_COMPLETED,
		},
		{
			name:           "retried after completing",
			currentCluster: testActive,
			restore:        restoreIn(enumspb.NAMESPACE_RESTORE_STATE_COMPLETED),
		},
		{
			name:           "abandoned",
			currentCluster: testActive,
			restore:        restoreIn(enumspb.NAMESPACE_RESTORE_STATE_FAILED),
			wantErr:        true,
		},
		{
			name:           "failed over",
			currentCluster: testStandby,
			restore:        runningRestore(),
			closeCluster:   testActive,
			closeState:     enumspb.NAMESPACE_RESTORE_STATE_FAILED,
			wantErr:        true,
		},
		{
			name:           "superseded while closing",
			currentCluster: testActive,
			restore:        runningRestore(),
			closeCluster:   testActive,
			closeState:     enumspb.NAMESPACE_RESTORE_STATE_COMPLETED,
			closeErr:       serviceerror.NewFailedPrecondition("superseded"),
			wantErr:        true,
		},
		{
			name:           "unavailable",
			currentCluster: testActive,
			restore:        runningRestore(),
			closeCluster:   testActive,
			closeState:     enumspb.NAMESPACE_RESTORE_STATE_COMPLETED,
			closeErr:       serviceerror.NewUnavailable("down"),
			wantErr:        true,
			wantRetryable:  true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			te := newActivityTestEnv(t, tc.currentCluster)
			te.registry.EXPECT().GetNamespaceByID(namespace.ID(testNamespaceID)).Return(testNamespace(t, testConfigVersion, tc.restore), nil).AnyTimes()
			if tc.closeCluster != "" {
				te.clients[tc.closeCluster].EXPECT().CloseNamespaceRestore(gomock.Any(), &adminservice.CloseNamespaceRestoreRequest{
					NamespaceId: testNamespaceID,
					RestoreId:   testRestoreID,
					State:       tc.closeState,
				}).Return(&adminservice.CloseNamespaceRestoreResponse{}, tc.closeErr)
			}

			_, err := te.env.ExecuteActivity(CompleteRestoreActivityName, testInput)
			if !tc.wantErr {
				require.NoError(t, err)
				return
			}
			var appErr *temporal.ApplicationError
			require.ErrorAs(t, err, &appErr, err.Error())
			require.Equal(t, !tc.wantRetryable, appErr.NonRetryable())
		})
	}
}

func TestNextCluster(t *testing.T) {
	t.Parallel()
	te := newActivityTestEnv(t, testActive)
	te.registry.EXPECT().GetNamespaceByID(namespace.ID(testNamespaceID)).Return(testNamespace(t, testConfigVersion, nil), nil).AnyTimes()
	next := func(done ...string) string {
		cluster, err := te.activities.nextCluster(testNamespaceID, "wf", done)
		require.NoError(t, err)
		return cluster
	}
	require.Equal(t, testActive, next())
	require.Equal(t, testStandby, next(testActive))
	require.Equal(t, testActive, next(testStandby))
	require.Empty(t, next(testActive, testStandby))
}

func TestRefreshExecutions_WaitsForRegistry(t *testing.T) {
	t.Parallel()
	te := newActivityTestEnv(t, testActive)
	te.activities.registryWait = time.Minute
	gomock.InOrder(
		te.registry.EXPECT().GetNamespaceByID(namespace.ID(testNamespaceID)).Return(testNamespace(t, testConfigVersion-1, nil), nil),
		te.registry.EXPECT().GetNamespaceByID(namespace.ID(testNamespaceID)).Return(testNamespace(t, testConfigVersion, runningRestore()), nil).AnyTimes(),
	)
	te.visibility.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).Return(&manager.CountWorkflowExecutionsResponse{}, nil)
	te.visibility.EXPECT().ListWorkflowExecutions(gomock.Any(), gomock.Any()).Return(&manager.ListWorkflowExecutionsResponse{}, nil)

	_, err := te.env.ExecuteActivity(RefreshExecutionsActivityName, testInput)
	require.NoError(t, err)
}
