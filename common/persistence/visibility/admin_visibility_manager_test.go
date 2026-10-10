package visibility

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newTestAdminVisibilityManager(
	visStore store.VisibilityStore,
	nsRegistry namespace.Registry,
) *visibilityManagerImpl {
	return newVisibilityManagerImpl(
		visStore,
		log.NewNoopLogger(),
		searchattribute.NewTestMapperProvider(nil),
		nsRegistry,
		nil, // chasmRegistry
	)
}

// TestVisibilityManagerImpl_AdminAPIs_StoreNotAdmin covers a store that doesn't support the
// admin visibility APIs (e.g. the CHASM store).
func TestVisibilityManagerImpl_AdminAPIs_StoreNotAdmin(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	visManager := newTestAdminVisibilityManager(
		store.NewMockVisibilityStore(ctrl),
		namespace.NewMockRegistry(ctrl),
	)

	_, err := visManager.ListExecutions(context.Background(), &manager.AdminListExecutionsRequest{})
	require.ErrorIs(t, err, manager.ErrNotAdminVisibilityStore)

	_, err = visManager.CountExecutions(context.Background(), &manager.AdminCountExecutionsRequest{})
	require.ErrorIs(t, err, manager.ErrNotAdminVisibilityStore)
}

func TestVisibilityManagerImpl_ListExecutions(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	visStore := store.NewMockAdminVisibilityStore(ctrl)
	nsRegistry := namespace.NewMockRegistry(ctrl)
	visManager := newTestAdminVisibilityManager(visStore, nsRegistry)

	startTime := time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)
	closeTime := startTime.Add(time.Minute)
	deletedNamespaceID := namespace.ID("deleted-namespace-id")

	request := &manager.AdminListExecutionsRequest{
		Query:    "ExecutionStatus = 'Completed'",
		PageSize: 10,
	}
	visStore.EXPECT().ListExecutions(gomock.Any(), request).Return(
		&store.InternalListExecutionsResponse{
			Executions: []*store.InternalExecutionInfo{
				{
					NamespaceID: testNamespaceUUID.String(),
					WorkflowID:  "running-wid",
					RunID:       "running-rid",
					TypeName:    "test-workflow-type",
					Status:      enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
					StartTime:   startTime,
					// Closed-only fields are set here to assert they are dropped for
					// running executions.
					CloseTime:            closeTime,
					ExecutionDuration:    time.Minute,
					HistoryLength:        29,
					HistorySizeBytes:     1024,
					StateTransitionCount: 22,
				},
				{
					NamespaceID:          deletedNamespaceID.String(),
					WorkflowID:           "closed-wid",
					RunID:                "closed-rid",
					TypeName:             "test-workflow-type",
					Status:               enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
					StartTime:            startTime,
					CloseTime:            closeTime,
					ExecutionDuration:    time.Minute,
					HistoryLength:        29,
					HistorySizeBytes:     1024,
					StateTransitionCount: 22,
				},
			},
			NextPageToken: []byte("next-page-token"),
		},
		nil,
	)

	nsRegistry.EXPECT().GetNamespaceName(testNamespaceUUID).Return(testNamespace, nil)
	// A namespace that can't be resolved (e.g. it was deleted) doesn't fail the request;
	// the execution is returned with an empty namespace name.
	nsRegistry.EXPECT().
		GetNamespaceName(deletedNamespaceID).
		Return(namespace.EmptyName, serviceerror.NewNamespaceNotFound(deletedNamespaceID.String()))

	resp, err := visManager.ListExecutions(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, []byte("next-page-token"), resp.NextPageToken)
	protorequire.ProtoSliceEqual(
		t,
		[]*persistencespb.VisibilityExecutionInfo{
			{
				NamespaceId: testNamespaceUUID.String(),
				Namespace:   testNamespace.String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "running-wid",
					RunId:      "running-rid",
				},
				WorkflowType: &commonpb.WorkflowType{Name: "test-workflow-type"},
				Status:       enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				StartTime:    timestamppb.New(startTime),
			},
			{
				NamespaceId: deletedNamespaceID.String(),
				Namespace:   "",
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "closed-wid",
					RunId:      "closed-rid",
				},
				WorkflowType:         &commonpb.WorkflowType{Name: "test-workflow-type"},
				Status:               enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
				StartTime:            timestamppb.New(startTime),
				CloseTime:            timestamppb.New(closeTime),
				ExecutionDuration:    durationpb.New(time.Minute),
				HistoryLength:        29,
				HistorySizeBytes:     1024,
				StateTransitionCount: 22,
			},
		},
		resp.Executions,
	)
}

func TestVisibilityManagerImpl_ListExecutions_StoreError(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	visStore := store.NewMockAdminVisibilityStore(ctrl)
	visManager := newTestAdminVisibilityManager(visStore, namespace.NewMockRegistry(ctrl))

	storeErr := errors.New("store error")
	visStore.EXPECT().ListExecutions(gomock.Any(), gomock.Any()).Return(nil, storeErr)

	_, err := visManager.ListExecutions(
		context.Background(),
		&manager.AdminListExecutionsRequest{PageSize: 10},
	)
	require.ErrorIs(t, err, storeErr)
}

func TestVisibilityManagerImpl_CountExecutions(t *testing.T) {
	t.Parallel()

	completedPayload := payload.EncodeString(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED.String())
	runningPayload := payload.EncodeString(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String())

	testCases := []struct {
		name      string
		storeResp *store.InternalCountExecutionsResponse
		want      *manager.AdminCountExecutionsResponse
	}{
		{
			name:      "no group by",
			storeResp: &store.InternalCountExecutionsResponse{Count: 110},
			want:      &manager.AdminCountExecutionsResponse{Count: 110},
		},
		{
			name: "group by",
			storeResp: &store.InternalCountExecutionsResponse{
				Count: 110,
				Groups: []store.InternalAggregationGroup{
					{GroupValues: []*commonpb.Payload{completedPayload}, Count: 100},
					{GroupValues: []*commonpb.Payload{runningPayload}, Count: 10},
				},
			},
			want: &manager.AdminCountExecutionsResponse{
				Count: 110,
				Groups: []*adminservice.CountExecutionsResponse_AggregationGroup{
					{GroupValues: []*commonpb.Payload{completedPayload}, Count: 100},
					{GroupValues: []*commonpb.Payload{runningPayload}, Count: 10},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			visStore := store.NewMockAdminVisibilityStore(ctrl)
			visManager := newTestAdminVisibilityManager(visStore, namespace.NewMockRegistry(ctrl))

			request := &manager.AdminCountExecutionsRequest{Query: "GROUP BY ExecutionStatus"}
			visStore.EXPECT().CountExecutions(gomock.Any(), request).Return(tc.storeResp, nil)

			resp, err := visManager.CountExecutions(context.Background(), request)
			require.NoError(t, err)
			require.Equal(t, tc.want.Count, resp.Count)
			protorequire.ProtoSliceEqual(t, tc.want.Groups, resp.Groups)
		})
	}
}

func TestVisibilityManagerImpl_CountExecutions_StoreError(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	visStore := store.NewMockAdminVisibilityStore(ctrl)
	visManager := newTestAdminVisibilityManager(visStore, namespace.NewMockRegistry(ctrl))

	storeErr := errors.New("store error")
	visStore.EXPECT().CountExecutions(gomock.Any(), gomock.Any()).Return(nil, storeErr)

	_, err := visManager.CountExecutions(
		context.Background(),
		&manager.AdminCountExecutionsRequest{},
	)
	require.ErrorIs(t, err, storeErr)
}

func TestVisibilityManagerDual_AdminAPIs(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	primary := manager.NewMockVisibilityManager(ctrl)
	secondary := manager.NewMockVisibilityManager(ctrl)
	adminManager := manager.NewMockAdminVisibilityManager(ctrl)
	selector := NewMockmanagerSelector(ctrl)
	visManager := NewVisibilityManagerDual(
		primary,
		secondary,
		selector,
		dynamicconfig.GetBoolPropertyFn(false),
	)

	listRequest := &manager.AdminListExecutionsRequest{Namespace: testNamespace}
	listResponse := &manager.AdminListExecutionsResponse{NextPageToken: []byte("token")}
	selector.EXPECT().readManager(testNamespace).Return(adminManager)
	adminManager.EXPECT().ListExecutions(gomock.Any(), listRequest).Return(listResponse, nil)
	gotList, err := visManager.ListExecutions(context.Background(), listRequest)
	require.NoError(t, err)
	require.Equal(t, listResponse, gotList)

	countRequest := &manager.AdminCountExecutionsRequest{Namespace: testNamespace}
	countResponse := &manager.AdminCountExecutionsResponse{Count: 10}
	selector.EXPECT().readManager(testNamespace).Return(adminManager)
	adminManager.EXPECT().CountExecutions(gomock.Any(), countRequest).Return(countResponse, nil)
	gotCount, err := visManager.CountExecutions(context.Background(), countRequest)
	require.NoError(t, err)
	require.Equal(t, countResponse, gotCount)

	// The selected read manager doesn't support the admin visibility APIs.
	selector.EXPECT().readManager(testNamespace).Return(primary)
	_, err = visManager.ListExecutions(context.Background(), listRequest)
	require.ErrorIs(t, err, manager.ErrNotAdminVisibilityManager)

	selector.EXPECT().readManager(testNamespace).Return(primary)
	_, err = visManager.CountExecutions(context.Background(), countRequest)
	require.ErrorIs(t, err, manager.ErrNotAdminVisibilityManager)
}

func TestVisibilityManagerRateLimited_AdminAPIs(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	delegate := manager.NewMockAdminVisibilityManager(ctrl)
	visManager := NewVisibilityManagerRateLimited(
		delegate,
		dynamicconfig.GetIntPropertyFn(1),
		dynamicconfig.GetIntPropertyFn(1),
		dynamicconfig.GetFloatPropertyFn(0.2),
	)

	listRequest := &manager.AdminListExecutionsRequest{Namespace: testNamespace}
	listResponse := &manager.AdminListExecutionsResponse{NextPageToken: []byte("token")}
	delegate.EXPECT().ListExecutions(gomock.Any(), listRequest).Return(listResponse, nil)
	gotList, err := visManager.ListExecutions(context.Background(), listRequest)
	require.NoError(t, err)
	require.Equal(t, listResponse, gotList)

	// No remaining tokens: the read rate limiter rejects before reaching the delegate.
	_, err = visManager.ListExecutions(context.Background(), listRequest)
	require.ErrorIs(t, err, persistence.ErrPersistenceSystemLimitExceeded)

	countRequest := &manager.AdminCountExecutionsRequest{Namespace: testNamespace}
	_, err = visManager.CountExecutions(context.Background(), countRequest)
	require.ErrorIs(t, err, persistence.ErrPersistenceSystemLimitExceeded)
}

func TestVisibilityManagerRateLimited_AdminAPIs_DelegateNotAdmin(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	visManager := NewVisibilityManagerRateLimited(
		manager.NewMockVisibilityManager(ctrl),
		dynamicconfig.GetIntPropertyFn(1),
		dynamicconfig.GetIntPropertyFn(1),
		dynamicconfig.GetFloatPropertyFn(0.2),
	)

	_, err := visManager.ListExecutions(context.Background(), &manager.AdminListExecutionsRequest{})
	require.ErrorIs(t, err, manager.ErrNotAdminVisibilityManager)

	_, err = visManager.CountExecutions(context.Background(), &manager.AdminCountExecutionsRequest{})
	require.ErrorIs(t, err, manager.ErrNotAdminVisibilityManager)
}

func TestVisibilityManagerMetrics_AdminAPIs(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	delegate := manager.NewMockAdminVisibilityManager(ctrl)
	visManager := newTestVisibilityManagerMetrics(delegate)

	listRequest := &manager.AdminListExecutionsRequest{Namespace: testNamespace}
	listResponse := &manager.AdminListExecutionsResponse{NextPageToken: []byte("token")}
	delegate.EXPECT().ListExecutions(gomock.Any(), listRequest).Return(listResponse, nil)
	gotList, err := visManager.ListExecutions(context.Background(), listRequest)
	require.NoError(t, err)
	require.Equal(t, listResponse, gotList)

	countRequest := &manager.AdminCountExecutionsRequest{Namespace: testNamespace}
	countResponse := &manager.AdminCountExecutionsResponse{Count: 10}
	delegate.EXPECT().CountExecutions(gomock.Any(), countRequest).Return(countResponse, nil)
	gotCount, err := visManager.CountExecutions(context.Background(), countRequest)
	require.NoError(t, err)
	require.Equal(t, countResponse, gotCount)
}

func TestVisibilityManagerMetrics_AdminAPIs_DelegateNotAdmin(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	visManager := newTestVisibilityManagerMetrics(manager.NewMockVisibilityManager(ctrl))

	_, err := visManager.ListExecutions(context.Background(), &manager.AdminListExecutionsRequest{})
	require.ErrorIs(t, err, manager.ErrNotAdminVisibilityManager)

	_, err = visManager.CountExecutions(context.Background(), &manager.AdminCountExecutionsRequest{})
	require.ErrorIs(t, err, manager.ErrNotAdminVisibilityManager)
}

func newTestVisibilityManagerMetrics(
	delegate manager.VisibilityManager,
) *visibilityManagerMetrics {
	return NewVisibilityManagerMetrics(
		delegate,
		metrics.NoopMetricsHandler,
		log.NewNoopLogger(),
		dynamicconfig.GetDurationPropertyFn(time.Second),
		metrics.VisibilityPluginNameTag("test-plugin"),
		metrics.VisibilityIndexNameTag("test-index"),
	)
}
