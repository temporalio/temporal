package visibility

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
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

	_, err := visManager.AdminListExecutions(context.Background(), &manager.AdminListExecutionsRequest{})
	require.ErrorIs(t, err, manager.ErrNotAdminVisibilityStore)

	_, err = visManager.AdminCountExecutions(context.Background(), &manager.AdminCountExecutionsRequest{})
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

	// A CHASM execution carries its archetype ID in the namespace division.
	chasmArchetypeID := chasm.ArchetypeID(12345)
	chasmSearchAttributes := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			sadefs.TemporalNamespaceDivision: payload.EncodeString(
				strconv.Itoa(int(chasmArchetypeID)),
			),
		},
	}
	// A namespace division that isn't an archetype ID belongs to an ordinary workflow
	// that set one, e.g. the scheduler.
	userDivisionSearchAttributes := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			sadefs.TemporalNamespaceDivision: payload.EncodeString("user-division"),
		},
	}

	request := &manager.AdminListExecutionsRequest{
		Query:    "ExecutionStatus = 'Completed'",
		PageSize: 10,
	}
	visStore.EXPECT().AdminListExecutions(gomock.Any(), request).Return(
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
				{
					NamespaceID:      testNamespaceUUID.String(),
					WorkflowID:       "chasm-bid",
					RunID:            "chasm-rid",
					TypeName:         "test-workflow-type",
					Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
					StartTime:        startTime,
					SearchAttributes: chasmSearchAttributes,
				},
				{
					NamespaceID:      testNamespaceUUID.String(),
					WorkflowID:       "user-division-wid",
					RunID:            "user-division-rid",
					TypeName:         "test-workflow-type",
					Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
					StartTime:        startTime,
					SearchAttributes: userDivisionSearchAttributes,
				},
			},
			NextPageToken: []byte("next-page-token"),
		},
		nil,
	)

	nsRegistry.EXPECT().GetNamespaceName(testNamespaceUUID).Return(testNamespace, nil).Times(3)
	// A namespace that can't be resolved (e.g. it was deleted) doesn't fail the request;
	// the execution is returned with an empty namespace name.
	nsRegistry.EXPECT().
		GetNamespaceName(deletedNamespaceID).
		Return(namespace.EmptyName, serviceerror.NewNamespaceNotFound(deletedNamespaceID.String()))

	resp, err := visManager.AdminListExecutions(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, []byte("next-page-token"), resp.NextPageToken)
	protorequire.ProtoSliceEqual(
		t,
		[]*adminservice.VisibilityExecutionInfo{
			{
				// No namespace division: an ordinary workflow.
				NamespaceId: testNamespaceUUID.String(),
				Namespace:   testNamespace.String(),
				ArchetypeId: chasm.WorkflowArchetypeID,
				BusinessId:  "running-wid",
				RunId:       "running-rid",
				State:       enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
				StartTime:   timestamppb.New(startTime),
			},
			{
				NamespaceId:          deletedNamespaceID.String(),
				Namespace:            "",
				ArchetypeId:          chasm.WorkflowArchetypeID,
				BusinessId:           "closed-wid",
				RunId:                "closed-rid",
				State:                enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
				StartTime:            timestamppb.New(startTime),
				CloseTime:            timestamppb.New(closeTime),
				ExecutionDuration:    durationpb.New(time.Minute),
				HistoryLength:        29,
				HistorySizeBytes:     1024,
				StateTransitionCount: 22,
			},
			{
				NamespaceId: testNamespaceUUID.String(),
				Namespace:   testNamespace.String(),
				ArchetypeId: chasmArchetypeID,
				BusinessId:  "chasm-bid",
				RunId:       "chasm-rid",
				State:       enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
				StartTime:   timestamppb.New(startTime),
			},
			{
				// The namespace division doesn't parse as an archetype ID, so the
				// execution is reported as a workflow, the same as the no-division
				// workflows above.
				NamespaceId: testNamespaceUUID.String(),
				Namespace:   testNamespace.String(),
				ArchetypeId: chasm.WorkflowArchetypeID,
				BusinessId:  "user-division-wid",
				RunId:       "user-division-rid",
				State:       enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
				StartTime:   timestamppb.New(startTime),
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
	visStore.EXPECT().AdminListExecutions(gomock.Any(), gomock.Any()).Return(nil, storeErr)

	_, err := visManager.AdminListExecutions(
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
			visStore.EXPECT().AdminCountExecutions(gomock.Any(), request).Return(tc.storeResp, nil)

			resp, err := visManager.AdminCountExecutions(context.Background(), request)
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
	visStore.EXPECT().AdminCountExecutions(gomock.Any(), gomock.Any()).Return(nil, storeErr)

	_, err := visManager.AdminCountExecutions(
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
	adminManager.EXPECT().AdminListExecutions(gomock.Any(), listRequest).Return(listResponse, nil)
	gotList, err := visManager.AdminListExecutions(context.Background(), listRequest)
	require.NoError(t, err)
	require.Equal(t, listResponse, gotList)

	countRequest := &manager.AdminCountExecutionsRequest{Namespace: testNamespace}
	countResponse := &manager.AdminCountExecutionsResponse{Count: 10}
	selector.EXPECT().readManager(testNamespace).Return(adminManager)
	adminManager.EXPECT().AdminCountExecutions(gomock.Any(), countRequest).Return(countResponse, nil)
	gotCount, err := visManager.AdminCountExecutions(context.Background(), countRequest)
	require.NoError(t, err)
	require.Equal(t, countResponse, gotCount)

	// The selected read manager doesn't support the admin visibility APIs.
	selector.EXPECT().readManager(testNamespace).Return(primary)
	_, err = visManager.AdminListExecutions(context.Background(), listRequest)
	require.ErrorIs(t, err, manager.ErrNotAdminVisibilityManager)

	selector.EXPECT().readManager(testNamespace).Return(primary)
	_, err = visManager.AdminCountExecutions(context.Background(), countRequest)
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
	delegate.EXPECT().AdminListExecutions(gomock.Any(), listRequest).Return(listResponse, nil)
	gotList, err := visManager.AdminListExecutions(context.Background(), listRequest)
	require.NoError(t, err)
	require.Equal(t, listResponse, gotList)

	// No remaining tokens: the read rate limiter rejects before reaching the delegate.
	_, err = visManager.AdminListExecutions(context.Background(), listRequest)
	require.ErrorIs(t, err, persistence.ErrPersistenceSystemLimitExceeded)

	countRequest := &manager.AdminCountExecutionsRequest{Namespace: testNamespace}
	_, err = visManager.AdminCountExecutions(context.Background(), countRequest)
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

	_, err := visManager.AdminListExecutions(context.Background(), &manager.AdminListExecutionsRequest{})
	require.ErrorIs(t, err, manager.ErrNotAdminVisibilityManager)

	_, err = visManager.AdminCountExecutions(context.Background(), &manager.AdminCountExecutionsRequest{})
	require.ErrorIs(t, err, manager.ErrNotAdminVisibilityManager)
}

func TestVisibilityManagerMetrics_AdminAPIs(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	delegate := manager.NewMockAdminVisibilityManager(ctrl)
	visManager := newTestVisibilityManagerMetrics(delegate)

	listRequest := &manager.AdminListExecutionsRequest{Namespace: testNamespace}
	listResponse := &manager.AdminListExecutionsResponse{NextPageToken: []byte("token")}
	delegate.EXPECT().AdminListExecutions(gomock.Any(), listRequest).Return(listResponse, nil)
	gotList, err := visManager.AdminListExecutions(context.Background(), listRequest)
	require.NoError(t, err)
	require.Equal(t, listResponse, gotList)

	countRequest := &manager.AdminCountExecutionsRequest{Namespace: testNamespace}
	countResponse := &manager.AdminCountExecutionsResponse{Count: 10}
	delegate.EXPECT().AdminCountExecutions(gomock.Any(), countRequest).Return(countResponse, nil)
	gotCount, err := visManager.AdminCountExecutions(context.Background(), countRequest)
	require.NoError(t, err)
	require.Equal(t, countResponse, gotCount)
}

func TestVisibilityManagerMetrics_AdminAPIs_DelegateNotAdmin(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	visManager := newTestVisibilityManagerMetrics(manager.NewMockVisibilityManager(ctrl))

	_, err := visManager.AdminListExecutions(context.Background(), &manager.AdminListExecutionsRequest{})
	require.ErrorIs(t, err, manager.ErrNotAdminVisibilityManager)

	_, err = visManager.AdminCountExecutions(context.Background(), &manager.AdminCountExecutionsRequest{})
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

// TestExtractArchetypeID covers reading the archetype ID a CHASM execution stores in the
// namespace division search attribute.
func TestExtractArchetypeID(t *testing.T) {
	t.Parallel()

	withDivision := func(p *commonpb.Payload) *commonpb.SearchAttributes {
		return &commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{
				sadefs.TemporalNamespaceDivision: p,
			},
		}
	}

	testCases := []struct {
		name             string
		searchAttributes *commonpb.SearchAttributes
		archetypeID      chasm.ArchetypeID
		expectErr        bool
		// isChasm is the expected isChasmExecution result for the same input, which
		// drives whether the memo is split into its user and CHASM parts.
		isChasm bool
	}{
		{
			// No namespace division at all: an ordinary workflow. That is not an error,
			// it just leaves the archetype unspecified.
			name:             "no search attributes",
			searchAttributes: nil,
			archetypeID:      chasm.UnspecifiedArchetypeID,
			isChasm:          false,
		},
		{
			name:             "no namespace division",
			searchAttributes: &commonpb.SearchAttributes{},
			archetypeID:      chasm.UnspecifiedArchetypeID,
			isChasm:          false,
		},
		{
			name:             "archetype ID",
			searchAttributes: withDivision(payload.EncodeString("12345")),
			archetypeID:      chasm.ArchetypeID(12345),
			isChasm:          true,
		},
		{
			name:             "workflow archetype ID",
			searchAttributes: withDivision(payload.EncodeString(strconv.Itoa(int(chasm.WorkflowArchetypeID)))),
			archetypeID:      chasm.WorkflowArchetypeID,
			isChasm:          true,
		},
		{
			// A namespace division set by a workflow rather than by CHASM, e.g. the
			// scheduler's.
			name:             "non-numeric namespace division",
			searchAttributes: withDivision(payload.EncodeString("user-division")),
			expectErr:        true,
			isChasm:          false,
		},
		{
			name:             "empty namespace division",
			searchAttributes: withDivision(payload.EncodeString("")),
			archetypeID:      chasm.UnspecifiedArchetypeID,
			expectErr:        true,
			isChasm:          false,
		},
		{
			// Zero is the reserved unspecified value, so a division holding it does not
			// identify a CHASM execution even though it parses.
			name:             "unspecified archetype ID",
			searchAttributes: withDivision(payload.EncodeString("0")),
			archetypeID:      chasm.UnspecifiedArchetypeID,
			isChasm:          false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			archetypeID, err := extractArchetypeID(tc.searchAttributes)
			if tc.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.archetypeID, archetypeID)
			}
			require.Equal(t, tc.isChasm, isChasmExecution(tc.searchAttributes))
		})
	}
}

// TestSplitUserAndChasmMemo covers that only a CHASM execution's memo is split into its
// user and CHASM parts. An ordinary workflow's memo must be returned whole, including
// when the execution has no search attributes at all.
func TestSplitUserAndChasmMemo(t *testing.T) {
	t.Parallel()

	userMemo := &commonpb.Memo{
		Fields: map[string]*commonpb.Payload{"key": payload.EncodeString("value")},
	}
	chasmMemoPayload := payload.EncodeString("chasm-memo")

	userMemoBlob, err := serializeMemo(userMemo)
	require.NoError(t, err)
	userMemoPayload, err := payload.Encode(userMemo)
	require.NoError(t, err)
	combinedMemoBlob, err := serializeMemo(&commonpb.Memo{
		Fields: map[string]*commonpb.Payload{
			chasm.UserMemoKey:  userMemoPayload,
			chasm.ChasmMemoKey: chasmMemoPayload,
		},
	})
	require.NoError(t, err)

	t.Run("workflow without search attributes", func(t *testing.T) {
		t.Parallel()
		gotUserMemo, gotChasmMemo, err := splitUserAndChasmMemo(&store.InternalExecutionInfo{
			Memo: userMemoBlob,
		})
		require.NoError(t, err)
		protorequire.ProtoEqual(t, userMemo, gotUserMemo)
		require.Nil(t, gotChasmMemo)
	})

	t.Run("chasm execution", func(t *testing.T) {
		t.Parallel()
		gotUserMemo, gotChasmMemo, err := splitUserAndChasmMemo(&store.InternalExecutionInfo{
			Memo: combinedMemoBlob,
			SearchAttributes: &commonpb.SearchAttributes{
				IndexedFields: map[string]*commonpb.Payload{
					sadefs.TemporalNamespaceDivision: payload.EncodeString("12345"),
				},
			},
		})
		require.NoError(t, err)
		protorequire.ProtoEqual(t, userMemo, gotUserMemo)
		protorequire.ProtoEqual(t, chasmMemoPayload, gotChasmMemo)
	})
}
