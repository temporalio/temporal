package namespacereplication

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	serverclient "go.temporal.io/server/client"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// -----------------------------------------------------------------------------
// Pure error classification.
// -----------------------------------------------------------------------------

func TestClassifyLocalErr(t *testing.T) {
	testCases := []struct {
		name string
		err  error
		want string
	}{
		{"CAS conflict / store unavailable -> Unavailable", serviceerror.NewUnavailable("conditional failure"), localFailureUnavailable},
		{"persistence CAS conflict -> Unavailable", &persistence.ConditionFailedError{Msg: "conflict"}, localFailureUnavailable},
		{"persistence timeout -> Unavailable", &persistence.TimeoutError{Msg: "timeout"}, localFailureUnavailable},
		{"context canceled -> Unavailable", context.Canceled, localFailureUnavailable},
		{"context deadline exceeded -> Unavailable", context.DeadlineExceeded, localFailureUnavailable},
		{"invalid argument -> InvalidArgument", serviceerror.NewInvalidArgument("bad"), localFailureInvalidArgument},
		{"create collision -> AlreadyExists", serviceerror.NewNamespaceAlreadyExists("dup"), localFailureAlreadyExists},
		{"not found -> Internal (degenerate)", serviceerror.NewNotFound("missing"), localFailureInternal},
		{"plain error -> Internal", errors.New("boom"), localFailureInternal},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, classifyLocalErr(tc.err))
		})
	}
}

func TestClassifyPeerErr(t *testing.T) {
	testCases := []struct {
		name string
		err  error
		want namespacereplicationpb.PeerApplyOutcome
	}{
		{"unavailable -> retriable", serviceerror.NewUnavailable("down"), namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_RETRIABLE},
		{"invalid argument -> terminal", serviceerror.NewInvalidArgument("bad"), namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_TERMINAL},
		{"not found -> terminal", serviceerror.NewNotFound("missing"), namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_TERMINAL},
		{"unimplemented (peer too old) -> terminal", serviceerror.NewUnimplemented("no rpc"), namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_TERMINAL},
		{"unknown -> retriable (safe: apply-if-higher makes dup a no-op)", errors.New("weird"), namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_RETRIABLE},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, classifyPeerErr(tc.err))
		})
	}
}

func TestIsPeerDestinationDown(t *testing.T) {
	testCases := []struct {
		name string
		err  error
		want bool
	}{
		{"unavailable", serviceerror.NewUnavailable("down"), true},
		{"deadline exceeded", serviceerror.NewDeadlineExceeded("timeout"), true},
		{"dial error", errors.New("dial failed"), true},
		{"circuit breaker open", serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_CIRCUIT_BREAKER_OPEN, "open"), false},
		{"remote internal", serviceerror.NewInternal("failed"), false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isPeerDestinationDown(tc.err))
		})
	}
}

func TestPeerOutcomeFromResultRejectsZeroValue(t *testing.T) {
	outcome, err := peerOutcomeFromResult(PeerApplyResultUnspecified)
	require.Error(t, err)
	require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_UNSPECIFIED, outcome)
}

// -----------------------------------------------------------------------------
// Validate gating (pure — Validate ignores the chasm.Context).
// -----------------------------------------------------------------------------

func TestApplyLocalTaskHandler_Validate(t *testing.T) {
	h := &applyLocalTaskHandler{}
	newComp := func(status namespacereplicationpb.ComponentStatus, localOutcome namespacereplicationpb.LocalApplyOutcome) *NamespaceMutationComponent {
		return &NamespaceMutationComponent{NamespaceMutationState: &namespacereplicationpb.NamespaceMutationState{
			Status:     status,
			LocalApply: &namespacereplicationpb.LocalApplyStatus{Outcome: localOutcome},
		}}
	}
	testCases := []struct {
		name string
		comp *NamespaceMutationComponent
		want bool
	}{
		{"running + pending -> run", newComp(namespacereplicationpb.COMPONENT_STATUS_RUNNING, namespacereplicationpb.LOCAL_APPLY_OUTCOME_PENDING), true},
		{"already committed -> drop", newComp(namespacereplicationpb.COMPONENT_STATUS_RUNNING, namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED), false},
		{"component failed -> drop", newComp(namespacereplicationpb.COMPONENT_STATUS_FAILED, namespacereplicationpb.LOCAL_APPLY_OUTCOME_PENDING), false},
		{"component completed -> drop", newComp(namespacereplicationpb.COMPONENT_STATUS_COMPLETED, namespacereplicationpb.LOCAL_APPLY_OUTCOME_PENDING), false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			run, err := h.Validate(nil, tc.comp, chasm.TaskInvocation{}, &namespacereplicationpb.ApplyLocalTask{})
			require.NoError(t, err)
			require.Equal(t, tc.want, run)
		})
	}
}

func TestApplyPeerTaskHandler_Validate(t *testing.T) {
	h := &applyPeerTaskHandler{}
	const cell = "cellB"
	newComp := func(status namespacereplicationpb.ComponentStatus, local namespacereplicationpb.LocalApplyOutcome, peer *namespacereplicationpb.PeerApplyStatus) *NamespaceMutationComponent {
		peers := map[string]*namespacereplicationpb.PeerApplyStatus{}
		if peer != nil {
			peers[cell] = peer
		}
		return &NamespaceMutationComponent{NamespaceMutationState: &namespacereplicationpb.NamespaceMutationState{
			Status:     status,
			LocalApply: &namespacereplicationpb.LocalApplyStatus{Outcome: local},
			PeerApply:  peers,
		}}
	}
	pending := &namespacereplicationpb.PeerApplyStatus{Outcome: namespacereplicationpb.PEER_APPLY_OUTCOME_PENDING, AttemptCount: 0}

	testCases := []struct {
		name    string
		comp    *NamespaceMutationComponent
		attempt int32
		want    bool
	}{
		{"committed + pending + matching attempt -> run", newComp(namespacereplicationpb.COMPONENT_STATUS_RUNNING, namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED, pending), 0, true},
		{"local not committed -> drop", newComp(namespacereplicationpb.COMPONENT_STATUS_RUNNING, namespacereplicationpb.LOCAL_APPLY_OUTCOME_PENDING, pending), 0, false},
		{"component not running -> drop", newComp(namespacereplicationpb.COMPONENT_STATUS_FAILED, namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED, pending), 0, false},
		{"peer missing -> drop", newComp(namespacereplicationpb.COMPONENT_STATUS_RUNNING, namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED, nil), 0, false},
		{"peer already applied -> drop", newComp(namespacereplicationpb.COMPONENT_STATUS_RUNNING, namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED, &namespacereplicationpb.PeerApplyStatus{Outcome: namespacereplicationpb.PEER_APPLY_OUTCOME_APPLIED}), 0, false},
		{"stale attempt -> drop", newComp(namespacereplicationpb.COMPONENT_STATUS_RUNNING, namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED, &namespacereplicationpb.PeerApplyStatus{Outcome: namespacereplicationpb.PEER_APPLY_OUTCOME_PENDING, AttemptCount: 2}), 1, false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			run, err := h.Validate(nil, tc.comp, chasm.TaskInvocation{}, &namespacereplicationpb.ApplyPeerTask{TargetCell: cell, Attempt: tc.attempt})
			require.NoError(t, err)
			require.Equal(t, tc.want, run)
		})
	}
}

// -----------------------------------------------------------------------------
// Engine-driven Execute tests.
// -----------------------------------------------------------------------------

type nsreplTestEnv struct {
	t              *testing.T
	metadataMgr    *persistence.MockMetadataManager
	clientBean     *serverclient.MockBean
	adminClient    *adminservicemock.MockAdminServiceClient
	localHandler   *applyLocalTaskHandler
	peerHandler    *applyPeerTaskHandler
	backoffHandler *applyPeerBackoffTaskHandler
	engine         *chasmtest.Engine
	engineCtx      context.Context
}

func newNsreplTestEnv(t *testing.T) *nsreplTestEnv {
	t.Helper()
	logger := log.NewTestLogger()
	ctrl := gomock.NewController(t)
	metadataMgr := persistence.NewMockMetadataManager(ctrl)
	clientBean := serverclient.NewMockBean(ctrl)
	adminClient := adminservicemock.NewMockAdminServiceClient(ctrl)

	localHandler := &applyLocalTaskHandler{
		metadataManager: metadataMgr,
		metricsHandler:  metrics.NoopMetricsHandler,
		logger:          logger,
	}
	peerHandler := &applyPeerTaskHandler{
		// Exercise the real default transport (admin RPC) over the mocked client
		// bean, so the handler + default applier are covered together.
		peerApplier:    newAdminClientPeerApplier(clientBean),
		metricsHandler: metrics.NoopMetricsHandler,
		logger:         logger,
	}
	backoffHandler := newApplyPeerBackoffTaskHandler()

	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(&Library{
		ApplyLocalTaskHandler:  localHandler,
		ApplyPeerTaskHandler:   peerHandler,
		PeerBackoffTaskHandler: backoffHandler,
	}))

	engine := chasmtest.NewEngine(t, registry)
	return &nsreplTestEnv{
		t:              t,
		metadataMgr:    metadataMgr,
		clientBean:     clientBean,
		adminClient:    adminClient,
		localHandler:   localHandler,
		peerHandler:    peerHandler,
		backoffHandler: backoffHandler,
		engine:         engine,
		engineCtx:      chasm.NewEngineContext(context.Background(), engine),
	}
}

// start creates a NamespaceMutationComponent execution and returns its root ref.
// mutate optionally seeds initial state (e.g. a committed local apply for peer tests).
func (env *nsreplTestEnv) start(mutation *namespacereplicationpb.NamespaceMutation, mutate func(*NamespaceMutationComponent)) chasm.ComponentRef {
	env.t.Helper()
	key := chasm.ExecutionKey{NamespaceID: "namespace-id", BusinessID: "ns-id:uuid", RunID: "run-id"}
	_, err := chasm.StartExecution(
		env.engineCtx,
		key,
		func(mctx chasm.MutableContext, m *namespacereplicationpb.NamespaceMutation) (*NamespaceMutationComponent, error) {
			c := NewNamespaceMutationComponent(mctx, m)
			if mutate != nil {
				mutate(c)
			}
			return c, nil
		},
		mutation,
	)
	require.NoError(env.t, err)
	return chasm.NewComponentRef[*NamespaceMutationComponent](key)
}

func (env *nsreplTestEnv) read(ref chasm.ComponentRef) *NamespaceMutationComponent {
	env.t.Helper()
	c, err := chasm.ReadComponent(
		env.engineCtx,
		ref,
		func(c *NamespaceMutationComponent, _ chasm.Context, _ struct{}) (*NamespaceMutationComponent, error) {
			return c, nil
		},
		struct{}{},
	)
	require.NoError(env.t, err)
	return c
}

func testDetail() *persistencespb.NamespaceDetail {
	return &persistencespb.NamespaceDetail{
		Info:              &persistencespb.NamespaceInfo{Id: "ns-id", Name: "ns", State: enumspb.NAMESPACE_STATE_REGISTERED},
		Config:            &persistencespb.NamespaceConfig{},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{ActiveClusterName: "cellA", Clusters: []string{"cellA", "cellB"}},
		ConfigVersion:     5,
		FailoverVersion:   3,
	}
}

// A successful local UPDATE commits and schedules one peer task per peer cell.
func (env *nsreplTestEnv) mutationUpdate(peers ...string) *namespacereplicationpb.NamespaceMutation {
	return &namespacereplicationpb.NamespaceMutation{
		Operation:       namespacereplicationpb.NAMESPACE_OPERATION_UPDATE,
		NamespaceDetail: testDetail(),
		ExpectedVersion: 7,
		PeerCells:       peers,
	}
}

func TestApplyLocalTask_Execute_UpdateCommitSchedulesPeers(t *testing.T) {
	env := newNsreplTestEnv(t)
	ref := env.start(env.mutationUpdate("cellB", "cellC"), nil)

	env.metadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *persistence.UpdateNamespaceRequest) error {
			require.Equal(t, int64(7), req.NotificationVersion)
			require.True(t, req.IsGlobalNamespace)
			return nil
		})
	require.NoError(t, env.localHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyLocalTask{}))

	c := env.read(ref)
	require.Equal(t, namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED, c.GetLocalApply().GetOutcome())
	// Peers remain and are pending, so the component stays RUNNING for fan-out.
	require.Equal(t, namespacereplicationpb.COMPONENT_STATUS_RUNNING, c.GetStatus())
	require.Len(t, c.GetPeerApply(), 2)
	for _, cell := range []string{"cellB", "cellC"} {
		require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_PENDING, c.GetPeerApply()[cell].GetOutcome(), cell)
	}
}

func TestApplyLocalTask_Execute_ShadowSkipsMetadataWrite(t *testing.T) {
	env := newNsreplTestEnv(t)
	mutation := env.mutationUpdate("cellB")
	mutation.Shadow = true
	ref := env.start(mutation, nil)

	require.NoError(t, env.localHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyLocalTask{}))

	component := env.read(ref)
	require.Equal(t, namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED, component.GetLocalApply().GetOutcome())
	require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_PENDING, component.GetPeerApply()["cellB"].GetOutcome())
}

// A local commit with no peers (single-cluster global namespace) has nothing to
// fan out to and must complete immediately.
func TestApplyLocalTask_Execute_NoPeersCompletes(t *testing.T) {
	env := newNsreplTestEnv(t)
	ref := env.start(env.mutationUpdate(), nil) // no peer cells

	env.metadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Return(nil)
	require.NoError(t, env.localHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyLocalTask{}))

	c := env.read(ref)
	require.Equal(t, namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED, c.GetLocalApply().GetOutcome())
	require.Equal(t, namespacereplicationpb.COMPONENT_STATUS_COMPLETED, c.GetStatus(),
		"a zero-peer mutation has no fan-out and must reach COMPLETED, not linger RUNNING")
}

func TestApplyLocalTask_Execute_CreateCommit(t *testing.T) {
	env := newNsreplTestEnv(t)
	mutation := env.mutationUpdate("cellB")
	mutation.Operation = namespacereplicationpb.NAMESPACE_OPERATION_CREATE
	ref := env.start(mutation, nil)

	env.metadataMgr.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *persistence.CreateNamespaceRequest) (*persistence.CreateNamespaceResponse, error) {
			require.True(t, req.IsGlobalNamespace)
			return &persistence.CreateNamespaceResponse{ID: "ns-id"}, nil
		})
	require.NoError(t, env.localHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyLocalTask{}))

	c := env.read(ref)
	require.Equal(t, namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED, c.GetLocalApply().GetOutcome())
}

func TestApplyLocalTask_Execute_UpdateRetryRecoversCommittedWrite(t *testing.T) {
	env := newNsreplTestEnv(t)
	mutation := env.mutationUpdate("cellB")
	ref := env.start(mutation, nil)

	env.metadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Return(
		&persistence.TimeoutError{Msg: "write result unknown"})
	env.metadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{ID: "ns-id"}).Return(
		&persistence.GetNamespaceResponse{
			Namespace:           proto.Clone(mutation.GetNamespaceDetail()).(*persistencespb.NamespaceDetail),
			IsGlobalNamespace:   true,
			NotificationVersion: mutation.GetExpectedVersion(),
		},
		nil,
	)

	require.NoError(t, env.localHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyLocalTask{}))

	component := env.read(ref)
	require.Equal(t, namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED, component.GetLocalApply().GetOutcome())
	require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_PENDING, component.GetPeerApply()["cellB"].GetOutcome())
}

func TestApplyLocalTask_Execute_UpdateLateCommitRecoveredOnRetry(t *testing.T) {
	env := newNsreplTestEnv(t)
	mutation := env.mutationUpdate("cellB")
	ref := env.start(mutation, nil)

	oldNamespace := proto.Clone(mutation.GetNamespaceDetail()).(*persistencespb.NamespaceDetail)
	oldNamespace.Info.Description = "old value"
	gomock.InOrder(
		env.metadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Return(
			&persistence.TimeoutError{Msg: "write result unknown"}),
		env.metadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{ID: "ns-id"}).Return(
			&persistence.GetNamespaceResponse{
				Namespace:           oldNamespace,
				IsGlobalNamespace:   true,
				NotificationVersion: mutation.GetExpectedVersion(),
			},
			nil,
		),
		env.metadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(
			&persistence.GetMetadataResponse{NotificationVersion: mutation.GetExpectedVersion()},
			nil,
		),
		env.metadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Return(
			serviceerror.NewUnavailable("conditional failure")),
		env.metadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{ID: "ns-id"}).Return(
			&persistence.GetNamespaceResponse{
				Namespace:           proto.Clone(mutation.GetNamespaceDetail()).(*persistencespb.NamespaceDetail),
				IsGlobalNamespace:   true,
				NotificationVersion: mutation.GetExpectedVersion(),
			},
			nil,
		),
	)

	require.Error(t, env.localHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyLocalTask{}))
	component := env.read(ref)
	require.Equal(t, namespacereplicationpb.LOCAL_APPLY_OUTCOME_PENDING, component.GetLocalApply().GetOutcome())

	require.NoError(t, env.localHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyLocalTask{}))
	component = env.read(ref)
	require.Equal(t, namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED, component.GetLocalApply().GetOutcome())
	require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_PENDING, component.GetPeerApply()["cellB"].GetOutcome())
}

func TestApplyLocalTask_Execute_CreateRetryRecoversCommittedWrite(t *testing.T) {
	env := newNsreplTestEnv(t)
	mutation := env.mutationUpdate("cellB")
	mutation.Operation = namespacereplicationpb.NAMESPACE_OPERATION_CREATE
	ref := env.start(mutation, nil)

	env.metadataMgr.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).Return(
		nil,
		serviceerror.NewNamespaceAlreadyExists("namespace already exists"),
	)
	env.metadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{ID: "ns-id"}).Return(
		&persistence.GetNamespaceResponse{
			Namespace:         proto.Clone(mutation.GetNamespaceDetail()).(*persistencespb.NamespaceDetail),
			IsGlobalNamespace: true,
		},
		nil,
	)

	require.NoError(t, env.localHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyLocalTask{}))

	component := env.read(ref)
	require.Equal(t, namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED, component.GetLocalApply().GetOutcome())
}

func TestApplyLocalTask_Execute_CreateLateCommitRecoveredOnRetry(t *testing.T) {
	env := newNsreplTestEnv(t)
	mutation := env.mutationUpdate("cellB")
	mutation.Operation = namespacereplicationpb.NAMESPACE_OPERATION_CREATE
	ref := env.start(mutation, nil)

	gomock.InOrder(
		env.metadataMgr.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).Return(
			nil,
			&persistence.TimeoutError{Msg: "write result unknown"},
		),
		env.metadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{ID: "ns-id"}).Return(
			nil,
			serviceerror.NewNamespaceNotFound("namespace not visible yet"),
		),
		env.metadataMgr.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).Return(
			nil,
			serviceerror.NewNamespaceAlreadyExists("namespace already exists"),
		),
		env.metadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{ID: "ns-id"}).Return(
			&persistence.GetNamespaceResponse{
				Namespace:         proto.Clone(mutation.GetNamespaceDetail()).(*persistencespb.NamespaceDetail),
				IsGlobalNamespace: true,
			},
			nil,
		),
	)

	require.Error(t, env.localHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyLocalTask{}))
	component := env.read(ref)
	require.Equal(t, namespacereplicationpb.LOCAL_APPLY_OUTCOME_PENDING, component.GetLocalApply().GetOutcome())

	require.NoError(t, env.localHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyLocalTask{}))
	component = env.read(ref)
	require.Equal(t, namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED, component.GetLocalApply().GetOutcome())
}

func TestApplyLocalTask_Execute_CreateCollisionStillFails(t *testing.T) {
	env := newNsreplTestEnv(t)
	mutation := env.mutationUpdate("cellB")
	mutation.Operation = namespacereplicationpb.NAMESPACE_OPERATION_CREATE
	ref := env.start(mutation, nil)

	env.metadataMgr.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).Return(
		nil,
		serviceerror.NewNamespaceAlreadyExists("namespace already exists"),
	)
	existing := proto.Clone(mutation.GetNamespaceDetail()).(*persistencespb.NamespaceDetail)
	existing.Info.Name = "different-namespace"
	env.metadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{ID: "ns-id"}).Return(
		&persistence.GetNamespaceResponse{Namespace: existing, IsGlobalNamespace: true},
		nil,
	)

	require.NoError(t, env.localHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyLocalTask{}))

	component := env.read(ref)
	require.Equal(t, namespacereplicationpb.LOCAL_APPLY_OUTCOME_FAILED, component.GetLocalApply().GetOutcome())
	require.Equal(
		t,
		localFailureAlreadyExists,
		component.GetLocalApply().GetFailure().GetApplicationFailureInfo().GetType(),
	)
}

func TestApplyLocalTask_Execute_UpdateRetryRequiresExpectedNotificationVersion(t *testing.T) {
	env := newNsreplTestEnv(t)
	mutation := env.mutationUpdate("cellB")
	ref := env.start(mutation, nil)

	env.metadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Return(
		serviceerror.NewUnavailable("conditional failure"))
	env.metadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{ID: "ns-id"}).Return(
		&persistence.GetNamespaceResponse{
			Namespace:           proto.Clone(mutation.GetNamespaceDetail()).(*persistencespb.NamespaceDetail),
			IsGlobalNamespace:   true,
			NotificationVersion: mutation.GetExpectedVersion() + 1,
		},
		nil,
	)
	env.metadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(
		&persistence.GetMetadataResponse{NotificationVersion: mutation.GetExpectedVersion() + 1},
		nil,
	)

	require.NoError(t, env.localHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyLocalTask{}))

	component := env.read(ref)
	require.Equal(t, namespacereplicationpb.LOCAL_APPLY_OUTCOME_FAILED, component.GetLocalApply().GetOutcome())
}

func TestApplyLocalTask_Execute_ReconcileReadFailureKeepsPending(t *testing.T) {
	env := newNsreplTestEnv(t)
	ref := env.start(env.mutationUpdate("cellB"), nil)

	env.metadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Return(
		serviceerror.NewUnavailable("write result unknown"))
	env.metadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{ID: "ns-id"}).Return(
		nil,
		serviceerror.NewUnavailable("read unavailable"),
	)

	err := env.localHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyLocalTask{})
	require.Error(t, err)

	component := env.read(ref)
	require.Equal(t, namespacereplicationpb.LOCAL_APPLY_OUTCOME_PENDING, component.GetLocalApply().GetOutcome())
	require.Equal(t, namespacereplicationpb.COMPONENT_STATUS_RUNNING, component.GetStatus())
}

// A CAS conflict from the store surfaces as a terminal local failure carrying the
// Unavailable (retriable) error class, and NO peer task is scheduled — the "no
// divergence on caller-visible failure" gating invariant.
func TestApplyLocalTask_Execute_CASConflictFailsUnavailable(t *testing.T) {
	env := newNsreplTestEnv(t)
	ref := env.start(env.mutationUpdate("cellB", "cellC"), nil)

	env.metadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Return(
		serviceerror.NewUnavailable("UpdateNamespace operation failed because of conditional failure."))
	concurrentWinner := proto.Clone(testDetail()).(*persistencespb.NamespaceDetail)
	concurrentWinner.Info.Description = "concurrent winner"
	env.metadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{ID: "ns-id"}).Return(
		&persistence.GetNamespaceResponse{
			Namespace:           concurrentWinner,
			IsGlobalNamespace:   true,
			NotificationVersion: 7,
		},
		nil,
	)
	env.metadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(
		&persistence.GetMetadataResponse{NotificationVersion: 8},
		nil,
	)
	require.NoError(t, env.localHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyLocalTask{}))

	c := env.read(ref)
	require.Equal(t, namespacereplicationpb.LOCAL_APPLY_OUTCOME_FAILED, c.GetLocalApply().GetOutcome())
	require.Equal(t, namespacereplicationpb.COMPONENT_STATUS_FAILED, c.GetStatus())
	appInfo := c.GetLocalApply().GetFailure().GetApplicationFailureInfo()
	require.Equal(t, localFailureUnavailable, appInfo.GetType())
	require.False(t, appInfo.GetNonRetryable(), "CAS conflict must be retriable")
	// Gating invariant: peers are never advanced when the local commit fails.
	for _, cell := range []string{"cellB", "cellC"} {
		require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_PENDING, c.GetPeerApply()[cell].GetOutcome(), cell)
	}
}

// startCommitted seeds a component already past local commit, ready for peer fan-out.
func (env *nsreplTestEnv) startCommitted(peer string) chasm.ComponentRef {
	return env.start(env.mutationUpdate(peer), func(c *NamespaceMutationComponent) {
		c.LocalApply = &namespacereplicationpb.LocalApplyStatus{Outcome: namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED}
	})
}

func TestApplyPeerTask_Execute_Applied(t *testing.T) {
	env := newNsreplTestEnv(t)
	ref := env.startCommitted("cellB")

	env.clientBean.EXPECT().GetRemoteAdminClient("cellB").Return(env.adminClient, nil)
	env.adminClient.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).Return(
		&adminservice.ApplyNamespaceMutationResponse{Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED}, nil)

	require.NoError(t, env.peerHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{Destination: "cellB"}, &namespacereplicationpb.ApplyPeerTask{TargetCell: "cellB", Attempt: 0}))

	c := env.read(ref)
	require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_APPLIED, c.GetPeerApply()["cellB"].GetOutcome())
	// Only peer is now terminal -> component completes.
	require.Equal(t, namespacereplicationpb.COMPONENT_STATUS_COMPLETED, c.GetStatus())
}

func TestApplyPeerTask_Execute_NoOpStale(t *testing.T) {
	env := newNsreplTestEnv(t)
	ref := env.startCommitted("cellB")

	env.clientBean.EXPECT().GetRemoteAdminClient("cellB").Return(env.adminClient, nil)
	env.adminClient.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).Return(
		&adminservice.ApplyNamespaceMutationResponse{Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_NO_OP_STALE}, nil)

	require.NoError(t, env.peerHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{Destination: "cellB"}, &namespacereplicationpb.ApplyPeerTask{TargetCell: "cellB", Attempt: 0}))

	c := env.read(ref)
	require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_NO_OP_STALE, c.GetPeerApply()["cellB"].GetOutcome())
	require.Equal(t, namespacereplicationpb.COMPONENT_STATUS_COMPLETED, c.GetStatus())
}

func TestApplyPeerTask_Execute_TerminalError(t *testing.T) {
	env := newNsreplTestEnv(t)
	ref := env.startCommitted("cellB")

	env.clientBean.EXPECT().GetRemoteAdminClient("cellB").Return(env.adminClient, nil)
	env.adminClient.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).Return(
		nil, serviceerror.NewInvalidArgument("bad payload"))

	require.NoError(t, env.peerHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyPeerTask{TargetCell: "cellB", Attempt: 0}))

	c := env.read(ref)
	peer := c.GetPeerApply()["cellB"]
	require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_TERMINAL, peer.GetOutcome())
	require.NotNil(t, peer.GetLastFailure())
	require.Equal(t, namespacereplicationpb.COMPONENT_STATUS_COMPLETED, c.GetStatus())
}

// A retriable peer error keeps the peer PENDING, bumps the attempt, and does not
// complete the component — a later attempt within the retry budget can converge.
func TestApplyPeerTask_Execute_RetriableReschedules(t *testing.T) {
	env := newNsreplTestEnv(t)
	ref := env.startCommitted("cellB")

	env.clientBean.EXPECT().GetRemoteAdminClient("cellB").Return(env.adminClient, nil)
	env.adminClient.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).Return(
		nil, serviceerror.NewUnavailable("peer down"))

	err := env.peerHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{Destination: "cellB"}, &namespacereplicationpb.ApplyPeerTask{TargetCell: "cellB", Attempt: 0})
	var destinationDownErr *queueserrors.DestinationDownError
	require.ErrorAs(t, err, &destinationDownErr)

	c := env.read(ref)
	peer := c.GetPeerApply()["cellB"]
	require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_PENDING, peer.GetOutcome(), "retriable failure keeps peer pending")
	require.Equal(t, int32(1), peer.GetAttemptCount())
	require.Equal(t, namespacereplicationpb.COMPONENT_STATUS_RUNNING, c.GetStatus(), "component not complete while a peer is still retrying")
}

// -----------------------------------------------------------------------------
// PeerApplier transport seam.
// -----------------------------------------------------------------------------

// TestAdminClientPeerApplier_Apply covers the default (admin RPC) transport in
// isolation: outcome mapping and error propagation for the handler to classify.
func TestAdminClientPeerApplier_Apply(t *testing.T) {
	detail := testDetail()
	newApplier := func(t *testing.T) (*serverclient.MockBean, *adminservicemock.MockAdminServiceClient, PeerApplier) {
		ctrl := gomock.NewController(t)
		bean := serverclient.NewMockBean(ctrl)
		admin := adminservicemock.NewMockAdminServiceClient(ctrl)
		return bean, admin, newAdminClientPeerApplier(bean)
	}

	t.Run("applied outcome", func(t *testing.T) {
		bean, admin, applier := newApplier(t)
		bean.EXPECT().GetRemoteAdminClient("cellB").Return(admin, nil)
		admin.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, request *adminservice.ApplyNamespaceMutationRequest, _ ...grpc.CallOption) (*adminservice.ApplyNamespaceMutationResponse, error) {
				require.True(t, request.GetShadow())
				require.NotEmpty(t, request.GetFingerprint())
				return &adminservice.ApplyNamespaceMutationResponse{Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED}, nil
			})
		res, err := applier.Apply(context.Background(), "cellB", enumsspb.NAMESPACE_OPERATION_UPDATE, detail, true)
		require.NoError(t, err)
		require.Equal(t, PeerApplyResultApplied, res)
	})
	t.Run("created maps to applied", func(t *testing.T) {
		bean, admin, applier := newApplier(t)
		bean.EXPECT().GetRemoteAdminClient("cellB").Return(admin, nil)
		admin.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).Return(
			&adminservice.ApplyNamespaceMutationResponse{Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_CREATED}, nil)
		res, err := applier.Apply(context.Background(), "cellB", enumsspb.NAMESPACE_OPERATION_CREATE, detail, true)
		require.NoError(t, err)
		require.Equal(t, PeerApplyResultApplied, res)
	})
	t.Run("no-op-stale outcome", func(t *testing.T) {
		bean, admin, applier := newApplier(t)
		bean.EXPECT().GetRemoteAdminClient("cellB").Return(admin, nil)
		admin.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).Return(
			&adminservice.ApplyNamespaceMutationResponse{Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_NO_OP_STALE}, nil)
		res, err := applier.Apply(context.Background(), "cellB", enumsspb.NAMESPACE_OPERATION_UPDATE, detail, true)
		require.NoError(t, err)
		require.Equal(t, PeerApplyResultNoOpStale, res)
	})
	t.Run("duplicate maps to applied", func(t *testing.T) {
		bean, admin, applier := newApplier(t)
		bean.EXPECT().GetRemoteAdminClient("cellB").Return(admin, nil)
		admin.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).Return(
			&adminservice.ApplyNamespaceMutationResponse{Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_DUPLICATE}, nil)
		res, err := applier.Apply(context.Background(), "cellB", enumsspb.NAMESPACE_OPERATION_CREATE, detail, true)
		require.NoError(t, err)
		require.Equal(t, PeerApplyResultApplied, res)
	})
	t.Run("not-admitted is its own terminal result, not applied", func(t *testing.T) {
		bean, admin, applier := newApplier(t)
		bean.EXPECT().GetRemoteAdminClient("cellB").Return(admin, nil)
		admin.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).Return(
			&adminservice.ApplyNamespaceMutationResponse{Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_NOT_ADMITTED}, nil)
		res, err := applier.Apply(context.Background(), "cellB", enumsspb.NAMESPACE_OPERATION_UPDATE, detail, true)
		require.NoError(t, err)
		require.Equal(t, PeerApplyResultNotAdmitted, res)
	})
	t.Run("unspecified/unknown outcome surfaced as error, not phantom applied", func(t *testing.T) {
		bean, admin, applier := newApplier(t)
		bean.EXPECT().GetRemoteAdminClient("cellB").Return(admin, nil)
		admin.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).Return(
			&adminservice.ApplyNamespaceMutationResponse{Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_UNSPECIFIED}, nil)
		_, err := applier.Apply(context.Background(), "cellB", enumsspb.NAMESPACE_OPERATION_UPDATE, detail, true)
		require.Error(t, err)
	})
	t.Run("rpc error propagates", func(t *testing.T) {
		bean, admin, applier := newApplier(t)
		bean.EXPECT().GetRemoteAdminClient("cellB").Return(admin, nil)
		admin.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewUnavailable("down"))
		_, err := applier.Apply(context.Background(), "cellB", enumsspb.NAMESPACE_OPERATION_UPDATE, detail, true)
		require.Error(t, err)
	})
	t.Run("dial error propagates", func(t *testing.T) {
		bean, _, applier := newApplier(t)
		bean.EXPECT().GetRemoteAdminClient("cellB").Return(nil, serviceerror.NewUnavailable("no route"))
		_, err := applier.Apply(context.Background(), "cellB", enumsspb.NAMESPACE_OPERATION_UPDATE, detail, true)
		require.Error(t, err)
	})
}

// mockPeerApplier is a stand-in transport used to prove the handler delegates the
// peer write to the injected PeerApplier — the seam a deployment overrides.
type mockPeerApplier struct {
	result PeerApplyResult
	err    error
	cells  []string
}

func (m *mockPeerApplier) Apply(_ context.Context, targetCell string, _ enumsspb.NamespaceOperation, _ *persistencespb.NamespaceDetail, _ bool) (PeerApplyResult, error) {
	m.cells = append(m.cells, targetCell)
	return m.result, m.err
}

// TestApplyPeerTask_Execute_UsesInjectedApplier proves the transport is pluggable:
// a custom PeerApplier's result flows through the handler's unchanged policy
// (outcome recording + completion), with no admin RPC involved.
func TestApplyPeerTask_Execute_UsesInjectedApplier(t *testing.T) {
	env := newNsreplTestEnv(t)
	ref := env.startCommitted("cellB")

	applier := &mockPeerApplier{result: PeerApplyResultNoOpStale}
	handler := &applyPeerTaskHandler{
		peerApplier:    applier,
		metricsHandler: metrics.NoopMetricsHandler,
		logger:         log.NewTestLogger(),
	}

	require.NoError(t, handler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyPeerTask{TargetCell: "cellB", Attempt: 0}))

	require.Equal(t, []string{"cellB"}, applier.cells, "handler must delegate the peer transport to the injected applier")
	c := env.read(ref)
	require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_NO_OP_STALE, c.GetPeerApply()["cellB"].GetOutcome())
	require.Equal(t, namespacereplicationpb.COMPONENT_STATUS_COMPLETED, c.GetStatus())
}

func TestApplyPeerTask_Execute_UnknownInjectedResultRetries(t *testing.T) {
	env := newNsreplTestEnv(t)
	ref := env.startCommitted("cellB")

	applier := &mockPeerApplier{result: PeerApplyResult(999)}
	handler := &applyPeerTaskHandler{
		peerApplier:    applier,
		metricsHandler: metrics.NoopMetricsHandler,
		logger:         log.NewTestLogger(),
	}

	require.NoError(t, handler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyPeerTask{TargetCell: "cellB", Attempt: 0}))

	require.Equal(t, []string{"cellB"}, applier.cells)
	c := env.read(ref)
	peer := c.GetPeerApply()["cellB"]
	require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_PENDING, peer.GetOutcome(), "an unknown result must not be recorded as applied")
	require.Equal(t, int32(1), peer.GetAttemptCount())
	require.Contains(t, peer.GetLastFailure().GetMessage(), "unknown peer apply result: 999")
	require.Equal(t, namespacereplicationpb.COMPONENT_STATUS_RUNNING, c.GetStatus())
}
