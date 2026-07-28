package nsrepl

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	nsreplpb "go.temporal.io/server/chasm/lib/nsrepl/gen/nsreplpb/v1"
	serverclient "go.temporal.io/server/client"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.uber.org/mock/gomock"
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
		{"invalid argument -> InvalidArgument", serviceerror.NewInvalidArgument("bad"), localFailureInvalidArgument},
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
		want nsreplpb.PeerApplyOutcome
	}{
		{"unavailable -> retriable", serviceerror.NewUnavailable("down"), nsreplpb.PEER_APPLY_OUTCOME_FAILED_RETRIABLE},
		{"invalid argument -> terminal", serviceerror.NewInvalidArgument("bad"), nsreplpb.PEER_APPLY_OUTCOME_FAILED_TERMINAL},
		{"not found -> terminal", serviceerror.NewNotFound("missing"), nsreplpb.PEER_APPLY_OUTCOME_FAILED_TERMINAL},
		{"unimplemented (peer too old) -> terminal", serviceerror.NewUnimplemented("no rpc"), nsreplpb.PEER_APPLY_OUTCOME_FAILED_TERMINAL},
		{"unknown -> retriable (safe: apply-if-higher makes dup a no-op)", errors.New("weird"), nsreplpb.PEER_APPLY_OUTCOME_FAILED_RETRIABLE},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, classifyPeerErr(tc.err))
		})
	}
}

// -----------------------------------------------------------------------------
// Validate gating (pure — Validate ignores the chasm.Context).
// -----------------------------------------------------------------------------

func TestApplyLocalTaskHandler_Validate(t *testing.T) {
	h := &applyLocalTaskHandler{}
	newComp := func(status nsreplpb.ComponentStatus, localOutcome nsreplpb.LocalApplyOutcome) *NamespaceMutationComponent {
		return &NamespaceMutationComponent{NamespaceMutationState: &nsreplpb.NamespaceMutationState{
			Status:     status,
			LocalApply: &nsreplpb.LocalApplyStatus{Outcome: localOutcome},
		}}
	}
	testCases := []struct {
		name string
		comp *NamespaceMutationComponent
		want bool
	}{
		{"running + pending -> run", newComp(nsreplpb.COMPONENT_STATUS_RUNNING, nsreplpb.LOCAL_APPLY_OUTCOME_PENDING), true},
		{"already committed -> drop", newComp(nsreplpb.COMPONENT_STATUS_RUNNING, nsreplpb.LOCAL_APPLY_OUTCOME_COMMITTED), false},
		{"component failed -> drop", newComp(nsreplpb.COMPONENT_STATUS_FAILED, nsreplpb.LOCAL_APPLY_OUTCOME_PENDING), false},
		{"component completed -> drop", newComp(nsreplpb.COMPONENT_STATUS_COMPLETED, nsreplpb.LOCAL_APPLY_OUTCOME_PENDING), false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			run, err := h.Validate(nil, tc.comp, chasm.TaskAttributes{}, &nsreplpb.ApplyLocalTask{})
			require.NoError(t, err)
			require.Equal(t, tc.want, run)
		})
	}
}

func TestApplyPeerTaskHandler_Validate(t *testing.T) {
	h := &applyPeerTaskHandler{}
	const cell = "cellB"
	newComp := func(status nsreplpb.ComponentStatus, local nsreplpb.LocalApplyOutcome, peer *nsreplpb.PeerApplyStatus) *NamespaceMutationComponent {
		peers := map[string]*nsreplpb.PeerApplyStatus{}
		if peer != nil {
			peers[cell] = peer
		}
		return &NamespaceMutationComponent{NamespaceMutationState: &nsreplpb.NamespaceMutationState{
			Status:     status,
			LocalApply: &nsreplpb.LocalApplyStatus{Outcome: local},
			PeerApply:  peers,
		}}
	}
	pending := &nsreplpb.PeerApplyStatus{Outcome: nsreplpb.PEER_APPLY_OUTCOME_PENDING, AttemptCount: 0}

	testCases := []struct {
		name    string
		comp    *NamespaceMutationComponent
		attempt int32
		want    bool
	}{
		{"committed + pending + matching attempt -> run", newComp(nsreplpb.COMPONENT_STATUS_RUNNING, nsreplpb.LOCAL_APPLY_OUTCOME_COMMITTED, pending), 0, true},
		{"local not committed -> drop", newComp(nsreplpb.COMPONENT_STATUS_RUNNING, nsreplpb.LOCAL_APPLY_OUTCOME_PENDING, pending), 0, false},
		{"component not running -> drop", newComp(nsreplpb.COMPONENT_STATUS_FAILED, nsreplpb.LOCAL_APPLY_OUTCOME_COMMITTED, pending), 0, false},
		{"peer missing -> drop", newComp(nsreplpb.COMPONENT_STATUS_RUNNING, nsreplpb.LOCAL_APPLY_OUTCOME_COMMITTED, nil), 0, false},
		{"peer already applied -> drop", newComp(nsreplpb.COMPONENT_STATUS_RUNNING, nsreplpb.LOCAL_APPLY_OUTCOME_COMMITTED, &nsreplpb.PeerApplyStatus{Outcome: nsreplpb.PEER_APPLY_OUTCOME_APPLIED}), 0, false},
		{"stale attempt -> drop", newComp(nsreplpb.COMPONENT_STATUS_RUNNING, nsreplpb.LOCAL_APPLY_OUTCOME_COMMITTED, &nsreplpb.PeerApplyStatus{Outcome: nsreplpb.PEER_APPLY_OUTCOME_PENDING, AttemptCount: 2}), 1, false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			run, err := h.Validate(nil, tc.comp, chasm.TaskAttributes{}, &nsreplpb.ApplyPeerTask{TargetCell: cell, Attempt: tc.attempt})
			require.NoError(t, err)
			require.Equal(t, tc.want, run)
		})
	}
}

// -----------------------------------------------------------------------------
// Engine-driven Execute tests.
// -----------------------------------------------------------------------------

type nsreplTestEnv struct {
	t            *testing.T
	metadataMgr  *persistence.MockMetadataManager
	clientBean   *serverclient.MockBean
	adminClient  *adminservicemock.MockAdminServiceClient
	localHandler *applyLocalTaskHandler
	peerHandler  *applyPeerTaskHandler
	engine       *chasmtest.Engine
	engineCtx    context.Context
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
		clientBean:     clientBean,
		metricsHandler: metrics.NoopMetricsHandler,
		logger:         logger,
	}

	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(&Library{
		ApplyLocalTaskHandler: localHandler,
		ApplyPeerTaskHandler:  peerHandler,
	}))

	engine := chasmtest.NewEngine(t, registry)
	return &nsreplTestEnv{
		t:            t,
		metadataMgr:  metadataMgr,
		clientBean:   clientBean,
		adminClient:  adminClient,
		localHandler: localHandler,
		peerHandler:  peerHandler,
		engine:       engine,
		engineCtx:    chasm.NewEngineContext(context.Background(), engine),
	}
}

// start creates a NamespaceMutationComponent execution and returns its root ref.
// mutate optionally seeds initial state (e.g. a committed local apply for peer tests).
func (env *nsreplTestEnv) start(mutation *nsreplpb.NamespaceMutation, mutate func(*NamespaceMutationComponent)) chasm.ComponentRef {
	env.t.Helper()
	key := chasm.ExecutionKey{NamespaceID: "namespace-id", BusinessID: "ns-id:uuid", RunID: "run-id"}
	_, err := chasm.StartExecution(
		env.engineCtx,
		key,
		func(_ chasm.MutableContext, m *nsreplpb.NamespaceMutation) (*NamespaceMutationComponent, error) {
			c := NewNamespaceMutationComponent(m)
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

// A successful local UPDATE commits with the post-write notification_version and
// schedules one peer task per peer cell.
func (env *nsreplTestEnv) mutationUpdate(peers ...string) *nsreplpb.NamespaceMutation {
	return &nsreplpb.NamespaceMutation{
		Operation:       nsreplpb.NAMESPACE_OPERATION_UPDATE,
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
	env.metadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{NotificationVersion: 42}, nil)

	require.NoError(t, env.localHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &nsreplpb.ApplyLocalTask{}))

	c := env.read(ref)
	require.Equal(t, nsreplpb.LOCAL_APPLY_OUTCOME_COMMITTED, c.GetLocalApply().GetOutcome())
	require.Equal(t, int64(42), c.GetLocalApply().GetNewVersion())
	// Peers remain and are pending, so the component stays RUNNING for fan-out.
	require.Equal(t, nsreplpb.COMPONENT_STATUS_RUNNING, c.GetStatus())
	require.Len(t, c.GetPeerApply(), 2)
	for _, cell := range []string{"cellB", "cellC"} {
		require.Equal(t, nsreplpb.PEER_APPLY_OUTCOME_PENDING, c.GetPeerApply()[cell].GetOutcome(), cell)
	}
}

// A local commit with no peers (single-cluster global namespace) has nothing to
// fan out to and must complete immediately.
func TestApplyLocalTask_Execute_NoPeersCompletes(t *testing.T) {
	env := newNsreplTestEnv(t)
	ref := env.start(env.mutationUpdate(), nil) // no peer cells

	env.metadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Return(nil)
	env.metadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{NotificationVersion: 3}, nil)

	require.NoError(t, env.localHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &nsreplpb.ApplyLocalTask{}))

	c := env.read(ref)
	require.Equal(t, nsreplpb.LOCAL_APPLY_OUTCOME_COMMITTED, c.GetLocalApply().GetOutcome())
	require.Equal(t, nsreplpb.COMPONENT_STATUS_COMPLETED, c.GetStatus(),
		"a zero-peer mutation has no fan-out and must reach COMPLETED, not linger RUNNING")
}

func TestApplyLocalTask_Execute_CreateCommit(t *testing.T) {
	env := newNsreplTestEnv(t)
	mutation := env.mutationUpdate("cellB")
	mutation.Operation = nsreplpb.NAMESPACE_OPERATION_CREATE
	ref := env.start(mutation, nil)

	env.metadataMgr.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *persistence.CreateNamespaceRequest) (*persistence.CreateNamespaceResponse, error) {
			require.True(t, req.IsGlobalNamespace)
			return &persistence.CreateNamespaceResponse{ID: "ns-id"}, nil
		})
	env.metadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{NotificationVersion: 9}, nil)

	require.NoError(t, env.localHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &nsreplpb.ApplyLocalTask{}))

	c := env.read(ref)
	require.Equal(t, nsreplpb.LOCAL_APPLY_OUTCOME_COMMITTED, c.GetLocalApply().GetOutcome())
	require.Equal(t, int64(9), c.GetLocalApply().GetNewVersion())
}

// A CAS conflict from the store surfaces as a terminal local failure carrying the
// Unavailable (retriable) error class, and NO peer task is scheduled — the "no
// divergence on caller-visible failure" gating invariant.
func TestApplyLocalTask_Execute_CASConflictFailsUnavailable(t *testing.T) {
	env := newNsreplTestEnv(t)
	ref := env.start(env.mutationUpdate("cellB", "cellC"), nil)

	env.metadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Return(
		serviceerror.NewUnavailable("UpdateNamespace operation failed because of conditional failure."))
	// GetMetadata must NOT be read once the CAS write fails.
	env.metadataMgr.EXPECT().GetMetadata(gomock.Any()).Times(0)

	require.NoError(t, env.localHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &nsreplpb.ApplyLocalTask{}))

	c := env.read(ref)
	require.Equal(t, nsreplpb.LOCAL_APPLY_OUTCOME_FAILED, c.GetLocalApply().GetOutcome())
	require.Equal(t, nsreplpb.COMPONENT_STATUS_FAILED, c.GetStatus())
	appInfo := c.GetLocalApply().GetFailure().GetApplicationFailureInfo()
	require.Equal(t, localFailureUnavailable, appInfo.GetType())
	require.False(t, appInfo.GetNonRetryable(), "CAS conflict must be retriable")
	// Gating invariant: peers are never advanced when the local commit fails.
	for _, cell := range []string{"cellB", "cellC"} {
		require.Equal(t, nsreplpb.PEER_APPLY_OUTCOME_PENDING, c.GetPeerApply()[cell].GetOutcome(), cell)
	}
}

// startCommitted seeds a component already past local commit, ready for peer fan-out.
func (env *nsreplTestEnv) startCommitted(peer string) chasm.ComponentRef {
	return env.start(env.mutationUpdate(peer), func(c *NamespaceMutationComponent) {
		c.LocalApply = &nsreplpb.LocalApplyStatus{Outcome: nsreplpb.LOCAL_APPLY_OUTCOME_COMMITTED, NewVersion: 1}
	})
}

func TestApplyPeerTask_Execute_Applied(t *testing.T) {
	env := newNsreplTestEnv(t)
	ref := env.startCommitted("cellB")

	env.clientBean.EXPECT().GetRemoteAdminClient("cellB").Return(env.adminClient, nil)
	env.adminClient.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).Return(
		&adminservice.ApplyNamespaceMutationResponse{Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED}, nil)

	require.NoError(t, env.peerHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &nsreplpb.ApplyPeerTask{TargetCell: "cellB", Attempt: 0}))

	c := env.read(ref)
	require.Equal(t, nsreplpb.PEER_APPLY_OUTCOME_APPLIED, c.GetPeerApply()["cellB"].GetOutcome())
	// Only peer is now terminal -> component completes.
	require.Equal(t, nsreplpb.COMPONENT_STATUS_COMPLETED, c.GetStatus())
}

func TestApplyPeerTask_Execute_NoOpStale(t *testing.T) {
	env := newNsreplTestEnv(t)
	ref := env.startCommitted("cellB")

	env.clientBean.EXPECT().GetRemoteAdminClient("cellB").Return(env.adminClient, nil)
	env.adminClient.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).Return(
		&adminservice.ApplyNamespaceMutationResponse{Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_NO_OP_STALE}, nil)

	require.NoError(t, env.peerHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &nsreplpb.ApplyPeerTask{TargetCell: "cellB", Attempt: 0}))

	c := env.read(ref)
	require.Equal(t, nsreplpb.PEER_APPLY_OUTCOME_NO_OP_STALE, c.GetPeerApply()["cellB"].GetOutcome())
	require.Equal(t, nsreplpb.COMPONENT_STATUS_COMPLETED, c.GetStatus())
}

func TestApplyPeerTask_Execute_TerminalError(t *testing.T) {
	env := newNsreplTestEnv(t)
	ref := env.startCommitted("cellB")

	env.clientBean.EXPECT().GetRemoteAdminClient("cellB").Return(env.adminClient, nil)
	env.adminClient.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).Return(
		nil, serviceerror.NewInvalidArgument("bad payload"))

	require.NoError(t, env.peerHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &nsreplpb.ApplyPeerTask{TargetCell: "cellB", Attempt: 0}))

	c := env.read(ref)
	peer := c.GetPeerApply()["cellB"]
	require.Equal(t, nsreplpb.PEER_APPLY_OUTCOME_FAILED_TERMINAL, peer.GetOutcome())
	require.NotNil(t, peer.GetLastFailure())
	require.Equal(t, nsreplpb.COMPONENT_STATUS_COMPLETED, c.GetStatus())
}

// A retriable peer error keeps the peer PENDING, bumps the attempt, and does not
// complete the component — a later attempt within the retry budget can converge.
func TestApplyPeerTask_Execute_RetriableReschedules(t *testing.T) {
	env := newNsreplTestEnv(t)
	ref := env.startCommitted("cellB")

	env.clientBean.EXPECT().GetRemoteAdminClient("cellB").Return(env.adminClient, nil)
	env.adminClient.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).Return(
		nil, serviceerror.NewUnavailable("peer down"))

	require.NoError(t, env.peerHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &nsreplpb.ApplyPeerTask{TargetCell: "cellB", Attempt: 0}))

	c := env.read(ref)
	peer := c.GetPeerApply()["cellB"]
	require.Equal(t, nsreplpb.PEER_APPLY_OUTCOME_PENDING, peer.GetOutcome(), "retriable failure keeps peer pending")
	require.Equal(t, int32(1), peer.GetAttemptCount())
	require.Equal(t, nsreplpb.COMPONENT_STATUS_RUNNING, c.GetStatus(), "component not complete while a peer is still retrying")
}
