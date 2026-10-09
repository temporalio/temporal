package namespacereplication

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/embedded"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	serverclient "go.temporal.io/server/client"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	nsreplicationcommon "go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/wideevents"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	historytasks "go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
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
		{"shadow-skipped + pending + matching attempt -> run", newComp(namespacereplicationpb.COMPONENT_STATUS_RUNNING, namespacereplicationpb.LOCAL_APPLY_OUTCOME_SKIPPED_SHADOW, pending), 0, true},
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

type nsreplEventCaptureLogger struct {
	embedded.Logger
	records []otellog.Record
}

func (l *nsreplEventCaptureLogger) Emit(_ context.Context, record otellog.Record) {
	l.records = append(l.records, record)
}

func (l *nsreplEventCaptureLogger) Enabled(context.Context, otellog.EnabledParameters) bool {
	return true
}

func newNsreplTestEnv(t *testing.T) *nsreplTestEnv {
	return newNsreplTestEnvWithOptions(t)
}

func newNsreplTestEnvWithOptions(t *testing.T, opts ...chasmtest.EngineOption) *nsreplTestEnv {
	t.Helper()
	logger := log.NewTestLogger()
	ctrl := gomock.NewController(t)
	metadataMgr := persistence.NewMockMetadataManager(ctrl)
	clientBean := serverclient.NewMockBean(ctrl)
	adminClient := adminservicemock.NewMockAdminServiceClient(ctrl)

	localHandler := &applyLocalTaskHandler{
		metadataManager: metadataMgr,
		currentCluster:  "cellA",
		logger:          logger,
	}
	peerHandler := &applyPeerTaskHandler{
		// Exercise the real default transport (admin RPC) over the mocked client
		// bean, so the handler + default applier are covered together.
		peerApplier:    newAdminClientPeerApplier(clientBean),
		currentCluster: "cellA",
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

	engine := chasmtest.NewEngine(t, registry, opts...)
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

func (env *nsreplTestEnv) enableObservability() (*metricstest.Capture, *nsreplEventCaptureLogger) {
	env.t.Helper()
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	env.t.Cleanup(func() { metricsHandler.StopCapture(capture) })
	eventLogger := &nsreplEventCaptureLogger{}
	env.localHandler.metricsHandler = metricsHandler
	env.localHandler.eventLogger = eventLogger
	env.localHandler.emitNamespaceLifecycleEvents = dynamicconfig.GetBoolPropertyFn(true)
	env.peerHandler.metricsHandler = metricsHandler
	env.peerHandler.eventLogger = eventLogger
	env.peerHandler.emitNamespaceLifecycleEvents = dynamicconfig.GetBoolPropertyFn(true)
	return capture, eventLogger
}

func requireAuthoritativeObservation(
	t *testing.T,
	capture *metricstest.Capture,
	eventLogger *nsreplEventCaptureLogger,
	stage string,
	outcome string,
) map[string]any {
	t.Helper()
	requireAuthoritativeMetric(t, capture, stage, outcome)
	return requireAuthoritativeEvent(t, eventLogger, stage, outcome)
}

func requireAuthoritativeEvent(
	t *testing.T,
	eventLogger *nsreplEventCaptureLogger,
	stage string,
	outcome string,
) map[string]any {
	t.Helper()
	matches := authoritativeEventMatches(t, eventLogger, stage, outcome)
	require.Len(t, matches, 1)
	require.Equal(t, "chasm", matches[0]["transport"])
	require.Equal(t, "authoritative", matches[0]["mode"])
	return matches[0]
}

func authoritativeEventMatches(
	t *testing.T,
	eventLogger *nsreplEventCaptureLogger,
	stage string,
	outcome string,
) []map[string]any {
	t.Helper()
	var matches []map[string]any
	for _, record := range eventLogger.records {
		require.Equal(t, wideevents.NamespaceLifecycleEventName, record.EventName())
		attributes := make(map[string]string)
		record.WalkAttributes(func(kv otellog.KeyValue) bool {
			if kv.Value.Kind() == otellog.KindString {
				attributes[kv.Key] = kv.Value.AsString()
			}
			return true
		})
		require.Equal(t, string(wideevents.NamespaceReplicationProcessed), attributes["phase"])
		var details map[string]any
		require.NoError(t, json.Unmarshal([]byte(attributes["details"]), &details))
		if details["apply_stage"] == stage && details["outcome"] == outcome {
			matches = append(matches, details)
		}
	}
	return matches
}

func requireAuthoritativeMetric(
	t *testing.T,
	capture *metricstest.Capture,
	stage string,
	outcome string,
) {
	t.Helper()
	recordings := capture.SnapshotMetric(metrics.NamespaceReplicationCHASMApplyOutcomes.Name())
	require.Len(t, recordings, 1)
	require.Equal(t, stage, recordings[0].Tags["apply_stage"])
	require.Equal(t, outcome, recordings[0].Tags[metrics.OutcomeTag("").Key])
	require.Len(t, capture.SnapshotMetric(metrics.NamespaceReplicationCHASMApplyLatency.Name()), 1)
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
			c := NewNamespaceMutationComponent(m)
			c.initializeVisibility(mctx)
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

func persistenceNormalizedDetail(detail *persistencespb.NamespaceDetail) *persistencespb.NamespaceDetail {
	detail = proto.Clone(detail).(*persistencespb.NamespaceDetail)
	detail.Info.Data = map[string]string{}
	detail.Config.BadBinaries = &namespacepb.BadBinaries{
		Binaries: map[string]*namespacepb.BadBinaryInfo{},
	}
	return detail
}

func TestNamespaceDetailsEqualAfterPersistenceRead(t *testing.T) {
	expected := testDetail()
	expected.ReplicationConfig.ActiveClusterName = ""
	expected.ReplicationConfig.Clusters = nil

	actual := persistenceNormalizedDetail(expected)
	actual.ReplicationConfig.ActiveClusterName = "cellA"
	actual.ReplicationConfig.Clusters = []string{"cellA"}

	require.False(t, proto.Equal(expected, actual), "test must exercise persistence-materialized fields")
	require.True(t, namespaceDetailsEqualAfterPersistenceRead(expected, actual, "cellA"))

	wrongDefault := persistenceNormalizedDetail(expected)
	wrongDefault.ReplicationConfig.ActiveClusterName = "cellB"
	wrongDefault.ReplicationConfig.Clusters = []string{"cellB"}
	require.False(t, namespaceDetailsEqualAfterPersistenceRead(expected, wrongDefault, "cellA"))

	actual.Info.Description = "different persisted state"
	require.False(t, namespaceDetailsEqualAfterPersistenceRead(expected, actual, "cellA"))
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
	metricsCapture, eventLogger := env.enableObservability()
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
	details := requireAuthoritativeObservation(
		t,
		metricsCapture,
		eventLogger,
		nsreplicationcommon.CHASMApplyStageLocal,
		nsreplicationcommon.CHASMApplyOutcomeApplied,
	)
	require.Equal(t, ref.BusinessID, details["component_business_id"])
	require.Equal(t, ref.RunID, details["component_run_id"])
}

func TestApplyLocalTask_Execute_MetadataWriteCommitFailureObserved(t *testing.T) {
	env := newNsreplTestEnv(t)
	metricsCapture, eventLogger := env.enableObservability()
	ref := env.start(env.mutationUpdate("cellB"), nil)
	hooks := testhooks.NewTestHooks()
	env.localHandler.testHooks = hooks
	commitErr := errors.New("chasm commit unavailable")
	cleanup := testhooks.Set(
		hooks,
		testhooks.NamespaceReplicationBeforeLocalCommit,
		func(context.Context) error { return commitErr },
		namespace.Name("ns"),
	)
	t.Cleanup(cleanup)

	env.metadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Return(nil)
	err := env.localHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyLocalTask{})
	require.ErrorIs(t, err, commitErr)
	require.Equal(t, namespacereplicationpb.LOCAL_APPLY_OUTCOME_PENDING, env.read(ref).GetLocalApply().GetOutcome())
	details := requireAuthoritativeObservation(
		t,
		metricsCapture,
		eventLogger,
		nsreplicationcommon.CHASMApplyStageLocal,
		nsreplicationcommon.CHASMApplyOutcomeStateTransitionError,
	)
	require.Equal(t, commitErr.Error(), details["error"])
}

func TestApplyLocalTask_Execute_ShadowSkipsMetadataWrite(t *testing.T) {
	env := newNsreplTestEnv(t)
	metricsCapture, eventLogger := env.enableObservability()
	mutation := env.mutationUpdate("cellB")
	mutation.Shadow = true
	ref := env.start(mutation, nil)

	require.NoError(t, env.localHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyLocalTask{}))

	component := env.read(ref)
	require.Equal(t, namespacereplicationpb.LOCAL_APPLY_OUTCOME_SKIPPED_SHADOW, component.GetLocalApply().GetOutcome())
	require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_PENDING, component.GetPeerApply()["cellB"].GetOutcome())
	require.Empty(t, metricsCapture.SnapshotMetric(metrics.NamespaceReplicationCHASMApplyOutcomes.Name()))
	require.Empty(t, eventLogger.records)
}

func TestApplyLocalTask_Execute_ReplicateOnlySkipsLocalWriteAndSchedulesPeers(t *testing.T) {
	env := newNsreplTestEnv(t)
	metricsCapture, eventLogger := env.enableObservability()
	mutation := env.mutationUpdate("cellB")
	mutation.ReplicateOnly = true
	ref := env.start(mutation, nil)

	// No metadata-manager expectation: replicate-only must not touch the source.
	require.NoError(t, env.localHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyLocalTask{}))

	component := env.read(ref)
	require.Equal(t, namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED, component.GetLocalApply().GetOutcome())
	require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_PENDING, component.GetPeerApply()["cellB"].GetOutcome())
	require.False(t, component.GetMutation().GetShadow(), "peer apply must remain authoritative")
	details := requireAuthoritativeObservation(
		t,
		metricsCapture,
		eventLogger,
		nsreplicationcommon.CHASMApplyStageLocal,
		nsreplicationcommon.CHASMApplyOutcomeNoChange,
	)
	require.Equal(t, true, details["replicate_only"])
}

func TestApplyLocalTask_Execute_ClonesDetailForMetadataManager(t *testing.T) {
	env := newNsreplTestEnv(t)
	mutation := env.mutationUpdate()
	mutation.NamespaceDetail.Info.Description = "component-owned"
	ref := env.start(mutation, nil)

	env.metadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *persistence.UpdateNamespaceRequest) error {
			req.Namespace.Info.Description = "mutated by metadata manager"
			return nil
		})

	require.NoError(t, env.localHandler.Execute(
		env.engineCtx,
		ref,
		chasm.TaskAttributes{},
		&namespacereplicationpb.ApplyLocalTask{},
	))

	component := env.read(ref)
	require.Equal(t, "component-owned", component.GetMutation().GetNamespaceDetail().GetInfo().GetDescription())
}

// A local commit with no peers (single-cluster global namespace) has nothing to
// fan out to and must complete immediately.
func TestApplyLocalTask_Execute_NoPeersCompletes(t *testing.T) {
	env := newNsreplTestEnv(t)
	_, eventLogger := env.enableObservability()
	ref := env.start(env.mutationUpdate(), nil) // no peer cells

	env.metadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Return(nil)
	require.NoError(t, env.localHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyLocalTask{}))

	c := env.read(ref)
	require.Equal(t, namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED, c.GetLocalApply().GetOutcome())
	require.Equal(t, namespacereplicationpb.COMPONENT_STATUS_COMPLETED, c.GetStatus(),
		"a zero-peer mutation has no fan-out and must reach COMPLETED, not linger RUNNING")
	details := requireAuthoritativeEvent(
		t,
		eventLogger,
		nsreplicationcommon.CHASMApplyStageComponent,
		nsreplicationcommon.CHASMApplyOutcomeCompleted,
	)
	require.Equal(t, namespacereplicationpb.COMPONENT_STATUS_COMPLETED.String(), details["component_status"])
	require.InDelta(t, 0, details["peer_count"], 0)
	require.Equal(t, map[string]any{}, details["peer_outcomes"])
	require.Equal(t, map[string]any{}, details["peer_attempt_counts"])
	require.Equal(t, map[string]any{}, details["peer_outcome_counts"])
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
			Namespace:           persistenceNormalizedDetail(mutation.GetNamespaceDetail()),
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
			Namespace:         persistenceNormalizedDetail(mutation.GetNamespaceDetail()),
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

func TestLocalMutationIsCurrentPersistedStateRequiresGlobalNamespace(t *testing.T) {
	env := newNsreplTestEnv(t)
	mutation := env.mutationUpdate("cellB")

	env.metadataMgr.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		ID: mutation.GetNamespaceDetail().GetInfo().GetId(),
	}).Return(&persistence.GetNamespaceResponse{
		Namespace:           persistenceNormalizedDetail(mutation.GetNamespaceDetail()),
		IsGlobalNamespace:   false,
		NotificationVersion: mutation.GetExpectedVersion(),
	}, nil)

	isCurrentPersistedState, err := env.localHandler.localMutationIsCurrentPersistedState(
		env.engineCtx,
		mutation.GetOperation(),
		mutation.GetNamespaceDetail(),
		mutation.GetExpectedVersion(),
	)
	require.NoError(t, err)
	require.False(t, isCurrentPersistedState)
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

// A later metadata version means this UPDATE's CAS slot is gone. That can also
// happen when an earlier attempt committed and a newer same-namespace mutation
// subsequently superseded it. Resolve this component as a retryable failure and
// leave its peers pending; the newer full namespace snapshot owns peer fan-out.
func TestApplyLocalTask_Execute_SupersededUpdateFailsUnavailable(t *testing.T) {
	env := newNsreplTestEnv(t)
	metricsCapture, eventLogger := env.enableObservability()
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
	details := requireAuthoritativeObservation(
		t,
		metricsCapture,
		eventLogger,
		nsreplicationcommon.CHASMApplyStageLocal,
		nsreplicationcommon.CHASMApplyOutcomeTerminalError,
	)
	require.Equal(t, localFailureUnavailable, details["error_type"])
	componentDetails := requireAuthoritativeEvent(
		t,
		eventLogger,
		nsreplicationcommon.CHASMApplyStageComponent,
		nsreplicationcommon.CHASMApplyOutcomeFailed,
	)
	require.Equal(t, namespacereplicationpb.COMPONENT_STATUS_FAILED.String(), componentDetails["component_status"])
	require.Equal(t, namespacereplicationpb.LOCAL_APPLY_OUTCOME_FAILED.String(), componentDetails["local_apply_outcome"])
	require.InDelta(t, 2, componentDetails["peer_count"], 0)
	require.Equal(t, map[string]any{
		namespacereplicationpb.PEER_APPLY_OUTCOME_PENDING.String(): float64(2),
	}, componentDetails["peer_outcome_counts"])
	require.Equal(t, map[string]any{
		"cellB": namespacereplicationpb.PEER_APPLY_OUTCOME_PENDING.String(),
		"cellC": namespacereplicationpb.PEER_APPLY_OUTCOME_PENDING.String(),
	}, componentDetails["peer_outcomes"])
	require.Equal(t, map[string]any{
		"cellB": float64(0),
		"cellC": float64(0),
	}, componentDetails["peer_attempt_counts"])
}

// startCommitted seeds a component already past local commit, ready for peer fan-out.
func (env *nsreplTestEnv) startCommitted(peer string) chasm.ComponentRef {
	return env.start(env.mutationUpdate(peer), func(c *NamespaceMutationComponent) {
		c.LocalApply = &namespacereplicationpb.LocalApplyStatus{Outcome: namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED}
	})
}

func TestApplyPeerTask_Execute_Applied(t *testing.T) {
	env := newNsreplTestEnv(t)
	metricsCapture, eventLogger := env.enableObservability()
	ref := env.startCommitted("cellB")

	env.clientBean.EXPECT().GetRemoteAdminClient("cellB").Return(env.adminClient, nil)
	env.adminClient.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).Return(
		&adminservice.ApplyNamespaceMutationResponse{Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED}, nil)

	require.NoError(t, env.peerHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{Destination: "cellB"}, &namespacereplicationpb.ApplyPeerTask{TargetCell: "cellB", Attempt: 0}))

	c := env.read(ref)
	require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_APPLIED, c.GetPeerApply()["cellB"].GetOutcome())
	// Only peer is now terminal -> component completes.
	require.Equal(t, namespacereplicationpb.COMPONENT_STATUS_COMPLETED, c.GetStatus())
	details := requireAuthoritativeObservation(
		t,
		metricsCapture,
		eventLogger,
		nsreplicationcommon.CHASMApplyStagePeer,
		nsreplicationcommon.CHASMApplyOutcomeApplied,
	)
	require.Equal(t, "cellA", details["source_cluster"])
	require.Equal(t, "cellB", details["target_cluster"])
	require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_APPLIED.String(), details["attempted_peer_outcome"])
	require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_APPLIED.String(), details["persisted_peer_outcome"])
	require.Len(t, metricsCapture.SnapshotMetric(metrics.NamespaceReplicationCHASMPeerPendingLatency.Name()), 1)
	componentDetails := requireAuthoritativeEvent(
		t,
		eventLogger,
		nsreplicationcommon.CHASMApplyStageComponent,
		nsreplicationcommon.CHASMApplyOutcomeCompleted,
	)
	require.InDelta(t, 1, componentDetails["peer_count"], 0)
	require.InDelta(t, 1, componentDetails["peer_attempt_count"], 0)
	require.Equal(t, map[string]any{
		namespacereplicationpb.PEER_APPLY_OUTCOME_APPLIED.String(): float64(1),
	}, componentDetails["peer_outcome_counts"])
	require.Equal(t, map[string]any{
		"cellB": namespacereplicationpb.PEER_APPLY_OUTCOME_APPLIED.String(),
	}, componentDetails["peer_outcomes"])
	require.Equal(t, map[string]any{"cellB": float64(1)}, componentDetails["peer_attempt_counts"])
}

func TestApplyPeerTask_Execute_ComponentEventWaitsForAllPeers(t *testing.T) {
	env := newNsreplTestEnv(t)
	_, eventLogger := env.enableObservability()
	ref := env.start(env.mutationUpdate("cellB", "cellC"), func(c *NamespaceMutationComponent) {
		c.LocalApply = &namespacereplicationpb.LocalApplyStatus{
			Outcome: namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED,
		}
	})

	for _, cell := range []string{"cellB", "cellC"} {
		env.clientBean.EXPECT().GetRemoteAdminClient(cell).Return(env.adminClient, nil)
		env.adminClient.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).Return(
			&adminservice.ApplyNamespaceMutationResponse{
				Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED,
			}, nil)
		require.NoError(t, env.peerHandler.Execute(
			env.engineCtx,
			ref,
			chasm.TaskAttributes{Destination: cell},
			&namespacereplicationpb.ApplyPeerTask{TargetCell: cell},
		))

		component := env.read(ref)
		if cell == "cellB" {
			require.Equal(t, namespacereplicationpb.COMPONENT_STATUS_RUNNING, component.GetStatus())
			require.Empty(t, authoritativeEventMatches(
				t,
				eventLogger,
				nsreplicationcommon.CHASMApplyStageComponent,
				nsreplicationcommon.CHASMApplyOutcomeCompleted,
			))
		} else {
			require.Equal(t, namespacereplicationpb.COMPONENT_STATUS_COMPLETED, component.GetStatus())
		}
	}

	details := requireAuthoritativeEvent(
		t,
		eventLogger,
		nsreplicationcommon.CHASMApplyStageComponent,
		nsreplicationcommon.CHASMApplyOutcomeCompleted,
	)
	require.InDelta(t, 2, details["peer_count"], 0)
	require.Equal(t, map[string]any{
		namespacereplicationpb.PEER_APPLY_OUTCOME_APPLIED.String(): float64(2),
	}, details["peer_outcome_counts"])
	require.Equal(t, map[string]any{
		"cellB": namespacereplicationpb.PEER_APPLY_OUTCOME_APPLIED.String(),
		"cellC": namespacereplicationpb.PEER_APPLY_OUTCOME_APPLIED.String(),
	}, details["peer_outcomes"])
	require.Equal(t, map[string]any{
		"cellB": float64(1),
		"cellC": float64(1),
	}, details["peer_attempt_counts"])
}

func TestApplyPeerTask_Execute_StateTransitionFailureObserved(t *testing.T) {
	env := newNsreplTestEnv(t)
	metricsCapture, eventLogger := env.enableObservability()
	ref := env.startCommitted("cellB")

	env.clientBean.EXPECT().GetRemoteAdminClient("cellB").Return(env.adminClient, nil)
	env.adminClient.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, *adminservice.ApplyNamespaceMutationRequest, ...grpc.CallOption) (*adminservice.ApplyNamespaceMutationResponse, error) {
			_, err := env.engine.UpdateComponent(
				env.engineCtx,
				ref,
				func(_ chasm.MutableContext, component chasm.Component) error {
					component.(*NamespaceMutationComponent).Status = namespacereplicationpb.COMPONENT_STATUS_COMPLETED
					return nil
				},
			)
			require.NoError(t, err)
			return &adminservice.ApplyNamespaceMutationResponse{
				Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED,
			}, nil
		},
	)

	err := env.peerHandler.Execute(
		env.engineCtx,
		ref,
		chasm.TaskAttributes{Destination: "cellB"},
		&namespacereplicationpb.ApplyPeerTask{TargetCell: "cellB"},
	)
	require.Error(t, err)
	details := requireAuthoritativeObservation(
		t,
		metricsCapture,
		eventLogger,
		nsreplicationcommon.CHASMApplyStagePeer,
		nsreplicationcommon.CHASMApplyOutcomeStateTransitionError,
	)
	require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_APPLIED.String(), details["attempted_peer_outcome"])
	require.NotContains(t, details, "persisted_peer_outcome")
	require.Equal(
		t,
		namespacereplicationpb.PEER_APPLY_OUTCOME_PENDING,
		env.read(ref).GetPeerApply()["cellB"].GetOutcome(),
	)
	require.Empty(t, metricsCapture.SnapshotMetric(metrics.NamespaceReplicationCHASMPeerPendingLatency.Name()))
}

func TestApplyPeerTask_Execute_RetryTransitionFailureIsNotReportedAsScheduled(t *testing.T) {
	env := newNsreplTestEnv(t)
	metricsCapture, eventLogger := env.enableObservability()
	ref := env.startCommitted("cellB")

	env.clientBean.EXPECT().GetRemoteAdminClient("cellB").Return(env.adminClient, nil)
	env.adminClient.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, *adminservice.ApplyNamespaceMutationRequest, ...grpc.CallOption) (*adminservice.ApplyNamespaceMutationResponse, error) {
			_, err := env.engine.UpdateComponent(
				env.engineCtx,
				ref,
				func(_ chasm.MutableContext, component chasm.Component) error {
					component.(*NamespaceMutationComponent).Status = namespacereplicationpb.COMPONENT_STATUS_COMPLETED
					return nil
				},
			)
			require.NoError(t, err)
			return nil, serviceerror.NewUnavailable("peer down")
		},
	)

	err := env.peerHandler.Execute(
		env.engineCtx,
		ref,
		chasm.TaskAttributes{Destination: "cellB"},
		&namespacereplicationpb.ApplyPeerTask{TargetCell: "cellB"},
	)
	require.Error(t, err)
	details := requireAuthoritativeObservation(
		t,
		metricsCapture,
		eventLogger,
		nsreplicationcommon.CHASMApplyStagePeer,
		nsreplicationcommon.CHASMApplyOutcomeStateTransitionError,
	)
	require.Equal(t, false, details["retry_scheduled"])
	require.Equal(t, true, details["retry_requested"])
	require.Empty(t, metricsCapture.SnapshotMetric(metrics.NamespaceReplicationCHASMPeerPendingAge.Name()))
	require.Empty(t, metricsCapture.SnapshotMetric(metrics.NamespaceReplicationCHASMPeerPendingThresholdExceeded.Name()))
}

func TestApplyPeerTask_Execute_ShadowOutcome(t *testing.T) {
	for _, tc := range []struct {
		name        string
		wireOutcome adminservice.ApplyNamespaceMutationResponse_Outcome
		wantOutcome namespacereplicationpb.PeerApplyOutcome
	}{
		{
			name:        "match",
			wireOutcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_SHADOW_MATCH,
			wantOutcome: namespacereplicationpb.PEER_APPLY_OUTCOME_SHADOW_MATCH,
		},
		{
			name:        "mismatch",
			wireOutcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_SHADOW_MISMATCH,
			wantOutcome: namespacereplicationpb.PEER_APPLY_OUTCOME_SHADOW_MISMATCH,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newNsreplTestEnv(t)
			metricsCapture, eventLogger := env.enableObservability()
			mutation := env.mutationUpdate("cellB")
			mutation.Shadow = true
			ref := env.start(mutation, func(c *NamespaceMutationComponent) {
				c.LocalApply = &namespacereplicationpb.LocalApplyStatus{Outcome: namespacereplicationpb.LOCAL_APPLY_OUTCOME_SKIPPED_SHADOW}
			})

			env.clientBean.EXPECT().GetRemoteAdminClient("cellB").Return(env.adminClient, nil)
			env.adminClient.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).Return(
				&adminservice.ApplyNamespaceMutationResponse{Outcome: tc.wireOutcome}, nil)

			require.NoError(t, env.peerHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{Destination: "cellB"}, &namespacereplicationpb.ApplyPeerTask{TargetCell: "cellB", Attempt: 0}))

			c := env.read(ref)
			require.Equal(t, tc.wantOutcome, c.GetPeerApply()["cellB"].GetOutcome())
			require.Equal(t, namespacereplicationpb.COMPONENT_STATUS_COMPLETED, c.GetStatus())
			require.Empty(t, metricsCapture.SnapshotMetric(metrics.NamespaceReplicationCHASMApplyOutcomes.Name()))
			require.Empty(t, eventLogger.records)
		})
	}
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
	metricsCapture, eventLogger := env.enableObservability()
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
	requireAuthoritativeObservation(
		t,
		metricsCapture,
		eventLogger,
		nsreplicationcommon.CHASMApplyStagePeer,
		nsreplicationcommon.CHASMApplyOutcomeTerminalError,
	)
	componentDetails := requireAuthoritativeEvent(
		t,
		eventLogger,
		nsreplicationcommon.CHASMApplyStageComponent,
		nsreplicationcommon.CHASMApplyOutcomeCompletedWithFailures,
	)
	require.Equal(t, map[string]any{
		namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_TERMINAL.String(): float64(1),
	}, componentDetails["peer_outcome_counts"])
	require.Equal(t, map[string]any{
		"cellB": namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_TERMINAL.String(),
	}, componentDetails["peer_outcomes"])
	require.Equal(t, map[string]any{"cellB": float64(1)}, componentDetails["peer_attempt_counts"])
}

// A retriable peer error keeps the peer PENDING, bumps the attempt, and does not
// complete the component — a later attempt within the retry budget can converge.
func TestApplyPeerTask_Execute_RetriableReschedules(t *testing.T) {
	env := newNsreplTestEnv(t)
	metricsCapture, eventLogger := env.enableObservability()
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
	requireAuthoritativeMetric(
		t,
		metricsCapture,
		nsreplicationcommon.CHASMApplyStagePeer,
		nsreplicationcommon.CHASMApplyOutcomeRetryableError,
	)
	require.Empty(t, eventLogger.records)
	require.Empty(t, metricsCapture.SnapshotMetric(metrics.NamespaceReplicationCHASMPeerPendingLatency.Name()))
	pendingAge := metricsCapture.SnapshotMetric(metrics.NamespaceReplicationCHASMPeerPendingAge.Name())
	require.Len(t, pendingAge, 1)
	require.Equal(t, time.Duration(0), pendingAge[0].Value)
	require.Empty(t, metricsCapture.SnapshotMetric(metrics.NamespaceReplicationCHASMPeerPendingThresholdExceeded.Name()))
}

func TestApplyPeerTask_Execute_RetryBudgetBoundary(t *testing.T) {
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	testCases := []struct {
		name             string
		elapsed          time.Duration
		wantOutcome      namespacereplicationpb.PeerApplyOutcome
		wantStatus       namespacereplicationpb.ComponentStatus
		wantNewTimerTask int
		wantThreshold    bool
	}{
		{
			name:             "below one-minute pending alert threshold retries",
			elapsed:          time.Minute - time.Nanosecond,
			wantOutcome:      namespacereplicationpb.PEER_APPLY_OUTCOME_PENDING,
			wantStatus:       namespacereplicationpb.COMPONENT_STATUS_RUNNING,
			wantNewTimerTask: 1,
		},
		{
			name:             "at one-minute pending alert threshold retries and emits threshold metric",
			elapsed:          time.Minute,
			wantOutcome:      namespacereplicationpb.PEER_APPLY_OUTCOME_PENDING,
			wantStatus:       namespacereplicationpb.COMPONENT_STATUS_RUNNING,
			wantNewTimerTask: 1,
			wantThreshold:    true,
		},
		{
			name:             "below budget retries",
			elapsed:          peerRetryBudget - time.Nanosecond,
			wantOutcome:      namespacereplicationpb.PEER_APPLY_OUTCOME_PENDING,
			wantStatus:       namespacereplicationpb.COMPONENT_STATUS_RUNNING,
			wantNewTimerTask: 1,
			wantThreshold:    true,
		},
		{
			name:        "at budget is terminal",
			elapsed:     peerRetryBudget,
			wantOutcome: namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_TERMINAL,
			wantStatus:  namespacereplicationpb.COMPONENT_STATUS_COMPLETED,
		},
		{
			name:        "above budget is terminal",
			elapsed:     peerRetryBudget + time.Nanosecond,
			wantOutcome: namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_TERMINAL,
			wantStatus:  namespacereplicationpb.COMPONENT_STATUS_COMPLETED,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			timeSource := clock.NewEventTimeSource().Update(now)
			env := newNsreplTestEnvWithOptions(t, chasmtest.WithTimeSource(timeSource))
			metricsCapture, eventLogger := env.enableObservability()
			ref := env.start(env.mutationUpdate("cellB"), func(c *NamespaceMutationComponent) {
				c.LocalApply.Outcome = namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED
				peer := c.PeerApply["cellB"]
				peer.AttemptCount = 2
				peer.FirstAttemptAt = timestamppb.New(now.Add(-tc.elapsed))
			})

			beforeTasks, err := env.engine.Tasks(ref)
			require.NoError(t, err)
			beforeTimers := len(beforeTasks[historytasks.CategoryTimer])

			env.clientBean.EXPECT().GetRemoteAdminClient("cellB").Return(env.adminClient, nil)
			env.adminClient.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).Return(
				nil, serviceerror.NewUnavailable("peer down"))

			err = env.peerHandler.Execute(
				env.engineCtx,
				ref,
				chasm.TaskAttributes{Destination: "cellB"},
				&namespacereplicationpb.ApplyPeerTask{TargetCell: "cellB", Attempt: 2},
			)
			var destinationDownErr *queueserrors.DestinationDownError
			require.ErrorAs(t, err, &destinationDownErr)

			component := env.read(ref)
			peer := component.GetPeerApply()["cellB"]
			require.Equal(t, tc.wantOutcome, peer.GetOutcome())
			require.Equal(t, int32(3), peer.GetAttemptCount())
			require.Equal(t, tc.wantStatus, component.GetStatus())

			afterTasks, err := env.engine.Tasks(ref)
			require.NoError(t, err)
			require.Equal(t, tc.wantNewTimerTask, len(afterTasks[historytasks.CategoryTimer])-beforeTimers)
			wantMetricOutcome := nsreplicationcommon.CHASMApplyOutcomeRetryableError
			if tc.wantOutcome == namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_TERMINAL {
				wantMetricOutcome = nsreplicationcommon.CHASMApplyOutcomeRetryExhausted
			}
			if tc.wantOutcome == namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_TERMINAL {
				details := requireAuthoritativeObservation(
					t,
					metricsCapture,
					eventLogger,
					nsreplicationcommon.CHASMApplyStagePeer,
					wantMetricOutcome,
				)
				require.Equal(t, true, details["retry_exhausted"])
				require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_RETRIABLE.String(), details["attempted_peer_outcome"])
				require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_TERMINAL.String(), details["persisted_peer_outcome"])
			} else {
				requireAuthoritativeMetric(
					t,
					metricsCapture,
					nsreplicationcommon.CHASMApplyStagePeer,
					wantMetricOutcome,
				)
				require.Empty(t, eventLogger.records)
			}
			pendingLatency := metricsCapture.SnapshotMetric(metrics.NamespaceReplicationCHASMPeerPendingLatency.Name())
			pendingAge := metricsCapture.SnapshotMetric(metrics.NamespaceReplicationCHASMPeerPendingAge.Name())
			thresholdExceeded := metricsCapture.SnapshotMetric(metrics.NamespaceReplicationCHASMPeerPendingThresholdExceeded.Name())
			if tc.wantOutcome == namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_TERMINAL {
				require.Len(t, pendingLatency, 1)
				require.Empty(t, pendingAge)
				require.Empty(t, thresholdExceeded)
			} else {
				require.Empty(t, pendingLatency)
				require.Len(t, pendingAge, 1)
				require.Equal(t, max(tc.elapsed, 0), pendingAge[0].Value)
				if tc.wantThreshold {
					require.Len(t, thresholdExceeded, 1)
				} else {
					require.Empty(t, thresholdExceeded)
				}
			}
		})
	}
}

// -----------------------------------------------------------------------------
// PeerApplier transport seam.
// -----------------------------------------------------------------------------

func TestPeerApplyResultFromOutcome(t *testing.T) {
	tests := []struct {
		name    string
		shadow  bool
		outcome adminservice.ApplyNamespaceMutationResponse_Outcome
		want    PeerApplyResult
		wantErr bool
	}{
		{
			name:    "shadow match",
			shadow:  true,
			outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_SHADOW_MATCH,
			want:    PeerApplyResultShadowMatch,
		},
		{
			name:    "shadow mismatch",
			shadow:  true,
			outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_SHADOW_MISMATCH,
			want:    PeerApplyResultShadowMismatch,
		},
		{
			name:    "shadow rejects applied",
			shadow:  true,
			outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED,
			wantErr: true,
		},
		{
			name:    "shadow rejects created",
			shadow:  true,
			outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_CREATED,
			wantErr: true,
		},
		{
			name:    "shadow rejects duplicate",
			shadow:  true,
			outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_DUPLICATE,
			wantErr: true,
		},
		{
			name:    "shadow rejects no-op stale",
			shadow:  true,
			outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_NO_OP_STALE,
			wantErr: true,
		},
		{
			name:    "shadow rejects not admitted",
			shadow:  true,
			outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_NOT_ADMITTED,
			wantErr: true,
		},
		{
			name:    "shadow rejects unspecified",
			shadow:  true,
			outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_UNSPECIFIED,
			wantErr: true,
		},
		{
			name:    "authoritative applied",
			outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED,
			want:    PeerApplyResultApplied,
		},
		{
			name:    "authoritative created",
			outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_CREATED,
			want:    PeerApplyResultApplied,
		},
		{
			name:    "authoritative duplicate",
			outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_DUPLICATE,
			want:    PeerApplyResultApplied,
		},
		{
			name:    "authoritative no-op stale",
			outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_NO_OP_STALE,
			want:    PeerApplyResultNoOpStale,
		},
		{
			name:    "authoritative not admitted",
			outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_NOT_ADMITTED,
			want:    PeerApplyResultNotAdmitted,
		},
		{
			name:    "authoritative rejects shadow match",
			outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_SHADOW_MATCH,
			wantErr: true,
		},
		{
			name:    "authoritative rejects shadow mismatch",
			outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_SHADOW_MISMATCH,
			wantErr: true,
		},
		{
			name:    "authoritative rejects unspecified",
			outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_UNSPECIFIED,
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := peerApplyResultFromOutcome("cellB", test.shadow, test.outcome)
			if test.wantErr {
				require.Error(t, err)
				require.Equal(t, PeerApplyResultUnspecified, result)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, result)
		})
	}
}

// TestAdminClientPeerApplier_Apply covers request construction, local
// preprocessing, and transport error propagation. Outcome mapping is tested
// exhaustively by TestPeerApplyResultFromOutcome.
func TestAdminClientPeerApplier_Apply(t *testing.T) {
	detail := testDetail()
	request := func(operation enumsspb.NamespaceOperation, shadow bool) PeerApplyRequest {
		return PeerApplyRequest{
			SourceCluster:       "cellA",
			TargetCluster:       "cellB",
			ComponentBusinessID: "namespace-id:mutation-id",
			ComponentRunID:      "run-id",
			AttemptCount:        2,
			Operation:           operation,
			Detail:              detail,
			Shadow:              shadow,
		}
	}
	newApplier := func(t *testing.T) (*serverclient.MockBean, *adminservicemock.MockAdminServiceClient, PeerApplier) {
		ctrl := gomock.NewController(t)
		bean := serverclient.NewMockBean(ctrl)
		admin := adminservicemock.NewMockAdminServiceClient(ctrl)
		return bean, admin, newAdminClientPeerApplier(bean)
	}

	for _, tc := range []struct {
		name        string
		wireOutcome adminservice.ApplyNamespaceMutationResponse_Outcome
		wantResult  PeerApplyResult
	}{
		{
			name:        "shadow match outcome",
			wireOutcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_SHADOW_MATCH,
			wantResult:  PeerApplyResultShadowMatch,
		},
		{
			name:        "shadow mismatch outcome",
			wireOutcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_SHADOW_MISMATCH,
			wantResult:  PeerApplyResultShadowMismatch,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bean, admin, applier := newApplier(t)
			bean.EXPECT().GetRemoteAdminClient("cellB").Return(admin, nil)
			admin.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, request *adminservice.ApplyNamespaceMutationRequest, _ ...grpc.CallOption) (*adminservice.ApplyNamespaceMutationResponse, error) {
					require.True(t, request.GetShadow())
					payload := request.GetNamespaceTaskPayload()
					require.NotEmpty(t, payload)
					require.Equal(t, nsreplicationcommon.NamespaceTaskFingerprintFromPayload(payload), request.GetFingerprint())
					payloadTask := &replicationspb.NamespaceTaskAttributes{}
					require.NoError(t, proto.Unmarshal(payload, payloadTask))
					require.True(t, proto.Equal(payloadTask, request.GetNamespaceTask()))
					require.Equal(t, "cellA", request.GetSourceCluster())
					require.Equal(t, "namespace-id:mutation-id", request.GetComponentBusinessId())
					require.Equal(t, "run-id", request.GetComponentRunId())
					require.Equal(t, int32(2), request.GetAttemptCount())
					return &adminservice.ApplyNamespaceMutationResponse{Outcome: tc.wireOutcome}, nil
				})
			res, err := applier.Apply(context.Background(), request(enumsspb.NAMESPACE_OPERATION_UPDATE, true))
			require.NoError(t, err)
			require.Equal(t, tc.wantResult, res)
		})
	}
	for _, tc := range []struct {
		name        string
		shadow      bool
		wireOutcome adminservice.ApplyNamespaceMutationResponse_Outcome
	}{
		{
			name:        "shadow request rejects apply outcome",
			shadow:      true,
			wireOutcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED,
		},
		{
			name:        "authoritative request rejects shadow outcome",
			wireOutcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_SHADOW_MATCH,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bean, admin, applier := newApplier(t)
			bean.EXPECT().GetRemoteAdminClient("cellB").Return(admin, nil)
			admin.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).Return(
				&adminservice.ApplyNamespaceMutationResponse{Outcome: tc.wireOutcome}, nil)

			result, err := applier.Apply(
				context.Background(),
				request(enumsspb.NAMESPACE_OPERATION_UPDATE, tc.shadow),
			)
			require.Error(t, err)
			require.Equal(t, PeerApplyResultUnspecified, result)
		})
	}
	t.Run("created maps to applied", func(t *testing.T) {
		bean, admin, applier := newApplier(t)
		bean.EXPECT().GetRemoteAdminClient("cellB").Return(admin, nil)
		admin.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).Return(
			&adminservice.ApplyNamespaceMutationResponse{Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_CREATED}, nil)
		res, err := applier.Apply(context.Background(), request(enumsspb.NAMESPACE_OPERATION_CREATE, false))
		require.NoError(t, err)
		require.Equal(t, PeerApplyResultApplied, res)
	})
	t.Run("no-op-stale outcome", func(t *testing.T) {
		bean, admin, applier := newApplier(t)
		bean.EXPECT().GetRemoteAdminClient("cellB").Return(admin, nil)
		admin.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).Return(
			&adminservice.ApplyNamespaceMutationResponse{Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_NO_OP_STALE}, nil)
		res, err := applier.Apply(context.Background(), request(enumsspb.NAMESPACE_OPERATION_UPDATE, false))
		require.NoError(t, err)
		require.Equal(t, PeerApplyResultNoOpStale, res)
	})
	t.Run("duplicate maps to applied", func(t *testing.T) {
		bean, admin, applier := newApplier(t)
		bean.EXPECT().GetRemoteAdminClient("cellB").Return(admin, nil)
		admin.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).Return(
			&adminservice.ApplyNamespaceMutationResponse{Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_DUPLICATE}, nil)
		res, err := applier.Apply(context.Background(), request(enumsspb.NAMESPACE_OPERATION_CREATE, false))
		require.NoError(t, err)
		require.Equal(t, PeerApplyResultApplied, res)
	})
	t.Run("not-admitted is its own terminal result, not applied", func(t *testing.T) {
		bean, admin, applier := newApplier(t)
		bean.EXPECT().GetRemoteAdminClient("cellB").Return(admin, nil)
		admin.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).Return(
			&adminservice.ApplyNamespaceMutationResponse{Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_NOT_ADMITTED}, nil)
		res, err := applier.Apply(context.Background(), request(enumsspb.NAMESPACE_OPERATION_UPDATE, false))
		require.NoError(t, err)
		require.Equal(t, PeerApplyResultNotAdmitted, res)
	})
	t.Run("unspecified/unknown outcome surfaced as error, not phantom applied", func(t *testing.T) {
		bean, admin, applier := newApplier(t)
		bean.EXPECT().GetRemoteAdminClient("cellB").Return(admin, nil)
		admin.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).Return(
			&adminservice.ApplyNamespaceMutationResponse{Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_UNSPECIFIED}, nil)
		_, err := applier.Apply(context.Background(), request(enumsspb.NAMESPACE_OPERATION_UPDATE, true))
		require.Error(t, err)
	})
	t.Run("fingerprint error is terminal and does not resolve remote client", func(t *testing.T) {
		_, _, applier := newApplier(t)
		invalidDetail := proto.Clone(detail).(*persistencespb.NamespaceDetail)
		invalidDetail.Info.Name = string([]byte{0xff})
		invalidRequest := request(enumsspb.NAMESPACE_OPERATION_UPDATE, true)
		invalidRequest.Detail = invalidDetail

		result, err := applier.Apply(context.Background(), invalidRequest)
		var invalidArgument *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgument)
		require.Equal(t, PeerApplyResultUnspecified, result)
		require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_TERMINAL, classifyPeerErr(err))
		require.False(t, isPeerDestinationDown(err))
	})
	t.Run("rpc error propagates", func(t *testing.T) {
		bean, admin, applier := newApplier(t)
		bean.EXPECT().GetRemoteAdminClient("cellB").Return(admin, nil)
		admin.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewUnavailable("down"))
		_, err := applier.Apply(context.Background(), request(enumsspb.NAMESPACE_OPERATION_UPDATE, true))
		require.Error(t, err)
	})
	t.Run("dial error propagates", func(t *testing.T) {
		bean, _, applier := newApplier(t)
		bean.EXPECT().GetRemoteAdminClient("cellB").Return(nil, serviceerror.NewUnavailable("no route"))
		_, err := applier.Apply(context.Background(), request(enumsspb.NAMESPACE_OPERATION_UPDATE, true))
		require.Error(t, err)
	})
}

// mockPeerApplier is a stand-in transport used to prove the handler delegates the
// peer write to the injected PeerApplier — the seam a deployment overrides.
type mockPeerApplier struct {
	result       PeerApplyResult
	err          error
	cells        []string
	requests     []PeerApplyRequest
	mutateDetail func(*persistencespb.NamespaceDetail)
}

func (m *mockPeerApplier) Apply(_ context.Context, request PeerApplyRequest) (PeerApplyResult, error) {
	m.cells = append(m.cells, request.TargetCluster)
	m.requests = append(m.requests, request)
	if m.mutateDetail != nil {
		m.mutateDetail(request.Detail)
	}
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
		currentCluster: "cellA",
		metricsHandler: metrics.NoopMetricsHandler,
		logger:         log.NewTestLogger(),
	}

	require.NoError(t, handler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyPeerTask{TargetCell: "cellB", Attempt: 0}))

	require.Equal(t, []string{"cellB"}, applier.cells, "handler must delegate the peer transport to the injected applier")
	require.Equal(t, "cellA", applier.requests[0].SourceCluster)
	require.Equal(t, ref.BusinessID, applier.requests[0].ComponentBusinessID)
	require.Equal(t, ref.RunID, applier.requests[0].ComponentRunID)
	require.Equal(t, int32(1), applier.requests[0].AttemptCount)
	c := env.read(ref)
	require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_NO_OP_STALE, c.GetPeerApply()["cellB"].GetOutcome())
	require.Equal(t, namespacereplicationpb.COMPONENT_STATUS_COMPLETED, c.GetStatus())
}

func TestApplyPeerTask_Execute_ClonesDetailForInjectedApplier(t *testing.T) {
	env := newNsreplTestEnv(t)
	mutation := env.mutationUpdate("cellB")
	mutation.NamespaceDetail.Info.Description = "component-owned"
	ref := env.start(mutation, func(c *NamespaceMutationComponent) {
		c.LocalApply.Outcome = namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED
	})

	applier := &mockPeerApplier{
		result: PeerApplyResultApplied,
		mutateDetail: func(detail *persistencespb.NamespaceDetail) {
			detail.Info.Description = "mutated by peer applier"
		},
	}
	handler := &applyPeerTaskHandler{
		peerApplier:    applier,
		currentCluster: "cellA",
		metricsHandler: metrics.NoopMetricsHandler,
		logger:         log.NewTestLogger(),
	}

	require.NoError(t, handler.Execute(
		env.engineCtx,
		ref,
		chasm.TaskAttributes{},
		&namespacereplicationpb.ApplyPeerTask{TargetCell: "cellB", Attempt: 0},
	))

	component := env.read(ref)
	require.Equal(t, "component-owned", component.GetMutation().GetNamespaceDetail().GetInfo().GetDescription())
}

func TestApplyPeerTask_Execute_UnknownInjectedResultRetries(t *testing.T) {
	env := newNsreplTestEnv(t)
	ref := env.startCommitted("cellB")

	applier := &mockPeerApplier{result: PeerApplyResult(999)}
	handler := &applyPeerTaskHandler{
		peerApplier:    applier,
		currentCluster: "cellA",
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
