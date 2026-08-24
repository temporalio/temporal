package roundtrip

// Round-trip differential replication test.
//
// This sits under tests/ndc, but it is not a cluster test: it needs no onebox, no gRPC, and
// no replication stream. Two in-process clusters are assembled directly out of the real
// components, and the artifact is handed from producer to consumer. It runs in about a second.
//
// Files in this package:
//
//	framework_test.go   the fixture, the active driver, artifact production, passive apply
//	capture_test.go     how passive tasks are captured
//	diff_test.go        normalization, the allowlist, failure reporting
//	cases_*_test.go     one file per use case; cases_common_test.go holds shared step lists
//
// Rather than hand-write the expected task list, this framework derives the expectation from
// the active cluster:
//
//  1. drive an ACTIVE cluster's mutable state through the real active-side APIs
//     (AddWorkflowExecutionStartedEvent, AddActivityTaskScheduledEvent, ...) and close the
//     transaction under TransactionPolicyActive, capturing the tasks it generated;
//  2. produce a real replication artifact from that mutable state via the real
//     replication.SyncStateRetriever -- the same component the active side's raw task
//     converter calls;
//  3. apply it to a PASSIVE cluster through the real ndc.ReplicateVersionedTransition -- the
//     same entry point ExecutableSyncVersionedTransitionTask calls;
//  4. diff the transfer and timer tasks the two sides produced.
//
// Both clusters are real: each has its own in-memory sqlite ExecutionManager, its own cluster
// metadata reporting its own current-cluster name, and its own shard. History events are
// genuinely written on the active side, shipped in the artifact's EventBatches, and appended
// on the passive side, so event backfill runs for real.
//
// What is deliberately absent is everything between the producer and the consumer: the
// bidirectional stream, task scheduling and retries, the ack manager, and the replication
// progress cache. The active side does generate SyncVersionedTransitionTask replication tasks
// and nothing consumes them. One consequence worth knowing: production takes the sync target
// from the progress cache, which can lag; this test always uses the passive cluster's true
// position, so every artifact is the tightest possible delta.
//
// The two sides do NOT generate identical tasks, by design. Replication tasks are active-only
// (asserted in mutable_state_impl.go closeTransactionPrepareReplicationTasks); activity and
// user timer tasks come from closeTransactionHandleActivityUserTimerTasks on the active side
// but from CreateNextActivityTimer/CreateNextUserTimer in the task refresher on the passive
// side; visibility tasks differ in multiplicity. Those divergences live in an explicit,
// citation-carrying allowlist in diff_test.go, and anything outside it fails -- so
// the allowlist is the reviewable spec of where active and passive legitimately differ.
//
// Nothing in the ndc package is stubbed or reached into: the passive side runs its real
// transaction manager, and tasks are captured by wrapping the passive cluster's
// ExecutionManager (see capture_test.go). The diff therefore compares what the
// passive cluster actually stored.

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	workflowservicepb "go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/ndc"
	"go.temporal.io/server/service/history/replication"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	rtTaskQueue    = "roundtrip-test-tq"
	rtWorkflowType = "roundtrip-test-wf-type"
	rtShardID      = int32(1)
	// rtActivityRetries is how many failed attempts the retry case drives; the policy must
	// allow at least one more so every failure returns RETRY_STATE_IN_PROGRESS.
	rtActivityRetries     = 10
	rtActivityMaxAttempts = rtActivityRetries + 5
	// rtFailoverVersion must map to cluster.TestCurrentClusterName under the test cluster
	// metadata (initial versions 1 and 2, increment 10), otherwise the active side's
	// closeTransactionWithPolicyCheck rejects every close. Asserted in SetupTest.
	rtFailoverVersion = cluster.TestCurrentClusterInitialFailoverVersion
)

var rtWorkflowTaskCompletionLimits = historyi.WorkflowTaskCompletionLimits{
	MaxResetPoints:              primitives.DefaultHistoryMaxAutoResetPoints,
	MaxSearchAttributeValueSize: 2048,
}

type (
	// rtCluster is one side of the round trip: a real shard over a real in-memory sqlite
	// ExecutionManager, with cluster metadata reporting this cluster as the current one.
	rtCluster struct {
		name        string
		shard       *shard.StubContext
		execMgr     persistence.ExecutionManager
		wfCache     wcache.Cache
		eventsCache events.Cache
		blobCache   persistence.XDCCache
		// captured collects the tasks written through this cluster's ExecutionManager.
		captured *rtCapturedTasks
	}

	// rtPassivePos is where the passive cluster has caught up to. It becomes the
	// "target" the source diffs against when building the next artifact, exactly as the
	// replication progress cache does in production.
	rtPassivePos struct {
		versionedTransition *persistencespb.VersionedTransition
		versionHistoryItems [][]*historyspb.VersionHistoryItem
	}

	// rtStep is one active-side transaction.
	rtStep struct {
		name string
		fn   func(s *rtSuite, ms *workflow.MutableStateImpl) error
		// allow holds divergences this step alone legitimately causes; global rules live
		// in rtGlobalAllowlist.
		allow []rtAllowRule
		// allowNoTasks opts out of the anti-vacuity check for steps that genuinely
		// generate no transfer or timer task on either side.
		allowNoTasks bool
		// requireActive / forbidActive assert on the ACTIVE side's task types for this step,
		// as normalized Go type names ("*tasks.UserTimerTask").
		//
		// The diff itself only proves the two sides agree; it cannot notice both sides
		// losing a task together. Use these where a case exists to pin a specific behavior
		// and "neither side produced it" would be a silent regression rather than a pass.
		requireActive []string
		forbidActive  []string
	}

	rtCase struct {
		name  string
		steps []rtStep
	}

	rtSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller
		logger     log.Logger
		config     *configs.Config
		nsEntry    *namespace.Namespace

		active  *rtCluster
		passive *rtCluster

		workflowID string
		runID      string

		// Event IDs threaded between steps: activities, timers and child workflows are all
		// scheduled off a completed workflow task's event ID.
		lastCompletedWorkflowTaskID int64
		scheduledActivityID         int64
		// scheduledActivityIDs maps an activity ID to its scheduled event ID, for cases that
		// juggle several activities at once.
		scheduledActivityIDs map[string]int64

		// firedRules tracks which allowlist rules were used, so the suite can report
		// rules that have gone stale.
		firedRules map[string]int
	}
)

func TestReplicationRoundTripSuite(t *testing.T) {
	suite.Run(t, new(rtSuite))
}

func (s *rtSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.logger = log.NewTestLogger()
	s.config = tests.NewDynamicConfig()
	s.firedRules = make(map[string]int)
	s.scheduledActivityIDs = make(map[string]int64)

	s.workflowID = "roundtrip-wf-" + uuid.NewString()
	s.runID = uuid.NewString()

	s.nsEntry = rtNamespaceEntry()

	s.active = s.newCluster(cluster.TestCurrentClusterName)
	s.passive = s.newCluster(cluster.TestAlternativeClusterName)

	// If the namespace's failover version does not map to the active cluster,
	// closeTransactionWithPolicyCheck rejects every active close deep inside the first
	// step. Fail here instead, where the cause is obvious.
	s.Equal(
		cluster.TestCurrentClusterName,
		s.active.shard.GetClusterMetadata().ClusterNameForFailoverVersion(true, rtFailoverVersion),
		"namespace failover version must map to the active cluster",
	)
}

func (s *rtSuite) TearDownTest() {
	s.reportUnusedAllowRules()
	s.active.shard.StopForTest()
	s.passive.shard.StopForTest()
	s.controller.Finish()
}

// rtNamespaceEntry is a global namespace active in TestCurrentClusterName. It is built
// here rather than reused from tests.GlobalNamespaceEntry because that one's failover
// version (1234) does not map to either test cluster.
func rtNamespaceEntry() *namespace.Namespace {
	return namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: tests.NamespaceID.String(), Name: tests.Namespace.String()},
		&persistencespb.NamespaceConfig{
			Retention:   timestamp.DurationFromDays(1),
			BadBinaries: &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{}},
		},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		rtFailoverVersion,
	)
}

// newCluster builds one side: its own sqlite DB, ExecutionManager, cluster metadata and shard.
func (s *rtSuite) newCluster(clusterName string) *rtCluster {
	captured := newRtCapturedTasks()
	execMgr := newRtCapturingExecutionManager(s.newExecutionManager(clusterName), captured)

	// The two shards must disagree about which cluster they are, so each gets its own
	// metadata differing only in currentClusterName.
	clusterMetadata := cluster.NewMetadata(
		true, // enableGlobalNamespace
		cluster.TestFailoverVersionIncrement,
		cluster.TestCurrentClusterName, // master
		clusterName,                    // current
		cluster.TestAllClusterInfo,
		nil, // no cluster metadata store; nothing here refreshes
		nil,
		s.logger,
	)

	eventsCache := events.NewHostLevelEventsCache(execMgr, s.config, metrics.NoopMetricsHandler, s.logger, false)

	mockEngine := historyi.NewMockEngine(s.controller)
	mockEngine.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	mockEngine.EXPECT().NotifyNewHistoryEvent(gomock.Any()).AnyTimes()

	shardContext := shard.NewStubContext(
		s.controller,
		shard.ContextConfigOverrides{
			ShardInfo:        &persistencespb.ShardInfo{ShardId: rtShardID, RangeId: 1},
			Config:           s.config,
			ClusterMetadata:  clusterMetadata,
			ExecutionManager: execMgr,
		},
		mockEngine,
	)

	reg := hsm.NewRegistry()
	s.NoError(workflow.RegisterStateMachine(reg))
	shardContext.SetStateMachineRegistry(reg)
	shardContext.SetEventsCacheForTesting(eventsCache)

	shardContext.Resource.NamespaceCache.EXPECT().
		GetNamespaceByID(tests.NamespaceID).Return(s.nsEntry, nil).AnyTimes()
	shardContext.Resource.NamespaceCache.EXPECT().
		GetNamespace(tests.Namespace).Return(s.nsEntry, nil).AnyTimes()
	shardContext.Resource.NamespaceCache.EXPECT().
		GetNamespaceName(tests.NamespaceID).Return(tests.Namespace, nil).AnyTimes()

	return &rtCluster{
		name:        clusterName,
		shard:       shardContext,
		execMgr:     execMgr,
		wfCache:     wcache.NewHostLevelCache(s.config, s.logger, metrics.NoopMetricsHandler),
		eventsCache: eventsCache,
		blobCache:   persistence.NewEventsBlobCache(1024*1024, time.Minute, s.logger),
		captured:    captured,
	}
}

// newExecutionManager builds a real ExecutionManager over an in-process sqlite database.
// The schema is embedded and applied automatically for mode=memory. The factory must stay
// open for the lifetime of the test: closing the last connection drops the database.
func (s *rtSuite) newExecutionManager(clusterName string) persistence.ExecutionManager {
	cfg := config.SQL{
		PluginName:        "sqlite",
		DatabaseName:      uuid.NewString(),
		ConnectAttributes: map[string]string{"mode": "memory", "cache": "private"},
	}
	factory := sql.NewFactory(
		cfg,
		resolver.NewNoopResolver(),
		clusterName,
		s.logger,
		metrics.NoopMetricsHandler,
		serialization.NewSerializer(),
	)
	s.T().Cleanup(factory.Close)

	// Execution writes take a range lock on the shard row, so it has to exist first.
	shardStore, err := factory.NewShardStore()
	s.NoError(err)
	_, err = persistence.NewShardManager(shardStore, serialization.NewSerializer()).
		GetOrCreateShard(context.Background(), &persistence.GetOrCreateShardRequest{
			ShardID: rtShardID,
			InitialShardInfo: &persistencespb.ShardInfo{
				ShardId: rtShardID,
				RangeId: 1,
			},
		})
	s.NoError(err)

	store, err := factory.NewExecutionStore()
	s.NoError(err)

	return persistence.NewExecutionManager(
		store,
		serialization.NewSerializer(),
		nil,
		s.logger,
		dynamicconfig.GetIntPropertyFn(4*1024*1024),
		dynamicconfig.GetBoolPropertyFn(false),
	)
}

// ---------------------------------------------------------------------------
// the round trip
// ---------------------------------------------------------------------------

func (s *rtSuite) runCase(tc rtCase) {
	s.Run(tc.name, func() {
		ctx := context.Background()

		activeMS := workflow.NewMutableState(
			s.active.shard,
			s.active.eventsCache,
			s.logger,
			s.nsEntry,
			s.workflowID,
			s.runID,
			time.Now().UTC(),
		)
		s.NoError(activeMS.UpdateCurrentVersion(rtFailoverVersion, false))
		// Production does this in the start-workflow API; without it the mutable state has
		// no history branch token and the first event append hits a NOT NULL tree_id.
		s.NoError(activeMS.SetHistoryTree(nil, nil, s.runID))

		var pos rtPassivePos
		firstStep := true

		for i, step := range tc.steps {
			s.Run(step.name, func() {
				// 1. active: mutate and close the transaction.
				s.NoError(step.fn(s, activeMS))
				activeTasks := s.closeActiveTransaction(ctx, activeMS, firstStep)

				// 2. build the artifact the source cluster would ship.
				artifact := s.buildArtifact(ctx, activeMS, pos, firstStep)
				if !firstStep {
					// From step 2 on this must be an incremental mutation. If transition
					// history were disabled every step would silently produce a snapshot
					// and the framework would stop testing incremental replication while
					// still passing.
					s.NotNil(artifact.GetSyncWorkflowStateMutationAttributes(),
						"step %d %q: expected a mutation artifact, got a snapshot", i, step.name)
				}

				// 3. passive: apply it through the real replication stack.
				passiveTasks := s.applyArtifact(ctx, artifact)

				// 4. diff.
				s.diffTasks(step, activeTasks, passiveTasks)

				pos = s.passivePosition(ctx)
				firstStep = false
			})
		}
	})
}

// Note: deliberately no SetupSubTest/TearDownSubTest. A case's steps run as nested s.Run
// calls and must share one pair of clusters -- re-running setup between steps would throw
// away the state the previous step replicated.

// closeActiveTransaction closes the active transaction, captures the tasks it produced and
// persists both the events and the mutable state, so the source cluster can later read the
// events back when building EventBatches.
func (s *rtSuite) closeActiveTransaction(
	ctx context.Context,
	ms *workflow.MutableStateImpl,
	isCreate bool,
) map[tasks.Category][]tasks.Task {
	if isCreate {
		snapshot, eventsSeq, err := ms.CloseTransactionAsSnapshot(ctx, historyi.TransactionPolicyActive)
		s.NoError(err)
		captured := rtCopyTasks(snapshot.Tasks)
		s.persistActiveEvents(ctx, eventsSeq)
		return captured
	}

	mutation, eventsSeq, err := ms.CloseTransactionAsMutation(ctx, historyi.TransactionPolicyActive)
	s.NoError(err)
	// cleanupTransaction has already run; the returned map is the only surviving copy.
	captured := rtCopyTasks(mutation.Tasks)
	s.persistActiveEvents(ctx, eventsSeq)
	return captured
}

func (s *rtSuite) persistActiveEvents(ctx context.Context, eventsSeq []*persistence.WorkflowEvents) {
	for _, wfEvents := range eventsSeq {
		if len(wfEvents.Events) == 0 {
			continue
		}
		_, err := s.active.shard.AppendHistoryEvents(ctx, &persistence.AppendHistoryNodesRequest{
			ShardID:           s.active.shard.GetShardID(),
			IsNewBranch:       wfEvents.Events[0].GetEventId() == 1,
			BranchToken:       wfEvents.BranchToken,
			Events:            wfEvents.Events,
			PrevTransactionID: wfEvents.PrevTxnID,
			TransactionID:     wfEvents.TxnID,
			Info: persistence.BuildHistoryGarbageCleanupInfo(
				wfEvents.NamespaceID, wfEvents.WorkflowID, wfEvents.RunID,
			),
		},
			namespace.ID(wfEvents.NamespaceID),
			&commonpb.WorkflowExecution{WorkflowId: wfEvents.WorkflowID, RunId: wfEvents.RunID},
		)
		s.NoError(err)
	}
}

// buildArtifact produces the artifact the source cluster would ship for the delta between
// the passive cluster's position and the active cluster's current state.
func (s *rtSuite) buildArtifact(
	ctx context.Context,
	activeMS *workflow.MutableStateImpl,
	pos rtPassivePos,
	isFirstSync bool,
) *replicationspb.VersionedTransitionArtifact {
	retriever := replication.NewSyncStateRetriever(
		s.active.shard,
		s.active.wfCache,
		// Only GetSyncWorkflowStateArtifact dereferences the consistency checker; the
		// from-mutable-state entry points used below do not.
		nil,
		s.active.blobCache,
		s.logger,
	)

	execution := &commonpb.WorkflowExecution{WorkflowId: s.workflowID, RunId: s.runID}
	// We never took a cache lock on the active side, so releasing is a no-op.
	releaseFn := func(error) {}

	var result *replication.SyncStateResult
	var err error
	if isFirstSync {
		// The new-workflow variant is what production uses for a first sync, and it is the
		// one that ships history from event 1. The general variant with a nil target
		// position deliberately omits events and expects the target to fetch them from the
		// source cluster -- which in this test would mean the remote admin client.
		result, err = retriever.GetSyncWorkflowStateArtifactFromMutableStateForNewWorkflow(
			ctx, tests.NamespaceID.String(), execution, activeMS, releaseFn,
			transitionhistory.LastVersionedTransition(activeMS.GetExecutionInfo().TransitionHistory),
		)
	} else {
		result, err = retriever.GetSyncWorkflowStateArtifactFromMutableState(
			ctx, tests.NamespaceID.String(), execution, activeMS,
			pos.versionedTransition, pos.versionHistoryItems, releaseFn,
		)
	}
	s.NoError(err)
	s.NotNil(result)
	return result.VersionedTransitionArtifact
}

// applyArtifact runs the real passive-side replication and returns the tasks it produced.
//
// Nothing here is stubbed: the replicator uses its own transaction manager, which persists
// through the passive cluster's ExecutionManager. The capture happens at that boundary (see
// rtCapturingExecutionManager), so what the diff compares is what the passive cluster
// actually *stored*, not merely what the refresh computed before someone drained it.
func (s *rtSuite) applyArtifact(
	ctx context.Context,
	artifact *replicationspb.VersionedTransitionArtifact,
) map[tasks.Category][]tasks.Task {
	replicator := ndc.NewWorkflowStateReplicator(
		s.passive.shard,
		s.passive.wfCache,
		ndc.NewMockEventsReapplier(s.controller),
		serialization.NewSerializer(),
		quotas.NoopRequestRateLimiter,
		s.logger,
		nil,
	)

	s.passive.captured.reset()
	err := replicator.ReplicateVersionedTransition(
		ctx, chasm.WorkflowArchetypeID, artifact, s.active.name,
	)
	s.NoError(err)
	return s.passive.captured.drain()
}

// passivePosition reads back where the passive cluster now stands, which becomes the target
// for the next artifact.
func (s *rtSuite) passivePosition(ctx context.Context) rtPassivePos {
	wfCtx, release, err := s.passive.wfCache.GetOrCreateChasmExecution(
		ctx,
		s.passive.shard,
		tests.NamespaceID,
		&commonpb.WorkflowExecution{WorkflowId: s.workflowID, RunId: s.runID},
		chasm.WorkflowArchetypeID,
		locks.PriorityHigh,
	)
	s.NoError(err)
	defer release(nil)

	ms, err := wfCtx.LoadMutableState(ctx, s.passive.shard)
	s.NoError(err)

	executionInfo := ms.GetExecutionInfo()
	var items [][]*historyspb.VersionHistoryItem
	for _, history := range executionInfo.VersionHistories.GetHistories() {
		items = append(items, history.Items)
	}
	return rtPassivePos{
		versionedTransition: transitionhistory.LastVersionedTransition(executionInfo.TransitionHistory),
		versionHistoryItems: items,
	}
}

// ---------------------------------------------------------------------------
// active-side step helpers
// ---------------------------------------------------------------------------

func rtStartWorkflow(s *rtSuite, ms *workflow.MutableStateImpl) error {
	return rtStartWorkflowWith(s, ms, 0, enumspb.CONTINUE_AS_NEW_INITIATOR_UNSPECIFIED)
}

// rtStartWorkflowWith starts the workflow with a first-workflow-task backoff, which makes
// the start emit a WorkflowBackoffTimerTask instead of dispatching a workflow task right
// away. The initiator selects the backoff type that GenerateDelayedWorkflowTasks stamps on
// the task: unspecified means a delayed start, RETRY means a workflow retry, and
// CRON_SCHEDULE means a cron firing.
func rtStartWorkflowWith(
	s *rtSuite,
	ms *workflow.MutableStateImpl,
	firstWorkflowTaskBackoff time.Duration,
	initiator enumspb.ContinueAsNewInitiator,
) error {
	request := &historyservice.StartWorkflowExecutionRequest{
		Attempt:                1,
		NamespaceId:            tests.NamespaceID.String(),
		ContinueAsNewInitiator: initiator,
		StartRequest: &workflowservicepb.StartWorkflowExecutionRequest{
			Namespace:                tests.Namespace.String(),
			WorkflowId:               s.workflowID,
			WorkflowType:             &commonpb.WorkflowType{Name: rtWorkflowType},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: rtTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			WorkflowExecutionTimeout: durationpb.New(time.Hour),
			WorkflowRunTimeout:       durationpb.New(time.Hour),
			WorkflowTaskTimeout:      durationpb.New(10 * time.Second),
			Identity:                 "roundtrip-test",
		},
	}
	if firstWorkflowTaskBackoff > 0 {
		request.FirstWorkflowTaskBackoff = durationpb.New(firstWorkflowTaskBackoff)
	}
	if initiator == enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE {
		request.StartRequest.CronSchedule = "*/1 * * * *"
	}

	startEvent, err := ms.AddWorkflowExecutionStartedEvent(
		&commonpb.WorkflowExecution{WorkflowId: s.workflowID, RunId: s.runID},
		request,
	)
	if err != nil {
		return err
	}

	// Production starts a workflow and settles its first workflow task in one transaction,
	// via AddFirstWorkflowTaskScheduled: that is the branch which either emits a
	// WorkflowBackoffTimerTask (backoff set) or schedules the first workflow task (backoff
	// zero). Calling AddWorkflowTaskScheduledEvent directly instead would skip the branch
	// entirely and the backoff task would never be generated on the active side.
	_, err = ms.AddFirstWorkflowTaskScheduled(nil, startEvent, false)
	return err
}

func rtScheduleWorkflowTask(s *rtSuite, ms *workflow.MutableStateImpl) error {
	_, err := ms.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	return err
}

func rtStartWorkflowTask(s *rtSuite, ms *workflow.MutableStateImpl) error {
	workflowTask := ms.GetPendingWorkflowTask()
	if workflowTask == nil {
		return serviceerror.NewInternal("no pending workflow task to start")
	}
	_, _, err := ms.AddWorkflowTaskStartedEvent(
		workflowTask.ScheduledEventID,
		uuid.NewString(),
		&taskqueuepb.TaskQueue{Name: rtTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		"roundtrip-test",
		nil, nil, nil, false, nil, 0,
	)
	return err
}

// rtCompleteWorkflowTask completes the started workflow task. Activities, timers and child
// workflows are all scheduled off a completed workflow task's event ID, so most steps below
// need this to have run first.
// rtFailWorkflowTask fails the started workflow task, as RespondWorkflowTaskFailed does.
// This increments the workflow task attempt counter but does not reschedule: the caller
// schedules the retry, which is why the retry cases pair this with rtScheduleWorkflowTask.
func rtFailWorkflowTask(s *rtSuite, ms *workflow.MutableStateImpl) error {
	workflowTask := ms.GetStartedWorkflowTask()
	if workflowTask == nil {
		return serviceerror.NewInternal("no started workflow task to fail")
	}
	if _, err := ms.AddWorkflowTaskFailedEvent(
		workflowTask,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
		&failurepb.Failure{Message: "roundtrip-workflow-task-failure"},
		"roundtrip-test",
		nil, "", "", "", 0,
	); err != nil {
		return err
	}
	ms.FlushBufferedEvents()
	return nil
}

// rtTimeoutWorkflowTask times the started workflow task out, as the timer queue executor
// does when its start-to-close timer fires. Like a failure this increments the attempt
// counter and leaves rescheduling to the caller.
func rtTimeoutWorkflowTask(s *rtSuite, ms *workflow.MutableStateImpl) error {
	workflowTask := ms.GetStartedWorkflowTask()
	if workflowTask == nil {
		return serviceerror.NewInternal("no started workflow task to time out")
	}
	if _, err := ms.AddWorkflowTaskTimedOutEvent(workflowTask); err != nil {
		return err
	}
	ms.FlushBufferedEvents()
	return nil
}

func rtCompleteWorkflowTask(s *rtSuite, ms *workflow.MutableStateImpl) error {
	workflowTask := ms.GetStartedWorkflowTask()
	if workflowTask == nil {
		return serviceerror.NewInternal("no started workflow task to complete")
	}
	_, err := ms.AddWorkflowTaskCompletedEvent(
		workflowTask,
		&workflowservicepb.RespondWorkflowTaskCompletedRequest{Identity: "roundtrip-test"},
		rtWorkflowTaskCompletionLimits,
	)
	if err != nil {
		return err
	}
	ms.FlushBufferedEvents()
	s.lastCompletedWorkflowTaskID = ms.GetNextEventID() - 1
	return nil
}

func rtScheduleActivity(s *rtSuite, ms *workflow.MutableStateImpl) error {
	_, activityInfo, err := ms.AddActivityTaskScheduledEvent(
		s.lastCompletedWorkflowTaskID,
		&commandpb.ScheduleActivityTaskCommandAttributes{
			ActivityId:             "roundtrip-activity",
			ActivityType:           &commonpb.ActivityType{Name: "roundtrip-activity-type"},
			TaskQueue:              &taskqueuepb.TaskQueue{Name: rtTaskQueue},
			ScheduleToCloseTimeout: durationpb.New(20 * time.Minute),
			ScheduleToStartTimeout: durationpb.New(10 * time.Minute),
			StartToCloseTimeout:    durationpb.New(10 * time.Minute),
			HeartbeatTimeout:       durationpb.New(0),
		},
		false,
	)
	if err != nil {
		return err
	}
	s.scheduledActivityID = activityInfo.ScheduledEventId
	return nil
}

func rtStartActivity(s *rtSuite, ms *workflow.MutableStateImpl) error {
	activityInfo, ok := ms.GetActivityInfo(s.scheduledActivityID)
	if !ok {
		return serviceerror.NewInternal("no scheduled activity to start")
	}
	_, err := ms.AddActivityTaskStartedEvent(
		activityInfo, s.scheduledActivityID, uuid.NewString(), "roundtrip-test",
		nil, nil, nil, "", nil,
	)
	return err
}

func rtCompleteActivity(s *rtSuite, ms *workflow.MutableStateImpl) error {
	// Read the started event ID here rather than caching it at start time. Activities get a
	// default retry policy, so the start is transient: StartedEventId is TransientEventID
	// until the transaction closes and materializes the real started event.
	activityInfo, ok := ms.GetActivityInfo(s.scheduledActivityID)
	if !ok {
		return serviceerror.NewInternal("no activity to complete")
	}
	_, err := ms.AddActivityTaskCompletedEvent(
		s.scheduledActivityID,
		activityInfo.StartedEventId,
		&workflowservicepb.RespondActivityTaskCompletedRequest{Identity: "roundtrip-test"},
	)
	return err
}

// rtScheduleRetryableActivity schedules an activity with a retry policy generous enough to
// survive the retry loop below. Intervals are kept tiny so the backoff deadlines stay well
// inside the workflow's run timeout, which would otherwise suppress the retry timer.
func rtScheduleRetryableActivity(s *rtSuite, ms *workflow.MutableStateImpl) error {
	_, activityInfo, err := ms.AddActivityTaskScheduledEvent(
		s.lastCompletedWorkflowTaskID,
		&commandpb.ScheduleActivityTaskCommandAttributes{
			ActivityId:             "roundtrip-retry-activity",
			ActivityType:           &commonpb.ActivityType{Name: "roundtrip-activity-type"},
			TaskQueue:              &taskqueuepb.TaskQueue{Name: rtTaskQueue},
			ScheduleToCloseTimeout: durationpb.New(30 * time.Minute),
			ScheduleToStartTimeout: durationpb.New(10 * time.Minute),
			StartToCloseTimeout:    durationpb.New(10 * time.Minute),
			HeartbeatTimeout:       durationpb.New(0),
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(time.Second),
				BackoffCoefficient: 1, // flat, so attempt 10's deadline is still well inside the run timeout
				MaximumInterval:    durationpb.New(time.Second),
				MaximumAttempts:    rtActivityMaxAttempts,
			},
		},
		false,
	)
	if err != nil {
		return err
	}
	s.scheduledActivityID = activityInfo.ScheduledEventId
	return nil
}

// rtFailActivityWithRetry fails the in-flight attempt in a way that schedules another one.
// No ActivityTaskFailed event is written while retries remain: the attempt counter moves and
// an ActivityRetryTimerTask carries the activity back to the task queue.
func rtFailActivityWithRetry(s *rtSuite, ms *workflow.MutableStateImpl) error {
	activityInfo, ok := ms.GetActivityInfo(s.scheduledActivityID)
	if !ok {
		return serviceerror.NewInternal("no activity to fail")
	}
	retryState, err := ms.RetryActivity(activityInfo, &failurepb.Failure{
		Message: "roundtrip-retryable-failure",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
				Type:         "roundtrip-retryable",
				NonRetryable: false,
			},
		},
	})
	if err != nil {
		return err
	}
	if retryState != enumspb.RETRY_STATE_IN_PROGRESS {
		return serviceerror.NewInternalf(
			"expected the activity to keep retrying, got retry state %v", retryState)
	}
	return nil
}

// rtActivityTimeouts is the timeout configuration for one activity. A zero duration means
// the timeout is not configured, so that timer never applies.
type rtActivityTimeouts struct {
	scheduleToStart time.Duration
	scheduleToClose time.Duration
	startToClose    time.Duration
	heartbeat       time.Duration
}

// rtScheduleActivityNamed schedules an activity under an explicit ID with explicit timeouts,
// so a case can run several at once with deliberately different deadlines. maxAttempts of 1
// means no retry policy at all, which also makes the activity's start non-transient.
func rtScheduleActivityNamed(
	activityID string,
	timeouts rtActivityTimeouts,
	maxAttempts int32,
) func(*rtSuite, *workflow.MutableStateImpl) error {
	return func(s *rtSuite, ms *workflow.MutableStateImpl) error {
		attributes := &commandpb.ScheduleActivityTaskCommandAttributes{
			ActivityId:             activityID,
			ActivityType:           &commonpb.ActivityType{Name: "roundtrip-activity-type"},
			TaskQueue:              &taskqueuepb.TaskQueue{Name: rtTaskQueue},
			ScheduleToStartTimeout: durationpb.New(timeouts.scheduleToStart),
			ScheduleToCloseTimeout: durationpb.New(timeouts.scheduleToClose),
			StartToCloseTimeout:    durationpb.New(timeouts.startToClose),
			HeartbeatTimeout:       durationpb.New(timeouts.heartbeat),
		}
		if maxAttempts != 1 {
			attributes.RetryPolicy = &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(time.Second),
				BackoffCoefficient: 1,
				MaximumInterval:    durationpb.New(time.Second),
				MaximumAttempts:    maxAttempts,
			}
		}

		_, activityInfo, err := ms.AddActivityTaskScheduledEvent(
			s.lastCompletedWorkflowTaskID, attributes, false,
		)
		if err != nil {
			return err
		}
		s.scheduledActivityIDs[activityID] = activityInfo.ScheduledEventId
		return nil
	}
}

func rtStartActivityNamed(activityID string) func(*rtSuite, *workflow.MutableStateImpl) error {
	return func(s *rtSuite, ms *workflow.MutableStateImpl) error {
		scheduledEventID, ok := s.scheduledActivityIDs[activityID]
		if !ok {
			return serviceerror.NewInternalf("activity %q was never scheduled", activityID)
		}
		activityInfo, ok := ms.GetActivityInfo(scheduledEventID)
		if !ok {
			return serviceerror.NewInternalf("no activity info for %q", activityID)
		}
		_, err := ms.AddActivityTaskStartedEvent(
			activityInfo, scheduledEventID, uuid.NewString(), "roundtrip-test",
			nil, nil, nil, "", nil,
		)
		return err
	}
}

func rtCompleteActivityNamed(activityID string) func(*rtSuite, *workflow.MutableStateImpl) error {
	return func(s *rtSuite, ms *workflow.MutableStateImpl) error {
		scheduledEventID, ok := s.scheduledActivityIDs[activityID]
		if !ok {
			return serviceerror.NewInternalf("activity %q was never scheduled", activityID)
		}
		activityInfo, ok := ms.GetActivityInfo(scheduledEventID)
		if !ok {
			return serviceerror.NewInternalf("no activity info for %q", activityID)
		}
		_, err := ms.AddActivityTaskCompletedEvent(
			scheduledEventID, activityInfo.StartedEventId,
			&workflowservicepb.RespondActivityTaskCompletedRequest{Identity: "roundtrip-test"},
		)
		return err
	}
}

// rtHeartbeatActivityNamed records a heartbeat, which updates the activity without moving
// any of its timer deadlines when no heartbeat timeout is configured: StartedTime and the
// timeouts are untouched, and the heartbeat timer does not apply at all.
//
// That makes it the step that exercises getActivityTimerTaskStatus's carryover. The activity
// owning the live timer task is the one being replicated, and its mask has to survive the
// apply or the passive cluster generates a duplicate timeout task for a deadline that never
// moved.
func rtHeartbeatActivityNamed(activityID string) func(*rtSuite, *workflow.MutableStateImpl) error {
	return func(s *rtSuite, ms *workflow.MutableStateImpl) error {
		scheduledEventID, ok := s.scheduledActivityIDs[activityID]
		if !ok {
			return serviceerror.NewInternalf("activity %q was never scheduled", activityID)
		}
		activityInfo, ok := ms.GetActivityInfo(scheduledEventID)
		if !ok {
			return serviceerror.NewInternalf("no activity info for %q", activityID)
		}
		ms.UpdateActivityProgress(activityInfo, &workflowservicepb.RecordActivityTaskHeartbeatRequest{
			Identity: "roundtrip-test",
		})
		return nil
	}
}

// rtTimeoutActivityNamed times an activity out the way the timer queue executor does: hand
// RetryActivity a timeout failure and act on the retry state it returns.
//
// The timeout type decides the outcome. START_TO_CLOSE and HEARTBEAT go through the normal
// retry check, so the activity retries while attempts remain. SCHEDULE_TO_START and
// SCHEDULE_TO_CLOSE are server-enforced deadlines rather than execution failures, so
// RetryActivity short-circuits to RETRY_STATE_TIMEOUT and the activity closes instead.
func rtTimeoutActivityNamed(
	activityID string,
	timeoutType enumspb.TimeoutType,
) func(*rtSuite, *workflow.MutableStateImpl) error {
	return func(s *rtSuite, ms *workflow.MutableStateImpl) error {
		scheduledEventID, ok := s.scheduledActivityIDs[activityID]
		if !ok {
			return serviceerror.NewInternalf("activity %q was never scheduled", activityID)
		}
		activityInfo, ok := ms.GetActivityInfo(scheduledEventID)
		if !ok {
			return serviceerror.NewInternalf("no activity info for %q", activityID)
		}
		startedEventID := activityInfo.StartedEventId

		timeoutFailure := &failurepb.Failure{
			Message: "activity timeout",
			FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
				TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{TimeoutType: timeoutType},
			},
		}

		retryState, err := ms.RetryActivity(activityInfo, timeoutFailure)
		if err != nil {
			return err
		}
		if retryState == enumspb.RETRY_STATE_IN_PROGRESS {
			// Retrying: no timed-out event is written, the attempt counter moves and an
			// ActivityRetryTimerTask carries the activity back to the task queue.
			return nil
		}
		_, err = ms.AddActivityTaskTimedOutEvent(
			scheduledEventID, startedEventID, timeoutFailure, retryState,
		)
		return err
	}
}

func rtStartTimer(s *rtSuite, ms *workflow.MutableStateImpl) error {
	return rtStartTimerNamed("roundtrip-timer", 5*time.Minute)(s, ms)
}

// rtStartTimerNamed starts a user timer with an explicit ID and duration, so a case can set
// up several timers with a known firing order.
func rtStartTimerNamed(
	timerID string,
	startToFire time.Duration,
) func(*rtSuite, *workflow.MutableStateImpl) error {
	return func(s *rtSuite, ms *workflow.MutableStateImpl) error {
		_, _, err := ms.AddTimerStartedEvent(
			s.lastCompletedWorkflowTaskID,
			&commandpb.StartTimerCommandAttributes{
				TimerId:            timerID,
				StartToFireTimeout: durationpb.New(startToFire),
			},
		)
		return err
	}
}

// rtFireTimer fires a user timer, which is what the timer queue executor does when the
// timer task comes due. It also schedules a workflow task, as the executor does, so the
// workflow can react to the fired timer.
func rtFireTimer(timerID string) func(*rtSuite, *workflow.MutableStateImpl) error {
	return func(s *rtSuite, ms *workflow.MutableStateImpl) error {
		if _, err := ms.AddTimerFiredEvent(timerID); err != nil {
			return err
		}
		_, err := ms.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
		return err
	}
}

func rtStartChildWorkflow(s *rtSuite, ms *workflow.MutableStateImpl) error {
	_, _, err := ms.AddStartChildWorkflowExecutionInitiatedEvent(
		s.lastCompletedWorkflowTaskID,
		&commandpb.StartChildWorkflowExecutionCommandAttributes{
			Namespace:    tests.Namespace.String(),
			WorkflowId:   "roundtrip-child-wf",
			WorkflowType: &commonpb.WorkflowType{Name: "roundtrip-child-type"},
			TaskQueue:    &taskqueuepb.TaskQueue{Name: rtTaskQueue},
		},
		tests.NamespaceID,
	)
	return err
}

func rtCompleteWorkflow(s *rtSuite, ms *workflow.MutableStateImpl) error {
	_, err := ms.AddCompletedWorkflowEvent(
		s.lastCompletedWorkflowTaskID,
		&commandpb.CompleteWorkflowExecutionCommandAttributes{},
		"",
	)
	return err
}

// ---------------------------------------------------------------------------
// small utilities
// ---------------------------------------------------------------------------

func rtCopyTasks(in map[tasks.Category][]tasks.Task) map[tasks.Category][]tasks.Task {
	out := make(map[tasks.Category][]tasks.Task, len(in))
	for category, categoryTasks := range in {
		out[category] = append([]tasks.Task(nil), categoryTasks...)
	}
	return out
}

var _ = workflowpb.WorkflowExecutionInfo{}
