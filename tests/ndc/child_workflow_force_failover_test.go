package ndc

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/tests/testcore"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ChildWorkflowForceFailoverSuite reproduces the "orphaned child after force failover" bug.
//
// Real cluster = cluster-a (initialFailoverVersion 1). We inject, via replication into the
// passive cluster-a:
//   - a child workflow created by the OLD active (cluster-b, version 2), whose parent pointer
//     (parentInitiatedEventId=5, parentInitiatedEventVersion=2) is NOT on the parent's winning branch.
//   - a parent workflow whose winning/current branch (cluster-c, version 3) ends with a pending
//     StartChildWorkflowExecutionInitiated (event 5) for the SAME child workflow id.
//
// We then force-failover the namespace to cluster-a (making the real cluster active) and
// RefreshWorkflowTasks on the parent. This regenerates the parent's StartChildExecution transfer
// task, which the active executor processes: startWorkflow finds the child already exists and
// currently records START_CHILD_WORKFLOW_EXECUTION_FAILED(WORKFLOW_ALREADY_EXISTS), orphaning the child.
type ChildWorkflowForceFailoverSuite struct {
	*require.Assertions
	protorequire.ProtoAssertions
	suite.Suite

	testClusterFactory testcore.TestClusterFactory
	controller         *gomock.Controller
	cluster            *testcore.TestCluster
	serializer         serialization.Serializer
	logger             log.Logger
	mockAdminClient    map[string]adminservice.AdminServiceClient

	namespace   namespace.Name
	namespaceID namespace.ID
}

const (
	oldActiveVersion = int64(2) // cluster-b: where the child was created (losing branch).
	winningVersion   = int64(3) // cluster-c: parent's winning branch (higher version wins).
)

func TestChildWorkflowForceFailoverSuite(t *testing.T) {
	suite.Run(t, new(ChildWorkflowForceFailoverSuite))
}

func (s *ChildWorkflowForceFailoverSuite) SetupSuite() {
	s.logger = log.NewTestLogger()
	s.serializer = serialization.NewSerializer()
	s.testClusterFactory = testcore.NewTestClusterFactory()

	clusterConfigs := clustersConfig("cluster-a", "cluster-b", "cluster-c")
	clusterConfigs[0].WorkerConfig = testcore.WorkerConfig{DisableWorker: true}
	// Replacing an orphaned child suppresses an existing workflow, so it ships opt-in.
	clusterConfigs[0].DynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.EnableOrphanedChildWorkflowReplacement.Key(): true,
	}

	s.controller = gomock.NewController(s.T())
	mockStreamClient := adminservicemock.NewMockAdminService_StreamWorkflowReplicationMessagesClient(s.controller)
	mockStreamClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
	mockStreamClient.EXPECT().Recv().Return(&adminservice.StreamWorkflowReplicationMessagesResponse{
		Attributes: &adminservice.StreamWorkflowReplicationMessagesResponse_Messages{
			Messages: &replicationspb.WorkflowReplicationMessages{
				ReplicationTasks:           []*replicationspb.ReplicationTask{},
				ExclusiveHighWatermark:     100,
				ExclusiveHighWatermarkTime: timestamppb.New(time.Unix(0, 100)),
			},
		},
	}, nil).AnyTimes()
	mockStreamClient.EXPECT().CloseSend().Return(nil).AnyTimes()

	mockRemoteClient := adminservicemock.NewMockAdminServiceClient(s.controller)
	mockRemoteClient.EXPECT().GetReplicationMessages(gomock.Any(), gomock.Any()).Return(
		&adminservice.GetReplicationMessagesResponse{
			ShardMessages: make(map[int32]*replicationspb.ReplicationMessages),
		}, nil).AnyTimes()
	mockRemoteClient.EXPECT().StreamWorkflowReplicationMessages(gomock.Any()).Return(mockStreamClient, nil).AnyTimes()
	mockOtherClient := adminservicemock.NewMockAdminServiceClient(s.controller)
	mockOtherClient.EXPECT().GetReplicationMessages(gomock.Any(), gomock.Any()).Return(
		&adminservice.GetReplicationMessagesResponse{
			ShardMessages: make(map[int32]*replicationspb.ReplicationMessages),
		}, nil).AnyTimes()
	mockOtherClient.EXPECT().StreamWorkflowReplicationMessages(gomock.Any()).Return(mockStreamClient, nil).AnyTimes()
	s.mockAdminClient = map[string]adminservice.AdminServiceClient{
		"cluster-b": mockRemoteClient,
		"cluster-c": mockOtherClient,
	}
	clusterConfigs[0].MockAdminClient = s.mockAdminClient

	cluster, err := s.testClusterFactory.NewCluster(s.T(), clusterConfigs[0], log.With(s.logger, tag.ClusterName(clusterName[0])))
	s.Require().NoError(err)
	s.cluster = cluster
}

func (s *ChildWorkflowForceFailoverSuite) TearDownSuite() {
	s.controller.Finish()
	s.NoError(s.cluster.TearDownCluster())
}

func (s *ChildWorkflowForceFailoverSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.ProtoAssertions = protorequire.New(s.T())
	// Fresh global namespace per test, initially active on cluster-b so the real cluster-a is passive
	// and accepts replicated events. Each test then force-fails-over to cluster-a.
	s.registerNamespace()
}

func (s *ChildWorkflowForceFailoverSuite) registerNamespace() {
	s.namespace = namespace.Name("child-ff-ndc-" + common.GenerateRandomString(5))
	frontend := s.cluster.FrontendClient()
	_, err := frontend.RegisterNamespace(s.newContext(), &workflowservice.RegisterNamespaceRequest{
		Namespace:                        s.namespace.String(),
		IsGlobalNamespace:                true,
		Clusters:                         clusterReplicationConfig,
		ActiveClusterName:                clusterName[1], // cluster-b: real cluster-a is passive.
		WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
	})
	s.Require().NoError(err)
	time.Sleep(2 * testcore.NamespaceCacheRefreshInterval) //nolint:forbidigo

	resp, err := frontend.DescribeNamespace(s.newContext(), &workflowservice.DescribeNamespaceRequest{Namespace: s.namespace.String()})
	s.Require().NoError(err)
	s.namespaceID = namespace.ID(resp.GetNamespaceInfo().GetId())
	s.logger.Info("Registered namespace", tag.WorkflowNamespace(s.namespace.String()), tag.WorkflowNamespaceID(s.namespaceID.String()))
}

func (s *ChildWorkflowForceFailoverSuite) newContext() context.Context {
	ctx := testcore.NewContext()
	return headers.SetCallerInfo(ctx, headers.NewCallerInfo(s.namespace.String(), headers.CallerTypeAPI, ""))
}

// TestOrphanChild_Case1_ChildStartedOnly reproduces Case 1: the orphaned child has only
// WorkflowExecutionStarted (its first workflow task not yet scheduled).
func (s *ChildWorkflowForceFailoverSuite) TestOrphanChild_Case1_ChildStartedOnly() {
	parentID, parentRunID := s.newParent()
	childID, childRunID, taskqueue := s.newChild()

	childEvents := []*historypb.HistoryEvent{s.childStartedEvent(parentID, parentRunID, childRunID, taskqueue)}
	childVH := []*historyspb.VersionHistoryItem{{EventId: 1, Version: oldActiveVersion}}

	s.plantOrphanAndFailover(parentID, parentRunID, childID, childRunID, childEvents, childVH, enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE)
	s.assertChildAdopted(parentID, parentRunID, childID, childRunID)
}

// TestOrphanChild_Case2_ChildWorkflowTaskScheduled reproduces Case 2: the orphaned child has
// WorkflowExecutionStarted + WorkflowTaskScheduled (its first workflow task scheduled but not run).
func (s *ChildWorkflowForceFailoverSuite) TestOrphanChild_Case2_ChildWorkflowTaskScheduled() {
	parentID, parentRunID := s.newParent()
	childID, childRunID, taskqueue := s.newChild()

	childEvents := []*historypb.HistoryEvent{
		s.childStartedEvent(parentID, parentRunID, childRunID, taskqueue),
		{
			EventId:   2,
			Version:   oldActiveVersion,
			EventTime: timestamppb.New(time.Now().UTC()),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskqueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				StartToCloseTimeout: durationpb.New(10 * time.Second),
				Attempt:             1,
			}},
		},
	}
	childVH := []*historyspb.VersionHistoryItem{{EventId: 2, Version: oldActiveVersion}}

	s.plantOrphanAndFailover(parentID, parentRunID, childID, childRunID, childEvents, childVH, enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE)
	s.assertChildAdopted(parentID, parentRunID, childID, childRunID)
}

// TestOrphanChild_Case3_ChildClosed covers a CLOSED orphaned child. Adopting its completed result is
// the correct end state but is a proposal only (see child_workflow_force_failover_case3_proposal.md).
// Until then the fix must at least be SAFE: it must NOT replace a closed child (that
// would re-run its activities/side effects) — it falls back to StartChildExecutionFailed and leaves the
// completed child untouched.
func (s *ChildWorkflowForceFailoverSuite) TestOrphanChild_Case3_ChildClosed() {
	parentID, parentRunID := s.newParent()
	childID, childRunID, taskqueue := s.newChild()

	childEvents := s.closedChildEvents(parentID, parentRunID, childRunID, taskqueue)
	childVH := []*historyspb.VersionHistoryItem{{EventId: int64(len(childEvents)), Version: oldActiveVersion}}

	// REJECT_DUPLICATE so the closed child yields WorkflowExecutionAlreadyStarted (rather than silently
	// starting a fresh run under ALLOW_DUPLICATE, which reuse policy explicitly permits).
	s.plantOrphanAndFailover(parentID, parentRunID, childID, childRunID, childEvents, childVH, enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE)

	startFailed, cause, childStarted := s.waitForChildStartOutcome(parentID, parentRunID)
	s.True(startFailed, "closed child must fall back to StartChildExecutionFailed, not be re-run")
	s.Equal(enumspb.START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_EXISTS, cause)
	s.False(childStarted)

	// The closed child must be untouched: the current run is still the original, completed run.
	desc, err := s.cluster.FrontendClient().DescribeWorkflowExecution(s.newContext(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.namespace.String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: childID}, // no run id => current run
	})
	s.NoError(err)
	s.Equal(childRunID, desc.GetWorkflowExecutionInfo().GetExecution().GetRunId(), "closed child must not be re-run into a new run")
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, desc.GetWorkflowExecutionInfo().GetStatus())
}

// TestOrphanChild_ExecutedChild_Zombified covers an orphaned child that has already completed a
// workflow task and scheduled an activity. The old run is suppressed rather than terminated before
// the replacement run is created.
func (s *ChildWorkflowForceFailoverSuite) TestOrphanChild_ExecutedChild_Zombified() {
	parentID, parentRunID := s.newParent()
	childID, childRunID, taskqueue := s.newChild()

	childEvents := s.executedChildEvents(parentID, parentRunID, childRunID, taskqueue)
	childVH := []*historyspb.VersionHistoryItem{{EventId: int64(len(childEvents)), Version: oldActiveVersion}}

	s.plantOrphanAndFailover(parentID, parentRunID, childID, childRunID, childEvents, childVH, enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE)
	s.assertChildAdopted(parentID, parentRunID, childID, childRunID)
}

// TestOrphanChild_OperatorStartedSameId verifies that atomic ownership validation rejects an
// unrelated workflow holding the child workflow ID.
func (s *ChildWorkflowForceFailoverSuite) TestOrphanChild_OperatorStartedSameId() {
	parentID, parentRunID := s.newParent()
	childID, operatorRunID, taskqueue := s.newChild()

	// A standalone (parentless) running workflow occupying the child workflow id.
	operatorEvents := []*historypb.HistoryEvent{
		{
			EventId:   1,
			Version:   oldActiveVersion,
			EventTime: timestamppb.New(time.Now().UTC()),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
				WorkflowType:        &commonpb.WorkflowType{Name: "operator-workflow-type"},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskqueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				WorkflowRunTimeout:  durationpb.New(1000 * time.Second),
				WorkflowTaskTimeout: durationpb.New(10 * time.Second),
				// no ParentWorkflowExecution => not a child of anyone
				OriginalExecutionRunId: operatorRunID,
				FirstExecutionRunId:    operatorRunID,
				Attempt:                1,
			}},
		},
	}
	operatorVH := []*historyspb.VersionHistoryItem{{EventId: 1, Version: oldActiveVersion}}

	s.plantOrphanAndFailover(parentID, parentRunID, childID, operatorRunID, operatorEvents, operatorVH, enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE)

	// The parent must fail the start rather than adopt/terminate the unrelated workflow.
	startFailed, cause, childStarted := s.waitForChildStartOutcome(parentID, parentRunID)
	s.True(startFailed, "expected StartChildExecutionFailed for the unrelated workflow id")
	s.Equal(enumspb.START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_EXISTS, cause)
	s.False(childStarted, "parent must not adopt the operator's workflow as its child")

	// And the operator's workflow must be untouched (still running).
	desc, err := s.cluster.FrontendClient().DescribeWorkflowExecution(s.newContext(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.namespace.String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: childID, RunId: operatorRunID},
	})
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, desc.GetWorkflowExecutionInfo().GetStatus(),
		"operator's workflow must not be terminated by the parent's child start")
}

// TestOrphanChild_SelfWorkflowIdCollision verifies that a child start using the parent's own workflow
// ID fails without attempting to lock the parent recursively.
func (s *ChildWorkflowForceFailoverSuite) TestOrphanChild_SelfWorkflowIdCollision() {
	parentID, parentRunID := s.newParent()
	taskqueue := "child-ff-taskqueue"

	// Parent's winning branch ends in a pending StartChild whose child id == the parent's own id. No
	// separate child is planted; the "existing" workflow the start collides with is the parent itself.
	s.replicate(parentID, parentRunID, []*historyspb.VersionHistoryItem{{EventId: 5, Version: winningVersion}},
		s.parentEventsWithPendingChild(parentRunID, parentID /* childID == parentID */, taskqueue, winningVersion, enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE))

	s.failoverToRealCluster()
	s.refreshWorkflowTasks(parentID, parentRunID)

	// Must fail fast rather than deadlock on describing the locked parent.
	startFailed, cause, childStarted := s.waitForChildStartOutcome(parentID, parentRunID)
	s.True(startFailed, "self-id child collision must record StartChildExecutionFailed, not deadlock")
	s.Equal(enumspb.START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_EXISTS, cause)
	s.False(childStarted)
}

func (s *ChildWorkflowForceFailoverSuite) newParent() (parentID, parentRunID string) {
	return "parent-wf-" + uuid.NewString(), uuid.NewString()
}

func (s *ChildWorkflowForceFailoverSuite) newChild() (childID, childRunID, taskqueue string) {
	return "child-wf-" + uuid.NewString(), uuid.NewString(), "child-ff-taskqueue"
}

// plantOrphanAndFailover plants the orphaned child + a parent whose winning branch (version 3) ends
// in a pending StartChildWorkflowExecutionInitiated for the same child id, force-fails-over to the
// real cluster, and refreshes the parent's tasks to regenerate the StartChildExecution transfer task.
func (s *ChildWorkflowForceFailoverSuite) plantOrphanAndFailover(
	parentID string,
	parentRunID string,
	childID string,
	childRunID string,
	childEvents []*historypb.HistoryEvent,
	childVH []*historyspb.VersionHistoryItem,
	reusePolicy enumspb.WorkflowIdReusePolicy,
) {
	taskqueue := "child-ff-taskqueue"

	// 1) Plant the orphaned child (created by the old active, version 2), whose parent pointer
	//    (initiatedEventId=5, version=2) points at the parent's LOSING branch.
	s.replicate(childID, childRunID, childVH, childEvents)

	// 2) Plant the parent's stale LOSING branch (v2) — this replicates the lower-FV events (incl. the
	//    child's initiation at ev5@v2) and creates the workflow with the v2 branch current. Sent as two
	//    batches so ev3 is a batch boundary the winning branch can fork at.
	s.replicateBatches(parentID, parentRunID,
		[]*historyspb.VersionHistoryItem{{EventId: 5, Version: oldActiveVersion}},
		s.parentLosingBranch(parentRunID, childID, taskqueue))

	// 3) Plant the parent's WINNING branch (v3), forking after the shared prefix (ev3). Higher FV, so
	//    NDC conflict resolution makes it current and demotes the v2 branch to non-current. Its pending
	//    StartChild (ev8) is what the transfer task will process after failover.
	s.replicate(parentID, parentRunID,
		[]*historyspb.VersionHistoryItem{{EventId: 3, Version: oldActiveVersion}, {EventId: 8, Version: winningVersion}},
		s.parentWinningBranchSuffix(childID, taskqueue, reusePolicy))

	// 4) Force-failover to make the real cluster active.
	s.failoverToRealCluster()

	// 5) Regenerate the parent's StartChildExecution transfer task on the now-active cluster.
	s.refreshWorkflowTasks(parentID, parentRunID)
}

// childStartedEvent builds the child's WorkflowExecutionStarted event whose parent pointer references
// the parent at (initiatedEventId=5, version=oldActiveVersion) — not on the parent's winning branch.
func (s *ChildWorkflowForceFailoverSuite) childStartedEvent(parentID, parentRunID, childRunID, taskqueue string) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventId:   1,
		Version:   oldActiveVersion,
		EventTime: timestamppb.New(time.Now().UTC()),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
			WorkflowType:                &commonpb.WorkflowType{Name: "child-workflow-type"},
			TaskQueue:                   &taskqueuepb.TaskQueue{Name: taskqueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			WorkflowRunTimeout:          durationpb.New(1000 * time.Second),
			WorkflowTaskTimeout:         durationpb.New(10 * time.Second),
			ParentWorkflowNamespace:     s.namespace.String(),
			ParentWorkflowNamespaceId:   s.namespaceID.String(),
			ParentWorkflowExecution:     &commonpb.WorkflowExecution{WorkflowId: parentID, RunId: parentRunID},
			ParentInitiatedEventId:      5,
			ParentInitiatedEventVersion: oldActiveVersion,
			OriginalExecutionRunId:      childRunID,
			FirstExecutionRunId:         childRunID,
			Attempt:                     1,
		}},
	}
}

// executedChildEvents builds a child history for a run that is still RUNNING but has already executed:
// its first workflow task completed and it scheduled an activity. That makes
// LastCompletedWorkflowTaskStartedEventId and ActivityCount non-zero, which is what marks it unsafe to
// terminate and restart.
func (s *ChildWorkflowForceFailoverSuite) executedChildEvents(parentID, parentRunID, childRunID, taskqueue string) []*historypb.HistoryEvent {
	v := oldActiveVersion
	return []*historypb.HistoryEvent{
		s.childStartedEvent(parentID, parentRunID, childRunID, taskqueue),
		s.evWFTScheduled(2, v, taskqueue),
		s.evWFTStarted(3, v, 2),
		s.evWFTCompleted(4, v, 2, 3),
		{
			EventId: 5, Version: v, EventTime: timestamppb.New(time.Now().UTC()),
			EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
			Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
				WorkflowTaskCompletedEventId: 4,
				ActivityId:                   "0",
				ActivityType:                 &commonpb.ActivityType{Name: "child-activity-type"},
				TaskQueue:                    &taskqueuepb.TaskQueue{Name: taskqueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				ScheduleToCloseTimeout:       durationpb.New(100 * time.Second),
				ScheduleToStartTimeout:       durationpb.New(100 * time.Second),
				StartToCloseTimeout:          durationpb.New(100 * time.Second),
				HeartbeatTimeout:             durationpb.New(100 * time.Second),
			}},
		},
	}
}

// closedChildEvents builds a full child history that ends in WorkflowExecutionCompleted.
func (s *ChildWorkflowForceFailoverSuite) closedChildEvents(parentID, parentRunID, childRunID, taskqueue string) []*historypb.HistoryEvent {
	started := s.childStartedEvent(parentID, parentRunID, childRunID, taskqueue)
	return []*historypb.HistoryEvent{
		started,
		{
			EventId: 2, Version: oldActiveVersion, EventTime: timestamppb.New(time.Now().UTC()),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
				TaskQueue: &taskqueuepb.TaskQueue{Name: taskqueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}, StartToCloseTimeout: durationpb.New(10 * time.Second), Attempt: 1,
			}},
		},
		{
			EventId: 3, Version: oldActiveVersion, EventTime: timestamppb.New(time.Now().UTC()),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
				ScheduledEventId: 2, Identity: "worker", RequestId: uuid.NewString(),
			}},
		},
		{
			EventId: 4, Version: oldActiveVersion, EventTime: timestamppb.New(time.Now().UTC()),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
			Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
				ScheduledEventId: 2, StartedEventId: 3, Identity: "worker",
			}},
		},
		{
			EventId: 5, Version: oldActiveVersion, EventTime: timestamppb.New(time.Now().UTC()),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{
				WorkflowTaskCompletedEventId: 4,
			}},
		},
	}
}

// assertChildAdopted asserts the FIXED behavior: the parent replaces the orphaned child after
// suppressing the old run — it records CHILD_WORKFLOW_EXECUTION_STARTED and never
// START_CHILD_WORKFLOW_EXECUTION_FAILED.
// This FAILS before the fix (parent records StartChildExecutionFailed / WORKFLOW_ALREADY_EXISTS).
func (s *ChildWorkflowForceFailoverSuite) assertChildAdopted(parentID, parentRunID, childID, origChildRunID string) {
	startFailed, cause, childStarted := s.waitForChildStartOutcome(parentID, parentRunID)
	s.False(startFailed, "BUG: parent recorded StartChildExecutionFailed(%s) instead of replacing the orphaned child", cause)
	s.True(childStarted, "expected parent to record ChildWorkflowExecutionStarted after adopting the child")

	oldChild, err := s.cluster.AdminClient().DescribeMutableState(s.newContext(), &adminservice.DescribeMutableStateRequest{
		Namespace: s.namespace.String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: childID, RunId: origChildRunID},
		Archetype: chasm.WorkflowArchetype,
	})
	s.NoError(err)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		oldChild.GetDatabaseMutableState().GetExecutionState().GetState())

	currentChild, err := s.cluster.FrontendClient().DescribeWorkflowExecution(s.newContext(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.namespace.String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: childID},
	})
	s.NoError(err)
	s.NotEqual(origChildRunID, currentChild.GetWorkflowExecutionInfo().GetExecution().GetRunId())
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, currentChild.GetWorkflowExecutionInfo().GetStatus())
}

// waitForChildStartOutcome blocks until the parent records either a child-start failure or a
// child-started event, then returns the observed outcome.
func (s *ChildWorkflowForceFailoverSuite) waitForChildStartOutcome(parentID, parentRunID string) (startFailed bool, cause enumspb.StartChildWorkflowExecutionFailedCause, childStarted bool) {
	await.RequireTruef(s.T(), func() bool {
		startFailed, cause, childStarted = s.parentChildStartOutcome(parentID, parentRunID)
		return startFailed || childStarted
	}, 30*time.Second, 500*time.Millisecond, "no start-child outcome on the parent yet")
	return startFailed, cause, childStarted
}

// --- parent history event builders ---
//
// The parent is modeled with two real NDC branches, as a force failover produces:
//   - a stale LOSING branch at oldActiveVersion (v2): the in-flight WT (ev3) completes and initiates
//     the child at ev5 — this is where the replicated child's parent pointer (5, v2) lives;
//   - a WINNING branch at winningVersion (v3): forks after ev3 (the in-flight WT is failed with
//     FAILOVER_CLOSE_COMMAND, rescheduled, and re-run), re-issuing the child at ev8.
// v3 > v2, so conflict resolution makes the winning branch current and keeps the losing branch as a
// non-current version history — exactly the state on the new active after the failover.

func (s *ChildWorkflowForceFailoverSuite) evWFStarted(id, version int64, runID, taskqueue string) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventId: id, Version: version, EventTime: timestamppb.New(time.Now().UTC()),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
			WorkflowType:             &commonpb.WorkflowType{Name: "parent-workflow-type"},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: taskqueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			WorkflowRunTimeout:       durationpb.New(1000 * time.Second),
			WorkflowTaskTimeout:      durationpb.New(10 * time.Second),
			OriginalExecutionRunId:   runID,
			FirstExecutionRunId:      runID,
			Attempt:                  1,
			FirstWorkflowTaskBackoff: durationpb.New(0),
		}},
	}
}

func (s *ChildWorkflowForceFailoverSuite) evWFTScheduled(id, version int64, taskqueue string) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventId: id, Version: version, EventTime: timestamppb.New(time.Now().UTC()),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskqueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}, StartToCloseTimeout: durationpb.New(10 * time.Second), Attempt: 1,
		}},
	}
}

func (s *ChildWorkflowForceFailoverSuite) evWFTStarted(id, version, schedID int64) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventId: id, Version: version, EventTime: timestamppb.New(time.Now().UTC()),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
			ScheduledEventId: schedID, Identity: "worker", RequestId: uuid.NewString(),
		}},
	}
}

func (s *ChildWorkflowForceFailoverSuite) evWFTCompleted(id, version, schedID, startedID int64) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventId: id, Version: version, EventTime: timestamppb.New(time.Now().UTC()),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
			ScheduledEventId: schedID, StartedEventId: startedID, Identity: "worker",
		}},
	}
}

func (s *ChildWorkflowForceFailoverSuite) evWFTFailedFailover(id, version, schedID, startedID int64) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventId: id, Version: version, EventTime: timestamppb.New(time.Now().UTC()),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskFailedEventAttributes{WorkflowTaskFailedEventAttributes: &historypb.WorkflowTaskFailedEventAttributes{
			ScheduledEventId: schedID, StartedEventId: startedID, Cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_FAILOVER_CLOSE_COMMAND, Identity: "history-service",
		}},
	}
}

func (s *ChildWorkflowForceFailoverSuite) evStartChildInitiated(id, version, wftCompletedID int64, childID, taskqueue string, reusePolicy enumspb.WorkflowIdReusePolicy) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventId: id, Version: version, EventTime: timestamppb.New(time.Now().UTC()),
		EventType: enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED,
		Attributes: &historypb.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{StartChildWorkflowExecutionInitiatedEventAttributes: &historypb.StartChildWorkflowExecutionInitiatedEventAttributes{
			Namespace:                    s.namespace.String(),
			NamespaceId:                  s.namespaceID.String(),
			WorkflowId:                   childID,
			WorkflowType:                 &commonpb.WorkflowType{Name: "child-workflow-type"},
			TaskQueue:                    &taskqueuepb.TaskQueue{Name: taskqueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			WorkflowTaskCompletedEventId: wftCompletedID,
			WorkflowRunTimeout:           durationpb.New(1000 * time.Second),
			WorkflowTaskTimeout:          durationpb.New(10 * time.Second),
			ParentClosePolicy:            enumspb.PARENT_CLOSE_POLICY_ABANDON,
			WorkflowIdReusePolicy:        reusePolicy,
		}},
	}
}

// parentLosingBranch: events 1-5 at oldActiveVersion (v2), in TWO batches split after ev3. Ends with
// StartChildInitiated at ev5 — the initiation the replicated child (parentInitiatedEventId=5,
// version=2) points at. Version history [{5,2}]. The split makes ev3 a batch boundary so the winning
// branch can fork there (NDC can only branch at a batch boundary).
func (s *ChildWorkflowForceFailoverSuite) parentLosingBranch(runID, childID, taskqueue string) [][]*historypb.HistoryEvent {
	v := oldActiveVersion
	return [][]*historypb.HistoryEvent{
		{ // batch 1: ends at ev3 (the fork point / in-flight WT)
			s.evWFStarted(1, v, runID, taskqueue),
			s.evWFTScheduled(2, v, taskqueue),
			s.evWFTStarted(3, v, 2),
		},
		{ // batch 2: WT completes and initiates the child
			s.evWFTCompleted(4, v, 2, 3),
			s.evStartChildInitiated(5, v, 4, childID, taskqueue, enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE),
		},
	}
}

// parentWinningBranchSuffix: events 4-8 at winningVersion (v3), forking after the shared prefix (ev3).
// The in-flight WT is failed (FAILOVER), rescheduled, re-run, and re-issues the child at ev8 (pending).
// Version history [{3,2},{8,3}].
func (s *ChildWorkflowForceFailoverSuite) parentWinningBranchSuffix(childID, taskqueue string, reusePolicy enumspb.WorkflowIdReusePolicy) []*historypb.HistoryEvent {
	v := winningVersion
	return []*historypb.HistoryEvent{
		s.evWFTFailedFailover(4, v, 2, 3),
		s.evWFTScheduled(5, v, taskqueue),
		s.evWFTStarted(6, v, 5),
		s.evWFTCompleted(7, v, 5, 6),
		s.evStartChildInitiated(8, v, 7, childID, taskqueue, reusePolicy),
	}
}

// parentEventsWithPendingChild builds a single-branch parent history (events 1-5) whose last event is a
// pending StartChildWorkflowExecutionInitiated at the given version. Used where a divergent branch is
// not needed (e.g. the self-id collision test).
func (s *ChildWorkflowForceFailoverSuite) parentEventsWithPendingChild(
	runID string,
	childID string,
	taskqueue string,
	version int64,
	reusePolicy enumspb.WorkflowIdReusePolicy,
) []*historypb.HistoryEvent {
	return []*historypb.HistoryEvent{
		s.evWFStarted(1, version, runID, taskqueue),
		s.evWFTScheduled(2, version, taskqueue),
		s.evWFTStarted(3, version, 2),
		s.evWFTCompleted(4, version, 2, 3),
		s.evStartChildInitiated(5, version, 4, childID, taskqueue, reusePolicy),
	}
}

// replicate injects a single batch of history events into the passive cluster via ReplicateEventsV2.
func (s *ChildWorkflowForceFailoverSuite) replicate(
	workflowID string,
	runID string,
	versionHistoryItems []*historyspb.VersionHistoryItem,
	events []*historypb.HistoryEvent,
) {
	blob, err := s.serializer.SerializeEvents(events)
	s.NoError(err)
	req := &historyservice.ReplicateEventsV2Request{
		NamespaceId:         s.namespaceID.String(),
		WorkflowExecution:   &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		VersionHistoryItems: versionHistoryItems,
		Events:              blob,
	}
	_, err = s.cluster.HistoryClient().ReplicateEventsV2(s.newContext(), req)
	s.NoError(err, "failed to replicate events for %s", workflowID)
}

// replicateBatches injects multiple contiguous batches of history events (each its own ReplicateEventsV2
// call, all carrying the same full version history), so batch boundaries exist where later branches fork.
func (s *ChildWorkflowForceFailoverSuite) replicateBatches(
	workflowID string,
	runID string,
	versionHistoryItems []*historyspb.VersionHistoryItem,
	batches [][]*historypb.HistoryEvent,
) {
	for _, batch := range batches {
		s.replicate(workflowID, runID, versionHistoryItems, batch)
	}
}

// failoverToRealCluster force-fails-over the namespace to cluster-a (the real cluster), making it active.
func (s *ChildWorkflowForceFailoverSuite) failoverToRealCluster() {
	_, err := s.cluster.FrontendClient().UpdateNamespace(s.newContext(), &workflowservice.UpdateNamespaceRequest{
		Namespace: s.namespace.String(),
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: clusterName[0], // cluster-a
		},
	})
	s.Require().NoError(err)
	time.Sleep(2 * testcore.NamespaceCacheRefreshInterval) //nolint:forbidigo
}

func (s *ChildWorkflowForceFailoverSuite) refreshWorkflowTasks(workflowID, runID string) {
	_, err := s.cluster.AdminClient().RefreshWorkflowTasks(s.newContext(), &adminservice.RefreshWorkflowTasksRequest{
		NamespaceId: s.namespaceID.String(),
		Execution:   &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
	})
	s.Require().NoError(err)
}

// parentChildStartOutcome scans the parent history for the child-start outcome.
func (s *ChildWorkflowForceFailoverSuite) parentChildStartOutcome(workflowID, runID string) (startFailed bool, cause enumspb.StartChildWorkflowExecutionFailedCause, childStarted bool) {
	resp, err := s.cluster.FrontendClient().GetWorkflowExecutionHistory(s.newContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: s.namespace.String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
	})
	if err != nil {
		return false, enumspb.START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_UNSPECIFIED, false
	}
	for _, e := range resp.GetHistory().GetEvents() {
		switch e.GetEventType() {
		case enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED:
			return true, e.GetStartChildWorkflowExecutionFailedEventAttributes().GetCause(), childStarted
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED:
			childStarted = true
		default:
		}
	}
	return startFailed, enumspb.START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_UNSPECIFIED, childStarted
}
