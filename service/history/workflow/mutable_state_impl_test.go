package workflow

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally/v4"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
	serviceerror2 "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/testing/fakedata"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/historybuilder"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	mutableStateSuite struct {
		suite.Suite
		*require.Assertions

		controller      *gomock.Controller
		mockConfig      *configs.Config
		mockShard       *shard.ContextTest
		mockEventsCache *events.MockCache

		namespaceEntry *namespace.Namespace
		mutableState   *MutableStateImpl
		logger         log.Logger
		testScope      tally.TestScope

		replicationMultipleBatches bool
	}
)

var (
	testPayload = commonpb.Payload_builder{
		Metadata: map[string][]byte{
			"random metadata key": []byte("random metadata value"),
		},
		Data: []byte("random data"),
	}.Build()
	testPayloads                 = commonpb.Payloads_builder{Payloads: []*commonpb.Payload{testPayload}}.Build()
	workflowTaskCompletionLimits = historyi.WorkflowTaskCompletionLimits{
		MaxResetPoints:              10,
		MaxSearchAttributeValueSize: 1024,
	}
	deployment1 = deploymentpb.Deployment_builder{
		SeriesName: "my_app",
		BuildId:    "build_1",
	}.Build()
	deployment2 = deploymentpb.Deployment_builder{
		SeriesName: "my_app",
		BuildId:    "build_2",
	}.Build()
	deployment3 = deploymentpb.Deployment_builder{
		SeriesName: "my_app",
		BuildId:    "build_3",
	}.Build()
	versionOverrideMask = &fieldmaskpb.FieldMask{Paths: []string{"versioning_behavior_override"}}
	pinnedOptions1      = workflowpb.WorkflowExecutionOptions_builder{
		VersioningOverride: workflowpb.VersioningOverride_builder{
			Behavior:      enumspb.VERSIONING_BEHAVIOR_PINNED,
			PinnedVersion: worker_versioning.WorkerDeploymentVersionToStringV31(worker_versioning.DeploymentVersionFromDeployment(deployment1)),
		}.Build(),
	}.Build()
	pinnedOptions2 = workflowpb.WorkflowExecutionOptions_builder{
		VersioningOverride: workflowpb.VersioningOverride_builder{
			Behavior:      enumspb.VERSIONING_BEHAVIOR_PINNED,
			PinnedVersion: worker_versioning.WorkerDeploymentVersionToStringV31(worker_versioning.DeploymentVersionFromDeployment(deployment2)),
		}.Build(),
	}.Build()
	pinnedOptions3 = workflowpb.WorkflowExecutionOptions_builder{
		VersioningOverride: workflowpb.VersioningOverride_builder{
			Behavior:      enumspb.VERSIONING_BEHAVIOR_PINNED,
			PinnedVersion: worker_versioning.WorkerDeploymentVersionToStringV31(worker_versioning.DeploymentVersionFromDeployment(deployment3)),
		}.Build(),
	}.Build()
	unpinnedOptions = workflowpb.WorkflowExecutionOptions_builder{
		VersioningOverride: workflowpb.VersioningOverride_builder{
			Behavior: enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE,
		}.Build(),
	}.Build()
	emptyOptions = &workflowpb.WorkflowExecutionOptions{}
)

func TestMutableStateSuite(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		replicationMultipleBatches bool
	}{
		{
			name:                       "ReplicationMultipleBatchesEnabled",
			replicationMultipleBatches: true,
		},
		{
			name:                       "ReplicationMultipleBatchesDisabled",
			replicationMultipleBatches: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &mutableStateSuite{
				replicationMultipleBatches: tc.replicationMultipleBatches,
			}
			suite.Run(t, s)
		})
	}
}

func (s *mutableStateSuite) SetupSuite() {

}

func (s *mutableStateSuite) TearDownSuite() {

}

func (s *mutableStateSuite) Name() string {
	return "MutableStateSuite"
}

func (s *mutableStateSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockEventsCache = events.NewMockCache(s.controller)

	s.mockConfig = tests.NewDynamicConfig()
	s.mockConfig.ReplicationMultipleBatches = dynamicconfig.GetBoolPropertyFn(s.replicationMultipleBatches)
	s.mockShard = shard.NewTestContext(
		s.controller,
		persistencespb.ShardInfo_builder{
			ShardId: 0,
			RangeId: 1,
		}.Build(),
		s.mockConfig,
	)
	reg := hsm.NewRegistry()
	s.Require().NoError(RegisterStateMachine(reg))
	s.Require().NoError(callbacks.RegisterStateMachine(reg))
	s.mockShard.SetStateMachineRegistry(reg)

	s.mockConfig.MutableStateActivityFailureSizeLimitWarn = func(namespace string) int { return 1 * 1024 }
	s.mockConfig.MutableStateActivityFailureSizeLimitError = func(namespace string) int { return 2 * 1024 }
	s.mockConfig.EnableTransitionHistory = func() bool { return true }
	s.mockShard.SetEventsCacheForTesting(s.mockEventsCache)

	s.namespaceEntry = tests.GlobalNamespaceEntry
	s.mockShard.Resource.NamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(s.namespaceEntry, nil).AnyTimes()
	s.mockShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.namespaceEntry.FailoverVersion(tests.WorkflowID)).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockShard.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	s.testScope = s.mockShard.Resource.MetricsScope.(tally.TestScope)
	s.logger = s.mockShard.GetLogger()

	s.mutableState = NewMutableState(s.mockShard, s.mockEventsCache, s.logger, s.namespaceEntry, tests.WorkflowID, tests.RunID, time.Now().UTC())
}

func (s *mutableStateSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *mutableStateSuite) SetupSubTest() {
	// create a fresh mutable state for each sub test
	// this will be invoked upon s.Run()
	s.mutableState = NewMutableState(s.mockShard, s.mockEventsCache, s.logger, s.mutableState.GetNamespaceEntry(), tests.WorkflowID, tests.RunID, time.Now().UTC())
}

func (s *mutableStateSuite) TestTransientWorkflowTaskCompletionFirstBatchApplied_ApplyWorkflowTaskCompleted() {
	version := int64(12)
	workflowID := "some random workflow ID"
	runID := uuid.NewString()
	s.mutableState = TestGlobalMutableState(
		s.mockShard,
		s.mockEventsCache,
		s.logger,
		version,
		workflowID,
		runID,
	)

	newWorkflowTaskScheduleEvent, newWorkflowTaskStartedEvent := s.prepareTransientWorkflowTaskCompletionFirstBatchApplied(version, workflowID, runID)

	newWorkflowTaskCompletedEvent := historypb.HistoryEvent_builder{
		Version:   version,
		EventId:   newWorkflowTaskStartedEvent.GetEventId() + 1,
		EventTime: timestamppb.New(time.Now().UTC()),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		WorkflowTaskCompletedEventAttributes: historypb.WorkflowTaskCompletedEventAttributes_builder{
			ScheduledEventId: newWorkflowTaskScheduleEvent.GetEventId(),
			StartedEventId:   newWorkflowTaskStartedEvent.GetEventId(),
			Identity:         "some random identity",
		}.Build(),
	}.Build()
	s.mutableState.SetHistoryBuilder(historybuilder.NewImmutable([]*historypb.HistoryEvent{
		newWorkflowTaskCompletedEvent,
	}))
	err := s.mutableState.ApplyWorkflowTaskCompletedEvent(newWorkflowTaskCompletedEvent)
	s.NoError(err)
	s.Equal(0, s.mutableState.hBuilder.NumBufferedEvents())
}

func (s *mutableStateSuite) TestTransientWorkflowTaskCompletionFirstBatchApplied_FailoverWorkflowTaskTimeout() {
	version := int64(12)
	workflowID := "some random workflow ID"
	runID := uuid.NewString()
	s.mutableState = TestGlobalMutableState(
		s.mockShard,
		s.mockEventsCache,
		s.logger,
		version,
		workflowID,
		runID,
	)

	newWorkflowTaskScheduleEvent, _ := s.prepareTransientWorkflowTaskCompletionFirstBatchApplied(version, workflowID, runID)

	newWorkflowTask := s.mutableState.GetWorkflowTaskByID(newWorkflowTaskScheduleEvent.GetEventId())
	s.NotNil(newWorkflowTask)

	_, err := s.mutableState.AddWorkflowTaskTimedOutEvent(
		newWorkflowTask,
	)
	s.NoError(err)
	s.Equal(0, s.mutableState.hBuilder.NumBufferedEvents())
}

func (s *mutableStateSuite) TestTransientWorkflowTaskCompletionFirstBatchApplied_FailoverWorkflowTaskFailed() {
	version := int64(12)
	workflowID := "some random workflow ID"
	runID := uuid.NewString()
	s.mutableState = TestGlobalMutableState(
		s.mockShard,
		s.mockEventsCache,
		s.logger,
		version,
		workflowID,
		runID,
	)

	newWorkflowTaskScheduleEvent, _ := s.prepareTransientWorkflowTaskCompletionFirstBatchApplied(version, workflowID, runID)

	newWorkflowTask := s.mutableState.GetWorkflowTaskByID(newWorkflowTaskScheduleEvent.GetEventId())
	s.NotNil(newWorkflowTask)

	_, err := s.mutableState.AddWorkflowTaskFailedEvent(
		newWorkflowTask,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
		failure.NewServerFailure("some random workflow task failure details", false),
		"some random workflow task failure identity",
		nil,
		"",
		"",
		"",
		0,
	)
	s.NoError(err)
	s.Equal(0, s.mutableState.hBuilder.NumBufferedEvents())
}

func (s *mutableStateSuite) TestRedirectInfoValidation_Valid() {
	tq := taskqueuepb.TaskQueue_builder{Name: "tq"}.Build()
	s.createVersionedMutableStateWithCompletedWFT(tq)

	wft, err := s.mutableState.AddWorkflowTaskScheduledEvent(true, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	s.NoError(err)
	e, wft, err := s.mutableState.AddWorkflowTaskStartedEvent(
		wft.ScheduledEventID,
		"",
		tq,
		"",
		worker_versioning.StampFromBuildId("b2"),
		taskqueuespb.BuildIdRedirectInfo_builder{AssignedBuildId: "b1"}.Build(),
		nil,
		false,
		nil,
	)
	s.NoError(err)
	s.Equal("b2", wft.BuildId)
	s.Equal("b2", e.GetWorkflowTaskStartedEventAttributes().GetWorkerVersion().GetBuildId())
	s.Equal("b2", s.mutableState.GetAssignedBuildId())
	s.Equal(int64(1), wft.BuildIdRedirectCounter)
	s.Equal(int64(1), s.mutableState.GetExecutionInfo().GetBuildIdRedirectCounter())
}

func (s *mutableStateSuite) TestRedirectInfoValidation_Invalid() {
	tq := taskqueuepb.TaskQueue_builder{Name: "tq"}.Build()
	s.createVersionedMutableStateWithCompletedWFT(tq)

	wft, err := s.mutableState.AddWorkflowTaskScheduledEvent(true, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	s.NoError(err)
	_, _, err = s.mutableState.AddWorkflowTaskStartedEvent(
		wft.ScheduledEventID,
		"",
		tq,
		"",
		worker_versioning.StampFromBuildId("b2"),
		taskqueuespb.BuildIdRedirectInfo_builder{AssignedBuildId: "b0"}.Build(),
		nil,
		false,
		nil,
	)
	expectedErr := &serviceerror2.ObsoleteDispatchBuildId{}
	s.ErrorAs(err, &expectedErr)
	s.Equal("b1", s.mutableState.GetAssignedBuildId())
	s.Equal(int64(0), s.mutableState.GetExecutionInfo().GetBuildIdRedirectCounter())
}

func (s *mutableStateSuite) TestRedirectInfoValidation_EmptyRedirectInfo() {
	tq := taskqueuepb.TaskQueue_builder{Name: "tq"}.Build()
	s.createVersionedMutableStateWithCompletedWFT(tq)

	wft, err := s.mutableState.AddWorkflowTaskScheduledEvent(true, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	s.NoError(err)
	_, _, err = s.mutableState.AddWorkflowTaskStartedEvent(
		wft.ScheduledEventID,
		"",
		tq,
		"",
		worker_versioning.StampFromBuildId("b2"),
		nil,
		nil,
		false,
		nil,
	)
	expectedErr := &serviceerror2.ObsoleteDispatchBuildId{}
	s.ErrorAs(err, &expectedErr)
	s.Equal("b1", s.mutableState.GetAssignedBuildId())
	s.Equal(int64(0), s.mutableState.GetExecutionInfo().GetBuildIdRedirectCounter())
}

func (s *mutableStateSuite) TestRedirectInfoValidation_EmptyStamp() {
	tq := taskqueuepb.TaskQueue_builder{Name: "tq"}.Build()
	s.createVersionedMutableStateWithCompletedWFT(tq)

	wft, err := s.mutableState.AddWorkflowTaskScheduledEvent(true, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	s.NoError(err)
	_, _, err = s.mutableState.AddWorkflowTaskStartedEvent(
		wft.ScheduledEventID,
		"",
		tq,
		"",
		nil,
		taskqueuespb.BuildIdRedirectInfo_builder{AssignedBuildId: "b1"}.Build(),
		nil,
		false,
		nil,
	)
	expectedErr := &serviceerror2.ObsoleteDispatchBuildId{}
	s.ErrorAs(err, &expectedErr)
	s.Equal("b1", s.mutableState.GetAssignedBuildId())
	s.Equal(int64(0), s.mutableState.GetExecutionInfo().GetBuildIdRedirectCounter())
}

func (s *mutableStateSuite) TestRedirectInfoValidation_Sticky() {
	tq := taskqueuepb.TaskQueue_builder{Name: "tq"}.Build()
	sticky := taskqueuepb.TaskQueue_builder{Name: "sticky-tq", Kind: enumspb.TASK_QUEUE_KIND_STICKY}.Build()
	s.createVersionedMutableStateWithCompletedWFT(tq)

	s.mutableState.SetStickyTaskQueue(sticky.GetName(), durationpb.New(time.Second))
	wft, err := s.mutableState.AddWorkflowTaskScheduledEvent(true, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	s.NoError(err)
	e, wft, err := s.mutableState.AddWorkflowTaskStartedEvent(
		wft.ScheduledEventID,
		"",
		sticky,
		"",
		nil,
		nil,
		nil,
		false,
		nil,
	)
	s.NoError(err)
	s.Equal("", wft.BuildId)
	s.Equal("", e.GetWorkflowTaskStartedEventAttributes().GetWorkerVersion().GetBuildId())
	s.Equal("b1", s.mutableState.GetAssignedBuildId())
	s.Equal(int64(0), wft.BuildIdRedirectCounter)
	s.Equal(int64(0), s.mutableState.GetExecutionInfo().GetBuildIdRedirectCounter())
}

func (s *mutableStateSuite) TestRedirectInfoValidation_StickyInvalid() {
	tq := taskqueuepb.TaskQueue_builder{Name: "tq"}.Build()
	sticky := taskqueuepb.TaskQueue_builder{Name: "sticky-tq", Kind: enumspb.TASK_QUEUE_KIND_STICKY}.Build()
	s.createVersionedMutableStateWithCompletedWFT(tq)

	s.mutableState.SetStickyTaskQueue(sticky.GetName(), durationpb.New(time.Second))
	wft, err := s.mutableState.AddWorkflowTaskScheduledEvent(true, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	s.NoError(err)
	_, _, err = s.mutableState.AddWorkflowTaskStartedEvent(
		wft.ScheduledEventID,
		"",
		taskqueuepb.TaskQueue_builder{Name: "another-sticky-tq"}.Build(),
		"",
		nil,
		nil,
		nil,
		false,
		nil,
	)
	expectedErr := &serviceerror2.ObsoleteDispatchBuildId{}
	s.ErrorAs(err, &expectedErr)
	s.Equal("b1", s.mutableState.GetAssignedBuildId())
	s.Equal(int64(0), s.mutableState.GetExecutionInfo().GetBuildIdRedirectCounter())
}

func (s *mutableStateSuite) TestRedirectInfoValidation_UnexpectedSticky() {
	tq := taskqueuepb.TaskQueue_builder{Name: "tq"}.Build()
	sticky := taskqueuepb.TaskQueue_builder{Name: "sticky-tq", Kind: enumspb.TASK_QUEUE_KIND_STICKY}.Build()
	s.createVersionedMutableStateWithCompletedWFT(tq)

	wft, err := s.mutableState.AddWorkflowTaskScheduledEvent(true, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	s.NoError(err)
	_, _, err = s.mutableState.AddWorkflowTaskStartedEvent(
		wft.ScheduledEventID,
		"",
		sticky,
		"",
		nil,
		nil,
		nil,
		false,
		nil,
	)
	expectedErr := &serviceerror2.ObsoleteDispatchBuildId{}
	s.ErrorAs(err, &expectedErr)
	s.Equal("b1", s.mutableState.GetAssignedBuildId())
	s.Equal(int64(0), s.mutableState.GetExecutionInfo().GetBuildIdRedirectCounter())
}

func (s *mutableStateSuite) TestPopulateDeleteTasks_WithWorkflowTaskTimeouts() {
	// Test that workflow task timeout task references are added to BestEffortDeleteTasks when present.
	version := int64(1)
	workflowID := "wf-timeout-delete"
	runID := uuid.NewString()
	s.mutableState = TestGlobalMutableState(
		s.mockShard,
		s.mockEventsCache,
		s.logger,
		version,
		workflowID,
		runID,
	)

	// Create mock timeout tasks that meet the criteria
	now := time.Now().UTC()
	mockScheduleToStartTask := &tasks.WorkflowTaskTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.mutableState.GetExecutionInfo().GetNamespaceId(),
			workflowID,
			runID,
		),
		VisibilityTimestamp: now.Add(10 * time.Second), // < 120s
		TaskID:              123,
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		InMemory:            false, // Persisted task
	}

	mockStartToCloseTask := &tasks.WorkflowTaskTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.mutableState.GetExecutionInfo().GetNamespaceId(),
			workflowID,
			runID,
		),
		VisibilityTimestamp: now.Add(30 * time.Second), // < 120s
		TaskID:              456,
		TimeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		InMemory:            false, // Persisted task
	}

	// Schedule and start a workflow task
	wft, err := s.mutableState.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	s.NoError(err)

	sticky := taskqueuepb.TaskQueue_builder{Name: "sticky-tq", Kind: enumspb.TASK_QUEUE_KIND_STICKY}.Build()
	_, wft, err = s.mutableState.AddWorkflowTaskStartedEvent(
		wft.ScheduledEventID,
		"",
		sticky,
		"",
		nil,
		nil,
		nil,
		false,
		nil,
	)
	s.NoError(err)

	// Set timeout tasks directly in mutable state (simulating what task_generator does)
	s.mutableState.SetWorkflowTaskScheduleToStartTimeoutTask(mockScheduleToStartTask)
	s.mutableState.SetWorkflowTaskStartToCloseTimeoutTask(mockStartToCloseTask)
	// Call UpdateWorkflowTask to persist the workflow task info to ExecutionInfo
	s.mutableState.workflowTaskManager.UpdateWorkflowTask(wft)

	// Complete the workflow task
	_, err = s.mutableState.AddWorkflowTaskCompletedEvent(
		wft,
		&workflowservice.RespondWorkflowTaskCompletedRequest{},
		workflowTaskCompletionLimits,
	)
	s.NoError(err)

	// Verify that BestEffortDeleteTasks contains the timeout task keys
	del := s.mutableState.BestEffortDeleteTasks
	s.Contains(del, tasks.CategoryTimer)
	s.Equal(2, len(del[tasks.CategoryTimer]), "Should have both ScheduleToStart and StartToClose timeout tasks")
	s.Contains(del[tasks.CategoryTimer], mockScheduleToStartTask.GetKey())
	s.Contains(del[tasks.CategoryTimer], mockStartToCloseTask.GetKey())
}

func (s *mutableStateSuite) TestPopulateDeleteTasks_LongTimeout_NotIncluded() {
	// Test that timeout tasks with very long timeouts (> 120s) are NOT added to BestEffortDeleteTasks.
	version := int64(1)
	workflowID := "wf-long-timeout"
	runID := uuid.NewString()
	s.mutableState = TestGlobalMutableState(
		s.mockShard,
		s.mockEventsCache,
		s.logger,
		version,
		workflowID,
		runID,
	)

	// Schedule a workflow task - this sets the scheduled time
	wft, err := s.mutableState.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	s.NoError(err)

	sticky := taskqueuepb.TaskQueue_builder{Name: "sticky-tq", Kind: enumspb.TASK_QUEUE_KIND_STICKY}.Build()
	_, wft, err = s.mutableState.AddWorkflowTaskStartedEvent(
		wft.ScheduledEventID,
		"",
		sticky,
		"",
		nil,
		nil,
		nil,
		false,
		nil,
	)
	s.NoError(err)

	// Get the actual scheduled time from wft (this is what will be used in the calculation)
	scheduledTime := wft.ScheduledTime
	if scheduledTime.IsZero() {
		// If scheduled time is not set in wft, use current time
		scheduledTime = time.Now().UTC()
	}

	// Create a timeout task with timeout > 120s relative to the actual scheduled time
	mockLongTimeoutTask := &tasks.WorkflowTaskTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.mutableState.GetExecutionInfo().GetNamespaceId(),
			workflowID,
			runID,
		),
		VisibilityTimestamp: scheduledTime.Add(200 * time.Second), // > 120s from scheduled time
		TaskID:              123,
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		InMemory:            false, // Persisted task
	}

	// Set the long timeout task directly in mutable state
	s.mutableState.SetWorkflowTaskScheduleToStartTimeoutTask(mockLongTimeoutTask)
	// Clear the StartToClose task so it doesn't interfere with the test
	s.mutableState.SetWorkflowTaskStartToCloseTimeoutTask(nil)

	// Complete the workflow task
	_, err = s.mutableState.AddWorkflowTaskCompletedEvent(
		wft,
		&workflowservice.RespondWorkflowTaskCompletedRequest{},
		workflowTaskCompletionLimits,
	)
	s.NoError(err)

	// Verify that BestEffortDeleteTasks does NOT contain the long timeout task
	del := s.mutableState.BestEffortDeleteTasks
	if timerTasks, exists := del[tasks.CategoryTimer]; exists {
		s.Equal(0, len(timerTasks), "Tasks with timeout > 120s should not be added to BestEffortDeleteTasks")
	}
}

// creates a mutable state with first WFT completed on Build ID "b1"
func (s *mutableStateSuite) createVersionedMutableStateWithCompletedWFT(tq *taskqueuepb.TaskQueue) {
	version := int64(12)
	workflowID := "some random workflow ID"
	runID := uuid.NewString()
	s.mutableState = TestGlobalMutableState(
		s.mockShard,
		s.mockEventsCache,
		s.logger,
		version,
		workflowID,
		runID,
	)

	wft, err := s.mutableState.AddWorkflowTaskScheduledEvent(true, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	s.NoError(err)
	e, wft, err := s.mutableState.AddWorkflowTaskStartedEvent(
		wft.ScheduledEventID,
		"",
		tq,
		"",
		worker_versioning.StampFromBuildId("b1"),
		nil,
		nil,
		false,
		nil,
	)
	s.NoError(err)
	s.Equal("b1", wft.BuildId)
	s.Equal("b1", e.GetWorkflowTaskStartedEventAttributes().GetWorkerVersion().GetBuildId())
	s.Equal("b1", s.mutableState.GetAssignedBuildId())
	s.Equal(int64(0), wft.BuildIdRedirectCounter)
	s.Equal(int64(0), s.mutableState.GetExecutionInfo().GetBuildIdRedirectCounter())
	_, err = s.mutableState.AddWorkflowTaskCompletedEvent(
		wft,
		&workflowservice.RespondWorkflowTaskCompletedRequest{},
		workflowTaskCompletionLimits,
	)
	s.NoError(err)
}

func (s *mutableStateSuite) TestEffectiveDeployment() {
	tv := testvars.New(s)
	ms := TestGlobalMutableState(
		s.mockShard,
		s.mockEventsCache,
		s.logger,
		int64(12),
		tv.WorkflowID(),
		tv.RunID(),
	)
	s.Nil(ms.executionInfo.GetVersioningInfo())
	s.mutableState = ms
	s.verifyEffectiveDeployment(nil, enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED)

	versioningInfo := &workflowpb.WorkflowExecutionVersioningInfo{}
	ms.executionInfo.SetVersioningInfo(versioningInfo)
	s.verifyEffectiveDeployment(nil, enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED)

	dv1 := worker_versioning.WorkerDeploymentVersionToStringV31(worker_versioning.DeploymentVersionFromDeployment(deployment1))
	dv2 := worker_versioning.WorkerDeploymentVersionToStringV31(worker_versioning.DeploymentVersionFromDeployment(deployment2))
	dv3 := worker_versioning.WorkerDeploymentVersionToStringV31(worker_versioning.DeploymentVersionFromDeployment(deployment3))

	deploymentVersion1 := worker_versioning.ExternalWorkerDeploymentVersionFromDeployment(deployment1)
	deploymentVersion2 := worker_versioning.ExternalWorkerDeploymentVersionFromDeployment(deployment2)
	deploymentVersion3 := worker_versioning.ExternalWorkerDeploymentVersionFromDeployment(deployment3)

	for _, useV32 := range []bool{true, false} {
		// ------- Without override, without transition

		// deployment is set but behavior is not -> unversioned
		if useV32 {
			versioningInfo.SetDeploymentVersion(deploymentVersion1)
		} else {
			versioningInfo.SetVersion(dv1) //nolint:staticcheck // SA1019: worker versioning v0.31
		}
		versioningInfo.SetBehavior(enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED)
		s.verifyEffectiveDeployment(nil, enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED)

		if useV32 {
			versioningInfo.SetDeploymentVersion(deploymentVersion1)
		} else {
			versioningInfo.SetVersion(dv1) //nolint:staticcheck // SA1019: worker versioning v0.31
		}
		versioningInfo.SetBehavior(enumspb.VERSIONING_BEHAVIOR_PINNED)
		s.verifyEffectiveDeployment(deployment1, enumspb.VERSIONING_BEHAVIOR_PINNED)

		if useV32 {
			versioningInfo.SetDeploymentVersion(deploymentVersion1)
		} else {
			versioningInfo.SetVersion(dv1) //nolint:staticcheck // SA1019: worker versioning v0.31
		}
		versioningInfo.SetBehavior(enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE)
		s.verifyEffectiveDeployment(deployment1, enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE)

		// ------- With override, without transition

		// deployment and behavior are not set, but override behavior is AUTO_UPGRADE -> AUTO_UPGRADE
		if useV32 {
			versioningInfo.ClearDeploymentVersion()
			versioningInfo.SetVersioningOverride(workflowpb.VersioningOverride_builder{
				AutoUpgrade: proto.Bool(true),
			}.Build())
		} else {
			versioningInfo.SetVersion("") //nolint:staticcheck // SA1019: worker versioning v0.31
			versioningInfo.SetVersioningOverride(workflowpb.VersioningOverride_builder{
				Behavior: enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE, //nolint:staticcheck // SA1019: worker versioning v0.31
			}.Build())
		}
		versioningInfo.SetBehavior(enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED)
		s.verifyEffectiveDeployment(nil, enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE)

		// deployment is set, behavior is not, but override behavior is AUTO_UPGRADE -> AUTO_UPGRADE
		if useV32 {
			versioningInfo.SetDeploymentVersion(deploymentVersion1)
			versioningInfo.SetVersioningOverride(workflowpb.VersioningOverride_builder{
				AutoUpgrade: proto.Bool(true),
			}.Build())
		} else {
			versioningInfo.SetVersion(dv1) //nolint:staticcheck // SA1019: worker versioning v0.31
			versioningInfo.SetVersioningOverride(workflowpb.VersioningOverride_builder{
				Behavior: enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE, //nolint:staticcheck // SA1019: worker versioning v0.31
			}.Build())
		}
		versioningInfo.SetBehavior(enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED)
		s.verifyEffectiveDeployment(deployment1, enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE)

		// worker says PINNED, but override behavior is AUTO_UPGRADE -> AUTO_UPGRADE
		if useV32 {
			versioningInfo.SetDeploymentVersion(deploymentVersion1)
			versioningInfo.SetVersioningOverride(workflowpb.VersioningOverride_builder{
				AutoUpgrade: proto.Bool(true),
			}.Build())
		} else {
			versioningInfo.SetVersion(dv1) //nolint:staticcheck // SA1019: worker versioning v0.31
			versioningInfo.SetVersioningOverride(workflowpb.VersioningOverride_builder{
				Behavior: enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE, //nolint:staticcheck // SA1019: worker versioning v0.31
				// Technically, API should not allow deployment to be set for AUTO_UPGRADE override, but we
				// test it this way to make sure it is ignored.
				PinnedVersion: dv2, //nolint:staticcheck // SA1019: worker versioning v0.31
			}.Build())
		}
		versioningInfo.SetBehavior(enumspb.VERSIONING_BEHAVIOR_PINNED)
		s.verifyEffectiveDeployment(deployment1, enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE)

		// deployment and behavior are not set, but override behavior is PINNED -> PINNED
		if useV32 {
			versioningInfo.ClearDeploymentVersion()
			versioningInfo.SetVersioningOverride(workflowpb.VersioningOverride_builder{
				Pinned: workflowpb.VersioningOverride_PinnedOverride_builder{
					Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
					Version:  deploymentVersion2,
				}.Build(),
			}.Build())
		} else {
			versioningInfo.SetVersion("") //nolint:staticcheck // SA1019: worker versioning v0.31
			versioningInfo.SetVersioningOverride(workflowpb.VersioningOverride_builder{
				Behavior:      enumspb.VERSIONING_BEHAVIOR_PINNED, //nolint:staticcheck // SA1019: worker versioning v0.31
				PinnedVersion: dv2,                                //nolint:staticcheck // SA1019: worker versioning v0.31
			}.Build())
		}
		versioningInfo.SetBehavior(enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED)
		s.verifyEffectiveDeployment(deployment2, enumspb.VERSIONING_BEHAVIOR_PINNED)

		// deployment is set, behavior is not, but override behavior is PINNED --> PINNED
		if useV32 {
			versioningInfo.SetDeploymentVersion(deploymentVersion1)
			versioningInfo.SetVersioningOverride(workflowpb.VersioningOverride_builder{
				Pinned: workflowpb.VersioningOverride_PinnedOverride_builder{
					Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
					Version:  deploymentVersion2,
				}.Build(),
			}.Build())
		} else {
			versioningInfo.SetVersion(dv1) //nolint:staticcheck // SA1019: worker versioning v0.31
			versioningInfo.SetVersioningOverride(workflowpb.VersioningOverride_builder{
				Behavior:      enumspb.VERSIONING_BEHAVIOR_PINNED, //nolint:staticcheck // SA1019: worker versioning v0.31
				PinnedVersion: dv2,                                //nolint:staticcheck // SA1019: worker versioning v0.31
			}.Build())
		}
		versioningInfo.SetBehavior(enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED)
		s.verifyEffectiveDeployment(deployment2, enumspb.VERSIONING_BEHAVIOR_PINNED)

		// worker says AUTO_UPGRADE, but override behavior is PINNED --> PINNED
		if useV32 {
			versioningInfo.SetDeploymentVersion(deploymentVersion1)
			versioningInfo.SetVersioningOverride(workflowpb.VersioningOverride_builder{
				Pinned: workflowpb.VersioningOverride_PinnedOverride_builder{
					Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
					Version:  deploymentVersion2,
				}.Build(),
			}.Build())
		} else {
			versioningInfo.SetVersion(dv1) //nolint:staticcheck // SA1019: worker versioning v0.31
			versioningInfo.SetVersioningOverride(workflowpb.VersioningOverride_builder{
				Behavior:      enumspb.VERSIONING_BEHAVIOR_PINNED, //nolint:staticcheck // SA1019: worker versioning v0.31
				PinnedVersion: dv2,                                //nolint:staticcheck // SA1019: worker versioning v0.31
			}.Build())
		}
		versioningInfo.SetBehavior(enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE)
		s.verifyEffectiveDeployment(deployment2, enumspb.VERSIONING_BEHAVIOR_PINNED)

		// ------- With transition

		if useV32 {
			versioningInfo.SetDeploymentVersion(deploymentVersion1)
			versioningInfo.SetVersioningOverride(workflowpb.VersioningOverride_builder{
				AutoUpgrade: proto.Bool(true),
			}.Build())
			versioningInfo.SetVersionTransition(workflowpb.DeploymentVersionTransition_builder{
				DeploymentVersion: deploymentVersion3,
			}.Build())
		} else {
			versioningInfo.SetVersion(dv1) //nolint:staticcheck // SA1019: worker versioning v0.31
			versioningInfo.SetVersioningOverride(workflowpb.VersioningOverride_builder{
				Behavior:      enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE, //nolint:staticcheck // SA1019: worker versioning v0.31
				PinnedVersion: dv2,                                      //nolint:staticcheck // SA1019: worker versioning v0.31
			}.Build())
			versioningInfo.SetVersionTransition(workflowpb.DeploymentVersionTransition_builder{
				Version: dv3, //nolint:staticcheck // SA1019: worker versioning v0.31
			}.Build())
		}
		versioningInfo.SetBehavior(enumspb.VERSIONING_BEHAVIOR_PINNED)
		s.verifyEffectiveDeployment(deployment3, enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE)

		if useV32 {
			versioningInfo.SetDeploymentVersion(deploymentVersion1)
			versioningInfo.SetVersioningOverride(workflowpb.VersioningOverride_builder{
				AutoUpgrade: proto.Bool(true),
			}.Build())
			versioningInfo.SetVersionTransition(workflowpb.DeploymentVersionTransition_builder{
				DeploymentVersion: nil,
			}.Build())
		} else {
			versioningInfo.SetVersion(dv1) //nolint:staticcheck // SA1019: worker versioning v0.31
			versioningInfo.SetVersioningOverride(workflowpb.VersioningOverride_builder{
				// Transitioning to unversioned
				Behavior:      enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE, //nolint:staticcheck // SA1019: worker versioning v0.31
				PinnedVersion: dv2,                                      //nolint:staticcheck // SA1019: worker versioning v0.31
			}.Build())
			versioningInfo.SetVersionTransition(workflowpb.DeploymentVersionTransition_builder{
				Version: "", //nolint:staticcheck // SA1019: worker versioning v0.31
			}.Build())
		}
		versioningInfo.SetBehavior(enumspb.VERSIONING_BEHAVIOR_PINNED)
		s.verifyEffectiveDeployment(nil, enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE)

		// clear for next round
		versioningInfo.ClearVersioningOverride()
		versioningInfo.ClearVersionTransition()
		versioningInfo.ClearDeploymentVersion()
		versioningInfo.SetVersion("") //nolint:staticcheck // SA1019: worker versioning v0.31
	}
}

func (s *mutableStateSuite) verifyEffectiveDeployment(
	expectedDeployment *deploymentpb.Deployment,
	expectedBehavior enumspb.VersioningBehavior,
) {
	if !s.mutableState.GetEffectiveDeployment().Equal(expectedDeployment) {
		s.Fail(fmt.Sprintf("expected: {%s}, actual: {%s}",
			expectedDeployment.String(),
			s.mutableState.GetEffectiveDeployment().String(),
		),
		)
	}
	s.Equal(expectedBehavior, s.mutableState.GetEffectiveVersioningBehavior())
}

// Creates a mutable state with first WFT completed on the given deployment and behavior set
// to the given behavior, testing expected output after Add, Start, and Complete Workflow Task.
func (s *mutableStateSuite) createMutableStateWithVersioningBehavior(
	behavior enumspb.VersioningBehavior,
	deployment *deploymentpb.Deployment,
	tq *taskqueuepb.TaskQueue,
) {
	version := int64(12)
	workflowID := "some random workflow ID"
	runID := uuid.NewString()

	s.mutableState = TestGlobalMutableState(
		s.mockShard,
		s.mockEventsCache,
		s.logger,
		version,
		workflowID,
		runID,
	)
	s.EqualValues(0, s.mutableState.executionInfo.GetWorkflowTaskStamp())
	s.mutableState.executionInfo.SetAttempt(5) // pretend we are in the middle workflow task retries

	wft, err := s.mutableState.AddWorkflowTaskScheduledEvent(true, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	s.NoError(err)
	s.verifyEffectiveDeployment(nil, enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED)

	err = s.mutableState.StartDeploymentTransition(deployment, 0)
	s.NoError(err)
	s.verifyEffectiveDeployment(deployment, enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE)

	_, wft, err = s.mutableState.AddWorkflowTaskStartedEvent(
		wft.ScheduledEventID,
		"",
		tq,
		"",
		nil,
		nil,
		nil,
		false,
		nil,
	)
	s.NoError(err)
	s.verifyEffectiveDeployment(deployment, enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE)

	_, err = s.mutableState.AddWorkflowTaskCompletedEvent(
		wft,
		workflowservice.RespondWorkflowTaskCompletedRequest_builder{
			VersioningBehavior: behavior,
			Deployment:         deployment, //nolint:staticcheck // SA1019: worker versioning v0.30
		}.Build(),
		workflowTaskCompletionLimits,
	)
	s.verifyEffectiveDeployment(deployment, behavior)
	s.NoError(err)
}

func (s *mutableStateSuite) TestPinnedFirstWorkflowTask() {
	tq := taskqueuepb.TaskQueue_builder{Name: "tq"}.Build()
	s.createMutableStateWithVersioningBehavior(enumspb.VERSIONING_BEHAVIOR_PINNED, deployment1, tq)
	s.verifyEffectiveDeployment(deployment1, enumspb.VERSIONING_BEHAVIOR_PINNED)
}

func (s *mutableStateSuite) TestUnpinnedFirstWorkflowTask() {
	tq := taskqueuepb.TaskQueue_builder{Name: "tq"}.Build()
	s.createMutableStateWithVersioningBehavior(enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE, deployment1, tq)
	s.verifyEffectiveDeployment(deployment1, enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE)
}

func (s *mutableStateSuite) TestUnpinnedTransition() {
	tq := taskqueuepb.TaskQueue_builder{Name: "tq"}.Build()
	behavior := enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE
	s.createMutableStateWithVersioningBehavior(behavior, deployment1, tq)

	wft, err := s.mutableState.AddWorkflowTaskScheduledEvent(true, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	s.NoError(err)
	s.verifyEffectiveDeployment(deployment1, behavior)

	err = s.mutableState.StartDeploymentTransition(deployment2, 0)
	s.NoError(err)
	s.verifyEffectiveDeployment(deployment2, behavior)

	_, wft, err = s.mutableState.AddWorkflowTaskStartedEvent(
		wft.ScheduledEventID,
		"",
		tq,
		"",
		nil,
		nil,
		nil,
		false,
		nil,
	)
	s.NoError(err)
	s.verifyEffectiveDeployment(deployment2, behavior)

	_, err = s.mutableState.AddWorkflowTaskCompletedEvent(
		wft,
		workflowservice.RespondWorkflowTaskCompletedRequest_builder{
			// wf is pinned in the new build
			VersioningBehavior: enumspb.VERSIONING_BEHAVIOR_PINNED,
			Deployment:         deployment2, //nolint:staticcheck // SA1019: worker versioning v0.30
		}.Build(),
		workflowTaskCompletionLimits,
	)
	s.verifyEffectiveDeployment(deployment2, enumspb.VERSIONING_BEHAVIOR_PINNED)
	s.NoError(err)
}

func (s *mutableStateSuite) TestUnpinnedTransitionFailed() {
	tq := taskqueuepb.TaskQueue_builder{Name: "tq"}.Build()
	behavior := enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE
	s.createMutableStateWithVersioningBehavior(behavior, deployment1, tq)

	wft, err := s.mutableState.AddWorkflowTaskScheduledEvent(true, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	s.NoError(err)
	s.verifyEffectiveDeployment(deployment1, behavior)

	err = s.mutableState.StartDeploymentTransition(deployment2, 0)
	s.NoError(err)
	s.verifyEffectiveDeployment(deployment2, behavior)

	_, wft, err = s.mutableState.AddWorkflowTaskStartedEvent(
		wft.ScheduledEventID,
		"",
		tq,
		"",
		nil,
		nil,
		nil,
		false,
		nil,
	)
	s.NoError(err)
	s.verifyEffectiveDeployment(deployment2, behavior)

	_, err = s.mutableState.AddWorkflowTaskFailedEvent(
		wft,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
		failure.NewServerFailure("some random workflow task failure details", false),
		"some random workflow task failure identity",
		nil,
		"",
		"",
		"",
		0,
	)
	s.NoError(err)
	// WFT failure does not fail transition
	s.verifyEffectiveDeployment(deployment2, behavior)
}

func (s *mutableStateSuite) TestUnpinnedTransitionTimeout() {
	tq := taskqueuepb.TaskQueue_builder{Name: "tq"}.Build()
	behavior := enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE
	s.createMutableStateWithVersioningBehavior(behavior, deployment1, tq)

	wft, err := s.mutableState.AddWorkflowTaskScheduledEvent(true, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	s.NoError(err)
	s.verifyEffectiveDeployment(deployment1, behavior)

	err = s.mutableState.StartDeploymentTransition(deployment2, 0)
	s.NoError(err)
	s.verifyEffectiveDeployment(deployment2, behavior)

	_, wft, err = s.mutableState.AddWorkflowTaskStartedEvent(
		wft.ScheduledEventID,
		"",
		tq,
		"",
		nil,
		nil,
		nil,
		false,
		nil,
	)
	s.NoError(err)
	s.verifyEffectiveDeployment(deployment2, behavior)

	_, err = s.mutableState.AddWorkflowTaskTimedOutEvent(wft)
	s.NoError(err)
	// WFT timeout does not fail transition
	s.verifyEffectiveDeployment(deployment2, behavior)
}

func (s *mutableStateSuite) verifyWorkflowOptionsUpdatedEventAttr(
	actualAttr *historypb.WorkflowExecutionOptionsUpdatedEventAttributes,
	expectedAttr *historypb.WorkflowExecutionOptionsUpdatedEventAttributes,
) {
	expectedOverride := worker_versioning.ConvertOverrideToV32(expectedAttr.GetVersioningOverride())
	actualOverride := actualAttr.GetVersioningOverride()
	s.Equal(expectedOverride.GetPinned().GetBehavior(), actualOverride.GetPinned().GetBehavior())
	s.Equal(expectedOverride.GetPinned().GetVersion().GetDeploymentName(), actualOverride.GetPinned().GetVersion().GetDeploymentName())
	s.Equal(expectedOverride.GetPinned().GetVersion().GetBuildId(), actualOverride.GetPinned().GetVersion().GetBuildId())
	s.Equal(expectedOverride.GetAutoUpgrade(), actualOverride.GetAutoUpgrade())

	s.Equal(expectedOverride.GetBehavior(), actualOverride.GetBehavior())                                       //nolint:staticcheck // SA1019: worker versioning v0.31
	s.Equal(expectedOverride.GetDeployment().GetSeriesName(), expectedOverride.GetDeployment().GetSeriesName()) //nolint:staticcheck // SA1019: worker versioning v0.30
	s.Equal(expectedOverride.GetDeployment().GetBuildId(), expectedOverride.GetDeployment().GetBuildId())       //nolint:staticcheck // SA1019: worker versioning v0.30
	s.Equal(expectedOverride.GetPinnedVersion(), actualOverride.GetPinnedVersion())                             //nolint:staticcheck // SA1019: worker versioning v0.31

	s.Equal(actualAttr.GetUnsetVersioningOverride(), expectedAttr.GetUnsetVersioningOverride())
	s.Equal(actualAttr.GetIdentity(), expectedAttr.GetIdentity())
}

func (s *mutableStateSuite) verifyOverrides(
	expectedBehavior, expectedBehaviorOverride enumspb.VersioningBehavior,
	expectedDeployment, expectedDeploymentOverride *deploymentpb.Deployment,
) {
	versioningInfo := s.mutableState.GetExecutionInfo().GetVersioningInfo()
	s.Equal(expectedBehavior, versioningInfo.GetBehavior())
	if versioningInfo.GetVersioningOverride().GetAutoUpgrade() {
		s.Equal(expectedBehaviorOverride, enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE)
	} else if versioningInfo.GetVersioningOverride().GetPinned().GetBehavior() == workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED {
		s.Equal(expectedBehaviorOverride, enumspb.VERSIONING_BEHAVIOR_PINNED)
	}
	expectedVersion := ""
	if expectedDeployment != nil {
		expectedVersion = worker_versioning.WorkerDeploymentVersionToStringV31(worker_versioning.DeploymentVersionFromDeployment(expectedDeployment))
	}
	s.Equal(expectedVersion, versioningInfo.GetVersion()) //nolint:staticcheck // SA1019: worker versioning v0.31
	var expectedPinnedDeploymentVersion *deploymentpb.WorkerDeploymentVersion
	if expectedDeploymentOverride != nil {
		expectedPinnedDeploymentVersion = worker_versioning.ExternalWorkerDeploymentVersionFromDeployment(expectedDeploymentOverride)
	}
	s.Equal(expectedPinnedDeploymentVersion.GetDeploymentName(), versioningInfo.GetVersioningOverride().GetPinned().GetVersion().GetDeploymentName())
	s.Equal(expectedPinnedDeploymentVersion.GetBuildId(), versioningInfo.GetVersioningOverride().GetPinned().GetVersion().GetBuildId())
}

func (s *mutableStateSuite) TestOverride_UnpinnedBase_SetPinnedAndUnsetWithEmptyOptions() {
	tq := taskqueuepb.TaskQueue_builder{Name: "tq"}.Build()
	baseBehavior := enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE
	overrideBehavior := enumspb.VERSIONING_BEHAVIOR_PINNED
	id := uuid.NewString()
	s.createMutableStateWithVersioningBehavior(baseBehavior, deployment1, tq)

	// set pinned override
	event, err := s.mutableState.AddWorkflowExecutionOptionsUpdatedEvent(pinnedOptions2.GetVersioningOverride(), false, "", nil, nil, id, nil)
	s.NoError(err)
	s.verifyEffectiveDeployment(deployment2, overrideBehavior)
	s.verifyWorkflowOptionsUpdatedEventAttr(
		event.GetWorkflowExecutionOptionsUpdatedEventAttributes(),
		historypb.WorkflowExecutionOptionsUpdatedEventAttributes_builder{
			VersioningOverride:      pinnedOptions2.GetVersioningOverride(),
			UnsetVersioningOverride: false,
			Identity:                id,
		}.Build(),
	)
	s.verifyOverrides(baseBehavior, overrideBehavior, deployment1, deployment2)

	// unset pinned override with boolean
	id = uuid.NewString()
	event, err = s.mutableState.AddWorkflowExecutionOptionsUpdatedEvent(nil, true, "", nil, nil, id, nil)
	s.NoError(err)
	s.verifyEffectiveDeployment(deployment1, baseBehavior)
	s.verifyWorkflowOptionsUpdatedEventAttr(
		event.GetWorkflowExecutionOptionsUpdatedEventAttributes(),
		historypb.WorkflowExecutionOptionsUpdatedEventAttributes_builder{
			VersioningOverride:      nil,
			UnsetVersioningOverride: true,
			Identity:                id,
		}.Build(),
	)
	s.verifyOverrides(baseBehavior, enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED, deployment1, nil)
}

func (s *mutableStateSuite) TestOverride_PinnedBase_SetUnpinnedAndUnsetWithEmptyOptions() {
	tq := taskqueuepb.TaskQueue_builder{Name: "tq"}.Build()
	baseBehavior := enumspb.VERSIONING_BEHAVIOR_PINNED
	overrideBehavior := enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE
	id := uuid.NewString()
	s.createMutableStateWithVersioningBehavior(baseBehavior, deployment1, tq)

	// set unpinned override
	event, err := s.mutableState.AddWorkflowExecutionOptionsUpdatedEvent(unpinnedOptions.GetVersioningOverride(), false, "", nil, nil, id, nil)
	s.NoError(err)
	s.verifyEffectiveDeployment(deployment1, overrideBehavior)
	s.verifyWorkflowOptionsUpdatedEventAttr(
		event.GetWorkflowExecutionOptionsUpdatedEventAttributes(),
		historypb.WorkflowExecutionOptionsUpdatedEventAttributes_builder{
			VersioningOverride:      unpinnedOptions.GetVersioningOverride(),
			UnsetVersioningOverride: false,
			Identity:                id,
		}.Build(),
	)
	s.verifyOverrides(baseBehavior, overrideBehavior, deployment1, nil)

	// unset pinned override with empty
	id = uuid.NewString()
	event, err = s.mutableState.AddWorkflowExecutionOptionsUpdatedEvent(nil, true, "", nil, nil, id, nil)
	s.NoError(err)
	s.verifyEffectiveDeployment(deployment1, baseBehavior)
	s.verifyWorkflowOptionsUpdatedEventAttr(
		event.GetWorkflowExecutionOptionsUpdatedEventAttributes(),
		historypb.WorkflowExecutionOptionsUpdatedEventAttributes_builder{
			VersioningOverride:      nil,
			UnsetVersioningOverride: true,
			Identity:                id,
		}.Build(),
	)
	s.verifyOverrides(baseBehavior, enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED, deployment1, nil)
}

func (s *mutableStateSuite) TestOverride_RedirectFails() {
	tq := taskqueuepb.TaskQueue_builder{Name: "tq"}.Build()
	baseBehavior := enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE
	overrideBehavior := enumspb.VERSIONING_BEHAVIOR_PINNED
	id := uuid.NewString()
	s.createMutableStateWithVersioningBehavior(baseBehavior, deployment1, tq)

	event, err := s.mutableState.AddWorkflowExecutionOptionsUpdatedEvent(pinnedOptions3.GetVersioningOverride(), false, "", nil, nil, id, nil)
	s.NoError(err)
	s.verifyEffectiveDeployment(deployment3, overrideBehavior)
	s.verifyWorkflowOptionsUpdatedEventAttr(
		event.GetWorkflowExecutionOptionsUpdatedEventAttributes(),
		historypb.WorkflowExecutionOptionsUpdatedEventAttributes_builder{
			VersioningOverride:      pinnedOptions3.GetVersioningOverride(),
			UnsetVersioningOverride: false,
			Identity:                id,
		}.Build(),
	)
	s.verifyOverrides(baseBehavior, overrideBehavior, deployment1, deployment3)

	// assert that transition fails
	err = s.mutableState.StartDeploymentTransition(deployment2, 0)
	s.ErrorIs(err, ErrPinnedWorkflowCannotTransition)
	s.verifyEffectiveDeployment(deployment3, overrideBehavior)
	s.verifyOverrides(baseBehavior, overrideBehavior, deployment1, deployment3)
}

func (s *mutableStateSuite) TestOverride_BaseDeploymentUpdatedOnCompletion() {
	tq := taskqueuepb.TaskQueue_builder{Name: "tq"}.Build()
	baseBehavior := enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE
	overrideBehavior := enumspb.VERSIONING_BEHAVIOR_PINNED
	id := uuid.NewString()
	s.createMutableStateWithVersioningBehavior(baseBehavior, deployment1, tq)

	event, err := s.mutableState.AddWorkflowExecutionOptionsUpdatedEvent(pinnedOptions3.GetVersioningOverride(), false, "", nil, nil, id, nil)
	s.NoError(err)
	s.verifyEffectiveDeployment(deployment3, overrideBehavior)
	s.verifyWorkflowOptionsUpdatedEventAttr(
		event.GetWorkflowExecutionOptionsUpdatedEventAttributes(),
		historypb.WorkflowExecutionOptionsUpdatedEventAttributes_builder{
			VersioningOverride:      pinnedOptions3.GetVersioningOverride(),
			UnsetVersioningOverride: false,
			Identity:                id,
		}.Build(),
	)
	s.verifyOverrides(baseBehavior, overrideBehavior, deployment1, deployment3)

	// assert that redirect fails - should be its own test
	err = s.mutableState.StartDeploymentTransition(deployment2, 0)
	s.ErrorIs(err, ErrPinnedWorkflowCannotTransition)
	s.verifyEffectiveDeployment(deployment3, overrideBehavior)
	s.verifyOverrides(baseBehavior, overrideBehavior, deployment1, deployment3) // base deployment still deployment1 here -- good

	wft, err := s.mutableState.AddWorkflowTaskScheduledEvent(true, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	s.NoError(err)
	s.verifyEffectiveDeployment(deployment3, overrideBehavior)

	_, wft, err = s.mutableState.AddWorkflowTaskStartedEvent(
		wft.ScheduledEventID,
		"",
		tq,
		"",
		nil,
		nil,
		nil,
		false,
		nil,
	)
	s.NoError(err)
	s.verifyEffectiveDeployment(deployment3, overrideBehavior)
	s.verifyOverrides(baseBehavior, overrideBehavior, deployment1, deployment3)

	_, err = s.mutableState.AddWorkflowTaskCompletedEvent(
		wft,
		workflowservice.RespondWorkflowTaskCompletedRequest_builder{
			// sdk says wf is unpinned, but that does not take effect due to the override
			VersioningBehavior: baseBehavior,
			Deployment:         deployment2,
		}.Build(),
		workflowTaskCompletionLimits,
	)
	s.NoError(err)
	s.verifyEffectiveDeployment(deployment3, overrideBehavior)
	s.verifyOverrides(baseBehavior, overrideBehavior, deployment2, deployment3)

	// now we unset the override and check that the base deployment/behavior is in effect
	id = uuid.NewString()
	event, err = s.mutableState.AddWorkflowExecutionOptionsUpdatedEvent(nil, true, "", nil, nil, id, nil)
	s.NoError(err)
	s.verifyEffectiveDeployment(deployment2, baseBehavior)
	s.verifyWorkflowOptionsUpdatedEventAttr(
		event.GetWorkflowExecutionOptionsUpdatedEventAttributes(),
		historypb.WorkflowExecutionOptionsUpdatedEventAttributes_builder{
			VersioningOverride:      nil,
			UnsetVersioningOverride: true,
			Identity:                id,
		}.Build(),
	)
	s.verifyOverrides(baseBehavior, enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED, deployment2, nil)
}

func (s *mutableStateSuite) TestChecksum() {
	// set the checksum probabilities to 100% for exercising during test
	s.mockConfig.MutableStateChecksumGenProbability = func(namespace string) int { return 100 }
	s.mockConfig.MutableStateChecksumVerifyProbability = func(namespace string) int { return 100 }

	testCases := []struct {
		name                 string
		enableBufferedEvents bool
		closeTxFunc          func(ms *MutableStateImpl) (*persistencespb.Checksum, error)
	}{
		{
			name: "closeTransactionAsSnapshot",
			closeTxFunc: func(ms *MutableStateImpl) (*persistencespb.Checksum, error) {
				snapshot, _, err := ms.CloseTransactionAsSnapshot(context.Background(), historyi.TransactionPolicyPassive)
				if err != nil {
					return nil, err
				}
				return snapshot.Checksum, err
			},
		},
		{
			name:                 "closeTransactionAsMutation",
			enableBufferedEvents: true,
			closeTxFunc: func(ms *MutableStateImpl) (*persistencespb.Checksum, error) {
				mutation, _, err := ms.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyPassive)
				if err != nil {
					return nil, err
				}
				return mutation.Checksum, err
			},
		},
	}

	loadErrorsFunc := func() int64 {
		counter := s.testScope.Snapshot().Counters()["test.mutable_state_checksum_mismatch+operation=WorkflowContext,service_name=history"]
		if counter != nil {
			return counter.Value()
		}
		return 0
	}

	var loadErrors int64

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			dbState := s.buildWorkflowMutableState()
			if !tc.enableBufferedEvents {
				dbState.SetBufferedEvents(nil)
			}

			// create mutable state and verify checksum is generated on close
			loadErrors = loadErrorsFunc()
			var err error
			s.mutableState, err = NewMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, tests.LocalNamespaceEntry, dbState, 123)
			s.NoError(err)
			s.Equal(loadErrors, loadErrorsFunc()) // no errors expected
			s.EqualValues(dbState.GetChecksum(), s.mutableState.checksum)
			s.mutableState.namespaceEntry = s.newNamespaceCacheEntry()
			csum, err := tc.closeTxFunc(s.mutableState)
			s.Nil(err)
			s.NotNil(csum.GetValue())
			s.Equal(enumsspb.CHECKSUM_FLAVOR_IEEE_CRC32_OVER_PROTO3_BINARY, csum.GetFlavor())
			s.Equal(mutableStateChecksumPayloadV1, csum.GetVersion())
			s.EqualValues(csum, s.mutableState.checksum)

			// verify checksum is verified on Load
			dbState.SetChecksum(csum)
			s.mutableState, err = NewMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, tests.LocalNamespaceEntry, dbState, 123)
			s.NoError(err)
			s.Equal(loadErrors, loadErrorsFunc())

			// generate checksum again and verify its the same
			csum, err = tc.closeTxFunc(s.mutableState)
			s.Nil(err)
			s.NotNil(csum.GetValue())
			s.Equal(dbState.GetChecksum().GetValue(), csum.GetValue())

			// modify checksum and verify Load fails
			dbState.GetChecksum().GetValue()[0]++
			s.mutableState, err = NewMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, tests.LocalNamespaceEntry, dbState, 123)
			s.NoError(err)
			s.Equal(loadErrors+1, loadErrorsFunc())
			s.EqualValues(dbState.GetChecksum(), s.mutableState.checksum)

			// test checksum is invalidated
			loadErrors = loadErrorsFunc()
			s.mockConfig.MutableStateChecksumInvalidateBefore = func() float64 {
				return float64((s.mutableState.executionInfo.GetLastUpdateTime().AsTime().UnixNano() / int64(time.Second)) + 1)
			}
			s.mutableState, err = NewMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, tests.LocalNamespaceEntry, dbState, 123)
			s.NoError(err)
			s.Equal(loadErrors, loadErrorsFunc())
			s.Nil(s.mutableState.checksum)

			// revert the config value for the next test case
			s.mockConfig.MutableStateChecksumInvalidateBefore = func() float64 {
				return float64(0)
			}
		})
	}
}

func (s *mutableStateSuite) TestChecksumProbabilities() {
	for _, prob := range []int{0, 100} {
		s.mockConfig.MutableStateChecksumGenProbability = func(namespace string) int { return prob }
		s.mockConfig.MutableStateChecksumVerifyProbability = func(namespace string) int { return prob }
		for i := 0; i < 100; i++ {
			shouldGenerate := s.mutableState.shouldGenerateChecksum()
			shouldVerify := s.mutableState.shouldVerifyChecksum()
			s.Equal(prob == 100, shouldGenerate)
			s.Equal(prob == 100, shouldVerify)
		}
	}
}

func (s *mutableStateSuite) TestChecksumShouldInvalidate() {
	s.mockConfig.MutableStateChecksumInvalidateBefore = func() float64 { return 0 }
	s.False(s.mutableState.shouldInvalidateCheckum())
	s.mutableState.executionInfo.SetLastUpdateTime(timestamp.TimeNowPtrUtc())
	s.mockConfig.MutableStateChecksumInvalidateBefore = func() float64 {
		return float64((s.mutableState.executionInfo.GetLastUpdateTime().AsTime().UnixNano() / int64(time.Second)) + 1)
	}
	s.True(s.mutableState.shouldInvalidateCheckum())
	s.mockConfig.MutableStateChecksumInvalidateBefore = func() float64 {
		return float64((s.mutableState.executionInfo.GetLastUpdateTime().AsTime().UnixNano() / int64(time.Second)) - 1)
	}
	s.False(s.mutableState.shouldInvalidateCheckum())
}

func (s *mutableStateSuite) TestUpdateWorkflowStateStatus_Table() {
	s.SetupSubTest()
	cases := []struct {
		name          string
		currentState  enumsspb.WorkflowExecutionState
		currentStatus enumspb.WorkflowExecutionStatus
		toState       enumsspb.WorkflowExecutionState
		toStatus      enumspb.WorkflowExecutionStatus
		wantErr       bool
	}{
		{
			name:         "created-> {running, running}",
			currentState: enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
			toState:      enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			wantErr:      false,
		},
		{
			name:         "created-> {running, paused}",
			currentState: enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
			toState:      enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED,
			wantErr:      false,
		},
		{
			name:         "created-> {running, completed}",
			currentState: enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
			toState:      enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			wantErr:      true,
		},
		// CREATED -> CREATED (allowed for RUNNING/PAUSED)
		{
			name:         "created-> {created, running}",
			currentState: enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
			toState:      enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
			toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			wantErr:      false,
		},
		{
			name:         "created-> {created, paused}",
			currentState: enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
			toState:      enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
			toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED,
			wantErr:      true,
		},
		{
			name:         "created-> {created, completed} (invalid)",
			currentState: enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
			toState:      enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
			toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			wantErr:      true,
		},
		// CREATED -> COMPLETED (allowed only for TERMINATED/TIMED_OUT/CONTINUED_AS_NEW)
		{
			name:         "created-> {completed, terminated}",
			currentState: enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
			toState:      enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			wantErr:      false,
		},
		{
			name:         "created-> {completed, timed_out}",
			currentState: enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
			toState:      enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
			wantErr:      false,
		},
		{
			name:         "created-> {completed, continued_as_new}",
			currentState: enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
			toState:      enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
			wantErr:      false,
		},
		// CREATED -> ZOMBIE (allowed for RUNNING/PAUSED)
		{
			name:         "created-> {zombie, running}",
			currentState: enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
			toState:      enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
			toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			wantErr:      false,
		},
		{
			name:         "created-> {zombie, paused}",
			currentState: enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
			toState:      enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
			toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED,
			wantErr:      false,
		},
		// RUNNING state transitions
		{
			name:         "running-> {created, running} (invalid)",
			currentState: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			toState:      enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
			toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			wantErr:      true,
		},
		{
			name:         "running-> {running, paused}",
			currentState: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			toState:      enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED,
			wantErr:      false,
		},
		{
			name:         "running-> {running, terminated} (invalid)",
			currentState: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			toState:      enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			wantErr:      true,
		},
		{
			name:         "running-> {completed, completed}",
			currentState: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			toState:      enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			wantErr:      false,
		},
		{
			name:         "running-> {completed, paused} (invalid)",
			currentState: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			toState:      enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED,
			wantErr:      true,
		},
		{
			name:         "running-> {zombie, running}",
			currentState: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			toState:      enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
			toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			wantErr:      false,
		},
		{
			name:         "running-> {zombie, paused}",
			currentState: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			toState:      enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
			toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED,
			wantErr:      false,
		},
		{
			name:         "running-> {zombie, terminated} (invalid)",
			currentState: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			toState:      enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
			toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			wantErr:      true,
		},
		// COMPLETED state transitions
		{
			name:          "completed-> {completed, sameStatus} (no-op)",
			currentState:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			currentStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			toState:       enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			toStatus:      enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			wantErr:       false,
		},
		{
			name:          "completed-> {created, running} (invalid)",
			currentState:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			currentStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			toState:       enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
			toStatus:      enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			wantErr:       true,
		},
		{
			name:          "completed-> {running, running} (invalid)",
			currentState:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			currentStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			toState:       enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			toStatus:      enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			wantErr:       true,
		},
		{
			name:          "completed-> {zombie, running} (invalid)",
			currentState:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			currentStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			toState:       enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
			toStatus:      enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			wantErr:       true,
		},
		{
			name:          "completed-> {completed, differentStatus} (invalid)",
			currentState:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			currentStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			toState:       enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			toStatus:      enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
			wantErr:       true,
		},
		// ZOMBIE state transitions
		{
			name:         "zombie-> {created, running}",
			currentState: enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
			toState:      enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
			toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			wantErr:      false,
		},
		{
			name:         "zombie-> {created, paused}",
			currentState: enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
			toState:      enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
			toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED,
			wantErr:      true,
		},
		{
			name:         "zombie-> {running, paused}",
			currentState: enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
			toState:      enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED,
			wantErr:      false,
		},
		{
			name:         "zombie-> {completed, terminated}",
			currentState: enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
			toState:      enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			wantErr:      false,
		},
		{
			name:         "zombie-> {completed, paused} (invalid)",
			currentState: enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
			toState:      enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED,
			wantErr:      true,
		},
		{
			name:         "zombie-> {zombie, running}",
			currentState: enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
			toState:      enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
			toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			wantErr:      false,
		},
		{
			name:         "zombie-> {zombie, terminated} (invalid)",
			currentState: enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
			toState:      enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
			toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			wantErr:      true,
		},
		// VOID state (no validation)
		{
			name:         "void-> {running, running}",
			currentState: enumsspb.WORKFLOW_EXECUTION_STATE_VOID,
			toState:      enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			toStatus:     enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			wantErr:      false,
		},
	}

	for _, c := range cases {
		s.Run(c.name, func() {
			s.SetupSubTest()
			s.mutableState.executionState.SetState(c.currentState)
			// default current status to RUNNING unless specified
			curStatus := c.currentStatus
			if curStatus == 0 {
				curStatus = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
			}
			s.mutableState.executionState.SetStatus(curStatus)
			_, err := s.mutableState.UpdateWorkflowStateStatus(c.toState, c.toStatus)
			if c.wantErr {
				s.Error(err)
			} else {
				s.NoError(err)
			}
			if !c.wantErr { // if the transition was successful, verify the state and status are updated.
				s.Equal(c.toState, s.mutableState.executionState.GetState())
				s.Equal(c.toStatus, s.mutableState.executionState.GetStatus())
			}
		})
	}
}

func (s *mutableStateSuite) TestAddWorkflowExecutionPausedEvent() {
	s.SetupSubTest()
	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

	tq := taskqueuepb.TaskQueue_builder{Name: "tq"}.Build()
	s.createVersionedMutableStateWithCompletedWFT(tq)

	// Complete another WFT to obtain a valid completed event id for scheduling an activity.
	wft, err := s.mutableState.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	s.NoError(err)
	_, wft, err = s.mutableState.AddWorkflowTaskStartedEvent(
		wft.ScheduledEventID,
		"",
		tq,
		"",
		worker_versioning.StampFromBuildId("b1"),
		nil,
		nil,
		false,
		nil,
	)
	s.NoError(err)
	completedEvent, err := s.mutableState.AddWorkflowTaskCompletedEvent(
		wft,
		&workflowservice.RespondWorkflowTaskCompletedRequest{},
		workflowTaskCompletionLimits,
	)
	s.NoError(err)

	// Schedule an activity (pending) using the completed WFT event id.
	_, activityInfo, err := s.mutableState.AddActivityTaskScheduledEvent(
		completedEvent.GetEventId(),
		commandpb.ScheduleActivityTaskCommandAttributes_builder{
			ActivityId:   "act-1",
			ActivityType: commonpb.ActivityType_builder{Name: "activity-type"}.Build(),
			TaskQueue:    tq,
		}.Build(),
		false,
	)
	s.NoError(err)
	prevActivityStamp := activityInfo.GetStamp()

	// Create a pending workflow task.
	pendingWFT, err := s.mutableState.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	s.NoError(err)
	prevWFTStamp := pendingWFT.Stamp

	// Pause and assert stamps incremented.
	pausedEvent, err := s.mutableState.AddWorkflowExecutionPausedEvent("tester", "reason", uuid.NewString())
	s.NoError(err)

	updatedActivityInfo, ok := s.mutableState.GetActivityInfo(activityInfo.GetScheduledEventId())
	s.True(ok)
	s.Greater(updatedActivityInfo.GetStamp(), prevActivityStamp)

	wftInfo := s.mutableState.GetPendingWorkflowTask()
	s.NotNil(wftInfo)
	s.Greater(wftInfo.Stamp, prevWFTStamp)

	// assert the event is marked as 'worker may ignore' so that older SDKs can safely ignore it.
	s.True(pausedEvent.GetWorkerMayIgnore())
}

func (s *mutableStateSuite) TestAddWorkflowExecutionUnpausedEvent() {
	s.SetupSubTest()
	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

	tq := taskqueuepb.TaskQueue_builder{Name: "tq"}.Build()
	s.createVersionedMutableStateWithCompletedWFT(tq)

	// Complete another WFT to obtain a valid completed event id for scheduling an activity.
	wft, err := s.mutableState.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	s.NoError(err)
	_, wft, err = s.mutableState.AddWorkflowTaskStartedEvent(
		wft.ScheduledEventID,
		"",
		tq,
		"",
		worker_versioning.StampFromBuildId("b1"),
		nil,
		nil,
		false,
		nil,
	)
	s.NoError(err)
	completedEvent, err := s.mutableState.AddWorkflowTaskCompletedEvent(
		wft,
		&workflowservice.RespondWorkflowTaskCompletedRequest{},
		workflowTaskCompletionLimits,
	)
	s.NoError(err)

	// Schedule an activity (pending) using the completed WFT event id.
	_, activityInfo, err := s.mutableState.AddActivityTaskScheduledEvent(
		completedEvent.GetEventId(),
		commandpb.ScheduleActivityTaskCommandAttributes_builder{
			ActivityId:   "act-1",
			ActivityType: commonpb.ActivityType_builder{Name: "activity-type"}.Build(),
			TaskQueue:    tq,
		}.Build(),
		false,
	)
	s.NoError(err)
	// Create a pending workflow task.
	pendingWFT, err := s.mutableState.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	s.NoError(err)

	// Pause first to simulate paused workflow state.
	_, err = s.mutableState.AddWorkflowExecutionPausedEvent("tester", "reason", uuid.NewString())
	s.NoError(err)

	// Capture stamps after pause.
	pausedActivityInfo, ok := s.mutableState.GetActivityInfo(activityInfo.GetScheduledEventId())
	s.True(ok)
	pausedActivityStamp := pausedActivityInfo.GetStamp()
	pausedWFT := s.mutableState.GetPendingWorkflowTask()
	s.NotNil(pausedWFT)
	pausedWFTStamp := pausedWFT.Stamp

	// Unpause and verify.
	unpausedEvent, err := s.mutableState.AddWorkflowExecutionUnpausedEvent("tester", "reason", uuid.NewString())
	s.NoError(err)

	// PauseInfo should be cleared and status should be RUNNING.
	s.Nil(s.mutableState.executionInfo.GetPauseInfo())
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, s.mutableState.executionState.GetStatus())

	// Stamps should be incremented again (only for activities) on unpause.
	updatedActivityInfo, ok := s.mutableState.GetActivityInfo(activityInfo.GetScheduledEventId())
	s.True(ok)
	s.Greater(updatedActivityInfo.GetStamp(), pausedActivityStamp)

	currentWFT := s.mutableState.GetPendingWorkflowTask()
	s.NotNil(currentWFT)
	s.Equal(currentWFT.Stamp, pausedWFTStamp) // workflow task stamp should not change between pause and unpause.

	// assert the event is marked as 'worker may ignore' so that older SDKs can safely ignore it.
	s.True(unpausedEvent.GetWorkerMayIgnore())

	// Ensure the pending workflow task we created earlier still exists (no unexpected removal).
	s.Equal(pendingWFT.ScheduledEventID, currentWFT.ScheduledEventID)
}

func (s *mutableStateSuite) TestPauseWorkflowExecution_FailStateValidation() {
	s.SetupSubTest()
	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

	// Simulate a completed workflow where transitioning status to PAUSED is invalid.
	s.mutableState.executionState.SetState(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED)
	s.mutableState.executionState.SetStatus(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED)
	prevStatus := s.mutableState.executionState.GetStatus()

	_, err := s.mutableState.AddWorkflowExecutionPausedEvent("tester", "test_reason", uuid.NewString())
	s.Error(err)
	// Status should remain unchanged and PauseInfo should not be set when validation fails.
	s.Equal(prevStatus, s.mutableState.executionState.GetStatus())
	s.Nil(s.mutableState.executionInfo.GetPauseInfo())
}

func (s *mutableStateSuite) TestContinueAsNewMinBackoff() {
	// set ContinueAsNew min interval to 5s
	s.mockConfig.WorkflowIdReuseMinimalInterval = func(namespace string) time.Duration {
		return 5 * time.Second
	}

	// with no backoff, verify min backoff is in [3s, 5s]
	minBackoff := s.mutableState.ContinueAsNewMinBackoff(nil).AsDuration()
	s.NotZero(minBackoff)
	s.True(minBackoff >= 3*time.Second)
	s.True(minBackoff <= 5*time.Second)

	// with 2s backoff, verify min backoff is in [3s, 5s]
	minBackoff = s.mutableState.ContinueAsNewMinBackoff(durationpb.New(time.Second * 2)).AsDuration()
	s.NotZero(minBackoff)
	s.True(minBackoff >= 3*time.Second)
	s.True(minBackoff <= 5*time.Second)

	// with 6s backoff, verify min backoff unchanged
	backoff := time.Second * 6
	minBackoff = s.mutableState.ContinueAsNewMinBackoff(durationpb.New(backoff)).AsDuration()
	s.NotZero(minBackoff)
	s.True(minBackoff == backoff)

	// set start time to be 3s ago
	startTime := timestamppb.New(time.Now().Add(-time.Second * 3))
	s.mutableState.executionInfo.SetStartTime(startTime)
	s.mutableState.executionInfo.SetExecutionTime(startTime)
	s.mutableState.executionState.SetStartTime(startTime)
	// with no backoff, verify min backoff is in [0, 2s]
	minBackoff = s.mutableState.ContinueAsNewMinBackoff(nil).AsDuration()
	s.NotNil(minBackoff)
	s.True(minBackoff >= 0)
	s.True(minBackoff <= 2*time.Second, "%v\n", minBackoff)

	// with 2s backoff, verify min backoff not changed
	backoff = time.Second * 2
	minBackoff = s.mutableState.ContinueAsNewMinBackoff(durationpb.New(backoff)).AsDuration()
	s.True(minBackoff == backoff)

	// set start time to be 5s ago
	startTime = timestamppb.New(time.Now().Add(-time.Second * 5))
	s.mutableState.executionInfo.SetStartTime(startTime)
	s.mutableState.executionInfo.SetExecutionTime(startTime)
	s.mutableState.executionState.SetStartTime(startTime)
	// with no backoff, verify backoff unchanged (no backoff needed)
	minBackoff = s.mutableState.ContinueAsNewMinBackoff(nil).AsDuration()
	s.Zero(minBackoff)

	// with 2s backoff, verify backoff unchanged
	backoff = time.Second * 2
	minBackoff = s.mutableState.ContinueAsNewMinBackoff(durationpb.New(backoff)).AsDuration()
	s.True(minBackoff == backoff)
}

func (s *mutableStateSuite) TestEventReapplied() {
	runID := uuid.NewString()
	eventID := int64(1)
	version := int64(2)
	dedupResource := definition.NewEventReappliedID(runID, eventID, version)
	isReapplied := s.mutableState.IsResourceDuplicated(dedupResource)
	s.False(isReapplied)
	s.mutableState.UpdateDuplicatedResource(dedupResource)
	isReapplied = s.mutableState.IsResourceDuplicated(dedupResource)
	s.True(isReapplied)
}

func (s *mutableStateSuite) TestTransientWorkflowTaskSchedule_CurrentVersionChanged() {
	version := int64(2000)
	workflowID := "some random workflow ID"
	runID := uuid.NewString()
	s.mutableState = TestGlobalMutableState(
		s.mockShard,
		s.mockEventsCache,
		s.logger,
		version,
		workflowID,
		runID,
	)
	_, _ = s.prepareTransientWorkflowTaskCompletionFirstBatchApplied(version, workflowID, runID)
	err := s.mutableState.ApplyWorkflowTaskFailedEvent()
	s.NoError(err)

	err = s.mutableState.UpdateCurrentVersion(version+1, true)
	s.NoError(err)
	versionHistories := s.mutableState.GetExecutionInfo().GetVersionHistories()
	versionHistory, err := versionhistory.GetCurrentVersionHistory(versionHistories)
	s.NoError(err)
	err = versionhistory.AddOrUpdateVersionHistoryItem(versionHistory, historyspb.VersionHistoryItem_builder{
		EventId: s.mutableState.GetNextEventID() - 1,
		Version: version,
	}.Build())
	s.NoError(err)

	wt, err := s.mutableState.AddWorkflowTaskScheduledEventAsHeartbeat(true, timestamp.TimeNowPtrUtc(), enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	s.NoError(err)
	s.NotNil(wt)

	s.Equal(int32(1), s.mutableState.GetExecutionInfo().GetWorkflowTaskAttempt())
	s.Equal(0, s.mutableState.hBuilder.NumBufferedEvents())
}

func (s *mutableStateSuite) TestTransientWorkflowTaskStart_CurrentVersionChanged() {
	version := int64(2000)
	workflowID := "some random workflow ID"
	runID := uuid.NewString()
	s.mutableState = TestGlobalMutableState(
		s.mockShard,
		s.mockEventsCache,
		s.logger,
		version,
		workflowID,
		runID,
	)
	_, _ = s.prepareTransientWorkflowTaskCompletionFirstBatchApplied(version, workflowID, runID)
	err := s.mutableState.ApplyWorkflowTaskFailedEvent()
	s.NoError(err)

	versionHistories := s.mutableState.GetExecutionInfo().GetVersionHistories()
	versionHistory, err := versionhistory.GetCurrentVersionHistory(versionHistories)
	s.NoError(err)
	err = versionhistory.AddOrUpdateVersionHistoryItem(versionHistory, historyspb.VersionHistoryItem_builder{
		EventId: s.mutableState.GetNextEventID() - 1,
		Version: version,
	}.Build())
	s.NoError(err)

	wt, err := s.mutableState.AddWorkflowTaskScheduledEventAsHeartbeat(true, timestamp.TimeNowPtrUtc(), enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	s.NoError(err)
	s.NotNil(wt)

	err = s.mutableState.UpdateCurrentVersion(version+1, true)
	s.NoError(err)

	f, err := tqid.NewTaskQueueFamily("", "tq")
	s.NoError(err)

	_, _, err = s.mutableState.AddWorkflowTaskStartedEvent(
		s.mutableState.GetNextEventID(),
		uuid.NewString(),
		taskqueuepb.TaskQueue_builder{Name: f.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).NormalPartition(5).RpcName()}.Build(),
		"random identity",
		nil,
		nil,
		nil,
		false,
		nil,
	)
	s.NoError(err)
	s.Equal(0, s.mutableState.hBuilder.NumBufferedEvents())

	mutation, err := s.mutableState.hBuilder.Finish(true)
	s.NoError(err)
	s.Equal(1, len(mutation.DBEventsBatches))
	s.Equal(2, len(mutation.DBEventsBatches[0]))
	attrs := mutation.DBEventsBatches[0][0].GetWorkflowTaskScheduledEventAttributes()
	s.NotNil(attrs)
	s.Equal("tq", attrs.GetTaskQueue().GetName())
}

func (s *mutableStateSuite) TestNewMutableStateInChain() {
	executionTimerTaskStatuses := []int32{
		TimerTaskStatusNone,
		TimerTaskStatusCreated,
	}

	for _, taskStatus := range executionTimerTaskStatuses {
		s.T().Run(
			fmt.Sprintf("TimerTaskStatus: %v", taskStatus),
			func(t *testing.T) {
				currentMutableState := TestGlobalMutableState(
					s.mockShard,
					s.mockEventsCache,
					s.logger,
					1000,
					tests.WorkflowID,
					uuid.NewString(),
				)
				currentMutableState.GetExecutionInfo().SetWorkflowExecutionTimerTaskStatus(taskStatus)

				newMutableState, err := NewMutableStateInChain(
					s.mockShard,
					s.mockEventsCache,
					s.logger,
					tests.GlobalNamespaceEntry,
					tests.WorkflowID,
					uuid.NewString(),
					s.mockShard.GetTimeSource().Now(),
					currentMutableState,
				)
				s.NoError(err)
				s.Equal(taskStatus, newMutableState.GetExecutionInfo().GetWorkflowExecutionTimerTaskStatus())
			},
		)
	}
}

func (s *mutableStateSuite) TestSanitizedMutableState() {
	txnID := int64(2000)
	runID := uuid.NewString()
	mutableState := TestGlobalMutableState(
		s.mockShard,
		s.mockEventsCache,
		s.logger,
		1000,
		tests.WorkflowID,
		runID,
	)

	mutableState.executionInfo.SetLastFirstEventTxnId(txnID)
	mutableState.executionInfo.SetParentClock(clockspb.VectorClock_builder{
		ShardId: 1,
		Clock:   1,
	}.Build())
	mutableState.pendingChildExecutionInfoIDs = map[int64]*persistencespb.ChildExecutionInfo{1: persistencespb.ChildExecutionInfo_builder{
		Clock: clockspb.VectorClock_builder{
			ShardId: 1,
			Clock:   1,
		}.Build(),
	}.Build()}
	mutableState.executionInfo.SetWorkflowExecutionTimerTaskStatus(TimerTaskStatusCreated)
	mutableState.executionInfo.SetTaskGenerationShardClockTimestamp(1000)

	stateMachineDef := hsmtest.NewDefinition("test")
	err := s.mockShard.StateMachineRegistry().RegisterMachine(stateMachineDef)
	s.NoError(err)
	_, err = mutableState.HSM().AddChild(hsm.Key{Type: stateMachineDef.Type(), ID: "child_1"}, hsmtest.NewData(hsmtest.State1))
	s.NoError(err)

	mutableStateProto := mutableState.CloneToProto()
	sanitizedMutableState, err := NewSanitizedMutableState(s.mockShard, s.mockEventsCache, s.logger, tests.LocalNamespaceEntry, mutableStateProto, 0)
	s.NoError(err)
	s.Equal(int64(0), sanitizedMutableState.executionInfo.GetLastFirstEventTxnId())
	s.Nil(sanitizedMutableState.executionInfo.GetParentClock())
	for _, childInfo := range sanitizedMutableState.pendingChildExecutionInfoIDs {
		s.Nil(childInfo.GetClock())
	}
	s.Equal(int32(TimerTaskStatusNone), sanitizedMutableState.executionInfo.GetWorkflowExecutionTimerTaskStatus())
	s.Zero(sanitizedMutableState.executionInfo.GetTaskGenerationShardClockTimestamp())
	err = sanitizedMutableState.HSM().Walk(func(node *hsm.Node) error {
		if node.Parent != nil {
			s.Equal(int64(1), node.InternalRepr().GetTransitionCount())
		}
		return nil
	})
	s.NoError(err)
}

func (s *mutableStateSuite) prepareTransientWorkflowTaskCompletionFirstBatchApplied(version int64, workflowID, runID string) (*historypb.HistoryEvent, *historypb.HistoryEvent) {
	namespaceID := tests.NamespaceID
	execution := commonpb.WorkflowExecution_builder{
		WorkflowId: workflowID,
		RunId:      runID,
	}.Build()

	now := time.Now().UTC()
	workflowType := "some random workflow type"
	taskqueue := "some random taskqueue"
	workflowTimeout := 222 * time.Second
	runTimeout := 111 * time.Second
	workflowTaskTimeout := 11 * time.Second
	workflowTaskAttempt := int32(1)

	eventID := int64(1)
	workflowStartEvent := historypb.HistoryEvent_builder{
		Version:   version,
		EventId:   eventID,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		WorkflowExecutionStartedEventAttributes: historypb.WorkflowExecutionStartedEventAttributes_builder{
			WorkflowType:             commonpb.WorkflowType_builder{Name: workflowType}.Build(),
			TaskQueue:                taskqueuepb.TaskQueue_builder{Name: taskqueue}.Build(),
			Input:                    nil,
			WorkflowExecutionTimeout: durationpb.New(workflowTimeout),
			WorkflowRunTimeout:       durationpb.New(runTimeout),
			WorkflowTaskTimeout:      durationpb.New(workflowTaskTimeout),
		}.Build(),
	}.Build()
	eventID++

	workflowTaskScheduleEvent := historypb.HistoryEvent_builder{
		Version:   version,
		EventId:   eventID,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		WorkflowTaskScheduledEventAttributes: historypb.WorkflowTaskScheduledEventAttributes_builder{
			TaskQueue:           taskqueuepb.TaskQueue_builder{Name: taskqueue}.Build(),
			StartToCloseTimeout: durationpb.New(workflowTaskTimeout),
			Attempt:             workflowTaskAttempt,
		}.Build(),
	}.Build()
	eventID++

	workflowTaskStartedEvent := historypb.HistoryEvent_builder{
		Version:   version,
		EventId:   eventID,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		WorkflowTaskStartedEventAttributes: historypb.WorkflowTaskStartedEventAttributes_builder{
			ScheduledEventId: workflowTaskScheduleEvent.GetEventId(),
			RequestId:        uuid.NewString(),
		}.Build(),
	}.Build()
	eventID++

	_ = historypb.HistoryEvent_builder{
		Version:   version,
		EventId:   eventID,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED,
		WorkflowTaskFailedEventAttributes: historypb.WorkflowTaskFailedEventAttributes_builder{
			ScheduledEventId: workflowTaskScheduleEvent.GetEventId(),
			StartedEventId:   workflowTaskStartedEvent.GetEventId(),
		}.Build(),
	}.Build()
	eventID++

	s.mockEventsCache.EXPECT().PutEvent(
		events.EventKey{
			NamespaceID: namespaceID,
			WorkflowID:  execution.GetWorkflowId(),
			RunID:       execution.GetRunId(),
			EventID:     workflowStartEvent.GetEventId(),
			Version:     version,
		},
		workflowStartEvent,
	)
	err := s.mutableState.ApplyWorkflowExecutionStartedEvent(
		nil,
		execution,
		uuid.NewString(),
		workflowStartEvent,
	)
	s.Nil(err)

	// setup transient workflow task
	wt, err := s.mutableState.ApplyWorkflowTaskScheduledEvent(
		workflowTaskScheduleEvent.GetVersion(),
		workflowTaskScheduleEvent.GetEventId(),
		workflowTaskScheduleEvent.GetWorkflowTaskScheduledEventAttributes().GetTaskQueue(),
		workflowTaskScheduleEvent.GetWorkflowTaskScheduledEventAttributes().GetStartToCloseTimeout(),
		workflowTaskScheduleEvent.GetWorkflowTaskScheduledEventAttributes().GetAttempt(),
		nil,
		nil,
		enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
	)
	s.Nil(err)
	s.NotNil(wt)

	wt, err = s.mutableState.ApplyWorkflowTaskStartedEvent(
		nil,
		workflowTaskStartedEvent.GetVersion(),
		workflowTaskScheduleEvent.GetEventId(),
		workflowTaskStartedEvent.GetEventId(),
		workflowTaskStartedEvent.GetWorkflowTaskStartedEventAttributes().GetRequestId(),
		timestamp.TimeValue(workflowTaskStartedEvent.GetEventTime()),
		false,
		123678,
		nil,
		int64(0),
		nil,
	)
	s.Nil(err)
	s.NotNil(wt)

	err = s.mutableState.ApplyWorkflowTaskFailedEvent()
	s.Nil(err)

	workflowTaskAttempt = int32(123)
	newWorkflowTaskScheduleEvent := historypb.HistoryEvent_builder{
		Version:   version,
		EventId:   eventID,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		WorkflowTaskScheduledEventAttributes: historypb.WorkflowTaskScheduledEventAttributes_builder{
			TaskQueue:           taskqueuepb.TaskQueue_builder{Name: taskqueue}.Build(),
			StartToCloseTimeout: durationpb.New(workflowTaskTimeout),
			Attempt:             workflowTaskAttempt,
		}.Build(),
	}.Build()
	eventID++

	newWorkflowTaskStartedEvent := historypb.HistoryEvent_builder{
		Version:   version,
		EventId:   eventID,
		EventTime: timestamppb.New(now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		WorkflowTaskStartedEventAttributes: historypb.WorkflowTaskStartedEventAttributes_builder{
			ScheduledEventId: workflowTaskScheduleEvent.GetEventId(),
			RequestId:        uuid.NewString(),
		}.Build(),
	}.Build()
	eventID++

	wt, err = s.mutableState.ApplyWorkflowTaskScheduledEvent(
		newWorkflowTaskScheduleEvent.GetVersion(),
		newWorkflowTaskScheduleEvent.GetEventId(),
		newWorkflowTaskScheduleEvent.GetWorkflowTaskScheduledEventAttributes().GetTaskQueue(),
		newWorkflowTaskScheduleEvent.GetWorkflowTaskScheduledEventAttributes().GetStartToCloseTimeout(),
		newWorkflowTaskScheduleEvent.GetWorkflowTaskScheduledEventAttributes().GetAttempt(),
		nil,
		nil,
		enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
	)
	s.Nil(err)
	s.NotNil(wt)

	wt, err = s.mutableState.ApplyWorkflowTaskStartedEvent(
		nil,
		newWorkflowTaskStartedEvent.GetVersion(),
		newWorkflowTaskScheduleEvent.GetEventId(),
		newWorkflowTaskStartedEvent.GetEventId(),
		newWorkflowTaskStartedEvent.GetWorkflowTaskStartedEventAttributes().GetRequestId(),
		timestamp.TimeValue(newWorkflowTaskStartedEvent.GetEventTime()),
		false,
		123678,
		nil,
		int64(0),
		nil,
	)
	s.Nil(err)
	s.NotNil(wt)

	s.mutableState.SetHistoryBuilder(historybuilder.NewImmutable([]*historypb.HistoryEvent{
		newWorkflowTaskScheduleEvent,
		newWorkflowTaskStartedEvent,
	}))
	_, _, err = s.mutableState.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyPassive)
	s.NoError(err)

	return newWorkflowTaskScheduleEvent, newWorkflowTaskStartedEvent
}

func (s *mutableStateSuite) newNamespaceCacheEntry() *namespace.Namespace {
	return namespace.NewNamespaceForTest(
		persistencespb.NamespaceInfo_builder{Name: "mutableStateTest"}.Build(),
		&persistencespb.NamespaceConfig{},
		true,
		&persistencespb.NamespaceReplicationConfig{},
		1,
	)
}

func (s *mutableStateSuite) buildWorkflowMutableState() *persistencespb.WorkflowMutableState {

	namespaceID := s.namespaceEntry.ID()
	we := commonpb.WorkflowExecution_builder{
		WorkflowId: "wId",
		RunId:      tests.RunID,
	}.Build()
	tl := "testTaskQueue"
	failoverVersion := s.namespaceEntry.FailoverVersion(tests.WorkflowID)

	startTime := timestamppb.New(time.Date(2020, 8, 22, 1, 2, 3, 4, time.UTC))
	info := persistencespb.WorkflowExecutionInfo_builder{
		NamespaceId:                             namespaceID.String(),
		WorkflowId:                              we.GetWorkflowId(),
		TaskQueue:                               tl,
		WorkflowTypeName:                        "wType",
		WorkflowRunTimeout:                      timestamp.DurationFromSeconds(200),
		DefaultWorkflowTaskTimeout:              timestamp.DurationFromSeconds(100),
		LastCompletedWorkflowTaskStartedEventId: int64(99),
		LastUpdateTime:                          timestamp.TimeNowPtrUtc(),
		ExecutionTime:                           startTime,
		WorkflowTaskVersion:                     failoverVersion,
		WorkflowTaskScheduledEventId:            101,
		WorkflowTaskStartedEventId:              102,
		WorkflowTaskTimeout:                     timestamp.DurationFromSeconds(100),
		WorkflowTaskAttempt:                     1,
		WorkflowTaskType:                        enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
		VersionHistories: historyspb.VersionHistories_builder{
			Histories: []*historyspb.VersionHistory{
				historyspb.VersionHistory_builder{
					BranchToken: []byte("token#1"),
					Items: []*historyspb.VersionHistoryItem{
						historyspb.VersionHistoryItem_builder{EventId: 102, Version: failoverVersion}.Build(),
					},
				}.Build(),
			},
		}.Build(),
		TransitionHistory: []*persistencespb.VersionedTransition{
			persistencespb.VersionedTransition_builder{
				NamespaceFailoverVersion: failoverVersion,
				TransitionCount:          1024,
			}.Build(),
		},
		FirstExecutionRunId:              uuid.NewString(),
		WorkflowExecutionTimerTaskStatus: TimerTaskStatusCreated,
	}.Build()

	state := persistencespb.WorkflowExecutionState_builder{
		RunId:     we.GetRunId(),
		State:     enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		StartTime: startTime,
	}.Build()

	activityInfos := map[int64]*persistencespb.ActivityInfo{
		90: persistencespb.ActivityInfo_builder{
			Version:                failoverVersion,
			ScheduledEventId:       int64(90),
			ScheduledTime:          timestamppb.New(time.Now().UTC()),
			StartedEventId:         common.EmptyEventID,
			StartedTime:            timestamppb.New(time.Now().UTC()),
			ActivityId:             "activityID_5",
			ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
			ScheduleToCloseTimeout: timestamp.DurationFromSeconds(200),
			StartToCloseTimeout:    timestamp.DurationFromSeconds(300),
			HeartbeatTimeout:       timestamp.DurationFromSeconds(50),
		}.Build(),
	}

	expiryTime := timestamp.TimeNowPtrUtcAddDuration(time.Hour)
	timerInfos := map[string]*persistencespb.TimerInfo{
		"25": persistencespb.TimerInfo_builder{
			Version:        failoverVersion,
			TimerId:        "25",
			StartedEventId: 85,
			ExpiryTime:     expiryTime,
		}.Build(),
	}

	childInfos := map[int64]*persistencespb.ChildExecutionInfo{
		80: persistencespb.ChildExecutionInfo_builder{
			Version:               failoverVersion,
			InitiatedEventId:      80,
			InitiatedEventBatchId: 20,
			StartedEventId:        common.EmptyEventID,
			CreateRequestId:       uuid.NewString(),
			Namespace:             tests.Namespace.String(),
			WorkflowTypeName:      "code.uber.internal/test/foobar",
		}.Build(),
	}

	requestCancelInfo := map[int64]*persistencespb.RequestCancelInfo{
		70: persistencespb.RequestCancelInfo_builder{
			Version:               failoverVersion,
			InitiatedEventBatchId: 20,
			CancelRequestId:       uuid.NewString(),
			InitiatedEventId:      70,
		}.Build(),
	}

	signalInfos := map[int64]*persistencespb.SignalInfo{
		75: persistencespb.SignalInfo_builder{
			Version:               failoverVersion,
			InitiatedEventId:      75,
			InitiatedEventBatchId: 17,
			RequestId:             uuid.NewString(),
		}.Build(),
	}

	signalRequestIDs := []string{
		"signal_request_id_1",
	}

	chasmNodes := map[string]*persistencespb.ChasmNode{
		"component-path": persistencespb.ChasmNode_builder{
			Metadata: persistencespb.ChasmNodeMetadata_builder{
				InitialVersionedTransition:    persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: failoverVersion, TransitionCount: 1}.Build(),
				LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: failoverVersion, TransitionCount: 90}.Build(),
				ComponentAttributes:           &persistencespb.ChasmComponentAttributes{},
			}.Build(),
			Data: commonpb.DataBlob_builder{Data: []byte("test-data")}.Build(),
		}.Build(),
		"component-path/collection": persistencespb.ChasmNode_builder{
			Metadata: persistencespb.ChasmNodeMetadata_builder{
				InitialVersionedTransition:    persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: failoverVersion, TransitionCount: 1}.Build(),
				LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: failoverVersion, TransitionCount: 90}.Build(),
				CollectionAttributes:          &persistencespb.ChasmCollectionAttributes{},
			}.Build(),
		}.Build(),
	}

	bufferedEvents := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{
			EventId:   common.BufferedEventID,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
			Version:   failoverVersion,
			WorkflowExecutionSignaledEventAttributes: historypb.WorkflowExecutionSignaledEventAttributes_builder{
				SignalName: "test-signal-buffered",
				Input:      payloads.EncodeString("test-signal-buffered-input"),
			}.Build(),
		}.Build(),
	}

	return persistencespb.WorkflowMutableState_builder{
		ExecutionInfo:       info,
		ExecutionState:      state,
		NextEventId:         int64(103),
		ActivityInfos:       activityInfos,
		TimerInfos:          timerInfos,
		ChildExecutionInfos: childInfos,
		RequestCancelInfos:  requestCancelInfo,
		SignalInfos:         signalInfos,
		ChasmNodes:          chasmNodes,
		SignalRequestedIds:  signalRequestIDs,
		BufferedEvents:      bufferedEvents,
	}.Build()
}

func (s *mutableStateSuite) TestUpdateInfos() {
	ctx := context.Background()
	cacheStore := map[events.EventKey]*historypb.HistoryEvent{}
	dbstate := s.buildWorkflowMutableState()
	var err error

	namespaceEntry := tests.GlobalNamespaceEntry
	s.mutableState, err = NewMutableStateFromDB(
		s.mockShard,
		NewMapEventCache(s.T(), cacheStore),
		s.logger,
		namespaceEntry,
		dbstate,
		123,
	)
	s.NoError(err)
	err = s.mutableState.UpdateCurrentVersion(namespaceEntry.FailoverVersion(tests.WorkflowID), false)
	s.NoError(err)

	// 1st accepted update (without acceptedRequest)
	updateID1 := fmt.Sprintf("%s-1-accepted-update-id", s.T().Name())
	acptEvent1, err := s.mutableState.AddWorkflowExecutionUpdateAcceptedEvent(
		updateID1,
		fmt.Sprintf("%s-1-accepted-msg-id", s.T().Name()),
		1,
		nil) // no acceptedRequest!
	s.NoError(err)
	s.NotNil(acptEvent1)

	// 2nd accepted update (with acceptedRequest)
	updateID2 := fmt.Sprintf("%s-2-accepted-update-id", s.T().Name())
	acptEvent2, err := s.mutableState.AddWorkflowExecutionUpdateAcceptedEvent(
		updateID2,
		fmt.Sprintf("%s-2-accepted-msg-id", s.T().Name()),
		1,
		updatepb.Request_builder{Meta: updatepb.Meta_builder{UpdateId: updateID2}.Build()}.Build())
	s.NoError(err)
	s.NotNil(acptEvent2)

	_, err = s.mutableState.AddWorkflowExecutionUpdateCompletedEvent(
		1234,
		updatepb.Response_builder{
			Meta: updatepb.Meta_builder{UpdateId: s.T().Name() + "-completed-update-without-accepted-event"}.Build(),
			Outcome: updatepb.Outcome_builder{
				Success: proto.ValueOrDefault(testPayloads),
			}.Build(),
		}.Build(),
	)
	s.Error(err)

	completedEvent, err := s.mutableState.AddWorkflowExecutionUpdateCompletedEvent(
		acptEvent1.GetEventId(),
		updatepb.Response_builder{
			Meta: updatepb.Meta_builder{UpdateId: updateID1}.Build(),
			Outcome: updatepb.Outcome_builder{
				Success: proto.ValueOrDefault(testPayloads),
			}.Build(),
		}.Build(),
	)
	s.NoError(err)
	s.NotNil(completedEvent)

	s.Len(cacheStore, 3, "expected 1 UpdateCompleted event + 2 UpdateAccepted events in cache")

	numCompleted := 0
	numAccepted := 0
	s.mutableState.VisitUpdates(func(updID string, updInfo *persistencespb.UpdateInfo) {
		s.Contains([]string{updateID1, updateID2}, updID)
		switch {
		case updInfo.GetCompletion() != nil:
			numCompleted++
		case updInfo.GetAcceptance() != nil:
			numAccepted++
		}
	})
	s.Equal(numCompleted, 1, "expected 1 completed")
	s.Equal(numAccepted, 1, "expected 1 accepted")

	s.mockShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(
		namespaceEntry.IsGlobalNamespace(),
		namespaceEntry.FailoverVersion(tests.WorkflowID),
	).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	mutation, _, err := s.mutableState.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
	s.NoError(err)
	s.Len(mutation.ExecutionInfo.GetUpdateInfos(), 2,
		"expected 1 completed update + 1 accepted in mutation")

	// this must be done after the transaction is closed
	// as GetUpdateOutcome relies on event version history which is only updated when closing the transaction
	outcome, err := s.mutableState.GetUpdateOutcome(ctx, completedEvent.GetWorkflowExecutionUpdateCompletedEventAttributes().GetMeta().GetUpdateId())
	s.NoError(err)
	s.Equal(completedEvent.GetWorkflowExecutionUpdateCompletedEventAttributes().GetOutcome(), outcome)

	_, err = s.mutableState.GetUpdateOutcome(ctx, "not_an_update_id")
	s.Error(err)
	s.IsType((*serviceerror.NotFound)(nil), err)
}

func (s *mutableStateSuite) TestApplyActivityTaskStartedEvent() {
	state := s.buildWorkflowMutableState()

	var err error
	s.mutableState, err = NewMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, tests.LocalNamespaceEntry, state, 123)
	s.NoError(err)

	var scheduledEventID int64
	var ai *persistencespb.ActivityInfo
	for scheduledEventID, ai = range s.mutableState.GetPendingActivityInfos() {
		break
	}
	s.Nil(ai.GetLastHeartbeatDetails())

	now := time.Now().UTC()
	version := int64(101)
	requestID := "102"
	eventID := int64(104)
	attributes := historypb.ActivityTaskStartedEventAttributes_builder{
		ScheduledEventId: scheduledEventID,
		RequestId:        requestID,
	}.Build()
	err = s.mutableState.ApplyActivityTaskStartedEvent(historypb.HistoryEvent_builder{
		EventId:                            eventID,
		EventTime:                          timestamppb.New(now),
		Version:                            version,
		ActivityTaskStartedEventAttributes: proto.ValueOrDefault(attributes),
	}.Build())
	s.NoError(err)
	s.Assert().Equal(version, ai.GetVersion())
	s.Assert().Equal(eventID, ai.GetStartedEventId())
	s.NotNil(ai.GetStartedTime())
	s.Assert().Equal(now, ai.GetStartedTime().AsTime())
	s.Assert().Equal(requestID, ai.GetRequestId())
	s.Assert().Nil(ai.GetLastHeartbeatDetails())
}

func (s *mutableStateSuite) TestAddContinueAsNewEvent_Default() {
	dbState := s.buildWorkflowMutableState()
	dbState.SetBufferedEvents(nil)

	var err error
	s.mutableState, err = NewMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, tests.LocalNamespaceEntry, dbState, 123)
	s.NoError(err)

	workflowTaskInfo := s.mutableState.GetStartedWorkflowTask()
	workflowTaskCompletedEvent, err := s.mutableState.AddWorkflowTaskCompletedEvent(
		workflowTaskInfo,
		&workflowservice.RespondWorkflowTaskCompletedRequest{},
		workflowTaskCompletionLimits,
	)
	s.NoError(err)

	coll := callbacks.MachineCollection(s.mutableState.HSM())
	_, err = coll.Add(
		"test-callback-carryover",
		callbacks.NewCallback(
			"random-request-id",
			timestamppb.Now(),
			callbacks.NewWorkflowClosedTrigger(),
			persistencespb.Callback_builder{
				Nexus: persistencespb.Callback_Nexus_builder{
					Url: "test-callback-carryover-url",
				}.Build(),
			}.Build(),
		),
	)
	s.NoError(err)

	s.mockEventsCache.EXPECT().GetEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(&historypb.HistoryEvent{}, nil)
	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).Times(2)
	_, newRunMutableState, err := s.mutableState.AddContinueAsNewEvent(
		context.Background(),
		workflowTaskCompletedEvent.GetEventId(),
		workflowTaskCompletedEvent.GetEventId(),
		"",
		commandpb.ContinueAsNewWorkflowExecutionCommandAttributes_builder{
			// All other fields will default to those in the current run.
			WorkflowRunTimeout: s.mutableState.GetExecutionInfo().GetWorkflowRunTimeout(),
		}.Build(),
		nil,
	)
	s.NoError(err)

	newColl := callbacks.MachineCollection(newRunMutableState.HSM())
	s.Equal(1, newColl.Size())

	currentRunExecutionInfo := s.mutableState.GetExecutionInfo()
	newRunExecutionInfo := newRunMutableState.GetExecutionInfo()
	s.Equal(currentRunExecutionInfo.GetTaskQueue(), newRunExecutionInfo.GetTaskQueue())
	s.Equal(currentRunExecutionInfo.GetWorkflowTypeName(), newRunExecutionInfo.GetWorkflowTypeName())
	protorequire.ProtoEqual(s.T(), currentRunExecutionInfo.GetDefaultWorkflowTaskTimeout(), newRunExecutionInfo.GetDefaultWorkflowTaskTimeout())
	protorequire.ProtoEqual(s.T(), currentRunExecutionInfo.GetWorkflowRunTimeout(), newRunExecutionInfo.GetWorkflowRunTimeout())
	protorequire.ProtoEqual(s.T(), currentRunExecutionInfo.GetWorkflowExecutionExpirationTime(), newRunExecutionInfo.GetWorkflowExecutionExpirationTime())
	s.Equal(currentRunExecutionInfo.GetWorkflowExecutionTimerTaskStatus(), newRunExecutionInfo.GetWorkflowExecutionTimerTaskStatus())
	s.Equal(currentRunExecutionInfo.GetFirstExecutionRunId(), newRunExecutionInfo.GetFirstExecutionRunId())

	// Add more checks here if needed.
}

func (s *mutableStateSuite) TestTotalEntitiesCount() {
	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

	// scheduling, starting & completing workflow task is omitted here

	workflowTaskCompletedEventID := int64(4)
	_, _, err := s.mutableState.AddActivityTaskScheduledEvent(
		workflowTaskCompletedEventID,
		&commandpb.ScheduleActivityTaskCommandAttributes{},
		false,
	)
	s.NoError(err)

	_, _, err = s.mutableState.AddStartChildWorkflowExecutionInitiatedEvent(
		workflowTaskCompletedEventID,
		&commandpb.StartChildWorkflowExecutionCommandAttributes{},
		namespace.ID(uuid.NewString()),
	)
	s.NoError(err)

	_, _, err = s.mutableState.AddTimerStartedEvent(
		workflowTaskCompletedEventID,
		&commandpb.StartTimerCommandAttributes{},
	)
	s.NoError(err)

	_, _, err = s.mutableState.AddRequestCancelExternalWorkflowExecutionInitiatedEvent(
		workflowTaskCompletedEventID,
		uuid.NewString(),
		&commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes{},
		namespace.ID(uuid.NewString()),
	)
	s.NoError(err)

	_, _, err = s.mutableState.AddSignalExternalWorkflowExecutionInitiatedEvent(
		workflowTaskCompletedEventID,
		uuid.NewString(),
		commandpb.SignalExternalWorkflowExecutionCommandAttributes_builder{
			Execution: commonpb.WorkflowExecution_builder{
				WorkflowId: tests.WorkflowID,
				RunId:      tests.RunID,
			}.Build(),
		}.Build(),
		namespace.ID(uuid.NewString()),
	)
	s.NoError(err)

	updateID := "random-updateId"
	accptEvent, err := s.mutableState.AddWorkflowExecutionUpdateAcceptedEvent(
		updateID, "random", 0, nil)
	s.NoError(err)
	s.NotNil(accptEvent)

	completedEvent, err := s.mutableState.AddWorkflowExecutionUpdateCompletedEvent(
		accptEvent.GetEventId(), updatepb.Response_builder{Meta: updatepb.Meta_builder{UpdateId: updateID}.Build()}.Build())
	s.NoError(err)
	s.NotNil(completedEvent)

	_, err = s.mutableState.AddWorkflowExecutionSignaled(
		"signalName",
		&commonpb.Payloads{},
		"identity",
		&commonpb.Header{},
		nil,
	)
	s.NoError(err)

	mutation, _, err := s.mutableState.CloseTransactionAsMutation(
		context.Background(),
		historyi.TransactionPolicyActive,
	)
	s.NoError(err)

	s.Equal(int64(1), mutation.ExecutionInfo.GetActivityCount())
	s.Equal(int64(1), mutation.ExecutionInfo.GetChildExecutionCount())
	s.Equal(int64(1), mutation.ExecutionInfo.GetUserTimerCount())
	s.Equal(int64(1), mutation.ExecutionInfo.GetRequestCancelExternalCount())
	s.Equal(int64(1), mutation.ExecutionInfo.GetSignalExternalCount())
	s.Equal(int64(1), mutation.ExecutionInfo.GetSignalCount())
	s.Equal(int64(1), mutation.ExecutionInfo.GetUpdateCount())
}

func (s *mutableStateSuite) TestSpeculativeWorkflowTaskNotPersisted() {
	testCases := []struct {
		name                 string
		enableBufferedEvents bool
		closeTxFunc          func(ms *MutableStateImpl) (*persistencespb.WorkflowExecutionInfo, error)
	}{
		{
			name: "CloseTransactionAsSnapshot",
			closeTxFunc: func(ms *MutableStateImpl) (*persistencespb.WorkflowExecutionInfo, error) {
				snapshot, _, err := ms.CloseTransactionAsSnapshot(context.Background(), historyi.TransactionPolicyActive)
				if err != nil {
					return nil, err
				}
				return snapshot.ExecutionInfo, err
			},
		},
		{
			name:                 "CloseTransactionAsMutation",
			enableBufferedEvents: true,
			closeTxFunc: func(ms *MutableStateImpl) (*persistencespb.WorkflowExecutionInfo, error) {
				mutation, _, err := ms.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
				if err != nil {
					return nil, err
				}
				return mutation.ExecutionInfo, err
			},
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			dbState := s.buildWorkflowMutableState()
			if !tc.enableBufferedEvents {
				dbState.SetBufferedEvents(nil)
			}

			var err error
			namespaceEntry := tests.GlobalNamespaceEntry
			s.mutableState, err = NewMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, namespaceEntry, dbState, 123)
			s.NoError(err)
			err = s.mutableState.UpdateCurrentVersion(namespaceEntry.FailoverVersion(tests.WorkflowID), false)
			s.NoError(err)

			s.mutableState.executionInfo.SetWorkflowTaskScheduledEventId(s.mutableState.GetNextEventID())
			s.mutableState.executionInfo.SetWorkflowTaskStartedEventId(s.mutableState.GetNextEventID() + 1)

			s.mockShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(
				namespaceEntry.IsGlobalNamespace(),
				namespaceEntry.FailoverVersion(tests.WorkflowID),
			).Return(cluster.TestCurrentClusterName).AnyTimes()
			s.mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

			// Normal WT is persisted as is.
			execInfo, err := tc.closeTxFunc(s.mutableState)
			s.Nil(err)
			s.Equal(enumsspb.WORKFLOW_TASK_TYPE_NORMAL, execInfo.GetWorkflowTaskType())
			s.NotEqual(common.EmptyEventID, execInfo.GetWorkflowTaskScheduledEventId())
			s.NotEqual(common.EmptyEventID, execInfo.GetWorkflowTaskStartedEventId())

			s.mutableState.executionInfo.SetWorkflowTaskType(enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE)

			// Speculative WT is converted to normal.
			execInfo, err = tc.closeTxFunc(s.mutableState)
			s.Nil(err)
			s.Equal(enumsspb.WORKFLOW_TASK_TYPE_NORMAL, execInfo.GetWorkflowTaskType())
			s.NotEqual(common.EmptyEventID, execInfo.GetWorkflowTaskScheduledEventId())
			s.NotEqual(common.EmptyEventID, execInfo.GetWorkflowTaskStartedEventId())
		})
	}
}

func (s *mutableStateSuite) TestRetryWorkflowTask_WithNextRetryDelay() {
	expectedDelayDuration := time.Minute
	s.mutableState.executionInfo.SetHasRetryPolicy(true)
	applicationFailure := failurepb.Failure_builder{
		Message: "application failure with customized next retry delay",
		Source:  "application",
		ApplicationFailureInfo: failurepb.ApplicationFailureInfo_builder{
			Type:           "application-failure-type",
			NonRetryable:   false,
			NextRetryDelay: durationpb.New(expectedDelayDuration),
		}.Build(),
	}.Build()

	duration, retryState := s.mutableState.GetRetryBackoffDuration(applicationFailure)
	s.Equal(enumspb.RETRY_STATE_IN_PROGRESS, retryState)
	s.Equal(duration, expectedDelayDuration)
}
func (s *mutableStateSuite) TestRetryActivity_TruncateRetryableFailure() {
	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
	s.mockConfig.EnableActivityRetryStampIncrement = dynamicconfig.GetBoolPropertyFn(true)

	// scheduling, starting & completing workflow task is omitted here

	workflowTaskCompletedEventID := int64(4)
	_, activityInfo, err := s.mutableState.AddActivityTaskScheduledEvent(
		workflowTaskCompletedEventID,
		commandpb.ScheduleActivityTaskCommandAttributes_builder{
			ActivityId:   "5",
			ActivityType: commonpb.ActivityType_builder{Name: "activity-type"}.Build(),
			TaskQueue:    taskqueuepb.TaskQueue_builder{Name: "task-queue"}.Build(),
			RetryPolicy: commonpb.RetryPolicy_builder{
				InitialInterval: timestamp.DurationFromSeconds(1),
			}.Build(),
		}.Build(),
		false,
	)
	s.NoError(err)

	_, err = s.mutableState.AddActivityTaskStartedEvent(
		activityInfo,
		activityInfo.GetScheduledEventId(),
		uuid.NewString(),
		"worker-identity",
		nil,
		nil,
		nil,
	)
	s.NoError(err)

	failureSizeErrorLimit := s.mockConfig.MutableStateActivityFailureSizeLimitError(
		s.mutableState.namespaceEntry.Name().String(),
	)

	activityFailure := failurepb.Failure_builder{
		Message: "activity failure with large details",
		Source:  "application",
		ApplicationFailureInfo: failurepb.ApplicationFailureInfo_builder{
			Type:         "application-failure-type",
			NonRetryable: false,
			Details: commonpb.Payloads_builder{
				Payloads: []*commonpb.Payload{
					commonpb.Payload_builder{
						Data: make([]byte, failureSizeErrorLimit*2),
					}.Build(),
				},
			}.Build(),
		}.Build(),
	}.Build()
	s.Greater(activityFailure.Size(), failureSizeErrorLimit)

	prevStamp := activityInfo.GetStamp()

	retryState, err := s.mutableState.RetryActivity(activityInfo, activityFailure)
	s.NoError(err)
	s.Equal(enumspb.RETRY_STATE_IN_PROGRESS, retryState)

	activityInfo, ok := s.mutableState.GetActivityInfo(activityInfo.GetScheduledEventId())
	s.True(ok)
	s.Greater(activityInfo.GetStamp(), prevStamp)
	s.Equal(int32(2), activityInfo.GetAttempt())
	s.LessOrEqual(activityInfo.GetRetryLastFailure().Size(), failureSizeErrorLimit)
	s.Equal(activityFailure.GetMessage(), activityInfo.GetRetryLastFailure().GetCause().GetMessage())
}

func (s *mutableStateSuite) TestRetryActivity_PausedIncrementsStamp() {
	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
	s.mockConfig.EnableActivityRetryStampIncrement = dynamicconfig.GetBoolPropertyFn(true)

	workflowTaskCompletedEventID := int64(4)
	_, activityInfo, err := s.mutableState.AddActivityTaskScheduledEvent(
		workflowTaskCompletedEventID,
		commandpb.ScheduleActivityTaskCommandAttributes_builder{
			ActivityId:   "6",
			ActivityType: commonpb.ActivityType_builder{Name: "activity-type"}.Build(),
			TaskQueue:    taskqueuepb.TaskQueue_builder{Name: "task-queue"}.Build(),
			RetryPolicy: commonpb.RetryPolicy_builder{
				InitialInterval: timestamp.DurationFromSeconds(1),
			}.Build(),
		}.Build(),
		false,
	)
	s.NoError(err)

	_, err = s.mutableState.AddActivityTaskStartedEvent(
		activityInfo,
		activityInfo.GetScheduledEventId(),
		uuid.NewString(),
		"worker-identity",
		nil,
		nil,
		nil,
	)
	s.NoError(err)

	activityInfo.SetPaused(true)
	prevStamp := activityInfo.GetStamp()

	retryState, err := s.mutableState.RetryActivity(activityInfo, failurepb.Failure_builder{Message: "activity failure"}.Build())
	s.NoError(err)
	s.Equal(enumspb.RETRY_STATE_IN_PROGRESS, retryState)

	updatedActivityInfo, ok := s.mutableState.GetActivityInfo(activityInfo.GetScheduledEventId())
	s.True(ok)
	s.Greater(updatedActivityInfo.GetStamp(), prevStamp)
	s.Equal(int32(2), updatedActivityInfo.GetAttempt())
}

func (s *mutableStateSuite) TestupdateBuildIdsAndDeploymentSearchAttributes() {
	versioned := func(buildId string) *commonpb.WorkerVersionStamp {
		return commonpb.WorkerVersionStamp_builder{BuildId: buildId, UseVersioning: true}.Build()
	}
	versionedSearchAttribute := func(buildIds ...string) []string {
		attrs := []string{}
		for _, buildId := range buildIds {
			attrs = append(attrs, worker_versioning.VersionedBuildIdSearchAttribute(buildId))
		}
		return attrs
	}
	unversioned := func(buildId string) *commonpb.WorkerVersionStamp {
		return commonpb.WorkerVersionStamp_builder{BuildId: buildId, UseVersioning: false}.Build()
	}
	unversionedSearchAttribute := func(buildIds ...string) []string {
		// assumed limit is 2
		attrs := []string{worker_versioning.UnversionedSearchAttribute, worker_versioning.UnversionedBuildIdSearchAttribute(buildIds[len(buildIds)-1])}
		return attrs
	}

	type testCase struct {
		name            string
		searchAttribute func(buildIds ...string) []string
		stamp           func(buildId string) *commonpb.WorkerVersionStamp
	}
	matrix := []testCase{
		{name: "unversioned", searchAttribute: unversionedSearchAttribute, stamp: unversioned},
		{name: "versioned", searchAttribute: versionedSearchAttribute, stamp: versioned},
	}
	for _, c := range matrix {
		s.T().Run(c.name, func(t *testing.T) {
			dbState := s.buildWorkflowMutableState()
			var err error
			s.mutableState, err = NewMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, tests.LocalNamespaceEntry, dbState, 123)
			s.NoError(err)

			// Max 0
			err = s.mutableState.updateBuildIdsAndDeploymentSearchAttributes(c.stamp("0.1"), nil, 0)
			s.NoError(err)
			s.Equal([]string{}, s.getBuildIdsFromMutableState())

			err = s.mutableState.updateBuildIdsAndDeploymentSearchAttributes(c.stamp("0.1"), nil, 40)
			s.NoError(err)
			s.Equal(c.searchAttribute("0.1"), s.getBuildIdsFromMutableState())

			// Add the same build ID
			err = s.mutableState.updateBuildIdsAndDeploymentSearchAttributes(c.stamp("0.1"), nil, 40)
			s.NoError(err)
			s.Equal(c.searchAttribute("0.1"), s.getBuildIdsFromMutableState())

			err = s.mutableState.updateBuildIdsAndDeploymentSearchAttributes(c.stamp("0.2"), nil, 40)
			s.NoError(err)
			s.Equal(c.searchAttribute("0.1", "0.2"), s.getBuildIdsFromMutableState())

			// Limit applies
			err = s.mutableState.updateBuildIdsAndDeploymentSearchAttributes(c.stamp("0.3"), nil, 40)
			s.NoError(err)
			s.Equal(c.searchAttribute("0.2", "0.3"), s.getBuildIdsFromMutableState())
		})
	}
}

func (s *mutableStateSuite) TestUpdateUsedDeploymentVersionsSearchAttribute() {
	deploymentVersion := func(deploymentName, buildId string) *deploymentpb.WorkerDeploymentVersion {
		return deploymentpb.WorkerDeploymentVersion_builder{
			DeploymentName: deploymentName,
			BuildId:        buildId,
		}.Build()
	}

	// Set up initial state with a deployment version
	dbState := s.buildWorkflowMutableState()
	var err error
	s.mutableState, err = NewMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, tests.LocalNamespaceEntry, dbState, 123)
	s.NoError(err)

	// Test 1: First deployment version added
	err = s.mutableState.updateBuildIdsAndDeploymentSearchAttributes(nil, deploymentVersion("deployment-a", "build-1"), 1000)
	s.NoError(err)
	s.Equal([]string{"deployment-a:build-1"}, s.getUsedDeploymentVersionsFromMutableState())

	// Test 2: Same deployment version (no change, deduplication)
	err = s.mutableState.updateBuildIdsAndDeploymentSearchAttributes(nil, deploymentVersion("deployment-a", "build-1"), 1000)
	s.NoError(err)
	s.Equal([]string{"deployment-a:build-1"}, s.getUsedDeploymentVersionsFromMutableState())

	// Test 3: New deployment version added to list
	err = s.mutableState.updateBuildIdsAndDeploymentSearchAttributes(nil, deploymentVersion("deployment-a", "build-2"), 1000)
	s.NoError(err)
	s.Equal([]string{"deployment-a:build-1", "deployment-a:build-2"}, s.getUsedDeploymentVersionsFromMutableState())

	// Test 4: Another deployment version added
	err = s.mutableState.updateBuildIdsAndDeploymentSearchAttributes(nil, deploymentVersion("deployment-b", "build-1"), 1000)
	s.NoError(err)
	s.Equal([]string{"deployment-a:build-1", "deployment-a:build-2", "deployment-b:build-1"}, s.getUsedDeploymentVersionsFromMutableState())

	// Test 5: Size limit handling (oldest removed)
	// Set a very small limit to trigger size constraint
	err = s.mutableState.updateBuildIdsAndDeploymentSearchAttributes(nil, deploymentVersion("deployment-c", "build-1"), 40)
	s.NoError(err)
	// Should have removed oldest entries to fit within size limit
	versions := s.getUsedDeploymentVersionsFromMutableState()
	s.NotEmpty(versions, "Should have at least one version")
	// The newest one should be present
	s.Contains(versions, "deployment-c:build-1")

	// Test 6: Empty deployment version (unversioned workflows not tracked)
	beforeVersions := s.getUsedDeploymentVersionsFromMutableState()
	err = s.mutableState.updateBuildIdsAndDeploymentSearchAttributes(nil, nil, 1000)
	s.NoError(err)
	// Should not have added any new version (unversioned is skipped)
	s.Equal(beforeVersions, s.getUsedDeploymentVersionsFromMutableState())
}

func (s *mutableStateSuite) TestAddResetPointFromCompletion() {
	dbState := s.buildWorkflowMutableState()
	var err error
	s.mutableState, err = NewMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, tests.LocalNamespaceEntry, dbState, 123)
	s.NoError(err)

	s.Nil(s.cleanedResetPoints().GetPoints())

	s.mutableState.addResetPointFromCompletion("checksum1", "buildid1", 32, 10)
	p1 := workflowpb.ResetPointInfo_builder{
		BuildId:                      "buildid1",
		BinaryChecksum:               "checksum1",
		RunId:                        s.mutableState.executionState.GetRunId(),
		FirstWorkflowTaskCompletedId: 32,
	}.Build()
	s.Equal([]*workflowpb.ResetPointInfo{p1}, s.cleanedResetPoints().GetPoints())

	// new checksum + buildid
	s.mutableState.addResetPointFromCompletion("checksum2", "buildid2", 35, 10)
	p2 := workflowpb.ResetPointInfo_builder{
		BuildId:                      "buildid2",
		BinaryChecksum:               "checksum2",
		RunId:                        s.mutableState.executionState.GetRunId(),
		FirstWorkflowTaskCompletedId: 35,
	}.Build()
	s.Equal([]*workflowpb.ResetPointInfo{p1, p2}, s.cleanedResetPoints().GetPoints())

	// same checksum + buildid, does not add new point
	s.mutableState.addResetPointFromCompletion("checksum2", "buildid2", 42, 10)
	s.Equal([]*workflowpb.ResetPointInfo{p1, p2}, s.cleanedResetPoints().GetPoints())

	// back to 1, does not add new point
	s.mutableState.addResetPointFromCompletion("checksum1", "buildid1", 48, 10)
	s.Equal([]*workflowpb.ResetPointInfo{p1, p2}, s.cleanedResetPoints().GetPoints())

	// buildid changes
	s.mutableState.addResetPointFromCompletion("checksum2", "buildid3", 53, 10)
	p3 := workflowpb.ResetPointInfo_builder{
		BuildId:                      "buildid3",
		BinaryChecksum:               "checksum2",
		RunId:                        s.mutableState.executionState.GetRunId(),
		FirstWorkflowTaskCompletedId: 53,
	}.Build()
	s.Equal([]*workflowpb.ResetPointInfo{p1, p2, p3}, s.cleanedResetPoints().GetPoints())

	// limit to 3, p1 gets dropped
	s.mutableState.addResetPointFromCompletion("checksum2", "buildid4", 55, 3)
	p4 := workflowpb.ResetPointInfo_builder{
		BuildId:                      "buildid4",
		BinaryChecksum:               "checksum2",
		RunId:                        s.mutableState.executionState.GetRunId(),
		FirstWorkflowTaskCompletedId: 55,
	}.Build()
	s.Equal([]*workflowpb.ResetPointInfo{p2, p3, p4}, s.cleanedResetPoints().GetPoints())
}

func (s *mutableStateSuite) TestRolloverAutoResetPointsWithExpiringTime() {
	runID1 := uuid.NewString()
	runID2 := uuid.NewString()
	runID3 := uuid.NewString()

	retention := 3 * time.Hour
	base := time.Now()
	t1 := timestamppb.New(base)
	now := timestamppb.New(base.Add(1 * time.Hour))
	t2 := timestamppb.New(base.Add(2 * time.Hour))
	t3 := timestamppb.New(base.Add(4 * time.Hour))

	points := []*workflowpb.ResetPointInfo{
		workflowpb.ResetPointInfo_builder{
			BuildId:                      "buildid1",
			RunId:                        runID1,
			FirstWorkflowTaskCompletedId: 32,
			ExpireTime:                   t1,
		}.Build(),
		workflowpb.ResetPointInfo_builder{
			BuildId:                      "buildid2",
			RunId:                        runID1,
			FirstWorkflowTaskCompletedId: 63,
			ExpireTime:                   t1,
		}.Build(),
		workflowpb.ResetPointInfo_builder{
			BuildId:                      "buildid3",
			RunId:                        runID2,
			FirstWorkflowTaskCompletedId: 94,
			ExpireTime:                   t2,
		}.Build(),
		workflowpb.ResetPointInfo_builder{
			BuildId:                      "buildid4",
			RunId:                        runID3,
			FirstWorkflowTaskCompletedId: 125,
		}.Build(),
	}

	newPoints := rolloverAutoResetPointsWithExpiringTime(workflowpb.ResetPoints_builder{Points: points}.Build(), runID3, now.AsTime(), retention)
	expected := []*workflowpb.ResetPointInfo{
		workflowpb.ResetPointInfo_builder{
			BuildId:                      "buildid3",
			RunId:                        runID2,
			FirstWorkflowTaskCompletedId: 94,
			ExpireTime:                   t2,
		}.Build(),
		workflowpb.ResetPointInfo_builder{
			BuildId:                      "buildid4",
			RunId:                        runID3,
			FirstWorkflowTaskCompletedId: 125,
			ExpireTime:                   t3,
		}.Build(),
	}
	s.Equal(expected, newPoints.GetPoints())
}

func (s *mutableStateSuite) TestCloseTransactionUpdateTransition() {
	namespaceEntry := tests.GlobalNamespaceEntry

	completWorkflowTaskFn := func(ms historyi.MutableState) {
		workflowTaskInfo := ms.GetStartedWorkflowTask()
		_, err := ms.AddWorkflowTaskCompletedEvent(
			workflowTaskInfo,
			&workflowservice.RespondWorkflowTaskCompletedRequest{},
			workflowTaskCompletionLimits,
		)
		s.NoError(err)
	}

	testCases := []struct {
		name                       string
		dbStateMutationFn          func(dbState *persistencespb.WorkflowMutableState)
		txFunc                     func(ms historyi.MutableState) (*persistencespb.WorkflowExecutionInfo, error)
		versionedTransitionUpdated bool
	}{
		{
			name: "CloseTransactionAsMutation_HistoryEvents",
			dbStateMutationFn: func(dbState *persistencespb.WorkflowMutableState) {
				dbState.SetBufferedEvents(nil)
			},
			txFunc: func(ms historyi.MutableState) (*persistencespb.WorkflowExecutionInfo, error) {
				completWorkflowTaskFn(ms)

				mutation, _, err := ms.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
				if err != nil {
					return nil, err
				}
				return mutation.ExecutionInfo, err
			},
			versionedTransitionUpdated: true,
		},
		{
			name: "CloseTransactionAsMutation_BufferedEvents",
			dbStateMutationFn: func(dbState *persistencespb.WorkflowMutableState) {
				dbState.SetBufferedEvents(nil)
			},
			txFunc: func(ms historyi.MutableState) (*persistencespb.WorkflowExecutionInfo, error) {
				var activityScheduleEventID int64
				for activityScheduleEventID = range s.mutableState.GetPendingActivityInfos() {
					break
				}
				_, err := s.mutableState.AddActivityTaskTimedOutEvent(
					activityScheduleEventID,
					common.EmptyEventID,
					failure.NewTimeoutFailure("test-timeout", enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START),
					enumspb.RETRY_STATE_TIMEOUT,
				)
				s.NoError(err)

				mutation, _, err := ms.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
				if err != nil {
					return nil, err
				}
				return mutation.ExecutionInfo, err
			},
			versionedTransitionUpdated: true,
		},
		{
			name: "CloseTransactionAsMutation_SyncActivity",
			dbStateMutationFn: func(dbState *persistencespb.WorkflowMutableState) {
				dbState.SetBufferedEvents(nil)
			},
			txFunc: func(ms historyi.MutableState) (*persistencespb.WorkflowExecutionInfo, error) {
				for _, ai := range ms.GetPendingActivityInfos() {
					ms.UpdateActivityProgress(ai, &workflowservice.RecordActivityTaskHeartbeatRequest{})
					break
				}

				mutation, _, err := ms.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
				if err != nil {
					return nil, err
				}
				return mutation.ExecutionInfo, err
			},
			versionedTransitionUpdated: true,
		},
		{
			name: "CloseTransactionAsMutation_DirtyStateMachine",
			dbStateMutationFn: func(dbState *persistencespb.WorkflowMutableState) {
				dbState.SetBufferedEvents(nil)
			},
			txFunc: func(ms historyi.MutableState) (*persistencespb.WorkflowExecutionInfo, error) {
				root := ms.HSM()
				err := hsm.MachineTransition(root, func(*MutableStateImpl) (hsm.TransitionOutput, error) {
					return hsm.TransitionOutput{}, nil
				})
				s.NoError(err)
				s.True(root.Dirty())

				mutation, _, err := ms.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
				if err != nil {
					return nil, err
				}
				return mutation.ExecutionInfo, err
			},
			versionedTransitionUpdated: true,
		},
		{
			name: "CloseTransactionAsMutation_SignalWorkflow",
			dbStateMutationFn: func(dbState *persistencespb.WorkflowMutableState) {
				dbState.SetBufferedEvents(nil)
			},
			txFunc: func(ms historyi.MutableState) (*persistencespb.WorkflowExecutionInfo, error) {
				_, err := ms.AddWorkflowExecutionSignaledEvent(
					"signalName",
					&commonpb.Payloads{},
					"identity",
					&commonpb.Header{},
					nil,
					nil,
				)
				if err != nil {
					return nil, err
				}

				mutation, _, err := ms.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
				if err != nil {
					return nil, err
				}
				return mutation.ExecutionInfo, err
			},
			versionedTransitionUpdated: true,
		},
		{
			name: "CloseTransactionAsMutation_ChasmTree",
			dbStateMutationFn: func(dbState *persistencespb.WorkflowMutableState) {
				dbState.SetBufferedEvents(nil)
			},
			txFunc: func(ms historyi.MutableState) (*persistencespb.WorkflowExecutionInfo, error) {
				mockChasmTree := historyi.NewMockChasmTree(s.controller)
				mockChasmTree.EXPECT().ArchetypeID().Return(chasm.ArchetypeID(1234)).AnyTimes()
				gomock.InOrder(
					mockChasmTree.EXPECT().IsStateDirty().Return(true).AnyTimes(),
					mockChasmTree.EXPECT().CloseTransaction().Return(chasm.NodesMutation{
						UpdatedNodes: map[string]*persistencespb.ChasmNode{
							"node-path": persistencespb.ChasmNode_builder{
								Metadata: persistencespb.ChasmNodeMetadata_builder{
									DataAttributes: &persistencespb.ChasmDataAttributes{},
								}.Build(),
								Data: commonpb.DataBlob_builder{Data: []byte("test-data")}.Build(),
							}.Build(),
						},
					}, nil),
				)
				ms.(*MutableStateImpl).chasmTree = mockChasmTree

				mutation, _, err := ms.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
				if err != nil {
					return nil, err
				}
				return mutation.ExecutionInfo, err
			},
			versionedTransitionUpdated: true,
		},
		{
			name: "CloseTransactionAsSnapshot",
			dbStateMutationFn: func(dbState *persistencespb.WorkflowMutableState) {
				dbState.SetBufferedEvents(nil)
			},
			txFunc: func(ms historyi.MutableState) (*persistencespb.WorkflowExecutionInfo, error) {
				completWorkflowTaskFn(ms)

				mutation, _, err := ms.CloseTransactionAsSnapshot(context.Background(), historyi.TransactionPolicyActive)
				if err != nil {
					return nil, err
				}
				return mutation.ExecutionInfo, err
			},
			versionedTransitionUpdated: true,
		},
		{
			name: "CloseTransactionAsSnapshot, from unknown to enable",
			dbStateMutationFn: func(dbState *persistencespb.WorkflowMutableState) {
				dbState.SetBufferedEvents(nil)
			},
			txFunc: func(ms historyi.MutableState) (*persistencespb.WorkflowExecutionInfo, error) {
				ms.GetExecutionInfo().SetPreviousTransitionHistory(ms.GetExecutionInfo().GetTransitionHistory())
				ms.GetExecutionInfo().SetTransitionHistory(nil)
				completWorkflowTaskFn(ms)

				mutation, _, err := ms.CloseTransactionAsSnapshot(context.Background(), historyi.TransactionPolicyActive)
				if err != nil {
					return nil, err
				}
				return mutation.ExecutionInfo, err
			},
			versionedTransitionUpdated: true,
		},
		// TODO: add a test for flushing buffered events using last event version.
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			dbState := s.buildWorkflowMutableState()
			if tc.dbStateMutationFn != nil {
				tc.dbStateMutationFn(dbState)
			}

			var err error
			s.mutableState, err = NewMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, namespaceEntry, dbState, 123)
			s.NoError(err)
			err = s.mutableState.UpdateCurrentVersion(namespaceEntry.FailoverVersion(tests.WorkflowID), false)
			s.NoError(err)

			s.mockShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(
				namespaceEntry.IsGlobalNamespace(),
				namespaceEntry.FailoverVersion(tests.WorkflowID),
			).Return(cluster.TestCurrentClusterName).AnyTimes()
			s.mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
			var expectedTransitionHistory []*persistencespb.VersionedTransition
			if s.mutableState.executionInfo.GetTransitionHistory() == nil {
				expectedTransitionHistory = transitionhistory.CopyVersionedTransitions(s.mutableState.executionInfo.GetPreviousTransitionHistory())
			} else {
				expectedTransitionHistory = transitionhistory.CopyVersionedTransitions(s.mutableState.executionInfo.GetTransitionHistory())
			}

			if tc.versionedTransitionUpdated {
				expectedTransitionHistory = UpdatedTransitionHistory(expectedTransitionHistory, namespaceEntry.FailoverVersion(tests.WorkflowID))
			}

			execInfo, err := tc.txFunc(s.mutableState)
			s.Nil(err)

			protorequire.ProtoSliceEqual(t, expectedTransitionHistory, execInfo.GetTransitionHistory())
		})
	}
}

func (s *mutableStateSuite) TestCloseTransactionTrackLastUpdateVersionedTransition() {
	namespaceEntry := tests.GlobalNamespaceEntry
	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

	stateMachineDef := hsmtest.NewDefinition("test")
	err := s.mockShard.StateMachineRegistry().RegisterMachine(stateMachineDef)
	s.NoError(err)

	completWorkflowTaskFn := func(ms historyi.MutableState) *historypb.HistoryEvent {
		workflowTaskInfo := ms.GetStartedWorkflowTask()
		completedEvent, err := ms.AddWorkflowTaskCompletedEvent(
			workflowTaskInfo,
			&workflowservice.RespondWorkflowTaskCompletedRequest{},
			workflowTaskCompletionLimits,
		)
		s.NoError(err)
		return completedEvent
	}

	buildHSMFn := func(ms historyi.MutableState) {
		hsmRoot := ms.HSM()
		child1, err := hsmRoot.AddChild(hsm.Key{Type: stateMachineDef.Type(), ID: "child_1"}, hsmtest.NewData(hsmtest.State1))
		s.NoError(err)
		_, err = child1.AddChild(hsm.Key{Type: stateMachineDef.Type(), ID: "child_1_1"}, hsmtest.NewData(hsmtest.State2))
		s.NoError(err)
		_, err = hsmRoot.AddChild(hsm.Key{Type: stateMachineDef.Type(), ID: "child_2"}, hsmtest.NewData(hsmtest.State3))
		s.NoError(err)
	}

	testCases := []struct {
		name   string
		testFn func(ms historyi.MutableState)
	}{
		{
			name: "Activity",
			testFn: func(ms historyi.MutableState) {
				completedEvent := completWorkflowTaskFn(ms)
				scheduledEvent, _, err := ms.AddActivityTaskScheduledEvent(
					completedEvent.GetEventId(),
					&commandpb.ScheduleActivityTaskCommandAttributes{},
					false,
				)
				s.NoError(err)

				_, _, err = ms.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
				s.NoError(err)

				currentVersionedTransition := ms.CurrentVersionedTransition()

				s.Len(ms.GetPendingActivityInfos(), 2)
				for _, ai := range ms.GetPendingActivityInfos() {
					if ai.GetScheduledEventId() == scheduledEvent.GetEventId() {
						protorequire.ProtoEqual(s.T(), currentVersionedTransition, ai.GetLastUpdateVersionedTransition())
					} else {
						protorequire.NotProtoEqual(s.T(), currentVersionedTransition, ai.GetLastUpdateVersionedTransition())
					}
				}
			},
		},
		{
			name: "UserTimer",
			testFn: func(ms historyi.MutableState) {
				completedEvent := completWorkflowTaskFn(ms)
				newTimerID := "new-timer-id"
				_, _, err := ms.AddTimerStartedEvent(
					completedEvent.GetEventId(),
					commandpb.StartTimerCommandAttributes_builder{
						TimerId: newTimerID,
					}.Build(),
				)
				s.NoError(err)

				_, _, err = ms.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
				s.NoError(err)

				currentVersionedTransition := ms.CurrentVersionedTransition()

				s.Len(ms.GetPendingTimerInfos(), 2)
				for _, ti := range ms.GetPendingTimerInfos() {
					if ti.GetTimerId() == newTimerID {
						protorequire.ProtoEqual(s.T(), currentVersionedTransition, ti.GetLastUpdateVersionedTransition())
					} else {
						protorequire.NotProtoEqual(s.T(), currentVersionedTransition, ti.GetLastUpdateVersionedTransition())
					}
				}
			},
		},
		{
			name: "ChildExecution",
			testFn: func(ms historyi.MutableState) {
				completedEvent := completWorkflowTaskFn(ms)
				initiatedEvent, _, err := ms.AddStartChildWorkflowExecutionInitiatedEvent(
					completedEvent.GetEventId(),
					&commandpb.StartChildWorkflowExecutionCommandAttributes{},
					ms.GetNamespaceEntry().ID(),
				)
				s.NoError(err)

				_, _, err = ms.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
				s.NoError(err)

				currentVersionedTransition := ms.CurrentVersionedTransition()

				s.Len(ms.GetPendingChildExecutionInfos(), 2)
				for _, ci := range ms.GetPendingChildExecutionInfos() {
					if ci.GetInitiatedEventId() == initiatedEvent.GetEventId() {
						protorequire.ProtoEqual(s.T(), currentVersionedTransition, ci.GetLastUpdateVersionedTransition())
					} else {
						protorequire.NotProtoEqual(s.T(), currentVersionedTransition, ci.GetLastUpdateVersionedTransition())
					}
				}
			},
		},
		{
			name: "RequestCancelExternal",
			testFn: func(ms historyi.MutableState) {
				completedEvent := completWorkflowTaskFn(ms)
				initiatedEvent, _, err := ms.AddRequestCancelExternalWorkflowExecutionInitiatedEvent(
					completedEvent.GetEventId(),
					uuid.NewString(),
					&commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes{},
					ms.GetNamespaceEntry().ID(),
				)
				s.NoError(err)

				_, _, err = ms.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
				s.NoError(err)

				currentVersionedTransition := ms.CurrentVersionedTransition()

				s.Len(ms.GetPendingRequestCancelExternalInfos(), 2)
				for _, ci := range ms.GetPendingRequestCancelExternalInfos() {
					if ci.GetInitiatedEventId() == initiatedEvent.GetEventId() {
						protorequire.ProtoEqual(s.T(), currentVersionedTransition, ci.GetLastUpdateVersionedTransition())
					} else {
						protorequire.NotProtoEqual(s.T(), currentVersionedTransition, ci.GetLastUpdateVersionedTransition())
					}
				}
			},
		},
		{
			name: "SignalExternal",
			testFn: func(ms historyi.MutableState) {
				completedEvent := completWorkflowTaskFn(ms)
				initiatedEvent, _, err := ms.AddSignalExternalWorkflowExecutionInitiatedEvent(
					completedEvent.GetEventId(),
					uuid.NewString(),
					commandpb.SignalExternalWorkflowExecutionCommandAttributes_builder{
						Execution: commonpb.WorkflowExecution_builder{
							WorkflowId: "target-workflow-id",
							RunId:      "target-run-id",
						}.Build(),
					}.Build(),
					ms.GetNamespaceEntry().ID(),
				)
				s.NoError(err)

				_, _, err = ms.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
				s.NoError(err)

				currentVersionedTransition := ms.CurrentVersionedTransition()

				s.Len(ms.GetPendingSignalExternalInfos(), 2)
				for _, ci := range ms.GetPendingSignalExternalInfos() {
					if ci.GetInitiatedEventId() == initiatedEvent.GetEventId() {
						protorequire.ProtoEqual(s.T(), currentVersionedTransition, ci.GetLastUpdateVersionedTransition())
					} else {
						protorequire.NotProtoEqual(s.T(), currentVersionedTransition, ci.GetLastUpdateVersionedTransition())
					}
				}
			},
		},
		{
			name: "SignalRequestedID",
			testFn: func(ms historyi.MutableState) {
				ms.AddSignalRequested(uuid.NewString())

				_, _, err := ms.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
				s.NoError(err)

				currentVersionedTransition := ms.CurrentVersionedTransition()
				protorequire.ProtoEqual(s.T(), currentVersionedTransition, ms.GetExecutionInfo().GetSignalRequestIdsLastUpdateVersionedTransition())
			},
		},
		{
			name: "UpdateInfo",
			testFn: func(ms historyi.MutableState) {
				updateID := "test-updateId"
				_, err := ms.AddWorkflowExecutionUpdateAcceptedEvent(
					updateID,
					"update-message-id",
					65,
					nil, // this is an optional field
				)
				s.NoError(err)

				_, _, err = ms.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
				s.NoError(err)

				currentVersionedTransition := ms.CurrentVersionedTransition()
				s.Len(ms.GetExecutionInfo().GetUpdateInfos(), 1)
				protorequire.ProtoEqual(s.T(), currentVersionedTransition, ms.GetExecutionInfo().GetUpdateInfos()[updateID].GetLastUpdateVersionedTransition())
			},
		},
		{
			name: "WorkflowTask/Completed",
			testFn: func(ms historyi.MutableState) {
				completWorkflowTaskFn(ms)

				_, _, err := ms.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
				s.NoError(err)

				currentVersionedTransition := ms.CurrentVersionedTransition()
				protorequire.ProtoEqual(s.T(), currentVersionedTransition, ms.GetExecutionInfo().GetWorkflowTaskLastUpdateVersionedTransition())
			},
		},
		{
			name: "WorkflowTask/Scheduled",
			testFn: func(ms historyi.MutableState) {
				completWorkflowTaskFn(ms)
				_, err := ms.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
				s.NoError(err)

				_, _, err = ms.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
				s.NoError(err)

				currentVersionedTransition := ms.CurrentVersionedTransition()
				protorequire.ProtoEqual(s.T(), currentVersionedTransition, ms.GetExecutionInfo().GetWorkflowTaskLastUpdateVersionedTransition())
			},
		},
		{
			name: "Visibility",
			testFn: func(ms historyi.MutableState) {
				completedEvent := completWorkflowTaskFn(ms)
				_, err := ms.AddUpsertWorkflowSearchAttributesEvent(
					completedEvent.GetEventId(),
					&commandpb.UpsertWorkflowSearchAttributesCommandAttributes{},
				)
				s.NoError(err)

				_, _, err = ms.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
				s.NoError(err)

				currentVersionedTransition := ms.CurrentVersionedTransition()
				protorequire.ProtoEqual(s.T(), currentVersionedTransition, ms.GetExecutionInfo().GetVisibilityLastUpdateVersionedTransition())
			},
		},
		{
			name: "ExecutionState",
			testFn: func(ms historyi.MutableState) {
				completedEvent := completWorkflowTaskFn(ms)
				_, err := ms.AddCompletedWorkflowEvent(
					completedEvent.GetEventId(),
					&commandpb.CompleteWorkflowExecutionCommandAttributes{},
					"",
				)
				s.NoError(err)

				_, _, err = ms.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
				s.NoError(err)

				currentVersionedTransition := ms.CurrentVersionedTransition()
				protorequire.ProtoEqual(s.T(), currentVersionedTransition, ms.GetExecutionState().GetLastUpdateVersionedTransition())
			},
		},
		{
			name: "HSM/CloseAsMutation",
			testFn: func(ms historyi.MutableState) {
				completWorkflowTaskFn(ms)
				buildHSMFn(ms)

				_, _, err := ms.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
				s.NoError(err)

				currentVersionedTransition := ms.CurrentVersionedTransition()
				err = ms.HSM().Walk(func(n *hsm.Node) error {
					if n.Parent == nil {
						// skip root which is entire mutable state
						return nil
					}
					protorequire.ProtoEqual(s.T(), currentVersionedTransition, n.InternalRepr().GetLastUpdateVersionedTransition())
					return nil
				})
				s.NoError(err)
			},
		},
		{
			name: "HSM/CloseAsSnapshot",
			testFn: func(ms historyi.MutableState) {
				completWorkflowTaskFn(ms)
				buildHSMFn(ms)

				_, _, err := ms.CloseTransactionAsSnapshot(context.Background(), historyi.TransactionPolicyActive)
				s.NoError(err)

				currentVersionedTransition := ms.CurrentVersionedTransition()
				err = ms.HSM().Walk(func(n *hsm.Node) error {
					if n.Parent == nil {
						// skip root which is entire mutable state
						return nil
					}
					protorequire.ProtoEqual(s.T(), currentVersionedTransition, n.InternalRepr().GetLastUpdateVersionedTransition())
					return nil
				})
				s.NoError(err)
			},
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {

			dbState := s.buildWorkflowMutableState()
			dbState.SetBufferedEvents(nil)

			var err error
			s.mutableState, err = NewMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, namespaceEntry, dbState, 123)
			s.NoError(err)
			err = s.mutableState.UpdateCurrentVersion(namespaceEntry.FailoverVersion(tests.WorkflowID), false)
			s.NoError(err)

			s.mockShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(
				namespaceEntry.IsGlobalNamespace(),
				namespaceEntry.FailoverVersion(tests.WorkflowID),
			).Return(cluster.TestCurrentClusterName).AnyTimes()
			s.mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

			tc.testFn(s.mutableState)
		})
	}
}

func (s *mutableStateSuite) TestCloseTransactionHandleUnknownVersionedTransition() {
	namespaceEntry := tests.GlobalNamespaceEntry

	completWorkflowTaskFn := func(ms historyi.MutableState) {
		workflowTaskInfo := ms.GetStartedWorkflowTask()
		_, err := ms.AddWorkflowTaskCompletedEvent(
			workflowTaskInfo,
			&workflowservice.RespondWorkflowTaskCompletedRequest{},
			historyi.WorkflowTaskCompletionLimits{
				MaxResetPoints:              10,
				MaxSearchAttributeValueSize: 1024,
			},
		)
		s.NoError(err)
	}

	testCases := []struct {
		name              string
		dbStateMutationFn func(dbState *persistencespb.WorkflowMutableState)
		txFunc            func(ms historyi.MutableState) (*persistencespb.WorkflowExecutionInfo, error)
	}{
		{
			name: "CloseTransactionAsPassive", // this scenario simulate the case to clear the transition history (non state-based transition happened at passive side)
			dbStateMutationFn: func(dbState *persistencespb.WorkflowMutableState) {
				dbState.SetBufferedEvents(nil)
			},
			txFunc: func(ms historyi.MutableState) (*persistencespb.WorkflowExecutionInfo, error) {
				completWorkflowTaskFn(ms)

				mutation, _, err := ms.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyPassive)
				if err != nil {
					return nil, err
				}
				return mutation.ExecutionInfo, err
			},
		},
		{
			name: "CloseTransactionAsMutation_HistoryEvents",
			dbStateMutationFn: func(dbState *persistencespb.WorkflowMutableState) {
				dbState.SetBufferedEvents(nil)
			},
			txFunc: func(ms historyi.MutableState) (*persistencespb.WorkflowExecutionInfo, error) {
				completWorkflowTaskFn(ms)

				mutation, _, err := ms.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
				if err != nil {
					return nil, err
				}
				return mutation.ExecutionInfo, err
			},
		},
		{
			name: "CloseTransactionAsMutation_BufferedEvents",
			dbStateMutationFn: func(dbState *persistencespb.WorkflowMutableState) {
				dbState.SetBufferedEvents(nil)
			},
			txFunc: func(ms historyi.MutableState) (*persistencespb.WorkflowExecutionInfo, error) {
				var activityScheduleEventID int64
				for activityScheduleEventID = range s.mutableState.GetPendingActivityInfos() {
					break
				}
				_, err := s.mutableState.AddActivityTaskTimedOutEvent(
					activityScheduleEventID,
					common.EmptyEventID,
					failure.NewTimeoutFailure("test-timeout", enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START),
					enumspb.RETRY_STATE_TIMEOUT,
				)
				s.NoError(err)

				mutation, _, err := ms.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
				if err != nil {
					return nil, err
				}
				return mutation.ExecutionInfo, err
			},
		},
		{
			name: "CloseTransactionAsMutation_SyncActivity",
			dbStateMutationFn: func(dbState *persistencespb.WorkflowMutableState) {
				dbState.SetBufferedEvents(nil)
			},
			txFunc: func(ms historyi.MutableState) (*persistencespb.WorkflowExecutionInfo, error) {
				for _, ai := range ms.GetPendingActivityInfos() {
					ms.UpdateActivityProgress(ai, &workflowservice.RecordActivityTaskHeartbeatRequest{})
					break
				}

				mutation, _, err := ms.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
				if err != nil {
					return nil, err
				}
				return mutation.ExecutionInfo, err
			},
		},
		{
			name: "CloseTransactionAsMutation_DirtyStateMachine",
			dbStateMutationFn: func(dbState *persistencespb.WorkflowMutableState) {
				dbState.SetBufferedEvents(nil)
			},
			txFunc: func(ms historyi.MutableState) (*persistencespb.WorkflowExecutionInfo, error) {
				root := ms.HSM()
				err := hsm.MachineTransition(root, func(*MutableStateImpl) (hsm.TransitionOutput, error) {
					return hsm.TransitionOutput{}, nil
				})
				s.NoError(err)
				s.True(root.Dirty())

				mutation, _, err := ms.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
				if err != nil {
					return nil, err
				}
				return mutation.ExecutionInfo, err
			},
		},
		{
			name: "CloseTransactionAsSnapshot",
			dbStateMutationFn: func(dbState *persistencespb.WorkflowMutableState) {
				dbState.SetBufferedEvents(nil)
			},
			txFunc: func(ms historyi.MutableState) (*persistencespb.WorkflowExecutionInfo, error) {
				completWorkflowTaskFn(ms)

				mutation, _, err := ms.CloseTransactionAsSnapshot(context.Background(), historyi.TransactionPolicyActive)
				if err != nil {
					return nil, err
				}
				return mutation.ExecutionInfo, err
			},
		},
		// TODO: add a test for flushing buffered events using last event version.
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			dbState := s.buildWorkflowMutableState()
			if tc.dbStateMutationFn != nil {
				tc.dbStateMutationFn(dbState)
			}

			var err error
			s.mutableState, err = NewMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, namespaceEntry, dbState, 123)
			s.NoError(err)
			err = s.mutableState.UpdateCurrentVersion(namespaceEntry.FailoverVersion(tests.WorkflowID), false)
			s.NoError(err)
			s.mutableState.transitionHistoryEnabled = false

			s.mockShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(
				namespaceEntry.IsGlobalNamespace(),
				namespaceEntry.FailoverVersion(tests.WorkflowID),
			).Return(cluster.TestCurrentClusterName).AnyTimes()
			s.mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

			s.NotNil(s.mutableState.executionInfo.GetTransitionHistory())
			execInfo, err := tc.txFunc(s.mutableState)
			s.NotNil(execInfo.GetPreviousTransitionHistory())
			s.Nil(execInfo.GetTransitionHistory())
			s.Nil(err)
		})
	}
}

func (s *mutableStateSuite) getBuildIdsFromMutableState() []string {
	payload, found := s.mutableState.executionInfo.GetSearchAttributes()[sadefs.BuildIds]
	if !found {
		return []string{}
	}
	decoded, err := searchattribute.DecodeValue(payload, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, true)
	s.NoError(err)
	buildIDs, ok := decoded.([]string)
	s.True(ok)
	return buildIDs
}

func (s *mutableStateSuite) getUsedDeploymentVersionsFromMutableState() []string {
	payload, found := s.mutableState.executionInfo.GetSearchAttributes()[sadefs.TemporalUsedWorkerDeploymentVersions]
	if !found {
		return []string{}
	}
	decoded, err := searchattribute.DecodeValue(payload, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, true)
	s.NoError(err)
	usedDeploymentVersions, ok := decoded.([]string)
	s.True(ok)
	return usedDeploymentVersions
}

// return reset points minus a few fields that are hard to check for equality
func (s *mutableStateSuite) cleanedResetPoints() *workflowpb.ResetPoints {
	out := common.CloneProto(s.mutableState.executionInfo.GetAutoResetPoints())
	for _, point := range out.GetPoints() {
		point.ClearCreateTime() // current time
		point.ClearExpireTime()
	}
	return out
}

func (s *mutableStateSuite) TestCollapseVisibilityTasks() {
	testCases := []struct {
		name  string
		tasks []tasks.Task
		res   []enumsspb.TaskType
	}{
		{
			name: "start upsert close delete",
			tasks: []tasks.Task{
				&tasks.StartExecutionVisibilityTask{},
				&tasks.UpsertExecutionVisibilityTask{},
				&tasks.UpsertExecutionVisibilityTask{},
				&tasks.CloseExecutionVisibilityTask{},
				&tasks.DeleteExecutionVisibilityTask{},
			},
			res: []enumsspb.TaskType{
				enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION,
			},
		},
		{
			name: "upsert close delete",
			tasks: []tasks.Task{
				&tasks.UpsertExecutionVisibilityTask{},
				&tasks.UpsertExecutionVisibilityTask{},
				&tasks.CloseExecutionVisibilityTask{},
				&tasks.DeleteExecutionVisibilityTask{},
			},
			res: []enumsspb.TaskType{
				enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION,
			},
		},
		{
			name: "close delete",
			tasks: []tasks.Task{
				&tasks.CloseExecutionVisibilityTask{},
				&tasks.DeleteExecutionVisibilityTask{},
			},
			res: []enumsspb.TaskType{
				enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION,
			},
		},
		{
			name: "delete",
			tasks: []tasks.Task{
				&tasks.DeleteExecutionVisibilityTask{},
			},
			res: []enumsspb.TaskType{
				enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION,
			},
		},
		{
			name: "start upsert close",
			tasks: []tasks.Task{
				&tasks.StartExecutionVisibilityTask{},
				&tasks.UpsertExecutionVisibilityTask{},
				&tasks.UpsertExecutionVisibilityTask{},
				&tasks.CloseExecutionVisibilityTask{},
			},
			res: []enumsspb.TaskType{
				enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION,
			},
		},
		{
			name: "upsert close",
			tasks: []tasks.Task{
				&tasks.UpsertExecutionVisibilityTask{},
				&tasks.UpsertExecutionVisibilityTask{},
				&tasks.CloseExecutionVisibilityTask{},
			},
			res: []enumsspb.TaskType{
				enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION,
			},
		},
		{
			name: "close",
			tasks: []tasks.Task{
				&tasks.CloseExecutionVisibilityTask{},
			},
			res: []enumsspb.TaskType{
				enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION,
			},
		},
		{
			name: "start upsert",
			tasks: []tasks.Task{
				&tasks.StartExecutionVisibilityTask{},
				&tasks.UpsertExecutionVisibilityTask{},
				&tasks.UpsertExecutionVisibilityTask{},
			},
			res: []enumsspb.TaskType{
				enumsspb.TASK_TYPE_VISIBILITY_UPSERT_EXECUTION,
			},
		},
		{
			name: "upsert",
			tasks: []tasks.Task{
				&tasks.UpsertExecutionVisibilityTask{},
				&tasks.UpsertExecutionVisibilityTask{},
			},
			res: []enumsspb.TaskType{
				enumsspb.TASK_TYPE_VISIBILITY_UPSERT_EXECUTION,
			},
		},
		{
			name: "start",
			tasks: []tasks.Task{
				&tasks.StartExecutionVisibilityTask{},
			},
			res: []enumsspb.TaskType{
				enumsspb.TASK_TYPE_VISIBILITY_START_EXECUTION,
			},
		},
		{
			name: "upsert start delete close",
			tasks: []tasks.Task{
				&tasks.UpsertExecutionVisibilityTask{},
				&tasks.StartExecutionVisibilityTask{},
				&tasks.DeleteExecutionVisibilityTask{},
				&tasks.CloseExecutionVisibilityTask{},
			},
			res: []enumsspb.TaskType{
				enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION,
			},
		},
		{
			name: "close upsert",
			tasks: []tasks.Task{
				&tasks.CloseExecutionVisibilityTask{},
				&tasks.UpsertExecutionVisibilityTask{},
			},
			res: []enumsspb.TaskType{
				enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION,
			},
		},
	}

	ms := s.mutableState

	for _, tc := range testCases {
		s.Run(
			tc.name,
			func() {
				ms.InsertTasks[tasks.CategoryVisibility] = []tasks.Task{}
				ms.AddTasks(tc.tasks...)
				ms.closeTransactionCollapseVisibilityTasks()
				visTasks := ms.InsertTasks[tasks.CategoryVisibility]
				s.Equal(len(tc.res), len(visTasks))
				for i, expectTaskType := range tc.res {
					s.Equal(expectTaskType, visTasks[i].GetType())
				}
			},
		)
	}
}

func (s *mutableStateSuite) TestStartChildWorkflowRequestID() {
	workflowTaskCompletionEventID := rand.Int63()
	attributes := &commandpb.StartChildWorkflowExecutionCommandAttributes{}
	event := s.mutableState.hBuilder.AddStartChildWorkflowExecutionInitiatedEvent(
		workflowTaskCompletionEventID,
		attributes,
		tests.NamespaceID,
	)
	createRequestID := fmt.Sprintf("%s:%d:%d", s.mutableState.executionState.GetRunId(), event.GetEventId(), event.GetVersion())
	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

	ci, err := s.mutableState.ApplyStartChildWorkflowExecutionInitiatedEvent(
		workflowTaskCompletionEventID,
		event,
	)
	s.NoError(err)
	s.Equal(createRequestID, ci.GetCreateRequestId())
}

func (s *mutableStateSuite) TestGetCloseVersion() {
	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

	_, err := s.mutableState.AddWorkflowExecutionStartedEvent(
		commonpb.WorkflowExecution_builder{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		}.Build(),
		historyservice.StartWorkflowExecutionRequest_builder{
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{},
		}.Build(),
	)
	s.NoError(err)
	_, err = s.mutableState.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
	s.NoError(err)
	_, _, err = s.mutableState.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
	s.NoError(err)

	_, err = s.mutableState.GetCloseVersion()
	s.Error(err) // workflow still open

	namespaceEntry, err := s.mockShard.GetNamespaceRegistry().GetNamespaceByID(tests.NamespaceID)
	s.NoError(err)
	expectedVersion := namespaceEntry.FailoverVersion(tests.WorkflowID)

	_, err = s.mutableState.AddCompletedWorkflowEvent(
		5,
		&commandpb.CompleteWorkflowExecutionCommandAttributes{},
		"",
	)
	s.NoError(err)
	// get close version in the transaction that closes the workflow
	closeVersion, err := s.mutableState.GetCloseVersion()
	s.NoError(err)
	s.Equal(expectedVersion, closeVersion)

	_, _, err = s.mutableState.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
	s.NoError(err)

	// get close version after workflow is closed
	closeVersion, err = s.mutableState.GetCloseVersion()
	s.NoError(err)
	s.Equal(expectedVersion, closeVersion)

	// verify close version doesn't change after workflow is closed
	err = s.mutableState.UpdateCurrentVersion(12345, true)
	s.NoError(err)
	closeVersion, err = s.mutableState.GetCloseVersion()
	s.NoError(err)
	s.Equal(expectedVersion, closeVersion)
}

func (s *mutableStateSuite) TestCloseTransactionPrepareReplicationTasks_HistoryTask() {
	version := int64(777)
	firstEventID := int64(2)
	lastEventID := int64(3)
	now := time.Now().UTC()
	taskqueue := "taskqueue for test"
	workflowTaskTimeout := 11 * time.Second
	workflowTaskAttempt := int32(1)
	eventBatches := [][]*historypb.HistoryEvent{
		{
			historypb.HistoryEvent_builder{
				Version:   version,
				EventId:   firstEventID,
				EventTime: timestamppb.New(now),
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				WorkflowTaskScheduledEventAttributes: historypb.WorkflowTaskScheduledEventAttributes_builder{
					TaskQueue:           taskqueuepb.TaskQueue_builder{Name: taskqueue}.Build(),
					StartToCloseTimeout: durationpb.New(workflowTaskTimeout),
					Attempt:             workflowTaskAttempt,
				}.Build(),
			}.Build(),
		},
		{
			historypb.HistoryEvent_builder{
				Version:   version,
				EventId:   lastEventID,
				EventTime: timestamppb.New(now),
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
				WorkflowTaskStartedEventAttributes: historypb.WorkflowTaskStartedEventAttributes_builder{
					ScheduledEventId: firstEventID,
					RequestId:        uuid.NewString(),
				}.Build(),
			}.Build(),
		},
	}

	testCases := []struct {
		name                       string
		replicationMultipleBatches bool
		tasks                      []tasks.Task
	}{
		{
			name:                       "multiple event batches disabled",
			replicationMultipleBatches: false,
			tasks: []tasks.Task{
				&tasks.HistoryReplicationTask{
					WorkflowKey:  s.mutableState.GetWorkflowKey(),
					FirstEventID: firstEventID,
					NextEventID:  firstEventID + 1,
					Version:      version,
				},
				&tasks.HistoryReplicationTask{
					WorkflowKey:  s.mutableState.GetWorkflowKey(),
					FirstEventID: lastEventID,
					NextEventID:  lastEventID + 1,
					Version:      version,
				},
			},
		},
		{
			name:                       "multiple event batches enabled",
			replicationMultipleBatches: true,
			tasks: []tasks.Task{
				&tasks.HistoryReplicationTask{
					WorkflowKey:  s.mutableState.GetWorkflowKey(),
					FirstEventID: firstEventID,
					NextEventID:  lastEventID + 1,
					Version:      version,
				},
			},
		},
	}

	ms := s.mutableState
	ms.transitionHistoryEnabled = false
	for _, tc := range testCases {
		s.Run(
			tc.name,
			func() {
				if s.replicationMultipleBatches != tc.replicationMultipleBatches {
					return
				}
				ms.InsertTasks[tasks.CategoryReplication] = []tasks.Task{}
				err := ms.closeTransactionPrepareReplicationTasks(historyi.TransactionPolicyActive, eventBatches, false)
				if err != nil {
					s.Fail("closeTransactionPrepareReplicationTasks failed", err)
				}
				repicationTasks := ms.InsertTasks[tasks.CategoryReplication]
				s.Equal(len(tc.tasks), len(repicationTasks))
				for i, task := range tc.tasks {
					s.Equal(task, repicationTasks[i])
				}
			},
		)
	}
}

func (s *mutableStateSuite) TestCloseTransactionPrepareReplicationTasks_SyncVersionedTransitionTask() {
	if s.replicationMultipleBatches == true {
		return
	}
	version := int64(777)
	firstEventID := int64(2)
	lastEventID := int64(3)
	now := time.Now().UTC()
	taskqueue := "taskqueue for test"
	workflowTaskTimeout := 11 * time.Second
	workflowTaskAttempt := int32(1)
	eventBatches := [][]*historypb.HistoryEvent{
		{
			historypb.HistoryEvent_builder{
				Version:   version,
				EventId:   firstEventID,
				EventTime: timestamppb.New(now),
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				WorkflowTaskScheduledEventAttributes: historypb.WorkflowTaskScheduledEventAttributes_builder{
					TaskQueue:           taskqueuepb.TaskQueue_builder{Name: taskqueue}.Build(),
					StartToCloseTimeout: durationpb.New(workflowTaskTimeout),
					Attempt:             workflowTaskAttempt,
				}.Build(),
			}.Build(),
		},
		{
			historypb.HistoryEvent_builder{
				Version:   version,
				EventId:   lastEventID,
				EventTime: timestamppb.New(now),
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
				WorkflowTaskStartedEventAttributes: historypb.WorkflowTaskStartedEventAttributes_builder{
					ScheduledEventId: firstEventID,
					RequestId:        uuid.NewString(),
				}.Build(),
			}.Build(),
		},
	}

	ms := s.mutableState
	ms.transitionHistoryEnabled = true
	ms.syncActivityTasks[1] = struct{}{}
	ms.pendingActivityInfoIDs[1] = persistencespb.ActivityInfo_builder{
		Version:          version,
		ScheduledEventId: 1,
	}.Build()
	ms.InsertTasks[tasks.CategoryReplication] = []tasks.Task{}
	transitionHistory := []*persistencespb.VersionedTransition{
		persistencespb.VersionedTransition_builder{
			NamespaceFailoverVersion: 1,
			TransitionCount:          10,
		}.Build(),
	}
	ms.executionInfo.SetTransitionHistory(transitionHistory)
	err := ms.closeTransactionPrepareReplicationTasks(historyi.TransactionPolicyActive, eventBatches, false)
	s.NoError(err)
	replicationTasks := ms.InsertTasks[tasks.CategoryReplication]
	s.Equal(1, len(replicationTasks))
	historyTasks := []tasks.Task{
		&tasks.HistoryReplicationTask{
			WorkflowKey:  s.mutableState.GetWorkflowKey(),
			FirstEventID: firstEventID,
			NextEventID:  firstEventID + 1,
			Version:      version,
		},
		&tasks.HistoryReplicationTask{
			WorkflowKey:  s.mutableState.GetWorkflowKey(),
			FirstEventID: lastEventID,
			NextEventID:  lastEventID + 1,
			Version:      version,
		},
	}
	expectedTask := &tasks.SyncVersionedTransitionTask{
		WorkflowKey:         s.mutableState.GetWorkflowKey(),
		ArchetypeID:         chasm.WorkflowArchetypeID,
		VisibilityTimestamp: now,
		Priority:            enumsspb.TASK_PRIORITY_HIGH,
		VersionedTransition: transitionHistory[0],
		FirstEventID:        firstEventID,
		NextEventID:         lastEventID + 1,
	}
	s.Equal(enumsspb.TASK_TYPE_REPLICATION_SYNC_VERSIONED_TRANSITION, replicationTasks[0].GetType())
	actualTask, ok := replicationTasks[0].(*tasks.SyncVersionedTransitionTask)
	s.True(ok)
	s.Equal(expectedTask.WorkflowKey, actualTask.WorkflowKey)
	s.Equal(expectedTask.VersionedTransition, actualTask.VersionedTransition)
	s.Equal(expectedTask.ArchetypeID, actualTask.ArchetypeID)
	s.Equal(3, len(actualTask.TaskEquivalents))
	s.Equal(historyTasks[0], actualTask.TaskEquivalents[0])
	s.Equal(historyTasks[1], actualTask.TaskEquivalents[1])
	s.Equal(enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY, actualTask.TaskEquivalents[2].GetType())
}

func (s *mutableStateSuite) TestMaxAllowedTimer() {
	testCases := []struct {
		name                   string
		runTimeout             time.Duration
		runTimeoutTimerDropped bool
	}{
		{
			name:                   "run timeout timer preserved",
			runTimeout:             time.Hour * 24 * 365,
			runTimeoutTimerDropped: false,
		},
		{
			name:                   "run timeout timer dropped",
			runTimeout:             time.Hour * 24 * 365 * 100,
			runTimeoutTimerDropped: true,
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).Times(1)
			s.mutableState = NewMutableState(s.mockShard, s.mockEventsCache, s.logger, s.namespaceEntry, tests.WorkflowID, tests.RunID, time.Now().UTC())

			workflowKey := s.mutableState.GetWorkflowKey()
			_, err := s.mutableState.AddWorkflowExecutionStartedEvent(
				commonpb.WorkflowExecution_builder{
					WorkflowId: workflowKey.WorkflowID,
					RunId:      workflowKey.RunID,
				}.Build(),
				historyservice.StartWorkflowExecutionRequest_builder{
					NamespaceId: workflowKey.NamespaceID,
					StartRequest: workflowservice.StartWorkflowExecutionRequest_builder{
						Namespace:  s.mutableState.GetNamespaceEntry().Name().String(),
						WorkflowId: workflowKey.WorkflowID,
						WorkflowType: commonpb.WorkflowType_builder{
							Name: "test-workflow-type",
						}.Build(),
						TaskQueue: taskqueuepb.TaskQueue_builder{
							Name: "test-task-queue",
						}.Build(),
						WorkflowRunTimeout: durationpb.New(tc.runTimeout),
					}.Build(),
				}.Build(),
			)
			s.NoError(err)

			snapshot, _, err := s.mutableState.CloseTransactionAsSnapshot(context.Background(), historyi.TransactionPolicyActive)
			s.NoError(err)

			timerTasks := snapshot.Tasks[tasks.CategoryTimer]
			if tc.runTimeoutTimerDropped {
				s.Empty(timerTasks)
			} else {
				s.Len(timerTasks, 1)
				s.Equal(enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT, timerTasks[0].GetType())
			}
		})
	}
}

func (s *mutableStateSuite) TestCloseTransactionPrepareReplicationTasks_SyncHSMTask() {
	version := s.mutableState.GetCurrentVersion()
	stateMachineDef := hsmtest.NewDefinition("test")
	err := s.mockShard.StateMachineRegistry().RegisterMachine(stateMachineDef)
	s.NoError(err)

	testCases := []struct {
		name                    string
		hsmEmpty                bool
		hsmDirty                bool
		eventBatches            [][]*historypb.HistoryEvent
		clearBufferEvents       bool
		expectedReplicationTask tasks.Task
	}{
		{
			name:     "WithEvents",
			hsmEmpty: false,
			hsmDirty: true,
			eventBatches: [][]*historypb.HistoryEvent{
				{
					historypb.HistoryEvent_builder{
						Version:                                version,
						EventId:                                5,
						EventTime:                              timestamppb.New(time.Now()),
						EventType:                              enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
						NexusOperationScheduledEventAttributes: &historypb.NexusOperationScheduledEventAttributes{},
					}.Build(),
				},
			},
			clearBufferEvents: false,
			expectedReplicationTask: &tasks.HistoryReplicationTask{
				WorkflowKey:  s.mutableState.GetWorkflowKey(),
				FirstEventID: 5,
				NextEventID:  6,
				Version:      version,
			},
		},
		{
			name:              "NoEvents",
			hsmEmpty:          false,
			hsmDirty:          true,
			eventBatches:      nil,
			clearBufferEvents: false,
			expectedReplicationTask: &tasks.SyncHSMTask{
				WorkflowKey: s.mutableState.GetWorkflowKey(),
			},
		},
		{
			name:                    "NoChildren/ClearBufferFalse",
			hsmEmpty:                true,
			hsmDirty:                false,
			eventBatches:            nil,
			clearBufferEvents:       false,
			expectedReplicationTask: nil,
		},
		{
			name:                    "NoChildren/ClearBufferTrue",
			hsmEmpty:                true,
			hsmDirty:                false,
			eventBatches:            nil,
			clearBufferEvents:       true,
			expectedReplicationTask: nil,
		},
		{
			name:                    "CleanChildren/ClearBufferFalse",
			hsmEmpty:                false,
			hsmDirty:                false,
			clearBufferEvents:       false,
			expectedReplicationTask: nil,
		},
		{
			name:              "CleanChildren/ClearBufferTrue",
			hsmEmpty:          false,
			hsmDirty:          false,
			clearBufferEvents: true,
			expectedReplicationTask: &tasks.SyncHSMTask{
				WorkflowKey: s.mutableState.GetWorkflowKey(),
			},
		},
	}

	for _, tc := range testCases {
		s.Run(
			tc.name,
			func() {
				if !tc.hsmEmpty {
					_, err = s.mutableState.HSM().AddChild(
						hsm.Key{Type: stateMachineDef.Type(), ID: "child_1"},
						hsmtest.NewData(hsmtest.State1),
					)
					s.NoError(err)

					if !tc.hsmDirty {
						s.mutableState.HSM().ClearTransactionState()
					}
				}
				s.mutableState.transitionHistoryEnabled = false
				err := s.mutableState.closeTransactionPrepareReplicationTasks(historyi.TransactionPolicyActive, tc.eventBatches, tc.clearBufferEvents)
				s.NoError(err)

				repicationTasks := s.mutableState.PopTasks()[tasks.CategoryReplication]

				if tc.expectedReplicationTask != nil {
					s.Len(repicationTasks, 1)
					s.Equal(tc.expectedReplicationTask, repicationTasks[0])
				} else {
					s.Empty(repicationTasks)
				}
			},
		)
	}
}

func (s *mutableStateSuite) setDisablingTransitionHistory(ms *MutableStateImpl) {
	ms.versionedTransitionInDB = persistencespb.VersionedTransition_builder{
		NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion(tests.WorkflowID),
		TransitionCount:          1025,
	}.Build()
	ms.executionInfo.SetTransitionHistory(nil)
}

func (s *mutableStateSuite) TestCloseTransactionPrepareReplicationTasks_SyncActivityTask() {
	testCases := []struct {
		name                       string
		disablingTransitionHistory bool
		expectedReplicationTask    []tasks.SyncActivityTask
	}{
		{
			name:                       "NoDisablingTransitionHistory",
			disablingTransitionHistory: false,
			expectedReplicationTask: []tasks.SyncActivityTask{
				{
					ScheduledEventID: 100,
				},
			},
		},
		{
			name:                       "DisablingTransitionHistory",
			disablingTransitionHistory: true,
			expectedReplicationTask: []tasks.SyncActivityTask{
				{
					ScheduledEventID: 90,
				},
				{
					ScheduledEventID: 100,
				},
			},
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			dbState := s.buildWorkflowMutableState()
			dbState.GetActivityInfos()[100] = persistencespb.ActivityInfo_builder{
				ScheduledEventId: 100,
			}.Build()
			ms, err := NewMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, s.namespaceEntry, dbState, 123)
			s.NoError(err)

			if tc.disablingTransitionHistory {
				s.setDisablingTransitionHistory(ms)
			}

			ms.UpdateActivityProgress(ms.pendingActivityInfoIDs[100], &workflowservice.RecordActivityTaskHeartbeatRequest{})

			repicationTasks := ms.syncActivityToReplicationTask(historyi.TransactionPolicyActive)
			s.Len(repicationTasks, len(tc.expectedReplicationTask))
			sort.Slice(repicationTasks, func(i, j int) bool {
				return repicationTasks[i].(*tasks.SyncActivityTask).ScheduledEventID < repicationTasks[j].(*tasks.SyncActivityTask).ScheduledEventID
			})
			for i, task := range tc.expectedReplicationTask {
				s.Equal(task.ScheduledEventID, repicationTasks[i].(*tasks.SyncActivityTask).ScheduledEventID)
			}
		})
	}
}

func (s *mutableStateSuite) TestVersionedTransitionInDB() {
	// case 1: versionedTransitionInDB is not nil
	dbState := s.buildWorkflowMutableState()
	ms, err := NewMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, s.namespaceEntry, dbState, 123)
	s.NoError(err)

	s.True(proto.Equal(ms.CurrentVersionedTransition(), ms.versionedTransitionInDB))

	s.NoError(ms.cleanupTransaction())
	s.True(proto.Equal(ms.CurrentVersionedTransition(), ms.versionedTransitionInDB))

	ms.executionInfo.SetTransitionHistory(nil)
	s.NoError(ms.cleanupTransaction())
	s.Nil(ms.versionedTransitionInDB)

	// case 2: versionedTransitionInDB is nil
	dbState = s.buildWorkflowMutableState()
	dbState.GetExecutionInfo().SetTransitionHistory(nil)
	ms, err = NewMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, s.namespaceEntry, dbState, 123)
	s.NoError(err)

	s.Nil(ms.versionedTransitionInDB)

	ms.executionInfo.SetTransitionHistory(UpdatedTransitionHistory(ms.executionInfo.GetTransitionHistory(), s.namespaceEntry.FailoverVersion(tests.WorkflowID)))
	s.NoError(ms.cleanupTransaction())
	s.True(proto.Equal(ms.CurrentVersionedTransition(), ms.versionedTransitionInDB))
}

func (s *mutableStateSuite) TestCloseTransactionTrackTombstones() {
	testCases := []struct {
		name        string
		tombstoneFn func(ms historyi.MutableState) (*persistencespb.StateMachineTombstone, error)
	}{
		{
			name: "Activity",
			tombstoneFn: func(mutableState historyi.MutableState) (*persistencespb.StateMachineTombstone, error) {
				var activityScheduleEventID int64
				for activityScheduleEventID = range mutableState.GetPendingActivityInfos() {
					break
				}
				_, err := mutableState.AddActivityTaskTimedOutEvent(
					activityScheduleEventID,
					common.EmptyEventID,
					failure.NewTimeoutFailure("test-timeout", enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START),
					enumspb.RETRY_STATE_TIMEOUT,
				)
				return persistencespb.StateMachineTombstone_builder{
					ActivityScheduledEventId: proto.Int64(activityScheduleEventID),
				}.Build(), err
			},
		},
		{
			name: "UserTimer",
			tombstoneFn: func(mutableState historyi.MutableState) (*persistencespb.StateMachineTombstone, error) {
				var timerID string
				for timerID = range mutableState.GetPendingTimerInfos() {
					break
				}
				_, err := mutableState.AddTimerFiredEvent(timerID)
				return persistencespb.StateMachineTombstone_builder{
					TimerId: proto.String(timerID),
				}.Build(), err
			},
		},
		{
			name: "ChildWorkflow",
			tombstoneFn: func(mutableState historyi.MutableState) (*persistencespb.StateMachineTombstone, error) {
				var initiatedEventId int64
				var ci *persistencespb.ChildExecutionInfo
				for initiatedEventId, ci = range mutableState.GetPendingChildExecutionInfos() {
					break
				}
				childExecution := commonpb.WorkflowExecution_builder{
					WorkflowId: uuid.NewString(),
					RunId:      uuid.NewString(),
				}.Build()
				_, err := mutableState.AddChildWorkflowExecutionStartedEvent(
					childExecution,
					commonpb.WorkflowType_builder{Name: ci.GetWorkflowTypeName()}.Build(),
					initiatedEventId,
					nil,
					nil,
				)
				if err != nil {
					return nil, err
				}
				_, err = mutableState.AddChildWorkflowExecutionTerminatedEvent(
					initiatedEventId,
					childExecution,
				)
				return persistencespb.StateMachineTombstone_builder{
					ChildExecutionInitiatedEventId: proto.Int64(initiatedEventId),
				}.Build(), err
			},
		},
		{
			name: "RequestCancelExternal",
			tombstoneFn: func(mutableState historyi.MutableState) (*persistencespb.StateMachineTombstone, error) {
				var initiatedEventId int64
				for initiatedEventId = range mutableState.GetPendingRequestCancelExternalInfos() {
					break
				}
				_, err := mutableState.AddRequestCancelExternalWorkflowExecutionFailedEvent(
					initiatedEventId,
					s.namespaceEntry.Name(),
					s.namespaceEntry.ID(),
					uuid.NewString(),
					uuid.NewString(),
					enumspb.CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND,
				)
				return persistencespb.StateMachineTombstone_builder{
					RequestCancelInitiatedEventId: proto.Int64(initiatedEventId),
				}.Build(), err
			},
		},
		{
			name: "SignalExternal",
			tombstoneFn: func(mutableState historyi.MutableState) (*persistencespb.StateMachineTombstone, error) {
				var initiatedEventId int64
				for initiatedEventId = range mutableState.GetPendingSignalExternalInfos() {
					break
				}
				_, err := mutableState.AddSignalExternalWorkflowExecutionFailedEvent(
					initiatedEventId,
					s.namespaceEntry.Name(),
					s.namespaceEntry.ID(),
					uuid.NewString(),
					uuid.NewString(),
					"",
					enumspb.SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND,
				)
				return persistencespb.StateMachineTombstone_builder{
					SignalExternalInitiatedEventId: proto.Int64(initiatedEventId),
				}.Build(), err
			},
		},
		{
			name: "CHASM",
			tombstoneFn: func(mutableState historyi.MutableState) (*persistencespb.StateMachineTombstone, error) {
				deletedNodePath := "deleted-node-path"
				tombstone := persistencespb.StateMachineTombstone_builder{
					ChasmNodePath: proto.String(deletedNodePath),
				}.Build()

				mockChasmTree := historyi.NewMockChasmTree(s.controller)
				mockChasmTree.EXPECT().ArchetypeID().Return(chasm.ArchetypeID(1234)).AnyTimes()
				gomock.InOrder(
					mockChasmTree.EXPECT().IsStateDirty().Return(true).AnyTimes(),
					mockChasmTree.EXPECT().CloseTransaction().Return(chasm.NodesMutation{
						DeletedNodes: map[string]struct{}{deletedNodePath: {}},
					}, nil),
				)
				mutableState.(*MutableStateImpl).chasmTree = mockChasmTree

				return tombstone, nil
			},
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			dbState := s.buildWorkflowMutableState()

			mutableState, err := NewMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, s.namespaceEntry, dbState, 123)
			s.NoError(err)

			currentVersionedTransition := mutableState.CurrentVersionedTransition()
			newVersionedTranstion := common.CloneProto(currentVersionedTransition)
			newVersionedTranstion.SetTransitionCount(newVersionedTranstion.GetTransitionCount() + 1)

			_, err = mutableState.StartTransaction(s.namespaceEntry)
			s.NoError(err)

			expectedTombstone, err := tc.tombstoneFn(mutableState)
			s.NoError(err)

			_, _, err = mutableState.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
			s.NoError(err)

			tombstoneBatches := mutableState.GetExecutionInfo().GetSubStateMachineTombstoneBatches()
			s.Len(tombstoneBatches, 1)
			tombstoneBatch := tombstoneBatches[0]
			protorequire.ProtoEqual(s.T(), newVersionedTranstion, tombstoneBatch.GetVersionedTransition())
			s.True(tombstoneExists(tombstoneBatch.GetStateMachineTombstones(), expectedTombstone))
		})
	}
}

func tombstoneExists(
	tombstones []*persistencespb.StateMachineTombstone,
	expectedTombstone *persistencespb.StateMachineTombstone,
) bool {
	for _, tombstone := range tombstones {
		if tombstone.Equal(expectedTombstone) {
			return true
		}
	}
	return false
}

func (s *mutableStateSuite) TestCloseTransactionTrackTombstones_CapIfLargerThanLimit() {
	dbState := s.buildWorkflowMutableState()

	mutableState, err := NewMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, s.namespaceEntry, dbState, 123)
	s.NoError(err)
	mutableState.executionInfo.SetSubStateMachineTombstoneBatches([]*persistencespb.StateMachineTombstoneBatch{
		persistencespb.StateMachineTombstoneBatch_builder{
			VersionedTransition: persistencespb.VersionedTransition_builder{
				NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion(tests.WorkflowID),
				TransitionCount:          1,
			}.Build(),
		}.Build(),
	})

	currentVersionedTransition := mutableState.CurrentVersionedTransition()
	newVersionedTranstion := common.CloneProto(currentVersionedTransition)
	newVersionedTranstion.SetTransitionCount(newVersionedTranstion.GetTransitionCount() + 1)
	signalMap := mutableState.GetPendingSignalExternalInfos()
	for i := 0; i < s.mockConfig.MutableStateTombstoneCountLimit(); i++ {
		signalMap[int64(76+i)] = persistencespb.SignalInfo_builder{

			Version:               s.namespaceEntry.FailoverVersion(tests.WorkflowID),
			InitiatedEventId:      int64(76 + i),
			InitiatedEventBatchId: 17,
			RequestId:             uuid.NewString(),
		}.Build()
	}

	_, err = mutableState.StartTransaction(s.namespaceEntry)
	s.NoError(err)
	var initiatedEventId int64
	for initiatedEventId = range signalMap {
		_, err := mutableState.AddSignalExternalWorkflowExecutionFailedEvent(
			initiatedEventId,
			s.namespaceEntry.Name(),
			s.namespaceEntry.ID(),
			uuid.NewString(),
			uuid.NewString(),
			"",
			enumspb.SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND,
		)
		s.NoError(err)
	}

	_, _, err = mutableState.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
	s.NoError(err)

	tombstoneBatches := mutableState.GetExecutionInfo().GetSubStateMachineTombstoneBatches()
	s.Len(tombstoneBatches, 0)
}

func (s *mutableStateSuite) TestCloseTransactionTrackTombstones_OnlyTrackFirstEmpty() {
	dbState := s.buildWorkflowMutableState()

	mutableState, err := NewMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, s.namespaceEntry, dbState, 123)
	s.NoError(err)
	mutableState.executionInfo.SetSubStateMachineTombstoneBatches([]*persistencespb.StateMachineTombstoneBatch{
		persistencespb.StateMachineTombstoneBatch_builder{
			VersionedTransition: persistencespb.VersionedTransition_builder{
				NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion(tests.WorkflowID),
				TransitionCount:          1,
			}.Build(),
		}.Build(),
	})

	currentVersionedTransition := mutableState.CurrentVersionedTransition()
	newVersionedTranstion := common.CloneProto(currentVersionedTransition)
	newVersionedTranstion.SetTransitionCount(newVersionedTranstion.GetTransitionCount() + 1)

	_, err = mutableState.StartTransaction(s.namespaceEntry)
	s.NoError(err)

	_, _, err = mutableState.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
	s.NoError(err)

	tombstoneBatches := mutableState.GetExecutionInfo().GetSubStateMachineTombstoneBatches()
	s.Len(tombstoneBatches, 1)
	s.Equal(int64(1), tombstoneBatches[0].GetVersionedTransition().GetTransitionCount())
}

func (s *mutableStateSuite) TestCloseTransactionGenerateCHASMRetentionTask_Workflow() {
	dbState := s.buildWorkflowMutableState()

	mutableState, err := NewMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, s.namespaceEntry, dbState, 123)
	s.NoError(err)

	// First close transaction once to get rid of unrelated tasks like UserTimer and ActivityTimeout
	_, err = mutableState.StartTransaction(s.namespaceEntry)
	s.NoError(err)
	_, _, err = mutableState.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
	s.NoError(err)

	// Switch to a mock CHASM tree
	mockChasmTree := historyi.NewMockChasmTree(s.controller)
	mutableState.chasmTree = mockChasmTree

	// Is workflow, should not generate retention task
	mockChasmTree.EXPECT().IsStateDirty().Return(true).AnyTimes()
	mockChasmTree.EXPECT().ArchetypeID().Return(chasm.WorkflowArchetypeID).AnyTimes()
	mockChasmTree.EXPECT().CloseTransaction().Return(chasm.NodesMutation{}, nil).AnyTimes()
	mutation, _, err := mutableState.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
	s.NoError(err)
	s.Empty(mutation.Tasks[tasks.CategoryTimer])
}

func (s *mutableStateSuite) TestCloseTransactionGenerateCHASMRetentionTask_NonWorkflow_Active() {
	dbState := s.buildWorkflowMutableState()

	mutableState, err := NewMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, s.namespaceEntry, dbState, 123)
	s.NoError(err)

	// First close transaction once to get rid of unrelated tasks like UserTimer and ActivityTimeout
	_, err = mutableState.StartTransaction(s.namespaceEntry)
	s.NoError(err)
	_, _, err = mutableState.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
	s.NoError(err)

	// Switch to a mock CHASM tree
	mockChasmTree := historyi.NewMockChasmTree(s.controller)
	mutableState.chasmTree = mockChasmTree

	mockChasmTree.EXPECT().IsStateDirty().Return(true).AnyTimes()
	mockChasmTree.EXPECT().ArchetypeID().Return(chasm.WorkflowArchetypeID + 101).AnyTimes()
	mockChasmTree.EXPECT().CloseTransaction().Return(chasm.NodesMutation{}, nil).AnyTimes()
	_, err = mutableState.UpdateWorkflowStateStatus(
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	)
	s.NoError(err)
	mutation, _, err := mutableState.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
	s.NoError(err)
	s.Len(mutation.Tasks[tasks.CategoryTimer], 1)
	s.Equal(enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT, mutation.Tasks[tasks.CategoryTimer][0].GetType())
	actualCloseTime, err := mutableState.GetWorkflowCloseTime(context.Background())
	s.NoError(err)
	s.False(actualCloseTime.IsZero())

	// Already closed before, should not generate retention task again.
	mutation, _, err = mutableState.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
	s.NoError(err)
	s.Empty(mutation.Tasks[tasks.CategoryTimer])
}

func (s *mutableStateSuite) TestCloseTransactionGenerateCHASMRetentionTask_NonWorkflow_Passive() {
	dbState := s.buildWorkflowMutableState()

	mutableState, err := NewMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, s.namespaceEntry, dbState, 123)
	s.NoError(err)

	// First close transaction once to get rid of unrelated tasks like UserTimer and ActivityTimeout
	_, err = mutableState.StartTransaction(s.namespaceEntry)
	s.NoError(err)
	_, _, err = mutableState.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
	s.NoError(err)

	// Switch to a mock CHASM tree
	mockChasmTree := historyi.NewMockChasmTree(s.controller)
	mutableState.chasmTree = mockChasmTree

	mockChasmTree.EXPECT().IsStateDirty().Return(true).AnyTimes()
	mockChasmTree.EXPECT().ArchetypeID().Return(chasm.WorkflowArchetypeID + 101).AnyTimes()
	mockChasmTree.EXPECT().CloseTransaction().Return(chasm.NodesMutation{}, nil).AnyTimes()
	mockChasmTree.EXPECT().ApplyMutation(gomock.Any()).Return(nil).AnyTimes()

	// On standby side, multiple transactions can be applied at the same time,
	// the latest VT may be larger than execution state's LastUpdateVT, we still need to
	// generate retention task in this case.
	updatedExecutionInfo := common.CloneProto(mutableState.GetExecutionInfo())
	closeTime := time.Now()
	currentTransitionCount := updatedExecutionInfo.GetTransitionHistory()[0].GetTransitionCount()
	updatedExecutionInfo.GetTransitionHistory()[0].SetTransitionCount(updatedExecutionInfo.GetTransitionHistory()[0].GetTransitionCount() + 10)
	updatedExecutionInfo.SetCloseTime(timestamppb.New(closeTime))

	updatedExecutionState := common.CloneProto(mutableState.GetExecutionState())
	updatedExecutionState.SetState(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED)
	updatedExecutionState.SetStatus(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED)
	updatedExecutionState.SetLastUpdateVersionedTransition(persistencespb.VersionedTransition_builder{
		NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion(s.mutableState.executionInfo.GetWorkflowId()),
		TransitionCount:          currentTransitionCount + 1,
	}.Build())

	err = mutableState.ApplyMutation(persistencespb.WorkflowMutableStateMutation_builder{
		ExecutionInfo:  updatedExecutionInfo,
		ExecutionState: updatedExecutionState,
	}.Build())
	s.NoError(err)

	mutation, _, err := mutableState.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyPassive)
	s.NoError(err)
	s.Len(mutation.Tasks[tasks.CategoryTimer], 1)
	s.Equal(enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT, mutation.Tasks[tasks.CategoryTimer][0].GetType())
	actualCloseTime, err := mutableState.GetWorkflowCloseTime(context.Background())
	s.NoError(err)
	s.True(actualCloseTime.Equal(closeTime))

	// Already closed before, should not generate retention task again.
	mutation, _, err = mutableState.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyPassive)
	s.NoError(err)
	s.Empty(mutation.Tasks[tasks.CategoryTimer])
}

func (s *mutableStateSuite) TestExecutionInfoClone() {
	newInstance := reflect.New(reflect.TypeOf(s.mutableState.executionInfo).Elem()).Interface()
	clone, ok := newInstance.(*persistencespb.WorkflowExecutionInfo)
	if !ok {
		s.T().Fatal("type assertion to *persistencespb.WorkflowExecutionInfo failed")
	}
	clone.SetNamespaceId("namespace-id")
	clone.SetWorkflowId("workflow-id")
	err := common.MergeProtoExcludingFields(s.mutableState.executionInfo, clone, func(v any) []interface{} {
		info, ok := v.(*persistencespb.WorkflowExecutionInfo)
		if !ok || info == nil {
			return nil
		}
		return []interface{}{proto.String(
			info.GetNamespaceId(),
		)}
	})
	s.Nil(err)
}

func (s *mutableStateSuite) addChangesForStateReplication(state *persistencespb.WorkflowMutableState) {
	// These fields will be updated during ApplySnapshot
	proto.Merge(state.GetExecutionInfo(), persistencespb.WorkflowExecutionInfo_builder{
		LastUpdateTime: timestamp.TimeNowPtrUtc(),
	}.Build())
	proto.Merge(state.GetExecutionState(), persistencespb.WorkflowExecutionState_builder{
		State: enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
	}.Build())

	state.GetActivityInfos()[90].SetTimerTaskStatus(TimerTaskStatusCreated)
	state.GetTimerInfos()["25"].SetExpiryTime(timestamp.TimeNowPtrUtcAddDuration(time.Hour))
	state.GetChildExecutionInfos()[80].SetStartedEventId(84)
	state.GetRequestCancelInfos()[70].SetCancelRequestId(uuid.NewString())
	state.GetSignalInfos()[75].SetRequestId(uuid.NewString())

	// These infos will be deleted during ApplySnapshot
	state.GetActivityInfos()[89] = &persistencespb.ActivityInfo{}
	state.GetTimerInfos()["to-be-deleted"] = &persistencespb.TimerInfo{}
	state.GetChildExecutionInfos()[79] = &persistencespb.ChildExecutionInfo{}
	state.GetRequestCancelInfos()[69] = &persistencespb.RequestCancelInfo{}
	state.GetSignalInfos()[74] = &persistencespb.SignalInfo{}
	state.SetSignalRequestedIds([]string{"to-be-deleted"})
}

func compareMapOfProto[K comparable, V proto.Message](s *mutableStateSuite, expected, actual map[K]V) {
	s.Equal(len(expected), len(actual))
	for k, v := range expected {
		s.True(proto.Equal(v, actual[k]))
	}
}

func (s *mutableStateSuite) verifyChildExecutionInfos(expectedMap, actualMap, originMap map[int64]*persistencespb.ChildExecutionInfo) {
	s.Equal(len(expectedMap), len(actualMap))
	for k, expected := range expectedMap {
		actual, ok := actualMap[k]
		s.True(ok)
		origin := originMap[k]

		s.Equal(expected.GetVersion(), actual.GetVersion(), "Version mismatch")
		s.Equal(expected.GetInitiatedEventBatchId(), actual.GetInitiatedEventBatchId(), "InitiatedEventBatchId mismatch")
		s.Equal(expected.GetStartedEventId(), actual.GetStartedEventId(), "StartedEventId mismatch")
		s.Equal(expected.GetStartedWorkflowId(), actual.GetStartedWorkflowId(), "StartedWorkflowId mismatch")
		s.Equal(expected.GetStartedRunId(), actual.GetStartedRunId(), "StartedRunId mismatch")
		s.Equal(expected.GetCreateRequestId(), actual.GetCreateRequestId(), "CreateRequestId mismatch")
		s.Equal(expected.GetNamespace(), actual.GetNamespace(), "Namespace mismatch")
		s.Equal(expected.GetWorkflowTypeName(), actual.GetWorkflowTypeName(), "WorkflowTypeName mismatch")
		s.Equal(expected.GetParentClosePolicy(), actual.GetParentClosePolicy(), "ParentClosePolicy mismatch")
		s.Equal(expected.GetInitiatedEventId(), actual.GetInitiatedEventId(), "InitiatedEventId mismatch")
		s.Equal(expected.GetNamespaceId(), actual.GetNamespaceId(), "NamespaceId mismatch")
		s.True(proto.Equal(expected.GetLastUpdateVersionedTransition(), actual.GetLastUpdateVersionedTransition()), "LastUpdateVersionedTransition mismatch")

		// special handled fields
		if origin != nil {
			s.Equal(origin.GetClock(), actual.GetClock(), "Clock mismatch")
		}
	}
}

func (s *mutableStateSuite) verifyActivityInfos(expectedMap, actualMap map[int64]*persistencespb.ActivityInfo) {
	s.Equal(len(expectedMap), len(actualMap))
	for k, expected := range expectedMap {
		actual, ok := actualMap[k]
		s.True(ok)

		s.Equal(expected.GetVersion(), actual.GetVersion(), "Version mismatch")
		s.Equal(expected.GetScheduledEventBatchId(), actual.GetScheduledEventBatchId(), "ScheduledEventBatchId mismatch")
		s.True(proto.Equal(expected.GetScheduledTime(), actual.GetScheduledTime()), "ScheduledTime mismatch")
		s.Equal(expected.GetStartedEventId(), actual.GetStartedEventId(), "StartedEventId mismatch")
		s.True(proto.Equal(expected.GetStartedTime(), actual.GetStartedTime()), "StartedTime mismatch")
		s.Equal(expected.GetActivityId(), actual.GetActivityId(), "ActivityId mismatch")
		s.Equal(expected.GetRequestId(), actual.GetRequestId(), "RequestId mismatch")
		s.True(proto.Equal(expected.GetScheduleToStartTimeout(), actual.GetScheduleToStartTimeout()), "ScheduleToStartTimeout mismatch")
		s.True(proto.Equal(expected.GetScheduleToCloseTimeout(), actual.GetScheduleToCloseTimeout()), "ScheduleToCloseTimeout mismatch")
		s.True(proto.Equal(expected.GetStartToCloseTimeout(), actual.GetStartToCloseTimeout()), "StartToCloseTimeout mismatch")
		s.True(proto.Equal(expected.GetHeartbeatTimeout(), actual.GetHeartbeatTimeout()), "HeartbeatTimeout mismatch")
		s.Equal(expected.GetCancelRequested(), actual.GetCancelRequested(), "CancelRequested mismatch")
		s.Equal(expected.GetCancelRequestId(), actual.GetCancelRequestId(), "CancelRequestId mismatch")
		s.Equal(expected.GetAttempt(), actual.GetAttempt(), "Attempt mismatch")
		s.Equal(expected.GetTaskQueue(), actual.GetTaskQueue(), "TaskQueue mismatch")
		s.Equal(expected.GetStartedIdentity(), actual.GetStartedIdentity(), "StartedIdentity mismatch")
		s.Equal(expected.GetHasRetryPolicy(), actual.GetHasRetryPolicy(), "HasRetryPolicy mismatch")
		s.True(proto.Equal(expected.GetRetryInitialInterval(), actual.GetRetryInitialInterval()), "RetryInitialInterval mismatch")
		s.True(proto.Equal(expected.GetRetryMaximumInterval(), actual.GetRetryMaximumInterval()), "RetryMaximumInterval mismatch")
		s.Equal(expected.GetRetryMaximumAttempts(), actual.GetRetryMaximumAttempts(), "RetryMaximumAttempts mismatch")
		s.True(proto.Equal(expected.GetRetryExpirationTime(), actual.GetRetryExpirationTime()), "RetryExpirationTime mismatch")
		s.Equal(expected.GetRetryBackoffCoefficient(), actual.GetRetryBackoffCoefficient(), "RetryBackoffCoefficient mismatch")
		s.Equal(expected.GetRetryNonRetryableErrorTypes(), actual.GetRetryNonRetryableErrorTypes(), "RetryNonRetryableErrorTypes mismatch")
		s.True(proto.Equal(expected.GetRetryLastFailure(), actual.GetRetryLastFailure()), "RetryLastFailure mismatch")
		s.Equal(expected.GetRetryLastWorkerIdentity(), actual.GetRetryLastWorkerIdentity(), "RetryLastWorkerIdentity mismatch")
		s.Equal(expected.GetScheduledEventId(), actual.GetScheduledEventId(), "ScheduledEventId mismatch")
		s.True(proto.Equal(expected.GetLastHeartbeatDetails(), actual.GetLastHeartbeatDetails()), "LastHeartbeatDetails mismatch")
		s.True(proto.Equal(expected.GetLastHeartbeatUpdateTime(), actual.GetLastHeartbeatUpdateTime()), "LastHeartbeatUpdateTime mismatch")
		s.Equal(expected.GetUseCompatibleVersion(), actual.GetUseCompatibleVersion(), "UseCompatibleVersion mismatch")
		s.True(proto.Equal(expected.GetActivityType(), actual.GetActivityType()), "ActivityType mismatch")
		s.True(proto.Equal(expected.GetLastWorkerVersionStamp(), actual.GetLastWorkerVersionStamp()), "LastWorkerVersionStamp mismatch")
		s.True(proto.Equal(expected.GetLastStartedDeployment(), actual.GetLastStartedDeployment()), "LastStartedDeployment mismatch")
		s.True(proto.Equal(expected.GetLastUpdateVersionedTransition(), actual.GetLastUpdateVersionedTransition()), "LastUpdateVersionedTransition mismatch")

		// special handled fields
		s.Equal(int32(TimerTaskStatusNone), actual.GetTimerTaskStatus(), "TimerTaskStatus mismatch")
	}
}

func (s *mutableStateSuite) verifyExecutionInfo(current, target, origin *persistencespb.WorkflowExecutionInfo) {
	// These fields should not change.
	s.Equal(origin.GetWorkflowTaskVersion(), current.GetWorkflowTaskVersion(), "WorkflowTaskVersion mismatch")
	s.Equal(origin.GetWorkflowTaskScheduledEventId(), current.GetWorkflowTaskScheduledEventId(), "WorkflowTaskScheduledEventId mismatch")
	s.Equal(origin.GetWorkflowTaskStartedEventId(), current.GetWorkflowTaskStartedEventId(), "WorkflowTaskStartedEventId mismatch")
	s.Equal(origin.GetWorkflowTaskRequestId(), current.GetWorkflowTaskRequestId(), "WorkflowTaskRequestId mismatch")
	s.Equal(origin.GetWorkflowTaskTimeout(), current.GetWorkflowTaskTimeout(), "WorkflowTaskTimeout mismatch")
	s.Equal(origin.GetWorkflowTaskAttempt(), current.GetWorkflowTaskAttempt(), "WorkflowTaskAttempt mismatch")
	s.Equal(origin.GetWorkflowTaskStartedTime(), current.GetWorkflowTaskStartedTime(), "WorkflowTaskStartedTime mismatch")
	s.Equal(origin.GetWorkflowTaskScheduledTime(), current.GetWorkflowTaskScheduledTime(), "WorkflowTaskScheduledTime mismatch")
	s.Equal(origin.GetWorkflowTaskOriginalScheduledTime(), current.GetWorkflowTaskOriginalScheduledTime(), "WorkflowTaskOriginalScheduledTime mismatch")
	s.Equal(origin.GetWorkflowTaskType(), current.GetWorkflowTaskType(), "WorkflowTaskType mismatch")
	s.Equal(origin.GetWorkflowTaskSuggestContinueAsNew(), current.GetWorkflowTaskSuggestContinueAsNew(), "WorkflowTaskSuggestContinueAsNew mismatch")
	s.Equal(origin.GetWorkflowTaskSuggestContinueAsNewReasons(), current.GetWorkflowTaskSuggestContinueAsNewReasons(), "WorkflowTaskSuggestContinueAsNewReasons mismatch")
	s.Equal(origin.GetWorkflowTaskHistorySizeBytes(), current.GetWorkflowTaskHistorySizeBytes(), "WorkflowTaskHistorySizeBytes mismatch")
	s.Equal(origin.GetWorkflowTaskBuildId(), current.GetWorkflowTaskBuildId(), "WorkflowTaskBuildId mismatch")
	s.Equal(origin.GetWorkflowTaskBuildIdRedirectCounter(), current.GetWorkflowTaskBuildIdRedirectCounter(), "WorkflowTaskBuildIdRedirectCounter mismatch")
	s.True(proto.Equal(origin.GetVersioningInfo(), current.GetVersioningInfo()), "VersioningInfo mismatch")
	s.True(proto.Equal(origin.GetVersionHistories(), current.GetVersionHistories()), "VersionHistories mismatch")
	s.True(proto.Equal(origin.GetExecutionStats(), current.GetExecutionStats()), "ExecutionStats mismatch")
	s.Equal(origin.GetLastFirstEventTxnId(), current.GetLastFirstEventTxnId(), "LastFirstEventTxnId mismatch")
	s.True(proto.Equal(origin.GetParentClock(), current.GetParentClock()), "ParentClock mismatch")
	s.Equal(origin.GetCloseTransferTaskId(), current.GetCloseTransferTaskId(), "CloseTransferTaskId mismatch")
	s.Equal(origin.GetCloseVisibilityTaskId(), current.GetCloseVisibilityTaskId(), "CloseVisibilityTaskId mismatch")
	s.Equal(origin.GetRelocatableAttributesRemoved(), current.GetRelocatableAttributesRemoved(), "RelocatableAttributesRemoved mismatch")
	s.Equal(origin.GetWorkflowExecutionTimerTaskStatus(), current.GetWorkflowExecutionTimerTaskStatus(), "WorkflowExecutionTimerTaskStatus mismatch")
	s.Equal(origin.GetSubStateMachinesByType(), current.GetSubStateMachinesByType(), "SubStateMachinesByType mismatch")
	s.Equal(origin.GetStateMachineTimers(), current.GetStateMachineTimers(), "StateMachineTimers mismatch")
	s.Equal(origin.GetTaskGenerationShardClockTimestamp(), current.GetTaskGenerationShardClockTimestamp(), "TaskGenerationShardClockTimestamp mismatch")
	s.Equal(origin.GetUpdateInfos(), current.GetUpdateInfos(), "UpdateInfos mismatch")

	// These fields should be updated.
	s.Equal(target.GetNamespaceId(), current.GetNamespaceId(), "NamespaceId mismatch")
	s.Equal(target.GetWorkflowId(), current.GetWorkflowId(), "WorkflowId mismatch")
	s.Equal(target.GetParentNamespaceId(), current.GetParentNamespaceId(), "ParentNamespaceId mismatch")
	s.Equal(target.GetParentWorkflowId(), current.GetParentWorkflowId(), "ParentWorkflowId mismatch")
	s.Equal(target.GetParentRunId(), current.GetParentRunId(), "ParentRunId mismatch")
	s.Equal(target.GetParentInitiatedId(), current.GetParentInitiatedId(), "ParentInitiatedId mismatch")
	s.Equal(target.GetCompletionEventBatchId(), current.GetCompletionEventBatchId(), "CompletionEventBatchId mismatch")
	s.Equal(target.GetTaskQueue(), current.GetTaskQueue(), "TaskQueue mismatch")
	s.Equal(target.GetWorkflowTypeName(), current.GetWorkflowTypeName(), "WorkflowTypeName mismatch")
	s.True(proto.Equal(target.GetWorkflowExecutionTimeout(), current.GetWorkflowExecutionTimeout()), "WorkflowExecutionTimeout mismatch")
	s.True(proto.Equal(target.GetWorkflowRunTimeout(), current.GetWorkflowRunTimeout()), "WorkflowRunTimeout mismatch")
	s.True(proto.Equal(target.GetDefaultWorkflowTaskTimeout(), current.GetDefaultWorkflowTaskTimeout()), "DefaultWorkflowTaskTimeout mismatch")
	s.Equal(target.GetLastRunningClock(), current.GetLastRunningClock(), "LastRunningClock mismatch")
	s.Equal(target.GetLastFirstEventId(), current.GetLastFirstEventId(), "LastFirstEventId mismatch")
	s.Equal(target.GetLastCompletedWorkflowTaskStartedEventId(), current.GetLastCompletedWorkflowTaskStartedEventId(), "LastCompletedWorkflowTaskStartedEventId mismatch")
	s.True(proto.Equal(target.GetStartTime(), current.GetStartTime()), "StartTime mismatch")
	s.True(proto.Equal(target.GetLastUpdateTime(), current.GetLastUpdateTime()), "LastUpdateTime mismatch")
	s.Equal(target.GetCancelRequested(), current.GetCancelRequested(), "CancelRequested mismatch")
	s.Equal(target.GetCancelRequestId(), current.GetCancelRequestId(), "CancelRequestId mismatch")
	s.Equal(target.GetStickyTaskQueue(), current.GetStickyTaskQueue(), "StickyTaskQueue mismatch")
	s.True(proto.Equal(target.GetStickyScheduleToStartTimeout(), current.GetStickyScheduleToStartTimeout()), "StickyScheduleToStartTimeout mismatch")
	s.Equal(target.GetAttempt(), current.GetAttempt(), "Attempt mismatch")
	s.Equal(target.GetWorkflowTaskStamp(), current.GetWorkflowTaskStamp(), "WorkflowTaskStamp mismatch")
	s.True(proto.Equal(target.GetRetryInitialInterval(), current.GetRetryInitialInterval()), "RetryInitialInterval mismatch")
	s.True(proto.Equal(target.GetRetryMaximumInterval(), current.GetRetryMaximumInterval()), "RetryMaximumInterval mismatch")
	s.Equal(target.GetRetryMaximumAttempts(), current.GetRetryMaximumAttempts(), "RetryMaximumAttempts mismatch")
	s.Equal(target.GetRetryBackoffCoefficient(), current.GetRetryBackoffCoefficient(), "RetryBackoffCoefficient mismatch")
	s.True(proto.Equal(target.GetWorkflowExecutionExpirationTime(), current.GetWorkflowExecutionExpirationTime()), "WorkflowExecutionExpirationTime mismatch")
	s.Equal(target.GetRetryNonRetryableErrorTypes(), current.GetRetryNonRetryableErrorTypes(), "RetryNonRetryableErrorTypes mismatch")
	s.Equal(target.GetHasRetryPolicy(), current.GetHasRetryPolicy(), "HasRetryPolicy mismatch")
	s.Equal(target.GetCronSchedule(), current.GetCronSchedule(), "CronSchedule mismatch")
	s.Equal(target.GetSignalCount(), current.GetSignalCount(), "SignalCount mismatch")
	s.Equal(target.GetActivityCount(), current.GetActivityCount(), "ActivityCount mismatch")
	s.Equal(target.GetChildExecutionCount(), current.GetChildExecutionCount(), "ChildExecutionCount mismatch")
	s.Equal(target.GetUserTimerCount(), current.GetUserTimerCount(), "UserTimerCount mismatch")
	s.Equal(target.GetRequestCancelExternalCount(), current.GetRequestCancelExternalCount(), "RequestCancelExternalCount mismatch")
	s.Equal(target.GetSignalExternalCount(), current.GetSignalExternalCount(), "SignalExternalCount mismatch")
	s.Equal(target.GetUpdateCount(), current.GetUpdateCount(), "UpdateCount mismatch")
	s.True(proto.Equal(target.GetAutoResetPoints(), current.GetAutoResetPoints()), "AutoResetPoints mismatch")
	s.Equal(target.GetSearchAttributes(), current.GetSearchAttributes(), "SearchAttributes mismatch")
	s.Equal(target.GetMemo(), current.GetMemo(), "Memo mismatch")
	s.Equal(target.GetFirstExecutionRunId(), current.GetFirstExecutionRunId(), "FirstExecutionRunId mismatch")
	s.True(proto.Equal(target.GetWorkflowRunExpirationTime(), current.GetWorkflowRunExpirationTime()), "WorkflowRunExpirationTime mismatch")
	s.Equal(target.GetStateTransitionCount(), current.GetStateTransitionCount(), "StateTransitionCount mismatch")
	s.True(proto.Equal(target.GetExecutionTime(), current.GetExecutionTime()), "ExecutionTime mismatch")
	s.Equal(target.GetNewExecutionRunId(), current.GetNewExecutionRunId(), "NewExecutionRunId mismatch")
	s.Equal(target.GetParentInitiatedVersion(), current.GetParentInitiatedVersion(), "ParentInitiatedVersion mismatch")
	s.True(proto.Equal(target.GetCloseTime(), current.GetCloseTime()), "CloseTime mismatch")
	s.True(proto.Equal(target.GetBaseExecutionInfo(), current.GetBaseExecutionInfo()), "BaseExecutionInfo mismatch")
	s.True(proto.Equal(target.GetMostRecentWorkerVersionStamp(), current.GetMostRecentWorkerVersionStamp()), "MostRecentWorkerVersionStamp mismatch")
	s.Equal(target.GetAssignedBuildId(), current.GetAssignedBuildId(), "AssignedBuildId mismatch")
	s.Equal(target.GetInheritedBuildId(), current.GetInheritedBuildId(), "InheritedBuildId mismatch")
	s.Equal(target.GetBuildIdRedirectCounter(), current.GetBuildIdRedirectCounter(), "BuildIdRedirectCounter mismatch")
	s.Equal(target.GetSubStateMachinesByType(), current.GetSubStateMachinesByType(), "SubStateMachinesByType mismatch")
	s.Equal(target.GetRootWorkflowId(), current.GetRootWorkflowId(), "RootWorkflowId mismatch")
	s.Equal(target.GetRootRunId(), current.GetRootRunId(), "RootRunId mismatch")
	s.Equal(target.GetStateMachineTimers(), current.GetStateMachineTimers(), "StateMachineTimers mismatch")
	s.True(proto.Equal(target.GetWorkflowTaskLastUpdateVersionedTransition(), current.GetWorkflowTaskLastUpdateVersionedTransition()), "WorkflowTaskLastUpdateVersionedTransition mismatch")
	s.True(proto.Equal(target.GetVisibilityLastUpdateVersionedTransition(), current.GetVisibilityLastUpdateVersionedTransition()), "VisibilityLastUpdateVersionedTransition mismatch")
	s.True(proto.Equal(target.GetSignalRequestIdsLastUpdateVersionedTransition(), current.GetSignalRequestIdsLastUpdateVersionedTransition()), "SignalRequestIdsLastUpdateVersionedTransition mismatch")
	s.Equal(target.GetSubStateMachineTombstoneBatches(), current.GetSubStateMachineTombstoneBatches(), "SubStateMachineTombstoneBatches mismatch")
}

func (s *mutableStateSuite) verifyMutableState(current, target, origin *MutableStateImpl) {
	s.verifyExecutionInfo(current.executionInfo, target.executionInfo, origin.executionInfo)
	s.True(proto.Equal(target.executionState, current.executionState), "executionState mismatch")

	s.Equal(target.pendingActivityTimerHeartbeats, current.pendingActivityTimerHeartbeats, "pendingActivityTimerHeartbeats mismatch")
	s.verifyActivityInfos(target.pendingActivityInfoIDs, current.pendingActivityInfoIDs)
	s.Equal(target.pendingActivityIDToEventID, current.pendingActivityIDToEventID, "pendingActivityIDToEventID mismatch")
	compareMapOfProto(s, current.pendingActivityInfoIDs, current.updateActivityInfos)
	s.Equal(map[int64]struct{}{89: {}}, current.deleteActivityInfos, "deleteActivityInfos mismatch")

	compareMapOfProto(s, target.pendingTimerInfoIDs, current.pendingTimerInfoIDs)
	s.Equal(target.pendingTimerEventIDToID, current.pendingTimerEventIDToID, "pendingTimerEventIDToID mismatch")
	compareMapOfProto(s, target.pendingTimerInfoIDs, current.updateTimerInfos)
	s.Equal(map[string]struct{}{"to-be-deleted": {}}, current.deleteTimerInfos, "deleteTimerInfos mismatch")

	s.verifyChildExecutionInfos(target.pendingChildExecutionInfoIDs, current.pendingChildExecutionInfoIDs, origin.pendingChildExecutionInfoIDs)
	s.verifyChildExecutionInfos(target.pendingChildExecutionInfoIDs, current.updateChildExecutionInfos, origin.pendingChildExecutionInfoIDs)
	s.Equal(map[int64]struct{}{79: {}}, current.deleteChildExecutionInfos, "deleteChildExecutionInfos mismatch")

	compareMapOfProto(s, target.pendingRequestCancelInfoIDs, current.pendingRequestCancelInfoIDs)
	compareMapOfProto(s, target.pendingRequestCancelInfoIDs, current.updateRequestCancelInfos)
	s.Equal(map[int64]struct{}{69: {}}, current.deleteRequestCancelInfos, "deleteRequestCancelInfos mismatch")

	compareMapOfProto(s, target.pendingSignalInfoIDs, current.pendingSignalInfoIDs)
	compareMapOfProto(s, target.pendingSignalInfoIDs, current.updateSignalInfos)
	s.Equal(map[int64]struct{}{74: {}}, current.deleteSignalInfos, "deleteSignalInfos mismatch")

	s.Equal(target.pendingSignalRequestedIDs, current.pendingSignalRequestedIDs, "pendingSignalRequestedIDs mismatch")
	s.Equal(target.pendingSignalRequestedIDs, current.updateSignalRequestedIDs, "updateSignalRequestedIDs mismatch")
	s.Equal(map[string]struct{}{"to-be-deleted": {}}, current.deleteSignalRequestedIDs, "deleteSignalRequestedIDs mismatch")

	s.Equal(target.currentVersion, current.currentVersion, "currentVersion mismatch")
	s.Equal(target.totalTombstones, current.totalTombstones, "totalTombstones mismatch")
	s.Equal(target.dbRecordVersion, current.dbRecordVersion, "dbRecordVersion mismatch")
	s.True(proto.Equal(target.checksum, current.checksum), "checksum mismatch")
}

func (s *mutableStateSuite) buildSnapshot(state *MutableStateImpl) *persistencespb.WorkflowMutableState {
	snapshot := persistencespb.WorkflowMutableState_builder{
		ActivityInfos: map[int64]*persistencespb.ActivityInfo{
			90: persistencespb.ActivityInfo_builder{
				Version:                       1234,
				ScheduledTime:                 state.pendingActivityInfoIDs[90].GetScheduledTime(),
				StartedTime:                   state.pendingActivityInfoIDs[90].GetStartedTime(),
				ActivityId:                    "activityID_5",
				ScheduleToStartTimeout:        timestamp.DurationPtr(time.Second * 100),
				ScheduleToCloseTimeout:        timestamp.DurationPtr(time.Second * 200),
				StartToCloseTimeout:           timestamp.DurationPtr(time.Second * 300),
				HeartbeatTimeout:              timestamp.DurationPtr(time.Second * 50),
				ScheduledEventId:              90,
				LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{TransitionCount: 1025}.Build(),
			}.Build(),
			91: persistencespb.ActivityInfo_builder{
				ActivityId:                    "activity_id_91",
				LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{TransitionCount: 1025}.Build(),
			}.Build(),
		},
		TimerInfos: map[string]*persistencespb.TimerInfo{
			"25": persistencespb.TimerInfo_builder{
				Version:                       1234,
				StartedEventId:                85,
				ExpiryTime:                    state.pendingTimerInfoIDs["25"].GetExpiryTime(),
				TimerId:                       "25",
				LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{TransitionCount: 1025}.Build(),
			}.Build(),
			"26": persistencespb.TimerInfo_builder{
				TimerId:                       "26",
				LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{TransitionCount: 1025}.Build(),
			}.Build(),
		},
		ChildExecutionInfos: map[int64]*persistencespb.ChildExecutionInfo{
			80: persistencespb.ChildExecutionInfo_builder{
				Version:                       1234,
				InitiatedEventBatchId:         20,
				CreateRequestId:               state.pendingChildExecutionInfoIDs[80].GetCreateRequestId(),
				Namespace:                     "mock namespace name",
				WorkflowTypeName:              "code.uber.internal/test/foobar",
				InitiatedEventId:              80,
				LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{TransitionCount: 1025}.Build(),
			}.Build(),
			81: persistencespb.ChildExecutionInfo_builder{
				InitiatedEventBatchId:         81,
				LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{TransitionCount: 1025}.Build(),
			}.Build(),
		},
		RequestCancelInfos: map[int64]*persistencespb.RequestCancelInfo{
			70: persistencespb.RequestCancelInfo_builder{
				Version:                       1234,
				InitiatedEventBatchId:         20,
				CancelRequestId:               state.pendingRequestCancelInfoIDs[70].GetCancelRequestId(),
				InitiatedEventId:              70,
				LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{TransitionCount: 1025}.Build(),
			}.Build(),
			71: persistencespb.RequestCancelInfo_builder{
				InitiatedEventBatchId:         71,
				LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{TransitionCount: 1025}.Build(),
			}.Build(),
		},
		SignalInfos: map[int64]*persistencespb.SignalInfo{
			75: persistencespb.SignalInfo_builder{
				Version:                       1234,
				InitiatedEventBatchId:         17,
				RequestId:                     state.pendingSignalInfoIDs[75].GetRequestId(),
				InitiatedEventId:              75,
				LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{TransitionCount: 1025}.Build(),
			}.Build(),
			76: persistencespb.SignalInfo_builder{
				InitiatedEventBatchId:         76,
				LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{TransitionCount: 1025}.Build(),
			}.Build(),
		},
		ChasmNodes:         state.chasmTree.Snapshot(nil).Nodes,
		SignalRequestedIds: []string{"signal_request_id_1", "signal_requested_id_2"},
		ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
			NamespaceId:                             "deadbeef-0123-4567-890a-bcdef0123456",
			WorkflowId:                              "wId",
			TaskQueue:                               "testTaskQueue",
			WorkflowTypeName:                        "wType",
			WorkflowRunTimeout:                      timestamp.DurationPtr(time.Second * 200),
			DefaultWorkflowTaskTimeout:              timestamp.DurationPtr(time.Second * 100),
			LastCompletedWorkflowTaskStartedEventId: 99,
			LastUpdateTime:                          state.executionInfo.GetLastUpdateTime(),
			WorkflowTaskVersion:                     1234,
			WorkflowTaskScheduledEventId:            101,
			WorkflowTaskStartedEventId:              102,
			WorkflowTaskTimeout:                     timestamp.DurationPtr(time.Second * 100),
			WorkflowTaskAttempt:                     1,
			WorkflowTaskType:                        enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
			VersionHistories: historyspb.VersionHistories_builder{
				Histories: []*historyspb.VersionHistory{
					historyspb.VersionHistory_builder{
						BranchToken: []byte("token#1"),
						Items: []*historyspb.VersionHistoryItem{
							historyspb.VersionHistoryItem_builder{EventId: 102, Version: 1234}.Build(),
						},
					}.Build(),
				},
			}.Build(),
			FirstExecutionRunId: state.executionInfo.GetFirstExecutionRunId(),
			ExecutionTime:       state.executionInfo.GetExecutionTime(),
			TransitionHistory: []*persistencespb.VersionedTransition{
				persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 1234, TransitionCount: 1024}.Build(),
				persistencespb.VersionedTransition_builder{TransitionCount: 1025}.Build(),
			},
			SignalRequestIdsLastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{TransitionCount: 1025}.Build(),
			WorkflowTaskLastUpdateVersionedTransition:     state.executionInfo.GetWorkflowTaskLastUpdateVersionedTransition(),
		}.Build(),
		ExecutionState: persistencespb.WorkflowExecutionState_builder{
			RunId:     state.executionState.GetRunId(),
			State:     enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			StartTime: state.executionState.GetStartTime(),
		}.Build(),
		NextEventId: 103,
	}.Build()
	return snapshot
}

func (s *mutableStateSuite) TestApplySnapshot() {
	testCases := []struct {
		name                        string
		updateWorkflowTask          bool
		speculativeTask             bool
		expectedWorkflowTaskUpdated bool
	}{
		{
			name:                        "update workflow task",
			updateWorkflowTask:          true,
			speculativeTask:             false,
			expectedWorkflowTaskUpdated: true,
		},
		{
			name:                        "not update workflow task",
			updateWorkflowTask:          false,
			speculativeTask:             false,
			expectedWorkflowTaskUpdated: false,
		},
		{
			name:                        "update speculative workflow task",
			updateWorkflowTask:          true,
			speculativeTask:             true,
			expectedWorkflowTaskUpdated: true,
		},
		{
			name:                        "not update speculative workflow task",
			updateWorkflowTask:          false,
			speculativeTask:             true,
			expectedWorkflowTaskUpdated: false,
		},
	}
	for _, tc := range testCases {
		s.Run(tc.name, func() {
			state := s.buildWorkflowMutableState()
			s.addChangesForStateReplication(state)

			chasmNodesSnapshot := chasm.NodesSnapshot{
				Nodes: state.GetChasmNodes(),
			}

			originMS, err := NewMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, tests.LocalNamespaceEntry, state, 123)
			s.NoError(err)

			currentMS, err := NewMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, tests.LocalNamespaceEntry, state, 123)
			s.NoError(err)
			currentMockChasmTree := historyi.NewMockChasmTree(s.controller)
			currentMockChasmTree.EXPECT().ApplySnapshot(chasmNodesSnapshot).Return(nil).Times(1)
			currentMS.chasmTree = currentMockChasmTree

			state = s.buildWorkflowMutableState()
			state.GetActivityInfos()[91] = persistencespb.ActivityInfo_builder{
				ActivityId: "activity_id_91",
			}.Build()
			state.GetTimerInfos()["26"] = persistencespb.TimerInfo_builder{
				TimerId: "26",
			}.Build()
			state.GetChildExecutionInfos()[81] = persistencespb.ChildExecutionInfo_builder{
				InitiatedEventBatchId: 81,
			}.Build()
			state.GetRequestCancelInfos()[71] = persistencespb.RequestCancelInfo_builder{
				InitiatedEventBatchId: 71,
			}.Build()
			state.GetSignalInfos()[76] = persistencespb.SignalInfo_builder{
				InitiatedEventBatchId: 76,
			}.Build()
			state.SetSignalRequestedIds(append(state.GetSignalRequestedIds(), "signal_requested_id_2"))

			targetMS, err := NewMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, tests.LocalNamespaceEntry, state, 123)
			s.NoError(err)
			targetMockChasmTree := historyi.NewMockChasmTree(s.controller)
			targetMockChasmTree.EXPECT().Snapshot(nil).Return(chasmNodesSnapshot).Times(1)
			targetMS.chasmTree = targetMockChasmTree

			targetMS.GetExecutionInfo().SetTransitionHistory(UpdatedTransitionHistory(targetMS.GetExecutionInfo().GetTransitionHistory(), targetMS.GetCurrentVersion()))

			// set updateXXX so LastUpdateVersionedTransition will be updated
			targetMS.updateActivityInfos = targetMS.pendingActivityInfoIDs
			for key := range targetMS.updateActivityInfos {
				targetMS.activityInfosUserDataUpdated[key] = struct{}{}
			}
			targetMS.updateTimerInfos = targetMS.pendingTimerInfoIDs
			for key := range targetMS.updateTimerInfos {
				targetMS.timerInfosUserDataUpdated[key] = struct{}{}
			}
			targetMS.updateChildExecutionInfos = targetMS.pendingChildExecutionInfoIDs
			targetMS.updateRequestCancelInfos = targetMS.pendingRequestCancelInfoIDs
			targetMS.updateSignalInfos = targetMS.pendingSignalInfoIDs
			targetMS.updateSignalRequestedIDs = targetMS.pendingSignalRequestedIDs

			if tc.updateWorkflowTask {
				// test mutation with workflow task update
				workflowTask := &historyi.WorkflowTaskInfo{
					ScheduledEventID: 1234,
				}
				targetMS.workflowTaskManager.UpdateWorkflowTask(workflowTask)
			}

			if tc.speculativeTask {
				targetMS.executionInfo.SetWorkflowTaskType(enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE)
			}

			targetMS.closeTransactionTrackLastUpdateVersionedTransition(historyi.TransactionPolicyActive)

			snapshot := s.buildSnapshot(targetMS)
			s.Nil(snapshot.GetExecutionInfo().GetSubStateMachinesByType())
			err = currentMS.ApplySnapshot(snapshot)
			s.NoError(err)
			s.NotNil(currentMS.GetExecutionInfo().GetSubStateMachinesByType())

			s.verifyMutableState(currentMS, targetMS, originMS)
			s.Equal(tc.expectedWorkflowTaskUpdated, currentMS.workflowTaskUpdated)
		})
	}
}

func (s *mutableStateSuite) buildMutation(
	state *MutableStateImpl,
	tombstones []*persistencespb.StateMachineTombstoneBatch,
) *persistencespb.WorkflowMutableStateMutation {
	executionInfoClone := common.CloneProto(state.executionInfo)
	executionInfoClone.SetSubStateMachineTombstoneBatches(nil)
	mutation := persistencespb.WorkflowMutableStateMutation_builder{
		UpdatedActivityInfos:            state.pendingActivityInfoIDs,
		UpdatedTimerInfos:               state.pendingTimerInfoIDs,
		UpdatedChildExecutionInfos:      state.pendingChildExecutionInfoIDs,
		UpdatedRequestCancelInfos:       state.pendingRequestCancelInfoIDs,
		UpdatedSignalInfos:              state.pendingSignalInfoIDs,
		UpdatedChasmNodes:               state.chasmTree.Snapshot(nil).Nodes,
		SignalRequestedIds:              state.GetPendingSignalRequestedIds(),
		SubStateMachineTombstoneBatches: tombstones,
		ExecutionInfo:                   executionInfoClone,
		ExecutionState:                  state.executionState,
	}.Build()
	return mutation
}

func (s *mutableStateSuite) TestApplyMutation() {
	testCases := []struct {
		name                        string
		updateWorkflowTask          bool
		speculativeTask             bool
		expectedWorkflowTaskUpdated bool
	}{
		{
			name:                        "update workflow task",
			updateWorkflowTask:          true,
			speculativeTask:             false,
			expectedWorkflowTaskUpdated: true,
		},
		{
			name:                        "not update workflow task",
			updateWorkflowTask:          false,
			speculativeTask:             false,
			expectedWorkflowTaskUpdated: false,
		},
		{
			name:                        "update speculative workflow task",
			updateWorkflowTask:          true,
			speculativeTask:             true,
			expectedWorkflowTaskUpdated: true,
		},
		{
			name:                        "not update speculative workflow task",
			updateWorkflowTask:          false,
			speculativeTask:             true,
			expectedWorkflowTaskUpdated: false,
		},
	}
	for _, tc := range testCases {
		s.Run(tc.name, func() {
			state := s.buildWorkflowMutableState()
			s.addChangesForStateReplication(state)

			originMS, err := NewMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, tests.LocalNamespaceEntry, state, 123)
			s.NoError(err)
			tombstones := []*persistencespb.StateMachineTombstoneBatch{
				persistencespb.StateMachineTombstoneBatch_builder{
					VersionedTransition: persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 1, TransitionCount: 1}.Build(),
					StateMachineTombstones: []*persistencespb.StateMachineTombstone{
						persistencespb.StateMachineTombstone_builder{
							ActivityScheduledEventId: proto.Int64(10),
						}.Build(),
					},
				}.Build(),
			}
			originMS.GetExecutionInfo().SetSubStateMachineTombstoneBatches(tombstones)

			currentMS, err := NewMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, tests.LocalNamespaceEntry, state, 123)
			s.NoError(err)
			currentMockChasmTree := historyi.NewMockChasmTree(s.controller)
			currentMS.chasmTree = currentMockChasmTree

			currentMS.GetExecutionInfo().SetSubStateMachineTombstoneBatches(tombstones)

			state = s.buildWorkflowMutableState()

			targetMS, err := NewMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, tests.LocalNamespaceEntry, state, 123)
			s.NoError(err)

			targetMockChasmTree := historyi.NewMockChasmTree(s.controller)
			updateChasmNodes := map[string]*persistencespb.ChasmNode{
				"node-path": persistencespb.ChasmNode_builder{
					Metadata: persistencespb.ChasmNodeMetadata_builder{
						InitialVersionedTransition:    persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 1, TransitionCount: 1}.Build(),
						LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 1, TransitionCount: 1}.Build(),
						DataAttributes:                &persistencespb.ChasmDataAttributes{},
					}.Build(),
					Data: commonpb.DataBlob_builder{Data: []byte("test-data")}.Build(),
				}.Build(),
				"node-path/collection-node": persistencespb.ChasmNode_builder{
					Metadata: persistencespb.ChasmNodeMetadata_builder{
						InitialVersionedTransition:    persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 1, TransitionCount: 1}.Build(),
						LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 1, TransitionCount: 1}.Build(),
						CollectionAttributes:          &persistencespb.ChasmCollectionAttributes{},
					}.Build(),
				}.Build(),
			}
			targetMockChasmTree.EXPECT().Snapshot(nil).Return(chasm.NodesSnapshot{
				Nodes: updateChasmNodes,
			})
			targetMS.chasmTree = targetMockChasmTree

			transitionHistory := targetMS.executionInfo.GetTransitionHistory()
			failoverVersion := transitionhistory.LastVersionedTransition(transitionHistory).GetNamespaceFailoverVersion()
			targetMS.executionInfo.SetTransitionHistory(UpdatedTransitionHistory(transitionHistory, failoverVersion))

			// set updateXXX so LastUpdateVersionedTransition will be updated
			targetMS.updateActivityInfos = targetMS.pendingActivityInfoIDs
			for key := range targetMS.updateActivityInfos {
				targetMS.activityInfosUserDataUpdated[key] = struct{}{}
			}
			targetMS.updateTimerInfos = targetMS.pendingTimerInfoIDs
			for key := range targetMS.updateTimerInfos {
				targetMS.timerInfosUserDataUpdated[key] = struct{}{}
			}
			targetMS.updateChildExecutionInfos = targetMS.pendingChildExecutionInfoIDs
			targetMS.updateRequestCancelInfos = targetMS.pendingRequestCancelInfoIDs
			targetMS.updateSignalInfos = targetMS.pendingSignalInfoIDs
			targetMS.updateSignalRequestedIDs = targetMS.pendingSignalRequestedIDs

			if tc.updateWorkflowTask {
				// test mutation with workflow task update
				workflowTask := &historyi.WorkflowTaskInfo{
					ScheduledEventID: 1234,
				}
				targetMS.workflowTaskManager.UpdateWorkflowTask(workflowTask)
			}

			if tc.speculativeTask {
				targetMS.executionInfo.SetWorkflowTaskType(enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE)
			}

			targetMS.closeTransactionTrackLastUpdateVersionedTransition(historyi.TransactionPolicyActive)

			tombstonesToAdd := []*persistencespb.StateMachineTombstoneBatch{
				persistencespb.StateMachineTombstoneBatch_builder{
					VersionedTransition: targetMS.CurrentVersionedTransition(),
					StateMachineTombstones: []*persistencespb.StateMachineTombstone{
						persistencespb.StateMachineTombstone_builder{
							ActivityScheduledEventId: proto.Int64(89),
						}.Build(),
						persistencespb.StateMachineTombstone_builder{
							ActivityScheduledEventId: proto.Int64(9999), // not exist
						}.Build(),
						persistencespb.StateMachineTombstone_builder{
							TimerId: proto.String("to-be-deleted"),
						}.Build(),
						persistencespb.StateMachineTombstone_builder{

							TimerId: proto.String("not-exist"),
						}.Build(),
						persistencespb.StateMachineTombstone_builder{
							ChildExecutionInitiatedEventId: proto.Int64(79),
						}.Build(),
						persistencespb.StateMachineTombstone_builder{
							ChildExecutionInitiatedEventId: proto.Int64(9998), // not exist
						}.Build(),
						persistencespb.StateMachineTombstone_builder{
							RequestCancelInitiatedEventId: proto.Int64(69),
						}.Build(),
						persistencespb.StateMachineTombstone_builder{
							RequestCancelInitiatedEventId: proto.Int64(9997), // not exist
						}.Build(),
						persistencespb.StateMachineTombstone_builder{
							SignalExternalInitiatedEventId: proto.Int64(74),
						}.Build(),
						persistencespb.StateMachineTombstone_builder{
							SignalExternalInitiatedEventId: proto.Int64(9996), // not exist
						}.Build(),
						persistencespb.StateMachineTombstone_builder{
							ChasmNodePath: proto.String("deleted-node-path"),
						}.Build(),
					},
				}.Build(),
			}
			targetMS.GetExecutionInfo().SetSubStateMachineTombstoneBatches(append(tombstones, tombstonesToAdd...))
			targetMS.totalTombstones = len(tombstones[0].GetStateMachineTombstones()) + len(tombstonesToAdd[0].GetStateMachineTombstones())
			mutation := s.buildMutation(targetMS, tombstonesToAdd)

			currentMockChasmTree.EXPECT().ApplyMutation(chasm.NodesMutation{
				DeletedNodes: map[string]struct{}{"deleted-node-path": {}},
			}).Return(nil).Times(1)
			currentMockChasmTree.EXPECT().ApplyMutation(chasm.NodesMutation{
				UpdatedNodes: updateChasmNodes,
			}).Return(nil).Times(1)

			err = currentMS.ApplyMutation(mutation)
			s.NoError(err)
			s.verifyMutableState(currentMS, targetMS, originMS)
			s.Equal(tc.expectedWorkflowTaskUpdated, currentMS.workflowTaskUpdated)
		})
	}
}

func (s *mutableStateSuite) TestRefreshTask_DiffCluster() {
	version := int64(99)
	attempt := int32(1)
	incomingActivityInfo := persistencespb.ActivityInfo_builder{
		Version: version,
		Attempt: attempt,
	}.Build()
	localActivityInfo := persistencespb.ActivityInfo_builder{
		Version: int64(100),
		Attempt: incomingActivityInfo.GetAttempt(),
	}.Build()

	s.mockShard.Resource.ClusterMetadata.EXPECT().IsVersionFromSameCluster(localActivityInfo.GetVersion(), version).Return(false)

	shouldReset := s.mutableState.ShouldResetActivityTimerTaskMask(
		localActivityInfo,
		incomingActivityInfo,
	)
	s.True(shouldReset)
}

func (s *mutableStateSuite) TestRefreshTask_SameCluster_DiffAttempt() {
	version := int64(99)
	attempt := int32(1)
	incomingActivityInfo := persistencespb.ActivityInfo_builder{
		Version: version,
		Attempt: attempt,
	}.Build()
	localActivityInfo := persistencespb.ActivityInfo_builder{
		Version: version,
		Attempt: attempt + 1,
	}.Build()

	s.mockShard.Resource.ClusterMetadata.EXPECT().IsVersionFromSameCluster(version, version).Return(true)

	shouldReset := s.mutableState.ShouldResetActivityTimerTaskMask(
		localActivityInfo,
		incomingActivityInfo,
	)
	s.True(shouldReset)
}

func (s *mutableStateSuite) TestRefreshTask_SameCluster_SameAttempt() {
	version := int64(99)
	attempt := int32(1)
	incomingActivityInfo := persistencespb.ActivityInfo_builder{
		Version: version,
		Attempt: attempt,
	}.Build()
	localActivityInfo := persistencespb.ActivityInfo_builder{
		Version: version,
		Attempt: attempt,
	}.Build()

	s.mockShard.Resource.ClusterMetadata.EXPECT().IsVersionFromSameCluster(version, version).Return(true)

	shouldReset := s.mutableState.ShouldResetActivityTimerTaskMask(
		localActivityInfo,
		incomingActivityInfo,
	)
	s.False(shouldReset)
}

func (s *mutableStateSuite) TestUpdateActivityTaskStatusWithTimerHeartbeat() {
	dbState := s.buildWorkflowMutableState()
	scheduleEventId := int64(781)
	dbState.GetActivityInfos()[scheduleEventId] = persistencespb.ActivityInfo_builder{
		Version:                5,
		ScheduledEventId:       int64(90),
		ScheduledTime:          timestamppb.New(time.Now().UTC()),
		StartedEventId:         common.EmptyEventID,
		StartedTime:            timestamppb.New(time.Now().UTC()),
		ActivityId:             "activityID_5",
		TimerTaskStatus:        0,
		ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
		ScheduleToCloseTimeout: timestamp.DurationFromSeconds(200),
		StartToCloseTimeout:    timestamp.DurationFromSeconds(300),
		HeartbeatTimeout:       timestamp.DurationFromSeconds(50),
	}.Build()
	mutableState, err := NewMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, s.namespaceEntry, dbState, 123)
	s.NoError(err)
	originalTime := time.Now().UTC().Add(time.Second * 60)
	mutableState.pendingActivityTimerHeartbeats[scheduleEventId] = originalTime
	status := int32(1)
	err = mutableState.UpdateActivityTaskStatusWithTimerHeartbeat(scheduleEventId, status, nil)
	s.NoError(err)
	s.Equal(status, dbState.GetActivityInfos()[scheduleEventId].GetTimerTaskStatus())
	s.Equal(originalTime, mutableState.pendingActivityTimerHeartbeats[scheduleEventId])
}

func (s *mutableStateSuite) TestHasRequestID() {
	testCases := []struct {
		name          string
		requestID     string
		setupFunc     func(ms *MutableStateImpl) // Setup function to prepare the mutable state
		expectedFound bool
	}{
		{
			name:      "empty_request_id",
			requestID: "",
			setupFunc: func(ms *MutableStateImpl) {
				// No setup needed
			},
			expectedFound: false,
		},
		{
			name:      "request_id_not_found",
			requestID: "non-existent-request-id",
			setupFunc: func(ms *MutableStateImpl) {
				// No setup needed
			},
			expectedFound: false,
		},
		{
			name:      "request_id_found",
			requestID: "existing-request-id",
			setupFunc: func(ms *MutableStateImpl) {
				// Add request ID to execution state
				ms.AttachRequestID("existing-request-id", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, 100)
			},
			expectedFound: true,
		},
		{
			name:      "multiple_request_ids_found_target",
			requestID: "target-request-id",
			setupFunc: func(ms *MutableStateImpl) {
				// Add multiple request IDs
				ms.AttachRequestID("request-id-1", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, 101)
				ms.AttachRequestID("target-request-id", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, 102)
				ms.AttachRequestID("request-id-3", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, 103)
			},
			expectedFound: true,
		},
		{
			name:      "multiple_request_ids_not_found_target",
			requestID: "missing-request-id",
			setupFunc: func(ms *MutableStateImpl) {
				// Add multiple request IDs, but not the target one
				ms.AttachRequestID("request-id-1", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, 104)
				ms.AttachRequestID("request-id-2", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, 105)
				ms.AttachRequestID("request-id-3", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, 106)
			},
			expectedFound: false,
		},
		{
			name:      "request_id_case_sensitive",
			requestID: "Case-Sensitive-ID",
			setupFunc: func(ms *MutableStateImpl) {
				// Add request ID with different case
				ms.AttachRequestID("case-sensitive-id", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, 107)
			},
			expectedFound: false,
		},
		{
			name:      "request_id_exact_match",
			requestID: "exact-match-id",
			setupFunc: func(ms *MutableStateImpl) {
				// Add exact match
				ms.AttachRequestID("exact-match-id", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, 108)
			},
			expectedFound: true,
		},
		{
			name:      "request_id_with_special_characters",
			requestID: "request-id-with-$pecial-ch@racters_123",
			setupFunc: func(ms *MutableStateImpl) {
				ms.AttachRequestID("request-id-with-$pecial-ch@racters_123", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, 109)
			},
			expectedFound: true,
		},
		{
			name:      "uuid_style_request_id",
			requestID: "12345678-1234-1234-1234-123456789abc",
			setupFunc: func(ms *MutableStateImpl) {
				ms.AttachRequestID("12345678-1234-1234-1234-123456789abc", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, 110)
			},
			expectedFound: true,
		},
		{
			name:      "very_long_request_id",
			requestID: "very-long-request-id-" + strings.Repeat("x", 100),
			setupFunc: func(ms *MutableStateImpl) {
				ms.AttachRequestID("very-long-request-id-"+strings.Repeat("x", 100), enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, 111)
			},
			expectedFound: true,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			s.SetupSubTest()

			// Setup the mutable state
			tc.setupFunc(s.mutableState)

			// Test HasRequestID
			found := s.mutableState.HasRequestID(tc.requestID)
			s.Equal(tc.expectedFound, found, "HasRequestID result should match expected")

			// Verify the request ID existence directly in the execution state if expected to be found
			if tc.expectedFound && tc.requestID != "" {
				_, exists := s.mutableState.executionState.GetRequestIds()[tc.requestID]
				s.True(exists, "Request ID should exist in execution state RequestIds map")
			}
		})
	}
}

func (s *mutableStateSuite) TestHasRequestID_StateConsistency() {
	// Test that HasRequestID is consistent with AttachRequestID
	requestID := "consistency-test-request-id"

	// Initially should not exist
	s.False(s.mutableState.HasRequestID(requestID))

	// After attaching, should exist
	s.mutableState.AttachRequestID(requestID, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, 200)
	s.True(s.mutableState.HasRequestID(requestID))

	// Should still exist after multiple calls
	s.True(s.mutableState.HasRequestID(requestID))
	s.True(s.mutableState.HasRequestID(requestID))
}

func (s *mutableStateSuite) TestHasRequestID_EmptyExecutionState() {
	// Ensure execution state has no request IDs initially
	if s.mutableState.executionState.GetRequestIds() == nil {
		s.mutableState.executionState.SetRequestIds(make(map[string]*persistencespb.RequestIDInfo))
	}

	// Clear any existing request IDs
	for k := range s.mutableState.executionState.GetRequestIds() {
		delete(s.mutableState.executionState.GetRequestIds(), k)
	}

	// Test various request IDs on empty state
	testRequestIDs := []string{
		"",
		"test-id",
		"another-id",
	}

	for _, requestID := range testRequestIDs {
		s.False(s.mutableState.HasRequestID(requestID), "Should return false for request ID: %s", requestID)
	}
}

func (s *mutableStateSuite) TestAddTasks_CHASMPureTask() {
	s.mockConfig.ChasmMaxInMemoryPureTasks = dynamicconfig.GetIntPropertyFn(5)
	totalTasks := 2 * s.mockConfig.ChasmMaxInMemoryPureTasks()

	visTimestamp := s.mockShard.GetTimeSource().Now()
	for i := 0; i < totalTasks; i++ {
		task := &tasks.ChasmTaskPure{
			VisibilityTimestamp: visTimestamp,
		}
		s.mutableState.AddTasks(task)
		s.LessOrEqual(len(s.mutableState.chasmPureTasks), s.mockConfig.ChasmMaxInMemoryPureTasks())

		visTimestamp = visTimestamp.Add(-time.Minute)
	}

	s.mockConfig.ChasmMaxInMemoryPureTasks = dynamicconfig.GetIntPropertyFn(2)
	s.mutableState.AddTasks(&tasks.ChasmTaskPure{
		VisibilityTimestamp: visTimestamp,
	})
	s.Len(s.mutableState.chasmPureTasks, 2)
}

func (s *mutableStateSuite) TestDeleteCHASMPureTasks() {
	now := s.mockShard.GetTimeSource().Now()

	testCases := []struct {
		name              string
		maxScheduledTime  time.Time
		expectedRemaining int
	}{
		{
			name:              "none",
			maxScheduledTime:  now,
			expectedRemaining: 3,
		},
		{
			name:              "paritial",
			maxScheduledTime:  now.Add(2 * time.Minute),
			expectedRemaining: 2,
		},
		{
			name:              "all",
			maxScheduledTime:  now.Add(5 * time.Minute),
			expectedRemaining: 0,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			s.mutableState.chasmPureTasks = []*tasks.ChasmTaskPure{
				{
					VisibilityTimestamp: now.Add(3 * time.Minute),
				},
				{
					VisibilityTimestamp: now.Add(2 * time.Minute),
				},
				{
					VisibilityTimestamp: now.Add(time.Minute),
				},
			}
			s.mutableState.BestEffortDeleteTasks = make(map[tasks.Category][]tasks.Key)

			s.mutableState.DeleteCHASMPureTasks(tc.maxScheduledTime)

			s.Len(s.mutableState.chasmPureTasks, tc.expectedRemaining)
			for _, task := range s.mutableState.chasmPureTasks {
				s.False(task.VisibilityTimestamp.Before(tc.maxScheduledTime))
			}

			s.Len(s.mutableState.BestEffortDeleteTasks[tasks.CategoryTimer], 3-tc.expectedRemaining)
		})
	}
}

func (s *mutableStateSuite) TestCHASMNodeSize() {
	dbState := s.buildWorkflowMutableState()
	dbState = persistencespb.WorkflowMutableState_builder{
		ExecutionInfo:  dbState.GetExecutionInfo(),
		ExecutionState: dbState.GetExecutionState(),
		ChasmNodes:     dbState.GetChasmNodes(),
	}.Build()

	mutableState, err := NewMutableStateFromDB(s.mockShard, s.mockEventsCache, s.logger, s.namespaceEntry, dbState, 123)
	s.NoError(err)

	chasmNodesSize := 0
	for key, node := range dbState.GetChasmNodes() {
		chasmNodesSize += len(key) + node.Size()
	}
	expectedTotalSize := chasmNodesSize + dbState.GetExecutionInfo().Size() + dbState.GetExecutionState().Size()
	s.Equal(expectedTotalSize, mutableState.GetApproximatePersistedSize())

	// Switch to a mock CHASM tree
	mockChasmTree := historyi.NewMockChasmTree(s.controller)
	mutableState.chasmTree = mockChasmTree

	var nodeKeyToDelete string
	for nodeKeyToDelete = range dbState.GetChasmNodes() {
		break
	}

	var nodeKeyToUpdate string
	for nodeKeyToUpdate = range dbState.GetChasmNodes() {
		if nodeKeyToUpdate != nodeKeyToDelete {
			break
		}
	}
	var updateNode persistencespb.ChasmNode
	_ = fakedata.FakeStruct(&updateNode)

	newNodeKey := "new-node-path"
	var newNode persistencespb.ChasmNode
	_ = fakedata.FakeStruct(&newNode)

	mockChasmTree.EXPECT().IsDirty().Return(false).Times(1)
	_, err = mutableState.StartTransaction(s.namespaceEntry)
	s.NoError(err)

	mockChasmTree.EXPECT().IsStateDirty().Return(true).AnyTimes()
	mockChasmTree.EXPECT().ArchetypeID().Return(chasm.WorkflowArchetypeID + 101).AnyTimes()
	mockChasmTree.EXPECT().CloseTransaction().Return(chasm.NodesMutation{
		UpdatedNodes: map[string]*persistencespb.ChasmNode{
			nodeKeyToUpdate: &updateNode,
			newNodeKey:      &newNode,
		},
		DeletedNodes: map[string]struct{}{
			nodeKeyToDelete: {},
		},
	}, nil).Times(1)
	_, _, err = mutableState.CloseTransactionAsMutation(context.Background(), historyi.TransactionPolicyActive)
	s.NoError(err)

	expectedTotalSize -= len(nodeKeyToDelete) + dbState.GetChasmNodes()[nodeKeyToDelete].Size()
	expectedTotalSize += updateNode.Size() - dbState.GetChasmNodes()[nodeKeyToUpdate].Size()
	expectedTotalSize += len(newNodeKey) + newNode.Size()
	s.Equal(expectedTotalSize, mutableState.GetApproximatePersistedSize())
}
