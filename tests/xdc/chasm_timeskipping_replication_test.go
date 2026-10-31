package xdc

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type chasmTimeSkippingReplicationSuite struct {
	xdcBaseSuite
}

func TestCHASMTimeSkippingReplicationSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(chasmTimeSkippingReplicationSuite))
}

func (s *chasmTimeSkippingReplicationSuite) SetupSuite() {
	s.enableTransitionHistory = true
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.EnableChasm.Key():                          true,
		dynamicconfig.EnableCHASMSchedulerCreation.Key():         true,
		dynamicconfig.CHASMSchedulerCreationRolloutPercent.Key(): 100,
		dynamicconfig.CHASMSchedulerTimeSkippingEnabled.Key():    true,
	}
	s.setupSuite(testcore.WithClusterHistoryTaskRecorder())
}

func (s *chasmTimeSkippingReplicationSuite) SetupTest() {
	s.setupTest()
}

func (s *chasmTimeSkippingReplicationSuite) TearDownSuite() {
	s.tearDownSuite()
}

// A replicated skip changes the virtual-clock offset without changing the logical
// timer. The standby must write a new physical timer using the replicated offset.
func (s *chasmTimeSkippingReplicationSuite) TestStandbyRestampsCHASMTimerAfterSkip() {
	ctx, cancel := context.WithTimeout(s.T().Context(), 2*time.Minute)
	defer cancel()
	chasmCtx, err := s.clusters[0].Host().ChasmContext(ctx)
	s.Require().NoError(err)

	const interval = 24 * time.Hour
	firstTick := time.Now().UTC().Add(4 * time.Hour).Truncate(time.Second)
	scheduleID := "chasm-skip-replication-" + uuid.NewString()
	namespaceName := s.createGlobalNamespace()
	_, err = s.clusters[0].FrontendClient().CreateSchedule(chasmCtx, &workflowservice.CreateScheduleRequest{
		Namespace:  namespaceName,
		ScheduleId: scheduleID,
		Schedule: &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{
				Interval: durationpb.New(interval),
				Phase:    durationpb.New(time.Duration(firstTick.UnixNano() % interval.Nanoseconds())),
			}}},
			Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   "workflow-" + scheduleID,
					WorkflowType: &commonpb.WorkflowType{Name: "unused"},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: "unused", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			}},
		},
		Identity:  "test",
		RequestId: uuid.NewString(),
	})
	s.Require().NoError(err)

	nsResp, err := s.clusters[0].FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: namespaceName})
	s.Require().NoError(err)
	filter := testcore.TaskFilter{NamespaceID: nsResp.GetNamespaceInfo().GetId(), WorkflowID: scheduleID}
	standbyRecorder := s.clusters[1].GetHistoryTaskRecorder()
	s.Require().NotNil(standbyRecorder)
	var originalFireTime time.Time
	await.Require(ctx, s.T(), func(t *await.T) {
		for _, recorded := range standbyRecorder.GetRecordedTasksByCategoryFiltered(tasks.CategoryTimer, filter) {
			if _, ok := recorded.Task.(*tasks.ChasmTaskPure); ok && recorded.Task.GetVisibilityTime().After(firstTick.Add(-time.Minute)) {
				originalFireTime = recorded.Task.GetVisibilityTime()
				return
			}
		}
		require.Fail(t, "standby did not create the initial CHASM timer")
	}, replicationWaitTime, replicationCheckInterval)
	s.Require().WithinDuration(firstTick, originalFireTime, time.Minute)

	_, err = s.clusters[0].FrontendClient().PatchSchedule(chasmCtx, &workflowservice.PatchScheduleRequest{
		Namespace:  namespaceName,
		ScheduleId: scheduleID,
		Patch: &schedulepb.SchedulePatch{TimeSkippingConfig: &commonpb.TimeSkippingConfig{
			Enabled: true,
			FastForwardConfig: &commonpb.FastForwardConfig{
				Id:       uuid.NewString(),
				Duration: durationpb.New(2 * time.Hour),
			},
		}},
		Identity:  "test",
		RequestId: uuid.NewString(),
	})
	s.Require().NoError(err)

	await.Require(ctx, s.T(), func(t *await.T) {
		desc, err := s.clusters[1].AdminClient().DescribeMutableState(t.Context(), &adminservice.DescribeMutableStateRequest{
			Namespace:   namespaceName,
			Execution:   &commonpb.WorkflowExecution{WorkflowId: scheduleID},
			ArchetypeId: uint32(chasm.SchedulerArchetypeID),
		})
		require.NoError(t, err)
		skip := desc.GetDatabaseMutableState().GetExecutionInfo().GetTimeSkippingInfo().GetAccumulatedSkippedDuration().AsDuration()
		require.Greater(t, skip, 90*time.Minute, "skip offset must replicate to standby")
		expectedFireTime := originalFireTime.Add(-skip)
		for _, recorded := range standbyRecorder.GetRecordedTasksByCategoryFiltered(tasks.CategoryTimer, filter) {
			if _, ok := recorded.Task.(*tasks.ChasmTaskPure); ok &&
				recorded.Task.GetVisibilityTime().Sub(expectedFireTime).Abs() < time.Minute {
				return
			}
		}
		require.Failf(t, "standby did not restamp the CHASM timer", "expected a physical timer near %v after offset %v", expectedFireTime, skip)
	}, replicationWaitTime, replicationCheckInterval)
}
