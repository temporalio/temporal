package tests

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/worker/scheduler"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

/*
more tests to write:

various validation errors

overlap policies, esp. buffer

worker restart/long-poll activity failure:
	get it in a state where it's waiting for a wf to exit, say with bufferone
	restart the worker/force activity to fail
	terminate the wf
	check that new one starts immediately
*/

type (
	scheduleFunctionalSuiteBase struct {
		testcore.FunctionalTestBase

		sdkClient     sdkclient.Client
		worker        worker.Worker
		taskQueue     string
		dataConverter converter.DataConverter
		newContext    func() context.Context
	}

	ScheduleCHASMFunctionalSuite struct {
		scheduleFunctionalSuiteBase
	}

	ScheduleV1FunctionalSuite struct {
		scheduleFunctionalSuiteBase
	}
)

func TestScheduleFunctionalSuite(t *testing.T) {
	t.Parallel()

	// CHASM tests must run as a separate suite, with a separate cluster/functional environment, because the tests
	// assume a fully clean state. For example, TestBasics has assertions on visibility entries for workflow runs
	// started by the scheduler, which would not be cleaned up even when the associated scheduler has been deleted.
	suite.Run(t, new(ScheduleCHASMFunctionalSuite))
	suite.Run(t, new(ScheduleV1FunctionalSuite))
}

func (s *ScheduleCHASMFunctionalSuite) SetupTest() {
	s.newContext = func() context.Context {
		baseCtx := testcore.NewContext()
		return metadata.NewOutgoingContext(baseCtx, metadata.Pairs(
			headers.ExperimentHeaderName, "chasm-scheduler",
		))
	}
	s.scheduleFunctionalSuiteBase.SetupTest()
}

func (s *ScheduleV1FunctionalSuite) SetupTest() {
	s.newContext = func() context.Context {
		return testcore.NewContext()
	}
	s.scheduleFunctionalSuiteBase.SetupTest()
}

func (s *scheduleFunctionalSuiteBase) SetupTest() {
	s.OverrideDynamicConfig(dynamicconfig.EnableChasm, true)
	s.OverrideDynamicConfig(dynamicconfig.FrontendAllowedExperiments, []string{"*"})
	s.FunctionalTestBase.SetupTest()
	s.dataConverter = testcore.NewTestDataConverter()

	var err error
	s.sdkClient, err = sdkclient.Dial(sdkclient.Options{
		HostPort:      s.FrontendGRPCAddress(),
		Namespace:     s.Namespace().String(),
		DataConverter: s.dataConverter,
	})
	s.NoError(err)

	s.taskQueue = testcore.RandomizeStr("tq")
	s.worker = worker.New(s.sdkClient, s.taskQueue, worker.Options{})
	err = s.worker.Start()
	s.NoError(err)
}

func (s *scheduleFunctionalSuiteBase) TearDownTest() {
	if s.worker != nil {
		s.worker.Stop()
	}
	if s.sdkClient != nil {
		s.sdkClient.Close()
	}
	s.FunctionalTestBase.TearDownTest()
}

func (s *scheduleFunctionalSuiteBase) TestBasics() {
	sid := "sched-test-basics"
	wid := "sched-test-basics-wf"
	wt := "sched-test-basics-wt"
	wt2 := "sched-test-basics-wt2"
	// switch this to test with search attribute mapper:
	// csaKeyword := "AliasForCustomKeywordField"
	csaKeyword := "CustomKeywordField"
	csaInt := "CustomIntField"
	csaBool := "CustomBoolField"

	wfMemo := payload.EncodeString("workflow memo")
	wfSAValue := payload.EncodeString("workflow sa value")
	schMemo := payload.EncodeString("schedule memo")
	schSAValue := payload.EncodeString("schedule sa value")
	schSAIntValue, _ := payload.Encode(123)
	schSABoolValue, _ := payload.Encode(true)

	schedule := schedulepb.Schedule_builder{
		Spec: schedulepb.ScheduleSpec_builder{
			Interval: []*schedulepb.IntervalSpec{
				schedulepb.IntervalSpec_builder{Interval: durationpb.New(5 * time.Second)}.Build(),
			},
			Calendar: []*schedulepb.CalendarSpec{
				schedulepb.CalendarSpec_builder{DayOfMonth: "10", Year: "2010"}.Build(),
			},
			CronString: []string{"11 11/11 11 11 1 2011"},
		}.Build(),
		Action: schedulepb.ScheduleAction_builder{
			StartWorkflow: workflowpb.NewWorkflowExecutionInfo_builder{
				WorkflowId:   wid,
				WorkflowType: commonpb.WorkflowType_builder{Name: wt}.Build(),
				TaskQueue:    taskqueuepb.TaskQueue_builder{Name: s.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
				Memo: commonpb.Memo_builder{
					Fields: map[string]*commonpb.Payload{"wfmemo1": wfMemo},
				}.Build(),
				SearchAttributes: commonpb.SearchAttributes_builder{
					IndexedFields: map[string]*commonpb.Payload{csaKeyword: wfSAValue},
				}.Build(),
			}.Build(),
		}.Build(),
	}.Build()
	req := workflowservice.CreateScheduleRequest_builder{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
		Memo: commonpb.Memo_builder{
			Fields: map[string]*commonpb.Payload{"schedmemo1": schMemo},
		}.Build(),
		SearchAttributes: commonpb.SearchAttributes_builder{
			IndexedFields: map[string]*commonpb.Payload{
				csaKeyword: schSAValue,
				csaInt:     schSAIntValue,
				csaBool:    schSABoolValue,
			},
		}.Build(),
	}.Build()

	var runs, runs2 int32
	workflowFn := func(ctx workflow.Context) error {
		workflow.SideEffect(ctx, func(ctx workflow.Context) any {
			atomic.AddInt32(&runs, 1)
			return 0
		})
		return nil
	}
	s.worker.RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})
	workflow2Fn := func(ctx workflow.Context) error {
		workflow.SideEffect(ctx, func(ctx workflow.Context) any {
			atomic.AddInt32(&runs2, 1)
			return 0
		})
		return nil
	}
	s.worker.RegisterWorkflowWithOptions(workflow2Fn, workflow.RegisterOptions{Name: wt2})

	// create

	ctx := s.newContext()
	createTime := time.Now()
	_, err := s.FrontendClient().CreateSchedule(ctx, req)
	s.NoError(err)
	s.cleanup(sid)

	// describe immediately after create and verify FutureActionTimes
	describeRespAfterCreate, err := s.FrontendClient().DescribeSchedule(ctx, workflowservice.DescribeScheduleRequest_builder{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	}.Build())
	s.NoError(err)
	s.NotEmpty(describeRespAfterCreate.GetInfo().GetFutureActionTimes(), "FutureActionTimes should be set immediately after create")
	// FutureActionTimes should be in the future (after createTime) and aligned to 5-second intervals
	for i, fat := range describeRespAfterCreate.GetInfo().GetFutureActionTimes() {
		s.True(fat.AsTime().After(createTime) || fat.AsTime().Equal(createTime),
			"FutureActionTimes[%d] should be >= createTime", i)
		s.Equal(int64(0), fat.AsTime().UnixNano()%int64(5*time.Second),
			"FutureActionTimes[%d] should be aligned to 5-second intervals", i)
	}

	// sleep until we see two runs, plus a bit more to ensure that the second run has completed
	s.Eventually(func() bool { return atomic.LoadInt32(&runs) == 2 }, 15*time.Second, 500*time.Millisecond)
	time.Sleep(2 * time.Second) //nolint:forbidigo

	// wait for visibility to stabilize on completed before calling describe,
	// otherwise their recent actions may flake and differ

	visibilityResponse := s.getScheduleEntryFomVisibility(sid, func(ent *schedulepb.ScheduleListEntry) bool {
		recentActions := ent.GetInfo().GetRecentActions()
		return len(recentActions) >= 2 && recentActions[1].GetStartWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	})

	describeResp, err := s.FrontendClient().DescribeSchedule(s.newContext(), workflowservice.DescribeScheduleRequest_builder{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	}.Build())
	s.NoError(err)

	// validate describe response

	checkSpec := func(spec *schedulepb.ScheduleSpec) {
		protorequire.ProtoSliceEqual(s.T(), schedule.GetSpec().GetInterval(), spec.GetInterval())
		s.Nil(spec.GetCalendar())
		s.Nil(spec.GetCronString())
		s.ProtoElementsMatch([]*schedulepb.StructuredCalendarSpec{
			schedulepb.StructuredCalendarSpec_builder{
				Second:     []*schedulepb.Range{schedulepb.Range_builder{Start: 0, End: 0, Step: 1}.Build()},
				Minute:     []*schedulepb.Range{schedulepb.Range_builder{Start: 11, End: 11, Step: 1}.Build()},
				Hour:       []*schedulepb.Range{schedulepb.Range_builder{Start: 11, End: 23, Step: 11}.Build()},
				DayOfMonth: []*schedulepb.Range{schedulepb.Range_builder{Start: 11, End: 11, Step: 1}.Build()},
				Month:      []*schedulepb.Range{schedulepb.Range_builder{Start: 11, End: 11, Step: 1}.Build()},
				DayOfWeek:  []*schedulepb.Range{schedulepb.Range_builder{Start: 1, End: 1, Step: 1}.Build()},
				Year:       []*schedulepb.Range{schedulepb.Range_builder{Start: 2011, End: 2011, Step: 1}.Build()},
			}.Build(),
			schedulepb.StructuredCalendarSpec_builder{
				Second:     []*schedulepb.Range{schedulepb.Range_builder{Start: 0, End: 0, Step: 1}.Build()},
				Minute:     []*schedulepb.Range{schedulepb.Range_builder{Start: 0, End: 0, Step: 1}.Build()},
				Hour:       []*schedulepb.Range{schedulepb.Range_builder{Start: 0, End: 0, Step: 1}.Build()},
				DayOfMonth: []*schedulepb.Range{schedulepb.Range_builder{Start: 10, End: 10, Step: 1}.Build()},
				Month:      []*schedulepb.Range{schedulepb.Range_builder{Start: 1, End: 12, Step: 1}.Build()},
				DayOfWeek:  []*schedulepb.Range{schedulepb.Range_builder{Start: 0, End: 6, Step: 1}.Build()},
				Year:       []*schedulepb.Range{schedulepb.Range_builder{Start: 2010, End: 2010, Step: 1}.Build()},
			}.Build(),
		}, spec.GetStructuredCalendar())
	}
	checkSpec(describeResp.GetSchedule().GetSpec())

	s.Equal(enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, describeResp.GetSchedule().GetPolicies().GetOverlapPolicy())     // set to default value
	s.EqualValues(365*24*3600, describeResp.GetSchedule().GetPolicies().GetCatchupWindow().AsDuration().Seconds()) // set to default value

	s.Equal(schSAValue.GetData(), describeResp.GetSearchAttributes().GetIndexedFields()[csaKeyword].GetData())
	s.Equal(schSAIntValue.GetData(), describeResp.GetSearchAttributes().GetIndexedFields()[csaInt].GetData())
	s.Equal(schSABoolValue.GetData(), describeResp.GetSearchAttributes().GetIndexedFields()[csaBool].GetData())
	s.Nil(describeResp.GetSearchAttributes().GetIndexedFields()[sadefs.BinaryChecksums])
	s.Nil(describeResp.GetSearchAttributes().GetIndexedFields()[sadefs.BuildIds])
	s.Nil(describeResp.GetSearchAttributes().GetIndexedFields()[sadefs.TemporalNamespaceDivision])
	s.Equal(schMemo.GetData(), describeResp.GetMemo().GetFields()["schedmemo1"].GetData())
	s.Equal(wfSAValue.GetData(), describeResp.GetSchedule().GetAction().GetStartWorkflow().GetSearchAttributes().GetIndexedFields()[csaKeyword].GetData())
	s.Equal(wfMemo.GetData(), describeResp.GetSchedule().GetAction().GetStartWorkflow().GetMemo().GetFields()["wfmemo1"].GetData())

	// GreaterOrEqual is used as we may have had other runs start while waiting for visibility
	s.DurationNear(describeResp.GetInfo().GetCreateTime().AsTime().Sub(createTime), 0, 3*time.Second)
	s.GreaterOrEqual(describeResp.GetInfo().GetActionCount(), int64(2))
	s.EqualValues(0, describeResp.GetInfo().GetMissedCatchupWindow())
	s.EqualValues(0, describeResp.GetInfo().GetOverlapSkipped())
	s.GreaterOrEqual(len(describeResp.GetInfo().GetRunningWorkflows()), 0)
	s.GreaterOrEqual(len(describeResp.GetInfo().GetRecentActions()), 2)
	action0 := describeResp.GetInfo().GetRecentActions()[0]
	s.WithinRange(action0.GetScheduleTime().AsTime(), createTime, time.Now())
	s.True(action0.GetScheduleTime().AsTime().UnixNano()%int64(5*time.Second) == 0)
	s.DurationNear(action0.GetActualTime().AsTime().Sub(action0.GetScheduleTime().AsTime()), 0, 3*time.Second)

	// validate list response

	s.Equal(sid, visibilityResponse.GetScheduleId())
	s.Equal(schSAValue.GetData(), visibilityResponse.GetSearchAttributes().GetIndexedFields()[csaKeyword].GetData())
	s.Equal(schSAIntValue.GetData(), describeResp.GetSearchAttributes().GetIndexedFields()[csaInt].GetData())
	s.Equal(schSABoolValue.GetData(), describeResp.GetSearchAttributes().GetIndexedFields()[csaBool].GetData())
	s.Nil(visibilityResponse.GetSearchAttributes().GetIndexedFields()[sadefs.BinaryChecksums])
	s.Nil(visibilityResponse.GetSearchAttributes().GetIndexedFields()[sadefs.BuildIds])
	s.Nil(visibilityResponse.GetSearchAttributes().GetIndexedFields()[sadefs.TemporalNamespaceDivision])
	s.Equal(schMemo.GetData(), visibilityResponse.GetMemo().GetFields()["schedmemo1"].GetData())
	checkSpec(visibilityResponse.GetInfo().GetSpec())
	s.Equal(wt, visibilityResponse.GetInfo().GetWorkflowType().GetName())
	s.False(visibilityResponse.GetInfo().GetPaused())
	s.assertSameRecentActions(describeResp, visibilityResponse)

	// list workflows

	wfResp, err := s.FrontendClient().ListWorkflowExecutions(s.newContext(), workflowservice.ListWorkflowExecutionsRequest_builder{
		Namespace: s.Namespace().String(),
		PageSize:  5,
		Query:     "",
	}.Build())
	s.NoError(err)
	s.GreaterOrEqual(len(wfResp.GetExecutions()), 2) // could have had a 3rd run while waiting for visibility
	for _, ex := range wfResp.GetExecutions() {
		s.Equal(wt, ex.GetType().GetName(), "should only see started workflows")
	}
	ex0 := wfResp.GetExecutions()[0]
	s.True(strings.HasPrefix(ex0.GetExecution().GetWorkflowId(), wid))
	matchingRunId := false
	for _, recentAction := range describeResp.GetInfo().GetRecentActions() {
		if ex0.GetExecution().GetRunId() == recentAction.GetStartWorkflowResult().GetRunId() {
			matchingRunId = true
			break
		}
	}
	s.True(matchingRunId, "ListWorkflowExecutions returned a run ID wasn't in the describe response")
	s.Equal(wt, ex0.GetType().GetName())
	s.Nil(ex0.GetParentExecution()) // not a child workflow
	s.Equal(wfMemo.GetData(), ex0.GetMemo().GetFields()["wfmemo1"].GetData())
	s.Equal(wfSAValue.GetData(), ex0.GetSearchAttributes().GetIndexedFields()[csaKeyword].GetData())
	s.Equal(payload.EncodeString(sid).GetData(), ex0.GetSearchAttributes().GetIndexedFields()[sadefs.TemporalScheduledById].GetData())
	var ex0StartTime time.Time
	s.NoError(payload.Decode(ex0.GetSearchAttributes().GetIndexedFields()[sadefs.TemporalScheduledStartTime], &ex0StartTime))
	s.WithinRange(ex0StartTime, createTime, time.Now())
	s.True(ex0StartTime.UnixNano()%int64(5*time.Second) == 0)

	// list schedules with search attribute filter

	listResp, err := s.FrontendClient().ListSchedules(s.newContext(), workflowservice.ListSchedulesRequest_builder{
		Namespace:       s.Namespace().String(),
		MaximumPageSize: 5,
		Query:           "CustomKeywordField = 'schedule sa value' AND TemporalSchedulePaused = false",
	}.Build())
	s.NoError(err)
	s.Len(listResp.GetSchedules(), 1)
	entry := listResp.GetSchedules()[0]
	s.Equal(sid, entry.GetScheduleId())

	// list schedules with invalid search attribute filter

	_, err = s.FrontendClient().ListSchedules(s.newContext(), workflowservice.ListSchedulesRequest_builder{
		Namespace:       s.Namespace().String(),
		MaximumPageSize: 5,
		Query:           "ExecutionDuration > '1s'",
	}.Build())
	s.Error(err)

	// update schedule, no updates to search attributes

	schedule.GetSpec().GetInterval()[0].SetPhase(durationpb.New(1 * time.Second))
	schedule.GetAction().GetStartWorkflow().GetWorkflowType().SetName(wt2)

	updateTime := time.Now()
	_, err = s.FrontendClient().UpdateSchedule(s.newContext(), workflowservice.UpdateScheduleRequest_builder{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	}.Build())
	s.NoError(err)

	// wait for one new run
	s.Eventually(
		func() bool { return atomic.LoadInt32(&runs2) == 1 },
		7*time.Second,
		500*time.Millisecond,
	)

	// describe again
	describeResp, err = s.FrontendClient().DescribeSchedule(
		s.newContext(),
		workflowservice.DescribeScheduleRequest_builder{
			Namespace:  s.Namespace().String(),
			ScheduleId: sid,
		}.Build(),
	)
	s.NoError(err)

	s.Len(describeResp.GetSearchAttributes().GetIndexedFields(), 3)
	s.Equal(schSAValue.GetData(), describeResp.GetSearchAttributes().GetIndexedFields()[csaKeyword].GetData())
	s.Equal(schSAIntValue.GetData(), describeResp.GetSearchAttributes().GetIndexedFields()[csaInt].GetData())
	s.Equal(schSABoolValue.GetData(), describeResp.GetSearchAttributes().GetIndexedFields()[csaBool].GetData())
	s.Equal(schMemo.GetData(), describeResp.GetMemo().GetFields()["schedmemo1"].GetData())
	s.Equal(wfSAValue.GetData(), describeResp.GetSchedule().GetAction().GetStartWorkflow().GetSearchAttributes().GetIndexedFields()[csaKeyword].GetData())
	s.Equal(wfMemo.GetData(), describeResp.GetSchedule().GetAction().GetStartWorkflow().GetMemo().GetFields()["wfmemo1"].GetData())

	s.DurationNear(describeResp.GetInfo().GetUpdateTime().AsTime().Sub(updateTime), 0, 3*time.Second)
	lastAction := describeResp.GetInfo().GetRecentActions()[len(describeResp.GetInfo().GetRecentActions())-1]
	s.True(lastAction.GetScheduleTime().AsTime().UnixNano()%int64(5*time.Second) == 1000000000, lastAction.GetScheduleTime().AsTime().UnixNano())

	// update schedule and search attributes

	schedule.GetSpec().GetInterval()[0].SetPhase(durationpb.New(1 * time.Second))
	schedule.GetAction().GetStartWorkflow().GetWorkflowType().SetName(wt2)

	csaDouble := "CustomDoubleField"
	schSADoubleValue, _ := payload.Encode(3.14)
	schSAIntValue, _ = payload.Encode(321)
	_, err = s.FrontendClient().UpdateSchedule(s.newContext(), workflowservice.UpdateScheduleRequest_builder{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
		SearchAttributes: commonpb.SearchAttributes_builder{
			IndexedFields: map[string]*commonpb.Payload{
				csaKeyword: schSAValue,       // same key, same value
				csaInt:     schSAIntValue,    // same key, new value
				csaDouble:  schSADoubleValue, // new key
				// csaBool is removed
			},
		}.Build(),
	}.Build())
	s.NoError(err)

	// wait until search attributes are updated
	s.EventuallyWithT(
		func(c *assert.CollectT) {
			describeResp, err = s.FrontendClient().DescribeSchedule(
				s.newContext(),
				workflowservice.DescribeScheduleRequest_builder{
					Namespace:  s.Namespace().String(),
					ScheduleId: sid,
				}.Build(),
			)
			require.NoError(c, err)
			require.Len(c, describeResp.GetSearchAttributes().GetIndexedFields(), 3)
			require.Equal(c, schSAValue.GetData(), describeResp.GetSearchAttributes().GetIndexedFields()[csaKeyword].GetData())
			require.Equal(c, schSAIntValue.GetData(), describeResp.GetSearchAttributes().GetIndexedFields()[csaInt].GetData())
			require.Equal(c, schSADoubleValue.GetData(), describeResp.GetSearchAttributes().GetIndexedFields()[csaDouble].GetData())
			require.NotContains(c, describeResp.GetSearchAttributes().GetIndexedFields(), csaBool)
		},
		2*time.Second,
		500*time.Millisecond,
	)

	// update schedule and unset search attributes

	schedule.GetSpec().GetInterval()[0].SetPhase(durationpb.New(1 * time.Second))
	schedule.GetAction().GetStartWorkflow().GetWorkflowType().SetName(wt2)

	_, err = s.FrontendClient().UpdateSchedule(s.newContext(), workflowservice.UpdateScheduleRequest_builder{
		Namespace:        s.Namespace().String(),
		ScheduleId:       sid,
		Schedule:         schedule,
		Identity:         "test",
		RequestId:        uuid.NewString(),
		SearchAttributes: &commonpb.SearchAttributes{},
	}.Build())
	s.NoError(err)

	// wait until search attributes are updated
	s.EventuallyWithT(
		func(c *assert.CollectT) {
			describeResp, err = s.FrontendClient().DescribeSchedule(
				s.newContext(),
				workflowservice.DescribeScheduleRequest_builder{
					Namespace:  s.Namespace().String(),
					ScheduleId: sid,
				}.Build(),
			)
			require.NoError(c, err)
			require.Empty(c, describeResp.GetSearchAttributes().GetIndexedFields())
		},
		5*time.Second,
		500*time.Millisecond,
	)

	// pause

	_, err = s.FrontendClient().PatchSchedule(s.newContext(), workflowservice.PatchScheduleRequest_builder{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Patch: schedulepb.SchedulePatch_builder{
			Pause: "because I said so",
		}.Build(),
		Identity:  "test",
		RequestId: uuid.NewString(),
	}.Build())
	s.NoError(err)

	time.Sleep(7 * time.Second) //nolint:forbidigo
	s.EqualValues(1, atomic.LoadInt32(&runs2), "has not run again")

	describeResp, err = s.FrontendClient().DescribeSchedule(s.newContext(), workflowservice.DescribeScheduleRequest_builder{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	}.Build())
	s.NoError(err)

	s.True(describeResp.GetSchedule().GetState().GetPaused())
	s.Equal("because I said so", describeResp.GetSchedule().GetState().GetNotes())

	// don't loop to wait for visibility, we already waited 7s from the patch
	listResp, err = s.FrontendClient().ListSchedules(s.newContext(), workflowservice.ListSchedulesRequest_builder{
		Namespace:       s.Namespace().String(),
		MaximumPageSize: 5,
	}.Build())
	s.NoError(err)
	s.Equal(1, len(listResp.GetSchedules()))
	entry = listResp.GetSchedules()[0]
	s.Equal(sid, entry.GetScheduleId())
	s.True(entry.GetInfo().GetPaused())
	s.Equal("because I said so", entry.GetInfo().GetNotes())

	// finally delete

	_, err = s.FrontendClient().DeleteSchedule(s.newContext(), workflowservice.DeleteScheduleRequest_builder{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Identity:   "test",
	}.Build())
	s.NoError(err)

	describeResp, err = s.FrontendClient().DescribeSchedule(s.newContext(), workflowservice.DescribeScheduleRequest_builder{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	}.Build())
	s.Error(err)

	s.Eventually(func() bool { // wait for visibility
		listResp, err := s.FrontendClient().ListSchedules(s.newContext(), workflowservice.ListSchedulesRequest_builder{
			Namespace:       s.Namespace().String(),
			MaximumPageSize: 5,
		}.Build())
		s.NoError(err)
		return len(listResp.GetSchedules()) == 0
	}, 10*time.Second, 1*time.Second)
}

func (s *scheduleFunctionalSuiteBase) TestInput() {
	sid := "sched-test-input"
	wid := "sched-test-input-wf"
	wt := "sched-test-input-wt"

	type myData struct {
		Stuff  string
		Things []int
	}

	input1 := &myData{
		Stuff:  "here's some data",
		Things: []int{7, 8, 9},
	}
	input2 := map[int]float64{11: 1.4375}
	inputPayloads, err := s.dataConverter.ToPayloads(input1, input2)
	s.NoError(err)

	schedule := schedulepb.Schedule_builder{
		Spec: schedulepb.ScheduleSpec_builder{
			Interval: []*schedulepb.IntervalSpec{
				schedulepb.IntervalSpec_builder{Interval: durationpb.New(3 * time.Second)}.Build(),
			},
		}.Build(),
		Action: schedulepb.ScheduleAction_builder{
			StartWorkflow: workflowpb.NewWorkflowExecutionInfo_builder{
				WorkflowId:   wid,
				WorkflowType: commonpb.WorkflowType_builder{Name: wt}.Build(),
				TaskQueue:    taskqueuepb.TaskQueue_builder{Name: s.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
				Input:        inputPayloads,
			}.Build(),
		}.Build(),
	}.Build()
	req := workflowservice.CreateScheduleRequest_builder{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	}.Build()

	var runs int32
	workflowFn := func(ctx workflow.Context, arg1 *myData, arg2 map[int]float64) error {
		workflow.SideEffect(ctx, func(ctx workflow.Context) any {
			s.Equal(*input1, *arg1)
			s.Equal(input2, arg2)
			atomic.AddInt32(&runs, 1)
			return 0
		})
		return nil
	}
	s.worker.RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})

	ctx := s.newContext()
	_, err = s.FrontendClient().CreateSchedule(ctx, req)
	s.NoError(err)
	s.cleanup(sid)

	s.Eventually(func() bool { return atomic.LoadInt32(&runs) == 1 }, 8*time.Second, 200*time.Millisecond)
}

func (s *scheduleFunctionalSuiteBase) TestLastCompletionAndError() {
	sid := "sched-test-last"
	wid := "sched-test-last-wf"
	wt := "sched-test-last-wt"

	schedule := schedulepb.Schedule_builder{
		Spec: schedulepb.ScheduleSpec_builder{
			Interval: []*schedulepb.IntervalSpec{
				schedulepb.IntervalSpec_builder{Interval: durationpb.New(3 * time.Second)}.Build(),
			},
		}.Build(),
		Action: schedulepb.ScheduleAction_builder{
			StartWorkflow: workflowpb.NewWorkflowExecutionInfo_builder{
				WorkflowId:   wid,
				WorkflowType: commonpb.WorkflowType_builder{Name: wt}.Build(),
				TaskQueue:    taskqueuepb.TaskQueue_builder{Name: s.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
			}.Build(),
		}.Build(),
	}.Build()
	req := workflowservice.CreateScheduleRequest_builder{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	}.Build()

	runs := make(map[string]struct{})
	var testComplete int32

	workflowFn := func(ctx workflow.Context) (string, error) {
		var num int
		_ = workflow.SideEffect(ctx, func(ctx workflow.Context) any {
			runs[workflow.GetInfo(ctx).WorkflowExecution.ID] = struct{}{}
			return len(runs)
		}).Get(&num)

		var lcr string
		if workflow.HasLastCompletionResult(ctx) {
			s.NoError(workflow.GetLastCompletionResult(ctx, &lcr))
		}

		lastErr := workflow.GetLastError(ctx)

		switch num {
		case 1:
			s.Equal("", lcr)
			s.NoError(lastErr)
			return "this one succeeds", nil
		case 2:
			s.NoError(lastErr)
			s.Equal("this one succeeds", lcr)
			return "", errors.New("this one fails")
		case 3:
			s.Equal("this one succeeds", lcr)
			s.ErrorContains(lastErr, "this one fails")
			atomic.StoreInt32(&testComplete, 1)
			return "done", nil
		default:
			panic("shouldn't be running anymore")
		}
	}
	s.worker.RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})

	ctx := s.newContext()
	_, err := s.FrontendClient().CreateSchedule(ctx, req)
	s.NoError(err)
	s.cleanup(sid)

	s.Eventually(func() bool { return atomic.LoadInt32(&testComplete) == 1 }, 20*time.Second, 200*time.Millisecond)
}

// Tests that a schedule created in the V1 stack will also be visible in the V2 stack.
func (s *ScheduleV1FunctionalSuite) TestCHASMCanListV1Schedules() {
	sid := "schedule-created-on-v1"
	schedule := schedulepb.Schedule_builder{
		Spec: schedulepb.ScheduleSpec_builder{
			Interval: []*schedulepb.IntervalSpec{
				schedulepb.IntervalSpec_builder{Interval: durationpb.New(3 * time.Second)}.Build(),
			},
		}.Build(),
		Action: schedulepb.ScheduleAction_builder{
			StartWorkflow: workflowpb.NewWorkflowExecutionInfo_builder{
				WorkflowId:   "wf-",
				WorkflowType: commonpb.WorkflowType_builder{Name: "action"}.Build(),
				TaskQueue:    taskqueuepb.TaskQueue_builder{Name: s.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
			}.Build(),
		}.Build(),
	}.Build()
	req := workflowservice.CreateScheduleRequest_builder{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	}.Build()

	// Create on V1 stack.
	_, err := s.FrontendClient().CreateSchedule(s.newContext(), req)
	s.NoError(err)

	// Pause so that `FutureActionTimes` doesn't change between calls.
	_, err = s.FrontendClient().PatchSchedule(s.newContext(), workflowservice.PatchScheduleRequest_builder{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Patch: schedulepb.SchedulePatch_builder{
			Pause: "halt",
		}.Build(),
		Identity:  "test",
		RequestId: uuid.NewString(),
	}.Build())
	s.NoError(err)

	// Sanity test, list with V1 handler.
	v1Entry := s.getScheduleEntryFomVisibility(sid, func(sle *schedulepb.ScheduleListEntry) bool {
		return sle.GetInfo().GetPaused()
	})
	s.NotNil(v1Entry.GetInfo())

	// Flip on CHASM experiment and make sure we can still list.
	s.newContext = func() context.Context {
		return metadata.NewOutgoingContext(testcore.NewContext(), metadata.Pairs(
			headers.ExperimentHeaderName, "chasm-scheduler",
		))
	}
	chasmEntry := s.getScheduleEntryFomVisibility(sid, nil)
	s.NotNil(chasmEntry.GetInfo())
	s.ProtoEqual(chasmEntry.GetInfo(), v1Entry.GetInfo())
}

// TestRefresh applies to V1 scheduler only; V2 does not support/need manual refresh.
func (s *ScheduleV1FunctionalSuite) TestRefresh() {
	sid := "sched-test-refresh"
	wid := "sched-test-refresh-wf"
	wt := "sched-test-refresh-wt"

	schedule := schedulepb.Schedule_builder{
		Spec: schedulepb.ScheduleSpec_builder{
			Interval: []*schedulepb.IntervalSpec{
				schedulepb.IntervalSpec_builder{
					Interval: durationpb.New(30 * time.Second),
					// start within three seconds
					Phase: durationpb.New(time.Duration((time.Now().Unix()+3)%30) * time.Second),
				}.Build(),
			},
		}.Build(),
		Action: schedulepb.ScheduleAction_builder{
			StartWorkflow: workflowpb.NewWorkflowExecutionInfo_builder{
				WorkflowId:               wid,
				WorkflowType:             commonpb.WorkflowType_builder{Name: wt}.Build(),
				TaskQueue:                taskqueuepb.TaskQueue_builder{Name: s.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
				WorkflowExecutionTimeout: durationpb.New(3 * time.Second),
			}.Build(),
		}.Build(),
	}.Build()
	req := workflowservice.CreateScheduleRequest_builder{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	}.Build()

	var runs int32
	workflowFn := func(ctx workflow.Context) error {
		workflow.SideEffect(ctx, func(ctx workflow.Context) any {
			atomic.AddInt32(&runs, 1)
			return 0
		})
		s.NoError(workflow.Sleep(ctx, 10*time.Second)) // longer than execution timeout
		return nil
	}
	s.worker.RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})

	_, err := s.FrontendClient().CreateSchedule(s.newContext(), req)
	s.NoError(err)
	s.cleanup(sid)

	s.Eventually(func() bool { return atomic.LoadInt32(&runs) == 1 }, 6*time.Second, 200*time.Millisecond)

	// workflow has started but is now sleeping. it will timeout in 2 seconds.

	describeResp, err := s.FrontendClient().DescribeSchedule(s.newContext(), workflowservice.DescribeScheduleRequest_builder{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	}.Build())
	s.NoError(err)
	s.EqualValues(1, len(describeResp.GetInfo().GetRunningWorkflows()))

	events1 := s.GetHistory(s.Namespace().String(), commonpb.WorkflowExecution_builder{WorkflowId: scheduler.WorkflowIDPrefix + sid}.Build())
	expectedHistory := `
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 MarkerRecorded
  6 MarkerRecorded
  7 UpsertWorkflowSearchAttributes
  8 TimerStarted
  9 TimerFired
 10 WorkflowTaskScheduled
 11 WorkflowTaskStarted
 12 WorkflowTaskCompleted
 13 MarkerRecorded
 14 MarkerRecorded
 15 WorkflowPropertiesModified
 16 TimerStarted`

	s.EqualHistoryEvents(expectedHistory, events1)

	time.Sleep(4 * time.Second) //nolint:forbidigo
	// now it has timed out, but the scheduler hasn't noticed yet. we can prove it by checking
	// its history.

	events2 := s.GetHistory(s.Namespace().String(), commonpb.WorkflowExecution_builder{WorkflowId: scheduler.WorkflowIDPrefix + sid}.Build())
	s.EqualHistoryEvents(expectedHistory, events2)

	// when we describe we'll force a refresh and see it timed out
	describeResp, err = s.FrontendClient().DescribeSchedule(s.newContext(), workflowservice.DescribeScheduleRequest_builder{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	}.Build())
	s.NoError(err)
	s.EqualValues(0, len(describeResp.GetInfo().GetRunningWorkflows()))

	// check scheduler has gotten the refresh and done some stuff. signal is sent without waiting so we need to wait.
	s.Eventually(func() bool {
		events3 := s.GetHistory(s.Namespace().String(), commonpb.WorkflowExecution_builder{WorkflowId: scheduler.WorkflowIDPrefix + sid}.Build())
		return len(events3) > len(events2)
	}, 5*time.Second, 100*time.Millisecond)
}

// TestListBeforeRun only applies to V1, as V2 scheduler does not involve the
// per-NS worker or workflow.
func (s *ScheduleV1FunctionalSuite) TestListBeforeRun() {
	sid := "sched-test-list-before-run"
	wid := "sched-test-list-before-run-wf"
	wt := "sched-test-list-before-run-wt"

	// disable per-ns worker so that the schedule workflow never runs
	s.OverrideDynamicConfig(dynamicconfig.WorkerPerNamespaceWorkerCount, 0)

	schedule := schedulepb.Schedule_builder{
		Spec: schedulepb.ScheduleSpec_builder{
			Interval: []*schedulepb.IntervalSpec{
				schedulepb.IntervalSpec_builder{Interval: durationpb.New(3 * time.Second)}.Build(),
			},
		}.Build(),
		Action: schedulepb.ScheduleAction_builder{
			StartWorkflow: workflowpb.NewWorkflowExecutionInfo_builder{
				WorkflowId:   wid,
				WorkflowType: commonpb.WorkflowType_builder{Name: wt}.Build(),
				TaskQueue:    taskqueuepb.TaskQueue_builder{Name: s.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
			}.Build(),
		}.Build(),
	}.Build()
	req := workflowservice.CreateScheduleRequest_builder{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	}.Build()

	startTime := time.Now()

	_, err := s.FrontendClient().CreateSchedule(s.newContext(), req)
	s.NoError(err)
	s.cleanup(sid)

	entry := s.getScheduleEntryFomVisibility(sid, nil)
	s.NotNil(entry.GetInfo())
	s.ProtoEqual(schedule.GetSpec(), entry.GetInfo().GetSpec())
	s.Equal(wt, entry.GetInfo().GetWorkflowType().GetName())
	s.False(entry.GetInfo().GetPaused())
	s.Greater(len(entry.GetInfo().GetFutureActionTimes()), 1)
	s.True(entry.GetInfo().GetFutureActionTimes()[0].AsTime().After(startTime))
}

// TestRateLimit applies only to V1, as V2 scheduler does not impose its own
// rate limiting.
func (s *ScheduleV1FunctionalSuite) TestRateLimit() {
	sid := "sched-test-rate-limit-%d"
	wid := "sched-test-rate-limit-wf-%d"
	wt := "sched-test-rate-limit-wt"

	// Set 1/sec rate limit per namespace.
	s.OverrideDynamicConfig(dynamicconfig.SchedulerNamespaceStartWorkflowRPS, 1.0)

	var runs int32
	workflowFn := func(ctx workflow.Context) error {
		workflow.SideEffect(ctx, func(ctx workflow.Context) any {
			atomic.AddInt32(&runs, 1)
			return 0
		})
		return nil
	}
	s.worker.RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})

	// create 10 copies of the schedule
	for i := 0; i < 10; i++ {
		schedule := schedulepb.Schedule_builder{
			Spec: schedulepb.ScheduleSpec_builder{
				Interval: []*schedulepb.IntervalSpec{
					schedulepb.IntervalSpec_builder{Interval: durationpb.New(1 * time.Second)}.Build(),
				},
			}.Build(),
			Action: schedulepb.ScheduleAction_builder{
				StartWorkflow: workflowpb.NewWorkflowExecutionInfo_builder{
					WorkflowId:   fmt.Sprintf(wid, i),
					WorkflowType: commonpb.WorkflowType_builder{Name: wt}.Build(),
					TaskQueue:    taskqueuepb.TaskQueue_builder{Name: s.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
				}.Build(),
			}.Build(),
		}.Build()
		_, err := s.FrontendClient().CreateSchedule(s.newContext(), workflowservice.CreateScheduleRequest_builder{
			Namespace:  s.Namespace().String(),
			ScheduleId: fmt.Sprintf(sid, i),
			Schedule:   schedule,
			Identity:   "test",
			RequestId:  uuid.NewString(),
		}.Build())
		s.NoError(err)
		s.cleanup(fmt.Sprintf(sid, i))
	}

	time.Sleep(5 * time.Second) //nolint:forbidigo

	// With no rate limit, we'd see 10/second == 50 workflows run. With a limit of 1/sec, we
	// expect to see around 5.
	s.Less(atomic.LoadInt32(&runs), int32(10))
}

func (s *scheduleFunctionalSuiteBase) TestListSchedulesReturnsWorkflowStatus() {
	sid := "sched-test-list-running"
	wid := "sched-test-list-running-wf"
	wt := "sched-test-list-running-wt"

	// Set up a schedule that immediately starts a single running workflow
	schedule := schedulepb.Schedule_builder{
		Spec: schedulepb.ScheduleSpec_builder{
			Interval: []*schedulepb.IntervalSpec{
				schedulepb.IntervalSpec_builder{Interval: durationpb.New(3 * time.Second)}.Build(),
			},
		}.Build(),
		Action: schedulepb.ScheduleAction_builder{
			StartWorkflow: workflowpb.NewWorkflowExecutionInfo_builder{
				WorkflowId:   wid,
				WorkflowType: commonpb.WorkflowType_builder{Name: wt}.Build(),
				TaskQueue:    taskqueuepb.TaskQueue_builder{Name: s.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
			}.Build(),
		}.Build(),
	}.Build()
	patch := schedulepb.SchedulePatch_builder{
		TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{},
	}.Build()

	// The workflow sits open until we've asserted it can be listed as running
	resumeSignal := "resume"
	workflowFn := func(ctx workflow.Context) error {
		selector := workflow.NewSelector(ctx)
		selector.AddReceive(workflow.GetSignalChannel(ctx, resumeSignal), func(c workflow.ReceiveChannel, more bool) {
			// nothing to do
		})
		selector.Select(ctx)
		return nil
	}
	s.worker.RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})

	req := workflowservice.CreateScheduleRequest_builder{
		Namespace:    s.Namespace().String(),
		ScheduleId:   sid,
		Schedule:     schedule,
		InitialPatch: patch,
		RequestId:    uuid.NewString(),
	}.Build()
	ctx := s.newContext()
	_, err := s.FrontendClient().CreateSchedule(ctx, req)
	s.NoError(err)
	s.cleanup(sid)

	// validate RecentActions made it to visibility
	listResp := s.getScheduleEntryFomVisibility(sid, func(listResp *schedulepb.ScheduleListEntry) bool {
		return len(listResp.GetInfo().GetRecentActions()) >= 1
	})
	s.Equal(1, len(listResp.GetInfo().GetRecentActions()))

	a1 := listResp.GetInfo().GetRecentActions()[0]
	s.True(strings.HasPrefix(a1.GetStartWorkflowResult().GetWorkflowId(), wid))
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, a1.GetStartWorkflowStatus())

	// let the started workflow complete
	_, err = s.FrontendClient().SignalWorkflowExecution(s.newContext(), workflowservice.SignalWorkflowExecutionRequest_builder{
		Namespace: s.Namespace().String(),
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: a1.GetStartWorkflowResult().GetWorkflowId(),
			RunId:      a1.GetStartWorkflowResult().GetRunId(),
		}.Build(),
		SignalName: resumeSignal,
	}.Build())
	s.NoError(err)

	// now wait for second recent action to land in visbility
	listResp = s.getScheduleEntryFomVisibility(sid, func(listResp *schedulepb.ScheduleListEntry) bool {
		return len(listResp.GetInfo().GetRecentActions()) >= 2
	})

	a1 = listResp.GetInfo().GetRecentActions()[0]
	a2 := listResp.GetInfo().GetRecentActions()[1]
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, a1.GetStartWorkflowStatus())
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, a2.GetStartWorkflowStatus())

	// Also verify that DescribeSchedule's output matches
	descResp, err := s.FrontendClient().DescribeSchedule(s.newContext(), workflowservice.DescribeScheduleRequest_builder{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	}.Build())
	s.NoError(err)
	s.assertSameRecentActions(descResp, listResp)
}

func (s *scheduleFunctionalSuiteBase) TestUpdateIntervalTakesEffect() {
	sid := "sched-test-update-interval"
	wid := "sched-test-update-interval-wf"
	wt := "sched-test-update-interval-wt"

	var runs int32
	workflowFn := func(ctx workflow.Context) error {
		workflow.SideEffect(ctx, func(ctx workflow.Context) any {
			atomic.AddInt32(&runs, 1)
			return 0
		})
		return nil
	}
	s.worker.RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})

	// Create schedule with a long interval (300s) - won't fire for 5 minutes.
	schedule := schedulepb.Schedule_builder{
		Spec: schedulepb.ScheduleSpec_builder{
			Interval: []*schedulepb.IntervalSpec{
				schedulepb.IntervalSpec_builder{Interval: durationpb.New(300 * time.Second)}.Build(),
			},
		}.Build(),
		Action: schedulepb.ScheduleAction_builder{
			StartWorkflow: workflowpb.NewWorkflowExecutionInfo_builder{
				WorkflowId:   wid,
				WorkflowType: commonpb.WorkflowType_builder{Name: wt}.Build(),
				TaskQueue:    taskqueuepb.TaskQueue_builder{Name: s.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
			}.Build(),
		}.Build(),
	}.Build()

	ctx := s.newContext()
	_, err := s.FrontendClient().CreateSchedule(ctx, workflowservice.CreateScheduleRequest_builder{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	}.Build())
	s.NoError(err)
	s.cleanup(sid)

	// Update the interval to be very short (1s).
	schedule.GetSpec().GetInterval()[0].SetInterval(durationpb.New(1 * time.Second))
	_, err = s.FrontendClient().UpdateSchedule(ctx, workflowservice.UpdateScheduleRequest_builder{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	}.Build())
	s.NoError(err)

	// After updating to 1s interval, we should see runs start within a few seconds.
	s.Eventually(
		func() bool { return atomic.LoadInt32(&runs) >= 2 },
		10*time.Second,
		500*time.Millisecond,
		"expected at least 2 runs within 10s after updating interval to 1s",
	)
}

func (s *scheduleFunctionalSuiteBase) TestListScheduleMatchingTimes() {
	sid := "sched-test-list-matching-times"

	schedule := schedulepb.Schedule_builder{
		Spec: schedulepb.ScheduleSpec_builder{
			Interval: []*schedulepb.IntervalSpec{
				schedulepb.IntervalSpec_builder{Interval: durationpb.New(1 * time.Hour)}.Build(),
			},
		}.Build(),
		Action: schedulepb.ScheduleAction_builder{
			StartWorkflow: workflowpb.NewWorkflowExecutionInfo_builder{
				WorkflowId:   "wf-list-matching-times",
				WorkflowType: commonpb.WorkflowType_builder{Name: "action"}.Build(),
				TaskQueue:    taskqueuepb.TaskQueue_builder{Name: s.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
			}.Build(),
		}.Build(),
	}.Build()
	req := workflowservice.CreateScheduleRequest_builder{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	}.Build()

	ctx := s.newContext()
	_, err := s.FrontendClient().CreateSchedule(ctx, req)
	s.NoError(err)
	s.cleanup(sid)

	// Query for matching times over a 5-hour window.
	now := time.Now().UTC().Truncate(time.Hour).Add(time.Hour) // Start of next hour
	startTime := timestamppb.New(now)
	endTime := timestamppb.New(now.Add(5 * time.Hour))

	resp, err := s.FrontendClient().ListScheduleMatchingTimes(ctx, workflowservice.ListScheduleMatchingTimesRequest_builder{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		StartTime:  startTime,
		EndTime:    endTime,
	}.Build())
	s.NoError(err)
	// With 1-hour interval over 5 hours, we expect 5 matching times.
	s.Len(resp.GetStartTime(), 5)
}

// A schedule's memo should have an upper bound on the number of spec items stored.
func (s *scheduleFunctionalSuiteBase) TestLimitMemoSpecSize() {
	expectedLimit := scheduler.CurrentTweakablePolicies.SpecFieldLengthLimit

	sid := "sched-test-limit-memo-size"
	wid := "sched-test-limit-memo-size-wf"
	wt := "sched-test-limit-memo-size-wt"

	schedule := schedulepb.Schedule_builder{
		Spec: &schedulepb.ScheduleSpec{},
		Action: schedulepb.ScheduleAction_builder{
			StartWorkflow: workflowpb.NewWorkflowExecutionInfo_builder{
				WorkflowId:   wid,
				WorkflowType: commonpb.WorkflowType_builder{Name: wt}.Build(),
				TaskQueue:    taskqueuepb.TaskQueue_builder{Name: s.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
			}.Build(),
		}.Build(),
	}.Build()

	// Set up a schedule with a large number of spec items that should be trimmed in
	// the memo block.
	for i := 0; i < expectedLimit*2; i++ {
		schedule.GetSpec().SetInterval(append(schedule.GetSpec().GetInterval(), schedulepb.IntervalSpec_builder{
			Interval: durationpb.New(time.Duration(i+1) * time.Second),
		}.Build()))
		schedule.GetSpec().SetStructuredCalendar(append(schedule.GetSpec().GetStructuredCalendar(), schedulepb.StructuredCalendarSpec_builder{
			Minute: []*schedulepb.Range{
				schedulepb.Range_builder{
					Start: int32(i + 1),
					End:   int32(i + 1),
				}.Build(),
			},
		}.Build()))
		schedule.GetSpec().SetExcludeStructuredCalendar(append(schedule.GetSpec().GetExcludeStructuredCalendar(), schedulepb.StructuredCalendarSpec_builder{
			Second: []*schedulepb.Range{
				schedulepb.Range_builder{
					Start: int32(i + 1),
					End:   int32(i + 1),
				}.Build(),
			},
		}.Build()))
	}

	// Create the schedule.
	req := workflowservice.CreateScheduleRequest_builder{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	}.Build()
	s.worker.RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error { return nil },
		workflow.RegisterOptions{Name: wt},
	)
	ctx := s.newContext()
	_, err := s.FrontendClient().CreateSchedule(ctx, req)
	s.NoError(err)
	s.cleanup(sid)

	// Verify the memo field length limit was enforced.
	entry := s.getScheduleEntryFomVisibility(sid, nil)
	s.Require().NotNil(entry)
	spec := entry.GetInfo().GetSpec()
	s.Require().Equal(expectedLimit, len(spec.GetInterval()))
	s.Require().Equal(expectedLimit, len(spec.GetStructuredCalendar()))
	s.Require().Equal(expectedLimit, len(spec.GetExcludeStructuredCalendar()))
}

// TestNextTimeCache only applies to V1.
func (s *ScheduleV1FunctionalSuite) TestNextTimeCache() {
	sid := "sched-test-next-time-cache"
	wid := "sched-test-next-time-cache-wf"
	wt := "sched-test-next-time-cache-wt"

	schedule := schedulepb.Schedule_builder{
		Spec: schedulepb.ScheduleSpec_builder{
			Interval: []*schedulepb.IntervalSpec{
				schedulepb.IntervalSpec_builder{Interval: durationpb.New(1 * time.Second)}.Build(),
			},
		}.Build(),
		Action: schedulepb.ScheduleAction_builder{
			StartWorkflow: workflowpb.NewWorkflowExecutionInfo_builder{
				WorkflowId:   wid,
				WorkflowType: commonpb.WorkflowType_builder{Name: wt}.Build(),
				TaskQueue:    taskqueuepb.TaskQueue_builder{Name: s.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
			}.Build(),
		}.Build(),
	}.Build()
	req := workflowservice.CreateScheduleRequest_builder{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	}.Build()

	var runs atomic.Int32
	workflowFn := func(ctx workflow.Context) error {
		workflow.SideEffect(ctx, func(ctx workflow.Context) any {
			runs.Add(1)
			return 0
		})
		return nil
	}
	s.worker.RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})

	_, err := s.FrontendClient().CreateSchedule(s.newContext(), req)
	s.NoError(err)
	s.cleanup(sid)

	// wait for at least 13 runs
	const count = 13
	s.Eventually(func() bool { return runs.Load() >= count }, (count+10)*time.Second, 500*time.Millisecond)

	// there should be only four side effects for 13 runs, and only two mentioning "Next"
	// (cache refills)
	events := s.GetHistory(s.Namespace().String(), commonpb.WorkflowExecution_builder{WorkflowId: scheduler.WorkflowIDPrefix + sid}.Build())
	var sideEffects, nextTimeSideEffects int
	for _, e := range events {
		if marker := e.GetMarkerRecordedEventAttributes(); marker.GetMarkerName() == "SideEffect" {
			sideEffects++
			if p, ok := marker.GetDetails()["data"]; ok && len(p.GetPayloads()) == 1 {
				if string(p.GetPayloads()[0].GetMetadata()["messageType"]) == "temporal.server.api.schedule.v1.NextTimeCache" ||
					strings.Contains(payloads.ToString(p), `"Next"`) {
					nextTimeSideEffects++
				}
			}
		}
	}

	const (
		// These match the ones in the scheduler workflow, but they're not exported.
		// Change these if those change.
		FutureActionCountForList = 5
		NextTimeCacheV2Size      = 14

		// Calculate expected results
		expectedCacheSize = NextTimeCacheV2Size - FutureActionCountForList + 1
		expectedRefills   = (count + expectedCacheSize - 1) / expectedCacheSize
		uuidCacheRefills  = (count + 9) / 10
	)
	s.Equal(expectedRefills+uuidCacheRefills, sideEffects)
	s.Equal(expectedRefills, nextTimeSideEffects)
}

// getScheduleEntryFomVisibility polls visibility using ListSchedules until it finds a schedule
// with the given id and for which the optional predicate function returns true.
func (s *scheduleFunctionalSuiteBase) getScheduleEntryFomVisibility(sid string, predicate func(*schedulepb.ScheduleListEntry) bool) *schedulepb.ScheduleListEntry {
	var slEntry *schedulepb.ScheduleListEntry
	s.Require().Eventually(func() bool { // wait for visibility
		listResp, err := s.FrontendClient().ListSchedules(s.newContext(), workflowservice.ListSchedulesRequest_builder{
			Namespace:       s.Namespace().String(),
			MaximumPageSize: 5,
		}.Build())
		if err != nil {
			return false
		}
		for _, ent := range listResp.GetSchedules() {
			if ent.GetScheduleId() == sid {
				if predicate != nil && !predicate(ent) {
					return false
				}
				slEntry = ent
				return true
			}
		}
		return false
	}, 15*time.Second, 1*time.Second)
	return slEntry
}

func (s *scheduleFunctionalSuiteBase) assertSameRecentActions(
	expected *workflowservice.DescribeScheduleResponse, actual *schedulepb.ScheduleListEntry,
) {
	s.T().Helper()
	if len(expected.GetInfo().GetRecentActions()) != len(actual.GetInfo().GetRecentActions()) {
		s.T().Fatalf(
			"RecentActions have different length expected %d, got %d",
			len(expected.GetInfo().GetRecentActions()),
			len(actual.GetInfo().GetRecentActions()))
	}
	for i := range expected.GetInfo().GetRecentActions() {
		if !proto.Equal(expected.GetInfo().GetRecentActions()[i], actual.GetInfo().GetRecentActions()[i]) {
			s.T().Errorf(
				"RecentActions are differ at index %d. Expected %v, got %v",
				i,
				expected.GetInfo().GetRecentActions()[i],
				actual.GetInfo().GetRecentActions()[i],
			)
		}
	}
}

func (s *scheduleFunctionalSuiteBase) cleanup(sid string) {
	s.T().Cleanup(func() {
		_, _ = s.FrontendClient().DeleteSchedule(s.newContext(), workflowservice.DeleteScheduleRequest_builder{
			Namespace:  s.Namespace().String(),
			ScheduleId: sid,
			Identity:   "test",
		}.Build())
	})
}

// TestCreateScheduleAlreadyExists verifies that creating a schedule with the same ID
// returns an AlreadyExists serviceerror.
func (s *ScheduleCHASMFunctionalSuite) TestCreateScheduleAlreadyExists() {
	sid := "sched-test-already-exists"

	schedule := schedulepb.Schedule_builder{
		Spec: schedulepb.ScheduleSpec_builder{
			Interval: []*schedulepb.IntervalSpec{
				schedulepb.IntervalSpec_builder{Interval: durationpb.New(1 * time.Hour)}.Build(),
			},
		}.Build(),
		Action: schedulepb.ScheduleAction_builder{
			StartWorkflow: workflowpb.NewWorkflowExecutionInfo_builder{
				WorkflowId:   "wf-already-exists",
				WorkflowType: commonpb.WorkflowType_builder{Name: "action"}.Build(),
				TaskQueue:    taskqueuepb.TaskQueue_builder{Name: s.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}.Build(),
			}.Build(),
		}.Build(),
	}.Build()
	req := workflowservice.CreateScheduleRequest_builder{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	}.Build()

	ctx := s.newContext()
	_, err := s.FrontendClient().CreateSchedule(ctx, req)
	s.NoError(err)
	s.cleanup(sid)

	// Try to create again with a different request ID - should fail with AlreadyExists
	req.SetRequestId(uuid.NewString())
	_, err = s.FrontendClient().CreateSchedule(ctx, req)
	s.Error(err)

	var alreadyExists *serviceerror.AlreadyExists
	s.ErrorAs(err, &alreadyExists)
	s.Contains(err.Error(), sid)
}
