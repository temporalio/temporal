// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package tests

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/testing/historyrequire"
	"go.temporal.io/server/common/testing/protorequire"
	schedulerhsm "go.temporal.io/server/components/scheduler"
	"go.temporal.io/server/service/worker/scheduler"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
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
	ScheduleFunctionalSuite struct {
		protorequire.ProtoAssertions
		historyrequire.HistoryRequire
		testcore.FunctionalTestBase
		sdkClient     sdkclient.Client
		worker        worker.Worker
		taskQueue     string
		dataConverter converter.DataConverter
	}
)

func TestScheduleFunctionalSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(ScheduleFunctionalSuite))
}

func (s *ScheduleFunctionalSuite) SetupSuite() {
	if testcore.UsingSQLAdvancedVisibility() {
		s.FunctionalTestBase.SetupSuite("testdata/cluster.yaml")
		s.Logger.Info(fmt.Sprintf("Running schedule tests with %s/%s persistence", testcore.TestFlags.PersistenceType, testcore.TestFlags.PersistenceDriver))
	} else {
		s.FunctionalTestBase.SetupSuite("testdata/es_cluster.yaml")
		s.Logger.Info("Running schedule tests with Elasticsearch persistence")
	}
}

func (s *ScheduleFunctionalSuite) TearDownSuite() {
	s.FunctionalTestBase.TearDownSuite()
}

func (s *ScheduleFunctionalSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()

	s.ProtoAssertions = protorequire.New(s.T())
	s.HistoryRequire = historyrequire.New(s.T())
	s.dataConverter = testcore.NewTestDataConverter()
	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:      s.FrontendGRPCAddress(),
		Namespace:     s.Namespace(),
		DataConverter: s.dataConverter,
	})
	if err != nil {
		s.Logger.Fatal("Error when creating SDK client", tag.Error(err))
	}
	s.sdkClient = sdkClient
	s.taskQueue = testcore.RandomizeStr("tq")
	s.worker = worker.New(s.sdkClient, s.taskQueue, worker.Options{})
	if err := s.worker.Start(); err != nil {
		s.Logger.Fatal("Error when starting worker", tag.Error(err))
	}
}

func (s *ScheduleFunctionalSuite) TearDownTest() {
	if s.worker != nil {
		s.worker.Stop()
	}
	if s.sdkClient != nil {
		s.sdkClient.Close()
	}
}

func (s *ScheduleFunctionalSuite) TestBasics() {
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

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(5 * time.Second)},
			},
			Calendar: []*schedulepb.CalendarSpec{
				{DayOfMonth: "10", Year: "2010"},
			},
			CronString: []string{"11 11/11 11 11 1 2011"},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Memo: &commonpb.Memo{
						Fields: map[string]*commonpb.Payload{"wfmemo1": wfMemo},
					},
					SearchAttributes: &commonpb.SearchAttributes{
						IndexedFields: map[string]*commonpb.Payload{csaKeyword: wfSAValue},
					},
				},
			},
		},
	}
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.New(),
		Memo: &commonpb.Memo{
			Fields: map[string]*commonpb.Payload{"schedmemo1": schMemo},
		},
		SearchAttributes: &commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{
				csaKeyword: schSAValue,
				csaInt:     schSAIntValue,
				csaBool:    schSABoolValue,
			},
		},
	}

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

	createTime := time.Now()
	_, err := s.FrontendClient().CreateSchedule(testcore.NewContext(), req)
	s.NoError(err)
	s.cleanup(sid)

	// sleep until we see two runs, plus a bit more to ensure that the second run has completed
	s.Eventually(func() bool { return atomic.LoadInt32(&runs) == 2 }, 15*time.Second, 500*time.Millisecond)
	time.Sleep(2 * time.Second) //nolint:forbidigo

	// describe

	describeResp, err := s.FrontendClient().DescribeSchedule(testcore.NewContext(), &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace(),
		ScheduleId: sid,
	})
	s.NoError(err)

	checkSpec := func(spec *schedulepb.ScheduleSpec) {
		protorequire.ProtoSliceEqual(s.T(), schedule.Spec.Interval, spec.Interval)
		s.Nil(spec.Calendar)
		s.Nil(spec.CronString)
		s.ProtoElementsMatch([]*schedulepb.StructuredCalendarSpec{
			{
				Second:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
				Minute:     []*schedulepb.Range{{Start: 11, End: 11, Step: 1}},
				Hour:       []*schedulepb.Range{{Start: 11, End: 23, Step: 11}},
				DayOfMonth: []*schedulepb.Range{{Start: 11, End: 11, Step: 1}},
				Month:      []*schedulepb.Range{{Start: 11, End: 11, Step: 1}},
				DayOfWeek:  []*schedulepb.Range{{Start: 1, End: 1, Step: 1}},
				Year:       []*schedulepb.Range{{Start: 2011, End: 2011, Step: 1}},
			},
			{
				Second:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
				Minute:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
				Hour:       []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
				DayOfMonth: []*schedulepb.Range{{Start: 10, End: 10, Step: 1}},
				Month:      []*schedulepb.Range{{Start: 1, End: 12, Step: 1}},
				DayOfWeek:  []*schedulepb.Range{{Start: 0, End: 6, Step: 1}},
				Year:       []*schedulepb.Range{{Start: 2010, End: 2010, Step: 1}},
			},
		}, spec.StructuredCalendar)
	}
	checkSpec(describeResp.Schedule.Spec)

	s.Equal(enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, describeResp.Schedule.Policies.OverlapPolicy)     // set to default value
	s.EqualValues(365*24*3600, describeResp.Schedule.Policies.CatchupWindow.AsDuration().Seconds()) // set to default value

	s.Equal(schSAValue.Data, describeResp.SearchAttributes.IndexedFields[csaKeyword].Data)
	s.Equal(schSAIntValue.Data, describeResp.SearchAttributes.IndexedFields[csaInt].Data)
	s.Equal(schSABoolValue.Data, describeResp.SearchAttributes.IndexedFields[csaBool].Data)
	s.Nil(describeResp.SearchAttributes.IndexedFields[searchattribute.BinaryChecksums])
	s.Nil(describeResp.SearchAttributes.IndexedFields[searchattribute.BuildIds])
	s.Nil(describeResp.SearchAttributes.IndexedFields[searchattribute.TemporalNamespaceDivision])
	s.Equal(schMemo.Data, describeResp.Memo.Fields["schedmemo1"].Data)
	s.Equal(wfSAValue.Data, describeResp.Schedule.Action.GetStartWorkflow().SearchAttributes.IndexedFields[csaKeyword].Data)
	s.Equal(wfMemo.Data, describeResp.Schedule.Action.GetStartWorkflow().Memo.Fields["wfmemo1"].Data)

	s.DurationNear(describeResp.Info.CreateTime.AsTime().Sub(createTime), 0, 3*time.Second)
	s.EqualValues(2, describeResp.Info.ActionCount)
	s.EqualValues(0, describeResp.Info.MissedCatchupWindow)
	s.EqualValues(0, describeResp.Info.OverlapSkipped)
	s.EqualValues(0, len(describeResp.Info.RunningWorkflows))
	s.EqualValues(2, len(describeResp.Info.RecentActions))
	action0 := describeResp.Info.RecentActions[0]
	s.WithinRange(action0.ScheduleTime.AsTime(), createTime, time.Now())
	s.True(action0.ScheduleTime.AsTime().UnixNano()%int64(5*time.Second) == 0)
	s.DurationNear(action0.ActualTime.AsTime().Sub(action0.ScheduleTime.AsTime()), 0, 3*time.Second)

	// list

	visibilityResponse := s.getScheduleEntryFomVisibility(sid, func(ent *schedulepb.ScheduleListEntry) bool {
		return len(ent.GetInfo().GetRecentActions()) >= 2
	})
	s.Equal(sid, visibilityResponse.ScheduleId)
	s.Equal(schSAValue.Data, visibilityResponse.SearchAttributes.IndexedFields[csaKeyword].Data)
	s.Equal(schSAIntValue.Data, describeResp.SearchAttributes.IndexedFields[csaInt].Data)
	s.Equal(schSABoolValue.Data, describeResp.SearchAttributes.IndexedFields[csaBool].Data)
	s.Nil(visibilityResponse.SearchAttributes.IndexedFields[searchattribute.BinaryChecksums])
	s.Nil(visibilityResponse.SearchAttributes.IndexedFields[searchattribute.BuildIds])
	s.Nil(visibilityResponse.SearchAttributes.IndexedFields[searchattribute.TemporalNamespaceDivision])
	s.Equal(schMemo.Data, visibilityResponse.Memo.Fields["schedmemo1"].Data)
	checkSpec(visibilityResponse.Info.Spec)
	s.Equal(wt, visibilityResponse.Info.WorkflowType.Name)
	s.False(visibilityResponse.Info.Paused)
	s.assertSameRecentActions(describeResp, visibilityResponse)

	// list workflows

	wfResp, err := s.FrontendClient().ListWorkflowExecutions(testcore.NewContext(), &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.Namespace(),
		PageSize:  5,
		Query:     "",
	})
	s.NoError(err)
	s.GreaterOrEqual(len(wfResp.Executions), 2) // could have had a 3rd run while waiting for visibility
	for _, ex := range wfResp.Executions {
		s.Equal(wt, ex.Type.Name, "should only see started workflows")
	}
	ex0 := wfResp.Executions[0]
	s.True(strings.HasPrefix(ex0.Execution.WorkflowId, wid))
	s.True(ex0.Execution.RunId == describeResp.Info.RecentActions[0].GetStartWorkflowResult().RunId ||
		ex0.Execution.RunId == describeResp.Info.RecentActions[1].GetStartWorkflowResult().RunId)
	s.Equal(wt, ex0.Type.Name)
	s.Nil(ex0.ParentExecution) // not a child workflow
	s.Equal(wfMemo.Data, ex0.Memo.Fields["wfmemo1"].Data)
	s.Equal(wfSAValue.Data, ex0.SearchAttributes.IndexedFields[csaKeyword].Data)
	s.Equal(payload.EncodeString(sid).Data, ex0.SearchAttributes.IndexedFields[searchattribute.TemporalScheduledById].Data)
	var ex0StartTime time.Time
	s.NoError(payload.Decode(ex0.SearchAttributes.IndexedFields[searchattribute.TemporalScheduledStartTime], &ex0StartTime))
	s.WithinRange(ex0StartTime, createTime, time.Now())
	s.True(ex0StartTime.UnixNano()%int64(5*time.Second) == 0)

	// list with QueryWithAnyNamespaceDivision, we should see the scheduler workflow

	wfResp, err = s.FrontendClient().ListWorkflowExecutions(testcore.NewContext(), &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.Namespace(),
		PageSize:  5,
		Query:     searchattribute.QueryWithAnyNamespaceDivision(`ExecutionStatus = "Running"`),
	})
	s.NoError(err)
	count := 0
	for _, ex := range wfResp.Executions {
		if ex.Type.Name == scheduler.WorkflowType {
			count++
		}
	}
	s.EqualValues(1, count, "should see scheduler workflow")

	// list workflows with an exact match on namespace division (implementation details here, not public api)

	wfResp, err = s.FrontendClient().ListWorkflowExecutions(testcore.NewContext(), &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.Namespace(),
		PageSize:  5,
		Query:     fmt.Sprintf("%s = '%s'", searchattribute.TemporalNamespaceDivision, scheduler.NamespaceDivision),
	})
	s.NoError(err)
	s.EqualValues(1, len(wfResp.Executions), "should see scheduler workflow")
	ex0 = wfResp.Executions[0]
	s.Equal(scheduler.WorkflowType, ex0.Type.Name)

	// list schedules with search attribute filter

	listResp, err := s.FrontendClient().ListSchedules(testcore.NewContext(), &workflowservice.ListSchedulesRequest{
		Namespace:       s.Namespace(),
		MaximumPageSize: 5,
		Query:           "CustomKeywordField = 'schedule sa value' AND TemporalSchedulePaused = false",
	})
	s.NoError(err)
	s.Len(listResp.Schedules, 1)
	entry := listResp.Schedules[0]
	s.Equal(sid, entry.ScheduleId)

	// list schedules with invalid search attribute filter

	_, err = s.FrontendClient().ListSchedules(testcore.NewContext(), &workflowservice.ListSchedulesRequest{
		Namespace:       s.Namespace(),
		MaximumPageSize: 5,
		Query:           "ExecutionDuration > '1s'",
	})
	s.Error(err)

	// update schedule, no updates to search attributes

	schedule.Spec.Interval[0].Phase = durationpb.New(1 * time.Second)
	schedule.Action.GetStartWorkflow().WorkflowType.Name = wt2

	updateTime := time.Now()
	_, err = s.FrontendClient().UpdateSchedule(testcore.NewContext(), &workflowservice.UpdateScheduleRequest{
		Namespace:  s.Namespace(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.New(),
	})
	s.NoError(err)

	// wait for one new run
	s.Eventually(
		func() bool { return atomic.LoadInt32(&runs2) == 1 },
		7*time.Second,
		500*time.Millisecond,
	)

	// describe again
	describeResp, err = s.FrontendClient().DescribeSchedule(
		testcore.NewContext(),
		&workflowservice.DescribeScheduleRequest{
			Namespace:  s.Namespace(),
			ScheduleId: sid,
		},
	)
	s.NoError(err)

	s.Len(describeResp.SearchAttributes.GetIndexedFields(), 3)
	s.Equal(schSAValue.Data, describeResp.SearchAttributes.IndexedFields[csaKeyword].Data)
	s.Equal(schSAIntValue.Data, describeResp.SearchAttributes.IndexedFields[csaInt].Data)
	s.Equal(schSABoolValue.Data, describeResp.SearchAttributes.IndexedFields[csaBool].Data)
	s.Equal(schMemo.Data, describeResp.Memo.Fields["schedmemo1"].Data)
	s.Equal(wfSAValue.Data, describeResp.Schedule.Action.GetStartWorkflow().SearchAttributes.IndexedFields[csaKeyword].Data)
	s.Equal(wfMemo.Data, describeResp.Schedule.Action.GetStartWorkflow().Memo.Fields["wfmemo1"].Data)

	s.DurationNear(describeResp.Info.UpdateTime.AsTime().Sub(updateTime), 0, 3*time.Second)
	lastAction := describeResp.Info.RecentActions[len(describeResp.Info.RecentActions)-1]
	s.True(lastAction.ScheduleTime.AsTime().UnixNano()%int64(5*time.Second) == 1000000000, lastAction.ScheduleTime.AsTime().UnixNano())

	// update schedule and search attributes

	schedule.Spec.Interval[0].Phase = durationpb.New(1 * time.Second)
	schedule.Action.GetStartWorkflow().WorkflowType.Name = wt2

	csaDouble := "CustomDoubleField"
	schSADoubleValue, _ := payload.Encode(3.14)
	schSAIntValue, _ = payload.Encode(321)
	_, err = s.FrontendClient().UpdateSchedule(testcore.NewContext(), &workflowservice.UpdateScheduleRequest{
		Namespace:  s.Namespace(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.New(),
		SearchAttributes: &commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{
				csaKeyword: schSAValue,       // same key, same value
				csaInt:     schSAIntValue,    // same key, new value
				csaDouble:  schSADoubleValue, // new key
				// csaBool is removed
			},
		},
	})
	s.NoError(err)

	// wait until search attributes are updated
	s.EventuallyWithT(
		func(c *assert.CollectT) {
			describeResp, err = s.FrontendClient().DescribeSchedule(
				testcore.NewContext(),
				&workflowservice.DescribeScheduleRequest{
					Namespace:  s.Namespace(),
					ScheduleId: sid,
				},
			)
			assert.NoError(c, err)
			assert.Len(c, describeResp.SearchAttributes.GetIndexedFields(), 3)
			assert.Equal(c, schSAValue.Data, describeResp.SearchAttributes.IndexedFields[csaKeyword].Data)
			assert.Equal(c, schSAIntValue.Data, describeResp.SearchAttributes.IndexedFields[csaInt].Data)
			assert.Equal(c, schSADoubleValue.Data, describeResp.SearchAttributes.IndexedFields[csaDouble].Data)
			assert.NotContains(c, describeResp.SearchAttributes.IndexedFields, csaBool)
		},
		2*time.Second,
		500*time.Millisecond,
	)

	// update schedule and unset search attributes

	schedule.Spec.Interval[0].Phase = durationpb.New(1 * time.Second)
	schedule.Action.GetStartWorkflow().WorkflowType.Name = wt2

	_, err = s.FrontendClient().UpdateSchedule(testcore.NewContext(), &workflowservice.UpdateScheduleRequest{
		Namespace:        s.Namespace(),
		ScheduleId:       sid,
		Schedule:         schedule,
		Identity:         "test",
		RequestId:        uuid.New(),
		SearchAttributes: &commonpb.SearchAttributes{},
	})
	s.NoError(err)

	// wait until search attributes are updated
	s.EventuallyWithT(
		func(c *assert.CollectT) {
			describeResp, err = s.FrontendClient().DescribeSchedule(
				testcore.NewContext(),
				&workflowservice.DescribeScheduleRequest{
					Namespace:  s.Namespace(),
					ScheduleId: sid,
				},
			)
			assert.NoError(c, err)
			assert.Empty(c, describeResp.SearchAttributes.GetIndexedFields())
		},
		5*time.Second,
		500*time.Millisecond,
	)

	// pause

	_, err = s.FrontendClient().PatchSchedule(testcore.NewContext(), &workflowservice.PatchScheduleRequest{
		Namespace:  s.Namespace(),
		ScheduleId: sid,
		Patch: &schedulepb.SchedulePatch{
			Pause: "because I said so",
		},
		Identity:  "test",
		RequestId: uuid.New(),
	})
	s.NoError(err)

	time.Sleep(7 * time.Second) //nolint:forbidigo
	s.EqualValues(1, atomic.LoadInt32(&runs2), "has not run again")

	describeResp, err = s.FrontendClient().DescribeSchedule(testcore.NewContext(), &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace(),
		ScheduleId: sid,
	})
	s.NoError(err)

	s.True(describeResp.Schedule.State.Paused)
	s.Equal("because I said so", describeResp.Schedule.State.Notes)

	// don't loop to wait for visibility, we already waited 7s from the patch
	listResp, err = s.FrontendClient().ListSchedules(testcore.NewContext(), &workflowservice.ListSchedulesRequest{
		Namespace:       s.Namespace(),
		MaximumPageSize: 5,
	})
	s.NoError(err)
	s.Equal(1, len(listResp.Schedules))
	entry = listResp.Schedules[0]
	s.Equal(sid, entry.ScheduleId)
	s.True(entry.Info.Paused)
	s.Equal("because I said so", entry.Info.Notes)

	// finally delete

	_, err = s.FrontendClient().DeleteSchedule(testcore.NewContext(), &workflowservice.DeleteScheduleRequest{
		Namespace:  s.Namespace(),
		ScheduleId: sid,
		Identity:   "test",
	})
	s.NoError(err)

	describeResp, err = s.FrontendClient().DescribeSchedule(testcore.NewContext(), &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace(),
		ScheduleId: sid,
	})
	s.Error(err)

	s.Eventually(func() bool { // wait for visibility
		listResp, err := s.FrontendClient().ListSchedules(testcore.NewContext(), &workflowservice.ListSchedulesRequest{
			Namespace:       s.Namespace(),
			MaximumPageSize: 5,
		})
		s.NoError(err)
		return len(listResp.Schedules) == 0
	}, 10*time.Second, 1*time.Second)
}

func (s *ScheduleFunctionalSuite) TestInput() {
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

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(3 * time.Second)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:        inputPayloads,
				},
			},
		},
	}
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.New(),
	}

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

	_, err = s.FrontendClient().CreateSchedule(testcore.NewContext(), req)
	s.NoError(err)
	s.cleanup(sid)

	s.Eventually(func() bool { return atomic.LoadInt32(&runs) == 1 }, 8*time.Second, 200*time.Millisecond)
}

func (s *ScheduleFunctionalSuite) TestExperimentalHsmInput() {
	sid := "sched-test-experimental-hsm"
	wid := "sched-test-experimental-hsm-wf"
	wt := "sched-test-experimental-hsm-wt"

	s.OverrideDynamicConfig(schedulerhsm.UseExperimentalHsmScheduler, true)

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

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(3 * time.Second)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:        inputPayloads,
				},
			},
		},
	}
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.New(),
	}

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

	_, err = s.FrontendClient().CreateSchedule(testcore.NewContext(), req)
	s.NoError(err)
	s.cleanup(sid)

	s.Eventually(func() bool { return atomic.LoadInt32(&runs) == 1 }, 8*time.Second, 200*time.Millisecond)

	events := s.GetHistory(s.Namespace(), &commonpb.WorkflowExecution{WorkflowId: scheduler.WorkflowIDPrefix + sid})
	expectedHistory := `1 WorkflowExecutionStarted`
	s.EqualHistoryEvents(expectedHistory, events)
}

func (s *ScheduleFunctionalSuite) TestLastCompletionAndError() {
	sid := "sched-test-last"
	wid := "sched-test-last-wf"
	wt := "sched-test-last-wt"

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(3 * time.Second)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.New(),
	}

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
			return "", errors.New("this one fails") //nolint:goerr113
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

	_, err := s.FrontendClient().CreateSchedule(testcore.NewContext(), req)
	s.NoError(err)
	s.cleanup(sid)

	s.Eventually(func() bool { return atomic.LoadInt32(&testComplete) == 1 }, 20*time.Second, 200*time.Millisecond)
}

func (s *ScheduleFunctionalSuite) TestExperimentalHsmLastCompletionAndError() {
	sid := "sched-test-last"
	wid := "sched-test-last-wf"
	wt := "sched-test-last-wt"

	s.OverrideDynamicConfig(schedulerhsm.UseExperimentalHsmScheduler, true)

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(3 * time.Second)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.New(),
	}

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
			return "", errors.New("this one fails") //nolint:goerr113
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

	_, err := s.FrontendClient().CreateSchedule(testcore.NewContext(), req)
	s.NoError(err)
	s.cleanup(sid)

	s.Eventually(func() bool { return atomic.LoadInt32(&testComplete) == 1 }, 20*time.Second, 200*time.Millisecond)
}

func (s *ScheduleFunctionalSuite) TestRefresh() {
	sid := "sched-test-refresh"
	wid := "sched-test-refresh-wf"
	wt := "sched-test-refresh-wt"

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{
					Interval: durationpb.New(30 * time.Second),
					// start within three seconds
					Phase: durationpb.New(time.Duration((time.Now().Unix()+3)%30) * time.Second),
				},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:               wid,
					WorkflowType:             &commonpb.WorkflowType{Name: wt},
					TaskQueue:                &taskqueuepb.TaskQueue{Name: s.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					WorkflowExecutionTimeout: durationpb.New(3 * time.Second),
				},
			},
		},
	}
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.New(),
	}

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

	_, err := s.FrontendClient().CreateSchedule(testcore.NewContext(), req)
	s.NoError(err)
	s.cleanup(sid)

	s.Eventually(func() bool { return atomic.LoadInt32(&runs) == 1 }, 6*time.Second, 200*time.Millisecond)

	// workflow has started but is now sleeping. it will timeout in 2 seconds.

	describeResp, err := s.FrontendClient().DescribeSchedule(testcore.NewContext(), &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace(),
		ScheduleId: sid,
	})
	s.NoError(err)
	s.EqualValues(1, len(describeResp.Info.RunningWorkflows))

	events1 := s.GetHistory(s.Namespace(), &commonpb.WorkflowExecution{WorkflowId: scheduler.WorkflowIDPrefix + sid})
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

	events2 := s.GetHistory(s.Namespace(), &commonpb.WorkflowExecution{WorkflowId: scheduler.WorkflowIDPrefix + sid})
	s.EqualHistoryEvents(expectedHistory, events2)

	// when we describe we'll force a refresh and see it timed out
	describeResp, err = s.FrontendClient().DescribeSchedule(testcore.NewContext(), &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace(),
		ScheduleId: sid,
	})
	s.NoError(err)
	s.EqualValues(0, len(describeResp.Info.RunningWorkflows))

	// check scheduler has gotten the refresh and done some stuff. signal is sent without waiting so we need to wait.
	s.Eventually(func() bool {
		events3 := s.GetHistory(s.Namespace(), &commonpb.WorkflowExecution{WorkflowId: scheduler.WorkflowIDPrefix + sid})
		return len(events3) > len(events2)
	}, 5*time.Second, 100*time.Millisecond)
}

func (s *ScheduleFunctionalSuite) TestListBeforeRun() {
	sid := "sched-test-list-before-run"
	wid := "sched-test-list-before-run-wf"
	wt := "sched-test-list-before-run-wt"

	// disable per-ns worker so that the schedule workflow never runs
	s.OverrideDynamicConfig(dynamicconfig.WorkerPerNamespaceWorkerCount, 0)

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(3 * time.Second)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.New(),
	}

	startTime := time.Now()

	_, err := s.FrontendClient().CreateSchedule(testcore.NewContext(), req)
	s.NoError(err)
	s.cleanup(sid)

	entry := s.getScheduleEntryFomVisibility(sid, nil)
	s.NotNil(entry.Info)
	s.ProtoEqual(schedule.Spec, entry.Info.Spec)
	s.Equal(wt, entry.Info.WorkflowType.Name)
	s.False(entry.Info.Paused)
	s.Greater(len(entry.Info.FutureActionTimes), 1)
	s.True(entry.Info.FutureActionTimes[0].AsTime().After(startTime))
}

func (s *ScheduleFunctionalSuite) TestRateLimit() {
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
		schedule := &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{
				Interval: []*schedulepb.IntervalSpec{
					{Interval: durationpb.New(1 * time.Second)},
				},
			},
			Action: &schedulepb.ScheduleAction{
				Action: &schedulepb.ScheduleAction_StartWorkflow{
					StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
						WorkflowId:   fmt.Sprintf(wid, i),
						WorkflowType: &commonpb.WorkflowType{Name: wt},
						TaskQueue:    &taskqueuepb.TaskQueue{Name: s.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
		}
		_, err := s.FrontendClient().CreateSchedule(testcore.NewContext(), &workflowservice.CreateScheduleRequest{
			Namespace:  s.Namespace(),
			ScheduleId: fmt.Sprintf(sid, i),
			Schedule:   schedule,
			Identity:   "test",
			RequestId:  uuid.New(),
		})
		s.NoError(err)
		s.cleanup(fmt.Sprintf(sid, i))
	}

	time.Sleep(5 * time.Second) //nolint:forbidigo

	// With no rate limit, we'd see 10/second == 50 workflows run. With a limit of 1/sec, we
	// expect to see around 5.
	s.Less(atomic.LoadInt32(&runs), int32(10))
}

func (s *ScheduleFunctionalSuite) TestNextTimeCache() {
	sid := "sched-test-next-time-cache"
	wid := "sched-test-next-time-cache-wf"
	wt := "sched-test-next-time-cache-wt"

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(1 * time.Second)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.New(),
	}

	var runs atomic.Int32
	workflowFn := func(ctx workflow.Context) error {
		workflow.SideEffect(ctx, func(ctx workflow.Context) any {
			runs.Add(1)
			return 0
		})
		return nil
	}
	s.worker.RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})

	_, err := s.FrontendClient().CreateSchedule(testcore.NewContext(), req)
	s.NoError(err)
	s.cleanup(sid)

	// wait for at least 13 runs
	const count = 13
	s.Eventually(func() bool { return runs.Load() >= count }, (count+10)*time.Second, 500*time.Millisecond)

	// there should be only four side effects for 13 runs, and only two mentioning "Next"
	// (cache refills)
	events := s.GetHistory(s.Namespace(), &commonpb.WorkflowExecution{WorkflowId: scheduler.WorkflowIDPrefix + sid})
	var sideEffects, nextTimeSideEffects int
	for _, e := range events {
		if marker := e.GetMarkerRecordedEventAttributes(); marker.GetMarkerName() == "SideEffect" {
			sideEffects++
			if p, ok := marker.Details["data"]; ok && len(p.Payloads) == 1 {
				if string(p.Payloads[0].Metadata["messageType"]) == "temporal.server.api.schedule.v1.NextTimeCache" ||
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
func (s *ScheduleFunctionalSuite) getScheduleEntryFomVisibility(sid string, predicate func(*schedulepb.ScheduleListEntry) bool) *schedulepb.ScheduleListEntry {
	var slEntry *schedulepb.ScheduleListEntry
	s.Require().Eventually(func() bool { // wait for visibility
		listResp, err := s.FrontendClient().ListSchedules(testcore.NewContext(), &workflowservice.ListSchedulesRequest{
			Namespace:       s.Namespace(),
			MaximumPageSize: 5,
		})
		if err != nil {
			return false
		}
		for _, ent := range listResp.Schedules {
			if ent.ScheduleId == sid {
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

func (s *ScheduleFunctionalSuite) assertSameRecentActions(
	expected *workflowservice.DescribeScheduleResponse, actual *schedulepb.ScheduleListEntry,
) {
	s.T().Helper()
	if len(expected.Info.RecentActions) != len(actual.Info.RecentActions) {
		s.T().Fatalf(
			"RecentActions have different length expected %d, got %d",
			len(expected.Info.RecentActions),
			len(actual.Info.RecentActions))
	}
	for i := range expected.Info.RecentActions {
		if !proto.Equal(expected.Info.RecentActions[i], actual.Info.RecentActions[i]) {
			s.T().Errorf(
				"RecentActions are differ at index %d. Expected %v, got %v",
				i,
				expected.Info.RecentActions[i],
				actual.Info.RecentActions[i],
			)
		}
	}
}

func (s *ScheduleFunctionalSuite) cleanup(sid string) {
	s.T().Cleanup(func() {
		_, _ = s.FrontendClient().DeleteSchedule(testcore.NewContext(), &workflowservice.DeleteScheduleRequest{
			Namespace:  s.Namespace(),
			ScheduleId: sid,
			Identity:   "test",
		})
	})
}
