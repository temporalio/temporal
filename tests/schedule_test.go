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
	"flag"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
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
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/worker/scheduler"
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
	scheduleIntegrationSuite struct {
		*require.Assertions
		IntegrationBase
		hostPort      string
		sdkClient     sdkclient.Client
		worker        worker.Worker
		taskQueue     string
		dataConverter converter.DataConverter
	}
)

func TestScheduleIntegrationSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(scheduleIntegrationSuite))
}

func (s *scheduleIntegrationSuite) SetupSuite() {
	s.hostPort = "127.0.0.1:7134"
	if TestFlags.FrontendAddr != "" {
		s.hostPort = TestFlags.FrontendAddr
	}
	switch TestFlags.PersistenceDriver {
	case mysql.PluginNameV8, postgresql.PluginNameV12, sqlite.PluginName:
		s.setupSuite("testdata/integration_test_cluster.yaml")
		s.Logger.Info(fmt.Sprintf("Running schedule tests with %s/%s persistence", TestFlags.PersistenceType, TestFlags.PersistenceDriver))
	default:
		s.setupSuite("testdata/integration_test_es_cluster.yaml")
		s.Logger.Info("Running schedule tests with Elasticsearch persistence")
	}
}

func (s *scheduleIntegrationSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *scheduleIntegrationSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.dataConverter = newTestDataConverter()
	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:      s.hostPort,
		Namespace:     s.namespace,
		DataConverter: s.dataConverter,
	})
	if err != nil {
		s.Logger.Fatal("Error when creating SDK client", tag.Error(err))
	}
	s.sdkClient = sdkClient
	s.taskQueue = s.randomizeStr("tq")
	s.worker = worker.New(s.sdkClient, s.taskQueue, worker.Options{})
	if err := s.worker.Start(); err != nil {
		s.Logger.Fatal("Error when starting worker", tag.Error(err))
	}
}

func (s *scheduleIntegrationSuite) TearDownTest() {
	s.worker.Stop()
	s.sdkClient.Close()
}

func (s *scheduleIntegrationSuite) TestBasics() {
	sid := "sched-test-basics"
	wid := "sched-test-basics-wf"
	wt := "sched-test-basics-wt"
	wt2 := "sched-test-basics-wt2"

	// switch this to test with search attribute mapper:
	// csa := "AliasForCustomKeywordField"
	csa := "CustomKeywordField"

	wfMemo := payload.EncodeString("workflow memo")
	wfSAValue := payload.EncodeString("workflow sa value")
	schMemo := payload.EncodeString("schedule memo")
	schSAValue := payload.EncodeString("schedule sa value")

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: timestamp.DurationPtr(5 * time.Second)},
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
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.taskQueue},
					Memo: &commonpb.Memo{
						Fields: map[string]*commonpb.Payload{"wfmemo1": wfMemo},
					},
					SearchAttributes: &commonpb.SearchAttributes{
						IndexedFields: map[string]*commonpb.Payload{csa: wfSAValue},
					},
				},
			},
		},
	}
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  s.namespace,
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.New(),
		Memo: &commonpb.Memo{
			Fields: map[string]*commonpb.Payload{"schedmemo1": schMemo},
		},
		SearchAttributes: &commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{csa: schSAValue},
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
	_, err := s.engine.CreateSchedule(NewContext(), req)
	s.NoError(err)

	// sleep until we see two runs, plus a bit more to ensure that the second run has completed
	s.Eventually(func() bool { return atomic.LoadInt32(&runs) == 2 }, 12*time.Second, 500*time.Millisecond)
	time.Sleep(1 * time.Second)

	// describe

	describeResp, err := s.engine.DescribeSchedule(NewContext(), &workflowservice.DescribeScheduleRequest{
		Namespace:  s.namespace,
		ScheduleId: sid,
	})
	s.NoError(err)

	checkSpec := func(spec *schedulepb.ScheduleSpec) {
		s.Equal(schedule.Spec.Interval, spec.Interval)
		s.Nil(spec.Calendar)
		s.Nil(spec.CronString)
		s.ElementsMatch([]*schedulepb.StructuredCalendarSpec{
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

	s.Equal(enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, describeResp.Schedule.Policies.OverlapPolicy) // set to default value
	s.EqualValues(365*24*3600, describeResp.Schedule.Policies.CatchupWindow.Seconds())          // set to default value

	s.Equal(schSAValue.Data, describeResp.SearchAttributes.IndexedFields[csa].Data)
	s.Nil(describeResp.SearchAttributes.IndexedFields[searchattribute.BinaryChecksums])
	s.Nil(describeResp.SearchAttributes.IndexedFields[searchattribute.BuildIds])
	s.Equal(schMemo.Data, describeResp.Memo.Fields["schedmemo1"].Data)
	s.Equal(wfSAValue.Data, describeResp.Schedule.Action.GetStartWorkflow().SearchAttributes.IndexedFields[csa].Data)
	s.Equal(wfMemo.Data, describeResp.Schedule.Action.GetStartWorkflow().Memo.Fields["wfmemo1"].Data)

	s.DurationNear(describeResp.Info.CreateTime.Sub(createTime), 0, 3*time.Second)
	s.EqualValues(2, describeResp.Info.ActionCount)
	s.EqualValues(0, describeResp.Info.MissedCatchupWindow)
	s.EqualValues(0, describeResp.Info.OverlapSkipped)
	s.EqualValues(0, len(describeResp.Info.RunningWorkflows))
	s.EqualValues(2, len(describeResp.Info.RecentActions))
	action0 := describeResp.Info.RecentActions[0]
	s.WithinRange(*action0.ScheduleTime, createTime, time.Now())
	s.True(action0.ScheduleTime.UnixNano()%int64(5*time.Second) == 0)
	s.DurationNear(action0.ActualTime.Sub(*action0.ScheduleTime), 0, 3*time.Second)

	// list

	s.Eventually(func() bool { // wait for visibility
		listResp, err := s.engine.ListSchedules(NewContext(), &workflowservice.ListSchedulesRequest{
			Namespace:       s.namespace,
			MaximumPageSize: 5,
		})
		if err != nil || len(listResp.Schedules) != 1 || listResp.Schedules[0].ScheduleId != sid || len(listResp.Schedules[0].GetInfo().GetRecentActions()) < 2 {
			return false
		}
		s.NoError(err)
		entry := listResp.Schedules[0]
		s.Equal(sid, entry.ScheduleId)
		s.Equal(schSAValue.Data, entry.SearchAttributes.IndexedFields[csa].Data)
		s.Equal(schMemo.Data, entry.Memo.Fields["schedmemo1"].Data)
		checkSpec(entry.Info.Spec)
		s.Equal(wt, entry.Info.WorkflowType.Name)
		s.False(entry.Info.Paused)
		s.Equal(describeResp.Info.RecentActions, entry.Info.RecentActions) // 2 is below the limit where list entry might be cut off
		return true
	}, 10*time.Second, 1*time.Second)

	// list workflows

	wfResp, err := s.engine.ListWorkflowExecutions(NewContext(), &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace,
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
	s.Equal(wfSAValue.Data, ex0.SearchAttributes.IndexedFields[csa].Data)
	s.Equal(payload.EncodeString(sid).Data, ex0.SearchAttributes.IndexedFields[searchattribute.TemporalScheduledById].Data)
	var ex0StartTime time.Time
	s.NoError(payload.Decode(ex0.SearchAttributes.IndexedFields[searchattribute.TemporalScheduledStartTime], &ex0StartTime))
	s.WithinRange(ex0StartTime, createTime, time.Now())
	s.True(ex0StartTime.UnixNano()%int64(5*time.Second) == 0)

	// list workflows with namespace division (implementation details here, not public api)

	wfResp, err = s.engine.ListWorkflowExecutions(NewContext(), &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace,
		PageSize:  5,
		Query:     fmt.Sprintf("%s = \"%s\"", searchattribute.TemporalNamespaceDivision, scheduler.NamespaceDivision),
	})
	s.NoError(err)
	s.EqualValues(1, len(wfResp.Executions), "should see scheduler workflow")
	ex0 = wfResp.Executions[0]
	s.Equal(scheduler.WorkflowType, ex0.Type.Name)

	// update

	schedule.Spec.Interval[0].Phase = timestamp.DurationPtr(1 * time.Second)
	schedule.Action.GetStartWorkflow().WorkflowType.Name = wt2

	updateTime := time.Now()
	_, err = s.engine.UpdateSchedule(NewContext(), &workflowservice.UpdateScheduleRequest{
		Namespace:  s.namespace,
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.New(),
	})
	s.NoError(err)

	// wait for one new run
	s.Eventually(func() bool { return atomic.LoadInt32(&runs2) == 1 }, 7*time.Second, 500*time.Millisecond)

	// describe again
	describeResp, err = s.engine.DescribeSchedule(NewContext(), &workflowservice.DescribeScheduleRequest{
		Namespace:  s.namespace,
		ScheduleId: sid,
	})
	s.NoError(err)

	s.Equal(schSAValue.Data, describeResp.SearchAttributes.IndexedFields[csa].Data)
	s.Equal(schMemo.Data, describeResp.Memo.Fields["schedmemo1"].Data)
	s.Equal(wfSAValue.Data, describeResp.Schedule.Action.GetStartWorkflow().SearchAttributes.IndexedFields[csa].Data)
	s.Equal(wfMemo.Data, describeResp.Schedule.Action.GetStartWorkflow().Memo.Fields["wfmemo1"].Data)

	s.DurationNear(describeResp.Info.UpdateTime.Sub(updateTime), 0, 3*time.Second)
	lastAction := describeResp.Info.RecentActions[len(describeResp.Info.RecentActions)-1]
	s.True(lastAction.ScheduleTime.UnixNano()%int64(5*time.Second) == 1000000000, lastAction.ScheduleTime.UnixNano())

	// pause

	_, err = s.engine.PatchSchedule(NewContext(), &workflowservice.PatchScheduleRequest{
		Namespace:  s.namespace,
		ScheduleId: sid,
		Patch: &schedulepb.SchedulePatch{
			Pause: "because I said so",
		},
		Identity:  "test",
		RequestId: uuid.New(),
	})
	s.NoError(err)

	time.Sleep(7 * time.Second)
	s.EqualValues(1, atomic.LoadInt32(&runs2), "has not run again")

	describeResp, err = s.engine.DescribeSchedule(NewContext(), &workflowservice.DescribeScheduleRequest{
		Namespace:  s.namespace,
		ScheduleId: sid,
	})
	s.NoError(err)

	s.True(describeResp.Schedule.State.Paused)
	s.Equal("because I said so", describeResp.Schedule.State.Notes)

	// don't loop to wait for visibility, we already waited 7s from the patch
	listResp, err := s.engine.ListSchedules(NewContext(), &workflowservice.ListSchedulesRequest{
		Namespace:       s.namespace,
		MaximumPageSize: 5,
	})
	s.NoError(err)
	s.Equal(1, len(listResp.Schedules))
	entry := listResp.Schedules[0]
	s.Equal(sid, entry.ScheduleId)
	s.True(entry.Info.Paused)
	s.Equal("because I said so", entry.Info.Notes)

	// finally delete

	_, err = s.engine.DeleteSchedule(NewContext(), &workflowservice.DeleteScheduleRequest{
		Namespace:  s.namespace,
		ScheduleId: sid,
		Identity:   "test",
	})
	s.NoError(err)

	describeResp, err = s.engine.DescribeSchedule(NewContext(), &workflowservice.DescribeScheduleRequest{
		Namespace:  s.namespace,
		ScheduleId: sid,
	})
	s.Error(err)

	s.Eventually(func() bool { // wait for visibility
		listResp, err := s.engine.ListSchedules(NewContext(), &workflowservice.ListSchedulesRequest{
			Namespace:       s.namespace,
			MaximumPageSize: 5,
		})
		s.NoError(err)
		return len(listResp.Schedules) == 0
	}, 10*time.Second, 1*time.Second)
}

func (s *scheduleIntegrationSuite) TestInput() {
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
				{Interval: timestamp.DurationPtr(3 * time.Second)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.taskQueue},
					Input:        inputPayloads,
				},
			},
		},
	}
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  s.namespace,
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

	_, err = s.engine.CreateSchedule(NewContext(), req)
	s.NoError(err)
	s.Eventually(func() bool { return atomic.LoadInt32(&runs) == 1 }, 5*time.Second, 200*time.Millisecond)

	// cleanup
	_, err = s.engine.DeleteSchedule(NewContext(), &workflowservice.DeleteScheduleRequest{
		Namespace:  s.namespace,
		ScheduleId: sid,
		Identity:   "test",
	})
	s.NoError(err)
}

func (s *scheduleIntegrationSuite) TestLastCompletionAndError() {
	sid := "sched-test-last"
	wid := "sched-test-last-wf"
	wt := "sched-test-last-wt"

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: timestamp.DurationPtr(3 * time.Second)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.taskQueue},
				},
			},
		},
	}
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  s.namespace,
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

	_, err := s.engine.CreateSchedule(NewContext(), req)
	s.NoError(err)
	s.Eventually(func() bool { return atomic.LoadInt32(&testComplete) == 1 }, 15*time.Second, 200*time.Millisecond)

	// cleanup
	_, err = s.engine.DeleteSchedule(NewContext(), &workflowservice.DeleteScheduleRequest{
		Namespace:  s.namespace,
		ScheduleId: sid,
		Identity:   "test",
	})
	s.NoError(err)
}

func (s *scheduleIntegrationSuite) TestRefresh() {
	sid := "sched-test-refresh"
	wid := "sched-test-refresh-wf"
	wt := "sched-test-refresh-wt"

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{
					Interval: timestamp.DurationPtr(30 * time.Second),
					// start within three seconds
					Phase: timestamp.DurationPtr(time.Duration((time.Now().Unix()+3)%30) * time.Second),
				},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:               wid,
					WorkflowType:             &commonpb.WorkflowType{Name: wt},
					TaskQueue:                &taskqueuepb.TaskQueue{Name: s.taskQueue},
					WorkflowExecutionTimeout: timestamp.DurationPtr(3 * time.Second),
				},
			},
		},
	}
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  s.namespace,
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

	_, err := s.engine.CreateSchedule(NewContext(), req)
	s.NoError(err)
	s.Eventually(func() bool { return atomic.LoadInt32(&runs) == 1 }, 6*time.Second, 200*time.Millisecond)

	// workflow has started but is now sleeping. it will timeout in 2 seconds.

	describeResp, err := s.engine.DescribeSchedule(NewContext(), &workflowservice.DescribeScheduleRequest{
		Namespace:  s.namespace,
		ScheduleId: sid,
	})
	s.NoError(err)
	s.EqualValues(1, len(describeResp.Info.RunningWorkflows))

	events1 := s.getHistory(s.namespace, &commonpb.WorkflowExecution{WorkflowId: scheduler.WorkflowIDPrefix + sid})

	time.Sleep(4 * time.Second)
	// now it has timed out, but the scheduler hasn't noticed yet. we can prove it by checking
	// its history.

	events2 := s.getHistory(s.namespace, &commonpb.WorkflowExecution{WorkflowId: scheduler.WorkflowIDPrefix + sid})
	s.Equal(len(events1), len(events2))

	// when we describe we'll force a refresh and see it timed out
	describeResp, err = s.engine.DescribeSchedule(NewContext(), &workflowservice.DescribeScheduleRequest{
		Namespace:  s.namespace,
		ScheduleId: sid,
	})
	s.NoError(err)
	s.EqualValues(0, len(describeResp.Info.RunningWorkflows))

	// scheduler has done some stuff
	events3 := s.getHistory(s.namespace, &commonpb.WorkflowExecution{WorkflowId: scheduler.WorkflowIDPrefix + sid})
	s.Greater(len(events3), len(events2))

	// cleanup
	_, err = s.engine.DeleteSchedule(NewContext(), &workflowservice.DeleteScheduleRequest{
		Namespace:  s.namespace,
		ScheduleId: sid,
		Identity:   "test",
	})
	s.NoError(err)
}

func (s *scheduleIntegrationSuite) TestListBeforeRun() {
	sid := "sched-test-list-before-run"
	wid := "sched-test-list-before-run-wf"
	wt := "sched-test-list-before-run-wt"

	// disable per-ns worker so that the schedule workflow never runs
	dc := s.testCluster.host.dcClient
	dc.OverrideValue(dynamicconfig.WorkerPerNamespaceWorkerCount, 0)
	s.testCluster.host.workerService.RefreshPerNSWorkerManager()
	time.Sleep(2 * time.Second)

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: timestamp.DurationPtr(3 * time.Second)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.taskQueue},
				},
			},
		},
	}
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  s.namespace,
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.New(),
	}

	startTime := time.Now()

	_, err := s.engine.CreateSchedule(NewContext(), req)
	s.NoError(err)

	s.Eventually(func() bool { // wait for visibility
		listResp, err := s.engine.ListSchedules(NewContext(), &workflowservice.ListSchedulesRequest{
			Namespace:       s.namespace,
			MaximumPageSize: 5,
		})
		if err != nil || len(listResp.Schedules) != 1 || listResp.Schedules[0].ScheduleId != sid {
			return false
		}
		s.NoError(err)
		entry := listResp.Schedules[0]
		s.Equal(sid, entry.ScheduleId)
		s.NotNil(entry.Info)
		s.Equal(schedule.Spec, entry.Info.Spec)
		s.Equal(wt, entry.Info.WorkflowType.Name)
		s.False(entry.Info.Paused)
		s.Greater(len(entry.Info.FutureActionTimes), 1)
		s.True(entry.Info.FutureActionTimes[0].After(startTime))
		return true
	}, 10*time.Second, 1*time.Second)

	// cleanup
	_, err = s.engine.DeleteSchedule(NewContext(), &workflowservice.DeleteScheduleRequest{
		Namespace:  s.namespace,
		ScheduleId: sid,
		Identity:   "test",
	})
	s.NoError(err)

	dc.RemoveOverride(dynamicconfig.WorkerPerNamespaceWorkerCount)
	s.testCluster.host.workerService.RefreshPerNSWorkerManager()
	time.Sleep(2 * time.Second)
}

func (s *scheduleIntegrationSuite) TestRateLimit() {
	sid := "sched-test-rate-limit-%d"
	wid := "sched-test-rate-limit-wf-%d"
	wt := "sched-test-rate-limit-wt"

	// Set 1/sec rate limit per namespace. To force this to take effect immediately (instead of
	// waiting one minute) we have to cause the whole worker to be stopped and started. The
	// sleeps are needed because the refresh is asynchronous, and there's no way to get access
	// to the actual rate limiter object to refresh it directly.
	s.testCluster.host.dcClient.OverrideValue(dynamicconfig.SchedulerNamespaceStartWorkflowRPS, 1.0)
	s.testCluster.host.dcClient.OverrideValue(dynamicconfig.WorkerPerNamespaceWorkerCount, 0)
	s.testCluster.host.workerService.RefreshPerNSWorkerManager()
	time.Sleep(2 * time.Second)
	s.testCluster.host.dcClient.RemoveOverride(dynamicconfig.WorkerPerNamespaceWorkerCount)
	s.testCluster.host.workerService.RefreshPerNSWorkerManager()
	time.Sleep(2 * time.Second)

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
					{Interval: timestamp.DurationPtr(1 * time.Second)},
				},
			},
			Action: &schedulepb.ScheduleAction{
				Action: &schedulepb.ScheduleAction_StartWorkflow{
					StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
						WorkflowId:   fmt.Sprintf(wid, i),
						WorkflowType: &commonpb.WorkflowType{Name: wt},
						TaskQueue:    &taskqueuepb.TaskQueue{Name: s.taskQueue},
					},
				},
			},
		}
		_, err := s.engine.CreateSchedule(NewContext(), &workflowservice.CreateScheduleRequest{
			Namespace:  s.namespace,
			ScheduleId: fmt.Sprintf(sid, i),
			Schedule:   schedule,
			Identity:   "test",
			RequestId:  uuid.New(),
		})
		s.NoError(err)
	}

	time.Sleep(5 * time.Second)

	// With no rate limit, we'd see 10/second == 50 workflows run. With a limit of 1/sec, we
	// expect to see around 5.
	s.Less(atomic.LoadInt32(&runs), int32(10))

	// clean up
	for i := 0; i < 10; i++ {
		_, err := s.engine.DeleteSchedule(NewContext(), &workflowservice.DeleteScheduleRequest{
			Namespace:  s.namespace,
			ScheduleId: fmt.Sprintf(sid, i),
			Identity:   "test",
		})
		s.NoError(err)
	}

	s.testCluster.host.dcClient.RemoveOverride(dynamicconfig.SchedulerNamespaceStartWorkflowRPS)
	s.testCluster.host.dcClient.OverrideValue(dynamicconfig.WorkerPerNamespaceWorkerCount, 0)
	s.testCluster.host.workerService.RefreshPerNSWorkerManager()
	time.Sleep(2 * time.Second)
	s.testCluster.host.dcClient.RemoveOverride(dynamicconfig.WorkerPerNamespaceWorkerCount)
	s.testCluster.host.workerService.RefreshPerNSWorkerManager()
}
