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

package host

import (
	"flag"
	"fmt"
	"strings"
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
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payload"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/worker/scheduler"
)

/*
stuff to test:

create schedule
	pause
		check pause in describe
		check pause search attr

checkvarious validation errors

use some dataconverter for input, check that it starts okay

check last completion result and last error

check: what happens to a lp watcher on worker restart?
	get it in a state where it's waiting for a wf to exit, say with bufferone
	restart the worker
	terminate the wf
	check that new one starts immediately

describe updates running workflows:
	@every 30s
	workflow has 2s timeout
	let it start
	describe and see it running
	let it time out
	describe again and see it stopped

*/

type (
	scheduleIntegrationSuite struct {
		*require.Assertions
		IntegrationBase
		hostPort  string
		sdkClient sdkclient.Client
		worker    worker.Worker
		esClient  esclient.IntegrationTestsClient
		taskQueue string
	}
)

func TestScheduleIntegrationSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(scheduleIntegrationSuite))
}

func (s *scheduleIntegrationSuite) SetupSuite() {
	s.setupSuite("testdata/schedule_integration_test_cluster.yaml")
	s.hostPort = "127.0.0.1:7134"
	if TestFlags.FrontendAddr != "" {
		s.hostPort = TestFlags.FrontendAddr
	}
	// ES setup so we can test search attributes
	s.esClient = CreateESClient(&s.Suite, s.testClusterConfig.ESConfig, s.Logger)
	PutIndexTemplate(&s.Suite, s.esClient, fmt.Sprintf("testdata/es_%s_index_template.json", s.testClusterConfig.ESConfig.Version), "test-visibility-template")
	CreateIndex(&s.Suite, s.esClient, s.testClusterConfig.ESConfig.GetVisibilityIndex())
}

func (s *scheduleIntegrationSuite) TearDownSuite() {
	s.tearDownSuite()
	DeleteIndex(&s.Suite, s.esClient, s.testClusterConfig.ESConfig.GetVisibilityIndex())
}

func (s *scheduleIntegrationSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.hostPort,
		Namespace: s.namespace,
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

func (s *scheduleIntegrationSuite) TestScheduleBase() {
	sid := "sched-test-base"
	wid := "sched-test-base-wf"
	wt := "sched-test-base-wt"
	wt2 := "sched-test-base-wt2"

	// switch this to test with search attribute mapper
	// csa := "AliasForCustomKeywordField"
	csa := "CustomKeywordField"

	wfMemo := payload.EncodeString("workflow memo")
	wfSAValue := payload.EncodeString("workflow sa value")
	schMemo := payload.EncodeString("schedule memo")
	schSAValue := payload.EncodeString("schedule sa value")

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

	runs := 0
	workflowFn := func(ctx workflow.Context) error {
		workflow.SideEffect(ctx, func(ctx workflow.Context) any {
			runs++
			return nil
		})
		return nil
	}
	s.worker.RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})
	runs2 := 0
	workflow2Fn := func(ctx workflow.Context) error {
		workflow.SideEffect(ctx, func(ctx workflow.Context) any {
			runs2++
			return nil
		})
		return nil
	}
	s.worker.RegisterWorkflowWithOptions(workflow2Fn, workflow.RegisterOptions{Name: wt2})

	// create

	createTime := time.Now()
	_, err := s.engine.CreateSchedule(NewContext(), req)
	s.NoError(err)

	// sleep until we see two runs, plus a bit more
	s.Eventually(func() bool { return runs == 2 }, 8*time.Second, 200*time.Millisecond)
	time.Sleep(100 * time.Millisecond)

	// describe

	describeResp, err := s.engine.DescribeSchedule(NewContext(), &workflowservice.DescribeScheduleRequest{
		Namespace:  s.namespace,
		ScheduleId: sid,
	})
	s.NoError(err)

	s.Equal(schedule.Spec, describeResp.Schedule.Spec)
	s.Equal(enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, describeResp.Schedule.Policies.OverlapPolicy) // set to default value

	s.Equal(schSAValue.Data, describeResp.SearchAttributes.IndexedFields[csa].Data)
	s.Equal(schMemo.Data, describeResp.Memo.Fields["schedmemo1"].Data)
	s.Equal(wfSAValue.Data, describeResp.Schedule.Action.GetStartWorkflow().SearchAttributes.IndexedFields[csa].Data)
	s.Equal(wfMemo.Data, describeResp.Schedule.Action.GetStartWorkflow().Memo.Fields["wfmemo1"].Data)

	s.DurationNear(describeResp.Info.CreateTime.Sub(createTime), 0, 500*time.Millisecond)
	s.EqualValues(2, describeResp.Info.ActionCount)
	s.EqualValues(0, describeResp.Info.MissedCatchupWindow)
	s.EqualValues(0, describeResp.Info.OverlapSkipped)
	s.EqualValues(0, len(describeResp.Info.RunningWorkflows))
	s.EqualValues(2, len(describeResp.Info.RecentActions))
	action0 := describeResp.Info.RecentActions[0]
	s.WithinRange(*action0.ScheduleTime, createTime, time.Now())
	s.True(action0.ScheduleTime.UnixNano()%int64(3*time.Second) == 0)
	s.DurationNear(action0.ActualTime.Sub(*action0.ScheduleTime), 0, 100*time.Millisecond)

	// list

	s.Eventually(func() bool { // wait for visibility
		listResp, err := s.engine.ListSchedules(NewContext(), &workflowservice.ListSchedulesRequest{
			Namespace:       s.namespace,
			MaximumPageSize: 5,
		})
		if err != nil || len(listResp.Schedules) != 1 || len(listResp.Schedules[0].Info.RecentActions) < 2 {
			return false
		}
		s.NoError(err)
		entry := listResp.Schedules[0]
		s.Equal(sid, entry.ScheduleId)
		s.Equal(schSAValue.Data, entry.SearchAttributes.IndexedFields[csa].Data)
		s.Equal(schMemo.Data, entry.Memo.Fields["schedmemo1"].Data)
		s.Equal(schedule.Spec, entry.Info.Spec)
		s.Equal(wt, entry.Info.WorkflowType.Name)
		s.False(entry.Info.Paused)
		s.Equal(describeResp.Info.RecentActions, entry.Info.RecentActions) // 2 is below the limit where list entry might be cut off
		return true
	}, 3*time.Second, 200*time.Millisecond)

	// list workflows

	wfResp, err := s.engine.ListWorkflowExecutions(NewContext(), &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.namespace,
		PageSize:  5,
		Query:     "",
	})
	s.NoError(err)
	s.EqualValues(2, len(wfResp.Executions), "should see only two completed workflows, _not_ the schedule itself")
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
	s.True(ex0StartTime.UnixNano()%int64(3*time.Second) == 0)

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
	s.Eventually(func() bool { return runs2 == 1 }, 4*time.Second, 200*time.Millisecond)
	time.Sleep(100 * time.Millisecond)

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

	s.DurationNear(describeResp.Info.UpdateTime.Sub(updateTime), 0, 500*time.Millisecond)
	s.EqualValues(3, len(describeResp.Info.RecentActions))
	action2 := describeResp.Info.RecentActions[2]
	s.True(action2.ScheduleTime.UnixNano()%int64(3*time.Second) == 1000000000, action2.ScheduleTime.UnixNano())

	// pause (TODO)

	// finally delete

	_, err = s.engine.DeleteSchedule(NewContext(), &workflowservice.DeleteScheduleRequest{
		Namespace:  s.namespace,
		ScheduleId: sid,
		Identity:   "test",
	})
	s.NoError(err)
}
