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
)

/*
stuff to test:

create schedule,
	check that it starts a workflow,
	check that it starts _another_ workflow (can check status w/frontend),
	describe it,
	list it,
	update it,
		check that it starts a workflow again,
		describe it again,
		list it again,
	pause
		check pause in describe
		check pause search attr
	terminate it

create schedule with bad schedule spec, check for eager error

use some dataconverter for input, check that it starts okay

check last completion result and last error

check: what happens to a lp watcher on worker restart?
	get it in a state where it's waiting for a wf to exit, say with bufferone
	restart the worker
	terminate the wf
	check that new one starts immediately

search attribute mapping:
	create, update, search

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
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  s.namespace,
		ScheduleId: sid,
		Schedule: &schedulepb.Schedule{
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
							Fields: map[string]*commonpb.Payload{
								"wfmemo1": payload.EncodeString("workflow memo"),
							},
						},
						SearchAttributes: &commonpb.SearchAttributes{
							IndexedFields: map[string]*commonpb.Payload{
								"CustomKeywordField": payload.EncodeString("workflow value"),
							},
						},
					},
				},
			},
		},
		Identity:  "test",
		RequestId: uuid.New(),
		Memo: &commonpb.Memo{
			Fields: map[string]*commonpb.Payload{
				"schedmemo1": payload.EncodeString("schedule memo"),
			},
		},
		SearchAttributes: &commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{
				"CustomKeywordField": payload.EncodeString("schedule value"),
			},
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

	// create

	createTime := time.Now()
	_, err := s.engine.CreateSchedule(NewContext(), req)
	s.NoError(err)

	// sleep until we see two runs
	s.Eventually(func() bool { return runs == 2 }, 8*time.Second, 200*time.Millisecond)

	// describe

	describeResp, err := s.engine.DescribeSchedule(NewContext(), &workflowservice.DescribeScheduleRequest{
		Namespace:  s.namespace,
		ScheduleId: sid,
	})
	s.NoError(err)

	s.Equal(req.Schedule.Spec, describeResp.Schedule.Spec)
	s.Equal(enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, describeResp.Schedule.Policies.OverlapPolicy) // set to default value

	s.Equal(req.SearchAttributes.IndexedFields["CustomKeywordField"].Data, describeResp.SearchAttributes.IndexedFields["CustomKeywordField"].Data)
	s.Equal(req.Memo.Fields["schedmemo1"].Data, describeResp.Memo.Fields["schedmemo1"].Data)

	s.DurationNear(describeResp.Info.CreateTime.Sub(createTime), 0, 500*time.Millisecond)
	s.EqualValues(2, describeResp.Info.ActionCount)
	s.EqualValues(0, describeResp.Info.MissedCatchupWindow)
	s.EqualValues(0, describeResp.Info.OverlapSkipped)
	s.LessOrEqual(len(describeResp.Info.RunningWorkflows), 1) // should be short-lived but we might catch one if we get unlucky
	s.EqualValues(2, len(describeResp.Info.RecentActions))

	// list

	s.Eventually(func() bool {
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
		s.Equalf(req.SearchAttributes.IndexedFields["CustomKeywordField"].Data, entry.SearchAttributes.GetIndexedFields()["CustomKeywordField"].GetData(),
			"missing search attributes, value is %#v", entry.SearchAttributes)
		s.Equal(req.Memo.Fields["schedmemo1"].Data, entry.Memo.Fields["schedmemo1"].Data)
		s.Equal(req.Schedule.Spec, entry.Info.Spec)
		s.Equal(wt, entry.Info.WorkflowType.Name)
		s.False(entry.Info.Paused)
		s.Equal(describeResp.Info.RecentActions, entry.Info.RecentActions) // 2-3 is below the limit where list entry might be cut off
		return true
	}, 3*time.Second, 200*time.Millisecond)

	// update

	_, err = s.engine.DeleteSchedule(NewContext(), &workflowservice.DeleteScheduleRequest{
		Namespace:  s.namespace,
		ScheduleId: sid,
		Identity:   "test",
	})
	s.NoError(err)
}
