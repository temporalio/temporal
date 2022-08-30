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
	"time"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/common/payload"
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
func (s *clientIntegrationSuite) TestScheduleBase() {
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

	_, err := s.engine.CreateSchedule(NewContext(), req)
	s.NoError(err)

	time.Sleep(7 * time.Second)
	// should be two or three runs so far
	s.GreaterOrEqual(runs, 2)
	s.LessOrEqual(runs, 3)

	_, err = s.engine.DeleteSchedule(NewContext(), &workflowservice.DeleteScheduleRequest{
		Namespace:  s.namespace,
		ScheduleId: sid,
		Identity:   "test",
	})
	s.NoError(err)
}
