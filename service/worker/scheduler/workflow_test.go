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

package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	schedpb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	schedspb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
)

type (
	workflowSuite struct {
		suite.Suite
		testsuite.WorkflowTestSuite
		env *testsuite.TestWorkflowEnvironment
	}
)

func TestWorkflow(t *testing.T) {
	suite.Run(t, new(workflowSuite))
}

func (s *workflowSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
}

func (s *workflowSuite) AfterTest(suiteName, testName string) {
	s.env.AssertExpectations(s.T())
}

func (s *workflowSuite) run(sched *schedpb.Schedule, iterations int) {
	// test workflows will run until "completion", in our case that means until
	// continue-as-new. we only need a small number of iterations to test, though. note the
	// off-by-one error in the workflow, so we get one more iterations than this value.
	currentTweakablePolicies.IterationsBeforeContinueAsNew = iterations - 1

	// fixed start time
	s.env.SetStartTime(time.Date(2022, 6, 1, 0, 0, 0, 0, time.UTC))

	// fill this in so callers don't need to
	sched.Action = &schedpb.ScheduleAction{
		Action: &schedpb.ScheduleAction_StartWorkflow{
			StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
				WorkflowId:   "myid",
				WorkflowType: &commonpb.WorkflowType{Name: "mywf"},
				TaskQueue:    &taskqueuepb.TaskQueue{Name: "mytq"},
				Memo: &commonpb.Memo{
					Fields: map[string]*commonpb.Payload{
						"mymemo": payload.EncodeString("value"),
					},
				},
				SearchAttributes: &commonpb.SearchAttributes{
					IndexedFields: map[string]*commonpb.Payload{
						"myfield": payload.EncodeString("value"),
					},
				},
			},
		},
	}

	s.env.ExecuteWorkflow(SchedulerWorkflow, &schedspb.StartScheduleArgs{
		Schedule: sched,
		State: &schedspb.InternalState{
			Namespace:     "myns",
			NamespaceId:   "mynsid",
			ScheduleId:    "myschedule",
			ConflictToken: InitialConflictToken,
		},
	})
}

func (s *workflowSuite) expectStart(f func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error)) {
	s.env.OnActivity(new(activities).StartWorkflow, mock.Anything, mock.Anything).Return(
		func(_ context.Context, req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
			return f(req)
		})
}

func (s *workflowSuite) TestStart() {
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.Equal("mynsid", req.NamespaceId)
		s.True(time.Date(2022, 6, 1, 0, 15, 0, 0, time.UTC).Equal(timestamp.TimeValue(req.StartTime)))
		s.Nil(req.LastCompletionResult)
		s.Nil(req.ContinuedFailure)
		s.Equal("myns", req.Request.Namespace)
		s.Equal("myid-2022-06-01T00:15:00Z", req.Request.WorkflowId)
		s.Equal("mywf", req.Request.WorkflowType.Name)
		s.Equal("mytq", req.Request.TaskQueue.Name)
		s.Equal(`"value"`, payload.ToString(req.Request.Memo.Fields["mymemo"]))
		s.Equal(`"value"`, payload.ToString(req.Request.SearchAttributes.IndexedFields["myfield"]))
		s.Equal(`"myschedule"`, payload.ToString(req.Request.SearchAttributes.IndexedFields[searchattribute.TemporalScheduledById]))
		s.Equal(`"2022-06-01T00:15:00Z"`, payload.ToString(req.Request.SearchAttributes.IndexedFields[searchattribute.TemporalScheduledStartTime]))

		return &schedspb.StartWorkflowResponse{
			RunId:         uuid.NewString(),
			RealStartTime: timestamp.TimePtr(time.Now()),
		}, nil
	})

	s.run(&schedpb.Schedule{
		Spec: &schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{{
				Interval: timestamp.DurationPtr(55 * time.Minute),
			}},
		},
	}, 2)
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

/*

initial patch (trigger immediate)

time range stuff, e.g. what if we sleep for 55 min but only get woken up after 200?

early wakeup (by signal). check signal handled and also no other workflows run

schedule compile error

trigger immediately

backfill

pause with message

unpause with message

catchup window

remaining actions

run another while first is still running, with:
	overlap skip
	overlap buffer one
	overlap buffer all
	overlap cancel other
	overlap terminate other
	overlap allow

completed workflow is not in info anymore

pause on failure

last completion result

continuedfailure

update config
	rejected update due to conflict token

refresh (maybe let this be done by integration test)

failed StartWorkflow

ensure watcher is started when necessary? or just test result? can't really, need mock results

test that weird logic used to set scheduletoclosetimeout in startworkflow?

test stuff across c-a-n
	have a workflow running already and check that we can start a new one


activities:

test long poll watcher more

*/
