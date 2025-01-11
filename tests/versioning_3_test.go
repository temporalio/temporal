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
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	querypb "go.temporal.io/api/query/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	tqTypeWf        = enumspb.TASK_QUEUE_TYPE_WORKFLOW
	tqTypeAct       = enumspb.TASK_QUEUE_TYPE_ACTIVITY
	tqTypeNexus     = enumspb.TASK_QUEUE_TYPE_NEXUS
	vbUnspecified   = enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED
	vbPinned        = enumspb.VERSIONING_BEHAVIOR_PINNED
	vbUnpinned      = enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE
	ver3MinPollTime = common.MinLongPollTimeout + time.Millisecond*200
)

type Versioning3Suite struct {
	// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
	// not merely log an error
	testcore.FunctionalTestBase
}

func TestVersioning3FunctionalSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Versioning3Suite))
}

func (s *Versioning3Suite) SetupSuite() {
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		dynamicconfig.EnableDeployments.Key():                          true,
		dynamicconfig.FrontendEnableWorkerVersioningWorkflowAPIs.Key(): true,
		dynamicconfig.MatchingForwarderMaxChildrenPerNode.Key():        partitionTreeDegree,

		// Make sure we don't hit the rate limiter in tests
		dynamicconfig.FrontendGlobalNamespaceNamespaceReplicationInducingAPIsRPS.Key():                1000,
		dynamicconfig.FrontendMaxNamespaceNamespaceReplicationInducingAPIsBurstRatioPerInstance.Key(): 1,
		dynamicconfig.FrontendNamespaceReplicationInducingAPIsRPS.Key():                               1000,

		// this is overridden for tests using RunTestWithMatchingBehavior
		dynamicconfig.MatchingNumTaskqueueReadPartitions.Key():  4,
		dynamicconfig.MatchingNumTaskqueueWritePartitions.Key(): 4,
	}
	s.SetDynamicConfigOverrides(dynamicConfigOverrides)
	s.FunctionalTestBase.SetupSuite("testdata/es_cluster.yaml")
}

func (s *Versioning3Suite) TearDownSuite() {
	s.FunctionalTestBase.TearDownSuite()
}

func (s *Versioning3Suite) SetupTest() {
	s.FunctionalTestBase.SetupTest()
}

func (s *Versioning3Suite) TestPinnedTask_NoProperPoller() {
	s.RunTestWithMatchingBehavior(
		func() {
			tv := testvars.New(s)

			tv2 := tv.WithBuildIDNumber(2)
			go s.idlePollWorkflow(tv2, true, ver3MinPollTime, "second deployment should not receive pinned task")

			s.startWorkflow(tv, makePinnedOverride(tv.Deployment()))
			s.idlePollWorkflow(tv, false, ver3MinPollTime, "unversioned worker should not receive pinned task")

			// Sleeping to let the pollers arrive to server before ending the test.
			time.Sleep(200 * time.Millisecond) //nolint:forbidigo
		})
}

func (s *Versioning3Suite) TestUnpinnedTask_NonCurrentDeployment() {
	s.RunTestWithMatchingBehavior(
		func() {
			tv := testvars.New(s)
			go s.idlePollWorkflow(tv, true, ver3MinPollTime, "non-current versioned poller should not receive unpinned task")

			s.startWorkflow(tv, nil)

			// Sleeping to let the pollers arrive to server before ending the test.
			time.Sleep(200 * time.Millisecond) //nolint:forbidigo
		})
}

func (s *Versioning3Suite) TestUnpinnedTask_OldDeployment() {
	s.RunTestWithMatchingBehavior(
		func() {
			tv := testvars.New(s)
			tvOldDeployment := tv.WithBuildIDNumber(1)
			tvNewDeployment := tv.WithBuildIDNumber(2)
			// previous current deployment
			s.updateTaskQueueDeploymentData(tvOldDeployment, time.Minute, tqTypeWf)
			// current deployment
			s.updateTaskQueueDeploymentData(tvNewDeployment, 0, tqTypeWf)

			s.startWorkflow(tv, nil)

			s.idlePollWorkflow(
				tvOldDeployment,
				true,
				ver3MinPollTime,
				"old deployment should not receive unpinned task",
			)
			// Sleeping to let the pollers arrive to server before ending the test.
			time.Sleep(200 * time.Millisecond) //nolint:forbidigo
		},
	)
}

func (s *Versioning3Suite) TestWorkflowWithPinnedOverride_Sticky() {
	s.RunTestWithMatchingBehavior(
		func() {
			s.testWorkflowWithPinnedOverride(true)
		},
	)
}

func (s *Versioning3Suite) TestWorkflowWithPinnedOverride_NoSticky() {
	s.RunTestWithMatchingBehavior(
		func() {
			s.testWorkflowWithPinnedOverride(false)
		},
	)
}

func (s *Versioning3Suite) testWorkflowWithPinnedOverride(sticky bool) {
	tv := testvars.New(s)

	if sticky {
		s.warmUpSticky(tv)
	}

	wftCompleted := make(chan interface{})
	s.pollWftAndHandle(tv, false, wftCompleted,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondWftWithActivities(tv, tv, sticky, vbUnpinned, "5"), nil
		})

	actCompleted := make(chan interface{})
	s.pollActivityAndHandle(tv, actCompleted,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondActivity(), nil
		})

	override := makePinnedOverride(tv.Deployment())
	we := s.startWorkflow(tv, override)

	<-wftCompleted
	s.verifyWorkflowVersioning(tv, vbUnpinned, tv.Deployment(), override, nil)
	if sticky {
		s.verifyWorkflowStickyQueue(we, tv.StickyTaskQueue())
	}

	<-actCompleted
	s.verifyWorkflowVersioning(tv, vbUnpinned, tv.Deployment(), override, nil)

	s.pollWftAndHandle(tv, sticky, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondCompleteWorkflow(tv, vbUnpinned), nil
		})
	s.verifyWorkflowVersioning(tv, vbUnpinned, tv.Deployment(), override, nil)
}

func (s *Versioning3Suite) TestQueryWithPinnedOverride_NoSticky() {
	s.RunTestWithMatchingBehavior(
		func() {
			s.testQueryWithPinnedOverride(false)
		},
	)
}

func (s *Versioning3Suite) TestQueryWithPinnedOverride_Sticky() {
	s.RunTestWithMatchingBehavior(
		func() {
			s.testQueryWithPinnedOverride(true)
		},
	)
}

func (s *Versioning3Suite) testQueryWithPinnedOverride(sticky bool) {
	tv := testvars.New(s)

	if sticky {
		s.warmUpSticky(tv)
	}

	wftCompleted := make(chan interface{})
	s.pollWftAndHandle(tv, false, wftCompleted,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondEmptyWft(tv, sticky, vbUnpinned), nil
		})

	override := makePinnedOverride(tv.Deployment())
	we := s.startWorkflow(tv, override)

	<-wftCompleted
	s.verifyWorkflowVersioning(tv, vbUnpinned, tv.Deployment(), override, nil)
	if sticky {
		s.verifyWorkflowStickyQueue(we, tv.StickyTaskQueue())
	}

	s.pollAndQueryWorkflow(tv, sticky)
}

func (s *Versioning3Suite) TestUnpinnedQuery_NoSticky() {
	s.RunTestWithMatchingBehavior(
		func() {
			s.testUnpinnedQuery(false)
		},
	)
}

func (s *Versioning3Suite) TestUnpinnedQuery_Sticky() {
	s.RunTestWithMatchingBehavior(
		func() {
			s.testUnpinnedQuery(true)
		},
	)
}

func (s *Versioning3Suite) testUnpinnedQuery(sticky bool) {
	tv := testvars.New(s)
	tv2 := tv.WithBuildIDNumber(2)
	d := tv.Deployment()
	if sticky {
		s.warmUpSticky(tv)
	}

	wftCompleted := make(chan interface{})
	s.pollWftAndHandle(tv, false, wftCompleted,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.verifyWorkflowVersioning(tv, vbUnspecified, nil, nil, transitionTo(d))
			return respondEmptyWft(tv, sticky, vbUnpinned), nil
		})

	s.setCurrentDeployment(d)
	s.waitForDeploymentDataPropagation(tv, tqTypeWf)

	we := s.startWorkflow(tv, nil)

	<-wftCompleted
	s.verifyWorkflowVersioning(tv, vbUnpinned, d, nil, nil)
	if sticky {
		s.verifyWorkflowStickyQueue(we, tv.StickyTaskQueue())
	}

	pollerDone := make(chan interface{})
	go func() {
		s.idlePollWorkflow(tv2, true, ver3MinPollTime, "new deployment should not receive query")
		close(pollerDone)
	}()
	s.pollAndQueryWorkflow(tv, sticky)
	<-pollerDone // wait for the idle poller to complete to not interfere with the next poller

	// redirect query to new deployment
	s.updateTaskQueueDeploymentData(tv2, 0, tqTypeWf, tqTypeAct)

	go s.idlePollWorkflow(tv, true, ver3MinPollTime, "old deployment should not receive query")
	// Since the current deployment has changed, task will move to the normal queue (thus, sticky=false)
	s.pollAndQueryWorkflow(tv2, false)

}

func (s *Versioning3Suite) pollAndQueryWorkflow(
	tv *testvars.TestVars,
	sticky bool,
) {
	queryResultCh := make(chan interface{})
	s.pollWftAndHandleQueries(tv, sticky, queryResultCh,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondQueryTaskCompletedRequest, error) {
			return &workflowservice.RespondQueryTaskCompletedRequest{}, nil
		})

	_, err := s.queryWorkflow(tv)
	s.NoError(err)

	<-queryResultCh
}

func (s *Versioning3Suite) TestUnpinnedWorkflow_Sticky() {
	s.RunTestWithMatchingBehavior(
		func() {
			s.testUnpinnedWorkflow(true)
		},
	)
}

func (s *Versioning3Suite) TestUnpinnedWorkflow_NoSticky() {
	s.RunTestWithMatchingBehavior(
		func() {
			s.testUnpinnedWorkflow(false)
		},
	)
}

func (s *Versioning3Suite) testUnpinnedWorkflow(sticky bool) {
	tv := testvars.New(s)
	d := tv.Deployment()

	if sticky {
		s.warmUpSticky(tv)
	}

	wftCompleted := make(chan interface{})
	s.pollWftAndHandle(tv, false, wftCompleted,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.verifyWorkflowVersioning(tv, vbUnspecified, nil, nil, transitionTo(d))
			return respondWftWithActivities(tv, tv, sticky, vbUnpinned, "5"), nil
		})

	actCompleted := make(chan interface{})
	s.pollActivityAndHandle(tv, actCompleted,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondActivity(), nil
		})

	s.setCurrentDeployment(d)
	s.waitForDeploymentDataPropagation(tv, tqTypeWf, tqTypeAct)

	we := s.startWorkflow(tv, nil)

	<-wftCompleted
	s.verifyWorkflowVersioning(tv, vbUnpinned, d, nil, nil)
	if sticky {
		s.verifyWorkflowStickyQueue(we, tv.StickyTaskQueue())
	}

	<-actCompleted
	s.verifyWorkflowVersioning(tv, vbUnpinned, d, nil, nil)

	s.pollWftAndHandle(tv, sticky, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondCompleteWorkflow(tv, vbUnpinned), nil
		})
	s.verifyWorkflowVersioning(tv, vbUnpinned, d, nil, nil)
}

func (s *Versioning3Suite) TestTransitionFromWft_Sticky() {
	s.testTransitionFromWft(true)
}

func (s *Versioning3Suite) TestTransitionFromWft_NoSticky() {
	s.testTransitionFromWft(false)
}

func (s *Versioning3Suite) testTransitionFromWft(sticky bool) {
	// Wf runs one WFT and one AT on d1, then the second WFT is redirected to d2 and
	// transitions the wf with it.

	tv1 := testvars.New(s).WithBuildIDNumber(1)
	tv2 := tv1.WithBuildIDNumber(2)
	d1 := tv1.Deployment()
	d2 := tv2.Deployment()
	if sticky {
		s.warmUpSticky(tv1)
	}

	s.updateTaskQueueDeploymentData(tv1, 0, tqTypeWf, tqTypeAct)
	we := s.startWorkflow(tv1, nil)

	s.pollWftAndHandle(tv1, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.verifyWorkflowVersioning(tv1, vbUnspecified, nil, nil, transitionTo(d1))
			return respondWftWithActivities(tv1, tv1, sticky, vbUnpinned, "5"), nil
		})
	s.verifyWorkflowVersioning(tv1, vbUnpinned, d1, nil, nil)
	if sticky {
		s.verifyWorkflowStickyQueue(we, tv1.StickyTaskQueue())
	}

	s.pollActivityAndHandle(tv1, nil,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondActivity(), nil
		})
	s.verifyWorkflowVersioning(tv1, vbUnpinned, d1, nil, nil)

	// Set B as the current deployment
	s.updateTaskQueueDeploymentData(tv2, 0, tqTypeWf, tqTypeAct)

	s.pollWftAndHandle(tv2, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.verifyWorkflowVersioning(tv1, vbUnpinned, d1, nil, transitionTo(d2))
			return respondCompleteWorkflow(tv2, vbUnpinned), nil
		})
	s.verifyWorkflowVersioning(tv2, vbUnpinned, d2, nil, nil)
}

func (s *Versioning3Suite) TestNexusTask_StaysOnCurrentDeployment() {
	s.RunTestWithMatchingBehavior(
		func() {
			s.nexusTaskStaysOnCurrentDeployment()
		},
	)
}

func (s *Versioning3Suite) nexusTaskStaysOnCurrentDeployment() {
	tv1 := testvars.New(s).WithBuildIDNumber(1)
	tv2 := tv1.WithBuildIDNumber(2)

	nexusRequest := &matchingservice.DispatchNexusTaskRequest{
		NamespaceId: s.NamespaceID().String(),
		TaskQueue:   tv1.TaskQueue(),
		Request: &nexuspb.Request{
			Header: map[string]string{
				// placeholder value as passing in an empty map would result in protoc deserializing
				// it as nil, which breaks existing logic inside of matching
				"request-timeout": "1",
			},
		},
	}

	// current deployment is -> A
	s.updateTaskQueueDeploymentData(tv1, 0, tqTypeNexus)
	s.waitForDeploymentDataPropagation(tv1, tqTypeNexus)

	// local poller with deployment A receives task
	s.pollAndDispatchNexusTask(tv1, nexusRequest)

	// current deployment is now -> B
	s.updateTaskQueueDeploymentData(tv2, 0, tqTypeNexus)
	s.waitForDeploymentDataPropagation(tv2, tqTypeNexus)

	// Pollers of A are there but should not get any task
	go s.idlePollNexus(tv1, true, ver3MinPollTime, "nexus task should not go to the old deployment")

	s.pollAndDispatchNexusTask(tv2, nexusRequest)
}

func (s *Versioning3Suite) pollAndDispatchNexusTask(
	tv *testvars.TestVars,
	nexusRequest *matchingservice.DispatchNexusTaskRequest,
) {
	matchingClient := s.GetTestCluster().MatchingClient()

	nexusCompleted := make(chan interface{})
	s.pollNexusTaskAndHandle(tv, false, nexusCompleted,
		func(task *workflowservice.PollNexusTaskQueueResponse) (*workflowservice.RespondNexusTaskCompletedRequest, error) {
			s.NotNil(task)
			return &workflowservice.RespondNexusTaskCompletedRequest{}, nil // response object gets filled during processing
		})

	_, err := matchingClient.DispatchNexusTask(context.Background(), nexusRequest)
	s.NoError(err)
	<-nexusCompleted
}

func (s *Versioning3Suite) TestEagerActivity() {
	// The first WFT asks for an activity to starts and get it eagerly in the WFT completion
	// response. The activity is processed without issues and wf completes.

	s.OverrideDynamicConfig(dynamicconfig.EnableActivityEagerExecution, true)
	tv := testvars.New(s)
	d := tv.Deployment()

	s.updateTaskQueueDeploymentData(tv, 0, tqTypeWf, tqTypeAct)
	s.startWorkflow(tv, nil)

	poller, resp := s.pollWftAndHandle(tv, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.verifyWorkflowVersioning(tv, vbUnspecified, nil, nil, transitionTo(d))
			resp := respondWftWithActivities(tv, tv, true, vbUnpinned, "5")
			resp.Commands[0].GetScheduleActivityTaskCommandAttributes().RequestEagerExecution = true
			return resp, nil
		})
	s.verifyWorkflowVersioning(tv, vbUnpinned, d, nil, nil)

	s.NotEmpty(resp.GetActivityTasks())

	_, err := poller.HandleActivityTask(tv, resp.GetActivityTasks()[0],
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondActivity(), nil
		})
	s.NoError(err)
	s.verifyWorkflowVersioning(tv, vbUnpinned, d, nil, nil)

	s.pollWftAndHandle(tv, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondCompleteWorkflow(tv, vbUnpinned), nil
		})
	s.verifyWorkflowVersioning(tv, vbUnpinned, d, nil, nil)
}

func (s *Versioning3Suite) TestTransitionFromActivity_Sticky() {
	s.testTransitionFromActivity(true)
}

func (s *Versioning3Suite) TestTransitionFromActivity_NoSticky() {
	s.testTransitionFromActivity(false)
}

func (s *Versioning3Suite) testTransitionFromActivity(sticky bool) {
	// Wf runs one TWF on d1 and schedules four activities, then:
	// 1. The first and second activities starts on d1
	// 2. Current deployment becomes d2
	// 3. The third activity is redirected to d2 and starts a transition in the wf, without being
	//    dispatched.
	// 4. The 4th activity also does not start on any of the builds although there are pending
	//    pollers on both.
	// 5. The transition generates a WFT, and it is started in d2.
	// 6. The 1st act is completed here while the transition is going on.
	// 7. The 2nd act fails and makes another attempt. But it is not dispatched.
	// 8. WFT completes and the transition completes.
	// 9. All the 3 remaining activities are now dispatched and completed.

	tv1 := testvars.New(s).WithBuildIDNumber(1)
	tv2 := tv1.WithBuildIDNumber(2)
	d2 := tv2.Deployment()
	d1 := tv1.Deployment()
	if sticky {
		s.warmUpSticky(tv1)
	}

	s.updateTaskQueueDeploymentData(tv1, 0, tqTypeWf, tqTypeAct)
	we := s.startWorkflow(tv1, nil)

	s.pollWftAndHandle(tv1, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.verifyWorkflowVersioning(tv1, vbUnspecified, nil, nil, transitionTo(d1))
			return respondWftWithActivities(tv1, tv1, sticky, vbUnpinned, "5", "6", "7", "8"), nil
		})
	s.verifyWorkflowVersioning(tv1, vbUnpinned, d1, nil, nil)
	if sticky {
		s.verifyWorkflowStickyQueue(we, tv1.StickyTaskQueue())
	}

	transitionCompleted := atomic.Bool{}
	transitionStarted := make(chan interface{})
	act1Started := make(chan interface{})
	act1Completed := make(chan interface{})
	act2Started := make(chan interface{})
	act2Failed := make(chan interface{})
	act2To4Completed := make(chan interface{})

	// 1. Start 1st and 2nd activities
	s.pollActivityAndHandle(tv1, act1Completed,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			s.NotNil(task)
			s.Logger.Info(fmt.Sprintf("Activity 1 started ID: %s", task.ActivityId))
			close(act1Started)
			// block until the transition WFT starts
			<-transitionStarted
			// 6. the 1st act completes during transition
			s.Logger.Info(fmt.Sprintf("Activity 1 completed ID: %s", task.ActivityId))
			return respondActivity(), nil
		})

	<-act1Started
	s.pollActivityAndHandle(tv1, act2Failed,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			s.NotNil(task)
			s.Logger.Info(fmt.Sprintf("Activity 2 started ID: %s", task.ActivityId))
			close(act2Started)
			// block until the transition WFT starts
			<-transitionStarted
			// 7. 2nd activity fails. Respond with error so it is retried.
			s.Logger.Info(fmt.Sprintf("Activity 2 failed ID: %s", task.ActivityId))
			return nil, errors.New("intentional activity failure")
		})

	<-act2Started
	s.verifyWorkflowVersioning(tv1, vbUnpinned, d1, nil, nil)

	// 2. Set d2 as the current deployment
	s.updateTaskQueueDeploymentData(tv2, 0, tqTypeWf, tqTypeAct)
	// Although updateTaskQueueDeploymentData waits for deployment data to reach the TQs, backlogged
	// tasks might still be waiting behind the old deployment's poll channel. Partition manage should
	// immediately react to the deployment data changes, but there still is a race possible and the
	// only way to safeguard against it is to wait a little while before proceeding.
	time.Sleep(time.Millisecond * 100) //nolint:forbidigo

	// Pollers of d1 are there, but should not get any task
	go s.idlePollActivity(tv1, true, ver3MinPollTime, "activities should not go to the old deployment")

	go func() {
		for i := 2; i <= 4; i++ {
			// 3-4. The new d2 poller should trigger the third activity to be redirected, but the activity should
			// not start until transition completes in the next wft.
			// Repeating the handler so it processes all the three activities
			s.pollActivityAndHandle(tv2, nil,
				func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
					// Activity should not start until the transition is completed
					s.True(transitionCompleted.Load())
					s.NotNil(task)
					s.Logger.Info(fmt.Sprintf("Remaining activity completed ID: %s", task.ActivityId))
					return respondActivity(), nil
				})
		}
		close(act2To4Completed)
	}()

	// 5. The transition should create a new WFT to be sent to d2.
	s.pollWftAndHandle(tv2, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.verifyWorkflowVersioning(tv1, vbUnpinned, d1, nil, transitionTo(d2))
			close(transitionStarted)
			s.Logger.Info("Transition wft started")
			// 8. Complete the transition after act1 completes and act2's first attempt fails.
			<-act1Completed
			<-act2Failed
			transitionCompleted.Store(true)
			s.Logger.Info("Transition wft completed")
			return respondEmptyWft(tv2, sticky, vbUnpinned), nil
		})
	s.verifyWorkflowVersioning(tv2, vbUnpinned, d2, nil, nil)
	if sticky {
		s.verifyWorkflowStickyQueue(tv2.WorkflowExecution(), tv2.StickyTaskQueue())
	}

	// 9. Now all activities should complete.
	<-act2To4Completed
	s.pollWftAndHandle(tv2, sticky, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.Logger.Info("Final wft completed")
			return respondCompleteWorkflow(tv2, vbUnpinned), nil
		})
	s.verifyWorkflowVersioning(tv2, vbUnpinned, d2, nil, nil)
}

func (s *Versioning3Suite) TestIndependentActivity_Pinned() {
	// TODO (shahab): need to handle pinned activities properly so the wf does not get stuck.
	s.T().Skip()
	s.testIndependentActivity(vbPinned)
}

func (s *Versioning3Suite) TestIndependentActivity_Unpinned() {
	s.testIndependentActivity(vbUnpinned)
}

func (s *Versioning3Suite) testIndependentActivity(behavior enumspb.VersioningBehavior) {
	// This test starts a wf on wf-series. The workflow runs an activity that is sent to act-tq with
	// workers on a different deployment series, act-series. We make sure that the activity is
	// dispatched and processed properly without affecting versioning of the workflow. Note that it
	// is not required for independent activities to use a different TQ name but in here we test the
	// more common case where the TQ name is different.

	tvWf := testvars.New(s).WithDeploymentSeriesNumber(1)
	tvAct := testvars.New(s).WithDeploymentSeriesNumber(2).WithTaskQueueNumber(2)
	dWf := tvWf.Deployment()

	// Set current deployment for each TQ
	s.updateTaskQueueDeploymentData(tvWf, 0, tqTypeWf)
	s.updateTaskQueueDeploymentData(tvAct, 0, tqTypeAct)

	s.startWorkflow(tvWf, nil)

	s.pollWftAndHandle(tvWf, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.verifyWorkflowVersioning(tvWf, vbUnspecified, nil, nil, transitionTo(dWf))
			s.Logger.Info("First wf task completed")
			return respondWftWithActivities(tvWf, tvAct, false, behavior, "5"), nil
		})
	s.verifyWorkflowVersioning(tvWf, behavior, dWf, nil, nil)

	s.pollActivityAndHandle(tvAct, nil,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			s.NotNil(task)
			s.Logger.Info("Activity completed")
			return respondActivity(), nil
		})
	s.verifyWorkflowVersioning(tvWf, behavior, dWf, nil, nil)

	s.pollWftAndHandle(tvWf, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondCompleteWorkflow(tvWf, behavior), nil
		})
	s.verifyWorkflowVersioning(tvWf, behavior, dWf, nil, nil)
}

func transitionTo(d *deploymentpb.Deployment) *workflowpb.DeploymentTransition {
	return &workflowpb.DeploymentTransition{
		Deployment: d,
	}
}

func (s *Versioning3Suite) setCurrentDeployment(
	deployment *deploymentpb.Deployment,
) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	_, err := s.FrontendClient().SetCurrentDeployment(ctx,
		&workflowservice.SetCurrentDeploymentRequest{
			Namespace:  s.Namespace().String(),
			Deployment: deployment,
		})
	s.NoError(err)
}

func (s *Versioning3Suite) updateTaskQueueDeploymentData(
	tv *testvars.TestVars,
	timeSinceCurrent time.Duration,
	tqTypes ...enumspb.TaskQueueType,
) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	lastBecameCurrent := time.Now().Add(-timeSinceCurrent)
	for _, t := range tqTypes {
		_, err := s.GetTestCluster().MatchingClient().SyncDeploymentUserData(
			ctx, &matchingservice.SyncDeploymentUserDataRequest{
				NamespaceId:   s.NamespaceID().String(),
				TaskQueue:     tv.TaskQueue().GetName(),
				TaskQueueType: t,
				Deployment:    tv.Deployment(),
				Data: &deploymentspb.TaskQueueData{
					FirstPollerTime:       timestamp.TimePtr(lastBecameCurrent),
					LastBecameCurrentTime: timestamp.TimePtr(lastBecameCurrent),
				},
			},
		)
		s.NoError(err)
	}
	s.waitForDeploymentDataPropagation(tv, tqTypes...)
}

func (s *Versioning3Suite) verifyWorkflowVersioning(
	tv *testvars.TestVars,
	behavior enumspb.VersioningBehavior,
	deployment *deploymentpb.Deployment,
	override *workflowpb.VersioningOverride,
	transition *workflowpb.DeploymentTransition,
) {
	dwf, err := s.FrontendClient().DescribeWorkflowExecution(
		context.Background(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: tv.WorkflowID(),
			},
		},
	)
	s.NoError(err)

	versioningInfo := dwf.WorkflowExecutionInfo.GetVersioningInfo()
	s.Equal(behavior.String(), versioningInfo.GetBehavior().String())
	if !deployment.Equal(versioningInfo.GetDeployment()) {
		s.Fail(fmt.Sprintf("deployment mismatch. expected: {%s}, actual: {%s}",
			deployment,
			versioningInfo.GetDeployment(),
		))
	}

	s.Equal(override.GetBehavior().String(), versioningInfo.GetVersioningOverride().GetBehavior().String())
	if actualOverrideDeployment := versioningInfo.GetVersioningOverride().GetDeployment(); !override.GetDeployment().Equal(actualOverrideDeployment) {
		s.Fail(fmt.Sprintf("deployment override mismatch. expected: {%s}, actual: {%s}",
			override.GetDeployment(),
			actualOverrideDeployment,
		))
	}

	if !versioningInfo.GetDeploymentTransition().Equal(transition) {
		s.Fail(fmt.Sprintf("deployment transition mismatch. expected: {%s}, actual: {%s}",
			transition,
			versioningInfo.GetDeploymentTransition(),
		))
	}
}

func respondActivity() *workflowservice.RespondActivityTaskCompletedRequest {
	return &workflowservice.RespondActivityTaskCompletedRequest{}
}

func respondWftWithActivities(
	tvWf *testvars.TestVars,
	tvAct *testvars.TestVars,
	sticky bool,
	behavior enumspb.VersioningBehavior,
	activityIds ...string,
) *workflowservice.RespondWorkflowTaskCompletedRequest {
	var stickyAttr *taskqueuepb.StickyExecutionAttributes
	if sticky {
		stickyAttr = &taskqueuepb.StickyExecutionAttributes{
			WorkerTaskQueue:        tvWf.StickyTaskQueue(),
			ScheduleToStartTimeout: durationpb.New(5 * time.Second),
		}
	}
	var commands []*commandpb.Command
	for _, a := range activityIds {
		commands = append(commands, &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
				ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:   a,
					ActivityType: &commonpb.ActivityType{Name: "act"},
					TaskQueue:    tvAct.TaskQueue(),
					Input:        payloads.EncodeString("input"),
					// TODO (shahab): tests with forced task forward take multiple seconds. Need to know why?
					ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(1 * time.Second),
					HeartbeatTimeout:       durationpb.New(1 * time.Second),
					RequestEagerExecution:  false,
				},
			},
		})
	}
	return &workflowservice.RespondWorkflowTaskCompletedRequest{
		Commands:                   commands,
		StickyAttributes:           stickyAttr,
		ForceCreateNewWorkflowTask: false,
		Deployment:                 tvWf.Deployment(),
		VersioningBehavior:         behavior,
	}
}

func respondEmptyWft(
	tv *testvars.TestVars,
	sticky bool,
	behavior enumspb.VersioningBehavior,
) *workflowservice.RespondWorkflowTaskCompletedRequest {
	return respondWftWithActivities(tv, tv, sticky, behavior)
}

func respondCompleteWorkflow(
	tv *testvars.TestVars,
	behavior enumspb.VersioningBehavior,
) *workflowservice.RespondWorkflowTaskCompletedRequest {
	return &workflowservice.RespondWorkflowTaskCompletedRequest{
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: payloads.EncodeString("done"),
					},
				},
			},
		},
		ForceCreateNewWorkflowTask: false,
		Deployment:                 tv.Deployment(),
		VersioningBehavior:         behavior,
	}
}

func respondScheduleNexusOperation(
	tv *testvars.TestVars,
	behavior enumspb.VersioningBehavior,
	endpointName string,
) *workflowservice.RespondWorkflowTaskCompletedRequest {
	return &workflowservice.RespondWorkflowTaskCompletedRequest{
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Endpoint:  endpointName,
						Service:   "service",
						Operation: "operation",
						Input:     mustToPayload("input"),
					},
				},
			},
		},
		ForceCreateNewWorkflowTask: false,
		Deployment:                 tv.Deployment(),
		VersioningBehavior:         behavior,
	}
}

func mustToPayload(v any) *commonpb.Payload {
	conv := converter.GetDefaultDataConverter()
	payload, err := conv.ToPayload(v)
	if err != nil {

		return &commonpb.Payload{}
	}
	return payload
}

func (s *Versioning3Suite) startWorkflow(
	tv *testvars.TestVars,
	override *workflowpb.VersioningOverride,
) *commonpb.WorkflowExecution {
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            tv.WorkerIdentity(),
		VersioningOverride:  override,
	}

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)
	return &commonpb.WorkflowExecution{
		WorkflowId: tv.WorkflowID(),
		RunId:      we.GetRunId(),
	}
}

func (s *Versioning3Suite) queryWorkflow(
	tv *testvars.TestVars,
) (*workflowservice.QueryWorkflowResponse, error) {
	request := &workflowservice.QueryWorkflowRequest{
		Namespace: s.Namespace().String(),
		Execution: tv.WorkflowExecution(),
		Query: &querypb.WorkflowQuery{
			QueryType: tv.Any().String(),
		},
	}

	shortCtx, cancel := context.WithTimeout(testcore.NewContext(), common.MinLongPollTimeout)
	defer cancel()
	response, err := s.FrontendClient().QueryWorkflow(shortCtx, request)
	return response, err
}

// Name is used by testvars. We use a shorten test name in variables so that physical task queue IDs
// do not grow larger that DB column limit (currently as low as 272 chars).
func (s *Versioning3Suite) Name() string {
	fullName := s.T().Name()
	if len(fullName) <= 30 {
		return fullName
	}
	return fmt.Sprintf("%s-%08x",
		fullName[len(fullName)-21:],
		farm.Fingerprint32([]byte(fullName)),
	)
}

// pollWftAndHandle can be used in sync and async mode. For async mode pass the async channel. It
// will be closed when the task is handled.
// Returns the poller and poll response only in sync mode (can be used to process new wft in the response)
func (s *Versioning3Suite) pollWftAndHandle(
	tv *testvars.TestVars,
	sticky bool,
	async chan<- interface{},
	handler func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error),
) (*taskpoller.TaskPoller, *workflowservice.RespondWorkflowTaskCompletedResponse) {
	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())
	d := tv.Deployment()
	tq := tv.TaskQueue()
	if sticky {
		tq = tv.StickyTaskQueue()
	}
	f := func() *workflowservice.RespondWorkflowTaskCompletedResponse {
		resp, err := poller.PollWorkflowTask(
			&workflowservice.PollWorkflowTaskQueueRequest{
				WorkerVersionCapabilities: &commonpb.WorkerVersionCapabilities{
					BuildId:              d.BuildId,
					DeploymentSeriesName: d.SeriesName,
					UseVersioning:        true,
				},
				TaskQueue: tq,
			},
		).HandleTask(tv, handler)
		s.NoError(err)
		return resp
	}
	if async == nil {
		return poller, f()
	} else {
		go func() {
			f()
			close(async)
		}()
	}
	return nil, nil
}

func (s *Versioning3Suite) pollWftAndHandleQueries(
	tv *testvars.TestVars,
	sticky bool,
	async chan<- interface{},
	handler func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondQueryTaskCompletedRequest, error),
) (*taskpoller.TaskPoller, *workflowservice.RespondQueryTaskCompletedResponse) {
	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())
	d := tv.Deployment()
	tq := tv.TaskQueue()
	if sticky {
		tq = tv.StickyTaskQueue()
	}
	f := func() *workflowservice.RespondQueryTaskCompletedResponse {
		resp, err := poller.PollWorkflowTask(
			&workflowservice.PollWorkflowTaskQueueRequest{
				WorkerVersionCapabilities: &commonpb.WorkerVersionCapabilities{
					BuildId:              d.BuildId,
					DeploymentSeriesName: d.SeriesName,
					UseVersioning:        true,
				},
				TaskQueue: tq,
			},
		).HandleLegacyQuery(tv, handler)
		s.NoError(err)
		return resp
	}
	if async == nil {
		return poller, f()
	}
	go func() {
		f()
		close(async)
	}()
	return nil, nil
}

func (s *Versioning3Suite) pollNexusTaskAndHandle(
	tv *testvars.TestVars,
	sticky bool,
	async chan<- interface{},
	handler func(task *workflowservice.PollNexusTaskQueueResponse) (*workflowservice.RespondNexusTaskCompletedRequest, error),
) (*taskpoller.TaskPoller, *workflowservice.RespondNexusTaskCompletedResponse) {
	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())
	d := tv.Deployment()
	tq := tv.TaskQueue()
	if sticky {
		tq = tv.StickyTaskQueue()
	}
	f := func() *workflowservice.RespondNexusTaskCompletedResponse {
		resp, err := poller.PollNexusTask(
			&workflowservice.PollNexusTaskQueueRequest{
				WorkerVersionCapabilities: &commonpb.WorkerVersionCapabilities{
					BuildId:              d.BuildId,
					DeploymentSeriesName: d.SeriesName,
					UseVersioning:        true,
				},
				TaskQueue: tq,
			},
		).HandleTask(tv, handler)
		s.NoError(err)
		return resp
	}
	if async == nil {
		return poller, f()
	}
	go func() {
		f()
		close(async)
	}()
	return nil, nil
}

func (s *Versioning3Suite) pollActivityAndHandle(
	tv *testvars.TestVars,
	async chan<- interface{},
	handler func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error),
) {
	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())
	d := tv.Deployment()
	f := func() {
		_, err := poller.PollActivityTask(
			&workflowservice.PollActivityTaskQueueRequest{
				WorkerVersionCapabilities: &commonpb.WorkerVersionCapabilities{
					BuildId:              d.BuildId,
					DeploymentSeriesName: d.SeriesName,
					UseVersioning:        true,
				},
			},
		).HandleTask(tv, handler, taskpoller.WithTimeout(time.Minute))
		s.NoError(err)
	}
	if async == nil {
		f()
	} else {
		go func() {
			f()
			close(async)
		}()
	}
}

func (s *Versioning3Suite) idlePollWorkflow(
	tv *testvars.TestVars,
	versioned bool,
	timeout time.Duration,
	unexpectedTaskMessage string,
) {
	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())
	d := tv.Deployment()
	_, _ = poller.PollWorkflowTask(
		&workflowservice.PollWorkflowTaskQueueRequest{
			WorkerVersionCapabilities: &commonpb.WorkerVersionCapabilities{
				BuildId:              d.BuildId,
				DeploymentSeriesName: d.SeriesName,
				UseVersioning:        versioned,
			},
		},
	).HandleTask(
		tv,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.Fail(unexpectedTaskMessage)
			return nil, nil
		},
		taskpoller.WithTimeout(timeout),
	)
}

func (s *Versioning3Suite) idlePollActivity(
	tv *testvars.TestVars,
	versioned bool,
	timeout time.Duration,
	unexpectedTaskMessage string,
) {
	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())
	d := tv.Deployment()
	_, _ = poller.PollActivityTask(
		&workflowservice.PollActivityTaskQueueRequest{
			WorkerVersionCapabilities: &commonpb.WorkerVersionCapabilities{
				BuildId:              d.BuildId,
				DeploymentSeriesName: d.SeriesName,
				UseVersioning:        versioned,
			},
		},
	).HandleTask(
		tv,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			if task != nil {
				s.Logger.Error(fmt.Sprintf("Unexpected activity task received, ID: %s", task.ActivityId))
				s.Fail(unexpectedTaskMessage)
			}
			return nil, nil
		},
		taskpoller.WithTimeout(timeout),
	)
}

func (s *Versioning3Suite) idlePollNexus(
	tv *testvars.TestVars,
	versioned bool,
	timeout time.Duration,
	unexpectedTaskMessage string,
) {
	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())
	d := tv.Deployment()
	_, _ = poller.PollNexusTask(
		&workflowservice.PollNexusTaskQueueRequest{
			WorkerVersionCapabilities: &commonpb.WorkerVersionCapabilities{
				BuildId:              d.BuildId,
				UseVersioning:        versioned,
				DeploymentSeriesName: d.SeriesName,
			},
		}).HandleTask(
		tv,
		func(task *workflowservice.PollNexusTaskQueueResponse) (*workflowservice.RespondNexusTaskCompletedRequest, error) {
			if task != nil {
				s.Fail(unexpectedTaskMessage)
			}
			return nil, nil
		},
		taskpoller.WithTimeout(timeout),
	)
}

func (s *Versioning3Suite) verifyWorkflowStickyQueue(
	we *commonpb.WorkflowExecution,
	stickyQ *taskqueuepb.TaskQueue,
) {
	ms, err := s.GetTestCluster().HistoryClient().GetMutableState(
		context.Background(), &historyservice.GetMutableStateRequest{
			NamespaceId: s.NamespaceID().String(),
			Execution:   we,
		},
	)
	s.NoError(err)
	s.Equal(stickyQ.GetName(), ms.StickyTaskQueue.GetName())
}

// Sticky queue needs to be created in server before tasks can schedule in it. Call to this method
// create the sticky queue by polling it.
func (s *Versioning3Suite) warmUpSticky(
	tv *testvars.TestVars,
) {
	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())
	_, _ = poller.PollWorkflowTask(
		&workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: tv.StickyTaskQueue(),
		},
	).HandleTask(
		tv,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.Fail("sticky task is not expected")
			return nil, nil
		},
		taskpoller.WithTimeout(ver3MinPollTime),
	)
}

func (s *Versioning3Suite) waitForDeploymentDataPropagation(
	tv *testvars.TestVars,
	tqTypes ...enumspb.TaskQueueType,
) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	v := s.GetTestCluster().Host().DcClient().GetValue(dynamicconfig.MatchingNumTaskqueueReadPartitions.Key())
	s.NotEmpty(v, "versioning tests require setting explicit number of partitions")
	count, ok := v[0].Value.(int)
	s.True(ok, "partition count is not an int")
	partitionCount := count

	type partAndType struct {
		part int
		tp   enumspb.TaskQueueType
	}
	remaining := make(map[partAndType]struct{})
	for i := 0; i < partitionCount; i++ {
		for _, tqt := range tqTypes {
			remaining[partAndType{i, tqt}] = struct{}{}
		}
	}
	f, err := tqid.NewTaskQueueFamily(s.NamespaceID().String(), tv.TaskQueue().GetName())
	deployment := tv.Deployment()
	s.Eventually(func() bool {
		for pt := range remaining {
			s.NoError(err)
			partition := f.TaskQueue(pt.tp).NormalPartition(pt.part)
			// Use lower-level GetTaskQueueUserData instead of GetWorkerBuildIdCompatibility
			// here so that we can target activity queues.
			res, err := s.GetTestCluster().MatchingClient().GetTaskQueueUserData(
				ctx,
				&matchingservice.GetTaskQueueUserDataRequest{
					NamespaceId:   s.NamespaceID().String(),
					TaskQueue:     partition.RpcName(),
					TaskQueueType: partition.TaskType(),
				})
			s.NoError(err)
			perTypes := res.GetUserData().GetData().GetPerType()
			if perTypes != nil {
				deps := perTypes[int32(pt.tp)].GetDeploymentData().GetDeployments()
				for _, d := range deps {
					if d.GetDeployment().Equal(deployment) {
						delete(remaining, pt)
					}
				}
			}
		}
		return len(remaining) == 0
	}, 10*time.Second, 100*time.Millisecond)
}

func makePinnedOverride(d *deploymentpb.Deployment) *workflowpb.VersioningOverride {
	return &workflowpb.VersioningOverride{Behavior: vbPinned, Deployment: d}
}
