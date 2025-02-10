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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	testcore.FunctionalTestSuite
}

func TestVersioning3FunctionalSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Versioning3Suite))
}

func (s *Versioning3Suite) SetupSuite() {
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		dynamicconfig.EnableDeployments.Key():                          true,
		dynamicconfig.EnableDeploymentVersions.Key():                   true,
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
	s.FunctionalTestBase.SetupSuiteWithDefaultCluster(testcore.WithDynamicConfigOverrides(dynamicConfigOverrides))
}

func (s *Versioning3Suite) TestPinnedTask_NoProperPoller() {
	s.RunTestWithMatchingBehavior(
		func() {
			tv := testvars.New(s)

			tv2 := tv.WithBuildIDNumber(2)
			go s.idlePollWorkflow(tv2, true, ver3MinPollTime, "second deployment should not receive pinned task")

			s.startWorkflow(tv, tv.VersioningOverridePinned())
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
			s.updateTaskQueueDeploymentData(tvOldDeployment, true, 0, false, time.Minute, tqTypeWf)
			// current deployment
			s.updateTaskQueueDeploymentData(tvNewDeployment, true, 0, false, 0, tqTypeWf)

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

	wftCompleted := make(chan any)
	s.pollWftAndHandle(tv, false, wftCompleted,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.verifyVersioningSAs(tv, vbPinned)
			return respondWftWithActivities(tv, tv, sticky, vbUnpinned, "5"), nil
		})

	actCompleted := make(chan any)
	s.pollActivityAndHandle(tv, actCompleted,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondActivity(), nil
		})

	runID := s.startWorkflow(tv, tv.VersioningOverridePinned())

	<-wftCompleted
	s.verifyWorkflowVersioning(tv, vbUnpinned, tv.Deployment(), tv.VersioningOverridePinned(), nil)
	s.verifyVersioningSAs(tv, vbPinned, tv)
	if sticky {
		s.verifyWorkflowStickyQueue(tv.WithRunID(runID))
	}

	<-actCompleted
	s.verifyWorkflowVersioning(tv, vbUnpinned, tv.Deployment(), tv.VersioningOverridePinned(), nil)

	s.pollWftAndHandle(tv, sticky, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondCompleteWorkflow(tv, vbUnpinned), nil
		})
	s.verifyWorkflowVersioning(tv, vbUnpinned, tv.Deployment(), tv.VersioningOverridePinned(), nil)
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

	wftCompleted := make(chan any)
	s.pollWftAndHandle(tv, false, wftCompleted,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondEmptyWft(tv, sticky, vbUnpinned), nil
		})

	runID := s.startWorkflow(tv, tv.VersioningOverridePinned())

	<-wftCompleted
	s.verifyWorkflowVersioning(tv, vbUnpinned, tv.Deployment(), tv.VersioningOverridePinned(), nil)
	if sticky {
		s.verifyWorkflowStickyQueue(tv.WithRunID(runID))
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
	if sticky {
		s.warmUpSticky(tv)
	}

	wftCompleted := make(chan any)
	s.pollWftAndHandle(tv, false, wftCompleted,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.verifyWorkflowVersioning(tv, vbUnspecified, nil, nil, tv.DeploymentVersionTransition())
			return respondEmptyWft(tv, sticky, vbUnpinned), nil
		})

	s.setCurrentDeployment(tv)

	runID := s.startWorkflow(tv, nil)

	<-wftCompleted
	s.verifyWorkflowVersioning(tv, vbUnpinned, tv.Deployment(), nil, nil)
	if sticky {
		s.verifyWorkflowStickyQueue(tv.WithRunID(runID))
	}

	pollerDone := make(chan any)
	go func() {
		s.idlePollWorkflow(tv2, true, ver3MinPollTime, "new deployment should not receive query")
		close(pollerDone)
	}()
	s.pollAndQueryWorkflow(tv, sticky)
	<-pollerDone // wait for the idle poller to complete to not interfere with the next poller

	// redirect query to new deployment
	s.updateTaskQueueDeploymentData(tv2, true, 0, false, 0, tqTypeWf, tqTypeAct)

	go s.idlePollWorkflow(tv, true, ver3MinPollTime, "old deployment should not receive query")
	// Since the current deployment has changed, task will move to the normal queue (thus, sticky=false)
	s.pollAndQueryWorkflow(tv2, false)

}

func (s *Versioning3Suite) pollAndQueryWorkflow(
	tv *testvars.TestVars,
	sticky bool,
) {
	queryResultCh := make(chan any)
	s.pollWftAndHandleQueries(tv, sticky, queryResultCh,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondQueryTaskCompletedRequest, error) {
			return &workflowservice.RespondQueryTaskCompletedRequest{}, nil
		})

	_, err := s.queryWorkflow(tv)
	s.NoError(err)

	<-queryResultCh
}

func (s *Versioning3Suite) TestPinnedWorkflowWithLateActivityPoller() {
	s.RunTestWithMatchingBehavior(
		func() {
			s.testPinnedWorkflowWithLateActivityPoller()
		},
	)
}

func (s *Versioning3Suite) testPinnedWorkflowWithLateActivityPoller() {
	// Here, we test that designating activities as independent is revisited if the missing activity
	// pollers arrive to server while the so-far-independent activity is backlogged.
	// Summary: a wf starts with a pinned override. The first wft schedules an activity before
	// any activity poller on the pinned deployment is seen by the server. The activity is sent
	// to the default queue. Then, the activity poller on the pinned deployment arrives, the task
	// should be now sent to that poller although no current deployment is set on the TQs.

	tv := testvars.New(s)

	wftCompleted := make(chan interface{})
	s.pollWftAndHandle(tv, false, wftCompleted,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondWftWithActivities(tv, tv, false, vbUnpinned, "5"), nil
		})
	s.waitForDeploymentDataPropagation(tv, false, tqTypeWf)

	override := tv.VersioningOverridePinned()
	s.startWorkflow(tv, override)

	<-wftCompleted
	s.verifyWorkflowVersioning(tv, vbUnpinned, tv.Deployment(), override, nil)
	// Wait long enough to make sure the activity is backlogged.
	s.validateBacklogCount(tv, tqTypeAct, 1)

	// When the first activity poller arrives from this deployment, it registers the TQ in the
	// deployment and that will trigger reevaluation of backlog queue.
	s.pollActivityAndHandle(tv, nil,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondActivity(), nil
		})
	s.verifyWorkflowVersioning(tv, vbUnpinned, tv.Deployment(), override, nil)
	s.validateBacklogCount(tv, tqTypeAct, 0)

	s.pollWftAndHandle(tv, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondCompleteWorkflow(tv, vbUnpinned), nil
		})
	s.verifyWorkflowVersioning(tv, vbUnpinned, tv.Deployment(), override, nil)
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

	if sticky {
		s.warmUpSticky(tv)
	}

	wftCompleted := make(chan any)
	s.pollWftAndHandle(tv, false, wftCompleted,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.verifyWorkflowVersioning(tv, vbUnspecified, nil, nil, tv.DeploymentVersionTransition())
			return respondWftWithActivities(tv, tv, sticky, vbUnpinned, "5"), nil
		})

	actCompleted := make(chan any)
	s.pollActivityAndHandle(tv, actCompleted,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondActivity(), nil
		})

	s.setCurrentDeployment(tv)

	runID := s.startWorkflow(tv, nil)

	<-wftCompleted
	s.verifyWorkflowVersioning(tv, vbUnpinned, tv.Deployment(), nil, nil)
	s.verifyVersioningSAs(tv, vbUnpinned, tv)
	if sticky {
		s.verifyWorkflowStickyQueue(tv.WithRunID(runID))
	}

	<-actCompleted
	s.verifyWorkflowVersioning(tv, vbUnpinned, tv.Deployment(), nil, nil)

	s.pollWftAndHandle(tv, sticky, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondCompleteWorkflow(tv, vbUnpinned), nil
		})
	s.verifyWorkflowVersioning(tv, vbUnpinned, tv.Deployment(), nil, nil)
}

func (s *Versioning3Suite) TestUnpinnedWorkflowWithRamp_ToVersioned() {
	s.RunTestWithMatchingBehavior(
		func() {
			s.testUnpinnedWorkflowWithRamp(false)
		},
	)
}

func (s *Versioning3Suite) TestUnpinnedWorkflowWithRamp_ToUnversioned() {
	s.RunTestWithMatchingBehavior(
		func() {
			s.testUnpinnedWorkflowWithRamp(true)
		},
	)
}

func (s *Versioning3Suite) testUnpinnedWorkflowWithRamp(toUnversioned bool) {
	// This test sets a 50% ramp and runs 50 wfs and ensures both versions got some wf and
	// activity tasks.

	tv1 := testvars.New(s).WithBuildIDNumber(1)
	tv2 := tv1.WithBuildIDNumber(2)

	wf := func(ctx workflow.Context, version string) (string, error) {
		var ret string
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 1 * time.Second,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    1 * time.Second,
				BackoffCoefficient: 1,
			},
		}), "act").Get(ctx, &ret)
		s.NoError(err)
		return version, nil
	}

	wf1 := func(ctx workflow.Context) (string, error) {
		return wf(ctx, "v1")
	}
	wf2 := func(ctx workflow.Context) (string, error) {
		return wf(ctx, "v2")
	}
	act1 := func() (string, error) {
		return "v1", nil
	}
	act2 := func() (string, error) {
		return "v2", nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.FrontendGRPCAddress(),
		Namespace: s.Namespace().String(),
	})
	s.NoError(err)

	w1 := worker.New(sdkClient, tv1.TaskQueue().GetName(), worker.Options{
		BuildID:                 tv1.BuildID(),
		UseBuildIDForVersioning: true,
		DeploymentOptions: worker.DeploymentOptions{
			DeploymentSeriesName:      tv1.DeploymentSeries(),
			DefaultVersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
		},
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	w1.RegisterActivityWithOptions(act1, activity.RegisterOptions{Name: "act"})
	s.NoError(w1.Start())
	defer w1.Stop()

	// v1 is current and v2 is ramping at 50%

	// Make sure both TQs are registered in v1 which is the current version. This is to make sure
	// we don't get to ramping while one of the TQs has not yet got v1 as it's current version.
	// (note that s.setCurrentDeployment(tv1) can pass even with one TQ added to the version)
	s.waitForDeploymentDataPropagation(tv1, false, tqTypeWf, tqTypeAct)
	s.setCurrentDeployment(tv1)

	w2 := worker.New(sdkClient, tv2.TaskQueue().GetName(), worker.Options{
		BuildID:                 tv2.BuildID(),
		UseBuildIDForVersioning: !toUnversioned,
		DeploymentOptions: worker.DeploymentOptions{
			DeploymentSeriesName:      tv2.DeploymentSeries(),
			DefaultVersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
		},
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf"})
	w2.RegisterActivityWithOptions(act2, activity.RegisterOptions{Name: "act"})
	s.NoError(w2.Start())
	defer w2.Stop()

	s.setRampingDeployment(tv2, 50, toUnversioned)

	counter := make(map[string]int)
	for i := 0; i < 50; i++ {
		run, err := sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tv1.TaskQueue().GetName()}, "wf")
		s.NoError(err)
		var out string
		s.NoError(run.Get(ctx, &out))
		counter[out]++
	}

	// both versions should've got executions
	s.Greater(counter["v1"], 0)
	s.Greater(counter["v2"], 0)
	s.Equal(50, counter["v1"]+counter["v2"])
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
	if sticky {
		s.warmUpSticky(tv1)
	}

	s.updateTaskQueueDeploymentData(tv1, true, 0, false, 0, tqTypeWf, tqTypeAct)
	runID := s.startWorkflow(tv1, nil)

	s.pollWftAndHandle(tv1, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.verifyWorkflowVersioning(tv1, vbUnspecified, nil, nil, tv1.DeploymentVersionTransition())
			return respondWftWithActivities(tv1, tv1, sticky, vbUnpinned, "5"), nil
		})
	s.verifyWorkflowVersioning(tv1, vbUnpinned, tv1.Deployment(), nil, nil)
	if sticky {
		s.verifyWorkflowStickyQueue(tv1.WithRunID(runID))
	}

	s.pollActivityAndHandle(tv1, nil,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondActivity(), nil
		})
	s.verifyWorkflowVersioning(tv1, vbUnpinned, tv1.Deployment(), nil, nil)

	// Set B as the current deployment
	s.updateTaskQueueDeploymentData(tv2, true, 0, false, 0, tqTypeWf, tqTypeAct)

	s.pollWftAndHandle(tv2, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.verifyWorkflowVersioning(tv1, vbUnpinned, tv1.Deployment(), nil, tv2.DeploymentVersionTransition())
			return respondCompleteWorkflow(tv2, vbUnpinned), nil
		})
	s.verifyWorkflowVersioning(tv2, vbUnpinned, tv2.Deployment(), nil, nil)
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
				tv1.Any().String(): tv1.Any().String(),
			},
		},
	}

	// current deployment is -> A
	s.updateTaskQueueDeploymentData(tv1, true, 0, false, 0, tqTypeNexus)

	// local poller with deployment A receives task
	s.pollAndDispatchNexusTask(tv1, nexusRequest)

	// current deployment is now -> B
	s.updateTaskQueueDeploymentData(tv2, true, 0, false, 0, tqTypeNexus)

	// Pollers of A are there but should not get any task
	go s.idlePollNexus(tv1, true, ver3MinPollTime, "nexus task should not go to the old deployment")

	s.pollAndDispatchNexusTask(tv2, nexusRequest)
}

func (s *Versioning3Suite) pollAndDispatchNexusTask(
	tv *testvars.TestVars,
	nexusRequest *matchingservice.DispatchNexusTaskRequest,
) {
	matchingClient := s.GetTestCluster().MatchingClient()

	nexusCompleted := make(chan any)
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

	s.updateTaskQueueDeploymentData(tv, true, 0, false, 0, tqTypeWf, tqTypeAct)
	s.startWorkflow(tv, nil)

	poller, resp := s.pollWftAndHandle(tv, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.verifyWorkflowVersioning(tv, vbUnspecified, nil, nil, tv.DeploymentVersionTransition())
			resp := respondWftWithActivities(tv, tv, true, vbUnpinned, "5")
			resp.Commands[0].GetScheduleActivityTaskCommandAttributes().RequestEagerExecution = true
			return resp, nil
		})
	s.verifyWorkflowVersioning(tv, vbUnpinned, tv.Deployment(), nil, nil)

	s.NotEmpty(resp.GetActivityTasks())

	_, err := poller.HandleActivityTask(tv, resp.GetActivityTasks()[0],
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondActivity(), nil
		})
	s.NoError(err)
	s.verifyWorkflowVersioning(tv, vbUnpinned, tv.Deployment(), nil, nil)

	s.pollWftAndHandle(tv, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondCompleteWorkflow(tv, vbUnpinned), nil
		})
	s.verifyWorkflowVersioning(tv, vbUnpinned, tv.Deployment(), nil, nil)
}

func (s *Versioning3Suite) TestTransitionFromActivity_Sticky() {
	s.testTransitionFromActivity(true)
}

func (s *Versioning3Suite) TestTransitionFromActivity_NoSticky() {
	s.testTransitionFromActivity(false)
}

func (s *Versioning3Suite) testTransitionFromActivity(sticky bool) {
	// The workflow runs one WFT on d1 which schedules four activities, then:
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
	if sticky {
		s.warmUpSticky(tv1)
	}

	s.updateTaskQueueDeploymentData(tv1, true, 0, false, 0, tqTypeWf, tqTypeAct)
	runID := s.startWorkflow(tv1, nil)

	s.pollWftAndHandle(tv1, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.verifyWorkflowVersioning(tv1, vbUnspecified, nil, nil, tv1.DeploymentVersionTransition())
			return respondWftWithActivities(tv1, tv1, sticky, vbUnpinned, "5", "6", "7", "8"), nil
		})
	s.verifyWorkflowVersioning(tv1, vbUnpinned, tv1.Deployment(), nil, nil)
	if sticky {
		s.verifyWorkflowStickyQueue(tv1.WithRunID(runID))
	}

	transitionCompleted := atomic.Bool{}
	transitionStarted := make(chan any)
	act1Started := make(chan any)
	act1Completed := make(chan any)
	act2Started := make(chan any)
	act2Failed := make(chan any)
	act2To4Completed := make(chan any)

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
	s.verifyWorkflowVersioning(tv1, vbUnpinned, tv1.Deployment(), nil, nil)

	// 2. Set d2 as the current deployment
	s.updateTaskQueueDeploymentData(tv2, true, 0, false, 0, tqTypeWf, tqTypeAct)
	// Although updateTaskQueueDeploymentData waits for deployment data to reach the TQs, backlogged
	// tasks might still be waiting behind the old deployment's poll channel. Partition manage should
	// immediately react to the deployment data changes, but there still is a race possible and the
	// only way to safeguard against it is to wait a little while before proceeding.
	time.Sleep(time.Millisecond * 200) //nolint:forbidigo

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
			s.verifyWorkflowVersioning(tv1, vbUnpinned, tv1.Deployment(), nil, tv2.DeploymentVersionTransition())
			close(transitionStarted)
			s.Logger.Info("Transition wft started")
			// 8. Complete the transition after act1 completes and act2's first attempt fails.
			<-act1Completed
			<-act2Failed
			transitionCompleted.Store(true)
			s.Logger.Info("Transition wft completed")
			return respondEmptyWft(tv2, sticky, vbUnpinned), nil
		})
	s.verifyWorkflowVersioning(tv2, vbUnpinned, tv2.Deployment(), nil, nil)
	if sticky {
		s.verifyWorkflowStickyQueue(tv2)
	}

	// 9. Now all activities should complete.
	<-act2To4Completed
	s.pollWftAndHandle(tv2, sticky, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.Logger.Info("Final wft completed")
			return respondCompleteWorkflow(tv2, vbUnpinned), nil
		})
	s.verifyWorkflowVersioning(tv2, vbUnpinned, tv2.Deployment(), nil, nil)
}

func (s *Versioning3Suite) TestIndependentActivity_Pinned() {
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

	// Set current deployment for each TQ
	s.updateTaskQueueDeploymentData(tvWf, true, 0, false, 0, tqTypeWf)
	s.updateTaskQueueDeploymentData(tvAct, true, 0, false, 0, tqTypeAct)

	s.startWorkflow(tvWf, nil)

	s.pollWftAndHandle(tvWf, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.verifyWorkflowVersioning(tvWf, vbUnspecified, nil, nil, tvWf.DeploymentVersionTransition())
			s.Logger.Info("First wf task completed")
			return respondWftWithActivities(tvWf, tvAct, false, behavior, "5"), nil
		})
	s.verifyWorkflowVersioning(tvWf, behavior, tvWf.Deployment(), nil, nil)

	s.pollActivityAndHandle(tvAct, nil,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			s.NotNil(task)
			s.Logger.Info("Activity completed")
			return respondActivity(), nil
		})
	s.verifyWorkflowVersioning(tvWf, behavior, tvWf.Deployment(), nil, nil)

	s.pollWftAndHandle(tvWf, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondCompleteWorkflow(tvWf, behavior), nil
		})
	s.verifyWorkflowVersioning(tvWf, behavior, tvWf.Deployment(), nil, nil)
}

func (s *Versioning3Suite) TestChildWorkflowInheritance_PinnedParent() {
	s.testChildWorkflowInheritance_ExpectInherit(false, false, vbPinned)
}

func (s *Versioning3Suite) TestChildWorkflowInheritance_ParentPinnedByOverride() {
	s.testChildWorkflowInheritance_ExpectInherit(false, true, vbUnpinned)
}

func (s *Versioning3Suite) TestChildWorkflowInheritance_PinnedParent_CrossTQ() {
	s.T().Skip() // until cross TQ inheritance is implemented
	s.testChildWorkflowInheritance_ExpectInherit(true, false, vbPinned)
}

func (s *Versioning3Suite) TestChildWorkflowInheritance_PinnedParent_CrossTQ_WithOverride() {
	s.T().Skip() // until cross TQ inheritance is implemented
	s.testChildWorkflowInheritance_ExpectInherit(true, true, vbPinned)
}

func (s *Versioning3Suite) testChildWorkflowInheritance_ExpectInherit(crossTq bool, withOverride bool, parentRegistrationBehavior enumspb.VersioningBehavior) {
	// Child wf of a pinned parent starts on the parents pinned version.

	tv1 := testvars.New(s).WithBuildIDNumber(1).WithWorkflowIDNumber(1)
	tv2 := tv1.WithBuildIDNumber(2)
	tv1Child := tv1.WithWorkflowIDNumber(2)
	if crossTq {
		tv1Child = tv1Child.WithTaskQueue("child-tq")
	}
	tv2Child := tv1Child.WithBuildIDNumber(2)

	var override *workflowpb.VersioningOverride
	var sdkOverride sdkclient.VersioningOverride
	if withOverride {
		override = tv1.VersioningOverridePinned()
		sdkOverride = sdkclient.VersioningOverride{
			Behavior: workflow.VersioningBehaviorPinned,
			Deployment: sdkclient.Deployment{
				SeriesName: tv1.DeploymentSeries(),
				BuildID:    tv1.BuildID(),
			},
		}
	}

	// This is the registered behavior which can be unpinned, but only if withOverride. We want
	// parent's effective behavior to always be pinned.
	sdkParentBehavior := workflow.VersioningBehaviorPinned
	if parentRegistrationBehavior == vbUnpinned {
		sdkParentBehavior = workflow.VersioningBehaviorAutoUpgrade
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wfStarted := make(chan struct{}, 1)
	currentChanged := make(chan struct{}, 1)

	childv1 := func(ctx workflow.Context) (string, error) {
		s.verifyWorkflowVersioning(tv1Child, vbPinned, tv1Child.Deployment(), override, nil)
		return "v1", nil
	}
	wf1 := func(ctx workflow.Context) (string, error) {
		wfStarted <- struct{}{}
		// wait for current version to change
		<-currentChanged

		// run two child workflows
		fut1 := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			TaskQueue:  tv1Child.TaskQueue().GetName(),
			WorkflowID: tv1Child.WorkflowID(),
		}), "child")
		var val1 string
		s.NoError(fut1.Get(ctx, &val1))

		s.verifyWorkflowVersioning(tv1, parentRegistrationBehavior, tv1.Deployment(), override, nil)
		return val1, nil
	}

	// v1 is current for both parent and child
	s.updateTaskQueueDeploymentData(tv1, true, 0, false, 0, tqTypeWf)
	if crossTq {
		s.updateTaskQueueDeploymentData(tv1Child, true, 0, false, 0, tqTypeWf)
	}

	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.FrontendGRPCAddress(),
		Namespace: s.Namespace().String(),
	})
	s.NoError(err)

	if crossTq {
		w1xtq := worker.New(sdkClient, tv1Child.TaskQueue().GetName(), worker.Options{
			BuildID:                 tv1Child.BuildID(),
			UseBuildIDForVersioning: true,
			DeploymentOptions: worker.DeploymentOptions{
				DeploymentSeriesName:      tv1Child.DeploymentSeries(),
				DefaultVersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
			},
			MaxConcurrentWorkflowTaskPollers: numPollers,
		})
		w1xtq.RegisterWorkflowWithOptions(childv1, workflow.RegisterOptions{Name: "child", VersioningBehavior: workflow.VersioningBehaviorPinned})
		s.NoError(w1xtq.Start())
		defer w1xtq.Stop()
	}

	w1 := worker.New(sdkClient, tv1.TaskQueue().GetName(), worker.Options{
		BuildID:                 tv1.BuildID(),
		UseBuildIDForVersioning: true,
		DeploymentOptions: worker.DeploymentOptions{
			DeploymentSeriesName:      tv1.DeploymentSeries(),
			DefaultVersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
		},
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf", VersioningBehavior: sdkParentBehavior})
	if !crossTq {
		w1.RegisterWorkflowWithOptions(childv1, workflow.RegisterOptions{Name: "child", VersioningBehavior: workflow.VersioningBehaviorPinned})
	}
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                 tv1.WorkflowID(),
		TaskQueue:          tv1.TaskQueue().GetName(),
		VersioningOverride: sdkOverride,
	}, "wf")
	s.NoError(err)
	// wait for it to start on v1
	s.WaitForChannel(ctx, wfStarted)
	close(wfStarted) // force panic if replayed

	// make v2 current for both parent and child and unblock the wf to start the child
	s.updateTaskQueueDeploymentData(tv2, true, 0, false, 0, tqTypeWf)
	if crossTq {
		s.updateTaskQueueDeploymentData(tv2Child, true, 0, false, 0, tqTypeWf)
	}
	currentChanged <- struct{}{}

	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("v1", out)
}

func (s *Versioning3Suite) TestChildWorkflowInheritance_UnpinnedParent() {
	s.testChildWorkflowInheritance_ExpectNoInherit(false, vbUnpinned)
}

func (s *Versioning3Suite) TestChildWorkflowInheritance_CrossTQ() {
	// TODO: remove this test once cross-TQ inheritance is implemented
	s.testChildWorkflowInheritance_ExpectNoInherit(true, vbPinned)
}

func (s *Versioning3Suite) testChildWorkflowInheritance_ExpectNoInherit(crossTq bool, parentBehavior enumspb.VersioningBehavior) {
	// Child wf of an unpinned parent is always started on the Current Version of its TQ.
	// For the time being, cross-TQ children do not inherit parents pinned version until that part
	// is implemented.

	tv1 := testvars.New(s).WithBuildIDNumber(1).WithWorkflowIDNumber(1)
	tv2 := tv1.WithBuildIDNumber(2)
	tv1Child := tv1.WithWorkflowIDNumber(2)
	if crossTq {
		tv1Child = tv1Child.WithTaskQueue("child-tq")
	}
	tv2Child := tv1Child.WithBuildIDNumber(2)

	sdkParentBehavior := workflow.VersioningBehaviorPinned
	if parentBehavior == vbUnpinned {
		sdkParentBehavior = workflow.VersioningBehaviorAutoUpgrade
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wfStarted := make(chan struct{}, 1)
	currentChanged := make(chan struct{}, 1)

	childv1 := func(ctx workflow.Context) (string, error) {
		panic("child should not run on v1")
	}
	childv2 := func(ctx workflow.Context) (string, error) {
		fmt.Printf("shahab> child started \n")
		s.verifyWorkflowVersioning(tv2Child, vbUnspecified, nil, nil, tv2Child.DeploymentVersionTransition())
		return "v2", nil
	}
	wf1 := func(ctx workflow.Context) (string, error) {
		wfStarted <- struct{}{}
		// wait for current version to change
		<-currentChanged

		// run two child workflows
		fut1 := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			TaskQueue:  tv2Child.TaskQueue().GetName(),
			WorkflowID: tv2Child.WorkflowID(),
		}), "child")
		var val1 string
		s.NoError(fut1.Get(ctx, &val1))

		s.verifyWorkflowVersioning(tv1, parentBehavior, tv1.Deployment(), nil, nil)
		return val1, nil
	}

	// Same as v1 without channel blocking
	wf2 := func(ctx workflow.Context) (string, error) {
		// run two child workflows
		fut1 := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			TaskQueue:  tv2Child.TaskQueue().GetName(),
			WorkflowID: tv2Child.WorkflowID(),
		}), "child")
		var val1 string
		s.NoError(fut1.Get(ctx, &val1))

		s.verifyWorkflowVersioning(tv1, parentBehavior, tv2.Deployment(), nil, nil)
		return val1, nil
	}

	// v1 is current for both parent and child
	s.updateTaskQueueDeploymentData(tv1, true, 0, false, 0, tqTypeWf)
	if crossTq {
		s.updateTaskQueueDeploymentData(tv1Child, true, 0, false, 0, tqTypeWf)
	}

	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.FrontendGRPCAddress(),
		Namespace: s.Namespace().String(),
	})
	s.NoError(err)

	if crossTq {
		w1xtq := worker.New(sdkClient, tv1Child.TaskQueue().GetName(), worker.Options{
			BuildID:                 tv1Child.BuildID(),
			UseBuildIDForVersioning: true,
			DeploymentOptions: worker.DeploymentOptions{
				DeploymentSeriesName:      tv1Child.DeploymentSeries(),
				DefaultVersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
			},
			MaxConcurrentWorkflowTaskPollers: numPollers,
		})
		w1xtq.RegisterWorkflowWithOptions(childv1, workflow.RegisterOptions{Name: "child", VersioningBehavior: workflow.VersioningBehaviorPinned})
		s.NoError(w1xtq.Start())
		defer w1xtq.Stop()
	}
	w1 := worker.New(sdkClient, tv1.TaskQueue().GetName(), worker.Options{
		BuildID:                 tv1.BuildID(),
		UseBuildIDForVersioning: true,
		DeploymentOptions: worker.DeploymentOptions{
			DeploymentSeriesName:      tv1.DeploymentSeries(),
			DefaultVersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
		},
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf", VersioningBehavior: sdkParentBehavior})
	if !crossTq {
		w1.RegisterWorkflowWithOptions(childv1, workflow.RegisterOptions{Name: "child", VersioningBehavior: workflow.VersioningBehaviorPinned})
	}
	s.NoError(w1.Start())
	defer w1.Stop()

	if crossTq {
		w2xtq := worker.New(sdkClient, tv2Child.TaskQueue().GetName(), worker.Options{
			BuildID:                 tv2Child.BuildID(),
			UseBuildIDForVersioning: true,
			DeploymentOptions: worker.DeploymentOptions{
				DeploymentSeriesName:      tv2Child.DeploymentSeries(),
				DefaultVersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
			},
			MaxConcurrentWorkflowTaskPollers: numPollers,
		})
		w2xtq.RegisterWorkflowWithOptions(childv2, workflow.RegisterOptions{Name: "child", VersioningBehavior: workflow.VersioningBehaviorPinned})
		s.NoError(w2xtq.Start())
		defer w2xtq.Stop()
	}
	w2 := worker.New(sdkClient, tv2.TaskQueue().GetName(), worker.Options{
		BuildID:                 tv2.BuildID(),
		UseBuildIDForVersioning: true,
		DeploymentOptions: worker.DeploymentOptions{
			DeploymentSeriesName:      tv2.DeploymentSeries(),
			DefaultVersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
		},
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf", VersioningBehavior: sdkParentBehavior})
	if !crossTq {
		w2.RegisterWorkflowWithOptions(childv2, workflow.RegisterOptions{Name: "child", VersioningBehavior: workflow.VersioningBehaviorPinned})
	}
	s.NoError(w2.Start())
	defer w2.Stop()

	run, err := sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        tv1.WorkflowID(),
		TaskQueue: tv1.TaskQueue().GetName(),
	}, "wf")
	s.NoError(err)

	// wait for it to start on v1
	s.WaitForChannel(ctx, wfStarted)
	close(wfStarted) // force panic if replayed

	// make v2 current for both parent and child and unblock the wf to start the child
	s.updateTaskQueueDeploymentData(tv2, true, 0, false, 0, tqTypeWf)
	if crossTq {
		s.updateTaskQueueDeploymentData(tv2Child, true, 0, false, 0, tqTypeWf)
	}
	currentChanged <- struct{}{}

	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("v2", out)
}

func (s *Versioning3Suite) TestPinnedCaN() {
	s.testCan(vbPinned)
}

func (s *Versioning3Suite) TestUnpinnedCaN() {
	s.testCan(vbUnpinned)
}

func (s *Versioning3Suite) testCan(behavior enumspb.VersioningBehavior) {
	// CaN always go to the current version.

	tv1 := testvars.New(s).WithBuildIDNumber(1).WithWorkflowIDNumber(1)
	tv2 := tv1.WithBuildIDNumber(2)
	sdkBehavior := workflow.VersioningBehaviorAutoUpgrade
	if behavior == vbPinned {
		sdkBehavior = workflow.VersioningBehaviorPinned
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wfStarted := make(chan struct{}, 1)
	currentChanged := make(chan struct{}, 1)

	wf1 := func(ctx workflow.Context) (string, error) {
		s.verifyWorkflowVersioning(tv1, vbUnspecified, nil, nil, tv1.DeploymentVersionTransition())
		wfStarted <- struct{}{}
		// wait for current version to change.
		<-currentChanged
		return "", workflow.NewContinueAsNewError(ctx, "wf")
	}

	wf2 := func(ctx workflow.Context) (string, error) {
		s.verifyWorkflowVersioning(tv2, vbUnspecified, nil, nil, tv2.DeploymentVersionTransition())
		return "v2", nil
	}

	// make v1 current
	s.updateTaskQueueDeploymentData(tv1, true, 0, false, 0, tqTypeWf)

	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.FrontendGRPCAddress(),
		Namespace: s.Namespace().String(),
	})
	s.NoError(err)

	w1 := worker.New(sdkClient, tv1.TaskQueue().GetName(), worker.Options{
		BuildID:                 tv1.BuildID(),
		UseBuildIDForVersioning: true,
		DeploymentOptions: worker.DeploymentOptions{
			DeploymentSeriesName: tv1.DeploymentSeries(),
		},
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf", VersioningBehavior: sdkBehavior})
	s.NoError(w1.Start())
	defer w1.Stop()

	w2 := worker.New(sdkClient, tv2.TaskQueue().GetName(), worker.Options{
		BuildID:                 tv2.BuildID(),
		UseBuildIDForVersioning: true,
		DeploymentOptions: worker.DeploymentOptions{
			DeploymentSeriesName: tv2.DeploymentSeries(),
		},
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf", VersioningBehavior: sdkBehavior})
	s.NoError(w2.Start())
	defer w2.Stop()

	run, err := sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        tv1.WorkflowID(),
		TaskQueue: tv1.TaskQueue().GetName(),
	}, "wf")
	s.NoError(err)

	// wait for it to start on v1
	s.WaitForChannel(ctx, wfStarted)
	close(wfStarted) // force panic if replayed

	// make v2 current
	s.updateTaskQueueDeploymentData(tv2, true, 0, false, 0, tqTypeWf)
	currentChanged <- struct{}{}

	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("v2", out)
}

func (s *Versioning3Suite) TestDescribeTaskQueueVersioningInfo() {
	tv := testvars.New(s)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	t1 := time.Now()
	t2 := t1.Add(time.Second)

	s.syncTaskQueueDeploymentData(tv, false, 20, false, t1, tqTypeWf)
	wfInfo, err := s.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
		Namespace:     s.Namespace().String(),
		TaskQueue:     tv.TaskQueue(),
		TaskQueueType: tqTypeWf,
	})
	s.NoError(err)
	s.ProtoEqual(&taskqueuepb.TaskQueueVersioningInfo{CurrentVersion: "__unversioned__", RampingVersion: tv.DeploymentVersionString(), RampingVersionPercentage: 20, UpdateTime: timestamp.TimePtr(t1)}, wfInfo.GetVersioningInfo())

	s.syncTaskQueueDeploymentData(tv, true, 0, false, t1, tqTypeAct)
	actInfo, err := s.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
		Namespace:     s.Namespace().String(),
		TaskQueue:     tv.TaskQueue(),
		TaskQueueType: tqTypeAct,
	})
	s.NoError(err)
	s.ProtoEqual(&taskqueuepb.TaskQueueVersioningInfo{CurrentVersion: tv.DeploymentVersionString(), UpdateTime: timestamp.TimePtr(t1)}, actInfo.GetVersioningInfo())

	// Now ramp to unversioned
	s.syncTaskQueueDeploymentData(tv, false, 10, true, t2, tqTypeAct)
	actInfo, err = s.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
		Namespace:     s.Namespace().String(),
		TaskQueue:     tv.TaskQueue(),
		TaskQueueType: tqTypeAct,
	})
	s.NoError(err)
	s.ProtoEqual(&taskqueuepb.TaskQueueVersioningInfo{CurrentVersion: tv.DeploymentVersionString(), RampingVersion: "__unversioned__", RampingVersionPercentage: 10, UpdateTime: timestamp.TimePtr(t2)}, actInfo.GetVersioningInfo())
}

func (s *Versioning3Suite) TestSyncDeploymentUserData_Update() {
	tv := testvars.New(s)

	data := s.getTaskQueueDeploymentData(tv, tqTypeAct)
	s.Nil(data)
	data = s.getTaskQueueDeploymentData(tv, tqTypeWf)
	s.Nil(data)

	t1 := time.Now()
	tv1 := tv.WithBuildIDNumber(1)

	s.syncTaskQueueDeploymentData(tv1, true, 0, false, t1, tqTypeAct)
	data = s.getTaskQueueDeploymentData(tv, tqTypeAct)
	s.ProtoEqual(&persistencespb.DeploymentData{Versions: []*deploymentspb.DeploymentVersionData{
		{Version: tv1.DeploymentVersion(), CurrentSinceTime: timestamp.TimePtr(t1), RoutingUpdateTime: timestamp.TimePtr(t1)},
	}}, data)
	data = s.getTaskQueueDeploymentData(tv, tqTypeWf)
	s.Nil(data)

	// Changing things with an older timestamp should not have effect.
	t0 := t1.Add(-time.Second)
	s.syncTaskQueueDeploymentData(tv1, false, 0, false, t0, tqTypeAct)
	data = s.getTaskQueueDeploymentData(tv, tqTypeAct)
	s.ProtoEqual(&persistencespb.DeploymentData{Versions: []*deploymentspb.DeploymentVersionData{
		{Version: tv1.DeploymentVersion(), CurrentSinceTime: timestamp.TimePtr(t1), RoutingUpdateTime: timestamp.TimePtr(t1)},
	}}, data)

	// Changing things with a newer timestamp should apply
	t2 := t1.Add(time.Second)
	s.syncTaskQueueDeploymentData(tv1, false, 20, false, t2, tqTypeAct)
	data = s.getTaskQueueDeploymentData(tv, tqTypeAct)
	s.ProtoEqual(&persistencespb.DeploymentData{Versions: []*deploymentspb.DeploymentVersionData{
		{Version: tv1.DeploymentVersion(), CurrentSinceTime: nil, RampingSinceTime: timestamp.TimePtr(t2), RampPercentage: 20, RoutingUpdateTime: timestamp.TimePtr(t2)},
	}}, data)

	// Add another version, this time to both tq types
	tv2 := tv.WithBuildIDNumber(2)
	s.syncTaskQueueDeploymentData(tv2, false, 10, false, t1, tqTypeAct, tqTypeWf)
	data = s.getTaskQueueDeploymentData(tv, tqTypeAct)
	s.ProtoEqual(&persistencespb.DeploymentData{Versions: []*deploymentspb.DeploymentVersionData{
		{Version: tv1.DeploymentVersion(), CurrentSinceTime: nil, RampingSinceTime: timestamp.TimePtr(t2), RampPercentage: 20, RoutingUpdateTime: timestamp.TimePtr(t2)},
		{Version: tv2.DeploymentVersion(), CurrentSinceTime: nil, RampingSinceTime: timestamp.TimePtr(t1), RampPercentage: 10, RoutingUpdateTime: timestamp.TimePtr(t1)},
	}}, data)
	data = s.getTaskQueueDeploymentData(tv, tqTypeWf)
	s.ProtoEqual(&persistencespb.DeploymentData{Versions: []*deploymentspb.DeploymentVersionData{
		{Version: tv2.DeploymentVersion(), CurrentSinceTime: nil, RampingSinceTime: timestamp.TimePtr(t1), RampPercentage: 10, RoutingUpdateTime: timestamp.TimePtr(t1)},
	}}, data)

	// Make v2 current
	s.syncTaskQueueDeploymentData(tv2, true, 0, false, t2, tqTypeAct)
	data = s.getTaskQueueDeploymentData(tv, tqTypeAct)
	s.ProtoEqual(&persistencespb.DeploymentData{Versions: []*deploymentspb.DeploymentVersionData{
		{Version: tv1.DeploymentVersion(), CurrentSinceTime: nil, RampingSinceTime: timestamp.TimePtr(t2), RampPercentage: 20, RoutingUpdateTime: timestamp.TimePtr(t2)},
		{Version: tv2.DeploymentVersion(), CurrentSinceTime: timestamp.TimePtr(t2), RoutingUpdateTime: timestamp.TimePtr(t2)},
	}}, data)

	// Forget v1
	s.forgetTaskQueueDeploymentVersion(tv1, tqTypeAct, false)
	data = s.getTaskQueueDeploymentData(tv, tqTypeAct)
	s.ProtoEqual(&persistencespb.DeploymentData{Versions: []*deploymentspb.DeploymentVersionData{
		{Version: tv2.DeploymentVersion(), CurrentSinceTime: timestamp.TimePtr(t2), RoutingUpdateTime: timestamp.TimePtr(t2)},
	}}, data)

	// Forget v1 again should be a noop
	s.forgetTaskQueueDeploymentVersion(tv1, tqTypeAct, false)
	data = s.getTaskQueueDeploymentData(tv, tqTypeAct)
	s.ProtoEqual(&persistencespb.DeploymentData{Versions: []*deploymentspb.DeploymentVersionData{
		{Version: tv2.DeploymentVersion(), CurrentSinceTime: timestamp.TimePtr(t2), RoutingUpdateTime: timestamp.TimePtr(t2)},
	}}, data)

	// Ramp unversioned
	s.syncTaskQueueDeploymentData(tv2, false, 90, true, t2, tqTypeAct)
	data = s.getTaskQueueDeploymentData(tv, tqTypeAct)
	s.ProtoEqual(&persistencespb.DeploymentData{Versions: []*deploymentspb.DeploymentVersionData{
		{Version: tv2.DeploymentVersion(), CurrentSinceTime: timestamp.TimePtr(t2), RoutingUpdateTime: timestamp.TimePtr(t2)},
	},
		UnversionedRampData: &deploymentspb.DeploymentVersionData{RampingSinceTime: timestamp.TimePtr(t2), RampPercentage: 90, RoutingUpdateTime: timestamp.TimePtr(t2)},
	}, data)

	// Forget v2
	s.forgetTaskQueueDeploymentVersion(tv2, tqTypeAct, false)
	data = s.getTaskQueueDeploymentData(tv, tqTypeAct)
	s.ProtoEqual(&persistencespb.DeploymentData{
		UnversionedRampData: &deploymentspb.DeploymentVersionData{RampingSinceTime: timestamp.TimePtr(t2), RampPercentage: 90, RoutingUpdateTime: timestamp.TimePtr(t2)},
	}, data)
}

func (s *Versioning3Suite) waitForDeploymentVersionCreation(
	tv *testvars.TestVars,
) {
	v := tv.DeploymentVersionString()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		res, _ := s.FrontendClient().DescribeWorkerDeployment(ctx,
			&workflowservice.DescribeWorkerDeploymentRequest{
				Namespace:      s.Namespace().String(),
				DeploymentName: tv.DeploymentSeries(),
			})
		if res != nil {
			found := false
			for _, vs := range res.GetWorkerDeploymentInfo().GetVersionSummaries() {
				if vs.GetVersion() == v {
					found = true
				}
			}
			a.True(found)
		}
	}, 5*time.Second, 100*time.Millisecond)
}

func (s *Versioning3Suite) setCurrentDeployment(
	tv *testvars.TestVars,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s.Eventually(func() bool {
		_, err := s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx,
			&workflowservice.SetWorkerDeploymentCurrentVersionRequest{
				Namespace:      s.Namespace().String(),
				DeploymentName: tv.DeploymentSeries(),
				Version:        tv.DeploymentVersionString(),
			})
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return false
		}
		s.NoError(err)
		return err == nil
	}, 5*time.Second, 100*time.Millisecond)
}

func (s *Versioning3Suite) setRampingDeployment(
	tv *testvars.TestVars,
	percentage float32,
	rampUnversioned bool,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	v := tv.DeploymentVersionString()
	if rampUnversioned {
		v = "__unversioned__"
	}

	s.Eventually(func() bool {
		_, err := s.FrontendClient().SetWorkerDeploymentRampingVersion(ctx,
			&workflowservice.SetWorkerDeploymentRampingVersionRequest{
				Namespace:      s.Namespace().String(),
				DeploymentName: tv.DeploymentSeries(),
				Version:        v,
				Percentage:     percentage,
			})
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return false
		}
		s.NoError(err)
		return err == nil
	}, 20*time.Second, 100*time.Millisecond)
}

func (s *Versioning3Suite) updateTaskQueueDeploymentData(
	tv *testvars.TestVars,
	isCurrent bool,
	ramp float32,
	rampUnversioned bool,
	timeSinceUpdate time.Duration,
	tqTypes ...enumspb.TaskQueueType,
) {
	s.syncTaskQueueDeploymentData(tv, isCurrent, ramp, rampUnversioned, time.Now().Add(-timeSinceUpdate), tqTypes...)
	s.waitForDeploymentDataPropagation(tv, rampUnversioned, tqTypes...)
}

// getTaskQueueDeploymentData gets the deployment data for a given TQ type. The data is always
// returned from the WF type root partition, so no need to wait for propagation before calling this
// function.
func (s *Versioning3Suite) getTaskQueueDeploymentData(
	tv *testvars.TestVars,
	tqType enumspb.TaskQueueType,
) *persistencespb.DeploymentData {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	resp, err := s.GetTestCluster().MatchingClient().GetTaskQueueUserData(
		ctx,
		&matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:   s.NamespaceID().String(),
			TaskQueue:     tv.TaskQueue().GetName(),
			TaskQueueType: tqTypeWf,
		})
	s.NoError(err)
	return resp.GetUserData().GetData().GetPerType()[int32(tqType)].GetDeploymentData()
}

func (s *Versioning3Suite) syncTaskQueueDeploymentData(
	tv *testvars.TestVars,
	isCurrent bool,
	ramp float32,
	rampUnversioned bool,
	updateTime time.Time,
	t ...enumspb.TaskQueueType,
) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	v := tv.DeploymentVersion()
	if rampUnversioned {
		v = nil
	}

	routingUpdateTime := timestamp.TimePtr(updateTime)
	var currentSinceTime, rampingSinceTime *timestamppb.Timestamp
	if isCurrent {
		currentSinceTime = routingUpdateTime
	}
	if ramp > 0 { // todo carly / shahab: this doesn't account for setting 0 ramp, or for changing the ramp while ramping_since_time stays the same.
		rampingSinceTime = routingUpdateTime
	}

	_, err := s.GetTestCluster().MatchingClient().SyncDeploymentUserData(
		ctx, &matchingservice.SyncDeploymentUserDataRequest{
			NamespaceId:    s.NamespaceID().String(),
			TaskQueue:      tv.TaskQueue().GetName(),
			TaskQueueTypes: t,
			Operation: &matchingservice.SyncDeploymentUserDataRequest_UpdateVersionData{
				UpdateVersionData: &deploymentspb.DeploymentVersionData{
					Version:           v,
					RoutingUpdateTime: routingUpdateTime,
					CurrentSinceTime:  currentSinceTime,
					RampingSinceTime:  rampingSinceTime,
					RampPercentage:    ramp,
				},
			},
		},
	)
	s.NoError(err)
}

func (s *Versioning3Suite) forgetTaskQueueDeploymentVersion(
	tv *testvars.TestVars,
	t enumspb.TaskQueueType,
	forgetUnversionedRamp bool,
) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	v := tv.DeploymentVersion()
	if forgetUnversionedRamp {
		v.BuildId = ""
	}
	_, err := s.GetTestCluster().MatchingClient().SyncDeploymentUserData(
		ctx, &matchingservice.SyncDeploymentUserDataRequest{
			NamespaceId:   s.NamespaceID().String(),
			TaskQueue:     tv.TaskQueue().GetName(),
			TaskQueueType: t,
			Operation: &matchingservice.SyncDeploymentUserDataRequest_ForgetVersion{
				ForgetVersion: v,
			},
		},
	)
	s.NoError(err)
}

func (s *Versioning3Suite) verifyWorkflowVersioning(
	tv *testvars.TestVars,
	behavior enumspb.VersioningBehavior,
	deployment *deploymentpb.Deployment,
	override *workflowpb.VersioningOverride,
	transition *workflowpb.DeploymentVersionTransition,
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
	var v *deploymentspb.WorkerDeploymentVersion
	if versioningInfo.GetVersion() != "" {
		v, err = worker_versioning.WorkerDeploymentVersionFromString(versioningInfo.GetVersion())
		s.NoError(err)
	}
	actualDeployment := worker_versioning.DeploymentFromDeploymentVersion(v)
	if !deployment.Equal(actualDeployment) {
		s.Fail(fmt.Sprintf("deployment version mismatch. expected: {%s}, actual: {%s}",
			deployment,
			actualDeployment,
		))
	}

	s.Equal(override.GetBehavior().String(), versioningInfo.GetVersioningOverride().GetBehavior().String())
	if actualOverrideDeployment := versioningInfo.GetVersioningOverride().GetPinnedVersion(); override.GetPinnedVersion() != actualOverrideDeployment {
		s.Fail(fmt.Sprintf("pinned override mismatch. expected: {%s}, actual: {%s}",
			override.GetPinnedVersion(),
			actualOverrideDeployment,
		))
	}

	if !versioningInfo.GetVersionTransition().Equal(transition) {
		s.Fail(fmt.Sprintf("version transition mismatch. expected: {%s}, actual: {%s}",
			transition,
			versioningInfo.GetVersionTransition(),
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
					ActivityType: tvAct.ActivityType(),
					TaskQueue:    tvAct.TaskQueue(),
					Input:        tvAct.Any().Payloads(),
					// TODO (shahab): tests with forced task forward take multiple seconds. Need to know why?
					ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(3 * time.Second),
					HeartbeatTimeout:       durationpb.New(3 * time.Second),
					RequestEagerExecution:  false,
				},
			},
		})
	}
	return &workflowservice.RespondWorkflowTaskCompletedRequest{
		Commands:                   commands,
		StickyAttributes:           stickyAttr,
		ForceCreateNewWorkflowTask: false,
		VersioningBehavior:         behavior,
		DeploymentOptions: &deploymentpb.WorkerDeploymentOptions{
			BuildId:              tvWf.BuildID(),
			DeploymentName:       tvWf.DeploymentSeries(),
			WorkerVersioningMode: enumspb.WORKER_VERSIONING_MODE_VERSIONED,
		},
		// TODO (shahab): remove stamp once build ID is added to wftc event
		WorkerVersionStamp: &commonpb.WorkerVersionStamp{
			BuildId:       tvWf.BuildID(),
			UseVersioning: true,
		},
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
						Result: tv.Any().Payloads(),
					},
				},
			},
		},
		ForceCreateNewWorkflowTask: false,
		VersioningBehavior:         behavior,
		DeploymentOptions: &deploymentpb.WorkerDeploymentOptions{
			BuildId:              tv.BuildID(),
			DeploymentName:       tv.DeploymentSeries(),
			WorkerVersioningMode: enumspb.WORKER_VERSIONING_MODE_VERSIONED,
		},
	}
}

func (s *Versioning3Suite) startWorkflow(
	tv *testvars.TestVars,
	override *workflowpb.VersioningOverride,
) string {
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          tv.Any().String(),
		Namespace:          s.Namespace().String(),
		WorkflowId:         tv.WorkflowID(),
		WorkflowType:       tv.WorkflowType(),
		TaskQueue:          tv.TaskQueue(),
		Identity:           tv.WorkerIdentity(),
		VersioningOverride: override,
	}

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)
	return we.GetRunId()
}

func (s *Versioning3Suite) queryWorkflow(
	tv *testvars.TestVars,
) (*workflowservice.QueryWorkflowResponse, error) {
	request := &workflowservice.QueryWorkflowRequest{
		Namespace: s.Namespace().String(),
		Execution: tv.WorkflowExecution(),
		Query:     tv.Query(),
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
	short := fmt.Sprintf("%s-%08x",
		fullName[len(fullName)-21:],
		farm.Fingerprint32([]byte(fullName)),
	)
	return strings.Replace(short, ".", "|", -1)
}

// pollWftAndHandle can be used in sync and async mode. For async mode pass the async channel. It
// will be closed when the task is handled.
// Returns the poller and poll response only in sync mode (can be used to process new wft in the response)
func (s *Versioning3Suite) pollWftAndHandle(
	tv *testvars.TestVars,
	sticky bool,
	async chan<- any,
	handler func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error),
) (*taskpoller.TaskPoller, *workflowservice.RespondWorkflowTaskCompletedResponse) {
	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())
	f := func() *workflowservice.RespondWorkflowTaskCompletedResponse {
		tq := tv.TaskQueue()
		if sticky {
			tq = tv.StickyTaskQueue()
		}
		resp, err := poller.PollWorkflowTask(
			&workflowservice.PollWorkflowTaskQueueRequest{
				DeploymentOptions: tv.WorkerDeploymentOptions(true),
				TaskQueue:         tq,
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
	async chan<- any,
	handler func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondQueryTaskCompletedRequest, error),
) (*taskpoller.TaskPoller, *workflowservice.RespondQueryTaskCompletedResponse) {
	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())
	f := func() *workflowservice.RespondQueryTaskCompletedResponse {
		tq := tv.TaskQueue()
		if sticky {
			tq = tv.StickyTaskQueue()
		}
		resp, err := poller.PollWorkflowTask(
			&workflowservice.PollWorkflowTaskQueueRequest{
				DeploymentOptions: tv.WorkerDeploymentOptions(true),
				TaskQueue:         tq,
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
	async chan<- any,
	handler func(task *workflowservice.PollNexusTaskQueueResponse) (*workflowservice.RespondNexusTaskCompletedRequest, error),
) (*taskpoller.TaskPoller, *workflowservice.RespondNexusTaskCompletedResponse) {
	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())
	f := func() *workflowservice.RespondNexusTaskCompletedResponse {
		tq := tv.TaskQueue()
		if sticky {
			tq = tv.StickyTaskQueue()
		}
		resp, err := poller.PollNexusTask(
			&workflowservice.PollNexusTaskQueueRequest{
				DeploymentOptions: tv.WorkerDeploymentOptions(true),
				TaskQueue:         tq,
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
	async chan<- any,
	handler func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error),
) {
	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())
	f := func() {
		_, err := poller.PollActivityTask(
			&workflowservice.PollActivityTaskQueueRequest{
				DeploymentOptions: tv.WorkerDeploymentOptions(true),
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
	_, _ = poller.PollWorkflowTask(
		&workflowservice.PollWorkflowTaskQueueRequest{
			DeploymentOptions: tv.WorkerDeploymentOptions(versioned),
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
	_, _ = poller.PollActivityTask(
		&workflowservice.PollActivityTaskQueueRequest{
			DeploymentOptions: tv.WorkerDeploymentOptions(versioned),
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
	_, _ = poller.PollNexusTask(
		&workflowservice.PollNexusTaskQueueRequest{
			DeploymentOptions: tv.WorkerDeploymentOptions(versioned),
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
	tv *testvars.TestVars,
) {
	ms, err := s.GetTestCluster().HistoryClient().GetMutableState(
		context.Background(), &historyservice.GetMutableStateRequest{
			NamespaceId: s.NamespaceID().String(),
			Execution:   tv.WorkflowExecution(),
		},
	)
	s.NoError(err)
	s.Equal(tv.StickyTaskQueue().GetName(), ms.StickyTaskQueue.GetName())
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
	unversionedRamp bool,
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
					if d.GetDeployment().Equal(tv.Deployment()) {
						delete(remaining, pt)
					}
				}
				versions := perTypes[int32(pt.tp)].GetDeploymentData().GetVersions()
				for _, d := range versions {
					if d.GetVersion().Equal(tv.DeploymentVersion()) {
						delete(remaining, pt)
					}
				}
				if unversionedRamp && perTypes[int32(pt.tp)].GetDeploymentData().GetUnversionedRampData() != nil {
					delete(remaining, pt)
				}
			}
		}
		return len(remaining) == 0
	}, 10*time.Second, 100*time.Millisecond)
}

func (s *Versioning3Suite) validateBacklogCount(
	tv *testvars.TestVars,
	tqType enumspb.TaskQueueType,
	expectedCount int64,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var resp *workflowservice.DescribeTaskQueueResponse
	var err error

	s.EventuallyWithT(func(t *assert.CollectT) {
		resp, err = s.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
			Namespace:              s.Namespace().String(),
			TaskQueue:              tv.TaskQueue(),
			ApiMode:                enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
			Versions:               nil, // default version, in this case unversioned queue
			TaskQueueTypes:         []enumspb.TaskQueueType{tqType},
			ReportPollers:          false,
			ReportTaskReachability: false,
			ReportStats:            true,
		})
		s.NoError(err)
		s.NotNil(resp)
		s.Equal(1, len(resp.GetVersionsInfo()), "should be 1 because only default/unversioned queue")
		versionInfo := resp.GetVersionsInfo()[""]
		typeInfo, ok := versionInfo.GetTypesInfo()[int32(tqType)]
		s.True(ok)
		a := assert.New(t)
		a.Equal(expectedCount, typeInfo.Stats.GetApproximateBacklogCount())
	}, 6*time.Second, 100*time.Millisecond)
}

func (s *Versioning3Suite) verifyVersioningSAs(
	tv *testvars.TestVars,
	behavior enumspb.VersioningBehavior,
	usedBuilds ...*testvars.TestVars,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s.EventuallyWithT(func(t *assert.CollectT) {
		query := fmt.Sprintf("WorkflowId = '%s' AND TemporalWorkerDeployment = '%s' AND TemporalWorkerDeploymentVersion= '%s' AND TemporalWorkflowVersioningBehavior = '%s'",
			tv.WorkflowID(), tv.DeploymentSeries(), tv.DeploymentVersionString(), behavior.String())
		resp, err := s.FrontendClient().ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: s.Namespace().String(),
			Query:     query,
		})
		a := assert.New(t)
		a.Nil(err)
		if a.NotEmpty(resp.GetExecutions()) {
			w := resp.GetExecutions()[0]
			payload, ok := w.GetSearchAttributes().GetIndexedFields()["BuildIds"]
			a.True(ok)
			searchAttrAny, err := searchattribute.DecodeValue(payload, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, true)
			a.NoError(err)
			var searchAttr []string
			if searchAttrAny != nil {
				searchAttr = searchAttrAny.([]string)
			}
			if behavior == enumspb.VERSIONING_BEHAVIOR_PINNED {
				a.Contains(searchAttr, "pinned:"+tv.DeploymentVersionString())
			}
			for _, b := range usedBuilds {
				a.Contains(searchAttr, "versioned:"+b.BuildID())
			}
			fmt.Println(resp.GetExecutions()[0])
		}
	}, 5*time.Second, 50*time.Millisecond)
}
