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
	"fmt"
	"testing"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
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

var (
	unspecified      = enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED
	pinned           = enumspb.VERSIONING_BEHAVIOR_PINNED
	unpinned         = enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE
	unpinnedOverride = &workflow.VersioningOverride{Behavior: unpinned}
	minPollTime      = common.MinLongPollTimeout + time.Millisecond*200
)

func pinnedOverride(d *deploymentpb.Deployment) *workflow.VersioningOverride {
	return &workflow.VersioningOverride{Behavior: pinned, Deployment: d}
}

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
		dynamicconfig.FrontendEnableDeployments.Key():                  true,
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

			s.startWorkflow(tv, pinnedOverride(tv.Deployment()))

			go func() {
				s.idlePollWorkflow(tv.WithBuildId("other"), true, minPollTime, "other deployment should not receive pinned task")
			}()

			s.idlePollWorkflow(tv, false, minPollTime, "unversioned worker should not receive pinned task")
		})
}

func (s *Versioning3Suite) TestUnpinnedTask_NonCurrentDeployment() {
	s.RunTestWithMatchingBehavior(
		func() {
			tv := testvars.New(s)
			s.startWorkflow(tv, nil)
			s.idlePollWorkflow(tv, true, minPollTime, "non-current versioned poller should not receive unpinned task")
		})
}

func (s *Versioning3Suite) TestUnpinnedTask_OldDeployment() {
	s.RunTestWithMatchingBehavior(
		func() {
			tv := testvars.New(s)
			// previous current deployment
			s.updateTaskQueueDeploymentData(tv.WithBuildId("older"), time.Minute)
			// current deployment
			s.updateTaskQueueDeploymentData(tv, 0)

			s.startWorkflow(tv, nil)

			s.idlePollWorkflow(
				tv.WithBuildId("older"),
				true,
				minPollTime,
				"old deployment should not receive unpinned task",
			)
		},
	)
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

	// current deployment
	s.updateTaskQueueDeploymentData(tv, 0)

	we := s.startWorkflow(tv, nil)

	_, err := s.pollWftAndHandle(tv, false,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.verifyWorkflowVersioning(we, unspecified, nil, nil)
			return respondWftWithActivity(tv, sticky, unpinned, "5"), nil
		})
	s.NoError(err)
	s.verifyWorkflowVersioning(we, unpinned, tv.Deployment(), nil)
	if sticky {
		s.verifyWorkflowStickyQueue(we, tv.StickyTaskQueue())
	}

	_, err = s.pollActivityAndHandle(tv,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondActivity(), nil
		})
	s.NoError(err)
	s.verifyWorkflowVersioning(we, unpinned, tv.Deployment(), nil)

	_, err = s.pollWftAndHandle(tv, sticky,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondCompleteWorkflow(tv, unpinned), nil
		})
	s.NoError(err)
	s.verifyWorkflowVersioning(we, unpinned, tv.Deployment(), nil)
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

	override := pinnedOverride(tv.Deployment())
	we := s.startWorkflow(tv, override)

	_, err := s.pollWftAndHandle(tv, false,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondWftWithActivity(tv, sticky, unpinned, "5"), nil
		})
	s.NoError(err)
	s.verifyWorkflowVersioning(we, unpinned, tv.Deployment(), override)
	if sticky {
		s.verifyWorkflowStickyQueue(we, tv.StickyTaskQueue())
	}

	_, err = s.pollActivityAndHandle(tv,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondActivity(), nil
		})
	s.NoError(err)
	s.verifyWorkflowVersioning(we, unpinned, tv.Deployment(), override)

	_, err = s.pollWftAndHandle(tv, sticky,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondCompleteWorkflow(tv, unpinned), nil
		})
	s.NoError(err)
	s.verifyWorkflowVersioning(we, unpinned, tv.Deployment(), override)
}

func (s *Versioning3Suite) updateTaskQueueDeploymentData(
	tv *testvars.TestVars,
	timeSinceCurrent time.Duration,
) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	lastBecameCurrent := time.Now().Add(-timeSinceCurrent)
	types := []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW, enumspb.TASK_QUEUE_TYPE_ACTIVITY}
	for _, t := range types {
		_, err := s.GetTestCluster().MatchingClient().SyncDeploymentUserData(
			ctx, &matchingservice.SyncDeploymentUserDataRequest{
				NamespaceId:   s.GetNamespaceID(s.Namespace()),
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
	s.waitForDeploymentDataPropagation(ctx, tv)
}

func (s *Versioning3Suite) waitForDeploymentDataPropagation(
	ctx context.Context,
	tv *testvars.TestVars,
) {
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
		remaining[partAndType{i, enumspb.TASK_QUEUE_TYPE_ACTIVITY}] = struct{}{}
		remaining[partAndType{i, enumspb.TASK_QUEUE_TYPE_WORKFLOW}] = struct{}{}
	}
	nsId := s.GetNamespaceID(s.Namespace())
	f, err := tqid.NewTaskQueueFamily(nsId, tv.TaskQueue().GetName())
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
					NamespaceId:   nsId,
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

func (s *Versioning3Suite) verifyWorkflowVersioning(
	we *commonpb.WorkflowExecution,
	behavior enumspb.VersioningBehavior,
	deployment *deploymentpb.Deployment,
	override *workflow.VersioningOverride,
) {
	dwf, err := s.FrontendClient().DescribeWorkflowExecution(
		context.Background(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace(),
			Execution: we,
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
}

func respondActivity() *workflowservice.RespondActivityTaskCompletedRequest {
	return &workflowservice.RespondActivityTaskCompletedRequest{}
}

func respondWftWithActivity(
	tv *testvars.TestVars,
	sticky bool,
	behavior enumspb.VersioningBehavior,
	activityId string,
) *workflowservice.RespondWorkflowTaskCompletedRequest {
	var stickyAttr *taskqueuepb.StickyExecutionAttributes
	if sticky {
		stickyAttr = &taskqueuepb.StickyExecutionAttributes{
			WorkerTaskQueue:        tv.StickyTaskQueue(),
			ScheduleToStartTimeout: durationpb.New(5 * time.Second),
		}
	}
	return &workflowservice.RespondWorkflowTaskCompletedRequest{
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
					ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId:   activityId,
						ActivityType: &commonpb.ActivityType{Name: "act"},
						TaskQueue:    tv.TaskQueue(),
						Input:        payloads.EncodeString("input"),
						// TODO (shahab): tests with forced task forward take multiple seconds. Need to know why?
						ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
						ScheduleToStartTimeout: durationpb.New(10 * time.Second),
						StartToCloseTimeout:    durationpb.New(1 * time.Second),
						HeartbeatTimeout:       durationpb.New(1 * time.Second),
						RequestEagerExecution:  false,
					},
				},
			},
		},
		StickyAttributes:           stickyAttr,
		ReturnNewWorkflowTask:      false,
		ForceCreateNewWorkflowTask: false,
		Deployment:                 tv.Deployment(),
		VersioningBehavior:         behavior,
	}
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
		ReturnNewWorkflowTask:      false,
		ForceCreateNewWorkflowTask: false,
		Deployment:                 tv.Deployment(),
		VersioningBehavior:         behavior,
	}
}

func (s *Versioning3Suite) startWorkflow(
	tv *testvars.TestVars,
	override *workflow.VersioningOverride,
) *commonpb.WorkflowExecution {
	id := tv.WorkflowID()
	wt := "MyWfType"
	identity := "worker1"

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.Namespace(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           tv.TaskQueue(),
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
		VersioningOverride:  override,
	}

	we, err0 := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	s.NoError(err0)
	return &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.GetRunId(),
	}
}

// Name is used by testvars. We use a shorten test name in variables so that physical task queue IDs
// do not grow larger that DB column limit (currently as low as 272 chars).
func (s *Versioning3Suite) Name() string {
	fullName := s.T().Name()
	if len(fullName) <= 300 {
		return fullName
	}
	return fmt.Sprintf("%s-%08x",
		fullName[len(fullName)-25:],
		farm.Fingerprint32([]byte(fullName)),
	)
}

func (s *Versioning3Suite) pollWftAndHandle(
	tv *testvars.TestVars,
	sticky bool,
	handler func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error),
) (*workflowservice.RespondWorkflowTaskCompletedResponse, error) {
	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace())
	d := tv.Deployment()
	tq := tv.TaskQueue()
	if sticky {
		tq = tv.StickyTaskQueue()
	}
	return poller.PollWorkflowTask(
		&workflowservice.PollWorkflowTaskQueueRequest{
			WorkerVersionCapabilities: &commonpb.WorkerVersionCapabilities{
				BuildId:              d.BuildId,
				DeploymentSeriesName: d.SeriesName,
				UseVersioning:        true,
			},
			TaskQueue: tq,
		},
	).HandleTask(tv, handler)
}

func (s *Versioning3Suite) pollActivityAndHandle(
	tv *testvars.TestVars,
	handler func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error),
) (*workflowservice.RespondActivityTaskCompletedResponse, error) {
	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace())
	d := tv.Deployment()
	return poller.PollActivityTask(
		&workflowservice.PollActivityTaskQueueRequest{
			WorkerVersionCapabilities: &commonpb.WorkerVersionCapabilities{
				BuildId:              d.BuildId,
				DeploymentSeriesName: d.SeriesName,
				UseVersioning:        true,
			},
		},
	).HandleTask(tv, handler, taskpoller.WithTimeout(time.Minute))
}

func (s *Versioning3Suite) idlePollWorkflow(
	tv *testvars.TestVars,
	versioned bool,
	timeout time.Duration,
	unexpectedTaskMessage string,
) {
	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace())
	d := tv.Deployment()
	poller.PollWorkflowTask(
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
	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace())
	d := tv.Deployment()
	poller.PollActivityTask(
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
			s.Fail(unexpectedTaskMessage)
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
			NamespaceId: s.GetNamespaceID(s.Namespace()),
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
	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace())
	poller.PollWorkflowTask(
		&workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: tv.StickyTaskQueue(),
		},
	).HandleTask(
		tv,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.Fail("sticky task is not expected")
			return nil, nil
		},
		taskpoller.WithTimeout(minPollTime),
	)
}
