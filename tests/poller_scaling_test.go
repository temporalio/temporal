package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type PollerScalingIntegSuite struct {
	parallelsuite.Suite[*PollerScalingIntegSuite]
}

func TestPollerScalingFunctionalSuite(t *testing.T) {
	parallelsuite.Run(t, &PollerScalingIntegSuite{})
}

func (s *PollerScalingIntegSuite) setupEnv(opts ...testcore.TestOption) *testcore.TestEnv {
	opts = append([]testcore.TestOption{
		testcore.WithWorkerService("worker-deployment version registration"),

		// Force one partition so we can reliably see the backlog
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
		testcore.WithDynamicConfig(dynamicconfig.MatchingPollerScalingBacklogAgeScaleUp, 50*time.Millisecond),
	}, opts...)

	return testcore.NewEnv(s.T(), opts...)
}

func (s *PollerScalingIntegSuite) TestPollerScalingSimpleBacklog() {
	env := s.setupEnv()

	tq := testcore.RandomizeStr(s.T().Name())
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())

	_, err := env.GetTestCluster().OperatorClient().CreateNexusEndpoint(s.Context(), &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_Worker_{
					Worker: &nexuspb.EndpointTarget_Worker{
						Namespace: env.Namespace().String(),
						TaskQueue: tq,
					},
				},
			},
		},
	})
	s.NoError(err)

	// Queue up a couple workflows
	for range 5 {
		_, err := env.SdkClient().ExecuteWorkflow(
			s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
		s.NoError(err)
	}

	// Poll for a task and see attached decision is to scale up b/c of backlog
	feClient := env.FrontendClient()
	// This needs to be done in an eventually loop because nexus endpoints don't become available immediately...
	s.EventuallyWithT(func(t *assert.CollectT) {
		resp, err := feClient.PollWorkflowTaskQueue(s.Context(), &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.NotNil(t, resp.PollerScalingDecision)
		require.GreaterOrEqual(t, int32(1), resp.PollerScalingDecision.PollRequestDeltaSuggestion)

		// Start enough activities / nexus tasks to ensure we will see scale up decisions
		commands := make([]*commandpb.Command, 0, 5)
		for i := range 5 {
			commands = append(commands, &commandpb.Command{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:          fmt.Sprintf("%v", i),
					ActivityType:        &commonpb.ActivityType{Name: "test-activity-type"},
					TaskQueue:           &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:               payloads.EncodeString("test-input"),
					StartToCloseTimeout: durationpb.New(10 * time.Second),
				}},
			})
			commands = append(commands, &commandpb.Command{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Endpoint:  endpointName,
						Service:   "service",
						Operation: "operation",
						Input:     testcore.MustToPayload(s.T(), "input"),
					},
				},
			},
			)
		}
		_, err = feClient.RespondWorkflowTaskCompleted(s.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
			Identity:  "test",
			TaskToken: resp.TaskToken,
			Commands:  commands,
		})
		require.NoError(t, err)
	}, 20*time.Second, 200*time.Millisecond)

	// Wait to ensure add rate exceeds dispatch rate & backlog age grows
	tqtyp := enumspb.TASK_QUEUE_TYPE_ACTIVITY
	s.EventuallyWithT(func(t *assert.CollectT) {
		res, err := feClient.DescribeTaskQueue(s.Context(), &workflowservice.DescribeTaskQueueRequest{
			Namespace:      env.Namespace().String(),
			TaskQueue:      &taskqueuepb.TaskQueue{Name: tq},
			ApiMode:        enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
			TaskQueueTypes: []enumspb.TaskQueueType{tqtyp},
			ReportStats:    true,
		})
		require.NoError(t, err)
		stats := res.GetVersionsInfo()[""].TypesInfo[int32(tqtyp)].Stats
		require.GreaterOrEqual(t, stats.ApproximateBacklogAge.AsDuration(), 200*time.Millisecond)
	}, 20*time.Second, 200*time.Millisecond)

	actResp, err := feClient.PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
	})
	s.NoError(err)
	s.NotNil(actResp.PollerScalingDecision)
	s.GreaterOrEqual(int32(1), actResp.PollerScalingDecision.PollRequestDeltaSuggestion)

	nexusResp, err := feClient.PollNexusTaskQueue(s.Context(), &workflowservice.PollNexusTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
	})
	s.NoError(err)
	s.NotNil(nexusResp.PollerScalingDecision)
}

// Here we verify that, even with multiple partitions, pollers see scaling decisions at least often enough
// that SDKs have information to work with. Benchmark style testing that exists on the SDK side is better at ensuring
// that the desired outcomes actually happen.
func (s *PollerScalingIntegSuite) TestPollerScalingDecisionsAreSeenProbabilistically() {
	env := s.setupEnv(
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 5),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 5),
	)

	ctx, startWfCancel := context.WithCancel(s.Context())
	tq := testcore.RandomizeStr(s.T().Name())

	// Fire off workflows until polling stops
	go func() {
		for {
			_, _ = env.SdkClient().ExecuteWorkflow(
				s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
			select {
			case <-ctx.Done():
				return
			case <-time.NewTimer(200 * time.Millisecond).C:
				continue
			}
		}
	}()

	allScaleDecisions := make([]*taskqueuepb.PollerScalingDecision, 0, 15)
	for range 15 {
		resp, _ := env.FrontendClient().PollWorkflowTaskQueue(s.Context(), &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		if resp != nil {
			allScaleDecisions = append(allScaleDecisions, resp.PollerScalingDecision)
		}
		// Poll slightly less frequently than we insert tasks
		time.Sleep(300 * time.Millisecond) //nolint:forbidigo
	}
	startWfCancel()

	// We must have seen at least a handful of non-nil scaling decisions
	nonNilDecisions := util.FilterSlice(allScaleDecisions, func(d *taskqueuepb.PollerScalingDecision) bool { return d != nil })
	s.GreaterOrEqual(len(nonNilDecisions), 3)
}

// The following tests verify poller scaling decisions work with worker-versioning based concepts.
func (s *PollerScalingIntegSuite) TestPollerScalingOnCurrentVersionConsidersUnversionedQueueBacklog() {
	s.testPollerScalingOnPromotedVersionConsidersUnversionedQueueBacklog(
		enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
		func(env *testcore.TestEnv) error {
			_, err := env.FrontendClient().SetWorkerDeploymentCurrentVersion(s.Context(), &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
				Namespace:      env.Namespace().String(),
				DeploymentName: env.Tv().DeploymentSeries(),
				BuildId:        env.Tv().BuildID(),
			})
			return err
		},
	)
}

func (s *PollerScalingIntegSuite) TestPollerScalingOnRampingVersionConsidersUnversionedQueueBacklog() {
	// Use 100% ramp so the ramping version absorbs the entire unversioned backlog.
	const rampPercentage = float32(100)
	s.testPollerScalingOnPromotedVersionConsidersUnversionedQueueBacklog(
		enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING,
		func(env *testcore.TestEnv) error {
			_, err := env.FrontendClient().SetWorkerDeploymentRampingVersion(s.Context(), &workflowservice.SetWorkerDeploymentRampingVersionRequest{
				Namespace:      env.Namespace().String(),
				DeploymentName: env.Tv().DeploymentSeries(),
				BuildId:        env.Tv().BuildID(),
				Percentage:     rampPercentage,
			})
			return err
		},
	)
}

func (s *PollerScalingIntegSuite) testPollerScalingOnPromotedVersionConsidersUnversionedQueueBacklog(
	expectedStatus enumspb.WorkerDeploymentVersionStatus,
	promoteDeploymentVersion func(env *testcore.TestEnv) error,
) {
	// 1. Create a backlog of unversioned workflows.
	// 2. Set the current/ramping version for a worker-deployment (depending on the test case)
	// 3. Verify that the poller scaling decision reports a 1 since the deployment version (current/ramping) absorbs the unversioned backlog.

	env := s.setupEnv()
	tq := testcore.RandomizeStr("test-poller-scaling-tq")

	// Queueing up unversioned workflows
	for range 5 {
		_, err := env.SdkClient().ExecuteWorkflow(
			s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
		s.NoError(err)
	}

	// Start a workflow poller with DeploymentOptions so the deployment version is registered
	type pollResult struct {
		resp *workflowservice.PollWorkflowTaskQueueResponse
		err  error
	}
	pollResultCh := make(chan pollResult, 1)

	go func() {
		pollResp, err := env.FrontendClient().PollWorkflowTaskQueue(s.Context(), &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			DeploymentOptions: &deploymentpb.WorkerDeploymentOptions{
				DeploymentName:       env.Tv().DeploymentSeries(),
				BuildId:              env.Tv().BuildID(),
				WorkerVersioningMode: enumspb.WORKER_VERSIONING_MODE_VERSIONED,
			},
		})
		pollResultCh <- pollResult{resp: pollResp, err: err}
	}()

	// Also start a versioned activity poller so that the activity task queue is registered in the version
	pollCtx, pollCancel := context.WithTimeout(context.Background(), 1*time.Minute)
	go func() {
		_, _ = env.FrontendClient().PollActivityTaskQueue(pollCtx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			DeploymentOptions: &deploymentpb.WorkerDeploymentOptions{
				DeploymentName:       env.Tv().DeploymentSeries(),
				BuildId:              env.Tv().BuildID(),
				WorkerVersioningMode: enumspb.WORKER_VERSIONING_MODE_VERSIONED,
			},
		})
	}()

	// This needs to be done in an eventually loop since version existence in the server is eventually consistent.
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)

		// Verify the version status is Inactive and has been registered due to a poller.
		descResp, err := env.FrontendClient().DescribeWorkerDeploymentVersion(s.Context(), &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: env.Namespace().String(),
			DeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
				DeploymentName: env.Tv().DeploymentSeries(),
				BuildId:        env.Tv().BuildID(),
			},
		})
		a.NoError(err)
		a.NotNil(descResp)
		a.Equal(enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE, descResp.GetWorkerDeploymentVersionInfo().GetStatus())
		a.Len(descResp.GetVersionTaskQueues(), 2) // one for workflow TQ, one for activity TQ

		// Promote the deployment version to either current or ramping.
		err = promoteDeploymentVersion(env)
		a.NoError(err)

		// Verify the version status is the expected status.
		descResp, err = env.FrontendClient().DescribeWorkerDeploymentVersion(s.Context(), &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: env.Namespace().String(),
			DeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
				DeploymentName: env.Tv().DeploymentSeries(),
				BuildId:        env.Tv().BuildID(),
			},
		})
		a.NoError(err)
		a.NotNil(descResp)
		a.Equal(expectedStatus, descResp.GetWorkerDeploymentVersionInfo().GetStatus())
	}, 20*time.Second, 200*time.Millisecond)

	// Stop the activity poller to grow the backlog and to see poller scaling decisions.
	pollCancel()

	// Wait for the workflow poller to poll and receive a task.
	poll := <-pollResultCh
	s.NoError(poll.err)
	s.NotNil(poll.resp)
	pollResp := poll.resp

	// Start enough activities to ensure we will see scale up decisions. These are scheduled by an unversioned poller.
	commands := make([]*commandpb.Command, 0, 10)
	for i := range 10 {
		commands = append(commands, &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
				ActivityId:          fmt.Sprintf("%v", i),
				ActivityType:        &commonpb.ActivityType{Name: "test-activity-type"},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				Input:               payloads.EncodeString("test-input"),
				StartToCloseTimeout: durationpb.New(10 * time.Second),
			}},
		})
	}

	_, err := env.FrontendClient().RespondWorkflowTaskCompleted(s.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands:  commands,
		// Only doing this for the purpose of this test. Setting the versioning behavior to AutoUpgrade so that
		// we transition the workflow to the deployment version and can poll the activities that are scheduled.
		// If this was not present, all the scheduled activities would have to be dropped and we would have had to
		// complete the transition task.
		VersioningBehavior: enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE,
		DeploymentOptions: &deploymentpb.WorkerDeploymentOptions{
			DeploymentName:       env.Tv().DeploymentSeries(),
			BuildId:              env.Tv().BuildID(),
			WorkerVersioningMode: enumspb.WORKER_VERSIONING_MODE_VERSIONED,
		},
	})
	s.NoError(err)

	// Wait to ensure add rate exceeds dispatch rate & backlog age grows.
	tqtyp := enumspb.TASK_QUEUE_TYPE_ACTIVITY
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		res, err := env.FrontendClient().DescribeTaskQueue(s.Context(), &workflowservice.DescribeTaskQueueRequest{
			Namespace:     env.Namespace().String(),
			TaskQueue:     &taskqueuepb.TaskQueue{Name: tq},
			TaskQueueType: tqtyp,
			ReportStats:   true,
		})
		require.NoError(t, err)
		stats := res.GetStats()
		a.GreaterOrEqual(stats.ApproximateBacklogAge.AsDuration(), 200*time.Millisecond)

		// Describe the deployment version to see the activity stats attributed to it.
		versionDescResp, err := env.FrontendClient().DescribeWorkerDeploymentVersion(s.Context(), &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: env.Namespace().String(),
			DeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
				DeploymentName: env.Tv().DeploymentSeries(),
				BuildId:        env.Tv().BuildID(),
			},
			ReportTaskQueueStats: true,
		})
		a.NoError(err)
		a.NotNil(versionDescResp)

		found := false
		for _, info := range versionDescResp.GetVersionTaskQueues() {
			if info.Type == enumspb.TASK_QUEUE_TYPE_ACTIVITY {
				a.Equal(int64(10), info.GetStats().GetApproximateBacklogCount())
				found = true
			}
		}
		a.True(found)
	}, 20*time.Second, 200*time.Millisecond)

	// Start an activity poller that's in the deployment version. This should see a scale up decision since the backlog
	// is absorbed by the deployment version.
	actResp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		DeploymentOptions: &deploymentpb.WorkerDeploymentOptions{
			DeploymentName:       env.Tv().DeploymentSeries(),
			BuildId:              env.Tv().BuildID(),
			WorkerVersioningMode: enumspb.WORKER_VERSIONING_MODE_VERSIONED,
		},
	})
	s.NoError(err)
	s.NotNil(actResp.PollerScalingDecision)
	s.Equal(int32(1), actResp.PollerScalingDecision.PollRequestDeltaSuggestion)
}
