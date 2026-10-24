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
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type PollerScalingIntegSuite struct {
	parallelsuite.Suite[*PollerScalingIntegSuite]
}

func TestPollerScalingFunctionalSuite(t *testing.T) {
	testcore.UseSuiteScopedCluster(t)                                //nolint:staticcheck // SA1019: suite reuses one worker-service cluster to avoid per-test cluster churn.
	parallelsuite.RunLegacySequential(t, &PollerScalingIntegSuite{}) //nolint:staticcheck // SA1019: suite reuses one worker-service cluster to avoid per-test cluster churn.
}

func (s *PollerScalingIntegSuite) setupEnv(opts ...testcore.TestOption) *testcore.TestEnv {
	opts = append([]testcore.TestOption{
		// Force one partition so we can reliably see the backlog
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
		testcore.WithDynamicConfig(dynamicconfig.MatchingPollerScalingBacklogAgeScaleUp, 50*time.Millisecond),

		// Keep deployment versions short because worker-deployment system workflow IDs must fit into 255 characters.
		testcore.WithTestVars(func(tv *testvars.TestVars) *testvars.TestVars {
			return tv.WithDeploymentSeries("poller-scaling").WithBuildID("v1")
		}),
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
		require.Equal(t, int32(1), resp.PollerScalingDecision.PollRequestDeltaSuggestion)

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
	s.Equal(int32(1), actResp.PollerScalingDecision.PollRequestDeltaSuggestion)

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
		func(env *testcore.TestEnv, tv *testvars.TestVars) error {
			_, err := env.FrontendClient().SetWorkerDeploymentCurrentVersion(s.Context(), &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
				Namespace:      env.Namespace().String(),
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
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
		func(env *testcore.TestEnv, tv *testvars.TestVars) error {
			_, err := env.FrontendClient().SetWorkerDeploymentRampingVersion(s.Context(), &workflowservice.SetWorkerDeploymentRampingVersionRequest{
				Namespace:      env.Namespace().String(),
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
				Percentage:     rampPercentage,
			})
			return err
		},
	)
}

func (s *PollerScalingIntegSuite) testPollerScalingOnPromotedVersionConsidersUnversionedQueueBacklog(
	expectedStatus enumspb.WorkerDeploymentVersionStatus,
	promoteDeploymentVersion func(env *testcore.TestEnv, tv *testvars.TestVars) error,
) {
	// 1. Create a backlog of unversioned workflows.
	// 2. Set the current/ramping version for a worker-deployment (depending on the test case)
	// 3. Verify that the poller scaling decision reports a 1 since the deployment version (current/ramping) absorbs the unversioned backlog.

	env := s.setupEnv()
	tq := testcore.RandomizeStr("test-poller-scaling-tq")
	tv := env.Tv()

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
				DeploymentName:       tv.DeploymentSeries(),
				BuildId:              tv.BuildID(),
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
				DeploymentName:       tv.DeploymentSeries(),
				BuildId:              tv.BuildID(),
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
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
			},
		})
		a.NoError(err)
		a.NotNil(descResp)
		a.Equal(enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE, descResp.GetWorkerDeploymentVersionInfo().GetStatus())
		a.Len(descResp.GetVersionTaskQueues(), 2) // one for workflow TQ, one for activity TQ

		// Promote the deployment version to either current or ramping.
		err = promoteDeploymentVersion(env, tv)
		a.NoError(err)

		// Verify the version status is the expected status.
		descResp, err = env.FrontendClient().DescribeWorkerDeploymentVersion(s.Context(), &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: env.Namespace().String(),
			DeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
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
			DeploymentName:       tv.DeploymentSeries(),
			BuildId:              tv.BuildID(),
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
				DeploymentName: tv.DeploymentSeries(),
				BuildId:        tv.BuildID(),
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
			DeploymentName:       tv.DeploymentSeries(),
			BuildId:              tv.BuildID(),
			WorkerVersioningMode: enumspb.WORKER_VERSIONING_MODE_VERSIONED,
		},
	})
	s.NoError(err)
	s.NotNil(actResp.PollerScalingDecision)
	s.Equal(int32(1), actResp.PollerScalingDecision.PollRequestDeltaSuggestion)
}

// The following tests cover the hold and scale-down branches of the poller scaling decision, plus the
// v2 delay signal. Each opts into the poller_scale_decision metric so the reason behind a decision
// (or a withheld one) can be asserted, not just its presence.

func (s *PollerScalingIntegSuite) TestPollerScalingScaleDownWhenIdle() {
	env := s.setupEnv(
		testcore.WithDynamicConfig(dynamicconfig.MatchingEnablePollerScalingDecisionMetrics, true),
		testcore.WithDynamicConfig(dynamicconfig.MatchingPollerScalingWaitTime, 200*time.Millisecond),
	)
	capture := env.StartNamespaceMetricCapture()
	tq := testcore.RandomizeStr(s.T().Name())

	resp := s.pollAfterIdle(env, tq, 400*time.Millisecond)
	s.NotNil(resp.PollerScalingDecision)
	s.Equal(int32(-1), resp.PollerScalingDecision.PollRequestDeltaSuggestion)
	s.GreaterOrEqual(pollerScaleDecisionCount(capture, metrics.PollerScaleDecisionDown, metrics.PollerScaleReasonIdle), 1)
}

func (s *PollerScalingIntegSuite) TestPollerScalingNoScaleDownForBacklogTasks() {
	env := s.setupEnv(
		testcore.WithDynamicConfig(dynamicconfig.MatchingEnablePollerScalingDecisionMetrics, true),
		// Every poll waits at least zero, so any task not exempted from scale-down would get a -1.
		testcore.WithDynamicConfig(dynamicconfig.MatchingPollerScalingWaitTime, time.Duration(0)),
	)
	capture := env.StartNamespaceMetricCapture()
	tq := testcore.RandomizeStr(s.T().Name())

	const numTasks = 5
	s.startBacklog(env, tq, numTasks)

	scaleUps := 0
	for range numTasks {
		resp := s.pollWorkflowTask(env, tq)
		s.NotEmpty(resp.GetTaskToken())
		if d := resp.GetPollerScalingDecision(); d != nil {
			s.NotEqual(int32(-1), d.PollRequestDeltaSuggestion, "backlog tasks must never suggest scaling down")
			if d.PollRequestDeltaSuggestion == 1 {
				scaleUps++
			}
		}
	}
	// The backlog is older than the scale-up threshold, so the delay signal should fire at least once.
	s.GreaterOrEqual(scaleUps, 1)
	s.Zero(pollerScaleDecisionCount(capture, metrics.PollerScaleDecisionDown, metrics.PollerScaleReasonIdle))
}

func (s *PollerScalingIntegSuite) TestPollerScalingDecisionsRateLimited() {
	env := s.setupEnv(
		testcore.WithDynamicConfig(dynamicconfig.MatchingEnablePollerScalingDecisionMetrics, true),
		testcore.WithDynamicConfig(dynamicconfig.MatchingPollerScalingWaitTime, 200*time.Millisecond),
		// Low enough that the limiter's burst can't cover a single decision.
		testcore.WithDynamicConfig(dynamicconfig.MatchingPollerScalingDecisionsPerSecond, 0.001),
	)
	capture := env.StartNamespaceMetricCapture()
	tq := testcore.RandomizeStr(s.T().Name())

	const numTasks = 5
	s.startBacklog(env, tq, numTasks)

	nonNil := 0
	for range numTasks {
		resp := s.pollWorkflowTask(env, tq)
		s.NotEmpty(resp.GetTaskToken())
		if resp.GetPollerScalingDecision() != nil {
			nonNil++
		}
	}
	s.LessOrEqual(nonNil, 1)
	s.GreaterOrEqual(pollerScaleDecisionCount(capture, metrics.PollerScaleDecisionHold, metrics.PollerScaleReasonRateLimited), numTasks-1)

	// Scale-down is decided before the limiter is consulted, so an idle poller still gets one.
	resp := s.pollAfterIdle(env, tq, 400*time.Millisecond)
	s.NotNil(resp.PollerScalingDecision)
	s.Equal(int32(-1), resp.PollerScalingDecision.PollRequestDeltaSuggestion)
}

func (s *PollerScalingIntegSuite) TestPollerScalingHoldsWhenTaskQueueRateLimited() {
	env := s.setupEnv(
		testcore.WithDynamicConfig(dynamicconfig.MatchingEnablePollerScalingDecisionMetrics, true),
	)
	capture := env.StartNamespaceMetricCapture()
	tq := testcore.RandomizeStr(s.T().Name())

	// Rate limits are only allowed on activity queues.
	_, err := env.FrontendClient().UpdateTaskQueueConfig(s.Context(), &workflowservice.UpdateTaskQueueConfigRequest{
		Namespace:     env.Namespace().String(),
		Identity:      "test",
		TaskQueue:     tq,
		TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY,
		UpdateQueueRateLimit: &workflowservice.UpdateTaskQueueConfigRequest_RateLimitUpdate{
			RateLimit: &taskqueuepb.RateLimit{RequestsPerSecond: 1},
			Reason:    "poller scaling test",
		},
	})
	s.NoError(err)

	// Build an activity backlog by scheduling activities from a single workflow task.
	const numActivities = 10
	_, err = env.SdkClient().ExecuteWorkflow(
		s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)
	wfResp := s.pollWorkflowTask(env, tq)
	s.NotEmpty(wfResp.GetTaskToken())
	commands := make([]*commandpb.Command, 0, numActivities)
	for i := range numActivities {
		commands = append(commands, &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
				ActivityId:          fmt.Sprintf("%v", i),
				ActivityType:        &commonpb.ActivityType{Name: "test-activity-type"},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				Input:               payloads.EncodeString("test-input"),
				StartToCloseTimeout: durationpb.New(time.Minute),
			}},
		})
	}
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(s.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: wfResp.TaskToken,
		Commands:  commands,
	})
	s.NoError(err)

	// Dispatch is capped at 1 RPS, so polls block on the rate limiter and the queue reports rate limiting
	// as active. A backlog that adding pollers can't drain any faster must not produce a scale-up.
	// The rate-limited tracker is a sliding window, so assert on the metric rather than per-poll timing.
	s.EventuallyWithT(func(t *assert.CollectT) {
		resp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  "test",
		})
		require.NoError(t, err)
		require.NotEmpty(t, resp.GetTaskToken())
		require.Positive(t, pollerScaleDecisionCount(capture, metrics.PollerScaleDecisionHold, metrics.PollerScaleReasonTaskQueueRateLimited))
	}, 20*time.Second, 10*time.Millisecond)
}

func (s *PollerScalingIntegSuite) TestPollerScalingNexusRatioScaleUp_SignalsV1() {
	s.testPollerScalingNexusRatioScaleUp(false)
}

func (s *PollerScalingIntegSuite) TestPollerScalingNexusRatioScaleUp_SignalsV2() {
	s.testPollerScalingNexusRatioScaleUp(true)
}

// Nexus tasks are never backlogged, so the ratio signal is what scales up Nexus pollers. The delay
// signal is disabled here so a scale-up can only come from the ratio of adds to dispatches (v1) or
// sync matches (v2).
func (s *PollerScalingIntegSuite) testPollerScalingNexusRatioScaleUp(useSignalsV2 bool) {
	env := s.setupEnv(
		testcore.WithDynamicConfig(dynamicconfig.MatchingEnablePollerScalingDecisionMetrics, true),
		testcore.WithDynamicConfig(dynamicconfig.MatchingUseSignalsV2ForPollerScaling, useSignalsV2),
		testcore.WithDynamicConfig(dynamicconfig.MatchingPollerScalingBacklogAgeScaleUp, time.Hour),
	)
	capture := env.StartNamespaceMetricCapture()
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

	// Queue a few workflows; each attempt below consumes one workflow task.
	for range 5 {
		_, err := env.SdkClient().ExecuteWorkflow(
			s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
		s.NoError(err)
	}

	// Schedule a batch of Nexus operations with no Nexus poller present. Retry because the new
	// endpoint isn't visible to history immediately.
	const numOperations = 10
	s.EventuallyWithT(func(t *assert.CollectT) {
		resp := s.pollWorkflowTask(env, tq)
		require.NotEmpty(t, resp.GetTaskToken())
		commands := make([]*commandpb.Command, 0, numOperations)
		for range numOperations {
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
			})
		}
		_, err := env.FrontendClient().RespondWorkflowTaskCompleted(s.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
			Identity:  "test",
			TaskToken: resp.TaskToken,
			Commands:  commands,
		})
		require.NoError(t, err)
	}, 20*time.Second, 200*time.Millisecond)

	// Operations are added faster than this single poller takes them, so the ratio signal fires.
	// Poll until a decision comes back, since the operations reach matching asynchronously.
	s.EventuallyWithT(func(t *assert.CollectT) {
		ctx, cancel := context.WithTimeout(s.Context(), 5*time.Second)
		defer cancel()
		resp, err := env.FrontendClient().PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  "test",
		})
		require.NoError(t, err)
		require.NotEmpty(t, resp.GetTaskToken())
		require.NotNil(t, resp.GetPollerScalingDecision())
		require.Equal(t, int32(1), resp.GetPollerScalingDecision().GetPollRequestDeltaSuggestion())
	}, 30*time.Second, 100*time.Millisecond)

	s.GreaterOrEqual(pollerScaleDecisionCount(capture, metrics.PollerScaleDecisionUp, metrics.PollerScaleReasonRatio), 1)
	s.Zero(pollerScaleDecisionCount(capture, metrics.PollerScaleDecisionUp, metrics.PollerScaleReasonDelay))
}

// startBacklog starts n workflows with no pollers present and waits until their tasks are persisted, so
// they are later dispatched from the DB backlog rather than sync-matched.
func (s *PollerScalingIntegSuite) startBacklog(env *testcore.TestEnv, tq string, n int) {
	for range n {
		_, err := env.SdkClient().ExecuteWorkflow(
			s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
		s.NoError(err)
	}
	s.EventuallyWithT(func(t *assert.CollectT) {
		res, err := env.FrontendClient().DescribeTaskQueue(s.Context(), &workflowservice.DescribeTaskQueueRequest{
			Namespace:     env.Namespace().String(),
			TaskQueue:     &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			ReportStats:   true,
		})
		require.NoError(t, err)
		require.GreaterOrEqual(t, res.GetStats().GetApproximateBacklogCount(), int64(n))
	}, 20*time.Second, 100*time.Millisecond)
}

func (s *PollerScalingIntegSuite) pollWorkflowTask(env *testcore.TestEnv, tq string) *workflowservice.PollWorkflowTaskQueueResponse {
	resp, err := env.FrontendClient().PollWorkflowTaskQueue(s.Context(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "test",
	})
	s.NoError(err)
	return resp
}

// pollAfterIdle parks a workflow poller on an empty queue for at least idle, then starts a workflow so
// its task sync-matches to that poller, and returns the poll response.
func (s *PollerScalingIntegSuite) pollAfterIdle(env *testcore.TestEnv, tq string, idle time.Duration) *workflowservice.PollWorkflowTaskQueueResponse {
	const identity = "idle-poller"
	respCh := make(chan *workflowservice.PollWorkflowTaskQueueResponse, 1)
	go func() {
		resp, err := env.FrontendClient().PollWorkflowTaskQueue(s.Context(), &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  identity,
		})
		s.NoError(err)
		respCh <- resp
	}()

	s.EventuallyWithT(func(t *assert.CollectT) {
		res, err := env.FrontendClient().DescribeTaskQueue(s.Context(), &workflowservice.DescribeTaskQueueRequest{
			Namespace:     env.Namespace().String(),
			TaskQueue:     &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		})
		require.NoError(t, err)
		found := false
		for _, p := range res.GetPollers() {
			found = found || p.GetIdentity() == identity
		}
		require.True(t, found)
	}, 10*time.Second, 50*time.Millisecond)
	time.Sleep(idle) //nolint:forbidigo // the poll has to wait past PollerScalingWaitTime to look idle

	_, err := env.SdkClient().ExecuteWorkflow(
		s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)

	select {
	case resp := <-respCh:
		s.NotEmpty(resp.GetTaskToken())
		return resp
	case <-s.Context().Done():
		s.FailNow("idle poll did not return")
		return nil
	}
}

func pollerScaleDecisionCount(capture *testcore.NamespaceMetricCapture, decision string, reason metrics.ReasonString) int {
	return len(capture.CollectMetric(metrics.PollerScaleDecisionCounter.Name(), func(rec *metricstest.CapturedRecording) bool {
		return rec.Tags["decision"] == decision && rec.Tags["reason"] == string(reason)
	}))
}
