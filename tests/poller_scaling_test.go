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
	"go.temporal.io/sdk/converter"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// pollerScalingTestEnv defines the interface for test environment used in poller scaling tests.
type pollerScalingTestEnv interface {
	testcore.Env
	OperatorClient() operatorservice.OperatorServiceClient
	HttpAPIAddress() string
	FrontendGRPCAddress() string
}

// createSdkClientForPollerScaling creates an SDK client for poller scaling tests.
func createSdkClientForPollerScaling(t *testing.T, s pollerScalingTestEnv, logger log.Logger) sdkclient.Client {
	client, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.FrontendGRPCAddress(),
		Namespace: s.Namespace().String(),
		Logger:    log.NewSdkLogger(logger),
	})
	require.NoError(t, err)
	return client
}

func mustToPayloadForPollerScaling(t *testing.T, v any) *commonpb.Payload {
	conv := converter.GetDefaultDataConverter()
	payload, err := conv.ToPayload(v)
	require.NoError(t, err)
	return payload
}

func TestPollerScalingFunctionalSuite(t *testing.T) {
	t.Run("PollerScalingSimpleBacklog", func(t *testing.T) {
		s := testcore.NewEnv(t,
			testcore.WithDedicatedCluster(),
			testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
			testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
			testcore.WithDynamicConfig(dynamicconfig.MatchingPollerScalingBacklogAgeScaleUp, 50*time.Millisecond),
		)

		sdkClient := createSdkClientForPollerScaling(t, s, s.Logger)
		defer sdkClient.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()

		tq := testcore.RandomizeStr(t.Name())
		endpointName := testcore.RandomizedNexusEndpoint(t.Name())
		s.OverrideDynamicConfig(
			nexusoperations.CallbackURLTemplate,
			"http://"+s.HttpAPIAddress()+"/namespaces/{{.NamespaceName}}/nexus/callback")

		_, err := s.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
			Spec: &nexuspb.EndpointSpec{
				Name: endpointName,
				Target: &nexuspb.EndpointTarget{
					Variant: &nexuspb.EndpointTarget_Worker_{
						Worker: &nexuspb.EndpointTarget_Worker{
							Namespace: s.Namespace().String(),
							TaskQueue: tq,
						},
					},
				},
			},
		})
		require.NoError(t, err)

		// Queue up a couple workflows
		for i := 0; i < 5; i++ {
			_, err := sdkClient.ExecuteWorkflow(
				ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
			require.NoError(t, err)
		}

		// Poll for a task and see attached decision is to scale up b/c of backlog
		feClient := s.FrontendClient()
		// This needs to be done in an eventually loop because nexus endpoints don't become available immediately...
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			resp, err := feClient.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
				Namespace: s.Namespace().String(),
				TaskQueue: &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			})
			require.NoError(ct, err)
			require.NotNil(ct, resp.PollerScalingDecision)
			require.GreaterOrEqual(ct, int32(1), resp.PollerScalingDecision.PollRequestDeltaSuggestion)

			// Start enough activities / nexus tasks to ensure we will see scale up decisions
			commands := make([]*commandpb.Command, 0, 5)
			for i := 0; i < 5; i++ {
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
							Input:     mustToPayloadForPollerScaling(t, "input"),
						},
					},
				},
				)
			}
			_, err = feClient.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
				Identity:  "test",
				TaskToken: resp.TaskToken,
				Commands:  commands,
			})
			require.NoError(ct, err)
		}, 20*time.Second, 200*time.Millisecond)

		// Wait to ensure add rate exceeds dispatch rate & backlog age grows
		tqtyp := enumspb.TASK_QUEUE_TYPE_ACTIVITY
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			res, err := feClient.DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
				Namespace:      s.Namespace().String(),
				TaskQueue:      &taskqueuepb.TaskQueue{Name: tq},
				ApiMode:        enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
				TaskQueueTypes: []enumspb.TaskQueueType{tqtyp},
				ReportStats:    true,
			})
			require.NoError(ct, err)
			stats := res.GetVersionsInfo()[""].TypesInfo[int32(tqtyp)].Stats
			require.GreaterOrEqual(ct, stats.ApproximateBacklogAge.AsDuration(), 200*time.Millisecond)
		}, 20*time.Second, 200*time.Millisecond)

		actResp, err := feClient.PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.NotNil(t, actResp.PollerScalingDecision)
		assert.GreaterOrEqual(t, int32(1), actResp.PollerScalingDecision.PollRequestDeltaSuggestion)

		nexusResp, err := feClient.PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		require.NoError(t, err)
		require.NotNil(t, nexusResp.PollerScalingDecision)
	})

	t.Run("PollerScalingDecisionsAreSeenProbabilistically", func(t *testing.T) {
		s := testcore.NewEnv(t,
			testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 5),
			testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 5),
			testcore.WithDynamicConfig(dynamicconfig.MatchingPollerScalingBacklogAgeScaleUp, 50*time.Millisecond),
		)

		sdkClient := createSdkClientForPollerScaling(t, s, s.Logger)
		defer sdkClient.Close()

		longctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()
		ctx, startWfCancel := context.WithCancel(longctx)
		tq := testcore.RandomizeStr(t.Name())

		// Fire off workflows until polling stops
		go func() {
			for {
				_, _ = sdkClient.ExecuteWorkflow(
					longctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
				select {
				case <-ctx.Done():
					return
				case <-time.NewTimer(200 * time.Millisecond).C:
					continue
				}
			}
		}()

		allScaleDecisions := make([]*taskqueuepb.PollerScalingDecision, 0, 15)
		for i := 0; i < 15; i++ {
			resp, _ := s.FrontendClient().PollWorkflowTaskQueue(longctx, &workflowservice.PollWorkflowTaskQueueRequest{
				Namespace: s.Namespace().String(),
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
		assert.GreaterOrEqual(t, len(nonNilDecisions), 3)
	})

	t.Run("PollerScalingOnCurrentVersionConsidersUnversionedQueueBacklog", func(t *testing.T) {
		s := testcore.NewEnv(t,
			testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
			testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
			testcore.WithDynamicConfig(dynamicconfig.MatchingPollerScalingBacklogAgeScaleUp, 50*time.Millisecond),
		)

		sdkClient := createSdkClientForPollerScaling(t, s, s.Logger)
		defer sdkClient.Close()

		buildID := testcore.RandomizeStr("test-build-id")
		ns := s.Namespace().String()
		testPollerScalingOnPromotedVersionConsidersUnversionedQueueBacklog(
			t, s, sdkClient,
			enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
			buildID,
			func(ctx context.Context, feClient workflowservice.WorkflowServiceClient, deploymentName, buildID string) error {
				_, err := feClient.SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
					Namespace:      ns,
					DeploymentName: deploymentName,
					BuildId:        buildID,
				})
				return err
			},
		)
	})

	t.Run("PollerScalingOnRampingVersionConsidersUnversionedQueueBacklog", func(t *testing.T) {
		s := testcore.NewEnv(t,
			testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
			testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
			testcore.WithDynamicConfig(dynamicconfig.MatchingPollerScalingBacklogAgeScaleUp, 50*time.Millisecond),
		)

		sdkClient := createSdkClientForPollerScaling(t, s, s.Logger)
		defer sdkClient.Close()

		// Use 100% ramp so the ramping version absorbs the entire unversioned backlog.
		ns := s.Namespace().String()

		const rampPercentage = float32(100)
		rampingBuildID := testcore.RandomizeStr("test-ramping-build-id")
		testPollerScalingOnPromotedVersionConsidersUnversionedQueueBacklog(
			t, s, sdkClient,
			enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING,
			rampingBuildID,
			func(ctx context.Context, feClient workflowservice.WorkflowServiceClient, deploymentName, buildID string) error {
				_, err := feClient.SetWorkerDeploymentRampingVersion(ctx, &workflowservice.SetWorkerDeploymentRampingVersionRequest{
					Namespace:      ns,
					DeploymentName: deploymentName,
					BuildId:        buildID,
					Percentage:     rampPercentage,
				})
				return err
			},
		)
	})
}

func testPollerScalingOnPromotedVersionConsidersUnversionedQueueBacklog(
	t *testing.T,
	s pollerScalingTestEnv,
	sdkClient sdkclient.Client,
	expectedStatus enumspb.WorkerDeploymentVersionStatus,
	testBuildID string,
	promoteDeploymentVersion func(ctx context.Context, feClient workflowservice.WorkflowServiceClient, deploymentName, buildID string) error,
) {
	// 1. Create a backlog of unversioned workflows.
	// 2. Set the current/ramping version for a worker-deployment (depending on the test case)
	// 3. Verify that the poller scaling decision reports a 1 since the deployment version (current/ramping) absorbs the unversioned backlog.

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tq := testcore.RandomizeStr("test-poller-scaling-tq")
	feClient := s.FrontendClient()

	const (
		deploymentNamePrefix = "test-deployment"
	)
	deploymentName := testcore.RandomizeStr(deploymentNamePrefix)

	// Queueing up unversioned workflows
	for i := 0; i < 5; i++ {
		_, err := sdkClient.ExecuteWorkflow(
			ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
		require.NoError(t, err)
	}

	// Start a workflow poller with DeploymentOptions so the deployment version is registered
	type pollResult struct {
		resp *workflowservice.PollWorkflowTaskQueueResponse
		err  error
	}
	pollResultCh := make(chan pollResult, 1)

	go func() {
		pollResp, err := feClient.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			DeploymentOptions: &deploymentpb.WorkerDeploymentOptions{
				DeploymentName:       deploymentName,
				BuildId:              testBuildID,
				WorkerVersioningMode: enumspb.WORKER_VERSIONING_MODE_VERSIONED,
			},
		})
		pollResultCh <- pollResult{resp: pollResp, err: err}
	}()

	// Also start a versioned activity poller so that the activity task queue is registered in the version
	pollCtx, pollCancel := context.WithTimeout(context.Background(), 1*time.Minute)
	go func() {
		_, _ = feClient.PollActivityTaskQueue(pollCtx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			DeploymentOptions: &deploymentpb.WorkerDeploymentOptions{
				DeploymentName:       deploymentName,
				BuildId:              testBuildID,
				WorkerVersioningMode: enumspb.WORKER_VERSIONING_MODE_VERSIONED,
			},
		})
	}()

	// This needs to be done in an eventually loop since version existence in the server is eventually consistent.
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		a := require.New(ct)

		// Verify the version status is Inactive and has been registered due to a poller.
		descResp, err := feClient.DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: s.Namespace().String(),
			DeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
				DeploymentName: deploymentName,
				BuildId:        testBuildID,
			},
		})
		a.NoError(err)
		a.NotNil(descResp)
		a.Equal(enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE, descResp.GetWorkerDeploymentVersionInfo().GetStatus())
		a.Len(descResp.GetVersionTaskQueues(), 2) // one for workflow TQ, one for activity TQ

		// Promote the deployment version to either current or ramping.
		err = promoteDeploymentVersion(ctx, feClient, deploymentName, testBuildID)
		a.NoError(err)

		// Verify the version status is the expected status.
		descResp, err = feClient.DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: s.Namespace().String(),
			DeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
				DeploymentName: deploymentName,
				BuildId:        testBuildID,
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
	require.NoError(t, poll.err)
	require.NotNil(t, poll.resp)
	pollResp := poll.resp

	// Start enough activities to ensure we will see scale up decisions. These are scheduled by an unversioned poller.
	commands := make([]*commandpb.Command, 0, 10)
	for i := 0; i < 10; i++ {
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

	_, err := feClient.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test",
		TaskToken: pollResp.TaskToken,
		Commands:  commands,
		// Only doing this for the purpose of this test. Setting the versioning behavior to AutoUpgrade so that
		// we transition the workflow to the deployment version and can poll the activities that are scheduled.
		// If this was not present, all the scheduled activities would have to be dropped and we would have had to
		// complete the transition task.
		VersioningBehavior: enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE,
		DeploymentOptions: &deploymentpb.WorkerDeploymentOptions{
			DeploymentName:       deploymentName,
			BuildId:              testBuildID,
			WorkerVersioningMode: enumspb.WORKER_VERSIONING_MODE_VERSIONED,
		},
	})
	require.NoError(t, err)

	// Wait to ensure add rate exceeds dispatch rate & backlog age grows.
	tqtyp := enumspb.TASK_QUEUE_TYPE_ACTIVITY
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		a := require.New(ct)
		res, err := feClient.DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
			Namespace:     s.Namespace().String(),
			TaskQueue:     &taskqueuepb.TaskQueue{Name: tq},
			TaskQueueType: tqtyp,
			ReportStats:   true,
		})
		require.NoError(ct, err)
		stats := res.GetStats()
		a.GreaterOrEqual(stats.ApproximateBacklogAge.AsDuration(), 200*time.Millisecond)

		// Describe the deployment version to see the activity stats attributed to it.
		versionDescResp, err := feClient.DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: s.Namespace().String(),
			DeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
				DeploymentName: deploymentName,
				BuildId:        testBuildID,
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
	actResp, err := feClient.PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		DeploymentOptions: &deploymentpb.WorkerDeploymentOptions{
			DeploymentName:       deploymentName,
			BuildId:              testBuildID,
			WorkerVersioningMode: enumspb.WORKER_VERSIONING_MODE_VERSIONED,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, actResp.PollerScalingDecision)
	require.Equal(t, int32(1), actResp.PollerScalingDecision.PollRequestDeltaSuggestion)
}
