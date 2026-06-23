package tests

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkworker "go.temporal.io/sdk/worker"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/worker/workerdeployment"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	WorkerDeploymentSuite struct {
		parallelsuite.Suite[*WorkerDeploymentSuite]
	}
)

func TestWorkerDeploymentSuite(t *testing.T) {
	testcore.UseSuiteScopedCluster(t)                              //nolint:staticcheck // SA1019: suite reuses one worker-service cluster to avoid per-test cluster churn.
	parallelsuite.RunLegacySequential(t, &WorkerDeploymentSuite{}) //nolint:staticcheck // SA1019: suite reuses one worker-service cluster to avoid per-test cluster churn.
}

// newTestEnv creates a TestEnv with the dynamic config this suite needs.
// Additional per-test options may be passed in opts.
func (s *WorkerDeploymentSuite) newTestEnv(opts ...testcore.TestOption) *testcore.TestEnv {
	baseOpts := []testcore.TestOption{
		testcore.WithDynamicConfig(dynamicconfig.MatchingDeploymentWorkflowVersion, int(workerdeployment.VersionDataRevisionNumber)),

		// Make sure we don't hit the rate limiter in tests
		testcore.WithDynamicConfig(dynamicconfig.FrontendGlobalNamespaceNamespaceReplicationInducingAPIsRPS, 1000),
		testcore.WithDynamicConfig(dynamicconfig.FrontendMaxNamespaceNamespaceReplicationInducingAPIsBurstRatioPerInstance, 1),

		// Make drainage happen sooner
		testcore.WithDynamicConfig(dynamicconfig.VersionDrainageStatusRefreshInterval, testVersionDrainageRefreshInterval),
		testcore.WithDynamicConfig(dynamicconfig.VersionDrainageStatusVisibilityGracePeriod, testVersionDrainageVisibilityGracePeriod),

		// To increase the rate at which the per-ns worker can consume tasks from a task queue. Required since
		// tests in this suite create a lot of tasks and expect them to be consumed quickly.
		testcore.WithDynamicConfig(dynamicconfig.WorkerPerNamespaceWorkerOptions, sdkworker.Options{
			MaxConcurrentWorkflowTaskPollers: 100,
			MaxConcurrentActivityTaskPollers: 100,
		}),

		testcore.WithDynamicConfig(dynamicconfig.MatchingMaxTaskQueuesInDeploymentVersion, 1000),

		// Keep deployment versions short because worker-deployment system workflow IDs must fit into 255 characters (database constraint).
		testcore.WithTestVars(func(tv *testvars.TestVars) *testvars.TestVars {
			return tv.WithDeploymentSeries("wd").WithBuildID("b")
		}),
	}
	return testcore.NewEnv(s.T(), append(baseOpts, opts...)...)
}

// pollFromDeployment calls PollWorkflowTaskQueue to start deployment related workflows
func (s *WorkerDeploymentSuite) pollFromDeployment(env *testcore.TestEnv, tv *testvars.TestVars) {
	s.pollFromDeploymentUntil(s.Context(), env, tv)
}

func (s *WorkerDeploymentSuite) pollFromDeploymentUntil(ctx context.Context, env *testcore.TestEnv, tv *testvars.TestVars) {
	_, _ = env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace:         env.Namespace().String(),
		TaskQueue:         tv.TaskQueue(),
		Identity:          "random",
		DeploymentOptions: tv.WorkerDeploymentOptions(true),
	})
}

func (s *WorkerDeploymentSuite) startDeploymentPoller(env *testcore.TestEnv, tv *testvars.TestVars) context.CancelFunc {
	ctx, cancel := context.WithCancel(s.Context())
	go s.pollFromDeploymentUntil(ctx, env, tv)
	return cancel
}

func (s *WorkerDeploymentSuite) pollFromDeploymentWithTaskQueueNumber(env *testcore.TestEnv, tv *testvars.TestVars, taskQueueNumber int) {
	_, _ = env.FrontendClient().PollWorkflowTaskQueue(s.Context(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace:         env.Namespace().String(),
		TaskQueue:         tv.WithTaskQueueNumber(taskQueueNumber).TaskQueue(),
		Identity:          "random",
		DeploymentOptions: tv.WorkerDeploymentOptions(true),
	})
}

func (s *WorkerDeploymentSuite) pollFromDeploymentExpectFail(env *testcore.TestEnv, tv *testvars.TestVars, expectedError string) {
	_, err := env.FrontendClient().PollWorkflowTaskQueue(s.Context(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace:         env.Namespace().String(),
		TaskQueue:         tv.TaskQueue(),
		Identity:          "random",
		DeploymentOptions: tv.WorkerDeploymentOptions(true),
	})
	s.Error(err)
	s.Equal(expectedError, err.Error())
}

func (s *WorkerDeploymentSuite) ensureCreateVersionWithExpectedTaskQueues(env *testcore.TestEnv, tv *testvars.TestVars, expectedTaskQueues int) {
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		respV, _ := env.FrontendClient().DescribeWorkerDeploymentVersion(s.Context(), &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: env.Namespace().String(),
			Version:   tv.DeploymentVersionString(),
		})

		a.Len(respV.GetWorkerDeploymentVersionInfo().GetTaskQueueInfos(), expectedTaskQueues)
	}, 5*time.Minute, 500*time.Millisecond)
}

func (s *WorkerDeploymentSuite) ensureCreateVersionInDeployment(
	env *testcore.TestEnv,
	tv *testvars.TestVars,
) {
	v := tv.DeploymentVersionString()
	ctx, cancel := context.WithTimeout(s.Context(), 5*time.Second)
	defer cancel()
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		res, _ := env.FrontendClient().DescribeWorkerDeployment(ctx,
			&workflowservice.DescribeWorkerDeploymentRequest{
				Namespace:      env.Namespace().String(),
				DeploymentName: tv.DeploymentSeries(),
			})

		found := false
		if res != nil {
			for _, vs := range res.GetWorkerDeploymentInfo().GetVersionSummaries() {
				if vs.GetVersion() == v {
					found = true
				}
			}
		}
		a.True(found)
	}, 1*time.Minute, 100*time.Millisecond)
}

func (s *WorkerDeploymentSuite) ensureCreateDeployment(
	env *testcore.TestEnv,
	tv *testvars.TestVars,
) {
	ctx, cancel := context.WithTimeout(s.Context(), 5*time.Second)
	defer cancel()
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		res, _ := env.FrontendClient().DescribeWorkerDeployment(ctx,
			&workflowservice.DescribeWorkerDeploymentRequest{
				Namespace:      env.Namespace().String(),
				DeploymentName: tv.DeploymentSeries(),
			})
		a.NotNil(res)
	}, 5*time.Second, 100*time.Millisecond)
}

func (s *WorkerDeploymentSuite) startVersionWorkflow(env *testcore.TestEnv, tv *testvars.TestVars) {
	go s.pollFromDeployment(env, tv)
	s.waitForDeploymentVersion(env, tv)
}

func (s *WorkerDeploymentSuite) waitForDeploymentVersion(env *testcore.TestEnv, tv *testvars.TestVars) {
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := env.FrontendClient().DescribeWorkerDeploymentVersion(s.Context(), &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: env.Namespace().String(),
			Version:   tv.DeploymentVersionString(),
		})
		a.NoError(err)
		a.Equal(tv.DeploymentVersionString(), resp.GetWorkerDeploymentVersionInfo().GetVersion())
	}, time.Minute, time.Second)

}

func (s *WorkerDeploymentSuite) TestForceCAN_NoOpenWFS() {
	env := s.newTestEnv()

	// Start a version workflow
	s.startVersionWorkflow(env, env.Tv())
	s.ensureCreateVersionInDeployment(env, env.Tv())

	// Set the version as current
	_, err := env.FrontendClient().SetWorkerDeploymentCurrentVersion(s.Context(), &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentSeries(),
		Version:        env.Tv().DeploymentVersionString(),
	})
	s.NoError(err)

	// ForceCAN
	workflowID := workerdeployment.GenerateDeploymentWorkflowID(env.Tv().DeploymentSeries())
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
	}

	err = env.SendSignal(env.Namespace().String(), workflowExecution, workerdeployment.ForceCANSignalName, nil, env.Tv().ClientIdentity())
	s.NoError(err)

	// Verify if the state is intact even after a CAN
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      env.Namespace().String(),
			DeploymentName: env.Tv().DeploymentSeries(),
		})
		a.NoError(err)
		a.Equal(env.Tv().DeploymentVersionString(), resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetCurrentVersion()) //nolint:staticcheck // SA1019: old worker versioning
	}, time.Second*10, time.Millisecond*1000)
}

func (s *WorkerDeploymentSuite) TestForceCAN_WithOverrideState() {
	env := s.newTestEnv()

	// Start a version workflow
	s.startVersionWorkflow(env, env.Tv())
	s.ensureCreateVersionInDeployment(env, env.Tv())

	// Set the version as current
	_, err := env.FrontendClient().SetWorkerDeploymentCurrentVersion(s.Context(), &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentSeries(),
		Version:        env.Tv().DeploymentVersionString(),
	})
	s.NoError(err)

	// Create a modified state with a different manager identity
	overrideState := &deploymentspb.WorkerDeploymentLocalState{
		CreateTime:           timestamppb.New(time.Now()),
		RoutingConfig:        &deploymentpb.RoutingConfig{CurrentVersion: env.Tv().DeploymentVersionString()},
		Versions:             map[string]*deploymentspb.WorkerDeploymentVersionSummary{env.Tv().DeploymentVersionString(): {Version: env.Tv().DeploymentVersionString(), CreateTime: timestamppb.New(time.Now())}},
		LastModifierIdentity: "override-test-identity",
		ManagerIdentity:      "override-manager-identity",
	}

	// Create signal args with the override state
	signalArgs := &deploymentspb.ForceCANDeploymentSignalArgs{
		OverrideState: overrideState,
	}
	marshaledData, err := proto.Marshal(signalArgs)
	s.NoError(err)
	signalPayload := &commonpb.Payloads{
		Payloads: []*commonpb.Payload{
			{
				Metadata: map[string][]byte{
					"encoding": []byte("binary/protobuf"),
				},
				Data: marshaledData,
			},
		},
	}

	// Send ForceCAN signal with override state
	workflowID := workerdeployment.GenerateDeploymentWorkflowID(env.Tv().DeploymentSeries())
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
	}

	err = env.SendSignal(env.Namespace().String(), workflowExecution, workerdeployment.ForceCANSignalName, signalPayload, env.Tv().ClientIdentity())
	s.NoError(err)

	// Verify that the override state is used after CAN
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      env.Namespace().String(),
			DeploymentName: env.Tv().DeploymentSeries(),
		})
		a.NoError(err)
		a.Equal("override-manager-identity", resp.GetWorkerDeploymentInfo().GetManagerIdentity())
	}, time.Second*10, time.Millisecond*1000)
}

func (s *WorkerDeploymentSuite) TestDeploymentVersionLimits() {
	env := s.newTestEnv(
		testcore.WithDynamicConfig(dynamicconfig.MatchingMaxVersionsInDeployment, 1),
		testcore.WithDynamicConfig(dynamicconfig.MatchingMaxTaskQueuesInDeploymentVersion, 1),
	)

	firstDeployment := env.Tv().WithDeploymentSeriesNumber(1)
	secondDeployment := env.Tv().WithDeploymentSeriesNumber(2)

	firstDeploymentVersionOne := firstDeployment.WithBuildIDNumber(1)
	firstDeploymentVersionTwo := firstDeployment.WithBuildIDNumber(2)

	secondDeploymentVersionOne := secondDeployment.WithBuildIDNumber(1)

	expectedErrorMaxVersions := fmt.Sprintf("cannot add version %v since maximum number of versions (1) have been registered in the deployment", firstDeploymentVersionTwo.DeploymentVersionString())
	expectedErrorMaxTaskQueues := fmt.Sprintf("cannot add task queue %v since maximum number of task queues (1) have been registered in deployment", secondDeploymentVersionOne.WithTaskQueueNumber(2).TaskQueue().GetName())

	// First deployment version should be fine
	go s.pollFromDeployment(env, firstDeploymentVersionOne)
	s.ensureCreateVersionInDeployment(env, firstDeploymentVersionOne)

	// pollers of second version in the same deployment should be rejected
	s.pollFromDeploymentExpectFail(env, firstDeploymentVersionTwo, expectedErrorMaxVersions)

	// But first version of another deployment fine
	go s.pollFromDeployment(env, secondDeploymentVersionOne)
	s.ensureCreateVersionInDeployment(env, secondDeploymentVersionOne)

	// pollers of the second TQ in the same deployment version should be rejected
	s.pollFromDeploymentExpectFail(env, secondDeploymentVersionOne.WithTaskQueueNumber(2), expectedErrorMaxTaskQueues)
}

func (s *WorkerDeploymentSuite) TestNamespaceDeploymentsLimit() {
	// TODO (carly): check the error messages that poller receives in each case and make sense they are informative and appropriate (e.g. do not expose internal stuff)
	// Also in TestCreateWorkerDeployment_MaxDeploymentsLimit
	s.T().Skip() // Need to separate this test so other tests do not create deployment in the same NS

	env := s.newTestEnv(testcore.WithDynamicConfig(dynamicconfig.MatchingMaxDeployments, 1))

	// First deployment version should be fine
	go s.pollFromDeployment(env, env.Tv())
	s.ensureCreateVersionInDeployment(env, env.Tv())

	// wait for all existing deployments to show up in visibility
	s.validateWorkerDeploymentCount(env, &workflowservice.ListWorkerDeploymentsRequest{Namespace: env.Namespace().String()}, 1)

	// pollers of the second deployment version should be rejected
	s.pollFromDeploymentExpectFail(env, env.Tv().WithDeploymentSeriesNumber(2), "reached maximum deployments in namespace (1)")
}

func (s *WorkerDeploymentSuite) TestDescribeWorkerDeployment_TwoVersions_Sorted() {
	env := s.newTestEnv()

	// Starting two versions of the deployment
	firstVersion := env.Tv().WithBuildIDNumber(1)
	secondVersion := env.Tv().WithBuildIDNumber(2)

	go s.pollFromDeployment(env, firstVersion)

	// Wait until the first version is registered in the deployment before starting the second.
	// This ensures that both versions get distinct CreateTime values in the deployment workflow
	// (each processed in a separate workflow task), so that the descending-by-CreateTime sort
	// produces a deterministic order. A wall-clock wait is not sufficient because in V2 mode
	// both registrations can queue up and be processed within the same workflow task millisecond.
	s.ensureCreateVersionInDeployment(env, firstVersion)

	go s.pollFromDeployment(env, secondVersion)

	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)

		resp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      env.Namespace().String(),
			DeploymentName: env.Tv().DeploymentSeries(),
		})
		a.NoError(err)
		a.NotNil(resp.GetWorkerDeploymentInfo())
		a.Equal(env.Tv().DeploymentSeries(), resp.GetWorkerDeploymentInfo().GetName())

		a.NotNil(resp.GetWorkerDeploymentInfo().GetVersionSummaries())
		a.Len(resp.GetWorkerDeploymentInfo().GetVersionSummaries(), 2)

		// Verify that the version summaries are non-nil and sorted.
		versionSummaries := resp.GetWorkerDeploymentInfo().GetVersionSummaries()

		a.NotNil(versionSummaries[0].GetVersion())
		a.NotNil(versionSummaries[1].GetVersion())
		a.Equal(versionSummaries[0].GetVersion(), secondVersion.DeploymentVersionString())
		a.Equal(versionSummaries[1].GetVersion(), firstVersion.DeploymentVersionString())

		a.NotNil(resp.GetWorkerDeploymentInfo().GetVersionSummaries()[0].GetCreateTime())
		a.NotNil(resp.GetWorkerDeploymentInfo().GetVersionSummaries()[1].GetCreateTime())

		a.NotNil(resp.GetWorkerDeploymentInfo().GetCreateTime())

		a.Equal(enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE, resp.GetWorkerDeploymentInfo().GetVersionSummaries()[0].GetStatus())
		a.Equal(enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE, resp.GetWorkerDeploymentInfo().GetVersionSummaries()[1].GetStatus())
	}, time.Second*10, time.Millisecond*1000)
}

func (s *WorkerDeploymentSuite) TestDescribeWorkerDeployment_MultipleVersions_Sorted() {
	env := s.newTestEnv()

	numVersions := 10

	for i := range numVersions {
		go s.pollFromDeployment(env, env.Tv().WithBuildIDNumber(i))

		// waiting for 1ms to start the next version later.
		startTime := time.Now()
		waitTime := 1 * time.Millisecond
		s.EventuallyWithT(func(t *assert.CollectT) {
			require.Greater(t, time.Since(startTime), waitTime)
		}, 10*time.Second, 1000*time.Millisecond)
	}

	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)

		resp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      env.Namespace().String(),
			DeploymentName: env.Tv().DeploymentSeries(),
		})
		a.NoError(err)

		a.NotNil(resp.GetWorkerDeploymentInfo().GetVersionSummaries())
		a.Len(resp.GetWorkerDeploymentInfo().GetVersionSummaries(), numVersions)

		// Verify that the version summaries are sorted.
		versionSummaries := resp.GetWorkerDeploymentInfo().GetVersionSummaries()
		for i := 0; i < numVersions-1; i++ {
			a.Less(versionSummaries[i+1].GetCreateTime().AsTime(), versionSummaries[i].GetCreateTime().AsTime())
		}
	}, time.Second*10, time.Millisecond*1000)
}

// Testing ConflictToken
func (s *WorkerDeploymentSuite) TestConflictToken_Describe_SetCurrent_SetRamping() {
	env := s.newTestEnv()

	firstVersion := env.Tv().WithBuildIDNumber(1)
	secondVersion := env.Tv().WithBuildIDNumber(2)

	// Start deployment version workflow + worker-deployment workflow.
	go s.pollFromDeployment(env, firstVersion)
	go s.pollFromDeployment(env, secondVersion)

	var cT []byte
	// No current deployment version set.
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)

		resp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      env.Namespace().String(),
			DeploymentName: env.Tv().DeploymentSeries(),
		})
		a.NoError(err)
		a.Equal(worker_versioning.UnversionedVersionId, resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetCurrentVersion())
		cT = resp.GetConflictToken()
	}, time.Second*10, time.Millisecond*1000)

	s.ensureCreateVersionInDeployment(env, firstVersion)
	// Set first version as current version
	_, err := env.FrontendClient().SetWorkerDeploymentCurrentVersion(s.Context(), &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentSeries(),
		Version:        firstVersion.DeploymentVersionString(),
		ConflictToken:  cT,
	})
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)

		resp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      env.Namespace().String(),
			DeploymentName: env.Tv().DeploymentSeries(),
		})
		a.NoError(err)
		a.Equal(firstVersion.DeploymentVersionString(), resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetCurrentVersion()) //nolint:staticcheck // SA1019: old worker versioning
		cT = resp.GetConflictToken()
	}, time.Second*10, time.Millisecond*1000)

	// Set a new second version and set it as the current version
	go s.pollFromDeployment(env, secondVersion)
	_, _ = env.FrontendClient().SetWorkerDeploymentRampingVersion(s.Context(), &workflowservice.SetWorkerDeploymentRampingVersionRequest{
		Namespace:               env.Namespace().String(),
		DeploymentName:          env.Tv().DeploymentSeries(),
		Version:                 secondVersion.DeploymentVersionString(),
		Percentage:              5,
		ConflictToken:           cT,
		IgnoreMissingTaskQueues: true, // here until we have 'has version started' safeguard in place
	})
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      env.Namespace().String(),
			DeploymentName: env.Tv().DeploymentSeries(),
		})
		a.NoError(err)
		a.Equal(secondVersion.DeploymentVersionString(), resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetRampingVersion())
	}, time.Second*10, time.Millisecond*1000)
}

func (s *WorkerDeploymentSuite) TestConflictToken_SetCurrent_SetRamping_Wrong() {
	env := s.newTestEnv(testcore.WithDynamicConfig(dynamicconfig.FrontendMaskInternalErrorDetails, true))

	expectedError := "conflict token mismatch"

	// Start deployment version workflow + worker-deployment workflow.
	go s.pollFromDeployment(env, env.Tv())

	cTWrong, _ := time.Now().MarshalBinary() // wrong token
	// Wait until deployment exists
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      env.Namespace().String(),
			DeploymentName: env.Tv().DeploymentSeries(),
		})
		a.NoError(err)
		a.Equal(worker_versioning.UnversionedVersionId, resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetCurrentVersion())
	}, time.Second*10, time.Millisecond*1000)

	// Set first version as current version with wrong token
	_, err := env.FrontendClient().SetWorkerDeploymentCurrentVersion(s.Context(), &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentSeries(),
		Version:        env.Tv().DeploymentVersionString(),
		ConflictToken:  cTWrong,
	})
	s.Equal(expectedError, err.Error())

	// Set first version as ramping version with wrong token
	_, err = env.FrontendClient().SetWorkerDeploymentRampingVersion(s.Context(), &workflowservice.SetWorkerDeploymentRampingVersionRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentSeries(),
		Version:        env.Tv().DeploymentVersionString(),
		Percentage:     5,
		ConflictToken:  cTWrong,
	})
	s.Equal(expectedError, err.Error())
}

// Testing ListWorkerDeployments

func (s *WorkerDeploymentSuite) TestListWorkerDeployments_OneVersion_OneDeployment() {
	env := s.newTestEnv()

	startTime := timestamppb.Now()

	s.startVersionWorkflow(env, env.Tv())
	s.ensureCreateVersionInDeployment(env, env.Tv())

	latestVersionSummary := &deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
		Version:              env.Tv().DeploymentVersionString(),
		CreateTime:           startTime,
		DrainageInfo:         nil,
		RampingSinceTime:     nil,
		CurrentSinceTime:     nil,
		RoutingUpdateTime:    nil,
		FirstActivationTime:  nil,
		LastDeactivationTime: nil,
		Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE,
	}

	expectedDeploymentSummaries := s.buildWorkerDeploymentSummary(
		env.Tv().DeploymentSeries(),
		startTime,
		&deploymentpb.RoutingConfig{
			CurrentVersion: worker_versioning.UnversionedVersionId, // default current version is __unversioned__
		},
		latestVersionSummary,
		nil,
		nil,
	)

	s.startAndValidateWorkerDeployments(env, &workflowservice.ListWorkerDeploymentsRequest{
		Namespace: env.Namespace().String(),
	}, []*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary{expectedDeploymentSummaries})
}

func (s *WorkerDeploymentSuite) TestListWorkerDeployments_TwoVersions_SameDeployment_OneCurrent_NoRamping() {
	env := s.newTestEnv()

	firstVersion := env.Tv().WithBuildIDNumber(1)
	secondVersion := env.Tv().WithBuildIDNumber(2)

	createVersion1Time := timestamppb.Now()
	s.startVersionWorkflow(env, firstVersion)
	s.ensureCreateVersionInDeployment(env, firstVersion)

	createVersion2Time := timestamppb.Now()
	s.startVersionWorkflow(env, secondVersion)
	s.ensureCreateVersionInDeployment(env, secondVersion)

	setCurrentTime := timestamppb.Now()
	s.setCurrentVersion(env, firstVersion, true, "")

	routingInfo := &deploymentpb.RoutingConfig{
		CurrentVersion:            firstVersion.DeploymentVersionString(),
		CurrentVersionChangedTime: setCurrentTime,
	}

	latestVersionSummary := &deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
		Version:              secondVersion.DeploymentVersionString(),
		CreateTime:           createVersion2Time,
		DrainageInfo:         nil,
		RampingSinceTime:     nil,
		CurrentSinceTime:     nil,
		RoutingUpdateTime:    nil,
		FirstActivationTime:  nil,
		LastDeactivationTime: nil,
		Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE,
	}
	currentVersionSummary := &deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
		Version:              firstVersion.DeploymentVersionString(),
		CreateTime:           createVersion1Time,
		DrainageInfo:         nil,
		RampingSinceTime:     nil,
		CurrentSinceTime:     setCurrentTime,
		RoutingUpdateTime:    setCurrentTime,
		FirstActivationTime:  setCurrentTime,
		LastCurrentTime:      setCurrentTime,
		LastDeactivationTime: nil,
		Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
	}

	expectedDeploymentSummary := s.buildWorkerDeploymentSummary(
		env.Tv().DeploymentSeries(),
		createVersion1Time,
		routingInfo,
		latestVersionSummary,
		currentVersionSummary,
		nil,
	)

	s.startAndValidateWorkerDeployments(env, &workflowservice.ListWorkerDeploymentsRequest{
		Namespace: env.Namespace().String(),
	}, []*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary{
		expectedDeploymentSummary,
	})
}

func (s *WorkerDeploymentSuite) TestListWorkerDeployments_TwoVersions_SameDeployment_OneCurrent_OneRamping() {
	env := s.newTestEnv()

	currentVersionVars := env.Tv().WithBuildIDNumber(1)
	rampingVersionVars := env.Tv().WithBuildIDNumber(2)

	createVersion1Time := timestamppb.Now()
	s.startVersionWorkflow(env, currentVersionVars)
	s.ensureCreateVersionInDeployment(env, currentVersionVars)

	createVersion2Time := timestamppb.Now()
	s.startVersionWorkflow(env, rampingVersionVars)
	s.ensureCreateVersionInDeployment(env, rampingVersionVars)

	setCurrentTime := timestamppb.Now()
	s.setCurrentVersion(env, currentVersionVars, true, "") // starts first version's version workflow + set it to current
	setRampingTime := timestamppb.Now()
	s.setAndVerifyRampingVersion(env, rampingVersionVars, false, 50, true, "")
	routingInfo := &deploymentpb.RoutingConfig{
		CurrentVersion:            currentVersionVars.DeploymentVersionString(),
		CurrentVersionChangedTime: setCurrentTime,
		RampingVersion:            rampingVersionVars.DeploymentVersionString(),
		RampingVersionPercentage:  50,
		RampingVersionChangedTime: setRampingTime,
	}

	latestVersionSummary := &deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
		Version:              rampingVersionVars.DeploymentVersionString(),
		CreateTime:           createVersion2Time,
		DrainageInfo:         nil,
		RampingSinceTime:     setRampingTime,
		CurrentSinceTime:     nil,
		RoutingUpdateTime:    setRampingTime,
		FirstActivationTime:  setRampingTime,
		LastDeactivationTime: nil,
		Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING,
	}
	currentVersionSummary := &deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
		Version:              currentVersionVars.DeploymentVersionString(),
		CreateTime:           createVersion1Time,
		DrainageInfo:         nil,
		RampingSinceTime:     nil,
		CurrentSinceTime:     setCurrentTime,
		RoutingUpdateTime:    setCurrentTime,
		FirstActivationTime:  setCurrentTime,
		LastCurrentTime:      setCurrentTime,
		LastDeactivationTime: nil,
		Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
	}

	expectedDeploymentSummary := s.buildWorkerDeploymentSummary(
		env.Tv().DeploymentSeries(),
		createVersion1Time,
		routingInfo,
		latestVersionSummary,
		currentVersionSummary,
		latestVersionSummary, // latest version added is the ramping version
	)

	s.startAndValidateWorkerDeployments(env, &workflowservice.ListWorkerDeploymentsRequest{
		Namespace: env.Namespace().String(),
	}, []*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary{
		expectedDeploymentSummary,
	})
}

func (s *WorkerDeploymentSuite) TestListWorkerDeployments_RampingVersionPercentageChange_RampingChangedTime() {
	env := s.newTestEnv()

	startTime := timestamppb.Now()
	s.startVersionWorkflow(env, env.Tv())
	setRampTime := timestamppb.Now()
	s.setAndVerifyRampingVersion(env, env.Tv(), false, 50, true, "") // set version as ramping

	routingInfo := &deploymentpb.RoutingConfig{
		CurrentVersion:                      worker_versioning.UnversionedVersionId,
		CurrentVersionChangedTime:           nil,
		RampingVersion:                      env.Tv().DeploymentVersionString(),
		RampingVersionPercentage:            50,
		RampingVersionChangedTime:           setRampTime,
		RampingVersionPercentageChangedTime: setRampTime,
	}

	// to simulate time passing before the next ramping version update
	//nolint:forbidigo
	time.Sleep(2 * time.Second)

	changeRampTime := timestamppb.Now()
	// modify ramping version percentage
	s.setAndVerifyRampingVersion(env, env.Tv(), false, 75, true, "")

	// only the ramping version percentage should be updated, not the ramping version update time
	// since we are not changing the ramping version
	routingInfo.RampingVersionPercentage = 75
	routingInfo.RampingVersionPercentageChangedTime = changeRampTime

	rampingVersionSummary := &deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
		Version:              env.Tv().DeploymentVersionString(),
		CreateTime:           startTime,
		DrainageInfo:         nil,
		RampingSinceTime:     setRampTime,
		CurrentSinceTime:     nil,
		RoutingUpdateTime:    changeRampTime, // since the ramp percentage changed, the routing update time is updated
		FirstActivationTime:  setRampTime,
		LastDeactivationTime: nil,
		Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING,
	}

	expectedDeploymentSummary := s.buildWorkerDeploymentSummary(
		env.Tv().DeploymentSeries(),
		startTime,
		routingInfo,
		rampingVersionSummary, // latest version added is the ramping version
		nil,
		rampingVersionSummary,
	)

	s.startAndValidateWorkerDeployments(env, &workflowservice.ListWorkerDeploymentsRequest{
		Namespace: env.Namespace().String(),
	}, []*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary{
		expectedDeploymentSummary,
	})

}

func (s *WorkerDeploymentSuite) TestListWorkerDeployments_MultipleVersions_MultipleDeployments_OnePage() {
	env := s.newTestEnv()

	expectedDeploymentSummaries := s.createVersionsInDeployments(env, env.Tv(), 2)

	s.startAndValidateWorkerDeployments(env, &workflowservice.ListWorkerDeploymentsRequest{
		Namespace: env.Namespace().String(),
	}, expectedDeploymentSummaries)
}

func (s *WorkerDeploymentSuite) TestListWorkerDeployments_MultipleVersions_MultipleDeployments_MultiplePages() {

	env := s.newTestEnv()

	expectedDeploymentSummaries := s.createVersionsInDeployments(env, env.Tv(), 5)

	s.startAndValidateWorkerDeployments(env, &workflowservice.ListWorkerDeploymentsRequest{
		Namespace: env.Namespace().String(),
		PageSize:  1,
	}, expectedDeploymentSummaries)
}

// Testing SetWorkerDeploymentRampingVersion
// Also tests whether LastCurrentTime is successfully updated when a previously-current version is demoted and then promoted back to current.
func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_Ramping_With_Current() {
	env := s.newTestEnv()

	rampingVersionVars := env.Tv().WithBuildIDNumber(1)
	currentVersionVars := env.Tv().WithBuildIDNumber(2)

	versionCreateTime := timestamppb.Now()
	s.startVersionWorkflow(env, rampingVersionVars)
	s.startVersionWorkflow(env, currentVersionVars)

	// set version as ramping
	setRampingUpdateTime := timestamppb.Now()
	s.setAndVerifyRampingVersion(env, rampingVersionVars, false, 50, true, "")
	resp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       env.Tv().DeploymentSeries(),
			CreateTime: versionCreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:                      rampingVersionVars.DeploymentVersionString(),
				RampingVersionPercentage:            50,
				RampingVersionChangedTime:           setRampingUpdateTime,
				RampingVersionPercentageChangedTime: setRampingUpdateTime,
				CurrentVersion:                      worker_versioning.UnversionedVersionId,
				CurrentVersionChangedTime:           nil,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              rampingVersionVars.DeploymentVersionString(),
					CreateTime:           versionCreateTime,
					DrainageInfo:         nil,
					RampingSinceTime:     setRampingUpdateTime,
					CurrentSinceTime:     nil,
					RoutingUpdateTime:    setRampingUpdateTime,
					FirstActivationTime:  setRampingUpdateTime,
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING,
				},
			},
			LastModifierIdentity: env.Tv().ClientIdentity(),
		},
	})

	// set current version
	setCurrentUpdateTime := timestamppb.Now()
	s.setCurrentVersion(env, currentVersionVars, true, "")

	// fresh DescribeWorkerDeployment call
	resp, err = env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       env.Tv().DeploymentSeries(),
			CreateTime: versionCreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:                      rampingVersionVars.DeploymentVersionString(),
				RampingVersionPercentage:            50,
				RampingVersionChangedTime:           setRampingUpdateTime,
				RampingVersionPercentageChangedTime: setRampingUpdateTime,
				CurrentVersion:                      currentVersionVars.DeploymentVersionString(),
				CurrentVersionChangedTime:           setCurrentUpdateTime,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              currentVersionVars.DeploymentVersionString(),
					CreateTime:           versionCreateTime,
					DrainageInfo:         nil,
					RoutingUpdateTime:    setCurrentUpdateTime,
					CurrentSinceTime:     setCurrentUpdateTime,
					RampingSinceTime:     nil,
					FirstActivationTime:  setCurrentUpdateTime,
					LastCurrentTime:      setCurrentUpdateTime,
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
				},
				{
					Version:              rampingVersionVars.DeploymentVersionString(),
					CreateTime:           versionCreateTime,
					DrainageInfo:         nil,
					RoutingUpdateTime:    setRampingUpdateTime,
					CurrentSinceTime:     nil,
					RampingSinceTime:     setRampingUpdateTime,
					FirstActivationTime:  setRampingUpdateTime,
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING,
				},
			},
			LastModifierIdentity: env.Tv().ClientIdentity(),
		},
	})

	// Now, demote the current version (by promoting the ramping version) and verify that LastCurrentTime is unchanged.
	setRampingAsCurrentUpdateTime := timestamppb.Now()
	s.setCurrentVersion(env, rampingVersionVars, true, "")
	// fresh DescribeWorkerDeployment call
	resp, err = env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       env.Tv().DeploymentSeries(),
			CreateTime: versionCreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:                      "",
				RampingVersionPercentage:            0,
				RampingVersionChangedTime:           setRampingAsCurrentUpdateTime,
				RampingVersionPercentageChangedTime: setRampingAsCurrentUpdateTime,
				CurrentVersion:                      rampingVersionVars.DeploymentVersionString(),
				CurrentVersionChangedTime:           setRampingAsCurrentUpdateTime,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:    currentVersionVars.DeploymentVersionString(),
					CreateTime: versionCreateTime,
					DrainageInfo: &deploymentpb.VersionDrainageInfo{
						Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINING,
					},
					RoutingUpdateTime:    setRampingAsCurrentUpdateTime,
					CurrentSinceTime:     nil,
					RampingSinceTime:     nil,
					FirstActivationTime:  setCurrentUpdateTime,
					LastCurrentTime:      setCurrentUpdateTime,
					LastDeactivationTime: setRampingAsCurrentUpdateTime,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
				},
				{
					Version:              rampingVersionVars.DeploymentVersionString(),
					CreateTime:           versionCreateTime,
					DrainageInfo:         nil,
					RoutingUpdateTime:    setRampingAsCurrentUpdateTime,
					CurrentSinceTime:     setRampingAsCurrentUpdateTime,
					RampingSinceTime:     nil,
					FirstActivationTime:  setRampingUpdateTime,
					LastCurrentTime:      setRampingAsCurrentUpdateTime,
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
				},
			},
			LastModifierIdentity: env.Tv().ClientIdentity(),
		},
	})

	// Now, re-promote the current version to Current and verify that LastCurrentTime is updated for that version (and unchanged for the originally-ramping version).
	rePromoteCurrentUpdateTime := timestamppb.Now()
	s.setCurrentVersion(env, currentVersionVars, true, "")
	// fresh DescribeWorkerDeployment call
	resp, err = env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       env.Tv().DeploymentSeries(),
			CreateTime: versionCreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:                      "",
				RampingVersionPercentage:            0,
				RampingVersionChangedTime:           setRampingAsCurrentUpdateTime,
				RampingVersionPercentageChangedTime: setRampingAsCurrentUpdateTime,
				CurrentVersion:                      currentVersionVars.DeploymentVersionString(),
				CurrentVersionChangedTime:           rePromoteCurrentUpdateTime,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              currentVersionVars.DeploymentVersionString(),
					CreateTime:           versionCreateTime,
					DrainageInfo:         nil,
					RoutingUpdateTime:    rePromoteCurrentUpdateTime,
					CurrentSinceTime:     rePromoteCurrentUpdateTime,
					RampingSinceTime:     nil,
					FirstActivationTime:  setCurrentUpdateTime,
					LastCurrentTime:      rePromoteCurrentUpdateTime, // updated
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
				},
				{
					Version:    rampingVersionVars.DeploymentVersionString(),
					CreateTime: versionCreateTime,
					DrainageInfo: &deploymentpb.VersionDrainageInfo{
						Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINING,
					},
					RoutingUpdateTime:    rePromoteCurrentUpdateTime,
					CurrentSinceTime:     nil,
					RampingSinceTime:     nil,
					FirstActivationTime:  setRampingUpdateTime,
					LastCurrentTime:      setRampingAsCurrentUpdateTime,
					LastDeactivationTime: rePromoteCurrentUpdateTime,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
				},
			},
			LastModifierIdentity: env.Tv().ClientIdentity(),
		},
	})

}

func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_DuplicateRamp() {
	env := s.newTestEnv()

	versionCreateTime := timestamppb.Now()
	s.startVersionWorkflow(env, env.Tv())

	// set version as ramping
	setRampingUpdateTime := timestamppb.Now()
	s.setAndVerifyRampingVersion(env, env.Tv(), false, 50, true, "")
	resp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       env.Tv().DeploymentSeries(),
			CreateTime: versionCreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:                      env.Tv().DeploymentVersionString(),
				RampingVersionPercentage:            50,
				RampingVersionChangedTime:           setRampingUpdateTime,
				RampingVersionPercentageChangedTime: setRampingUpdateTime,
				CurrentVersion:                      worker_versioning.UnversionedVersionId,
				CurrentVersionChangedTime:           nil,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              env.Tv().DeploymentVersionString(),
					CreateTime:           versionCreateTime,
					DrainageInfo:         nil,
					RampingSinceTime:     setRampingUpdateTime,
					CurrentSinceTime:     nil,
					RoutingUpdateTime:    setRampingUpdateTime,
					FirstActivationTime:  setRampingUpdateTime,
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING,
				},
			},
			LastModifierIdentity: env.Tv().ClientIdentity(),
		},
	})

	// setting version as ramping again
	s.setAndVerifyRampingVersion(env, env.Tv(), false, 50, true, "")
}

func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_Invalid_SetCurrent_To_Ramping() {
	env := s.newTestEnv()

	versionCreateTime := timestamppb.Now()
	s.startVersionWorkflow(env, env.Tv())

	setCurrentUpdateTime := timestamppb.Now()
	s.setCurrentVersion(env, env.Tv(), true, "")

	resp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       env.Tv().DeploymentSeries(),
			CreateTime: versionCreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:                      "",  // no ramping info should be set
				RampingVersionPercentage:            0,   // no ramping info should be set
				RampingVersionChangedTime:           nil, // no ramping info should be set
				RampingVersionPercentageChangedTime: nil, // no ramping info should be set
				CurrentVersion:                      env.Tv().DeploymentVersionString(),
				CurrentVersionChangedTime:           setCurrentUpdateTime,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              env.Tv().DeploymentVersionString(),
					CreateTime:           versionCreateTime,
					DrainageInfo:         nil,
					RoutingUpdateTime:    setCurrentUpdateTime,
					CurrentSinceTime:     setCurrentUpdateTime,
					RampingSinceTime:     nil,
					FirstActivationTime:  setCurrentUpdateTime,
					LastCurrentTime:      setCurrentUpdateTime,
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
				},
			},
			LastModifierIdentity: env.Tv().ClientIdentity(),
		},
	})

	expectedError := fmt.Errorf("ramping version %s is already current", env.Tv().DeploymentVersionString())
	s.setAndVerifyRampingVersion(env, env.Tv(), false, 50, true, expectedError.Error()) // setting current version to ramping should fail

	resp, err = env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       env.Tv().DeploymentSeries(),
			CreateTime: versionCreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:                      "",  // no ramping info should be set
				RampingVersionPercentage:            0,   // no ramping info should be set
				RampingVersionChangedTime:           nil, // no ramping info should be set
				RampingVersionPercentageChangedTime: nil, // no ramping info should be set
				CurrentVersion:                      env.Tv().DeploymentVersionString(),
				CurrentVersionChangedTime:           versionCreateTime,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              env.Tv().DeploymentVersionString(),
					CreateTime:           versionCreateTime,
					DrainageInfo:         nil,
					RoutingUpdateTime:    setCurrentUpdateTime,
					CurrentSinceTime:     setCurrentUpdateTime,
					RampingSinceTime:     nil,
					FirstActivationTime:  setCurrentUpdateTime,
					LastCurrentTime:      setCurrentUpdateTime,
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
				},
			},
			LastModifierIdentity: env.Tv().ClientIdentity(),
		},
	})
}

func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_Valid_SetNilCurrent_To_Ramping() {
	env := s.newTestEnv()

	s.startVersionWorkflow(env, env.Tv())

	// set ramping version to unversioned will change the modifier identity, so it's not a no-op
	s.setAndVerifyRampingVersion(env, env.Tv(), true, 0, false, "")

	// set a non-nil ramping version so that we can unset it in the next step
	s.setAndVerifyRampingVersion(env, env.Tv(), false, 5, false, "")

	// should be able to unset ramping version while current version is nil with no error
	s.setAndVerifyRampingVersion(env, env.Tv(), true, 0, true, "")
}

func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_ModifyExistingRampVersionPercentage() {
	env := s.newTestEnv()

	versionCreateTime := timestamppb.Now()
	s.startVersionWorkflow(env, env.Tv())

	// set version as ramping
	setRampingUpdateTime := timestamppb.Now()
	s.setAndVerifyRampingVersion(env, env.Tv(), false, 50, true, "")
	resp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       env.Tv().DeploymentSeries(),
			CreateTime: versionCreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:                      env.Tv().DeploymentVersionString(),
				RampingVersionPercentage:            50,
				RampingVersionChangedTime:           setRampingUpdateTime,
				RampingVersionPercentageChangedTime: setRampingUpdateTime,
				CurrentVersion:                      worker_versioning.UnversionedVersionId,
				CurrentVersionChangedTime:           nil,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              env.Tv().DeploymentVersionString(),
					CreateTime:           versionCreateTime,
					DrainageInfo:         nil,
					RampingSinceTime:     setRampingUpdateTime,
					CurrentSinceTime:     nil,
					RoutingUpdateTime:    setRampingUpdateTime,
					FirstActivationTime:  setRampingUpdateTime,
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING,
				},
			},
			LastModifierIdentity: env.Tv().ClientIdentity(),
		},
	})

	// modify ramping version percentage
	modifyRampingPercentageTime := timestamppb.Now()
	s.setAndVerifyRampingVersion(env, env.Tv(), false, 75, true, "")

	// RampingVersionPercentage and RampingVersionPercentageChangedTime should be updated
	resp, err = env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       env.Tv().DeploymentSeries(),
			CreateTime: versionCreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:                      env.Tv().DeploymentVersionString(),
				RampingVersionPercentage:            75, // ramping version percentage is updated to 75
				RampingVersionChangedTime:           setRampingUpdateTime,
				RampingVersionPercentageChangedTime: modifyRampingPercentageTime, // timestamp is updated as the ramp percentage changed from 50 -> 75
				CurrentVersion:                      worker_versioning.UnversionedVersionId,
				CurrentVersionChangedTime:           nil,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              env.Tv().DeploymentVersionString(),
					CreateTime:           versionCreateTime,
					DrainageInfo:         nil,
					RoutingUpdateTime:    modifyRampingPercentageTime,
					CurrentSinceTime:     nil,
					RampingSinceTime:     setRampingUpdateTime,
					FirstActivationTime:  setRampingUpdateTime,
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING,
				},
			},
			LastModifierIdentity: env.Tv().ClientIdentity(),
		},
	})

}

func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_WithCurrent_Unset_Ramp() {
	env := s.newTestEnv()

	rampingVersionVars := env.Tv().WithBuildIDNumber(1)
	currentVersionVars := env.Tv().WithBuildIDNumber(2)

	version1CreateTime := timestamppb.Now()
	s.startVersionWorkflow(env, rampingVersionVars)
	version2CreateTime := timestamppb.Now()
	s.startVersionWorkflow(env, currentVersionVars)

	setRampingUpdateTime := timestamppb.Now()
	s.setAndVerifyRampingVersion(env, rampingVersionVars, false, 50, true, "") // set version as ramping

	setCurrentUpdateTime := timestamppb.Now()
	s.setCurrentVersion(env, currentVersionVars, true, "") // set version as curent

	resp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       env.Tv().DeploymentSeries(),
			CreateTime: version1CreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:                      rampingVersionVars.DeploymentVersionString(),
				RampingVersionPercentage:            50,
				RampingVersionChangedTime:           setRampingUpdateTime,
				RampingVersionPercentageChangedTime: setRampingUpdateTime,
				CurrentVersion:                      currentVersionVars.DeploymentVersionString(),
				CurrentVersionChangedTime:           setCurrentUpdateTime,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              rampingVersionVars.DeploymentVersionString(),
					CreateTime:           version1CreateTime,
					DrainageInfo:         nil,
					RoutingUpdateTime:    setRampingUpdateTime,
					CurrentSinceTime:     nil,
					RampingSinceTime:     setRampingUpdateTime,
					FirstActivationTime:  setRampingUpdateTime,
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING,
				},
				{
					Version:              currentVersionVars.DeploymentVersionString(),
					CreateTime:           version2CreateTime,
					DrainageInfo:         nil,
					RoutingUpdateTime:    setCurrentUpdateTime,
					CurrentSinceTime:     setCurrentUpdateTime,
					RampingSinceTime:     nil,
					FirstActivationTime:  setCurrentUpdateTime,
					LastCurrentTime:      setCurrentUpdateTime,
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
				},
			},
			LastModifierIdentity: env.Tv().ClientIdentity(),
		},
	})

	// unset ramping version
	unsetRampingUpdateTime := timestamppb.Now()
	s.setAndVerifyRampingVersion(env, rampingVersionVars, true, 0, true, "")

	resp, err = env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       env.Tv().DeploymentSeries(),
			CreateTime: version1CreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:                      "",
				RampingVersionPercentage:            0,
				RampingVersionChangedTime:           unsetRampingUpdateTime,
				RampingVersionPercentageChangedTime: unsetRampingUpdateTime,
				CurrentVersion:                      currentVersionVars.DeploymentVersionString(),
				CurrentVersionChangedTime:           setCurrentUpdateTime,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              rampingVersionVars.DeploymentVersionString(),
					CreateTime:           version1CreateTime,
					DrainageInfo:         &deploymentpb.VersionDrainageInfo{Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINING},
					RoutingUpdateTime:    unsetRampingUpdateTime,
					CurrentSinceTime:     nil,
					RampingSinceTime:     nil,
					FirstActivationTime:  setRampingUpdateTime,
					LastDeactivationTime: unsetRampingUpdateTime,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
				},
				{
					Version:              currentVersionVars.DeploymentVersionString(),
					CreateTime:           version2CreateTime,
					DrainageInfo:         nil,
					RoutingUpdateTime:    setCurrentUpdateTime,
					CurrentSinceTime:     setCurrentUpdateTime,
					RampingSinceTime:     nil,
					FirstActivationTime:  setCurrentUpdateTime,
					LastCurrentTime:      setCurrentUpdateTime,
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
				},
			},
			LastModifierIdentity: env.Tv().ClientIdentity(),
		},
	})
}

func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_SetRampingAsCurrent() {
	env := s.newTestEnv()

	versionCreateTime := timestamppb.Now()
	s.startVersionWorkflow(env, env.Tv())

	setRampingUpdateTime := timestamppb.Now()
	s.setAndVerifyRampingVersion(env, env.Tv(), false, 50, true, "")

	// set ramping version as current
	setCurrentUpdateTime := timestamppb.Now()
	s.setCurrentVersion(env, env.Tv(), true, "")

	resp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       env.Tv().DeploymentSeries(),
			CreateTime: versionCreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:                      "",                   // no ramping info should be set
				RampingVersionPercentage:            0,                    // no ramping info should be set
				RampingVersionChangedTime:           setCurrentUpdateTime, // ramping version got updated to ""
				RampingVersionPercentageChangedTime: setCurrentUpdateTime, // ramping version got updated to ""
				CurrentVersion:                      env.Tv().DeploymentVersionString(),
				CurrentVersionChangedTime:           setCurrentUpdateTime,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              env.Tv().DeploymentVersionString(),
					CreateTime:           versionCreateTime,
					DrainageInfo:         nil,
					RoutingUpdateTime:    setCurrentUpdateTime,
					CurrentSinceTime:     setCurrentUpdateTime,
					RampingSinceTime:     nil,
					FirstActivationTime:  setRampingUpdateTime,
					LastCurrentTime:      setCurrentUpdateTime,
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
				},
			},
			LastModifierIdentity: env.Tv().ClientIdentity(),
		},
	})
}

func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_NoCurrent_Unset_Ramp() {
	env := s.newTestEnv()

	s.startVersionWorkflow(env, env.Tv())

	s.setAndVerifyRampingVersion(env, env.Tv(), false, 50, true, "")
	s.setAndVerifyRampingVersion(env, env.Tv(), true, 0, true, "")
}

func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_Batching() {
	env := s.newTestEnv()
	env.InjectHook(testhooks.NewHook(testhooks.TaskQueuesInDeploymentSyncBatchSize, 1))

	// registering 5 task-queues in the version which would result in the creation of 5 batches, each with 1 task-queue, during the SyncState call.
	versionCreateTime := timestamppb.Now()
	taskQueues := 5
	for i := range taskQueues {
		go s.pollFromDeploymentWithTaskQueueNumber(env, env.Tv(), i)
	}

	// ensure the version has been created in the deployment with the right number of task-queues
	s.ensureCreateVersionInDeployment(env, env.Tv())
	s.ensureCreateVersionWithExpectedTaskQueues(env, env.Tv(), taskQueues)

	// verify that all the registered task-queues have "" set as their ramping version
	for i := range taskQueues {
		s.verifyTaskQueueVersioningInfo(env, env.Tv().WithTaskQueueNumber(i).TaskQueue(), worker_versioning.UnversionedVersionId, "", 0)
	}

	// set ramping version to 50%
	setRampingUpdateTime := timestamppb.Now()
	s.setAndVerifyRampingVersion(env, env.Tv(), false, 50, true, "")

	// verify the task queues have new ramping version
	for i := range taskQueues {
		s.verifyTaskQueueVersioningInfo(env, env.Tv().WithTaskQueueNumber(i).TaskQueue(), worker_versioning.UnversionedVersionId, env.Tv().DeploymentVersionString(), 50)
	}

	// verify if the worker-deployment has the right ramping version set
	resp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentSeries(),
	})
	s.NoError(err)
	s.Equal(env.Tv().DeploymentVersionString(), resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetRampingVersion()) //nolint:staticcheck // SA1019: old worker versioning

	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       env.Tv().DeploymentSeries(),
			CreateTime: versionCreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:                      env.Tv().DeploymentVersionString(),
				RampingVersionPercentage:            50,
				RampingVersionChangedTime:           setRampingUpdateTime,
				RampingVersionPercentageChangedTime: setRampingUpdateTime,
				CurrentVersion:                      worker_versioning.UnversionedVersionId,
				CurrentVersionChangedTime:           nil,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              env.Tv().DeploymentVersionString(),
					CreateTime:           versionCreateTime,
					DrainageInfo:         nil,
					RoutingUpdateTime:    setRampingUpdateTime,
					CurrentSinceTime:     nil,
					RampingSinceTime:     setRampingUpdateTime,
					FirstActivationTime:  setRampingUpdateTime,
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING,
				},
			},
			LastModifierIdentity: env.Tv().ClientIdentity(),
		},
	})

}

// TestSetWorkerDeploymentRampingVersion_UnversionedRamp_Batching verifies that the batching functionality works
// when ramping unversioned.
func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_UnversionedRamp_Batching() {
	env := s.newTestEnv()
	env.InjectHook(testhooks.NewHook(testhooks.TaskQueuesInDeploymentSyncBatchSize, 1))

	// registering 5 task-queues in the version which would result in the creation of 5 batches, each with 1 task-queue, during the SyncState call.
	versionCreateTime := timestamppb.Now()
	taskQueues := 5
	for i := range taskQueues {
		go s.pollFromDeploymentWithTaskQueueNumber(env, env.Tv(), i)
	}

	// ensure the version has been created in the deployment with the right number of task-queues
	s.ensureCreateVersionInDeployment(env, env.Tv())
	s.ensureCreateVersionWithExpectedTaskQueues(env, env.Tv(), taskQueues)

	// make the current version versioned, so that we can set ramp to unversioned later
	setCurrentUpdateTime := timestamppb.Now()
	s.setCurrentVersion(env, env.Tv(), true, "")

	// set ramp to unversioned which should trigger a batch of SyncDeploymentVersionUserData requests.
	setRampingUpdateTime := timestamppb.Now()
	s.setAndVerifyRampingVersionUnversionedOption(env, env.Tv(), true, false, 75, true, false, true, "")

	// check that the current version's task queues have ramping version == __unversioned__
	for i := range taskQueues {
		s.verifyTaskQueueVersioningInfo(env, env.Tv().WithTaskQueueNumber(i).TaskQueue(), env.Tv().DeploymentVersionString(), worker_versioning.UnversionedVersionId, 75)
	}

	// verify if the worker-deployment has the right ramping version set
	resp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentSeries(),
	})
	s.NoError(err)
	// nolint:staticcheck // SA1019: old worker versioning
	s.Equal(worker_versioning.UnversionedVersionId, resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetRampingVersion())
	s.Nil(resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetRampingDeploymentVersion())

	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       env.Tv().DeploymentSeries(),
			CreateTime: versionCreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:                      worker_versioning.UnversionedVersionId,
				RampingVersionPercentage:            75,
				RampingVersionChangedTime:           setRampingUpdateTime,
				RampingVersionPercentageChangedTime: setRampingUpdateTime,
				CurrentVersion:                      env.Tv().DeploymentVersionString(),
				CurrentVersionChangedTime:           setCurrentUpdateTime,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              env.Tv().DeploymentVersionString(),
					CreateTime:           versionCreateTime,
					DrainageInfo:         nil,
					RoutingUpdateTime:    setCurrentUpdateTime,
					CurrentSinceTime:     setCurrentUpdateTime,
					RampingSinceTime:     nil,
					FirstActivationTime:  setCurrentUpdateTime,
					LastCurrentTime:      setCurrentUpdateTime,
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
				},
			},
			LastModifierIdentity: env.Tv().ClientIdentity(),
		},
	})

}

// SetCurrent tests

func (s *WorkerDeploymentSuite) TestDescribeWorkerDeployment_SetCurrentVersion() {
	env := s.newTestEnv()

	firstVersion := env.Tv().WithBuildIDNumber(1)
	secondVersion := env.Tv().WithBuildIDNumber(2)

	version1CreateTime := timestamppb.Now()
	// Start poller with cancellable context for deterministic control
	pollerCancel := s.startDeploymentPoller(env, firstVersion)
	defer pollerCancel()

	// Wait for version to be created deterministically
	s.ensureCreateVersionInDeployment(env, firstVersion)

	// Verify no current deployment version is set initially
	resp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentSeries(),
	})
	s.NoError(err)
	s.Equal(worker_versioning.UnversionedVersionId, resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetCurrentVersion()) //nolint:staticcheck // SA1019: old worker versioning
	s.Nil(resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetCurrentDeploymentVersion())

	// Set first version as current version
	firstVersionCurrentUpdateTime := timestamppb.Now()
	s.setCurrentVersion(env, firstVersion, true, "")

	resp, err = env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       env.Tv().DeploymentSeries(),
			CreateTime: version1CreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				CurrentVersion:            firstVersion.DeploymentVersionString(),
				CurrentVersionChangedTime: firstVersionCurrentUpdateTime,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              firstVersion.DeploymentVersionString(),
					CreateTime:           version1CreateTime,
					DrainageInfo:         nil,
					RoutingUpdateTime:    firstVersionCurrentUpdateTime,
					CurrentSinceTime:     firstVersionCurrentUpdateTime,
					RampingSinceTime:     nil,
					FirstActivationTime:  firstVersionCurrentUpdateTime,
					LastCurrentTime:      firstVersionCurrentUpdateTime,
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
				},
			},
			LastModifierIdentity: env.Tv().ClientIdentity(),
		},
	})

	// Set a new second version and set it as the current version
	version2CreateTime := timestamppb.Now()
	// Start second poller with cancellable context
	poller2Cancel := s.startDeploymentPoller(env, secondVersion)
	defer poller2Cancel()
	secondVersionCurrentUpdateTime := timestamppb.Now()
	s.setCurrentVersion(env, secondVersion, true, "")

	// Verify that the first version is draining, has an updated last deactivation time + second version is current, has an updated first activation time.
	resp, err = env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       env.Tv().DeploymentSeries(),
			CreateTime: version1CreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				CurrentVersion:            secondVersion.DeploymentVersionString(),
				CurrentVersionChangedTime: secondVersionCurrentUpdateTime,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              firstVersion.DeploymentVersionString(),
					CreateTime:           version1CreateTime,
					DrainageInfo:         &deploymentpb.VersionDrainageInfo{Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINING},
					RoutingUpdateTime:    firstVersionCurrentUpdateTime,
					CurrentSinceTime:     nil,
					RampingSinceTime:     nil,
					FirstActivationTime:  firstVersionCurrentUpdateTime,
					LastCurrentTime:      firstVersionCurrentUpdateTime,
					LastDeactivationTime: secondVersionCurrentUpdateTime,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
				},
				{
					Version:              secondVersion.DeploymentVersionString(),
					CreateTime:           version2CreateTime,
					DrainageInfo:         nil,
					RoutingUpdateTime:    secondVersionCurrentUpdateTime,
					CurrentSinceTime:     secondVersionCurrentUpdateTime,
					RampingSinceTime:     nil,
					FirstActivationTime:  secondVersionCurrentUpdateTime,
					LastCurrentTime:      secondVersionCurrentUpdateTime,
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
				},
			},
			LastModifierIdentity: env.Tv().ClientIdentity(),
		},
	})

}

func (s *WorkerDeploymentSuite) TestSetCurrentVersion_Batching() {
	env := s.newTestEnv()
	env.InjectHook(testhooks.NewHook(testhooks.TaskQueuesInDeploymentSyncBatchSize, 1))

	// registering 5 task-queues in the version which would result in the creation of 5 batches, each with 1 task-queue, during the SyncState call.
	versionCreateTime := timestamppb.Now()
	taskQueues := 5
	for i := range taskQueues {
		go s.pollFromDeploymentWithTaskQueueNumber(env, env.Tv(), i)
	}

	// ensure the version has been created in the deployment with the right number of task-queues
	s.ensureCreateVersionInDeployment(env, env.Tv())
	s.ensureCreateVersionWithExpectedTaskQueues(env, env.Tv(), taskQueues)

	// verify that all the registered task-queues have "__unversioned__" as their current version
	for i := range taskQueues {
		s.verifyTaskQueueVersioningInfo(env, env.Tv().WithTaskQueueNumber(i).TaskQueue(), worker_versioning.UnversionedVersionId, "", 0)
	}

	// set current and check that the current version's task queues have new current version
	setCurrentUpdateTime := timestamppb.Now()
	s.setCurrentVersion(env, env.Tv(), true, "")

	// verify the current version has propogated to all the registered task-queues userData
	for i := range taskQueues {
		s.verifyTaskQueueVersioningInfo(env, env.Tv().WithTaskQueueNumber(i).TaskQueue(), env.Tv().DeploymentVersionString(), "", 0)
	}

	// verify if the worker-deployment has the right current version set
	resp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentSeries(),
	})
	s.NoError(err)

	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       env.Tv().DeploymentSeries(),
			CreateTime: versionCreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:                      "",
				RampingVersionPercentage:            0,
				RampingVersionChangedTime:           nil,
				RampingVersionPercentageChangedTime: nil,
				CurrentVersion:                      env.Tv().DeploymentVersionString(),
				CurrentVersionChangedTime:           setCurrentUpdateTime,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              env.Tv().DeploymentVersionString(),
					CreateTime:           versionCreateTime,
					DrainageInfo:         nil,
					RoutingUpdateTime:    setCurrentUpdateTime,
					CurrentSinceTime:     setCurrentUpdateTime,
					RampingSinceTime:     nil,
					FirstActivationTime:  setCurrentUpdateTime,
					LastCurrentTime:      setCurrentUpdateTime,
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
				},
			},
			LastModifierIdentity: env.Tv().ClientIdentity(),
		},
	})

}

func (s *WorkerDeploymentSuite) TestSetManagerIdentity_RW() {
	env := s.newTestEnv()

	go s.pollFromDeployment(env, env.Tv())
	s.ensureCreateVersionInDeployment(env, env.Tv())

	// set identity to self
	s.setAndValidateManagerIdentity(env, env.Tv(), true, false, "", "")

	// set identity to other
	s.setAndValidateManagerIdentity(env, env.Tv(), false, false, "other", "")

	// set identity to other again (should be idempotent)
	s.setAndValidateManagerIdentity(env, env.Tv(), false, false, "other", "")

	// unset identity
	s.setAndValidateManagerIdentity(env, env.Tv(), false, false, "", "")

	// set identity with bad conflict token
	s.setAndValidateManagerIdentity(env, env.Tv(), true, true, "", "conflict token mismatch")
}

func (s *WorkerDeploymentSuite) TestSetManagerIdentity_WithSetRampSetCurrent() {
	env := s.newTestEnv()

	go s.pollFromDeployment(env, env.Tv())
	s.ensureCreateVersionInDeployment(env, env.Tv())

	// set identity to self
	s.setAndValidateManagerIdentity(env, env.Tv(), true, false, "", "")
	// -> self can successfully set ramp
	s.setAndVerifyRampingVersion(env, env.Tv(), false, 1, true, "")

	// set identity to other
	s.setAndValidateManagerIdentity(env, env.Tv(), false, false, "other", "")
	// -> self cannot set ramp
	s.setAndVerifyRampingVersion(env, env.Tv(), false, 2, true, fmt.Sprintf(workerdeployment.ErrManagerIdentityMismatch, "other", env.Tv().ClientIdentity()))
	// -> self cannot set current
	s.setCurrentVersion(env, env.Tv(), true, fmt.Sprintf(workerdeployment.ErrManagerIdentityMismatch, "other", env.Tv().ClientIdentity()))

	// unset identity
	s.setAndValidateManagerIdentity(env, env.Tv(), false, false, "", "")
	// -> self can now set ramp
	s.setAndVerifyRampingVersion(env, env.Tv(), false, 2, true, "")

	// set identity to self
	s.setAndValidateManagerIdentity(env, env.Tv(), true, false, "", "")
	// -> self can now set current
	s.setCurrentVersion(env, env.Tv(), true, "")
}

func (s *WorkerDeploymentSuite) TestSetManagerIdentity_WithDeleteVersion() {
	env := s.newTestEnv(testcore.WithDynamicConfig(dynamicconfig.PollerHistoryTTL, 500*time.Millisecond))

	// start and stop polling so that version is eligible for deletion
	pollerCancel := s.startDeploymentPoller(env, env.Tv())
	s.ensureCreateVersionInDeployment(env, env.Tv())
	pollerCancel()

	// set identity to other
	s.setAndValidateManagerIdentity(env, env.Tv(), false, false, "other", "")
	// -> self cannot delete version
	s.tryDeleteVersion(env, env.Tv(), fmt.Sprintf(workerdeployment.ErrManagerIdentityMismatch, "other", env.Tv().ClientIdentity()))

	// set identity to self
	s.setAndValidateManagerIdentity(env, env.Tv(), true, false, "", "")
	// -> self can now delete version
	s.tryDeleteVersion(env, env.Tv(), "")
}

// TestDeleteVersion_ServerDeleteMaxVersionsReached tests that when the internal limit for the number of versions
// in a worker-deployment (defaultMaxVersions) is reached, the server deletes the oldest version to register the new version.
// Additionally, the test verifies that the last modifier identity is not set to the identity of the worker-deployment workflow.
func (s *WorkerDeploymentSuite) TestDeleteVersion_ServerDeleteMaxVersionsReached() {
	env := s.newTestEnv(
		testcore.WithDynamicConfig(dynamicconfig.PollerHistoryTTL, 1*time.Millisecond),
		testcore.WithDynamicConfig(dynamicconfig.MatchingMaxVersionsInDeployment, 1),
	)

	tv1 := env.Tv().WithBuildIDNumber(1)
	tv2 := env.Tv().WithBuildIDNumber(2)

	// start and stop polling so that version is eligible for deletion
	pollerCancel := s.startDeploymentPoller(env, tv1)
	s.ensureCreateVersionInDeployment(env, tv1)
	pollerCancel()

	// Set a different manager identity so that we can verify that the internal delete operation does not conduct the manager identity check
	// while deleting the version
	s.setAndValidateManagerIdentity(env, tv1, false, false, "other", "")

	// Start another poller which shall aim to create a new version. This should, in turn, delete the first version.
	pollerCancel2 := s.startDeploymentPoller(env, tv2)
	s.ensureCreateVersionInDeployment(env, tv2)
	pollerCancel2()

	// Verify that the worker deployment only has one version in it's version summaries.
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      env.Namespace().String(),
			DeploymentName: env.Tv().DeploymentSeries(),
		})
		a.NoError(err)
		a.Len(resp.GetWorkerDeploymentInfo().GetVersionSummaries(), 1)
		a.Equal(tv2.ExternalDeploymentVersion().GetBuildId(), resp.GetWorkerDeploymentInfo().GetVersionSummaries()[0].GetDeploymentVersion().GetBuildId())

		// Also verify that the last modifier identity is not set to the identity of the worker-deployment workflow.
		a.NotEqual(env.Tv().ClientIdentity(), resp.GetWorkerDeploymentInfo().GetLastModifierIdentity())
	}, time.Second*5, time.Millisecond*200)
}

// Should see that the current version of the task queues becomes unversioned
func (s *WorkerDeploymentSuite) TestSetCurrentVersion_Unversioned_NoRamp() {
	env := s.newTestEnv()
	versionCreateTime := timestamppb.Now()
	go s.pollFromDeployment(env, env.Tv())
	s.ensureCreateVersionInDeployment(env, env.Tv())

	// check that the current version's task queues have current version unversioned to start
	s.verifyTaskQueueVersioningInfo(env, env.Tv().TaskQueue(), worker_versioning.UnversionedVersionId, "", 0)

	// set current and check that the current version's task queues have new current version
	firstCurrentUpdateTime := timestamppb.Now()
	s.setCurrentVersion(env, env.Tv(), true, "")
	s.verifyTaskQueueVersioningInfo(env, env.Tv().TaskQueue(), env.Tv().DeploymentVersionString(), "", 0)

	// set current unversioned and check that the current version's task queues have current version unversioned again
	secondCurrentUpdateTime := timestamppb.Now()
	s.setCurrentVersionUnversionedOption(env, env.Tv(), true, true, false, true, "")
	s.verifyTaskQueueVersioningInfo(env, env.Tv().TaskQueue(), worker_versioning.UnversionedVersionId, "", 0)

	// check that deployment has current version == __unversioned__
	resp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       env.Tv().DeploymentSeries(),
			CreateTime: versionCreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				CurrentVersion:            worker_versioning.UnversionedVersionId,
				CurrentVersionChangedTime: secondCurrentUpdateTime,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              env.Tv().DeploymentVersionString(),
					CreateTime:           versionCreateTime,
					DrainageInfo:         &deploymentpb.VersionDrainageInfo{Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINING},
					RoutingUpdateTime:    secondCurrentUpdateTime,
					CurrentSinceTime:     nil,
					RampingSinceTime:     nil,
					FirstActivationTime:  firstCurrentUpdateTime,
					LastCurrentTime:      firstCurrentUpdateTime,
					LastDeactivationTime: secondCurrentUpdateTime,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
				},
			},
			LastModifierIdentity: env.Tv().ClientIdentity(),
		},
	})
}

// Should see that the current version of the task queue becomes unversioned, and the unversioned ramping version of the task queue is removed
func (s *WorkerDeploymentSuite) TestSetCurrentVersion_Unversioned_PromoteUnversionedRamp() {
	env := s.newTestEnv()
	go s.pollFromDeployment(env, env.Tv())
	s.ensureCreateVersionInDeployment(env, env.Tv())

	// make the current version versioned, so that we can set ramp to unversioned
	s.setCurrentVersion(env, env.Tv(), true, "")
	// set ramp to unversioned
	s.setAndVerifyRampingVersionUnversionedOption(env, env.Tv(), true, false, 75, true, false, true, "")
	// check that the current version's task queues have ramping version == __unversioned__
	s.verifyTaskQueueVersioningInfo(env, env.Tv().TaskQueue(), env.Tv().DeploymentVersionString(), worker_versioning.UnversionedVersionId, 75)

	// set current to unversioned
	s.setCurrentVersionUnversionedOption(env, env.Tv(), true, true, false, true, "")

	// check that the current version's task queues have ramping version == "" and current version == "__unversioned__"
	s.verifyTaskQueueVersioningInfo(env, env.Tv().TaskQueue(), worker_versioning.UnversionedVersionId, "", 0)
}

func (s *WorkerDeploymentSuite) TestSetCurrentVersion_Concurrent_DifferentVersions_NoUnexpectedErrors() {
	env := s.newTestEnv(testcore.WithDynamicConfig(dynamicconfig.WorkflowExecutionMaxInFlightUpdates, 10)) // this is the default

	errChan := make(chan error)

	versions := 10
	for i := range versions {
		s.startVersionWorkflow(env, env.Tv().WithBuildIDNumber(i))
	}

	// Concurrently set 10 different versions as current version
	for i := range versions {
		go func() {
			_, err := env.FrontendClient().SetWorkerDeploymentCurrentVersion(s.Context(), &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
				Namespace:               env.Namespace().String(),
				DeploymentName:          env.Tv().DeploymentVersion().GetDeploymentName(),
				Version:                 env.Tv().WithBuildIDNumber(i).DeploymentVersionString(),
				IgnoreMissingTaskQueues: true,
				Identity:                env.Tv().ClientIdentity(),
			})
			errChan <- err
		}()
	}

	for range versions {
		err := <-errChan
		if err != nil {
			switch err.(type) {
			// DeadlineExceeded and ResourceExhausted are expected errors since there could be more
			// in-flight updates than WorkflowExecutionMaxInFlightUpdates or we could get a timeout error.
			case *serviceerror.DeadlineExceeded, *serviceerror.ResourceExhausted:
				continue
			default:
				s.FailNow("Unexpected error: ", err)
			}
		}
	}

	// Verify that the current version is set.
	resp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentVersion().GetDeploymentName(),
	})
	s.NoError(err)
	s.NotEqual(worker_versioning.UnversionedVersionId, resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetCurrentVersion())
}

func (s *WorkerDeploymentSuite) TestSetCurrentVersion_Concurrent_SameVersion_NoUnexpectedErrors() {
	env := s.newTestEnv(testcore.WithDynamicConfig(dynamicconfig.WorkflowExecutionMaxInFlightUpdates, 10)) // this is the default

	errChan := make(chan error)

	s.startVersionWorkflow(env, env.Tv()) // create version

	// Concurrently set the same version as current version 10 times.
	for range 10 {
		go func() {
			_, err := env.FrontendClient().SetWorkerDeploymentCurrentVersion(s.Context(), &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
				Namespace:               env.Namespace().String(),
				DeploymentName:          env.Tv().DeploymentVersion().GetDeploymentName(),
				Version:                 env.Tv().DeploymentVersionString(),
				IgnoreMissingTaskQueues: true,
				Identity:                env.Tv().ClientIdentity(),
			})
			errChan <- err
		}()
	}

	for range 10 {
		err := <-errChan
		if err != nil {
			switch err.(type) {
			// DeadlineExceeded and ResourceExhausted are expected errors since there could be more
			// in-flight updates than WorkflowExecutionMaxInFlightUpdates or we could get a timeout error.
			case *serviceerror.DeadlineExceeded, *serviceerror.ResourceExhausted:
				continue
			default:
				s.FailNow("Unexpected error: ", err)
			}
		}
	}

	// Verify that the current version is set.
	resp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentVersion().GetDeploymentName(),
	})
	s.NoError(err)
	s.Equal(env.Tv().DeploymentVersionString(), resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetCurrentVersion()) //nolint:staticcheck // SA1019: old worker versioning
}

// TestConcurrentPollers_DifferentTaskQueues_SameVersion_SetCurrentVersion aims to test that when there are multiple pollers polling on different task queues,
// all belonging to the same version, a setCurrentVersion call succeeds with all the task queues eventually having this version as the current version in their versioning info.
func (s *WorkerDeploymentSuite) TestConcurrentPollers_DifferentTaskQueues_SameVersion_SetCurrentVersion() {
	env := s.newTestEnv()

	// start 10 different pollers each polling on a different task queue but belonging to the same version

	tqs := 10
	// Start all pollers concurrently (pollFromDeployment has no assertions, so it's safe to call from goroutines)
	for i := range tqs {
		go s.pollFromDeployment(env, env.Tv().WithTaskQueueNumber(i))
	}
	// Wait for all version workflows to appear (must run in the test goroutine due to assertions)
	for i := range tqs {
		tvI := env.Tv().WithTaskQueueNumber(i)
		s.EventuallyWithT(func(t *assert.CollectT) {
			a := require.New(t)
			resp, err := env.FrontendClient().DescribeWorkerDeploymentVersion(s.Context(), &workflowservice.DescribeWorkerDeploymentVersionRequest{
				Namespace: env.Namespace().String(),
				Version:   tvI.DeploymentVersionString(),
			})
			a.NoError(err)
			a.Equal(tvI.ExternalDeploymentVersion(), resp.GetWorkerDeploymentVersionInfo().GetDeploymentVersion())
		}, time.Minute, time.Second)
	}

	// set this version as current version
	s.setCurrentVersion(env, env.Tv(), false, "")

	// verify that the task queues, eventually, have this version as the current version in their versioning info
	for i := range tqs {
		s.verifyTaskQueueVersioningInfo(env, env.Tv().WithTaskQueueNumber(i).TaskQueue(), env.Tv().DeploymentVersionString(), "", 0)
	}
}

func (s *WorkerDeploymentSuite) TestSetRampingVersion_Concurrent_DifferentVersions_NoUnexpectedErrors() {
	env := s.newTestEnv(testcore.WithDynamicConfig(dynamicconfig.WorkflowExecutionMaxInFlightUpdates, 10)) // this is the default

	errChan := make(chan error)

	versions := 10
	for i := range versions {
		s.startVersionWorkflow(env, env.Tv().WithBuildIDNumber(i))
	}

	// Concurrently set 10 different versions as ramping version
	for i := range versions {
		go func() {
			_, err := env.FrontendClient().SetWorkerDeploymentRampingVersion(s.Context(), &workflowservice.SetWorkerDeploymentRampingVersionRequest{
				Namespace:               env.Namespace().String(),
				DeploymentName:          env.Tv().DeploymentVersion().GetDeploymentName(),
				Version:                 env.Tv().WithBuildIDNumber(i).DeploymentVersionString(),
				IgnoreMissingTaskQueues: true,
				Identity:                env.Tv().ClientIdentity(),
				Percentage:              50,
			})
			errChan <- err
		}()
	}

	for range versions {
		err := <-errChan
		if err != nil {
			switch err.(type) {
			// DeadlineExceeded and ResourceExhausted are expected errors since there could be more
			// in-flight updates than WorkflowExecutionMaxInFlightUpdates or we could get a timeout error.
			case *serviceerror.DeadlineExceeded, *serviceerror.ResourceExhausted:
				continue
			default:
				s.FailNow("Unexpected error: ", err)
			}
		}
	}

	// Verify that the ramping version is set.
	resp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentVersion().GetDeploymentName(),
	})
	s.NoError(err)
	s.NotNil(resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetRampingVersion())
}

func (s *WorkerDeploymentSuite) TestSetRampingVersion_Concurrent_SameVersion_NoUnexpectedErrors() {
	env := s.newTestEnv(testcore.WithDynamicConfig(dynamicconfig.WorkflowExecutionMaxInFlightUpdates, 10)) // this is the default

	errChan := make(chan error)

	s.startVersionWorkflow(env, env.Tv()) // create version

	// Concurrently set the same version as ramping version 10 times.
	for range 10 {
		go func() {
			_, err := env.FrontendClient().SetWorkerDeploymentRampingVersion(s.Context(), &workflowservice.SetWorkerDeploymentRampingVersionRequest{
				Namespace:               env.Namespace().String(),
				DeploymentName:          env.Tv().DeploymentVersion().GetDeploymentName(),
				Version:                 env.Tv().DeploymentVersionString(),
				IgnoreMissingTaskQueues: true,
				Identity:                env.Tv().ClientIdentity(),
				Percentage:              50,
			})
			errChan <- err
		}()
	}

	for range 10 {
		err := <-errChan
		if err != nil {
			switch err.(type) {
			// DeadlineExceeded and ResourceExhausted are expected errors since there could be more
			// in-flight updates than WorkflowExecutionMaxInFlightUpdates or we could get a timeout error.
			case *serviceerror.DeadlineExceeded, *serviceerror.ResourceExhausted:
				continue
			default:
				s.FailNow("Unexpected error: ", err)
			}
		}
	}

	// Verify that the ramping version is set.
	resp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentVersion().GetDeploymentName(),
	})
	s.NoError(err)
	s.Equal(env.Tv().DeploymentVersionString(), resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetRampingVersion()) //nolint:staticcheck // SA1019: old worker versioning
}

// TODO: this test reliably produces a rare error seemingly about speculative tasks. use it for debugging the error
// ex: "history_events: premature end of stream, expectedLastEventID=13 but no more events after eventID=11"
// (error does not seems to be related to versioning)
func (s *WorkerDeploymentSuite) TestConcurrentPollers_ManyTaskQueues_RapidRoutingUpdates_RevisionConsistency() {
	s.T().Skip("Skipping until we can figure out why this test is flaky in sqlite")

	// This test should work for InitialVersion, but it takes much longer (4m vs 1m15s vs 55s, in order, for 50 TQs).
	// Also skipping for AsyncSetCurrentAndRampingVersion, to reduce flake chance.
	s.skipBeforeVersion(workerdeployment.VersionDataRevisionNumber)

	// Start pollers on many task queues across 3 versions
	numTaskQueues := 50
	numVersions := 3
	syncBatchSize := 2 // reducing batch size to cause more delay
	numOperations := 20

	env := s.newTestEnv(
		testcore.WithDynamicConfig(dynamicconfig.MatchingMaxTaskQueuesInDeploymentVersion, numTaskQueues),
		// Need to increase max pending activities because it is set only to 10 for functional tests. it's 2000 by default.
		testcore.WithDynamicConfig(dynamicconfig.NumPendingActivitiesLimitError, numOperations),
	)
	env.InjectHook(testhooks.NewHook(testhooks.TaskQueuesInDeploymentSyncBatchSize, syncBatchSize))
	env.InjectHook(testhooks.NewHook(testhooks.MatchingDeploymentRegisterErrorBackoff, time.Millisecond*500))

	dn := env.Tv().DeploymentVersion().GetDeploymentName()
	start := time.Now()

	// For each version send pollers regularly until all TQs are registered from DescribeVersion POV
	for i := range numVersions {
		pollCtx, cancelPollers := context.WithTimeout(context.Background(), 5*time.Minute)

		sendPollers := func() {
			for j := range numTaskQueues {
				go s.pollFromDeploymentUntil(pollCtx, env, env.Tv().WithBuildIDNumber(i).WithTaskQueueNumber(j))
			}
		}

		sendPollers()

		// Send new pollers regularly, this is needed because the registration might take more time than initial pollers
		t := time.NewTicker(10 * time.Second)
		go func() {
			for {
				select {
				case <-t.C:
					sendPollers()
				case <-pollCtx.Done():
					return
				}
			}
		}()

		// Wait for all task queues to be added to versions

		// Wait for the versions to be created
		s.EventuallyWithT(func(t *assert.CollectT) {
			a := require.New(t)
			resp, err := env.FrontendClient().DescribeWorkerDeployment(pollCtx, &workflowservice.DescribeWorkerDeploymentRequest{
				Namespace:      env.Namespace().String(),
				DeploymentName: dn,
			})
			a.NoError(err)
			a.NotNil(resp.GetWorkerDeploymentInfo())
			a.Len(resp.GetWorkerDeploymentInfo().GetVersionSummaries(), i+1)
		}, 3*time.Minute, 500*time.Millisecond)

		fmt.Printf(">>> Time taken version %d added: %v\n", i, time.Since(start))

		s.EventuallyWithT(func(t *assert.CollectT) {
			a := require.New(t)
			resp, err := env.FrontendClient().DescribeWorkerDeploymentVersion(pollCtx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
				Namespace:         env.Namespace().String(),
				DeploymentVersion: env.Tv().WithBuildIDNumber(i).ExternalDeploymentVersion(),
			})
			a.NoError(err)
			a.Len(resp.GetVersionTaskQueues(), numTaskQueues)
		}, 5*time.Minute, 1000*time.Millisecond)

		t.Stop()
		cancelPollers()

		fmt.Printf(">>> Time taken registration for version %d: %v\n", i, time.Since(start))
	}

	// Rapidly perform 20 setCurrent and setRamping operations, each targeting one of the 3 versions
	for i := range numOperations {
		// Alternate between setCurrent and setRamping
		targetVersion := i % numVersions
		versionTV := env.Tv().WithBuildIDNumber(targetVersion)
		for {
			var err error
			if i%2 == 0 {
				// setCurrent operation
				_, err = env.FrontendClient().SetWorkerDeploymentCurrentVersion(s.Context(), &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
					Namespace:               env.Namespace().String(),
					DeploymentName:          dn,
					BuildId:                 versionTV.DeploymentVersion().GetBuildId(),
					IgnoreMissingTaskQueues: true,
					Identity:                env.Tv().ClientIdentity(),
				})
			} else {
				// setRamping operation
				_, err = env.FrontendClient().SetWorkerDeploymentRampingVersion(s.Context(), &workflowservice.SetWorkerDeploymentRampingVersionRequest{
					Namespace:               env.Namespace().String(),
					DeploymentName:          dn,
					BuildId:                 versionTV.DeploymentVersion().GetBuildId(),
					IgnoreMissingTaskQueues: true,
					Identity:                env.Tv().ClientIdentity(),
					Percentage:              float32(50 + (i % 50)),
				})
			}
			if common.IsResourceExhausted(err) {
				fmt.Printf("ResourceExhausted error, retrying operation %d\n", i)
				time.Sleep(100 * time.Millisecond) //nolint:forbidigo // throttling requests
				continue
			}
			s.NoError(err)
			break
		}
		fmt.Printf(">>> Time taken operation %d: %v\n", i, time.Since(start))
	}

	fmt.Printf(">>> Time taken operations: %v\n", time.Since(start))

	// Wait for the routing update status to be completed
	var latestRoutingConfig *deploymentpb.RoutingConfig
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      env.Namespace().String(),
			DeploymentName: dn,
		})
		a.NoError(err)
		a.NotNil(resp.GetWorkerDeploymentInfo())
		a.Equal(enumspb.ROUTING_CONFIG_UPDATE_STATE_COMPLETED, resp.GetWorkerDeploymentInfo().GetRoutingConfigUpdateState())
		latestRoutingConfig = resp.GetWorkerDeploymentInfo().GetRoutingConfig()
	}, 120*time.Second, 1*time.Second)

	fmt.Printf(">>> Time taken propagation: %v\n", time.Since(start))

	// Verify that the routing info revision number in each of the task queues matches the latest revision number
	// Note: The public API doesn't expose revision numbers at the task queue level, so we verify that the
	// versioning info has been propagated correctly by checking the current/ramping versions
	for j := range numTaskQueues {
		tqTV := env.Tv().WithTaskQueueNumber(j)
		tqUD, err := env.GetTestCluster().MatchingClient().GetTaskQueueUserData(s.Context(), &matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:   env.NamespaceID().String(),
			TaskQueueType: tqTypeWf,
			TaskQueue:     tqTV.TaskQueue().GetName(),
		})
		s.NoError(err)
		s.Len(tqUD.GetUserData().GetData().GetPerType(), 1)
		s.Len(tqUD.GetUserData().GetData().GetPerType()[int32(tqTypeWf)].GetDeploymentData().GetDeploymentsData(), 1)
		s.ProtoEqual(latestRoutingConfig, tqUD.GetUserData().GetData().GetPerType()[int32(tqTypeWf)].GetDeploymentData().GetDeploymentsData()[dn].GetRoutingConfig())
	}
}

func (s *WorkerDeploymentSuite) TestResourceExhaustedErrors_Converted_To_ReadableMessage() {
	env := s.newTestEnv(testcore.WithDynamicConfig(dynamicconfig.WorkflowExecutionMaxInFlightUpdates, 2)) // Lowering the limit to encounter ResourceExhausted errors

	versions := 5
	errChan := make(chan error, versions)

	// Start all version workflows first
	for i := range versions {
		s.startVersionWorkflow(env, env.Tv().WithBuildIDNumber(i))
	}

	// Test SetRampingVersion
	s.testConcurrentRequestsResourceExhausted(versions, errChan, "SetWorkerDeploymentRampingVersion", func(i int) error {
		_, err := env.FrontendClient().SetWorkerDeploymentRampingVersion(s.Context(), &workflowservice.SetWorkerDeploymentRampingVersionRequest{
			Namespace:               env.Namespace().String(),
			DeploymentName:          env.Tv().DeploymentVersion().GetDeploymentName(),
			Version:                 env.Tv().WithBuildIDNumber(i).DeploymentVersionString(),
			IgnoreMissingTaskQueues: true,
			Identity:                env.Tv().ClientIdentity(),
			Percentage:              50,
		})
		return err
	})

	// Test SetCurrentVersion
	s.testConcurrentRequestsResourceExhausted(versions, errChan, "SetWorkerDeploymentCurrentVersion", func(i int) error {
		_, err := env.FrontendClient().SetWorkerDeploymentCurrentVersion(s.Context(), &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
			Namespace:               env.Namespace().String(),
			DeploymentName:          env.Tv().DeploymentVersion().GetDeploymentName(),
			Version:                 env.Tv().WithBuildIDNumber(i).DeploymentVersionString(),
			IgnoreMissingTaskQueues: true,
			Identity:                env.Tv().ClientIdentity(),
		})
		return err
	})

	// Test UpdateVersionMetadata
	metadata := map[string]*commonpb.Payload{
		"key1": {Data: testRandomMetadataValue},
		"key2": {Data: testRandomMetadataValue},
	}
	s.testConcurrentRequestsResourceExhausted(versions, errChan, "UpdateWorkerDeploymentVersionMetadata", func(i int) error {
		_, err := env.FrontendClient().UpdateWorkerDeploymentVersionMetadata(s.Context(), &workflowservice.UpdateWorkerDeploymentVersionMetadataRequest{
			Namespace:     env.Namespace().String(),
			Version:       env.Tv().WithBuildIDNumber(i).DeploymentVersionString(),
			UpsertEntries: metadata,
		})
		return err
	})
}

func (s *WorkerDeploymentSuite) testConcurrentRequestsResourceExhausted(
	versions int,
	errChan chan error,
	apiName string,
	requestFn func(int) error,
) {
	// Launch concurrent requests
	for i := range versions {
		go func(i int) {
			errChan <- requestFn(i)
		}(i)
	}

	// Expect ResourceExhausted errors to be converted to Internal errors with the appropriate message
	for range versions {
		err := <-errChan
		if err != nil {
			switch err.(type) {
			case *serviceerror.ResourceExhausted:
				s.Equal(err.Error(), fmt.Sprintf("Too many %s requests have been issued in rapid succession. Please throttle the request rate to avoid exceeding Worker Deployment resource limits.", apiName))
				continue
			default:
				s.FailNow("Unexpected error: ", err)
			}
		}
	}
}

// Should see it fail because unversioned is already current
func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_Unversioned_UnversionedCurrent() {
	env := s.newTestEnv()

	rampingVars := env.Tv()
	s.startVersionWorkflow(env, rampingVars)
	s.setAndVerifyRampingVersionUnversionedOption(env, rampingVars, true, false, 50, true, false, true, "ramping version __unversioned__ is already current")
}

// Should see that the ramping version of the task queues in the current version is unversioned
func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_Unversioned_VersionedCurrent() {
	env := s.newTestEnv()
	go s.pollFromDeployment(env, env.Tv())
	s.ensureCreateVersionInDeployment(env, env.Tv())

	// check that the current version's task queues have ramping version == ""
	s.setCurrentVersion(env, env.Tv(), true, "")
	s.verifyTaskQueueVersioningInfo(env, env.Tv().TaskQueue(), env.Tv().DeploymentVersionString(), "", 0)

	// set ramp to unversioned
	s.setAndVerifyRampingVersionUnversionedOption(env, env.Tv(), true, false, 75, true, false, true, "")

	// check that deployment has ramping version == __unversioned__
	resp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentSeries(),
	})
	s.NoError(err)
	s.Equal(worker_versioning.UnversionedVersionId, resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetRampingVersion())

	// check that the current version's task queues have ramping version == __unversioned__
	s.verifyTaskQueueVersioningInfo(env, env.Tv().TaskQueue(), env.Tv().DeploymentVersionString(), worker_versioning.UnversionedVersionId, 75)
}

func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentCurrentVersion_NoPollers() {
	env := s.newTestEnv()

	// try to set current with allowNoPollers=false --> error
	allowNoPollers := false
	expectedErr := fmt.Sprintf(workerdeployment.ErrWorkerDeploymentNotFound, env.Tv().DeploymentVersion().GetDeploymentName())
	s.setCurrentVersionAllowNoPollersOption(env, env.Tv(), true, allowNoPollers, false, expectedErr)

	// try to set current with allowNoPollers=true --> success
	allowNoPollers = true
	expectedErr = ""
	versionCreateTime := timestamppb.Now()
	s.setCurrentVersionAllowNoPollersOption(env, env.Tv(), true, allowNoPollers, false, expectedErr)

	// let a poller arrive with that version --> triggers user data propagation
	go s.pollFromDeployment(env, env.Tv())

	s.Await(func(s *WorkerDeploymentSuite) {
		// check describe worker deployment
		resp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      env.Namespace().String(),
			DeploymentName: env.Tv().DeploymentSeries(),
		})
		s.NoError(err)
		s.verifyWorkerDeploymentInfo(&deploymentpb.WorkerDeploymentInfo{
			Name:       env.Tv().DeploymentSeries(),
			CreateTime: versionCreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				CurrentVersion:            env.Tv().DeploymentVersionString(),
				CurrentVersionChangedTime: versionCreateTime,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              env.Tv().DeploymentVersionString(),
					CreateTime:           versionCreateTime,
					DrainageInfo:         nil,
					RoutingUpdateTime:    versionCreateTime,
					CurrentSinceTime:     versionCreateTime,
					RampingSinceTime:     nil,
					FirstActivationTime:  versionCreateTime,
					LastCurrentTime:      versionCreateTime,
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
				},
			},
			LastModifierIdentity: env.Tv().ClientIdentity(),
		}, resp.GetWorkerDeploymentInfo())
	}, 10*time.Second, 100*time.Millisecond)

	// that poller's task queue should have the current versioning info
	s.verifyTaskQueueVersioningInfo(env, env.Tv().TaskQueue(), env.Tv().DeploymentVersionString(), "", 0)
}

func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_NoPollers() {
	env := s.newTestEnv()

	// try to set ramping with allowNoPollers=false --> error
	allowNoPollers := false
	expectedErr := fmt.Sprintf(workerdeployment.ErrWorkerDeploymentNotFound, env.Tv().DeploymentVersion().GetDeploymentName())
	s.setAndVerifyRampingVersionUnversionedOption(env, env.Tv(), false, false, 5, true, allowNoPollers, false, expectedErr)

	// try to set ramping with allowNoPollers=true --> success
	allowNoPollers = true
	expectedErr = ""
	versionCreateTime := timestamppb.Now()
	s.setAndVerifyRampingVersionUnversionedOption(env, env.Tv(), false, false, 5, true, allowNoPollers, false, expectedErr)

	// let a poller arrive with that version --> triggers user data propagation
	go s.pollFromDeployment(env, env.Tv())

	s.Await(func(s *WorkerDeploymentSuite) {
		// check describe worker deployment
		resp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      env.Namespace().String(),
			DeploymentName: env.Tv().DeploymentSeries(),
		})
		s.NoError(err)
		s.verifyWorkerDeploymentInfo(&deploymentpb.WorkerDeploymentInfo{
			Name:       env.Tv().DeploymentSeries(),
			CreateTime: versionCreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:                      env.Tv().DeploymentVersionString(),
				RampingVersionPercentage:            5,
				RampingVersionChangedTime:           versionCreateTime,
				RampingVersionPercentageChangedTime: versionCreateTime,
				CurrentVersion:                      worker_versioning.UnversionedVersionId,
				CurrentVersionChangedTime:           nil,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              env.Tv().DeploymentVersionString(),
					CreateTime:           versionCreateTime,
					DrainageInfo:         nil,
					RoutingUpdateTime:    versionCreateTime,
					CurrentSinceTime:     nil,
					RampingSinceTime:     versionCreateTime,
					FirstActivationTime:  versionCreateTime,
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING,
				},
			},
			LastModifierIdentity: env.Tv().ClientIdentity(),
		}, resp.GetWorkerDeploymentInfo())
	}, 10*time.Second, 100*time.Millisecond)

	// that poller's task queue should have the ramping version info
	s.verifyTaskQueueVersioningInfo(env, env.Tv().TaskQueue(), worker_versioning.UnversionedVersionId, env.Tv().DeploymentVersionString(), 5)
}

func (s *WorkerDeploymentSuite) TestTwoPollers_EnsureCreateVersion() {
	env := s.newTestEnv()

	tv1 := env.Tv().WithBuildIDNumber(1)
	tv2 := env.Tv().WithBuildIDNumber(2)

	go s.pollFromDeployment(env, tv1)
	go s.pollFromDeployment(env, tv2)
	s.ensureCreateVersionWithExpectedTaskQueues(env, tv1, 1)
	s.ensureCreateVersionWithExpectedTaskQueues(env, tv2, 1)
}

func (s *WorkerDeploymentSuite) verifyTaskQueueVersioningInfo(env *testcore.TestEnv, tq *taskqueuepb.TaskQueue, expectedCurrentVersion, expectedRampingVersion string, expectedPercentage float32) {
	s.EventuallyWithT(func(t *assert.CollectT) {
		tqDesc, err := env.FrontendClient().DescribeTaskQueue(s.Context(), &workflowservice.DescribeTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: tq,
		})
		a := require.New(t)
		a.NoError(err)
		a.Equal(worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(expectedCurrentVersion), tqDesc.GetVersioningInfo().GetCurrentDeploymentVersion())
		a.Equal(worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(expectedRampingVersion), tqDesc.GetVersioningInfo().GetRampingDeploymentVersion())
		a.Equal(expectedCurrentVersion, tqDesc.GetVersioningInfo().GetCurrentVersion()) //nolint:staticcheck // SA1019: old worker versioning
		a.Equal(expectedRampingVersion, tqDesc.GetVersioningInfo().GetRampingVersion()) //nolint:staticcheck // SA1019: old worker versioning
		a.Equal(expectedPercentage, tqDesc.GetVersioningInfo().GetRampingVersionPercentage())

	}, time.Second*10, time.Millisecond*1000)
}

// This test shall first rollback a drained version to a current version. After that, it shall deploy a new version
// which shall further drain this current version.
// Note: This test reproduces a bug we saw in production where the drainage status was not being properly cleared when a draining version
// is reactivated and then re-deactivated
func (s *WorkerDeploymentSuite) TestDrainRollbackedVersion() {
	env := s.newTestEnv(testcore.WithDynamicConfig(dynamicconfig.PollerHistoryTTL, 500*time.Millisecond))

	tv1 := env.Tv().WithBuildIDNumber(1)
	tv2 := env.Tv().WithBuildIDNumber(2)
	tv3 := env.Tv().WithBuildIDNumber(3)

	// Start deployment workflow 1 and wait for the deployment version to exist
	v1CreateTime := timestamppb.New(time.Now())
	s.startVersionWorkflow(env, tv1)

	// Set v1 as current version
	setCurrentV1UpdateTime := timestamppb.Now()
	s.setCurrentVersion(env, tv1, true, "")
	resp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       env.Tv().DeploymentSeries(),
			CreateTime: v1CreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				CurrentVersion:            tv1.DeploymentVersionString(),
				CurrentVersionChangedTime: setCurrentV1UpdateTime,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              tv1.DeploymentVersionString(),
					CreateTime:           v1CreateTime,
					DrainageInfo:         nil,
					RoutingUpdateTime:    setCurrentV1UpdateTime,
					CurrentSinceTime:     setCurrentV1UpdateTime,
					RampingSinceTime:     nil,
					FirstActivationTime:  setCurrentV1UpdateTime,
					LastCurrentTime:      setCurrentV1UpdateTime,
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
				},
			},
			LastModifierIdentity: env.Tv().ClientIdentity(),
		},
	})

	// Start deployment workflow 2 and set v2 to current so that v1 can start draining
	v2CreateTime := timestamppb.New(time.Now())
	s.startVersionWorkflow(env, tv2)

	setCurrentV2UpdateTime := timestamppb.New(time.Now())
	s.setCurrentVersion(env, tv2, true, "")
	resp, err = env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: tv2.DeploymentSeries(),
	})
	s.NoError(err)

	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       tv2.DeploymentSeries(),
			CreateTime: v1CreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:                      "",
				RampingVersionPercentage:            0,
				RampingVersionChangedTime:           nil,
				RampingVersionPercentageChangedTime: nil,
				CurrentVersion:                      tv2.DeploymentVersionString(),
				CurrentVersionChangedTime:           setCurrentV2UpdateTime,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              tv1.DeploymentVersionString(),
					CreateTime:           v1CreateTime,
					DrainageInfo:         &deploymentpb.VersionDrainageInfo{Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINING},
					RoutingUpdateTime:    setCurrentV2UpdateTime,
					CurrentSinceTime:     nil,
					RampingSinceTime:     nil,
					FirstActivationTime:  setCurrentV1UpdateTime,
					LastCurrentTime:      setCurrentV1UpdateTime,
					LastDeactivationTime: setCurrentV2UpdateTime,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
				},
				{
					Version:              tv2.DeploymentVersionString(),
					CreateTime:           v2CreateTime,
					DrainageInfo:         nil,
					RoutingUpdateTime:    setCurrentV2UpdateTime,
					CurrentSinceTime:     setCurrentV2UpdateTime,
					RampingSinceTime:     nil,
					FirstActivationTime:  setCurrentV2UpdateTime,
					LastCurrentTime:      setCurrentV2UpdateTime,
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
				},
			},
			LastModifierIdentity: tv2.ClientIdentity(),
		},
	})

	// wait for v1 to be drained
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := env.FrontendClient().DescribeWorkerDeploymentVersion(s.Context(), &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: env.Namespace().String(),
			Version:   tv1.DeploymentVersionString(),
		})
		a.NoError(err)
		a.Equal(enumspb.VERSION_DRAINAGE_STATUS_DRAINED, resp.GetWorkerDeploymentVersionInfo().GetDrainageInfo().GetStatus())
	}, time.Second*10, time.Millisecond*1000)

	// Verify that the drainageStatus of v1 has been updated in the VersionSummaries
	s.Await(func(s *WorkerDeploymentSuite) {
		resp, err = env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      env.Namespace().String(),
			DeploymentName: tv2.DeploymentSeries(),
		})
		s.NoError(err)
		s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
			WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
				Name:       tv2.DeploymentSeries(),
				CreateTime: v1CreateTime,
				RoutingConfig: &deploymentpb.RoutingConfig{
					RampingVersion:                      "",
					RampingVersionPercentage:            0,
					RampingVersionChangedTime:           nil,
					RampingVersionPercentageChangedTime: nil,
					CurrentVersion:                      tv2.DeploymentVersionString(),
					CurrentVersionChangedTime:           setCurrentV2UpdateTime,
				},
				VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
					{
						Version:              tv1.DeploymentVersionString(),
						CreateTime:           v1CreateTime,
						DrainageInfo:         &deploymentpb.VersionDrainageInfo{Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINED},
						RoutingUpdateTime:    setCurrentV2UpdateTime,
						CurrentSinceTime:     nil,
						RampingSinceTime:     nil,
						FirstActivationTime:  setCurrentV1UpdateTime,
						LastCurrentTime:      setCurrentV1UpdateTime,
						LastDeactivationTime: setCurrentV2UpdateTime,
						Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED,
					},
					{
						Version:              tv2.DeploymentVersionString(),
						CreateTime:           v2CreateTime,
						DrainageInfo:         nil,
						RoutingUpdateTime:    setCurrentV2UpdateTime,
						CurrentSinceTime:     setCurrentV2UpdateTime,
						RampingSinceTime:     nil,
						FirstActivationTime:  setCurrentV2UpdateTime,
						LastCurrentTime:      setCurrentV2UpdateTime,
						LastDeactivationTime: nil,
						Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
					},
				},
				LastModifierIdentity: tv2.ClientIdentity(),
			},
		})
	}, time.Second*10, time.Millisecond*1000)

	// start ramping traffic back to v1
	setRampingUpdateTime := timestamppb.Now()
	s.setAndVerifyRampingVersion(env, tv1, false, 10, false, "")

	// verify if the right information is set in the DescribeWorkerDeployment response
	s.Await(func(s *WorkerDeploymentSuite) {
		resp, err = env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      env.Namespace().String(),
			DeploymentName: tv2.DeploymentSeries(),
		})
		s.NoError(err)
		s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
			WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
				Name:       tv2.DeploymentSeries(),
				CreateTime: v1CreateTime,
				RoutingConfig: &deploymentpb.RoutingConfig{
					RampingVersion:                      tv1.DeploymentVersionString(),
					RampingVersionPercentage:            10,
					RampingVersionChangedTime:           setRampingUpdateTime,
					RampingVersionPercentageChangedTime: setRampingUpdateTime,
					CurrentVersion:                      tv2.DeploymentVersionString(),
					CurrentVersionChangedTime:           setCurrentV2UpdateTime,
				},
				VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
					{
						Version:              tv1.DeploymentVersionString(),
						CreateTime:           v1CreateTime,
						DrainageInfo:         nil,
						RoutingUpdateTime:    setRampingUpdateTime,
						CurrentSinceTime:     nil,
						RampingSinceTime:     setRampingUpdateTime,
						FirstActivationTime:  setCurrentV1UpdateTime, // note: this is setCurrentV1UpdateTime since the version was initially activated when it was current.
						LastCurrentTime:      setCurrentV1UpdateTime,
						LastDeactivationTime: nil,
						Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING,
					},
					{
						Version:              tv2.DeploymentVersionString(),
						CreateTime:           v2CreateTime,
						DrainageInfo:         nil,
						RoutingUpdateTime:    setCurrentV2UpdateTime,
						CurrentSinceTime:     setCurrentV2UpdateTime,
						RampingSinceTime:     nil,
						FirstActivationTime:  setCurrentV2UpdateTime,
						LastCurrentTime:      setCurrentV2UpdateTime,
						LastDeactivationTime: nil,
						Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
					},
				},
				LastModifierIdentity: tv2.ClientIdentity(),
			},
		})
	}, time.Second*10, time.Millisecond*1000)

	// Set version v1 as the current version; this shall drain out v2
	newCurrentV1UpdateTime := timestamppb.Now()
	s.setCurrentVersion(env, tv1, true, "")

	// Verify that v2 is drained
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := env.FrontendClient().DescribeWorkerDeploymentVersion(s.Context(), &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: env.Namespace().String(),
			Version:   tv2.DeploymentVersionString(),
		})
		a.NoError(err)
		a.Equal(enumspb.VERSION_DRAINAGE_STATUS_DRAINED, resp.GetWorkerDeploymentVersionInfo().GetDrainageInfo().GetStatus())
	}, time.Second*10, time.Millisecond*1000)

	// verify if the right information is set in the DescribeWorkerDeployment response
	s.Await(func(s *WorkerDeploymentSuite) {
		resp, err = env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      env.Namespace().String(),
			DeploymentName: env.Tv().DeploymentSeries(),
		})
		s.NoError(err)
		s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
			WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
				Name:       env.Tv().DeploymentSeries(),
				CreateTime: v1CreateTime,
				RoutingConfig: &deploymentpb.RoutingConfig{
					CurrentVersion:                      tv1.DeploymentVersionString(),
					CurrentVersionChangedTime:           newCurrentV1UpdateTime,
					RampingVersion:                      "",
					RampingVersionPercentage:            0,
					RampingVersionChangedTime:           newCurrentV1UpdateTime,
					RampingVersionPercentageChangedTime: newCurrentV1UpdateTime,
				},
				VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
					{
						Version:              tv1.DeploymentVersionString(),
						CreateTime:           v1CreateTime,
						DrainageInfo:         nil,
						RoutingUpdateTime:    newCurrentV1UpdateTime,
						CurrentSinceTime:     newCurrentV1UpdateTime,
						RampingSinceTime:     nil,
						FirstActivationTime:  setCurrentV1UpdateTime, // note: this is setCurrentV1UpdateTime since the version was initially activated when it was current.
						LastCurrentTime:      newCurrentV1UpdateTime, // updated
						LastDeactivationTime: nil,
						Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
					},
					{
						Version:              tv2.DeploymentVersionString(),
						CreateTime:           v2CreateTime,
						DrainageInfo:         &deploymentpb.VersionDrainageInfo{Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINED},
						RoutingUpdateTime:    newCurrentV1UpdateTime,
						CurrentSinceTime:     nil,
						RampingSinceTime:     nil,
						FirstActivationTime:  setCurrentV2UpdateTime,
						LastCurrentTime:      setCurrentV2UpdateTime,
						LastDeactivationTime: newCurrentV1UpdateTime,
						Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED,
					},
				},
				LastModifierIdentity: tv2.ClientIdentity(),
			},
		})
	}, time.Second*10, time.Millisecond*1000)

	// Roll out a new version v3 and set it to current
	v3CreateTime := timestamppb.Now()
	s.startVersionWorkflow(env, tv3)

	newCurrentV3UpdateTime := timestamppb.Now()
	s.setCurrentVersion(env, tv3, true, "")

	// Verify that v1 is drained eventually
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := env.FrontendClient().DescribeWorkerDeploymentVersion(s.Context(), &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: env.Namespace().String(),
			Version:   tv1.DeploymentVersionString(),
		})
		a.NoError(err)
		a.Equal(enumspb.VERSION_DRAINAGE_STATUS_DRAINED, resp.GetWorkerDeploymentVersionInfo().GetDrainageInfo().GetStatus())
	}, time.Second*10, time.Millisecond*1000)

	// Verify that v1, which was rolled back to being current previously, is drained with it's information present
	// in the deployment workflow.
	s.Await(func(s *WorkerDeploymentSuite) {
		resp, err = env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      env.Namespace().String(),
			DeploymentName: env.Tv().DeploymentSeries(),
		})
		s.NoError(err)
		s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
			WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
				Name:       env.Tv().DeploymentSeries(),
				CreateTime: v1CreateTime,
				RoutingConfig: &deploymentpb.RoutingConfig{
					CurrentVersion:                      tv3.DeploymentVersionString(),
					CurrentVersionChangedTime:           newCurrentV3UpdateTime,
					RampingVersion:                      "",
					RampingVersionPercentage:            0,
					RampingVersionChangedTime:           newCurrentV1UpdateTime,
					RampingVersionPercentageChangedTime: newCurrentV1UpdateTime,
				},
				VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
					{
						Version:              tv1.DeploymentVersionString(),
						CreateTime:           v1CreateTime,
						RoutingUpdateTime:    newCurrentV3UpdateTime,
						DrainageInfo:         &deploymentpb.VersionDrainageInfo{Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINED},
						CurrentSinceTime:     nil,
						RampingSinceTime:     nil,
						FirstActivationTime:  setCurrentV1UpdateTime, // note: this is setCurrentV1UpdateTime since the version was initially activated when it was current.
						LastCurrentTime:      newCurrentV1UpdateTime, // updated
						LastDeactivationTime: newCurrentV3UpdateTime,
						Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED,
					},
					{
						Version:              tv2.DeploymentVersionString(),
						CreateTime:           v2CreateTime,
						DrainageInfo:         &deploymentpb.VersionDrainageInfo{Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINED},
						RoutingUpdateTime:    newCurrentV1UpdateTime,
						CurrentSinceTime:     nil,
						RampingSinceTime:     nil,
						FirstActivationTime:  setCurrentV2UpdateTime,
						LastCurrentTime:      setCurrentV2UpdateTime,
						LastDeactivationTime: newCurrentV1UpdateTime,
						Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED,
					},
					{
						Version:              tv3.DeploymentVersionString(),
						CreateTime:           v3CreateTime,
						RoutingUpdateTime:    newCurrentV3UpdateTime,
						DrainageInfo:         nil,
						CurrentSinceTime:     newCurrentV3UpdateTime,
						RampingSinceTime:     nil,
						FirstActivationTime:  newCurrentV3UpdateTime,
						LastCurrentTime:      newCurrentV3UpdateTime,
						LastDeactivationTime: nil,
						Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
					},
				},
				LastModifierIdentity: tv2.ClientIdentity(),
			},
		})
	}, time.Second*10, time.Millisecond*1000)
}

// Test that rolling back to a drained version works
func (s *WorkerDeploymentSuite) TestSetRampingVersion_AfterDrained() {
	env := s.newTestEnv(testcore.WithDynamicConfig(dynamicconfig.PollerHistoryTTL, 500*time.Millisecond))

	tv1 := env.Tv().WithBuildIDNumber(1)
	tv2 := env.Tv().WithBuildIDNumber(2)

	// Start deployment workflow 1 and wait for the deployment version to exist
	v1CreateTime := timestamppb.New(time.Now())
	s.startVersionWorkflow(env, tv1)

	// Set v1 as current version
	setCurrentV1UpdateTime := timestamppb.Now()
	s.setCurrentVersion(env, tv1, true, "")
	resp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       env.Tv().DeploymentSeries(),
			CreateTime: v1CreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				CurrentVersion:            tv1.DeploymentVersionString(),
				CurrentVersionChangedTime: setCurrentV1UpdateTime,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              tv1.DeploymentVersionString(),
					CreateTime:           v1CreateTime,
					DrainageInfo:         nil,
					RoutingUpdateTime:    setCurrentV1UpdateTime,
					CurrentSinceTime:     setCurrentV1UpdateTime,
					RampingSinceTime:     nil,
					FirstActivationTime:  setCurrentV1UpdateTime,
					LastCurrentTime:      setCurrentV1UpdateTime,
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
				},
			},
			LastModifierIdentity: env.Tv().ClientIdentity(),
		},
	})

	// Start deployment workflow 2 and set v2 to current so that v1 can start draining
	v2CreateTime := timestamppb.New(time.Now())
	s.startVersionWorkflow(env, tv2)

	setCurrentV2UpdateTime := timestamppb.New(time.Now())
	s.setCurrentVersion(env, tv2, true, "")
	resp, err = env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: tv2.DeploymentSeries(),
	})
	s.NoError(err)

	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       tv2.DeploymentSeries(),
			CreateTime: v1CreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:                      "",
				RampingVersionPercentage:            0,
				RampingVersionChangedTime:           nil,
				RampingVersionPercentageChangedTime: nil,
				CurrentVersion:                      tv2.DeploymentVersionString(),
				CurrentVersionChangedTime:           setCurrentV2UpdateTime,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              tv1.DeploymentVersionString(),
					CreateTime:           v1CreateTime,
					DrainageInfo:         &deploymentpb.VersionDrainageInfo{Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINING},
					RoutingUpdateTime:    setCurrentV2UpdateTime,
					CurrentSinceTime:     nil,
					RampingSinceTime:     nil,
					FirstActivationTime:  setCurrentV1UpdateTime,
					LastCurrentTime:      setCurrentV1UpdateTime,
					LastDeactivationTime: setCurrentV2UpdateTime,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
				},
				{
					Version:              tv2.DeploymentVersionString(),
					CreateTime:           v2CreateTime,
					DrainageInfo:         nil,
					RoutingUpdateTime:    setCurrentV2UpdateTime,
					CurrentSinceTime:     setCurrentV2UpdateTime,
					RampingSinceTime:     nil,
					FirstActivationTime:  setCurrentV2UpdateTime,
					LastCurrentTime:      setCurrentV2UpdateTime,
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
				},
			},
			LastModifierIdentity: tv2.ClientIdentity(),
		},
	})

	// wait for v1 to be drained
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := env.FrontendClient().DescribeWorkerDeploymentVersion(s.Context(), &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: env.Namespace().String(),
			Version:   tv1.DeploymentVersionString(),
		})
		a.NoError(err)
		a.Equal(enumspb.VERSION_DRAINAGE_STATUS_DRAINED, resp.GetWorkerDeploymentVersionInfo().GetDrainageInfo().GetStatus())
	}, time.Second*10, time.Millisecond*1000)

	// Verify that the drainageStatus of v1 has been updated in the VersionSummaries
	s.Await(func(s *WorkerDeploymentSuite) {
		resp, err = env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      env.Namespace().String(),
			DeploymentName: tv2.DeploymentSeries(),
		})
		s.NoError(err)
		s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
			WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
				Name:       tv2.DeploymentSeries(),
				CreateTime: v1CreateTime,
				RoutingConfig: &deploymentpb.RoutingConfig{
					RampingVersion:                      "",
					RampingVersionPercentage:            0,
					RampingVersionChangedTime:           nil,
					RampingVersionPercentageChangedTime: nil,
					CurrentVersion:                      tv2.DeploymentVersionString(),
					CurrentVersionChangedTime:           setCurrentV2UpdateTime,
				},
				VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
					{
						Version:              tv1.DeploymentVersionString(),
						CreateTime:           v1CreateTime,
						DrainageInfo:         &deploymentpb.VersionDrainageInfo{Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINED},
						RoutingUpdateTime:    setCurrentV2UpdateTime,
						CurrentSinceTime:     nil,
						RampingSinceTime:     nil,
						FirstActivationTime:  setCurrentV1UpdateTime,
						LastCurrentTime:      setCurrentV1UpdateTime,
						LastDeactivationTime: setCurrentV2UpdateTime,
						Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED,
					},
					{
						Version:              tv2.DeploymentVersionString(),
						CreateTime:           v2CreateTime,
						DrainageInfo:         nil,
						RoutingUpdateTime:    setCurrentV2UpdateTime,
						CurrentSinceTime:     setCurrentV2UpdateTime,
						RampingSinceTime:     nil,
						FirstActivationTime:  setCurrentV2UpdateTime,
						LastCurrentTime:      setCurrentV2UpdateTime,
						LastDeactivationTime: nil,
						Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
					},
				},
				LastModifierIdentity: tv2.ClientIdentity(),
			},
		})
	}, time.Second*10, time.Millisecond*1000)

	// start ramping traffic back to v1
	setRampingUpdateTime := timestamppb.Now()
	s.setAndVerifyRampingVersion(env, tv1, false, 10, false, "")

	// verify if the right information is set in the DescribeWorkerDeployment response
	s.Await(func(s *WorkerDeploymentSuite) {
		resp, err = env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      env.Namespace().String(),
			DeploymentName: tv2.DeploymentSeries(),
		})
		s.NoError(err)
		s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
			WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
				Name:       tv2.DeploymentSeries(),
				CreateTime: v1CreateTime,
				RoutingConfig: &deploymentpb.RoutingConfig{
					RampingVersion:                      tv1.DeploymentVersionString(),
					RampingVersionPercentage:            10,
					RampingVersionChangedTime:           setRampingUpdateTime,
					RampingVersionPercentageChangedTime: setRampingUpdateTime,
					CurrentVersion:                      tv2.DeploymentVersionString(),
					CurrentVersionChangedTime:           setCurrentV2UpdateTime,
				},
				VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
					{
						Version:              tv1.DeploymentVersionString(),
						CreateTime:           v1CreateTime,
						DrainageInfo:         nil,
						RoutingUpdateTime:    setRampingUpdateTime,
						CurrentSinceTime:     nil,
						RampingSinceTime:     setRampingUpdateTime,
						FirstActivationTime:  setCurrentV1UpdateTime, // note: this is setCurrentV1UpdateTime since the version was initially activated when it was current.
						LastCurrentTime:      setCurrentV1UpdateTime,
						LastDeactivationTime: nil,
						Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING,
					},
					{
						Version:              tv2.DeploymentVersionString(),
						CreateTime:           v2CreateTime,
						DrainageInfo:         nil,
						RoutingUpdateTime:    setCurrentV2UpdateTime,
						CurrentSinceTime:     setCurrentV2UpdateTime,
						RampingSinceTime:     nil,
						FirstActivationTime:  setCurrentV2UpdateTime,
						LastCurrentTime:      setCurrentV2UpdateTime,
						LastDeactivationTime: nil,
						Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
					},
				},
				LastModifierIdentity: tv2.ClientIdentity(),
			},
		})
	}, time.Second*10, time.Millisecond*1000)
}

func (s *WorkerDeploymentSuite) TestDeleteWorkerDeployment_ValidDelete() {
	env := s.newTestEnv(testcore.WithDynamicConfig(dynamicconfig.PollerHistoryTTL, 500*time.Millisecond))

	// Start deployment workflow 1 and wait for the deployment version to exist
	// Use a cancellable context so we can stop the poller before checking pollers disappeared
	pollerCancel := s.startDeploymentPoller(env, env.Tv())
	defer pollerCancel()
	s.waitForDeploymentVersion(env, env.Tv())

	// Signal the first version to be drained. Only do this in tests.
	versionWorkflowID := workerdeployment.GenerateVersionWorkflowID(env.Tv().DeploymentSeries(), env.Tv().BuildID())
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: versionWorkflowID,
	}
	input := &deploymentpb.VersionDrainageInfo{
		Status:          enumspb.VERSION_DRAINAGE_STATUS_DRAINED,
		LastChangedTime: timestamppb.New(time.Now()),
		LastCheckedTime: timestamppb.New(time.Now()),
	}
	marshaledData, err := input.Marshal()
	s.NoError(err)
	signalPayload := &commonpb.Payloads{
		Payloads: []*commonpb.Payload{
			{
				Metadata: map[string][]byte{
					"encoding": []byte("binary/protobuf"),
				},
				Data: marshaledData,
			},
		},
	}

	err = env.SendSignal(env.Namespace().String(), workflowExecution, workerdeployment.SyncDrainageSignalName, signalPayload, env.Tv().ClientIdentity())
	s.NoError(err)

	// Stop the poller so it doesn't keep polling
	pollerCancel()

	// Wait for pollers going away
	s.EventuallyWithT(func(t *assert.CollectT) {
		resp, err := env.FrontendClient().DescribeTaskQueue(s.Context(), &workflowservice.DescribeTaskQueueRequest{
			Namespace:     env.Namespace().String(),
			TaskQueue:     env.Tv().TaskQueue(),
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		})
		require.NoError(t, err)
		require.Empty(t, resp.Pollers)
	}, 5*time.Second, time.Second)

	// delete succeeds
	s.tryDeleteVersion(env, env.Tv(), "")

	// deployment version does not exist in the deployment list
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      env.Namespace().String(),
			DeploymentName: env.Tv().DeploymentSeries(),
		})
		a.NoError(err)
		for _, vs := range resp.GetWorkerDeploymentInfo().GetVersionSummaries() {
			//nolint:staticcheck // SA1019 deprecated Version will clean up later
			a.NotEqual(env.Tv().DeploymentVersionString(), vs.Version)
			a.False(proto.Equal(env.Tv().ExternalDeploymentVersion(), vs.GetDeploymentVersion()))
		}
	}, time.Second*5, time.Millisecond*200)

	// Deleting the worker deployment should succeed since there are no associated versions left
	_, err = env.FrontendClient().DeleteWorkerDeployment(s.Context(), &workflowservice.DeleteWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentSeries(),
		Identity:       env.Tv().ClientIdentity(),
	})
	s.NoError(err)

	// Describe Worker Deployment should give not found
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		_, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      env.Namespace().String(),
			DeploymentName: env.Tv().DeploymentSeries(),
		})
		a.Error(err)
		var nfe *serviceerror.NotFound
		a.ErrorAs(err, &nfe)
	}, time.Second*5, time.Millisecond*200)

	// ListDeployments should not show the closed/deleted Worker Deployment
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		listResp, err := env.FrontendClient().ListWorkerDeployments(s.Context(), &workflowservice.ListWorkerDeploymentsRequest{
			Namespace: env.Namespace().String(),
		})
		a.NoError(err)
		for _, dInfo := range listResp.GetWorkerDeployments() {
			a.NotEqual(env.Tv().DeploymentSeries(), dInfo.GetName())
		}
	}, time.Second*5, time.Millisecond*200)
}

func (s *WorkerDeploymentSuite) TestDeleteWorkerDeployment_Idempotent() {
	env := s.newTestEnv()

	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		_, err := env.FrontendClient().DeleteWorkerDeployment(s.Context(), &workflowservice.DeleteWorkerDeploymentRequest{
			Namespace:      env.Namespace().String(),
			DeploymentName: env.Tv().DeploymentSeries(),
			Identity:       env.Tv().ClientIdentity(),
		})
		a.NoError(err)
	}, time.Second*5, time.Millisecond*200)
}

func (s *WorkerDeploymentSuite) TestDeleteWorkerDeployment_InvalidDelete() {
	env := s.newTestEnv()

	// Start deployment workflow 1 and wait for the deployment version and deployment workflow to exist
	go s.pollFromDeployment(env, env.Tv())
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := env.FrontendClient().DescribeWorkerDeploymentVersion(s.Context(), &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: env.Namespace().String(),
			Version:   env.Tv().DeploymentVersionString(),
		})
		a.NoError(err)
		a.Equal(env.Tv().DeploymentVersionString(), resp.GetWorkerDeploymentVersionInfo().GetVersion()) //nolint:staticcheck // SA1019: old worker versioning
	}, time.Second*5, time.Millisecond*200)

	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      env.Namespace().String(),
			DeploymentName: env.Tv().DeploymentSeries(),
		})
		a.NoError(err)
		a.NotEmpty(resp.GetWorkerDeploymentInfo().GetVersionSummaries())
		//nolint:staticcheck // SA1019 deprecated Version will clean up later
		a.Equal(env.Tv().DeploymentVersionString(), resp.GetWorkerDeploymentInfo().GetVersionSummaries()[0].Version)
		s.ProtoEqual(env.Tv().ExternalDeploymentVersion(), resp.GetWorkerDeploymentInfo().GetVersionSummaries()[0].GetDeploymentVersion())
	}, time.Second*5, time.Millisecond*200)

	// Delete the worker deployment should fail since there are versions associated with it
	_, err := env.FrontendClient().DeleteWorkerDeployment(s.Context(), &workflowservice.DeleteWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentSeries(),
		Identity:       env.Tv().ClientIdentity(),
	})
	s.Error(err)
}

func (s *WorkerDeploymentSuite) tryDeleteVersion(
	env *testcore.TestEnv,
	tv *testvars.TestVars,
	expectedError string,
) {
	_, err := env.FrontendClient().DeleteWorkerDeploymentVersion(s.Context(), &workflowservice.DeleteWorkerDeploymentVersionRequest{
		Namespace: env.Namespace().String(),
		Version:   tv.DeploymentVersionString(),
		Identity:  tv.ClientIdentity(),
	})
	if expectedError == "" {
		s.NoError(err)
	} else {
		s.Error(err)
		s.Contains(err.Error(), expectedError)
	}
}

// verifyTimestampWithinRange asserts if the actual timestamp is set to an appropriate value. It
// does this check by checking if the timestamp set is within respectable bounds or is equal to the
// expected timestamp.
func (s *WorkerDeploymentSuite) verifyTimestampWithinRange(expected, actual *timestamppb.Timestamp, msg string) {
	s.Equalf(expected == nil, actual == nil,
		"expected %s to be: '%s', actual: %s", msg, expected.AsTime().String(), actual.AsTime().String())
	if expected == nil {
		return
	}
	acceptableDelayBetweenExpectedAndActual := 5 * time.Second // works locally with 2s, but setting to 5 to avoid flakes in CI
	s.Truef(expected.AsTime().Equal(actual.AsTime()) || (actual.AsTime().After(expected.AsTime()) && actual.AsTime().Before(expected.AsTime().Add(acceptableDelayBetweenExpectedAndActual))),
		"expected %s to be: '%s', actual: %s", msg, expected.AsTime().String(), actual.AsTime().String())
}

func (s *WorkerDeploymentSuite) verifyVersionSummary(expected, actual *deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary) {
	s.Equal(expected.GetVersion(), actual.GetVersion())                                                                           //nolint:staticcheck // SA1019: old worker versioning
	s.Equal(worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(expected.GetVersion()), actual.GetDeploymentVersion()) //nolint:staticcheck // SA1019: old worker versioning
	s.Equalf(expected.GetDrainageInfo().GetStatus().String(), actual.GetDrainageInfo().GetStatus().String(),
		"Version %s:%s drainage status mismatch", actual.GetDeploymentVersion().GetDeploymentName(), actual.GetDeploymentVersion().GetBuildId())
	s.Equalf(expected.GetStatus(), actual.GetStatus(),
		"Version %s:%s status mismatch", actual.GetDeploymentVersion().GetDeploymentName(), actual.GetDeploymentVersion().GetBuildId())

	s.verifyTimestampWithinRange(expected.GetCreateTime(), actual.GetCreateTime(),
		fmt.Sprintf("Version %s:%s create time", actual.GetDeploymentVersion().GetDeploymentName(), actual.GetDeploymentVersion().GetBuildId()))
	s.verifyTimestampWithinRange(expected.GetRoutingUpdateTime(), actual.GetRoutingUpdateTime(),
		fmt.Sprintf("Version %s:%s routing update time", actual.GetDeploymentVersion().GetDeploymentName(), actual.GetDeploymentVersion().GetBuildId()))
	s.verifyTimestampWithinRange(expected.GetCurrentSinceTime(), actual.GetCurrentSinceTime(),
		fmt.Sprintf("Version %s:%s current since time", actual.GetDeploymentVersion().GetDeploymentName(), actual.GetDeploymentVersion().GetBuildId()))
	s.verifyTimestampWithinRange(expected.GetRampingSinceTime(), actual.GetRampingSinceTime(),
		fmt.Sprintf("Version %s:%s ramping since time", actual.GetDeploymentVersion().GetDeploymentName(), actual.GetDeploymentVersion().GetBuildId()))
	s.verifyTimestampWithinRange(expected.GetFirstActivationTime(), actual.GetFirstActivationTime(),
		fmt.Sprintf("Version %s:%s first activation time", actual.GetDeploymentVersion().GetDeploymentName(), actual.GetDeploymentVersion().GetBuildId()))
	s.verifyTimestampWithinRange(expected.GetLastCurrentTime(), actual.GetLastCurrentTime(),
		fmt.Sprintf("Version %s:%s last current time", actual.GetDeploymentVersion().GetDeploymentName(), actual.GetDeploymentVersion().GetBuildId()))
	s.verifyTimestampWithinRange(expected.GetLastDeactivationTime(), actual.GetLastDeactivationTime(),
		fmt.Sprintf("Version %s:%s last deactivation time", actual.GetDeploymentVersion().GetDeploymentName(), actual.GetDeploymentVersion().GetBuildId()))
}

func (s *WorkerDeploymentSuite) verifyRoutingConfig(expected, actual *deploymentpb.RoutingConfig, deploymentName string) {
	s.Equal(expected.GetRampingVersion(), actual.GetRampingVersion())                                                                                //nolint:staticcheck // SA1019: old worker versioning
	s.ProtoEqual(worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(expected.GetRampingVersion()), actual.GetRampingDeploymentVersion()) //nolint:staticcheck // SA1019: worker versioning v0.31
	s.InDelta(expected.GetRampingVersionPercentage(), actual.GetRampingVersionPercentage(), 0.001)
	s.Equal(expected.GetCurrentVersion(), actual.GetCurrentVersion())                                                                                //nolint:staticcheck // SA1019: old worker versioning
	s.ProtoEqual(worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(expected.GetCurrentVersion()), actual.GetCurrentDeploymentVersion()) //nolint:staticcheck // SA1019: worker versioning v0.31

	s.verifyTimestampWithinRange(expected.GetRampingVersionChangedTime(), actual.GetRampingVersionChangedTime(),
		fmt.Sprintf("Deployment %s ramping version changed time", deploymentName))
	s.verifyTimestampWithinRange(expected.GetCurrentVersionChangedTime(), actual.GetCurrentVersionChangedTime(),
		fmt.Sprintf("Deployment %s current version changed time", deploymentName))
	s.verifyTimestampWithinRange(expected.GetRampingVersionPercentageChangedTime(), actual.GetRampingVersionPercentageChangedTime(),
		fmt.Sprintf("Deployment %s ramping percentage changed time", deploymentName))
}

func (s *WorkerDeploymentSuite) verifyWorkerDeploymentInfo(expected, actual *deploymentpb.WorkerDeploymentInfo) {
	s.Equal((actual == nil), (expected == nil))
	s.Equal(expected.GetName(), actual.GetName())
	s.Equal((actual.GetRoutingConfig() == nil), (expected.GetRoutingConfig() == nil))
	s.Equal(expected.GetLastModifierIdentity(), actual.GetLastModifierIdentity())

	s.verifyTimestampWithinRange(expected.GetCreateTime(), actual.GetCreateTime(),
		fmt.Sprintf("Deployment %s create time", expected.GetName()))
	s.verifyRoutingConfig(expected.GetRoutingConfig(), actual.GetRoutingConfig(), expected.GetName())

	// Verify version summaries
	for _, expectedSummary := range expected.GetVersionSummaries() {
		found := false
		for _, actualSummary := range actual.GetVersionSummaries() {
			if actualSummary.Version == expectedSummary.Version { //nolint:staticcheck // SA1019: old worker versioning
				s.verifyVersionSummary(expectedSummary, actualSummary)
				found = true
				break
			}
		}
		s.True(found)
	}
}

func (s *WorkerDeploymentSuite) verifyDescribeWorkerDeployment(
	actualResp *workflowservice.DescribeWorkerDeploymentResponse,
	expectedResp *workflowservice.DescribeWorkerDeploymentResponse,
) {
	if expectedResp == nil {
		s.Nil(actualResp)
	} else {
		s.NotNil(actualResp)
	}
	s.verifyWorkerDeploymentInfo(expectedResp.GetWorkerDeploymentInfo(), actualResp.GetWorkerDeploymentInfo())
}

func (s *WorkerDeploymentSuite) setAndVerifyRampingVersion(
	env *testcore.TestEnv,
	tv *testvars.TestVars,
	unset bool,
	percentage int,
	ignoreMissingTaskQueues bool,
	expectedError string,
) {
	s.setAndVerifyRampingVersionUnversionedOption(env, tv, false, unset, percentage, ignoreMissingTaskQueues, false, true, expectedError)
}

//nolint:staticcheck // SA1019
func (s *WorkerDeploymentSuite) setAndVerifyRampingVersionUnversionedOption(
	env *testcore.TestEnv,
	tv *testvars.TestVars,
	unversioned, unset bool,
	percentage int,
	ignoreMissingTaskQueues, allowNoPollers, ensureSystemWorkflowsExist bool,
	expectedError string,
) {
	bld := tv.BuildID()
	if unversioned || unset {
		bld = ""
	}
	if unset {
		percentage = 0
	}
	if !allowNoPollers && ensureSystemWorkflowsExist {
		if !unversioned && !unset {
			s.ensureCreateVersionInDeployment(env, tv)
		} else {
			s.ensureCreateDeployment(env, tv)
		}
	}
	_, err := env.FrontendClient().SetWorkerDeploymentRampingVersion(s.Context(), &workflowservice.SetWorkerDeploymentRampingVersionRequest{
		Namespace:               env.Namespace().String(),
		DeploymentName:          tv.DeploymentVersion().GetDeploymentName(),
		BuildId:                 bld,
		Percentage:              float32(percentage),
		Identity:                tv.ClientIdentity(),
		IgnoreMissingTaskQueues: ignoreMissingTaskQueues,
		AllowNoPollers:          allowNoPollers,
	})
	if expectedError != "" {
		s.Error(err)
		s.Contains(err.Error(), expectedError)
		return
	}
	s.NoError(err)
}

func (s *WorkerDeploymentSuite) setCurrentVersion(env *testcore.TestEnv, tv *testvars.TestVars, ignoreMissingTaskQueues bool, expectedError string) {
	s.setCurrentVersionUnversionedOption(env, tv, false, ignoreMissingTaskQueues, false, true, expectedError)
}

func (s *WorkerDeploymentSuite) setCurrentVersionAllowNoPollersOption(
	env *testcore.TestEnv,
	tv *testvars.TestVars,
	ignoreMissingTaskQueues, allowNoPollers, ensureSystemWorkflowsExist bool,
	expectedError string,
) {
	s.setCurrentVersionUnversionedOption(env, tv, false, ignoreMissingTaskQueues, allowNoPollers, ensureSystemWorkflowsExist, expectedError)
}

func (s *WorkerDeploymentSuite) setCurrentVersionUnversionedOption(
	env *testcore.TestEnv,
	tv *testvars.TestVars,
	unversioned, ignoreMissingTaskQueues, allowNoPollers, ensureSystemWorkflowsExist bool,
	expectedError string,
) {
	bld := tv.DeploymentVersion().GetBuildId()
	if unversioned {
		bld = ""
	}
	if !allowNoPollers && ensureSystemWorkflowsExist {
		if unversioned {
			s.ensureCreateDeployment(env, tv)
		} else {
			s.ensureCreateVersionInDeployment(env, tv)
		}
	}

	_, err := env.FrontendClient().SetWorkerDeploymentCurrentVersion(s.Context(), &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:               env.Namespace().String(),
		DeploymentName:          tv.DeploymentVersion().GetDeploymentName(),
		BuildId:                 bld,
		IgnoreMissingTaskQueues: ignoreMissingTaskQueues,
		Identity:                tv.ClientIdentity(),
		AllowNoPollers:          allowNoPollers,
	})
	if expectedError != "" {
		s.Error(err)
		s.Contains(err.Error(), expectedError)
		return
	}
	s.NoError(err)
}

func (s *WorkerDeploymentSuite) setAndValidateManagerIdentity(env *testcore.TestEnv, tv *testvars.TestVars, self, useWrongConflictToken bool, newManager, expectedError string) {
	s.ensureCreateDeployment(env, tv)
	s.ensureCreateVersionInDeployment(env, tv)

	var cT []byte
	if useWrongConflictToken {
		cT, _ = time.Now().MarshalBinary()
	} else {
		desc, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      env.Namespace().String(),
			DeploymentName: tv.DeploymentVersion().GetDeploymentName(),
		})
		s.NoError(err)
		cT = desc.GetConflictToken()
	}

	req := &workflowservice.SetWorkerDeploymentManagerRequest{
		Namespace:          env.Namespace().String(),
		DeploymentName:     tv.DeploymentVersion().GetDeploymentName(),
		NewManagerIdentity: &workflowservice.SetWorkerDeploymentManagerRequest_ManagerIdentity{},
		Identity:           tv.ClientIdentity(),
		ConflictToken:      cT,
	}
	var expectedManagerIdentity string
	if self {
		req.NewManagerIdentity = &workflowservice.SetWorkerDeploymentManagerRequest_Self{
			Self: true,
		}
		expectedManagerIdentity = tv.ClientIdentity()
	} else {
		req.NewManagerIdentity = &workflowservice.SetWorkerDeploymentManagerRequest_ManagerIdentity{
			ManagerIdentity: newManager,
		}
		expectedManagerIdentity = newManager
		if newManager == "" {
			req.Identity = tv.ClientIdentity()
		} else {
			req.Identity = newManager
		}
	}
	_, err := env.FrontendClient().SetWorkerDeploymentManager(s.Context(), req)
	if expectedError != "" {
		s.Error(err)
		s.Contains(err.Error(), expectedError)
		return
	}
	s.NoError(err)

	desc, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: tv.DeploymentVersion().GetDeploymentName(),
	})
	s.NoError(err)
	s.Equal(expectedManagerIdentity, desc.GetWorkerDeploymentInfo().GetManagerIdentity())
}

func (s *WorkerDeploymentSuite) createVersionsInDeployments(env *testcore.TestEnv, tv *testvars.TestVars, n int) []*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary {
	var expectedDeploymentSummaries []*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary

	for i := range n {
		deployment := tv.WithDeploymentSeriesNumber(i)
		version := deployment.WithBuildIDNumber(i)

		startTime := timestamppb.Now()
		s.startVersionWorkflow(env, version)
		setCurrentTime := timestamppb.Now()
		s.setCurrentVersion(env, version, true, "")

		currentVersionSummary := &deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
			Version:              version.DeploymentVersionString(),
			CreateTime:           startTime,
			CurrentSinceTime:     setCurrentTime,
			FirstActivationTime:  setCurrentTime,
			LastCurrentTime:      setCurrentTime,
			RoutingUpdateTime:    setCurrentTime,
			DrainageInfo:         nil,
			RampingSinceTime:     nil,
			LastDeactivationTime: nil,
			Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
		}

		expectedDeployment := s.buildWorkerDeploymentSummary(
			deployment.DeploymentSeries(),
			startTime,
			&deploymentpb.RoutingConfig{
				CurrentVersion:            version.DeploymentVersionString(),
				CurrentVersionChangedTime: setCurrentTime,
			},
			currentVersionSummary, // latest version added is the current version
			currentVersionSummary,
			nil,
		)
		expectedDeploymentSummaries = append(expectedDeploymentSummaries, expectedDeployment)
	}

	return expectedDeploymentSummaries
}

func (s *WorkerDeploymentSuite) verifyWorkerDeploymentSummary(
	expectedSummary *workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary,
	actualSummary *workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary,
) bool {

	s.verifyTimestampWithinRange(expectedSummary.CreateTime, actualSummary.CreateTime,
		fmt.Sprintf("Deployment %s create time", expectedSummary.GetName()))

	// Current version checks
	s.Equal(expectedSummary.RoutingConfig.GetCurrentVersion(), actualSummary.RoutingConfig.GetCurrentVersion(), "Current version mismatch")                                                    //nolint:staticcheck // SA1019: old worker versioning
	s.ProtoEqual(worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(expectedSummary.RoutingConfig.GetCurrentVersion()), actualSummary.RoutingConfig.GetCurrentDeploymentVersion()) //nolint:staticcheck // SA1019: worker versioning v0.31
	s.verifyTimestampWithinRange(expectedSummary.RoutingConfig.GetCurrentVersionChangedTime(), actualSummary.RoutingConfig.GetCurrentVersionChangedTime(),
		fmt.Sprintf("Deployment %s current version changed time", expectedSummary.GetName()))

	// Ramping version checks
	s.Equal(expectedSummary.RoutingConfig.GetRampingVersion(), actualSummary.RoutingConfig.GetRampingVersion(), "Ramping version mismatch")                                                    //nolint:staticcheck // SA1019: old worker versioning
	s.ProtoEqual(worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(expectedSummary.RoutingConfig.GetRampingVersion()), actualSummary.RoutingConfig.GetRampingDeploymentVersion()) //nolint:staticcheck // SA1019: worker versioning v0.31
	s.InDelta(expectedSummary.RoutingConfig.GetRampingVersionPercentage(), actualSummary.RoutingConfig.GetRampingVersionPercentage(), 0.001, "Ramping version percentage mismatch")

	s.verifyTimestampWithinRange(expectedSummary.RoutingConfig.GetRampingVersionChangedTime(), actualSummary.RoutingConfig.GetRampingVersionChangedTime(),
		fmt.Sprintf("Deployment %s ramping version changed time", expectedSummary.GetName()))

	// Latest version summary checks
	s.verifyVersionSummary(expectedSummary.LatestVersionSummary, actualSummary.LatestVersionSummary)

	// Current version summary checks
	s.verifyVersionSummary(expectedSummary.CurrentVersionSummary, actualSummary.CurrentVersionSummary)

	// Ramping version summary checks
	s.verifyVersionSummary(expectedSummary.RampingVersionSummary, actualSummary.RampingVersionSummary)

	return true
}

func (s *WorkerDeploymentSuite) listWorkerDeployments(env *testcore.TestEnv, request *workflowservice.ListWorkerDeploymentsRequest) ([]*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary, error) {
	var resp *workflowservice.ListWorkerDeploymentsResponse
	var err error
	var deploymentSummaries []*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary
	for resp == nil || len(resp.NextPageToken) > 0 {
		resp, err = env.FrontendClient().ListWorkerDeployments(s.Context(), request)
		if err != nil {
			return nil, err
		}
		deploymentSummaries = append(deploymentSummaries, resp.GetWorkerDeployments()...)
		request.NextPageToken = resp.NextPageToken
	}
	return deploymentSummaries, nil
}

func (s *WorkerDeploymentSuite) startAndValidateWorkerDeployments(
	env *testcore.TestEnv,
	request *workflowservice.ListWorkerDeploymentsRequest,
	expectedDeploymentSummaries []*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary,
) {

	s.Await(func(s *WorkerDeploymentSuite) {
		actualDeploymentSummaries, err := s.listWorkerDeployments(env, request)
		s.NoError(err)
		if len(actualDeploymentSummaries) < len(expectedDeploymentSummaries) {
			s.Failf("not enough deployment summaries", "got %d, want at least %d", len(actualDeploymentSummaries), len(expectedDeploymentSummaries))
			return
		}

		for _, expectedDeploymentSummary := range expectedDeploymentSummaries {
			deploymentSummaryFound := false
			for _, actualDeploymentSummary := range actualDeploymentSummaries {
				// Our assumption that deployment summaries with the same name are fully ready for checks
				// may not be true since visibility might take time to update other fields.
				if actualDeploymentSummary.Name != expectedDeploymentSummary.Name {
					continue
				}
				s.verifyWorkerDeploymentSummary(expectedDeploymentSummary, actualDeploymentSummary)
				deploymentSummaryFound = true
				break
			}
			s.True(deploymentSummaryFound)
		}
	}, time.Second*10, time.Millisecond*1000)
}

func (s *WorkerDeploymentSuite) validateWorkerDeploymentCount(
	env *testcore.TestEnv,
	request *workflowservice.ListWorkerDeploymentsRequest,
	expectedCount int,
) {
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		actualDeploymentSummaries, err := s.listWorkerDeployments(env, request)
		a.NoError(err)
		a.Len(actualDeploymentSummaries, expectedCount)
	}, time.Second*5, time.Millisecond*200)
}

func (s *WorkerDeploymentSuite) buildWorkerDeploymentSummary(
	deploymentName string, createTime *timestamppb.Timestamp,
	routingConfig *deploymentpb.RoutingConfig,
	latestVersionSummary *deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary,
	currentVersionSummary *deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary,
	rampingVersionSummary *deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary,
) *workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary {
	return &workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary{
		Name:                  deploymentName,
		CreateTime:            createTime,
		RoutingConfig:         routingConfig,
		LatestVersionSummary:  latestVersionSummary,
		RampingVersionSummary: rampingVersionSummary,
		CurrentVersionSummary: currentVersionSummary,
	}
}

func (s *WorkerDeploymentSuite) TestCreateWorkerDeployment_Success() {
	env := s.newTestEnv()

	deploymentName := env.Tv().DeploymentSeries()
	requestID := env.Tv().Any().String()
	identity := env.Tv().Any().String()

	// Create a new worker deployment
	resp, err := env.FrontendClient().CreateWorkerDeployment(s.Context(), &workflowservice.CreateWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: deploymentName,
		Identity:       identity,
		RequestId:      requestID,
	})

	s.NoError(err)
	s.NotNil(resp)
	s.NotEmpty(resp.ConflictToken)

	// Verify the deployment was created
	descResp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: deploymentName,
	})
	s.NoError(err)
	s.NotNil(descResp)
	s.NotNil(descResp.WorkerDeploymentInfo)
	s.Equal(deploymentName, descResp.WorkerDeploymentInfo.Name)
	s.Equal(identity, descResp.WorkerDeploymentInfo.LastModifierIdentity)
	s.NotNil(descResp.WorkerDeploymentInfo.CreateTime)
	s.Empty(descResp.WorkerDeploymentInfo.VersionSummaries) // No versions initially
}

func (s *WorkerDeploymentSuite) TestCreateWorkerDeployment_Idempotent() {
	env := s.newTestEnv()

	deploymentName := env.Tv().DeploymentSeries()
	requestID := env.Tv().Any().String()

	// Create a worker deployment
	resp1, err := env.FrontendClient().CreateWorkerDeployment(s.Context(), &workflowservice.CreateWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: deploymentName,
		RequestId:      requestID,
	})
	s.NoError(err)
	s.NotNil(resp1)
	token1 := resp1.ConflictToken

	// Create the same deployment again with same request ID - should be idempotent
	resp2, err := env.FrontendClient().CreateWorkerDeployment(s.Context(), &workflowservice.CreateWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: deploymentName,
		RequestId:      requestID,
	})
	s.NoError(err)
	s.NotNil(resp2)
	s.Equal(token1, resp2.ConflictToken) // Should get the same conflict token
}

func (s *WorkerDeploymentSuite) TestCreateWorkerDeployment_AlreadyExists_DifferentRequestID() {
	env := s.newTestEnv()

	deploymentName := env.Tv().DeploymentSeries()
	requestID1 := env.Tv().Any().String()
	requestID2 := env.Tv().Any().String()

	// Create a worker deployment
	_, err := env.FrontendClient().CreateWorkerDeployment(s.Context(), &workflowservice.CreateWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: deploymentName,
		RequestId:      requestID1,
	})
	s.NoError(err)

	// Try to create the same deployment with different request ID - should fail
	_, err = env.FrontendClient().CreateWorkerDeployment(s.Context(), &workflowservice.CreateWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: deploymentName,
		RequestId:      requestID2,
	})
	s.Error(err)
	var alreadyExists *serviceerror.AlreadyExists
	s.ErrorAs(err, &alreadyExists)
	s.Contains(alreadyExists.Message, deploymentName)
}

func (s *WorkerDeploymentSuite) TestCreateWorkerDeployment_AutoCreatedByPoller_ConflictWithExplicitCreate() {
	env := s.newTestEnv()

	// First, create deployment via polling
	go s.pollFromDeployment(env, env.Tv())
	s.ensureCreateDeployment(env, env.Tv())

	// Try to explicitly create the same deployment
	_, err := env.FrontendClient().CreateWorkerDeployment(s.Context(), &workflowservice.CreateWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentSeries(),
		RequestId:      env.Tv().Any().String(),
	})
	s.Error(err)
	var alreadyExists *serviceerror.AlreadyExists
	s.ErrorAs(err, &alreadyExists)
	s.Contains(alreadyExists.Message, "auto-created from worker polls")
}

func (s *WorkerDeploymentSuite) TestCreateWorkerDeployment_InvalidDeploymentName() {
	testCases := []struct {
		name           string
		deploymentName string
		expectedError  string
	}{
		{
			name:           "empty name",
			deploymentName: "",
			expectedError:  "deployment name cannot be empty",
		},
		{
			name:           "name with dot",
			deploymentName: "test.deployment",
			expectedError:  "worker deployment name cannot contain '.'",
		},
		{
			name:           "name starting with __",
			deploymentName: "__reserved",
			expectedError:  "WorkerDeploymentName cannot start with '__'",
		},
		{
			name:           "name too long",
			deploymentName: strings.Repeat("a", 1001),
			expectedError:  "size of WorkerDeploymentName larger than the maximum allowed",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func(s *WorkerDeploymentSuite) {
			env := s.newTestEnv()
			_, err := env.FrontendClient().CreateWorkerDeployment(s.Context(), &workflowservice.CreateWorkerDeploymentRequest{
				Namespace:      env.Namespace().String(),
				DeploymentName: tc.deploymentName,
				RequestId:      env.Tv().Any().String(),
			})
			s.Error(err)
			var invalidArg *serviceerror.InvalidArgument
			s.ErrorAs(err, &invalidArg)
			s.Contains(invalidArg.Message, tc.expectedError)
		})
	}
}

func (s *WorkerDeploymentSuite) TestCreateWorkerDeployment_MaxDeploymentsLimit() {
	// TODO (carly): check the error messages that poller receives in each case and make sense they are informative and appropriate (e.g. do not expose internal stuff)
	// Also in TestNamespaceDeploymentsLimit
	s.T().Skip() // Need to separate this test so other tests do not create deployment in the same NS

	// Override the max deployments limit for this test
	env := s.newTestEnv(testcore.WithDynamicConfig(dynamicconfig.MatchingMaxDeployments, 2))

	// Create first deployment
	_, err := env.FrontendClient().CreateWorkerDeployment(s.Context(), &workflowservice.CreateWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentSeries() + "_1",
		RequestId:      env.Tv().Any().String(),
	})
	s.NoError(err)

	// Create second deployment
	_, err = env.FrontendClient().CreateWorkerDeployment(s.Context(), &workflowservice.CreateWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentSeries() + "_2",
		RequestId:      env.Tv().Any().String(),
	})
	s.NoError(err)

	// wait for all existing deployments to show up in visibility
	s.validateWorkerDeploymentCount(env, &workflowservice.ListWorkerDeploymentsRequest{Namespace: env.Namespace().String()}, 2)

	// Try to create third deployment - should fail
	_, err = env.FrontendClient().CreateWorkerDeployment(s.Context(), &workflowservice.CreateWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: env.Tv().DeploymentSeries() + "_3",
		RequestId:      env.Tv().Any().String(),
	})
	s.Error(err)
	var resourceExhausted *serviceerror.ResourceExhausted
	s.ErrorAs(err, &resourceExhausted)
	s.Contains(resourceExhausted.Message, "reached maximum deployments in namespace")
	s.Equal(enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE, resourceExhausted.Scope)
	s.Equal(enumspb.RESOURCE_EXHAUSTED_CAUSE_WORKER_DEPLOYMENT_LIMITS, resourceExhausted.Cause)
}

func (s *WorkerDeploymentSuite) TestCreateWorkerDeployment_AfterDelete_CanRecreate() {
	env := s.newTestEnv()

	deploymentName := env.Tv().DeploymentSeries()
	requestID1 := env.Tv().Any().String()
	requestID2 := env.Tv().Any().String()

	// Create a worker deployment
	resp1, err := env.FrontendClient().CreateWorkerDeployment(s.Context(), &workflowservice.CreateWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: deploymentName,
		RequestId:      requestID1,
	})
	s.NoError(err)
	s.NotNil(resp1)

	// Delete the deployment
	_, err = env.FrontendClient().DeleteWorkerDeployment(s.Context(), &workflowservice.DeleteWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: deploymentName,
		Identity:       "test",
	})
	s.NoError(err)

	// Should be able to create a deployment with the same name again
	resp2, err := env.FrontendClient().CreateWorkerDeployment(s.Context(), &workflowservice.CreateWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: deploymentName,
		RequestId:      requestID2,
	})
	s.NoError(err)
	s.NotNil(resp2)
	s.NotEqual(resp1.ConflictToken, resp2.ConflictToken) // Should be a new deployment with different token

	// Verify the deployment was created
	descResp, err := env.FrontendClient().DescribeWorkerDeployment(s.Context(), &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      env.Namespace().String(),
		DeploymentName: deploymentName,
	})
	s.NoError(err)
	s.NotNil(descResp)
	s.NotNil(descResp.WorkerDeploymentInfo)
	s.Equal(deploymentName, descResp.WorkerDeploymentInfo.Name)
	s.NotNil(descResp.WorkerDeploymentInfo.CreateTime)
	s.Empty(descResp.WorkerDeploymentInfo.VersionSummaries) // No versions initially
}

func (s *WorkerDeploymentSuite) skipBeforeVersion(version workerdeployment.DeploymentWorkflowVersion) {
	if workerdeployment.VersionDataRevisionNumber < version {
		s.T().Skipf("test supports version %v and newer", version)
	}
}

func (s *WorkerDeploymentSuite) skipFromVersion(version workerdeployment.DeploymentWorkflowVersion) {
	if workerdeployment.VersionDataRevisionNumber >= version {
		s.T().Skipf("test supports version older than %v", version)
	}
}
