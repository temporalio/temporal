package tests

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/worker/workerdeployment"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	WorkerDeploymentSuite struct {
		testcore.FunctionalTestBase
	}
)

func TestWorkerDeploymentSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(WorkerDeploymentSuite))
}

func (s *WorkerDeploymentSuite) SetupSuite() {
	s.FunctionalTestBase.SetupSuiteWithCluster(testcore.WithDynamicConfigOverrides(map[dynamicconfig.Key]any{
		dynamicconfig.EnableDeploymentVersions.Key():                   true,
		dynamicconfig.FrontendEnableWorkerVersioningDataAPIs.Key():     true, // [wv-cleanup-pre-release]
		dynamicconfig.FrontendEnableWorkerVersioningWorkflowAPIs.Key(): true, // [wv-cleanup-pre-release]
		dynamicconfig.FrontendEnableWorkerVersioningRuleAPIs.Key():     true, // [wv-cleanup-pre-release]
		dynamicconfig.FrontendEnableExecuteMultiOperation.Key():        true,

		// Make sure we don't hit the rate limiter in tests
		dynamicconfig.FrontendGlobalNamespaceNamespaceReplicationInducingAPIsRPS.Key():                1000,
		dynamicconfig.FrontendMaxNamespaceNamespaceReplicationInducingAPIsBurstRatioPerInstance.Key(): 1,
		dynamicconfig.FrontendNamespaceReplicationInducingAPIsRPS.Key():                               1000,

		// Make drainage happen sooner
		dynamicconfig.VersionDrainageStatusRefreshInterval.Key():       testVersionDrainageRefreshInterval,
		dynamicconfig.VersionDrainageStatusVisibilityGracePeriod.Key(): testVersionDrainageVisibilityGracePeriod,

		// To increase the rate at which the per-ns worker can consume tasks from a task queue. Required since
		// tests in this suite create a lot of tasks and expect them to be consumed quickly.
		dynamicconfig.WorkerPerNamespaceWorkerOptions.Key(): sdkworker.Options{
			MaxConcurrentWorkflowTaskPollers: 100,
			MaxConcurrentActivityTaskPollers: 100,
		},

		dynamicconfig.MatchingMaxTaskQueuesInDeploymentVersion.Key(): 1000,
		dynamicconfig.VisibilityPersistenceSlowQueryThreshold.Key():  60 * time.Second,
		dynamicconfig.WorkflowExecutionMaxInFlightUpdates.Key():      1000,
	}))
}

func (s *WorkerDeploymentSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()
}

// pollFromDeployment calls PollWorkflowTaskQueue to start deployment related workflows
func (s *WorkerDeploymentSuite) pollFromDeployment(ctx context.Context, tv *testvars.TestVars) {
	_, _ = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace:         s.Namespace().String(),
		TaskQueue:         tv.TaskQueue(),
		Identity:          "random",
		DeploymentOptions: tv.WorkerDeploymentOptions(true),
	})
}

func (s *WorkerDeploymentSuite) pollFromDeploymentWithTaskQueueNumber(ctx context.Context, tv *testvars.TestVars, taskQueueNumber int) {
	_, _ = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace:         s.Namespace().String(),
		TaskQueue:         tv.WithTaskQueueNumber(taskQueueNumber).TaskQueue(),
		Identity:          "random",
		DeploymentOptions: tv.WorkerDeploymentOptions(true),
	})
}

func (s *WorkerDeploymentSuite) pollFromDeploymentExpectFail(ctx context.Context, tv *testvars.TestVars, expectedError string) {
	_, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace:         s.Namespace().String(),
		TaskQueue:         tv.TaskQueue(),
		Identity:          "random",
		DeploymentOptions: tv.WorkerDeploymentOptions(true),
	})
	s.Error(err)
	s.Equal(expectedError, err.Error())
}

func (s *WorkerDeploymentSuite) ensureCreateVersionWithExpectedTaskQueues(ctx context.Context, tv *testvars.TestVars, expectedTaskQueues int) {
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		respV, _ := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: s.Namespace().String(),
			Version:   tv.DeploymentVersionString(),
		})

		a.Equal(expectedTaskQueues, len(respV.GetWorkerDeploymentVersionInfo().GetTaskQueueInfos()))
	}, 5*time.Minute, 500*time.Millisecond)
}

func (s *WorkerDeploymentSuite) ensureCreateVersionInDeployment(
	tv *testvars.TestVars,
) {
	v := tv.DeploymentVersionString()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		res, _ := s.FrontendClient().DescribeWorkerDeployment(ctx,
			&workflowservice.DescribeWorkerDeploymentRequest{
				Namespace:      s.Namespace().String(),
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
	tv *testvars.TestVars,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		res, _ := s.FrontendClient().DescribeWorkerDeployment(ctx,
			&workflowservice.DescribeWorkerDeploymentRequest{
				Namespace:      s.Namespace().String(),
				DeploymentName: tv.DeploymentSeries(),
			})
		a.NotNil(res)
	}, 5*time.Second, 100*time.Millisecond)
}

func (s *WorkerDeploymentSuite) startVersionWorkflow(ctx context.Context, tv *testvars.TestVars) {
	go s.pollFromDeployment(ctx, tv)
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: s.Namespace().String(),
			Version:   tv.DeploymentVersionString(),
		})
		a.NoError(err)
		a.Equal(tv.DeploymentVersionString(), resp.GetWorkerDeploymentVersionInfo().GetVersion())
	}, time.Minute, time.Second)

}

func (s *WorkerDeploymentSuite) TestForceCAN_NoOpenWFS() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tv := testvars.New(s)

	// Start a version workflow
	s.startVersionWorkflow(ctx, tv)
	s.ensureCreateVersionInDeployment(tv)

	// Set the version as current
	_, err := s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
		Version:        tv.DeploymentVersionString(),
	})
	s.NoError(err)

	// ForceCAN
	workflowID := worker_versioning.GenerateDeploymentWorkflowID(tv.DeploymentSeries())
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
	}

	err = s.SendSignal(s.Namespace().String(), workflowExecution, workerdeployment.ForceCANSignalName, nil, tv.ClientIdentity())
	s.NoError(err)

	// Verify if the state is intact even after a CAN
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentSeries(),
		})
		a.NoError(err)
		a.Equal(tv.DeploymentVersionString(), resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetCurrentVersion())
	}, time.Second*10, time.Millisecond*1000)
}

func (s *WorkerDeploymentSuite) TestDeploymentVersionLimits() {
	s.OverrideDynamicConfig(dynamicconfig.MatchingMaxVersionsInDeployment, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingMaxTaskQueuesInDeploymentVersion, 1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*120)
	defer cancel()

	firstDeployment := testvars.New(s).WithDeploymentSeriesNumber(1)
	secondDeployment := testvars.New(s).WithDeploymentSeriesNumber(2)

	firstDeploymentVersionOne := firstDeployment.WithBuildIDNumber(1)
	firstDeploymentVersionTwo := firstDeployment.WithBuildIDNumber(2)

	secondDeploymentVersionOne := secondDeployment.WithBuildIDNumber(1)

	expectedErrorMaxVersions := fmt.Sprintf("cannot add version %v since maximum number of versions (1) have been registered in the deployment", firstDeploymentVersionTwo.DeploymentVersionString())
	expectedErrorMaxTaskQueues := fmt.Sprintf("cannot add task queue %v since maximum number of task queues (1) have been registered in deployment", secondDeploymentVersionOne.WithTaskQueueNumber(2).TaskQueue().GetName())

	// First deployment version should be fine
	go s.pollFromDeployment(ctx, firstDeploymentVersionOne)
	s.ensureCreateVersionInDeployment(firstDeploymentVersionOne)

	// pollers of second version in the same deployment should be rejected
	s.pollFromDeploymentExpectFail(ctx, firstDeploymentVersionTwo, expectedErrorMaxVersions)

	// But first version of another deployment fine
	go s.pollFromDeployment(ctx, secondDeploymentVersionOne)
	s.ensureCreateVersionInDeployment(secondDeploymentVersionOne)

	// pollers of the second TQ in the same deployment version should be rejected
	s.pollFromDeploymentExpectFail(ctx, secondDeploymentVersionOne.WithTaskQueueNumber(2), expectedErrorMaxTaskQueues)
}

func (s *WorkerDeploymentSuite) TestNamespaceDeploymentsLimit() {
	// TODO (carly): check the error messages that poller receives in each case and make sense they are informative and appropriate (e.g. do not expose internal stuff)
	s.T().Skip() // Need to separate this test so other tests do not create deployment in the same NS

	s.OverrideDynamicConfig(dynamicconfig.MatchingMaxDeployments, 1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	tv := testvars.New(s)

	// First deployment version should be fine
	go s.pollFromDeployment(ctx, tv)
	s.ensureCreateVersionInDeployment(tv)

	// pollers of the second deployment version should be rejected
	s.pollFromDeploymentExpectFail(ctx, tv.WithDeploymentSeriesNumber(2), "Namespace deployments limit reached")
}

func (s *WorkerDeploymentSuite) TestDescribeWorkerDeployment_TwoVersions_Sorted() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	tv := testvars.New(s)

	// Starting two versions of the deployment
	firstVersion := tv.WithBuildIDNumber(1)
	secondVersion := tv.WithBuildIDNumber(2)

	go s.pollFromDeployment(ctx, firstVersion)

	// waiting for 1ms to start the second version later.
	startTime := time.Now()
	waitTime := 1 * time.Millisecond
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		a.Greater(time.Since(startTime), waitTime)
	}, 10*time.Second, 1000*time.Millisecond)

	go s.pollFromDeployment(ctx, secondVersion)

	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)

		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentSeries(),
		})
		a.NoError(err)
		a.NotNil(resp.GetWorkerDeploymentInfo())
		a.Equal(tv.DeploymentSeries(), resp.GetWorkerDeploymentInfo().GetName())

		a.NotNil(resp.GetWorkerDeploymentInfo().GetVersionSummaries())
		a.Equal(2, len(resp.GetWorkerDeploymentInfo().GetVersionSummaries()))

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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	tv := testvars.New(s)

	numVersions := 10

	for i := 0; i < numVersions; i++ {
		go s.pollFromDeployment(ctx, tv.WithBuildIDNumber(i))

		// waiting for 1ms to start the next version later.
		startTime := time.Now()
		waitTime := 1 * time.Millisecond
		s.EventuallyWithT(func(t *assert.CollectT) {
			require.Greater(t, time.Since(startTime), waitTime)
		}, 10*time.Second, 1000*time.Millisecond)
	}

	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)

		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentSeries(),
		})
		a.NoError(err)

		a.NotNil(resp.GetWorkerDeploymentInfo().GetVersionSummaries())
		a.Equal(numVersions, len(resp.GetWorkerDeploymentInfo().GetVersionSummaries()))

		// Verify that the version summaries are sorted.
		versionSummaries := resp.GetWorkerDeploymentInfo().GetVersionSummaries()
		for i := 0; i < numVersions-1; i++ {
			a.Less(versionSummaries[i+1].GetCreateTime().AsTime(), versionSummaries[i].GetCreateTime().AsTime())
		}
	}, time.Second*10, time.Millisecond*1000)
}

// Testing ConflictToken
func (s *WorkerDeploymentSuite) TestConflictToken_Describe_SetCurrent_SetRamping() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	tv := testvars.New(s)

	firstVersion := tv.WithBuildIDNumber(1)
	secondVersion := tv.WithBuildIDNumber(2)

	// Start deployment version workflow + worker-deployment workflow.
	go s.pollFromDeployment(ctx, firstVersion)
	go s.pollFromDeployment(ctx, secondVersion)

	var cT []byte
	// No current deployment version set.
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)

		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentSeries(),
		})
		a.NoError(err)
		a.Equal(worker_versioning.UnversionedVersionId, resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetCurrentVersion())
		cT = resp.GetConflictToken()
	}, time.Second*10, time.Millisecond*1000)

	s.ensureCreateVersionInDeployment(firstVersion)
	// Set first version as current version
	_, err := s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
		Version:        firstVersion.DeploymentVersionString(),
		ConflictToken:  cT,
	})
	s.Nil(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)

		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentSeries(),
		})
		a.NoError(err)
		a.Equal(firstVersion.DeploymentVersionString(), resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetCurrentVersion())
		cT = resp.GetConflictToken()
	}, time.Second*10, time.Millisecond*1000)

	// Set a new second version and set it as the current version
	go s.pollFromDeployment(ctx, secondVersion)
	_, _ = s.FrontendClient().SetWorkerDeploymentRampingVersion(ctx, &workflowservice.SetWorkerDeploymentRampingVersionRequest{
		Namespace:               s.Namespace().String(),
		DeploymentName:          tv.DeploymentSeries(),
		Version:                 secondVersion.DeploymentVersionString(),
		Percentage:              5,
		ConflictToken:           cT,
		IgnoreMissingTaskQueues: true, // here until we have 'has version started' safeguard in place
	})
	s.Nil(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentSeries(),
		})
		a.NoError(err)
		a.Equal(secondVersion.DeploymentVersionString(), resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetRampingVersion())
	}, time.Second*10, time.Millisecond*1000)
}

func (s *WorkerDeploymentSuite) TestConflictToken_SetCurrent_SetRamping_Wrong() {
	s.OverrideDynamicConfig(dynamicconfig.FrontendMaskInternalErrorDetails, true)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	tv := testvars.New(s)
	expectedError := "conflict token mismatch"

	firstVersion := tv.WithBuildIDNumber(1)

	// Start deployment version workflow + worker-deployment workflow.
	go s.pollFromDeployment(ctx, firstVersion)

	cTWrong, _ := time.Now().MarshalBinary() // wrong token
	// Wait until deployment exists
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentSeries(),
		})
		a.NoError(err)
		a.Equal(worker_versioning.UnversionedVersionId, resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetCurrentVersion())
	}, time.Second*10, time.Millisecond*1000)

	// Set first version as current version with wrong token
	_, err := s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
		Version:        firstVersion.DeploymentVersionString(),
		ConflictToken:  cTWrong,
	})
	s.Equal(err.Error(), expectedError)

	// Set first version as ramping version with wrong token
	_, err = s.FrontendClient().SetWorkerDeploymentRampingVersion(ctx, &workflowservice.SetWorkerDeploymentRampingVersionRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
		Version:        firstVersion.DeploymentVersionString(),
		Percentage:     5,
		ConflictToken:  cTWrong,
	})
	s.Equal(err.Error(), expectedError)
}

// Testing ListWorkerDeployments

func (s *WorkerDeploymentSuite) TestListWorkerDeployments_OneVersion_OneDeployment() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tv := testvars.New(s)

	s.startVersionWorkflow(ctx, tv)
	s.ensureCreateVersionInDeployment(tv)

	latestVersionSummary := &deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
		Version:              tv.DeploymentVersionString(),
		CreateTime:           timestamppb.Now(),
		DrainageInfo:         nil,
		RampingSinceTime:     nil,
		CurrentSinceTime:     nil,
		RoutingUpdateTime:    nil,
		FirstActivationTime:  nil,
		LastDeactivationTime: nil,
		Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE,
	}

	expectedDeploymentSummaries := s.buildWorkerDeploymentSummary(
		tv.DeploymentSeries(),
		timestamppb.Now(),
		&deploymentpb.RoutingConfig{
			CurrentVersion: worker_versioning.UnversionedVersionId, // default current version is __unversioned__
		},
		latestVersionSummary,
		nil,
		nil,
	)

	s.startAndValidateWorkerDeployments(ctx, &workflowservice.ListWorkerDeploymentsRequest{
		Namespace: s.Namespace().String(),
	}, []*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary{expectedDeploymentSummaries})
}

func (s *WorkerDeploymentSuite) TestListWorkerDeployments_TwoVersions_SameDeployment_OneCurrent_NoRamping() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tv := testvars.New(s)

	firstVersion := tv.WithBuildIDNumber(1)
	secondVersion := tv.WithBuildIDNumber(2)

	routingInfo := &deploymentpb.RoutingConfig{
		CurrentVersion:            firstVersion.DeploymentVersionString(),
		CurrentVersionChangedTime: timestamppb.Now(),
	}

	s.startVersionWorkflow(ctx, firstVersion)
	s.ensureCreateVersionInDeployment(firstVersion)

	s.startVersionWorkflow(ctx, secondVersion)
	s.ensureCreateVersionInDeployment(secondVersion)

	s.setCurrentVersion(ctx, firstVersion, worker_versioning.UnversionedVersionId, true, "")

	latestVersionSummary := &deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
		Version:              secondVersion.DeploymentVersionString(),
		CreateTime:           timestamppb.Now(),
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
		CreateTime:           timestamppb.Now(),
		DrainageInfo:         nil,
		RampingSinceTime:     nil,
		CurrentSinceTime:     timestamppb.Now(),
		RoutingUpdateTime:    timestamppb.Now(),
		FirstActivationTime:  timestamppb.Now(),
		LastDeactivationTime: nil,
		Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
	}

	expectedDeploymentSummary := s.buildWorkerDeploymentSummary(
		tv.DeploymentSeries(),
		timestamppb.Now(),
		routingInfo,
		latestVersionSummary,
		currentVersionSummary,
		nil,
	)

	s.startAndValidateWorkerDeployments(ctx, &workflowservice.ListWorkerDeploymentsRequest{
		Namespace: s.Namespace().String(),
	}, []*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary{
		expectedDeploymentSummary,
	})
}

func (s *WorkerDeploymentSuite) TestListWorkerDeployments_TwoVersions_SameDeployment_OneCurrent_OneRamping() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tv := testvars.New(s)

	currentVersionVars := tv.WithBuildIDNumber(1)
	rampingVersionVars := tv.WithBuildIDNumber(2)

	routingInfo := &deploymentpb.RoutingConfig{
		CurrentVersion:            currentVersionVars.DeploymentVersionString(),
		CurrentVersionChangedTime: timestamppb.Now(),
		RampingVersion:            rampingVersionVars.DeploymentVersionString(),
		RampingVersionPercentage:  50,
		RampingVersionChangedTime: timestamppb.Now(),
	}

	s.startVersionWorkflow(ctx, currentVersionVars)
	s.ensureCreateVersionInDeployment(currentVersionVars)

	s.startVersionWorkflow(ctx, rampingVersionVars)
	s.ensureCreateVersionInDeployment(rampingVersionVars)

	s.setCurrentVersion(ctx, currentVersionVars, worker_versioning.UnversionedVersionId, true, "") // starts first version's version workflow + set it to current
	s.setAndVerifyRampingVersion(ctx, rampingVersionVars, false, 50, true, "", &workflowservice.SetWorkerDeploymentRampingVersionResponse{
		PreviousVersion:    "",
		PreviousPercentage: 0,
	})

	latestVersionSummary := &deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
		Version:              rampingVersionVars.DeploymentVersionString(),
		CreateTime:           timestamppb.Now(),
		DrainageInfo:         nil,
		RampingSinceTime:     timestamppb.Now(),
		CurrentSinceTime:     nil,
		RoutingUpdateTime:    timestamppb.Now(),
		FirstActivationTime:  timestamppb.Now(),
		LastDeactivationTime: nil,
		Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING,
	}
	currentVersionSummary := &deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
		Version:              currentVersionVars.DeploymentVersionString(),
		CreateTime:           timestamppb.Now(),
		DrainageInfo:         nil,
		RampingSinceTime:     nil,
		CurrentSinceTime:     timestamppb.Now(),
		RoutingUpdateTime:    timestamppb.Now(),
		FirstActivationTime:  timestamppb.Now(),
		LastDeactivationTime: nil,
		Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
	}

	expectedDeploymentSummary := s.buildWorkerDeploymentSummary(
		tv.DeploymentSeries(),
		timestamppb.Now(),
		routingInfo,
		latestVersionSummary,
		currentVersionSummary,
		latestVersionSummary, // latest version added is the ramping version
	)

	s.startAndValidateWorkerDeployments(ctx, &workflowservice.ListWorkerDeploymentsRequest{
		Namespace: s.Namespace().String(),
	}, []*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary{
		expectedDeploymentSummary,
	})
}

func (s *WorkerDeploymentSuite) TestListWorkerDeployments_RampingVersionPercentageChange_RampingChangedTime() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tv := testvars.New(s)

	s.startVersionWorkflow(ctx, tv)
	s.setAndVerifyRampingVersion(ctx, tv, false, 50, true, "", nil) // set version as ramping
	rampingVersionChangedTime := timestamppb.Now()

	routingInfo := &deploymentpb.RoutingConfig{
		CurrentVersion:            worker_versioning.UnversionedVersionId,
		CurrentVersionChangedTime: nil,
		RampingVersion:            tv.DeploymentVersionString(),
		RampingVersionPercentage:  50,
		RampingVersionChangedTime: rampingVersionChangedTime,
	}

	// to simulate time passing before the next ramping version update
	//nolint:forbidigo
	time.Sleep(2 * time.Second)

	// modify ramping version percentage
	s.setAndVerifyRampingVersion(ctx, tv, false, 75, true, "", &workflowservice.SetWorkerDeploymentRampingVersionResponse{
		PreviousVersion:    tv.DeploymentVersionString(),
		PreviousPercentage: 50,
	})

	// only the ramping version percentage should be updated, not the ramping version update time
	// since we are not changing the ramping version
	routingInfo.RampingVersionPercentage = 75

	rampingVersionSummary := &deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
		Version:              tv.DeploymentVersionString(),
		CreateTime:           rampingVersionChangedTime,
		DrainageInfo:         nil,
		RampingSinceTime:     rampingVersionChangedTime,
		CurrentSinceTime:     nil,
		RoutingUpdateTime:    timestamppb.Now(), // since the ramp percentage changed, the routing update time is updated
		FirstActivationTime:  rampingVersionChangedTime,
		LastDeactivationTime: nil,
		Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING,
	}

	expectedDeploymentSummary := s.buildWorkerDeploymentSummary(
		tv.DeploymentSeries(),
		rampingVersionChangedTime,
		routingInfo,
		rampingVersionSummary, // latest version added is the ramping version
		nil,
		rampingVersionSummary,
	)

	s.startAndValidateWorkerDeployments(ctx, &workflowservice.ListWorkerDeploymentsRequest{
		Namespace: s.Namespace().String(),
	}, []*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary{
		expectedDeploymentSummary,
	})

}

func (s *WorkerDeploymentSuite) TestListWorkerDeployments_MultipleVersions_MultipleDeployments_OnePage() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tv := testvars.New(s)

	expectedDeploymentSummaries := s.createVersionsInDeployments(ctx, tv, 2)

	s.startAndValidateWorkerDeployments(ctx, &workflowservice.ListWorkerDeploymentsRequest{
		Namespace: s.Namespace().String(),
	}, expectedDeploymentSummaries)
}

func (s *WorkerDeploymentSuite) TestListWorkerDeployments_MultipleVersions_MultipleDeployments_MultiplePages() {

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tv := testvars.New(s)

	expectedDeploymentSummaries := s.createVersionsInDeployments(ctx, tv, 5)

	s.startAndValidateWorkerDeployments(ctx, &workflowservice.ListWorkerDeploymentsRequest{
		Namespace: s.Namespace().String(),
		PageSize:  1,
	}, expectedDeploymentSummaries)
}

// Testing SetWorkerDeploymentRampingVersion
func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_Ramping_With_Current() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tv := testvars.New(s)

	rampingVersionVars := tv.WithBuildIDNumber(1)
	currentVersionVars := tv.WithBuildIDNumber(2)

	versionCreateTime := timestamppb.Now()
	s.startVersionWorkflow(ctx, rampingVersionVars)
	s.startVersionWorkflow(ctx, currentVersionVars)

	// set version as ramping
	setRampingUpdateTime := timestamppb.Now()
	s.setAndVerifyRampingVersion(ctx, rampingVersionVars, false, 50, true, "", &workflowservice.SetWorkerDeploymentRampingVersionResponse{
		PreviousVersion:    "",
		PreviousPercentage: 0,
	})
	resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       tv.DeploymentSeries(),
			CreateTime: versionCreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:            rampingVersionVars.DeploymentVersionString(),
				RampingVersionPercentage:  50,
				RampingVersionChangedTime: setRampingUpdateTime,
				CurrentVersion:            worker_versioning.UnversionedVersionId,
				CurrentVersionChangedTime: nil,
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
			LastModifierIdentity: tv.ClientIdentity(),
		},
	})

	// set current version
	setCurrentUpdateTime := timestamppb.Now()
	s.setCurrentVersion(ctx, currentVersionVars, worker_versioning.UnversionedVersionId, true, "")

	// fresh DescribeWorkerDeployment call
	resp, err = s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       tv.DeploymentSeries(),
			CreateTime: versionCreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:            rampingVersionVars.DeploymentVersionString(),
				RampingVersionPercentage:  50,
				RampingVersionChangedTime: setRampingUpdateTime,
				CurrentVersion:            currentVersionVars.DeploymentVersionString(),
				CurrentVersionChangedTime: setCurrentUpdateTime,
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
			LastModifierIdentity: tv.ClientIdentity(),
		},
	})
}

func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_DuplicateRamp() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	rampingVersionVars := testvars.New(s).WithBuildIDNumber(1)

	versionCreateTime := timestamppb.Now()
	s.startVersionWorkflow(ctx, rampingVersionVars)

	// set version as ramping
	setRampingUpdateTime := timestamppb.Now()
	s.setAndVerifyRampingVersion(ctx, rampingVersionVars, false, 50, true, "", &workflowservice.SetWorkerDeploymentRampingVersionResponse{
		PreviousVersion:    "",
		PreviousPercentage: 0,
	})
	resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: rampingVersionVars.DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       rampingVersionVars.DeploymentSeries(),
			CreateTime: versionCreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:            rampingVersionVars.DeploymentVersionString(),
				RampingVersionPercentage:  50,
				RampingVersionChangedTime: setRampingUpdateTime,
				CurrentVersion:            worker_versioning.UnversionedVersionId,
				CurrentVersionChangedTime: nil,
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
			LastModifierIdentity: rampingVersionVars.ClientIdentity(),
		},
	})

	// setting version as ramping again
	s.setAndVerifyRampingVersion(ctx, rampingVersionVars, false, 50, true, "", &workflowservice.SetWorkerDeploymentRampingVersionResponse{
		PreviousVersion:    rampingVersionVars.DeploymentVersionString(),
		PreviousPercentage: 50,
	})
}

func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_Invalid_SetCurrent_To_Ramping() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	versionCreateTime := timestamppb.Now()
	currentVersionVars := testvars.New(s).WithBuildIDNumber(1)
	s.startVersionWorkflow(ctx, currentVersionVars)

	setCurrentUpdateTime := timestamppb.Now()
	s.setCurrentVersion(ctx, currentVersionVars, worker_versioning.UnversionedVersionId, true, "")

	resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: currentVersionVars.DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       currentVersionVars.DeploymentSeries(),
			CreateTime: versionCreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:            "",  // no ramping info should be set
				RampingVersionPercentage:  0,   // no ramping info should be set
				RampingVersionChangedTime: nil, // no ramping info should be set
				CurrentVersion:            currentVersionVars.DeploymentVersionString(),
				CurrentVersionChangedTime: setCurrentUpdateTime,
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
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
				},
			},
			LastModifierIdentity: currentVersionVars.ClientIdentity(),
		},
	})

	expectedError := fmt.Errorf("ramping version %s is already current", currentVersionVars.DeploymentVersionString())
	s.setAndVerifyRampingVersion(ctx, currentVersionVars, false, 50, true, expectedError.Error(), nil) // setting current version to ramping should fail

	resp, err = s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: currentVersionVars.DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       currentVersionVars.DeploymentSeries(),
			CreateTime: versionCreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:            "",  // no ramping info should be set
				RampingVersionPercentage:  0,   // no ramping info should be set
				RampingVersionChangedTime: nil, // no ramping info should be set
				CurrentVersion:            currentVersionVars.DeploymentVersionString(),
				CurrentVersionChangedTime: timestamppb.Now(),
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
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
				},
			},
			LastModifierIdentity: currentVersionVars.ClientIdentity(),
		},
	})
}

func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_ModifyExistingRampVersionPercentage() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tv := testvars.New(s)

	rampingVersionVars := tv.WithBuildIDNumber(1)
	s.startVersionWorkflow(ctx, rampingVersionVars)

	s.setAndVerifyRampingVersion(ctx, rampingVersionVars, false, 50, true, "", nil) // set version as ramping

	// modify ramping version percentage
	s.setAndVerifyRampingVersion(ctx, rampingVersionVars, false, 75, true, "", &workflowservice.SetWorkerDeploymentRampingVersionResponse{
		PreviousVersion:    rampingVersionVars.DeploymentVersionString(),
		PreviousPercentage: 50,
	})

}

func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_WithCurrent_Unset_Ramp() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tv := testvars.New(s)

	versionCreateTime := timestamppb.Now()
	rampingVersionVars := tv.WithBuildIDNumber(1)
	currentVersionVars := tv.WithBuildIDNumber(2)

	s.startVersionWorkflow(ctx, rampingVersionVars)
	s.startVersionWorkflow(ctx, currentVersionVars)

	setRampingUpdateTime := timestamppb.Now()
	s.setAndVerifyRampingVersion(ctx, rampingVersionVars, false, 50, true, "", nil) // set version as ramping

	setCurrentUpdateTime := timestamppb.Now()
	s.setCurrentVersion(ctx, currentVersionVars, worker_versioning.UnversionedVersionId, true, "") // set version as curent

	resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       tv.DeploymentSeries(),
			CreateTime: versionCreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:            rampingVersionVars.DeploymentVersionString(),
				RampingVersionPercentage:  50,
				RampingVersionChangedTime: setRampingUpdateTime,
				CurrentVersion:            currentVersionVars.DeploymentVersionString(),
				CurrentVersionChangedTime: setCurrentUpdateTime,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
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
				{
					Version:              currentVersionVars.DeploymentVersionString(),
					CreateTime:           versionCreateTime,
					DrainageInfo:         nil,
					RoutingUpdateTime:    setCurrentUpdateTime,
					CurrentSinceTime:     setCurrentUpdateTime,
					RampingSinceTime:     nil,
					FirstActivationTime:  setCurrentUpdateTime,
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
				},
			},
			LastModifierIdentity: tv.ClientIdentity(),
		},
	})

	// unset ramping version
	unsetRampingUpdateTime := timestamppb.Now()
	s.setAndVerifyRampingVersion(ctx, rampingVersionVars, true, 0, true, "", &workflowservice.SetWorkerDeploymentRampingVersionResponse{
		PreviousVersion:    rampingVersionVars.DeploymentVersionString(),
		PreviousPercentage: 50,
	})

	resp, err = s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       tv.DeploymentSeries(),
			CreateTime: versionCreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:            "",
				RampingVersionPercentage:  0,
				RampingVersionChangedTime: unsetRampingUpdateTime,
				CurrentVersion:            currentVersionVars.DeploymentVersionString(),
				CurrentVersionChangedTime: setCurrentUpdateTime,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              rampingVersionVars.DeploymentVersionString(),
					CreateTime:           versionCreateTime,
					DrainageInfo:         &deploymentpb.VersionDrainageInfo{Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINING},
					RoutingUpdateTime:    setRampingUpdateTime,
					CurrentSinceTime:     nil,
					RampingSinceTime:     nil,
					FirstActivationTime:  setRampingUpdateTime,
					LastDeactivationTime: unsetRampingUpdateTime,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
				},
				{
					Version:              currentVersionVars.DeploymentVersionString(),
					CreateTime:           versionCreateTime,
					DrainageInfo:         nil,
					RoutingUpdateTime:    setCurrentUpdateTime,
					CurrentSinceTime:     setCurrentUpdateTime,
					RampingSinceTime:     nil,
					FirstActivationTime:  setCurrentUpdateTime,
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
				},
			},
			LastModifierIdentity: tv.ClientIdentity(),
		},
	})
}

func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_SetRampingAsCurrent() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tv := testvars.New(s)

	versionCreateTime := timestamppb.Now()
	rampingVersionVars := tv.WithBuildIDNumber(1)
	s.startVersionWorkflow(ctx, rampingVersionVars)

	setRampingUpdateTime := timestamppb.Now()
	s.setAndVerifyRampingVersion(ctx, rampingVersionVars, false, 50, true, "", nil)

	// set ramping version as current
	setCurrentUpdateTime := timestamppb.Now()
	s.setCurrentVersion(ctx, rampingVersionVars, worker_versioning.UnversionedVersionId, true, "")

	resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       tv.DeploymentSeries(),
			CreateTime: versionCreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:            "",                   // no ramping info should be set
				RampingVersionPercentage:  0,                    // no ramping info should be set
				RampingVersionChangedTime: setRampingUpdateTime, // ramping version got updated to ""
				CurrentVersion:            rampingVersionVars.DeploymentVersionString(),
				CurrentVersionChangedTime: setCurrentUpdateTime,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              rampingVersionVars.DeploymentVersionString(),
					CreateTime:           versionCreateTime,
					DrainageInfo:         nil,
					RoutingUpdateTime:    setRampingUpdateTime,
					CurrentSinceTime:     setCurrentUpdateTime,
					RampingSinceTime:     nil,
					FirstActivationTime:  setRampingUpdateTime,
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
				},
			},
			LastModifierIdentity: tv.ClientIdentity(),
		},
	})
}

func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_NoCurrent_Unset_Ramp() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tv := testvars.New(s)

	rampingVersionVars := tv.WithBuildIDNumber(1)
	s.startVersionWorkflow(ctx, rampingVersionVars)

	s.setAndVerifyRampingVersion(ctx, rampingVersionVars, false, 50, true, "", nil)
	s.setAndVerifyRampingVersion(ctx, rampingVersionVars, true, 0, true, "", &workflowservice.SetWorkerDeploymentRampingVersionResponse{
		PreviousVersion:    rampingVersionVars.DeploymentVersionString(),
		PreviousPercentage: 50,
	})
}

func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_Batching() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	tv := testvars.New(s)

	s.InjectHook(testhooks.TaskQueuesInDeploymentSyncBatchSize, 1)

	// registering 5 task-queues in the version which would result in the creation of 5 batches, each with 1 task-queue, during the SyncState call.
	versionCreateTime := timestamppb.Now()
	taskQueues := 5
	for i := 0; i < taskQueues; i++ {
		go s.pollFromDeploymentWithTaskQueueNumber(ctx, tv, i)
	}

	// ensure the version has been created in the deployment with the right number of task-queues
	s.ensureCreateVersionInDeployment(tv)
	s.ensureCreateVersionWithExpectedTaskQueues(ctx, tv, taskQueues)

	// verify that all the registered task-queues have "" set as their ramping version
	for i := 0; i < taskQueues; i++ {
		s.verifyTaskQueueVersioningInfo(ctx, tv.WithTaskQueueNumber(i).TaskQueue(), worker_versioning.UnversionedVersionId, "", 0)
	}

	// set ramping version to 50%
	setRampingUpdateTime := timestamppb.Now()
	s.setAndVerifyRampingVersion(ctx, tv, false, 50, true, "", nil)

	// verify the task queues have new ramping version
	for i := 0; i < taskQueues; i++ {
		s.verifyTaskQueueVersioningInfo(ctx, tv.WithTaskQueueNumber(i).TaskQueue(), worker_versioning.UnversionedVersionId, tv.DeploymentVersionString(), 50)
	}

	// verify if the worker-deployment has the right ramping version set
	resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
	})
	s.Nil(err)
	s.Equal(tv.DeploymentVersionString(), resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetRampingVersion())

	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       tv.DeploymentSeries(),
			CreateTime: versionCreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:            tv.DeploymentVersionString(),
				RampingVersionPercentage:  50,
				RampingVersionChangedTime: setRampingUpdateTime,
				CurrentVersion:            worker_versioning.UnversionedVersionId,
				CurrentVersionChangedTime: nil,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              tv.DeploymentVersionString(),
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
			LastModifierIdentity: tv.ClientIdentity(),
		},
	})

}

// TestSetWorkerDeploymentRampingVersion_UnversionedRamp_Batching verifies that the batching functionality works
// when ramping unversioned.
func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_UnversionedRamp_Batching() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()
	tv := testvars.New(s)

	s.InjectHook(testhooks.TaskQueuesInDeploymentSyncBatchSize, 1)

	// registering 5 task-queues in the version which would result in the creation of 5 batches, each with 1 task-queue, during the SyncState call.
	taskQueues := 5
	for i := 0; i < taskQueues; i++ {
		go s.pollFromDeploymentWithTaskQueueNumber(ctx, tv, i)
	}

	// ensure the version has been created in the deployment with the right number of task-queues
	s.ensureCreateVersionInDeployment(tv)
	s.ensureCreateVersionWithExpectedTaskQueues(ctx, tv, taskQueues)

	// make the current version versioned, so that we can set ramp to unversioned later
	s.setCurrentVersion(ctx, tv, worker_versioning.UnversionedVersionId, true, "")

	// set ramp to unversioned which should trigger a batch of SyncDeploymentVersionUserData requests.
	s.setAndVerifyRampingVersionUnversionedOption(ctx, tv, true, false, 75, true, "", nil)

	// check that the current version's task queues have ramping version == __unversioned__
	for i := 0; i < taskQueues; i++ {
		s.verifyTaskQueueVersioningInfo(ctx, tv.WithTaskQueueNumber(i).TaskQueue(), tv.DeploymentVersionString(), worker_versioning.UnversionedVersionId, 75)
	}

}

// SetCurrent tests

func (s *WorkerDeploymentSuite) TestDescribeWorkerDeployment_SetCurrentVersion() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	tv := testvars.New(s)

	versionsCreateTime := timestamppb.Now()
	firstVersion := tv.WithBuildIDNumber(1)
	secondVersion := tv.WithBuildIDNumber(2)

	go s.pollFromDeployment(ctx, firstVersion)

	// No current deployment version set.
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)

		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentSeries(),
		})
		a.NoError(err)
		a.Equal(worker_versioning.UnversionedVersionId, resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetCurrentVersion()) //nolint:staticcheck // SA1019: old worker versioning
	}, time.Second*10, time.Millisecond*1000)

	// Set first version as current version
	firstVersionCurrentUpdateTime := timestamppb.Now()
	s.setCurrentVersion(ctx, firstVersion, worker_versioning.UnversionedVersionId, true, "")

	resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       tv.DeploymentSeries(),
			CreateTime: versionsCreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				CurrentVersion:            firstVersion.DeploymentVersionString(),
				CurrentVersionChangedTime: firstVersionCurrentUpdateTime,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              firstVersion.DeploymentVersionString(),
					CreateTime:           versionsCreateTime,
					DrainageInfo:         nil,
					RoutingUpdateTime:    firstVersionCurrentUpdateTime,
					CurrentSinceTime:     firstVersionCurrentUpdateTime,
					RampingSinceTime:     nil,
					FirstActivationTime:  firstVersionCurrentUpdateTime,
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
				},
			},
			LastModifierIdentity: tv.ClientIdentity(),
		},
	})

	// Set a new second version and set it as the current version
	go s.pollFromDeployment(ctx, secondVersion)
	secondVersionCurrentUpdateTime := timestamppb.Now()
	s.setCurrentVersion(ctx, secondVersion, firstVersion.DeploymentVersionString(), true, "")

	// Verify that the first version is draining, has an updated last deactivation time + second version is current, has an updated first activation time.
	resp, err = s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       tv.DeploymentSeries(),
			CreateTime: versionsCreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				CurrentVersion:            secondVersion.DeploymentVersionString(),
				CurrentVersionChangedTime: secondVersionCurrentUpdateTime,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              firstVersion.DeploymentVersionString(),
					CreateTime:           versionsCreateTime,
					DrainageInfo:         &deploymentpb.VersionDrainageInfo{Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINING},
					RoutingUpdateTime:    firstVersionCurrentUpdateTime,
					CurrentSinceTime:     nil,
					RampingSinceTime:     nil,
					FirstActivationTime:  firstVersionCurrentUpdateTime,
					LastDeactivationTime: secondVersionCurrentUpdateTime,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
				},
				{
					Version:              secondVersion.DeploymentVersionString(),
					CreateTime:           versionsCreateTime,
					DrainageInfo:         nil,
					RoutingUpdateTime:    secondVersionCurrentUpdateTime,
					CurrentSinceTime:     secondVersionCurrentUpdateTime,
					RampingSinceTime:     nil,
					FirstActivationTime:  secondVersionCurrentUpdateTime,
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
				},
			},
			LastModifierIdentity: tv.ClientIdentity(),
		},
	})

}

func (s *WorkerDeploymentSuite) TestSetCurrentVersion_Batching() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	tv := testvars.New(s)

	s.InjectHook(testhooks.TaskQueuesInDeploymentSyncBatchSize, 1)

	// registering 5 task-queues in the version which would result in the creation of 5 batches, each with 1 task-queue, during the SyncState call.
	versionCreateTime := timestamppb.Now()
	taskQueues := 5
	for i := 0; i < taskQueues; i++ {
		go s.pollFromDeploymentWithTaskQueueNumber(ctx, tv, i)
	}

	// ensure the version has been created in the deployment with the right number of task-queues
	s.ensureCreateVersionInDeployment(tv)
	s.ensureCreateVersionWithExpectedTaskQueues(ctx, tv, taskQueues)

	// verify that all the registered task-queues have "__unversioned__" as their current version
	for i := 0; i < taskQueues; i++ {
		s.verifyTaskQueueVersioningInfo(ctx, tv.WithTaskQueueNumber(i).TaskQueue(), worker_versioning.UnversionedVersionId, "", 0)
	}

	// set current and check that the current version's task queues have new current version
	setCurrentUpdateTime := timestamppb.Now()
	s.setCurrentVersion(ctx, tv, worker_versioning.UnversionedVersionId, true, "")

	// verify the current version has propogated to all the registered task-queues userData
	for i := 0; i < taskQueues; i++ {
		s.verifyTaskQueueVersioningInfo(ctx, tv.WithTaskQueueNumber(i).TaskQueue(), tv.DeploymentVersionString(), "", 0)
	}

	// verify if the worker-deployment has the right current version set
	resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
	})
	s.Nil(err)

	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       tv.DeploymentSeries(),
			CreateTime: versionCreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:            "",
				RampingVersionPercentage:  0,
				RampingVersionChangedTime: nil,
				CurrentVersion:            tv.DeploymentVersionString(),
				CurrentVersionChangedTime: setCurrentUpdateTime,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              tv.DeploymentVersionString(),
					CreateTime:           versionCreateTime,
					DrainageInfo:         nil,
					RoutingUpdateTime:    setCurrentUpdateTime,
					CurrentSinceTime:     setCurrentUpdateTime,
					RampingSinceTime:     nil,
					FirstActivationTime:  setCurrentUpdateTime,
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
				},
			},
			LastModifierIdentity: tv.ClientIdentity(),
		},
	})

}

// Should see that the current version of the task queues becomes unversioned
func (s *WorkerDeploymentSuite) TestSetCurrentVersion_Unversioned_NoRamp() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	currentVars := testvars.New(s).WithBuildIDNumber(1)

	versionCreateTime := timestamppb.Now()
	go s.pollFromDeployment(ctx, currentVars)
	s.ensureCreateVersionInDeployment(currentVars)

	// check that the current version's task queues have current version unversioned to start
	s.verifyTaskQueueVersioningInfo(ctx, currentVars.TaskQueue(), worker_versioning.UnversionedVersionId, "", 0)

	// set current and check that the current version's task queues have new current version
	firstCurrentUpdateTime := timestamppb.Now()
	s.setCurrentVersion(ctx, currentVars, worker_versioning.UnversionedVersionId, true, "")
	s.verifyTaskQueueVersioningInfo(ctx, currentVars.TaskQueue(), currentVars.DeploymentVersionString(), "", 0)

	// set current unversioned and check that the current version's task queues have current version unversioned again
	secondCurrentUpdateTime := timestamppb.Now()
	s.setCurrentVersionUnversionedOption(ctx, currentVars, true, currentVars.DeploymentVersionString(), true, "")
	s.verifyTaskQueueVersioningInfo(ctx, currentVars.TaskQueue(), worker_versioning.UnversionedVersionId, "", 0)

	// check that deployment has current version == __unversioned__
	resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: currentVars.DeploymentSeries(),
	})
	s.Nil(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       currentVars.DeploymentSeries(),
			CreateTime: versionCreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				CurrentVersion:            worker_versioning.UnversionedVersionId,
				CurrentVersionChangedTime: secondCurrentUpdateTime,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              currentVars.DeploymentVersionString(),
					CreateTime:           versionCreateTime,
					DrainageInfo:         &deploymentpb.VersionDrainageInfo{Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINING},
					RoutingUpdateTime:    secondCurrentUpdateTime,
					CurrentSinceTime:     nil,
					RampingSinceTime:     nil,
					FirstActivationTime:  firstCurrentUpdateTime,
					LastDeactivationTime: secondCurrentUpdateTime,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
				},
			},
			LastModifierIdentity: currentVars.ClientIdentity(),
		},
	})
}

// Should see that the current version of the task queue becomes unversioned, and the unversioned ramping version of the task queue is removed
func (s *WorkerDeploymentSuite) TestSetCurrentVersion_Unversioned_PromoteUnversionedRamp() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()
	tv := testvars.New(s)
	currentVars := tv.WithBuildIDNumber(1)

	go s.pollFromDeployment(ctx, currentVars)
	s.ensureCreateVersionInDeployment(currentVars)

	// make the current version versioned, so that we can set ramp to unversioned
	s.setCurrentVersion(ctx, currentVars, worker_versioning.UnversionedVersionId, true, "")
	// set ramp to unversioned
	s.setAndVerifyRampingVersionUnversionedOption(ctx, tv, true, false, 75, true, "", nil)
	// check that the current version's task queues have ramping version == __unversioned__
	s.verifyTaskQueueVersioningInfo(ctx, currentVars.TaskQueue(), currentVars.DeploymentVersionString(), worker_versioning.UnversionedVersionId, 75)

	// set current to unversioned
	s.setCurrentVersionUnversionedOption(ctx, tv, true, currentVars.DeploymentVersionString(), true, "")

	// check that the current version's task queues have ramping version == "" and current version == "__unversioned__"
	s.verifyTaskQueueVersioningInfo(ctx, currentVars.TaskQueue(), worker_versioning.UnversionedVersionId, "", 0)
}

func (s *WorkerDeploymentSuite) TestSetCurrentVersion_Concurrent_DifferentVersions_NoUnexpectedErrors() {
	s.OverrideDynamicConfig(dynamicconfig.WorkflowExecutionMaxInFlightUpdates, 10) // this is the default

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	tv := testvars.New(s)
	errChan := make(chan error)

	versions := 10
	for i := 0; i < versions; i++ {
		s.startVersionWorkflow(ctx, tv.WithBuildIDNumber(i))
	}

	// Concurrently set 10 different versions as current version
	for i := 0; i < versions; i++ {
		go func() {
			_, err := s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
				Namespace:               s.Namespace().String(),
				DeploymentName:          tv.DeploymentVersion().GetDeploymentName(),
				Version:                 tv.WithBuildIDNumber(i).DeploymentVersionString(),
				IgnoreMissingTaskQueues: true,
				Identity:                tv.ClientIdentity(),
			})
			errChan <- err
		}()
	}

	for i := 0; i < versions; i++ {
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
	resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentVersion().GetDeploymentName(),
	})
	s.NoError(err)
	s.NotEqual(worker_versioning.UnversionedVersionId, resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetCurrentVersion())
}

func (s *WorkerDeploymentSuite) TestSetCurrentVersion_Concurrent_SameVersion_NoUnexpectedErrors() {
	s.OverrideDynamicConfig(dynamicconfig.WorkflowExecutionMaxInFlightUpdates, 10) // this is the default

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	tv := testvars.New(s)
	errChan := make(chan error)

	s.startVersionWorkflow(ctx, tv) // create version

	// Concurrently set the same version as current version 10 times.
	for i := 0; i < 10; i++ {
		go func() {
			_, err := s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
				Namespace:               s.Namespace().String(),
				DeploymentName:          tv.DeploymentVersion().GetDeploymentName(),
				Version:                 tv.DeploymentVersionString(),
				IgnoreMissingTaskQueues: true,
				Identity:                tv.ClientIdentity(),
			})
			errChan <- err
		}()
	}

	for i := 0; i < 10; i++ {
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
	resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentVersion().GetDeploymentName(),
	})
	s.NoError(err)
	s.Equal(tv.DeploymentVersionString(), resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetCurrentVersion())
}

// TestConcurrentPollers_DifferentTaskQueues_SameVersion_SetCurrentVersion aims to test that when there are multiple pollers polling on different task queues,
// all belonging to the same version, a setCurrentVersion call succeeds with all the task queues eventually having this version as the current version in their versioning info.
func (s *WorkerDeploymentSuite) TestConcurrentPollers_DifferentTaskQueues_SameVersion_SetCurrentVersion() {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	// start 10 different pollers each polling on a different task queue but belonging to the same version
	tv := testvars.New(s)

	versions := 10
	for i := 0; i < versions; i++ {
		go s.startVersionWorkflow(ctx, tv.WithTaskQueueNumber(i))
	}

	// set this version as current version
	s.setCurrentVersion(ctx, tv, worker_versioning.UnversionedVersionId, false, "")

	// verify that the task queues, eventually, have this version as the current version in their versioning info
	for i := 0; i < versions; i++ {
		s.verifyTaskQueueVersioningInfo(ctx, tv.WithTaskQueueNumber(i).TaskQueue(), tv.DeploymentVersionString(), "", 0)
	}
}

func (s *WorkerDeploymentSuite) TestSetRampingVersion_Concurrent_DifferentVersions_NoUnexpectedErrors() {
	s.OverrideDynamicConfig(dynamicconfig.WorkflowExecutionMaxInFlightUpdates, 10) // this is the default

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	tv := testvars.New(s)
	errChan := make(chan error)

	versions := 10
	for i := 0; i < versions; i++ {
		s.startVersionWorkflow(ctx, tv.WithBuildIDNumber(i))
	}

	// Concurrently set 10 different versions as ramping version
	for i := 0; i < versions; i++ {
		go func() {
			_, err := s.FrontendClient().SetWorkerDeploymentRampingVersion(ctx, &workflowservice.SetWorkerDeploymentRampingVersionRequest{
				Namespace:               s.Namespace().String(),
				DeploymentName:          tv.DeploymentVersion().GetDeploymentName(),
				Version:                 tv.WithBuildIDNumber(i).DeploymentVersionString(),
				IgnoreMissingTaskQueues: true,
				Identity:                tv.ClientIdentity(),
				Percentage:              50,
			})
			errChan <- err
		}()
	}

	for i := 0; i < versions; i++ {
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
	resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentVersion().GetDeploymentName(),
	})
	s.NoError(err)
	s.NotNil(resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetRampingVersion())
}

func (s *WorkerDeploymentSuite) TestSetRampingVersion_Concurrent_SameVersion_NoUnexpectedErrors() {
	s.OverrideDynamicConfig(dynamicconfig.WorkflowExecutionMaxInFlightUpdates, 10) // this is the default

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	tv := testvars.New(s)
	errChan := make(chan error)

	s.startVersionWorkflow(ctx, tv) // create version

	// Concurrently set the same version as ramping version 10 times.
	for i := 0; i < 10; i++ {
		go func() {
			_, err := s.FrontendClient().SetWorkerDeploymentRampingVersion(ctx, &workflowservice.SetWorkerDeploymentRampingVersionRequest{
				Namespace:               s.Namespace().String(),
				DeploymentName:          tv.DeploymentVersion().GetDeploymentName(),
				Version:                 tv.DeploymentVersionString(),
				IgnoreMissingTaskQueues: true,
				Identity:                tv.ClientIdentity(),
				Percentage:              50,
			})
			errChan <- err
		}()
	}

	for i := 0; i < 10; i++ {
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
	resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentVersion().GetDeploymentName(),
	})
	s.NoError(err)
	s.Equal(tv.DeploymentVersionString(), resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetRampingVersion())
}

func (s *WorkerDeploymentSuite) TestResourceExhaustedErrors_Converted_To_ReadableMessage() {
	s.OverrideDynamicConfig(dynamicconfig.WorkflowExecutionMaxInFlightUpdates, 2) // Lowering the limit to encounter ResourceExhausted errors

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	tv := testvars.New(s)
	versions := 5
	errChan := make(chan error, versions)

	// Start all version workflows first
	for i := 0; i < versions; i++ {
		s.startVersionWorkflow(ctx, tv.WithBuildIDNumber(i))
	}

	// Test SetRampingVersion
	s.testConcurrentRequestsResourceExhausted(ctx, tv, versions, errChan, "SetWorkerDeploymentRampingVersion", func(i int) error {
		_, err := s.FrontendClient().SetWorkerDeploymentRampingVersion(ctx, &workflowservice.SetWorkerDeploymentRampingVersionRequest{
			Namespace:               s.Namespace().String(),
			DeploymentName:          tv.DeploymentVersion().GetDeploymentName(),
			Version:                 tv.WithBuildIDNumber(i).DeploymentVersionString(),
			IgnoreMissingTaskQueues: true,
			Identity:                tv.ClientIdentity(),
			Percentage:              50,
		})
		return err
	})

	// Test SetCurrentVersion
	s.testConcurrentRequestsResourceExhausted(ctx, tv, versions, errChan, "SetWorkerDeploymentCurrentVersion", func(i int) error {
		_, err := s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
			Namespace:               s.Namespace().String(),
			DeploymentName:          tv.DeploymentVersion().GetDeploymentName(),
			Version:                 tv.WithBuildIDNumber(i).DeploymentVersionString(),
			IgnoreMissingTaskQueues: true,
			Identity:                tv.ClientIdentity(),
		})
		return err
	})

	// Test UpdateVersionMetadata
	metadata := map[string]*commonpb.Payload{
		"key1": {Data: testRandomMetadataValue},
		"key2": {Data: testRandomMetadataValue},
	}
	s.testConcurrentRequestsResourceExhausted(ctx, tv, versions, errChan, "UpdateWorkerDeploymentVersionMetadata", func(i int) error {
		_, err := s.FrontendClient().UpdateWorkerDeploymentVersionMetadata(ctx, &workflowservice.UpdateWorkerDeploymentVersionMetadataRequest{
			Namespace:     s.Namespace().String(),
			Version:       tv.WithBuildIDNumber(i).DeploymentVersionString(),
			UpsertEntries: metadata,
		})
		return err
	})
}

func (s *WorkerDeploymentSuite) testConcurrentRequestsResourceExhausted(
	ctx context.Context,
	tv *testvars.TestVars,
	versions int,
	errChan chan error,
	apiName string,
	requestFn func(int) error,
) {
	// Launch concurrent requests
	for i := 0; i < versions; i++ {
		go func(i int) {
			errChan <- requestFn(i)
		}(i)
	}

	// Expect ResourceExhausted errors to be converted to Internal errors with the appropriate message
	for i := 0; i < versions; i++ {
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tv := testvars.New(s)
	rampingVars := tv.WithBuildIDNumber(1)
	s.startVersionWorkflow(ctx, rampingVars)
	s.setAndVerifyRampingVersionUnversionedOption(ctx, rampingVars, true, false, 50, true, "ramping version __unversioned__ is already current", nil)
}

// Should see that the ramping version of the task queues in the current version is unversioned
func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_Unversioned_VersionedCurrent() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	tv := testvars.New(s)
	currentVars := tv.WithBuildIDNumber(1)

	go s.pollFromDeployment(ctx, currentVars)
	s.ensureCreateVersionInDeployment(currentVars)

	// check that the current version's task queues have ramping version == ""
	s.setCurrentVersion(ctx, currentVars, worker_versioning.UnversionedVersionId, true, "")
	s.verifyTaskQueueVersioningInfo(ctx, currentVars.TaskQueue(), currentVars.DeploymentVersionString(), "", 0)

	// set ramp to unversioned
	s.setAndVerifyRampingVersionUnversionedOption(ctx, tv, true, false, 75, true, "", nil)

	// check that deployment has ramping version == __unversioned__
	resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
	})
	s.Nil(err)
	s.Equal(worker_versioning.UnversionedVersionId, resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetRampingVersion())

	// check that the current version's task queues have ramping version == __unversioned__
	s.verifyTaskQueueVersioningInfo(ctx, currentVars.TaskQueue(), currentVars.DeploymentVersionString(), worker_versioning.UnversionedVersionId, 75)
}

func (s *WorkerDeploymentSuite) TestTwoPollers_EnsureCreateVersion() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer func() {
		cancel()
	}()
	tv := testvars.New(s)
	tv1 := tv.WithBuildIDNumber(1)
	tv2 := tv.WithBuildIDNumber(2)

	go s.pollFromDeployment(ctx, tv1)
	go s.pollFromDeployment(ctx, tv2)
	s.ensureCreateVersionWithExpectedTaskQueues(ctx, tv1, 1)
	s.ensureCreateVersionWithExpectedTaskQueues(ctx, tv2, 1)
}

func (s *WorkerDeploymentSuite) verifyTaskQueueVersioningInfo(ctx context.Context, tq *taskqueuepb.TaskQueue, expectedCurrentVersion, expectedRampingVersion string, expectedPercentage float32) {
	s.EventuallyWithT(func(t *assert.CollectT) {
		tqDesc, err := s.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: tq,
		})
		a := require.New(t)
		a.Nil(err)
		a.Equal(expectedCurrentVersion, tqDesc.GetVersioningInfo().GetCurrentVersion()) //nolint:staticcheck // SA1019: old worker versioning
		a.Equal(expectedRampingVersion, tqDesc.GetVersioningInfo().GetRampingVersion()) //nolint:staticcheck // SA1019: old worker versioning
		a.Equal(expectedPercentage, tqDesc.GetVersioningInfo().GetRampingVersionPercentage())
	}, time.Second*10, time.Millisecond*1000)
}

// Test that rolling back to a drained version works
func (s *WorkerDeploymentSuite) TestSetRampingVersion_AfterDrained() {
	s.OverrideDynamicConfig(dynamicconfig.PollerHistoryTTL, 500*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tv1 := testvars.New(s).WithBuildIDNumber(1)
	tv2 := testvars.New(s).WithBuildIDNumber(2)

	// Start deployment workflow 1 and wait for the deployment version to exist
	v1CreateTime := timestamppb.New(time.Now())
	s.startVersionWorkflow(ctx, tv1)

	// Set v1 as current version
	setCurrentV1UpdateTime := timestamppb.Now()
	s.setCurrentVersion(ctx, tv1, worker_versioning.UnversionedVersionId, true, "")
	resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv1.DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       tv1.DeploymentSeries(),
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
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
				},
			},
			LastModifierIdentity: tv1.ClientIdentity(),
		},
	})

	// Start deployment workflow 2 and set v2 to current so that v1 can start draining
	v2CreateTime := timestamppb.New(time.Now())
	s.startVersionWorkflow(ctx, tv2)

	setCurrentV2UpdateTime := timestamppb.New(time.Now())
	s.setCurrentVersion(ctx, tv2, tv1.DeploymentVersionString(), true, "")
	resp, err = s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv2.DeploymentSeries(),
	})
	s.NoError(err)

	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       tv2.DeploymentSeries(),
			CreateTime: v1CreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:            "",
				RampingVersionPercentage:  0,
				RampingVersionChangedTime: nil,
				CurrentVersion:            tv2.DeploymentVersionString(),
				CurrentVersionChangedTime: setCurrentV2UpdateTime,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              tv1.DeploymentVersionString(),
					CreateTime:           v1CreateTime,
					DrainageInfo:         &deploymentpb.VersionDrainageInfo{Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINING},
					RoutingUpdateTime:    setCurrentV1UpdateTime,
					CurrentSinceTime:     nil,
					RampingSinceTime:     nil,
					FirstActivationTime:  setCurrentV1UpdateTime,
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
		resp, err := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: s.Namespace().String(),
			Version:   tv1.DeploymentVersionString(),
		})
		a.NoError(err)
		a.Equal(enumspb.VERSION_DRAINAGE_STATUS_DRAINED, resp.GetWorkerDeploymentVersionInfo().GetDrainageInfo().GetStatus())
	}, time.Second*10, time.Millisecond*1000)

	// Verify that the drainageStatus of v1 has been updated in the VersionSummaries
	resp, err = s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv2.DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       tv2.DeploymentSeries(),
			CreateTime: v1CreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:            "",
				RampingVersionPercentage:  0,
				RampingVersionChangedTime: nil,
				CurrentVersion:            tv2.DeploymentVersionString(),
				CurrentVersionChangedTime: setCurrentV2UpdateTime,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              tv1.DeploymentVersionString(),
					CreateTime:           v1CreateTime,
					DrainageInfo:         &deploymentpb.VersionDrainageInfo{Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINED},
					RoutingUpdateTime:    setCurrentV1UpdateTime,
					CurrentSinceTime:     nil,
					RampingSinceTime:     nil,
					FirstActivationTime:  setCurrentV1UpdateTime,
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
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
				},
			},
			LastModifierIdentity: tv2.ClientIdentity(),
		},
	})

	// start ramping traffic back to v1
	s.setAndVerifyRampingVersion(ctx, tv1, false, 10, false, "", &workflowservice.SetWorkerDeploymentRampingVersionResponse{
		ConflictToken:      nil,
		PreviousVersion:    "",
		PreviousPercentage: 0,
	})
}

func (s *WorkerDeploymentSuite) TestDeleteWorkerDeployment_ValidDelete() {
	s.OverrideDynamicConfig(dynamicconfig.PollerHistoryTTL, 500*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tv1 := testvars.New(s).WithBuildIDNumber(1)

	// Start deployment workflow 1 and wait for the deployment version to exist
	s.startVersionWorkflow(ctx, tv1)

	// Signal the first version to be drained. Only do this in tests.
	versionWorkflowID := worker_versioning.GenerateVersionWorkflowID(tv1.DeploymentSeries(), tv1.BuildID())
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

	err = s.SendSignal(s.Namespace().String(), workflowExecution, workerdeployment.SyncDrainageSignalName, signalPayload, tv1.ClientIdentity())
	s.Nil(err)

	// Wait for pollers going away
	s.EventuallyWithT(func(t *assert.CollectT) {
		resp, err := s.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
			Namespace:     s.Namespace().String(),
			TaskQueue:     tv1.TaskQueue(),
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		})
		require.NoError(t, err)
		require.Empty(t, resp.Pollers)
	}, 5*time.Second, time.Second)

	// delete succeeds
	s.tryDeleteVersion(ctx, tv1, true)

	// deployment version does not exist in the deployment list
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv1.DeploymentSeries(),
		})
		a.NoError(err)
		for _, vs := range resp.GetWorkerDeploymentInfo().GetVersionSummaries() {
			//nolint:staticcheck // SA1019 deprecated Version will clean up later
			a.NotEqual(tv1.DeploymentVersionString(), vs.Version)
		}
	}, time.Second*5, time.Millisecond*200)

	// Deleting the worker deployment should succeed since there are no associated versions left
	_, err = s.FrontendClient().DeleteWorkerDeployment(ctx, &workflowservice.DeleteWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv1.DeploymentSeries(),
		Identity:       tv1.ClientIdentity(),
	})
	s.Nil(err)

	// Describe Worker Deployment should give not found
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		_, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv1.DeploymentSeries(),
		})
		a.Error(err)
		var nfe *serviceerror.NotFound
		a.True(errors.As(err, &nfe))
	}, time.Second*5, time.Millisecond*200)

	// ListDeployments should not show the closed/deleted Worker Deployment
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		listResp, err := s.FrontendClient().ListWorkerDeployments(ctx, &workflowservice.ListWorkerDeploymentsRequest{
			Namespace: s.Namespace().String(),
		})
		a.Nil(err)
		for _, dInfo := range listResp.GetWorkerDeployments() {
			a.NotEqual(tv1.DeploymentSeries(), dInfo.GetName())
		}
	}, time.Second*5, time.Millisecond*200)
}

func (s *WorkerDeploymentSuite) TestDeleteWorkerDeployment_Idempotent() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tv1 := testvars.New(s).WithBuildIDNumber(1)

	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		_, err := s.FrontendClient().DeleteWorkerDeployment(ctx, &workflowservice.DeleteWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv1.DeploymentSeries(),
			Identity:       tv1.ClientIdentity(),
		})
		a.NoError(err)
	}, time.Second*5, time.Millisecond*200)
}

func (s *WorkerDeploymentSuite) TestDeleteWorkerDeployment_InvalidDelete() {

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tv1 := testvars.New(s).WithBuildIDNumber(1)

	// Start deployment workflow 1 and wait for the deployment version and deployment workflow to exist
	go s.pollFromDeployment(ctx, tv1)
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: s.Namespace().String(),
			Version:   tv1.DeploymentVersionString(),
		})
		a.NoError(err)
		a.Equal(tv1.DeploymentVersionString(), resp.GetWorkerDeploymentVersionInfo().GetVersion())
	}, time.Second*5, time.Millisecond*200)

	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv1.DeploymentSeries(),
		})
		a.NoError(err)
		a.NotEmpty(resp.GetWorkerDeploymentInfo().GetVersionSummaries())
		//nolint:staticcheck // SA1019 deprecated Version will clean up later
		a.Equal(tv1.DeploymentVersionString(), resp.GetWorkerDeploymentInfo().GetVersionSummaries()[0].Version)
	}, time.Second*5, time.Millisecond*200)

	// Delete the worker deployment should fail since there are versions associated with it
	_, err := s.FrontendClient().DeleteWorkerDeployment(ctx, &workflowservice.DeleteWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv1.DeploymentSeries(),
		Identity:       tv1.ClientIdentity(),
	})
	s.Error(err)
}

func (s *WorkerDeploymentSuite) tryDeleteVersion(
	ctx context.Context,
	tv *testvars.TestVars,
	expectSuccess bool,
) {
	_, err := s.FrontendClient().DeleteWorkerDeploymentVersion(ctx, &workflowservice.DeleteWorkerDeploymentVersionRequest{
		Namespace: s.Namespace().String(),
		Version:   tv.DeploymentVersionString(),
	})
	if expectSuccess {
		s.Nil(err)
	} else {
		s.Error(err)
	}
}

func (s *WorkerDeploymentSuite) verifyTimestampEquality(expected, actual *timestamppb.Timestamp, maxDuration time.Duration) {
	s.True((expected == nil) == (actual == nil))
	if expected != nil {
		s.True(expected.AsTime().Sub(actual.AsTime()) < maxDuration)
	}
}

func (s *WorkerDeploymentSuite) verifyVersionSummary(expected, actual *deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary, maxDuration time.Duration) {
	s.Equal(expected.GetVersion(), actual.GetVersion()) //nolint:staticcheck // SA1019: old worker versioning
	s.Equal(expected.GetDrainageInfo().GetStatus(), actual.GetDrainageInfo().GetStatus())
	s.Equal(expected.GetStatus(), actual.GetStatus())

	s.verifyTimestampEquality(expected.GetCreateTime(), actual.GetCreateTime(), maxDuration)
	s.verifyTimestampEquality(expected.GetRoutingUpdateTime(), actual.GetRoutingUpdateTime(), maxDuration)
	s.verifyTimestampEquality(expected.GetCurrentSinceTime(), actual.GetCurrentSinceTime(), maxDuration)
	s.verifyTimestampEquality(expected.GetRampingSinceTime(), actual.GetRampingSinceTime(), maxDuration)
	s.verifyTimestampEquality(expected.GetFirstActivationTime(), actual.GetFirstActivationTime(), maxDuration)
	s.verifyTimestampEquality(expected.GetLastDeactivationTime(), actual.GetLastDeactivationTime(), maxDuration)
}

func (s *WorkerDeploymentSuite) verifyRoutingConfig(expected, actual *deploymentpb.RoutingConfig, maxDuration time.Duration) {
	s.Equal(expected.GetRampingVersion(), actual.GetRampingVersion()) //nolint:staticcheck // SA1019: old worker versioning
	s.Equal(expected.GetRampingVersionPercentage(), actual.GetRampingVersionPercentage())
	s.Equal(expected.GetCurrentVersion(), actual.GetCurrentVersion()) //nolint:staticcheck // SA1019: old worker versioning

	s.verifyTimestampEquality(expected.GetRampingVersionChangedTime(), actual.GetRampingVersionChangedTime(), maxDuration)
	s.verifyTimestampEquality(expected.GetCurrentVersionChangedTime(), actual.GetCurrentVersionChangedTime(), maxDuration)
}

func (s *WorkerDeploymentSuite) verifyWorkerDeploymentInfo(expected, actual *deploymentpb.WorkerDeploymentInfo, maxDuration time.Duration) {
	s.Equal(expected.GetName(), actual.GetName())
	s.verifyTimestampEquality(expected.GetCreateTime(), actual.GetCreateTime(), maxDuration)
	s.Equal(expected.GetLastModifierIdentity(), actual.GetLastModifierIdentity())
	s.verifyRoutingConfig(expected.GetRoutingConfig(), actual.GetRoutingConfig(), maxDuration)

	// Verify version summaries
	for _, expectedSummary := range expected.GetVersionSummaries() {
		found := false
		for _, actualSummary := range actual.GetVersionSummaries() {
			if actualSummary.Version == expectedSummary.Version { //nolint:staticcheck // SA1019: old worker versioning
				s.verifyVersionSummary(expectedSummary, actualSummary, maxDuration)
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
	// relaxed timestamp constraint since the tests make sync calls, which could theoretically take seconds.
	maxDurationBetweenTimeStamps := 5 * time.Second

	s.True((actualResp == nil) == (expectedResp == nil))
	s.True((actualResp.GetWorkerDeploymentInfo() == nil) == (expectedResp.GetWorkerDeploymentInfo() == nil))
	s.True((actualResp.GetWorkerDeploymentInfo().GetRoutingConfig() == nil) == (expectedResp.GetWorkerDeploymentInfo().GetRoutingConfig() == nil))

	s.verifyWorkerDeploymentInfo(expectedResp.GetWorkerDeploymentInfo(), actualResp.GetWorkerDeploymentInfo(), maxDurationBetweenTimeStamps)
}

func (s *WorkerDeploymentSuite) setAndVerifyRampingVersion(
	ctx context.Context,
	tv *testvars.TestVars,
	unset bool,
	percentage int,
	ignoreMissingTaskQueues bool,
	expectedError string,
	expectedResp *workflowservice.SetWorkerDeploymentRampingVersionResponse,
) {
	s.setAndVerifyRampingVersionUnversionedOption(ctx, tv, false, unset, percentage, ignoreMissingTaskQueues, expectedError, expectedResp)
}

func (s *WorkerDeploymentSuite) setAndVerifyRampingVersionUnversionedOption(
	ctx context.Context,
	tv *testvars.TestVars,
	unversioned bool,
	unset bool,
	percentage int,
	ignoreMissingTaskQueues bool,
	expectedError string,
	expectedResp *workflowservice.SetWorkerDeploymentRampingVersionResponse,
) {
	version := tv.DeploymentVersionString()
	if unversioned {
		version = worker_versioning.UnversionedVersionId
	}
	if unset {
		version = ""
		percentage = 0
	}
	if !unversioned && !unset {
		s.ensureCreateVersionInDeployment(tv)
	} else {
		s.ensureCreateDeployment(tv)
	}
	resp, err := s.FrontendClient().SetWorkerDeploymentRampingVersion(ctx, &workflowservice.SetWorkerDeploymentRampingVersionRequest{
		Namespace:               s.Namespace().String(),
		DeploymentName:          tv.DeploymentVersion().GetDeploymentName(),
		Version:                 version,
		Percentage:              float32(percentage),
		Identity:                tv.ClientIdentity(),
		IgnoreMissingTaskQueues: ignoreMissingTaskQueues,
	})
	if expectedError != "" {
		s.Error(err)
		s.Contains(err.Error(), expectedError)
		return
	}
	s.NoError(err)
	s.Equal(expectedResp.GetPreviousVersion(), resp.GetPreviousVersion())
	s.Equal(expectedResp.GetPreviousPercentage(), resp.GetPreviousPercentage())
}

func (s *WorkerDeploymentSuite) setCurrentVersion(ctx context.Context, tv *testvars.TestVars, previousCurrent string, ignoreMissingTaskQueues bool, expectedError string) {
	s.setCurrentVersionUnversionedOption(ctx, tv, false, previousCurrent, ignoreMissingTaskQueues, expectedError)
}

func (s *WorkerDeploymentSuite) setCurrentVersionUnversionedOption(ctx context.Context, tv *testvars.TestVars, unversioned bool, previousCurrent string, ignoreMissingTaskQueues bool, expectedError string) {
	version := tv.DeploymentVersionString()
	if unversioned {
		version = worker_versioning.UnversionedVersionId
		s.ensureCreateDeployment(tv)
	} else {
		s.ensureCreateVersionInDeployment(tv)
	}

	resp, err := s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:               s.Namespace().String(),
		DeploymentName:          tv.DeploymentVersion().GetDeploymentName(),
		Version:                 version,
		IgnoreMissingTaskQueues: ignoreMissingTaskQueues,
		Identity:                tv.ClientIdentity(),
	})
	if expectedError != "" {
		s.Error(err)
		s.Contains(err.Error(), expectedError)
		return
	}
	s.NoError(err)
	s.NotNil(resp.PreviousVersion)
	s.Equal(previousCurrent, resp.PreviousVersion)
}

func (s *WorkerDeploymentSuite) createVersionsInDeployments(ctx context.Context, tv *testvars.TestVars, n int) []*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary {
	var expectedDeploymentSummaries []*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary

	for i := 0; i < n; i++ {
		deployment := tv.WithDeploymentSeriesNumber(i)
		version := deployment.WithBuildIDNumber(i)

		s.startVersionWorkflow(ctx, version)
		s.setCurrentVersion(ctx, version, worker_versioning.UnversionedVersionId, true, "")

		currentVersionSummary := &deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
			Version:              version.DeploymentVersionString(),
			CreateTime:           timestamppb.Now(),
			CurrentSinceTime:     timestamppb.Now(),
			FirstActivationTime:  timestamppb.Now(),
			RoutingUpdateTime:    timestamppb.Now(),
			DrainageInfo:         nil,
			RampingSinceTime:     nil,
			LastDeactivationTime: nil,
			Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
		}

		expectedDeployment := s.buildWorkerDeploymentSummary(
			deployment.DeploymentSeries(),
			timestamppb.Now(),
			&deploymentpb.RoutingConfig{
				CurrentVersion:            version.DeploymentVersionString(),
				CurrentVersionChangedTime: timestamppb.Now(),
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
	maxDurationBetweenTimeStamps := 5 * time.Second
	if expectedSummary.Name != actualSummary.Name {
		s.Logger.Info("Name mismatch")
		return false
	}
	if expectedSummary.CreateTime.AsTime().Sub(actualSummary.CreateTime.AsTime()) > maxDurationBetweenTimeStamps {
		s.Logger.Info("Create time mismatch")
		return false
	}

	// Current version checks
	if expectedSummary.RoutingConfig.GetCurrentVersion() != actualSummary.RoutingConfig.GetCurrentVersion() { //nolint:staticcheck // SA1019: old worker versioning
		s.Logger.Info("Current version mismatch")
		return false
	}
	if expectedSummary.RoutingConfig.GetCurrentVersionChangedTime().AsTime().Sub(actualSummary.RoutingConfig.GetCurrentVersionChangedTime().AsTime()) > maxDurationBetweenTimeStamps {
		s.Logger.Info("Current version update time mismatch")
		return false
	}

	// Ramping version checks
	if expectedSummary.RoutingConfig.GetRampingVersion() != actualSummary.RoutingConfig.GetRampingVersion() { //nolint:staticcheck // SA1019: old worker versioning
		s.Logger.Info("Ramping version mismatch")
		return false
	}
	if expectedSummary.RoutingConfig.GetRampingVersionPercentage() != actualSummary.RoutingConfig.GetRampingVersionPercentage() {
		s.Logger.Info("Ramping version percentage mismatch")
		return false
	}
	if expectedSummary.RoutingConfig.GetRampingVersionChangedTime().AsTime().Sub(actualSummary.RoutingConfig.GetRampingVersionChangedTime().AsTime()) > maxDurationBetweenTimeStamps {
		s.Logger.Info("Ramping version update time mismatch")
		return false
	}

	// Latest version summary checks
	s.verifyVersionSummary(expectedSummary.LatestVersionSummary, actualSummary.LatestVersionSummary, maxDurationBetweenTimeStamps)

	// Current version summary checks
	s.verifyVersionSummary(expectedSummary.CurrentVersionSummary, actualSummary.CurrentVersionSummary, maxDurationBetweenTimeStamps)

	// Ramping version summary checks
	s.verifyVersionSummary(expectedSummary.RampingVersionSummary, actualSummary.RampingVersionSummary, maxDurationBetweenTimeStamps)

	return true
}

func (s *WorkerDeploymentSuite) listWorkerDeployments(ctx context.Context, request *workflowservice.ListWorkerDeploymentsRequest) ([]*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary, error) {
	var resp *workflowservice.ListWorkerDeploymentsResponse
	var err error
	var deploymentSummaries []*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary
	for resp == nil || len(resp.NextPageToken) > 0 {
		resp, err = s.FrontendClient().ListWorkerDeployments(ctx, request)
		if err != nil {
			return nil, err
		}
		deploymentSummaries = append(deploymentSummaries, resp.GetWorkerDeployments()...)
		request.NextPageToken = resp.NextPageToken
	}
	return deploymentSummaries, nil
}

func (s *WorkerDeploymentSuite) startAndValidateWorkerDeployments(
	ctx context.Context,
	request *workflowservice.ListWorkerDeploymentsRequest,
	expectedDeploymentSummaries []*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary,
) {

	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)

		actualDeploymentSummaries, err := s.listWorkerDeployments(ctx, request)
		a.NoError(err)
		if len(actualDeploymentSummaries) < len(expectedDeploymentSummaries) {
			return
		}

		for _, expectedDeploymentSummary := range expectedDeploymentSummaries {
			deploymentSummaryValidated := false
			for _, actualDeploymentSummary := range actualDeploymentSummaries {
				deploymentSummaryValidated = deploymentSummaryValidated ||
					s.verifyWorkerDeploymentSummary(expectedDeploymentSummary, actualDeploymentSummary)
			}
			a.True(deploymentSummaryValidated)
		}
	}, time.Second*10, time.Millisecond*1000)
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

// Name is used by testvars. We use a shortened test name in variables so that physical task queue IDs
// do not grow larger than DB column limit (currently as low as 272 chars).
func (s *WorkerDeploymentSuite) Name() string {
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
