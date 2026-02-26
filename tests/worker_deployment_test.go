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
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
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
		testcore.FunctionalTestBase
		workflowVersion workerdeployment.DeploymentWorkflowVersion
	}
)

func TestWorkerDeploymentSuiteV0(t *testing.T) {
	t.Parallel()
	suite.Run(t, &WorkerDeploymentSuite{workflowVersion: workerdeployment.InitialVersion})
}

func TestWorkerDeploymentSuiteV2(t *testing.T) {
	t.Parallel()
	suite.Run(t, &WorkerDeploymentSuite{workflowVersion: workerdeployment.VersionDataRevisionNumber})
}

func (s *WorkerDeploymentSuite) SetupSuite() {
	s.FunctionalTestBase.SetupSuiteWithCluster(testcore.WithDynamicConfigOverrides(map[dynamicconfig.Key]any{
		dynamicconfig.MatchingDeploymentWorkflowVersion.Key(): int(s.workflowVersion),

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
	workflowID := workerdeployment.GenerateDeploymentWorkflowID(tv.DeploymentSeries())
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

func (s *WorkerDeploymentSuite) TestForceCAN_WithOverrideState() {
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

	// Create a modified state with a different manager identity
	overrideState := &deploymentspb.WorkerDeploymentLocalState{
		CreateTime:           timestamppb.New(time.Now()),
		RoutingConfig:        &deploymentpb.RoutingConfig{CurrentVersion: tv.DeploymentVersionString()},
		Versions:             map[string]*deploymentspb.WorkerDeploymentVersionSummary{tv.DeploymentVersionString(): {Version: tv.DeploymentVersionString(), CreateTime: timestamppb.New(time.Now())}},
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
	workflowID := workerdeployment.GenerateDeploymentWorkflowID(tv.DeploymentSeries())
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
	}

	err = s.SendSignal(s.Namespace().String(), workflowExecution, workerdeployment.ForceCANSignalName, signalPayload, tv.ClientIdentity())
	s.NoError(err)

	// Verify that the override state is used after CAN
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentSeries(),
		})
		a.NoError(err)
		a.Equal("override-manager-identity", resp.GetWorkerDeploymentInfo().GetManagerIdentity())
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
	s.pollFromDeploymentExpectFail(ctx, tv.WithDeploymentSeriesNumber(2), "reached maximum deployments in namespace (1)")
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

	for i := range numVersions {
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
	startTime := timestamppb.Now()

	s.startVersionWorkflow(ctx, tv)
	s.ensureCreateVersionInDeployment(tv)

	latestVersionSummary := &deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
		Version:              tv.DeploymentVersionString(),
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
		tv.DeploymentSeries(),
		startTime,
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

	createVersion1Time := timestamppb.Now()
	s.startVersionWorkflow(ctx, firstVersion)
	s.ensureCreateVersionInDeployment(firstVersion)

	createVersion2Time := timestamppb.Now()
	s.startVersionWorkflow(ctx, secondVersion)
	s.ensureCreateVersionInDeployment(secondVersion)

	setCurrentTime := timestamppb.Now()
	s.setCurrentVersion(ctx, firstVersion, true, "")

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
		tv.DeploymentSeries(),
		createVersion1Time,
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

	createVersion1Time := timestamppb.Now()
	s.startVersionWorkflow(ctx, currentVersionVars)
	s.ensureCreateVersionInDeployment(currentVersionVars)

	createVersion2Time := timestamppb.Now()
	s.startVersionWorkflow(ctx, rampingVersionVars)
	s.ensureCreateVersionInDeployment(rampingVersionVars)

	setCurrentTime := timestamppb.Now()
	s.setCurrentVersion(ctx, currentVersionVars, true, "") // starts first version's version workflow + set it to current
	setRampingTime := timestamppb.Now()
	s.setAndVerifyRampingVersion(ctx, rampingVersionVars, false, 50, true, "")
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
		tv.DeploymentSeries(),
		createVersion1Time,
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

	startTime := timestamppb.Now()
	s.startVersionWorkflow(ctx, tv)
	setRampTime := timestamppb.Now()
	s.setAndVerifyRampingVersion(ctx, tv, false, 50, true, "") // set version as ramping

	routingInfo := &deploymentpb.RoutingConfig{
		CurrentVersion:                      worker_versioning.UnversionedVersionId,
		CurrentVersionChangedTime:           nil,
		RampingVersion:                      tv.DeploymentVersionString(),
		RampingVersionPercentage:            50,
		RampingVersionChangedTime:           setRampTime,
		RampingVersionPercentageChangedTime: setRampTime,
	}

	// to simulate time passing before the next ramping version update
	//nolint:forbidigo
	time.Sleep(2 * time.Second)

	changeRampTime := timestamppb.Now()
	// modify ramping version percentage
	s.setAndVerifyRampingVersion(ctx, tv, false, 75, true, "")

	// only the ramping version percentage should be updated, not the ramping version update time
	// since we are not changing the ramping version
	routingInfo.RampingVersionPercentage = 75
	routingInfo.RampingVersionPercentageChangedTime = changeRampTime

	rampingVersionSummary := &deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
		Version:              tv.DeploymentVersionString(),
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
		tv.DeploymentSeries(),
		startTime,
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
// Also tests whether LastCurrentTime is successfully updated when a previously-current version is demoted and then promoted back to current.
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
	s.setAndVerifyRampingVersion(ctx, rampingVersionVars, false, 50, true, "")
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
			LastModifierIdentity: tv.ClientIdentity(),
		},
	})

	// set current version
	setCurrentUpdateTime := timestamppb.Now()
	s.setCurrentVersion(ctx, currentVersionVars, true, "")

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
			LastModifierIdentity: tv.ClientIdentity(),
		},
	})

	// Now, demote the current version (by promoting the ramping version) and verify that LastCurrentTime is unchanged.
	setRampingAsCurrentUpdateTime := timestamppb.Now()
	s.setCurrentVersion(ctx, rampingVersionVars, true, "")
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
			LastModifierIdentity: tv.ClientIdentity(),
		},
	})

	// Now, re-promote the current version to Current and verify that LastCurrentTime is updated for that version (and unchanged for the originally-ramping version).
	rePromoteCurrentUpdateTime := timestamppb.Now()
	s.setCurrentVersion(ctx, currentVersionVars, true, "")
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
	s.setAndVerifyRampingVersion(ctx, rampingVersionVars, false, 50, true, "")
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
			LastModifierIdentity: rampingVersionVars.ClientIdentity(),
		},
	})

	// setting version as ramping again
	s.setAndVerifyRampingVersion(ctx, rampingVersionVars, false, 50, true, "")
}

func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_Invalid_SetCurrent_To_Ramping() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	versionCreateTime := timestamppb.Now()
	currentVersionVars := testvars.New(s).WithBuildIDNumber(1)
	s.startVersionWorkflow(ctx, currentVersionVars)

	setCurrentUpdateTime := timestamppb.Now()
	s.setCurrentVersion(ctx, currentVersionVars, true, "")

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
				RampingVersion:                      "",  // no ramping info should be set
				RampingVersionPercentage:            0,   // no ramping info should be set
				RampingVersionChangedTime:           nil, // no ramping info should be set
				RampingVersionPercentageChangedTime: nil, // no ramping info should be set
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
			},
			LastModifierIdentity: currentVersionVars.ClientIdentity(),
		},
	})

	expectedError := fmt.Errorf("ramping version %s is already current", currentVersionVars.DeploymentVersionString())
	s.setAndVerifyRampingVersion(ctx, currentVersionVars, false, 50, true, expectedError.Error()) // setting current version to ramping should fail

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
				RampingVersion:                      "",  // no ramping info should be set
				RampingVersionPercentage:            0,   // no ramping info should be set
				RampingVersionChangedTime:           nil, // no ramping info should be set
				RampingVersionPercentageChangedTime: nil, // no ramping info should be set
				CurrentVersion:                      currentVersionVars.DeploymentVersionString(),
				CurrentVersionChangedTime:           versionCreateTime,
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
			},
			LastModifierIdentity: currentVersionVars.ClientIdentity(),
		},
	})
}

func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_Valid_SetNilCurrent_To_Ramping() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	tv := testvars.New(s).WithBuildIDNumber(1)
	s.startVersionWorkflow(ctx, tv)

	// set ramping version to unversioned will change the modifier identity, so it's not a no-op
	s.setAndVerifyRampingVersion(ctx, tv, true, 0, false, "")

	// set a non-nil ramping version so that we can unset it in the next step
	s.setAndVerifyRampingVersion(ctx, tv, false, 5, false, "")

	// should be able to unset ramping version while current version is nil with no error
	s.setAndVerifyRampingVersion(ctx, tv, true, 0, true, "")
}

func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_ModifyExistingRampVersionPercentage() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tv := testvars.New(s)

	versionCreateTime := timestamppb.Now()
	rampingVersionVars := tv.WithBuildIDNumber(1)
	s.startVersionWorkflow(ctx, rampingVersionVars)

	// set version as ramping
	setRampingUpdateTime := timestamppb.Now()
	s.setAndVerifyRampingVersion(ctx, rampingVersionVars, false, 50, true, "")
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
			LastModifierIdentity: tv.ClientIdentity(),
		},
	})

	// modify ramping version percentage
	modifyRampingPercentageTime := timestamppb.Now()
	s.setAndVerifyRampingVersion(ctx, rampingVersionVars, false, 75, true, "")

	// RampingVersionPercentage and RampingVersionPercentageChangedTime should be updated
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
				RampingVersion:                      rampingVersionVars.DeploymentVersionString(),
				RampingVersionPercentage:            75, // ramping version percentage is updated to 75
				RampingVersionChangedTime:           setRampingUpdateTime,
				RampingVersionPercentageChangedTime: modifyRampingPercentageTime, // timestamp is updated as the ramp percentage changed from 50 -> 75
				CurrentVersion:                      worker_versioning.UnversionedVersionId,
				CurrentVersionChangedTime:           nil,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              rampingVersionVars.DeploymentVersionString(),
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
			LastModifierIdentity: tv.ClientIdentity(),
		},
	})

}

func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_WithCurrent_Unset_Ramp() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tv := testvars.New(s)

	rampingVersionVars := tv.WithBuildIDNumber(1)
	currentVersionVars := tv.WithBuildIDNumber(2)

	version1CreateTime := timestamppb.Now()
	s.startVersionWorkflow(ctx, rampingVersionVars)
	version2CreateTime := timestamppb.Now()
	s.startVersionWorkflow(ctx, currentVersionVars)

	setRampingUpdateTime := timestamppb.Now()
	s.setAndVerifyRampingVersion(ctx, rampingVersionVars, false, 50, true, "") // set version as ramping

	setCurrentUpdateTime := timestamppb.Now()
	s.setCurrentVersion(ctx, currentVersionVars, true, "") // set version as curent

	resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       tv.DeploymentSeries(),
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
			LastModifierIdentity: tv.ClientIdentity(),
		},
	})

	// unset ramping version
	unsetRampingUpdateTime := timestamppb.Now()
	s.setAndVerifyRampingVersion(ctx, rampingVersionVars, true, 0, true, "")

	resp, err = s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       tv.DeploymentSeries(),
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
	s.setAndVerifyRampingVersion(ctx, rampingVersionVars, false, 50, true, "")

	// set ramping version as current
	setCurrentUpdateTime := timestamppb.Now()
	s.setCurrentVersion(ctx, rampingVersionVars, true, "")

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
				RampingVersion:                      "",                   // no ramping info should be set
				RampingVersionPercentage:            0,                    // no ramping info should be set
				RampingVersionChangedTime:           setCurrentUpdateTime, // ramping version got updated to ""
				RampingVersionPercentageChangedTime: setCurrentUpdateTime, // ramping version got updated to ""
				CurrentVersion:                      rampingVersionVars.DeploymentVersionString(),
				CurrentVersionChangedTime:           setCurrentUpdateTime,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              rampingVersionVars.DeploymentVersionString(),
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

	s.setAndVerifyRampingVersion(ctx, rampingVersionVars, false, 50, true, "")
	s.setAndVerifyRampingVersion(ctx, rampingVersionVars, true, 0, true, "")
}

func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_Batching() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	tv := testvars.New(s)

	s.InjectHook(testhooks.NewHook(testhooks.TaskQueuesInDeploymentSyncBatchSize, 1))

	// registering 5 task-queues in the version which would result in the creation of 5 batches, each with 1 task-queue, during the SyncState call.
	versionCreateTime := timestamppb.Now()
	taskQueues := 5
	for i := range taskQueues {
		go s.pollFromDeploymentWithTaskQueueNumber(ctx, tv, i)
	}

	// ensure the version has been created in the deployment with the right number of task-queues
	s.ensureCreateVersionInDeployment(tv)
	s.ensureCreateVersionWithExpectedTaskQueues(ctx, tv, taskQueues)

	// verify that all the registered task-queues have "" set as their ramping version
	for i := range taskQueues {
		s.verifyTaskQueueVersioningInfo(ctx, tv.WithTaskQueueNumber(i).TaskQueue(), worker_versioning.UnversionedVersionId, "", 0)
	}

	// set ramping version to 50%
	setRampingUpdateTime := timestamppb.Now()
	s.setAndVerifyRampingVersion(ctx, tv, false, 50, true, "")

	// verify the task queues have new ramping version
	for i := range taskQueues {
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
				RampingVersion:                      tv.DeploymentVersionString(),
				RampingVersionPercentage:            50,
				RampingVersionChangedTime:           setRampingUpdateTime,
				RampingVersionPercentageChangedTime: setRampingUpdateTime,
				CurrentVersion:                      worker_versioning.UnversionedVersionId,
				CurrentVersionChangedTime:           nil,
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

	s.InjectHook(testhooks.NewHook(testhooks.TaskQueuesInDeploymentSyncBatchSize, 1))

	// registering 5 task-queues in the version which would result in the creation of 5 batches, each with 1 task-queue, during the SyncState call.
	versionCreateTime := timestamppb.Now()
	taskQueues := 5
	for i := range taskQueues {
		go s.pollFromDeploymentWithTaskQueueNumber(ctx, tv, i)
	}

	// ensure the version has been created in the deployment with the right number of task-queues
	s.ensureCreateVersionInDeployment(tv)
	s.ensureCreateVersionWithExpectedTaskQueues(ctx, tv, taskQueues)

	// make the current version versioned, so that we can set ramp to unversioned later
	setCurrentUpdateTime := timestamppb.Now()
	s.setCurrentVersion(ctx, tv, true, "")

	// set ramp to unversioned which should trigger a batch of SyncDeploymentVersionUserData requests.
	setRampingUpdateTime := timestamppb.Now()
	s.setAndVerifyRampingVersionUnversionedOption(ctx, tv, true, false, 75, true, false, true, "")

	// check that the current version's task queues have ramping version == __unversioned__
	for i := range taskQueues {
		s.verifyTaskQueueVersioningInfo(ctx, tv.WithTaskQueueNumber(i).TaskQueue(), tv.DeploymentVersionString(), worker_versioning.UnversionedVersionId, 75)
	}

	// verify if the worker-deployment has the right ramping version set
	resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
	})
	s.Nil(err)
	// nolint:staticcheck // SA1019: old worker versioning
	s.Equal(worker_versioning.UnversionedVersionId, resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetRampingVersion())
	s.Nil(resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetRampingDeploymentVersion())

	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       tv.DeploymentSeries(),
			CreateTime: versionCreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:                      worker_versioning.UnversionedVersionId,
				RampingVersionPercentage:            75,
				RampingVersionChangedTime:           setRampingUpdateTime,
				RampingVersionPercentageChangedTime: setRampingUpdateTime,
				CurrentVersion:                      tv.DeploymentVersionString(),
				CurrentVersionChangedTime:           setCurrentUpdateTime,
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
					LastCurrentTime:      setCurrentUpdateTime,
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
				},
			},
			LastModifierIdentity: tv.ClientIdentity(),
		},
	})

}

// SetCurrent tests

func (s *WorkerDeploymentSuite) TestDescribeWorkerDeployment_SetCurrentVersion() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	tv := testvars.New(s)

	firstVersion := tv.WithBuildIDNumber(1)
	secondVersion := tv.WithBuildIDNumber(2)

	version1CreateTime := timestamppb.Now()
	// Start poller with cancellable context for deterministic control
	pollerCtx, pollerCancel := context.WithCancel(ctx)
	defer pollerCancel()
	go s.pollFromDeployment(pollerCtx, firstVersion)

	// Wait for version to be created deterministically
	s.ensureCreateVersionInDeployment(firstVersion)

	// Verify no current deployment version is set initially
	resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
	})
	s.NoError(err)
	s.Equal(worker_versioning.UnversionedVersionId, resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetCurrentVersion()) //nolint:staticcheck // SA1019: old worker versioning
	s.Nil(resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetCurrentDeploymentVersion())

	// Set first version as current version
	firstVersionCurrentUpdateTime := timestamppb.Now()
	s.setCurrentVersion(ctx, firstVersion, true, "")

	resp, err = s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       tv.DeploymentSeries(),
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
			LastModifierIdentity: tv.ClientIdentity(),
		},
	})

	// Set a new second version and set it as the current version
	version2CreateTime := timestamppb.Now()
	// Start second poller with cancellable context
	poller2Ctx, poller2Cancel := context.WithCancel(ctx)
	defer poller2Cancel()
	go s.pollFromDeployment(poller2Ctx, secondVersion)
	secondVersionCurrentUpdateTime := timestamppb.Now()
	s.setCurrentVersion(ctx, secondVersion, true, "")

	// Verify that the first version is draining, has an updated last deactivation time + second version is current, has an updated first activation time.
	resp, err = s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
	})
	s.NoError(err)
	s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
		WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
			Name:       tv.DeploymentSeries(),
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
			LastModifierIdentity: tv.ClientIdentity(),
		},
	})

}

func (s *WorkerDeploymentSuite) TestSetCurrentVersion_Batching() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	tv := testvars.New(s)

	s.InjectHook(testhooks.NewHook(testhooks.TaskQueuesInDeploymentSyncBatchSize, 1))

	// registering 5 task-queues in the version which would result in the creation of 5 batches, each with 1 task-queue, during the SyncState call.
	versionCreateTime := timestamppb.Now()
	taskQueues := 5
	for i := range taskQueues {
		go s.pollFromDeploymentWithTaskQueueNumber(ctx, tv, i)
	}

	// ensure the version has been created in the deployment with the right number of task-queues
	s.ensureCreateVersionInDeployment(tv)
	s.ensureCreateVersionWithExpectedTaskQueues(ctx, tv, taskQueues)

	// verify that all the registered task-queues have "__unversioned__" as their current version
	for i := range taskQueues {
		s.verifyTaskQueueVersioningInfo(ctx, tv.WithTaskQueueNumber(i).TaskQueue(), worker_versioning.UnversionedVersionId, "", 0)
	}

	// set current and check that the current version's task queues have new current version
	setCurrentUpdateTime := timestamppb.Now()
	s.setCurrentVersion(ctx, tv, true, "")

	// verify the current version has propogated to all the registered task-queues userData
	for i := range taskQueues {
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
				RampingVersion:                      "",
				RampingVersionPercentage:            0,
				RampingVersionChangedTime:           nil,
				RampingVersionPercentageChangedTime: nil,
				CurrentVersion:                      tv.DeploymentVersionString(),
				CurrentVersionChangedTime:           setCurrentUpdateTime,
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
					LastCurrentTime:      setCurrentUpdateTime,
					LastDeactivationTime: nil,
					Status:               enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
				},
			},
			LastModifierIdentity: tv.ClientIdentity(),
		},
	})

}

func (s *WorkerDeploymentSuite) TestSetManagerIdentity_RW() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	tv := testvars.New(s).WithBuildIDNumber(1)

	go s.pollFromDeployment(ctx, tv)
	s.ensureCreateVersionInDeployment(tv)

	// set identity to self
	s.setAndValidateManagerIdentity(ctx, tv, true, false, "", "")

	// set identity to other
	s.setAndValidateManagerIdentity(ctx, tv, false, false, "other", "")

	// set identity to other again (should be idempotent)
	s.setAndValidateManagerIdentity(ctx, tv, false, false, "other", "")

	// unset identity
	s.setAndValidateManagerIdentity(ctx, tv, false, false, "", "")

	// set identity with bad conflict token
	s.setAndValidateManagerIdentity(ctx, tv, true, true, "", "conflict token mismatch")
}

func (s *WorkerDeploymentSuite) TestSetManagerIdentity_WithSetRampSetCurrent() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	tv := testvars.New(s).WithBuildIDNumber(1)

	go s.pollFromDeployment(ctx, tv)
	s.ensureCreateVersionInDeployment(tv)

	// set identity to self
	s.setAndValidateManagerIdentity(ctx, tv, true, false, "", "")
	// -> self can successfully set ramp
	s.setAndVerifyRampingVersion(ctx, tv, false, 1, true, "")

	// set identity to other
	s.setAndValidateManagerIdentity(ctx, tv, false, false, "other", "")
	// -> self cannot set ramp
	s.setAndVerifyRampingVersion(ctx, tv, false, 2, true, fmt.Sprintf(workerdeployment.ErrManagerIdentityMismatch, "other", tv.ClientIdentity()))
	// -> self cannot set current
	s.setCurrentVersion(ctx, tv, true, fmt.Sprintf(workerdeployment.ErrManagerIdentityMismatch, "other", tv.ClientIdentity()))

	// unset identity
	s.setAndValidateManagerIdentity(ctx, tv, false, false, "", "")
	// -> self can now set ramp
	s.setAndVerifyRampingVersion(ctx, tv, false, 2, true, "")

	// set identity to self
	s.setAndValidateManagerIdentity(ctx, tv, true, false, "", "")
	// -> self can now set current
	s.setCurrentVersion(ctx, tv, true, "")
}

func (s *WorkerDeploymentSuite) TestSetManagerIdentity_WithDeleteVersion() {
	s.OverrideDynamicConfig(dynamicconfig.PollerHistoryTTL, 500*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	tv := testvars.New(s).WithBuildIDNumber(1)

	// start and stop polling so that version is eligible for deletion
	pollerCtx, pollerCancel := context.WithCancel(ctx)
	go s.pollFromDeployment(pollerCtx, tv)
	s.ensureCreateVersionInDeployment(tv)
	pollerCancel()

	// set identity to other
	s.setAndValidateManagerIdentity(ctx, tv, false, false, "other", "")
	// -> self cannot delete version
	s.tryDeleteVersion(ctx, tv, fmt.Sprintf(workerdeployment.ErrManagerIdentityMismatch, "other", tv.ClientIdentity()))

	// set identity to self
	s.setAndValidateManagerIdentity(ctx, tv, true, false, "", "")
	// -> self can now delete version
	s.tryDeleteVersion(ctx, tv, "")
}

// TestDeleteVersion_ServerDeleteMaxVersionsReached tests that when the internal limit for the number of versions
// in a worker-deployment (defaultMaxVersions) is reached, the server deletes the oldest version to register the new version.
// Additionally, the test verifies that the last modifier identity is not set to the identity of the worker-deployment workflow.
func (s *WorkerDeploymentSuite) TestDeleteVersion_ServerDeleteMaxVersionsReached() {
	s.OverrideDynamicConfig(dynamicconfig.PollerHistoryTTL, 1*time.Millisecond)
	s.OverrideDynamicConfig(dynamicconfig.MatchingMaxVersionsInDeployment, 1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	tv := testvars.New(s).WithBuildIDNumber(1)
	tv2 := testvars.New(s).WithBuildIDNumber(2)

	// start and stop polling so that version is eligible for deletion
	pollerCtx, pollerCancel := context.WithCancel(ctx)
	go s.pollFromDeployment(pollerCtx, tv)
	s.ensureCreateVersionInDeployment(tv)
	pollerCancel()

	// Set a different manager identity so that we can verify that the internal delete operation does not conduct the manager identity check
	// while deleting the version
	s.setAndValidateManagerIdentity(ctx, tv, false, false, "other", "")

	// Start another poller which shall aim to create a new version. This should, in turn, delete the first version.
	pollerCtx2, pollerCancel2 := context.WithCancel(ctx)
	go s.pollFromDeployment(pollerCtx2, tv2)
	s.ensureCreateVersionInDeployment(tv2)
	pollerCancel2()

	// Verify that the worker deployment only has one version in it's version summaries.
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentSeries(),
		})
		a.NoError(err)
		a.Len(resp.GetWorkerDeploymentInfo().GetVersionSummaries(), 1)
		a.Equal(tv2.ExternalDeploymentVersion().GetBuildId(), resp.GetWorkerDeploymentInfo().GetVersionSummaries()[0].GetDeploymentVersion().GetBuildId())

		// Also verify that the last modifier identity is not set to the identity of the worker-deployment workflow.
		a.NotEqual(tv.ClientIdentity(), resp.GetWorkerDeploymentInfo().GetLastModifierIdentity())
	}, time.Second*5, time.Millisecond*200)
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
	s.setCurrentVersion(ctx, currentVars, true, "")
	s.verifyTaskQueueVersioningInfo(ctx, currentVars.TaskQueue(), currentVars.DeploymentVersionString(), "", 0)

	// set current unversioned and check that the current version's task queues have current version unversioned again
	secondCurrentUpdateTime := timestamppb.Now()
	s.setCurrentVersionUnversionedOption(ctx, currentVars, true, true, false, true, "")
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
					LastCurrentTime:      firstCurrentUpdateTime,
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
	s.setCurrentVersion(ctx, currentVars, true, "")
	// set ramp to unversioned
	s.setAndVerifyRampingVersionUnversionedOption(ctx, tv, true, false, 75, true, false, true, "")
	// check that the current version's task queues have ramping version == __unversioned__
	s.verifyTaskQueueVersioningInfo(ctx, currentVars.TaskQueue(), currentVars.DeploymentVersionString(), worker_versioning.UnversionedVersionId, 75)

	// set current to unversioned
	s.setCurrentVersionUnversionedOption(ctx, tv, true, true, false, true, "")

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
	for i := range versions {
		s.startVersionWorkflow(ctx, tv.WithBuildIDNumber(i))
	}

	// Concurrently set 10 different versions as current version
	for i := range versions {
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
	for range 10 {
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

	tqs := 10
	for i := range tqs {
		go s.startVersionWorkflow(ctx, tv.WithTaskQueueNumber(i))
	}

	// set this version as current version
	s.setCurrentVersion(ctx, tv, false, "")

	// verify that the task queues, eventually, have this version as the current version in their versioning info
	for i := range tqs {
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
	for i := range versions {
		s.startVersionWorkflow(ctx, tv.WithBuildIDNumber(i))
	}

	// Concurrently set 10 different versions as ramping version
	for i := range versions {
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
	for range 10 {
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
	resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentVersion().GetDeploymentName(),
	})
	s.NoError(err)
	s.Equal(tv.DeploymentVersionString(), resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetRampingVersion())
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

	s.OverrideDynamicConfig(dynamicconfig.MatchingMaxTaskQueuesInDeploymentVersion, numTaskQueues)
	s.InjectHook(testhooks.NewHook(testhooks.TaskQueuesInDeploymentSyncBatchSize, syncBatchSize))
	s.InjectHook(testhooks.NewHook(testhooks.MatchingDeploymentRegisterErrorBackoff, time.Millisecond*500))

	// Need to increase max pending activities because it is set only to 10 for functional tests. it's 2000 by default.
	s.OverrideDynamicConfig(dynamicconfig.NumPendingActivitiesLimitError, numOperations)

	tv := testvars.New(s)
	dn := tv.DeploymentVersion().GetDeploymentName()
	start := time.Now()

	// For each version send pollers regularly until all TQs are registered from DescribeVersion POV
	for i := range numVersions {
		pollCtx, cancelPollers := context.WithTimeout(context.Background(), 5*time.Minute)

		sendPollers := func() {
			for j := range numTaskQueues {
				go s.pollFromDeployment(pollCtx, tv.WithBuildIDNumber(i).WithTaskQueueNumber(j))
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
			resp, err := s.FrontendClient().DescribeWorkerDeployment(pollCtx, &workflowservice.DescribeWorkerDeploymentRequest{
				Namespace:      s.Namespace().String(),
				DeploymentName: dn,
			})
			a.NoError(err)
			a.NotNil(resp.GetWorkerDeploymentInfo())
			a.Len(resp.GetWorkerDeploymentInfo().GetVersionSummaries(), i+1)
		}, 3*time.Minute, 500*time.Millisecond)

		fmt.Printf(">>> Time taken version %d added: %v\n", i, time.Since(start))

		s.EventuallyWithT(func(t *assert.CollectT) {
			a := require.New(t)
			resp, err := s.FrontendClient().DescribeWorkerDeploymentVersion(pollCtx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
				Namespace:         s.Namespace().String(),
				DeploymentVersion: tv.WithBuildIDNumber(i).ExternalDeploymentVersion(),
			})
			a.NoError(err)
			a.Len(resp.GetVersionTaskQueues(), numTaskQueues)
		}, 5*time.Minute, 1000*time.Millisecond)

		t.Stop()
		cancelPollers()

		fmt.Printf(">>> Time taken registration for version %d: %v\n", i, time.Since(start))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Rapidly perform 20 setCurrent and setRamping operations, each targeting one of the 3 versions
	for i := range numOperations {
		// Alternate between setCurrent and setRamping
		targetVersion := i % numVersions
		versionTV := tv.WithBuildIDNumber(targetVersion)
		for {
			var err error
			if i%2 == 0 {
				// setCurrent operation
				_, err = s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
					Namespace:               s.Namespace().String(),
					DeploymentName:          dn,
					BuildId:                 versionTV.DeploymentVersion().GetBuildId(),
					IgnoreMissingTaskQueues: true,
					Identity:                tv.ClientIdentity(),
				})
			} else {
				// setRamping operation
				_, err = s.FrontendClient().SetWorkerDeploymentRampingVersion(ctx, &workflowservice.SetWorkerDeploymentRampingVersionRequest{
					Namespace:               s.Namespace().String(),
					DeploymentName:          dn,
					BuildId:                 versionTV.DeploymentVersion().GetBuildId(),
					IgnoreMissingTaskQueues: true,
					Identity:                tv.ClientIdentity(),
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
		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
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
		tqTV := tv.WithTaskQueueNumber(j)
		tqUD, err := s.GetTestCluster().MatchingClient().GetTaskQueueUserData(ctx, &matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:   s.NamespaceID().String(),
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
	s.OverrideDynamicConfig(dynamicconfig.WorkflowExecutionMaxInFlightUpdates, 2) // Lowering the limit to encounter ResourceExhausted errors

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	tv := testvars.New(s)
	versions := 5
	errChan := make(chan error, versions)

	// Start all version workflows first
	for i := range versions {
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tv := testvars.New(s)
	rampingVars := tv.WithBuildIDNumber(1)
	s.startVersionWorkflow(ctx, rampingVars)
	s.setAndVerifyRampingVersionUnversionedOption(ctx, rampingVars, true, false, 50, true, false, true, "ramping version __unversioned__ is already current")
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
	s.setCurrentVersion(ctx, currentVars, true, "")
	s.verifyTaskQueueVersioningInfo(ctx, currentVars.TaskQueue(), currentVars.DeploymentVersionString(), "", 0)

	// set ramp to unversioned
	s.setAndVerifyRampingVersionUnversionedOption(ctx, tv, true, false, 75, true, false, true, "")

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

func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentCurrentVersion_NoPollers() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	tv := testvars.New(s).WithBuildIDNumber(1)

	// try to set current with allowNoPollers=false --> error
	allowNoPollers := false
	expectedErr := fmt.Sprintf(workerdeployment.ErrWorkerDeploymentNotFound, tv.DeploymentVersion().GetDeploymentName())
	s.setCurrentVersionAllowNoPollersOption(ctx, tv, true, allowNoPollers, false, expectedErr)

	// try to set current with allowNoPollers=true --> success
	allowNoPollers = true
	expectedErr = ""
	versionCreateTime := timestamppb.Now()
	s.setCurrentVersionAllowNoPollersOption(ctx, tv, true, allowNoPollers, false, expectedErr)

	// let a poller arrive with that version --> triggers user data propagation
	go s.pollFromDeployment(ctx, tv)

	s.EventuallyWithT(func(t *assert.CollectT) {
		// check describe worker deployment
		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentSeries(),
		})
		a := require.New(t)
		a.NoError(err)
		s.verifyWorkerDeploymentInfo(a, &deploymentpb.WorkerDeploymentInfo{
			Name:       tv.DeploymentSeries(),
			CreateTime: versionCreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				CurrentVersion:            tv.DeploymentVersionString(),
				CurrentVersionChangedTime: versionCreateTime,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              tv.DeploymentVersionString(),
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
			LastModifierIdentity: tv.ClientIdentity(),
		}, resp.GetWorkerDeploymentInfo())
	}, 10*time.Second, 100*time.Millisecond)

	// that poller's task queue should have the current versioning info
	s.verifyTaskQueueVersioningInfo(ctx, tv.TaskQueue(), tv.DeploymentVersionString(), "", 0)
}

func (s *WorkerDeploymentSuite) TestSetWorkerDeploymentRampingVersion_NoPollers() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	tv := testvars.New(s).WithBuildIDNumber(1)

	// try to set ramping with allowNoPollers=false --> error
	allowNoPollers := false
	expectedErr := fmt.Sprintf(workerdeployment.ErrWorkerDeploymentNotFound, tv.DeploymentVersion().GetDeploymentName())
	s.setAndVerifyRampingVersionUnversionedOption(ctx, tv, false, false, 5, true, allowNoPollers, false, expectedErr)

	// try to set ramping with allowNoPollers=true --> success
	allowNoPollers = true
	expectedErr = ""
	versionCreateTime := timestamppb.Now()
	s.setAndVerifyRampingVersionUnversionedOption(ctx, tv, false, false, 5, true, allowNoPollers, false, expectedErr)

	// let a poller arrive with that version --> triggers user data propagation
	go s.pollFromDeployment(ctx, tv)

	s.EventuallyWithT(func(t *assert.CollectT) {
		// check describe worker deployment
		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentSeries(),
		})
		a := require.New(t)
		a.NoError(err)
		s.verifyWorkerDeploymentInfo(a, &deploymentpb.WorkerDeploymentInfo{
			Name:       tv.DeploymentSeries(),
			CreateTime: versionCreateTime,
			RoutingConfig: &deploymentpb.RoutingConfig{
				RampingVersion:                      tv.DeploymentVersionString(),
				RampingVersionPercentage:            5,
				RampingVersionChangedTime:           versionCreateTime,
				RampingVersionPercentageChangedTime: versionCreateTime,
				CurrentVersion:                      worker_versioning.UnversionedVersionId,
				CurrentVersionChangedTime:           nil,
			},
			VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{
				{
					Version:              tv.DeploymentVersionString(),
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
			LastModifierIdentity: tv.ClientIdentity(),
		}, resp.GetWorkerDeploymentInfo())
	}, 10*time.Second, 100*time.Millisecond)

	// that poller's task queue should have the ramping version info
	s.verifyTaskQueueVersioningInfo(ctx, tv.TaskQueue(), worker_versioning.UnversionedVersionId, tv.DeploymentVersionString(), 5)
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
	s.OverrideDynamicConfig(dynamicconfig.PollerHistoryTTL, 500*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tv1 := testvars.New(s).WithBuildIDNumber(1)
	tv2 := testvars.New(s).WithBuildIDNumber(2)
	tv3 := testvars.New(s).WithBuildIDNumber(3)

	// Start deployment workflow 1 and wait for the deployment version to exist
	v1CreateTime := timestamppb.New(time.Now())
	s.startVersionWorkflow(ctx, tv1)

	// Set v1 as current version
	setCurrentV1UpdateTime := timestamppb.Now()
	s.setCurrentVersion(ctx, tv1, true, "")
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
					LastCurrentTime:      setCurrentV1UpdateTime,
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
	s.setCurrentVersion(ctx, tv2, true, "")
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
		resp, err := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: s.Namespace().String(),
			Version:   tv1.DeploymentVersionString(),
		})
		a.NoError(err)
		a.Equal(enumspb.VERSION_DRAINAGE_STATUS_DRAINED, resp.GetWorkerDeploymentVersionInfo().GetDrainageInfo().GetStatus())
	}, time.Second*10, time.Millisecond*1000)

	// Verify that the drainageStatus of v1 has been updated in the VersionSummaries
	s.EventuallyWithT(func(t *assert.CollectT) {
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
	s.setAndVerifyRampingVersion(ctx, tv1, false, 10, false, "")

	// verify if the right information is set in the DescribeWorkerDeployment response
	s.EventuallyWithT(func(t *assert.CollectT) {
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
	s.setCurrentVersion(ctx, tv1, true, "")

	// Verify that v2 is drained
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: s.Namespace().String(),
			Version:   tv2.DeploymentVersionString(),
		})
		a.NoError(err)
		a.Equal(enumspb.VERSION_DRAINAGE_STATUS_DRAINED, resp.GetWorkerDeploymentVersionInfo().GetDrainageInfo().GetStatus())
	}, time.Second*10, time.Millisecond*1000)

	// verify if the right information is set in the DescribeWorkerDeployment response
	s.EventuallyWithT(func(t *assert.CollectT) {
		resp, err = s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv1.DeploymentSeries(),
		})
		s.NoError(err)
		s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
			WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
				Name:       tv1.DeploymentSeries(),
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
	s.startVersionWorkflow(ctx, tv3)

	newCurrentV3UpdateTime := timestamppb.Now()
	s.setCurrentVersion(ctx, tv3, true, "")

	// Verify that v1 is drained eventually
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: s.Namespace().String(),
			Version:   tv1.DeploymentVersionString(),
		})
		a.NoError(err)
		a.Equal(enumspb.VERSION_DRAINAGE_STATUS_DRAINED, resp.GetWorkerDeploymentVersionInfo().GetDrainageInfo().GetStatus())
	}, time.Second*10, time.Millisecond*1000)

	// Verify that v1, which was rolled back to being current previously, is drained with it's information present
	// in the deployment workflow.
	s.EventuallyWithT(func(t *assert.CollectT) {
		resp, err = s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv1.DeploymentSeries(),
		})
		s.NoError(err)
		s.verifyDescribeWorkerDeployment(resp, &workflowservice.DescribeWorkerDeploymentResponse{
			WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
				Name:       tv1.DeploymentSeries(),
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
	s.setCurrentVersion(ctx, tv1, true, "")
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
					LastCurrentTime:      setCurrentV1UpdateTime,
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
	s.setCurrentVersion(ctx, tv2, true, "")
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
		resp, err := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: s.Namespace().String(),
			Version:   tv1.DeploymentVersionString(),
		})
		a.NoError(err)
		a.Equal(enumspb.VERSION_DRAINAGE_STATUS_DRAINED, resp.GetWorkerDeploymentVersionInfo().GetDrainageInfo().GetStatus())
	}, time.Second*10, time.Millisecond*1000)

	// Verify that the drainageStatus of v1 has been updated in the VersionSummaries
	s.EventuallyWithT(func(t *assert.CollectT) {
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
	s.setAndVerifyRampingVersion(ctx, tv1, false, 10, false, "")

	// verify if the right information is set in the DescribeWorkerDeployment response
	s.EventuallyWithT(func(t *assert.CollectT) {
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
	s.OverrideDynamicConfig(dynamicconfig.PollerHistoryTTL, 500*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	tv1 := testvars.New(s).WithBuildIDNumber(1)

	// Start deployment workflow 1 and wait for the deployment version to exist
	// Use a cancellable context so we can stop the poller before checking pollers disappeared
	pollerCtx, pollerCancel := context.WithCancel(ctx)
	defer pollerCancel()
	s.startVersionWorkflow(pollerCtx, tv1)

	// Signal the first version to be drained. Only do this in tests.
	versionWorkflowID := workerdeployment.GenerateVersionWorkflowID(tv1.DeploymentSeries(), tv1.BuildID())
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

	// Stop the poller so it doesn't keep polling
	pollerCancel()

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
	s.tryDeleteVersion(ctx, tv1, "")

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
			s.False(proto.Equal(tv1.ExternalDeploymentVersion(), vs.GetDeploymentVersion()))
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
		s.ProtoEqual(tv1.ExternalDeploymentVersion(), resp.GetWorkerDeploymentInfo().GetVersionSummaries()[0].GetDeploymentVersion())
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
	expectedError string,
) {
	_, err := s.FrontendClient().DeleteWorkerDeploymentVersion(ctx, &workflowservice.DeleteWorkerDeploymentVersionRequest{
		Namespace: s.Namespace().String(),
		Version:   tv.DeploymentVersionString(),
		Identity:  tv.ClientIdentity(),
	})
	if expectedError == "" {
		s.Nil(err)
	} else {
		s.Error(err)
		s.Contains(err.Error(), expectedError)
	}
}

// verifyTimestampWithinRange asserts if the actual timestamp is set to an appropriate value. It
// does this check by checking if the timestamp set is within respectable bounds or is equal to the
// expected timestamp.
func (s *WorkerDeploymentSuite) verifyTimestampWithinRange(a *require.Assertions, expected, actual *timestamppb.Timestamp, msg string) {
	a.Equalf(expected == nil, actual == nil,
		"expected %s to be: '%s', actual: %s", msg, expected.AsTime().String(), actual.AsTime().String())
	if expected == nil {
		return
	}
	acceptableDelayBetweenExpectedAndActual := 5 * time.Second // works locally with 2s, but setting to 5 to avoid flakes in CI
	a.Truef(expected.AsTime().Equal(actual.AsTime()) || (actual.AsTime().After(expected.AsTime()) && actual.AsTime().Before(expected.AsTime().Add(acceptableDelayBetweenExpectedAndActual))),
		"expected %s to be: '%s', actual: %s", msg, expected.AsTime().String(), actual.AsTime().String())
}

func (s *WorkerDeploymentSuite) verifyVersionSummary(a *require.Assertions, expected, actual *deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary) {
	a.Equal(expected.GetVersion(), actual.GetVersion())                                                                           //nolint:staticcheck // SA1019: old worker versioning
	a.Equal(worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(expected.GetVersion()), actual.GetDeploymentVersion()) //nolint:staticcheck // SA1019: old worker versioning
	a.Equalf(expected.GetDrainageInfo().GetStatus().String(), actual.GetDrainageInfo().GetStatus().String(),
		"Version %s:%s drainage status mismatch", actual.GetDeploymentVersion().GetDeploymentName(), actual.GetDeploymentVersion().GetBuildId())
	a.Equalf(expected.GetStatus(), actual.GetStatus(),
		"Version %s:%s status mismatch", actual.GetDeploymentVersion().GetDeploymentName(), actual.GetDeploymentVersion().GetBuildId())

	s.verifyTimestampWithinRange(a, expected.GetCreateTime(), actual.GetCreateTime(),
		fmt.Sprintf("Version %s:%s create time", actual.GetDeploymentVersion().GetDeploymentName(), actual.GetDeploymentVersion().GetBuildId()))
	s.verifyTimestampWithinRange(a, expected.GetRoutingUpdateTime(), actual.GetRoutingUpdateTime(),
		fmt.Sprintf("Version %s:%s routing update time", actual.GetDeploymentVersion().GetDeploymentName(), actual.GetDeploymentVersion().GetBuildId()))
	s.verifyTimestampWithinRange(a, expected.GetCurrentSinceTime(), actual.GetCurrentSinceTime(),
		fmt.Sprintf("Version %s:%s current since time", actual.GetDeploymentVersion().GetDeploymentName(), actual.GetDeploymentVersion().GetBuildId()))
	s.verifyTimestampWithinRange(a, expected.GetRampingSinceTime(), actual.GetRampingSinceTime(),
		fmt.Sprintf("Version %s:%s ramping since time", actual.GetDeploymentVersion().GetDeploymentName(), actual.GetDeploymentVersion().GetBuildId()))
	s.verifyTimestampWithinRange(a, expected.GetFirstActivationTime(), actual.GetFirstActivationTime(),
		fmt.Sprintf("Version %s:%s first activation time", actual.GetDeploymentVersion().GetDeploymentName(), actual.GetDeploymentVersion().GetBuildId()))
	s.verifyTimestampWithinRange(a, expected.GetLastCurrentTime(), actual.GetLastCurrentTime(),
		fmt.Sprintf("Version %s:%s last current time", actual.GetDeploymentVersion().GetDeploymentName(), actual.GetDeploymentVersion().GetBuildId()))
	s.verifyTimestampWithinRange(a, expected.GetLastDeactivationTime(), actual.GetLastDeactivationTime(),
		fmt.Sprintf("Version %s:%s last deactivation time", actual.GetDeploymentVersion().GetDeploymentName(), actual.GetDeploymentVersion().GetBuildId()))
}

func (s *WorkerDeploymentSuite) verifyRoutingConfig(a *require.Assertions, expected, actual *deploymentpb.RoutingConfig, deploymentName string) {
	a.Equal(expected.GetRampingVersion(), actual.GetRampingVersion())                                                                                //nolint:staticcheck // SA1019: old worker versioning
	s.ProtoEqual(worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(expected.GetRampingVersion()), actual.GetRampingDeploymentVersion()) //nolint:staticcheck // SA1019: worker versioning v0.31
	a.Equal(expected.GetRampingVersionPercentage(), actual.GetRampingVersionPercentage())
	a.Equal(expected.GetCurrentVersion(), actual.GetCurrentVersion())                                                                                //nolint:staticcheck // SA1019: old worker versioning
	s.ProtoEqual(worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(expected.GetCurrentVersion()), actual.GetCurrentDeploymentVersion()) //nolint:staticcheck // SA1019: worker versioning v0.31

	s.verifyTimestampWithinRange(a, expected.GetRampingVersionChangedTime(), actual.GetRampingVersionChangedTime(),
		fmt.Sprintf("Deployment %s ramping version changed time", deploymentName))
	s.verifyTimestampWithinRange(a, expected.GetCurrentVersionChangedTime(), actual.GetCurrentVersionChangedTime(),
		fmt.Sprintf("Deployment %s current version changed time", deploymentName))
	s.verifyTimestampWithinRange(a, expected.GetRampingVersionPercentageChangedTime(), actual.GetRampingVersionPercentageChangedTime(),
		fmt.Sprintf("Deployment %s ramping percentage changed time", deploymentName))
}

func (s *WorkerDeploymentSuite) verifyWorkerDeploymentInfo(a *require.Assertions, expected, actual *deploymentpb.WorkerDeploymentInfo) {
	a.True((actual == nil) == (expected == nil))
	a.Equal(expected.GetName(), actual.GetName())
	a.True((actual.GetRoutingConfig() == nil) == (expected.GetRoutingConfig() == nil))
	a.Equal(expected.GetLastModifierIdentity(), actual.GetLastModifierIdentity())

	s.verifyTimestampWithinRange(a, expected.GetCreateTime(), actual.GetCreateTime(),
		fmt.Sprintf("Deployment %s create time", expected.GetName()))
	s.verifyRoutingConfig(a, expected.GetRoutingConfig(), actual.GetRoutingConfig(), expected.GetName())

	// Verify version summaries
	for _, expectedSummary := range expected.GetVersionSummaries() {
		found := false
		for _, actualSummary := range actual.GetVersionSummaries() {
			if actualSummary.Version == expectedSummary.Version { //nolint:staticcheck // SA1019: old worker versioning
				s.verifyVersionSummary(a, expectedSummary, actualSummary)
				found = true
				break
			}
		}
		a.True(found)
	}
}

func (s *WorkerDeploymentSuite) verifyDescribeWorkerDeployment(
	actualResp *workflowservice.DescribeWorkerDeploymentResponse,
	expectedResp *workflowservice.DescribeWorkerDeploymentResponse,
) {
	s.True((actualResp == nil) == (expectedResp == nil))
	s.verifyWorkerDeploymentInfo(s.Assertions, expectedResp.GetWorkerDeploymentInfo(), actualResp.GetWorkerDeploymentInfo())
}

func (s *WorkerDeploymentSuite) setAndVerifyRampingVersion(
	ctx context.Context,
	tv *testvars.TestVars,
	unset bool,
	percentage int,
	ignoreMissingTaskQueues bool,
	expectedError string,
) {
	s.setAndVerifyRampingVersionUnversionedOption(ctx, tv, false, unset, percentage, ignoreMissingTaskQueues, false, true, expectedError)
}

//nolint:staticcheck // SA1019
func (s *WorkerDeploymentSuite) setAndVerifyRampingVersionUnversionedOption(
	ctx context.Context,
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
			s.ensureCreateVersionInDeployment(tv)
		} else {
			s.ensureCreateDeployment(tv)
		}
	}
	_, err := s.FrontendClient().SetWorkerDeploymentRampingVersion(ctx, &workflowservice.SetWorkerDeploymentRampingVersionRequest{
		Namespace:               s.Namespace().String(),
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

func (s *WorkerDeploymentSuite) setCurrentVersion(ctx context.Context, tv *testvars.TestVars, ignoreMissingTaskQueues bool, expectedError string) {
	s.setCurrentVersionUnversionedOption(ctx, tv, false, ignoreMissingTaskQueues, false, true, expectedError)
}

func (s *WorkerDeploymentSuite) setCurrentVersionAllowNoPollersOption(
	ctx context.Context,
	tv *testvars.TestVars,
	ignoreMissingTaskQueues, allowNoPollers, ensureSystemWorkflowsExist bool,
	expectedError string,
) {
	s.setCurrentVersionUnversionedOption(ctx, tv, false, ignoreMissingTaskQueues, allowNoPollers, ensureSystemWorkflowsExist, expectedError)
}

func (s *WorkerDeploymentSuite) setCurrentVersionUnversionedOption(
	ctx context.Context,
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
			s.ensureCreateDeployment(tv)
		} else {
			s.ensureCreateVersionInDeployment(tv)
		}
	}

	_, err := s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:               s.Namespace().String(),
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

func (s *WorkerDeploymentSuite) setAndValidateManagerIdentity(ctx context.Context, tv *testvars.TestVars, self, useWrongConflictToken bool, newManager, expectedError string) {
	s.ensureCreateDeployment(tv)
	s.ensureCreateVersionInDeployment(tv)

	var cT []byte
	if useWrongConflictToken {
		cT, _ = time.Now().MarshalBinary()
	} else {
		desc, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentVersion().GetDeploymentName(),
		})
		s.NoError(err)
		cT = desc.GetConflictToken()
	}

	req := &workflowservice.SetWorkerDeploymentManagerRequest{
		Namespace:          s.Namespace().String(),
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
	_, err := s.FrontendClient().SetWorkerDeploymentManager(ctx, req)
	if expectedError != "" {
		s.Error(err)
		s.Contains(err.Error(), expectedError)
		return
	}
	s.NoError(err)

	desc, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentVersion().GetDeploymentName(),
	})
	s.NoError(err)
	s.Equal(expectedManagerIdentity, desc.GetWorkerDeploymentInfo().GetManagerIdentity())
}

func (s *WorkerDeploymentSuite) createVersionsInDeployments(ctx context.Context, tv *testvars.TestVars, n int) []*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary {
	var expectedDeploymentSummaries []*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary

	for i := range n {
		deployment := tv.WithDeploymentSeriesNumber(i)
		version := deployment.WithBuildIDNumber(i)

		startTime := timestamppb.Now()
		s.startVersionWorkflow(ctx, version)
		setCurrentTime := timestamppb.Now()
		s.setCurrentVersion(ctx, version, true, "")

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
	a *require.Assertions,
	expectedSummary *workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary,
	actualSummary *workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary,
) bool {

	s.verifyTimestampWithinRange(a, expectedSummary.CreateTime, actualSummary.CreateTime,
		fmt.Sprintf("Deployment %s create time", expectedSummary.GetName()))

	// Current version checks
	a.Equal(expectedSummary.RoutingConfig.GetCurrentVersion(), actualSummary.RoutingConfig.GetCurrentVersion(), "Current version mismatch")                                                    //nolint:staticcheck // SA1019: old worker versioning
	s.ProtoEqual(worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(expectedSummary.RoutingConfig.GetCurrentVersion()), actualSummary.RoutingConfig.GetCurrentDeploymentVersion()) //nolint:staticcheck // SA1019: worker versioning v0.31
	s.verifyTimestampWithinRange(a, expectedSummary.RoutingConfig.GetCurrentVersionChangedTime(), actualSummary.RoutingConfig.GetCurrentVersionChangedTime(),
		fmt.Sprintf("Deployment %s current version changed time", expectedSummary.GetName()))

	// Ramping version checks
	a.Equal(expectedSummary.RoutingConfig.GetRampingVersion(), actualSummary.RoutingConfig.GetRampingVersion(), "Ramping version mismatch")                                                    //nolint:staticcheck // SA1019: old worker versioning
	s.ProtoEqual(worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(expectedSummary.RoutingConfig.GetRampingVersion()), actualSummary.RoutingConfig.GetRampingDeploymentVersion()) //nolint:staticcheck // SA1019: worker versioning v0.31
	a.Equal(expectedSummary.RoutingConfig.GetRampingVersionPercentage(), actualSummary.RoutingConfig.GetRampingVersionPercentage(), "Ramping version percentage mismatch")

	s.verifyTimestampWithinRange(a, expectedSummary.RoutingConfig.GetRampingVersionChangedTime(), actualSummary.RoutingConfig.GetRampingVersionChangedTime(),
		fmt.Sprintf("Deployment %s ramping version changed time", expectedSummary.GetName()))

	// Latest version summary checks
	s.verifyVersionSummary(a, expectedSummary.LatestVersionSummary, actualSummary.LatestVersionSummary)

	// Current version summary checks
	s.verifyVersionSummary(a, expectedSummary.CurrentVersionSummary, actualSummary.CurrentVersionSummary)

	// Ramping version summary checks
	s.verifyVersionSummary(a, expectedSummary.RampingVersionSummary, actualSummary.RampingVersionSummary)

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
			deploymentSummaryFound := false
			for _, actualDeploymentSummary := range actualDeploymentSummaries {
				// Our assumption that deployment summaries with the same name are fully ready for checks
				// may not be true since visibility might take time to update other fields.
				if actualDeploymentSummary.Name != expectedDeploymentSummary.Name {
					continue
				}
				s.verifyWorkerDeploymentSummary(a, expectedDeploymentSummary, actualDeploymentSummary)
				deploymentSummaryFound = true
				break
			}
			a.True(deploymentSummaryFound)
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

func (s *WorkerDeploymentSuite) skipBeforeVersion(version workerdeployment.DeploymentWorkflowVersion) {
	if s.workflowVersion < version {
		s.T().Skipf("test supports version %v and newer", version)
	}
}

func (s *WorkerDeploymentSuite) skipFromVersion(version workerdeployment.DeploymentWorkflowVersion) {
	if s.workflowVersion >= version {
		s.T().Skipf("test supports version older than %v", version)
	}
}
