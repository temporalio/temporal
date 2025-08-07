package tests

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/worker/workerdeployment"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	maxConcurrentBatchOperations             = 3
	testVersionDrainageRefreshInterval       = 3 * time.Second
	testVersionDrainageVisibilityGracePeriod = 3 * time.Second
	testMaxVersionsInDeployment              = 5
)

type (
	DeploymentVersionSuite struct {
		useV32 bool
		testcore.FunctionalTestBase
	}
)

var (
	testRandomMetadataValue = []byte("random metadata value")
)

func TestDeploymentVersionSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, &DeploymentVersionSuite{useV32: true})
	suite.Run(t, &DeploymentVersionSuite{useV32: false})
}

func (s *DeploymentVersionSuite) SetupSuite() {
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

		// Reduce the chance of hitting max batch job limit in tests
		dynamicconfig.FrontendMaxConcurrentBatchOperationPerNamespace.Key(): maxConcurrentBatchOperations,

		dynamicconfig.VersionDrainageStatusRefreshInterval.Key():       testVersionDrainageRefreshInterval,
		dynamicconfig.VersionDrainageStatusVisibilityGracePeriod.Key(): testVersionDrainageVisibilityGracePeriod,
	}))
}

// pollFromDeployment calls PollWorkflowTaskQueue to start deployment related workflows
func (s *DeploymentVersionSuite) pollFromDeployment(ctx context.Context, tv *testvars.TestVars) {
	_, _ = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace:         s.Namespace().String(),
		TaskQueue:         tv.TaskQueue(),
		Identity:          "random",
		DeploymentOptions: tv.WorkerDeploymentOptions(true),
	})
}

func (s *DeploymentVersionSuite) pollActivityFromDeployment(ctx context.Context, tv *testvars.TestVars) {
	_, _ = s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace:         s.Namespace().String(),
		TaskQueue:         tv.TaskQueue(),
		Identity:          "random",
		DeploymentOptions: tv.WorkerDeploymentOptions(true),
	})
}

func (s *DeploymentVersionSuite) describeVersion(tv *testvars.TestVars) (*workflowservice.DescribeWorkerDeploymentVersionResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req := &workflowservice.DescribeWorkerDeploymentVersionRequest{
		Namespace: s.Namespace().String(),
	}
	if s.useV32 {
		req.DeploymentVersion = tv.ExternalDeploymentVersion()
	} else {
		req.Version = tv.DeploymentVersionString() //nolint:staticcheck // SA1019: worker versioning v0.31
	}
	return s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, req)
}

func (s *DeploymentVersionSuite) updateMetadata(tv *testvars.TestVars, upsertEntries map[string]*commonpb.Payload, removeEntries []string) (*workflowservice.UpdateWorkerDeploymentVersionMetadataResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req := &workflowservice.UpdateWorkerDeploymentVersionMetadataRequest{
		Namespace:     s.Namespace().String(),
		UpsertEntries: upsertEntries,
		RemoveEntries: removeEntries,
	}
	if s.useV32 {
		req.DeploymentVersion = tv.ExternalDeploymentVersion()
	} else {
		req.Version = tv.DeploymentVersionString() //nolint:staticcheck // SA1019: worker versioning v0.31
	}
	return s.FrontendClient().UpdateWorkerDeploymentVersionMetadata(ctx, req)
}

func (s *DeploymentVersionSuite) startVersionWorkflow(ctx context.Context, tv *testvars.TestVars) {
	go s.pollFromDeployment(ctx, tv)
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		resp, err := s.describeVersion(tv)
		a.NoError(err)
		// regardless of s.useV32, we want to read both version formats
		a.Equal(tv.DeploymentVersionString(), resp.GetWorkerDeploymentVersionInfo().GetVersion())
		a.Equal(tv.ExternalDeploymentVersion().GetDeploymentName(), resp.GetWorkerDeploymentVersionInfo().GetDeploymentVersion().GetDeploymentName())
		a.Equal(tv.ExternalDeploymentVersion().GetBuildId(), resp.GetWorkerDeploymentVersionInfo().GetDeploymentVersion().GetBuildId())
		a.Equal(enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE, resp.GetWorkerDeploymentVersionInfo().GetStatus())

		newResp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentSeries(),
		})
		a.NoError(err)
		var versionSummaryNames []string
		var versionSummaryVersions []*deploymentpb.WorkerDeploymentVersion
		for _, versionSummary := range newResp.GetWorkerDeploymentInfo().GetVersionSummaries() {
			versionSummaryNames = append(versionSummaryNames, versionSummary.GetVersion())
			versionSummaryVersions = append(versionSummaryVersions, versionSummary.GetDeploymentVersion())
		}
		a.Contains(versionSummaryNames, tv.DeploymentVersionString())
		contains := slices.ContainsFunc(versionSummaryVersions, func(v *deploymentpb.WorkerDeploymentVersion) bool {
			return v.GetDeploymentName() == tv.ExternalDeploymentVersion().GetDeploymentName() &&
				v.GetBuildId() == tv.ExternalDeploymentVersion().GetBuildId()
		})
		a.True(contains)
	}, time.Second*5, time.Millisecond*200)
}

func (s *DeploymentVersionSuite) startVersionWorkflowExpectFailAddVersion(ctx context.Context, tv *testvars.TestVars) {
	_, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace:         s.Namespace().String(),
		TaskQueue:         tv.TaskQueue(),
		Identity:          "random",
		DeploymentOptions: tv.WorkerDeploymentOptions(true),
	})
	s.Error(err, serviceerror.NewUnavailable("cannot add version, already at max versions 4"))
}

func (s *DeploymentVersionSuite) TestForceCAN_NoOpenWFS() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tv := testvars.New(s)

	// Start a version workflow
	s.startVersionWorkflow(ctx, tv)

	// Set the version as current
	err := s.setCurrent(tv, false)
	s.NoError(err)

	// ForceCAN
	versionWorkflowID := worker_versioning.GenerateVersionWorkflowID(tv.DeploymentSeries(), tv.BuildID())
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: versionWorkflowID,
	}

	err = s.SendSignal(s.Namespace().String(), workflowExecution, workerdeployment.ForceCANSignalName, nil, tv.ClientIdentity())
	s.NoError(err)

	// verifying we see our registered workers in the version deployment even after a CAN
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)

		resp, err := s.describeVersion(tv)
		if !a.NoError(err) {
			return
		}
		a.Equal(tv.DeploymentVersionString(), resp.GetWorkerDeploymentVersionInfo().GetVersion()) //nolint:staticcheck // SA1019: worker versioning v0.31
		a.Equal(tv.ExternalDeploymentVersion().GetDeploymentName(), resp.GetWorkerDeploymentVersionInfo().GetDeploymentVersion().GetDeploymentName())
		a.Equal(tv.ExternalDeploymentVersion().GetBuildId(), resp.GetWorkerDeploymentVersionInfo().GetDeploymentVersion().GetBuildId())

		a.Equal(1, len(resp.GetVersionTaskQueues()))
		a.Equal(1, len(resp.GetWorkerDeploymentVersionInfo().GetTaskQueueInfos()))

		// verify that the version state is intact even after a CAN
		a.Equal(tv.TaskQueue().GetName(), resp.GetVersionTaskQueues()[0].Name)
		a.Equal(tv.TaskQueue().GetName(), resp.GetWorkerDeploymentVersionInfo().GetTaskQueueInfos()[0].Name)
		a.NotNil(resp.GetWorkerDeploymentVersionInfo().GetCurrentSinceTime())
		a.NotNil(resp.GetWorkerDeploymentVersionInfo().GetRoutingChangedTime())
		a.NotNil(resp.GetWorkerDeploymentVersionInfo().GetCurrentSinceTime())
		a.Nil(resp.GetWorkerDeploymentVersionInfo().GetDrainageInfo())
		a.Equal(enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT, resp.GetWorkerDeploymentVersionInfo().GetStatus())
	}, time.Second*10, time.Millisecond*1000)
}

func (s *DeploymentVersionSuite) TestDescribeVersion_RegisterTaskQueue() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tv := testvars.New(s)

	numberOfDeployments := 1

	// Starting a deployment workflow
	go s.pollFromDeployment(ctx, tv)

	// Querying the Deployment
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)

		resp, err := s.describeVersion(tv)
		a.NoError(err)

		a.Equal(tv.DeploymentVersionString(), resp.GetWorkerDeploymentVersionInfo().GetVersion()) //nolint:staticcheck // SA1019: worker versioning v0.31
		a.Equal(tv.ExternalDeploymentVersion().GetDeploymentName(), resp.GetWorkerDeploymentVersionInfo().GetDeploymentVersion().GetDeploymentName())
		a.Equal(tv.ExternalDeploymentVersion().GetBuildId(), resp.GetWorkerDeploymentVersionInfo().GetDeploymentVersion().GetBuildId())
		a.Equal(enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE, resp.GetWorkerDeploymentVersionInfo().GetStatus())

		a.Equal(numberOfDeployments, len(resp.GetWorkerDeploymentVersionInfo().GetTaskQueueInfos()))
		a.Equal(tv.TaskQueue().GetName(), resp.GetWorkerDeploymentVersionInfo().GetTaskQueueInfos()[0].Name)

		a.Equal(numberOfDeployments, len(resp.GetVersionTaskQueues()))
		a.Equal(tv.TaskQueue().GetName(), resp.GetVersionTaskQueues()[0].Name)
	}, time.Second*5, time.Millisecond*200)
}

func (s *DeploymentVersionSuite) TestDescribeVersion_RegisterTaskQueue_ConcurrentPollers() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	tv := testvars.New(s)

	root, err := tqid.PartitionFromProto(tv.TaskQueue(), s.Namespace().String(), enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	s.NoError(err)
	// Making concurrent polls to 4 partitions, 3 polls to each
	for p := 0; p < 4; p++ {
		tv2 := tv.WithTaskQueue(root.TaskQueue().NormalPartition(p).RpcName())
		for i := 0; i < 3; i++ {
			go s.pollFromDeployment(ctx, tv2)
			go s.pollActivityFromDeployment(ctx, tv2)
		}
	}

	// Querying the Worker Deployment Version
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)

		resp, err := s.describeVersion(tv)
		if !a.NoError(err) {
			return
		}
		a.Equal(tv.DeploymentVersionString(), resp.GetWorkerDeploymentVersionInfo().GetVersion()) //nolint:staticcheck // SA1019: worker versioning v0.31
		a.Equal(tv.ExternalDeploymentVersion().GetDeploymentName(), resp.GetWorkerDeploymentVersionInfo().GetDeploymentVersion().GetDeploymentName())
		a.Equal(tv.ExternalDeploymentVersion().GetBuildId(), resp.GetWorkerDeploymentVersionInfo().GetDeploymentVersion().GetBuildId())
		a.Equal(enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE, resp.GetWorkerDeploymentVersionInfo().GetStatus())
		a.Equal(2, len(resp.GetWorkerDeploymentVersionInfo().GetTaskQueueInfos()))
		a.Equal(tv.TaskQueue().GetName(), resp.GetWorkerDeploymentVersionInfo().GetTaskQueueInfos()[0].Name)
		a.Equal(2, len(resp.GetVersionTaskQueues()))
		a.Equal(tv.TaskQueue().GetName(), resp.GetVersionTaskQueues()[0].Name)
	}, time.Second*10, time.Millisecond*1000)
}

// Name is used by testvars. We use a shorten test name in variables so that physical task queue IDs
// do not grow larger that DB column limit (currently as low as 272 chars).
func (s *DeploymentVersionSuite) Name() string {
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

//nolint:forbidigo
func (s *DeploymentVersionSuite) TestDrainageStatus_SetCurrentVersion_NoOpenWFs() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tv1 := testvars.New(s).WithBuildIDNumber(1)
	tv2 := testvars.New(s).WithBuildIDNumber(2)

	// Start deployment workflow 1 and wait for the deployment version to exist
	s.startVersionWorkflow(ctx, tv1)

	// Start deployment workflow 2 and wait for the deployment version to exist
	s.startVersionWorkflow(ctx, tv2)

	// non-current deployments have never been used and have no drainage info
	s.checkVersionDrainageAndVersionStatus(ctx, tv1, &deploymentpb.VersionDrainageInfo{}, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE, false, false)
	s.checkVersionDrainageAndVersionStatus(ctx, tv2, &deploymentpb.VersionDrainageInfo{}, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE, false, false)

	// SetCurrent tv1
	err := s.setCurrent(tv1, true)
	s.Nil(err)

	// Both versions have no drainage info and tv1 has it's status updated to current
	s.checkVersionDrainageAndVersionStatus(ctx, tv1, &deploymentpb.VersionDrainageInfo{}, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT, false, false)
	s.checkVersionDrainageAndVersionStatus(ctx, tv2, &deploymentpb.VersionDrainageInfo{}, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE, false, false)

	baseTime := time.Now()
	// SetCurrent tv2 --> tv1 starts the child drainage workflow
	err = s.setCurrent(tv2, true)
	s.Nil(err)

	changed1, checked1 := s.checkVersionDrainageAndVersionStatus(ctx, tv1, &deploymentpb.VersionDrainageInfo{
		Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINING,
	}, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING, false, false)
	s.Greater(changed1, baseTime)
	s.GreaterOrEqual(checked1, changed1)

	// tv1 should now be "drained"
	changed2, checked2 := s.checkVersionDrainageAndVersionStatus(ctx, tv1, &deploymentpb.VersionDrainageInfo{
		Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINED,
	}, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED, true, false)
	s.Greater(changed2, changed1)
	s.GreaterOrEqual(checked2, changed2)
}

func (s *DeploymentVersionSuite) TestDrainageStatus_SetCurrentVersion_YesOpenWFs() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tv1 := testvars.New(s).WithBuildIDNumber(1)
	tv2 := testvars.New(s).WithBuildIDNumber(2)

	// start deployment workflow 1 and wait for the deployment version to exist
	s.startVersionWorkflow(ctx, tv1)

	// start deployment workflow 2 and wait for the deployment version to exist
	s.startVersionWorkflow(ctx, tv2)

	// non-current deployments have never been used and have no drainage info
	s.checkVersionDrainageAndVersionStatus(ctx, tv1, &deploymentpb.VersionDrainageInfo{}, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE, false, false)
	s.checkVersionDrainageAndVersionStatus(ctx, tv2, &deploymentpb.VersionDrainageInfo{}, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE, false, false)

	// SetCurrent tv1
	err := s.setCurrent(tv1, true)
	s.Nil(err)

	// both versions have no drainage info and tv1 has it's status updated to current
	s.checkVersionDrainageAndVersionStatus(ctx, tv1, &deploymentpb.VersionDrainageInfo{}, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT, false, false)
	s.checkVersionDrainageAndVersionStatus(ctx, tv2, &deploymentpb.VersionDrainageInfo{}, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE, false, false)

	// start a pinned workflow on v1
	run := s.startPinnedWorkflow(ctx, tv1)

	baseTime := time.Now()
	// SetCurrent tv2 --> tv1 starts the child drainage workflow
	err = s.setCurrent(tv2, true)
	s.Nil(err)

	// tv1 should now be "draining" for visibilityGracePeriod duration
	changed1, checked1 := s.checkVersionDrainageAndVersionStatus(ctx, tv1, &deploymentpb.VersionDrainageInfo{
		Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINING,
	}, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING, false, false)
	s.Greater(changed1, baseTime)
	s.GreaterOrEqual(checked1, changed1)

	// tv1 should still be "draining" for visibilityGracePeriod duration
	changed2, checked2 := s.checkVersionDrainageAndVersionStatus(ctx, tv1, &deploymentpb.VersionDrainageInfo{
		Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINING,
	}, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING, true, false)
	s.Equal(changed2, changed1)
	s.Greater(checked2, checked1)

	// tv1 should still be "draining" after a refresh intervals
	changed3, checked3 := s.checkVersionDrainageAndVersionStatus(ctx, tv1, &deploymentpb.VersionDrainageInfo{
		Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINING,
	}, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING, false, true)
	s.Equal(changed3, changed1)
	s.Greater(checked3, checked2)

	// terminate workflow
	_, err = s.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: run.GetID(),
			RunId:      run.GetRunID(),
		},
		Reason:   "test",
		Identity: tv1.ClientIdentity(),
	})
	s.Nil(err)

	// tv1 should now be "drained"
	changed4, checked4 := s.checkVersionDrainageAndVersionStatus(ctx, tv1, &deploymentpb.VersionDrainageInfo{
		Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINED,
	}, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED, false, false)
	s.Greater(changed4, changed3)
	s.GreaterOrEqual(checked4, changed4)
}

func (s *DeploymentVersionSuite) startPinnedWorkflow(ctx context.Context, tv *testvars.TestVars) sdkclient.WorkflowRun {
	started := make(chan struct{}, 1)
	wf := func(ctx workflow.Context) (string, error) {
		started <- struct{}{}
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		if workflow.GetInfo(ctx).Attempt == 1 {
			return "", errors.New("try again") //nolint:err113
		}
		panic("oops")
	}
	wId := testcore.RandomizeStr("id")
	w := worker.New(s.SdkClient(), tv.TaskQueue().String(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			DeploymentSeriesName: tv.DeploymentSeries(), //nolint:staticcheck // SA1019: worker versioning v0.30
		},
		UseBuildIDForVersioning: true,         //nolint:staticcheck // SA1019: old worker versioning
		BuildID:                 tv.BuildID(), //nolint:staticcheck // SA1019: old worker versioning
		Identity:                wId,
	})
	w.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{VersioningBehavior: workflow.VersioningBehaviorPinned})
	s.NoError(w.Start())
	defer w.Stop()
	run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tv.TaskQueue().String()}, wf)
	s.NoError(err)
	s.WaitForChannel(ctx, started)
	return run
}

func (s *DeploymentVersionSuite) TestVersionIgnoresDrainageSignalWhenCurrentOrRamping() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tv1 := testvars.New(s).WithBuildIDNumber(1)

	// Start deployment workflow 1 and wait for the deployment version to exist
	s.startVersionWorkflow(ctx, tv1)

	// Make it current
	err := s.setCurrent(tv1, false)
	s.Nil(err)

	// Signal it to be drained. Only do this in tests.
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

	// describe version and confirm that it is not drained
	// add a 3s time requirement so that it does not succeed immediately
	sentSignal := time.Now()
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		a.Greater(time.Since(sentSignal), 2*time.Second)
		resp, err := s.describeVersion(tv1)
		a.NoError(err)
		a.NotEqual(enumspb.VERSION_DRAINAGE_STATUS_DRAINED, resp.GetWorkerDeploymentVersionInfo().GetDrainageInfo().GetStatus())
	}, time.Second*10, time.Millisecond*1000)
}

// Testing DeleteVersion

func (s *DeploymentVersionSuite) TestDeleteVersion_DeleteCurrentVersion() {
	// Override the dynamic config so that we can verify we don't get any unexpected masked errors.
	s.OverrideDynamicConfig(dynamicconfig.FrontendMaskInternalErrorDetails, true)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tv1 := testvars.New(s).WithBuildIDNumber(1)

	// Create a deployment version
	s.startVersionWorkflow(ctx, tv1)

	// Set version as current
	err := s.setCurrent(tv1, false)
	s.Nil(err)

	// Deleting this version should fail since the version is current
	s.tryDeleteVersion(ctx, tv1, workerdeployment.ErrVersionIsCurrentOrRamping, false)

	// Verifying workflow is not in a locked state after an invalid delete request such as the one above. If the workflow were in a locked
	// state, the passed context would have timed out making the following operation fail.
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv1.DeploymentSeries(),
		})
		a.NoError(err)
		a.Equal(tv1.DeploymentVersionString(), resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetCurrentVersion()) //nolint:staticcheck // SA1019: worker versioning v0.31
		a.Equal(tv1.ExternalDeploymentVersion(), resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetCurrentDeploymentVersion())
	}, time.Second*5, time.Millisecond*200)

}

func (s *DeploymentVersionSuite) TestDeleteVersion_DeleteRampedVersion() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tv1 := testvars.New(s).WithBuildIDNumber(1)

	// Create a deployment version
	s.startVersionWorkflow(ctx, tv1)

	// Set version as ramping
	err := s.setRamping(tv1, 0)
	s.Nil(err)

	// Deleting this version should fail since the version is ramping
	s.tryDeleteVersion(ctx, tv1, workerdeployment.ErrVersionIsCurrentOrRamping, false)

	// Verifying workflow is not in a locked state after an invalid delete request such as the one above. If the workflow were in a locked
	// state, the passed context would have timed out making the following operation fail.
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv1.DeploymentSeries(),
		})
		a.NoError(err)
		a.Equal(tv1.DeploymentVersionString(), resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetRampingVersion()) //nolint:staticcheck // SA1019: worker versioning v0.31
		a.Equal(tv1.ExternalDeploymentVersion(), resp.GetWorkerDeploymentInfo().GetRoutingConfig().GetRampingDeploymentVersion())
	}, time.Second*5, time.Millisecond*200)
}

func (s *DeploymentVersionSuite) TestDeleteVersion_NoWfs() {
	s.OverrideDynamicConfig(dynamicconfig.PollerHistoryTTL, 500*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tv1 := testvars.New(s).WithBuildIDNumber(1)

	// Create a deployment version
	s.startVersionWorkflow(ctx, tv1)

	//nolint:forbidigo
	time.Sleep(2 * time.Second) // todo (Shivam): remove this after the above skip is removed

	// delete should succeed
	s.tryDeleteVersion(ctx, tv1, "", false)

	// deployment version does not exist in the deployment list
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv1.DeploymentSeries(),
		})
		a.NoError(err)
		if resp != nil {
			for _, vs := range resp.GetWorkerDeploymentInfo().GetVersionSummaries() {
				a.NotEqual(tv1.DeploymentVersionString(), vs.Version) //nolint:staticcheck // SA1019: worker versioning v0.31
				a.NotEqual(tv1.ExternalDeploymentVersion().GetDeploymentName(), vs.GetDeploymentVersion().GetDeploymentName())
				a.NotEqual(tv1.ExternalDeploymentVersion().GetBuildId(), vs.GetDeploymentVersion().GetBuildId())
			}
		}
	}, time.Second*5, time.Millisecond*200)
}

func (s *DeploymentVersionSuite) TestDeleteVersion_DrainingVersion() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tv1 := testvars.New(s).WithBuildIDNumber(1)

	// Start deployment workflow 1 and wait for the deployment version to exist
	s.startVersionWorkflow(ctx, tv1)

	// Make the version current
	err := s.setCurrent(tv1, false)
	s.Nil(err)

	// Start another version workflow
	tv2 := testvars.New(s).WithBuildIDNumber(2)
	s.startVersionWorkflow(ctx, tv2)

	// Setting this version to current should start the drainage workflow for version1 and make it draining
	err = s.setCurrent(tv2, true)
	s.Nil(err)

	// Version should be draining
	s.checkVersionDrainageAndVersionStatus(ctx, tv1, &deploymentpb.VersionDrainageInfo{
		Status:          enumspb.VERSION_DRAINAGE_STATUS_DRAINING,
		LastChangedTime: nil, // don't test this now
		LastCheckedTime: nil, // don't test this now
	}, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING, false, false)

	// delete should fail
	s.tryDeleteVersion(ctx, tv1, workerdeployment.ErrVersionIsDraining, false)

}

func (s *DeploymentVersionSuite) TestDeleteVersion_Drained_But_Pollers_Exist() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tv1 := testvars.New(s).WithBuildIDNumber(1)

	// Start deployment workflow 1 and wait for the deployment version to exist
	s.startVersionWorkflow(ctx, tv1)

	// Make the version current
	err := s.setCurrent(tv1, false)
	s.Nil(err)

	// Start another version workflow
	tv2 := testvars.New(s).WithBuildIDNumber(2)
	s.startVersionWorkflow(ctx, tv2)

	// Setting this version to current should start the drainage workflow for version1
	err = s.setCurrent(tv2, true)
	s.Nil(err)

	// Signal the first version to be drained. Only do this in tests.
	s.signalAndWaitForDrained(ctx, tv1)

	// Version will bypass "drained" check but delete should still fail since we have active pollers.
	s.tryDeleteVersion(ctx, tv1, workerdeployment.ErrVersionHasPollers, false)
}

func (s *DeploymentVersionSuite) signalAndWaitForDrained(ctx context.Context, tv *testvars.TestVars) {
	versionWorkflowID := worker_versioning.GenerateVersionWorkflowID(tv.DeploymentSeries(), tv.BuildID())
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
	err = s.SendSignal(s.Namespace().String(), workflowExecution, workerdeployment.SyncDrainageSignalName, signalPayload, tv.ClientIdentity())
	s.Nil(err)

	// wait for drained
	s.EventuallyWithT(func(t *assert.CollectT) {
		resp, err := s.describeVersion(tv)
		assert.NoError(t, err)
		assert.Equal(t, enumspb.VERSION_DRAINAGE_STATUS_DRAINED, resp.GetWorkerDeploymentVersionInfo().GetDrainageInfo().GetStatus())
	}, 10*time.Second, time.Second)
}

func (s *DeploymentVersionSuite) waitForNoPollers(ctx context.Context, tv *testvars.TestVars) {
	s.EventuallyWithT(func(t *assert.CollectT) {
		resp, err := s.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
			Namespace:     s.Namespace().String(),
			TaskQueue:     tv.TaskQueue(),
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		})
		require.NoError(t, err)
		require.Empty(t, resp.Pollers)
	}, 10*time.Second, 100*time.Millisecond)
}

func (s *DeploymentVersionSuite) TestVersionScavenger_DeleteOnAdd() {
	testMaxVersionsInDeployment := 4
	s.OverrideDynamicConfig(dynamicconfig.PollerHistoryTTL, 2*time.Second)
	s.OverrideDynamicConfig(dynamicconfig.MatchingMaxVersionsInDeployment, testMaxVersionsInDeployment)
	// we don't want the version to drain in this test
	s.OverrideDynamicConfig(dynamicconfig.VersionDrainageStatusVisibilityGracePeriod, 60*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	tvs := make([]*testvars.TestVars, testMaxVersionsInDeployment)

	// max out the versions
	for i := 0; i < testMaxVersionsInDeployment; i++ {
		tvs[i] = testvars.New(s).WithBuildIDNumber(i)
		s.startVersionWorkflow(ctx, tvs[i])
	}

	// Make tvs[0] current
	err := s.setCurrent(tvs[0], false)
	s.Nil(err)
	// Make tvs[1] current, hence tvs[0] should go to draining
	err = s.setCurrent(tvs[1], false)
	s.Nil(err)

	// stuff above can take a long time, sending some fresh polls to ensure that deletion logic sees them.
	for i := 0; i < testMaxVersionsInDeployment; i++ {
		go s.pollFromDeployment(ctx, tvs[i])
	}
	tvMax := testvars.New(s).WithBuildIDNumber(9999)

	// try to add a version and it fails because none of the versions can be deleted
	s.startVersionWorkflowExpectFailAddVersion(ctx, tvMax)

	// this waits for no pollers from any version
	s.waitForNoPollers(ctx, tvs[0])

	// try to add a version again, and it succeeds, after deleting tvs[2] version but not tvs[3] (both are eligible)
	// TODO: This fails if I try to add tvMax again...
	s.startVersionWorkflow(ctx, testvars.New(s).WithBuildIDNumber(1111))

	// tvs[0] is draining so can't be deleted. tvs[1] is current, so tvs[2] should be deleted.
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tvMax.DeploymentSeries(),
		})
		a.NoError(err)
		var versions []string
		for _, vs := range resp.GetWorkerDeploymentInfo().GetVersionSummaries() {
			versions = append(versions, vs.Version) //nolint:staticcheck // SA1019: worker versioning v0.31
		}
		a.NotContains(versions, tvs[2].DeploymentVersionString())
		a.Contains(versions, tvs[0].DeploymentVersionString())
		a.Contains(versions, tvs[1].DeploymentVersionString())
		a.Contains(versions, tvs[3].DeploymentVersionString())
	}, time.Second*5, time.Millisecond*200)
}

func (s *DeploymentVersionSuite) TestDeleteVersion_ValidDelete() {
	s.OverrideDynamicConfig(dynamicconfig.PollerHistoryTTL, 500*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tv1 := testvars.New(s).WithBuildIDNumber(1)

	// Start deployment workflow 1 and wait for the deployment version to exist
	s.startVersionWorkflow(ctx, tv1)

	// Signal the first version to be drained. Only do this in tests.
	s.signalAndWaitForDrained(ctx, tv1)

	// Wait for pollers going away
	s.waitForNoPollers(ctx, tv1)

	// delete succeeds
	s.tryDeleteVersion(ctx, tv1, "", false)

	// deployment version does not exist in the deployment list
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv1.DeploymentSeries(),
		})
		a.NoError(err)
		if resp != nil {
			for _, vs := range resp.GetWorkerDeploymentInfo().GetVersionSummaries() {
				a.NotEqual(tv1.DeploymentVersionString(), vs.Version) //nolint:staticcheck // SA1019: worker versioning v0.31
				a.NotEqual(tv1.ExternalDeploymentVersion().GetDeploymentName(), vs.GetDeploymentVersion().GetDeploymentName())
				a.NotEqual(tv1.ExternalDeploymentVersion().GetBuildId(), vs.GetDeploymentVersion().GetBuildId())
			}
		}
	}, time.Second*5, time.Millisecond*200)

	// idempotency check: deleting the same version again should succeed
	s.tryDeleteVersion(ctx, tv1, "", false)
}

func (s *DeploymentVersionSuite) TestDeleteVersion_ValidDelete_SkipDrainage() {
	s.OverrideDynamicConfig(dynamicconfig.PollerHistoryTTL, 500*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tv1 := testvars.New(s).WithBuildIDNumber(1)

	// Start deployment workflow 1 and wait for the deployment version to exist
	s.startVersionWorkflow(ctx, tv1)

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

	// skipDrainage=true will make delete succeed
	s.tryDeleteVersion(ctx, tv1, "", false)

	// deployment version does not exist in the deployment list
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv1.DeploymentSeries(),
		})
		a.NoError(err)
		if resp != nil {
			for _, vs := range resp.GetWorkerDeploymentInfo().GetVersionSummaries() {
				a.NotEqual(tv1.DeploymentVersionString(), vs.Version) //nolint:staticcheck // SA1019: worker versioning v0.31
				a.NotEqual(tv1.ExternalDeploymentVersion().GetDeploymentName(), vs.GetDeploymentVersion().GetDeploymentName())
				a.NotEqual(tv1.ExternalDeploymentVersion().GetBuildId(), vs.GetDeploymentVersion().GetBuildId())
			}
		}
	}, time.Second*5, time.Millisecond*200)

	// idempotency check: deleting the same version again should succeed
	s.tryDeleteVersion(ctx, tv1, "", false)

	// Describe Worker Deployment should give not found
	// describe deployment version gives not found error
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		_, err := s.describeVersion(tv1)
		a.Error(err)
		var nfe *serviceerror.NotFound
		a.True(errors.As(err, &nfe))
	}, time.Second*5, time.Millisecond*200)
}

func (s *DeploymentVersionSuite) TestDeleteVersion_ConcurrentDeleteVersion() {
	s.OverrideDynamicConfig(dynamicconfig.PollerHistoryTTL, 500*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tv1 := testvars.New(s).WithBuildIDNumber(1)

	// Start deployment workflow 1 and wait for the deployment version to exist
	s.startVersionWorkflow(ctx, tv1)

	// Wait for pollers going away
	s.EventuallyWithT(func(t *assert.CollectT) {
		resp, err := s.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
			Namespace:     s.Namespace().String(),
			TaskQueue:     tv1.TaskQueue(),
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		})
		require.NoError(t, err)
		require.Empty(t, resp.Pollers)
	}, 10*time.Second, time.Second)

	// concurrent delete version requests should not break the system.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		s.tryDeleteVersion(ctx, tv1, "", false)
	}()
	go func() {
		defer wg.Done()
		s.tryDeleteVersion(ctx, tv1, "", false)
	}()
	wg.Wait()

	// deployment version does not exist in the deployment list
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv1.DeploymentSeries(),
		})
		a.NoError(err)
		if resp != nil {
			for _, vs := range resp.GetWorkerDeploymentInfo().GetVersionSummaries() {
				a.NotEqual(tv1.DeploymentVersionString(), vs.Version) //nolint:staticcheck // SA1019: worker versioning v0.31
				a.NotEqual(tv1.ExternalDeploymentVersion().GetDeploymentName(), vs.GetDeploymentVersion().GetDeploymentName())
				a.NotEqual(tv1.ExternalDeploymentVersion().GetBuildId(), vs.GetDeploymentVersion().GetBuildId())
			}
		}
	}, time.Second*10, time.Millisecond*200)
}

// VersionMissingTaskQueues
func (s *DeploymentVersionSuite) TestVersionMissingTaskQueues_InvalidSetCurrentVersion() {
	// Override the dynamic config to verify we don't get any unexpected masked errors.
	s.OverrideDynamicConfig(dynamicconfig.FrontendMaskInternalErrorDetails, true)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tv := testvars.New(s)
	tv1 := tv.WithBuildIDNumber(1).WithTaskQueue(tv.Any().String())

	// Start deployment workflow 1 and wait for the deployment version to exist
	pollerCtx1, pollerCancel1 := context.WithCancel(ctx)
	s.startVersionWorkflow(pollerCtx1, tv1)

	// SetCurrent so that the task queue puts the version in its versions info
	err := s.setCurrent(tv1, false)
	s.Nil(err)

	// new version with a different registered task-queue
	tv2 := testvars.New(s).WithBuildIDNumber(2).WithTaskQueue(testvars.New(s.T()).Any().String())
	s.startVersionWorkflow(ctx, tv2)

	// Cancel pollers on task_queue_1 to increase the backlog of tasks
	pollerCancel1()

	// Start a workflow on task_queue_1 to increase the add rate
	s.startWorkflow(tv1, tv1.VersioningOverridePinned(s.useV32))

	// SetCurrent tv2
	err = s.setCurrent(tv2, false)

	// SetCurrent should fail since task_queue_1 does not have a current version than the deployment's existing current version
	// and it either has a backlog of tasks being present or an add rate > 0.
	s.EqualError(err, workerdeployment.ErrCurrentVersionDoesNotHaveAllTaskQueues)
}

func (s *DeploymentVersionSuite) TestVersionMissingTaskQueues_ValidSetCurrentVersion() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tv := testvars.New(s)

	tv1 := tv.WithBuildIDNumber(1).WithTaskQueue(tv.Any().String())
	s.startVersionWorkflow(ctx, tv1)

	// SetCurrent so that the task queue puts the version in its versions info
	err := s.setCurrent(tv1, false)
	s.Nil(err)

	// new version with a different registered task-queue
	tv2 := tv.WithBuildIDNumber(2).WithTaskQueue(tv.Any().String())
	s.startVersionWorkflow(ctx, tv2)

	// SetCurrent tv2
	err = s.setCurrent(tv2, false)

	// SetCurrent tv2 should succeed as task_queue_1, despite missing from the new current version, has no backlogged tasks/add-rate > 0
	s.Nil(err)
}

func (s *DeploymentVersionSuite) TestVersionMissingTaskQueues_InvalidSetRampingVersion() {
	// Override the dynamic config to verify we don't get any unexpected masked errors.
	s.OverrideDynamicConfig(dynamicconfig.FrontendMaskInternalErrorDetails, true)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tv := testvars.New(s)
	tv1 := tv.WithBuildIDNumber(1).WithTaskQueue(tv.Any().String())

	// Start deployment workflow 1 and wait for the deployment version to exist
	pollerCtx1, pollerCancel1 := context.WithCancel(ctx)
	s.startVersionWorkflow(pollerCtx1, tv1)

	// SetCurrent so that the task queue puts the version in its versions info
	err := s.setCurrent(tv1, false)
	s.Nil(err)

	// new version with a different registered task-queue
	tv2 := tv.WithBuildIDNumber(2).WithTaskQueue(tv.Any().String())
	s.startVersionWorkflow(ctx, tv2)

	// Cancel pollers on task_queue_1 to increase the backlog of tasks
	pollerCancel1()

	// Start a workflow on task_queue_1 to increase the add rate
	s.startWorkflow(tv1, tv1.VersioningOverridePinned(s.useV32))

	// SetRampingVersion to tv2
	err = s.setRamping(tv2, 0)

	// SetRampingVersion should fail since task_queue_1 does not have a current version than the deployment's existing current version
	// and it either has a backlog of tasks being present or an add rate > 0.
	s.EqualError(err, workerdeployment.ErrRampingVersionDoesNotHaveAllTaskQueues)
}

func (s *DeploymentVersionSuite) TestVersionMissingTaskQueues_ValidSetRampingVersion() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tv := testvars.New(s)
	tv1 := tv.WithBuildIDNumber(1).WithTaskQueue(tv.Any().String())

	// Start deployment workflow 1 and wait for the deployment version to exist
	s.startVersionWorkflow(ctx, tv1)

	// SetCurrent so that the task queue puts the version in its versions info
	err := s.setCurrent(tv1, false)
	s.Nil(err)

	// new version with a different registered task-queue
	tv2 := tv.WithBuildIDNumber(2).WithTaskQueue(tv.Any().String())
	s.startVersionWorkflow(ctx, tv2)

	// SetRampingVersion to tv2
	err = s.setRamping(tv2, 0)

	// SetRampingVersion to tv2 should succeed as task_queue_1, despite missing from the new current version, has no backlogged tasks/add-rate > 0
	s.Nil(err)
}

func (s *DeploymentVersionSuite) TestUpdateVersionMetadata() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tv1 := testvars.New(s).WithBuildIDNumber(1)

	// Start deployment workflow 1 and wait for the deployment version to exist
	s.startVersionWorkflow(ctx, tv1)

	metadata := map[string]*commonpb.Payload{
		"key1": {Data: testRandomMetadataValue},
		"key2": {Data: testRandomMetadataValue},
	}
	_, err := s.updateMetadata(tv1, metadata, nil)
	s.NoError(err)

	resp, err := s.describeVersion(tv1)
	s.NoError(err)

	// validating the metadata
	entries := resp.GetWorkerDeploymentVersionInfo().GetMetadata().GetEntries()
	s.Equal(2, len(entries))
	s.Equal(testRandomMetadataValue, entries["key1"].Data)
	s.Equal(testRandomMetadataValue, entries["key2"].Data)

	// Remove all the entries
	_, err = s.updateMetadata(tv1, nil, []string{"key1", "key2"})
	s.NoError(err)

	resp, err = s.describeVersion(tv1)
	s.NoError(err)
	entries = resp.GetWorkerDeploymentVersionInfo().GetMetadata().GetEntries()
	s.Equal(0, len(entries))
}

func (s *DeploymentVersionSuite) checkVersionDrainageAndVersionStatus(
	ctx context.Context,
	tv *testvars.TestVars,
	expectedDrainageInfo *deploymentpb.VersionDrainageInfo,
	expectedStatus enumspb.WorkerDeploymentVersionStatus,
	addGracePeriod, addRefreshInterval bool,
) (changedTime, checkedTime time.Time) {
	var waitFor time.Duration
	if addGracePeriod {
		waitFor += testVersionDrainageVisibilityGracePeriod
	}
	if addRefreshInterval {
		waitFor += testVersionDrainageRefreshInterval
	}
	if waitFor > 0 {
		// wait for the requested duration before looking at the result ( +1 sec for system latency)
		time.Sleep(waitFor + 1*time.Second) //nolint:forbidigo
	}

	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		resp, err := s.describeVersion(tv)
		a.NoError(err)
		dInfo := resp.GetWorkerDeploymentVersionInfo().GetDrainageInfo()
		a.Equal(expectedDrainageInfo.Status, dInfo.GetStatus())
		if expectedDrainageInfo.LastCheckedTime != nil {
			a.Equal(expectedDrainageInfo.LastCheckedTime, dInfo.GetLastCheckedTime())
		}
		if expectedDrainageInfo.LastChangedTime != nil {
			a.Equal(expectedDrainageInfo.LastChangedTime, dInfo.GetLastChangedTime())
		}
		a.Equal(expectedStatus, resp.GetWorkerDeploymentVersionInfo().GetStatus())
		changedTime = dInfo.GetLastChangedTime().AsTime()
		checkedTime = dInfo.GetLastCheckedTime().AsTime()
	}, 5*time.Second, time.Millisecond*100)
	return changedTime, checkedTime
}

func (s *DeploymentVersionSuite) checkDescribeWorkflowAfterOverride(
	ctx context.Context,
	wf *commonpb.WorkflowExecution,
	expectedOverride *workflowpb.VersioningOverride,
) {
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: wf,
		})
		a.NoError(err)
		a.NotNil(resp)
		a.NotNil(resp.GetWorkflowExecutionInfo())
		actualOverride := resp.GetWorkflowExecutionInfo().GetVersioningInfo().GetVersioningOverride()

		if s.useV32 {
			// v0.32 override
			a.Equal(expectedOverride.GetAutoUpgrade(), actualOverride.GetAutoUpgrade())
			a.Equal(expectedOverride.GetPinned().GetVersion().GetBuildId(), actualOverride.GetPinned().GetVersion().GetBuildId())
			a.Equal(expectedOverride.GetPinned().GetVersion().GetDeploymentName(), actualOverride.GetPinned().GetVersion().GetDeploymentName())
			a.Equal(expectedOverride.GetPinned().GetBehavior(), actualOverride.GetPinned().GetBehavior())
			if worker_versioning.OverrideIsPinned(expectedOverride) {
				a.Equal(expectedOverride.GetPinned().GetVersion().GetDeploymentName(), resp.GetWorkflowExecutionInfo().GetWorkerDeploymentName())
			}
		} else {
			// v0.31 override
			a.Equal(expectedOverride.GetBehavior().String(), actualOverride.GetBehavior().String())                                             //nolint:staticcheck // SA1019: worker versioning v0.31
			if actualOverrideDeployment := actualOverride.GetPinnedVersion(); expectedOverride.GetPinnedVersion() != actualOverrideDeployment { //nolint:staticcheck // SA1019: worker versioning v0.31
				a.Fail(fmt.Sprintf("pinned override mismatch. expected: {%s}, actual: {%s}",
					expectedOverride.GetPinnedVersion(), //nolint:staticcheck // SA1019: worker versioning v0.31
					actualOverrideDeployment,
				))
			}
			if worker_versioning.OverrideIsPinned(expectedOverride) {
				d, _ := worker_versioning.WorkerDeploymentVersionFromStringV31(expectedOverride.GetPinnedVersion()) //nolint:staticcheck // SA1019: worker versioning v0.31
				a.Equal(d.GetDeploymentName(), resp.GetWorkflowExecutionInfo().GetWorkerDeploymentName())
			}
		}
	}, 10*time.Second, 50*time.Millisecond)
}

func (s *DeploymentVersionSuite) checkVersionIsCurrent(ctx context.Context, tv *testvars.TestVars) {
	// Querying the Deployment Version
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		resp, err := s.describeVersion(tv)
		if !a.NoError(err) {
			return
		}
		a.Equal(tv.DeploymentVersionString(), resp.GetWorkerDeploymentVersionInfo().GetVersion()) //nolint:staticcheck // SA1019: worker versioning v0.31
		a.Equal(tv.ExternalDeploymentVersion().GetDeploymentName(), resp.GetWorkerDeploymentVersionInfo().GetDeploymentVersion().GetDeploymentName())
		a.Equal(tv.ExternalDeploymentVersion().GetBuildId(), resp.GetWorkerDeploymentVersionInfo().GetDeploymentVersion().GetBuildId())

		a.NotNil(resp.GetWorkerDeploymentVersionInfo().GetCurrentSinceTime())
		a.Equal(enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT, resp.GetWorkerDeploymentVersionInfo().GetStatus())
	}, time.Second*10, time.Millisecond*1000)
}

func (s *DeploymentVersionSuite) checkVersionIsRamping(ctx context.Context, tv *testvars.TestVars) {
	// Querying the Deployment Version
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		resp, err := s.describeVersion(tv)
		if !a.NoError(err) {
			return
		}
		a.Equal(tv.DeploymentVersionString(), resp.GetWorkerDeploymentVersionInfo().GetVersion()) //nolint:staticcheck // SA1019: worker versioning v0.31
		a.Equal(tv.ExternalDeploymentVersion().GetDeploymentName(), resp.GetWorkerDeploymentVersionInfo().GetDeploymentVersion().GetDeploymentName())
		a.Equal(tv.ExternalDeploymentVersion().GetBuildId(), resp.GetWorkerDeploymentVersionInfo().GetDeploymentVersion().GetBuildId())

		a.NotNil(resp.GetWorkerDeploymentVersionInfo().GetRampingSinceTime())
		a.Equal(enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING, resp.GetWorkerDeploymentVersionInfo().GetStatus())
	}, time.Second*10, time.Millisecond*1000)
}

func (s *DeploymentVersionSuite) setCurrent(tv *testvars.TestVars, ignoreMissingTQs bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req := &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
		Namespace:               s.Namespace().String(),
		DeploymentName:          tv.DeploymentSeries(),
		IgnoreMissingTaskQueues: ignoreMissingTQs,
		Identity:                tv.ClientIdentity(),
	}
	if s.useV32 {
		req.BuildId = tv.BuildID()
	} else {
		req.Version = tv.DeploymentVersionString() //nolint:staticcheck // SA1019: worker versioning v0.31
	}
	_, err := s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, req)
	if err == nil {
		s.checkVersionIsCurrent(ctx, tv)
	}
	return err
}

func (s *DeploymentVersionSuite) setRamping(
	tv *testvars.TestVars,
	percentage float32,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	v := tv.DeploymentVersionString()
	bid := tv.BuildID()
	req := &workflowservice.SetWorkerDeploymentRampingVersionRequest{
		Namespace:      s.Namespace().String(),
		DeploymentName: tv.DeploymentSeries(),
		Percentage:     percentage,
		Identity:       tv.ClientIdentity(),
	}
	if s.useV32 {
		req.BuildId = bid
	} else {
		req.Version = v //nolint:staticcheck // SA1019: worker versioning v0.31
	}
	_, err := s.FrontendClient().SetWorkerDeploymentRampingVersion(ctx, req)
	if err == nil {
		s.checkVersionIsRamping(ctx, tv)
	}
	return err
}

func (s *DeploymentVersionSuite) startWorkflow(
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

func (s *DeploymentVersionSuite) tryDeleteVersion(
	ctx context.Context,
	tv *testvars.TestVars,
	expectedError string,
	skipDrainage bool,
) {
	req := &workflowservice.DeleteWorkerDeploymentVersionRequest{
		Namespace:    s.Namespace().String(),
		SkipDrainage: skipDrainage,
	}
	if s.useV32 {
		req.DeploymentVersion = tv.ExternalDeploymentVersion()
	} else {
		req.Version = tv.DeploymentVersionString() //nolint:staticcheck // SA1019: worker versioning v0.31
	}
	_, err := s.FrontendClient().DeleteWorkerDeploymentVersion(ctx, req)
	if expectedError == "" {
		s.Nil(err)
	} else {
		s.EqualErrorf(err, expectedError, err.Error())
	}
}

func (s *DeploymentVersionSuite) setAndCheckOverride(ctx context.Context, tv *testvars.TestVars, override *workflowpb.VersioningOverride) {
	opts := &workflowpb.WorkflowExecutionOptions{VersioningOverride: override}
	// 1. Set unpinned override --> describe workflow shows the override
	updateResp, err := s.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                s.Namespace().String(),
		WorkflowExecution:        tv.WorkflowExecution(),
		WorkflowExecutionOptions: opts,
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
	})
	s.NoError(err)
	s.True(proto.Equal(updateResp.GetWorkflowExecutionOptions(), opts))
	s.checkDescribeWorkflowAfterOverride(ctx, tv.WorkflowExecution(), override)
}

func (s *DeploymentVersionSuite) TestUpdateWorkflowExecutionOptions_SetUnpinnedThenUnset() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tv := testvars.New(s)

	// start an unversioned workflow
	s.startWorkflow(tv, nil)

	// 1. Set unpinned override --> describe workflow shows the override
	s.setAndCheckOverride(ctx, tv, s.makeAutoUpgradeOverride())

	// 2. Unset using empty update opts with mutation mask --> describe workflow shows no more override
	s.setAndCheckOverride(ctx, tv, nil)
}

func (s *DeploymentVersionSuite) TestUpdateWorkflowExecutionOptions_SetPinnedThenUnset() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tv := testvars.New(s)

	// start an unversioned workflow
	s.startWorkflow(tv, nil)

	// 1. Set pinned override on our new unversioned workflow --> describe workflow shows the override
	s.setAndCheckOverride(ctx, tv, s.makePinnedOverride(tv))

	// 2. Unset using empty update opts with mutation mask --> describe workflow shows no more override
	s.setAndCheckOverride(ctx, tv, nil)
}

func (s *DeploymentVersionSuite) TestUpdateWorkflowExecutionOptions_EmptyFields() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tv := testvars.New(s)

	// start an unversioned workflow
	s.startWorkflow(tv, nil)

	// 1. Pinned update with empty mask --> describe workflow shows no change
	updateResp, err := s.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: tv.WorkflowExecution(),
		WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{
			VersioningOverride: s.makePinnedOverride(tv),
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{}},
	})
	s.NoError(err)
	s.True(proto.Equal(updateResp.GetWorkflowExecutionOptions(), &workflowpb.WorkflowExecutionOptions{}))
	s.checkDescribeWorkflowAfterOverride(ctx, tv.WorkflowExecution(), nil)
}

func (s *DeploymentVersionSuite) TestUpdateWorkflowExecutionOptions_SetPinnedSetPinned() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tv := testvars.New(s)
	tv1 := tv.WithBuildIDNumber(1)
	tv2 := tv.WithBuildIDNumber(2)

	// start an unversioned workflow
	s.startWorkflow(tv, nil)

	// 1. Set pinned override 1 --> describe workflow shows the override
	s.setAndCheckOverride(ctx, tv, s.makePinnedOverride(tv1))

	// 3. Set pinned override 2 --> describe workflow shows the override
	s.setAndCheckOverride(ctx, tv, s.makePinnedOverride(tv2))
}

func (s *DeploymentVersionSuite) TestUpdateWorkflowExecutionOptions_SetUnpinnedSetUnpinned() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tv := testvars.New(s)

	// start an unversioned workflow
	s.startWorkflow(tv, nil)

	// 1. Set unpinned override --> describe workflow shows the override
	s.setAndCheckOverride(ctx, tv, s.makeAutoUpgradeOverride())

	// 2. Set unpinned override --> describe workflow shows the override
	s.setAndCheckOverride(ctx, tv, s.makeAutoUpgradeOverride())
}

func (s *DeploymentVersionSuite) TestUpdateWorkflowExecutionOptions_SetUnpinnedSetPinned() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tv := testvars.New(s)

	// start an unversioned workflow
	s.startWorkflow(tv, nil)

	// 1. Set unpinned override --> describe workflow shows the override
	s.setAndCheckOverride(ctx, tv, s.makeAutoUpgradeOverride())

	// 2. Set pinned override 1 --> describe workflow shows the override
	s.setAndCheckOverride(ctx, tv, s.makePinnedOverride(tv))
}

func (s *DeploymentVersionSuite) TestUpdateWorkflowExecutionOptions_SetPinnedSetUnpinned() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tv := testvars.New(s)

	// start an unversioned workflow
	s.startWorkflow(tv, nil)

	// 1. Set pinned override 1 --> describe workflow shows the override
	s.setAndCheckOverride(ctx, tv, s.makePinnedOverride(tv))

	// 2. Set unpinned override --> describe workflow shows the override
	s.setAndCheckOverride(ctx, tv, s.makeAutoUpgradeOverride())
}

func (s *DeploymentVersionSuite) TestBatchUpdateWorkflowExecutionOptions_SetPinnedThenUnset() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	tv := testvars.New(s)

	// start some unversioned workflows
	workflowType := "UpdateOptionsBatchTestFunc"
	workflows := make([]*commonpb.WorkflowExecution, 0)
	for i := 0; i < 5; i++ {
		run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tv.TaskQueue().Name}, workflowType)
		s.NoError(err)
		workflows = append(workflows, &commonpb.WorkflowExecution{
			WorkflowId: run.GetID(),
			RunId:      run.GetRunID(),
		})
	}

	// start batch update-options operation
	pinnedOverride := s.makePinnedOverride(tv)
	batchJobId := uuid.New()

	// unpause the activities in both workflows with batch unpause
	_, err := s.SdkClient().WorkflowService().StartBatchOperation(context.Background(), &workflowservice.StartBatchOperationRequest{
		Namespace: s.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_UpdateWorkflowOptionsOperation{
			UpdateWorkflowOptionsOperation: &batchpb.BatchOperationUpdateWorkflowExecutionOptions{
				Identity:                 uuid.New(),
				WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{VersioningOverride: pinnedOverride},
				UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
			},
		},
		Executions: workflows,
		JobId:      batchJobId,
		Reason:     "test",
	})
	s.NoError(err)

	// wait til batch completes
	s.checkListAndWaitForBatchCompletion(ctx, batchJobId)

	// check all the workflows
	for _, wf := range workflows {
		s.checkDescribeWorkflowAfterOverride(ctx, wf, pinnedOverride)
	}

	// unset with empty update opts with mutation mask
	batchJobId = uuid.New()
	err = s.startBatchJobWithinConcurrentJobLimit(ctx, &workflowservice.StartBatchOperationRequest{
		Namespace:  s.Namespace().String(),
		JobId:      batchJobId,
		Reason:     "test",
		Executions: workflows,
		Operation: &workflowservice.StartBatchOperationRequest_UpdateWorkflowOptionsOperation{
			UpdateWorkflowOptionsOperation: &batchpb.BatchOperationUpdateWorkflowExecutionOptions{
				Identity:                 uuid.New(),
				WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{},
				UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
			},
		},
	})
	s.NoError(err)

	// wait til batch completes
	s.checkListAndWaitForBatchCompletion(ctx, batchJobId)

	// check all the workflows
	for _, wf := range workflows {
		s.checkDescribeWorkflowAfterOverride(ctx, wf, nil)
	}
}

func (s *DeploymentVersionSuite) startBatchJobWithinConcurrentJobLimit(ctx context.Context, req *workflowservice.StartBatchOperationRequest) error {
	var err error
	s.Eventually(func() bool {
		_, err = s.FrontendClient().StartBatchOperation(ctx, req)
		if err == nil {
			return true
		} else if strings.Contains(err.Error(), "Max concurrent batch operations is reached") {
			return false // retry
		}
		return true
	}, 5*time.Second, 500*time.Millisecond)
	return err
}

func (s *DeploymentVersionSuite) checkListAndWaitForBatchCompletion(ctx context.Context, jobId string) {
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		listResp, err := s.FrontendClient().ListBatchOperations(ctx, &workflowservice.ListBatchOperationsRequest{
			Namespace: s.Namespace().String(),
		})
		a.NoError(err)
		a.Greater(len(listResp.GetOperationInfo()), 0)
		if len(listResp.GetOperationInfo()) > 0 {
			a.Equal(jobId, listResp.GetOperationInfo()[0].GetJobId())
		}
	}, 10*time.Second, 50*time.Millisecond)

	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		descResp, err := s.FrontendClient().DescribeBatchOperation(ctx, &workflowservice.DescribeBatchOperationRequest{
			Namespace: s.Namespace().String(),
			JobId:     jobId,
		})
		a.NoError(err)
		a.NotEqual(enumspb.BATCH_OPERATION_STATE_FAILED, descResp.GetState(), fmt.Sprintf("batch operation failed. description: %+v", descResp))
		a.Equal(enumspb.BATCH_OPERATION_STATE_COMPLETED, descResp.GetState())
	}, 10*time.Second, 50*time.Millisecond)
}

func (s *DeploymentVersionSuite) makePinnedOverride(tv *testvars.TestVars) *workflowpb.VersioningOverride {
	if s.useV32 {
		return &workflowpb.VersioningOverride{Override: &workflowpb.VersioningOverride_Pinned{
			Pinned: &workflowpb.VersioningOverride_PinnedOverride{
				Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
				Version:  tv.ExternalDeploymentVersion(),
			},
		}}
	}
	return &workflowpb.VersioningOverride{
		PinnedVersion: tv.DeploymentVersionString(),       //nolint:staticcheck // SA1019: worker versioning v0.31
		Behavior:      enumspb.VERSIONING_BEHAVIOR_PINNED, //nolint:staticcheck // SA1019: worker versioning v0.31
	}
}

func (s *DeploymentVersionSuite) makeAutoUpgradeOverride() *workflowpb.VersioningOverride {
	if s.useV32 {
		return &workflowpb.VersioningOverride{Override: &workflowpb.VersioningOverride_AutoUpgrade{AutoUpgrade: true}}
	}
	return &workflowpb.VersioningOverride{Behavior: enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE} //nolint:staticcheck // SA1019: worker versioning v0.31
}

func (s *DeploymentVersionSuite) TestStartWorkflowExecution_WithPinnedOverride() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tv := testvars.New(s)

	override := s.makePinnedOverride(tv)
	wf := &commonpb.WorkflowExecution{
		WorkflowId: tv.WorkflowID(),
		RunId:      s.startWorkflow(tv, override),
	}
	s.checkDescribeWorkflowAfterOverride(ctx, wf, override)
}

func (s *DeploymentVersionSuite) TestStartWorkflowExecution_WithUnpinnedOverride() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tv := testvars.New(s)

	override := s.makeAutoUpgradeOverride()
	wf := &commonpb.WorkflowExecution{
		WorkflowId: tv.WorkflowID(),
		RunId:      s.startWorkflow(tv, override),
	}
	s.checkDescribeWorkflowAfterOverride(ctx, wf, override)
}

func (s *DeploymentVersionSuite) TestSignalWithStartWorkflowExecution_WithPinnedOverride() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tv := testvars.New(s)

	override := s.makePinnedOverride(tv)

	resp, err := s.FrontendClient().SignalWithStartWorkflowExecution(ctx, &workflowservice.SignalWithStartWorkflowExecutionRequest{
		Namespace:          s.Namespace().String(),
		WorkflowId:         tv.WorkflowID(),
		WorkflowType:       tv.WorkflowType(),
		TaskQueue:          tv.TaskQueue(),
		Identity:           tv.ClientIdentity(),
		RequestId:          tv.RequestID(),
		SignalName:         "test-signal",
		SignalInput:        nil,
		VersioningOverride: override,
	})
	s.NoError(err)
	s.True(resp.GetStarted())

	wf := &commonpb.WorkflowExecution{
		WorkflowId: tv.WorkflowID(),
		RunId:      resp.GetRunId(),
	}
	s.checkDescribeWorkflowAfterOverride(ctx, wf, override)
}

func (s *DeploymentVersionSuite) TestSignalWithStartWorkflowExecution_WithUnpinnedOverride() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tv := testvars.New(s)

	override := s.makeAutoUpgradeOverride()

	resp, err := s.FrontendClient().SignalWithStartWorkflowExecution(ctx, &workflowservice.SignalWithStartWorkflowExecutionRequest{
		Namespace:          s.Namespace().String(),
		WorkflowId:         tv.WorkflowID(),
		WorkflowType:       tv.WorkflowType(),
		TaskQueue:          tv.TaskQueue(),
		Identity:           tv.ClientIdentity(),
		RequestId:          tv.RequestID(),
		SignalName:         "test-signal",
		SignalInput:        nil,
		VersioningOverride: override,
	})
	s.NoError(err)
	s.True(resp.GetStarted())

	wf := &commonpb.WorkflowExecution{
		WorkflowId: tv.WorkflowID(),
		RunId:      resp.GetRunId(),
	}
	s.checkDescribeWorkflowAfterOverride(ctx, wf, override)
}
