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
	"github.com/google/uuid"
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
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/testhooks"
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
	maxConcurrentBatchOperations                 = 3
	testVersionDrainageRefreshInterval           = 3 * time.Second
	testVersionDrainageVisibilityGracePeriod     = 3 * time.Second
	testLongVersionDrainageRefreshInterval       = 10 * time.Second
	testLongVersionDrainageVisibilityGracePeriod = 10 * time.Second
	testExtraLongVersionDrainageRefreshInterval  = 30 * time.Second
	testVersionMembershipCacheTTL                = 5 * time.Second
	testLongVersionReactivationCacheTTL          = 5 * time.Minute
	testMaxVersionsInDeployment                  = 4
)

type (
	DeploymentVersionSuite struct {
		// TODO: this is always true. cleanup code
		useV32 bool
		testcore.FunctionalTestBase
		workflowVersion workerdeployment.DeploymentWorkflowVersion
	}
)

var (
	testRandomMetadataValue = []byte("random metadata value")
)

func TestDeploymentVersionSuiteV0(t *testing.T) {
	t.Parallel()
	suite.Run(t, &DeploymentVersionSuite{workflowVersion: workerdeployment.InitialVersion, useV32: true})
}

func TestDeploymentVersionSuiteV2(t *testing.T) {
	t.Parallel()
	suite.Run(t, &DeploymentVersionSuite{workflowVersion: workerdeployment.VersionDataRevisionNumber, useV32: true})
}

func (s *DeploymentVersionSuite) SetupSuite() {
	s.FunctionalTestBase.SetupSuiteWithCluster(testcore.WithDynamicConfigOverrides(map[dynamicconfig.Key]any{
		dynamicconfig.MatchingDeploymentWorkflowVersion.Key(): int(s.workflowVersion),

		// Make sure we don't hit the rate limiter in tests
		dynamicconfig.FrontendGlobalNamespaceNamespaceReplicationInducingAPIsRPS.Key():                1000,
		dynamicconfig.FrontendMaxNamespaceNamespaceReplicationInducingAPIsBurstRatioPerInstance.Key(): 1,
		dynamicconfig.FrontendNamespaceReplicationInducingAPIsRPS.Key():                               1000,
		dynamicconfig.MatchingNumTaskqueueReadPartitions.Key():                                        1,
		dynamicconfig.MatchingNumTaskqueueWritePartitions.Key():                                       1,

		// Reduce the chance of hitting max batch job limit in tests
		dynamicconfig.FrontendMaxConcurrentBatchOperationPerNamespace.Key(): maxConcurrentBatchOperations,

		dynamicconfig.VersionDrainageStatusRefreshInterval.Key():       testVersionDrainageRefreshInterval,
		dynamicconfig.VersionDrainageStatusVisibilityGracePeriod.Key(): testVersionDrainageVisibilityGracePeriod,
		dynamicconfig.VersionMembershipCacheTTL.Key():                  testVersionMembershipCacheTTL,

		// Large TTL for deduplication test. Must be set at suite level for cache initialization to work.
		dynamicconfig.VersionReactivationSignalCacheTTL.Key(): testLongVersionReactivationCacheTTL,
	}))
}

// pollFromDeployment calls PollWorkflowTaskQueue to start deployment related workflows
func (s *DeploymentVersionSuite) pollFromDeployment(ctx context.Context, tv *testvars.TestVars) {
	_, _ = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace:         s.Namespace().String(),
		TaskQueue:         tv.TaskQueue(),
		Identity:          uuid.NewString(),
		DeploymentOptions: tv.WorkerDeploymentOptions(true),
	})
}

func (s *DeploymentVersionSuite) pollActivityFromDeployment(ctx context.Context, tv *testvars.TestVars) {
	_, _ = s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace:         s.Namespace().String(),
		TaskQueue:         tv.TaskQueue(),
		Identity:          uuid.NewString(),
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
	versionWorkflowID := workerdeployment.GenerateVersionWorkflowID(tv.DeploymentSeries(), tv.BuildID())
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

func (s *DeploymentVersionSuite) TestForceCAN_WithOverrideState() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	tv := testvars.New(s)

	// Start a version workflow
	s.startVersionWorkflow(ctx, tv)

	// Create a modified state with metadata to verify override works
	overrideState := &deploymentspb.VersionLocalState{
		Version:    tv.DeploymentVersion(),
		CreateTime: timestamppb.New(time.Now()),
		Status:     enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE,
		Metadata: &deploymentpb.VersionMetadata{
			Entries: map[string]*commonpb.Payload{
				"override-key": {Data: []byte("override-value")},
			},
		},
		TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
			tv.TaskQueue().GetName(): {
				TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{
					int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {},
				},
			},
		},
	}

	// Create signal args with the override state
	signalArgs := &deploymentspb.ForceCANVersionSignalArgs{
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
	versionWorkflowID := workerdeployment.GenerateVersionWorkflowID(tv.DeploymentSeries(), tv.BuildID())
	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: versionWorkflowID,
	}

	err = s.SendSignal(s.Namespace().String(), workflowExecution, workerdeployment.ForceCANSignalName, signalPayload, tv.ClientIdentity())
	s.NoError(err)

	// Verify that the override state is used after CAN (metadata should be present)
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)

		resp, err := s.describeVersion(tv)
		if !a.NoError(err) {
			return
		}

		// Verify the metadata from override state is present
		entries := resp.GetWorkerDeploymentVersionInfo().GetMetadata().GetEntries()
		a.Len(entries, 1)
		a.Equal([]byte("override-value"), entries["override-key"].Data)
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
	for p := range 4 {
		tv2 := tv.WithTaskQueue(root.TaskQueue().NormalPartition(p).RpcName())
		for range 3 {
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
	s.checkVersionDrainageAndVersionStatus(ctx, tv1, &deploymentpb.VersionDrainageInfo{}, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE, 0)
	s.checkVersionDrainageAndVersionStatus(ctx, tv2, &deploymentpb.VersionDrainageInfo{}, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE, 0)

	// SetCurrent tv1
	err := s.setCurrent(tv1, true)
	s.Nil(err)

	// Both versions have no drainage info and tv1 has it's status updated to current
	s.checkVersionDrainageAndVersionStatus(ctx, tv1, &deploymentpb.VersionDrainageInfo{}, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT, 0)
	s.checkVersionDrainageAndVersionStatus(ctx, tv2, &deploymentpb.VersionDrainageInfo{}, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE, 0)

	baseTime := time.Now()
	// SetCurrent tv2 --> tv1 starts the child drainage workflow
	err = s.setCurrent(tv2, true)
	s.Nil(err)

	changed1, checked1 := s.checkVersionDrainageAndVersionStatus(ctx, tv1, &deploymentpb.VersionDrainageInfo{
		Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINING,
	}, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING, 0)
	s.Greater(changed1, baseTime)
	s.GreaterOrEqual(checked1, changed1)

	// tv1 should now be "drained"
	changed2, checked2 := s.checkVersionDrainageAndVersionStatus(ctx, tv1, &deploymentpb.VersionDrainageInfo{
		Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINED,
	}, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED, testVersionDrainageVisibilityGracePeriod)
	s.Greater(changed2, changed1)
	s.GreaterOrEqual(checked2, changed2)
}

func (s *DeploymentVersionSuite) TestDrainageStatus_SetCurrentVersion_YesOpenWFs() {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	tv1 := testvars.New(s).WithBuildIDNumber(1)
	tv2 := testvars.New(s).WithBuildIDNumber(2)

	// start deployment workflow 1 and wait for the deployment version to exist
	s.startVersionWorkflow(ctx, tv1)

	// start deployment workflow 2 and wait for the deployment version to exist
	s.startVersionWorkflow(ctx, tv2)

	// non-current deployments have never been used and have no drainage info
	s.checkVersionDrainageAndVersionStatus(ctx, tv1, &deploymentpb.VersionDrainageInfo{}, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE, 0)
	s.checkVersionDrainageAndVersionStatus(ctx, tv2, &deploymentpb.VersionDrainageInfo{}, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE, 0)

	// SetCurrent tv1
	err := s.setCurrent(tv1, true)
	s.Nil(err)

	// both versions have no drainage info and tv1 has it's status updated to current
	s.checkVersionDrainageAndVersionStatus(ctx, tv1, &deploymentpb.VersionDrainageInfo{}, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT, 0)
	s.checkVersionDrainageAndVersionStatus(ctx, tv2, &deploymentpb.VersionDrainageInfo{}, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE, 0)

	// start a pinned workflow on v1
	run := s.startPinnedWorkflow(ctx, tv1)

	baseTime := time.Now()
	// SetCurrent tv2 --> tv1 starts the child drainage workflow
	err = s.setCurrent(tv2, true)
	s.Nil(err)

	// tv1 should now be "draining" for visibilityGracePeriod duration
	changed1, checked1 := s.checkVersionDrainageAndVersionStatus(ctx, tv1, &deploymentpb.VersionDrainageInfo{
		Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINING,
	}, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING, 0)
	s.Greater(changed1, baseTime)
	s.GreaterOrEqual(checked1, changed1)

	// tv1 should still be "draining" for visibilityGracePeriod duration
	changed2, checked2 := s.checkVersionDrainageAndVersionStatus(ctx, tv1, &deploymentpb.VersionDrainageInfo{
		Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINING,
	}, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING, testVersionDrainageVisibilityGracePeriod)
	s.Equal(changed2, changed1)
	s.Greater(checked2, checked1)

	// tv1 should still be "draining" after a refresh intervals
	changed3, checked3 := s.checkVersionDrainageAndVersionStatus(ctx, tv1, &deploymentpb.VersionDrainageInfo{
		Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINING,
	}, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING, testVersionDrainageRefreshInterval)
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
	}, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED, 0)
	s.Greater(changed4, changed3)
	s.GreaterOrEqual(checked4, changed4)
}

func (s *DeploymentVersionSuite) startVersionedWorkflow(ctx context.Context, tv *testvars.TestVars, behavior workflow.VersioningBehavior) sdkclient.WorkflowRun {
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
			Version:       tv.SDKDeploymentVersion(),
			UseVersioning: true,
		},
		Identity: wId,
	})
	w.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{VersioningBehavior: behavior})
	s.NoError(w.Start())
	defer w.Stop()
	run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tv.TaskQueue().String()}, wf)
	s.NoError(err)
	s.WaitForChannel(ctx, started)
	return run
}

func (s *DeploymentVersionSuite) startPinnedWorkflow(ctx context.Context, tv *testvars.TestVars) sdkclient.WorkflowRun {
	return s.startVersionedWorkflow(ctx, tv, workflow.VersioningBehaviorPinned)
}

func (s *DeploymentVersionSuite) startUnpinnedWorkflow(ctx context.Context, tv *testvars.TestVars) sdkclient.WorkflowRun {
	return s.startVersionedWorkflow(ctx, tv, workflow.VersioningBehaviorAutoUpgrade)
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
	s.tryDeleteVersion(ctx, tv1, fmt.Sprintf(workerdeployment.ErrVersionIsCurrentOrRamping, tv1.DeploymentVersionStringV32()), false)

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
	s.tryDeleteVersion(ctx, tv1, fmt.Sprintf(workerdeployment.ErrVersionIsCurrentOrRamping, tv1.DeploymentVersionStringV32()), false)

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
	}, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING, 0)

	// delete should fail
	s.tryDeleteVersion(ctx, tv1, fmt.Sprintf(workerdeployment.ErrVersionIsDraining, tv1.DeploymentVersionStringV32()), false)

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
	s.tryDeleteVersion(ctx, tv1, fmt.Sprintf(workerdeployment.ErrVersionHasPollers, tv1.DeploymentVersionStringV32()), false)
}

func (s *DeploymentVersionSuite) signalAndWaitForDrained(ctx context.Context, tv *testvars.TestVars) {
	versionWorkflowID := workerdeployment.GenerateVersionWorkflowID(tv.DeploymentSeries(), tv.BuildID())
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

func (s *DeploymentVersionSuite) waitForPollers(ctx context.Context, tv *testvars.TestVars, moreExpectedVersions ...*testvars.TestVars) {
	expectedVersionsStr := []string{tv.DeploymentVersionStringV32()}
	for _, tv2 := range moreExpectedVersions {
		if !tv2.ExternalDeploymentVersion().Equal(tv.ExternalDeploymentVersion()) {
			expectedVersionsStr = append(expectedVersionsStr, tv2.DeploymentVersionStringV32())
		}
	}
	s.EventuallyWithT(func(t *assert.CollectT) {
		resp, err := s.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
			Namespace:     s.Namespace().String(),
			TaskQueue:     tv.TaskQueue(),
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		})
		require.NoError(t, err)
		versionsSeen := 0
		for _, poller := range resp.Pollers {
			pollerV := worker_versioning.WorkerDeploymentVersionToStringV32(worker_versioning.DeploymentVersionFromOptions(poller.GetDeploymentOptions()))
			for _, v := range expectedVersionsStr {
				if pollerV == v {
					versionsSeen++
				}
			}
		}
		require.Equal(t, len(expectedVersionsStr), versionsSeen)
	}, 10*time.Second, 100*time.Millisecond)
}

func (s *DeploymentVersionSuite) waitForNoPollers(ctx context.Context, tv *testvars.TestVars, moreUnexpectedVersions ...*testvars.TestVars) {
	unexpectedVersionsStr := []string{tv.DeploymentVersionStringV32()}
	for _, tv2 := range moreUnexpectedVersions {
		if !tv2.ExternalDeploymentVersion().Equal(tv.ExternalDeploymentVersion()) {
			unexpectedVersionsStr = append(unexpectedVersionsStr, tv2.DeploymentVersionStringV32())
		}
	}
	s.EventuallyWithT(func(t *assert.CollectT) {
		resp, err := s.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
			Namespace:     s.Namespace().String(),
			TaskQueue:     tv.TaskQueue(),
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		})
		require.NoError(t, err)

		versionsSeen := 0
		fmt.Printf("Pollers: %+v\n", resp.Pollers)
		for _, poller := range resp.Pollers {
			pollerV := worker_versioning.WorkerDeploymentVersionToStringV32(worker_versioning.DeploymentVersionFromOptions(poller.GetDeploymentOptions()))
			for _, v := range unexpectedVersionsStr {
				if pollerV == v {
					versionsSeen++
				}
			}
		}
		require.Equal(t, 0, versionsSeen)
	}, 10*time.Second, 100*time.Millisecond)
}

func (s *DeploymentVersionSuite) TestVersionScavenger_DeleteOnAdd() {
	s.OverrideDynamicConfig(dynamicconfig.PollerHistoryTTL, 3*time.Second)
	s.OverrideDynamicConfig(dynamicconfig.MatchingMaxVersionsInDeployment, testMaxVersionsInDeployment)
	// we don't want the version to drain in this test
	s.OverrideDynamicConfig(dynamicconfig.VersionDrainageStatusVisibilityGracePeriod, 60*time.Second)
	s.OverrideDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 0)
	// Set deployment register error backoff to zero so to speed up the test.
	s.InjectHook(testhooks.NewHook(testhooks.MatchingDeploymentRegisterErrorBackoff, 0*time.Second))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	tvs := make([]*testvars.TestVars, testMaxVersionsInDeployment)

	// max out the versions
	for i := range testMaxVersionsInDeployment {
		tvs[i] = testvars.New(s).WithBuildIDNumber(i)
		s.startVersionWorkflow(ctx, tvs[i])
	}

	// Make tvs[0] current
	err := s.setCurrent(tvs[0], false)
	s.Nil(err)
	// Make tvs[1] current, hence tvs[0] should go to draining
	err = s.setCurrent(tvs[1], false)
	s.Nil(err)

	// CI can be slow, keep sending fresh polls to ensure that auto deletion logic sees them when we want to add tvMax so it can't add.
	pollContext, cancelPolls := context.WithTimeout(context.Background(), 3*time.Second)
	go func() {
		for i := range testMaxVersionsInDeployment {
			go s.pollFromDeployment(pollContext, tvs[i])
		}

		t := time.NewTicker(time.Second)
		for {
			select {
			case <-pollContext.Done():
				return
			case <-t.C:
				for i := range testMaxVersionsInDeployment {
					go s.pollFromDeployment(pollContext, tvs[i])
				}
			}
		}
	}()
	s.waitForPollers(ctx, tvs[0], tvs...)

	tvMax := testvars.New(s).WithBuildIDNumber(9999)

	cancelPolls()

	// try to add a version and it fails because none of the versions can be deleted
	s.startVersionWorkflowExpectFailAddVersion(ctx, tvMax)

	// this waits for no pollers from any of original versions (tvMax pollers should be fine)
	s.waitForNoPollers(ctx, tvs[0], tvs...)

	// try to add the version again, and it succeeds, after deleting tvs[2] version but not tvs[3] (both are eligible)
	s.startVersionWorkflow(ctx, tvMax)

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
	s.waitForNoPollers(ctx, tv1, tv1)

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
	s.EqualError(err, fmt.Sprintf(workerdeployment.ErrCurrentVersionDoesNotHaveAllTaskQueues, tv2.DeploymentVersionStringV32()))
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
	s.EqualError(err, fmt.Sprintf(workerdeployment.ErrRampingVersionDoesNotHaveAllTaskQueues, tv2.DeploymentVersionStringV32()))
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
	s.Len(entries, 2)
	s.Equal(testRandomMetadataValue, entries["key1"].Data)
	s.Equal(testRandomMetadataValue, entries["key2"].Data)

	// Remove all the entries
	_, err = s.updateMetadata(tv1, nil, []string{"key1", "key2"})
	s.NoError(err)

	resp, err = s.describeVersion(tv1)
	s.NoError(err)
	entries = resp.GetWorkerDeploymentVersionInfo().GetMetadata().GetEntries()
	s.Equal(0, len(entries))

	// update metadata for the second time
	_, err = s.updateMetadata(tv1, metadata, nil)
	s.NoError(err)

	resp, err = s.describeVersion(tv1)
	s.NoError(err)

	// validating the metadata
	entries = resp.GetWorkerDeploymentVersionInfo().GetMetadata().GetEntries()
	s.Len(entries, 2)
	s.Equal(testRandomMetadataValue, entries["key1"].Data)
	s.Equal(testRandomMetadataValue, entries["key2"].Data)
}

func (s *DeploymentVersionSuite) checkVersionDrainageAndVersionStatus(
	ctx context.Context,
	tv *testvars.TestVars,
	expectedDrainageInfo *deploymentpb.VersionDrainageInfo,
	expectedStatus enumspb.WorkerDeploymentVersionStatus,
	waitFor time.Duration,
) (changedTime, checkedTime time.Time) {
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
	}, 15*time.Second, time.Second)
	return changedTime, checkedTime
}

func (s *DeploymentVersionSuite) checkVersionStatusInDeployment(
	ctx context.Context,
	tv *testvars.TestVars,
	expectedStatus enumspb.WorkerDeploymentVersionStatus,
) {
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeployment(ctx, &workflowservice.DescribeWorkerDeploymentRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentSeries(),
		})
		a.NoError(err)
		found := false
		for _, versionSummary := range resp.GetWorkerDeploymentInfo().GetVersionSummaries() {
			if versionSummary.GetVersion() == tv.DeploymentVersionString() { //nolint:staticcheck // SA1019: worker versioning v0.31
				a.Equal(expectedStatus, versionSummary.GetStatus(),
					"DescribeWorkerDeployment should show version %s as %s", tv.DeploymentVersionString(), expectedStatus)
				found = true
				break
			}
		}
		a.True(found, "Version %s should be found in DescribeWorkerDeployment response", tv.DeploymentVersionString())
	}, 10*time.Second, 500*time.Millisecond)
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
			a.Equalf(expectedOverride.GetPinned().GetVersion().GetBuildId(), actualOverride.GetPinned().GetVersion().GetBuildId(),
				"expected pinned version build id %v, got %v", expectedOverride.GetPinned().GetVersion().GetBuildId(), actualOverride.GetPinned().GetVersion().GetBuildId())
			a.Equalf(expectedOverride.GetPinned().GetVersion().GetDeploymentName(), actualOverride.GetPinned().GetVersion().GetDeploymentName(),
				"expected pinned version deployment name %v, got %v", expectedOverride.GetPinned().GetVersion().GetDeploymentName(), actualOverride.GetPinned().GetVersion().GetDeploymentName())
			a.Equalf(expectedOverride.GetPinned().GetBehavior(), actualOverride.GetPinned().GetBehavior(),
				"expected pinned override behavior %v, got %v", expectedOverride.GetPinned().GetBehavior(), actualOverride.GetPinned().GetBehavior())
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

func (s *DeploymentVersionSuite) checkWorkflowUpdateOptionsEventIdentity(
	ctx context.Context,
	wf *commonpb.WorkflowExecution,
	expectedIdentity string,
) {
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := s.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: s.Namespace().String(),
			Execution: wf,
		})
		a.NoError(err)
		a.NotNil(resp)
		events := resp.GetHistory().GetEvents()
		for resp.NextPageToken != nil { // probably there won't ever be more than one page of events in these tests
			resp, err = s.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
				Namespace:     s.Namespace().String(),
				Execution:     wf,
				NextPageToken: resp.NextPageToken,
			})
			a.NoError(err)
			a.NotNil(resp)
			events = append(events, resp.GetHistory().GetEvents()...)
		}
		for _, event := range events {
			if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED {
				a.Equal(expectedIdentity, event.GetWorkflowExecutionOptionsUpdatedEventAttributes().GetIdentity())
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
	s.setAndCheckOverrideWithExpectedOutput(ctx, tv, override, override)
}

func (s *DeploymentVersionSuite) setAndCheckOverrideWithExpectedOutput(ctx context.Context, tv *testvars.TestVars, inputOverride, expectedOutputOverride *workflowpb.VersioningOverride) {
	optsIn := &workflowpb.WorkflowExecutionOptions{VersioningOverride: inputOverride}
	optsOut := &workflowpb.WorkflowExecutionOptions{VersioningOverride: expectedOutputOverride}
	// Set input override --> describe workflow shows the expected output override
	updateResp, err := s.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                s.Namespace().String(),
		WorkflowExecution:        tv.WorkflowExecution(),
		WorkflowExecutionOptions: optsIn,
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
		Identity:                 tv.ClientIdentity(),
	})
	s.NoError(err)
	s.True(proto.Equal(updateResp.GetWorkflowExecutionOptions(), optsOut))
	s.checkDescribeWorkflowAfterOverride(ctx, tv.WorkflowExecution(), expectedOutputOverride)
	s.checkWorkflowUpdateOptionsEventIdentity(ctx, tv.WorkflowExecution(), tv.ClientIdentity())
}

// The following tests test the VersioningOverride functionality when passed via the UpdateWorkflowExecutionOptions API.
func (s *DeploymentVersionSuite) TestUpdateWorkflowExecutionOptions_SetPinned_CacheMissAndHits() {

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tv := testvars.New(s)

	// start an unversioned workflow
	s.startWorkflow(tv, nil)

	opts := &workflowpb.WorkflowExecutionOptions{VersioningOverride: s.makePinnedOverride(tv)}

	// Setting a pinned override should fail since the version does not exist
	resp, err := s.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                s.Namespace().String(),
		WorkflowExecution:        tv.WorkflowExecution(),
		WorkflowExecutionOptions: opts,
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
		Identity:                 tv.ClientIdentity(),
	})
	s.Error(err)
	s.Nil(resp)

	// Start a versioned poller which shall create a version; however, the cache TTL is not expired yet. This would result in a cache hit which would return
	// a stale value for the version presence in the task queue.
	s.startVersionWorkflow(ctx, tv)

	// Setting a pinned override should fail since the stale cache entry is returned.
	resp, err = s.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                s.Namespace().String(),
		WorkflowExecution:        tv.WorkflowExecution(),
		WorkflowExecutionOptions: opts,
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
		Identity:                 tv.ClientIdentity(),
	})
	s.Error(err)
	s.Nil(resp)

	// Wait for the cache TTL to expire
	s.Eventually(func() bool {
		_, err := s.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
			Namespace:                s.Namespace().String(),
			WorkflowExecution:        tv.WorkflowExecution(),
			WorkflowExecutionOptions: opts,
			UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
			Identity:                 tv.ClientIdentity(),
		})
		return err == nil
	}, 10*time.Second, 500*time.Millisecond)

	// The Pinned Override should have now succeeded with no error. Verify that the
	// the workflow shows the override.
	s.checkDescribeWorkflowAfterOverride(ctx, tv.WorkflowExecution(), opts.VersioningOverride)
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

	// Start a versioned poller which shall create a version; the version must be present before it can be set as an override.
	s.startVersionWorkflow(ctx, tv)

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

	// Start a versioned poller which shall create a version; the version must be present before it can be set as an override.
	s.startVersionWorkflow(ctx, tv)

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

	// Start a versioned poller which shall create the two versions; the versions must be present before they can be set as overrides.
	s.startVersionWorkflow(ctx, tv1)
	s.startVersionWorkflow(ctx, tv2)

	// start an unversioned workflow
	s.startWorkflow(tv, nil)

	// 1. Set pinned override 1 --> describe workflow shows the override
	s.setAndCheckOverride(ctx, tv, s.makePinnedOverride(tv1))

	// 3. Set pinned override 2 --> describe workflow shows the override
	s.setAndCheckOverride(ctx, tv, s.makePinnedOverride(tv2))
}

func (s *DeploymentVersionSuite) TestUpdateWorkflowExecutionOptions_SetImpliedPinnedSuccess() {
	if !s.useV32 {
		s.T().Skip("Implied pinned overrides are only supported in v3.2+")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tv1 := testvars.New(s).WithBuildIDNumber(1)

	// Start a versioned poller which shall create the two versions; the versions must be present before they can be set as overrides.
	s.startVersionWorkflow(ctx, tv1)

	// Set tv1 to current, so that the test workflow will be naturally pinned to tv1.
	err := s.setCurrent(tv1, true)
	s.NoError(err)

	// Start a workflow pinned to tv1.
	run := s.startPinnedWorkflow(ctx, tv1)

	noVersionPinnedOverride := &workflowpb.VersioningOverride{Override: &workflowpb.VersioningOverride_Pinned{
		Pinned: &workflowpb.VersioningOverride_PinnedOverride{
			Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
		},
	}}

	yesVersionPinnedOverride := &workflowpb.VersioningOverride{Override: &workflowpb.VersioningOverride_Pinned{
		Pinned: &workflowpb.VersioningOverride_PinnedOverride{
			Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
			Version:  tv1.ExternalDeploymentVersion(),
		},
	}}

	// 1. Set pinned override without a version --> describe workflow shows the override with pinned override version set to tv1.
	s.setAndCheckOverrideWithExpectedOutput(ctx, tv1.WithWorkflowID(run.GetID()), noVersionPinnedOverride, yesVersionPinnedOverride)
}

func (s *DeploymentVersionSuite) TestUpdateWorkflowExecutionOptions_SetImpliedPinnedError() {
	if !s.useV32 {
		s.T().Skip("Implied pinned overrides are only supported in v3.2+")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tv1 := testvars.New(s).WithBuildIDNumber(1)

	// Start a versioned poller which shall create the two versions; the versions must be present before they can be set as overrides.
	s.startVersionWorkflow(ctx, tv1)

	// Set tv1 to current, so that the test workflow will run on v1.
	err := s.setCurrent(tv1, true)
	s.NoError(err)

	// Start an auto-upgrade workflow.
	run := s.startUnpinnedWorkflow(ctx, tv1)

	noVersionPinnedOverride := &workflowpb.VersioningOverride{Override: &workflowpb.VersioningOverride_Pinned{
		Pinned: &workflowpb.VersioningOverride_PinnedOverride{
			Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
		},
	}}

	// 1. Set pinned override without a version --> errors because workflow is not already pinned to a version
	optsIn := &workflowpb.WorkflowExecutionOptions{VersioningOverride: noVersionPinnedOverride}
	// Set input override --> describe workflow shows the expected output override
	_, err = s.FrontendClient().UpdateWorkflowExecutionOptions(ctx, &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                s.Namespace().String(),
		WorkflowExecution:        tv1.WithWorkflowID(run.GetID()).WorkflowExecution(),
		WorkflowExecutionOptions: optsIn,
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
		Identity:                 tv1.ClientIdentity(),
	})
	s.Error(err)
	s.Contains(err.Error(),
		fmt.Sprintf("must specify a specific pinned override version because workflow with id %v has behavior %s and is not yet pinned to any version",
			run.GetID(), enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE.String()),
	)
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

	// Start a versioned poller which shall create a version; the version must be present before it can be set as an override.
	s.startVersionWorkflow(ctx, tv)

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

	// Start a versioned poller which shall create a version; the version must be present before it can be set as an override.
	s.startVersionWorkflow(ctx, tv)

	// start an unversioned workflow
	s.startWorkflow(tv, nil)

	// 1. Set pinned override 1 --> describe workflow shows the override
	s.setAndCheckOverride(ctx, tv, s.makePinnedOverride(tv))

	// 2. Set unpinned override --> describe workflow shows the override
	s.setAndCheckOverride(ctx, tv, s.makeAutoUpgradeOverride())
}

func (s *DeploymentVersionSuite) TestUpdateWorkflowExecutionOptions_ReactivateVersionOnPinned() {
	s.OverrideDynamicConfig(dynamicconfig.VersionDrainageStatusVisibilityGracePeriod, testLongVersionDrainageVisibilityGracePeriod)
	s.OverrideDynamicConfig(dynamicconfig.VersionDrainageStatusRefreshInterval, testLongVersionDrainageRefreshInterval)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// Use shorter, explicit deployment series names to avoid truncation issues
	// Include workflow version in deployment name to avoid conflicts in parallel tests
	deploymentName := fmt.Sprintf("test-reactivate-wfv%d", s.workflowVersion)
	tv1 := testvars.New(s).WithDeploymentSeries(deploymentName).WithBuildID(deploymentName + "-v1").WithTaskQueue("test-task-queue") // First version (will become DRAINED)
	tv2 := testvars.New(s).WithDeploymentSeries(deploymentName).WithBuildID(deploymentName + "-v2").WithTaskQueue("test-task-queue") // Second version (will become CURRENT)

	// set version 1 as current
	s.startVersionWorkflow(ctx, tv1)
	err := s.setCurrent(tv1, true)
	s.NoError(err)

	// set version 2 as current which shall cause version 1 to be in 'Draining' state
	s.startVersionWorkflow(ctx, tv2)
	err = s.setCurrent(tv2, true)
	s.NoError(err)

	// Wait for version 1 to become DRAINED
	s.checkVersionDrainageAndVersionStatus(ctx, tv1,
		&deploymentpb.VersionDrainageInfo{
			Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINED,
		},
		enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED,
		testLongVersionDrainageVisibilityGracePeriod+testLongVersionDrainageRefreshInterval) // wait for grace period and refresh interval

	wf := func(version string) func(ctx workflow.Context) (string, error) {
		return func(ctx workflow.Context) (string, error) {
			workflow.GetSignalChannel(ctx, "complete").Receive(ctx, nil)
			return "done from " + version, nil
		}
	}

	// Register a worker for version 1 (DRAINED) so it can accept workflows when
	// UpdateWorkflowExecutionOptions is called to pin the workflow to version 1.
	w1 := worker.New(s.SdkClient(), tv1.TaskQueue().String(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			Version:       tv1.SDKDeploymentVersion(),
			UseVersioning: true,
		},
	})
	w1.RegisterWorkflowWithOptions(wf("v1"), workflow.RegisterOptions{
		Name:               "waitingWorkflow",
		VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
	})
	s.NoError(w1.Start())
	defer w1.Stop()

	// Register and start worker for version 2 on THE SAME task queue as version 1
	w2 := worker.New(s.SdkClient(), tv1.TaskQueue().String(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			Version:       tv2.SDKDeploymentVersion(),
			UseVersioning: true,
		},
	})
	w2.RegisterWorkflowWithOptions(wf("v2"), workflow.RegisterOptions{
		Name:               "waitingWorkflow",
		VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
	})
	s.NoError(w2.Start())
	defer w2.Stop()

	s.waitForPollers(ctx, tv1, tv2)

	// Start the workflow. The workflow shall start on version 2, by default, since it is the current version.
	wfTV := testvars.New(s)
	run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue: tv1.TaskQueue().String(),
		ID:        wfTV.WorkflowID(),
	}, "waitingWorkflow")
	s.NoError(err)

	// Pin the running workflow to version 1 (DRAINED) using UpdateWorkflowExecutionOptions.
	pinnedOverride := &workflowpb.VersioningOverride{
		Override: &workflowpb.VersioningOverride_Pinned{
			Pinned: &workflowpb.VersioningOverride_PinnedOverride{
				Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
				Version:  tv1.ExternalDeploymentVersion(),
			},
		},
	}

	// Pin the workflow to version 1 (both versions are on the same task queue).
	// Use Eventually to bypass version membership cache checks.
	s.Eventually(func() bool {
		_, err = s.FrontendClient().UpdateWorkflowExecutionOptions(ctx,
			&workflowservice.UpdateWorkflowExecutionOptionsRequest{
				Namespace: s.Namespace().String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: wfTV.WorkflowID(),
					RunId:      run.GetRunID(),
				},
				WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{
					VersioningOverride: pinnedOverride,
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
			})
		return err == nil
	}, 10*time.Second, 500*time.Millisecond)

	// Verify workflow has the pinned override
	s.checkDescribeWorkflowAfterOverride(ctx,
		&commonpb.WorkflowExecution{
			WorkflowId: wfTV.WorkflowID(),
			RunId:      run.GetRunID(),
		},
		pinnedOverride)

	// Wait for version 1 to show up as DRAINING
	s.checkVersionDrainageAndVersionStatus(ctx, tv1,
		&deploymentpb.VersionDrainageInfo{
			Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINING,
		},
		enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
		0)

	// Verify via DescribeWorkerDeployment that the version status is updated
	s.checkVersionStatusInDeployment(ctx, tv1, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING)

	// Signal workflow to complete
	s.NoError(s.SdkClient().SignalWorkflow(ctx,
		wfTV.WorkflowID(), run.GetRunID(), "complete", nil))

	// Wait for workflow to complete and verify it ran on version 1
	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("done from v1", result, "Workflow should have completed on version 1")
}

func (s *DeploymentVersionSuite) TestStartWorkflowExecution_ReactivateVersionOnPinned() {
	s.OverrideDynamicConfig(dynamicconfig.VersionDrainageStatusVisibilityGracePeriod, testLongVersionDrainageVisibilityGracePeriod)
	s.OverrideDynamicConfig(dynamicconfig.VersionDrainageStatusRefreshInterval, testLongVersionDrainageRefreshInterval)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// Use shorter, explicit deployment series names to avoid truncation issues
	// Include workflow version in deployment name to avoid conflicts in parallel tests
	deploymentName := fmt.Sprintf("test-start-reactivate-wfv%d", s.workflowVersion)
	tv1 := testvars.New(s).WithDeploymentSeries(deploymentName).WithBuildID(deploymentName + "-v1").WithTaskQueue("test-start-task-queue") // First version (will become DRAINED)
	tv2 := testvars.New(s).WithDeploymentSeries(deploymentName).WithBuildID(deploymentName + "-v2").WithTaskQueue("test-start-task-queue") // Second version (will become CURRENT)

	// set version 1 as current
	s.startVersionWorkflow(ctx, tv1)
	err := s.setCurrent(tv1, true)
	s.NoError(err)

	// set version 2 as current which shall cause version 1 to be in 'Draining' state
	s.startVersionWorkflow(ctx, tv2)
	err = s.setCurrent(tv2, true)
	s.NoError(err)

	// Wait for version 1 to become DRAINED
	s.checkVersionDrainageAndVersionStatus(ctx, tv1,
		&deploymentpb.VersionDrainageInfo{
			Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINED,
		},
		enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED,
		testLongVersionDrainageVisibilityGracePeriod+testLongVersionDrainageRefreshInterval) // wait for grace period and refresh interval

	wf := func(version string) func(ctx workflow.Context) (string, error) {
		return func(ctx workflow.Context) (string, error) {
			workflow.GetSignalChannel(ctx, "complete").Receive(ctx, nil)
			return "done from " + version, nil
		}
	}

	// Register a worker for version 1 (DRAINED) so it can accept workflows when
	// StartWorkflowExecution is called with a pinned override to version 1.
	w1 := worker.New(s.SdkClient(), tv1.TaskQueue().String(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			Version:       tv1.SDKDeploymentVersion(),
			UseVersioning: true,
		},
	})
	w1.RegisterWorkflowWithOptions(wf("v1"), workflow.RegisterOptions{
		Name:               "waitingWorkflow",
		VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
	})
	s.NoError(w1.Start())
	defer w1.Stop()

	// Start a new workflow with the pinned override pointing to version 1 (DRAINED).
	wfTV := testvars.New(s)
	var run sdkclient.WorkflowRun
	s.Eventually(func() bool {
		var startErr error
		run, startErr = s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			TaskQueue: tv1.TaskQueue().String(),
			ID:        wfTV.WorkflowID(),
			VersioningOverride: &sdkclient.PinnedVersioningOverride{
				Version: tv1.SDKDeploymentVersion(),
			},
		}, "waitingWorkflow")
		return startErr == nil
	}, 10*time.Second, 500*time.Millisecond)

	// Verify workflow has the pinned override
	s.checkDescribeWorkflowAfterOverride(ctx,
		&commonpb.WorkflowExecution{
			WorkflowId: wfTV.WorkflowID(),
			RunId:      run.GetRunID(),
		},
		&workflowpb.VersioningOverride{
			Override: &workflowpb.VersioningOverride_Pinned{
				Pinned: &workflowpb.VersioningOverride_PinnedOverride{
					Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
					Version:  tv1.ExternalDeploymentVersion(),
				},
			},
		})

	// Wait for version 1 to show up as DRAINING (reactivated from DRAINED)
	s.checkVersionDrainageAndVersionStatus(ctx, tv1,
		&deploymentpb.VersionDrainageInfo{
			Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINING,
		},
		enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
		0)

	// Verify via DescribeWorkerDeployment that the version status is updated
	s.checkVersionStatusInDeployment(ctx, tv1, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING)

	// Signal workflow to complete
	s.NoError(s.SdkClient().SignalWorkflow(ctx,
		wfTV.WorkflowID(), run.GetRunID(), "complete", nil))

	// Wait for workflow to complete and verify it ran on version 1
	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("done from v1", result, "Workflow should have completed on version 1")
}

func (s *DeploymentVersionSuite) TestStartWorkflowExecution_ReactivateVersionOnPinned_WithConflictPolicy() {
	s.OverrideDynamicConfig(dynamicconfig.VersionDrainageStatusVisibilityGracePeriod, testLongVersionDrainageVisibilityGracePeriod)
	s.OverrideDynamicConfig(dynamicconfig.VersionDrainageStatusRefreshInterval, testLongVersionDrainageRefreshInterval)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	deploymentName := fmt.Sprintf("test-start-conflict-reactivate-wfv%d", s.workflowVersion)
	tv1 := testvars.New(s).WithDeploymentSeries(deploymentName).WithBuildID(deploymentName + "-v1").WithTaskQueue("test-conflict-task-queue")
	tv2 := testvars.New(s).WithDeploymentSeries(deploymentName).WithBuildID(deploymentName + "-v2").WithTaskQueue("test-conflict-task-queue")

	// set version 1 as current
	s.startVersionWorkflow(ctx, tv1)
	err := s.setCurrent(tv1, true)
	s.NoError(err)

	// set version 2 as current which shall cause version 1 to be in 'Draining' state
	s.startVersionWorkflow(ctx, tv2)
	err = s.setCurrent(tv2, true)
	s.NoError(err)

	// Wait for version 1 to become DRAINED
	s.checkVersionDrainageAndVersionStatus(ctx, tv1,
		&deploymentpb.VersionDrainageInfo{
			Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINED,
		},
		enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED,
		testLongVersionDrainageVisibilityGracePeriod+testLongVersionDrainageRefreshInterval)

	wf := func(version string) func(ctx workflow.Context) (string, error) {
		return func(ctx workflow.Context) (string, error) {
			workflow.GetSignalChannel(ctx, "complete").Receive(ctx, nil)
			return "done from " + version, nil
		}
	}

	// Register a worker for version 1 (DRAINED) so it can accept workflows
	w1 := worker.New(s.SdkClient(), tv1.TaskQueue().String(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			Version:       tv1.SDKDeploymentVersion(),
			UseVersioning: true,
		},
	})
	w1.RegisterWorkflowWithOptions(wf("v1"), workflow.RegisterOptions{
		Name:               "waitingWorkflow",
		VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
	})
	s.NoError(w1.Start())
	defer w1.Stop()

	// Start a first workflow (no pinning, uses current version v2) to create a running execution
	// with a specific workflow ID that we will terminate via conflict policy.
	wfTV := testvars.New(s)
	w2 := worker.New(s.SdkClient(), tv2.TaskQueue().String(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			Version:       tv2.SDKDeploymentVersion(),
			UseVersioning: true,
		},
	})
	w2.RegisterWorkflowWithOptions(wf("v2"), workflow.RegisterOptions{
		Name:               "waitingWorkflow",
		VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
	})
	s.NoError(w2.Start())
	defer w2.Stop()

	s.Eventually(func() bool {
		_, startErr := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			TaskQueue: tv2.TaskQueue().String(),
			ID:        wfTV.WorkflowID(),
		}, "waitingWorkflow")
		return startErr == nil
	}, 10*time.Second, 500*time.Millisecond)

	// Now start a second workflow with the SAME workflow ID, pinned to v1 (DRAINED),
	// using TERMINATE_EXISTING conflict policy. This goes through the handleConflict method in api.go.
	var run sdkclient.WorkflowRun
	s.Eventually(func() bool {
		var startErr error
		run, startErr = s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			TaskQueue: tv1.TaskQueue().String(),
			ID:        wfTV.WorkflowID(),
			VersioningOverride: &sdkclient.PinnedVersioningOverride{
				Version: tv1.SDKDeploymentVersion(),
			},
			WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING,
		}, "waitingWorkflow")
		return startErr == nil
	}, 10*time.Second, 500*time.Millisecond)

	// Verify workflow has the pinned override
	s.checkDescribeWorkflowAfterOverride(ctx,
		&commonpb.WorkflowExecution{
			WorkflowId: wfTV.WorkflowID(),
			RunId:      run.GetRunID(),
		},
		&workflowpb.VersioningOverride{
			Override: &workflowpb.VersioningOverride_Pinned{
				Pinned: &workflowpb.VersioningOverride_PinnedOverride{
					Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
					Version:  tv1.ExternalDeploymentVersion(),
				},
			},
		})

	// Wait for version 1 to show up as DRAINING (reactivated from DRAINED)
	s.checkVersionDrainageAndVersionStatus(ctx, tv1,
		&deploymentpb.VersionDrainageInfo{
			Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINING,
		},
		enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
		0)

	// Verify via DescribeWorkerDeployment that the version status is updated
	s.checkVersionStatusInDeployment(ctx, tv1, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING)

	// Signal workflow to complete
	s.NoError(s.SdkClient().SignalWorkflow(ctx,
		wfTV.WorkflowID(), run.GetRunID(), "complete", nil))

	// Wait for workflow to complete and verify it ran on version 1
	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("done from v1", result, "Workflow should have completed on version 1")
}

func (s *DeploymentVersionSuite) TestSignalWithStartWorkflowExecution_ReactivateVersionOnPinned() {
	s.OverrideDynamicConfig(dynamicconfig.VersionDrainageStatusVisibilityGracePeriod, testLongVersionDrainageVisibilityGracePeriod)
	s.OverrideDynamicConfig(dynamicconfig.VersionDrainageStatusRefreshInterval, testLongVersionDrainageRefreshInterval)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// Use shorter, explicit deployment series names to avoid truncation issues
	// Include workflow version in deployment name to avoid conflicts in parallel tests
	deploymentName := fmt.Sprintf("test-sws-reactivate-wfv%d", s.workflowVersion)
	tv1 := testvars.New(s).WithDeploymentSeries(deploymentName).WithBuildID(deploymentName + "-v1").WithTaskQueue("test-sws-task-queue") // First version (will become DRAINED)
	tv2 := testvars.New(s).WithDeploymentSeries(deploymentName).WithBuildID(deploymentName + "-v2").WithTaskQueue("test-sws-task-queue") // Second version (will become CURRENT)

	// set version 1 as current
	s.startVersionWorkflow(ctx, tv1)
	err := s.setCurrent(tv1, true)
	s.NoError(err)

	// set version 2 as current which shall cause version 1 to be in 'Draining' state
	s.startVersionWorkflow(ctx, tv2)
	err = s.setCurrent(tv2, true)
	s.NoError(err)

	// Wait for version 1 to become DRAINED
	s.checkVersionDrainageAndVersionStatus(ctx, tv1,
		&deploymentpb.VersionDrainageInfo{
			Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINED,
		},
		enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED,
		testLongVersionDrainageVisibilityGracePeriod+testLongVersionDrainageRefreshInterval) // wait for grace period and refresh interval

	wf := func(version string) func(ctx workflow.Context) (string, error) {
		return func(ctx workflow.Context) (string, error) {
			workflow.GetSignalChannel(ctx, "complete").Receive(ctx, nil)
			return "done from " + version, nil
		}
	}

	// Register a worker for version 1 (DRAINED) so it can accept workflows when
	// SignalWithStartWorkflowExecution is called with a pinned override to version 1.
	w1 := worker.New(s.SdkClient(), tv1.TaskQueue().String(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			Version:       tv1.SDKDeploymentVersion(),
			UseVersioning: true,
		},
	})
	w1.RegisterWorkflowWithOptions(wf("v1"), workflow.RegisterOptions{
		Name:               "waitingWorkflow",
		VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
	})
	s.NoError(w1.Start())
	defer w1.Stop()

	// Use SignalWithStart with the pinned override pointing to version 1 (DRAINED).
	// This should START a new workflow (not signal an existing one) since no workflow exists yet.
	wfTV := testvars.New(s)
	var run sdkclient.WorkflowRun
	s.Eventually(func() bool {
		var startErr error
		run, startErr = s.SdkClient().SignalWithStartWorkflow(ctx,
			wfTV.WorkflowID(),
			"start-signal", // signal name
			nil,            // signal arg
			sdkclient.StartWorkflowOptions{
				TaskQueue: tv1.TaskQueue().String(),
				VersioningOverride: &sdkclient.PinnedVersioningOverride{
					Version: tv1.SDKDeploymentVersion(),
				},
			},
			"waitingWorkflow",
		)
		return startErr == nil
	}, 10*time.Second, 500*time.Millisecond)

	// Verify workflow has the pinned override
	s.checkDescribeWorkflowAfterOverride(ctx,
		&commonpb.WorkflowExecution{
			WorkflowId: wfTV.WorkflowID(),
			RunId:      run.GetRunID(),
		},
		&workflowpb.VersioningOverride{
			Override: &workflowpb.VersioningOverride_Pinned{
				Pinned: &workflowpb.VersioningOverride_PinnedOverride{
					Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
					Version:  tv1.ExternalDeploymentVersion(),
				},
			},
		})

	// Wait for version 1 to show up as DRAINING (reactivated from DRAINED)
	s.checkVersionDrainageAndVersionStatus(ctx, tv1,
		&deploymentpb.VersionDrainageInfo{
			Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINING,
		},
		enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
		0)

	// Verify via DescribeWorkerDeployment that the version status is updated
	s.checkVersionStatusInDeployment(ctx, tv1, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING)

	// Signal workflow to complete
	s.NoError(s.SdkClient().SignalWorkflow(ctx,
		wfTV.WorkflowID(), run.GetRunID(), "complete", nil))

	// Wait for workflow to complete and verify it ran on version 1
	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("done from v1", result, "Workflow should have completed on version 1")
}

func (s *DeploymentVersionSuite) TestResetWorkflowExecution_ReactivateVersionOnPinned() {
	s.OverrideDynamicConfig(dynamicconfig.VersionDrainageStatusVisibilityGracePeriod, testLongVersionDrainageVisibilityGracePeriod)
	s.OverrideDynamicConfig(dynamicconfig.VersionDrainageStatusRefreshInterval, testLongVersionDrainageRefreshInterval)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// Use shorter, explicit deployment series names to avoid truncation issues
	// Include workflow version in deployment name to avoid conflicts in parallel tests
	deploymentName := fmt.Sprintf("test-reset-reactivate-wfv%d", s.workflowVersion)
	tv1 := testvars.New(s).WithDeploymentSeries(deploymentName).WithBuildID(deploymentName + "-v1").WithTaskQueue("test-reset-task-queue") // First version (will become DRAINED)
	tv2 := testvars.New(s).WithDeploymentSeries(deploymentName).WithBuildID(deploymentName + "-v2").WithTaskQueue("test-reset-task-queue") // Second version (will become CURRENT)

	// set version 1 as current
	s.startVersionWorkflow(ctx, tv1)
	err := s.setCurrent(tv1, true)
	s.NoError(err)

	// set version 2 as current which shall cause version 1 to be in 'Draining' state
	s.startVersionWorkflow(ctx, tv2)
	err = s.setCurrent(tv2, true)
	s.NoError(err)

	// Wait for version 1 to become DRAINED
	s.checkVersionDrainageAndVersionStatus(ctx, tv1,
		&deploymentpb.VersionDrainageInfo{
			Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINED,
		},
		enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED,
		testLongVersionDrainageVisibilityGracePeriod+testLongVersionDrainageRefreshInterval) // wait for grace period and refresh interval

	// Workflow that waits for a signal, used for both versions.
	// Returns a string indicating which version completed it.
	wf := func(version string) func(ctx workflow.Context) (string, error) {
		return func(ctx workflow.Context) (string, error) {
			workflow.GetSignalChannel(ctx, "complete").Receive(ctx, nil)
			return "done from " + version, nil
		}
	}

	// Register a worker for version 1 (DRAINED) so it can accept workflows when
	// ResetWorkflowExecution is called with a pinned override to version 1.
	w1 := worker.New(s.SdkClient(), tv1.TaskQueue().String(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			Version:       tv1.SDKDeploymentVersion(),
			UseVersioning: true,
		},
	})
	w1.RegisterWorkflowWithOptions(wf("v1"), workflow.RegisterOptions{
		Name:               "waitingWorkflow",
		VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
	})
	s.NoError(w1.Start())
	defer w1.Stop()

	// Register and start worker for version 2 on THE SAME task queue as version 1
	w2 := worker.New(s.SdkClient(), tv1.TaskQueue().String(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			Version:       tv2.SDKDeploymentVersion(),
			UseVersioning: true,
		},
	})
	w2.RegisterWorkflowWithOptions(wf("v2"), workflow.RegisterOptions{
		Name:               "waitingWorkflow",
		VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
	})
	s.NoError(w2.Start())
	defer w2.Stop()

	s.waitForPollers(ctx, tv1, tv2)

	// Start a workflow on the current version (v2)
	wfTV := testvars.New(s)
	run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue: tv1.TaskQueue().String(),
		ID:        wfTV.WorkflowID(),
	}, "waitingWorkflow")
	s.NoError(err)

	// Wait for the workflow to start and complete its first workflow task (creates a reset point)
	s.Eventually(func() bool {
		hist := s.SdkClient().GetWorkflowHistory(ctx, wfTV.WorkflowID(), run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for hist.HasNext() {
			event, err := hist.Next()
			if err != nil {
				return false
			}
			if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
				return true
			}
		}
		return false
	}, 10*time.Second, 200*time.Millisecond, "Workflow should have completed its first workflow task")

	// Find the first workflow task complete event ID for the reset point
	var resetEventID int64
	hist := s.SdkClient().GetWorkflowHistory(ctx, wfTV.WorkflowID(), run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for hist.HasNext() {
		event, err := hist.Next()
		s.NoError(err)
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			resetEventID = event.EventId
			break
		}
	}
	s.Positive(resetEventID, "Should have found a workflow task complete event")

	// Reset the workflow with PostResetOperations containing a versioning override pinned to v1 (which is currently DRAINED)
	var resetResp *workflowservice.ResetWorkflowExecutionResponse
	s.Eventually(func() bool {
		var resetErr error
		resetResp, resetErr = s.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: wfTV.WorkflowID(),
				RunId:      run.GetRunID(),
			},
			Reason:                    "testing-reset-reactivation",
			RequestId:                 uuid.NewString(),
			WorkflowTaskFinishEventId: resetEventID,
			PostResetOperations: []*workflowpb.PostResetOperation{
				{
					Variant: &workflowpb.PostResetOperation_UpdateWorkflowOptions_{
						UpdateWorkflowOptions: &workflowpb.PostResetOperation_UpdateWorkflowOptions{
							WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{
								VersioningOverride: &workflowpb.VersioningOverride{
									Override: &workflowpb.VersioningOverride_Pinned{
										Pinned: &workflowpb.VersioningOverride_PinnedOverride{
											Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
											Version:  tv1.ExternalDeploymentVersion(),
										},
									},
								},
							},
							UpdateMask: &fieldmaskpb.FieldMask{
								Paths: []string{"versioning_override"},
							},
						},
					},
				},
			},
		})
		return resetErr == nil
	}, 10*time.Second, 500*time.Millisecond)

	newRunID := resetResp.RunId

	// Verify the reset workflow has the pinned override
	s.checkDescribeWorkflowAfterOverride(ctx,
		&commonpb.WorkflowExecution{
			WorkflowId: wfTV.WorkflowID(),
			RunId:      newRunID,
		},
		&workflowpb.VersioningOverride{
			Override: &workflowpb.VersioningOverride_Pinned{
				Pinned: &workflowpb.VersioningOverride_PinnedOverride{
					Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
					Version:  tv1.ExternalDeploymentVersion(),
				},
			},
		})

	// Wait for version 1 to show up as DRAINING (reactivated from DRAINED)
	s.checkVersionDrainageAndVersionStatus(ctx, tv1,
		&deploymentpb.VersionDrainageInfo{
			Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINING,
		},
		enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
		0)

	// Verify via DescribeWorkerDeployment that the version status is DRAINING
	s.checkVersionStatusInDeployment(ctx, tv1, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING)

	// Signal the reset workflow to complete
	s.NoError(s.SdkClient().SignalWorkflow(ctx,
		wfTV.WorkflowID(), newRunID, "complete", nil))

	// Wait for the reset workflow to complete and verify it ran on version 1
	resetRun := s.SdkClient().GetWorkflow(ctx, wfTV.WorkflowID(), newRunID)
	var result string
	s.NoError(resetRun.Get(ctx, &result))
	s.Equal("done from v1", result, "Reset workflow should have completed on version 1")
}

// The following tests test the VersioningOverride functionality when passed via the BatchUpdateWorkflowExecutionOptions API.
func (s *DeploymentVersionSuite) TestBatchUpdateWorkflowExecutionOptions_SetPinned_VersionDoesNotExist() {
	s.runBatchUpdateWorkflowExecutionOptionsTest(false)
}

func (s *DeploymentVersionSuite) TestBatchUpdateWorkflowExecutionOptions_SetPinnedThenUnset() {
	s.runBatchUpdateWorkflowExecutionOptionsTest(true)
}

func (s *DeploymentVersionSuite) runBatchUpdateWorkflowExecutionOptionsTest(createVersionFirst bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	tv := testvars.New(s)

	// start some unversioned workflows
	workflowType := "UpdateOptionsBatchTestFunc"
	workflows := make([]*commonpb.WorkflowExecution, 0)
	for range 5 {
		run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tv.TaskQueue().Name}, workflowType)
		s.NoError(err)
		workflows = append(workflows, &commonpb.WorkflowExecution{
			WorkflowId: run.GetID(),
			RunId:      run.GetRunID(),
		})
	}

	pinnedOverride := s.makePinnedOverride(tv)
	batchJobID := uuid.NewString()

	if createVersionFirst {
		// Start a versioned poller which shall create a version
		s.startVersionWorkflow(ctx, tv)
	}

	// start batch update-options operation
	_, err := s.SdkClient().WorkflowService().StartBatchOperation(context.Background(), &workflowservice.StartBatchOperationRequest{
		Namespace: s.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_UpdateWorkflowOptionsOperation{
			UpdateWorkflowOptionsOperation: &batchpb.BatchOperationUpdateWorkflowExecutionOptions{
				Identity:                 tv.ClientIdentity(),
				WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{VersioningOverride: pinnedOverride},
				UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
			},
		},
		Executions: workflows,
		JobId:      batchJobID,
		Reason:     "test",
	})
	s.NoError(err)

	if !createVersionFirst {
		s.checkBatchOperationFails(ctx, batchJobID, len(workflows))
		for _, wf := range workflows {
			s.checkDescribeWorkflowAfterOverride(ctx, wf, nil)
		}
		return
	}

	// wait til batch completes successfully
	s.checkListAndWaitForBatchCompletion(ctx, batchJobID)

	// check all the workflows
	for _, wf := range workflows {
		s.checkDescribeWorkflowAfterOverride(ctx, wf, pinnedOverride)
		s.checkWorkflowUpdateOptionsEventIdentity(ctx, wf, tv.ClientIdentity())
	}

	// unset with empty update opts with mutation mask
	batchJobID = uuid.NewString()
	err = s.startBatchJobWithinConcurrentJobLimit(ctx, &workflowservice.StartBatchOperationRequest{
		Namespace:  s.Namespace().String(),
		JobId:      batchJobID,
		Reason:     "test",
		Executions: workflows,
		Operation: &workflowservice.StartBatchOperationRequest_UpdateWorkflowOptionsOperation{
			UpdateWorkflowOptionsOperation: &batchpb.BatchOperationUpdateWorkflowExecutionOptions{
				Identity:                 tv.ClientIdentity(),
				WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{},
				UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
			},
		},
	})
	s.NoError(err)

	// wait til batch completes
	s.checkListAndWaitForBatchCompletion(ctx, batchJobID)

	// check all the workflows
	for _, wf := range workflows {
		s.checkDescribeWorkflowAfterOverride(ctx, wf, nil)
		s.checkWorkflowUpdateOptionsEventIdentity(ctx, wf, tv.ClientIdentity())
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

func (s *DeploymentVersionSuite) checkBatchOperationFails(ctx context.Context, jobID string, numWorkflows int) {
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		descResp, err := s.FrontendClient().DescribeBatchOperation(ctx, &workflowservice.DescribeBatchOperationRequest{
			Namespace: s.Namespace().String(),
			JobId:     jobID,
		})
		a.NoError(err)
		// All workflows should have failed validation
		a.Equal(int64(numWorkflows), descResp.GetFailureOperationCount(), "expected all operations to fail")
	}, 30*time.Second, 500*time.Millisecond)
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

func (s *DeploymentVersionSuite) TestStartWorkflowExecution_WithPinnedOverride_CacheMissAndHits() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tv := testvars.New(s)

	override := s.makePinnedOverride(tv)
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          tv.Any().String(),
		Namespace:          s.Namespace().String(),
		WorkflowId:         tv.WorkflowID(),
		WorkflowType:       tv.WorkflowType(),
		TaskQueue:          tv.TaskQueue(),
		Identity:           tv.WorkerIdentity(),
		VersioningOverride: override,
	}

	// First call should fail since the version to override is not present in the task queue.
	_, err0 := s.FrontendClient().StartWorkflowExecution(ctx, request)
	s.Error(err0)

	// Start a versioned poller which shall create the version; the version must be present before it can be set as an override.
	s.startVersionWorkflow(ctx, tv)

	// Wait for the cache TTL to expire; On expiry of the cache TTL, it would result in a fresh RPC which would verify the version presence,
	// eventually leading to the StartWorkflowExecution call succeeding.
	var resp *workflowservice.StartWorkflowExecutionResponse
	s.Eventually(func() bool {
		var err error
		resp, err = s.FrontendClient().StartWorkflowExecution(ctx, request)
		return err == nil
	}, 10*time.Second, 500*time.Millisecond)

	// The StartWorkflowExecution should now succeed with no error. Verify that the workflow shows the override.
	s.checkDescribeWorkflowAfterOverride(ctx, &commonpb.WorkflowExecution{
		WorkflowId: tv.WorkflowID(),
		RunId:      resp.GetRunId(),
	}, override)
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

func (s *DeploymentVersionSuite) TestSignalWithStartWorkflowExecution_WithPinnedOverride_CacheMissAndHits() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tv := testvars.New(s)

	override := s.makePinnedOverride(tv)
	request := &workflowservice.SignalWithStartWorkflowExecutionRequest{
		Namespace:          s.Namespace().String(),
		WorkflowId:         tv.WorkflowID(),
		WorkflowType:       tv.WorkflowType(),
		TaskQueue:          tv.TaskQueue(),
		Identity:           tv.ClientIdentity(),
		RequestId:          tv.RequestID(),
		SignalName:         "test-signal",
		SignalInput:        nil,
		VersioningOverride: override,
	}

	// Since the version to override is not present in the task queue, the call should fail.
	_, err := s.FrontendClient().SignalWithStartWorkflowExecution(ctx, request)
	s.Error(err)

	// Start a versioned poller which shall create the version; the version must be present before it can be set as an override.
	s.startVersionWorkflow(ctx, tv)

	// Wait for the cache TTL to expire; On expiry of the cache TTL, it would result in a fresh RPC which would verify the version presence,
	// eventually leading to the SignalWithStartWorkflowExecution call succeeding.
	var resp *workflowservice.SignalWithStartWorkflowExecutionResponse
	s.Eventually(func() bool {
		var err error
		resp, err = s.FrontendClient().SignalWithStartWorkflowExecution(ctx, request)
		return err == nil && resp.GetStarted()
	}, 10*time.Second, 500*time.Millisecond)

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

// The following tests verify that the version_reactivation_signal_cache works as intended.
func (s *DeploymentVersionSuite) TestReactivationSignalCache_Deduplication_StartWorkflow() {
	s.OverrideDynamicConfig(dynamicconfig.VersionDrainageStatusVisibilityGracePeriod, testLongVersionDrainageVisibilityGracePeriod)
	s.OverrideDynamicConfig(dynamicconfig.VersionDrainageStatusRefreshInterval, testExtraLongVersionDrainageRefreshInterval)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Use shorter, explicit deployment series names to avoid truncation issues
	deploymentName := fmt.Sprintf("test-cache-dedup-wfv%d", s.workflowVersion)
	tv1 := testvars.New(s).WithDeploymentSeries(deploymentName).WithBuildID(deploymentName + "-v1").WithTaskQueue("test-cache-dedup-tq")
	tv2 := testvars.New(s).WithDeploymentSeries(deploymentName).WithBuildID(deploymentName + "-v2").WithTaskQueue("test-cache-dedup-tq")

	// Set version 1 as current
	s.startVersionWorkflow(ctx, tv1)
	err := s.setCurrent(tv1, true)
	s.NoError(err)

	// Set version 2 as current → version 1 starts draining
	s.startVersionWorkflow(ctx, tv2)
	err = s.setCurrent(tv2, true)
	s.NoError(err)

	// Wait for version 1 to become DRAINED
	s.checkVersionDrainageAndVersionStatus(ctx, tv1,
		&deploymentpb.VersionDrainageInfo{Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINED},
		enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED,
		testLongVersionDrainageVisibilityGracePeriod+testExtraLongVersionDrainageRefreshInterval)

	// Workflow that waits for a signal before completing
	wf := func(ctx workflow.Context) (string, error) {
		workflow.GetSignalChannel(ctx, "complete").Receive(ctx, nil)
		return "done", nil
	}

	// Register worker for version 1
	w1 := worker.New(s.SdkClient(), tv1.TaskQueue().String(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			Version:       tv1.SDKDeploymentVersion(),
			UseVersioning: true,
		},
	})
	w1.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{
		Name:               "waitingWorkflow",
		VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
	})
	s.NoError(w1.Start())
	defer w1.Stop()

	// === First workflow run: Should trigger reactivation signal to be sent (cache miss) ===
	wfTV1 := testvars.New(s)
	var run1 sdkclient.WorkflowRun
	s.Eventually(func() bool {
		var startErr error
		run1, startErr = s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			TaskQueue: tv1.TaskQueue().String(),
			ID:        wfTV1.WorkflowID(),
			VersioningOverride: &sdkclient.PinnedVersioningOverride{
				Version: tv1.SDKDeploymentVersion(),
			},
		}, "waitingWorkflow")
		return startErr == nil
	}, 10*time.Second, 500*time.Millisecond)

	// Version 1 should transition to DRAINING
	s.checkVersionDrainageAndVersionStatus(ctx, tv1,
		&deploymentpb.VersionDrainageInfo{Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINING},
		enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
		0)

	// Signal workflow to complete.
	s.NoError(s.SdkClient().SignalWorkflow(ctx, wfTV1.WorkflowID(), run1.GetRunID(), "complete", nil))

	// Wait for workflow to complete.
	var result string
	s.NoError(run1.Get(ctx, &result))

	// Wait for version 1 to become Drained again
	s.checkVersionDrainageAndVersionStatus(ctx, tv1,
		&deploymentpb.VersionDrainageInfo{Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINED},
		enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED,
		testLongVersionDrainageVisibilityGracePeriod+testExtraLongVersionDrainageRefreshInterval)

	// === Second workflow run: Should NOT trigger reactivation signal to be sent (cache hit) ===
	wfTV2 := testvars.New(s)
	var run2 sdkclient.WorkflowRun
	s.Eventually(func() bool {
		var startErr error
		run2, startErr = s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			TaskQueue: tv1.TaskQueue().String(),
			ID:        wfTV2.WorkflowID(),
			VersioningOverride: &sdkclient.PinnedVersioningOverride{
				Version: tv1.SDKDeploymentVersion(),
			},
		}, "waitingWorkflow")
		return startErr == nil
	}, 10*time.Second, 500*time.Millisecond)

	// Verify version stays Drained for several checks (even though there is a workflow running)
	// Use Eventually with a counter to check multiple times that the reactivation signal was cached
	drainedCheckCount := 0
	s.Eventually(func() bool {
		resp, err := s.describeVersion(tv1)
		s.NoError(err)
		s.Equalf(enumspb.VERSION_DRAINAGE_STATUS_DRAINED, resp.GetWorkerDeploymentVersionInfo().GetDrainageInfo().GetStatus(),
			"Version should remain DRAINED because reactivation signal was cached (check %d)", drainedCheckCount)
		s.Equalf(enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED, resp.GetWorkerDeploymentVersionInfo().GetStatus(),
			"Version status should remain DRAINED because reactivation signal was cached (check %d)", drainedCheckCount)
		drainedCheckCount++
		return drainedCheckCount >= 5
	}, 10*time.Second, 1*time.Second)

	// Signal the workflow to complete.
	s.NoError(s.SdkClient().SignalWorkflow(ctx, wfTV2.WorkflowID(), run2.GetRunID(), "complete", nil))
	s.NoError(run2.Get(ctx, &result))
}

func (s *DeploymentVersionSuite) TestReactivationSignalCache_Deduplication_SignalWithStart() {
	s.OverrideDynamicConfig(dynamicconfig.VersionDrainageStatusVisibilityGracePeriod, testLongVersionDrainageVisibilityGracePeriod)
	s.OverrideDynamicConfig(dynamicconfig.VersionDrainageStatusRefreshInterval, testExtraLongVersionDrainageRefreshInterval)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Use shorter, explicit deployment series names to avoid truncation issues
	deploymentName := fmt.Sprintf("test-sws-cache-dedup-wfv%d", s.workflowVersion)
	tv1 := testvars.New(s).WithDeploymentSeries(deploymentName).WithBuildID(deploymentName + "-v1").WithTaskQueue("test-sws-cache-dedup-tq")
	tv2 := testvars.New(s).WithDeploymentSeries(deploymentName).WithBuildID(deploymentName + "-v2").WithTaskQueue("test-sws-cache-dedup-tq")

	// Set version 1 as current
	s.startVersionWorkflow(ctx, tv1)
	err := s.setCurrent(tv1, true)
	s.NoError(err)

	// Set version 2 as current → version 1 starts draining
	s.startVersionWorkflow(ctx, tv2)
	err = s.setCurrent(tv2, true)
	s.NoError(err)

	// Wait for version 1 to become DRAINED
	s.checkVersionDrainageAndVersionStatus(ctx, tv1,
		&deploymentpb.VersionDrainageInfo{Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINED},
		enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED,
		testLongVersionDrainageVisibilityGracePeriod+testExtraLongVersionDrainageRefreshInterval)

	// Workflow that waits for a signal before completing
	wf := func(ctx workflow.Context) (string, error) {
		workflow.GetSignalChannel(ctx, "complete").Receive(ctx, nil)
		return "done", nil
	}

	// Register worker for version 1
	w1 := worker.New(s.SdkClient(), tv1.TaskQueue().String(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			Version:       tv1.SDKDeploymentVersion(),
			UseVersioning: true,
		},
	})
	w1.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{
		Name:               "waitingWorkflow",
		VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
	})
	s.NoError(w1.Start())
	defer w1.Stop()

	// === FIRST SIGNAL WITH START: Should trigger reactivation (cache miss) ===
	wfTV1 := testvars.New(s)
	var run1 sdkclient.WorkflowRun
	s.Eventually(func() bool {
		var startErr error
		run1, startErr = s.SdkClient().SignalWithStartWorkflow(ctx,
			wfTV1.WorkflowID(),
			"start-signal",
			nil,
			sdkclient.StartWorkflowOptions{
				TaskQueue: tv1.TaskQueue().String(),
				VersioningOverride: &sdkclient.PinnedVersioningOverride{
					Version: tv1.SDKDeploymentVersion(),
				},
			},
			"waitingWorkflow",
		)
		return startErr == nil
	}, 10*time.Second, 500*time.Millisecond)

	// Version 1 should transition to DRAINING (reactivated)
	s.checkVersionDrainageAndVersionStatus(ctx, tv1,
		&deploymentpb.VersionDrainageInfo{Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINING},
		enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
		0)

	// Signal workflow to complete
	s.NoError(s.SdkClient().SignalWorkflow(ctx, wfTV1.WorkflowID(), run1.GetRunID(), "complete", nil))

	// Wait for workflow to complete
	var result string
	s.NoError(run1.Get(ctx, &result))

	// Wait for version 1 to become DRAINED again (workflow completed)
	s.checkVersionDrainageAndVersionStatus(ctx, tv1,
		&deploymentpb.VersionDrainageInfo{Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINED},
		enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED,
		testLongVersionDrainageVisibilityGracePeriod+testExtraLongVersionDrainageRefreshInterval)

	// === SECOND SIGNAL WITH START: Should NOT trigger reactivation (cache hit) ===
	wfTV2 := testvars.New(s)
	var run2 sdkclient.WorkflowRun
	s.Eventually(func() bool {
		var startErr error
		run2, startErr = s.SdkClient().SignalWithStartWorkflow(ctx,
			wfTV2.WorkflowID(),
			"start-signal",
			nil,
			sdkclient.StartWorkflowOptions{
				TaskQueue: tv1.TaskQueue().String(),
				VersioningOverride: &sdkclient.PinnedVersioningOverride{
					Version: tv1.SDKDeploymentVersion(),
				},
			},
			"waitingWorkflow",
		)
		return startErr == nil
	}, 10*time.Second, 500*time.Millisecond)

	// Verify version stays DRAINED for several checks (workflow is still running)
	// Use Eventually with a counter to check multiple times that the signal was cached
	drainedCheckCount := 0
	s.Eventually(func() bool {
		resp, err := s.describeVersion(tv1)
		s.NoError(err)
		s.Equal(enumspb.VERSION_DRAINAGE_STATUS_DRAINED, resp.GetWorkerDeploymentVersionInfo().GetDrainageInfo().GetStatus(),
			"Version should remain DRAINED because reactivation signal was cached")
		s.Equal(enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED, resp.GetWorkerDeploymentVersionInfo().GetStatus(),
			"Version status should remain DRAINED because reactivation signal was cached")
		drainedCheckCount++
		return drainedCheckCount >= 5
	}, 10*time.Second, 1*time.Second)

	// Signal the workflow to complete
	s.NoError(s.SdkClient().SignalWorkflow(ctx, wfTV2.WorkflowID(), run2.GetRunID(), "complete", nil))
	s.NoError(run2.Get(ctx, &result))
}

func (s *DeploymentVersionSuite) TestReactivationSignalCache_Deduplication_UpdateOptions() {
	s.OverrideDynamicConfig(dynamicconfig.VersionDrainageStatusVisibilityGracePeriod, testLongVersionDrainageVisibilityGracePeriod)
	s.OverrideDynamicConfig(dynamicconfig.VersionDrainageStatusRefreshInterval, testExtraLongVersionDrainageRefreshInterval)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Use shorter, explicit deployment series names to avoid truncation issues
	deploymentName := fmt.Sprintf("test-opts-cache-dedup-wfv%d", s.workflowVersion)
	tv1 := testvars.New(s).WithDeploymentSeries(deploymentName).WithBuildID(deploymentName + "-v1").WithTaskQueue("test-opts-cache-dedup-tq")
	tv2 := testvars.New(s).WithDeploymentSeries(deploymentName).WithBuildID(deploymentName + "-v2").WithTaskQueue("test-opts-cache-dedup-tq")

	// Set version 1 as current
	s.startVersionWorkflow(ctx, tv1)
	err := s.setCurrent(tv1, true)
	s.NoError(err)

	// Set version 2 as current → version 1 starts draining
	s.startVersionWorkflow(ctx, tv2)
	err = s.setCurrent(tv2, true)
	s.NoError(err)

	// Wait for version 1 to become DRAINED
	s.checkVersionDrainageAndVersionStatus(ctx, tv1,
		&deploymentpb.VersionDrainageInfo{Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINED},
		enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED,
		testLongVersionDrainageVisibilityGracePeriod+testExtraLongVersionDrainageRefreshInterval)

	// Workflow that waits for a signal before completing
	wf := func(ctx workflow.Context) (string, error) {
		workflow.GetSignalChannel(ctx, "complete").Receive(ctx, nil)
		return "done", nil
	}

	// Register worker for version 1 (DRAINED)
	w1 := worker.New(s.SdkClient(), tv1.TaskQueue().String(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			Version:       tv1.SDKDeploymentVersion(),
			UseVersioning: true,
		},
	})
	w1.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{
		Name:               "waitingWorkflow",
		VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
	})
	s.NoError(w1.Start())
	defer w1.Stop()

	// Register worker for version 2 (CURRENT) on the same task queue
	w2 := worker.New(s.SdkClient(), tv1.TaskQueue().String(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			Version:       tv2.SDKDeploymentVersion(),
			UseVersioning: true,
		},
	})
	w2.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{
		Name:               "waitingWorkflow",
		VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
	})
	s.NoError(w2.Start())
	defer w2.Stop()

	s.waitForPollers(ctx, tv1, tv2)

	pinnedOverride := &workflowpb.VersioningOverride{
		Override: &workflowpb.VersioningOverride_Pinned{
			Pinned: &workflowpb.VersioningOverride_PinnedOverride{
				Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
				Version:  tv1.ExternalDeploymentVersion(),
			},
		},
	}

	// === FIRST UPDATE OPTIONS: Should trigger reactivation (cache miss) ===
	// Start workflow on v2 (current version) - waits for signal to complete
	wfTV1 := testvars.New(s)
	run1, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue: tv1.TaskQueue().String(),
		ID:        wfTV1.WorkflowID(),
	}, "waitingWorkflow")
	s.NoError(err)

	// Pin the workflow to v1 using UpdateWorkflowExecutionOptions
	s.Eventually(func() bool {
		_, err = s.FrontendClient().UpdateWorkflowExecutionOptions(ctx,
			&workflowservice.UpdateWorkflowExecutionOptionsRequest{
				Namespace: s.Namespace().String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: wfTV1.WorkflowID(),
					RunId:      run1.GetRunID(),
				},
				WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{
					VersioningOverride: pinnedOverride,
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
			})
		return err == nil
	}, 10*time.Second, 500*time.Millisecond)

	// Signal workflow to complete
	s.NoError(s.SdkClient().SignalWorkflow(ctx, wfTV1.WorkflowID(), run1.GetRunID(), "complete", nil))

	// Wait for workflow to complete
	var result string
	s.NoError(run1.Get(ctx, &result))

	// Version 1 should transition to DRAINING (reactivated)
	s.checkVersionDrainageAndVersionStatus(ctx, tv1,
		&deploymentpb.VersionDrainageInfo{Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINING},
		enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
		0)

	// Wait for version 1 to become DRAINED again (workflow completed)
	s.checkVersionDrainageAndVersionStatus(ctx, tv1,
		&deploymentpb.VersionDrainageInfo{Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINED},
		enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED,
		testLongVersionDrainageVisibilityGracePeriod+testExtraLongVersionDrainageRefreshInterval)

	// === SECOND UPDATE OPTIONS: Should NOT trigger reactivation (cache hit) ===
	// Start another workflow on v2 - waits for signal to complete
	wfTV2 := testvars.New(s)
	run2, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue: tv1.TaskQueue().String(),
		ID:        wfTV2.WorkflowID(),
	}, "waitingWorkflow")
	s.NoError(err)

	// Pin this workflow to v1 using UpdateWorkflowExecutionOptions (should be cached)
	s.Eventually(func() bool {
		_, err = s.FrontendClient().UpdateWorkflowExecutionOptions(ctx,
			&workflowservice.UpdateWorkflowExecutionOptionsRequest{
				Namespace: s.Namespace().String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: wfTV2.WorkflowID(),
					RunId:      run2.GetRunID(),
				},
				WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{
					VersioningOverride: pinnedOverride,
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
			})
		return err == nil
	}, 10*time.Second, 500*time.Millisecond)

	// Verify version stays DRAINED for several checks (workflow is still running)
	// Use Eventually with a counter to check multiple times that the signal was cached
	drainedCheckCount := 0
	s.Eventually(func() bool {
		resp, err := s.describeVersion(tv1)
		s.NoError(err)
		s.Equal(enumspb.VERSION_DRAINAGE_STATUS_DRAINED, resp.GetWorkerDeploymentVersionInfo().GetDrainageInfo().GetStatus(),
			"Version should remain DRAINED because reactivation signal was cached")
		s.Equal(enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED, resp.GetWorkerDeploymentVersionInfo().GetStatus(),
			"Version status should remain DRAINED because reactivation signal was cached")
		drainedCheckCount++
		return drainedCheckCount >= 5
	}, 10*time.Second, 1*time.Second)

	// Signal the workflow to complete
	s.NoError(s.SdkClient().SignalWorkflow(ctx, wfTV2.WorkflowID(), run2.GetRunID(), "complete", nil))
	s.NoError(run2.Get(ctx, &result))
}

// TestReactivationSignalCache_Deduplication_Reset verifies that the version reactivation signal cache
// deduplicates signals when ResetWorkflowExecution is called multiple times with a pinned override
// to a DRAINED version.
func (s *DeploymentVersionSuite) TestReactivationSignalCache_Deduplication_Reset() {
	s.OverrideDynamicConfig(dynamicconfig.VersionDrainageStatusVisibilityGracePeriod, testLongVersionDrainageVisibilityGracePeriod)
	s.OverrideDynamicConfig(dynamicconfig.VersionDrainageStatusRefreshInterval, testExtraLongVersionDrainageRefreshInterval)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Use shorter, explicit deployment series names to avoid truncation issues
	deploymentName := fmt.Sprintf("test-reset-cache-dedup-wfv%d", s.workflowVersion)
	tv1 := testvars.New(s).WithDeploymentSeries(deploymentName).WithBuildID(deploymentName + "-v1").WithTaskQueue("test-reset-cache-dedup-tq")
	tv2 := testvars.New(s).WithDeploymentSeries(deploymentName).WithBuildID(deploymentName + "-v2").WithTaskQueue("test-reset-cache-dedup-tq")

	// Set version 1 as current
	s.startVersionWorkflow(ctx, tv1)
	err := s.setCurrent(tv1, true)
	s.NoError(err)

	// Set version 2 as current → version 1 starts draining
	s.startVersionWorkflow(ctx, tv2)
	err = s.setCurrent(tv2, true)
	s.NoError(err)

	// Wait for version 1 to become DRAINED
	s.checkVersionDrainageAndVersionStatus(ctx, tv1,
		&deploymentpb.VersionDrainageInfo{Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINED},
		enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED,
		testLongVersionDrainageVisibilityGracePeriod+testExtraLongVersionDrainageRefreshInterval)

	// Workflow that waits for a signal before completing
	wf := func(ctx workflow.Context) (string, error) {
		workflow.GetSignalChannel(ctx, "complete").Receive(ctx, nil)
		return "done", nil
	}

	// Register worker for version 1 (DRAINED)
	w1 := worker.New(s.SdkClient(), tv1.TaskQueue().String(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			Version:       tv1.SDKDeploymentVersion(),
			UseVersioning: true,
		},
	})
	w1.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{
		Name:               "waitingWorkflow",
		VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
	})
	s.NoError(w1.Start())
	defer w1.Stop()

	// Register worker for version 2 (CURRENT) on the same task queue
	w2 := worker.New(s.SdkClient(), tv1.TaskQueue().String(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			Version:       tv2.SDKDeploymentVersion(),
			UseVersioning: true,
		},
	})
	w2.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{
		Name:               "waitingWorkflow",
		VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
	})
	s.NoError(w2.Start())
	defer w2.Stop()

	s.waitForPollers(ctx, tv1, tv2)

	pinnedOverride := &workflowpb.VersioningOverride{
		Override: &workflowpb.VersioningOverride_Pinned{
			Pinned: &workflowpb.VersioningOverride_PinnedOverride{
				Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
				Version:  tv1.ExternalDeploymentVersion(),
			},
		},
	}

	// Helper function to start a workflow, wait for task completion, and get reset event ID
	startAndGetResetEventID := func(wfID string) (sdkclient.WorkflowRun, int64) {
		run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			TaskQueue: tv1.TaskQueue().String(),
			ID:        wfID,
		}, "waitingWorkflow")
		s.NoError(err)

		// Wait for workflow task completion (creates a reset point)
		s.Eventually(func() bool {
			hist := s.SdkClient().GetWorkflowHistory(ctx, wfID, run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
			for hist.HasNext() {
				event, err := hist.Next()
				if err != nil {
					return false
				}
				if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
					return true
				}
			}
			return false
		}, 10*time.Second, 200*time.Millisecond)

		// Find the reset event ID
		var resetEventID int64
		hist := s.SdkClient().GetWorkflowHistory(ctx, wfID, run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for hist.HasNext() {
			event, err := hist.Next()
			s.NoError(err)
			if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
				resetEventID = event.EventId
				break
			}
		}
		s.Positive(resetEventID)
		return run, resetEventID
	}

	// === FIRST RESET: Should trigger reactivation (cache miss) ===
	wfTV1 := testvars.New(s)
	run1, resetEventID1 := startAndGetResetEventID(wfTV1.WorkflowID())

	// Reset with pinned override to v1 (DRAINED)
	var resetResp1 *workflowservice.ResetWorkflowExecutionResponse
	s.Eventually(func() bool {
		var resetErr error
		resetResp1, resetErr = s.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: wfTV1.WorkflowID(),
				RunId:      run1.GetRunID(),
			},
			Reason:                    "testing-reset-cache-dedup-1",
			RequestId:                 uuid.NewString(),
			WorkflowTaskFinishEventId: resetEventID1,
			PostResetOperations: []*workflowpb.PostResetOperation{
				{
					Variant: &workflowpb.PostResetOperation_UpdateWorkflowOptions_{
						UpdateWorkflowOptions: &workflowpb.PostResetOperation_UpdateWorkflowOptions{
							WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{
								VersioningOverride: pinnedOverride,
							},
							UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
						},
					},
				},
			},
		})
		return resetErr == nil
	}, 10*time.Second, 500*time.Millisecond)

	// Signal the reset workflow to complete
	s.NoError(s.SdkClient().SignalWorkflow(ctx, wfTV1.WorkflowID(), resetResp1.RunId, "complete", nil))

	// Wait for workflow to complete
	resetRun1 := s.SdkClient().GetWorkflow(ctx, wfTV1.WorkflowID(), resetResp1.RunId)
	var result string
	s.NoError(resetRun1.Get(ctx, &result))

	// Version 1 should transition to DRAINING (reactivated)
	s.checkVersionDrainageAndVersionStatus(ctx, tv1,
		&deploymentpb.VersionDrainageInfo{Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINING},
		enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
		0)

	// Wait for version 1 to become DRAINED again (workflow completed)
	s.checkVersionDrainageAndVersionStatus(ctx, tv1,
		&deploymentpb.VersionDrainageInfo{Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINED},
		enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED,
		testLongVersionDrainageVisibilityGracePeriod+testExtraLongVersionDrainageRefreshInterval)

	// === SECOND RESET: Should NOT trigger reactivation (cache hit) ===
	wfTV2 := testvars.New(s)
	run2, resetEventID2 := startAndGetResetEventID(wfTV2.WorkflowID())

	// Reset with pinned override to v1 (should be cached)
	var resetResp2 *workflowservice.ResetWorkflowExecutionResponse
	s.Eventually(func() bool {
		var resetErr error
		resetResp2, resetErr = s.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: wfTV2.WorkflowID(),
				RunId:      run2.GetRunID(),
			},
			Reason:                    "testing-reset-cache-dedup-2",
			RequestId:                 uuid.NewString(),
			WorkflowTaskFinishEventId: resetEventID2,
			PostResetOperations: []*workflowpb.PostResetOperation{
				{
					Variant: &workflowpb.PostResetOperation_UpdateWorkflowOptions_{
						UpdateWorkflowOptions: &workflowpb.PostResetOperation_UpdateWorkflowOptions{
							WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{
								VersioningOverride: pinnedOverride,
							},
							UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
						},
					},
				},
			},
		})
		return resetErr == nil
	}, 10*time.Second, 500*time.Millisecond)

	// Verify version stays DRAINED for several checks (reset workflow is still running)
	// Use Eventually with a counter to check multiple times that the signal was cached
	drainedCheckCount := 0
	s.Eventually(func() bool {
		resp, err := s.describeVersion(tv1)
		s.NoError(err)
		s.Equal(enumspb.VERSION_DRAINAGE_STATUS_DRAINED, resp.GetWorkerDeploymentVersionInfo().GetDrainageInfo().GetStatus(),
			"Version should remain DRAINED because reactivation signal was cached")
		s.Equal(enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED, resp.GetWorkerDeploymentVersionInfo().GetStatus(),
			"Version status should remain DRAINED because reactivation signal was cached")
		drainedCheckCount++
		return drainedCheckCount >= 5
	}, 10*time.Second, 1*time.Second)

	// Signal the reset workflow to complete
	s.NoError(s.SdkClient().SignalWorkflow(ctx, wfTV2.WorkflowID(), resetResp2.RunId, "complete", nil))

	// Wait for workflow to complete
	resetRun2 := s.SdkClient().GetWorkflow(ctx, wfTV2.WorkflowID(), resetResp2.RunId)
	var result2 string
	s.NoError(resetRun2.Get(ctx, &result2))
}
