package xdc

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/workflowservice/v1"
	wciclient "go.temporal.io/auto-scaled-workers/wci/client"
	wcimetrics "go.temporal.io/auto-scaled-workers/wci/metrics"
	"go.temporal.io/auto-scaled-workers/wci/workflow/iface"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm/lib/wci"
	wcipb "go.temporal.io/server/chasm/lib/wci/gen/wcipb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	wciEast = "aws-us-east-1"
	wciWest = "aws-us-west-2"
)

// WCIFailoverSuite runs a CHASM WCI across two clusters, one per region (ha-plan 8e).
type WCIFailoverSuite struct {
	xdcBaseSuite
	restorePollInterval func()
}

// wciPollInterval stands in for the 5-minute first-poll delay and overdue margin. It must outlast a cluster
// metadata refresh, so the replication cut lands before the first poll.
const wciPollInterval = 8 * time.Second

func TestWCIFailoverSuite(t *testing.T) {
	t.Parallel()
	s := &WCIFailoverSuite{}
	s.enableTransitionHistory = true
	suite.Run(t, s)
}

func (s *WCIFailoverSuite) SetupSuite() {
	s.restorePollInterval = wci.SetMaxPollIntervalForTest(wciPollInterval)
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		// Replication can be cut while the clusters stay connected, as in a real outage (ha-plan 8e.0b); the cut
		// takes effect on the next metadata refresh.
		dynamicconfig.EnableSeparateReplicationEnableFlag.Key():  true,
		dynamicconfig.ClusterMetadataRefreshInterval.Key():       time.Second,
		dynamicconfig.StandbyTaskMissingEventsDiscardDelay.Key(): 2 * time.Second,
		dynamicconfig.EnableChasm.Key():                          true,
		wciclient.WorkerControllerEnabled.Key():                  true,
		wciclient.WorkerControllerEnabledComputeProviders.Key():  []string{string(iface.ComputeProviderTypeTestInvoke)},
	}
	s.setupSuite()
	// Each cluster's history serves its own region.
	s.clusters[0].OverrideDynamicConfig(s.T(), wci.HostRegion, wciEast)
	s.clusters[1].OverrideDynamicConfig(s.T(), wci.HostRegion, wciWest)
}

func (s *WCIFailoverSuite) SetupTest() { s.setupTest() }
func (s *WCIFailoverSuite) TearDownSuite() {
	s.tearDownSuite()
	s.restorePollInterval()
}

type wciRef struct {
	ns, nsID, instanceID string
}

// createRegionalWCI creates a WCI with an "east" and a "west" group, each serving WORKFLOW in its own region.
func (s *WCIFailoverSuite) createRegionalWCI() wciRef {
	ns := s.createGlobalNamespace()
	nsResp, err := s.clusters[0].FrontendClient().DescribeNamespace(testcore.NewContext(), &workflowservice.DescribeNamespaceRequest{Namespace: ns})
	s.Require().NoError(err)
	ref := wciRef{ns: ns, nsID: nsResp.GetNamespaceInfo().GetId(), instanceID: "wci-xdc-" + uuid.NewString()[:8]}

	upserts := map[string]*wcipb.ScalingGroupSpecUpdate{}
	for id, region := range map[string]string{"east": wciEast, "west": wciWest} {
		upserts[id] = &wcipb.ScalingGroupSpecUpdate{Spec: &wcipb.ScalingGroupSpec{
			TaskTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
			RegionIds: []string{region},
			Compute:   &wcipb.ComputeProviderSpec{ProviderType: string(iface.ComputeProviderTypeTestInvoke)},
			Scaling:   &wcipb.ScalingAlgorithmSpec{ScalingAlgorithm: string(iface.ScalingAlgorithmNoSync)},
		}}
	}
	_, err = s.clusters[0].WCIClient().UpdateWorkerControllerInstance(testcore.NewContext(), &wcipb.UpdateWorkerControllerInstanceRequest{
		NamespaceId: ref.nsID, InstanceId: ref.instanceID, DeploymentName: "dep", BuildId: ref.instanceID,
		Identity: "xdc", UpsertScalingGroups: upserts,
	})
	s.Require().NoError(err)

	desc := &adminservice.DescribeMutableStateRequest{
		Namespace: ns, Execution: &commonpb.WorkflowExecution{WorkflowId: ref.instanceID}, Archetype: wci.LibraryName + "." + wci.InstanceComponent,
	}
	s.Require().Eventually(func() bool {
		_, err := s.clusters[1].AdminClient().DescribeMutableState(testcore.NewContext(), desc)
		return err == nil
	}, 10*time.Second, 100*time.Millisecond, "replicated to the standby")
	return ref
}

func (s *WCIFailoverSuite) recordTaskAdd(c int, ref wciRef) {
	_, err := s.clusters[c].WCIClient().RecordTaskAdd(testcore.NewContext(), &wcipb.RecordTaskAddRequest{
		NamespaceId: ref.nsID, InstanceId: ref.instanceID,
		Event: &wcipb.TaskAddEvent{TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW},
	})
	s.Require().NoError(err)
}

// lastTaskAdds reads, on cluster c, when each group last handled a task-add.
func (s *WCIFailoverSuite) lastTaskAdds(c int, ref wciRef) map[string]*timestamppb.Timestamp {
	resp, err := s.clusters[c].WCIClient().DescribeWorkerControllerInstance(testcore.NewContext(), &wcipb.DescribeWorkerControllerInstanceRequest{
		NamespaceId: ref.nsID, InstanceId: ref.instanceID,
	})
	s.Require().NoError(err)
	out := map[string]*timestamppb.Timestamp{}
	for id, o := range resp.GetTaskQueueObservations() {
		out[id] = o.GetLastTaskAddTime()
	}
	return out
}

// After a failover the new active cluster's region scales: its group takes the task-adds.
func (s *WCIFailoverSuite) TestFailoverMovesScalingToTheNewRegion() {
	ref := s.createRegionalWCI()

	s.recordTaskAdd(0, ref)
	before := s.lastTaskAdds(0, ref)
	s.Require().NotNil(before["east"], "east scales while cluster 0 is active")
	s.Require().Nil(before["west"])

	s.failover(ref.ns, 0, s.clusters[1].ClusterName(), 2)
	s.EventuallyWithT(func(t *assert.CollectT) {
		_, err := s.clusters[1].WCIClient().RecordTaskAdd(testcore.NewContext(), &wcipb.RecordTaskAddRequest{
			NamespaceId: ref.nsID, InstanceId: ref.instanceID,
			Event: &wcipb.TaskAddEvent{TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		})
		require.NoError(t, err)
	}, 10*time.Second, 200*time.Millisecond)

	after := s.lastTaskAdds(1, ref)
	s.Require().NotNil(after["west"], "west scales once cluster 1 is active")
	s.Require().True(before["east"].AsTime().Equal(after["east"].AsTime()), "east took no task-add after the failover")
}

// setReplication turns replication between the clusters on or off, keeping them connected.
func (s *WCIFailoverSuite) setReplication(enable bool) {
	for i, j := range []int{1, 0} {
		_, err := s.clusters[i].AdminClient().AddOrUpdateRemoteCluster(testcore.NewContext(), &adminservice.AddOrUpdateRemoteClusterRequest{
			FrontendAddress:               s.clusters[j].Host().RemoteFrontendGRPCAddress(),
			FrontendHttpAddress:           s.clusters[j].Host().FrontendHTTPAddress(),
			EnableRemoteClusterConnection: true,
			EnableReplication:             enable,
		})
		s.Require().NoError(err)
	}
}

// polls counts the metrics polls cluster c's history ran for the WCI since capture started.
func polls(capture *metricstest.Capture, ref wciRef) int {
	n := 0
	for _, r := range capture.Snapshot()[wcimetrics.Operations.Name()] {
		if r.Tags[wcimetrics.OperationTagName] == wcimetrics.OperationTypePullStats && r.Tags[wcimetrics.WorkerDeploymentBuildIDTag] == ref.instanceID {
			n++
		}
	}
	return n
}

// An outage longer than the standby discard delay loses the poll timer: the standby discards it and the
// failover doesn't re-queue it. The next task-add re-arms it (ha-plan 8d.2 item 7); an idle WCI stays stuck
// until the CHASM framework re-queues discarded timers.
func (s *WCIFailoverSuite) TestOutageLosesThePollTimerUntilATaskAdd() {
	ref := s.createRegionalWCI()
	s.setReplication(false)
	defer s.setReplication(true)

	// The active cluster polls at create + wciPollInterval; the standby's copy waits, then is discarded.
	time.Sleep(wciPollInterval + 2*time.Second + 3*time.Second) //nolint:forbidigo // past the first poll and the discard delay

	_, err := s.clusters[1].FrontendClient().UpdateNamespace(testcore.NewContext(), &workflowservice.UpdateNamespaceRequest{
		Namespace:         ref.ns,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{ActiveClusterName: s.clusters[1].ClusterName()},
	})
	s.Require().NoError(err, "failover issued on the standby")
	s.waitForNamespaceCacheRefresh()

	// The suite owns these clusters, and polls() filters to this WCI, so cluster-global capture is fine here.
	handler := s.clusters[1].Host().CaptureMetricsHandler() //nolint:staticcheck // xdc has no TestEnv capture helper
	capture := handler.StartCapture()
	defer handler.StopCapture(capture)
	time.Sleep(3 * wciPollInterval) //nolint:forbidigo // several poll intervals
	s.Require().Zero(polls(capture, ref), "the discarded poll timer isn't re-queued on the new active cluster")

	s.recordTaskAdd(1, ref)
	s.Require().Eventually(func() bool { return polls(capture, ref) > 0 }, 3*wciPollInterval, 200*time.Millisecond,
		"a task-add re-arms the overdue poll")
}
