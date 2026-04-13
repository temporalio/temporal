package xdc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/tests"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	chasmTestTimeout = 30 * time.Second * debug.TimeoutMultiplier
)

type ChasmSuite struct {
	xdcBaseSuite

	chasmContext context.Context
}

func TestChasmSuite(t *testing.T) {
	t.Parallel()

	s := &ChasmSuite{}
	s.enableTransitionHistory = true
	suite.Run(t, s)
}

func (s *ChasmSuite) SetupSuite() {
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.EnableChasm.Key():                      true,
		activity.Enabled.Key():                               true,
		dynamicconfig.ChasmStandbyTaskDiscardDelay.Key():     1 * time.Second,
		dynamicconfig.TransferProcessorMaxPollInterval.Key(): 1 * time.Second,
		dynamicconfig.NamespaceMinRetentionGlobal.Key():      1 * time.Second,
	}
	s.setupSuite()
}

func (s *ChasmSuite) SetupTest() {
	s.setupTest()

	chasmEngine, err := s.clusters[0].Host().ChasmEngine()
	s.Require().NoError(err)
	s.Require().NotNil(chasmEngine)

	chasmVisibilityMgr := s.clusters[0].Host().ChasmVisibilityManager()
	s.Require().NotNil(chasmVisibilityMgr)

	s.chasmContext = chasm.NewEngineContext(context.Background(), chasmEngine)
	s.chasmContext = chasm.NewVisibilityManagerContext(s.chasmContext, chasmVisibilityMgr)
}

func (s *ChasmSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *ChasmSuite) TestDeleteExecution_RunningExecution() {
	nsName := s.createGlobalNamespace()

	nsResp, err := s.clusters[0].FrontendClient().DescribeNamespace(testcore.NewContext(), &workflowservice.DescribeNamespaceRequest{
		Namespace: nsName,
	})
	s.NoError(err)
	nsID := nsResp.NamespaceInfo.GetId()

	tv := testvars.New(s.T())
	storeID := tv.Any().String()

	ctx, cancel := context.WithTimeout(s.chasmContext, chasmTestTimeout)
	defer cancel()

	_, err = tests.NewPayloadStoreHandler(
		ctx,
		tests.NewPayloadStoreRequest{
			NamespaceID:      namespace.ID(nsID),
			StoreID:          storeID,
			IDReusePolicy:    chasm.BusinessIDReusePolicyRejectDuplicate,
			IDConflictPolicy: chasm.BusinessIDConflictPolicyFail,
		},
	)
	s.NoError(err)

	chasmRegistry := s.clusters[0].Host().GetCHASMRegistry()
	archetypeID, ok := chasmRegistry.ComponentIDFor(&tests.PayloadStore{})
	s.True(ok)
	archetype, ok := chasmRegistry.ComponentFqnByID(archetypeID)
	s.True(ok)

	describeExecutionRequest := &adminservice.DescribeMutableStateRequest{
		Namespace: nsName,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: storeID,
		},
		Archetype: archetype,
	}
	_, err = s.clusters[0].AdminClient().DescribeMutableState(testcore.NewContext(), describeExecutionRequest)
	s.NoError(err)

	s.Eventually(func() bool {
		_, err = s.clusters[1].AdminClient().DescribeMutableState(testcore.NewContext(), describeExecutionRequest)
		return err == nil
	}, 10*time.Second, 100*time.Millisecond)

	err = tests.DeletePayloadStoreHandler(
		ctx,
		tests.DeletePayloadStoreRequest{
			NamespaceID: namespace.ID(nsID),
			StoreID:     storeID,
			Reason:      "xdc test deletion",
			Identity:    "test-identity",
		},
	)
	s.NoError(err)

	// Active cluster should fully delete the execution.
	s.Eventually(func() bool {
		_, err = s.clusters[0].AdminClient().DescribeMutableState(testcore.NewContext(), describeExecutionRequest)
		return errors.As(err, new(*serviceerror.NotFound))
	}, 10*time.Second, 100*time.Millisecond)

	// Standby cluster receives the close via replication but won't process
	// the DeleteExecutionTask itself; it will be cleaned up by the retention timer.
	// Verify the execution is terminated on the standby.
	s.Eventually(func() bool {
		resp, err := s.clusters[1].AdminClient().DescribeMutableState(testcore.NewContext(), describeExecutionRequest)
		if err != nil {
			// Execution may already be gone if replication of delete happened.
			return errors.As(err, new(*serviceerror.NotFound))
		}
		return resp.GetDatabaseMutableState().GetExecutionState().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED
	}, 10*time.Second, 100*time.Millisecond)
}

func (s *ChasmSuite) TestRetentionTimer() {
	nsName := s.createGlobalNamespace()

	nsResp, err := s.clusters[0].FrontendClient().DescribeNamespace(testcore.NewContext(), &workflowservice.DescribeNamespaceRequest{
		Namespace: nsName,
	})
	s.NoError(err)
	nsID := nsResp.NamespaceInfo.GetId()

	tv := testvars.New(s.T())
	storeID := tv.Any().String()

	ctx, cancel := context.WithTimeout(s.chasmContext, chasmTestTimeout)
	defer cancel()

	_, err = tests.NewPayloadStoreHandler(
		ctx,
		tests.NewPayloadStoreRequest{
			NamespaceID:      namespace.ID(nsID),
			StoreID:          storeID,
			IDReusePolicy:    chasm.BusinessIDReusePolicyRejectDuplicate,
			IDConflictPolicy: chasm.BusinessIDConflictPolicyFail,
		},
	)
	s.NoError(err)

	chasmRegistry := s.clusters[0].Host().GetCHASMRegistry()
	archetypeID, ok := chasmRegistry.ComponentIDFor(&tests.PayloadStore{})
	s.True(ok)
	archetype, ok := chasmRegistry.ComponentFqnByID(archetypeID)
	s.True(ok)

	describeExecutionRequest := &adminservice.DescribeMutableStateRequest{
		Namespace: nsName,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: storeID,
		},
		Archetype: archetype,
	}
	_, err = s.clusters[0].AdminClient().DescribeMutableState(testcore.NewContext(), describeExecutionRequest)
	s.NoError(err)

	s.Eventually(func() bool {
		// Wait for it to be replicated to the standby cluster
		_, err = s.clusters[1].AdminClient().DescribeMutableState(testcore.NewContext(), describeExecutionRequest)
		return err == nil
	}, 10*time.Second, 100*time.Millisecond)

	// Reduce namespace retention to trigger deletion
	retention := 3 * time.Second
	_, err = s.clusters[0].FrontendClient().UpdateNamespace(testcore.NewContext(), &workflowservice.UpdateNamespaceRequest{
		Namespace: nsName,
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: durationpb.New(retention),
		},
	})
	s.NoError(err)

	// Wait for ns update to be replicated
	s.Eventually(func() bool {
		// Wait for it to be replicated to the standby cluster
		resp, err := s.clusters[1].FrontendClient().DescribeNamespace(testcore.NewContext(), &workflowservice.DescribeNamespaceRequest{
			Namespace: nsName,
		})
		s.NoError(err)
		return resp.GetConfig().GetWorkflowExecutionRetentionTtl().AsDuration() == retention
	}, 10*time.Second, 100*time.Millisecond)

	// Wait for ns registry refresh
	time.Sleep(2 * testcore.NamespaceCacheRefreshInterval) //nolint:forbidigo

	// Close the execution and validate it's deleted on both clusters.
	_, err = tests.ClosePayloadStoreHandler(
		ctx,
		tests.ClosePayloadStoreRequest{
			NamespaceID: namespace.ID(nsID),
			StoreID:     storeID,
		},
	)
	s.NoError(err)

	for _, cluster := range []*testcore.TestCluster{s.clusters[0], s.clusters[1]} {
		s.Eventually(func() bool {
			// Wait for replication, retention period, and retention timer task processing.
			_, err = cluster.AdminClient().DescribeMutableState(testcore.NewContext(), describeExecutionRequest)
			return errors.As(err, new(*serviceerror.NotFound))
		}, 10*time.Second, 100*time.Millisecond)
	}
}

// TestActivityDispatchTaskStandbySpillover verifies that the standby transfer queue executor's spillover path works
// end-to-end for ActivityDispatchTask.
//
// Test flow:
//  1. Start a standalone activity on cluster 0 (active). This generates an ActivityDispatchTask.
//  2. Wait for replication to cluster 1 (standby).
//  3. Verify Discard fired by checking that the activity task appears in cluster 1's matching backlog.
//  4. Failover namespace to cluster 1, as SAA tasks are only pollable on active clusters
//  5. Poll the activity task on cluster 1 (now active).
//  6. Complete the activity from cluster 1.
//  7. Verify completion status on both clusters.
func (s *ChasmSuite) TestActivityDispatchTaskStandbySpillover() {
	nsName := s.createGlobalNamespace()

	tv := testvars.New(s.T())
	activityID := tv.Any().String()
	taskQueue := tv.Any().String()

	ctx, cancel := context.WithTimeout(context.Background(), chasmTestTimeout)
	defer cancel()

	// Start standalone activity on cluster 0 (active), creating an ActivityDispatchTask on immediate transfer queue.
	startResp, err := s.clusters[0].FrontendClient().StartActivityExecution(
		ctx,
		&workflowservice.StartActivityExecutionRequest{
			Namespace:  nsName,
			ActivityId: activityID,
			ActivityType: &commonpb.ActivityType{
				Name: "test-activity-type",
			},
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
			},
			Input:               payloads.EncodeString("test-input"),
			StartToCloseTimeout: durationpb.New(1 * time.Minute),
			Identity:            "test-worker",
		},
	)
	s.NoError(err)
	s.NotEmpty(startResp.GetRunId())

	// Wait for replication to cluster 1 (standby).
	describeExecutionRequest := &adminservice.DescribeMutableStateRequest{
		Namespace: nsName,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: activityID,
		},
		Archetype: activity.Archetype,
	}
	s.Eventually(func() bool {
		_, err = s.clusters[1].AdminClient().DescribeMutableState(testcore.NewContext(), describeExecutionRequest)
		return err == nil
	}, 10*time.Second, 100*time.Millisecond)

	// Verify Discard fired on cluster 1 (standby) by checking that the activity task was pushed into cluster 1's
	// matching backlog
	s.Eventually(func() bool {
		for partitionID := range int32(dynamicconfig.GlobalDefaultNumTaskQueuePartitions) {
			res, err := s.clusters[1].AdminClient().DescribeTaskQueuePartition(ctx,
				&adminservice.DescribeTaskQueuePartitionRequest{
					Namespace: nsName,
					TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
						TaskQueue:     taskQueue,
						TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY,
						PartitionId:   &taskqueuespb.TaskQueuePartition_NormalPartitionId{NormalPartitionId: partitionID},
					},
					BuildIds: &taskqueuepb.TaskQueueVersionSelection{Unversioned: true},
				})
			if err != nil {
				continue
			}
			for _, vi := range res.VersionsInfoInternal {
				for _, st := range vi.PhysicalTaskQueueInfo.InternalTaskQueueStatus {
					if st.ApproximateBacklogCount > 0 {
						return true
					}
				}
			}
		}
		return false
	}, 5*time.Second, 200*time.Millisecond)

	// Failover namespace to cluster 1. Matching on standby only serves query
	// tasks (not activity tasks), so the pre-positioned task only becomes
	// pollable once cluster 1 becomes the new active.
	s.failover(nsName, 0, s.clusters[1].ClusterName(), 2)

	// Poll activity task on cluster 1 (now active after failover).
	var pollResp *workflowservice.PollActivityTaskQueueResponse
	s.Eventually(func() bool {
		pollCtx, pollCancel := context.WithTimeout(ctx, 3*time.Second)
		defer pollCancel()
		pollResp, err = s.clusters[1].FrontendClient().PollActivityTaskQueue(
			pollCtx,
			&workflowservice.PollActivityTaskQueueRequest{
				Namespace: nsName,
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: taskQueue,
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
				Identity: "standby-worker",
			},
		)
		return err == nil && len(pollResp.GetTaskToken()) > 0
	}, 10*time.Second, 500*time.Millisecond)

	// Complete the activity from cluster 1.
	_, err = s.clusters[1].FrontendClient().RespondActivityTaskCompleted(
		ctx,
		&workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: nsName,
			TaskToken: pollResp.GetTaskToken(),
			Result:    payloads.EncodeString("done"),
			Identity:  "standby-worker",
		},
	)
	s.NoError(err)

	// Verify completion on both clusters.
	for _, cluster := range s.clusters {
		s.Eventually(func() bool {
			resp, err := cluster.FrontendClient().DescribeActivityExecution(
				ctx,
				&workflowservice.DescribeActivityExecutionRequest{
					Namespace:  nsName,
					ActivityId: activityID,
					RunId:      startResp.GetRunId(),
				},
			)
			if err != nil {
				return false
			}
			return resp.GetInfo().GetStatus() == enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED
		}, 10*time.Second, 100*time.Millisecond)
	}
}

// TestDeleteExecution_ReplicatedToStandby verifies that deleting a non-workflow Chasm execution
// (PayloadStore) on the active cluster replicates the deletion to the standby cluster.
func (s *ChasmSuite) TestDeleteExecution_ReplicatedToStandby() {
	for _, cluster := range s.clusters {
		cluster.OverrideDynamicConfig(s.T(), dynamicconfig.EnableDeleteWorkflowExecutionReplication, true)
	}

	nsName := s.createGlobalNamespace()

	nsResp, err := s.clusters[0].FrontendClient().DescribeNamespace(testcore.NewContext(), &workflowservice.DescribeNamespaceRequest{
		Namespace: nsName,
	})
	s.NoError(err)
	nsID := nsResp.NamespaceInfo.GetId()

	tv := testvars.New(s.T())
	storeID := tv.Any().String()

	ctx, cancel := context.WithTimeout(s.chasmContext, chasmTestTimeout)
	defer cancel()

	_, err = tests.NewPayloadStoreHandler(
		ctx,
		tests.NewPayloadStoreRequest{
			NamespaceID:      namespace.ID(nsID),
			StoreID:          storeID,
			IDReusePolicy:    chasm.BusinessIDReusePolicyRejectDuplicate,
			IDConflictPolicy: chasm.BusinessIDConflictPolicyFail,
		},
	)
	s.NoError(err)

	chasmRegistry := s.clusters[0].Host().GetCHASMRegistry()
	archetypeID, ok := chasmRegistry.ComponentIDFor(&tests.PayloadStore{})
	s.True(ok)
	archetype, ok := chasmRegistry.ComponentFqnByID(archetypeID)
	s.True(ok)

	describeExecutionRequest := &adminservice.DescribeMutableStateRequest{
		Namespace: nsName,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: storeID,
		},
		Archetype: archetype,
	}

	s.Eventually(func() bool {
		_, err = s.clusters[1].AdminClient().DescribeMutableState(testcore.NewContext(), describeExecutionRequest)
		return err == nil
	}, 10*time.Second, 100*time.Millisecond)

	err = tests.DeletePayloadStoreHandler(
		ctx,
		tests.DeletePayloadStoreRequest{
			NamespaceID: namespace.ID(nsID),
			StoreID:     storeID,
			Reason:      "xdc delete replication test",
			Identity:    "test-identity",
		},
	)
	s.NoError(err)

	// Verify Chasm deletion on both clusters.
	for _, cluster := range s.clusters {
		s.Eventually(func() bool {
			_, err = cluster.AdminClient().DescribeMutableState(testcore.NewContext(), describeExecutionRequest)
			return errors.As(err, new(*serviceerror.NotFound))
		}, 10*time.Second, 100*time.Millisecond)
	}
}

// TODO Add test for ActivityDispatchTask timer task queue spillover when we have SAA scheduling support.
