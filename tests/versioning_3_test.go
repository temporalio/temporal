package tests

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/testing/protoutils"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/matching"
	"go.temporal.io/server/service/worker/workerdeployment"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var _ = testhooks.MatchingIgnoreRoutingConfigRevisionCheck

type versionStatus int

const (
	tqTypeWf        = enumspb.TASK_QUEUE_TYPE_WORKFLOW
	tqTypeAct       = enumspb.TASK_QUEUE_TYPE_ACTIVITY
	tqTypeNexus     = enumspb.TASK_QUEUE_TYPE_NEXUS
	vbUnspecified   = enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED
	vbPinned        = enumspb.VERSIONING_BEHAVIOR_PINNED
	vbUnpinned      = enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE
	ver3MinPollTime = common.MinLongPollTimeout + time.Millisecond*200

	versionStatusNil      = versionStatus(0)
	versionStatusInactive = versionStatus(1)
	versionStatusCurrent  = versionStatus(2)
	versionStatusRamping  = versionStatus(3)
	versionStatusDraining = versionStatus(4)
	versionStatusDrained  = versionStatus(5)
)

type Versioning3Suite struct {
	testcore.FunctionalTestBase
	useV32                    bool
	deploymentWorkflowVersion workerdeployment.DeploymentWorkflowVersion
	useRevisionNumbers        bool
	useNewDeploymentData      bool
}

func TestVersioning3FunctionalSuiteV0(t *testing.T) {
	t.Parallel()
	suite.Run(t, &Versioning3Suite{
		deploymentWorkflowVersion: workerdeployment.InitialVersion,
		useV32:                    true,
		useNewDeploymentData:      false,
		useRevisionNumbers:        false,
	})
}

func TestVersioning3FunctionalSuiteV2(t *testing.T) {
	t.Parallel()
	suite.Run(t, &Versioning3Suite{
		deploymentWorkflowVersion: workerdeployment.VersionDataRevisionNumber,
		useV32:                    true,
		useRevisionNumbers:        true,
		useNewDeploymentData:      true,
	})
}

func (s *Versioning3Suite) SetupSuite() {
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		dynamicconfig.MatchingDeploymentWorkflowVersion.Key():    int(s.deploymentWorkflowVersion),
		dynamicconfig.UseRevisionNumberForWorkerVersioning.Key(): s.useRevisionNumbers,
		dynamicconfig.MatchingForwarderMaxChildrenPerNode.Key():  partitionTreeDegree,

		// Make sure we don't hit the rate limiter in tests
		dynamicconfig.FrontendGlobalNamespaceNamespaceReplicationInducingAPIsRPS.Key():                1000,
		dynamicconfig.FrontendMaxNamespaceNamespaceReplicationInducingAPIsBurstRatioPerInstance.Key(): 1,
		dynamicconfig.FrontendNamespaceReplicationInducingAPIsRPS.Key():                               1000,

		// this is overridden for tests using RunTestWithMatchingBehavior
		dynamicconfig.MatchingNumTaskqueueReadPartitions.Key():  4,
		dynamicconfig.MatchingNumTaskqueueWritePartitions.Key(): 4,

		// Overriding the number of deployments that can be registered in a single namespace. Done only for this test suite
		// since it creates a large number of unique deployments in the test suite's namespace.
		dynamicconfig.MatchingMaxDeployments.Key(): 1000,

		// Use new matcher for versioning tests. Ideally we would run everything with old and new,
		// but for now we pick a subset of tests. Versioning tests exercise the most features of
		// matching so they're a good condidate.
		dynamicconfig.MatchingUseNewMatcher.Key(): true,
	}
	s.FunctionalTestBase.SetupSuiteWithCluster(testcore.WithDynamicConfigOverrides(dynamicConfigOverrides))
}

func (s *Versioning3Suite) TestPinnedTask_NoProperPoller() {
	s.RunTestWithMatchingBehavior(
		func() {
			tv := testvars.New(s)

			tv2 := tv.WithBuildIDNumber(2)
			go s.idlePollWorkflow(context.Background(), tv2, true, ver3MinPollTime, "second deployment should not receive pinned task")

			// Start a versioned poller for the first version so that it registers the version in the task queue.
			pollerCtx, cancelPoller := context.WithCancel(context.Background())
			go s.idlePollWorkflow(pollerCtx, tv, true, ver3MinPollTime, "first deployment should not receive any task. It is just creating a version in the task queue.")

			// Wait for the version to be present in the task queue
			s.validatePinnedVersionExistsInTaskQueue(tv)

			// Cancel the poller after condition is met
			cancelPoller()

			s.startWorkflow(tv, tv.VersioningOverridePinned(s.useV32))
			s.idlePollWorkflow(context.Background(), tv, false, ver3MinPollTime, "unversioned worker should not receive pinned task")

			// Sleeping to let the pollers arrive to server before ending the test.
			time.Sleep(200 * time.Millisecond) //nolint:forbidigo
		})
}

func (s *Versioning3Suite) TestUnpinnedTask_NonCurrentDeployment() {
	s.RunTestWithMatchingBehavior(
		func() {
			tv := testvars.New(s)
			go s.idlePollWorkflow(context.Background(), tv, true, ver3MinPollTime, "non-current versioned poller should not receive unpinned task")

			s.startWorkflow(tv, nil)

			// Sleeping to let the pollers arrive to server before ending the test.
			time.Sleep(200 * time.Millisecond) //nolint:forbidigo
		})
}

func (s *Versioning3Suite) TestUnpinnedTask_OldDeployment() {
	if s.useNewDeploymentData == true {
		s.RunTestWithMatchingBehavior(
			func() {
				tv := testvars.New(s)
				tvOldDeployment := tv.WithBuildIDNumber(1)
				tvNewDeployment := tv.WithBuildIDNumber(2)

				// previous current deployment
				s.updateTaskQueueDeploymentDataWithRoutingConfig(tvOldDeployment, &deploymentpb.RoutingConfig{
					CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tvOldDeployment.DeploymentVersionString()),
					CurrentVersionChangedTime: timestamp.TimePtr(time.Now()),
					RevisionNumber:            1,
				}, map[string]*deploymentspb.WorkerDeploymentVersionData{tvOldDeployment.DeploymentVersion().GetBuildId(): {
					Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
				}}, []string{}, tqTypeWf)

				// current deployment
				s.updateTaskQueueDeploymentDataWithRoutingConfig(tvNewDeployment, &deploymentpb.RoutingConfig{
					CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tvNewDeployment.DeploymentVersionString()),
					CurrentVersionChangedTime: timestamp.TimePtr(time.Now()),
					RevisionNumber:            2,
				}, map[string]*deploymentspb.WorkerDeploymentVersionData{tvNewDeployment.DeploymentVersion().GetBuildId(): {
					Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
				}}, []string{}, tqTypeWf)

				s.startWorkflow(tv, nil)

				s.idlePollWorkflow(
					context.Background(),
					tvOldDeployment,
					true,
					ver3MinPollTime,
					"old deployment should not receive unpinned task",
				)
				// Sleeping to let the pollers arrive to server before ending the test.
				time.Sleep(200 * time.Millisecond) //nolint:forbidigo
			},
		)
	} else {
		s.RunTestWithMatchingBehavior(
			func() {
				tv := testvars.New(s)
				tvOldDeployment := tv.WithBuildIDNumber(1)
				tvNewDeployment := tv.WithBuildIDNumber(2)
				// previous current deployment
				s.updateTaskQueueDeploymentData(tvOldDeployment, true, 0, false, time.Minute, tqTypeWf)
				// current deployment
				s.updateTaskQueueDeploymentData(tvNewDeployment, true, 0, false, 0, tqTypeWf)

				s.startWorkflow(tv, nil)

				s.idlePollWorkflow(
					context.Background(),
					tvOldDeployment,
					true,
					ver3MinPollTime,
					"old deployment should not receive unpinned task",
				)
				// Sleeping to let the pollers arrive to server before ending the test.
				time.Sleep(200 * time.Millisecond) //nolint:forbidigo
			},
		)
	}
}

func (s *Versioning3Suite) TestSessionActivityResourceSpecificTaskQueueNotRegisteredInVersion() {
	tv := testvars.New(s)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	const (
		wfName  = "session-wf"
		actName = "session-act"
	)

	wf := func(ctx workflow.Context) (string, error) {
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: time.Minute,
		})

		sessionCtx, err := workflow.CreateSession(ctx, &workflow.SessionOptions{
			CreationTimeout:  30 * time.Second,
			ExecutionTimeout: 30 * time.Second,
		})
		if err != nil {
			return "", err
		}
		defer workflow.CompleteSession(sessionCtx)

		var taskQueueName string
		if err := workflow.ExecuteActivity(sessionCtx, actName).Get(sessionCtx, &taskQueueName); err != nil {
			return "", err
		}
		return taskQueueName, nil
	}

	act := func(ctx context.Context) (string, error) {
		return activity.GetInfo(ctx).TaskQueue, nil
	}

	w := worker.New(s.SdkClient(), tv.TaskQueue().GetName(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			Version:                   tv.SDKDeploymentVersion(),
			UseVersioning:             true,
			DefaultVersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
		},
		EnableSessionWorker: true,
	})
	w.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: wfName})
	w.RegisterActivityWithOptions(act, activity.RegisterOptions{Name: actName})
	s.NoError(w.Start())
	defer w.Stop()

	// Ensure the version is current and propagated before starting the workflow.
	s.setCurrentDeployment(tv)
	s.waitForDeploymentDataPropagation(tv, versionStatusCurrent, false, tqTypeWf, tqTypeAct)

	run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue: tv.TaskQueue().GetName(),
	}, wfName)
	s.NoError(err)

	var sessionTaskQueue string
	s.NoError(run.Get(ctx, &sessionTaskQueue))
	s.NotEmpty(sessionTaskQueue)
	// Sanity: for sessions this should be a resource-specific activity task queue, not the base TQ.
	s.NotEqual(tv.TaskQueue().GetName(), sessionTaskQueue)

	// The session resource-specific task queue must NOT be registered in the version
	resp, err := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
		Namespace: s.Namespace().String(),
		Version:   tv.DeploymentVersionString(),
	})
	s.Require().NoError(err)

	totalActTQ := 0
	for _, tq := range resp.GetVersionTaskQueues() {
		// Only check activity queues since the session resource-specific queue is an activity TQ
		if tq.GetType() == tqTypeAct {
			totalActTQ++
			s.NotEqual(sessionTaskQueue, tq.GetName(), "session resource-specific task queue should not be registered in the version")
		}
	}
	// Ensure that there are only two activity task queues present.
	s.Equal(2, totalActTQ)

}

func (s *Versioning3Suite) TestWorkflowWithPinnedOverride_Sticky() {
	s.RunTestWithMatchingBehavior(
		func() {
			s.testWorkflowWithPinnedOverride(true)
		},
	)
}

func (s *Versioning3Suite) TestWorkflowWithPinnedOverride_NoSticky() {
	s.RunTestWithMatchingBehavior(
		func() {
			s.testWorkflowWithPinnedOverride(false)
		},
	)
}

func (s *Versioning3Suite) testWorkflowWithPinnedOverride(sticky bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tv := testvars.New(s)

	if sticky {
		s.warmUpSticky(tv)
	}

	wftCompleted := make(chan struct{})
	s.pollWftAndHandle(tv, false, wftCompleted,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.verifyVersioningSAs(tv, vbPinned, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)
			return respondWftWithActivities(tv, tv, sticky, vbUnpinned, "5"), nil
		})

	actCompleted := make(chan struct{})
	s.pollActivityAndHandle(tv, actCompleted,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondActivity(), nil
		})

	// Wait for the version to be present in the task queue. Version existence is required before it can be set as an override.
	s.validatePinnedVersionExistsInTaskQueue(tv)

	runID := s.startWorkflow(tv, tv.VersioningOverridePinned(s.useV32))

	s.WaitForChannel(ctx, wftCompleted)
	s.verifyWorkflowVersioning(s.Assertions, tv, vbUnpinned, tv.Deployment(), tv.VersioningOverridePinned(s.useV32), nil)
	s.verifyVersioningSAs(tv, vbPinned, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, tv)
	if sticky {
		s.verifyWorkflowStickyQueue(tv.WithRunID(runID))
	}

	s.WaitForChannel(ctx, actCompleted)
	s.verifyWorkflowVersioning(s.Assertions, tv, vbUnpinned, tv.Deployment(), tv.VersioningOverridePinned(s.useV32), nil)

	s.pollWftAndHandle(tv, sticky, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondCompleteWorkflow(tv, vbUnpinned), nil
		})
	s.verifyWorkflowVersioning(s.Assertions, tv, vbUnpinned, tv.Deployment(), tv.VersioningOverridePinned(s.useV32), nil)
}

func (s *Versioning3Suite) TestQueryWithPinnedOverride_NoSticky() {
	s.RunTestWithMatchingBehavior(
		func() {
			s.testQueryWithPinnedOverride(false)
		},
	)
}

func (s *Versioning3Suite) TestQueryWithPinnedOverride_Sticky() {
	s.RunTestWithMatchingBehavior(
		func() {
			s.testQueryWithPinnedOverride(true)
		},
	)
}

func (s *Versioning3Suite) TestPinnedQuery_DrainedVersion_PollersAbsent() {
	s.RunTestWithMatchingBehavior(
		func() {
			s.testPinnedQuery_DrainedVersion(false, false)
		},
	)
}

func (s *Versioning3Suite) TestPinnedQuery_DrainedVersion_PollersPresent() {
	s.RunTestWithMatchingBehavior(
		func() {
			s.testPinnedQuery_DrainedVersion(true, false)
		},
	)
}

func (s *Versioning3Suite) TestPinnedQuery_RollbackDrainedVersion() {
	s.RunTestWithMatchingBehavior(
		func() {
			s.testPinnedQuery_DrainedVersion(true, true)
		},
	)
}

func (s *Versioning3Suite) testPinnedQuery_DrainedVersion(pollersPresent bool, rollback bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.OverrideDynamicConfig(dynamicconfig.VersionDrainageStatusRefreshInterval, 1*time.Second)
	s.OverrideDynamicConfig(dynamicconfig.VersionDrainageStatusVisibilityGracePeriod, 1*time.Second)
	if !pollersPresent {
		s.OverrideDynamicConfig(dynamicconfig.PollerHistoryTTL, 500*time.Millisecond)
	}

	tv := testvars.New(s)

	// create version v1 and make it current
	idlePollerDone := make(chan struct{})
	go func() {
		s.idlePollWorkflow(context.Background(), tv, true, ver3MinPollTime, "should not have gotten any tasks since there are none")
		close(idlePollerDone)
	}()
	s.setCurrentDeployment(tv)
	s.WaitForChannel(ctx, idlePollerDone)

	wftCompleted := make(chan struct{})
	s.pollWftAndHandle(tv, false, wftCompleted,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondCompleteWorkflow(tv, vbPinned), nil
		})

	s.startWorkflow(tv, tv.VersioningOverridePinned(s.useV32))
	s.WaitForChannel(ctx, wftCompleted)
	s.verifyWorkflowVersioning(s.Assertions, tv, vbPinned, tv.Deployment(), tv.VersioningOverridePinned(s.useV32), nil)

	// create version v2 and make it current which shall make v1 go from current -> draining/drained
	idlePollerDone = make(chan struct{})
	tv2 := tv.WithBuildIDNumber(2)
	go func() {
		s.idlePollWorkflow(context.Background(), tv2, true, ver3MinPollTime, "should not have gotten any tasks since there are none")
		close(idlePollerDone)
	}()
	s.setCurrentDeployment(tv2)
	s.WaitForChannel(ctx, idlePollerDone)

	// wait for v1 to become drained
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: s.Namespace().String(),
			Version:   tv.DeploymentVersionString(),
		})
		a.NoError(err)
		a.Equal(enumspb.VERSION_DRAINAGE_STATUS_DRAINED, resp.GetWorkerDeploymentVersionInfo().GetDrainageInfo().GetStatus())
	}, time.Second*10, time.Millisecond*1000)

	if !pollersPresent {
		// simulate the pollers going away, which should make the query fail as now the version is drained + has no pollers polling it
		time.Sleep(1 * time.Second) //nolint:forbidigo
		versionStr := tv.Deployment().GetBuildId()
		if s.deploymentWorkflowVersion < workerdeployment.AsyncSetCurrentAndRamping {
			versionStr = worker_versioning.ExternalWorkerDeploymentVersionToString(worker_versioning.ExternalWorkerDeploymentVersionFromDeployment(tv.Deployment()))
		}

		_, err := s.queryWorkflow(tv)
		s.Error(err)
		s.ErrorContains(err, fmt.Sprintf(matching.ErrBlackholedQuery, versionStr, versionStr))
	} else {
		// since the version still has pollers, the query should succeed
		s.pollAndQueryWorkflow(tv, false)
	}

	if rollback {
		// ramp traffic back to v1 to remove it from drained state
		s.setRampingDeployment(tv, 50, false)

		// wait for v1 to become ramping
		s.EventuallyWithT(func(t *assert.CollectT) {
			a := require.New(t)
			resp, err := s.FrontendClient().DescribeWorkerDeploymentVersion(ctx, &workflowservice.DescribeWorkerDeploymentVersionRequest{
				Namespace: s.Namespace().String(),
				Version:   tv.DeploymentVersionString(),
			})
			a.NoError(err)
			a.Equal(enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING, resp.GetWorkerDeploymentVersionInfo().GetStatus())
		}, time.Second*10, time.Millisecond*1000)

		// the ramping status is propagated to the task queues
		s.waitForDeploymentDataPropagation(tv, versionStatusRamping, false, tqTypeWf)

		// the version has pollers and is ramping making the query succeed
		s.pollAndQueryWorkflow(tv, false)

	}
}

func (s *Versioning3Suite) testQueryWithPinnedOverride(sticky bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tv := testvars.New(s)

	if sticky {
		s.warmUpSticky(tv)
	}

	wftCompleted := make(chan struct{})
	s.pollWftAndHandle(tv, false, wftCompleted,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondEmptyWft(tv, sticky, vbUnpinned), nil
		})

	// Wait for the version to be present in the task queue. Version existence is required before it can be set as an override.
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := s.GetTestCluster().MatchingClient().CheckTaskQueueVersionMembership(ctx, &matchingservice.CheckTaskQueueVersionMembershipRequest{
			NamespaceId:   s.NamespaceID().String(),
			TaskQueue:     tv.TaskQueue().GetName(),
			TaskQueueType: tqTypeWf,
			Version:       worker_versioning.DeploymentVersionFromDeployment(tv.Deployment()),
		})
		a.NoError(err)
		a.True(resp.GetIsMember())
	}, 10*time.Second, 100*time.Millisecond)

	runID := s.startWorkflow(tv, tv.VersioningOverridePinned(s.useV32))

	s.WaitForChannel(ctx, wftCompleted)
	s.verifyWorkflowVersioning(s.Assertions, tv, vbUnpinned, tv.Deployment(), tv.VersioningOverridePinned(s.useV32), nil)
	if sticky {
		s.verifyWorkflowStickyQueue(tv.WithRunID(runID))
	}

	s.pollAndQueryWorkflow(tv, sticky)
}

func (s *Versioning3Suite) TestUnpinnedQuery_NoSticky() {
	s.RunTestWithMatchingBehavior(
		func() {
			s.testUnpinnedQuery(false)
		},
	)
}

func (s *Versioning3Suite) TestUnpinnedQuery_Sticky() {
	s.RunTestWithMatchingBehavior(
		func() {
			s.testUnpinnedQuery(true)
		},
	)
}

func (s *Versioning3Suite) testUnpinnedQuery(sticky bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	tv := testvars.New(s)
	tv2 := tv.WithBuildIDNumber(2)
	if sticky {
		s.warmUpSticky(tv)
	}

	wftCompleted := make(chan struct{})
	s.pollWftAndHandle(tv, false, wftCompleted,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.verifyWorkflowVersioning(s.Assertions, tv, vbUnspecified, nil, nil, tv.DeploymentVersionTransition())
			return respondEmptyWft(tv, sticky, vbUnpinned), nil
		})

	s.setCurrentDeployment(tv)
	s.waitForDeploymentDataPropagation(tv, versionStatusCurrent, false, tqTypeWf)

	runID := s.startWorkflow(tv, nil)

	s.WaitForChannel(ctx, wftCompleted)
	s.verifyWorkflowVersioning(s.Assertions, tv, vbUnpinned, tv.Deployment(), nil, nil)
	if sticky {
		s.verifyWorkflowStickyQueue(tv.WithRunID(runID))
	}

	pollerDone := make(chan struct{})
	go func() {
		s.idlePollWorkflow(context.Background(), tv2, true, 5*time.Second, "new deployment should not receive query")
		close(pollerDone)
	}()
	s.pollAndQueryWorkflow(tv, sticky)
	s.WaitForChannel(ctx, pollerDone) // wait for the idle poller to complete to not interfere with the next poller

	s.setCurrentDeployment(tv2)
	s.waitForDeploymentDataPropagation(tv2, versionStatusCurrent, false, tqTypeWf)

	go s.idlePollWorkflow(context.Background(), tv, true, ver3MinPollTime, "old deployment should not receive query")
	// Since the current deployment has changed, task will move to the normal queue (thus, sticky=false)
	s.pollAndQueryWorkflow(tv2, false)
}

func (s *Versioning3Suite) pollAndQueryWorkflow(
	tv *testvars.TestVars,
	sticky bool,
) {
	queryResultCh := make(chan any)
	s.pollWftAndHandleQueries(tv, sticky, queryResultCh,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondQueryTaskCompletedRequest, error) {
			return &workflowservice.RespondQueryTaskCompletedRequest{}, nil
		})

	_, err := s.queryWorkflow(tv)
	s.NoError(err)

	<-queryResultCh
}

func (s *Versioning3Suite) TestPinnedWorkflowWithLateActivityPoller() {
	s.RunTestWithMatchingBehavior(
		func() {
			s.testPinnedWorkflowWithLateActivityPoller()
		},
	)
}

func (s *Versioning3Suite) testPinnedWorkflowWithLateActivityPoller() {
	// Here, we test that designating activities as independent is revisited if the missing activity
	// pollers arrive to server while the so-far-independent activity is backlogged.
	// Summary: a wf starts with a pinned override. The first wft schedules an activity before
	// any activity poller on the pinned deployment is seen by the server. The activity is sent
	// to the default queue. Then, the activity poller on the pinned deployment arrives, the task
	// should be now sent to that poller although no current deployment is set on the TQs.

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tv := testvars.New(s)

	wftCompleted := make(chan struct{})
	s.pollWftAndHandle(tv, false, wftCompleted,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondWftWithActivities(tv, tv, false, vbUnpinned, "5"), nil
		})
	s.waitForDeploymentDataPropagation(tv, versionStatusInactive, false, tqTypeWf)

	override := tv.VersioningOverridePinned(s.useV32)
	s.startWorkflow(tv, override)

	s.WaitForChannel(ctx, wftCompleted)
	s.verifyWorkflowVersioning(s.Assertions, tv, vbUnpinned, tv.Deployment(), override, nil)
	// Wait long enough to make sure the activity is backlogged.
	s.validateBacklogCount(tv, tqTypeAct, 1)

	// When the first activity poller arrives from this deployment, it registers the TQ in the
	// deployment and that will trigger reevaluation of backlog queue.
	s.pollActivityAndHandle(tv, nil,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondActivity(), nil
		})
	s.verifyWorkflowVersioning(s.Assertions, tv, vbUnpinned, tv.Deployment(), override, nil)
	s.validateBacklogCount(tv, tqTypeAct, 0)

	s.pollWftAndHandle(tv, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondCompleteWorkflow(tv, vbUnpinned), nil
		})
	s.verifyWorkflowVersioning(s.Assertions, tv, vbUnpinned, tv.Deployment(), override, nil)
}

func (s *Versioning3Suite) TestUnpinnedWorkflow_Sticky() {
	s.RunTestWithMatchingBehavior(
		func() {
			s.testUnpinnedWorkflow(true)
		},
	)
}

func (s *Versioning3Suite) TestUnpinnedWorkflow_NoSticky() {
	s.RunTestWithMatchingBehavior(
		func() {
			s.testUnpinnedWorkflow(false)
		},
	)
}

func (s *Versioning3Suite) testUnpinnedWorkflow(sticky bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tv := testvars.New(s)

	if sticky {
		s.warmUpSticky(tv)
	}

	wftCompleted := make(chan struct{})
	s.pollWftAndHandle(tv, false, wftCompleted,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.verifyWorkflowVersioning(s.Assertions, tv, vbUnspecified, nil, nil, tv.DeploymentVersionTransition())
			return respondWftWithActivities(tv, tv, sticky, vbUnpinned, "5"), nil
		})

	actCompleted := make(chan struct{})
	s.pollActivityAndHandle(tv, actCompleted,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondActivity(), nil
		})

	s.setCurrentDeployment(tv)

	runID := s.startWorkflow(tv, nil)

	s.WaitForChannel(ctx, wftCompleted)
	s.verifyWorkflowVersioning(s.Assertions, tv, vbUnpinned, tv.Deployment(), nil, nil)
	s.verifyVersioningSAs(tv, vbUnpinned, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, tv)
	if sticky {
		s.verifyWorkflowStickyQueue(tv.WithRunID(runID))
	}

	s.WaitForChannel(ctx, actCompleted)
	s.verifyWorkflowVersioning(s.Assertions, tv, vbUnpinned, tv.Deployment(), nil, nil)

	s.pollWftAndHandle(tv, sticky, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondCompleteWorkflow(tv, vbUnpinned), nil
		})
	s.verifyWorkflowVersioning(s.Assertions, tv, vbUnpinned, tv.Deployment(), nil, nil)
}

func (s *Versioning3Suite) TestSearchByUsedVersion() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tv := testvars.New(s)

	wftCompleted := make(chan struct{})
	s.pollWftAndHandle(tv, false, wftCompleted,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			// Although pinned to the version by override, the query should not return a workflow because the workflow task is not completed yet.
			wfs, err := s.FrontendClient().ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     fmt.Sprintf("TemporalUsedWorkerDeploymentVersions = '%s'", tv.DeploymentVersionStringV32()),
			})
			s.NoError(err)
			s.Empty(wfs.Executions)
			return respondCompleteWorkflow(tv, vbPinned), nil
		})

	s.waitForDeploymentDataPropagation(tv, versionStatusInactive, false, tqTypeWf)
	s.startWorkflow(tv, tv.VersioningOverridePinned(s.useV32))
	<-wftCompleted

	s.verifyVersioningSAs(tv, vbPinned, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, tv)

	s.EventuallyWithT(func(t *assert.CollectT) {
		a := assert.New(t)
		wfs, err := s.FrontendClient().ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: s.Namespace().String(),
			Query:     fmt.Sprintf("TemporalUsedWorkerDeploymentVersions = '%s'", tv.DeploymentVersionStringV32()),
		})
		a.NoError(err)
		a.Len(wfs.Executions, 1)
	}, 5*time.Second, 200*time.Millisecond)
}

// drainWorkflowTaskAfterSetCurrent is a helper that sets the current deployment version,
// drains the initial workflow task from the execution, and ensures the task is correctly
// routed to the appropriate build.
func (s *Versioning3Suite) drainWorkflowTaskAfterSetCurrentWithOverride(
	tv *testvars.TestVars,
	override *workflowpb.VersioningOverride,
) (*commonpb.WorkflowExecution, string) {
	wftCompleted := make(chan struct{})
	s.pollWftAndHandle(tv, false, wftCompleted,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			if override != nil {
				s.verifyWorkflowVersioning(s.Assertions, tv, vbUnspecified, nil, override, nil)
			} else {
				s.verifyWorkflowVersioning(s.Assertions, tv, vbUnspecified, nil, override, tv.DeploymentVersionTransition())
			}
			return respondEmptyWft(tv, false, vbUnpinned), nil
		})
	s.waitForDeploymentDataPropagation(tv, versionStatusInactive, false, tqTypeWf)
	s.setCurrentDeployment(tv)

	runID := s.startWorkflow(tv, override)
	execution := tv.WithRunID(runID).WorkflowExecution()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	s.WaitForChannel(ctx, wftCompleted)

	return execution, runID
}

// drainWorkflowTaskAfterSetCurrent is a helper that sets the current deployment version,
// drains the initial workflow task from the execution, and ensures the task is correctly
// routed to the appropriate build.
func (s *Versioning3Suite) drainWorkflowTaskAfterSetCurrent(
	tv *testvars.TestVars,
) (*commonpb.WorkflowExecution, string) {
	wftCompleted := make(chan struct{})
	s.pollWftAndHandle(tv, false, wftCompleted,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.verifyWorkflowVersioning(s.Assertions, tv, vbUnspecified, nil, nil, tv.DeploymentVersionTransition())
			return respondEmptyWft(tv, false, vbUnpinned), nil
		})
	s.waitForDeploymentDataPropagation(tv, versionStatusInactive, false, tqTypeWf)
	s.setCurrentDeployment(tv)

	runID := s.startWorkflow(tv, nil)
	execution := tv.WithRunID(runID).WorkflowExecution()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	s.WaitForChannel(ctx, wftCompleted)

	return execution, runID
}

func (s *Versioning3Suite) TestUnpinnedWorkflow_SuccessfulUpdate_TransitionsToNewDeployment() {
	tv1 := testvars.New(s).WithBuildIDNumber(1)

	execution, _ := s.drainWorkflowTaskAfterSetCurrent(tv1)

	// Register the new version and set it to current
	tv2 := tv1.WithBuildIDNumber(2)
	s.idlePollWorkflow(context.Background(), tv2, true, ver3MinPollTime, "should not have gotten any tasks since there are none")
	s.setCurrentDeployment(tv2)

	// Send update
	updateResultCh := sendUpdateNoError(s, tv2)

	// Process update in workflow
	s.pollWftAndHandle(tv2, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)

			// Verify that events from the speculative task are written to the task history
			s.EqualHistory(`
			1 WorkflowExecutionStarted
			2 WorkflowTaskScheduled
			3 WorkflowTaskStarted
			4 WorkflowTaskCompleted
			5 WorkflowTaskScheduled // Speculative WT events are not written to the history yet.
			6 WorkflowTaskStarted
		  `, task.History)

			events := s.GetHistory(s.Namespace().String(), execution)
			s.EqualHistoryEvents(`
				1 WorkflowExecutionStarted
				2 WorkflowTaskScheduled
				3 WorkflowTaskStarted
				4 WorkflowTaskCompleted
				5 WorkflowTaskScheduled
				6 WorkflowTaskStarted
			`, events)

			// VersioningInfo should not have changed before the update has been processed by the poller.
			// Deployment version transition should also be nil since this is a speculative task.
			s.verifyWorkflowVersioning(s.Assertions, tv1, vbUnpinned, tv1.Deployment(), nil, nil)

			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands:           s.UpdateAcceptCompleteCommands(tv2),
				Messages:           s.UpdateAcceptCompleteMessages(tv2, task.Messages[0]),
				VersioningBehavior: vbUnpinned,
				DeploymentOptions: &deploymentpb.WorkerDeploymentOptions{
					BuildId:              tv2.BuildID(),
					DeploymentName:       tv2.DeploymentSeries(),
					WorkerVersioningMode: enumspb.WORKER_VERSIONING_MODE_VERSIONED,
				},
			}, nil
		})

	updateResult := <-updateResultCh
	s.EqualValues("success-result-of-"+tv2.UpdateID(), testcore.DecodeString(s.T(), updateResult.GetOutcome().GetSuccess()))

	// Verify that events from the speculative task are written to the history since the update was accepted
	events := s.GetHistory(s.Namespace().String(), execution)
	s.EqualHistoryEvents(`
1 WorkflowExecutionStarted
2 WorkflowTaskScheduled
3 WorkflowTaskStarted
4 WorkflowTaskCompleted
5 WorkflowTaskScheduled // Was speculative WT...
6 WorkflowTaskStarted
7 WorkflowTaskCompleted // ...and events were written to the history when WT completes.  
8 WorkflowExecutionUpdateAccepted {"AcceptedRequestSequencingEventId": 5} // WTScheduled event which delivered update to the worker.
9 WorkflowExecutionUpdateCompleted {"AcceptedEventId": 8}
`, events)

	// Verify that the versioning info is updated correctly.
	describeCall, err := s.FrontendClient().DescribeWorkflowExecution(context.Background(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		Execution: execution,
	})
	s.Nil(err)
	s.NotNil(describeCall)

	// Since the poller accepted the update, the Worker Deployment Version that completed the last workflow task
	// of this workflow execution should have changed to the new version. However, the version transition should
	// still be nil.
	s.verifyWorkflowVersioning(s.Assertions, tv2, vbUnpinned, tv2.Deployment(), nil, nil)

}

func (s *Versioning3Suite) TestUnpinnedWorkflow_FailedUpdate_DoesNotTransitionToNewDeployment() {
	tv1 := testvars.New(s).WithBuildIDNumber(1)

	execution, _ := s.drainWorkflowTaskAfterSetCurrent(tv1)

	// Register the new version and set it to current
	tv2 := tv1.WithBuildIDNumber(2)
	s.idlePollWorkflow(context.Background(), tv2, true, ver3MinPollTime, "should not have gotten any tasks since there are none")

	s.setCurrentDeployment(tv2)

	// Send update
	updateResultCh := sendUpdateNoError(s, tv2)

	// Process update in workflow
	s.pollWftAndHandle(tv2, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)

			// Verify that events from the speculative task are written to the task history
			s.EqualHistory(`
			1 WorkflowExecutionStarted
			2 WorkflowTaskScheduled
			3 WorkflowTaskStarted
			4 WorkflowTaskCompleted
			5 WorkflowTaskScheduled // Speculative WT events are not written to the history yet.
			6 WorkflowTaskStarted
		  `, task.History)

			events := s.GetHistory(s.Namespace().String(), execution)
			s.EqualHistoryEvents(`
				1 WorkflowExecutionStarted
				2 WorkflowTaskScheduled
				3 WorkflowTaskStarted
				4 WorkflowTaskCompleted
				5 WorkflowTaskScheduled
				6 WorkflowTaskStarted
			`, events)

			// VersioningInfo should not have changed before the update has been processed by the poller.
			// Deployment version transition should also be nil since this is a speculative task.
			s.verifyWorkflowVersioning(s.Assertions, tv1, vbUnpinned, tv1.Deployment(), nil, nil)

			updRequestMsg := task.Messages[0]
			updRequest := protoutils.UnmarshalAny[*updatepb.Request](s.T(), updRequestMsg.GetBody())

			s.Equal("args-value-of-"+tv2.UpdateID(), testcore.DecodeString(s.T(), updRequest.GetInput().GetArgs()))
			s.Equal(tv2.HandlerName(), updRequest.GetInput().GetName())
			s.EqualValues(5, updRequestMsg.GetEventId())

			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Messages:           s.UpdateRejectMessages(tv2, updRequestMsg),
				VersioningBehavior: vbUnpinned,
				DeploymentOptions: &deploymentpb.WorkerDeploymentOptions{
					BuildId:              tv2.BuildID(),
					DeploymentName:       tv2.DeploymentSeries(),
					WorkerVersioningMode: enumspb.WORKER_VERSIONING_MODE_VERSIONED,
				},
			}, nil
		})

	updateResult := <-updateResultCh
	s.Equal("rejection-of-"+tv2.UpdateID(), updateResult.GetOutcome().GetFailure().GetMessage())

	// Verify events from the speculative task are *not* written to the workflow history since the update was rejected
	events := s.GetHistory(s.Namespace().String(), execution)
	s.EqualHistoryEvents(`
	1 WorkflowExecutionStarted
	2 WorkflowTaskScheduled
	3 WorkflowTaskStarted
	4 WorkflowTaskCompleted
	`, events)

	// Since the poller rejected the update, the Worker Deployment Version that completed the last workflow task
	// of this workflow execution should not have changed.
	s.verifyWorkflowVersioning(s.Assertions, tv1, vbUnpinned, tv1.Deployment(), nil, nil)
}

func (s *Versioning3Suite) TestUnpinnedWorkflowWithRamp_ToVersioned() {
	s.RunTestWithMatchingBehavior(
		func() {
			s.testUnpinnedWorkflowWithRamp(false)
		},
	)
}

func (s *Versioning3Suite) TestUnpinnedWorkflowWithRamp_ToUnversioned() {
	s.RunTestWithMatchingBehavior(
		func() {
			s.testUnpinnedWorkflowWithRamp(true)
		},
	)
}

func (s *Versioning3Suite) TestWorkflowRetry_Pinned_ExpectInherit_RetryOfChild() {
	s.testWorkflowRetry(workflow.VersioningBehaviorPinned, true, true, false)
}

func (s *Versioning3Suite) TestWorkflowRetry_Pinned_ExpectInherit_RetryOfCaN() {
	s.testWorkflowRetry(workflow.VersioningBehaviorPinned, true, false, true)
}

func (s *Versioning3Suite) TestWorkflowRetry_Pinned_ExpectNoInherit() {
	s.testWorkflowRetry(workflow.VersioningBehaviorPinned, false, false, false)
}

func (s *Versioning3Suite) TestWorkflowRetry_Unpinned_ExpectAutoUpgradeToCurrent() {
	s.testWorkflowRetry(workflow.VersioningBehaviorAutoUpgrade, false, false, false)
}

func (s *Versioning3Suite) testWorkflowRetry(behavior workflow.VersioningBehavior, expectInherit, retryOfChild, retryOfCaN bool) {
	tv1 := testvars.New(s).WithBuildIDNumber(1)
	tv2 := tv1.WithBuildIDNumber(2)

	childWorkflowID := tv1.WorkflowID() + "-child"
	parentWf := func(ctx workflow.Context) error {
		fut1 := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			TaskQueue:   tv1.TaskQueue().GetName(),
			WorkflowID:  childWorkflowID,
			RetryPolicy: &temporal.RetryPolicy{},
		}), "wf")
		var val1 string
		s.NoError(fut1.Get(ctx, &val1))
		return nil
	}

	wf := func(ctx workflow.Context, attempt int) (string, error) {
		var ret string
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 1 * time.Second,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    1 * time.Second,
				BackoffCoefficient: 1,
			},
		}), "act").Get(ctx, &ret)
		s.NoError(err)
		if retryOfCaN && attempt == 1 {
			return "", workflow.NewContinueAsNewError(ctx, "wf", attempt+1)
		}
		// Use Temporal signal instead of Go channel to avoid replay issues
		workflow.GetSignalChannel(ctx, "currentVersionChanged").Receive(ctx, nil)
		return "", errors.New("explicit failure")
	}
	act1 := func() (string, error) {
		return "v1", nil
	}
	act2 := func() (string, error) {
		return "v2", nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	w1 := worker.New(s.SdkClient(), tv1.TaskQueue().GetName(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			Version:       tv1.SDKDeploymentVersion(),
			UseVersioning: true,
		},
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: "wf", VersioningBehavior: behavior})
	w1.RegisterWorkflowWithOptions(parentWf, workflow.RegisterOptions{Name: "parent-wf", VersioningBehavior: behavior})
	w1.RegisterActivityWithOptions(act1, activity.RegisterOptions{Name: "act"})
	s.NoError(w1.Start())
	defer w1.Stop()

	w2 := worker.New(s.SdkClient(), tv1.TaskQueue().GetName(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			Version:       tv2.SDKDeploymentVersion(),
			UseVersioning: true,
		},
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w2.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: "wf", VersioningBehavior: behavior})
	w2.RegisterWorkflowWithOptions(parentWf, workflow.RegisterOptions{Name: "parent-wf", VersioningBehavior: behavior})
	w2.RegisterActivityWithOptions(act2, activity.RegisterOptions{Name: "act"})
	s.NoError(w2.Start())
	defer w2.Stop()

	// Set v1 to current and propagate to all task queue partitions
	s.setCurrentDeployment(tv1)
	s.waitForDeploymentDataPropagation(tv1, versionStatusCurrent, false, tqTypeWf, tqTypeAct)

	wf0 := "wf"
	if retryOfChild {
		wf0 = "parent-wf"
	}
	run0, err := s.SdkClient().ExecuteWorkflow(
		ctx,
		sdkclient.StartWorkflowOptions{
			TaskQueue: tv1.TaskQueue().GetName(),
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval: time.Second,
			},
		},
		wf0,
		1,
	)
	s.NoError(err)

	wfIDOfRetryingWF := run0.GetID()
	runIDBeforeRetry := run0.GetRunID()

	if retryOfCaN {
		// wait for first run to continue-as-new
		s.EventuallyWithT(func(t *assert.CollectT) {
			desc, err := s.SdkClient().DescribeWorkflow(ctx, wfIDOfRetryingWF, run0.GetRunID())
			if !assert.NoError(t, err) {
				return
			}
			assert.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW, desc.Status)
		}, 5*time.Second, 100*time.Millisecond)
	}

	// wait for workflow to progress on v1 (activity completed and waiting for signal)
	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, wfIDOfRetryingWF, "")
		if !assert.NoError(t, err) {
			return
		}
		// Check if workflow is running on v1
		assert.Equal(t, tv1.BuildID(), desc.GetWorkflowExecutionInfo().GetVersioningInfo().GetDeploymentVersion().GetBuildId())
	}, 5*time.Second, 100*time.Millisecond)

	// get run ID of first run of the workflow before it fails
	if retryOfChild {
		wfIDOfRetryingWF = childWorkflowID
		// Wait for child workflow to be created
		s.EventuallyWithT(func(t *assert.CollectT) {
			desc, err := s.SdkClient().DescribeWorkflow(ctx, wfIDOfRetryingWF, "")
			if !assert.NoError(t, err) {
				return
			}
			runIDBeforeRetry = desc.WorkflowExecution.RunID
		}, 5*time.Second, 100*time.Millisecond)
	} else if retryOfCaN {
		// get the next run in the continue-as-new chain
		s.EventuallyWithT(func(t *assert.CollectT) {
			continuedAsNewRunResp, err := s.SdkClient().DescribeWorkflow(ctx, wfIDOfRetryingWF, "")
			if !assert.NoError(t, err) {
				return
			}
			caNRunID := continuedAsNewRunResp.WorkflowExecution.RunID
			// confirm that it's a new run
			if assert.NotEqual(t, run0.GetRunID(), caNRunID) {
				runIDBeforeRetry = caNRunID
			}
		}, 5*time.Second, 100*time.Millisecond)
	}

	// Set v2 to current and propagate to all task queue partitions
	s.setCurrentDeployment(tv2)
	s.waitForDeploymentDataPropagation(tv2, versionStatusCurrent, false, tqTypeWf, tqTypeAct)

	// signal workflow to continue (it will fail and then retry on v2 if it doesn't inherit)
	s.NoError(s.SdkClient().SignalWorkflow(ctx, wfIDOfRetryingWF, runIDBeforeRetry, "currentVersionChanged", nil))

	// wait for run that will retry to fail
	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := s.SdkClient().DescribeWorkflow(ctx, wfIDOfRetryingWF, runIDBeforeRetry)
		if !assert.NoError(t, err) {
			return
		}
		assert.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, desc.Status)
	}, 5*time.Second, 100*time.Millisecond)

	// get the execution info of the next run in the retry chain, wait for next run to start
	var secondRunID string
	s.EventuallyWithT(func(t *assert.CollectT) {
		secondRunResp, err := s.SdkClient().DescribeWorkflow(ctx, wfIDOfRetryingWF, "")
		if !assert.NoError(t, err) {
			return
		}
		secondRunID = secondRunResp.WorkflowExecution.RunID
		// confirm that it's a new run
		assert.NotEqual(t, runIDBeforeRetry, secondRunID)
	}, 5*time.Second, 100*time.Millisecond)

	// confirm that the second run eventually gets auto-upgrade behavior and runs on version 2 (no inherit)
	s.EventuallyWithT(func(t *assert.CollectT) {
		secondRunResp, err := s.SdkClient().DescribeWorkflowExecution(ctx, wfIDOfRetryingWF, secondRunID)
		if !assert.NoError(t, err) {
			return
		}
		switch behavior {
		case workflow.VersioningBehaviorPinned:
			if !assert.Equal(t, enumspb.VERSIONING_BEHAVIOR_PINNED, secondRunResp.GetWorkflowExecutionInfo().GetVersioningInfo().GetBehavior()) {
				return
			}
		case workflow.VersioningBehaviorAutoUpgrade:
			if !assert.Equal(t, enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE, secondRunResp.GetWorkflowExecutionInfo().GetVersioningInfo().GetBehavior()) {
				return
			}
		default:
		}
		switch expectInherit {
		case true:
			assert.Equal(t, tv1.BuildID(), secondRunResp.GetWorkflowExecutionInfo().GetVersioningInfo().GetDeploymentVersion().GetBuildId())
		case false:
			assert.Equal(t, tv2.BuildID(), secondRunResp.GetWorkflowExecutionInfo().GetVersioningInfo().GetDeploymentVersion().GetBuildId())
		default:
		}
	}, 5*time.Second, 100*time.Millisecond)
}

func (s *Versioning3Suite) testUnpinnedWorkflowWithRamp(toUnversioned bool) {
	// This test sets a 50% ramp and runs 50 wfs and ensures both versions got some wf and
	// activity tasks.

	tv1 := testvars.New(s).WithBuildIDNumber(1)
	tv2 := tv1.WithBuildIDNumber(2)

	wf := func(ctx workflow.Context, version string) (string, error) {
		var ret string
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 1 * time.Second,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    1 * time.Second,
				BackoffCoefficient: 1,
			},
		}), "act").Get(ctx, &ret)
		s.NoError(err)
		return version, nil
	}

	wf1 := func(ctx workflow.Context) (string, error) {
		return wf(ctx, "v1")
	}
	wf2 := func(ctx workflow.Context) (string, error) {
		return wf(ctx, "v2")
	}
	act1 := func() (string, error) {
		return "v1", nil
	}
	act2 := func() (string, error) {
		return "v2", nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	w1 := worker.New(s.SdkClient(), tv1.TaskQueue().GetName(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			Version:                   tv1.SDKDeploymentVersion(),
			UseVersioning:             true,
			DefaultVersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
		},
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	w1.RegisterActivityWithOptions(act1, activity.RegisterOptions{Name: "act"})
	s.NoError(w1.Start())
	defer w1.Stop()

	// v1 is current and v2 is ramping at 50%

	// Make sure both TQs are registered in v1 which will be the current version. This is to make sure
	// we don't get to ramping while one of the TQs has not yet got v1 as it's current version.
	// (note that s.setCurrentDeployment(tv1) can pass even with one TQ added to the version)
	s.waitForDeploymentDataPropagation(tv1, versionStatusInactive, false, tqTypeWf, tqTypeAct)
	s.setCurrentDeployment(tv1)

	// wait until all task queue partitions know that tv1 is current
	s.waitForDeploymentDataPropagation(tv1, versionStatusCurrent, false, tqTypeWf, tqTypeAct)

	deploymentOpts := worker.DeploymentOptions{
		Version: tv2.SDKDeploymentVersion(),
	}
	if !toUnversioned {
		deploymentOpts.UseVersioning = true
		deploymentOpts.DefaultVersioningBehavior = workflow.VersioningBehaviorAutoUpgrade
	}
	w2 := worker.New(s.SdkClient(), tv2.TaskQueue().GetName(), worker.Options{
		DeploymentOptions:                deploymentOpts,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf"})
	w2.RegisterActivityWithOptions(act2, activity.RegisterOptions{Name: "act"})
	s.NoError(w2.Start())
	defer w2.Stop()

	s.setRampingDeployment(tv2, 50, toUnversioned)
	// wait until all task queue partitions know that tv2 is ramping
	s.waitForDeploymentDataPropagation(tv2, versionStatusRamping, toUnversioned, tqTypeWf, tqTypeAct)

	numTests := 50
	counter := make(map[string]int)
	runs := make([]sdkclient.WorkflowRun, numTests)
	for i := range numTests {
		run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tv1.TaskQueue().GetName()}, "wf")
		s.NoError(err)
		runs[i] = run
	}
	for i := range numTests {
		var out string
		s.NoError(runs[i].Get(ctx, &out))
		counter[out]++
	}

	// both versions should've got executions
	s.Greater(counter["v1"], 0)
	s.Greater(counter["v2"], 0)
	s.Equal(numTests, counter["v1"]+counter["v2"])
}

func (s *Versioning3Suite) TestTransitionFromWft_Sticky() {
	s.testTransitionFromWft(true, false)
}

func (s *Versioning3Suite) TestTransitionFromWft_NoSticky() {
	s.testTransitionFromWft(false, false)
}

func (s *Versioning3Suite) TestTransitionFromWft_Sticky_ToUnversioned() {
	s.testTransitionFromWft(true, true)
}

func (s *Versioning3Suite) TestTransitionFromWft_NoSticky_ToUnversioned() {
	s.testTransitionFromWft(false, true)
}

func (s *Versioning3Suite) testTransitionFromWft(sticky bool, toUnversioned bool) {
	// Wf runs one WFT and one AT on d1, then the second WFT is redirected to d2 and
	// transitions the wf with it.

	tv1 := testvars.New(s).WithBuildIDNumber(1)
	tv2 := tv1.WithBuildIDNumber(2)
	if sticky {
		s.warmUpSticky(tv1)
	}

	if s.useNewDeploymentData {
		s.updateTaskQueueDeploymentDataWithRoutingConfig(tv1, &deploymentpb.RoutingConfig{
			CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv1.DeploymentVersionString()),
			CurrentVersionChangedTime: timestamp.TimePtr(time.Now()),
			RevisionNumber:            1,
		}, map[string]*deploymentspb.WorkerDeploymentVersionData{tv1.DeploymentVersion().GetBuildId(): {
			Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
		}}, []string{}, tqTypeWf, tqTypeAct)
	} else {
		s.updateTaskQueueDeploymentData(tv1, true, 0, false, 0, tqTypeWf, tqTypeAct)
	}
	runID := s.startWorkflow(tv1, nil)

	s.pollWftAndHandle(tv1, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.verifyWorkflowVersioning(s.Assertions, tv1, vbUnspecified, nil, nil, tv1.DeploymentVersionTransition())
			return respondWftWithActivities(tv1, tv1, sticky, vbUnpinned, "5"), nil
		})
	s.verifyWorkflowVersioning(s.Assertions, tv1, vbUnpinned, tv1.Deployment(), nil, nil)
	s.verifyVersioningSAs(tv1, vbUnpinned, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, tv1)
	if sticky {
		s.verifyWorkflowStickyQueue(tv1.WithRunID(runID))
	}

	s.pollActivityAndHandle(tv1, nil,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondActivity(), nil
		})
	s.verifyWorkflowVersioning(s.Assertions, tv1, vbUnpinned, tv1.Deployment(), nil, nil)

	if toUnversioned {
		// unset A as current
		if s.useNewDeploymentData {
			s.updateTaskQueueDeploymentDataWithRoutingConfig(tv1, &deploymentpb.RoutingConfig{
				CurrentDeploymentVersion:  nil,
				CurrentVersionChangedTime: timestamp.TimePtr(time.Now()),
				RevisionNumber:            2,
			}, map[string]*deploymentspb.WorkerDeploymentVersionData{}, []string{}, tqTypeWf, tqTypeAct)
		} else {
			s.updateTaskQueueDeploymentData(tv1, false, 0, false, 0, tqTypeWf, tqTypeAct)
		}

		s.unversionedPollWftAndHandle(tv1, false, nil,
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				s.NotNil(task)
				s.verifyWorkflowVersioning(s.Assertions, tv1, vbUnpinned, tv1.Deployment(), nil, &workflowpb.DeploymentVersionTransition{Version: "__unversioned__"})
				return respondCompleteWorkflowUnversioned(tv1), nil
			})
		s.verifyWorkflowVersioning(s.Assertions, tv1, vbUnspecified, nil, nil, nil)
		s.verifyVersioningSAs(tv1, vbUnspecified, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, tv1)
	} else {

		// Set B as the current deployment
		if s.useNewDeploymentData {
			s.updateTaskQueueDeploymentDataWithRoutingConfig(tv2, &deploymentpb.RoutingConfig{
				CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv2.DeploymentVersionString()),
				CurrentVersionChangedTime: timestamp.TimePtr(time.Now()),
				RevisionNumber:            2,
			}, map[string]*deploymentspb.WorkerDeploymentVersionData{tv2.DeploymentVersion().GetBuildId(): {
				Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
			}, tv1.DeploymentVersion().GetBuildId(): {
				Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
			}}, []string{}, tqTypeWf, tqTypeAct)
		} else {
			s.updateTaskQueueDeploymentData(tv2, true, 0, false, 0, tqTypeWf, tqTypeAct)
		}

		s.pollWftAndHandle(tv2, false, nil,
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				s.NotNil(task)
				s.verifyWorkflowVersioning(s.Assertions, tv1, vbUnpinned, tv1.Deployment(), nil, tv2.DeploymentVersionTransition())
				return respondCompleteWorkflow(tv2, vbUnpinned), nil
			})
		s.verifyWorkflowVersioning(s.Assertions, tv2, vbUnpinned, tv2.Deployment(), nil, nil)
		s.verifyVersioningSAs(tv2, vbUnpinned, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, tv1, tv2)
	}
}

func (s *Versioning3Suite) TestDoubleTransition() {
	s.testDoubleTransition(false, false)
}

func (s *Versioning3Suite) TestDoubleTransition_WithSignal() {
	s.testDoubleTransition(false, true)
}

func (s *Versioning3Suite) TestDoubleTransitionFromUnversioned() {
	s.testDoubleTransition(true, false)
}

func (s *Versioning3Suite) TestDoubleTransitionFromUnversioned_WithSignal() {
	s.testDoubleTransition(true, true)
}

func (s *Versioning3Suite) testDoubleTransition(unversionedSrc bool, signal bool) {
	// Scenario: make sure a transition on top of another transition works properly. Steps:
	// 1. Start a workflow, process first task with sourceV (which is v1 or unversioned depending on passed `unversionedSrc`).
	//    The first wft schedules an activity.
	// 2. (optional) Send a signal to wf so we generate a pending (not started) wft
	// 3. Set current version to v2.
	// 4. Poll activity from v2 worker. Activity is not dispatched but a transition starts from sourceV -> v2.
	// 5. Change routing config back to sourceV.
	// 6. Send a wf task poller from sourceV. This should get the pending wft and replace the transition to v2 with a transition
	//    back to sourceV. Note that in this case versioning info has a transition target same as the existing version.
	//    Therefore, a transition from x -> x is valid (although, can only happen indirectly by another hop).
	// 7. Once the wft completes we make sure the wf is on sourceV without any transition.
	// 8. Not that the transition is complete, the activity should be able to go to sourceV poller.
	// 9. Set v2 as current again and ensure the wf can complete on it.

	tv1 := testvars.New(s).WithBuildIDNumber(1)
	tv2 := tv1.WithBuildIDNumber(2)
	sourceV := tv1.Deployment()
	sourceVB := vbUnpinned
	sourceTransition := tv1.DeploymentVersionTransition()
	if unversionedSrc {
		sourceV = nil
		sourceVB = vbUnspecified
		sourceTransition = &workflowpb.DeploymentVersionTransition{Version: "__unversioned__"}
	}

	runId := s.startWorkflow(tv1, nil)

	if !unversionedSrc {
		// sourceV is v1, set current version to it
		if s.useNewDeploymentData {
			s.updateTaskQueueDeploymentDataWithRoutingConfig(tv1, &deploymentpb.RoutingConfig{
				CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv1.DeploymentVersionString()),
				CurrentVersionChangedTime: timestamp.TimePtr(time.Now()),
				RevisionNumber:            1,
			}, map[string]*deploymentspb.WorkerDeploymentVersionData{tv1.DeploymentVersion().GetBuildId(): {
				Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
			}}, []string{}, tqTypeWf, tqTypeAct)
		} else {
			s.updateTaskQueueDeploymentData(tv1, true, 0, false, 0, tqTypeWf, tqTypeAct)
		}
	}

	s.doPollWftAndHandle(tv1, !unversionedSrc, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondWftWithActivities(tv1, tv1, false, sourceVB, "5"), nil
		})
	s.verifyWorkflowVersioning(s.Assertions, tv1, sourceVB, sourceV, nil, nil)

	if signal {
		// Send a signal so a wf task is scheduled before we poll the activity
		err := s.SdkClient().SignalWorkflow(context.Background(), tv1.WorkflowID(), runId, "test-signal", nil)
		s.NoError(err)
	}

	// set current version to v2
	if s.useNewDeploymentData {
		s.updateTaskQueueDeploymentDataWithRoutingConfig(tv2, &deploymentpb.RoutingConfig{
			CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv2.DeploymentVersionString()),
			CurrentVersionChangedTime: timestamp.TimePtr(time.Now()),
			RevisionNumber:            2,
		}, map[string]*deploymentspb.WorkerDeploymentVersionData{tv2.DeploymentVersion().GetBuildId(): {
			Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
		}, tv1.DeploymentVersion().GetBuildId(): {
			Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
		}}, []string{}, tqTypeWf, tqTypeAct)
	} else {
		s.updateTaskQueueDeploymentData(tv2, true, 0, false, 0, tqTypeWf, tqTypeAct)
	}
	// poll activity from v2 worker, this should start a transition but should not immediately start the activity.
	go s.idlePollActivity(context.Background(), tv2, true, time.Minute, "v2 worker should not receive the activity")

	s.EventuallyWithT(func(t *assert.CollectT) {
		dwf, err := s.FrontendClient().DescribeWorkflowExecution(
			context.Background(), &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: s.Namespace().String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: tv1.WorkflowID(),
				},
			},
		)
		s.NoError(err)
		a := assert.New(t)
		a.Equal(tv2.DeploymentVersionTransition(), dwf.WorkflowExecutionInfo.GetVersioningInfo().GetVersionTransition())
	}, 10*time.Second, 100*time.Millisecond)

	// Back to sourceV
	if unversionedSrc {
		if s.useNewDeploymentData {
			s.updateTaskQueueDeploymentDataWithRoutingConfig(tv2, &deploymentpb.RoutingConfig{
				CurrentDeploymentVersion:  nil,
				CurrentVersionChangedTime: timestamp.TimePtr(time.Now()),
				RevisionNumber:            3,
			}, map[string]*deploymentspb.WorkerDeploymentVersionData{}, []string{}, tqTypeWf, tqTypeAct)
		} else {
			s.updateTaskQueueDeploymentData(tv2, false, 0, false, 0, tqTypeWf, tqTypeAct)
		}
	} else {
		if s.useNewDeploymentData {
			s.updateTaskQueueDeploymentDataWithRoutingConfig(tv1, &deploymentpb.RoutingConfig{
				CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv1.DeploymentVersionString()),
				CurrentVersionChangedTime: timestamp.TimePtr(time.Now()),
				RevisionNumber:            3,
			}, map[string]*deploymentspb.WorkerDeploymentVersionData{tv1.DeploymentVersion().GetBuildId(): {
				Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
			}, tv2.DeploymentVersion().GetBuildId(): {
				Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
			}}, []string{}, tqTypeWf, tqTypeAct)
		} else {
			s.updateTaskQueueDeploymentData(tv1, true, 0, false, 0, tqTypeWf, tqTypeAct)
		}
	}

	// Now poll for wf task from sourceV while there is a transition to v2
	// this poller should clear the transition and complete the wft.
	s.doPollWftAndHandle(tv1, !unversionedSrc, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.verifyWorkflowVersioning(s.Assertions, tv1, sourceVB, sourceV, nil, sourceTransition)
			return respondEmptyWft(tv1, false, sourceVB), nil
		})
	s.verifyWorkflowVersioning(s.Assertions, tv1, sourceVB, sourceV, nil, nil)

	// Activity should be unblocked now to sourceV poller
	s.doPollActivityAndHandle(tv1, !unversionedSrc, nil,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			s.NotNil(task)
			s.verifyWorkflowVersioning(s.Assertions, tv1, sourceVB, sourceV, nil, nil)
			return respondActivity(), nil
		})

	// Set v2 as the current version again
	if s.useNewDeploymentData {
		s.updateTaskQueueDeploymentDataWithRoutingConfig(tv2, &deploymentpb.RoutingConfig{
			CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv2.DeploymentVersionString()),
			CurrentVersionChangedTime: timestamp.TimePtr(time.Now()),
			RevisionNumber:            4,
		}, map[string]*deploymentspb.WorkerDeploymentVersionData{tv2.DeploymentVersion().GetBuildId(): {
			Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
		}, tv1.DeploymentVersion().GetBuildId(): {
			Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
		}}, []string{}, tqTypeWf, tqTypeAct)
	} else {
		s.updateTaskQueueDeploymentData(tv2, true, 0, false, 0, tqTypeWf, tqTypeAct)
	}

	s.pollWftAndHandle(tv2, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.verifyWorkflowVersioning(s.Assertions, tv2, sourceVB, sourceV, nil, tv2.DeploymentVersionTransition())
			return respondCompleteWorkflow(tv2, vbUnpinned), nil
		})
	s.verifyWorkflowVersioning(s.Assertions, tv2, vbUnpinned, tv2.Deployment(), nil, nil)
}

func (s *Versioning3Suite) TestNexusTask_StaysOnCurrentDeployment() {
	s.RunTestWithMatchingBehavior(
		func() {
			s.nexusTaskStaysOnCurrentDeployment()
		},
	)
}

func (s *Versioning3Suite) nexusTaskStaysOnCurrentDeployment() {
	tv1 := testvars.New(s).WithBuildIDNumber(1)
	tv2 := tv1.WithBuildIDNumber(2)

	nexusRequest := &matchingservice.DispatchNexusTaskRequest{
		NamespaceId: s.NamespaceID().String(),
		TaskQueue:   tv1.TaskQueue(),
		Request: &nexuspb.Request{
			Header: map[string]string{
				// placeholder value as passing in an empty map would result in protoc deserializing
				// it as nil, which breaks existing logic inside of matching
				tv1.Any().String(): tv1.Any().String(),
			},
		},
	}

	// current deployment is -> tv1
	if s.useNewDeploymentData {
		s.updateTaskQueueDeploymentDataWithRoutingConfig(tv1, &deploymentpb.RoutingConfig{
			CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv1.DeploymentVersionString()),
			CurrentVersionChangedTime: timestamp.TimePtr(time.Now()),
			RevisionNumber:            1,
		}, map[string]*deploymentspb.WorkerDeploymentVersionData{tv1.DeploymentVersion().GetBuildId(): {
			Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
		}}, []string{}, tqTypeNexus)
	} else {
		s.updateTaskQueueDeploymentData(tv1, true, 0, false, 0, tqTypeNexus)
	}

	// local poller with deployment A receives task
	s.pollAndDispatchNexusTask(tv1, nexusRequest)

	// current deployment is now -> tv2
	if s.useNewDeploymentData {
		s.updateTaskQueueDeploymentDataWithRoutingConfig(tv2, &deploymentpb.RoutingConfig{
			CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv2.DeploymentVersionString()),
			CurrentVersionChangedTime: timestamp.TimePtr(time.Now()),
			RevisionNumber:            2,
		}, map[string]*deploymentspb.WorkerDeploymentVersionData{tv2.DeploymentVersion().GetBuildId(): {
			Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
		}, tv1.DeploymentVersion().GetBuildId(): {
			Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
		}}, []string{}, tqTypeNexus)
	} else {
		s.updateTaskQueueDeploymentData(tv2, true, 0, false, 0, tqTypeNexus)
	}

	// Pollers of tv1 are there but should not get any task
	go s.idlePollNexus(tv1, true, ver3MinPollTime, "nexus task should not go to the old deployment")

	s.pollAndDispatchNexusTask(tv2, nexusRequest)
}

func (s *Versioning3Suite) pollAndDispatchNexusTask(
	tv *testvars.TestVars,
	nexusRequest *matchingservice.DispatchNexusTaskRequest,
) {
	matchingClient := s.GetTestCluster().MatchingClient()

	nexusCompleted := make(chan any)
	s.pollNexusTaskAndHandle(tv, false, nexusCompleted,
		func(task *workflowservice.PollNexusTaskQueueResponse) (*workflowservice.RespondNexusTaskCompletedRequest, error) {
			s.NotNil(task)
			return &workflowservice.RespondNexusTaskCompletedRequest{}, nil // response object gets filled during processing
		})

	_, err := matchingClient.DispatchNexusTask(context.Background(), nexusRequest)
	s.NoError(err)
	<-nexusCompleted
}

func (s *Versioning3Suite) TestEagerActivity() {
	// The first WFT asks for an activity to starts and get it eagerly in the WFT completion
	// response. The activity is processed without issues and wf completes.

	s.OverrideDynamicConfig(dynamicconfig.EnableActivityEagerExecution, true)
	tv := testvars.New(s)

	if s.useNewDeploymentData {
		s.updateTaskQueueDeploymentDataWithRoutingConfig(tv, &deploymentpb.RoutingConfig{
			CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv.DeploymentVersionString()),
			CurrentVersionChangedTime: timestamp.TimePtr(time.Now()),
			RevisionNumber:            1,
		}, map[string]*deploymentspb.WorkerDeploymentVersionData{tv.DeploymentVersion().GetBuildId(): {
			Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
		}}, []string{}, tqTypeWf, tqTypeAct)
	} else {
		s.updateTaskQueueDeploymentData(tv, true, 0, false, 0, tqTypeWf, tqTypeAct)
	}
	s.startWorkflow(tv, nil)

	poller, resp := s.pollWftAndHandle(tv, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.verifyWorkflowVersioning(s.Assertions, tv, vbUnspecified, nil, nil, tv.DeploymentVersionTransition())
			resp := respondWftWithActivities(tv, tv, true, vbUnpinned, "5")
			resp.Commands[0].GetScheduleActivityTaskCommandAttributes().RequestEagerExecution = true
			return resp, nil
		})
	s.verifyWorkflowVersioning(s.Assertions, tv, vbUnpinned, tv.Deployment(), nil, nil)

	s.NotEmpty(resp.GetActivityTasks())

	_, err := poller.HandleActivityTask(tv, resp.GetActivityTasks()[0],
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondActivity(), nil
		})
	s.NoError(err)
	s.verifyWorkflowVersioning(s.Assertions, tv, vbUnpinned, tv.Deployment(), nil, nil)

	s.pollWftAndHandle(tv, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondCompleteWorkflow(tv, vbUnpinned), nil
		})
	s.verifyWorkflowVersioning(s.Assertions, tv, vbUnpinned, tv.Deployment(), nil, nil)
}

func (s *Versioning3Suite) TestTransitionFromActivity_Sticky() {
	s.testTransitionFromActivity(true)
}

func (s *Versioning3Suite) TestTransitionFromActivity_NoSticky() {
	s.testTransitionFromActivity(false)
}

func (s *Versioning3Suite) testTransitionFromActivity(sticky bool) {
	// The workflow runs one WFT on d1 which schedules four activities, then:
	// 1. The first and second activities starts on d1
	// 2. Current deployment becomes d2
	// 3. The third activity is redirected to d2 and starts a transition in the wf, without being
	//    dispatched.
	// 4. The 4th activity also does not start on any of the builds although there are pending
	//    pollers on both.
	// 5. The transition generates a WFT, and it is started in d2.
	// 6. The 1st act is completed here while the transition is going on.
	// 7. The 2nd act fails and makes another attempt. But it is not dispatched.
	// 8. WFT completes and the transition completes.
	// 9. All the 3 remaining activities are now dispatched and completed.

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tv1 := testvars.New(s).WithBuildIDNumber(1)
	tv2 := tv1.WithBuildIDNumber(2)
	if sticky {
		s.warmUpSticky(tv1)
	}

	if s.useNewDeploymentData {
		s.updateTaskQueueDeploymentDataWithRoutingConfig(tv1, &deploymentpb.RoutingConfig{
			CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv1.DeploymentVersionString()),
			CurrentVersionChangedTime: timestamp.TimePtr(time.Now()),
			RevisionNumber:            1,
		}, map[string]*deploymentspb.WorkerDeploymentVersionData{tv1.DeploymentVersion().GetBuildId(): {
			Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
		}}, []string{}, tqTypeWf, tqTypeAct)
	} else {
		s.updateTaskQueueDeploymentData(tv1, true, 0, false, 0, tqTypeWf, tqTypeAct)
	}
	runID := s.startWorkflow(tv1, nil)

	s.pollWftAndHandle(tv1, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.verifyWorkflowVersioning(s.Assertions, tv1, vbUnspecified, nil, nil, tv1.DeploymentVersionTransition())
			return respondWftWithActivities(tv1, tv1, sticky, vbUnpinned, "5", "6", "7", "8"), nil
		})
	s.verifyWorkflowVersioning(s.Assertions, tv1, vbUnpinned, tv1.Deployment(), nil, nil)
	if sticky {
		s.verifyWorkflowStickyQueue(tv1.WithRunID(runID))
	}

	transitionCompleted := atomic.Bool{}
	transitionStarted := make(chan struct{})
	act1Started := make(chan struct{})
	act1Completed := make(chan struct{})
	act2Started := make(chan struct{})
	act2Failed := make(chan struct{})
	act2To4Completed := make(chan struct{})

	// 1. Start 1st and 2nd activities
	s.pollActivityAndHandle(tv1, act1Completed,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			s.NotNil(task)
			s.Logger.Info(fmt.Sprintf("Activity 1 started ID: %s", task.ActivityId))
			close(act1Started)
			// block until the transition WFT starts
			<-transitionStarted
			// 6. the 1st act completes during transition
			s.Logger.Info(fmt.Sprintf("Activity 1 completed ID: %s", task.ActivityId))
			return respondActivity(), nil
		})

	s.WaitForChannel(ctx, act1Started)
	s.pollActivityAndHandle(tv1, act2Failed,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			s.NotNil(task)
			s.Logger.Info(fmt.Sprintf("Activity 2 started ID: %s", task.ActivityId))
			close(act2Started)
			// block until the transition WFT starts
			<-transitionStarted
			// 7. 2nd activity fails. Respond with error so it is retried.
			s.Logger.Info(fmt.Sprintf("Activity 2 failed ID: %s", task.ActivityId))
			return nil, errors.New("intentional activity failure")
		})

	s.WaitForChannel(ctx, act2Started)
	s.verifyWorkflowVersioning(s.Assertions, tv1, vbUnpinned, tv1.Deployment(), nil, nil)

	// 2. Set d2 as the current deployment
	if s.useNewDeploymentData {
		s.updateTaskQueueDeploymentDataWithRoutingConfig(tv2, &deploymentpb.RoutingConfig{
			CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv2.DeploymentVersionString()),
			CurrentVersionChangedTime: timestamp.TimePtr(time.Now()),
			RevisionNumber:            2,
		}, map[string]*deploymentspb.WorkerDeploymentVersionData{tv2.DeploymentVersion().GetBuildId(): {
			Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
		}, tv1.DeploymentVersion().GetBuildId(): {
			Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
		}}, []string{}, tqTypeWf, tqTypeAct)
	} else {
		s.updateTaskQueueDeploymentData(tv2, true, 0, false, 0, tqTypeWf, tqTypeAct)
	}
	// Although updateTaskQueueDeploymentData waits for deployment data to reach the TQs, backlogged
	// tasks might still be waiting behind the old deployment's poll channel. Partition manage should
	// immediately react to the deployment data changes, but there still is a race possible and the
	// only way to safeguard against it is to wait a little while before proceeding.
	time.Sleep(time.Millisecond * 200) //nolint:forbidigo

	// Pollers of d1 are there, but should not get any task
	go s.idlePollActivity(ctx, tv1, true, ver3MinPollTime, "activities should not go to the old deployment")

	go func() {
		for i := 2; i <= 4; i++ {
			// 3-4. The new d2 poller should trigger the third activity to be redirected, but the activity should
			// not start until transition completes in the next wft.
			// Repeating the handler so it processes all the three activities
			s.pollActivityAndHandle(tv2, nil,
				func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
					// Activity should not start until the transition is completed
					s.True(transitionCompleted.Load())
					s.NotNil(task)
					s.Logger.Info(fmt.Sprintf("Remaining activity completed ID: %s", task.ActivityId))
					return respondActivity(), nil
				})
		}
		close(act2To4Completed)
	}()

	// 5. The transition should create a new WFT to be sent to d2.
	s.pollWftAndHandle(tv2, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.verifyWorkflowVersioning(s.Assertions, tv1, vbUnpinned, tv1.Deployment(), nil, tv2.DeploymentVersionTransition())
			close(transitionStarted)
			s.Logger.Info("Transition wft started")
			// 8. Complete the transition after act1 completes and act2's first attempt fails.
			<-act1Completed
			<-act2Failed
			transitionCompleted.Store(true)
			s.Logger.Info("Transition wft completed")
			return respondEmptyWft(tv2, sticky, vbUnpinned), nil
		})
	s.verifyWorkflowVersioning(s.Assertions, tv2, vbUnpinned, tv2.Deployment(), nil, nil)
	if sticky {
		s.verifyWorkflowStickyQueue(tv2)
	}

	// 9. Now all activities should complete.
	s.WaitForChannel(ctx, act2To4Completed)
	s.pollWftAndHandle(tv2, sticky, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.Logger.Info("Final wft completed")
			return respondCompleteWorkflow(tv2, vbUnpinned), nil
		})
	s.verifyWorkflowVersioning(s.Assertions, tv2, vbUnpinned, tv2.Deployment(), nil, nil)
}

func (s *Versioning3Suite) TestIndependentVersionedActivity_Pinned() {
	s.testIndependentActivity(vbPinned, false)
}

func (s *Versioning3Suite) TestIndependentVersionedActivity_Unpinned() {
	s.testIndependentActivity(vbUnpinned, false)
}

func (s *Versioning3Suite) TestIndependentUnversionedActivity_Pinned() {
	s.testIndependentActivity(vbPinned, true)
}

func (s *Versioning3Suite) TestIndependentUnversionedActivity_Unpinned() {
	s.testIndependentActivity(vbUnpinned, true)
}

func (s *Versioning3Suite) testIndependentActivity(behavior enumspb.VersioningBehavior, unversionedActivity bool) {
	// This test starts a wf on wf-series. The workflow runs an activity that is sent to act-tq with
	// workers on a different deployment series, act-series. We make sure that the activity is
	// dispatched and processed properly without affecting versioning of the workflow. Note that it
	// is not required for independent activities to use a different TQ name but in here we test the
	// more common case where the TQ name is different.

	tvWf := testvars.New(s).WithDeploymentSeriesNumber(1)
	tvAct := testvars.New(s).WithDeploymentSeriesNumber(2).WithTaskQueueNumber(2)

	// Set current deployment for each TQ
	if s.useNewDeploymentData {
		s.updateTaskQueueDeploymentDataWithRoutingConfig(tvWf, &deploymentpb.RoutingConfig{
			CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tvWf.DeploymentVersionString()),
			CurrentVersionChangedTime: timestamp.TimePtr(time.Now()),
			RevisionNumber:            1,
		}, map[string]*deploymentspb.WorkerDeploymentVersionData{tvWf.DeploymentVersion().GetBuildId(): {
			Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
		}}, []string{}, tqTypeWf)

		if !unversionedActivity {
			// Different deployment here for the activity TQ.
			s.updateTaskQueueDeploymentDataWithRoutingConfig(tvAct, &deploymentpb.RoutingConfig{
				CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tvAct.DeploymentVersionString()),
				CurrentVersionChangedTime: timestamp.TimePtr(time.Now()),
				RevisionNumber:            1,
			}, map[string]*deploymentspb.WorkerDeploymentVersionData{tvAct.DeploymentVersion().GetBuildId(): {
				Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
			}}, []string{}, tqTypeAct)
		}
	} else {
		s.updateTaskQueueDeploymentData(tvWf, true, 0, false, 0, tqTypeWf)
		if !unversionedActivity {
			s.updateTaskQueueDeploymentData(tvAct, true, 0, false, 0, tqTypeAct)
		}
	}

	s.startWorkflow(tvWf, nil)

	s.pollWftAndHandle(tvWf, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.verifyWorkflowVersioning(s.Assertions, tvWf, vbUnspecified, nil, nil, tvWf.DeploymentVersionTransition())
			s.Logger.Info("First wf task completed")
			return respondWftWithActivities(tvWf, tvAct, false, behavior, "5"), nil
		})
	s.verifyWorkflowVersioning(s.Assertions, tvWf, behavior, tvWf.Deployment(), nil, nil)

	if unversionedActivity {
		s.unversionedPollActivityAndHandle(tvAct, nil,
			func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
				s.NotNil(task)
				s.Logger.Info("Activity completed")
				return respondActivity(), nil
			})
	} else {
		s.pollActivityAndHandle(tvAct, nil,
			func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
				s.NotNil(task)
				s.Logger.Info("Activity completed")
				return respondActivity(), nil
			})
	}
	s.verifyWorkflowVersioning(s.Assertions, tvWf, behavior, tvWf.Deployment(), nil, nil)

	s.pollWftAndHandle(tvWf, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondCompleteWorkflow(tvWf, behavior), nil
		})
	s.verifyWorkflowVersioning(s.Assertions, tvWf, behavior, tvWf.Deployment(), nil, nil)
}

func (s *Versioning3Suite) TestChildWorkflowInheritance_PinnedParent() {
	s.testChildWorkflowInheritance_ExpectInherit(false, false, vbPinned)
}

func (s *Versioning3Suite) TestChildWorkflowInheritance_ParentPinnedByOverride() {
	s.testChildWorkflowInheritance_ExpectInherit(false, true, vbUnpinned)
}

func (s *Versioning3Suite) TestChildWorkflowInheritance_CrossTQ_Inherit() {
	// the ExpectInherit helper polls on the child's task queue with the parent's version,
	// so we expect the version to be inherited
	s.testChildWorkflowInheritance_ExpectInherit(true, false, vbPinned)
}

func (s *Versioning3Suite) testChildWorkflowInheritance_ExpectInherit(crossTq bool, withOverride bool, parentRegistrationBehavior enumspb.VersioningBehavior) {
	// Child wf of a pinned parent starts on the parents pinned version.

	tv1 := testvars.New(s).WithBuildIDNumber(1).WithWorkflowIDNumber(1)
	tv2 := tv1.WithBuildIDNumber(2)
	tv1Child := tv1.WithWorkflowIDNumber(2)
	if crossTq {
		tv1Child = tv1Child.WithTaskQueue("child-tq")
	}
	tv2Child := tv1Child.WithBuildIDNumber(2)

	var override *workflowpb.VersioningOverride
	if withOverride {
		override = tv1.VersioningOverridePinned(s.useV32)
	}

	// This is the registered behavior which can be unpinned, but only if withOverride. We want
	// parent's effective behavior to always be pinned.
	sdkParentBehavior := workflow.VersioningBehaviorPinned
	if parentRegistrationBehavior == vbUnpinned {
		sdkParentBehavior = workflow.VersioningBehaviorAutoUpgrade
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wfStarted := make(chan struct{}, 1)
	currentChanged := make(chan struct{}, 1)

	childv1 := func(ctx workflow.Context) (string, error) {
		s.verifyWorkflowVersioning(s.Assertions, tv1Child, vbPinned, tv1Child.Deployment(), override, nil)
		return "v1", nil
	}
	wf1 := func(ctx workflow.Context) (string, error) {
		wfStarted <- struct{}{}
		// wait for current version to change
		<-currentChanged

		// run two child workflows
		fut1 := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			TaskQueue:  tv1Child.TaskQueue().GetName(),
			WorkflowID: tv1Child.WorkflowID(),
		}), "child")
		var val1 string
		s.NoError(fut1.Get(ctx, &val1))

		s.verifyWorkflowVersioning(s.Assertions, tv1, parentRegistrationBehavior, tv1.Deployment(), override, nil)
		return val1, nil
	}

	if crossTq {
		w1xtq := worker.New(s.SdkClient(), tv1Child.TaskQueue().GetName(), worker.Options{
			DeploymentOptions: worker.DeploymentOptions{
				Version:                   tv1Child.SDKDeploymentVersion(),
				UseVersioning:             true,
				DefaultVersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
			},
			MaxConcurrentWorkflowTaskPollers: numPollers,
		})
		w1xtq.RegisterWorkflowWithOptions(childv1, workflow.RegisterOptions{Name: "child", VersioningBehavior: workflow.VersioningBehaviorPinned})
		s.NoError(w1xtq.Start())
		defer w1xtq.Stop()
	}

	w1 := worker.New(s.SdkClient(), tv1.TaskQueue().GetName(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			Version:                   tv1.SDKDeploymentVersion(),
			UseVersioning:             true,
			DefaultVersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
		},
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf", VersioningBehavior: sdkParentBehavior})
	if !crossTq {
		w1.RegisterWorkflowWithOptions(childv1, workflow.RegisterOptions{Name: "child", VersioningBehavior: workflow.VersioningBehaviorPinned})
	}
	s.NoError(w1.Start())
	defer w1.Stop()

	// v1 is current for both parent and child
	s.setCurrentDeployment(tv1)

	startOpts := sdkclient.StartWorkflowOptions{
		ID:                  tv1.WorkflowID(),
		TaskQueue:           tv1.TaskQueue().GetName(),
		VersioningOverride:  nil,
		WorkflowTaskTimeout: 10 * time.Second,
	}
	if withOverride {
		startOpts.VersioningOverride = &sdkclient.PinnedVersioningOverride{
			Version: tv1.SDKDeploymentVersion(),
		}
	}
	run, err := s.SdkClient().ExecuteWorkflow(ctx, startOpts, "wf")
	s.NoError(err)
	// wait for it to start on v1
	s.WaitForChannel(ctx, wfStarted)
	close(wfStarted) // force panic if replayed

	// make v2 current for both parent and child and unblock the wf to start the child
	if s.useNewDeploymentData {
		s.updateTaskQueueDeploymentDataWithRoutingConfig(tv2, &deploymentpb.RoutingConfig{
			CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv2.DeploymentVersionString()),
			CurrentVersionChangedTime: timestamp.TimePtr(time.Now()),
			RevisionNumber:            2,
		}, map[string]*deploymentspb.WorkerDeploymentVersionData{tv2.DeploymentVersion().GetBuildId(): {
			Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
		}}, []string{}, tqTypeWf)
	} else {
		s.updateTaskQueueDeploymentData(tv2, true, 0, false, 0, tqTypeWf)
	}
	if crossTq {
		if s.useNewDeploymentData {
			s.updateTaskQueueDeploymentDataWithRoutingConfig(tv2Child, &deploymentpb.RoutingConfig{
				CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv2Child.DeploymentVersionString()),
				CurrentVersionChangedTime: timestamp.TimePtr(time.Now()),
				RevisionNumber:            2,
			}, map[string]*deploymentspb.WorkerDeploymentVersionData{tv2Child.DeploymentVersion().GetBuildId(): {
				Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
			}}, []string{}, tqTypeWf)
		} else {
			s.updateTaskQueueDeploymentData(tv2Child, true, 0, false, 0, tqTypeWf)
		}
	}
	currentChanged <- struct{}{}

	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("v1", out)
}

func (s *Versioning3Suite) TestChildWorkflowInheritance_UnpinnedParent() {
	s.testChildWorkflowInheritance_ExpectNoInherit(false, vbUnpinned)
}

func (s *Versioning3Suite) TestChildWorkflowInheritance_CrossTQ_NoInherit() {
	// the ExpectNoInherit helper does NOT poll on the child's task queue with the parent's version,
	// so we DO NOT expect the version to be inherited
	s.testChildWorkflowInheritance_ExpectNoInherit(true, vbPinned)
}

func (s *Versioning3Suite) testChildWorkflowInheritance_ExpectNoInherit(crossTq bool, parentBehavior enumspb.VersioningBehavior) {
	// Child wf of an unpinned parent is always started on the Current Version of its TQ.
	// For the time being, cross-TQ children do not inherit parents pinned version until that part
	// is implemented.

	tv1 := testvars.New(s).WithBuildIDNumber(1).WithWorkflowIDNumber(1)
	tv1Child := tv1.WithWorkflowIDNumber(2)
	if crossTq {
		tv1Child = tv1Child.WithTaskQueue("child-tq")
	}
	tv2 := tv1.WithBuildIDNumber(2)
	tv2Child := tv1Child.WithBuildIDNumber(2)

	sdkParentBehavior := workflow.VersioningBehaviorPinned
	if parentBehavior == vbUnpinned {
		sdkParentBehavior = workflow.VersioningBehaviorAutoUpgrade
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	wfStarted := make(chan struct{}, 10)
	currentChanged := make(chan struct{}, 10)

	childv1 := func(ctx workflow.Context) (string, error) {
		panic("child should not run on v1")
	}
	childv2 := func(ctx workflow.Context) (string, error) {
		return "v2", nil
	}
	wf1 := func(ctx workflow.Context) (string, error) {
		wfStarted <- struct{}{}
		// wait for current version to change
		<-currentChanged

		fut1 := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			TaskQueue:  tv2Child.TaskQueue().GetName(),
			WorkflowID: tv2Child.WorkflowID(),
		}), "child")

		var val1 string
		s.NoError(fut1.Get(ctx, &val1))

		s.verifyWorkflowVersioning(s.Assertions, tv1, parentBehavior, tv1.Deployment(), nil, nil)
		return val1, nil
	}

	// Same as v1 without channel blocking
	wf2 := func(ctx workflow.Context) (string, error) {
		fut1 := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			TaskQueue:  tv2Child.TaskQueue().GetName(),
			WorkflowID: tv2Child.WorkflowID(),
		}), "child")
		var val1 string
		s.NoError(fut1.Get(ctx, &val1))

		return val1, nil
	}

	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.FrontendGRPCAddress(),
		Namespace: s.Namespace().String(),
	})
	s.NoError(err)

	w1 := worker.New(sdkClient, tv1.TaskQueue().GetName(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			Version:                   tv1.SDKDeploymentVersion(),
			UseVersioning:             true,
			DefaultVersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
		},
		MaxConcurrentWorkflowTaskPollers: numPollers,
		// Because the wf task can get stuck behind a channel, we make sure sdk does not panic too early
		DeadlockDetectionTimeout: 30 * time.Second,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf", VersioningBehavior: sdkParentBehavior})
	if !crossTq {
		w1.RegisterWorkflowWithOptions(childv1, workflow.RegisterOptions{Name: "child", VersioningBehavior: workflow.VersioningBehaviorPinned})
	}
	s.NoError(w1.Start())
	defer w1.Stop()

	// v1 is current for both parent and child
	s.setCurrentDeployment(tv1)

	if crossTq {
		w2xtq := worker.New(sdkClient, tv2Child.TaskQueue().GetName(), worker.Options{
			DeploymentOptions: worker.DeploymentOptions{
				Version:                   tv2Child.SDKDeploymentVersion(),
				UseVersioning:             true,
				DefaultVersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
			},
			MaxConcurrentWorkflowTaskPollers: numPollers,
		})
		w2xtq.RegisterWorkflowWithOptions(childv2, workflow.RegisterOptions{Name: "child", VersioningBehavior: workflow.VersioningBehaviorPinned})
		s.NoError(w2xtq.Start())
		defer w2xtq.Stop()
	}
	w2 := worker.New(sdkClient, tv2.TaskQueue().GetName(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			Version:                   tv2.SDKDeploymentVersion(),
			UseVersioning:             true,
			DefaultVersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
		},
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf", VersioningBehavior: sdkParentBehavior})
	if !crossTq {
		w2.RegisterWorkflowWithOptions(childv2, workflow.RegisterOptions{Name: "child", VersioningBehavior: workflow.VersioningBehaviorPinned})
	}
	s.NoError(w2.Start())
	defer w2.Stop()

	run, err := sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                  tv1.WorkflowID(),
		TaskQueue:           tv1.TaskQueue().GetName(),
		WorkflowTaskTimeout: 30 * time.Second,
	}, "wf")
	s.NoError(err)

	// wait for it to start on v1
	s.WaitForChannel(ctx, wfStarted)
	close(wfStarted)

	// make v2 current for both parent and child and unblock the wf to start the child
	s.setCurrentDeployment(tv2)

	currentChanged <- struct{}{}

	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("v2", out)

	if parentBehavior == vbPinned {
		s.verifyWorkflowVersioning(s.Assertions, tv1, parentBehavior, tv1.Deployment(), nil, nil)
	} else {
		s.verifyWorkflowVersioning(s.Assertions, tv1, parentBehavior, tv2.Deployment(), nil, nil)
	}
	s.verifyWorkflowVersioning(s.Assertions, tv2Child, vbPinned, tv2Child.Deployment(), nil, nil)
}

func (s *Versioning3Suite) TestPinnedCaN_SameTQ() {
	s.testCan(false, vbPinned, false, true, false)
}

func (s *Versioning3Suite) TestPinnedCaN_SameTQ_PinnedOverride() {
	s.testCan(false, vbPinned, false, true, true)
}

func (s *Versioning3Suite) TestPinnedCaN_CrossTQ_Inherit() {
	s.testCan(true, vbPinned, false, true, false)
}

func (s *Versioning3Suite) TestPinnedCaN_CrossTQ_NoInherit() {
	s.testCan(true, vbPinned, false, false, false)
}

func (s *Versioning3Suite) TestPinnedCaN_upgradeOnCaN_SameTQ() {
	s.T().Skip("run after SDK exposes CaN option")
	s.testCan(false, vbPinned, true, false, false)
}

func (s *Versioning3Suite) TestPinnedCaN_upgradeOnCaN_CrossTQ_Inherit() {
	s.T().Skip("run after SDK exposes CaN option")
	s.testCan(true, vbPinned, true, true, false)
}

func (s *Versioning3Suite) TestPinnedCaN_upgradeOnCaN_CrossTQ_NoInherit() {
	s.T().Skip("run after SDK exposes CaN option")
	s.testCan(true, vbPinned, true, false, false)
}

func (s *Versioning3Suite) TestUnpinnedCaN() {
	s.testCan(false, vbUnpinned, false, false, false)
}

func (s *Versioning3Suite) TestUnpinnedCaN_upgradeOnCaN() {
	s.T().Skip("run after SDK exposes CaN option")
	s.testCan(false, vbUnpinned, true, false, false)
}

// TestPinnedCaN_UpgradeOnCaN_NormalWFT tests ContinueAsNew with a normal WFT triggered by signal.
func (s *Versioning3Suite) TestPinnedCaN_UpgradeOnCaN_NormalWFT_WithSuggest() {
	s.testPinnedCaNUpgradeOnCaN(true, false, false, false, true)
}

// TestPinnedCaN_UpgradeOnCaN_SpeculativeWFT tests ContinueAsNew with a speculative WFT triggered by update.
func (s *Versioning3Suite) TestPinnedCaN_UpgradeOnCaN_SpeculativeWFT_WithSuggest() {
	s.testPinnedCaNUpgradeOnCaN(false, true, false, false, true)
}

// TestPinnedCaN_UpgradeOnCaN_TransientWFT tests ContinueAsNew with a transient WFT (failed + retry).
func (s *Versioning3Suite) TestPinnedCaN_UpgradeOnCaN_TransientWFT_WithSuggest() {
	s.testPinnedCaNUpgradeOnCaN(false, false, true, false, true)
}

// TestPinnedCaN_UpgradeOnCaN_NormalWFT tests ContinueAsNew with a normal WFT triggered by signal.
func (s *Versioning3Suite) TestPinnedCaN_UpgradeOnCaN_NormalWFT_PinnedOverride_WithSuggest() {
	s.testPinnedCaNUpgradeOnCaN(true, false, false, true, true)
}

// TestPinnedCaN_UpgradeOnCaN_NormalWFT tests ContinueAsNew with a normal WFT triggered by signal.
func (s *Versioning3Suite) TestPinnedCaN_UpgradeOnCaN_NormalWFT_NoSuggest() {
	s.testPinnedCaNUpgradeOnCaN(true, false, false, false, false)
}

// TestPinnedCaN_UpgradeOnCaN_SpeculativeWFT tests ContinueAsNew with a speculative WFT triggered by update.
func (s *Versioning3Suite) TestPinnedCaN_UpgradeOnCaN_SpeculativeWFT_NoSuggest() {
	s.testPinnedCaNUpgradeOnCaN(false, true, false, false, false)
}

// TestPinnedCaN_UpgradeOnCaN_TransientWFT tests ContinueAsNew with a transient WFT (failed + retry).
func (s *Versioning3Suite) TestPinnedCaN_UpgradeOnCaN_TransientWFT_NoSuggest() {
	s.testPinnedCaNUpgradeOnCaN(false, false, true, false, false)
}

// TestPinnedCaN_UpgradeOnCaN_NormalWFT tests ContinueAsNew with a normal WFT triggered by signal.
func (s *Versioning3Suite) TestPinnedCaN_UpgradeOnCaN_NormalWFT_PinnedOverride_NoSuggest() {
	s.testPinnedCaNUpgradeOnCaN(true, false, false, true, false)
}

func (s *Versioning3Suite) makePinnedOverride(tv *testvars.TestVars) *workflowpb.VersioningOverride {
	return &workflowpb.VersioningOverride{
		Override: &workflowpb.VersioningOverride_Pinned{
			Pinned: &workflowpb.VersioningOverride_PinnedOverride{
				Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
				Version:  tv.ExternalDeploymentVersion(),
			},
		}}
}

// testPinnedCaN_UpgradeOnCaN tests ContinueAsNew of a Pinned workflow with InitialVersioningBehavior
// set to AUTO_UPGRADE using task polling directly (without SDK). This allows testing the feature
// before it's exposed in the SDK.
//
// Flow:
// 1. Set v1 as current, start workflow
// 2. First WFT: task is sent to v1 worker, worker declares vbPinned -> workflow becomes pinned to v1
// 3. Set v2 as current
// 4. Trigger WFT (mode-dependent: signal (normal task), update (speculative task), or fail+retry(transient task))
// 5. On WFT: confirm ContinueAsNewSuggested=true, issue ContinueAsNew with AUTO_UPGRADE
// 6. The new run should start on v2 (current) and be pinned after WFT completion.
func (s *Versioning3Suite) testPinnedCaNUpgradeOnCaN(normalTask, speculativeTask, transientTask, pinnedOverride, enableSendTargetVersionChanged bool) {
	if !enableSendTargetVersionChanged {
		s.OverrideDynamicConfig(dynamicconfig.EnableSendTargetVersionChanged, false)
	}
	s.RunTestWithMatchingBehavior(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		tv1 := testvars.New(s).WithBuildIDNumber(1)
		tv2 := tv1.WithBuildIDNumber(2)
		// Set v1 pinned override if testing that
		var override *workflowpb.VersioningOverride
		var execution *commonpb.WorkflowExecution
		if pinnedOverride {
			override = s.makePinnedOverride(tv1)
			execution, _ = s.drainWorkflowTaskAfterSetCurrentWithOverride(tv1, override)
		} else {
			execution, _ = s.drainWorkflowTaskAfterSetCurrent(tv1)
		}

		// Trigger a normal WFT to make the workflow pinned
		s.triggerNormalWFT(ctx, tv1, execution)

		// Process the signal WFT and declare behavior to make the workflow versioned pinned on v1
		s.pollWftAndHandle(tv1, false, nil,
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				s.NotNil(task)
				return respondEmptyWft(tv1, false, vbPinned), nil
			})

		// Verify workflow is now pinned and running on v1
		s.verifyWorkflowVersioning(s.Assertions, tv1, vbPinned, tv1.Deployment(), override, nil)
		s.verifyVersioningSAs(tv1, vbPinned, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, tv1)

		// Register v2 poller before setting it as current
		s.idlePollWorkflow(ctx, tv2, true, ver3MinPollTime, "should not get any tasks yet")

		// Set v2 as current
		s.setCurrentDeployment(tv2)

		// Mode-specific: trigger the WFT
		var updateResultCh <-chan *workflowservice.UpdateWorkflowExecutionResponse
		if normalTask {
			s.triggerNormalWFT(ctx, tv1, execution)
		} else if speculativeTask {
			updateResultCh = sendUpdateNoError(s, tv1)
		} else if transientTask {
			s.triggerTransientWFT(ctx, tv1, execution)
		}

		// Process the WFT and issue ContinueAsNew with AUTO_UPGRADE
		s.pollWftAndHandle(tv1, false, nil,
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				s.NotNil(task)
				// Mode-specific validations
				historyEvents := task.History.GetEvents()
				if speculativeTask {
					s.verifySpeculativeTask(execution)
				} else if transientTask {
					s.verifyTransientTask(task)
					// Get events from server-side history, this includes transient events.
					historyEvents = s.GetHistory(s.Namespace().String(), execution)
				}

				wfTaskStartedEvents := make([]*historypb.HistoryEvent, 0)
				for _, event := range historyEvents { // get events
					if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED {
						wfTaskStartedEvents = append(wfTaskStartedEvents, event)
					}
				}
				if enableSendTargetVersionChanged {
					// Verify TargetWorkerDeploymentVersionChanged was sent on WFT started events after deployment change.
					// Events BEFORE deployment change (events 3, 7) should NOT have the flag.
					// Events AFTER deployment change (all subsequent WFTs) SHOULD have the flag, regardless of success/failure.
					// The flag is recomputed on every WFT, so both failed attempts and retries will have it if conditions persist.
					s.Greater(len(wfTaskStartedEvents), 2) // make sure there are at least 3 WFT started events

					// In this test, deployment is changed after event 7 (`s.setCurrentDeployment(tv2)`).
					// So the first 2 WFT started events should NOT have the flag,
					// and all subsequent events SHOULD have the flag.
					eventsBeforeDeploymentChange := 2 // Events 3 and 7

					for i, event := range wfTaskStartedEvents {
						attr := event.GetWorkflowTaskStartedEventAttributes()
						if i < eventsBeforeDeploymentChange {
							// Events before deployment change should NOT have the flag
							s.False(attr.GetSuggestContinueAsNew())
							s.Require().Empty(attr.GetSuggestContinueAsNewReasons())
							s.False(attr.GetTargetWorkerDeploymentVersionChanged(),
								"Event %d should not have flag (before deployment change)", event.GetEventId())
						} else {
							// Events after deployment change SHOULD have the flag (including failed attempts and transient retries)
							s.False(attr.GetSuggestContinueAsNew())
							s.Require().Empty(attr.GetSuggestContinueAsNewReasons())
							s.True(attr.GetTargetWorkerDeploymentVersionChanged(),
								"Event %d should have flag (after deployment change)", event.GetEventId())
						}
					}
				} else {
					for _, event := range wfTaskStartedEvents {
						attr := event.GetWorkflowTaskStartedEventAttributes()
						s.False(attr.GetSuggestContinueAsNew())
						s.Require().Empty(attr.GetSuggestContinueAsNewReasons())
						s.False(attr.GetTargetWorkerDeploymentVersionChanged())
					}
				}

				commands := []*commandpb.Command{
					{
						CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
						Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
							ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
								WorkflowType:              tv1.WorkflowType(),
								TaskQueue:                 tv1.TaskQueue(),
								Input:                     tv1.Any().Payloads(),
								InitialVersioningBehavior: enumspb.CONTINUE_AS_NEW_VERSIONING_BEHAVIOR_AUTO_UPGRADE,
							},
						},
					},
				}

				resp := &workflowservice.RespondWorkflowTaskCompletedRequest{
					Commands:                   commands,
					ForceCreateNewWorkflowTask: false,
					VersioningBehavior:         vbPinned,
					DeploymentOptions:          tv1.WorkerDeploymentOptions(true),
				}

				// For speculative WFT, include update accept/complete in the commands
				if speculativeTask {
					resp.Commands = append(s.UpdateAcceptCompleteCommands(tv1), commands...)
					resp.Messages = s.UpdateAcceptCompleteMessages(tv1, task.Messages[0])
				}

				return resp, nil
			})

		postCaNTV := tv2
		if pinnedOverride {
			postCaNTV = tv1
		}

		// For speculative mode, wait for and verify the update result
		if speculativeTask {
			updateResult := <-updateResultCh
			s.Equal("success-result-of-"+tv1.UpdateID(), testcore.DecodeString(s.T(), updateResult.GetOutcome().GetSuccess()))
		}

		// Start async poller to receive the ContinueAsNew new run
		wftNewRunDone := make(chan struct{})
		s.pollWftAndHandle(postCaNTV, false, wftNewRunDone,
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				s.NotNil(task)
				return respondCompleteWorkflow(postCaNTV, vbPinned), nil
			})

		// Wait for the new run's first workflow task
		s.WaitForChannel(ctx, wftNewRunDone)

		// Verify the new workflow run is on v2 (not v1) because of AUTO_UPGRADE initial behavior
		// But if testing with Pinned V1 override, it should be on v1
		s.verifyWorkflowVersioning(s.Assertions, postCaNTV, vbPinned, postCaNTV.Deployment(), override, nil)
		s.verifyVersioningSAs(postCaNTV, vbPinned, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, postCaNTV)
	})
}

// Signal to trigger a normal WFT
func (s *Versioning3Suite) triggerNormalWFT(ctx context.Context, tv *testvars.TestVars, execution *commonpb.WorkflowExecution) {
	_, err := s.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: execution,
		SignalName:        tv.SignalName(),
		Input:             tv.Any().Payloads(),
		Identity:          tv.WorkerIdentity(),
	})
	s.NoError(err)
}

// Trigger a normal task and then fail the task twice to trigger a transient WFT
func (s *Versioning3Suite) triggerTransientWFT(ctx context.Context, tv *testvars.TestVars, execution *commonpb.WorkflowExecution) {
	s.triggerNormalWFT(ctx, tv, execution)

	// Poll and FAIL the WFT to create a transient WFT situation
	pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace:         s.Namespace().String(),
		TaskQueue:         tv.TaskQueue(),
		Identity:          tv.WorkerIdentity(),
		DeploymentOptions: tv.WorkerDeploymentOptions(true),
	})
	s.NoError(err)
	s.NotNil(pollResp)
	s.NotEmpty(pollResp.TaskToken)

	// Fail the workflow task - this will cause a transient WFT to be scheduled
	_, err = s.FrontendClient().RespondWorkflowTaskFailed(ctx, &workflowservice.RespondWorkflowTaskFailedRequest{
		Namespace: s.Namespace().String(),
		TaskToken: pollResp.TaskToken,
		Cause:     enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
		Identity:  tv.WorkerIdentity(),
	})
	s.NoError(err)
}

// Verify this is a speculative task - events not yet in persisted history
func (s *Versioning3Suite) verifySpeculativeTask(execution *commonpb.WorkflowExecution) {
	events := s.GetHistory(s.Namespace().String(), execution)
	s.EqualHistoryEvents(`
						1 WorkflowExecutionStarted
						2 WorkflowTaskScheduled
						3 WorkflowTaskStarted
						4 WorkflowTaskCompleted
						5 WorkflowExecutionSignaled
						6 WorkflowTaskScheduled
						7 WorkflowTaskStarted
						8 WorkflowTaskCompleted
						9 WorkflowTaskScheduled
						10 WorkflowTaskStarted
					`, events)
}

func (s *Versioning3Suite) verifyTransientTask(task *workflowservice.PollWorkflowTaskQueueResponse) {
	// Verify this is attempt 2 (transient)
	var lastScheduledEvent *historypb.HistoryEvent
	for _, event := range task.History.Events {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED {
			lastScheduledEvent = event
		}
	}
	s.Require().NotNil(lastScheduledEvent)
	s.Equal(int32(2), lastScheduledEvent.GetWorkflowTaskScheduledEventAttributes().GetAttempt())
}

// TestAutoUpgradeCaN_UpgradeOnCaN tests ContinueAsNew of an AutoUpgrade workflow with InitialVersioningBehavior
// set to AUTO_UPGRADE using task polling directly (without SDK). This allows testing the feature
// before it's exposed in the SDK.
//
// Flow:
// 1. Set v1 as current, start workflow
// 2. First WFT: task is sent to v1 worker, worker declares vbUnpinned -> workflow becomes AutoUpgrade on v1
// 3. Set v2 as current
// 4. Signal workflow, then on WFT:
//   - Confirm that ContinueAsNewSuggested=false and ContinueAsNewSuggestedReasons=[]
//   - Issue ContinueAsNew with InitialVersioningBehavior AUTO_UPGRADE
//     -> first task should be AUTO_UPGRADE (same as it would be without InitialVersioningBehavior)
func (s *Versioning3Suite) TestAutoUpgradeCaN_UpgradeOnCaN() {
	s.RunTestWithMatchingBehavior(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		tv1 := testvars.New(s).WithBuildIDNumber(1)
		tv2 := tv1.WithBuildIDNumber(2)
		execution, _ := s.drainWorkflowTaskAfterSetCurrent(tv1)

		// Verify workflow is now versioned (AutoUpgrade) and running on v1
		s.verifyWorkflowVersioning(s.Assertions, tv1, vbUnpinned, tv1.Deployment(), nil, nil)

		// Register v2 poller before setting it as current
		s.idlePollWorkflow(ctx, tv2, true, ver3MinPollTime, "should not get any tasks yet")

		// Set v2 as current
		s.setCurrentDeployment(tv2)

		// Signal the workflow again to trigger the WFT with ContinueAsNewSuggested=true and reasons=[NewTargetVersion]
		_, err := s.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         s.Namespace().String(),
			WorkflowExecution: execution,
			SignalName:        tv1.SignalName(),
			Input:             tv1.Any().Payloads(),
			Identity:          tv1.WorkerIdentity(),
		})
		s.NoError(err)

		// Process the signal WFT and issue ContinueAsNew with AUTO_UPGRADE initial behavior
		s.pollWftAndHandle(tv2, false, nil,
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				s.NotNil(task)

				wfTaskStartedEvents := make([]*historypb.HistoryEvent, 0)
				for _, event := range task.History.Events {
					if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED {
						wfTaskStartedEvents = append(wfTaskStartedEvents, event)
					}
				}
				s.Require().Len(wfTaskStartedEvents, 2) // make sure we are actually verifying non-zero # of events

				for _, event := range wfTaskStartedEvents {
					attr := event.GetWorkflowTaskStartedEventAttributes()
					s.False(attr.GetSuggestContinueAsNew())
					s.Require().Empty(attr.GetSuggestContinueAsNewReasons())
				}

				// For AutoUpgrade, I want to test that once the workflow has transitioned to v2, it doesn't get the CaN suggestion anymore.
				return respondEmptyWft(tv2, false, vbUnpinned), nil
			})

		// Signal the workflow again to trigger another WFT with ContinueAsNewSuggested=false and reasons=[]
		_, err = s.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         s.Namespace().String(),
			WorkflowExecution: execution,
			SignalName:        tv1.SignalName(),
			Input:             tv1.Any().Payloads(),
			Identity:          tv1.WorkerIdentity(),
		})
		s.NoError(err)
		s.pollWftAndHandle(tv2, false, nil,
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				s.NotNil(task)

				wfTaskStartedEvents := make([]*historypb.HistoryEvent, 0)
				for _, event := range task.History.Events {
					if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED {
						wfTaskStartedEvents = append(wfTaskStartedEvents, event)
					}
				}
				s.NotEmpty(wfTaskStartedEvents) // make sure we are actually verifying non-zero # of events

				for _, event := range wfTaskStartedEvents {
					attr := event.GetWorkflowTaskStartedEventAttributes()
					s.False(attr.GetSuggestContinueAsNew())
					s.Require().Empty(attr.GetSuggestContinueAsNewReasons())
				}

				return &workflowservice.RespondWorkflowTaskCompletedRequest{
					Commands: []*commandpb.Command{
						{
							CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
							Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
								ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
									WorkflowType:              tv1.WorkflowType(),
									TaskQueue:                 tv1.TaskQueue(),
									Input:                     tv1.Any().Payloads(),
									InitialVersioningBehavior: enumspb.CONTINUE_AS_NEW_VERSIONING_BEHAVIOR_AUTO_UPGRADE,
								},
							},
						},
					},
					ForceCreateNewWorkflowTask: false,
					VersioningBehavior:         vbUnpinned,
					DeploymentOptions:          tv2.WorkerDeploymentOptions(true),
				}, nil
			})

		// Start async poller for v2 to receive the ContinueAsNew new run
		wftNewRunDone := make(chan struct{})
		s.pollWftAndHandle(tv2, false, wftNewRunDone,
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				s.NotNil(task)
				// The new run should be on v2 because InitialVersioningBehavior was AUTO_UPGRADE
				return respondCompleteWorkflow(tv2, vbUnpinned), nil
			})

		// Wait for the new run's first workflow task on v2
		s.WaitForChannel(ctx, wftNewRunDone)

		// Verify the new workflow run is on v2 (not v1) because of AUTO_UPGRADE initial behavior
		// The new workflow has the worker-sent AutoUpgrade behavior from the first WFT completion after CaN.
		s.verifyWorkflowVersioning(s.Assertions, tv2, vbUnpinned, tv2.Deployment(), nil, nil)
	})
}

func (s *Versioning3Suite) testCan(crossTq bool, behavior enumspb.VersioningBehavior, upgradeOnCaN bool, expectPinnedInherit bool, pinnedOverride bool) {
	// CaN inherits version if pinned and if new task queue is in pinned version, goes to current version if unpinned.
	tv1 := testvars.New(s).WithBuildIDNumber(1).WithWorkflowIDNumber(1)
	tv2 := tv1.WithBuildIDNumber(2)
	sdkBehavior := workflow.VersioningBehaviorAutoUpgrade
	if behavior == vbPinned {
		sdkBehavior = workflow.VersioningBehaviorPinned
	}
	canxTq := tv1.TaskQueue().GetName() + "_XTQ_CaN"

	var override *workflowpb.VersioningOverride
	if pinnedOverride {
		override = s.makePinnedOverride(tv1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
	defer cancel()

	wfStarted := make(chan struct{}, 10)
	currentChanged := make(chan struct{}, 10)

	wf1 := func(ctx workflow.Context, attempt int) (string, error) {
		switch attempt {
		case 0:
			newCtx := ctx
			if crossTq {
				newCtx = workflow.WithWorkflowTaskQueue(newCtx, canxTq)
			}
			// TODO(carlydf): use this code path once CaN option to choose InitialVersioningBehavior is exposed in SDK
			//  if upgradeOnCaN {
			//	  newCtx = workflow.WithInitialVersioningBehavior(newCtx, temporal.ContinueAsNewVersioningBehaviorAutoUpgrade)
			//  }
			if pinnedOverride {
				s.verifyWorkflowVersioning(s.Assertions, tv1, vbUnspecified, nil, override, nil)
			} else {
				s.verifyWorkflowVersioning(s.Assertions, tv1, vbUnspecified, nil, override, tv1.DeploymentVersionTransition())
			}
			wfStarted <- struct{}{}
			// wait for current version to change.
			<-currentChanged
			return "", workflow.NewContinueAsNewError(newCtx, "wf", attempt+1)
		case 1:
			s.verifyWorkflowVersioning(s.Assertions, tv1, vbPinned, tv1.Deployment(), override, nil)
			s.verifyVersioningSAs(tv1, vbPinned, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)
			return "v1", nil
		}
		s.FailNow("workflow should not get to this point")
		return "", nil
	}

	wf2 := func(ctx workflow.Context, attempt int) (string, error) {
		if behavior == vbUnpinned && s.deploymentWorkflowVersion >= workerdeployment.AsyncSetCurrentAndRamping {
			// Unpinned CaN should inherit parent deployment version and behaviour
			s.verifyWorkflowVersioning(s.Assertions, tv2, vbUnpinned, tv1.Deployment(), override, tv2.DeploymentVersionTransition())
			s.verifyVersioningSAs(tv1, vbUnpinned, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)
		} else {
			s.verifyWorkflowVersioning(s.Assertions, tv2, vbUnspecified, nil, override, tv2.DeploymentVersionTransition())
			s.verifyVersioningSAs(tv2, vbUnspecified, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)
		}
		return "v2", nil
	}

	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.FrontendGRPCAddress(),
		Namespace: s.Namespace().String(),
	})
	s.NoError(err)

	if crossTq && expectPinnedInherit {
		w1xtq := worker.New(sdkClient, canxTq, worker.Options{
			DeploymentOptions: worker.DeploymentOptions{
				Version:                   tv1.SDKDeploymentVersion(),
				UseVersioning:             true,
				DefaultVersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
			},
			MaxConcurrentWorkflowTaskPollers: numPollers,
		})
		w1xtq.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf", VersioningBehavior: workflow.VersioningBehaviorPinned})
		s.NoError(w1xtq.Start())
		defer w1xtq.Stop()
	}
	w1 := worker.New(sdkClient, tv1.TaskQueue().GetName(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			UseVersioning:             true,
			Version:                   tv1.SDKDeploymentVersion(),
			DefaultVersioningBehavior: workflow.VersioningBehaviorPinned,
		},
		MaxConcurrentWorkflowTaskPollers: numPollers,
		// Because the wf task can get stuck behind a channel, we make sure sdk does not panic too early
		DeadlockDetectionTimeout: 30 * time.Second,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf", VersioningBehavior: sdkBehavior})
	s.NoError(w1.Start())
	defer w1.Stop()

	if crossTq {
		w2xtq := worker.New(sdkClient, canxTq, worker.Options{
			DeploymentOptions: worker.DeploymentOptions{
				Version:                   tv2.SDKDeploymentVersion(),
				UseVersioning:             true,
				DefaultVersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
			},
			MaxConcurrentWorkflowTaskPollers: numPollers,
		})
		w2xtq.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf", VersioningBehavior: workflow.VersioningBehaviorPinned})
		s.NoError(w2xtq.Start())
		defer w2xtq.Stop()
	}
	w2 := worker.New(sdkClient, tv2.TaskQueue().GetName(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			UseVersioning:             true,
			Version:                   tv2.SDKDeploymentVersion(),
			DefaultVersioningBehavior: workflow.VersioningBehaviorPinned,
		},
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf", VersioningBehavior: sdkBehavior})
	s.NoError(w2.Start())
	defer w2.Stop()

	// make v1 current
	s.setCurrentDeployment(tv1)

	startOpts := sdkclient.StartWorkflowOptions{
		ID:                  tv1.WorkflowID(),
		TaskQueue:           tv1.TaskQueue().GetName(),
		WorkflowTaskTimeout: 30 * time.Second,
	}
	if override.GetPinned().GetBehavior() == workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED {
		startOpts.VersioningOverride = &sdkclient.PinnedVersioningOverride{
			Version: worker.WorkerDeploymentVersion{
				DeploymentName: override.GetPinned().GetVersion().GetDeploymentName(),
				BuildID:        override.GetPinned().GetVersion().GetBuildId(),
			},
		}
	}

	run, err := sdkClient.ExecuteWorkflow(ctx, startOpts, "wf")
	s.NoError(err)

	// wait for it to start on v1
	s.WaitForChannel(ctx, wfStarted)
	close(wfStarted) // force panic if replayed

	// make v2 current
	s.setCurrentDeployment(tv2)
	currentChanged <- struct{}{}

	var out string
	s.NoError(run.Get(ctx, &out))
	if expectPinnedInherit {
		s.Equal("v1", out)
		s.verifyVersioningSAs(tv1, vbPinned, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, tv1)
	} else {
		s.Equal("v2", out)
		s.verifyVersioningSAs(tv2, behavior, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, tv2)
	}
}

func (s *Versioning3Suite) TestDescribeTaskQueueVersioningInfo() {
	tv := testvars.New(s)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	t1 := time.Now()
	t2 := t1.Add(time.Second)

	revisionNumber := int64(1)

	// ramping a version up to 20%.
	newRoutingConfig := &deploymentpb.RoutingConfig{
		CurrentDeploymentVersion:            worker_versioning.ExternalWorkerDeploymentVersionFromStringV31("__unversioned__"),
		CurrentVersionChangedTime:           nil,
		RampingDeploymentVersion:            worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv.DeploymentVersionString()),
		RampingVersionPercentage:            20,
		RampingVersionChangedTime:           timestamp.TimePtr(t1),
		RampingVersionPercentageChangedTime: timestamp.TimePtr(t1),
		RevisionNumber:                      revisionNumber,
	}

	if s.useNewDeploymentData {
		s.syncTaskQueueDeploymentDataWithRoutingConfig(tv, newRoutingConfig, map[string]*deploymentspb.WorkerDeploymentVersionData{tv.DeploymentVersion().GetBuildId(): {
			Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
		}}, []string{}, tqTypeWf)
	} else {
		s.syncTaskQueueDeploymentData(tv, false, 20, false, t1, tqTypeWf)
	}
	wfInfo, err := s.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
		Namespace:     s.Namespace().String(),
		TaskQueue:     tv.TaskQueue(),
		TaskQueueType: tqTypeWf,
	})
	s.NoError(err)
	s.ProtoEqual(&taskqueuepb.TaskQueueVersioningInfo{
		CurrentDeploymentVersion: worker_versioning.ExternalWorkerDeploymentVersionFromStringV31("__unversioned__"),
		CurrentVersion:           "__unversioned__",
		RampingDeploymentVersion: worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv.DeploymentVersionString()),
		RampingVersion:           tv.DeploymentVersionString(),
		RampingVersionPercentage: 20,
		UpdateTime:               timestamp.TimePtr(t1),
	}, wfInfo.GetVersioningInfo())

	// Setting the current version for the activity TQ only.
	newRoutingConfig = &deploymentpb.RoutingConfig{
		CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv.DeploymentVersionString()),
		CurrentVersionChangedTime: timestamp.TimePtr(t1),
		RevisionNumber:            revisionNumber,
	}
	if s.useNewDeploymentData {
		s.syncTaskQueueDeploymentDataWithRoutingConfig(tv, newRoutingConfig, map[string]*deploymentspb.WorkerDeploymentVersionData{tv.DeploymentVersion().GetBuildId(): {
			Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
		}}, []string{}, tqTypeAct)
	} else {
		s.syncTaskQueueDeploymentData(tv, true, 0, false, t1, tqTypeAct)
	}

	actInfo, err := s.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
		Namespace:     s.Namespace().String(),
		TaskQueue:     tv.TaskQueue(),
		TaskQueueType: tqTypeAct,
	})
	s.NoError(err)
	s.ProtoEqual(&taskqueuepb.TaskQueueVersioningInfo{
		CurrentDeploymentVersion: worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv.DeploymentVersionString()),
		CurrentVersion:           tv.DeploymentVersionString(),
		UpdateTime:               timestamp.TimePtr(t1),
	}, actInfo.GetVersioningInfo())

	// Now ramp to unversioned
	newRoutingConfig = &deploymentpb.RoutingConfig{
		CurrentDeploymentVersion:            worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv.DeploymentVersionString()),
		CurrentVersionChangedTime:           timestamp.TimePtr(t1),
		RampingDeploymentVersion:            worker_versioning.ExternalWorkerDeploymentVersionFromStringV31("__unversioned__"),
		RampingVersionPercentage:            10,
		RampingVersionChangedTime:           timestamp.TimePtr(t2),
		RampingVersionPercentageChangedTime: timestamp.TimePtr(t2),
		RevisionNumber:                      2,
	}
	if s.useNewDeploymentData {
		s.syncTaskQueueDeploymentDataWithRoutingConfig(tv, newRoutingConfig, map[string]*deploymentspb.WorkerDeploymentVersionData{tv.DeploymentVersion().GetBuildId(): {
			Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
		}}, []string{}, tqTypeAct)
	} else {
		s.syncTaskQueueDeploymentData(tv, false, 10, true, t2, tqTypeAct)
	}
	s.waitForDeploymentDataPropagation(tv, versionStatusNil, true, tqTypeAct)

	actInfo, err = s.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
		Namespace:     s.Namespace().String(),
		TaskQueue:     tv.TaskQueue(),
		TaskQueueType: tqTypeAct,
	})
	s.NoError(err)
	s.ProtoEqual(&taskqueuepb.TaskQueueVersioningInfo{
		CurrentDeploymentVersion: worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv.DeploymentVersionString()),
		CurrentVersion:           tv.DeploymentVersionString(),
		RampingDeploymentVersion: worker_versioning.ExternalWorkerDeploymentVersionFromStringV31("__unversioned__"),
		RampingVersion:           "__unversioned__",
		RampingVersionPercentage: 10,
		UpdateTime:               timestamp.TimePtr(t2),
	}, actInfo.GetVersioningInfo())
}

func (s *Versioning3Suite) TestSyncDeploymentUserDataWithRoutingConfig_Update() {
	if s.useNewDeploymentData == false {
		s.T().Skip()
	}
	tv := testvars.New(s)

	data := s.getTaskQueueDeploymentData(tv, tqTypeAct)
	s.Nil(data)
	data = s.getTaskQueueDeploymentData(tv, tqTypeWf)
	s.Nil(data)

	t1 := time.Now()
	tv1 := tv.WithBuildIDNumber(1)
	revisionNumber := int64(1)

	routingConfig := &deploymentpb.RoutingConfig{
		CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv1.DeploymentVersionString()),
		CurrentVersionChangedTime: timestamp.TimePtr(t1),
		RevisionNumber:            revisionNumber,
	}
	s.syncTaskQueueDeploymentDataWithRoutingConfig(tv1, routingConfig, map[string]*deploymentspb.WorkerDeploymentVersionData{tv1.DeploymentVersion().GetBuildId(): {
		Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
	}}, []string{}, tqTypeAct)

	data = s.getTaskQueueDeploymentData(tv, tqTypeAct)
	s.ProtoEqual(&persistencespb.DeploymentData{DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
		tv1.DeploymentVersion().GetDeploymentName(): {
			RoutingConfig: routingConfig,
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{tv1.DeploymentVersion().GetBuildId(): {
				Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
			}},
		},
	}}, data)
	data = s.getTaskQueueDeploymentData(tv, tqTypeWf)
	s.Nil(data)

	// Changing things with an older revision number should not have effect.
	invalidRoutingConfigWithOlderRevisionNumber := &deploymentpb.RoutingConfig{
		CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv1.DeploymentVersionString()),
		CurrentVersionChangedTime: timestamp.TimePtr(t1),
		RevisionNumber:            revisionNumber - 1,
	}
	s.syncTaskQueueDeploymentDataWithRoutingConfig(tv1, invalidRoutingConfigWithOlderRevisionNumber, map[string]*deploymentspb.WorkerDeploymentVersionData{tv1.DeploymentVersion().GetBuildId(): {
		Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
	}}, []string{}, tqTypeAct)
	data = s.getTaskQueueDeploymentData(tv, tqTypeAct)
	s.ProtoEqual(&persistencespb.DeploymentData{DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
		tv1.DeploymentVersion().GetDeploymentName(): {
			RoutingConfig: routingConfig,
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{tv1.DeploymentVersion().GetBuildId(): {
				Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
			}},
		},
	}}, data)

	// Changing things with a newer revision number should apply
	revisionNumber++
	routingConfig = &deploymentpb.RoutingConfig{
		CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv1.DeploymentVersionString()),
		CurrentVersionChangedTime: timestamp.TimePtr(t1),
		RevisionNumber:            revisionNumber,
	}
	s.syncTaskQueueDeploymentDataWithRoutingConfig(tv1, routingConfig, map[string]*deploymentspb.WorkerDeploymentVersionData{tv1.DeploymentVersion().GetBuildId(): {
		Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
	}}, []string{}, tqTypeAct)
	data = s.getTaskQueueDeploymentData(tv, tqTypeAct)
	s.ProtoEqual(&persistencespb.DeploymentData{DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
		tv1.DeploymentVersion().GetDeploymentName(): {
			RoutingConfig: routingConfig,
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{tv1.DeploymentVersion().GetBuildId(): {
				Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
			}},
		},
	}}, data)

	// Add another version, with current status, this time to both tq types.
	tv2 := tv.WithBuildIDNumber(2)
	t2 := t1.Add(time.Second)
	revisionNumber++
	routingConfig = &deploymentpb.RoutingConfig{
		CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv2.DeploymentVersionString()),
		CurrentVersionChangedTime: timestamp.TimePtr(t2),
		RevisionNumber:            revisionNumber,
	}
	s.syncTaskQueueDeploymentDataWithRoutingConfig(tv2, routingConfig, map[string]*deploymentspb.WorkerDeploymentVersionData{tv2.DeploymentVersion().GetBuildId(): {
		Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
	}, tv1.DeploymentVersion().GetBuildId(): {
		Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
	}}, []string{}, tqTypeAct, tqTypeWf)

	// Verify the data is right for the activity TQ.
	data = s.getTaskQueueDeploymentData(tv, tqTypeAct)
	s.ProtoEqual(&persistencespb.DeploymentData{DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
		tv1.DeploymentVersion().GetDeploymentName(): {
			RoutingConfig: routingConfig,
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{tv2.DeploymentVersion().GetBuildId(): {
				Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
			}, tv1.DeploymentVersion().GetBuildId(): {
				Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
			}},
		},
	}}, data)

	// Verify the data is right for the workflow TQ.
	data = s.getTaskQueueDeploymentData(tv, tqTypeWf)
	s.ProtoEqual(&persistencespb.DeploymentData{DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
		tv2.DeploymentVersion().GetDeploymentName(): {
			RoutingConfig: routingConfig,
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{tv2.DeploymentVersion().GetBuildId(): {
				Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
			}, tv1.DeploymentVersion().GetBuildId(): {
				Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING,
			}},
		},
	}}, data)

	// Forget v1 from both tq types
	s.forgetDeploymentVersionsFromDeploymentData(tv1, tv1.DeploymentVersion().GetDeploymentName(), false, revisionNumber, tqTypeAct, tqTypeWf)
	data = s.getTaskQueueDeploymentData(tv, tqTypeAct)
	s.ProtoEqual(&persistencespb.DeploymentData{DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
		tv2.DeploymentVersion().GetDeploymentName(): {
			RoutingConfig: routingConfig,
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{tv2.DeploymentVersion().GetBuildId(): {
				Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
			}},
		},
	}}, data)

	data = s.getTaskQueueDeploymentData(tv, tqTypeWf)
	s.ProtoEqual(&persistencespb.DeploymentData{DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
		tv2.DeploymentVersion().GetDeploymentName(): {
			RoutingConfig: routingConfig,
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{tv2.DeploymentVersion().GetBuildId(): {
				Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
			}},
		},
	}}, data)

	// Forget v1 again should be a noop
	s.forgetDeploymentVersionsFromDeploymentData(tv1, tv1.DeploymentVersion().GetDeploymentName(), false, revisionNumber, tqTypeAct, tqTypeWf)
	data = s.getTaskQueueDeploymentData(tv, tqTypeAct)
	s.ProtoEqual(&persistencespb.DeploymentData{DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
		tv2.DeploymentVersion().GetDeploymentName(): {
			RoutingConfig: routingConfig,
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{tv2.DeploymentVersion().GetBuildId(): {
				Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
			}},
		},
	}}, data)

	// Ramp unversioned
	revisionNumber++
	t3 := t2.Add(time.Second)
	routingConfig = &deploymentpb.RoutingConfig{
		CurrentDeploymentVersion:            worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv2.DeploymentVersionString()),
		CurrentVersionChangedTime:           timestamp.TimePtr(t2),
		RampingDeploymentVersion:            worker_versioning.ExternalWorkerDeploymentVersionFromStringV31("__unversioned__"),
		RampingVersionPercentage:            10,
		RampingVersionChangedTime:           timestamp.TimePtr(t3),
		RampingVersionPercentageChangedTime: timestamp.TimePtr(t3),
		RevisionNumber:                      revisionNumber,
	}
	s.syncTaskQueueDeploymentDataWithRoutingConfig(tv2, routingConfig, map[string]*deploymentspb.WorkerDeploymentVersionData{
		tv2.DeploymentVersion().GetBuildId(): {
			Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
		},
	}, []string{}, tqTypeAct, tqTypeWf)
	data = s.getTaskQueueDeploymentData(tv, tqTypeAct)
	s.ProtoEqual(&persistencespb.DeploymentData{DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
		tv1.DeploymentVersion().GetDeploymentName(): {
			RoutingConfig: routingConfig,
			Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{tv2.DeploymentVersion().GetBuildId(): {
				Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
			}},
		},
	}}, data)

}

func (s *Versioning3Suite) TestSyncDeploymentUserData_Update() {
	if s.useNewDeploymentData == true {
		s.T().Skip()
	}
	tv := testvars.New(s)

	data := s.getTaskQueueDeploymentData(tv, tqTypeAct)
	s.Nil(data)
	data = s.getTaskQueueDeploymentData(tv, tqTypeWf)
	s.Nil(data)

	t1 := time.Now()
	tv1 := tv.WithBuildIDNumber(1)

	s.syncTaskQueueDeploymentData(tv1, true, 0, false, t1, tqTypeAct)
	data = s.getTaskQueueDeploymentData(tv, tqTypeAct)
	s.ProtoEqual(&persistencespb.DeploymentData{Versions: []*deploymentspb.DeploymentVersionData{
		{Version: tv1.DeploymentVersion(), CurrentSinceTime: timestamp.TimePtr(t1), RoutingUpdateTime: timestamp.TimePtr(t1)},
	}}, data)
	data = s.getTaskQueueDeploymentData(tv, tqTypeWf)
	s.Nil(data)

	// Changing things with an older timestamp should not have effect.
	t0 := t1.Add(-time.Second)
	s.syncTaskQueueDeploymentData(tv1, false, 0, false, t0, tqTypeAct)
	data = s.getTaskQueueDeploymentData(tv, tqTypeAct)
	s.ProtoEqual(&persistencespb.DeploymentData{Versions: []*deploymentspb.DeploymentVersionData{
		{Version: tv1.DeploymentVersion(), CurrentSinceTime: timestamp.TimePtr(t1), RoutingUpdateTime: timestamp.TimePtr(t1)},
	}}, data)

	// Changing things with a newer timestamp should apply
	t2 := t1.Add(time.Second)
	s.syncTaskQueueDeploymentData(tv1, false, 20, false, t2, tqTypeAct)
	data = s.getTaskQueueDeploymentData(tv, tqTypeAct)
	s.ProtoEqual(&persistencespb.DeploymentData{Versions: []*deploymentspb.DeploymentVersionData{
		{Version: tv1.DeploymentVersion(), CurrentSinceTime: nil, RampingSinceTime: timestamp.TimePtr(t2), RampPercentage: 20, RoutingUpdateTime: timestamp.TimePtr(t2)},
	}}, data)

	// Add another version, this time to both tq types
	tv2 := tv.WithBuildIDNumber(2)
	s.syncTaskQueueDeploymentData(tv2, false, 10, false, t1, tqTypeAct, tqTypeWf)
	data = s.getTaskQueueDeploymentData(tv, tqTypeAct)
	s.ProtoEqual(&persistencespb.DeploymentData{Versions: []*deploymentspb.DeploymentVersionData{
		{Version: tv1.DeploymentVersion(), CurrentSinceTime: nil, RampingSinceTime: timestamp.TimePtr(t2), RampPercentage: 20, RoutingUpdateTime: timestamp.TimePtr(t2)},
		{Version: tv2.DeploymentVersion(), CurrentSinceTime: nil, RampingSinceTime: timestamp.TimePtr(t1), RampPercentage: 10, RoutingUpdateTime: timestamp.TimePtr(t1)},
	}}, data)
	data = s.getTaskQueueDeploymentData(tv, tqTypeWf)
	s.ProtoEqual(&persistencespb.DeploymentData{Versions: []*deploymentspb.DeploymentVersionData{
		{Version: tv2.DeploymentVersion(), CurrentSinceTime: nil, RampingSinceTime: timestamp.TimePtr(t1), RampPercentage: 10, RoutingUpdateTime: timestamp.TimePtr(t1)},
	}}, data)

	// Make v2 current
	s.syncTaskQueueDeploymentData(tv2, true, 0, false, t2, tqTypeAct)
	data = s.getTaskQueueDeploymentData(tv, tqTypeAct)
	s.ProtoEqual(&persistencespb.DeploymentData{Versions: []*deploymentspb.DeploymentVersionData{
		{Version: tv1.DeploymentVersion(), CurrentSinceTime: nil, RampingSinceTime: timestamp.TimePtr(t2), RampPercentage: 20, RoutingUpdateTime: timestamp.TimePtr(t2)},
		{Version: tv2.DeploymentVersion(), CurrentSinceTime: timestamp.TimePtr(t2), RoutingUpdateTime: timestamp.TimePtr(t2)},
	}}, data)

	// Forget v1
	s.forgetTaskQueueDeploymentVersion(tv1, tqTypeAct, false)
	data = s.getTaskQueueDeploymentData(tv, tqTypeAct)
	s.ProtoEqual(&persistencespb.DeploymentData{Versions: []*deploymentspb.DeploymentVersionData{
		{Version: tv2.DeploymentVersion(), CurrentSinceTime: timestamp.TimePtr(t2), RoutingUpdateTime: timestamp.TimePtr(t2)},
	}}, data)

	// Forget v1 again should be a noop
	s.forgetTaskQueueDeploymentVersion(tv1, tqTypeAct, false)
	data = s.getTaskQueueDeploymentData(tv, tqTypeAct)
	s.ProtoEqual(&persistencespb.DeploymentData{Versions: []*deploymentspb.DeploymentVersionData{
		{Version: tv2.DeploymentVersion(), CurrentSinceTime: timestamp.TimePtr(t2), RoutingUpdateTime: timestamp.TimePtr(t2)},
	}}, data)

	// Ramp unversioned
	s.syncTaskQueueDeploymentData(tv2, false, 90, true, t2, tqTypeAct)
	data = s.getTaskQueueDeploymentData(tv, tqTypeAct)
	s.ProtoEqual(&persistencespb.DeploymentData{Versions: []*deploymentspb.DeploymentVersionData{
		{Version: tv2.DeploymentVersion(), CurrentSinceTime: timestamp.TimePtr(t2), RoutingUpdateTime: timestamp.TimePtr(t2)},
	},
		UnversionedRampData: &deploymentspb.DeploymentVersionData{RampingSinceTime: timestamp.TimePtr(t2), RampPercentage: 90, RoutingUpdateTime: timestamp.TimePtr(t2)},
	}, data)

	// Forget v2
	s.forgetTaskQueueDeploymentVersion(tv2, tqTypeAct, false)
	data = s.getTaskQueueDeploymentData(tv, tqTypeAct)
	s.ProtoEqual(&persistencespb.DeploymentData{
		UnversionedRampData: &deploymentspb.DeploymentVersionData{RampingSinceTime: timestamp.TimePtr(t2), RampPercentage: 90, RoutingUpdateTime: timestamp.TimePtr(t2)},
	}, data)
}

func (s *Versioning3Suite) setCurrentDeployment(tv *testvars.TestVars) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s.Eventually(func() bool {
		req := &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentSeries(),
		}
		if s.useV32 {
			req.BuildId = tv.BuildID()
		} else {
			req.Version = tv.DeploymentVersionString() //nolint:staticcheck // SA1019: worker versioning v0.31
		}
		_, err := s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, req)
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) || (err != nil && strings.Contains(err.Error(), serviceerror.NewFailedPreconditionf(workerdeployment.ErrCurrentVersionDoesNotHaveAllTaskQueues, tv.DeploymentVersionStringV32()).Error())) {
			return false
		}
		s.NoError(err)
		return err == nil
	}, 60*time.Second, 500*time.Millisecond)

	// Wait for propagation to complete since we have tests using async entity workflows to set the current version
	s.waitForDeploymentDataPropagationQueryWorkerDeployment(tv)
}

func (s *Versioning3Suite) unsetCurrentDeployment(tv *testvars.TestVars) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s.Eventually(func() bool {
		req := &workflowservice.SetWorkerDeploymentCurrentVersionRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentSeries(),
		}
		_, err := s.FrontendClient().SetWorkerDeploymentCurrentVersion(ctx, req)
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return false
		}
		s.NoError(err)
		return err == nil
	}, 60*time.Second, 500*time.Millisecond)

	// Wait for propagation to complete since we have tests using async entity workflows to set the current version
	s.waitForDeploymentDataPropagationQueryWorkerDeployment(tv)
}

func (s *Versioning3Suite) setRampingDeployment(
	tv *testvars.TestVars,
	percentage float32,
	rampUnversioned bool,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	v := tv.DeploymentVersionString()
	bid := tv.BuildID()
	if rampUnversioned {
		v = "__unversioned__"
		bid = ""
	}

	s.Eventually(func() bool {
		req := &workflowservice.SetWorkerDeploymentRampingVersionRequest{
			Namespace:      s.Namespace().String(),
			DeploymentName: tv.DeploymentSeries(),
			Percentage:     percentage,
		}
		if s.useV32 {
			req.BuildId = bid
		} else {
			req.Version = v //nolint:staticcheck // SA1019: worker versioning v0.31
		}
		_, err := s.FrontendClient().SetWorkerDeploymentRampingVersion(ctx, req)
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) || (err != nil && strings.Contains(err.Error(), serviceerror.NewFailedPreconditionf(workerdeployment.ErrRampingVersionDoesNotHaveAllTaskQueues, tv.DeploymentVersionStringV32()).Error())) {
			return false
		}
		s.NoError(err)
		return err == nil
	}, 60*time.Second, 500*time.Millisecond)

	// Wait for propagation to complete since we have tests using async entity workflows to set the current version
	s.waitForDeploymentDataPropagationQueryWorkerDeployment(tv)
}

func (s *Versioning3Suite) waitForDeploymentDataPropagationQueryWorkerDeployment(tv *testvars.TestVars) {
	if s.deploymentWorkflowVersion == workerdeployment.AsyncSetCurrentAndRamping {
		s.Eventually(func() bool {
			resp, err := s.FrontendClient().DescribeWorkerDeployment(context.Background(), &workflowservice.DescribeWorkerDeploymentRequest{
				Namespace:      s.Namespace().String(),
				DeploymentName: tv.DeploymentSeries(),
			})
			s.NoError(err)
			return resp.GetWorkerDeploymentInfo().GetRoutingConfigUpdateState() == enumspb.ROUTING_CONFIG_UPDATE_STATE_COMPLETED
		}, 10*time.Second, 100*time.Millisecond)
	}
}

func (s *Versioning3Suite) updateTaskQueueDeploymentData(
	tv *testvars.TestVars,
	isCurrent bool,
	ramp float32,
	rampUnversioned bool,
	timeSinceUpdate time.Duration,
	tqTypes ...enumspb.TaskQueueType,
) {
	s.syncTaskQueueDeploymentData(tv, isCurrent, ramp, rampUnversioned, time.Now().Add(-timeSinceUpdate), tqTypes...)
	var status versionStatus
	if isCurrent {
		status = versionStatusCurrent
	} else if ramp > 0 {
		status = versionStatusRamping
	} else {
		status = versionStatusInactive
	}
	if rampUnversioned {
		status = versionStatusNil
	}

	s.waitForDeploymentDataPropagation(tv, status, rampUnversioned, tqTypes...)

}

// updateTaskQueueDeploymentDataWithRoutingConfig updates the deployment data for the requested TQ types
// and also waits for the data to propagate to all the relevant partitions.
// TODO (Shivam): Update the name of this one.
func (s *Versioning3Suite) updateTaskQueueDeploymentDataWithRoutingConfig(
	tv *testvars.TestVars,
	routingConfig *deploymentpb.RoutingConfig,
	upsertVersions map[string]*deploymentspb.WorkerDeploymentVersionData,
	forgetVersions []string,
	tqTypes ...enumspb.TaskQueueType,
) {

	s.syncTaskQueueDeploymentDataWithRoutingConfig(tv, routingConfig, upsertVersions, forgetVersions, tqTypes...)

	// We need to know what the status of the version we are adding/forgetting is so that we can wait for it to propagate.
	for _, version := range upsertVersions {
		if version.GetStatus() == enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT {
			s.waitForDeploymentDataPropagation(tv, versionStatusCurrent, false, tqTypes...)
		} else if version.GetStatus() == enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING {
			s.waitForDeploymentDataPropagation(tv, versionStatusRamping, false, tqTypes...)
		} else if version.GetStatus() == enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE {
			s.waitForDeploymentDataPropagation(tv, versionStatusInactive, false, tqTypes...)
		}
	}
}

// getTaskQueueDeploymentData gets the deployment data for a given TQ type. The data is always
// returned from the WF type root partition, so no need to wait for propagation before calling this
// function.
func (s *Versioning3Suite) getTaskQueueDeploymentData(
	tv *testvars.TestVars,
	tqType enumspb.TaskQueueType,
) *persistencespb.DeploymentData {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	resp, err := s.GetTestCluster().MatchingClient().GetTaskQueueUserData(
		ctx,
		&matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:   s.NamespaceID().String(),
			TaskQueue:     tv.TaskQueue().GetName(),
			TaskQueueType: tqTypeWf,
		})
	s.NoError(err)
	return resp.GetUserData().GetData().GetPerType()[int32(tqType)].GetDeploymentData()
}

func (s *Versioning3Suite) syncTaskQueueDeploymentDataWithRoutingConfig(
	tv *testvars.TestVars,
	routingConfig *deploymentpb.RoutingConfig,
	upsertVersions map[string]*deploymentspb.WorkerDeploymentVersionData,
	forgetVersions []string,
	t ...enumspb.TaskQueueType,
) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	deploymentName := tv.DeploymentVersion().GetDeploymentName()
	var err error

	_, err = s.GetTestCluster().MatchingClient().SyncDeploymentUserData(
		ctx, &matchingservice.SyncDeploymentUserDataRequest{
			NamespaceId:         s.NamespaceID().String(),
			TaskQueue:           tv.TaskQueue().GetName(),
			TaskQueueTypes:      t,
			DeploymentName:      deploymentName,
			UpdateRoutingConfig: routingConfig,
			UpsertVersionsData:  upsertVersions,
			ForgetVersions:      forgetVersions,
		},
	)

	s.NoError(err)
}

// rollbackTaskQueueToVersion simulates routing config lag by rolling back the task queue user data
// to an older version with revision number 0. This is used to test that workflows correctly use
// inherited revision numbers instead of falling back to the (stale) current task queue version.
func (s *Versioning3Suite) rollbackTaskQueueToVersion(
	tv *testvars.TestVars,
) {

	cleanup := s.InjectHook(testhooks.NewHook(testhooks.MatchingIgnoreRoutingConfigRevisionCheck, true))
	defer cleanup()

	rc := &deploymentpb.RoutingConfig{
		CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv.DeploymentVersionString()),
		CurrentVersionChangedTime: timestamp.TimePtr(time.Now().Add(1 * time.Minute)),
		RevisionNumber:            0,
	}
	s.syncTaskQueueDeploymentDataWithRoutingConfig(tv, rc, map[string]*deploymentspb.WorkerDeploymentVersionData{tv.DeploymentVersion().GetBuildId(): {
		Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
	}}, nil, tqTypeWf)

	// Verify that the rollback propagated to all partitions
	s.Eventually(func() bool {
		ms, err := s.GetTestCluster().MatchingClient().GetTaskQueueUserData(context.Background(), &matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:   s.NamespaceID().String(),
			TaskQueue:     tv.TaskQueue().GetName(),
			TaskQueueType: tqTypeWf,
		})
		s.NoError(err)
		current, currentRevisionNumber, _, _, _, _, _, _ := worker_versioning.CalculateTaskQueueVersioningInfo(ms.GetUserData().GetData().GetPerType()[int32(tqTypeWf)].GetDeploymentData())
		return current.GetBuildId() == tv.DeploymentVersion().GetBuildId() && currentRevisionNumber == 0
	}, 10*time.Second, 100*time.Millisecond)
}

func (s *Versioning3Suite) syncTaskQueueDeploymentData(
	tv *testvars.TestVars,
	isCurrent bool,
	ramp float32,
	rampUnversioned bool,
	updateTime time.Time,
	t ...enumspb.TaskQueueType,
) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	v := tv.DeploymentVersion()
	if rampUnversioned {
		v = nil
	}

	routingUpdateTime := timestamp.TimePtr(updateTime)
	var currentSinceTime, rampingSinceTime *timestamppb.Timestamp
	if isCurrent {
		currentSinceTime = routingUpdateTime
	}
	if ramp > 0 { // todo carly / shahab: this doesn't account for setting 0 ramp, or for changing the ramp while ramping_since_time stays the same.
		rampingSinceTime = routingUpdateTime
	}

	_, err := s.GetTestCluster().MatchingClient().SyncDeploymentUserData(
		ctx, &matchingservice.SyncDeploymentUserDataRequest{
			NamespaceId:    s.NamespaceID().String(),
			TaskQueue:      tv.TaskQueue().GetName(),
			TaskQueueTypes: t,
			Operation: &matchingservice.SyncDeploymentUserDataRequest_UpdateVersionData{
				UpdateVersionData: &deploymentspb.DeploymentVersionData{
					Version:           v,
					RoutingUpdateTime: routingUpdateTime,
					CurrentSinceTime:  currentSinceTime,
					RampingSinceTime:  rampingSinceTime,
					RampPercentage:    ramp,
				},
			},
		},
	)
	s.NoError(err)
}

func (s *Versioning3Suite) forgetDeploymentVersionsFromDeploymentData(
	tv *testvars.TestVars,
	deploymentName string,
	forgetUnversionedRamp bool,
	revisionNumber int64,
	t ...enumspb.TaskQueueType,
) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	v := tv.DeploymentVersion()
	if forgetUnversionedRamp {
		v.BuildId = ""
	}
	_, err := s.GetTestCluster().MatchingClient().SyncDeploymentUserData(
		ctx, &matchingservice.SyncDeploymentUserDataRequest{
			NamespaceId:    s.NamespaceID().String(),
			TaskQueue:      tv.TaskQueue().GetName(),
			TaskQueueTypes: t,
			DeploymentName: deploymentName,
			ForgetVersions: []string{tv.BuildID()},
		},
	)
	s.NoError(err)
}

func (s *Versioning3Suite) forgetTaskQueueDeploymentVersion(
	tv *testvars.TestVars,
	t enumspb.TaskQueueType,
	forgetUnversionedRamp bool,
) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	v := tv.DeploymentVersion()
	if forgetUnversionedRamp {
		v.BuildId = ""
	}
	_, err := s.GetTestCluster().MatchingClient().SyncDeploymentUserData(
		ctx, &matchingservice.SyncDeploymentUserDataRequest{
			NamespaceId:    s.NamespaceID().String(),
			TaskQueue:      tv.TaskQueue().GetName(),
			TaskQueueTypes: []enumspb.TaskQueueType{t},
			Operation: &matchingservice.SyncDeploymentUserDataRequest_ForgetVersion{
				ForgetVersion: v,
			},
		},
	)
	s.NoError(err)
}

func (s *Versioning3Suite) verifyWorkflowVersioning(
	a *require.Assertions,
	tv *testvars.TestVars,
	behavior enumspb.VersioningBehavior,
	deployment *deploymentpb.Deployment,
	override *workflowpb.VersioningOverride,
	transition *workflowpb.DeploymentVersionTransition,
) {
	dwf, err := s.FrontendClient().DescribeWorkflowExecution(
		context.Background(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: tv.WorkflowID(),
			},
		},
	)
	a.NoError(err)

	versioningInfo := dwf.WorkflowExecutionInfo.GetVersioningInfo()
	a.Equal(behavior.String(), versioningInfo.GetBehavior().String())
	var v *deploymentspb.WorkerDeploymentVersion
	if versioningInfo.GetVersion() != "" { //nolint:staticcheck // SA1019: worker versioning v0.31
		//nolint:staticcheck // SA1019: worker versioning v0.31
		v, err = worker_versioning.WorkerDeploymentVersionFromStringV31(versioningInfo.GetVersion())
		a.NoError(err)
		a.NotNil(versioningInfo.GetDeploymentVersion()) // make sure we are always populating this whenever Version string is populated
	}
	if dv := versioningInfo.GetDeploymentVersion(); dv != nil {
		v = worker_versioning.DeploymentVersionFromDeployment(worker_versioning.DeploymentFromExternalDeploymentVersion(dv))
	}
	actualDeployment := worker_versioning.DeploymentFromDeploymentVersion(v)
	if !deployment.Equal(actualDeployment) {
		a.Fail(fmt.Sprintf("deployment version mismatch. expected: {%s}, actual: {%s}",
			deployment,
			actualDeployment,
		))
	}

	if s.useV32 {
		// v0.32 override
		a.Equal(override.GetAutoUpgrade(), versioningInfo.GetVersioningOverride().GetAutoUpgrade())
		a.Equal(override.GetPinned().GetVersion().GetBuildId(), versioningInfo.GetVersioningOverride().GetPinned().GetVersion().GetBuildId())
		a.Equal(override.GetPinned().GetVersion().GetDeploymentName(), versioningInfo.GetVersioningOverride().GetPinned().GetVersion().GetDeploymentName())
		a.Equal(override.GetPinned().GetBehavior(), versioningInfo.GetVersioningOverride().GetPinned().GetBehavior())
		if worker_versioning.OverrideIsPinned(override) {
			a.Equal(override.GetPinned().GetVersion().GetDeploymentName(), dwf.WorkflowExecutionInfo.GetWorkerDeploymentName())
		}
	} else {
		// v0.31 override
		a.Equal(override.GetBehavior().String(), versioningInfo.GetVersioningOverride().GetBehavior().String())                                             //nolint:staticcheck // SA1019: worker versioning v0.31
		if actualOverrideDeployment := versioningInfo.GetVersioningOverride().GetPinnedVersion(); override.GetPinnedVersion() != actualOverrideDeployment { //nolint:staticcheck // SA1019: worker versioning v0.31
			a.Fail(fmt.Sprintf("pinned override mismatch. expected: {%s}, actual: {%s}",
				override.GetPinnedVersion(), //nolint:staticcheck // SA1019: worker versioning v0.31
				actualOverrideDeployment,
			))
		}
		if worker_versioning.OverrideIsPinned(override) {
			d, _ := worker_versioning.WorkerDeploymentVersionFromStringV31(override.GetPinnedVersion()) //nolint:staticcheck // SA1019: worker versioning v0.31
			a.Equal(d.GetDeploymentName(), dwf.WorkflowExecutionInfo.GetWorkerDeploymentName())
		}
	}

	if !versioningInfo.GetVersionTransition().Equal(transition) {
		a.Fail(fmt.Sprintf("version transition mismatch. expected: {%s}, actual: {%s}",
			transition,
			versioningInfo.GetVersionTransition(),
		))
	}
}

func respondActivity() *workflowservice.RespondActivityTaskCompletedRequest {
	return &workflowservice.RespondActivityTaskCompletedRequest{}
}

func respondWftWithActivities(
	tvWf *testvars.TestVars,
	tvAct *testvars.TestVars,
	sticky bool,
	behavior enumspb.VersioningBehavior,
	activityIds ...string,
) *workflowservice.RespondWorkflowTaskCompletedRequest {
	var stickyAttr *taskqueuepb.StickyExecutionAttributes
	if sticky {
		stickyAttr = &taskqueuepb.StickyExecutionAttributes{
			WorkerTaskQueue:        tvWf.StickyTaskQueue(),
			ScheduleToStartTimeout: durationpb.New(5 * time.Second),
		}
	}
	var commands []*commandpb.Command
	for _, a := range activityIds {
		commands = append(commands, &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
				ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:   a,
					ActivityType: tvAct.ActivityType(),
					TaskQueue:    tvAct.TaskQueue(),
					Input:        tvAct.Any().Payloads(),
					// TODO(carlydf): tests with forced task forward take multiple seconds. Need to know why?
					ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(3 * time.Second),
					HeartbeatTimeout:       durationpb.New(3 * time.Second),
					RequestEagerExecution:  false,
				},
			},
		})
	}
	versioningMode := enumspb.WORKER_VERSIONING_MODE_VERSIONED
	if behavior == vbUnspecified {
		versioningMode = enumspb.WORKER_VERSIONING_MODE_UNVERSIONED
	}
	return &workflowservice.RespondWorkflowTaskCompletedRequest{
		Commands:                   commands,
		StickyAttributes:           stickyAttr,
		ForceCreateNewWorkflowTask: false,
		VersioningBehavior:         behavior,
		DeploymentOptions: &deploymentpb.WorkerDeploymentOptions{
			BuildId:              tvWf.BuildID(),
			DeploymentName:       tvWf.DeploymentSeries(),
			WorkerVersioningMode: versioningMode,
		},
		// TODO(carlydf): remove stamp once build ID is added to wftc event
		WorkerVersionStamp: &commonpb.WorkerVersionStamp{ //nolint:staticcheck // SA1019: worker versioning v0.20
			BuildId:       tvWf.BuildID(),
			UseVersioning: versioningMode == enumspb.WORKER_VERSIONING_MODE_VERSIONED,
		},
	}
}

func respondEmptyWft(
	tv *testvars.TestVars,
	sticky bool,
	behavior enumspb.VersioningBehavior,
) *workflowservice.RespondWorkflowTaskCompletedRequest {
	return respondWftWithActivities(tv, tv, sticky, behavior)
}

func respondCompleteWorkflow(
	tv *testvars.TestVars,
	behavior enumspb.VersioningBehavior,
) *workflowservice.RespondWorkflowTaskCompletedRequest {
	return &workflowservice.RespondWorkflowTaskCompletedRequest{
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: tv.Any().Payloads(),
					},
				},
			},
		},
		ForceCreateNewWorkflowTask: false,
		VersioningBehavior:         behavior,
		DeploymentOptions: &deploymentpb.WorkerDeploymentOptions{
			BuildId:              tv.BuildID(),
			DeploymentName:       tv.DeploymentSeries(),
			WorkerVersioningMode: enumspb.WORKER_VERSIONING_MODE_VERSIONED,
		},
	}
}

func respondCompleteWorkflowUnversioned(
	tv *testvars.TestVars,
) *workflowservice.RespondWorkflowTaskCompletedRequest {
	return &workflowservice.RespondWorkflowTaskCompletedRequest{
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: tv.Any().Payloads(),
					},
				},
			},
		},
		ForceCreateNewWorkflowTask: false,
	}
}

func (s *Versioning3Suite) startWorkflow(
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

func (s *Versioning3Suite) queryWorkflow(
	tv *testvars.TestVars,
) (*workflowservice.QueryWorkflowResponse, error) {
	request := &workflowservice.QueryWorkflowRequest{
		Namespace: s.Namespace().String(),
		Execution: tv.WorkflowExecution(),
		Query:     tv.Query(),
	}

	shortCtx, cancel := context.WithTimeout(testcore.NewContext(), common.MinLongPollTimeout)
	defer cancel()
	response, err := s.FrontendClient().QueryWorkflow(shortCtx, request)
	return response, err
}

// Name is used by testvars. We use a shorten test name in variables so that physical task queue IDs
// do not grow larger that DB column limit (currently as low as 272 chars).
func (s *Versioning3Suite) Name() string {
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

// pollWftAndHandle can be used in sync and async mode. For async mode pass the async channel. It
// will be closed when the task is handled.
// Returns the poller and poll response only in sync mode (can be used to process new wft in the response)
func (s *Versioning3Suite) pollWftAndHandle(
	tv *testvars.TestVars,
	sticky bool,
	async chan<- struct{},
	handler func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error),
) (*taskpoller.TaskPoller, *workflowservice.RespondWorkflowTaskCompletedResponse) {
	return s.doPollWftAndHandle(tv, true, sticky, async, handler)
}

func (s *Versioning3Suite) unversionedPollWftAndHandle(
	tv *testvars.TestVars,
	sticky bool,
	async chan<- struct{},
	handler func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error),
) (*taskpoller.TaskPoller, *workflowservice.RespondWorkflowTaskCompletedResponse) {
	return s.doPollWftAndHandle(tv, false, sticky, async, handler)
}

// doPollWftAndHandle can be used in sync and async mode. For async mode pass the async channel. It
// will be closed when the task is handled.
// Returns the poller and poll response only in sync mode (can be used to process new wft in the response)
func (s *Versioning3Suite) doPollWftAndHandle(
	tv *testvars.TestVars,
	versioned bool,
	sticky bool,
	async chan<- struct{},
	handler func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error),
) (*taskpoller.TaskPoller, *workflowservice.RespondWorkflowTaskCompletedResponse) {
	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())
	f := func() *workflowservice.RespondWorkflowTaskCompletedResponse {
		tq := tv.TaskQueue()
		if sticky {
			tq = tv.StickyTaskQueue()
		}
		resp, err := poller.PollWorkflowTask(
			&workflowservice.PollWorkflowTaskQueueRequest{
				DeploymentOptions: tv.WorkerDeploymentOptions(versioned),
				TaskQueue:         tq,
			},
		).HandleTask(tv, handler, taskpoller.WithTimeout(30*time.Second))
		s.NoError(err)
		return resp
	}
	if async == nil {
		return poller, f()
	} else {
		go func() {
			f()
			close(async)
		}()
	}
	return nil, nil
}

func (s *Versioning3Suite) pollWftAndHandleQueries(
	tv *testvars.TestVars,
	sticky bool,
	async chan<- any,
	handler func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondQueryTaskCompletedRequest, error),
) (*taskpoller.TaskPoller, *workflowservice.RespondQueryTaskCompletedResponse) {
	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())
	f := func() *workflowservice.RespondQueryTaskCompletedResponse {
		tq := tv.TaskQueue()
		if sticky {
			tq = tv.StickyTaskQueue()
		}
		resp, err := poller.PollWorkflowTask(
			&workflowservice.PollWorkflowTaskQueueRequest{
				DeploymentOptions: tv.WorkerDeploymentOptions(true),
				TaskQueue:         tq,
			},
		).HandleLegacyQuery(tv, handler)
		s.NoError(err)
		return resp
	}
	if async == nil {
		return poller, f()
	}
	go func() {
		f()
		close(async)
	}()
	return nil, nil
}

func (s *Versioning3Suite) pollNexusTaskAndHandle(
	tv *testvars.TestVars,
	sticky bool,
	async chan<- any,
	handler func(task *workflowservice.PollNexusTaskQueueResponse) (*workflowservice.RespondNexusTaskCompletedRequest, error),
) (*taskpoller.TaskPoller, *workflowservice.RespondNexusTaskCompletedResponse) {
	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())
	f := func() *workflowservice.RespondNexusTaskCompletedResponse {
		tq := tv.TaskQueue()
		if sticky {
			tq = tv.StickyTaskQueue()
		}
		resp, err := poller.PollNexusTask(
			&workflowservice.PollNexusTaskQueueRequest{
				DeploymentOptions: tv.WorkerDeploymentOptions(true),
				TaskQueue:         tq,
			},
		).HandleTask(tv, handler, taskpoller.WithTimeout(10*time.Second))
		s.NoError(err)
		return resp
	}
	if async == nil {
		return poller, f()
	}
	go func() {
		f()
		close(async)
	}()
	return nil, nil
}

func (s *Versioning3Suite) unversionedPollActivityAndHandle(
	tv *testvars.TestVars,
	async chan<- struct{},
	handler func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error),
) {
	s.doPollActivityAndHandle(tv, false, async, handler)
}

func (s *Versioning3Suite) pollActivityAndHandle(
	tv *testvars.TestVars,
	async chan<- struct{},
	handler func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error),
) {
	s.doPollActivityAndHandle(tv, true, async, handler)
}

func (s *Versioning3Suite) doPollActivityAndHandle(
	tv *testvars.TestVars,
	versioned bool,
	async chan<- struct{},
	handler func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error),
) {
	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())
	f := func() {
		_, err := poller.PollActivityTask(
			&workflowservice.PollActivityTaskQueueRequest{
				DeploymentOptions: tv.WorkerDeploymentOptions(versioned),
			},
		).HandleTask(tv, handler, taskpoller.WithTimeout(time.Minute))
		s.NoError(err)
	}
	if async == nil {
		f()
	} else {
		go func() {
			f()
			close(async)
		}()
	}
}

func (s *Versioning3Suite) idlePollWorkflow(
	ctx context.Context,
	tv *testvars.TestVars,
	versioned bool,
	timeout time.Duration,
	unexpectedTaskMessage string,
) {
	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())
	_, _ = poller.PollWorkflowTask(
		&workflowservice.PollWorkflowTaskQueueRequest{
			DeploymentOptions: tv.WorkerDeploymentOptions(versioned),
		},
	).HandleTask(
		tv,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			// Can't call s.Fail directly because linter will complain about unsafe assertion in another go routine.
			// s.Assert().Fail is safe but linter automatically converts it to s.Fail if written directly!
			a := s.Assert()
			a.Fail(unexpectedTaskMessage)
			return nil, nil
		},
		taskpoller.WithTimeout(timeout),
		taskpoller.WithContext(ctx),
	)
}

func (s *Versioning3Suite) idlePollUnversionedActivity(
	tv *testvars.TestVars,
	timeout time.Duration,
	unexpectedTaskMessage string,
) {
	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())
	_, _ = poller.PollActivityTask(
		&workflowservice.PollActivityTaskQueueRequest{},
	).HandleTask(
		tv,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			if task != nil {
				s.Logger.Error(fmt.Sprintf("Unexpected activity task received, ID: %s", task.ActivityId))
				s.Fail(unexpectedTaskMessage)
			}
			return nil, nil
		},
		taskpoller.WithTimeout(timeout),
	)
}

func (s *Versioning3Suite) idlePollActivity(
	ctx context.Context,
	tv *testvars.TestVars,
	versioned bool,
	timeout time.Duration,
	unexpectedTaskMessage string,
) {
	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())
	_, _ = poller.PollActivityTask(
		&workflowservice.PollActivityTaskQueueRequest{
			DeploymentOptions: tv.WorkerDeploymentOptions(versioned),
		},
	).HandleTask(
		tv,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			if task != nil {
				s.Logger.Error(fmt.Sprintf("Unexpected activity task received, ID: %s", task.ActivityId))
				a := s.Assert()
				a.Fail(unexpectedTaskMessage)
			}
			return nil, nil
		},
		taskpoller.WithTimeout(timeout),
		taskpoller.WithContext(ctx),
	)
}

func (s *Versioning3Suite) idlePollNexus(
	tv *testvars.TestVars,
	versioned bool,
	timeout time.Duration,
	unexpectedTaskMessage string,
) {
	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())
	_, _ = poller.PollNexusTask(
		&workflowservice.PollNexusTaskQueueRequest{
			DeploymentOptions: tv.WorkerDeploymentOptions(versioned),
		}).HandleTask(
		tv,
		func(task *workflowservice.PollNexusTaskQueueResponse) (*workflowservice.RespondNexusTaskCompletedRequest, error) {
			if task != nil {
				s.Fail(unexpectedTaskMessage)
			}
			return nil, nil
		},
		taskpoller.WithTimeout(timeout),
	)
}

func (s *Versioning3Suite) verifyWorkflowStickyQueue(
	tv *testvars.TestVars,
) {
	ms, err := s.GetTestCluster().HistoryClient().GetMutableState(
		context.Background(), &historyservice.GetMutableStateRequest{
			NamespaceId: s.NamespaceID().String(),
			Execution:   tv.WorkflowExecution(),
		},
	)
	s.NoError(err)
	s.Equal(tv.StickyTaskQueue().GetName(), ms.StickyTaskQueue.GetName())
}

// Sticky queue needs to be created in server before tasks can schedule in it. Call to this method
// create the sticky queue by polling it.
func (s *Versioning3Suite) warmUpSticky(
	tv *testvars.TestVars,
) {
	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())
	_, _ = poller.PollWorkflowTask(
		&workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: tv.StickyTaskQueue(),
		},
	).HandleTask(
		tv,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.Fail("sticky task is not expected")
			return nil, nil
		},
		taskpoller.WithTimeout(ver3MinPollTime),
	)
}

// TODO (Shivam): Clean up this function once sync entity workflows have been removed.
func (s *Versioning3Suite) waitForDeploymentDataPropagation(
	tv *testvars.TestVars,
	status versionStatus,
	unversionedRamp bool,
	tqTypes ...enumspb.TaskQueueType,
) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	v := s.GetTestCluster().Host().DcClient().GetValue(dynamicconfig.MatchingNumTaskqueueReadPartitions.Key())
	s.NotEmpty(v, "versioning tests require setting explicit number of partitions")
	count, ok := v[0].Value.(int)
	s.True(ok, "partition count is not an int")
	partitionCount := count

	type partAndType struct {
		part int
		tp   enumspb.TaskQueueType
	}
	remaining := make(map[partAndType]struct{})
	for i := range partitionCount {
		for _, tqt := range tqTypes {
			remaining[partAndType{i, tqt}] = struct{}{}
		}
	}
	f, err := tqid.NewTaskQueueFamily(s.NamespaceID().String(), tv.TaskQueue().GetName())
	s.Eventually(func() bool {
		for pt := range remaining {
			s.NoError(err)
			partition := f.TaskQueue(pt.tp).NormalPartition(pt.part)
			// Use lower-level GetTaskQueueUserData instead of GetWorkerBuildIdCompatibility
			// here so that we can target activity queues.
			res, err := s.GetTestCluster().MatchingClient().GetTaskQueueUserData(
				ctx,
				&matchingservice.GetTaskQueueUserDataRequest{
					NamespaceId:   s.NamespaceID().String(),
					TaskQueue:     partition.RpcName(),
					TaskQueueType: partition.TaskType(),
				})
			s.NoError(err)
			perTypes := res.GetUserData().GetData().GetPerType()
			if perTypes != nil {
				deploymentsData := perTypes[int32(pt.tp)].GetDeploymentData().GetDeploymentsData()
				workerDeploymentData := deploymentsData[tv.DeploymentVersion().GetDeploymentName()]

				if unversionedRamp {
					if perTypes[int32(pt.tp)].GetDeploymentData().GetUnversionedRampData() != nil {
						delete(remaining, pt)
					}

					// Using the new internal task-queue persistence schema that we have now defined.
					if workerDeploymentData != nil {
						routingConfig := workerDeploymentData.GetRoutingConfig()
						if routingConfig.GetRampingDeploymentVersion() == nil && routingConfig.GetRampingVersionPercentage() > 0 {
							delete(remaining, pt)
						}
					}
					continue
				}
				versions := perTypes[int32(pt.tp)].GetDeploymentData().GetVersions()
				for _, d := range versions {
					if d.GetVersion().Equal(tv.DeploymentVersion()) {
						switch status {
						case versionStatusInactive:
							if d.GetRampingSinceTime() == nil && d.GetCurrentSinceTime() == nil {
								delete(remaining, pt)
							}
						case versionStatusRamping:
							if d.GetRampingSinceTime() != nil {
								delete(remaining, pt)
							}
						case versionStatusCurrent:
							if d.GetCurrentSinceTime() != nil {
								delete(remaining, pt)
							}
						}
					}
				}

				// Using the new internal task-queue persistence schema that we have now defined.
				if workerDeploymentData != nil {
					versions := workerDeploymentData.GetVersions()
					for buildID, versionData := range versions {
						if buildID == tv.DeploymentVersion().GetBuildId() && status == versionStatus(versionData.GetStatus()) {
							delete(remaining, pt)
						}
					}
				}
			}
		}
		return len(remaining) == 0
	}, 30*time.Second, 100*time.Millisecond)
}

func (s *Versioning3Suite) validateBacklogCount(
	tv *testvars.TestVars,
	tqType enumspb.TaskQueueType,
	expectedCount int64,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var resp *workflowservice.DescribeTaskQueueResponse
	var err error

	s.EventuallyWithT(func(t *assert.CollectT) {
		resp, err = s.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
			Namespace:     s.Namespace().String(),
			TaskQueue:     tv.TaskQueue(),
			TaskQueueType: tqType,
			ReportStats:   true,
		})
		s.NoError(err)
		s.NotNil(resp)
		priorityStats, ok := resp.GetStatsByPriorityKey()[3]
		s.True(ok)
		a := require.New(t)
		a.Equal(expectedCount, priorityStats.GetApproximateBacklogCount())
	}, 6*time.Second, 100*time.Millisecond)
}

func (s *Versioning3Suite) verifyVersioningSAs(
	tv *testvars.TestVars,
	behavior enumspb.VersioningBehavior,
	executionStatus enumspb.WorkflowExecutionStatus,
	usedBuilds ...*testvars.TestVars,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s.EventuallyWithT(func(t *assert.CollectT) {
		var query string
		if behavior != vbUnspecified {
			query = fmt.Sprintf("WorkflowId = '%s' AND TemporalWorkerDeployment = '%s' AND TemporalWorkerDeploymentVersion= '%s' AND TemporalWorkflowVersioningBehavior = '%s' AND ExecutionStatus = '%s'",
				tv.WorkflowID(), tv.DeploymentSeries(), tv.DeploymentVersionStringV32(), behavior.String(), executionStatus)
		} else {
			query = fmt.Sprintf("WorkflowId = '%s' AND TemporalWorkerDeploymentVersion is null AND TemporalWorkflowVersioningBehavior is null AND ExecutionStatus = '%s'",
				tv.WorkflowID(), executionStatus)
		}
		resp, err := s.FrontendClient().ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: s.Namespace().String(),
			Query:     query,
		})
		a := assert.New(t)
		a.Nil(err)
		a.Greater(len(resp.GetExecutions()), 0)
		if a.NotEmpty(resp.GetExecutions()) {
			w := resp.GetExecutions()[0]
			if behavior == vbPinned {
				payload, ok := w.GetSearchAttributes().GetIndexedFields()["BuildIds"]
				a.True(ok)
				searchAttrAny, err := searchattribute.DecodeValue(payload, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, true)
				a.NoError(err)
				var searchAttr []string
				if searchAttrAny != nil {
					searchAttr = searchAttrAny.([]string)
				}
				if behavior == enumspb.VERSIONING_BEHAVIOR_PINNED {
					a.Contains(searchAttr, worker_versioning.PinnedBuildIdSearchAttribute(tv.DeploymentVersionStringV32()))
				}
			}

			if len(usedBuilds) > 0 {
				// Validate TemporalUsedWorkerDeploymentVersions search attribute
				versionPayload, ok := w.GetSearchAttributes().GetIndexedFields()["TemporalUsedWorkerDeploymentVersions"]
				a.True(ok)
				versionAttrAny, err := searchattribute.DecodeValue(versionPayload, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, true)
				a.NoError(err)
				var versionAttr []string
				if versionAttrAny != nil {
					versionAttr = versionAttrAny.([]string)
				}
				for _, b := range usedBuilds {
					a.Contains(versionAttr, b.DeploymentVersionStringV32())
				}
			}

			fmt.Println(resp.GetExecutions()[0])
		}
	}, 5*time.Second, 50*time.Millisecond)
}

func (s *Versioning3Suite) TestAutoUpgradeWorkflows_NoBouncingBetweenVersions() {
	if !s.useRevisionNumbers {
		s.T().Skip("This test is only supported on revision number mechanics")
	}

	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)

	tv0 := testvars.New(s).WithBuildIDNumber(0)
	tv1 := tv0.WithBuildIDNumber(1)

	wf := func(ctx workflow.Context) (string, error) {
		workflow.GetSignalChannel(ctx, "afterRollback").Receive(ctx, nil)
		return "v1", nil
	}

	// v1 workers
	w1 := worker.New(s.SdkClient(), tv1.TaskQueue().GetName(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{Version: tv1.SDKDeploymentVersion(), UseVersioning: true, DefaultVersioningBehavior: workflow.VersioningBehaviorAutoUpgrade},
	})
	w1.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: "wf", VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade})
	s.NoError(w1.Start())
	defer w1.Stop()

	// Set v1 to be the current version
	s.setCurrentDeployment(tv1)

	// Start a workflow on v1
	run, err := s.SdkClient().ExecuteWorkflow(context.Background(), sdkclient.StartWorkflowOptions{
		ID:        tv1.WorkflowID(),
		TaskQueue: tv1.TaskQueue().GetName(),
	}, "wf")
	s.NoError(err)

	// Verify that the workflow is running on v1
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		s.verifyWorkflowVersioning(a, tv1, vbUnpinned, tv1.Deployment(), nil, nil)
	}, 10*time.Second, 100*time.Millisecond)

	// Start v0 workers to ensure they never receive a task
	idlePollerCtx, idlePollerCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer idlePollerCancel()
	go s.idlePollWorkflow(idlePollerCtx, tv0, true, ver3MinPollTime, "workflows should not go to the old deployment")

	// Rollback the userData to v0 to simulate routing config lag within a single partition.
	s.rollbackTaskQueueToVersion(tv0)

	// Unblock the workflow to let it continue
	s.NoError(s.SdkClient().SignalWorkflow(context.Background(), tv1.WorkflowID(), "", "afterRollback", nil))

	// Verify that the workflow completed successfully on v1
	var result string
	s.NoError(run.Get(context.Background(), &result))
	s.Equal("v1", result)
}

func (s *Versioning3Suite) TestWorkflowTQLags_DependentActivityStartsTransition() {
	if !s.useRevisionNumbers {
		s.T().Skip("This test is only supported on revision number mechanics")
	}
	/*
		The aim of this test is to show the following does not occur when using revisionNumber mechanics:
		- If the workflow TQ lags behind the activity TQ, with respect to the current version of a deployment, the activity should not be
		- redirected to a new deployment and be thought of as an independent activity.
		- Rather, the activity should commence a workflow transition!


			Test plan:
			- Use only one read and write partition.
			- Update the userData, for workflow and activity TQ, by setting current version to v0.
			- Let a controller poller complete a workflow task on v0 and schedule an activity task.
			- Now, update the userData for the activity TQ by setting the current version to v1.
			- Let an activity poller complete the activity task on v1.
			- We should see a workflow transition happen to v1 even though the workflow TQ is lagging behind the activity TQ.
	*/
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.UseRevisionNumberForWorkerVersioning, true)

	tv0 := testvars.New(s).WithBuildIDNumber(0)
	tv1 := tv0.WithBuildIDNumber(1)

	// Update the userData by setting the current version to v0
	s.updateTaskQueueDeploymentDataWithRoutingConfig(tv0, &deploymentpb.RoutingConfig{
		CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv0.DeploymentVersionString()),
		CurrentVersionChangedTime: timestamp.TimePtr(time.Now()),
		RevisionNumber:            1,
	}, map[string]*deploymentspb.WorkerDeploymentVersionData{tv0.DeploymentVersion().GetBuildId(): {
		Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
	}}, []string{}, tqTypeWf, tqTypeAct)

	// Wait until all task queue partitions know that v0 is current.
	s.waitForDeploymentDataPropagation(tv0, versionStatusCurrent, false, tqTypeWf, tqTypeAct)

	// Start a workflow on v0.
	s.startWorkflow(tv0, nil)

	// Poll for the workflow task on v0.
	s.pollWftAndHandle(tv0, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondWftWithActivities(tv0, tv0, false, vbUnpinned, "activity1"), nil
		})

	// Verify that the workflow is running on v1.
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		s.verifyWorkflowVersioning(a, tv0, vbUnpinned, tv0.Deployment(), nil, nil)
	}, 10*time.Second, 100*time.Millisecond)

	// Update the userData for the activity TQ by setting the current version to v1.
	s.syncTaskQueueDeploymentDataWithRoutingConfig(tv1, &deploymentpb.RoutingConfig{
		CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv1.DeploymentVersionString()),
		CurrentVersionChangedTime: timestamp.TimePtr(time.Now()),
		RevisionNumber:            2,
	}, map[string]*deploymentspb.WorkerDeploymentVersionData{tv1.DeploymentVersion().GetBuildId(): {
		Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
	}}, []string{}, tqTypeAct)

	// Wait until all task queue partitions know that v1 is current.
	s.waitForDeploymentDataPropagation(tv1, versionStatusCurrent, false, tqTypeAct)

	// Poll and complete the workflow task which should have been scheduled by the activity task since it would have started a transition to v1.
	workflowTaskCh := make(chan struct{}, 1)
	activityTaskCh := make(chan struct{}, 1)

	// Poll and complete the activity task on v1.
	s.pollActivityAndHandle(tv1, activityTaskCh,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondActivity(), nil
		})

	// Workflow task poller transitions the workflow to v1.
	s.pollWftAndHandle(tv1, false, workflowTaskCh,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondEmptyWft(tv1, false, vbUnpinned), nil
		})

	<-workflowTaskCh
	<-activityTaskCh

	// Verify that the workflow is running on v1.
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		s.verifyWorkflowVersioning(a, tv0, vbUnpinned, tv1.Deployment(), nil, nil)
	}, 10*time.Second, 100*time.Millisecond)
}

func (s *Versioning3Suite) TestActivityTQLags_DependentActivityCompletesOnTheNewVersion() {
	if !s.useRevisionNumbers {
		s.T().Skip("This test is only supported on revision number mechanics")
	}
	/*
		The aim of this test is to show the following does not occur when using revisionNumber mechanics:
		- If the activity TQ lags behind the workflow TQ, with respect to the current version of a deployment, the activity should not be
		- dispatched to the deployment found in the un-synced activity TQ.
		- The main difference between this test and TestActivityQLags_DependentActivityOnUnversionedCompletesOnTheNewVersion is that this one
		- shall have the activity TQ versioned to some build ID.

			Test plan:
			- Use only one read and write partition.
			- Update the userData, for workflow and activity TQ, by setting current version to v0.
			- Let a controlled poller complete the workflow task on v0 and schedule another workflow task.
			- Update the userData, this time for the workflow TQ only, by setting the current version to v1.
			- Let a controlled poller complete the workflow task on v1 and schedule an activity task.
			- We should see the activity task be dispatched to a v1 poller even though it's lagging behind the workflow TQ.
	*/
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.UseRevisionNumberForWorkerVersioning, true)

	tv0 := testvars.New(s).WithBuildIDNumber(0)
	tv1 := tv0.WithBuildIDNumber(1)

	// Update the userData for the workflow TQ by setting the current version to v0
	s.updateTaskQueueDeploymentDataWithRoutingConfig(tv0, &deploymentpb.RoutingConfig{
		CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv0.DeploymentVersionString()),
		CurrentVersionChangedTime: timestamp.TimePtr(time.Now()),
		RevisionNumber:            1,
	}, map[string]*deploymentspb.WorkerDeploymentVersionData{tv0.DeploymentVersion().GetBuildId(): {
		Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
	}}, []string{}, tqTypeWf, tqTypeAct)
	// Wait until all task queue partitions know that v0 is current.
	s.waitForDeploymentDataPropagation(tv0, versionStatusCurrent, false, tqTypeWf, tqTypeAct)

	// Start a workflow on v0.
	s.startWorkflow(tv0, nil)

	// Let a controlled poller complete the workflow task on v0 and schedule another workflow task.
	s.pollWftAndHandle(tv0, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			resp := respondEmptyWft(tv0, false, vbUnpinned)
			resp.ForceCreateNewWorkflowTask = true
			return resp, nil
		})

	// Verify that the workflow is running on v0.
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		s.verifyWorkflowVersioning(a, tv0, vbUnpinned, tv0.Deployment(), nil, nil)
	}, 10*time.Second, 100*time.Millisecond)

	// Update the userData for the workflow TQ *only* by setting the current version to v1
	s.updateTaskQueueDeploymentDataWithRoutingConfig(tv1, &deploymentpb.RoutingConfig{
		CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv1.DeploymentVersionString()),
		CurrentVersionChangedTime: timestamp.TimePtr(time.Now()),
		RevisionNumber:            2,
	}, map[string]*deploymentspb.WorkerDeploymentVersionData{tv1.DeploymentVersion().GetBuildId(): {
		Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
	}}, []string{}, tqTypeWf)
	// Wait until all task queue partitions know that v1 is current.
	s.waitForDeploymentDataPropagation(tv1, versionStatusCurrent, false, tqTypeWf)

	// Register the v1 worker in the activity TQ to prevent matching from thinking this as an independent unpinned activity
	s.updateTaskQueueDeploymentDataWithRoutingConfig(tv1, nil, map[string]*deploymentspb.WorkerDeploymentVersionData{tv1.DeploymentVersion().GetBuildId(): {
		Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE,
	}}, []string{}, tqTypeAct)

	// Let a controlled poller complete the workflow task on v1 and schedule an activity task
	s.pollWftAndHandle(tv1, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondWftWithActivities(tv1, tv1, false, vbUnpinned, "activity1"), nil
		})

	// Start an idle activity poller on v0. This poller should not receive any activity tasks
	//nolint:testifylint
	go s.idlePollActivity(ctx, tv0, true, ver3MinPollTime, "activity should not go to the old deployment")

	// Start a poller on v1
	activityTaskCh := make(chan struct{}, 1)
	s.pollActivityAndHandle(tv1, activityTaskCh,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondActivity(), nil
		})

	s.WaitForChannel(ctx, activityTaskCh)

	// Verify that the workflow is still running on v1.
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		s.verifyWorkflowVersioning(a, tv1, vbUnpinned, tv1.Deployment(), nil, nil)
	}, 10*time.Second, 100*time.Millisecond)
}

// This is technically tested already by TestChildWorkflowInheritance_UnpinnedParent. However,
// the test is present to show that revision number mechanics work as expected even when the task-queue
// partitions have a more updated view of the current version than the mutable state of a workflow.
func (s *Versioning3Suite) TestChildStartsWithParentRevision_SameTQ_TQAhead() {
	if !s.useRevisionNumbers {
		s.T().Skip("This test is only supported on revision number mechanics")
	}

	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)

	tvParent := testvars.New(s).WithBuildIDNumber(1).WithWorkflowIDNumber(1)
	tvChild := tvParent.WithWorkflowIDNumber(2)

	// Parent workflow waits for a signal before starting the child.
	parent := func(ctx workflow.Context) (string, error) {
		workflow.GetSignalChannel(ctx, "startChild").Receive(ctx, nil)
		f := workflow.ExecuteChildWorkflow(
			workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
				TaskQueue:  tvChild.TaskQueue().GetName(),
				WorkflowID: tvChild.WorkflowID(),
			}),
			"child",
		)

		var childResult string
		err := f.Get(ctx, &childResult)
		return childResult, err
	}
	child := func(ctx workflow.Context) (string, error) {
		return "v2", nil
	}

	// Workers on parent and child TQs. Note: Both the parent and child workflows run on the same TQ.
	w := worker.New(s.SdkClient(), tvParent.TaskQueue().GetName(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{Version: tvParent.SDKDeploymentVersion(), UseVersioning: true, DefaultVersioningBehavior: workflow.VersioningBehaviorAutoUpgrade},
	})
	w.RegisterWorkflowWithOptions(parent, workflow.RegisterOptions{Name: "parent", VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade})
	w.RegisterWorkflowWithOptions(child, workflow.RegisterOptions{Name: "child", VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade})
	s.NoError(w.Start())
	defer w.Stop()

	// Also start a v2 worker, which polls on the same TQ.
	tv2 := tvParent.WithBuildIDNumber(2)
	w2 := worker.New(s.SdkClient(), tv2.TaskQueue().GetName(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{Version: tv2.SDKDeploymentVersion(), UseVersioning: true, DefaultVersioningBehavior: workflow.VersioningBehaviorAutoUpgrade},
	})
	w2.RegisterWorkflowWithOptions(parent, workflow.RegisterOptions{Name: "parent", VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade})
	w2.RegisterWorkflowWithOptions(child, workflow.RegisterOptions{Name: "child", VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade})
	s.NoError(w2.Start())
	defer w2.Stop()

	// Set v1 as the current version for the deployment
	s.setCurrentDeployment(tvParent)

	// Start parent
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        tvParent.WorkflowID(),
		TaskQueue: tvParent.TaskQueue().GetName(),
	}, "parent")
	s.NoError(err)

	// Set v2 as the current version of the deployment
	s.setCurrentDeployment(tv2)

	// Unblock parent to start the child
	s.NoError(s.SdkClient().SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "startChild", nil))

	// Verify that the child started and completed on the v2 worker.
	s.Eventually(func() bool {
		desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, tvChild.WorkflowID(), "")
		if err != nil {
			return false
		}
		execInfo := desc.GetWorkflowExecutionInfo()

		// Verify that the child workflow started and completed on the v1 worker
		if execInfo.GetVersioningInfo().GetDeploymentVersion().GetBuildId() != tv2.BuildID() {
			return false
		}
		if execInfo.GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
			return false
		}
		return true
	}, 10*time.Second, 100*time.Millisecond)

	// Verify that the parent workflow completed successfully. This shall only be possible if the child workflow
	// started and completed on the v2 worker.
	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("v2", result)
}

func (s *Versioning3Suite) TestVersionedPoller_FailsWithEmptyNormalName() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tv := testvars.New(s)

	// Simulate an old SDK polling a sticky task queue without providing normalName
	stickyTaskQueueWithoutNormalName := &taskqueuepb.TaskQueue{
		Name:       "sticky-" + uuid.NewString(),
		Kind:       enumspb.TASK_QUEUE_KIND_STICKY,
		NormalName: "",
	}

	// Poll the sticky task queue
	wfResponse, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		TaskQueue:         stickyTaskQueueWithoutNormalName,
		DeploymentOptions: tv.WorkerDeploymentOptions(true),
		Namespace:         s.Namespace().String(),
		Identity:          tv.WorkerIdentity(),
	})

	// Expect an error because a versioned poller always requires a non-empty NormalName
	s.Error(err)
	s.Nil(wfResponse)
	s.ErrorContains(err, "NormalName must be set on sticky queue when UseVersioning is true or DeploymentOptions are set.")

	// Poll activity task queue with an empty normalName (not possible, but conservative programming to safeguard against any potential bugs)
	activityResponse, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		TaskQueue:         stickyTaskQueueWithoutNormalName,
		DeploymentOptions: tv.WorkerDeploymentOptions(true),
		Namespace:         s.Namespace().String(),
		Identity:          tv.WorkerIdentity(),
	})

	// Expect an error because a versioned activity poller always requires a non-empty NormalName
	s.Error(err)
	s.Nil(activityResponse)
	s.ErrorContains(err, "NormalName must be set on sticky queue when UseVersioning is true or DeploymentOptions are set.")
}

func (s *Versioning3Suite) TestChildStartsWithParentRevision_SameTQ_TQLags() {
	if !s.useRevisionNumbers {
		s.T().Skip("This test is only supported on revision number mechanics")
	}

	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)

	tvParent := testvars.New(s).WithBuildIDNumber(1).WithWorkflowIDNumber(1)
	tvChild := tvParent.WithWorkflowIDNumber(2)

	// Parent workflow waits for a signal before starting the child.
	parent := func(ctx workflow.Context) (string, error) {
		workflow.GetSignalChannel(ctx, "startChild").Receive(ctx, nil)
		f := workflow.ExecuteChildWorkflow(
			workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
				TaskQueue:  tvChild.TaskQueue().GetName(),
				WorkflowID: tvChild.WorkflowID(),
			}),
			"child",
		)

		var childResult string
		err := f.Get(ctx, &childResult)
		return childResult, err
	}
	child := func(ctx workflow.Context) (string, error) {
		return "v1", nil
	}

	// Workers on parent and child TQs. Note: Both the parent and child workflows run on the same TQ.
	w := worker.New(s.SdkClient(), tvParent.TaskQueue().GetName(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{Version: tvParent.SDKDeploymentVersion(), UseVersioning: true, DefaultVersioningBehavior: workflow.VersioningBehaviorAutoUpgrade},
	})
	w.RegisterWorkflowWithOptions(parent, workflow.RegisterOptions{Name: "parent", VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade})
	w.RegisterWorkflowWithOptions(child, workflow.RegisterOptions{Name: "child", VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade})
	s.NoError(w.Start())
	defer w.Stop()

	// Set v1 as the current version for the deployment
	s.setCurrentDeployment(tvParent)

	// Start parent
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        tvParent.WorkflowID(),
		TaskQueue: tvParent.TaskQueue().GetName(),
	}, "parent")
	s.NoError(err)

	// Verify that the parent workflow has started on the v1 worker
	s.Eventually(func() bool {
		desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, tvParent.WorkflowID(), "")
		if err != nil {
			return false
		}
		return desc.GetWorkflowExecutionInfo().GetVersioningInfo().GetDeploymentVersion().GetBuildId() == tvParent.BuildID()
	}, 10*time.Second, 100*time.Millisecond)

	// Roll back the child TQ routing-config revision to simulate Routing Config lag in matching partitions (set v0 as current with older revision)
	tv0Child := tvChild.WithBuildIDNumber(0)
	s.rollbackTaskQueueToVersion(tv0Child)

	//nolint:testifylint
	idlePollerCtx, idlePollerCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer idlePollerCancel()
	go s.idlePollWorkflow(idlePollerCtx, tv0Child, true, 10*time.Second, "workflow should not go to the old deployment")

	// Unblock parent to start the child
	s.NoError(s.SdkClient().SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "startChild", nil))

	// Verify that the child started and completed on the v1 worker.
	s.Eventually(func() bool {
		desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, tvChild.WorkflowID(), "")
		if err != nil {
			return false
		}
		execInfo := desc.GetWorkflowExecutionInfo()

		// Verify that the child workflow started and completed on the v1 worker
		if execInfo.GetVersioningInfo().GetDeploymentVersion().GetBuildId() != tvChild.BuildID() {
			return false
		}
		if execInfo.GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
			return false
		}
		return true
	}, 10*time.Second, 100*time.Millisecond)

	// Verify that the parent workflow completed successfully. This shall only be possible if the child workflow
	// started and completed on the v1 worker.
	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("v1", result)

}

// TestChildStartsWithNoInheritedAutoUpgradeInfo_CrossTQ demonstrates that a child workflow of an AutoUpgrade parent, not sharing
// the same task queue, starts with no inherited auto upgrade info.
func (s *Versioning3Suite) TestChildStartsWithNoInheritedAutoUpgradeInfo_CrossTQ() {
	if !s.useRevisionNumbers {
		s.T().Skip("This test is only supported on revision number mechanics")
	}

	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)

	// Use different task queues that are in different deployments for both parent and child
	tvParent := testvars.New(s).WithDeploymentSeriesNumber(1).WithBuildIDNumber(1).WithWorkflowIDNumber(1).WithTaskQueue("parent-tq")
	tvChild := tvParent.WithDeploymentSeriesNumber(2).WithBuildIDNumber(2).WithWorkflowIDNumber(2).WithTaskQueue("child-tq")

	// Parent workflow waits for a signal before starting the child.
	parent := func(ctx workflow.Context) (string, error) {
		workflow.GetSignalChannel(ctx, "startChild").Receive(ctx, nil)
		f := workflow.ExecuteChildWorkflow(
			workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
				TaskQueue:  tvChild.TaskQueue().GetName(),
				WorkflowID: tvChild.WorkflowID(),
			}),
			"child",
		)

		var childResult string
		err := f.Get(ctx, &childResult)
		return childResult, err
	}
	child := func(ctx workflow.Context) (string, error) { return "v1", nil }

	// Workers on parent and child TQs
	w := worker.New(s.SdkClient(), tvParent.TaskQueue().GetName(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{Version: tvParent.SDKDeploymentVersion(), UseVersioning: true, DefaultVersioningBehavior: workflow.VersioningBehaviorAutoUpgrade},
	})
	w.RegisterWorkflowWithOptions(parent, workflow.RegisterOptions{Name: "parent", VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade})
	s.NoError(w.Start())
	defer w.Stop()

	// Start child worker on child TQ
	wChild := worker.New(s.SdkClient(), tvChild.TaskQueue().GetName(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{Version: tvChild.SDKDeploymentVersion(), UseVersioning: true, DefaultVersioningBehavior: workflow.VersioningBehaviorAutoUpgrade},
	})
	wChild.RegisterWorkflowWithOptions(child, workflow.RegisterOptions{Name: "child", VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade})
	s.NoError(wChild.Start())
	defer wChild.Stop()

	// Set tvParent as the current version for the parent deployment
	s.setCurrentDeployment(tvParent)

	// Set tvChild as the current version for the child deployment
	s.setCurrentDeployment(tvChild)

	// Start parent
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        tvParent.WorkflowID(),
		TaskQueue: tvParent.TaskQueue().GetName(),
	}, "parent")
	s.NoError(err)

	// Unblock parent to start the child
	s.NoError(s.SdkClient().SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "startChild", nil))

	// Verify that the child workflow started and completed on it's worker, which shall verify that no AutoUpgradeInheritedInfo was inherited.
	s.Eventually(func() bool {
		s.Eventually(func() bool {
			desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, tvChild.WorkflowID(), "")
			if err != nil {
				return false
			}
			execInfo := desc.GetWorkflowExecutionInfo()

			// Verify that the new run starts on the v1 worker
			if execInfo.GetVersioningInfo().GetDeploymentVersion().GetBuildId() != tvChild.BuildID() {
				return false
			}
			return true
		}, 10*time.Second, 100*time.Millisecond)
		return true
	}, 10*time.Second, 100*time.Millisecond)

	// Verify that the parent workflow completed successfully.
	// This shall only be possible if the child workflow started on it's worker and completed.
	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("v1", result)
}

// Tests testing continue-as-new of an AutoUpgrade workflow using revision number mechanics.
func (s *Versioning3Suite) TestContinueAsNewOfAutoUpgradeWorkflow_RevisionNumberMechanics() {
	if !s.useRevisionNumbers {
		s.T().Skip("This test is only supported on revision number mechanics")
	}

	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)

	tv1 := testvars.New(s).WithBuildIDNumber(1).WithWorkflowIDNumber(1)

	// Workflow that waits for a signal before continuing-as-new
	canWorkflow := func(ctx workflow.Context, attempt int) (string, error) {
		if attempt == 0 {
			workflow.GetSignalChannel(ctx, "triggerCAN").Receive(ctx, nil)
			return "", workflow.NewContinueAsNewError(ctx, "canWorkflow", attempt+1)
		}
		return "v1", nil
	}

	// Worker on v1
	w := worker.New(s.SdkClient(), tv1.TaskQueue().GetName(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{Version: tv1.SDKDeploymentVersion(), UseVersioning: true, DefaultVersioningBehavior: workflow.VersioningBehaviorAutoUpgrade},
	})
	w.RegisterWorkflowWithOptions(canWorkflow, workflow.RegisterOptions{Name: "canWorkflow", VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade})
	s.NoError(w.Start())
	defer w.Stop()

	// Set v1 to be the current version for the deployment
	s.setCurrentDeployment(tv1)

	// Start workflow
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        tv1.WorkflowID(),
		TaskQueue: tv1.TaskQueue().GetName(),
	}, "canWorkflow", 0)
	s.NoError(err)

	// Ensure the workflow has started on the v1 worker
	s.Eventually(func() bool {
		desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, tv1.WorkflowID(), "")
		if err != nil {
			return false
		}
		return desc.GetWorkflowExecutionInfo().GetVersioningInfo().GetDeploymentVersion().GetBuildId() == tv1.BuildID()
	}, 10*time.Second, 100*time.Millisecond)

	// Rollback the TaskQueueUserData to simulate task queue partition lag
	tv0 := tv1.WithBuildIDNumber(0)
	s.rollbackTaskQueueToVersion(tv0)

	idlePollerCtx, idlePollerCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer idlePollerCancel()
	go s.idlePollWorkflow(idlePollerCtx, tv0, true, 10*time.Second, "workflow should not go to the old deployment")

	// Signal the workflow to trigger CAN
	s.NoError(s.SdkClient().SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "triggerCAN", nil))

	// Wait for the new run to start
	var newRunID string
	s.Eventually(func() bool {
		desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, tv1.WorkflowID(), "")
		if err != nil {
			return false
		}
		execInfo := desc.GetWorkflowExecutionInfo()

		// Verify that the workflow truly CAN'ed and started a new run
		if execInfo.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
			newRunID = execInfo.GetExecution().GetRunId()
			if newRunID == run.GetRunID() {
				return false
			}
		}

		// Verify that the new run starts on the v1 worker
		if execInfo.GetVersioningInfo().GetDeploymentVersion().GetBuildId() != tv1.BuildID() {
			return false
		}

		return true
	}, 10*time.Second, 100*time.Millisecond)

	// Verify that the workflow completed successfully on v1
	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("v1", result)

}

// Verifies that a retry run starts on the same version the first run executed on,
// even if the task-queue partition's user data is rolled back to an older version.
// If testContinueAsNew is true, tests a ContinueAsNew followed by retry; otherwise tests a direct retry of a workflow.
// If testChildWorkflow is true, tests that a child workflow's retry doesn't bounce back (child spawned by parent with retry policy).
func (s *Versioning3Suite) testRetryNoBounceBack(testContinueAsNew bool, testChildWorkflow bool) {
	if !s.useRevisionNumbers {
		s.T().Skip("This test is only supported on revision number mechanics")
	}

	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)

	tv1 := testvars.New(s).WithBuildIDNumber(1)
	tv0 := tv1.WithBuildIDNumber(0)

	childWorkflowID := tv1.WorkflowID() + "-child"

	// Child workflow: executes activity, waits for signal, then fails to trigger retry
	childWf := func(ctx workflow.Context) (string, error) {
		var ret string
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    time.Second,
				BackoffCoefficient: 1,
			},
		}), "act").Get(ctx, &ret)
		if err != nil {
			return "", err
		}
		// Wait for signal and fail to trigger retry
		workflow.GetSignalChannel(ctx, "proceed").Receive(ctx, nil)
		return "", errors.New("explicit failure to trigger child retry")
	}

	// Parent workflow that spawns child with retry policy (parent itself doesn't retry)
	parentWf := func(ctx workflow.Context) error {
		fut := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			TaskQueue:  tv1.TaskQueue().GetName(),
			WorkflowID: childWorkflowID,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval: time.Second,
			},
		}), "child-wf")
		var val string
		s.NoError(fut.Get(ctx, &val))
		return nil
	}

	var wf func(workflow.Context, int) (string, error)
	if testContinueAsNew {
		// Workflow that does ContinueAsNew, then waits for signal to fail (triggering retry)
		wf = func(ctx workflow.Context, runCount int) (string, error) {
			var ret string
			err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 30 * time.Second,
				RetryPolicy: &temporal.RetryPolicy{
					InitialInterval:    time.Second,
					BackoffCoefficient: 1,
				},
			}), "act").Get(ctx, &ret)
			if err != nil {
				return "", err
			}

			// Check if we should continue as new
			if runCount == 0 {
				// First run: do ContinueAsNew with runCount = 1
				return "", workflow.NewContinueAsNewError(ctx, "wf-retry", 1)
			}

			// After CAN: wait for signal and fail to trigger retry
			workflow.GetSignalChannel(ctx, "proceed").Receive(ctx, nil)
			return "", errors.New("explicit failure to trigger retry")
		}
	} else {
		// Simple workflow: run an activity to ensure it's executing, then wait for a signal to fail to trigger retry.
		wf = func(ctx workflow.Context, runCount int) (string, error) {
			var ret string
			err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 30 * time.Second,
				RetryPolicy: &temporal.RetryPolicy{
					InitialInterval:    time.Second,
					BackoffCoefficient: 1,
				},
			}), "act").Get(ctx, &ret)
			if err != nil {
				return "", err
			}
			// Block until instructed to proceed (and then fail to cause retry)
			workflow.GetSignalChannel(ctx, "proceed").Receive(ctx, nil)
			return "", errors.New("explicit failure to trigger retry")
		}
	}
	act := func() (string, error) { return "ok", nil }

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Worker for version 1 (current).
	w1 := worker.New(s.SdkClient(), tv1.TaskQueue().GetName(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			Version:       tv1.SDKDeploymentVersion(),
			UseVersioning: true,
		},
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	if !testChildWorkflow {
		w1.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: "wf-retry", VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade})
	} else {
		w1.RegisterWorkflowWithOptions(parentWf, workflow.RegisterOptions{Name: "parent-wf", VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade})
		w1.RegisterWorkflowWithOptions(childWf, workflow.RegisterOptions{Name: "child-wf", VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade})
	}
	w1.RegisterActivityWithOptions(act, activity.RegisterOptions{Name: "act"})
	s.NoError(w1.Start())
	defer w1.Stop()

	// Set v1 to be the current version for the deployment
	s.setCurrentDeployment(tv1)

	var wfID string
	var runIDBeforeRetry string

	if testChildWorkflow {
		// Start parent workflow (which spawns child with retry policy)
		_, err := s.SdkClient().ExecuteWorkflow(
			ctx,
			sdkclient.StartWorkflowOptions{
				TaskQueue: tv1.TaskQueue().GetName(),
			},
			"parent-wf",
		)
		s.NoError(err)

		// Wait for child workflow to be created and executing on v1
		s.Eventually(func() bool {
			desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, childWorkflowID, "")
			if err != nil {
				return false
			}
			runIDBeforeRetry = desc.GetWorkflowExecutionInfo().GetExecution().GetRunId()
			return desc.GetWorkflowExecutionInfo().GetVersioningInfo().GetDeploymentVersion().GetBuildId() == tv1.BuildID()
		}, 10*time.Second, 100*time.Millisecond)

		wfID = childWorkflowID
	} else {
		// Start workflow with retry policy (so failure will trigger automatic retry).
		run0, err := s.SdkClient().ExecuteWorkflow(
			ctx,
			sdkclient.StartWorkflowOptions{
				TaskQueue: tv1.TaskQueue().GetName(),
				RetryPolicy: &temporal.RetryPolicy{
					InitialInterval: time.Second,
				},
			},
			"wf-retry",
			0, // runCount starts at 0
		)
		s.NoError(err)

		wfID = run0.GetID()

		if testContinueAsNew {
			// Wait for ContinueAsNew to happen
			s.Eventually(func() bool {
				desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, wfID, "")
				s.NoError(err)
				if err != nil {
					return false
				}
				// After CAN, the run ID changes and we should see execution on v1
				if desc.GetWorkflowExecutionInfo().GetExecution().GetRunId() != run0.GetRunID() {
					runIDBeforeRetry = desc.GetWorkflowExecutionInfo().GetExecution().GetRunId()
					return desc.GetWorkflowExecutionInfo().GetVersioningInfo().GetDeploymentVersion().GetBuildId() == tv1.BuildID()
				}
				return false
			}, 10*time.Second, 100*time.Millisecond)
		} else {
			runIDBeforeRetry = run0.GetRunID()
			// Ensure initial run is executing on v1.
			s.Eventually(func() bool {
				desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, wfID, runIDBeforeRetry)
				s.NoError(err)
				if err != nil {
					return false
				}
				return desc.GetWorkflowExecutionInfo().GetVersioningInfo().GetDeploymentVersion().GetBuildId() == tv1.BuildID()
			}, 10*time.Second, 100*time.Millisecond)
		}
	}

	// Roll back the child TQ routing-config revision to simulate Routing Config lag in matching partitions (set v0 as current with older revision)
	s.rollbackTaskQueueToVersion(tv0)

	// Start v0 pollers and ensure they don't receive a task
	idlePollerCtx, idlePollerCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer idlePollerCancel()
	go s.idlePollWorkflow(idlePollerCtx, tv0, true, 10*time.Second, "v0 poller should not receive a task")

	// Verify that the rollback propagated to all partitions
	s.Eventually(func() bool {
		ms, err := s.GetTestCluster().MatchingClient().GetTaskQueueUserData(context.Background(), &matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:   s.NamespaceID().String(),
			TaskQueue:     tv0.TaskQueue().GetName(),
			TaskQueueType: tqTypeWf,
		})
		s.NoError(err)

		current, currentRevisionNumber, _, _, _, _, _, _ := worker_versioning.CalculateTaskQueueVersioningInfo(ms.GetUserData().GetData().GetPerType()[int32(tqTypeWf)].GetDeploymentData())
		return current.GetBuildId() == tv0.DeploymentVersion().GetBuildId() && currentRevisionNumber == 0
	}, 10*time.Second, 100*time.Millisecond)

	// Trigger failure of the run to cause retry.
	s.NoError(s.SdkClient().SignalWorkflow(ctx, wfID, runIDBeforeRetry, "proceed", nil))

	// Wait for run to fail.
	s.Eventually(func() bool {
		desc, err := s.SdkClient().DescribeWorkflow(ctx, wfID, runIDBeforeRetry)
		s.NoError(err)
		if err != nil {
			return false
		}
		return desc.Status == enumspb.WORKFLOW_EXECUTION_STATUS_FAILED
	}, 10*time.Second, 100*time.Millisecond)

	// Verify that retry run is still on v1 (didn't bounce back to v0)
	s.Eventually(func() bool {
		desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, wfID, "")
		s.NoError(err)
		if err != nil {
			return false
		}
		// After retry, there should be a new run
		if desc.GetWorkflowExecutionInfo().GetExecution().GetRunId() != runIDBeforeRetry {
			// Verify workflow (parent or child) is still on v1
			return desc.GetWorkflowExecutionInfo().GetVersioningInfo().GetDeploymentVersion().GetBuildId() == tv1.BuildID()
		}
		return false
	}, 10*time.Second, 100*time.Millisecond)

}

func (s *Versioning3Suite) TestWorkflowRetry_AutoUpgrade_NoBounceBack() {
	s.testRetryNoBounceBack(false, false)
}

func (s *Versioning3Suite) TestWorkflowRetry_AutoUpgrade_AfterCAN_NoBounceBack() {
	s.testRetryNoBounceBack(true, false)
}

func (s *Versioning3Suite) TestWorkflowRetry_AutoUpgrade_ChildNoBounceBack() {
	s.testRetryNoBounceBack(false, true)
}

// The following tests test out the CheckTaskQueueVersionMembership RPC.
func (s *Versioning3Suite) TestCheckTaskQueueVersionMembership() {
	tv1 := testvars.New(s).WithBuildIDNumber(1)

	// No version exists in the task queue's userData as of now
	s.Eventually(func() bool {
		resp, err := s.GetTestCluster().MatchingClient().CheckTaskQueueVersionMembership(context.Background(), &matchingservice.CheckTaskQueueVersionMembershipRequest{
			NamespaceId:   s.NamespaceID().String(),
			TaskQueue:     tv1.TaskQueue().GetName(),
			TaskQueueType: tqTypeWf,
			Version:       worker_versioning.DeploymentVersionFromDeployment(tv1.Deployment()),
		})
		s.NoError(err)
		return !resp.GetIsMember() // the check should pass if no version is present
	}, 10*time.Second, 100*time.Millisecond)

	// Start v1 worker which shall register the version in the task queue
	w1 := worker.New(s.SdkClient(), tv1.TaskQueue().GetName(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			Version:       tv1.SDKDeploymentVersion(),
			UseVersioning: true,
		},
	})
	s.NoError(w1.Start())
	defer w1.Stop()

	// The version should eventually show up in the task queue's user data
	s.Eventually(func() bool {
		resp, err := s.GetTestCluster().MatchingClient().CheckTaskQueueVersionMembership(context.Background(), &matchingservice.CheckTaskQueueVersionMembershipRequest{
			NamespaceId:   s.NamespaceID().String(),
			TaskQueue:     tv1.TaskQueue().GetName(),
			TaskQueueType: tqTypeWf,
			Version:       worker_versioning.DeploymentVersionFromDeployment(tv1.Deployment()),
		})
		s.NoError(err)
		return resp.GetIsMember()
	}, 10*time.Second, 100*time.Millisecond)
}

// validatePinnedVersionExistsInTaskQueue validates that the version, to be pinned, exists in the task queue.
// TODO (future improvement): This can be further extended to validate the presence of any version instead of using the GetTaskQueueUserData RPC.
func (s *Versioning3Suite) validatePinnedVersionExistsInTaskQueue(tv *testvars.TestVars) {
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := s.GetTestCluster().MatchingClient().CheckTaskQueueVersionMembership(context.Background(), &matchingservice.CheckTaskQueueVersionMembershipRequest{
			NamespaceId:   s.NamespaceID().String(),
			TaskQueue:     tv.TaskQueue().GetName(),
			TaskQueueType: tqTypeWf,
			Version:       worker_versioning.DeploymentVersionFromDeployment(tv.Deployment()),
		})
		a.NoError(err)
		a.True(resp.GetIsMember())
	}, 10*time.Second, 100*time.Millisecond)

}

// TestMaxVersionsInTaskQueue tests that polling from a task queue with too many
// versions returns a RESOURCE_EXHAUSTED_CAUSE_WORKER_DEPLOYMENT_LIMITS error
func (s *Versioning3Suite) TestMaxVersionsInTaskQueue() {
	// This test is not dependent on a particular deployment workflow version, but we don't want it repetitively
	s.skipBeforeVersion(workerdeployment.VersionDataRevisionNumber)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Set a low limit for the test to make it faster
	maxVersions := 5
	s.OverrideDynamicConfig(dynamicconfig.MatchingMaxVersionsInTaskQueue, maxVersions)

	tv := testvars.New(s)

	// Pre-populate the task queue with maxVersions different deployment versions
	// Each version will be in a separate deployment to ensure they count toward the limit
	for i := 0; i <= maxVersions; i++ {
		tvVersion := tv.WithDeploymentSeriesNumber(i).WithBuildIDNumber(i)
		upsertVersions := make(map[string]*deploymentspb.WorkerDeploymentVersionData)
		upsertVersions[tvVersion.BuildID()] = &deploymentspb.WorkerDeploymentVersionData{
			Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_INACTIVE,
		}

		deploymentName := tvVersion.DeploymentVersion().GetDeploymentName()
		_, err := s.GetTestCluster().MatchingClient().SyncDeploymentUserData(
			ctx, &matchingservice.SyncDeploymentUserDataRequest{
				NamespaceId:        s.NamespaceID().String(),
				TaskQueue:          tv.TaskQueue().GetName(),
				TaskQueueTypes:     []enumspb.TaskQueueType{tqTypeWf},
				DeploymentName:     deploymentName,
				UpsertVersionsData: upsertVersions,
			},
		)
		s.NoError(err)
	}

	// Now try to poll with a new version from a new deployment
	// This should fail with RESOURCE_EXHAUSTED_CAUSE_WORKER_DEPLOYMENT_LIMITS
	tvNewVersion := tv.WithDeploymentSeriesNumber(999).WithBuildIDNumber(999)

	pollCtx, pollCancel := context.WithTimeout(ctx, 10*time.Second)
	defer pollCancel()

	_, pollErr := s.FrontendClient().PollWorkflowTaskQueue(pollCtx, &workflowservice.PollWorkflowTaskQueueRequest{
		TaskQueue:         tvNewVersion.TaskQueue(),
		DeploymentOptions: tvNewVersion.WorkerDeploymentOptions(true),
		Namespace:         s.Namespace().String(),
		Identity:          tvNewVersion.WorkerIdentity(),
	})

	// Verify we got the expected resource exhausted error
	s.Error(pollErr)
	var resourceExhausted *serviceerror.ResourceExhausted
	s.ErrorAs(pollErr, &resourceExhausted)
	s.Equal(enumspb.RESOURCE_EXHAUSTED_CAUSE_WORKER_DEPLOYMENT_LIMITS, resourceExhausted.Cause,
		"Expected RESOURCE_EXHAUSTED_CAUSE_WORKER_DEPLOYMENT_LIMITS")
	s.Contains(resourceExhausted.Message, "exceeding maximum number of Deployment Versions in this task queue",
		"Error message should mention exceeding deployment version limit")
	s.Contains(resourceExhausted.Message, fmt.Sprintf("limit = %d", maxVersions),
		"Error message should include the limit value")
}

func (s *Versioning3Suite) TestActivityRetryAutoUpgradeDuringBackoff() {
	// This test verifies that when an activity is in retry backoff and the current deployment
	// changes, the workflow will auto-upgrade to the new deployment when the activity retry
	// executes, if the workflow's versioning behavior is AutoUpgrade.

	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)

	numPollers := 4
	tv1 := testvars.New(s).WithBuildIDNumber(1)
	tv2 := tv1.WithBuildIDNumber(2)

	// Track activity attempts and which version executed them
	var attemptCount atomic.Int32
	var v1AttemptCount atomic.Int32
	var v2AttemptCount atomic.Int32

	// Activity function for v1 - fails on first attempt
	actV1 := func() (string, error) {
		attempt := attemptCount.Add(1)
		v1AttemptCount.Add(1)
		if attempt == 1 {
			return "", errors.New("intentional failure on v1 to trigger retry")
		}
		return "v1", nil
	}

	// Activity function for v2 - should succeed when it gets the retry attempt
	actV2 := func() (string, error) {
		attemptCount.Add(1)
		v2AttemptCount.Add(1)
		return "v2", nil
	}

	// Workflow that executes an activity with retry policy
	wf := func(ctx workflow.Context) (string, error) {
		var result string
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 10 * time.Second,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    5 * time.Second, // Give us time to change deployment
				BackoffCoefficient: 1.0,
				MaximumAttempts:    3,
			},
		}), "act").Get(ctx, &result)
		return result, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Create worker for v1
	w1 := worker.New(s.SdkClient(), tv1.TaskQueue().GetName(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			Version:       tv1.SDKDeploymentVersion(),
			UseVersioning: true,
		},
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{
		Name:               "wf",
		VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
	})
	w1.RegisterActivityWithOptions(actV1, activity.RegisterOptions{Name: "act"})
	s.NoError(w1.Start())
	defer w1.Stop()

	// Create worker for v2
	w2 := worker.New(s.SdkClient(), tv1.TaskQueue().GetName(), worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			Version:       tv2.SDKDeploymentVersion(),
			UseVersioning: true,
		},
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w2.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{
		Name:               "wf",
		VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
	})
	w2.RegisterActivityWithOptions(actV2, activity.RegisterOptions{Name: "act"})
	s.NoError(w2.Start())
	defer w2.Stop()

	// Set v1 as current deployment
	s.setCurrentDeployment(tv1)
	s.waitForDeploymentDataPropagation(tv1, versionStatusCurrent, false, tqTypeWf, tqTypeAct)

	// Start the workflow
	run, err := s.SdkClient().ExecuteWorkflow(
		ctx,
		sdkclient.StartWorkflowOptions{
			TaskQueue: tv1.TaskQueue().GetName(),
		},
		"wf",
	)
	s.NoError(err)

	// Wait for the workflow to start on v1
	s.Eventually(func() bool {
		desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, run.GetID(), "")
		if err != nil {
			return false
		}
		return desc.GetWorkflowExecutionInfo().GetVersioningInfo().GetDeploymentVersion().GetBuildId() == tv1.BuildID()
	}, 10*time.Second, 100*time.Millisecond)

	// Wait for first activity attempt to fail (should be on v1)
	s.Eventually(func() bool {
		return v1AttemptCount.Load() == 1
	}, 10*time.Second, 100*time.Millisecond)

	// Now the activity is in retry backoff. Change the current deployment to v2
	// while the activity is waiting to retry.
	s.setCurrentDeployment(tv2)
	s.waitForDeploymentDataPropagation(tv2, versionStatusCurrent, false, tqTypeWf, tqTypeAct)

	// Wait for the workflow to complete
	var result string
	err = run.Get(ctx, &result)
	s.NoError(err)

	// Verify results:
	// 1. The workflow should have succeeded
	s.Equal("v2", result, "Expected workflow to complete with result from v2")

	// 2. v1 should have processed exactly 1 attempt (the initial one that failed)
	s.Equal(int32(1), v1AttemptCount.Load(), "Expected v1 to process exactly 1 attempt")

	// 3. v2 should have processed at least 1 attempt (the retry after version change)
	s.GreaterOrEqual(v2AttemptCount.Load(), int32(1), "Expected v2 to process at least 1 attempt")

	// 4. Total attempts should be 2 (one on v1 that failed, one on v2 that succeeded)
	s.Equal(int32(2), attemptCount.Load(), "Expected total of 2 activity attempts")

	// 5. Verify the workflow is now running on v2
	desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, run.GetID(), "")
	s.NoError(err)
	s.Equal(tv2.BuildID(), desc.GetWorkflowExecutionInfo().GetVersioningInfo().GetDeploymentVersion().GetBuildId(),
		"Expected workflow to be on v2 after auto-upgrade")
}

func (s *Versioning3Suite) TestVersionedQueueUnload() {
	// Set MatchingMaxTaskQueueIdleTime to 5 seconds to shorten test duration
	s.OverrideDynamicConfig(dynamicconfig.MatchingMaxTaskQueueIdleTime, 5*time.Second)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tv1 := testvars.New(s)

	// Register v1 poller before setting it as current
	s.idlePollWorkflow(ctx, tv1, true, ver3MinPollTime, "should not get any tasks yet")

	// Set current version to v1
	s.setCurrentDeployment(tv1)
	s.waitForDeploymentDataPropagation(tv1, versionStatusCurrent, false, tqTypeWf)

	// Start workflow with autoUpgrade behavior without any poller
	s.startWorkflow(tv1, nil)

	// Ensure the task is backlogged by checking stats
	s.validateBacklogCount(tv1, tqTypeWf, 1)

	// Ensure the default queue is loaded by sending long polls for user data for 10 seconds
	keepAliveDone := make(chan struct{})
	go func() {
		defer close(keepAliveDone)
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		timeout := time.After(10 * time.Second)
		for {
			select {
			case <-timeout:
				return
			case <-ticker.C:
				smallCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
				_, _ = s.GetTestCluster().MatchingClient().GetTaskQueueUserData(smallCtx, &matchingservice.GetTaskQueueUserDataRequest{
					NamespaceId:   s.NamespaceID().String(),
					OnlyIfLoaded:  false,
					TaskQueue:     tv1.TaskQueue().GetName(),
					TaskQueueType: tqTypeWf,
					WaitNewData:   true,
				})
				cancel()
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for 10 seconds
	s.WaitForChannel(ctx, keepAliveDone)

	// Send a poll from v1 and ensure the poller gets the task
	taskReceived := false
	s.pollWftAndHandle(tv1, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			taskReceived = true
			return respondCompleteWorkflow(tv1, vbUnpinned), nil
		})

	s.True(taskReceived, "Poller should have received the task after keeping queue loaded")

	// Verify backlog is now empty
	s.validateBacklogCount(tv1, tqTypeWf, 0)
}

func (s *Versioning3Suite) TestTransitionDuringTransientTask_WithoutSignal() {
	s.testTransitionDuringTransientTask(false)
}

func (s *Versioning3Suite) TestTransitionDuringTransientTask_WithSignal() {
	s.testTransitionDuringTransientTask(true)
}

// TestTransitionDuringTransientTask verifies that a deployment version transition to v1
// is properly set when a workflow task fails and is retried, and that the transition completes
// successfully after a signal is sent during the retry backoff period.
func (s *Versioning3Suite) testTransitionDuringTransientTask(withSignal bool) {

	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)
	s.OverrideDynamicConfig(dynamicconfig.MatchingUseNewMatcher, false)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Create test vars for v1
	tv1 := testvars.New(s).WithBuildIDNumber(1)

	// We need to keep pollers until the task queues are properly registered
	pollCtx, pollCtxCancel := context.WithTimeout(ctx, 60*time.Second)
	go s.idlePollWorkflow(pollCtx, tv1, true, ver3MinPollTime, "should not get any tasks yet")
	go s.idlePollActivity(pollCtx, tv1, true, ver3MinPollTime, "should not get any tasks yet")
	s.waitForDeploymentDataPropagation(tv1, versionStatusInactive, false, tqTypeWf, tqTypeAct)
	pollCtxCancel()

	// Start the workflow
	execution := &commonpb.WorkflowExecution{
		WorkflowId: tv1.WorkflowID(),
	}
	runID := s.startWorkflow(tv1, nil)
	execution.RunId = runID

	// Poll first workflow task and schedule two activities
	s.unversionedPollWftAndHandle(tv1, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			// Schedule two activities
			return respondWftWithActivities(tv1, tv1, false, vbUnspecified, "activity-1", "activity-2"), nil
		})

	// Complete the first activity to generate a new wft
	s.unversionedPollActivityAndHandle(tv1, nil,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			s.NotNil(task)
			return respondActivity(), nil
		})

	// Poll and fail the next workflow task - this will cause a transient WFT to be scheduled with retry backoff
	s.unversionedPollWftAndHandle(tv1, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			// Return error to fail the task
			return nil, errors.New("intentional workflow task failure")
		})

	// Set v1 as current deployment
	s.setCurrentDeployment(tv1)
	s.waitForDeploymentDataPropagation(tv1, versionStatusCurrent, false, tqTypeWf, tqTypeAct)

	if withSignal {
		// While during retry backoff, send a signal to the workflow
		// Signal would convert the transient task to normal because of the new history event being inserted after the first task failure
		_, err := s.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         s.Namespace().String(),
			WorkflowExecution: execution,
			SignalName:        "test-signal",
			Input:             tv1.Any().Payloads(),
			Identity:          tv1.WorkerIdentity(),
		})
		s.NoError(err)
	}

	// Poll the second activity to cause transition to v1.
	s.idlePollActivity(ctx, tv1, true, ver3MinPollTime, "should not get the activity because it started a transition")
	s.verifyWorkflowVersioning(s.Assertions, tv1, vbUnspecified, nil, nil, tv1.DeploymentVersionTransition())

	// Print workflow describe and history
	descResp, err := s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		Execution: execution,
	})
	if err == nil && descResp != nil {
		fmt.Println("=== Workflow Describe ===")
		fmt.Printf("pending activities: %v\n", descResp.PendingActivities)
		fmt.Println("=========================")
	}

	histResp, err := s.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: s.Namespace().String(),
		Execution: execution,
	})
	if err == nil && histResp != nil {
		fmt.Println("=== Workflow Event History ===")
		for i, event := range histResp.History.Events {
			fmt.Printf("Event %d: %s (EventId: %d)\n", i, event.EventType, event.EventId)
		}
		fmt.Println("==============================")
	}

	// After the retry backoff, poll and complete the workflow task
	// This should complete the transition and the workflow should be on v1
	s.pollWftAndHandle(tv1, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			// Verify that a deployment version transition to v1 is set in the workflow
			s.verifyWorkflowVersioning(s.Assertions, tv1, vbUnspecified, nil, nil, tv1.DeploymentVersionTransition())

			if withSignal {
				s.EqualHistory(`
			1 WorkflowExecutionStarted
			2 WorkflowTaskScheduled
			3 WorkflowTaskStarted
			4 WorkflowTaskCompleted
			5 ActivityTaskScheduled // activity-1
			6 ActivityTaskScheduled // activity-2
			7 ActivityTaskStarted // activity-1
			8 ActivityTaskCompleted // activity-1
			9 WorkflowTaskScheduled
			10 WorkflowTaskStarted
			11 WorkflowTaskFailed
			12 WorkflowExecutionSignaled
			13 WorkflowTaskScheduled // normal task
			14 WorkflowTaskStarted
		  `, task.History)
			} else {
				// Verify this is the transient (retry) attempt
				s.verifyTransientTask(task)
				s.EqualHistory(`
			1 WorkflowExecutionStarted
			2 WorkflowTaskScheduled
			3 WorkflowTaskStarted
			4 WorkflowTaskCompleted
			5 ActivityTaskScheduled // activity-1
			6 ActivityTaskScheduled // activity-2
			7 ActivityTaskStarted // activity-1
			8 ActivityTaskCompleted // activity-1
			9 WorkflowTaskScheduled
			10 WorkflowTaskStarted
			11 WorkflowTaskFailed
			12 WorkflowTaskScheduled // Transient task
			13 WorkflowTaskStarted
		  `, task.History)
			}

			// Complete the workflow
			return respondCompleteWorkflow(tv1, vbUnpinned), nil
		})

	// Verify the transition is completed and the workflow is on v1
	s.verifyWorkflowVersioning(s.Assertions, tv1, vbUnpinned, tv1.Deployment(), nil, nil)
}

func (s *Versioning3Suite) skipBeforeVersion(version workerdeployment.DeploymentWorkflowVersion) {
	if s.deploymentWorkflowVersion < version {
		s.T().Skipf("test supports workflow version %v and newer", version)
	}
}
