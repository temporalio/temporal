package tests

import (
	"fmt"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/matching"
	"go.temporal.io/server/service/worker/workerdeployment"
	"go.temporal.io/server/tests/testcore"
)

type Versioning3QuerySuite struct {
	parallelsuite.Suite[*Versioning3QuerySuite]
}

func TestVersioning3QueryFunctionalSuite(t *testing.T) {
	testcore.UseSuiteScopedCluster(t)                              //nolint:staticcheck // SA1019: suite still requires legacy sequential execution
	parallelsuite.RunLegacySequential(t, &Versioning3QuerySuite{}) //nolint:staticcheck // SA1019: suite still requires legacy sequential execution
}

func (s *Versioning3QuerySuite) TestQueryWithPinnedOverride_NoSticky() {
	runVersioning3TestWithMatchingBehavior(s, func(env *VersioningTestEnv, s *Versioning3QuerySuite) {
		s.testQueryWithPinnedOverride(env, false)
	})
}

func (s *Versioning3QuerySuite) TestQueryWithPinnedOverride_Sticky() {
	runVersioning3TestWithMatchingBehavior(s, func(env *VersioningTestEnv, s *Versioning3QuerySuite) {
		s.testQueryWithPinnedOverride(env, true)
	})
}

func (s *Versioning3QuerySuite) TestPinnedQuery_DrainedVersion_PollersAbsent() {
	runVersioning3TestWithMatchingBehavior(s, func(env *VersioningTestEnv, s *Versioning3QuerySuite) {
		s.testPinnedQueryDrainedVersion(env, false, false)
	},
		testcore.WithDynamicConfig(dynamicconfig.VersionDrainageStatusRefreshInterval, 1*time.Second),
		testcore.WithDynamicConfig(dynamicconfig.VersionDrainageStatusVisibilityGracePeriod, 1*time.Second),
		testcore.WithDynamicConfig(dynamicconfig.PollerHistoryTTL, 500*time.Millisecond),
	)
}

func (s *Versioning3QuerySuite) TestPinnedQuery_DrainedVersion_PollersPresent() {
	runVersioning3TestWithMatchingBehavior(s, func(env *VersioningTestEnv, s *Versioning3QuerySuite) {
		s.testPinnedQueryDrainedVersion(env, true, false)
	},
		testcore.WithDynamicConfig(dynamicconfig.VersionDrainageStatusRefreshInterval, 1*time.Second),
		testcore.WithDynamicConfig(dynamicconfig.VersionDrainageStatusVisibilityGracePeriod, 1*time.Second),
	)
}

func (s *Versioning3QuerySuite) TestPinnedQuery_RollbackDrainedVersion() {
	runVersioning3TestWithMatchingBehavior(s, func(env *VersioningTestEnv, s *Versioning3QuerySuite) {
		s.testPinnedQueryDrainedVersion(env, true, true)
	},
		testcore.WithDynamicConfig(dynamicconfig.VersionDrainageStatusRefreshInterval, 1*time.Second),
		testcore.WithDynamicConfig(dynamicconfig.VersionDrainageStatusVisibilityGracePeriod, 1*time.Second),
	)
}

func (s *Versioning3QuerySuite) testPinnedQueryDrainedVersion(env *VersioningTestEnv, pollersPresent bool, rollback bool) {
	tv := env.Tv()

	// create version v1 and make it current
	idlePollerDone := make(chan struct{})
	go func() {
		env.idlePollWorkflow(s, tv, true, ver3MinPollTime, "should not have gotten any tasks since there are none")
		close(idlePollerDone)
	}()
	env.setCurrentDeployment(s, tv)
	env.WaitForChannel(idlePollerDone)

	wftCompleted := make(chan struct{})
	env.pollWftAndHandle(s, tv, false, wftCompleted,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return env.respondCompleteWorkflow(tv, vbPinned), nil
		})

	env.startWorkflow(s, tv, tv.VersioningOverridePinned())
	env.WaitForChannel(wftCompleted)
	env.verifyWorkflowVersioning(s, tv, vbPinned, tv.Deployment(), tv.VersioningOverridePinned(), nil)

	// create version v2 and make it current which shall make v1 go from current -> draining/drained
	idlePollerDone = make(chan struct{})
	tv2 := tv.WithBuildIDNumber(2)
	go func() {
		env.idlePollWorkflow(s, tv2, true, ver3MinPollTime, "should not have gotten any tasks since there are none")
		close(idlePollerDone)
	}()
	env.setCurrentDeployment(s, tv2)
	env.WaitForChannel(idlePollerDone)

	// wait for v1 to become drained
	s.Await(func(s *Versioning3QuerySuite) {
		resp, err := env.FrontendClient().DescribeWorkerDeploymentVersion(s.Context(), &workflowservice.DescribeWorkerDeploymentVersionRequest{
			Namespace: env.Namespace().String(),
			Version:   tv.DeploymentVersionString(),
		})
		s.NoError(err)
		s.Equal(enumspb.VERSION_DRAINAGE_STATUS_DRAINED, resp.GetWorkerDeploymentVersionInfo().GetDrainageInfo().GetStatus())
	}, time.Second*10, time.Millisecond*1000)

	if !pollersPresent {
		// simulate the pollers going away, which should make the query fail as now the version is drained + has no pollers polling it
		time.Sleep(1 * time.Second) //nolint:forbidigo
		versionStr := tv.Deployment().GetBuildId()
		if versioning3DeploymentWorkflowVersion < workerdeployment.AsyncSetCurrentAndRamping {
			versionStr = worker_versioning.ExternalWorkerDeploymentVersionToString(worker_versioning.ExternalWorkerDeploymentVersionFromDeployment(tv.Deployment()))
		}

		_, err := env.queryWorkflow(s.Context(), tv)
		s.Error(err)
		s.ErrorContains(err, fmt.Sprintf(matching.ErrBlackholedQuery, versionStr, versionStr))
	} else {
		// since the version still has pollers, the query should succeed
		env.pollAndQueryWorkflow(s, tv, false)
	}

	if rollback {
		// ramp traffic back to v1 to remove it from drained state
		env.setRampingDeployment(s, tv, 50, false)

		// wait for v1 to become ramping
		s.Await(func(s *Versioning3QuerySuite) {
			resp, err := env.FrontendClient().DescribeWorkerDeploymentVersion(s.Context(), &workflowservice.DescribeWorkerDeploymentVersionRequest{
				Namespace: env.Namespace().String(),
				Version:   tv.DeploymentVersionString(),
			})
			s.NoError(err)
			s.Equal(enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING, resp.GetWorkerDeploymentVersionInfo().GetStatus())
		}, time.Second*10, time.Millisecond*1000)

		// the ramping status is propagated to the task queues
		env.waitForDeploymentDataPropagation(s, tv, versionStatusRamping, false, tqTypeWf)

		// the version has pollers and is ramping making the query succeed
		env.pollAndQueryWorkflow(s, tv, false)

	}
}

func (s *Versioning3QuerySuite) testQueryWithPinnedOverride(env *VersioningTestEnv, sticky bool) {
	tv := env.Tv()

	if sticky {
		env.warmUpSticky(s, tv)
	}

	wftCompleted := make(chan struct{})
	env.pollWftAndHandle(s, tv, false, wftCompleted,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return env.respondEmptyWft(tv, sticky, vbUnpinned), nil
		})

	// Wait for the version to be present in the task queue. Version existence is required before it can be set as an override.
	s.Await(func(s *Versioning3QuerySuite) {
		resp, err := env.GetTestCluster().MatchingClient().CheckTaskQueueVersionMembership(s.Context(), &matchingservice.CheckTaskQueueVersionMembershipRequest{
			NamespaceId:   env.NamespaceID().String(),
			TaskQueue:     tv.TaskQueue().GetName(),
			TaskQueueType: tqTypeWf,
			Version:       worker_versioning.DeploymentVersionFromDeployment(tv.Deployment()),
		})
		s.NoError(err)
		s.True(resp.GetIsMember())
	}, 10*time.Second, 500*time.Millisecond)

	runID := env.startWorkflow(s, tv, tv.VersioningOverridePinned())

	env.WaitForChannel(wftCompleted)
	env.verifyWorkflowVersioning(s, tv, vbUnpinned, tv.Deployment(), tv.VersioningOverridePinned(), nil)
	if sticky {
		env.verifyWorkflowStickyQueue(s, tv.WithRunID(runID))
	}

	env.pollAndQueryWorkflow(s, tv, sticky)
}

func (s *Versioning3QuerySuite) TestUnpinnedQuery_NoSticky() {
	runVersioning3TestWithMatchingBehavior(s, func(env *VersioningTestEnv, s *Versioning3QuerySuite) {
		s.testUnpinnedQuery(env, false)
	})
}

func (s *Versioning3QuerySuite) TestUnpinnedQuery_Sticky() {
	runVersioning3TestWithMatchingBehavior(s, func(env *VersioningTestEnv, s *Versioning3QuerySuite) {
		s.testUnpinnedQuery(env, true)
	})
}

func (s *Versioning3QuerySuite) testUnpinnedQuery(env *VersioningTestEnv, sticky bool) {
	tv := env.Tv()
	tv2 := tv.WithBuildIDNumber(2)
	if sticky {
		env.warmUpSticky(s, tv)
	}

	wftCompleted := make(chan struct{})
	env.pollWftAndHandle(s, tv, false, wftCompleted,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			env.verifyWorkflowVersioning(s, tv, vbUnspecified, nil, nil, tv.DeploymentVersionTransition())
			return env.respondEmptyWft(tv, sticky, vbUnpinned), nil
		})

	env.setCurrentDeployment(s, tv)
	env.waitForDeploymentDataPropagation(s, tv, versionStatusCurrent, false, tqTypeWf)

	runID := env.startWorkflow(s, tv, nil)

	env.WaitForChannel(wftCompleted)
	env.verifyWorkflowVersioning(s, tv, vbUnpinned, tv.Deployment(), nil, nil)
	if sticky {
		env.verifyWorkflowStickyQueue(s, tv.WithRunID(runID))
	}

	pollerDone := make(chan struct{})
	go func() {
		env.idlePollWorkflow(s, tv2, true, 5*time.Second, "new deployment should not receive query")
		close(pollerDone)
	}()
	env.pollAndQueryWorkflow(s, tv, sticky)
	env.WaitForChannel(pollerDone) // wait for the idle poller to complete to not interfere with the next poller

	env.setCurrentDeployment(s, tv2)
	env.waitForDeploymentDataPropagation(s, tv2, versionStatusCurrent, false, tqTypeWf)

	go env.idlePollWorkflow(s, tv, true, ver3MinPollTime, "old deployment should not receive query")
	// Since the current deployment has changed, task will move to the normal queue (thus, sticky=false)
	env.pollAndQueryWorkflow(s, tv2, false)
}
