package tests

import (
	"testing"
	"time"

	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/tests/testcore"
)

type Versioning3IndependentActivitySuite struct {
	parallelsuite.Suite[*Versioning3IndependentActivitySuite]
}

func TestVersioning3IndependentActivityFunctionalSuite(t *testing.T) {
	testcore.UseSuiteScopedCluster(t)                                            //nolint:staticcheck // SA1019: suite still requires legacy sequential execution
	parallelsuite.RunLegacySequential(t, &Versioning3IndependentActivitySuite{}) //nolint:staticcheck // SA1019: suite still requires legacy sequential execution
}

func (s *Versioning3IndependentActivitySuite) TestPinnedWorkflowWithLateActivityPoller() {
	runVersioning3TestWithMatchingBehavior(s, func(env *VersioningTestEnv, s *Versioning3IndependentActivitySuite) {
		s.testPinnedWorkflowWithLateActivityPoller(env)
	})
}

func (s *Versioning3IndependentActivitySuite) testPinnedWorkflowWithLateActivityPoller(env *VersioningTestEnv) {
	// Here, we test that designating activities as independent is revisited if the missing activity
	// pollers arrive to server while the so-far-independent activity is backlogged.
	// Summary: a wf starts with a pinned override. The first wft schedules an activity before
	// any activity poller on the pinned deployment is seen by the server. The activity is sent
	// to the default queue. Then, the activity poller on the pinned deployment arrives, the task
	// should be now sent to that poller although no current deployment is set on the TQs.

	tv := env.Tv()

	wftCompleted := make(chan struct{})
	env.pollWftAndHandle(s, tv, false, wftCompleted,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return env.respondWftWithActivities(tv, tv, false, vbUnpinned, "5"), nil
		})
	env.waitForDeploymentDataPropagation(s, tv, versionStatusInactive, false, tqTypeWf)

	override := tv.VersioningOverridePinned()
	env.startWorkflow(s, tv, override)

	env.WaitForChannel(wftCompleted)
	env.verifyWorkflowVersioning(s, tv, vbUnpinned, tv.Deployment(), override, nil)
	// Wait long enough to make sure the activity is backlogged.
	env.validateBacklogCount(s, tv, tqTypeAct, 1)

	// When the first activity poller arrives from this deployment, it registers the TQ in the
	// deployment and that will trigger reevaluation of backlog queue.
	env.pollActivityAndHandle(s, tv, nil,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			s.NotNil(task)
			return env.respondActivity(), nil
		})
	env.verifyWorkflowVersioning(s, tv, vbUnpinned, tv.Deployment(), override, nil)
	env.validateBacklogCount(s, tv, tqTypeAct, 0)

	env.pollWftAndHandle(s, tv, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return env.respondCompleteWorkflow(tv, vbUnpinned), nil
		})
	env.verifyWorkflowVersioning(s, tv, vbUnpinned, tv.Deployment(), override, nil)
}

func (s *Versioning3IndependentActivitySuite) TestIndependentVersionedActivity_Pinned() {
	s.testIndependentActivity(vbPinned, false)
}

func (s *Versioning3IndependentActivitySuite) TestIndependentVersionedActivity_Unpinned() {
	s.testIndependentActivity(vbUnpinned, false)
}

func (s *Versioning3IndependentActivitySuite) TestIndependentUnversionedActivity_Pinned() {
	s.testIndependentActivity(vbPinned, true)
}

func (s *Versioning3IndependentActivitySuite) TestIndependentUnversionedActivity_Unpinned() {
	s.testIndependentActivity(vbUnpinned, true)
}

func (s *Versioning3IndependentActivitySuite) testIndependentActivity(behavior enumspb.VersioningBehavior, unversionedActivity bool) {
	env := setupVersioning3Env(s.T())

	// This test starts a wf on wf-series. The workflow runs an activity that is sent to act-tq with
	// workers on a different deployment series, act-series. We make sure that the activity is
	// dispatched and processed properly without affecting versioning of the workflow. Note that it
	// is not required for independent activities to use a different TQ name but in here we test the
	// more common case where the TQ name is different.

	tvWf := env.Tv().WithDeploymentSeriesNumber(1)
	tvAct := env.Tv().WithDeploymentSeriesNumber(2).WithTaskQueueNumber(2)

	// Set current deployment for each TQ
	env.updateTaskQueueDeploymentDataWithRoutingConfig(s, tvWf, &deploymentpb.RoutingConfig{
		CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tvWf.DeploymentVersionString()),
		CurrentVersionChangedTime: timestamp.TimePtr(time.Now()),
		RevisionNumber:            1,
	}, map[string]*deploymentspb.WorkerDeploymentVersionData{tvWf.DeploymentVersion().GetBuildId(): {
		Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
	}}, []string{}, tqTypeWf)

	if !unversionedActivity {
		// Different deployment here for the activity TQ.
		env.updateTaskQueueDeploymentDataWithRoutingConfig(s, tvAct, &deploymentpb.RoutingConfig{
			CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tvAct.DeploymentVersionString()),
			CurrentVersionChangedTime: timestamp.TimePtr(time.Now()),
			RevisionNumber:            1,
		}, map[string]*deploymentspb.WorkerDeploymentVersionData{tvAct.DeploymentVersion().GetBuildId(): {
			Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
		}}, []string{}, tqTypeAct)
	}

	env.startWorkflow(s, tvWf, nil)

	env.pollWftAndHandle(s, tvWf, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			env.verifyWorkflowVersioning(s, tvWf, vbUnspecified, nil, nil, tvWf.DeploymentVersionTransition())
			env.Logger.Info("First wf task completed")
			return env.respondWftWithActivities(tvWf, tvAct, false, behavior, "5"), nil
		})
	env.verifyWorkflowVersioning(s, tvWf, behavior, tvWf.Deployment(), nil, nil)

	if unversionedActivity {
		env.unversionedPollActivityAndHandle(s, tvAct, nil,
			func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
				s.NotNil(task)
				env.Logger.Info("Activity completed")
				return env.respondActivity(), nil
			})
	} else {
		env.pollActivityAndHandle(s, tvAct, nil,
			func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
				s.NotNil(task)
				env.Logger.Info("Activity completed")
				return env.respondActivity(), nil
			})
	}
	env.verifyWorkflowVersioning(s, tvWf, behavior, tvWf.Deployment(), nil, nil)

	env.pollWftAndHandle(s, tvWf, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return env.respondCompleteWorkflow(tvWf, behavior), nil
		})
	env.verifyWorkflowVersioning(s, tvWf, behavior, tvWf.Deployment(), nil, nil)
}
