package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

type Versioning3OneTimeOverrideSuite struct {
	parallelsuite.Suite[*Versioning3OneTimeOverrideSuite]
}

func TestVersioning3OneTimeOverrideFunctionalSuite(t *testing.T) {
	testcore.UseSuiteScopedCluster(t)                                        //nolint:staticcheck // SA1019: suite still requires legacy sequential execution
	parallelsuite.RunLegacySequential(t, &Versioning3OneTimeOverrideSuite{}) //nolint:staticcheck // SA1019: suite still requires legacy sequential execution
}

func (s *Versioning3OneTimeOverrideSuite) setupEnv(opts ...testcore.TestOption) *VersioningTestEnv {
	return setupVersioning3Env(s.T(), opts...)
}

// TestChildWorkflowExplicitOverrideRoutesToTargetAndClears verifies the
// child-start path for one-time overrides.
// Flow:
// 1. Parent starts on v1.
// 2. Parent starts a child with a one-time override to child v2.
// 3. Assert the child's first WFT routes to v2 while the override is pending.
// 4. Complete that WFT on v2 and assert the one-time override is cleared.
func (s *Versioning3OneTimeOverrideSuite) TestChildWorkflowExplicitOverrideRoutesToTargetAndClears() {
	env := s.setupEnv()

	tv1 := env.Tv().WithBuildIDNumber(1).WithWorkflowIDNumber(1)
	tvChildV1 := tv1.WithWorkflowIDNumber(2)
	tvChildV2 := tvChildV1.WithBuildIDNumber(2)
	childOverride := s.makeOneTimeOverride(tvChildV2)

	env.updateTaskQueueDeploymentDataWithRoutingConfig(s, tv1, &deploymentpb.RoutingConfig{
		CurrentDeploymentVersion:  worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(tv1.DeploymentVersionString()),
		CurrentVersionChangedTime: timestamp.TimePtr(time.Now()),
		RevisionNumber:            1,
	}, map[string]*deploymentspb.WorkerDeploymentVersionData{tv1.DeploymentVersion().GetBuildId(): {
		Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
	}}, []string{}, tqTypeWf)

	runID := env.startWorkflow(s, tv1, tv1.VersioningOverridePinned())
	env.pollUntilRegistered(s, tvChildV2)
	env.validatePinnedVersionExistsInTaskQueue(s, tvChildV2)

	env.pollWftAndHandle(s, tv1, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.Equal(runID, task.GetWorkflowExecution().GetRunId())
			startChildCommand := env.startChildWorkflowCommand(tvChildV1)
			startChildCommand.GetStartChildWorkflowExecutionCommandAttributes().VersioningOverride = childOverride
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{
					startChildCommand,
				},
				VersioningBehavior: vbPinned,
				DeploymentOptions:  tv1.WorkerDeploymentOptions(true),
			}, nil
		})

	parentHistory := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
		WorkflowId: tv1.WorkflowID(),
		RunId:      runID,
	})
	foundInitiated := false
	for _, event := range parentHistory {
		if event.GetEventType() == enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED {
			foundInitiated = true
			s.ProtoEqual(childOverride, event.GetStartChildWorkflowExecutionInitiatedEventAttributes().GetVersioningOverride())
		}
	}
	s.True(foundInitiated)

	var childExecution *commonpb.WorkflowExecution
	env.pollWftAndHandle(s, tvChildV2, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			childExecution = task.GetWorkflowExecution()
			s.Equal(tvChildV1.WorkflowID(), childExecution.GetWorkflowId())
			env.requireOneTimeOverride(s, childExecution, tvChildV2)
			return env.respondEmptyWft(tvChildV2, false, vbPinned), nil
		})

	env.requireNoVersioningOverride(s, childExecution)
	env.verifyWorkflowVersioning(s, tvChildV2, vbPinned, tvChildV2.Deployment(), nil, nil)
}

func (s *Versioning3OneTimeOverrideSuite) makeOneTimeOverride(tv *testvars.TestVars) *workflowpb.VersioningOverride {
	return &workflowpb.VersioningOverride{
		Override: &workflowpb.VersioningOverride_OneTime{
			OneTime: &workflowpb.VersioningOverride_OneTimeOverride{
				TargetDeploymentVersion: tv.ExternalDeploymentVersion(),
			},
		}}
}

// TestTargetWorkflowTaskClearsOverride verifies the core
// one-time override lifecycle. The workflow first runs on version 1, then an
// operator sets a one-time override to version 2. The next WFT must route to
// version 2 while the override is pending. Once that WFT completes from version
// 2, the server should clear the override and keep the workflow on the base
// behavior/version reported by that completion.
func (s *Versioning3OneTimeOverrideSuite) TestTargetWorkflowTaskClearsOverride() {
	env := s.setupEnv()
	tv1 := env.Tv().WithBuildIDNumber(1)
	tv2 := tv1.WithBuildIDNumber(2)

	execution, _ := env.drainWorkflowTaskAfterSetCurrent(s, tv1)
	env.pollUntilRegistered(s, tv2)

	env.updateVersioningOverride(s, execution, s.makeOneTimeOverride(tv2))
	env.requireOneTimeOverride(s, execution, tv2)

	env.triggerNormalWFT(s, tv1, execution)
	env.pollWftAndHandle(s, tv2, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return env.respondEmptyWft(tv2, false, vbPinned), nil
		})

	env.requireNoVersioningOverride(s, execution)
	env.verifyWorkflowVersioning(s, tv1, vbPinned, tv2.Deployment(), nil, nil)
}

// TestPendingWorkflowTaskRoutesToTargetAndClears verifies that
// a one-time override applies to a WFT that was scheduled but not yet started.
// The workflow is started while version 1 is current, leaving its first WFT
// pending. An operator then sets a one-time override to version 2 before any
// worker starts that task. The first WFT should be dispatched to version 2, and
// completion from version 2 should consume and clear the override.
func (s *Versioning3OneTimeOverrideSuite) TestPendingWorkflowTaskRoutesToTargetAndClears() {
	env := s.setupEnv()
	tv1 := env.Tv().WithBuildIDNumber(1)
	tv2 := tv1.WithBuildIDNumber(2)

	env.pollUntilRegistered(s, tv1)
	env.setCurrentDeployment(s, tv1)
	env.pollUntilRegistered(s, tv2)

	runID := env.startWorkflow(s, tv1, nil)
	execution := tv1.WithRunID(runID).WorkflowExecution()

	env.updateVersioningOverride(s, execution, s.makeOneTimeOverride(tv2))
	env.requireOneTimeOverride(s, execution, tv2)

	env.pollWftAndHandle(s, tv2, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return env.respondEmptyWft(tv2, false, vbPinned), nil
		})

	env.requireNoVersioningOverride(s, execution)
	env.verifyWorkflowVersioning(s, tv1, vbPinned, tv2.Deployment(), nil, nil)
}

// TestTargetWorkflowTaskReportsAutoUpgrade verifies that a
// one-time override only controls routing until the target WFT completes. The
// workflow first runs on version 1, then an operator sets a one-time override to
// version 2. The version 2 worker completes that WFT and reports AutoUpgrade,
// so the override should clear and the workflow's base state should become
// AutoUpgrade on version 2. When version 3 later becomes current, the workflow
// should route to version 3 through normal AutoUpgrade routing.
func (s *Versioning3OneTimeOverrideSuite) TestTargetWorkflowTaskReportsAutoUpgrade() {
	env := s.setupEnv()
	tv1 := env.Tv().WithBuildIDNumber(1)
	tv2 := tv1.WithBuildIDNumber(2)
	tv3 := tv1.WithBuildIDNumber(3)

	execution, _ := env.drainWorkflowTaskAfterSetCurrent(s, tv1)
	env.pollUntilRegistered(s, tv2)

	env.updateVersioningOverride(s, execution, s.makeOneTimeOverride(tv2))
	env.requireOneTimeOverride(s, execution, tv2)

	env.triggerNormalWFT(s, tv1, execution)
	env.pollWftAndHandle(s, tv2, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return env.respondEmptyWft(tv2, false, vbUnpinned), nil
		})

	env.requireNoVersioningOverride(s, execution)
	env.verifyWorkflowVersioning(s, tv1, vbUnpinned, tv2.Deployment(), nil, nil)

	env.pollUntilRegistered(s, tv3)
	env.setCurrentDeployment(s, tv3)

	env.triggerNormalWFT(s, tv1, execution)
	env.pollWftAndHandle(s, tv3, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return env.respondEmptyWft(tv3, false, vbUnpinned), nil
		})

	env.verifyWorkflowVersioning(s, tv1, vbUnpinned, tv3.Deployment(), nil, nil)
}

// TestStartedWorkflowTaskOnPreviousVersionDoesNotClear verifies
// the race where an operator sets a one-time override while a WFT from the
// previous version is already started. That old WFT completion should update
// base behavior/version from the old worker, but it must not consume the
// one-time override because it did not complete on the target version. The next
// WFT should still route to the one-time target and clear the override there.
func (s *Versioning3OneTimeOverrideSuite) TestStartedWorkflowTaskOnPreviousVersionDoesNotClear() {
	env := s.setupEnv()
	tv1 := env.Tv().WithBuildIDNumber(1)
	tv2 := tv1.WithBuildIDNumber(2)

	execution, _ := env.drainWorkflowTaskAfterSetCurrent(s, tv1)
	env.pollUntilRegistered(s, tv2)

	env.triggerNormalWFT(s, tv1, execution)
	startedTask := env.pollWorkflowTask(s, tv1)

	env.updateVersioningOverride(s, execution, s.makeOneTimeOverride(tv2))
	env.requireOneTimeOverride(s, execution, tv2)

	env.completeWorkflowTask(s, tv1, startedTask, env.respondEmptyWft(tv1, false, vbUnpinned))

	env.requireOneTimeOverride(s, execution, tv2)
	env.verifyWorkflowVersioning(s, tv1, vbUnpinned, tv1.Deployment(), s.makeOneTimeOverride(tv2), nil)

	env.triggerNormalWFT(s, tv1, execution)
	env.pollWftAndHandle(s, tv2, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return env.respondEmptyWft(tv2, false, vbPinned), nil
		})

	env.requireNoVersioningOverride(s, execution)
	env.verifyWorkflowVersioning(s, tv1, vbPinned, tv2.Deployment(), nil, nil)
}

// TestStartedWorkflowTaskContinueAsNewRejected verifies the
// live race where a WFT is already started on version 1, then an operator sets a
// one-time override to version 2, and the old WFT tries to close with
// Continue-As-New. The options update is a buffered event, so the CAN command
// must be rejected as UnhandledCommand before CAN inheritance can derive state
// from the pending one-time override. The follow-up WFT should then run on
// version 2 and consume the override on the original run.
func (s *Versioning3OneTimeOverrideSuite) TestStartedWorkflowTaskContinueAsNewRejected() {
	env := s.setupEnv()
	tv1 := env.Tv().WithBuildIDNumber(1)
	tv2 := tv1.WithBuildIDNumber(2)

	execution, _ := env.drainWorkflowTaskAfterSetCurrent(s, tv1)
	env.pollUntilRegistered(s, tv2)

	env.triggerNormalWFT(s, tv1, execution)
	startedTask := env.pollWorkflowTask(s, tv1)

	env.updateVersioningOverride(s, execution, s.makeOneTimeOverride(tv2))
	env.requireOneTimeOverride(s, execution, tv2)

	_, err := env.FrontendClient().RespondWorkflowTaskCompleted(s.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: env.Namespace().String(),
		Identity:  tv1.WorkerIdentity(),
		TaskToken: startedTask.GetTaskToken(),
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
					ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
						WorkflowType: tv1.WorkflowType(),
						TaskQueue:    tv1.TaskQueue(),
						Input:        tv1.Any().Payloads(),
					},
				},
			},
		},
		VersioningBehavior: vbPinned,
		DeploymentOptions:  tv1.WorkerDeploymentOptions(true),
	})
	s.Error(err)
	var invalidArgument *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgument)
	s.Equal("UnhandledCommand", err.Error())

	env.requireOneTimeOverride(s, execution, tv2)
	env.pollWftAndHandle(s, tv2, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.Equal(execution.GetRunId(), task.GetWorkflowExecution().GetRunId())
			return env.respondEmptyWft(tv2, false, vbPinned), nil
		})

	env.requireNoVersioningOverride(s, execution)
	env.verifyWorkflowVersioning(s, tv1, vbPinned, tv2.Deployment(), nil, nil)
}

// TestTargetWorkflowTaskContinueAsNewDoesNotInheritOverride
// verifies the CAN boundary after the one-time move has actually happened. The
// workflow routes to version 2 through the one-time override, and that version 2
// WFT completes with Continue-As-New. Since the WFT completed on the target
// version, the override should clear before the new run's versioning state is
// computed. The continued run should not inherit a stale one-time override.
func (s *Versioning3OneTimeOverrideSuite) TestTargetWorkflowTaskContinueAsNewDoesNotInheritOverride() {
	env := s.setupEnv()
	tv1 := env.Tv().WithBuildIDNumber(1)
	tv2 := tv1.WithBuildIDNumber(2)

	execution, _ := env.drainWorkflowTaskAfterSetCurrent(s, tv1)
	env.pollUntilRegistered(s, tv2)

	env.updateVersioningOverride(s, execution, s.makeOneTimeOverride(tv2))
	env.requireOneTimeOverride(s, execution, tv2)

	env.triggerNormalWFT(s, tv1, execution)
	env.pollWftAndHandle(s, tv2, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{
					{
						CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
						Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
							ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
								WorkflowType: tv1.WorkflowType(),
								TaskQueue:    tv1.TaskQueue(),
								Input:        tv1.Any().Payloads(),
							},
						},
					},
				},
				VersioningBehavior: vbPinned,
				DeploymentOptions:  tv2.WorkerDeploymentOptions(true),
			}, nil
		})

	env.pollWftAndHandle(s, tv2, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.NotEqual(execution.GetRunId(), task.GetWorkflowExecution().GetRunId()) // The workflow CAN'ed
			env.requireNoVersioningOverride(s, task.GetWorkflowExecution())
			return env.respondEmptyWft(tv2, false, vbPinned), nil
		})

	currentExecution := &commonpb.WorkflowExecution{WorkflowId: execution.GetWorkflowId()}
	env.requireNoVersioningOverride(s, currentExecution)
	env.verifyWorkflowVersioning(s, tv1, vbPinned, tv2.Deployment(), nil, nil)
}

// TestClearedMoveAllowsUpgradeOnContinueAsNewToNewCurrent
// verifies the main one-time override use case. An operator moves a running
// pinned workflow from version 1 to patched version 2. After one successful WFT
// on version 2, the override clears and version 2 becomes the workflow's base
// pinned version. When version 3 later becomes current, an explicit
// upgrade-on-CAN should not be blocked by the old override; the new run should
// start on version 3 through normal AutoUpgrade initial-versioning behavior.
func (s *Versioning3OneTimeOverrideSuite) TestClearedMoveAllowsUpgradeOnContinueAsNewToNewCurrent() {
	env := s.setupEnv()
	tv1 := env.Tv().WithBuildIDNumber(1)
	tv2 := tv1.WithBuildIDNumber(2)
	tv3 := tv1.WithBuildIDNumber(3)

	execution, _ := env.drainWorkflowTaskAfterSetCurrent(s, tv1)
	env.pollUntilRegistered(s, tv2)

	env.updateVersioningOverride(s, execution, s.makeOneTimeOverride(tv2))
	env.requireOneTimeOverride(s, execution, tv2)

	env.triggerNormalWFT(s, tv1, execution)
	env.pollWftAndHandle(s, tv2, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return env.respondEmptyWft(tv2, false, vbPinned), nil
		})

	env.requireNoVersioningOverride(s, execution)
	env.verifyWorkflowVersioning(s, tv1, vbPinned, tv2.Deployment(), nil, nil)

	env.pollUntilRegistered(s, tv3)
	env.setCurrentDeployment(s, tv3)

	env.triggerNormalWFT(s, tv1, execution)
	env.pollWftAndHandle(s, tv2, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
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
				VersioningBehavior: vbPinned,
				DeploymentOptions:  tv2.WorkerDeploymentOptions(true),
			}, nil
		})

	var newRunID string
	env.pollWftAndHandle(s, tv3, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			newRunID = task.GetWorkflowExecution().GetRunId()
			s.NotEqual(execution.GetRunId(), newRunID)
			env.requireNoVersioningOverride(s, task.GetWorkflowExecution())
			return env.respondCompleteWorkflow(tv3, vbPinned), nil
		})
	s.NotEmpty(newRunID)

	currentExecution := &commonpb.WorkflowExecution{WorkflowId: execution.GetWorkflowId()}
	env.requireNoVersioningOverride(s, currentExecution)
	env.verifyWorkflowVersioning(s, tv3, vbPinned, tv3.Deployment(), nil, nil)
}

// TestResetReappliesPendingMove verifies reset/reapply
// semantics for a consumed one-time override. The original run moves from
// version 1 to version 2 and consumes the one-time override. Reset then goes
// back to a point before the override was set. Default reset reapply should
// reapply the WorkflowExecutionOptionsUpdated event that set the one-time
// override, so the reset run gets one pending WFT routed to version 2.
func (s *Versioning3OneTimeOverrideSuite) TestResetReappliesPendingMove() {
	env := s.setupEnv()
	tv1 := env.Tv().WithBuildIDNumber(1)
	tv2 := tv1.WithBuildIDNumber(2)

	execution, _ := env.drainWorkflowTaskAfterSetCurrent(s, tv1)
	env.pollUntilRegistered(s, tv2)

	var resetEventID int64
	for _, event := range env.GetHistory(env.Namespace().String(), execution) {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			resetEventID = event.GetEventId() // just capturing the resetID here so that we come back to this event when we actually do the reset!
			break
		}
	}
	s.NotZero(resetEventID)

	env.updateVersioningOverride(s, execution, s.makeOneTimeOverride(tv2))
	env.requireOneTimeOverride(s, execution, tv2)

	env.triggerNormalWFT(s, tv1, execution)
	env.pollWftAndHandle(s, tv2, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			return env.respondEmptyWft(tv2, false, vbPinned), nil
		})
	env.requireNoVersioningOverride(s, execution)
	env.verifyWorkflowVersioning(s, tv1, vbPinned, tv2.Deployment(), nil, nil)

	resetResp, err := env.FrontendClient().ResetWorkflowExecution(s.Context(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: execution.GetWorkflowId(),
			RunId:      execution.GetRunId(),
		},
		Reason:                    "reset before one-time override",
		WorkflowTaskFinishEventId: resetEventID,
		RequestId:                 uuid.NewString(),
	})
	s.NoError(err)

	resetExecution := &commonpb.WorkflowExecution{
		WorkflowId: execution.GetWorkflowId(),
		RunId:      resetResp.GetRunId(),
	}
	env.requireOneTimeOverride(s, resetExecution, tv2)

	env.pollWftAndHandle(s, tv2, false, nil,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.NotNil(task)
			s.Equal(resetResp.GetRunId(), task.GetWorkflowExecution().GetRunId())
			return env.respondEmptyWft(tv2, false, vbPinned), nil
		})

	env.requireNoVersioningOverride(s, resetExecution)
	env.verifyWorkflowVersioning(s, tv1, vbPinned, tv2.Deployment(), nil, nil)
}

// TestSameNamespaceChildInheritsPendingOverride verifies the
// child-workflow boundary when a one-time override is pending. A parent WFT is
// already started on version 1, then an operator sets a one-time override to
// version 2. That old parent WFT starts a same-namespace child. The parent WFT
// itself must not consume the one-time override because it completed on version
// 1, but the child should inherit the pending override, route its first WFT to
// version 2, and clear the override after that first child WFT completes.
func (s *Versioning3OneTimeOverrideSuite) TestSameNamespaceChildInheritsPendingOverride() {
	env := s.setupEnv()
	tv1 := env.Tv().WithBuildIDNumber(1)
	tv2 := tv1.WithBuildIDNumber(2)
	childTV1 := tv1.WithWorkflowIDNumber(2)
	childTV2 := childTV1.WithBuildIDNumber(2)

	execution, _ := env.drainWorkflowTaskAfterSetCurrent(s, tv1)
	env.pollUntilRegistered(s, tv2)

	env.triggerNormalWFT(s, tv1, execution)
	startedTask := env.pollWorkflowTask(s, tv1)

	env.updateVersioningOverride(s, execution, s.makeOneTimeOverride(tv2))
	env.requireOneTimeOverride(s, execution, tv2)

	env.completeWorkflowTask(s, tv1, startedTask, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Commands: []*commandpb.Command{
			env.startChildWorkflowCommand(childTV1),
		},
		VersioningBehavior: vbPinned,
		DeploymentOptions:  tv1.WorkerDeploymentOptions(true),
	})
	env.requireOneTimeOverride(s, execution, tv2)

	childExecution := env.pollUntilChildWorkflowTask(s, tv2, childTV1.WorkflowID(),
		func(task *workflowservice.PollWorkflowTaskQueueResponse) *workflowservice.RespondWorkflowTaskCompletedRequest {
			env.requireOneTimeOverride(s, task.GetWorkflowExecution(), childTV2)
			return env.respondEmptyWft(tv2, false, vbPinned)
		})

	env.requireNoVersioningOverride(s, childExecution)
	env.verifyWorkflowVersioning(s, childTV2, vbPinned, childTV2.Deployment(), nil, nil)
}

// TestCrossTaskQueueChildInheritsOnlyWhenTargetOwnsTaskQueue
// verifies the child task-queue ownership boundary. If the one-time target
// version is registered on the child's task queue, the child inherits the
// pending one-time override and routes its first WFT to the target. If the
// target version is not registered on the child's task queue, the child does not
// inherit the parent override and starts through the child task queue's own
// current routing.
func (s *Versioning3OneTimeOverrideSuite) TestCrossTaskQueueChildInheritsOnlyWhenTargetOwnsTaskQueue() {
	for _, tc := range []struct {
		name                          string
		targetPresentInChildTaskQueue bool
	}{
		{
			name:                          "target present in child task queue",
			targetPresentInChildTaskQueue: true,
		},
		{
			name:                          "target not present in child task queue",
			targetPresentInChildTaskQueue: false,
		},
	} {
		s.Run(tc.name, func(s *Versioning3OneTimeOverrideSuite) {
			env := s.setupEnv()
			tv1 := env.Tv().WithBuildIDNumber(1)
			tv2 := tv1.WithBuildIDNumber(2)
			childTV1 := tv1.WithWorkflowIDNumber(2).WithTaskQueueNumber(2)
			childTV2 := childTV1.WithBuildIDNumber(2)

			execution, _ := env.drainWorkflowTaskAfterSetCurrent(s, tv1)
			env.pollUntilRegistered(s, tv2)
			env.pollUntilRegistered(s, childTV1)
			env.waitForDeploymentDataPropagation(s, childTV1, versionStatusCurrent, false, tqTypeWf)
			if tc.targetPresentInChildTaskQueue {
				env.pollUntilRegistered(s, childTV2)
				env.waitForDeploymentDataPropagation(s, childTV2, versionStatusInactive, false, tqTypeWf)
			}

			env.triggerNormalWFT(s, tv1, execution)
			startedTask := env.pollWorkflowTask(s, tv1)

			env.updateVersioningOverride(s, execution, s.makeOneTimeOverride(tv2))
			env.requireOneTimeOverride(s, execution, tv2)

			env.completeWorkflowTask(s, tv1, startedTask, &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{
					env.startChildWorkflowCommand(childTV1),
				},
				VersioningBehavior: vbPinned,
				DeploymentOptions:  tv1.WorkerDeploymentOptions(true),
			})

			if tc.targetPresentInChildTaskQueue {
				env.pollWftAndHandle(s, childTV2, false, nil,
					func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
						s.NotNil(task)
						s.Equal(childTV1.WorkflowID(), task.GetWorkflowExecution().GetWorkflowId())
						env.requireOneTimeOverride(s, task.GetWorkflowExecution(), childTV2)
						return env.respondEmptyWft(childTV2, false, vbPinned), nil
					})

				env.requireNoVersioningOverride(s, childTV1.WorkflowExecution())
				env.verifyWorkflowVersioning(s, childTV2, vbPinned, childTV2.Deployment(), nil, nil)
			} else {
				env.pollWftAndHandle(s, childTV1, false, nil,
					func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
						s.NotNil(task)
						s.Equal(childTV1.WorkflowID(), task.GetWorkflowExecution().GetWorkflowId())
						env.requireNoVersioningOverride(s, task.GetWorkflowExecution())
						return env.respondEmptyWft(childTV1, false, vbPinned), nil
					})

				env.verifyWorkflowVersioning(s, childTV1, vbPinned, childTV1.Deployment(), nil, nil)
			}
		})
	}
}

// TestInvalidTargetVersionRejected verifies that the operator
// cannot set a one-time override to a version that is not registered on the
// workflow's task queue. The update should fail before persistence, leaving the
// workflow without a pending override.
func (s *Versioning3OneTimeOverrideSuite) TestInvalidTargetVersionRejected() {
	env := s.setupEnv()
	tv1 := env.Tv().WithBuildIDNumber(1)
	missingTV := tv1.WithBuildIDNumber(2)

	execution, _ := env.drainWorkflowTaskAfterSetCurrent(s, tv1)

	_, err := env.FrontendClient().UpdateWorkflowExecutionOptions(s.Context(), &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                env.Namespace().String(),
		WorkflowExecution:        execution,
		WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{VersioningOverride: s.makeOneTimeOverride(missingTV)},
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
	})
	s.Error(err)
	var failedPrecondition *serviceerror.FailedPrecondition
	s.ErrorAs(err, &failedPrecondition)
	s.Contains(err.Error(), worker_versioning.ErrPinnedVersionNotInTaskQueueSubstring)
	env.requireNoVersioningOverride(s, execution)
}
