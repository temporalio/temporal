package workerdeployment

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testvars"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type continueAsNewTest struct {
	env      *testsuite.TestWorkflowEnvironment
	tv       *testvars.TestVars
	args     *deploymentspb.WorkerDeploymentVersionWorkflowArgs
	syncArgs *deploymentspb.SyncVersionStateUpdateArgs
}

func newContinueAsNewTest(t *testing.T) *continueAsNewTest {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	tv := testvars.New(t)
	now := timestamppb.New(env.Now())
	versionWorkflow := func(ctx workflow.Context, args *deploymentspb.WorkerDeploymentVersionWorkflowArgs) error {
		return VersionWorkflow(ctx,
			func() DeploymentWorkflowVersion { return VersionDataRevisionNumber },
			func() time.Duration { return defaultVisibilityRefresh },
			func() time.Duration { return defaultVisibilityGrace }, args)
	}
	env.RegisterWorkflowWithOptions(versionWorkflow, workflow.RegisterOptions{Name: WorkerDeploymentVersionWorkflowType})
	var a *VersionActivities
	env.OnActivity(a.DescribeWorkerControllerInstanceStatus, mock.Anything, mock.Anything).
		Return(&deploymentpb.ComputeStatus{}, nil).Maybe()
	t.Cleanup(func() { env.AssertExpectations(t) })
	return &continueAsNewTest{
		env: env,
		tv:  tv,
		args: &deploymentspb.WorkerDeploymentVersionWorkflowArgs{
			NamespaceName: tv.NamespaceName().String(),
			NamespaceId:   tv.NamespaceID().String(),
			VersionState: &deploymentspb.VersionLocalState{
				Version: tv.DeploymentVersion(),
				TaskQueueFamilies: map[string]*deploymentspb.VersionLocalState_TaskQueueFamilyData{
					tv.TaskQueue().GetName(): {
						TaskQueues: map[int32]*deploymentspb.TaskQueueVersionData{int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {}},
					},
				},
				SyncBatchSize: 10,
			},
		},
		syncArgs: &deploymentspb.SyncVersionStateUpdateArgs{
			RoutingUpdateTime: now, //nolint:staticcheck // SA1019: required by validateSyncState.
			RoutingConfig: &deploymentpb.RoutingConfig{
				CurrentDeploymentVersion:  tv.ExternalDeploymentVersion(),
				CurrentVersionChangedTime: now,
				RevisionNumber:            5,
			},
		},
	}
}

func mockPropagation(f *continueAsNewTest) (*testsuite.MockCallWrapper, *testsuite.MockCallWrapper) {
	var a *VersionActivities
	syncCall := f.env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, mock.Anything).
		Return(&deploymentspb.SyncDeploymentVersionUserDataResponse{
			TaskQueueMaxVersions: map[string]int64{f.tv.TaskQueue().GetName(): 10},
		}, nil)
	checkCall := f.env.OnActivity(a.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.Anything).Return(nil)
	return syncCall, checkCall
}

func (f *continueAsNewTest) syncState(t *testing.T) {
	t.Helper()
	f.env.UpdateWorkflow(SyncVersionState, "", &testsuite.TestUpdateCallback{
		OnReject:   func(err error) { require.NoError(t, err) },
		OnAccept:   func() {},
		OnComplete: func(_ any, err error) { require.NoError(t, err) },
	}, f.syncArgs)
}

func TestCANWaitsForPropagation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		trigger func(*testsuite.TestWorkflowEnvironment)
	}{
		{name: "state_change"},
		{name: "history_suggested", trigger: func(env *testsuite.TestWorkflowEnvironment) { env.SetContinueAsNewSuggested(true) }},
		{name: "force_nil", trigger: func(env *testsuite.TestWorkflowEnvironment) { env.SignalWorkflow(ForceCANSignalName, nil) }},
		{name: "force_false", trigger: func(env *testsuite.TestWorkflowEnvironment) {
			env.SignalWorkflow(ForceCANSignalName, &deploymentspb.ForceCANVersionSignalArgs{})
		}},
		{name: "force_under_history_pressure", trigger: func(env *testsuite.TestWorkflowEnvironment) {
			env.SetContinueAsNewSuggested(true)
			env.SignalWorkflow(ForceCANSignalName, nil)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, stage := range []string{"sync", "check"} {
				t.Run(stage, func(t *testing.T) {
					t.Parallel()
					f := newContinueAsNewTest(t)
					syncCall, checkCall := mockPropagation(f)
					syncCall.Once()
					checkCall.Once()
					if stage == "sync" {
						syncCall.After(10 * time.Second)
					} else {
						checkCall.After(10 * time.Second)
					}
					f.env.OnSignalExternalWorkflow(mock.Anything,
						GenerateDeploymentWorkflowID(f.tv.DeploymentSeries()), "", PropagationCompleteSignal,
						&deploymentspb.PropagationCompletionInfo{RevisionNumber: 5, BuildId: f.tv.BuildID()}).Return(nil).Once()
					f.env.RegisterDelayedCallback(func() { f.syncState(t) }, 0)
					if tc.trigger != nil {
						f.env.RegisterDelayedCallback(func() { tc.trigger(f.env) }, time.Second)
					}
					observedWaiting := false
					f.env.RegisterDelayedCallback(func() {
						observedWaiting = true
						require.False(t, f.env.IsWorkflowCompleted())
					}, 2*time.Second)

					f.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, f.args)

					require.True(t, observedWaiting)
					require.True(t, workflow.IsContinueAsNewError(f.env.GetWorkflowError()))
				})
			}
		})
	}
}

func TestForceCANSkipsPendingPropagation(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"sync", "check"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			f := newContinueAsNewTest(t)
			syncCall, checkCall := mockPropagation(f)
			syncCall.Once()
			if stage == "sync" {
				syncCall.After(10 * time.Second)
				checkCall.Maybe()
			} else {
				checkCall.After(10 * time.Second).Once()
			}
			f.env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, PropagationCompleteSignal, mock.Anything).
				Return(nil).Maybe()
			f.env.RegisterDelayedCallback(func() { f.syncState(t) }, 0)
			forceSent := false
			f.env.RegisterDelayedCallback(func() {
				forceSent = true
				f.env.SignalWorkflow(ForceCANSignalName, &deploymentspb.ForceCANVersionSignalArgs{SkipPendingPropagations: true})
			}, time.Second)

			f.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, f.args)

			require.True(t, forceSent)
			require.True(t, workflow.IsContinueAsNewError(f.env.GetWorkflowError()))
			f.env.AssertNotCalled(t, "workflow.SignalExternalWorkflow", mock.Anything, mock.Anything, mock.Anything, PropagationCompleteSignal, mock.Anything)
		})
	}
}

func TestLegacyCANBehavior(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		trigger  func(*testsuite.TestWorkflowEnvironment)
		wantWait bool
	}{
		{name: "state_change", wantWait: true},
		{name: "history_suggested", trigger: func(env *testsuite.TestWorkflowEnvironment) { env.SetContinueAsNewSuggested(true) }},
		{name: "force_nil", trigger: func(env *testsuite.TestWorkflowEnvironment) { env.SignalWorkflow(ForceCANSignalName, nil) }},
		{name: "force_false", trigger: func(env *testsuite.TestWorkflowEnvironment) {
			env.SignalWorkflow(ForceCANSignalName, &deploymentspb.ForceCANVersionSignalArgs{})
		}},
		{name: "force_skip", trigger: func(env *testsuite.TestWorkflowEnvironment) {
			env.SignalWorkflow(ForceCANSignalName, &deploymentspb.ForceCANVersionSignalArgs{SkipPendingPropagations: true})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, stage := range []string{"sync", "check"} {
				t.Run(stage, func(t *testing.T) {
					t.Parallel()
					f := newContinueAsNewTest(t)
					f.env.OnGetVersion("wait-for-pending-propagations", workflow.DefaultVersion, 1).Return(workflow.DefaultVersion).Once()
					syncCall, checkCall := mockPropagation(f)
					syncCall.Once()
					if stage == "sync" {
						syncCall.After(10 * time.Second)
						if tc.wantWait {
							checkCall.Once()
						} else {
							checkCall.Maybe()
						}
					} else {
						checkCall.After(10 * time.Second).Once()
					}
					completionCall := f.env.OnSignalExternalWorkflow(mock.Anything,
						GenerateDeploymentWorkflowID(f.tv.DeploymentSeries()), "", PropagationCompleteSignal,
						&deploymentspb.PropagationCompletionInfo{RevisionNumber: 5, BuildId: f.tv.BuildID()}).Return(nil)
					if tc.wantWait {
						completionCall.Once()
					} else {
						completionCall.Maybe()
					}
					f.env.RegisterDelayedCallback(func() { f.syncState(t) }, 0)
					if tc.trigger != nil {
						f.env.RegisterDelayedCallback(func() { tc.trigger(f.env) }, time.Second)
					}
					observedWaiting := false
					f.env.RegisterDelayedCallback(func() {
						observedWaiting = true
						require.False(t, f.env.IsWorkflowCompleted())
					}, 2*time.Second)

					f.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, f.args)

					require.Equal(t, tc.wantWait, observedWaiting)
					require.True(t, workflow.IsContinueAsNewError(f.env.GetWorkflowError()))
					if !tc.wantWait {
						f.env.AssertNotCalled(t, "workflow.SignalExternalWorkflow", mock.Anything, mock.Anything, mock.Anything, PropagationCompleteSignal, mock.Anything)
					}
				})
			}
		})
	}
}

func TestForceCANSkipReplacesWait(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"sync", "check"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			f := newContinueAsNewTest(t)
			syncCall, checkCall := mockPropagation(f)
			syncCall.Once()
			if stage == "sync" {
				syncCall.After(10 * time.Second)
				checkCall.Maybe()
			} else {
				checkCall.After(10 * time.Second).Once()
			}
			f.env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, PropagationCompleteSignal, mock.Anything).
				Return(nil).Maybe()
			f.env.RegisterDelayedCallback(func() { f.syncState(t) }, 0)
			f.env.RegisterDelayedCallback(func() { f.env.SignalWorkflow(ForceCANSignalName, nil) }, time.Second)
			observedWaiting := false
			f.env.RegisterDelayedCallback(func() {
				observedWaiting = true
				require.False(t, f.env.IsWorkflowCompleted())
				f.env.SignalWorkflow(ForceCANSignalName, &deploymentspb.ForceCANVersionSignalArgs{SkipPendingPropagations: true})
			}, 2*time.Second)

			f.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, f.args)

			require.True(t, observedWaiting)
			require.True(t, workflow.IsContinueAsNewError(f.env.GetWorkflowError()))
			f.env.AssertNotCalled(t, "workflow.SignalExternalWorkflow", mock.Anything, mock.Anything, mock.Anything, PropagationCompleteSignal, mock.Anything)
		})
	}
}

func TestForceCANWaitReplacesSkip(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		args *deploymentspb.ForceCANVersionSignalArgs
	}{
		{name: "nil"},
		{name: "false", args: &deploymentspb.ForceCANVersionSignalArgs{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, stage := range []string{"sync", "check"} {
				t.Run(stage, func(t *testing.T) {
					t.Parallel()
					f := newContinueAsNewTest(t)
					syncCall, checkCall := mockPropagation(f)
					syncCall.Once()
					checkCall.Once()
					if stage == "sync" {
						syncCall.After(10 * time.Second)
					} else {
						checkCall.After(10 * time.Second)
					}
					f.env.OnSignalExternalWorkflow(mock.Anything,
						GenerateDeploymentWorkflowID(f.tv.DeploymentSeries()), "", PropagationCompleteSignal,
						&deploymentspb.PropagationCompletionInfo{RevisionNumber: 5, BuildId: f.tv.BuildID()}).Return(nil).Once()
					f.env.RegisterDelayedCallback(func() { f.syncState(t) }, 0)
					f.env.RegisterDelayedCallback(func() {
						f.env.SignalWorkflowSkippingWorkflowTask(ForceCANSignalName, &deploymentspb.ForceCANVersionSignalArgs{SkipPendingPropagations: true})
						f.env.SignalWorkflow(ForceCANSignalName, tc.args)
					}, time.Second)
					observedWaiting := false
					f.env.RegisterDelayedCallback(func() {
						observedWaiting = true
						require.False(t, f.env.IsWorkflowCompleted())
					}, 2*time.Second)

					f.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, f.args)

					require.True(t, observedWaiting)
					require.True(t, workflow.IsContinueAsNewError(f.env.GetWorkflowError()))
				})
			}
		})
	}
}

func TestDeletedVersionWaitsForPropagation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		trigger func(*testsuite.TestWorkflowEnvironment)
	}{
		{name: "force_skip", trigger: func(env *testsuite.TestWorkflowEnvironment) {
			env.SignalWorkflow(ForceCANSignalName, &deploymentspb.ForceCANVersionSignalArgs{SkipPendingPropagations: true})
		}},
		{name: "history_suggested", trigger: func(env *testsuite.TestWorkflowEnvironment) { env.SetContinueAsNewSuggested(true) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, stage := range []string{"sync", "check"} {
				t.Run(stage, func(t *testing.T) {
					t.Parallel()
					f := newContinueAsNewTest(t)
					var a *VersionActivities
					f.env.OnActivity(a.CheckIfTaskQueuesHavePollers, mock.Anything, mock.Anything).Return(false, nil).Once()
					f.env.OnActivity(a.DeleteWorkerControllerInstance, mock.Anything, mock.Anything).Return(nil).Once()
					syncCall, checkCall := mockPropagation(f)
					syncCall.Once()
					checkCall.Once()
					if stage == "sync" {
						syncCall.After(10 * time.Second)
					} else {
						checkCall.After(10 * time.Second)
					}
					f.env.RegisterDelayedCallback(func() {
						f.env.UpdateWorkflow(DeleteVersion, "", &testsuite.TestUpdateCallback{
							OnReject:   func(err error) { require.NoError(t, err) },
							OnAccept:   func() {},
							OnComplete: func(_ any, err error) { require.NoError(t, err) },
						}, &deploymentspb.DeleteVersionArgs{SkipDrainage: true, AsyncPropagation: true})
					}, 0)
					f.env.RegisterDelayedCallback(func() { tc.trigger(f.env) }, time.Second)
					observedWaiting := false
					f.env.RegisterDelayedCallback(func() {
						observedWaiting = true
						require.False(t, f.env.IsWorkflowCompleted())
					}, 2*time.Second)

					f.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, f.args)

					require.True(t, observedWaiting)
					require.True(t, f.env.IsWorkflowCompleted())
					require.NoError(t, f.env.GetWorkflowError())
				})
			}
		})
	}
}

func TestForceCANPreservesOverrideState(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"sync", "check"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			f := newContinueAsNewTest(t)
			syncCall, checkCall := mockPropagation(f)
			syncCall.Once()
			checkCall.Once()
			if stage == "sync" {
				syncCall.After(10 * time.Second)
			} else {
				checkCall.After(10 * time.Second)
			}
			f.env.OnSignalExternalWorkflow(mock.Anything,
				GenerateDeploymentWorkflowID(f.tv.DeploymentSeries()), "", PropagationCompleteSignal,
				&deploymentspb.PropagationCompletionInfo{RevisionNumber: 5, BuildId: f.tv.BuildID()}).Return(nil).Once()
			f.env.RegisterDelayedCallback(func() { f.syncState(t) }, 0)
			override := &deploymentspb.VersionLocalState{Version: f.tv.DeploymentVersion()}
			f.env.RegisterDelayedCallback(func() {
				f.env.SignalWorkflow(ForceCANSignalName, &deploymentspb.ForceCANVersionSignalArgs{OverrideState: override})
			}, time.Second)
			observedWaiting := false
			f.env.RegisterDelayedCallback(func() {
				observedWaiting = true
				require.False(t, f.env.IsWorkflowCompleted())
			}, 2*time.Second)

			f.env.ExecuteWorkflow(WorkerDeploymentVersionWorkflowType, f.args)

			require.True(t, observedWaiting)
			var canErr *workflow.ContinueAsNewError
			require.ErrorAs(t, f.env.GetWorkflowError(), &canErr)
			var nextArgs deploymentspb.WorkerDeploymentVersionWorkflowArgs
			require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(canErr.Input, &nextArgs))
			protorequire.ProtoEqual(t, override, nextArgs.GetVersionState())
		})
	}
}
