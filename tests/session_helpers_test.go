package tests

// Helpers shared by the single-session and multi-session functional tests.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// versioningOverrideUpdates are the two ways UpdateWorkflowExecutionOptions can touch the versioning override of an
// unversioned workflow bound to a session worker. Only setting an auto-upgrade override changes its effective
// versioning behavior, so only that is rejected; removing the override it does not have changes nothing.
func versioningOverrideUpdates() []struct {
	name     string
	override *workflowpb.VersioningOverride
	rejected bool
} {
	return []struct {
		name     string
		override *workflowpb.VersioningOverride
		rejected bool
	}{
		{name: "set", override: autoUpgradeOverride(), rejected: true},
		{name: "remove"},
	}
}

// updateVersioningOverride sets the execution's versioning override through UpdateWorkflowExecutionOptions, or removes
// it when override is nil.
func updateVersioningOverride(t *testing.T, env *testcore.TestEnv, execution *commonpb.WorkflowExecution, override *workflowpb.VersioningOverride) error {
	_, err := env.FrontendClient().UpdateWorkflowExecutionOptions(t.Context(), &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:                env.Namespace().String(),
		WorkflowExecution:        execution,
		WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{VersioningOverride: override},
		UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
	})
	return err
}

// requireVersioningOverrideUpdate requires err to be the rejection of an override that would change the version of a
// workflow bound to a session worker when rejected, or no error otherwise.
func requireVersioningOverrideUpdate(t *testing.T, err error, rejected bool) {
	if !rejected {
		require.NoError(t, err)
		return
	}
	var invalidArgument *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArgument)
	require.ErrorContains(t, err, "would change the version of a workflow bound to a worker servicing sessions")
}

func resetStickyTaskQueue(t *testing.T, env *testcore.TestEnv, execution *commonpb.WorkflowExecution) error {
	_, err := env.FrontendClient().ResetStickyTaskQueue(t.Context(), &workflowservice.ResetStickyTaskQueueRequest{
		Namespace: env.Namespace().String(), Execution: execution,
	})
	return err
}

// autoUpgradeOverride is an auto-upgrade versioning override. Unlike a pinned override, it needs no registered version.
func autoUpgradeOverride() *workflowpb.VersioningOverride {
	return &workflowpb.VersioningOverride{Override: &workflowpb.VersioningOverride_AutoUpgrade{AutoUpgrade: true}}
}

// startWithVersioningOverride starts tv's workflow with the override built by override, or with none when it is nil.
func startWithVersioningOverride(t *testing.T, env *testcore.TestEnv, tv *testvars.TestVars, override func(*testvars.TestVars) *workflowpb.VersioningOverride) {
	request := &workflowservice.StartWorkflowExecutionRequest{
		Namespace: env.Namespace().String(), WorkflowId: tv.WorkflowID(), RequestId: tv.Any().String(),
		WorkflowType: tv.WorkflowType(), TaskQueue: tv.TaskQueue(),
		WorkflowRunTimeout: durationpb.New(time.Minute), WorkflowTaskTimeout: durationpb.New(30 * time.Second),
	}
	if override != nil {
		request.VersioningOverride = override(tv)
	}
	_, err := env.FrontendClient().StartWorkflowExecution(t.Context(), request)
	require.NoError(t, err)
}

// requireWorkflowTaskFailedWith requires the last WorkflowTaskFailed event in the execution's history to have cause.
func requireWorkflowTaskFailedWith(t *testing.T, env *testcore.TestEnv, execution *commonpb.WorkflowExecution, cause enumspb.WorkflowTaskFailedCause) {
	var failedCause enumspb.WorkflowTaskFailedCause
	for _, event := range env.GetHistory(env.Namespace().String(), execution) {
		if attributes := event.GetWorkflowTaskFailedEventAttributes(); attributes != nil {
			failedCause = attributes.GetCause()
		}
	}
	require.Equal(t, cause, failedCause)
}

// upgradeOnContinueAsNewCmd continues the workflow as new, asking its next run to upgrade to the current version.
func upgradeOnContinueAsNewCmd(tv *testvars.TestVars) *commandpb.Command {
	command := continueAsNewCmd(tv.WorkflowType(), tv.TaskQueue())
	command.GetContinueAsNewWorkflowExecutionCommandAttributes().InitialVersioningBehavior =
		enumspb.CONTINUE_AS_NEW_VERSIONING_BEHAVIOR_AUTO_UPGRADE
	return command
}
