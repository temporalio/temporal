package tdbg

import (
	"fmt"

	"github.com/urfave/cli/v2"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/api/adminservice/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/worker_versioning"
)

// AdminDescribeWorkerDeployment prints the Worker Deployment workflow's state, decoded from the start input
// of the given (or current) run, followed by the number of versions in the deployment. The input is read
// from history rather than queried so that it works even when the workflow is stuck.
func AdminDescribeWorkerDeployment(c *cli.Context, clientFactory ClientFactory) error {
	namespace, err := getRequiredOption(c, FlagNamespace)
	if err != nil {
		return err
	}

	deploymentName, err := getRequiredOption(c, FlagDeploymentName)
	if err != nil {
		return err
	}
	// Built inline rather than via workerdeployment.GenerateDeploymentWorkflowID to keep the
	// worker service's dependency graph out of tdbg.
	workflowID := worker_versioning.WorkerDeploymentWorkflowIDPrefix +
		worker_versioning.WorkerDeploymentVersionDelimiter + deploymentName

	client := clientFactory.AdminClient(c)
	ctx, cancel := newContext(c)
	defer cancel()

	msResponse, err := client.DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      c.String(FlagRunID),
		},
		Archetype: chasm.WorkflowArchetype,
	})
	if err != nil {
		return fmt.Errorf("unable to describe Worker Deployment workflow: %w", err)
	}
	mutableState := msResponse.GetDatabaseMutableState()
	runID := mutableState.GetExecutionState().GetRunId()

	// Both event ID bounds are exclusive, so this fetches only the WorkflowExecutionStarted event.
	historyResponse, err := client.GetWorkflowExecutionRawHistoryV2(ctx, &adminservice.GetWorkflowExecutionRawHistoryV2Request{
		NamespaceId: mutableState.GetExecutionInfo().GetNamespaceId(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		StartEventId:    common.EmptyEventID,
		EndEventId:      common.FirstEventID + 1,
		MaximumPageSize: 1,
	})
	if err != nil {
		return fmt.Errorf("unable to get Worker Deployment workflow history: %w", err)
	}
	if len(historyResponse.GetHistoryBatches()) == 0 {
		return fmt.Errorf("worker deployment workflow %q has no history", workflowID)
	}
	events, err := serialization.NewSerializer().DeserializeEvents(historyResponse.GetHistoryBatches()[0])
	if err != nil {
		return fmt.Errorf("unable to deserialize Worker Deployment workflow history: %w", err)
	}
	if len(events) == 0 || events[0].GetWorkflowExecutionStartedEventAttributes() == nil {
		return fmt.Errorf("worker deployment workflow %q has no WorkflowExecutionStarted event", workflowID)
	}

	var args deploymentspb.WorkerDeploymentWorkflowArgs
	payloads := events[0].GetWorkflowExecutionStartedEventAttributes().GetInput().GetPayloads()
	if len(payloads) == 0 {
		return fmt.Errorf("worker deployment workflow %q has no input", workflowID)
	}
	if err := sdk.PreferProtoDataConverter.FromPayload(payloads[0], &args); err != nil {
		return fmt.Errorf("unable to decode Worker Deployment workflow input: %w", err)
	}

	// nolint:errcheck // assuming that write will succeed.
	fmt.Fprintf(c.App.Writer, "Run ID: %s (%s), state as of the start of this run:\n", runID, mutableState.GetExecutionState().GetStatus())
	prettyPrintJSONObject(c, &args)
	// nolint:errcheck // assuming that write will succeed.
	fmt.Fprintf(c.App.Writer, "Version count: %d\n", len(args.GetState().GetVersions()))
	return nil
}
