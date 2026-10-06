package tdbg

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/urfave/cli/v2"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/adminservice/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/codec"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/worker_versioning"
)

// AdminDescribeWorkerDeployment prints the Worker Deployment workflow's state as JSON, decoded from the
// start input of the given (or current) run, followed by the number of versions in the deployment.
// The input is read from history rather than queried so that it works even when the workflow is stuck,
// at the cost of not reflecting changes made since the run started.
// CONSIDER(veeral): add a --live option that uses the describe-deployment query instead.
func AdminDescribeWorkerDeployment(c *cli.Context, clientFactory ClientFactory) error {
	nsName, err := getRequiredOption(c, FlagNamespace)
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

	adminClient := clientFactory.AdminClient(c)
	ctx, cancel := newContext(c)
	defer cancel()

	msResp, err := adminClient.DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: nsName,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      c.String(FlagRunID),
		},
		Archetype: chasm.WorkflowArchetype,
	})
	if err != nil {
		return fmt.Errorf("unable to describe worker deployment workflow %q: %w", workflowID, err)
	}
	executionState := msResp.GetDatabaseMutableState().GetExecutionState()
	runID := executionState.GetRunId()

	nsID, err := getNamespaceID(c, clientFactory, namespace.Name(nsName))
	if err != nil {
		return fmt.Errorf("unable to get namespace ID of %q: %w", nsName, err)
	}
	// Both event ID bounds are exclusive, so this fetches only the WorkflowExecutionStarted event.
	historyResp, err := adminClient.GetWorkflowExecutionRawHistoryV2(ctx, &adminservice.GetWorkflowExecutionRawHistoryV2Request{
		NamespaceId: nsID.String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		StartEventId:    common.EmptyEventID,
		EndEventId:      common.FirstEventID + 1,
		MaximumPageSize: 1,
	})
	if err != nil {
		return fmt.Errorf("unable to get history of worker deployment workflow %q: %w", workflowID, err)
	}
	batches := historyResp.GetHistoryBatches()
	if len(batches) == 0 {
		return fmt.Errorf("worker deployment workflow %q has no history events", workflowID)
	}
	events, err := serialization.NewSerializer().DeserializeEvents(batches[0])
	if err != nil {
		return fmt.Errorf("unable to deserialize history of worker deployment workflow %q: %w", workflowID, err)
	}
	if len(events) == 0 {
		return fmt.Errorf("worker deployment workflow %q has no history events", workflowID)
	}
	startedAttrs := events[0].GetWorkflowExecutionStartedEventAttributes()
	if startedAttrs == nil {
		return fmt.Errorf("first event of worker deployment workflow %q is %v, expected WorkflowExecutionStarted",
			workflowID, events[0].GetEventType())
	}
	payloads := startedAttrs.GetInput().GetPayloads()
	if len(payloads) == 0 {
		return fmt.Errorf("worker deployment workflow %q has no input", workflowID)
	}

	var args deploymentspb.WorkerDeploymentWorkflowArgs
	if err := sdk.PreferProtoDataConverter.FromPayload(payloads[0], &args); err != nil {
		return fmt.Errorf("unable to decode input of worker deployment workflow %q: %w", workflowID, err)
	}
	stateJSON, err := codec.NewJSONPBIndentEncoder("  ").Encode(&args)
	if err != nil {
		return fmt.Errorf("unable to encode state of worker deployment workflow %q: %w", workflowID, err)
	}

	_, _ = fmt.Fprintf(c.App.Writer, "Run ID:     %s\n", runID)
	_, _ = fmt.Fprintf(c.App.Writer, "Start time: %s\n", executionState.GetStartTime().AsTime())
	_, _ = fmt.Fprintf(c.App.Writer, "Status:     %s\n", executionState.GetStatus())
	_, _ = fmt.Fprintln(c.App.Writer, "State as of the start of this run (changes made during the run are not included):")
	_, _ = fmt.Fprintln(c.App.Writer, string(stateJSON))
	_, _ = fmt.Fprintf(c.App.Writer, "Version count: %s\n", formatVersionCount(args.GetState().GetVersions()))
	return nil
}

// formatVersionCount returns the total number of versions followed by a per-status breakdown,
// e.g. "4 (Current: 1, Drained: 3)".
func formatVersionCount(versions map[string]*deploymentspb.WorkerDeploymentVersionSummary) string {
	if len(versions) == 0 {
		return "0"
	}
	countByStatus := make(map[enumspb.WorkerDeploymentVersionStatus]int)
	for _, v := range versions {
		countByStatus[v.GetStatus()]++
	}
	parts := make([]string, 0, len(countByStatus))
	for _, s := range slices.Sorted(maps.Keys(countByStatus)) {
		parts = append(parts, fmt.Sprintf("%s: %d", s, countByStatus[s]))
	}
	return fmt.Sprintf("%d (%s)", len(versions), strings.Join(parts, ", "))
}
