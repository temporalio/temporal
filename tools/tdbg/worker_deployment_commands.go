package tdbg

import (
	"fmt"
	"slices"
	"strings"

	"github.com/urfave/cli/v2"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/service/worker/workerdeployment"
)

// AdminDescribeWorkerDeployment prints the Worker Deployment workflow's state as JSON, decoded from the
// start input of the given (or current) run, followed by the number of versions in the deployment.
// The input is read from history rather than queried so that it works even when the workflow is stuck.
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
	workflowID := workerdeployment.GenerateDeploymentWorkflowID(deploymentName)

	ctx, cancel := newContext(c)
	defer cancel()
	resp, err := clientFactory.WorkflowClient(c).GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: nsName,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      c.String(FlagRunID),
		},
		MaximumPageSize: 1,
	})
	if err != nil {
		return fmt.Errorf("unable to get history of worker deployment workflow %q: %w", workflowID, err)
	}

	events := resp.GetHistory().GetEvents()
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

	prettyPrintJSONObject(c, &args)
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
	statuses := make([]enumspb.WorkerDeploymentVersionStatus, 0, len(countByStatus))
	for s := range countByStatus {
		statuses = append(statuses, s)
	}
	slices.Sort(statuses)
	parts := make([]string, 0, len(statuses))
	for _, s := range statuses {
		parts = append(parts, fmt.Sprintf("%s: %d", s, countByStatus[s]))
	}
	return fmt.Sprintf("%d (%s)", len(versions), strings.Join(parts, ", "))
}
