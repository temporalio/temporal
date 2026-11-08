package tdbg

import (
	"encoding/json"
	"fmt"

	"github.com/urfave/cli/v2"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/api/adminservice/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/codec"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/worker_versioning"
)

// workerDeploymentDescription is the JSON output of AdminDescribeWorkerDeployment.
type workerDeploymentDescription struct {
	RunID        string          `json:"runId"`
	Status       string          `json:"status"`
	VersionCount int             `json:"versionCount"`
	State        json.RawMessage `json:"state"`
}

// AdminDescribeWorkerDeployment prints, as JSON, the Worker Deployment workflow's state decoded from the start
// input of the given (or current) run, along with the run's ID and status and the number of versions in the
// deployment. The input is read from history rather than queried so that it works even when the workflow is stuck.
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
		return fmt.Errorf("no history for Worker Deployment workflow %q", workflowID)
	}
	events, err := serialization.NewSerializer().DeserializeEvents(historyResponse.GetHistoryBatches()[0])
	if err != nil {
		return fmt.Errorf("unable to deserialize Worker Deployment workflow history: %w", err)
	}
	if len(events) == 0 || events[0].GetWorkflowExecutionStartedEventAttributes() == nil {
		return fmt.Errorf("no WorkflowExecutionStarted event for Worker Deployment workflow %q", workflowID)
	}

	var args deploymentspb.WorkerDeploymentWorkflowArgs
	payloads := events[0].GetWorkflowExecutionStartedEventAttributes().GetInput().GetPayloads()
	if len(payloads) == 0 {
		return fmt.Errorf("no input for Worker Deployment workflow %q", workflowID)
	}
	if err := sdk.PreferProtoDataConverter.FromPayload(payloads[0], &args); err != nil {
		return fmt.Errorf("unable to decode Worker Deployment workflow input: %w", err)
	}

	state, err := codec.NewJSONPBEncoder().Encode(&args)
	if err != nil {
		return fmt.Errorf("unable to encode Worker Deployment workflow state: %w", err)
	}
	prettyPrintJSONObject(c, workerDeploymentDescription{
		RunID:        runID,
		Status:       mutableState.GetExecutionState().GetStatus().String(),
		VersionCount: len(args.GetState().GetVersions()),
		State:        state,
	})
	return nil
}
