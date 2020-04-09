package cli

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/urfave/cli"
	executionpb "go.temporal.io/temporal-proto/execution"
	"go.temporal.io/temporal-proto/workflowservice"
	sdkclient "go.temporal.io/temporal/client"

	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/service/worker/batcher"
)

// TerminateBatchJob stops abatch job
func TerminateBatchJob(c *cli.Context) {
	jobID := getRequiredOption(c, FlagJobID)
	reason := getRequiredOption(c, FlagReason)
	client := cFactory.SDKClient(c, common.SystemLocalNamespace)
	tcCtx, cancel := newContext(c)
	defer cancel()
	err := client.TerminateWorkflow(tcCtx, jobID, "", reason, nil)
	if err != nil {
		ErrorAndExit("Failed to terminate batch job", err)
	}
	output := map[string]interface{}{
		"msg": "batch job is terminated",
	}
	prettyPrintJSONObject(output)
}

// DescribeBatchJob describe the status of the batch job
func DescribeBatchJob(c *cli.Context) {
	jobID := getRequiredOption(c, FlagJobID)

	client := cFactory.SDKClient(c, common.SystemLocalNamespace)
	tcCtx, cancel := newContext(c)
	defer cancel()
	wf, err := client.DescribeWorkflowExecution(tcCtx, jobID, "")
	if err != nil {
		ErrorAndExit("Failed to describe batch job", err)
	}

	output := map[string]interface{}{}
	if wf.WorkflowExecutionInfo.GetStatus() != executionpb.WorkflowExecutionStatus_Running {
		if wf.WorkflowExecutionInfo.GetStatus() != executionpb.WorkflowExecutionStatus_Completed {
			output["msg"] = "batch job stopped status: " + wf.WorkflowExecutionInfo.GetStatus().String()
		} else {
			output["msg"] = "batch job is finished successfully"
		}
	} else {
		output["msg"] = "batch job is running"
		if len(wf.PendingActivities) > 0 {
			hbdBinary := wf.PendingActivities[0].HeartbeatDetails
			hbd := batcher.HeartBeatDetails{}
			err := json.Unmarshal(hbdBinary, &hbd)
			if err != nil {
				ErrorAndExit("Failed to describe batch job", err)
			}
			output["progress"] = hbd
		}
	}
	prettyPrintJSONObject(output)
}

// ListBatchJobs list the started batch jobs
func ListBatchJobs(c *cli.Context) {
	namespace := getRequiredGlobalOption(c, FlagNamespace)
	pageSize := c.Int(FlagPageSize)
	client := cFactory.SDKClient(c, common.SystemLocalNamespace)
	tcCtx, cancel := newContext(c)
	defer cancel()
	resp, err := client.ListWorkflow(tcCtx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: common.SystemLocalNamespace,
		PageSize:  int32(pageSize),
		Query:     fmt.Sprintf("CustomNamespace = '%v'", namespace),
	})
	if err != nil {
		ErrorAndExit("Failed to list batch jobs", err)
	}
	output := make([]interface{}, 0, len(resp.Executions))
	for _, wf := range resp.Executions {
		job := map[string]string{
			"jobID":     wf.Execution.GetWorkflowId(),
			"startTime": convertTime(wf.GetStartTime().GetValue(), false),
			"reason":    string(wf.Memo.Fields["Reason"]),
			"operator":  string(wf.SearchAttributes.IndexedFields["Operator"]),
		}

		if wf.GetStatus() != executionpb.WorkflowExecutionStatus_Running {
			job["status"] = wf.GetStatus().String()
			job["closeTime"] = convertTime(wf.GetCloseTime().GetValue(), false)
		} else {
			job["status"] = "RUNNING"
		}

		output = append(output, job)
	}
	prettyPrintJSONObject(output)
}

// StartBatchJob starts a batch job
func StartBatchJob(c *cli.Context) {
	namespace := getRequiredGlobalOption(c, FlagNamespace)
	query := getRequiredOption(c, FlagListQuery)
	reason := getRequiredOption(c, FlagReason)
	batchType := getRequiredOption(c, FlagBatchType)
	if !validateBatchType(batchType) {
		ErrorAndExit("batchType is not valid, supported:"+strings.Join(batcher.AllBatchTypes, ","), nil)
	}
	operator := getCurrentUserFromEnv()
	var sigName, sigVal string
	if batchType == batcher.BatchTypeSignal {
		sigName = getRequiredOption(c, FlagSignalName)
		sigVal = getRequiredOption(c, FlagInput)
	}
	rps := c.Int(FlagRPS)

	client := cFactory.SDKClient(c, common.SystemLocalNamespace)
	tcCtx, cancel := newContext(c)
	defer cancel()
	resp, err := client.CountWorkflow(tcCtx, &workflowservice.CountWorkflowExecutionsRequest{
		Namespace: namespace,
		Query:     query,
	})
	if err != nil {
		ErrorAndExit("Failed to count impacting workflows for starting a batch job", err)
	}
	fmt.Printf("This batch job will be operating on %v workflows.\n", resp.GetCount())
	if !c.Bool(FlagYes) {
		reader := bufio.NewReader(os.Stdin)
		for {
			fmt.Print("Please confirm[Yes/No]:")
			text, err := reader.ReadString('\n')
			if err != nil {
				ErrorAndExit("Failed to  get confirmation for starting a batch job", err)
			}
			if strings.EqualFold(strings.TrimSpace(text), "yes") {
				break
			} else {
				fmt.Println("Batch job is not started")
				return
			}
		}

	}
	tcCtx, cancel = newContext(c)
	defer cancel()
	options := sdkclient.StartWorkflowOptions{
		TaskList:                     batcher.BatcherTaskListName,
		ExecutionStartToCloseTimeout: batcher.InfiniteDuration,
		Memo: map[string]interface{}{
			"Reason": reason,
		},
		SearchAttributes: map[string]interface{}{
			"CustomNamespace": namespace,
			"Operator":        operator,
		},
	}
	params := batcher.BatchParams{
		Namespace: namespace,
		Query:     query,
		Reason:    reason,
		BatchType: batchType,
		SignalParams: batcher.SignalParams{
			SignalName: sigName,
			Input:      sigVal,
		},
		RPS: rps,
	}
	wf, err := client.ExecuteWorkflow(tcCtx, options, batcher.BatchWFTypeName, params)
	if err != nil {
		ErrorAndExit("Failed to start batch job", err)
	}
	output := map[string]interface{}{
		"msg":   "batch job is started",
		"jobID": wf.GetID(),
	}
	prettyPrintJSONObject(output)
}

func validateBatchType(bt string) bool {
	for _, b := range batcher.AllBatchTypes {
		if b == bt {
			return true
		}
	}
	return false
}
