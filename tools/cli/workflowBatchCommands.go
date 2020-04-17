// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cli

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/urfave/cli"
	executionpb "go.temporal.io/temporal-proto/execution"
	"go.temporal.io/temporal-proto/workflowservice"
	sdkclient "go.temporal.io/temporal/client"
	"go.temporal.io/temporal/encoded"

	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/codec"
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
			hbdPayload := wf.PendingActivities[0].HeartbeatDetails
			var hbd batcher.HeartBeatDetails
			err := codec.Decode(hbdPayload, &hbd)
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

	dc := encoded.GetDefaultDataConverter()

	output := make([]interface{}, 0, len(resp.Executions))
	for _, wf := range resp.Executions {
		var reason string
		err = dc.FromData(wf.Memo.Fields["Reason"], &reason)
		if err != nil {
			ErrorAndExit("Failed to deserialize reason memo field", err)
		}

		job := map[string]string{
			"jobID":     wf.Execution.GetWorkflowId(),
			"startTime": convertTime(wf.GetStartTime().GetValue(), false),
			"reason":    reason,
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

	dc := encoded.GetDefaultDataConverter()
	sigInput, err := dc.ToData(sigVal)
	if err != nil {
		ErrorAndExit("Failed to serialize signal value", err)
	}

	params := batcher.BatchParams{
		Namespace: namespace,
		Query:     query,
		Reason:    reason,
		BatchType: batchType,
		SignalParams: batcher.SignalParams{
			SignalName: sigName,
			Input:      sigInput,
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
