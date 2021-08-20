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
	"os"

	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	"github.com/olekukonko/tablewriter"
	"github.com/urfave/cli"
)

// DescribeTaskQueue show pollers info of a given taskqueue
func DescribeTaskQueue(c *cli.Context) {
	sdkClient := getSDKClient(c)
	taskQueue := getRequiredOption(c, FlagTaskQueue)
	taskQueueType := strToTaskQueueType(c.String(FlagTaskQueueType)) // default type is workflow

	ctx, cancel := newContext(c)
	defer cancel()
	response, err := sdkClient.DescribeTaskQueue(ctx, taskQueue, taskQueueType)
	if err != nil {
		ErrorAndExit("Operation DescribeTaskQueue failed.", err)
	}

	pollers := response.Pollers
	printPollerInfo(pollers, taskQueueType)
}

// ListTaskQueuePartitions gets all the taskqueue partition and host information.
func ListTaskQueuePartitions(c *cli.Context) {
	frontendClient := cFactory.FrontendClient(c)
	namespace := getRequiredGlobalOption(c, FlagNamespace)
	taskQueue := getRequiredOption(c, FlagTaskQueue)

	ctx, cancel := newContext(c)
	defer cancel()
	request := &workflowservice.ListTaskQueuePartitionsRequest{
		Namespace: namespace,
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
	}

	response, err := frontendClient.ListTaskQueuePartitions(ctx, request)
	if err != nil {
		ErrorAndExit("Operation ListTaskQueuePartitions failed.", err)
	}
	if len(response.WorkflowTaskQueuePartitions) > 0 {
		printTaskQueuePartitions("Workflow", response.WorkflowTaskQueuePartitions)
	}
	if len(response.ActivityTaskQueuePartitions) > 0 {
		printTaskQueuePartitions("Activity", response.ActivityTaskQueuePartitions)
	}
}

func printTaskQueuePartitions(taskQueueType string, partitions []*taskqueuepb.TaskQueuePartitionMetadata) {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetBorder(false)
	table.SetColumnSeparator("|")
	table.SetHeader([]string{taskQueueType + "TaskQueuePartition", "Host"})
	table.SetHeaderLine(false)
	table.SetHeaderColor(tableHeaderBlue, tableHeaderBlue)
	for _, partition := range partitions {
		table.Append([]string{partition.GetKey(), partition.GetOwnerHostName()})
	}
	table.Render()
}
