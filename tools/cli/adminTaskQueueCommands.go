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
	"fmt"
	"os"

	"github.com/olekukonko/tablewriter"
	"github.com/urfave/cli"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/convert"
)

// AdminDescribeTaskQueue displays poller and status information of task queue.
func AdminDescribeTaskQueue(c *cli.Context) {
	frontendClient := cFactory.FrontendClient(c)
	namespace := getRequiredGlobalOption(c, FlagNamespace)
	taskQueue := getRequiredOption(c, FlagTaskQueue)
	tlTypeInt, err := stringToEnum(c.String(FlagTaskQueueType), enumspb.TaskQueueType_value)
	if err != nil {
		ErrorAndExit("Failed to parse TaskQueue Type", err)
	}
	tlType := enumspb.TaskQueueType(tlTypeInt)
	if tlType == enumspb.TASK_QUEUE_TYPE_UNSPECIFIED {
		ErrorAndExit("TaskQueue type Unspecified is currently not supported", nil)
	}
	ctx, cancel := newContext(c)
	defer cancel()
	request := &workflowservice.DescribeTaskQueueRequest{
		Namespace: namespace,
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		TaskQueueType:          tlType,
		IncludeTaskQueueStatus: true,
	}

	response, err := frontendClient.DescribeTaskQueue(ctx, request)
	if err != nil {
		ErrorAndExit("Operation DescribeTaskQueue failed.", err)
	}

	taskQueueStatus := response.GetTaskQueueStatus()
	if taskQueueStatus == nil {
		ErrorAndExit(colorMagenta("No taskqueue status information."), nil)
	}
	printTaskQueueStatus(taskQueueStatus)
	fmt.Printf("\n")

	pollers := response.Pollers
	printPollerInfo(pollers, tlType)
}

func printTaskQueueStatus(taskQueueStatus *taskqueuepb.TaskQueueStatus) {
	taskIDBlock := taskQueueStatus.GetTaskIdBlock()

	table := tablewriter.NewWriter(os.Stdout)
	table.SetBorder(false)
	table.SetColumnSeparator("|")
	table.SetHeader([]string{"Read Level", "Ack Level", "Backlog", "Lease Start TaskId", "Lease End TaskId"})
	table.SetHeaderLine(false)
	table.SetHeaderColor(tableHeaderBlue, tableHeaderBlue, tableHeaderBlue, tableHeaderBlue, tableHeaderBlue)
	table.Append([]string{convert.Int64ToString(taskQueueStatus.GetReadLevel()),
		convert.Int64ToString(taskQueueStatus.GetAckLevel()),
		convert.Int64ToString(taskQueueStatus.GetBacklogCountHint()),
		convert.Int64ToString(taskIDBlock.GetStartId()),
		convert.Int64ToString(taskIDBlock.GetEndId())})
	table.Render()
}

// AdminListTaskQueueTasks displays task information
func AdminListTaskQueueTasks(c *cli.Context) {
	namespace := getRequiredOption(c, FlagNamespace)
	tqName := getRequiredOption(c, FlagTaskQueue)
	tlTypeInt, err := stringToEnum(c.String(FlagTaskQueueType), enumspb.TaskQueueType_value)
	if err != nil {
		ErrorAndExit("Failed to parse TaskQueue Type", err)
	}
	tqType := enumspb.TaskQueueType(tlTypeInt)
	if tqType == enumspb.TASK_QUEUE_TYPE_UNSPECIFIED {
		ErrorAndExit("TaskQueue type Unspecified is currently not supported", nil)
	}
	minTaskID := c.Int64(FlagMinTaskID)
	maxTaskID := c.Int64(FlagMaxTaskID)
	workflowID := c.String(FlagWorkflowID)
	runID := c.String(FlagRunID)

	client := cFactory.AdminClient(c)

	req := &adminservice.GetTaskQueueTasksRequest{
		Namespace:     namespace,
		TaskQueue:     tqName,
		TaskQueueType: tqType,
		MinTaskId:     minTaskID,
		MaxTaskId:     maxTaskID,
	}

	ctx, cancel := newContext(c)
	defer cancel()
	paginationFunc := func(paginationToken []byte) ([]interface{}, []byte, error) {
		response, err := client.GetTaskQueueTasks(ctx, req)
		if err != nil {
			return nil, nil, err
		}

		tasks := response.Tasks
		if workflowID != "" {
			filteredTasks := tasks[:0]

			for _, task := range tasks {
				if task.Data.WorkflowId != workflowID {
					continue
				}
				if runID != "" && task.Data.RunId != runID {
					continue
				}
				filteredTasks = append(filteredTasks, task)
			}

			tasks = filteredTasks
		}

		var items []interface{}
		for _, task := range tasks {
			items = append(items, task)
		}
		return items, nil, nil
	}

	paginate(c, paginationFunc)
}
