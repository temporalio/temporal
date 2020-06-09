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
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
	"github.com/urfave/cli"
	tasklistpb "go.temporal.io/temporal-proto/tasklist"
	"go.temporal.io/temporal-proto/workflowservice"
)

// AdminDescribeTaskList displays poller and status information of task list.
func AdminDescribeTaskList(c *cli.Context) {
	frontendClient := cFactory.FrontendClient(c)
	namespace := getRequiredGlobalOption(c, FlagNamespace)
	taskList := getRequiredOption(c, FlagTaskList)
	taskListType := tasklistpb.TASK_LIST_TYPE_DECISION
	if strings.ToLower(c.String(FlagTaskListType)) == "activity" {
		taskListType = tasklistpb.TASK_LIST_TYPE_ACTIVITY
	}

	ctx, cancel := newContext(c)
	defer cancel()
	request := &workflowservice.DescribeTaskListRequest{
		Namespace:             namespace,
		TaskList:              &tasklistpb.TaskList{Name: taskList},
		TaskListType:          taskListType,
		IncludeTaskListStatus: true,
	}

	response, err := frontendClient.DescribeTaskList(ctx, request)
	if err != nil {
		ErrorAndExit("Operation DescribeTaskList failed.", err)
	}

	taskListStatus := response.GetTaskListStatus()
	if taskListStatus == nil {
		ErrorAndExit(colorMagenta("No tasklist status information."), nil)
	}
	printTaskListStatus(taskListStatus)
	fmt.Printf("\n")

	pollers := response.Pollers
	if len(pollers) == 0 {
		ErrorAndExit(colorMagenta("No poller for tasklist: "+taskList), nil)
	}
	printPollerInfo(pollers, taskListType)
}

func printTaskListStatus(taskListStatus *tasklistpb.TaskListStatus) {
	taskIDBlock := taskListStatus.GetTaskIdBlock()

	table := tablewriter.NewWriter(os.Stdout)
	table.SetBorder(false)
	table.SetColumnSeparator("|")
	table.SetHeader([]string{"Read Level", "Ack Level", "Backlog", "Lease Start TaskId", "Lease End TaskId"})
	table.SetHeaderLine(false)
	table.SetHeaderColor(tableHeaderBlue, tableHeaderBlue, tableHeaderBlue, tableHeaderBlue, tableHeaderBlue)
	table.Append([]string{strconv.FormatInt(taskListStatus.GetReadLevel(), 10),
		strconv.FormatInt(taskListStatus.GetAckLevel(), 10),
		strconv.FormatInt(taskListStatus.GetBacklogCountHint(), 10),
		strconv.FormatInt(taskIDBlock.GetStartId(), 10),
		strconv.FormatInt(taskIDBlock.GetEndId(), 10)})
	table.Render()
}

func printPollerInfo(pollers []*tasklistpb.PollerInfo, taskListType tasklistpb.TaskListType) {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetBorder(false)
	table.SetColumnSeparator("|")
	if taskListType == tasklistpb.TASK_LIST_TYPE_ACTIVITY {
		table.SetHeader([]string{"Activity Poller Identity", "Last Access Time"})
	} else {
		table.SetHeader([]string{"Decision Poller Identity", "Last Access Time"})
	}
	table.SetHeaderLine(false)
	table.SetHeaderColor(tableHeaderBlue, tableHeaderBlue)
	for _, poller := range pollers {
		table.Append([]string{poller.GetIdentity(), convertTime(poller.GetLastAccessTime(), false)})
	}
	table.Render()
}
