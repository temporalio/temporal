// Copyright (c) 2017 Uber Technologies, Inc.
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

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"

	"github.com/olekukonko/tablewriter"
	"github.com/urfave/cli"
	s "go.uber.org/cadence/.gen/go/shared"
)

// DescribeTaskList show pollers info of a given tasklist
func DescribeTaskList(c *cli.Context) {
	wfClient := getWorkflowClient(c)
	taskList := getRequiredOption(c, FlagTaskList)
	taskListType := strToTaskListType(c.String(FlagTaskListType)) // default type is decision

	ctx, cancel := newContext(c)
	defer cancel()
	response, err := wfClient.DescribeTaskList(ctx, taskList, taskListType)
	if err != nil {
		ErrorAndExit("Operation DescribeTaskList failed.", err)
	}

	pollers := response.Pollers
	if len(pollers) == 0 {
		ErrorAndExit(colorMagenta("No poller for tasklist: "+taskList), nil)
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetBorder(false)
	table.SetColumnSeparator("|")
	if taskListType == s.TaskListTypeActivity {
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

// ListTaskListPartitions gets all the tasklist partition and host information.
func ListTaskListPartitions(c *cli.Context) {
	frontendClient := cFactory.ServerFrontendClient(c)
	domain := getRequiredGlobalOption(c, FlagDomain)
	taskList := getRequiredOption(c, FlagTaskList)

	ctx, cancel := newContext(c)
	defer cancel()
	request := &shared.ListTaskListPartitionsRequest{
		Domain:   common.StringPtr(domain),
		TaskList: &shared.TaskList{Name: common.StringPtr(taskList)},
	}

	response, err := frontendClient.ListTaskListPartitions(ctx, request)
	if err != nil {
		ErrorAndExit("Operation ListTaskListPartitions failed.", err)
	}
	if len(response.DecisionTaskListPartitions) > 0 {
		printTaskListPartitions("Decision", response.DecisionTaskListPartitions)
	}
	if len(response.ActivityTaskListPartitions) > 0 {
		printTaskListPartitions("Activity", response.ActivityTaskListPartitions)
	}
}

func printTaskListPartitions(taskListType string, partitions []*shared.TaskListPartitionMetadata) {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetBorder(false)
	table.SetColumnSeparator("|")
	table.SetHeader([]string{taskListType + "TaskListPartition", "Host"})
	table.SetHeaderLine(false)
	table.SetHeaderColor(tableHeaderBlue, tableHeaderBlue)
	for _, partition := range partitions {
		table.Append([]string{partition.GetKey(), partition.GetOwnerHostName()})
	}
	table.Render()
}
