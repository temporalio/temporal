package cli

import (
	"os"

	tasklistpb "go.temporal.io/temporal-proto/tasklist"
	"go.temporal.io/temporal-proto/workflowservice"

	"github.com/olekukonko/tablewriter"
	"github.com/urfave/cli"
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
	if taskListType == tasklistpb.TaskListType_Activity {
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
	frontendClient := cFactory.FrontendClient(c)
	namespace := getRequiredGlobalOption(c, FlagNamespace)
	taskList := getRequiredOption(c, FlagTaskList)

	ctx, cancel := newContext(c)
	defer cancel()
	request := &workflowservice.ListTaskListPartitionsRequest{
		Namespace: namespace,
		TaskList:  &tasklistpb.TaskList{Name: taskList},
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

func printTaskListPartitions(taskListType string, partitions []*tasklistpb.TaskListPartitionMetadata) {
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
