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

import "github.com/urfave/cli"

func newWorkflowCommands() []cli.Command {
	return []cli.Command{
		{
			Name:  "show",
			Usage: "show workflow history",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  FlagWorkflowIDWithAlias,
					Usage: "WorkflowID",
				},
				cli.StringFlag{
					Name:  FlagRunIDWithAlias,
					Usage: "RunID",
				},
				cli.BoolFlag{
					Name:  FlagPrintDateTimeWithAlias,
					Usage: "Print time stamp",
				},
				cli.BoolFlag{
					Name:  FlagPrintEventVersionWithAlias,
					Usage: "Print event version",
				},
				cli.BoolFlag{
					Name:  FlagPrintFullyDetailWithAlias,
					Usage: "Print fully event detail",
				},
				cli.BoolFlag{
					Name:  FlagPrintRawTimeWithAlias,
					Usage: "Print raw time stamp",
				},
				cli.StringFlag{
					Name:  FlagOutputFilenameWithAlias,
					Usage: "Serialize history event to a file",
				},
				cli.IntFlag{
					Name:  FlagEventIDWithAlias,
					Usage: "Print specific event details",
				},
				cli.IntFlag{
					Name:  FlagMaxFieldLengthWithAlias,
					Usage: "Maximum length for each attribute field",
					Value: defaultMaxFieldLength,
				},
			},
			Action: func(c *cli.Context) {
				ShowHistory(c)
			},
		},
		{
			Name:        "showid",
			Usage:       "show workflow history with given workflow_id and optional run_id (a shortcut of `show -w <wid> -r <rid>`)",
			Description: "cadence workflow showid <workflow_id> <run_id>. workflow_id is required; run_id is optional",
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  FlagPrintDateTimeWithAlias,
					Usage: "Print time stamp",
				},
				cli.BoolFlag{
					Name:  FlagPrintRawTimeWithAlias,
					Usage: "Print raw time stamp",
				},
				cli.StringFlag{
					Name:  FlagOutputFilenameWithAlias,
					Usage: "Serialize history event to a file",
				},
				cli.BoolFlag{
					Name:  FlagPrintFullyDetailWithAlias,
					Usage: "Print fully event detail",
				},
				cli.BoolFlag{
					Name:  FlagPrintEventVersionWithAlias,
					Usage: "Print event version",
				},
				cli.IntFlag{
					Name:  FlagEventIDWithAlias,
					Usage: "Print specific event details",
				},
				cli.IntFlag{
					Name:  FlagMaxFieldLengthWithAlias,
					Usage: "Maximum length for each attribute field",
					Value: defaultMaxFieldLength,
				},
			},
			Action: func(c *cli.Context) {
				ShowHistoryWithWID(c)
			},
		},
		{
			Name:  "start",
			Usage: "start a new workflow execution",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  FlagTaskListWithAlias,
					Usage: "TaskList",
				},
				cli.StringFlag{
					Name:  FlagWorkflowIDWithAlias,
					Usage: "WorkflowID",
				},
				cli.StringFlag{
					Name:  FlagWorkflowTypeWithAlias,
					Usage: "WorkflowTypeName",
				},
				cli.IntFlag{
					Name:  FlagExecutionTimeoutWithAlias,
					Usage: "Execution start to close timeout in seconds",
				},
				cli.IntFlag{
					Name:  FlagDecisionTimeoutWithAlias,
					Value: defaultDecisionTimeoutInSeconds,
					Usage: "Decision task start to close timeout in seconds",
				},
				cli.StringFlag{
					Name:  FlagInputWithAlias,
					Usage: "Optional input for the workflow, in JSON format. If there are multiple parameters, concatenate them and separate by space.",
				},
				cli.StringFlag{
					Name: FlagInputFileWithAlias,
					Usage: "Optional input for the workflow from JSON file. If there are multiple JSON, concatenate them and separate by space or newline. " +
						"Input from file will be overwrite by input from command line",
				},
			},
			Action: func(c *cli.Context) {
				StartWorkflow(c)
			},
		},
		{
			Name:  "run",
			Usage: "start a new workflow execution and get workflow progress",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  FlagTaskListWithAlias,
					Usage: "TaskList",
				},
				cli.StringFlag{
					Name:  FlagWorkflowIDWithAlias,
					Usage: "WorkflowID",
				},
				cli.StringFlag{
					Name:  FlagWorkflowTypeWithAlias,
					Usage: "WorkflowTypeName",
				},
				cli.IntFlag{
					Name:  FlagExecutionTimeoutWithAlias,
					Usage: "Execution start to close timeout in seconds",
				},
				cli.IntFlag{
					Name:  FlagDecisionTimeoutWithAlias,
					Value: defaultDecisionTimeoutInSeconds,
					Usage: "Decision task start to close timeout in seconds",
				},
				cli.IntFlag{
					Name:  FlagContextTimeoutWithAlias,
					Usage: "Optional timeout for start command context in seconds, default value is 120",
				},
				cli.StringFlag{
					Name:  FlagInputWithAlias,
					Usage: "Optional input for the workflow, in JSON format. If there are multiple parameters, concatenate them and separate by space.",
				},
				cli.StringFlag{
					Name: FlagInputFileWithAlias,
					Usage: "Optional input for the workflow from JSON file. If there are multiple JSON, concatenate them and separate by space or newline. " +
						"Input from file will be overwrite by input from command line",
				},
				cli.BoolFlag{
					Name:  FlagShowDetailWithAlias,
					Usage: "Show event details",
				},
				cli.IntFlag{
					Name:  FlagMaxFieldLengthWithAlias,
					Usage: "Maximum length for each attribute field",
				},
			},
			Action: func(c *cli.Context) {
				RunWorkflow(c)
			},
		},
		{
			Name:    "cancel",
			Aliases: []string{"c"},
			Usage:   "cancel a workflow execution",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  FlagWorkflowIDWithAlias,
					Usage: "WorkflowID",
				},
				cli.StringFlag{
					Name:  FlagRunIDWithAlias,
					Usage: "RunID",
				},
			},
			Action: func(c *cli.Context) {
				CancelWorkflow(c)
			},
		},
		{
			Name:    "signal",
			Aliases: []string{"s"},
			Usage:   "signal a workflow execution",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  FlagWorkflowIDWithAlias,
					Usage: "WorkflowID",
				},
				cli.StringFlag{
					Name:  FlagRunIDWithAlias,
					Usage: "RunID",
				},
				cli.StringFlag{
					Name:  FlagNameWithAlias,
					Usage: "SignalName",
				},
				cli.StringFlag{
					Name:  FlagInputWithAlias,
					Usage: "Input for the signal, in JSON format.",
				},
				cli.StringFlag{
					Name:  FlagInputFileWithAlias,
					Usage: "Input for the signal from JSON file.",
				},
			},
			Action: func(c *cli.Context) {
				SignalWorkflow(c)
			},
		},
		{
			Name:    "terminate",
			Aliases: []string{"term"},
			Usage:   "terminate a new workflow execution",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  FlagWorkflowIDWithAlias,
					Usage: "WorkflowID",
				},
				cli.StringFlag{
					Name:  FlagRunIDWithAlias,
					Usage: "RunID",
				},
				cli.StringFlag{
					Name:  FlagReasonWithAlias,
					Usage: "The reason you want to terminate the workflow",
				},
			},
			Action: func(c *cli.Context) {
				TerminateWorkflow(c)
			},
		},
		{
			Name:        "list",
			Aliases:     []string{"l"},
			Usage:       "list open or closed workflow executions",
			Description: "list one page (default size 10 items) by default, use flag --pagesize to change page size",
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  FlagOpenWithAlias,
					Usage: "List for open workflow executions, default is to list for closed ones",
				},
				cli.BoolFlag{
					Name:  FlagMoreWithAlias,
					Usage: "List more pages, default is to list one page of default page size 10",
				},
				cli.IntFlag{
					Name:  FlagPageSizeWithAlias,
					Value: 10,
					Usage: "Result page size",
				},
				cli.StringFlag{
					Name:  FlagEarliestTimeWithAlias,
					Usage: "EarliestTime of start time, supported formats are '2006-01-02T15:04:05Z07:00' and raw UnixNano",
				},
				cli.StringFlag{
					Name:  FlagLatestTimeWithAlias,
					Usage: "LatestTime of start time, supported formats are '2006-01-02T15:04:05Z07:00' and raw UnixNano",
				},
				cli.StringFlag{
					Name:  FlagWorkflowIDWithAlias,
					Usage: "WorkflowID",
				},
				cli.StringFlag{
					Name:  FlagWorkflowTypeWithAlias,
					Usage: "WorkflowTypeName",
				},
				cli.BoolFlag{
					Name:  FlagPrintRawTimeWithAlias,
					Usage: "Print raw time stamp",
				},
				cli.BoolFlag{
					Name:  FlagPrintDateTimeWithAlias,
					Usage: "Print full date time in '2006-01-02T15:04:05Z07:00' format",
				},
				cli.IntFlag{
					Name:  FlagContextTimeoutWithAlias,
					Value: 30,
					Usage: "Optional timeout for list command context in seconds",
				},
			},
			Action: func(c *cli.Context) {
				ListWorkflow(c)
			},
		},
		{
			Name:    "listall",
			Aliases: []string{"la"},
			Usage:   "list all open or closed workflow executions",
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  FlagOpenWithAlias,
					Usage: "List for open workflow executions, default is to list for closed ones",
				},
				cli.StringFlag{
					Name:  FlagEarliestTimeWithAlias,
					Usage: "EarliestTime of start time, supported formats are '2006-01-02T15:04:05Z07:00' and raw UnixNano",
				},
				cli.StringFlag{
					Name:  FlagLatestTimeWithAlias,
					Usage: "LatestTime of start time, supported formats are '2006-01-02T15:04:05Z07:00' and raw UnixNano",
				},
				cli.StringFlag{
					Name:  FlagWorkflowIDWithAlias,
					Usage: "WorkflowID",
				},
				cli.StringFlag{
					Name:  FlagWorkflowTypeWithAlias,
					Usage: "WorkflowTypeName",
				},
				cli.BoolFlag{
					Name:  FlagPrintRawTimeWithAlias,
					Usage: "Print raw time stamp",
				},
				cli.BoolFlag{
					Name:  FlagPrintDateTimeWithAlias,
					Usage: "Print full date time in '2006-01-02T15:04:05Z07:00' format",
				},
				cli.IntFlag{
					Name:  FlagContextTimeoutWithAlias,
					Value: 30,
					Usage: "Optional timeout for list command context in seconds",
				},
			},
			Action: func(c *cli.Context) {
				ListAllWorkflow(c)
			},
		},
		{
			Name:  "query",
			Usage: "query workflow execution",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  FlagWorkflowIDWithAlias,
					Usage: "WorkflowID",
				},
				cli.StringFlag{
					Name:  FlagRunIDWithAlias,
					Usage: "RunID",
				},
				cli.StringFlag{
					Name:  FlagQueryTypeWithAlias,
					Usage: "The query type you want to run",
				},
				cli.StringFlag{
					Name:  FlagInputWithAlias,
					Usage: "Optional input for the query, in JSON format. If there are multiple parameters, concatenate them and separate by space.",
				},
				cli.StringFlag{
					Name: FlagInputFileWithAlias,
					Usage: "Optional input for the query from JSON file. If there are multiple JSON, concatenate them and separate by space or newline. " +
						"Input from file will be overwrite by input from command line",
				},
			},
			Action: func(c *cli.Context) {
				QueryWorkflow(c)
			},
		},
		{
			Name:  "stack",
			Usage: "query workflow execution with __stack_trace as query type",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  FlagWorkflowIDWithAlias,
					Usage: "WorkflowID",
				},
				cli.StringFlag{
					Name:  FlagRunIDWithAlias,
					Usage: "RunID",
				},
				cli.StringFlag{
					Name:  FlagInputWithAlias,
					Usage: "Optional input for the query, in JSON format. If there are multiple parameters, concatenate them and separate by space.",
				},
				cli.StringFlag{
					Name: FlagInputFileWithAlias,
					Usage: "Optional input for the query from JSON file. If there are multiple JSON, concatenate them and separate by space or newline. " +
						"Input from file will be overwrite by input from command line",
				},
			},
			Action: func(c *cli.Context) {
				QueryWorkflowUsingStackTrace(c)
			},
		},
		{
			Name:    "describe",
			Aliases: []string{"desc"},
			Usage:   "show information of workflow execution",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  FlagWorkflowIDWithAlias,
					Usage: "WorkflowID",
				},
				cli.StringFlag{
					Name:  FlagRunIDWithAlias,
					Usage: "RunID",
				},
				cli.BoolFlag{
					Name:  FlagPrintRawTimeWithAlias,
					Usage: "Print raw time stamp",
				},
			},
			Action: func(c *cli.Context) {
				DescribeWorkflow(c)
			},
		},
		{
			Name:        "describeid",
			Aliases:     []string{"descid"},
			Usage:       "show information of workflow execution with given workflow_id and optional run_id (a shortcut of `describe -w <wid> -r <rid>`)",
			Description: "cadence workflow describeid <workflow_id> <run_id>. workflow_id is required; run_id is optional",
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  FlagPrintRawTimeWithAlias,
					Usage: "Print raw time stamp",
				},
			},
			Action: func(c *cli.Context) {
				DescribeWorkflowWithID(c)
			},
		},
		{
			Name:    "observe",
			Aliases: []string{"ob"},
			Usage:   "show the progress of workflow history",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  FlagWorkflowIDWithAlias,
					Usage: "WorkflowID",
				},
				cli.StringFlag{
					Name:  FlagRunIDWithAlias,
					Usage: "RunID",
				},
				cli.IntFlag{
					Name:  FlagContextTimeoutWithAlias,
					Usage: "Optional timeout for start command context in seconds, default value is 120",
				},
				cli.BoolFlag{
					Name:  FlagShowDetailWithAlias,
					Usage: "Optional show event details",
				},
				cli.IntFlag{
					Name:  FlagMaxFieldLengthWithAlias,
					Usage: "Optional maximum length for each attribute field when show details",
				},
			},
			Action: func(c *cli.Context) {
				ObserveHistory(c)
			},
		},
		{
			Name:    "observeid",
			Aliases: []string{"obid"},
			Usage:   "show the progress of workflow history with given workflow_id and optional run_id (a shortcut of `observe -w <wid> -r <rid>`)",
			Flags: []cli.Flag{
				cli.IntFlag{
					Name:  FlagContextTimeoutWithAlias,
					Usage: "Optional timeout for start command context in seconds, default value is 120",
				},
				cli.BoolFlag{
					Name:  FlagShowDetailWithAlias,
					Usage: "Optional show event details",
				},
				cli.IntFlag{
					Name:  FlagMaxFieldLengthWithAlias,
					Usage: "Optional maximum length for each attribute field when show details",
				},
			},
			Action: func(c *cli.Context) {
				ObserveHistoryWithID(c)
			},
		},
	}
}
