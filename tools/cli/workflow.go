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
			Flags: getFlagsForShow(),
			Action: func(c *cli.Context) {
				ShowHistory(c)
			},
		},
		{
			Name:        "showid",
			Usage:       "show workflow history with given workflow_id and optional run_id (a shortcut of `show -w <wid> -r <rid>`)",
			Description: "cadence workflow showid <workflow_id> <run_id>. workflow_id is required; run_id is optional",
			Flags:       getFlagsForShowID(),
			Action: func(c *cli.Context) {
				ShowHistoryWithWID(c)
			},
		},
		{
			Name:  "start",
			Usage: "start a new workflow execution",
			Flags: getFlagsForStart(),
			Action: func(c *cli.Context) {
				StartWorkflow(c)
			},
		},
		{
			Name:  "run",
			Usage: "start a new workflow execution and get workflow progress",
			Flags: getFlagsForRun(),
			Action: func(c *cli.Context) {
				RunWorkflow(c)
			},
		},
		{
			Name:    "cancel",
			Aliases: []string{"c"},
			Usage:   "cancel a workflow execution",
			Flags:   flagsForExecution,
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
			Flags:       getFlagsForList(),
			Action: func(c *cli.Context) {
				ListWorkflow(c)
			},
		},
		{
			Name:    "listall",
			Aliases: []string{"la"},
			Usage:   "list all open or closed workflow executions",
			Flags:   getFlagsForListAll(),
			Action: func(c *cli.Context) {
				ListAllWorkflow(c)
			},
		},
		{
			Name:  "query",
			Usage: "query workflow execution",
			Flags: getFlagsForQuery(),
			Action: func(c *cli.Context) {
				QueryWorkflow(c)
			},
		},
		{
			Name:  "stack",
			Usage: "query workflow execution with __stack_trace as query type",
			Flags: getFlagsForStack(),
			Action: func(c *cli.Context) {
				QueryWorkflowUsingStackTrace(c)
			},
		},
		{
			Name:    "describe",
			Aliases: []string{"desc"},
			Usage:   "show information of workflow execution",
			Flags:   getFlagsForDescribe(),
			Action: func(c *cli.Context) {
				DescribeWorkflow(c)
			},
		},
		{
			Name:        "describeid",
			Aliases:     []string{"descid"},
			Usage:       "show information of workflow execution with given workflow_id and optional run_id (a shortcut of `describe -w <wid> -r <rid>`)",
			Description: "cadence workflow describeid <workflow_id> <run_id>. workflow_id is required; run_id is optional",
			Flags:       getFlagsForDescribeID(),
			Action: func(c *cli.Context) {
				DescribeWorkflowWithID(c)
			},
		},
		{
			Name:    "observe",
			Aliases: []string{"ob"},
			Usage:   "show the progress of workflow history",
			Flags:   getFlagsForObserve(),
			Action: func(c *cli.Context) {
				ObserveHistory(c)
			},
		},
		{
			Name:    "observeid",
			Aliases: []string{"obid"},
			Usage:   "show the progress of workflow history with given workflow_id and optional run_id (a shortcut of `observe -w <wid> -r <rid>`)",
			Flags:   getFlagsForObserveID(),
			Action: func(c *cli.Context) {
				ObserveHistoryWithID(c)
			},
		},
		{
			Name:    "reset",
			Aliases: []string{"rs"},
			Usage:   "reset the workflow",
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
					Name:  FlagEventID,
					Usage: "The eventID of a DecisionTaskCompleted/DecisionTaskFailed you want to reset to (exclusive)",
				},
				cli.StringFlag{
					Name:  FlagReason,
					Usage: "reason to do the reset",
				},
			},
			Action: func(c *cli.Context) {
				ResetWorkflow(c)
			},
		},
	}
}
