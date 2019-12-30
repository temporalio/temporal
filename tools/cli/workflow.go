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
	"strings"

	"github.com/urfave/cli"

	"github.com/uber/cadence/service/worker/batcher"
)

func newWorkflowCommands() []cli.Command {
	return []cli.Command{
		{
			Name:        "activity",
			Aliases:     []string{"act"},
			Usage:       "operate activities of workflow",
			Subcommands: newActivityCommands(),
		},
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
			Name:  "listarchived",
			Usage: "list archived workflow executions",
			Flags: getFlagsForListArchived(),
			Action: func(c *cli.Context) {
				ListArchivedWorkflow(c)
			},
		},
		{
			Name:    "scan",
			Aliases: []string{"sc", "scanall"},
			Usage: "scan workflow executions (need to enable Cadence server on ElasticSearch). " +
				"It will be faster than listall, but result are not sorted.",
			Flags: getFlagsForScan(),
			Action: func(c *cli.Context) {
				ScanAllWorkflow(c)
			},
		},
		{
			Name:    "count",
			Aliases: []string{"cnt"},
			Usage:   "count number of workflow executions (need to enable Cadence server on ElasticSearch)",
			Flags:   getFlagsForCount(),
			Action: func(c *cli.Context) {
				CountWorkflow(c)
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
			Usage:   "reset the workflow, by either eventID or resetType.",
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
					Usage: "The eventID of any event after DecisionTaskStarted you want to reset to (exclusive). It can be DecisionTaskCompleted, DecisionTaskFailed or others",
				},
				cli.StringFlag{
					Name:  FlagReason,
					Usage: "reason to do the reset",
				},
				cli.StringFlag{
					Name:  FlagResetType,
					Usage: "where to reset. Support one of these: " + strings.Join(mapKeysToArray(resetTypesMap), ","),
				},
				cli.StringFlag{
					Name:  FlagResetBadBinaryChecksum,
					Usage: "Binary checksum for resetType of BadBinary",
				},
			},
			Action: func(c *cli.Context) {
				ResetWorkflow(c)
			},
		},
		{
			Name: "reset-batch",
			Usage: "reset workflow in batch by resetType: " + strings.Join(mapKeysToArray(resetTypesMap), ",") +
				"To get base workflowIDs/runIDs to reset, source is from input file or visibility query.",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  FlagInputFileWithAlias,
					Usage: "Input file to use for resetting, one workflow per line of WorkflowID and RunID. RunID is optional, default to current runID if not specified. ",
				},
				cli.StringFlag{
					Name:  FlagListQueryWithAlias,
					Usage: "visibility query to get workflows to reset",
				},
				cli.StringFlag{
					Name:  FlagExcludeFile,
					Value: "",
					Usage: "Another input file to use for excluding from resetting, only workflowID is needed.",
				},
				cli.StringFlag{
					Name:  FlagInputSeparator,
					Value: "\t",
					Usage: "Separator for input file(default to tab)",
				},
				cli.StringFlag{
					Name:  FlagReason,
					Usage: "Reason for reset",
				},
				cli.IntFlag{
					Name:  FlagParallism,
					Value: 1,
					Usage: "Number of goroutines to run in parallel. Each goroutine would process one line for every second.",
				},
				cli.BoolFlag{
					Name:  FlagSkipCurrentOpen,
					Usage: "Skip the workflow if the current run is open for the same workflowID as base.",
				},
				cli.BoolFlag{
					Name: FlagSkipBaseIsNotCurrent,
					// TODO https://github.com/uber/cadence/issues/2930
					// The right way to prevent needs server side implementation .
					// This client side is only best effort
					Usage: "Skip if base run is not current run.",
				},
				cli.BoolFlag{
					Name:  FlagNonDeterministicOnly,
					Usage: "Only apply onto workflows whose last event is decisionTaskFailed with non deterministic error.",
				},
				cli.BoolFlag{
					Name:  FlagDryRun,
					Usage: "Not do real action of reset(just logging in STDOUT)",
				},
				cli.StringFlag{
					Name:  FlagResetType,
					Usage: "where to reset. Support one of these: " + strings.Join(mapKeysToArray(resetTypesMap), ","),
				},
				cli.StringFlag{
					Name:  FlagResetBadBinaryChecksum,
					Usage: "Binary checksum for resetType of BadBinary",
				},
			},
			Action: func(c *cli.Context) {
				ResetInBatch(c)
			},
		},
		{
			Name:        "batch",
			Usage:       "batch operation on a list of workflows from query.",
			Subcommands: newBatchCommands(),
		},
	}
}

func newActivityCommands() []cli.Command {
	return []cli.Command{
		{
			Name:    "complete",
			Aliases: []string{"comp"},
			Usage:   "complete an activity",
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
					Name:  FlagActivityIDWithAlias,
					Usage: "The activityID to operate on",
				},
				cli.StringFlag{
					Name:  FlagResult,
					Usage: "Result of the activity",
				},
				cli.StringFlag{
					Name:  FlagIdentity,
					Usage: "Identity of the operator",
				},
			},
			Action: func(c *cli.Context) {
				CompleteActivity(c)
			},
		},
		{
			Name:  "fail",
			Usage: "fail an activity",
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
					Name:  FlagActivityIDWithAlias,
					Usage: "The activityID to operate on",
				},
				cli.StringFlag{
					Name:  FlagReason,
					Usage: "Reason to fail the activity",
				},
				cli.StringFlag{
					Name:  FlagDetail,
					Usage: "Detail to fail the activity",
				},
				cli.StringFlag{
					Name:  FlagIdentity,
					Usage: "Identity of the operator",
				},
			},
			Action: func(c *cli.Context) {
				FailActivity(c)
			},
		},
	}
}

func newBatchCommands() []cli.Command {
	return []cli.Command{
		{
			Name:    "describe",
			Aliases: []string{"desc"},
			Usage:   "Describe a batch operation job",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  FlagJobIDWithAlias,
					Usage: "Batch Job ID",
				},
			},
			Action: func(c *cli.Context) {
				DescribeBatchJob(c)
			},
		},
		{
			Name:  "terminate",
			Usage: "terminate a batch operation job",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  FlagJobIDWithAlias,
					Usage: "Batch Job ID",
				},
				cli.StringFlag{
					Name:  FlagReasonWithAlias,
					Usage: "Reason to stop this batch job",
				},
			},
			Action: func(c *cli.Context) {
				TerminateBatchJob(c)
			},
		},
		{
			Name:    "list",
			Aliases: []string{"l"},
			Usage:   "Describe a batch operation job",
			Flags: []cli.Flag{
				cli.IntFlag{
					Name:  FlagPageSizeWithAlias,
					Value: 30,
					Usage: "Result page size",
				},
			},
			Action: func(c *cli.Context) {
				ListBatchJobs(c)
			},
		},
		{
			Name:  "start",
			Usage: "Start a batch operation job",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  FlagListQueryWithAlias,
					Usage: "Query to get workflows for being executed this batch operation",
				},
				cli.StringFlag{
					Name:  FlagReasonWithAlias,
					Usage: "Reason to run this batch job",
				},
				cli.StringFlag{
					Name:  FlagBatchTypeWithAlias,
					Usage: "Types supported: " + strings.Join(batcher.AllBatchTypes, ","),
				},
				//below are optional
				cli.StringFlag{
					Name:  FlagSignalNameWithAlias,
					Usage: "Required for batch signal",
				},
				cli.StringFlag{
					Name:  FlagInputWithAlias,
					Usage: "Optional input of signal",
				},
				cli.IntFlag{
					Name:  FlagRPS,
					Value: batcher.DefaultRPS,
					Usage: "RPS of processing",
				},
				cli.BoolFlag{
					Name:  FlagYes,
					Usage: "Optional flag to disable confirmation prompt",
				},
			},
			Action: func(c *cli.Context) {
				StartBatchJob(c)
			},
		},
	}
}
