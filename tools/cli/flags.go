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

// Flags used to specify cli command line arguments
const (
	FlagPort                        = "port"
	FlagUsername                    = "username"
	FlagPassword                    = "password"
	FlagKeyspace                    = "keyspace"
	FlagAddress                     = "address"
	FlagAddressWithAlias            = FlagAddress + ", ad"
	FlagHistoryAddress              = "history_address"
	FlagHistoryAddressWithAlias     = FlagHistoryAddress + ", had"
	FlagDomainID                    = "domain_id"
	FlagDomain                      = "domain"
	FlagDomainWithAlias             = FlagDomain + ", do"
	FlagShardID                     = "shard_id"
	FlagShardIDWithAlias            = FlagShardID + ", sid"
	FlagWorkflowID                  = "workflow_id"
	FlagWorkflowIDWithAlias         = FlagWorkflowID + ", wid, w"
	FlagRunID                       = "run_id"
	FlagTreeID                      = "tree_id"
	FlagBranchID                    = "branch_id"
	FlagNumberOfShards              = "number_of_shards"
	FlagRunIDWithAlias              = FlagRunID + ", rid, r"
	FlagTargetCluster               = "target_cluster"
	FlagMinEventID                  = "min_event_id"
	FlagMaxEventID                  = "max_event_id"
	FlagTaskList                    = "tasklist"
	FlagTaskListWithAlias           = FlagTaskList + ", tl"
	FlagTaskListType                = "tasklisttype"
	FlagTaskListTypeWithAlias       = FlagTaskListType + ", tlt"
	FlagWorkflowIDReusePolicy       = "workflowidreusepolicy"
	FlagWorkflowIDReusePolicyAlias  = FlagWorkflowIDReusePolicy + ", wrp"
	FlagCronSchedule                = "cron"
	FlagWorkflowType                = "workflow_type"
	FlagWorkflowTypeWithAlias       = FlagWorkflowType + ", wt"
	FlagWorkflowStatus              = "status"
	FlagWorkflowStatusWithAlias     = FlagWorkflowStatus + ", s"
	FlagExecutionTimeout            = "execution_timeout"
	FlagExecutionTimeoutWithAlias   = FlagExecutionTimeout + ", et"
	FlagDecisionTimeout             = "decision_timeout"
	FlagDecisionTimeoutWithAlias    = FlagDecisionTimeout + ", dt"
	FlagContextTimeout              = "context_timeout"
	FlagContextTimeoutWithAlias     = FlagContextTimeout + ", ct"
	FlagInput                       = "input"
	FlagInputWithAlias              = FlagInput + ", i"
	FlagInputFile                   = "input_file"
	FlagInputFileWithAlias          = FlagInputFile + ", if"
	FlagInputTopic                  = "input_topic"
	FlagInputTopicWithAlias         = FlagInputTopic + ", it"
	FlagHostFile                    = "host_file"
	FlagCluster                     = "cluster"
	FlagInputCluster                = "input_cluster"
	FlagStartOffset                 = "start_offset"
	FlagTopic                       = "topic"
	FlagGroup                       = "group"
	FlagResult                      = "result"
	FlagIdentity                    = "identity"
	FlagDetail                      = "detail"
	FlagReason                      = "reason"
	FlagReasonWithAlias             = FlagReason + ", re"
	FlagOpen                        = "open"
	FlagOpenWithAlias               = FlagOpen + ", op"
	FlagMore                        = "more"
	FlagMoreWithAlias               = FlagMore + ", m"
	FlagPageSize                    = "pagesize"
	FlagPageSizeWithAlias           = FlagPageSize + ", ps"
	FlagEarliestTime                = "earliest_time"
	FlagEarliestTimeWithAlias       = FlagEarliestTime + ", et"
	FlagLatestTime                  = "latest_time"
	FlagLatestTimeWithAlias         = FlagLatestTime + ", lt"
	FlagPrintEventVersion           = "print_event_version"
	FlagPrintEventVersionWithAlias  = FlagPrintEventVersion + ", pev"
	FlagPrintFullyDetail            = "print_full"
	FlagPrintFullyDetailWithAlias   = FlagPrintFullyDetail + ", pf"
	FlagPrintRawTime                = "print_raw_time"
	FlagPrintRawTimeWithAlias       = FlagPrintRawTime + ", prt"
	FlagPrintDateTime               = "print_datetime"
	FlagPrintDateTimeWithAlias      = FlagPrintDateTime + ", pdt"
	FlagPrintMemo                   = "print_memo"
	FlagPrintMemoWithAlias          = FlagPrintMemo + ", pme"
	FlagDescription                 = "description"
	FlagDescriptionWithAlias        = FlagDescription + ", desc"
	FlagOwnerEmail                  = "owner_email"
	FlagOwnerEmailWithAlias         = FlagOwnerEmail + ", oe"
	FlagRetentionDays               = "retention"
	FlagRetentionDaysWithAlias      = FlagRetentionDays + ", rd"
	FlagEmitMetric                  = "emit_metric"
	FlagEmitMetricWithAlias         = FlagEmitMetric + ", em"
	FlagArchivalStatus              = "archival_status"
	FlagArchivalStatusWithAlias     = FlagArchivalStatus + ", as"
	FlagArchivalBucketName          = "bucket"
	FlagArchivalBucketNameWithAlias = FlagArchivalBucketName + ", ab"
	FlagName                        = "name"
	FlagNameWithAlias               = FlagName + ", n"
	FlagOutputFilename              = "output_filename"
	FlagOutputFilenameWithAlias     = FlagOutputFilename + ", of"
	FlagQueryType                   = "query_type"
	FlagQueryTypeWithAlias          = FlagQueryType + ", qt"
	FlagShowDetail                  = "show_detail"
	FlagShowDetailWithAlias         = FlagShowDetail + ", sd"
	FlagActiveClusterName           = "active_cluster"
	FlagActiveClusterNameWithAlias  = FlagActiveClusterName + ", ac"
	FlagClusters                    = "clusters"
	FlagClustersWithAlias           = FlagClusters + ", cl"
	FlagDomainData                  = "domain_data"
	FlagDomainDataWithAlias         = FlagDomainData + ", dmd"
	FlagEventID                     = "event_id"
	FlagEventIDWithAlias            = FlagEventID + ", eid"
	FlagActivityID                  = "activity_id"
	FlagActivityIDWithAlias         = FlagActivityID + ", aid"
	FlagMaxFieldLength              = "max_field_length"
	FlagMaxFieldLengthWithAlias     = FlagMaxFieldLength + ", maxl"
	FlagSecurityToken               = "security_token"
	FlagSecurityTokenWithAlias      = FlagSecurityToken + ", st"
	FlagSkipErrorMode               = "skip_errors"
	FlagSkipErrorModeWithAlias      = FlagSkipErrorMode + ", serr"
	FlagHeadersMode                 = "headers"
	FlagHeadersModeWithAlias        = FlagHeadersMode + ", he"
	FlagMessageType                 = "message_type"
	FlagMessageTypeWithAlias        = FlagMessageType + ", mt"
	FlagURL                         = "url"
	FlagMuttleyDestination          = "muttely_destination"
	FlagMuttleyDestinationWithAlias = FlagMuttleyDestination + ", muttley"
	FlagIndex                       = "index"
	FlagBatchSize                   = "batch_size"
	FlagBatchSizeWithAlias          = FlagBatchSize + ", bs"
	FlagMemoKey                     = "memo_key"
	FlagMemo                        = "memo"
	FlagMemoFile                    = "memo_file"
)

var flagsForExecution = []cli.Flag{
	cli.StringFlag{
		Name:  FlagWorkflowIDWithAlias,
		Usage: "WorkflowID",
	},
	cli.StringFlag{
		Name:  FlagRunIDWithAlias,
		Usage: "RunID",
	},
}

func getFlagsForShow() []cli.Flag {
	return append(flagsForExecution, getFlagsForShowID()...)
}

func getFlagsForShowID() []cli.Flag {
	return []cli.Flag{
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
	}
}

func getFlagsForStart() []cli.Flag {
	return []cli.Flag{
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
			Name: FlagCronSchedule,
			Usage: "Optional cron schedule for the workflow. Cron spec is as following: \n" +
				"\t┌───────────── minute (0 - 59) \n" +
				"\t│ ┌───────────── hour (0 - 23) \n" +
				"\t│ │ ┌───────────── day of the month (1 - 31) \n" +
				"\t│ │ │ ┌───────────── month (1 - 12) \n" +
				"\t│ │ │ │ ┌───────────── day of the week (0 - 6) (Sunday to Saturday) \n" +
				"\t│ │ │ │ │ \n" +
				"\t* * * * *",
		},
		cli.IntFlag{
			Name: FlagWorkflowIDReusePolicyAlias,
			Usage: "Optional input to configure if the same workflow ID is allow to use for new workflow execution. " +
				"Available options: 0: AllowDuplicateFailedOnly, 1: AllowDuplicate, 2: RejectDuplicate",
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
		cli.StringFlag{
			Name:  FlagMemoKey,
			Usage: "Optional key of memo. If there are multiple keys, concatenate them and separate by space",
		},
		cli.StringFlag{
			Name: FlagMemo,
			Usage: "Optional info that can be showed when list workflow, in JSON format. If there are multiple JSON, concatenate them and separate by space. " +
				"The order must be same as memo_key",
		},
		cli.StringFlag{
			Name: FlagMemoFile,
			Usage: "Optional info that can be listed in list workflow, from JSON format file. If there are multiple JSON, concatenate them and separate by space or newline. " +
				"The order must be same as memo_key",
		},
	}
}

func getFlagsForRun() []cli.Flag {
	flagsForRun := []cli.Flag{
		cli.BoolFlag{
			Name:  FlagShowDetailWithAlias,
			Usage: "Show event details",
		},
		cli.IntFlag{
			Name:  FlagMaxFieldLengthWithAlias,
			Usage: "Maximum length for each attribute field",
		},
	}
	flagsForRun = append(getFlagsForStart(), flagsForRun...)
	return flagsForRun
}

func getFlagsForList() []cli.Flag {
	flagsForList := []cli.Flag{
		cli.BoolFlag{
			Name:  FlagMoreWithAlias,
			Usage: "List more pages, default is to list one page of default page size 10",
		},
		cli.IntFlag{
			Name:  FlagPageSizeWithAlias,
			Value: 10,
			Usage: "Result page size",
		},
	}
	flagsForList = append(getFlagsForListAll(), flagsForList...)
	return flagsForList
}

func getFlagsForListAll() []cli.Flag {
	return []cli.Flag{
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
		cli.BoolFlag{
			Name:  FlagPrintMemoWithAlias,
			Usage: "Print memo",
		},
		cli.StringFlag{
			Name:  FlagWorkflowStatusWithAlias,
			Usage: "Closed workflow status [completed, failed, canceled, terminated, continuedasnew, timedout]",
		},
	}
}

func getFlagsForQuery() []cli.Flag {
	return []cli.Flag{
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
	}
}

// all flags of query except QueryType
func getFlagsForStack() []cli.Flag {
	flags := getFlagsForQuery()
	for i := 0; i < len(flags); i++ {
		if flags[i].GetName() == FlagQueryTypeWithAlias {
			return append(flags[:i], flags[i+1:]...)
		}
	}
	return flags
}

func getFlagsForDescribe() []cli.Flag {
	return append(flagsForExecution, getFlagsForDescribeID()...)
}

func getFlagsForDescribeID() []cli.Flag {
	return []cli.Flag{
		cli.BoolFlag{
			Name:  FlagPrintRawTimeWithAlias,
			Usage: "Print raw time stamp",
		},
	}
}

func getFlagsForObserve() []cli.Flag {
	return append(flagsForExecution, getFlagsForObserveID()...)
}

func getFlagsForObserveID() []cli.Flag {
	return []cli.Flag{
		cli.BoolFlag{
			Name:  FlagShowDetailWithAlias,
			Usage: "Optional show event details",
		},
		cli.IntFlag{
			Name:  FlagMaxFieldLengthWithAlias,
			Usage: "Optional maximum length for each attribute field when show details",
		},
	}
}
