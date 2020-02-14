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
	FlagUsername                          = "username"
	FlagPassword                          = "password"
	FlagKeyspace                          = "keyspace"
	FlagAddress                           = "address"
	FlagAddressWithAlias                  = FlagAddress + ", ad"
	FlagHistoryAddress                    = "history_address"
	FlagDBAddress                         = "db_address"
	FlagDBPort                            = "db_port"
	FlagHistoryAddressWithAlias           = FlagHistoryAddress + ", had"
	FlagDomainID                          = "domain_id"
	FlagDomain                            = "domain"
	FlagDomainWithAlias                   = FlagDomain + ", do"
	FlagShardID                           = "shard_id"
	FlagShardIDWithAlias                  = FlagShardID + ", sid"
	FlagWorkflowID                        = "workflow_id"
	FlagWorkflowIDWithAlias               = FlagWorkflowID + ", wid, w"
	FlagRunID                             = "run_id"
	FlagTreeID                            = "tree_id"
	FlagBranchID                          = "branch_id"
	FlagNumberOfShards                    = "number_of_shards"
	FlagRunIDWithAlias                    = FlagRunID + ", rid, r"
	FlagTargetCluster                     = "target_cluster"
	FlagMinEventID                        = "min_event_id"
	FlagMaxEventID                        = "max_event_id"
	FlagTaskList                          = "tasklist"
	FlagTaskListWithAlias                 = FlagTaskList + ", tl"
	FlagTaskListType                      = "tasklisttype"
	FlagTaskListTypeWithAlias             = FlagTaskListType + ", tlt"
	FlagWorkflowIDReusePolicy             = "workflowidreusepolicy"
	FlagWorkflowIDReusePolicyAlias        = FlagWorkflowIDReusePolicy + ", wrp"
	FlagCronSchedule                      = "cron"
	FlagWorkflowType                      = "workflow_type"
	FlagWorkflowTypeWithAlias             = FlagWorkflowType + ", wt"
	FlagWorkflowStatus                    = "status"
	FlagWorkflowStatusWithAlias           = FlagWorkflowStatus + ", s"
	FlagExecutionTimeout                  = "execution_timeout"
	FlagExecutionTimeoutWithAlias         = FlagExecutionTimeout + ", et"
	FlagDecisionTimeout                   = "decision_timeout"
	FlagDecisionTimeoutWithAlias          = FlagDecisionTimeout + ", dt"
	FlagContextTimeout                    = "context_timeout"
	FlagContextTimeoutWithAlias           = FlagContextTimeout + ", ct"
	FlagInput                             = "input"
	FlagInputWithAlias                    = FlagInput + ", i"
	FlagInputFile                         = "input_file"
	FlagInputFileWithAlias                = FlagInputFile + ", if"
	FlagExcludeFile                       = "exclude_file"
	FlagInputSeparator                    = "input_separator"
	FlagParallism                         = "input_parallism"
	FlagSkipCurrentOpen                   = "skip_current_open"
	FlagSkipBaseIsNotCurrent              = "skip_base_is_not_current"
	FlagDryRun                            = "dry_run"
	FlagNonDeterministicOnly              = "only_non_deterministic"
	FlagInputTopic                        = "input_topic"
	FlagInputTopicWithAlias               = FlagInputTopic + ", it"
	FlagHostFile                          = "host_file"
	FlagCluster                           = "cluster"
	FlagInputCluster                      = "input_cluster"
	FlagStartOffset                       = "start_offset"
	FlagTopic                             = "topic"
	FlagGroup                             = "group"
	FlagResult                            = "result"
	FlagIdentity                          = "identity"
	FlagDetail                            = "detail"
	FlagReason                            = "reason"
	FlagReasonWithAlias                   = FlagReason + ", re"
	FlagOpen                              = "open"
	FlagOpenWithAlias                     = FlagOpen + ", op"
	FlagMore                              = "more"
	FlagMoreWithAlias                     = FlagMore + ", m"
	FlagAll                               = "all"
	FlagAllWithAlias                      = FlagAll + ", a"
	FlagPageSize                          = "pagesize"
	FlagPageSizeWithAlias                 = FlagPageSize + ", ps"
	FlagEarliestTime                      = "earliest_time"
	FlagEarliestTimeWithAlias             = FlagEarliestTime + ", et"
	FlagLatestTime                        = "latest_time"
	FlagLatestTimeWithAlias               = FlagLatestTime + ", lt"
	FlagPrintEventVersion                 = "print_event_version"
	FlagPrintEventVersionWithAlias        = FlagPrintEventVersion + ", pev"
	FlagPrintFullyDetail                  = "print_full"
	FlagPrintFullyDetailWithAlias         = FlagPrintFullyDetail + ", pf"
	FlagPrintRawTime                      = "print_raw_time"
	FlagPrintRawTimeWithAlias             = FlagPrintRawTime + ", prt"
	FlagPrintRaw                          = "print_raw"
	FlagPrintRawWithAlias                 = FlagPrintRaw + ", praw"
	FlagPrintDateTime                     = "print_datetime"
	FlagPrintDateTimeWithAlias            = FlagPrintDateTime + ", pdt"
	FlagPrintMemo                         = "print_memo"
	FlagPrintMemoWithAlias                = FlagPrintMemo + ", pme"
	FlagPrintSearchAttr                   = "print_search_attr"
	FlagPrintSearchAttrWithAlias          = FlagPrintSearchAttr + ", psa"
	FlagPrintJSON                         = "print_json"
	FlagPrintJSONWithAlias                = FlagPrintJSON + ", pjson"
	FlagDescription                       = "description"
	FlagDescriptionWithAlias              = FlagDescription + ", desc"
	FlagOwnerEmail                        = "owner_email"
	FlagOwnerEmailWithAlias               = FlagOwnerEmail + ", oe"
	FlagRetentionDays                     = "retention"
	FlagRetentionDaysWithAlias            = FlagRetentionDays + ", rd"
	FlagHistoryArchivalStatus             = "history_archival_status"
	FlagHistoryArchivalStatusWithAlias    = FlagHistoryArchivalStatus + ", has"
	FlagHistoryArchivalURI                = "history_uri"
	FlagHistoryArchivalURIWithAlias       = FlagHistoryArchivalURI + ", huri"
	FlagVisibilityArchivalStatus          = "visibility_archival_status"
	FlagVisibilityArchivalStatusWithAlias = FlagVisibilityArchivalStatus + ", vas"
	FlagVisibilityArchivalURI             = "visibility_uri"
	FlagVisibilityArchivalURIWithAlias    = FlagVisibilityArchivalURI + ", vuri"
	FlagName                              = "name"
	FlagNameWithAlias                     = FlagName + ", n"
	FlagOutputFilename                    = "output_filename"
	FlagOutputFilenameWithAlias           = FlagOutputFilename + ", of"
	FlagOutputFormat                      = "output"
	FlagQueryType                         = "query_type"
	FlagQueryTypeWithAlias                = FlagQueryType + ", qt"
	FlagQueryRejectCondition              = "query_reject_condition"
	FlagQueryRejectConditionWithAlias     = FlagQueryRejectCondition + ", qrc"
	FlagQueryConsistencyLevel             = "query_consistency_level"
	FlagQueryConsistencyLevelWithAlias    = FlagQueryConsistencyLevel + ", qcl"
	FlagShowDetail                        = "show_detail"
	FlagShowDetailWithAlias               = FlagShowDetail + ", sd"
	FlagActiveClusterName                 = "active_cluster"
	FlagActiveClusterNameWithAlias        = FlagActiveClusterName + ", ac"
	FlagClusters                          = "clusters"
	FlagClustersWithAlias                 = FlagClusters + ", cl"
	FlagIsGlobalDomain                    = "global_domain"
	FlagIsGlobalDomainWithAlias           = FlagIsGlobalDomain + ", gd"
	FlagDomainData                        = "domain_data"
	FlagDomainDataWithAlias               = FlagDomainData + ", dmd"
	FlagEventID                           = "event_id"
	FlagEventIDWithAlias                  = FlagEventID + ", eid"
	FlagActivityID                        = "activity_id"
	FlagActivityIDWithAlias               = FlagActivityID + ", aid"
	FlagMaxFieldLength                    = "max_field_length"
	FlagMaxFieldLengthWithAlias           = FlagMaxFieldLength + ", maxl"
	FlagSecurityToken                     = "security_token"
	FlagSecurityTokenWithAlias            = FlagSecurityToken + ", st"
	FlagSkipErrorMode                     = "skip_errors"
	FlagSkipErrorModeWithAlias            = FlagSkipErrorMode + ", serr"
	FlagHeadersMode                       = "headers"
	FlagHeadersModeWithAlias              = FlagHeadersMode + ", he"
	FlagMessageType                       = "message_type"
	FlagMessageTypeWithAlias              = FlagMessageType + ", mt"
	FlagURL                               = "url"
	FlagMuttleyDestination                = "muttely_destination"
	FlagMuttleyDestinationWithAlias       = FlagMuttleyDestination + ", muttley"
	FlagIndex                             = "index"
	FlagBatchSize                         = "batch_size"
	FlagBatchSizeWithAlias                = FlagBatchSize + ", bs"
	FlagMemoKey                           = "memo_key"
	FlagMemo                              = "memo"
	FlagMemoFile                          = "memo_file"
	FlagSearchAttributesKey               = "search_attr_key"
	FlagSearchAttributesVal               = "search_attr_value"
	FlagSearchAttributesType              = "search_attr_type"
	FlagAddBadBinary                      = "add_bad_binary"
	FlagRemoveBadBinary                   = "remove_bad_binary"
	FlagResetType                         = "reset_type"
	FlagResetPointsOnly                   = "reset_points_only"
	FlagResetBadBinaryChecksum            = "reset_bad_binary_checksum"
	FlagListQuery                         = "query"
	FlagListQueryWithAlias                = FlagListQuery + ", q"
	FlagBatchType                         = "batch_type"
	FlagBatchTypeWithAlias                = FlagBatchType + ", bt"
	FlagSignalName                        = "signal_name"
	FlagSignalNameWithAlias               = FlagSignalName + ", sig"
	FlagRemoveTaskID                      = "task_id"
	FlagRemoveTypeID                      = "type_id"
	FlagRPS                               = "rps"
	FlagJobID                             = "job_id"
	FlagJobIDWithAlias                    = FlagJobID + ", jid"
	FlagYes                               = "yes"
	FlagServiceConfigDir                  = "service_config_dir"
	FlagServiceConfigDirWithAlias         = FlagServiceConfigDir + ", scd"
	FlagServiceEnv                        = "service_env"
	FlagServiceEnvWithAlias               = FlagServiceEnv + ", se"
	FlagServiceZone                       = "service_zone"
	FlagServiceZoneWithAlias              = FlagServiceZone + ", sz"
	FlagEnableTLS                         = "tls"
	FlagTLSCertPath                       = "tls_cert_path"
	FlagTLSKeyPath                        = "tls_key_path"
	FlagTLSCaPath                         = "tls_ca_path"
	FlagTLSEnableHostVerification         = "tls_enable_host_verification"
	FlagDLQType                           = "dlq_type"
	FlagDLQTypeWithAlias                  = FlagDLQType + ", dt"
	FlagMaxMessageCount                   = "max_message_count"
	FlagMaxMessageCountWithAlias          = FlagMaxMessageCount + ", mmc"
	FlagLastMessageID                     = "last_message_id"
	FlagLastMessageIDWithAlias            = FlagLastMessageID + ", lm"
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
			Usage: "Print timestamp",
		},
		cli.BoolFlag{
			Name:  FlagPrintRawTimeWithAlias,
			Usage: "Print raw timestamp",
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
		cli.BoolFlag{
			Name:  FlagResetPointsOnly,
			Usage: "Only show events that are eligible for reset",
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
		cli.StringFlag{
			Name: FlagSearchAttributesKey,
			Usage: "Optional search attributes keys that can be be used in list query. If there are multiple keys, concatenate them and separate by |. " +
				"Use 'cluster get-search-attr' cmd to list legal keys.",
		},
		cli.StringFlag{
			Name: FlagSearchAttributesVal,
			Usage: "Optional search attributes value that can be be used in list query. If there are multiple keys, concatenate them and separate by |. " +
				"If value is array, use json array like [\"a\",\"b\"], [1,2], [\"true\",\"false\"], [\"2019-06-07T17:16:34-08:00\",\"2019-06-07T18:16:34-08:00\"]. " +
				"Use 'cluster get-search-attr' cmd to list legal keys and value types",
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

func getCommonFlagsForVisibility() []cli.Flag {
	return []cli.Flag{
		cli.BoolFlag{
			Name:  FlagPrintRawTimeWithAlias,
			Usage: "Print raw timestamp",
		},
		cli.BoolFlag{
			Name:  FlagPrintDateTimeWithAlias,
			Usage: "Print full date time in '2006-01-02T15:04:05Z07:00' format",
		},
		cli.BoolFlag{
			Name:  FlagPrintMemoWithAlias,
			Usage: "Print memo",
		},
		cli.BoolFlag{
			Name:  FlagPrintSearchAttrWithAlias,
			Usage: "Print search attributes",
		},
		cli.BoolFlag{
			Name:  FlagPrintFullyDetailWithAlias,
			Usage: "Print full message without table format",
		},
		cli.BoolFlag{
			Name:  FlagPrintJSONWithAlias,
			Usage: "Print in raw json format",
		},
	}
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
	flagsForListAll := []cli.Flag{
		cli.BoolFlag{
			Name:  FlagOpenWithAlias,
			Usage: "List for open workflow executions, default is to list for closed ones",
		},
		cli.StringFlag{
			Name: FlagEarliestTimeWithAlias,
			Usage: "EarliestTime of start time, supported formats are '2006-01-02T15:04:05+07:00', raw UnixNano and " +
				"time range (N<duration>), where 0 < N < 1000000 and duration (full-notation/short-notation) can be second/s, " +
				"minute/m, hour/h, day/d, week/w, month/M or year/y. For example, '15minute' or '15m' implies last 15 minutes.",
		},
		cli.StringFlag{
			Name: FlagLatestTimeWithAlias,
			Usage: "LatestTime of start time, supported formats are '2006-01-02T15:04:05+07:00', raw UnixNano and " +
				"time range (N<duration>), where 0 < N < 1000000 and duration (in full-notation/short-notation) can be second/s, " +
				"minute/m, hour/h, day/d, week/w, month/M or year/y. For example, '15minute' or '15m' implies last 15 minutes",
		},
		cli.StringFlag{
			Name:  FlagWorkflowIDWithAlias,
			Usage: "WorkflowID",
		},
		cli.StringFlag{
			Name:  FlagWorkflowTypeWithAlias,
			Usage: "WorkflowTypeName",
		},
		cli.StringFlag{
			Name:  FlagWorkflowStatusWithAlias,
			Usage: "Closed workflow status [completed, failed, canceled, terminated, continuedasnew, timedout]",
		},
		cli.StringFlag{
			Name: FlagListQueryWithAlias,
			Usage: "Optional SQL like query for use of search attributes. NOTE: using query will ignore all other filter flags including: " +
				"[open, earliest_time, latest_time, workflow_id, workflow_type]",
		},
	}
	flagsForListAll = append(getCommonFlagsForVisibility(), flagsForListAll...)
	return flagsForListAll
}

func getFlagsForScan() []cli.Flag {
	flagsForScan := []cli.Flag{
		cli.IntFlag{
			Name:  FlagPageSizeWithAlias,
			Value: 2000,
			Usage: "Page size for each Scan API call",
		},
		cli.StringFlag{
			Name:  FlagListQueryWithAlias,
			Usage: "Optional SQL like query",
		},
	}
	flagsForScan = append(getCommonFlagsForVisibility(), flagsForScan...)
	return flagsForScan
}

func getFlagsForListArchived() []cli.Flag {
	flagsForListArchived := []cli.Flag{
		cli.StringFlag{
			Name:  FlagListQueryWithAlias,
			Usage: "SQL like query. Please check the documentation of the visibility archiver used by your domain for detailed instructions",
		},
		cli.IntFlag{
			Name:  FlagPageSizeWithAlias,
			Value: 100,
			Usage: "Count of visibility records included in a single page, default to 100",
		},
		cli.BoolFlag{
			Name:  FlagAllWithAlias,
			Usage: "List all pages",
		},
	}
	flagsForListArchived = append(getCommonFlagsForVisibility(), flagsForListArchived...)
	return flagsForListArchived
}

func getFlagsForCount() []cli.Flag {
	return []cli.Flag{
		cli.StringFlag{
			Name:  FlagListQueryWithAlias,
			Usage: "Optional SQL like query. e.g count all open workflows 'CloseTime = missing'; 'WorkflowType=\"wtype\" and CloseTime > 0'",
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
		cli.StringFlag{
			Name:  FlagQueryRejectConditionWithAlias,
			Usage: "Optional flag to reject queries based on workflow state. Valid values are \"not_open\" and \"not_completed_cleanly\"",
		},
		cli.StringFlag{
			Name:  FlagQueryConsistencyLevelWithAlias,
			Usage: "Optional flag to set query consistency level. Valid values are \"eventual\" and \"strong\"",
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
			Name:  FlagPrintRawWithAlias,
			Usage: "Print properties as they are stored",
		},
		cli.BoolFlag{
			Name:  FlagResetPointsOnly,
			Usage: "Only show auto-reset points",
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
