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

	"github.com/urfave/cli"
)

// Flags used to specify cli command line arguments
const (
	FlagUsername                              = "username"
	FlagPassword                              = "password"
	FlagKeyspace                              = "keyspace"
	FlagAddress                               = "address"
	FlagAddressWithAlias                      = FlagAddress + ", ad"
	FlagDBEngine                              = "db_engine"
	FlagDBAddress                             = "db_address"
	FlagDBPort                                = "db_port"
	FlagHistoryAddress                        = "history_address"
	FlagHistoryAddressWithAlias               = FlagHistoryAddress + ", had"
	FlagFrontendAddress                       = "frontend_address"
	FlagFrontendAddressWithAlias              = FlagFrontendAddress + ", fad"
	FlagNamespaceID                           = "namespace_id"
	FlagNamespace                             = "namespace"
	FlagNamespaceWithAlias                    = FlagNamespace + ", ns"
	FlagShardID                               = "shard_id"
	FlagShardIDWithAlias                      = FlagShardID + ", sid"
	FlagWorkflowID                            = "workflow_id"
	FlagWorkflowIDWithAlias                   = FlagWorkflowID + ", wid, w"
	FlagRunID                                 = "run_id"
	FlagTreeID                                = "tree_id"
	FlagBranchID                              = "branch_id"
	FlagNumberOfShards                        = "number_of_shards"
	FlagRunIDWithAlias                        = FlagRunID + ", rid, r"
	FlagTargetCluster                         = "target_cluster"
	FlagMinEventID                            = "min_event_id"
	FlagMaxEventID                            = "max_event_id"
	FlagMinEventVersion                       = "min_event_version"
	FlagMaxEventVersion                       = "max_event_version"
	FlagTaskQueue                             = "taskqueue"
	FlagTaskQueueWithAlias                    = FlagTaskQueue + ", tq"
	FlagTaskQueueType                         = "taskqueuetype"
	FlagTaskQueueTypeWithAlias                = FlagTaskQueueType + ", tqt"
	FlagWorkflowIDReusePolicy                 = "workflowidreusepolicy"
	FlagWorkflowIDReusePolicyAlias            = FlagWorkflowIDReusePolicy + ", wrp"
	FlagCronSchedule                          = "cron"
	FlagWorkflowType                          = "workflow_type"
	FlagWorkflowTypeWithAlias                 = FlagWorkflowType + ", wt"
	FlagWorkflowStatus                        = "status"
	FlagWorkflowStatusWithAlias               = FlagWorkflowStatus + ", s"
	FlagWorkflowExecutionTimeout              = "execution_timeout"
	FlagWorkflowExecutionTimeoutWithAlias     = FlagWorkflowExecutionTimeout + ", et"
	FlagWorkflowRunTimeout                    = "run-timeout"
	FlagWorkflowRunTimeoutWithAlias           = FlagWorkflowRunTimeout + ", rt"
	FlagWorkflowTaskTimeout                   = "workflow_task_timeout"
	FlagWorkflowTaskTimeoutWithAlias          = FlagWorkflowTaskTimeout + ", wtt"
	FlagContextTimeout                        = "context_timeout"
	FlagContextTimeoutWithAlias               = FlagContextTimeout + ", ct"
	FlagInput                                 = "input"
	FlagInputWithAlias                        = FlagInput + ", i"
	FlagInputFile                             = "input_file"
	FlagInputFileWithAlias                    = FlagInputFile + ", if"
	FlagExcludeFile                           = "exclude_file"
	FlagInputSeparator                        = "input_separator"
	FlagParallism                             = "input_parallism"
	FlagSkipCurrentOpen                       = "skip_current_open"
	FlagSkipBaseIsNotCurrent                  = "skip_base_is_not_current"
	FlagDryRun                                = "dry_run"
	FlagNonDeterministicOnly                  = "only_non_deterministic"
	FlagCluster                               = "cluster"
	FlagResult                                = "result"
	FlagIdentity                              = "identity"
	FlagDetail                                = "detail"
	FlagReason                                = "reason"
	FlagReasonWithAlias                       = FlagReason + ", re"
	FlagOpen                                  = "open"
	FlagOpenWithAlias                         = FlagOpen + ", op"
	FlagMore                                  = "more"
	FlagMoreWithAlias                         = FlagMore + ", m"
	FlagAll                                   = "all"
	FlagAllWithAlias                          = FlagAll + ", a"
	FlagPageSize                              = "pagesize"
	FlagPageSizeWithAlias                     = FlagPageSize + ", ps"
	FlagEarliestTime                          = "earliest_time"
	FlagEarliestTimeWithAlias                 = FlagEarliestTime + ", et"
	FlagLatestTime                            = "latest_time"
	FlagLatestTimeWithAlias                   = FlagLatestTime + ", lt"
	FlagPrintEventVersion                     = "print_event_version"
	FlagPrintEventVersionWithAlias            = FlagPrintEventVersion + ", pev"
	FlagPrintFullyDetail                      = "print_full"
	FlagPrintFullyDetailWithAlias             = FlagPrintFullyDetail + ", pf"
	FlagPrintRawTime                          = "print_raw_time"
	FlagPrintRawTimeWithAlias                 = FlagPrintRawTime + ", prt"
	FlagPrintRaw                              = "print_raw"
	FlagPrintRawWithAlias                     = FlagPrintRaw + ", praw"
	FlagPrintDateTime                         = "print_datetime"
	FlagPrintDateTimeWithAlias                = FlagPrintDateTime + ", pdt"
	FlagPrintMemo                             = "print_memo"
	FlagPrintMemoWithAlias                    = FlagPrintMemo + ", pme"
	FlagPrintSearchAttr                       = "print_search_attr"
	FlagPrintSearchAttrWithAlias              = FlagPrintSearchAttr + ", psa"
	FlagPrintJSON                             = "print_json"
	FlagPrintJSONWithAlias                    = FlagPrintJSON + ", pjson"
	FlagDescription                           = "description"
	FlagDescriptionWithAlias                  = FlagDescription + ", desc"
	FlagOwnerEmail                            = "owner_email"
	FlagOwnerEmailWithAlias                   = FlagOwnerEmail + ", oe"
	FlagRetention                             = "retention"
	FlagRetentionWithAlias                    = FlagRetention + ", rd"
	FlagHistoryArchivalState                  = "history_archival_state"
	FlagHistoryArchivalStateWithAlias         = FlagHistoryArchivalState + ", has"
	FlagHistoryArchivalURI                    = "history_uri"
	FlagHistoryArchivalURIWithAlias           = FlagHistoryArchivalURI + ", huri"
	FlagHeartbeatedWithin                     = "heartbeated_within"
	FlagVisibilityArchivalState               = "visibility_archival_state"
	FlagVisibilityArchivalStateWithAlias      = FlagVisibilityArchivalState + ", vas"
	FlagVisibilityArchivalURI                 = "visibility_uri"
	FlagVisibilityArchivalURIWithAlias        = FlagVisibilityArchivalURI + ", vuri"
	FlagName                                  = "name"
	FlagNameWithAlias                         = FlagName + ", n"
	FlagOutputFilename                        = "output_filename"
	FlagOutputFilenameWithAlias               = FlagOutputFilename + ", of"
	FlagOutputFormat                          = "output"
	FlagQueryType                             = "query_type"
	FlagQueryTypeWithAlias                    = FlagQueryType + ", qt"
	FlagQueryRejectCondition                  = "query_reject_condition"
	FlagQueryRejectConditionWithAlias         = FlagQueryRejectCondition + ", qrc"
	FlagShowDetail                            = "show_detail"
	FlagShowDetailWithAlias                   = FlagShowDetail + ", sd"
	FlagActiveClusterName                     = "active_cluster"
	FlagActiveClusterNameWithAlias            = FlagActiveClusterName + ", ac"
	FlagClusters                              = "clusters"
	FlagClustersWithAlias                     = FlagClusters + ", cl"
	FlagClusterMembershipRole                 = "role"
	FlagIsGlobalNamespace                     = "global_namespace"
	FlagIsGlobalNamespaceWithAlias            = FlagIsGlobalNamespace + ", gd"
	FlagPromoteNamespace                      = "promote_namespace"
	FlagPromoteNamespaceWithAlias             = FlagPromoteNamespace + ", pn"
	FlagNamespaceData                         = "namespace_data"
	FlagNamespaceDataWithAlias                = FlagNamespaceData + ", dmd"
	FlagEventID                               = "event_id"
	FlagEventIDWithAlias                      = FlagEventID + ", eid"
	FlagActivityID                            = "activity_id"
	FlagActivityIDWithAlias                   = FlagActivityID + ", aid"
	FlagMaxFieldLength                        = "max_field_length"
	FlagMaxFieldLengthWithAlias               = FlagMaxFieldLength + ", maxl"
	FlagSkipErrorMode                         = "skip_errors"
	FlagSkipErrorModeWithAlias                = FlagSkipErrorMode + ", serr"
	FlagElasticsearchURL                      = "url"
	FlagElasticsearchUsername                 = "es-username"
	FlagElasticsearchPassword                 = "es-password"
	FlagElasticsearchIndex                    = "index"
	FlagMemoKey                               = "memo_key"
	FlagMemo                                  = "memo"
	FlagMemoFile                              = "memo_file"
	FlagSearchAttributeKey                    = "search_attr_key"
	FlagSearchAttributeValue                  = "search_attr_value"
	FlagAddBadBinary                          = "add_bad_binary"
	FlagRemoveBadBinary                       = "remove_bad_binary"
	FlagResetType                             = "reset_type"
	FlagResetReapplyType                      = "reset_reapply_type"
	FlagResetPointsOnly                       = "reset_points_only"
	FlagResetBadBinaryChecksum                = "reset_bad_binary_checksum"
	FlagListQuery                             = "query"
	FlagListQueryWithAlias                    = FlagListQuery + ", q"
	FlagBatchType                             = "batch_type"
	FlagBatchTypeWithAlias                    = FlagBatchType + ", bt"
	FlagSignalName                            = "signal_name"
	FlagSignalNameWithAlias                   = FlagSignalName + ", sig"
	FlagTaskID                                = "task_id"
	FlagTaskType                              = "task_type"
	FlagMinTaskID                             = "min_task_id"
	FlagMaxTaskID                             = "max_task_id"
	FlagTaskVisibilityTimestamp               = "task_timestamp"
	FlagMinVisibilityTimestamp                = "min_visibility_ts"
	FlagMaxVisibilityTimestamp                = "max_visibility_ts"
	FlagStartingRPS                           = "starting_rps"
	FlagRPS                                   = "rps"
	FlagJobID                                 = "job_id"
	FlagJobIDWithAlias                        = FlagJobID + ", jid"
	FlagYes                                   = "yes"
	FlagServiceConfigDir                      = "service_config_dir"
	FlagServiceConfigDirWithAlias             = FlagServiceConfigDir + ", scd"
	FlagServiceEnv                            = "service_env"
	FlagServiceEnvWithAlias                   = FlagServiceEnv + ", se"
	FlagServiceZone                           = "service_zone"
	FlagServiceZoneWithAlias                  = FlagServiceZone + ", sz"
	FlagEnableTLS                             = "tls"
	FlagTLSCertPath                           = "tls_cert_path"
	FlagTLSKeyPath                            = "tls_key_path"
	FlagTLSCaPath                             = "tls_ca_path"
	FlagTLSDisableHostVerification            = "tls_disable_host_verification"
	FlagTLSServerName                         = "tls_server_name"
	FlagTLSRootCaData                         = "tls_ca_data"
	FlagTLSForceEnable                        = "tls_force_enable"
	FlagDLQType                               = "dlq_type"
	FlagDLQTypeWithAlias                      = FlagDLQType + ", dt"
	FlagMaxMessageCount                       = "max_message_count"
	FlagMaxMessageCountWithAlias              = FlagMaxMessageCount + ", mmc"
	FlagLastMessageID                         = "last_message_id"
	FlagConcurrency                           = "concurrency"
	FlagReportRate                            = "report_rate"
	FlagLowerShardBound                       = "lower_shard_bound"
	FlagUpperShardBound                       = "upper_shard_bound"
	FlagInputDirectory                        = "input_directory"
	FlagAutoConfirm                           = "auto_confirm"
	FlagDataConverterPlugin                   = "data_converter_plugin"
	FlagDataConverterPluginWithAlias          = FlagDataConverterPlugin + ", dcp"
	FlagWebURL                                = "web_ui_url"
	FlagHeadersProviderPlugin                 = "headers_provider_plugin"
	FlagHeadersProviderPluginWithAlias        = FlagHeadersProviderPlugin + ", hpp"
	FlagHeadersProviderPluginOptions          = "headers_provider_plugin_options"
	FlagHeadersProviderPluginOptionsWithAlias = FlagHeadersProviderPluginOptions + ", hppo"
	FlagType                                  = "type"
	FlagTypeWithAlias                         = FlagType + ", t"
	FlagVersion                               = "version"
	FlagPort                                  = "port"
	FlagConnectionEnable                      = "enable_connection"
	FlagConnectionEnableWithAlias             = FlagConnectionEnable + ", ec"

	FlagProtoType  = "type"
	FlagHexData    = "hex_data"
	FlagHexFile    = "hex_file"
	FlagBinaryFile = "binary_file"
	FlagBase64Data = "base64_data"
	FlagBase64File = "base64_file"

	FlagSkipSchemaUpdate = "skip-schema-update"
)

var flagsForExecution = []cli.Flag{
	cli.StringFlag{
		Name:  FlagWorkflowIDWithAlias,
		Usage: "WorkflowId",
	},
	cli.StringFlag{
		Name:  FlagRunIDWithAlias,
		Usage: "RunId",
	},
}

var flagsForPagination = []cli.Flag{
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
			Name:  FlagTaskQueueWithAlias,
			Usage: "TaskQueue",
		},
		cli.StringFlag{
			Name:  FlagWorkflowIDWithAlias,
			Usage: "WorkflowId",
		},
		cli.StringFlag{
			Name:  FlagWorkflowTypeWithAlias,
			Usage: "WorkflowTypeName",
		},
		cli.IntFlag{
			Name:  FlagWorkflowExecutionTimeoutWithAlias,
			Usage: "Workflow execution timeout, including retries and continue-as-new (seconds)",
		},
		cli.IntFlag{
			Name:  FlagWorkflowRunTimeoutWithAlias,
			Usage: "Single workflow run timeout (seconds)",
		},
		cli.IntFlag{
			Name:  FlagWorkflowTaskTimeoutWithAlias,
			Value: defaultWorkflowTaskTimeoutInSeconds,
			Usage: "Workflow task start to close timeout (seconds)",
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
		cli.StringFlag{
			Name: FlagWorkflowIDReusePolicyAlias,
			Usage: "Configure if the same workflow Id is allowed for use in new workflow execution. " +
				"Options: AllowDuplicate, AllowDuplicateFailedOnly, RejectDuplicate",
		},
		cli.StringSliceFlag{
			Name: FlagInputWithAlias,
			Usage: "Optional input for the workflow in JSON format. If there are multiple parameters, pass each as a separate input flag. " +
				"Pass \"null\" for null values",
		},
		cli.StringFlag{
			Name: FlagInputFileWithAlias,
			Usage: "Optional input for the workflow from JSON file. If there are multiple JSON, concatenate them and separate by space or newline. " +
				"Input from file will be overwrite by input from command line",
		},
		cli.StringSliceFlag{
			Name:  FlagMemoKey,
			Usage: fmt.Sprintf("Optional key of memo. If there are multiple keys, provide multiple %s flags", FlagMemoKey),
		},
		cli.StringSliceFlag{
			Name: FlagMemo,
			Usage: fmt.Sprintf("Optional info that can be showed when list workflow. If there are multiple values, provide multiple %s flags. "+
				"The order must be same as %s", FlagMemo, FlagMemoKey),
		},
		cli.StringFlag{
			Name: FlagMemoFile,
			Usage: fmt.Sprintf("File name of optional info that can be showed when list workflow. If there are multiple values, separate them by newline. "+
				"The order of lines must be same as %s", FlagMemoKey),
		},
		cli.StringFlag{
			Name: FlagSearchAttributeKey,
			Usage: "Optional search attributes keys that can be be used in list query. If there are multiple keys, concatenate them and separate by |. " +
				"Use 'cluster get-search-attr' cmd to list legal keys.",
		},
		cli.StringFlag{
			Name: FlagSearchAttributeValue,
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
	flagsForList := append(getFlagsForListAll(), flagsForPagination...)
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
			Usage: "WorkflowId",
		},
		cli.StringFlag{
			Name:  FlagWorkflowTypeWithAlias,
			Usage: "WorkflowTypeName",
		},
		cli.StringFlag{
			Name:  FlagWorkflowStatusWithAlias,
			Usage: "Workflow status [completed, failed, canceled, terminated, continuedasnew, timedout]",
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
			Usage: "SQL like query. Please check the documentation of the visibility archiver used by your namespace for detailed instructions",
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
			Usage: "Optional SQL like query. e.g count all open workflows \"ExecutionStatus='Running'\"; 'WorkflowType=\"wtype\" and CloseTime > 0'",
		},
	}
}

func getFlagsForQuery() []cli.Flag {
	return []cli.Flag{
		cli.StringFlag{
			Name:  FlagWorkflowIDWithAlias,
			Usage: "WorkflowId",
		},
		cli.StringFlag{
			Name:  FlagRunIDWithAlias,
			Usage: "RunId",
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

func getDBFlags() []cli.Flag {
	return []cli.Flag{
		cli.StringFlag{
			Name:  FlagDBEngine,
			Value: "cassandra",
			Usage: "Type of the DB engine to use (cassandra, mysql, postgres..)",
		},
		cli.StringFlag{
			Name:  FlagDBAddress,
			Value: "127.0.0.1",
			Usage: "persistence address",
		},
		cli.IntFlag{
			Name:  FlagDBPort,
			Value: 9042,
			Usage: "persistence port",
		},
		cli.StringFlag{
			Name:  FlagUsername,
			Usage: "DB username",
		},
		cli.StringFlag{
			Name:  FlagPassword,
			Usage: "DB password",
		},
		cli.StringFlag{
			Name:  FlagKeyspace,
			Value: "temporal",
			Usage: "DB keyspace",
		},
		cli.BoolFlag{
			Name:  FlagEnableTLS,
			Usage: "enable TLS over the DB connection",
		},
		cli.StringFlag{
			Name:  FlagTLSCertPath,
			Usage: "DB tls client cert path (tls must be enabled)",
		},
		cli.StringFlag{
			Name:  FlagTLSKeyPath,
			Usage: "DB tls client key path (tls must be enabled)",
		},
		cli.StringFlag{
			Name:  FlagTLSCaPath,
			Usage: "DB tls client ca path (tls must be enabled)",
		},
		cli.StringFlag{
			Name:  FlagTLSServerName,
			Usage: "DB tls server name (tls must be enabled)",
		},
		cli.BoolFlag{
			Name:  FlagTLSDisableHostVerification,
			Usage: "DB tls verify hostname and server cert (tls must be enabled)",
		},
	}
}

func getESFlags(index bool) []cli.Flag {
	flags := []cli.Flag{
		cli.StringFlag{
			Name:  FlagElasticsearchURL,
			Value: "http://127.0.0.1:9200",
			Usage: "URL of Elasticsearch cluster",
		},
		cli.StringFlag{
			Name:  FlagElasticsearchUsername,
			Value: "",
			Usage: "Username for Elasticsearch cluster",
		},
		cli.StringFlag{
			Name:  FlagElasticsearchPassword,
			Value: "",
			Usage: "Password for Elasticsearch cluster",
		},
		cli.StringFlag{
			Name:  FlagVersion,
			Value: "v7",
			Usage: "Version of Elasticsearch cluster: v6 or v7 (default)",
		},
	}
	if index {
		flags = append(flags,
			cli.StringFlag{
				Name:  FlagElasticsearchIndex,
				Usage: "Elasticsearch index name",
			},
		)
	}
	return flags
}

func getDBAndESFlags() []cli.Flag {
	return append(getDBFlags(), getESFlags(true)...)
}
