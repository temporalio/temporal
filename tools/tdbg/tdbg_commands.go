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

package tdbg

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/urfave/cli/v2"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/multierr"
)

func getCommands(
	clientFactory ClientFactory,
	dlqServiceProvider *DLQServiceProvider,
	taskCategoryRegistry tasks.TaskCategoryRegistry,
	prompterFactory PrompterFactory,
	taskBlobEncoder TaskBlobEncoder,
) []*cli.Command {
	return []*cli.Command{
		{
			Name:        "workflow",
			Aliases:     []string{"w"},
			Usage:       "Run admin operation on workflow",
			Subcommands: newAdminWorkflowCommands(clientFactory, prompterFactory),
		},
		{
			Name:        "shard",
			Aliases:     []string{"s"},
			Usage:       "Run admin operation on specific shard",
			Subcommands: newAdminShardManagementCommands(clientFactory, taskCategoryRegistry),
		},
		{
			Name:        "history-host",
			Aliases:     []string{"h"},
			Usage:       "Run admin operation on history host",
			Subcommands: newAdminHistoryHostCommands(clientFactory),
		},
		{
			Name:        "taskqueue",
			Aliases:     []string{"tq"},
			Usage:       "Run admin operation on taskQueue",
			Subcommands: newAdminTaskQueueCommands(clientFactory),
		},
		{
			Name:        "membership",
			Aliases:     []string{"m"},
			Usage:       "Run admin operation on membership",
			Subcommands: newAdminMembershipCommands(clientFactory),
		},
		{
			Name:        "dlq",
			Usage:       "Run admin operation on DLQ",
			Subcommands: newAdminDLQCommands(dlqServiceProvider, taskCategoryRegistry),
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  FlagDLQVersion,
					Usage: "Version of DLQ to manage, options: v1, v2",
					Value: "v1",
				},
			},
		},
		{
			Name:        "decode",
			Usage:       "Decode payload",
			Subcommands: newDecodeCommands(taskBlobEncoder),
		},
	}
}

func newAdminWorkflowCommands(clientFactory ClientFactory, prompterFactory PrompterFactory) []*cli.Command {
	return []*cli.Command{
		{
			Name:  "import",
			Usage: "import workflow history to database",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    FlagWorkflowID,
					Aliases: FlagWorkflowIDAlias,
					Usage:   "Workflow ID",
				},
				&cli.StringFlag{
					Name:    FlagRunID,
					Aliases: FlagRunIDAlias,
					Usage:   "Run ID",
				},
				&cli.StringFlag{
					Name:  FlagInputFilename,
					Usage: "input file",
				}},
			Action: func(c *cli.Context) error {
				return AdminImportWorkflow(c, clientFactory)
			},
		},
		{
			Name:  "show",
			Usage: "show workflow history from database",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    FlagWorkflowID,
					Aliases: FlagWorkflowIDAlias,
					Usage:   "Workflow ID",
				},
				&cli.StringFlag{
					Name:    FlagRunID,
					Aliases: FlagRunIDAlias,
					Usage:   "Run ID",
				},
				&cli.Int64Flag{
					Name:  FlagMinEventID,
					Usage: "Minimum event ID to be included in the history",
				},
				&cli.Int64Flag{
					Name:  FlagMaxEventID,
					Usage: "Maximum event ID to be included in the history",
					Value: 1<<63 - 1,
				},
				&cli.Int64Flag{
					Name:  FlagMinEventVersion,
					Usage: "Start event version to be included in the history",
				},
				&cli.Int64Flag{
					Name:  FlagMaxEventVersion,
					Usage: "End event version to be included in the history",
				},
				&cli.StringFlag{
					Name:  FlagOutputFilename,
					Usage: "output file",
				}},
			Action: func(c *cli.Context) error {
				return AdminShowWorkflow(c, clientFactory)
			},
		},
		{
			Name:    "describe",
			Aliases: []string{"d"},
			Usage:   "Describe internal information of workflow execution",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    FlagWorkflowID,
					Aliases: FlagWorkflowIDAlias,
					Usage:   "Workflow ID",
				},
				&cli.StringFlag{
					Name:    FlagRunID,
					Aliases: FlagRunIDAlias,
					Usage:   "Run ID",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminDescribeWorkflow(c, clientFactory)
			},
		},
		{
			Name:    "refresh-tasks",
			Aliases: []string{"rt"},
			Usage:   "Refreshes all the tasks of a workflow",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    FlagWorkflowID,
					Aliases: FlagWorkflowIDAlias,
					Usage:   "Workflow ID",
				},
				&cli.StringFlag{
					Name:    FlagRunID,
					Aliases: FlagRunIDAlias,
					Usage:   "Run ID",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminRefreshWorkflowTasks(c, clientFactory)
			},
		},
		{
			Name:    "rebuild",
			Aliases: []string{},
			Usage:   "Rebuild a workflow mutable state using persisted history events",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    FlagWorkflowID,
					Aliases: FlagWorkflowIDAlias,
					Usage:   "Workflow ID",
				},
				&cli.StringFlag{
					Name:    FlagRunID,
					Aliases: FlagRunIDAlias,
					Usage:   "Run ID",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminRebuildMutableState(c, clientFactory)
			},
		},
		{
			Name:    "delete",
			Aliases: []string{"del"},
			Usage:   "Delete current workflow execution and the mutableState record",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    FlagWorkflowID,
					Aliases: FlagWorkflowIDAlias,
					Usage:   "Workflow ID",
				},
				&cli.StringFlag{
					Name:    FlagRunID,
					Aliases: FlagRunIDAlias,
					Usage:   "Run ID",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminDeleteWorkflow(c, clientFactory, prompterFactory(c))
			},
		},
	}
}

func newAdminShardManagementCommands(clientFactory ClientFactory, taskCategoryRegistry tasks.TaskCategoryRegistry) []*cli.Command {
	// There are two different flags for the task type, and they have slightly different semantics. The first is the
	// task type for the list-tasks command, which is required and does not have a default. The second is the task type
	// for the remove-task command, which is optional and defaults to transfer.
	taskTypeFlag := getTaskTypeFlag(taskCategoryRegistry)
	listTasksCategory := *taskTypeFlag
	listTasksCategory.Required = true
	removeTaskCategory := *taskTypeFlag
	removeTaskCategory.Value = tasks.CategoryTransfer.Name()

	return []*cli.Command{
		{
			Name:    "describe",
			Aliases: []string{"d"},
			Usage:   "Describe shard by ID",
			Flags: []cli.Flag{
				&cli.IntFlag{
					Name:  FlagShardID,
					Usage: "The ID of the shard to describe",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminDescribeShard(c, clientFactory)
			},
		},
		{
			Name:  "list-tasks",
			Usage: "List tasks for given shard ID and task type",
			Flags: []cli.Flag{
				&cli.BoolFlag{
					Name:  FlagMore,
					Usage: "List more pages, default is to list one page of default page size 10",
				},
				&cli.IntFlag{
					Name:  FlagPageSize,
					Value: defaultPageSize,
					Usage: "Result page size",
				},
				&cli.IntFlag{
					Name:     FlagShardID,
					Usage:    "The ID of the shard",
					Required: true,
				},
				&listTasksCategory,
				&cli.Int64Flag{
					Name:  FlagMinTaskID,
					Usage: "Inclusive min taskID. Optional for transfer, replication, visibility tasks. Can't be specified for timer task",
				},
				&cli.Int64Flag{
					Name:  FlagMaxTaskID,
					Usage: "Exclusive max taskID. Required for transfer, replication, visibility tasks. Can't be specified for timer task",
				},
				&cli.StringFlag{
					Name: FlagMinVisibilityTimestamp,
					Usage: "Inclusive min task fire timestamp. Optional for timer task. Can't be specified for transfer, replication, visibility tasks. " +
						"Supported formats are '2006-01-02T15:04:05+07:00', raw UnixNano and " +
						"time range (N<duration>), where 0 < N < 1000000 and duration (full-notation/short-notation) can be second/s, " +
						"minute/m, hour/h, day/d, week/w, month/M or year/y. For example, '15minute' or '15m' implies last 15 minutes.",
				},
				&cli.StringFlag{
					Name: FlagMaxVisibilityTimestamp,
					Usage: "Exclusive max task fire timestamp. Required for timer task. Can't be specified for transfer, replication, visibility tasks. " +
						"Supported formats are '2006-01-02T15:04:05+07:00', raw UnixNano and " +
						"time range (N<duration>), where 0 < N < 1000000 and duration (full-notation/short-notation) can be second/s, " +
						"minute/m, hour/h, day/d, week/w, month/M or year/y. For example, '15minute' or '15m' implies last 15 minutes.",
				},
				&cli.BoolFlag{
					Name:  FlagPrintJSON,
					Value: true,
					Usage: "Print in raw json format",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminListShardTasks(c, clientFactory, taskCategoryRegistry)
			},
		},
		{
			Name:  "close-shard",
			Usage: "close a shard given a shard id",
			Flags: []cli.Flag{
				&cli.IntFlag{
					Name:  FlagShardID,
					Usage: "ShardId for the temporal cluster to manage",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminShardManagement(c, clientFactory)
			},
		},
		{
			Name:    "remove-task",
			Aliases: []string{"rmtk"},
			Usage:   "remove a task based on shardId, task type, taskId, and task visibility timestamp",
			Flags: []cli.Flag{
				&cli.IntFlag{
					Name:  FlagShardID,
					Usage: "shardId",
				},
				&cli.Int64Flag{
					Name:  FlagTaskID,
					Usage: "taskId",
				},
				&removeTaskCategory,
				&cli.Int64Flag{
					Name:  FlagTaskVisibilityTimestamp,
					Usage: "task visibility timestamp in nano (required for removing timer task)",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminRemoveTask(c, clientFactory, taskCategoryRegistry)
			},
		},
	}
}

func getTaskTypeFlag(taskCategoryRegistry tasks.TaskCategoryRegistry) *cli.StringFlag {
	categories := taskCategoryRegistry.GetCategories()
	options := make([]string, 0, len(categories))
	for _, category := range categories {
		options = append(options, category.Name())
	}
	flag := &cli.StringFlag{
		Name:  FlagTaskType,
		Usage: "Task type: " + strings.Join(options, ", "),
	}
	return flag
}

func newAdminMembershipCommands(clientFactory ClientFactory) []*cli.Command {
	return []*cli.Command{
		{
			Name:  "list-gossip",
			Usage: "List ringpop membership items",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  FlagClusterMembershipRole,
					Value: "all",
					Usage: "Membership role filter: all (default), frontend, history, matching, worker",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminListGossipMembers(c, clientFactory)
			},
		},
		{
			Name:  "list-db",
			Usage: "List cluster membership items",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  FlagHeartbeatedWithin,
					Value: "15m",
					Usage: "Filter by last heartbeat date time. Supported formats are '2006-01-02T15:04:05+07:00', raw UnixNano and " +
						"time range (N<duration>), where 0 < N < 1000000 and duration (full-notation/short-notation) can be second/s, " +
						"minute/m, hour/h, day/d, week/w, month/M or year/y. For example, '15minute' or '15m' implies last 15 minutes.",
				},
				&cli.StringFlag{
					Name:  FlagClusterMembershipRole,
					Value: "all",
					Usage: "Membership role filter: all (default), frontend, history, matching, worker",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminListClusterMembers(c, clientFactory)
			},
		},
	}
}

func newAdminHistoryHostCommands(clientFactory ClientFactory) []*cli.Command {
	return []*cli.Command{
		{
			Name:    "describe",
			Aliases: []string{"d"},
			Usage:   "Describe internal information of history host",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    FlagWorkflowID,
					Aliases: FlagWorkflowIDAlias,
					Usage:   "Workflow ID",
				},
				&cli.StringFlag{
					Name:  FlagHistoryAddress,
					Usage: "History Host address(IP:PORT)",
				},
				&cli.IntFlag{
					Name:  FlagShardID,
					Usage: "ShardId",
				},
				&cli.BoolFlag{
					Name:  FlagPrintFullyDetail,
					Usage: "Print fully detail",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminDescribeHistoryHost(c, clientFactory)
			},
		},
		{
			Name:  "get-shardid",
			Usage: "Get shardId for a namespaceId and workflowId combination",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  FlagNamespaceID,
					Usage: "NamespaceId",
				},
				&cli.StringFlag{
					Name:    FlagWorkflowID,
					Aliases: FlagWorkflowIDAlias,
					Usage:   "Workflow ID",
				},
				&cli.IntFlag{
					Name:  FlagNumberOfShards,
					Usage: "NumberOfShards for the temporal cluster(see config for numHistoryShards)",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminGetShardID(c)
			},
		},
	}
}

func newAdminTaskQueueCommands(clientFactory ClientFactory) []*cli.Command {
	return []*cli.Command{
		{
			Name:  "list-tasks",
			Usage: "List tasks of a task queue",
			Flags: []cli.Flag{
				&cli.BoolFlag{
					Name:  FlagMore,
					Usage: "List more pages, default is to list one page of default page size 10",
				},
				&cli.IntFlag{
					Name:  FlagPageSize,
					Value: 10,
					Usage: "Result page size",
				},
				&cli.StringFlag{
					Name:  FlagTaskQueueType,
					Value: "activity",
					Usage: "Task Queue type: activity, workflow",
				},
				&cli.StringFlag{
					Name:  FlagTaskQueue,
					Usage: "Task Queue name",
				},
				&cli.Int64Flag{
					Name:  FlagMinTaskID,
					Usage: "Minimum task ID",
					Value: -12346, // include default task id
				},
				&cli.Int64Flag{
					Name:  FlagMaxTaskID,
					Usage: "Maximum task ID",
				},
				&cli.BoolFlag{
					Name:  FlagPrintJSON,
					Usage: "Print in raw json format",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminListTaskQueueTasks(c, clientFactory)
			},
		},
	}
}

func newAdminDLQCommands(
	dlqServiceProvider *DLQServiceProvider,
	taskCategoryRegistry tasks.TaskCategoryRegistry,
) []*cli.Command {
	return []*cli.Command{
		{
			Name:    "read",
			Aliases: []string{"r"},
			Usage:   "Read DLQ Messages",
			Flags: append(
				getDLQFlags(taskCategoryRegistry),
				&cli.IntFlag{
					Name: FlagMaxMessageCount,
					Usage: fmt.Sprintf(
						"Max message size to fetch, defaults to %d for v2 and nothing for v1",
						dlqV2DefaultMaxMessageCount,
					),
				},
				&cli.StringFlag{
					Name:  FlagOutputFilename,
					Usage: "Output file to write to, if not provided output is written to stdout",
				},
				&cli.IntFlag{
					Name:  FlagPageSize,
					Usage: "Page size to use when reading messages from the DB, v2 only",
					Value: defaultPageSize,
				},
			),
			Action: func(c *cli.Context) error {
				ac, err := dlqServiceProvider.GetDLQService(c)
				if err != nil {
					return err
				}
				return ac.ReadMessages(c)
			},
		},
		{
			Name:    "purge",
			Aliases: []string{"p"},
			Usage:   "Delete DLQ messages with equal or smaller ids than the provided task id",
			Flags:   getDLQFlags(taskCategoryRegistry),
			Action: func(c *cli.Context) error {
				ac, err := dlqServiceProvider.GetDLQService(c)
				if err != nil {
					return err
				}
				return ac.PurgeMessages(c)
			},
		},
		{
			Name:        "merge",
			Aliases:     []string{"m"},
			Usage:       "Merge DLQ messages with equal or smaller ids than the provided task id",
			Description: "This command will delete messages after they've been re-enqueued if using v2.",
			Flags: append(getDLQFlags(taskCategoryRegistry),
				&cli.IntFlag{
					Name: FlagPageSize,
					Usage: "Batch size to use when purging messages from the DB, v2 only. Will use server default if " +
						"not provided.",
				},
			),
			Action: func(c *cli.Context) error {
				ac, err := dlqServiceProvider.GetDLQService(c)
				if err != nil {
					return err
				}
				return ac.MergeMessages(c)
			},
		},
		{
			Name:    "list",
			Aliases: []string{"l"},
			Usage:   "List all DLQs, only for v2",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  FlagOutputFilename,
					Usage: "Output file to write to, if not provided output is written to stdout",
				},
				&cli.IntFlag{
					Name:  FlagPageSize,
					Usage: "Page size to use when listing queues from the DB",
					Value: defaultPageSize,
				},
				&cli.BoolFlag{
					Name:  FlagPrintJSON,
					Usage: "Print in raw json format",
				},
			},
			Action: func(c *cli.Context) error {
				ac, err := dlqServiceProvider.GetDLQService(c)
				if err != nil {
					return err
				}
				return ac.ListQueues(c)
			},
		},
		{
			Name:        "job",
			Usage:       "Run admin operation on DLQ Job",
			Subcommands: newAdminDLQJobCommands(dlqServiceProvider),
		},
	}
}

func newAdminDLQJobCommands(
	dlqServiceProvider *DLQServiceProvider,
) []*cli.Command {
	return []*cli.Command{
		{
			Name:        "describe",
			Aliases:     []string{"d"},
			Usage:       "Get details of the DLQ job with provided job token",
			Description: "This command will get details of the DLQ job with provided job token if using v2",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:     FlagJobToken,
					Usage:    "Token of the DLQ job. This token will be printed in the output of merge and purge commands",
					Required: true,
				},
			},
			Action: func(c *cli.Context) error {
				ac := dlqServiceProvider.GetDLQJobService()
				return ac.DescribeJob(c)
			},
		},
		{
			Name:        "cancel",
			Aliases:     []string{"c"},
			Usage:       "Cancel the DLQ job with provided job token",
			Description: "This command will cancel the DLQ job with provided job token",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:     FlagJobToken,
					Usage:    "Token of the DLQ job. This token will be printed in the output of merge and purge commands",
					Required: true,
				},
				&cli.StringFlag{
					Name:     FlagReason,
					Usage:    "Reason for job cancellation",
					Required: true,
				},
			},
			Action: func(c *cli.Context) error {
				ac := dlqServiceProvider.GetDLQJobService()
				return ac.CancelJob(c)
			},
		},
	}
}

func getDLQFlags(taskCategoryRegistry tasks.TaskCategoryRegistry) []cli.Flag {
	categoriesString := getCategoriesList(taskCategoryRegistry)
	return []cli.Flag{
		&cli.StringFlag{
			Name: FlagDLQType,
			Usage: fmt.Sprintf(
				"Type of DLQ to manage, options: namespace, history for v1; %s for v2",
				categoriesString,
			),
		},
		&cli.StringFlag{
			Name:  FlagCluster,
			Usage: "Source cluster",
		},
		&cli.IntFlag{
			Name:  FlagShardID,
			Usage: "ShardId, v1 only",
		},
		&cli.IntFlag{
			Name: FlagLastMessageID,
			Usage: "The upper boundary of messages to operate on. If not provided, all messages will be operated on. " +
				"However, you will be prompted for confirmation unless the --yes flag is also provided.",
		},
		&cli.StringFlag{
			Name:  FlagTargetCluster,
			Usage: "Target cluster, v2 only. If not provided, current cluster is used.",
		},
	}
}

func newDecodeCommands(
	taskBlobEncoder TaskBlobEncoder,
) []*cli.Command {
	return []*cli.Command{
		{
			Name:  "proto",
			Usage: "Decode proto payload",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  FlagProtoType,
					Usage: "full name of proto type to decode to (i.e. temporal.server.api.persistence.v1.WorkflowExecutionInfo).",
				},
				&cli.StringFlag{
					Name:  FlagHexData,
					Usage: "data in hex format (i.e. 0x0a243462613036633466...).",
				},
				&cli.StringFlag{
					Name:  FlagHexFile,
					Usage: "file with data in hex format (i.e. 0x0a243462613036633466...).",
				},
				&cli.StringFlag{
					Name:  FlagBinaryFile,
					Usage: "file with data in binary format.",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminDecodeProto(c)
			},
		},
		{
			Name:  "base64",
			Usage: "Decode base64 payload",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  FlagBase64Data,
					Usage: "data in base64 format (i.e. anNvbi9wbGFpbg==).",
				},
				&cli.StringFlag{
					Name:  FlagBase64File,
					Usage: "file with data in base64 format (i.e. anNvbi9wbGFpbg==).",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminDecodeBase64(c)
			},
		},
		{
			Name:  "task",
			Usage: "Decode a history task blob into a JSON message.",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:     FlagBinaryFile,
					Usage:    "file with data in binary format.",
					Required: true,
				},
				&cli.IntFlag{
					Name:     FlagTaskCategoryID,
					Usage:    "Task category ID (see the history/tasks package)",
					Required: true,
				},
				&cli.StringFlag{
					Name:     FlagEncoding,
					Usage:    "Encoding type (see temporal.api.enums.v1.EncodingType)",
					Required: true,
				},
			},
			Action: func(c *cli.Context) (err error) {
				encoding := c.String(FlagEncoding)
				encodingType, err := enumspb.EncodingTypeFromString(encoding)
				if err != nil {
					return err
				}
				taskCategoryID := c.Int(FlagTaskCategoryID)
				file, err := os.Open(c.String(FlagBinaryFile))
				if err != nil {
					return fmt.Errorf("failed to open file: %w", err)
				}
				defer func() {
					err = multierr.Combine(err, file.Close())
				}()
				b, err := io.ReadAll(file)
				if err != nil {
					return fmt.Errorf("failed to read file: %w", err)
				}
				blob := commonpb.DataBlob{
					EncodingType: encodingType,
					Data:         b,
				}
				if err := taskBlobEncoder.Encode(c.App.Writer, taskCategoryID, &blob); err != nil {
					return fmt.Errorf("failed to decode task blob: %w", err)
				}
				return nil
			},
		},
	}
}
