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

func newAdminWorkflowCommands() []cli.Command {
	return []cli.Command{
		{
			Name:    "show",
			Aliases: []string{"show"},
			Usage:   "show workflow history from database",
			Flags: []cli.Flag{
				// v1 history events
				cli.StringFlag{
					Name:  FlagDomainID,
					Usage: "DomainID",
				},
				cli.StringFlag{
					Name:  FlagWorkflowIDWithAlias,
					Usage: "WorkflowID",
				},
				cli.StringFlag{
					Name:  FlagRunIDWithAlias,
					Usage: "RunID",
				},
				// v2 history events
				cli.StringFlag{
					Name:  FlagTreeID,
					Usage: "TreeID",
				},
				cli.StringFlag{
					Name:  FlagBranchID,
					Usage: "BranchID",
				},
				cli.StringFlag{
					Name:  FlagOutputFilenameWithAlias,
					Usage: "output file",
				},

				// for cassandra connection
				cli.StringFlag{
					Name:  FlagAddress,
					Usage: "cassandra host address",
				},
				cli.IntFlag{
					Name:  FlagPort,
					Usage: "cassandra port for the host (default is 9042)",
				},
				cli.StringFlag{
					Name:  FlagUsername,
					Usage: "cassandra username",
				},
				cli.StringFlag{
					Name:  FlagPassword,
					Usage: "cassandra password",
				},
				cli.StringFlag{
					Name:  FlagKeyspace,
					Usage: "cassandra keyspace",
				},
			},
			Action: func(c *cli.Context) {
				AdminShowWorkflow(c)
			},
		},
		{
			Name:    "describe",
			Aliases: []string{"desc"},
			Usage:   "Describe internal information of workflow execution",
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
				AdminDescribeWorkflow(c)
			},
		},
		{
			Name:    "delete",
			Aliases: []string{"del"},
			Usage:   "Delete current workflow execution and the mutableState record",
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
					Name:  FlagDomainID,
					Usage: "DomainID",
				},
				cli.IntFlag{
					Name:  FlagShardID,
					Usage: "ShardID",
				},

				// for cassandra connection
				cli.StringFlag{
					Name:  FlagAddress,
					Usage: "cassandra host address",
				},
				cli.IntFlag{
					Name:  FlagPort,
					Usage: "cassandra port for the host (default is 9042)",
				},
				cli.StringFlag{
					Name:  FlagUsername,
					Usage: "cassandra username",
				},
				cli.StringFlag{
					Name:  FlagPassword,
					Usage: "cassandra password",
				},
				cli.StringFlag{
					Name:  FlagKeyspace,
					Usage: "cassandra keyspace",
				},
			},
			Action: func(c *cli.Context) {
				AdminDeleteWorkflow(c)
			},
		},
	}
}

func newAdminHistoryHostCommands() []cli.Command {
	return []cli.Command{
		{
			Name:    "describe",
			Aliases: []string{"desc"},
			Usage:   "Describe internal information of history host",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  FlagWorkflowIDWithAlias,
					Usage: "WorkflowID",
				},
				cli.StringFlag{
					Name:  FlagHistoryAddressWithAlias,
					Usage: "History Host address(IP:PORT)",
				},
				cli.IntFlag{
					Name:  FlagShardIDWithAlias,
					Usage: "ShardID",
				},
				cli.BoolFlag{
					Name:  FlagPrintFullyDetailWithAlias,
					Usage: "Print fully detail",
				},
			},
			Action: func(c *cli.Context) {
				AdminDescribeHistoryHost(c)
			},
		},
		{
			Name:    "getshard",
			Aliases: []string{"gsh"},
			Usage:   "Get shardID for a workflowID",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  FlagWorkflowIDWithAlias,
					Usage: "WorkflowID",
				},
				cli.IntFlag{
					Name:  FlagNumberOfShards,
					Usage: "NumberOfShards for the cadence cluster(see config for numHistoryShards)",
				},
			},
			Action: func(c *cli.Context) {
				AdminGetShardID(c)
			},
		},
	}
}

func newAdminDomainCommands() []cli.Command {
	return []cli.Command{
		{
			Name:    "getdomainidorname",
			Aliases: []string{"getdn"},
			Usage:   "Get domainID or domainName",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  FlagDomain,
					Usage: "DomainName",
				},
				cli.StringFlag{
					Name:  FlagDomainID,
					Usage: "Domain ID(uuid)",
				},

				// for cassandra connection
				cli.StringFlag{
					Name:  FlagAddress,
					Usage: "cassandra host address",
				},
				cli.IntFlag{
					Name:  FlagPort,
					Usage: "cassandra port for the host (default is 9042)",
				},
				cli.StringFlag{
					Name:  FlagUsername,
					Usage: "cassandra username",
				},
				cli.StringFlag{
					Name:  FlagPassword,
					Usage: "cassandra password",
				},
				cli.StringFlag{
					Name:  FlagKeyspace,
					Usage: "cassandra keyspace",
				},
			},
			Action: func(c *cli.Context) {
				AdminGetDomainIDOrName(c)
			},
		},
		// TODO: remove this command and add archival config options to domains.go once archival is finished
		{
			Name:    "register",
			Aliases: []string{"re"},
			Usage:   "Register workflow domain",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  FlagDescriptionWithAlias,
					Usage: "Domain description",
				},
				cli.StringFlag{
					Name:  FlagOwnerEmailWithAlias,
					Usage: "Owner email",
				},
				cli.StringFlag{
					Name:  FlagRetentionDaysWithAlias,
					Usage: "Workflow execution retention in days",
				},
				cli.StringFlag{
					Name:  FlagEmitMetricWithAlias,
					Usage: "Flag to emit metric",
				},
				cli.StringFlag{
					Name:  FlagActiveClusterNameWithAlias,
					Usage: "Active cluster name",
				},
				cli.StringFlag{ // use StringFlag instead of buggy StringSliceFlag
					Name:  FlagClustersWithAlias,
					Usage: "Clusters",
				},
				cli.StringFlag{
					Name:  FlagDomainDataWithAlias,
					Usage: "Domain data of key value pairs, in format of k1:v1,k2:v2,k3:v3",
				},
				cli.StringFlag{
					Name:  FlagSecurityTokenWithAlias,
					Usage: "Security token with permission",
				},
				cli.StringFlag{
					Name:  FlagArchivalStatusWithAlias,
					Usage: "Flag to set archival status, valid values are: {never_enabled, disabled, enabled}",
				},
				cli.StringFlag{
					Name:  FlagArchivalBucketNameWithAlias,
					Usage: "Optionally specify bucket (cannot be changed after first time status is set to disabled or enabled)",
				},
			},
			Action: func(c *cli.Context) {
				AdminRegisterDomain(c)
			},
		},
		// TODO: remove this command and add archival config options to domains.go once archival is finished
		{
			Name:    "update",
			Aliases: []string{"up", "u"},
			Usage:   "Update existing workflow domain",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  FlagDescriptionWithAlias,
					Usage: "Domain description",
				},
				cli.StringFlag{
					Name:  FlagOwnerEmailWithAlias,
					Usage: "Owner email",
				},
				cli.StringFlag{
					Name:  FlagRetentionDaysWithAlias,
					Usage: "Workflow execution retention in days",
				},
				cli.StringFlag{
					Name:  FlagEmitMetricWithAlias,
					Usage: "Flag to emit metric",
				},
				cli.StringFlag{
					Name:  FlagActiveClusterNameWithAlias,
					Usage: "Active cluster name",
				},
				cli.StringFlag{ // use StringFlag instead of buggy StringSliceFlag
					Name:  FlagClustersWithAlias,
					Usage: "Clusters",
				},
				cli.StringFlag{
					Name:  FlagDomainDataWithAlias,
					Usage: "Domain data of key value pairs, in format of k1:v1,k2:v2,k3:v3 ",
				},
				cli.StringFlag{
					Name:  FlagSecurityTokenWithAlias,
					Usage: "Security token with permission ",
				},
				cli.StringFlag{
					Name:  FlagArchivalStatusWithAlias,
					Usage: "Flag to set archival status, valid values are: {never_enabled, disabled, enabled}",
				},
				cli.StringFlag{
					Name:  FlagArchivalBucketNameWithAlias,
					Usage: "Optionally specify bucket (cannot be changed after first time status is set to disabled or enabled)",
				},
			},
			Action: func(c *cli.Context) {
				AdminUpdateDomain(c)
			},
		},
		// TODO: remove this command and add archival config options to domains.go once archival is finished
		{
			Name:    "describe",
			Aliases: []string{"desc"},
			Usage:   "Describe existing workflow domain",
			Action: func(c *cli.Context) {
				AdminDescribeDomain(c)
			},
		},
	}
}

func newAdminKafkaCommands() []cli.Command {
	return []cli.Command{
		{
			Name:    "parse",
			Aliases: []string{"par"},
			Usage:   "Parse replication tasks from kafka messages",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  FlagInputFileWithAlias,
					Usage: "Input file to use, if not present assumes piping",
				},
				cli.StringFlag{
					Name:  FlagWorkflowIDWithAlias,
					Usage: "WorkflowID, if not provided then no filters by WorkflowID are applied",
				},
				cli.StringFlag{
					Name:  FlagRunIDWithAlias,
					Usage: "RunID, if not provided then no filters by RunID are applied",
				},
				cli.StringFlag{
					Name:  FlagOutputFilenameWithAlias,
					Usage: "Output file to write to, if not provided output is written to stdout",
				},
				cli.BoolFlag{
					Name:  FlagSkipErrorModeWithAlias,
					Usage: "Skip errors in parsing messages",
				},
				cli.BoolFlag{
					Name:  FlagHeadersModeWithAlias,
					Usage: "Output headers of messages in format: DomainID, WorkflowID, RunID, FirstEventID, NextEventID",
				},
				cli.IntFlag{
					Name:  FlagMessageTypeWithAlias,
					Usage: "Kafka message type (0: replicationTasks; 1: visibility)",
					Value: 0,
				},
			},
			Action: func(c *cli.Context) {
				AdminKafkaParse(c)
			},
		},
		{
			Name:    "purgeTopic",
			Aliases: []string{"purge"},
			Usage:   "purge Kafka topic by consumer group",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  FlagCluster,
					Usage: "Name of the Kafka cluster to publish replicationTasks",
				},
				cli.StringFlag{
					Name:  FlagTopic,
					Usage: "Topic to publish replication task",
				},
				cli.StringFlag{
					Name:  FlagGroup,
					Usage: "Group to read DLQ",
				},
				cli.StringFlag{
					Name: FlagHostFile,
					Usage: "Kafka host config file in format of: " + `
clusters:
	localKafka:
		brokers:
		- 127.0.0.1
		- 127.0.0.2`,
				},
			},
			Action: func(c *cli.Context) {
				AdminPurgeTopic(c)
			},
		},
		{
			Name:    "mergeDLQ",
			Aliases: []string{"mgdlq"},
			Usage:   "Merge replication tasks to target topic(from input file or DLQ topic)",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  FlagInputFileWithAlias,
					Usage: "Input file to use to read as JSON of ReplicationTask, separated by line",
				},
				cli.StringFlag{
					Name:  FlagInputTopicWithAlias,
					Usage: "Input topic to read ReplicationTask",
				},
				cli.StringFlag{
					Name:  FlagInputCluster,
					Usage: "Name of the Kafka cluster for reading DLQ topic for ReplicationTask",
				},
				cli.Int64Flag{
					Name:  FlagStartOffset,
					Usage: "Starting offset for reading DLQ topic for ReplicationTask",
				},
				cli.StringFlag{
					Name:  FlagCluster,
					Usage: "Name of the Kafka cluster to publish replicationTasks",
				},
				cli.StringFlag{
					Name:  FlagTopic,
					Usage: "Topic to publish replication task",
				},
				cli.StringFlag{
					Name:  FlagGroup,
					Usage: "Group to read DLQ",
				},
				cli.StringFlag{
					Name: FlagHostFile,
					Usage: "Kafka host config file in format of: " + `
clusters:
	localKafka:
		brokers:
		- 127.0.0.1
		- 127.0.0.2`,
				},
			},
			Action: func(c *cli.Context) {
				AdminMergeDLQ(c)
			},
		},
		{
			Name:    "rereplicate",
			Aliases: []string{"rrp"},
			Usage:   "Rereplicate replication tasks to target topic from history tables",
			Flags: []cli.Flag{

				cli.StringFlag{
					Name:  FlagTargetCluster,
					Usage: "Name of targetCluster to receive the replication task",
				},
				cli.IntFlag{
					Name:  FlagNumberOfShards,
					Usage: "NumberOfShards is required to calculate shardID. (see server config for numHistoryShards)",
				},

				// for multiple workflow
				cli.StringFlag{
					Name:  FlagInputFileWithAlias,
					Usage: "Input file to read multiple workflow line by line. For each line: domainID,workflowID,runID,minEventID,maxEventID (minEventID/maxEventID are optional.)",
				},

				// for one workflow
				cli.Int64Flag{
					Name:  FlagMinEventID,
					Usage: "MinEventID. Optional, default to all events",
				},
				cli.Int64Flag{
					Name:  FlagMaxEventID,
					Usage: "MaxEventID Optional, default to all events",
				},
				cli.StringFlag{
					Name:  FlagWorkflowIDWithAlias,
					Usage: "WorkflowID",
				},
				cli.StringFlag{
					Name:  FlagRunIDWithAlias,
					Usage: "RunID",
				},
				cli.StringFlag{
					Name:  FlagDomainID,
					Usage: "DomainID",
				},

				// for cassandra connection
				cli.StringFlag{
					Name:  FlagAddress,
					Usage: "cassandra host address",
				},
				cli.IntFlag{
					Name:  FlagPort,
					Usage: "cassandra port for the host (default is 9042)",
				},
				cli.StringFlag{
					Name:  FlagUsername,
					Usage: "cassandra username",
				},
				cli.StringFlag{
					Name:  FlagPassword,
					Usage: "cassandra password",
				},
				cli.StringFlag{
					Name:  FlagKeyspace,
					Usage: "cassandra keyspace",
				},

				// kafka
				cli.StringFlag{
					Name:  FlagCluster,
					Usage: "Name of the Kafka cluster to publish replicationTasks",
				},
				cli.StringFlag{
					Name:  FlagTopic,
					Usage: "Topic to publish replication task",
				},
				cli.StringFlag{
					Name: FlagHostFile,
					Usage: "Kafka host config file in format of: " + `
clusters:
	localKafka:
		brokers:
		- 127.0.0.1
		- 127.0.0.2`,
				},
			},
			Action: func(c *cli.Context) {
				AdminRereplicate(c)
			},
		},
	}
}

func newAdminElasticSearchCommands() []cli.Command {
	return []cli.Command{
		{
			Name:    "catIndex",
			Aliases: []string{"cind"},
			Usage:   "Cat Indices on ElasticSearch",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  FlagURL,
					Usage: "URL of ElasticSearch cluster",
				},
			},
			Action: func(c *cli.Context) {
				AdminCatIndices(c)
			},
		},
		{
			Name:    "index",
			Aliases: []string{"ind"},
			Usage:   "Index docs on ElasticSearch",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  FlagURL,
					Usage: "URL of ElasticSearch cluster",
				},
				cli.StringFlag{
					Name:  FlagMuttleyDestinationWithAlias,
					Usage: "Optional muttely destination to ElasticSearch cluster",
				},
				cli.StringFlag{
					Name:  FlagIndex,
					Usage: "ElasticSearch target index",
				},
				cli.StringFlag{
					Name:  FlagInputFileWithAlias,
					Usage: "Input file of indexer.Message in json format, separated by newline",
				},
				cli.IntFlag{
					Name:  FlagBatchSizeWithAlias,
					Usage: "Optional batch size of actions for bulk operations",
					Value: 1000,
				},
			},
			Action: func(c *cli.Context) {
				AdminIndex(c)
			},
		},
	}
}
