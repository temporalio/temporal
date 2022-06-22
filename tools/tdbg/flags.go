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
	"github.com/urfave/cli/v2"
)

// Flags used to specify cli command line arguments
var (
	FlagUsername                   = "username"
	FlagPassword                   = "password"
	FlagKeyspace                   = "keyspace"
	FlagAddress                    = "address"
	FlagHistoryAddress             = "history-address"
	FlagDBEngine                   = "db-engine"
	FlagDBAddress                  = "db-address"
	FlagDBPort                     = "db-port"
	FlagNamespaceID                = "namespace-id"
	FlagNamespace                  = "namespace"
	FlagNamespaceAlias             = []string{"n"}
	FlagShardID                    = "shard-id"
	FlagWorkflowID                 = "workflow-id"
	FlagWorkflowIDAlias            = []string{"wid"}
	FlagRunID                      = "run-id"
	FlagRunIDAlias                 = []string{"rid"}
	FlagNumberOfShards             = "number-of-shards"
	FlagMinEventID                 = "min-event-id"
	FlagMaxEventID                 = "max-event-id"
	FlagTaskQueue                  = "task-queue"
	FlagTaskQueueType              = "task-queue-type"
	FlagContextTimeout             = "context-timeout"
	FlagContextTimeoutAlias        = []string{"ct"}
	FlagInput                      = "input"
	FlagInputAlias                 = []string{"i"}
	FlagInputFile                  = "input-file"
	FlagInputFileAlias             = []string{"if"}
	FlagCluster                    = "cluster"
	FlagPageSize                   = "pagesize"
	FlagFrom                       = "from"
	FlagPrintFullyDetail           = "print-full"
	FlagPrintJSON                  = "print-json"
	FlagHeartbeatedWithin          = "heartbeated-within"
	FlagOutputFilename             = "output-filename"
	FlagClusterMembershipRole      = "role"
	FlagSkipErrorMode              = "skip-errors"
	FlagTaskID                     = "task-id"
	FlagTaskType                   = "task-type"
	FlagTaskVisibilityTimestamp    = "task-timestamp"
	FlagMinVisibilityTimestamp     = "min-visibility-ts"
	FlagMaxVisibilityTimestamp     = "max-visibility-ts"
	FlagEnableTLS                  = "tls"
	FlagTLSCertPath                = "tls-cert-path"
	FlagTLSKeyPath                 = "tls-key-path"
	FlagTLSCaPath                  = "tls-ca-path"
	FlagTLSDisableHostVerification = "tls-disable-host-verification"
	FlagTLSServerName              = "tls-server-name"
	FlagLastMessageID              = "last-message-id"
	FlagAutoConfirm                = "auto-confirm"
	FlagVersion                    = "version"
	FlagMore                       = "more"
	FlagElasticsearchURL           = "url"
	FlagMinEventVersion            = "min-event-version"
	FlagMaxEventVersion            = "max-event-version"
	FlagMinTaskID                  = "min-task-id"
	FlagMaxTaskID                  = "max-task-id"
	FlagElasticsearchIndex         = "index"
	FlagElasticsearchUsername      = "es-username"
	FlagElasticsearchPassword      = "es-password"
	FlagFrontendAddress            = "frontend-address"
	FlagConnectionEnable           = "enable-connection"
	FlagDLQType                    = "dlq-type"
	FlagMaxMessageCount            = "max-message-count"
	FlagProtoType                  = "type"
	FlagHexData                    = "hex-data"
	FlagHexFile                    = "hex-file"
	FlagBinaryFile                 = "binary-file"
	FlagBase64Data                 = "base64-data"
	FlagBase64File                 = "base64-file"
)

func getDBFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:  FlagDBEngine,
			Value: "cassandra",
			Usage: "Type of the DB engine to use (cassandra, mysql, postgres..)",
		},
		&cli.StringFlag{
			Name:  FlagDBAddress,
			Value: "127.0.0.1",
			Usage: "persistence address",
		},
		&cli.IntFlag{
			Name:  FlagDBPort,
			Value: 9042,
			Usage: "persistence port",
		},
		&cli.StringFlag{
			Name:  FlagUsername,
			Usage: "DB username",
		},
		&cli.StringFlag{
			Name:  FlagPassword,
			Usage: "DB password",
		},
		&cli.StringFlag{
			Name:  FlagKeyspace,
			Value: "temporal",
			Usage: "DB keyspace",
		},
		&cli.BoolFlag{
			Name:  FlagEnableTLS,
			Usage: "enable TLS over the DB connection",
		},
		&cli.StringFlag{
			Name:  FlagTLSCertPath,
			Usage: "DB tls client cert path (tls must be enabled)",
		},
		&cli.StringFlag{
			Name:  FlagTLSKeyPath,
			Usage: "DB tls client key path (tls must be enabled)",
		},
		&cli.StringFlag{
			Name:  FlagTLSCaPath,
			Usage: "DB tls client ca path (tls must be enabled)",
		},
		&cli.StringFlag{
			Name:  FlagTLSServerName,
			Usage: "DB tls server name (tls must be enabled)",
		},
		&cli.BoolFlag{
			Name:  FlagTLSDisableHostVerification,
			Usage: "DB tls verify hostname and server cert (tls must be enabled)",
		},
	}
}

func getDBAndESFlags() []cli.Flag {
	return append(getDBFlags(), getESFlags(true)...)
}

func getESFlags(index bool) []cli.Flag {
	flags := []cli.Flag{
		&cli.StringFlag{
			Name:  FlagElasticsearchURL,
			Value: "http://127.0.0.1:9200",
			Usage: "URL of Elasticsearch cluster",
		},
		&cli.StringFlag{
			Name:  FlagElasticsearchUsername,
			Value: "",
			Usage: "Username for Elasticsearch cluster",
		},
		&cli.StringFlag{
			Name:  FlagElasticsearchPassword,
			Value: "",
			Usage: "Password for Elasticsearch cluster",
		},
		&cli.StringFlag{
			Name:  FlagVersion,
			Value: "v7",
			Usage: "Version of Elasticsearch cluster: v6 or v7 (default)",
		},
	}
	if index {
		flags = append(flags,
			&cli.StringFlag{
				Name:  FlagElasticsearchIndex,
				Usage: "Elasticsearch index name",
			},
		)
	}
	return flags
}
