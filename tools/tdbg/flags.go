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

// Flags used to specify cli command line arguments
var (
	FlagAddress                    = "address"
	FlagHistoryAddress             = "history-address"
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
	FlagYes                        = "yes"
	FlagMore                       = "more"
	FlagMinEventVersion            = "min-event-version"
	FlagMaxEventVersion            = "max-event-version"
	FlagMinTaskID                  = "min-task-id"
	FlagMaxTaskID                  = "max-task-id"
	FlagDLQType                    = "dlq-type"
	FlagMaxMessageCount            = "max-message-count"
	FlagProtoType                  = "type"
	FlagHexData                    = "hex-data"
	FlagHexFile                    = "hex-file"
	FlagBinaryFile                 = "binary-file"
	FlagBase64Data                 = "base64-data"
	FlagBase64File                 = "base64-file"
)
