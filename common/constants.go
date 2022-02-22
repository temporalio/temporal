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

package common

import (
	"math"
	"time"
)

const (
	// FirstEventID is the id of the first event in the history
	FirstEventID int64 = 1
	// LastEventID is the id of the last possible event in the history
	LastEventID int64 = math.MaxInt64
	// EmptyEventID is the id of the empty event
	EmptyEventID int64 = 0
	// EmptyVersion is used as the default value for failover version when no value is provided
	EmptyVersion int64 = 0
	// EndEventID is the id of the end event, here we use the int64 max
	EndEventID int64 = 1<<63 - 1
	// BufferedEventID is the id of the buffered event
	BufferedEventID int64 = -123
	// EmptyEventTaskID is uninitialized id of the task id within event
	EmptyEventTaskID int64 = 0
	// TransientEventID is the id of the transient event
	TransientEventID int64 = -124
	// FirstBlobPageToken is the page token identifying the first blob for each history archival
	FirstBlobPageToken = 1
	// LastBlobNextPageToken is the next page token on the last blob for each history archival
	LastBlobNextPageToken = -1
	// EndMessageID is the id of the end message, here we use the int64 max
	EndMessageID int64 = 1<<63 - 1
)

const (
	// FrontendServiceName is the name of the frontend service
	FrontendServiceName = "frontend"
	// HistoryServiceName is the name of the history service
	HistoryServiceName = "history"
	// MatchingServiceName is the name of the matching service
	MatchingServiceName = "matching"
	// WorkerServiceName is the name of the worker service
	WorkerServiceName = "worker"
)

const (
	// GetHistoryMaxPageSize is the max page size for get history
	GetHistoryMaxPageSize = 256
	// ReadDLQMessagesPageSize is the max page size for read DLQ messages
	ReadDLQMessagesPageSize = 1000
)

// This was flagged by salus as potentially hardcoded credentials. This is a false positive by the scanner and should be
// disregarded.
// #nosec
const (
	// SystemGlobalNamespace is global namespace name for temporal system workflows running globally
	SystemGlobalNamespace = "temporal-system-global"
	// SystemLocalNamespace is namespace name for temporal system workflows running in local cluster
	SystemLocalNamespace = "temporal-system"
	// SystemNamespaceID is namespace id for all temporal system workflows
	SystemNamespaceID = "32049b68-7872-4094-8e63-d0dd59896a83"
	// SystemNamespaceRetention is retention config for all temporal system workflows
	SystemNamespaceRetention = time.Hour * 24 * 7
)

const (
	// MinLongPollTimeout is the minimum context timeout for long poll API, below which
	// the request won't be processed
	MinLongPollTimeout = time.Second * 2
	// CriticalLongPollTimeout is a threshold for the context timeout passed into long poll API,
	// below which a warning will be logged
	CriticalLongPollTimeout = time.Second * 20
	// MaxWorkflowRetentionPeriod is the maximum of workflow retention when registering namespace
	// !!! Do NOT simply decrease this number, because it is being used by history scavenger to avoid race condition against history archival.
	// Check more details in history scanner(scavenger)
	MaxWorkflowRetentionPeriod = 30 * time.Hour * 24
)

const (
	// DefaultWorkflowTaskTimeout sets the Default Workflow Task timeout for a Workflow
	DefaultWorkflowTaskTimeout = 10 * time.Second

	// MaxWorkflowTaskStartToCloseTimeout sets the Max Workflow Task start to close timeout for a Workflow
	MaxWorkflowTaskStartToCloseTimeout = 120 * time.Second
)

const (
	// DefaultTransactionSizeLimit is the largest allowed transaction size to persistence
	DefaultTransactionSizeLimit = 4 * 1024 * 1024
)

const (
	// TimeoutFailureTypePrefix is the prefix for timeout failure types
	// used in retry policy
	// the actual failure type will be prefix + enums.TimeoutType.String()
	// e.g. "TemporalTimeout:StartToClose" or "TemporalTimeout:Heartbeat"
	TimeoutFailureTypePrefix = "TemporalTimeout:"
)
