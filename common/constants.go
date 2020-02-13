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

package common

import (
	"time"
)

const (
	// FirstEventID is the id of the first event in the history
	FirstEventID int64 = 1
	// EmptyEventID is the id of the empty event
	EmptyEventID int64 = -23
	// EmptyVersion is used as the default value for failover version when no value is provided
	EmptyVersion int64 = -24
	// EndEventID is the id of the end event, here we use the int64 max
	EndEventID int64 = 1<<63 - 1
	// BufferedEventID is the id of the buffered event
	BufferedEventID int64 = -123
	// EmptyEventTaskID is uninitialized id of the task id within event
	EmptyEventTaskID int64 = -1234
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
	FrontendServiceName = "cadence-frontend"
	// HistoryServiceName is the name of the history service
	HistoryServiceName = "cadence-history"
	// MatchingServiceName is the name of the matching service
	MatchingServiceName = "cadence-matching"
	// WorkerServiceName is the name of the worker service
	WorkerServiceName = "cadence-worker"
)

// Data encoding types
const (
	EncodingTypeJSON     EncodingType = "json"
	EncodingTypeThriftRW EncodingType = "thriftrw"
	EncodingTypeGob      EncodingType = "gob"
	EncodingTypeUnknown  EncodingType = "unknow"
	EncodingTypeEmpty    EncodingType = ""
)

type (
	// EncodingType is an enum that represents various data encoding types
	EncodingType string
)

// MaxTaskTimeout is maximum task timeout allowed. 366 days in seconds
const MaxTaskTimeout = 31622400

const (
	// GetHistoryMaxPageSize is the max page size for get history
	GetHistoryMaxPageSize = 1000
	// ReadDLQMessagesPageSize is the max page size for read DLQ messages
	ReadDLQMessagesPageSize = 1000
)

const (
	// VisibilityAppName is used to find kafka topics and ES indexName for visibility
	VisibilityAppName = "visibility"
)

// This was flagged by salus as potentially hardcoded credentials. This is a false positive by the scanner and should be
// disregarded.
// #nosec
const (
	// SystemGlobalDomainName is global domain name for cadence system workflows running globally
	SystemGlobalDomainName = "cadence-system-global"
	// SystemLocalDomainName is domain name for cadence system workflows running in local cluster
	SystemLocalDomainName = "cadence-system"
	// SystemDomainID is domain id for all cadence system workflows
	SystemDomainID = "32049b68-7872-4094-8e63-d0dd59896a83"
	// SystemDomainRetentionDays is retention config for all cadence system workflows
	SystemDomainRetentionDays = 7
	// DefaultAdminOperationToken is the default dynamic config value for AdminOperationToken
	DefaultAdminOperationToken = "CadenceTeamONLY"
)

const (
	// MinLongPollTimeout is the minimum context timeout for long poll API, below which
	// the request won't be processed
	MinLongPollTimeout = time.Second * 2
	// CriticalLongPollTimeout is a threshold for the context timeout passed into long poll API,
	// below which a warning will be logged
	CriticalLongPollTimeout = time.Second * 20
	// MaxWorkflowRetentionPeriodInDays is the maximum of workflow retention when registering domain
	// !!! Do NOT simply decrease this number, because it is being used by history scavenger to avoid race condition against history archival.
	// Check more details in history scanner(scavenger)
	MaxWorkflowRetentionPeriodInDays = 30
)

const (
	// DefaultTransactionSizeLimit is the largest allowed transaction size to persistence
	DefaultTransactionSizeLimit = 14 * 1024 * 1024
)

const (
	// ArchivalEnabled is the status for enabling archival
	ArchivalEnabled = "enabled"
	// ArchivalDisabled is the status for disabling archival
	ArchivalDisabled = "disabled"
	// ArchivalPaused is the status for pausing archival
	ArchivalPaused = "paused"
)

// enum for dynamic config AdvancedVisibilityWritingMode
const (
	// AdvancedVisibilityWritingModeOff means do not write to advanced visibility store
	AdvancedVisibilityWritingModeOff = "off"
	// AdvancedVisibilityWritingModeOn means only write to advanced visibility store
	AdvancedVisibilityWritingModeOn = "on"
	// AdvancedVisibilityWritingModeDual means write to both normal visibility and advanced visibility store
	AdvancedVisibilityWritingModeDual = "dual"
)
