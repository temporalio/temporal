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

import "time"

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
	// TransientEventID is the id of the transient event
	TransientEventID int64 = -124
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
	EncodingTypeThriftRW              = "thriftrw"
	EncodingTypeGob                   = "gob"
	EncodingTypeUnknown               = "unknow"
)

// NoRetryBackoff is used to represent backoff when no retry is needed
const NoRetryBackoff = time.Duration(-1)

type (
	// EncodingType is an enum that represents various data encoding types
	EncodingType string
)

// MaxTaskTimeout is maximum task timeout allowed. 366 days in seconds
const MaxTaskTimeout = 31622400

const (
	// GetHistoryWarnSizeLimit is the threshold for emitting warn log
	GetHistoryWarnSizeLimit = 500 * 1024 // Warn when size goes over 500KB
	// GetHistoryMaxPageSize is the max page size for get history
	GetHistoryMaxPageSize = 1000
)

const (
	// VisibilityAppName is used to find kafka topics and ES indexName for visibility
	VisibilityAppName = "visibility"
)
