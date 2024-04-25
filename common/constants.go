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
	// MinLongPollTimeout is the minimum context timeout for long poll API, below which
	// the request won't be processed
	MinLongPollTimeout = time.Second * 2
	// CriticalLongPollTimeout is a threshold for the context timeout passed into long poll API,
	// below which a warning will be logged
	CriticalLongPollTimeout = time.Second * 20
)

const (
	// MaxWorkflowTaskStartToCloseTimeout sets the Max Workflow Task start to close timeout for a Workflow
	MaxWorkflowTaskStartToCloseTimeout = 120 * time.Second
)

const (
	// Limit for schedule notes field
	ScheduleNotesSizeLimit = 1000
)

const (
	// DefaultQueueReaderID is the default readerID when loading history tasks
	DefaultQueueReaderID int64 = 0
)

const (
	// DefaultOperatorRPSRatio is the default percentage of rate limit that should be used for operator priority requests
	DefaultOperatorRPSRatio float64 = 0.2
)
