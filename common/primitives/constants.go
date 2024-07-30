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

package primitives

import (
	"time"

	"go.temporal.io/server/common/debug"
)

const (
	// DefaultTransactionSizeLimit is the largest allowed transaction size to persistence
	DefaultTransactionSizeLimit = 4 * 1024 * 1024
)

const (
	// DefaultWorkflowTaskTimeout sets the Default Workflow Task timeout for a Workflow
	DefaultWorkflowTaskTimeout = 10 * time.Second * debug.TimeoutMultiplier
)

const (
	// GetHistoryMaxPageSize is the max page size for get history
	GetHistoryMaxPageSize = 256
	// ReadDLQMessagesPageSize is the max page size for read DLQ messages
	ReadDLQMessagesPageSize = 1000
)

const (
	DefaultHistoryMaxAutoResetPoints = 20
)

const (
	// WorkflowIDPrefix ID prefix for schedules that are implemented by a workflow
	WorkflowIDPrefix = "temporal-sys-scheduler:"
)
