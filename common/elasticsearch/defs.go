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

package elasticsearch

import (
	enumsspb "go.temporal.io/server/api/enums/v1"
)

// All legal fields allowed in elastic search index
const (
	NamespaceID     = "NamespaceId"
	WorkflowID      = "WorkflowId"
	RunID           = "RunId"
	WorkflowType    = "WorkflowType"
	StartTime       = "StartTime"
	ExecutionTime   = "ExecutionTime"
	CloseTime       = "CloseTime"
	ExecutionStatus = "ExecutionStatus"
	HistoryLength   = "HistoryLength"
	Memo            = "Memo"
	Encoding        = "Encoding"
	TaskQueue       = "TaskQueue"

	KafkaKey = "KafkaKey"
)

// Supported field types
var (
	FieldTypeString = enumsspb.FIELD_TYPE_STRING
	FieldTypeInt    = enumsspb.FIELD_TYPE_INT
	FieldTypeBool   = enumsspb.FIELD_TYPE_BOOL
	FieldTypeBinary = enumsspb.FIELD_TYPE_BINARY
)
