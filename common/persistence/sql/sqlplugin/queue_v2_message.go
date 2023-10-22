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

package sqlplugin

import (
	"context"
	"database/sql"

	"go.temporal.io/server/common/persistence"
)

type (
	// QueueV2MessageRow represents a row in queue_messages table
	QueueV2MessageRow struct {
		QueueType       persistence.QueueV2Type
		QueueName       string
		QueuePartition  int64
		MessageID       int64
		MessagePayload  []byte
		MessageEncoding string
	}

	// QueueV2MessagesFilter is used to filter rows in queue_messages table
	QueueV2MessagesFilter struct {
		QueueType    persistence.QueueV2Type
		QueueName    string
		Partition    int64
		MinMessageID int64
		MaxMessageID int64 // used for RangeDelete
		PageSize     int   // used for RangeSelect
	}

	// QueueV2Filter is used to filter rows in queues table
	QueueV2Filter struct {
		QueueType persistence.QueueV2Type
		QueueName string
		Partition int
	}

	QueueV2Message interface {
		InsertIntoQueueV2Messages(ctx context.Context, row []QueueV2MessageRow) (sql.Result, error)
		RangeSelectFromQueueV2Messages(ctx context.Context, filter QueueV2MessagesFilter) ([]QueueV2MessageRow, error)
		RangeDeleteFromQueueV2Messages(ctx context.Context, filter QueueV2MessagesFilter) (sql.Result, error)
		GetLastEnqueuedMessageIDForUpdateV2(ctx context.Context, filter QueueV2Filter) (int64, error)
	}
)
