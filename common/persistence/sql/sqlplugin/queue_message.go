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
	// QueueMessageRow represents a row in queue table
	QueueMessageRow struct {
		QueueType       persistence.QueueType
		MessageID       int64
		MessagePayload  []byte
		MessageEncoding string
	}

	// QueueMessagesFilter
	QueueMessagesFilter struct {
		QueueType persistence.QueueType
		MessageID int64
	}

	// QueueMessagesRangeFilter
	QueueMessagesRangeFilter struct {
		QueueType    persistence.QueueType
		MinMessageID int64
		MaxMessageID int64
		PageSize     int
	}

	QueueMessage interface {
		InsertIntoMessages(ctx context.Context, row []QueueMessageRow) (sql.Result, error)
		SelectFromMessages(ctx context.Context, filter QueueMessagesFilter) ([]QueueMessageRow, error)
		RangeSelectFromMessages(ctx context.Context, filter QueueMessagesRangeFilter) ([]QueueMessageRow, error)
		DeleteFromMessages(ctx context.Context, filter QueueMessagesFilter) (sql.Result, error)
		RangeDeleteFromMessages(ctx context.Context, filter QueueMessagesRangeFilter) (sql.Result, error)

		GetLastEnqueuedMessageIDForUpdate(ctx context.Context, queueType persistence.QueueType) (int64, error)
	}
)
