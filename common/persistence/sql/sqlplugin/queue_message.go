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
	"database/sql"
	"math"

	"go.temporal.io/server/common/persistence"
)

const (
	EmptyMessageID = int64(-1)
	MinMessageID   = EmptyMessageID + 1
	MaxMessageID   = math.MaxInt64
)

type (
	// QueueRow represents a row in queue table
	QueueRow struct {
		QueueType      persistence.QueueType
		MessageID      int64
		MessagePayload []byte
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

	// QueueMetadataRow represents a row in queue_metadata table
	QueueMetadataRow struct {
		QueueType persistence.QueueType
		Data      []byte
	}

	QueueMessage interface {
		InsertIntoMessages(row []QueueRow) (sql.Result, error)
		SelectFromMessages(filter QueueMessagesFilter) ([]QueueRow, error)
		RangeSelectFromMessages(filter QueueMessagesRangeFilter) ([]QueueRow, error)
		DeleteFromMessages(filter QueueMessagesFilter) (sql.Result, error)
		RangeDeleteFromMessages(filter QueueMessagesRangeFilter) (sql.Result, error)

		GetLastEnqueuedMessageIDForUpdate(queueType persistence.QueueType) (int64, error)

		InsertAckLevel(queueType persistence.QueueType, messageID int64, clusterName string) error
		UpdateAckLevels(queueType persistence.QueueType, clusterAckLevels map[string]int64) error
		GetAckLevels(queueType persistence.QueueType, forUpdate bool) (map[string]int64, error)
	}
)
