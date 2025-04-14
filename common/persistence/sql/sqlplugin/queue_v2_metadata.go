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
	// QueueV2MetadataRow represents a row in queue_metadata table
	QueueV2MetadataRow struct {
		QueueType        persistence.QueueV2Type
		QueueName        string
		MetadataPayload  []byte
		MetadataEncoding string
	}

	QueueV2MetadataFilter struct {
		QueueType persistence.QueueV2Type
		QueueName string
	}

	QueueV2MetadataTypeFilter struct {
		QueueType  persistence.QueueV2Type
		PageSize   int
		PageOffset int64
	}

	QueueV2Metadata interface {
		InsertIntoQueueV2Metadata(ctx context.Context, row *QueueV2MetadataRow) (sql.Result, error)
		UpdateQueueV2Metadata(ctx context.Context, row *QueueV2MetadataRow) (sql.Result, error)
		SelectFromQueueV2Metadata(ctx context.Context, filter QueueV2MetadataFilter) (*QueueV2MetadataRow, error)
		SelectFromQueueV2MetadataForUpdate(ctx context.Context, filter QueueV2MetadataFilter) (*QueueV2MetadataRow, error)
		SelectNameFromQueueV2Metadata(ctx context.Context, filter QueueV2MetadataTypeFilter) ([]QueueV2MetadataRow, error)
	}
)
