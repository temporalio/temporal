// The MIT License
//
// Copyright (c) 2021 Datadog, Inc.
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

package postgresql

import (
	"context"
	"database/sql"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	templateEnqueueMessageQueryV2            = `INSERT INTO queue_messages (queue_type, queue_name, queue_partition, message_id, message_payload, message_encoding) VALUES(:queue_type, :queue_name, :queue_partition, :message_id, :message_payload, :message_encoding)`
	templateGetMessagesQueryV2               = `SELECT message_id, message_payload, message_encoding FROM queue_messages WHERE queue_type = $1 and queue_name = $2 and queue_partition = $3 and message_id >= $4 ORDER BY message_id ASC LIMIT $5`
	templateRangeDeleteMessagesQueryV2       = `DELETE FROM queue_messages WHERE queue_type = $1 and queue_name = $2 and queue_partition = $3 and message_id >= $4 and message_id <= $5`
	templateGetLastMessageIDQueryV2          = `SELECT message_id FROM queue_messages WHERE queue_type=$1 and queue_name=$2 and queue_partition=$3 and message_id >= (SELECT message_id FROM queue_messages WHERE queue_type=$1 and queue_name=$2 and queue_partition=$3 ORDER BY message_id DESC LIMIT 1) FOR UPDATE`
	templateCreateQueueMetadataQueryV2       = `INSERT INTO queues (queue_type, queue_name, metadata_payload, metadata_encoding) VALUES(:queue_type, :queue_name, :metadata_payload, :metadata_encoding)`
	templateUpdateQueueMetadataQueryV2       = `UPDATE queues SET metadata_payload = :metadata_payload, metadata_encoding = :metadata_encoding WHERE queue_type = :queue_type and queue_name = :queue_name`
	templateGetQueueMetadataQueryV2          = `SELECT metadata_payload, metadata_encoding from queues WHERE queue_type=$1 and queue_name=$2`
	templateGetQueueMetadataQueryV2ForUpdate = `SELECT metadata_payload, metadata_encoding from queues WHERE queue_type=$1 and queue_name=$2 FOR UPDATE`
	templateGetNameFromQueueMetadataV2       = `SELECT queue_type, queue_name, metadata_payload, metadata_encoding from queues WHERE queue_type=$1 LIMIT $2 OFFSET $3`
)

func (pdb *db) InsertIntoQueueV2Metadata(ctx context.Context, row *sqlplugin.QueueV2MetadataRow) (sql.Result, error) {
	return pdb.NamedExecContext(ctx,
		templateCreateQueueMetadataQueryV2,
		row,
	)
}

func (pdb *db) UpdateQueueV2Metadata(ctx context.Context, row *sqlplugin.QueueV2MetadataRow) (sql.Result, error) {
	return pdb.NamedExecContext(ctx,
		templateUpdateQueueMetadataQueryV2,
		row,
	)
}

func (pdb *db) SelectFromQueueV2Metadata(ctx context.Context, filter sqlplugin.QueueV2MetadataFilter) (*sqlplugin.QueueV2MetadataRow, error) {
	var row sqlplugin.QueueV2MetadataRow
	err := pdb.GetContext(ctx,
		&row,
		templateGetQueueMetadataQueryV2,
		filter.QueueType,
		filter.QueueName,
	)
	if err != nil {
		return nil, err
	}
	return &row, nil
}

func (pdb *db) SelectFromQueueV2MetadataForUpdate(ctx context.Context, filter sqlplugin.QueueV2MetadataFilter) (*sqlplugin.QueueV2MetadataRow, error) {
	var row sqlplugin.QueueV2MetadataRow
	err := pdb.GetContext(ctx,
		&row,
		templateGetQueueMetadataQueryV2ForUpdate,
		filter.QueueType,
		filter.QueueName,
	)
	if err != nil {
		return nil, err
	}
	return &row, nil
}

func (pdb *db) SelectNameFromQueueV2Metadata(ctx context.Context, filter sqlplugin.QueueV2MetadataTypeFilter) ([]sqlplugin.QueueV2MetadataRow, error) {
	var rows []sqlplugin.QueueV2MetadataRow
	err := pdb.SelectContext(ctx,
		&rows,
		templateGetNameFromQueueMetadataV2,
		filter.QueueType,
		filter.PageSize,
		filter.PageOffset,
	)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (pdb *db) InsertIntoQueueV2Messages(ctx context.Context, row []sqlplugin.QueueV2MessageRow) (sql.Result, error) {
	return pdb.NamedExecContext(ctx,
		templateEnqueueMessageQueryV2,
		row,
	)
}

func (pdb *db) RangeSelectFromQueueV2Messages(ctx context.Context, filter sqlplugin.QueueV2MessagesFilter) ([]sqlplugin.QueueV2MessageRow, error) {
	var rows []sqlplugin.QueueV2MessageRow
	err := pdb.SelectContext(ctx,
		&rows,
		templateGetMessagesQueryV2,
		filter.QueueType,
		filter.QueueName,
		filter.Partition,
		filter.MinMessageID,
		filter.PageSize,
	)
	return rows, err
}

func (pdb *db) RangeDeleteFromQueueV2Messages(ctx context.Context, filter sqlplugin.QueueV2MessagesFilter) (sql.Result, error) {
	return pdb.ExecContext(ctx,
		templateRangeDeleteMessagesQueryV2,
		filter.QueueType,
		filter.QueueName,
		filter.Partition,
		filter.MinMessageID,
		filter.MaxMessageID,
	)
}

func (pdb *db) GetLastEnqueuedMessageIDForUpdateV2(ctx context.Context, filter sqlplugin.QueueV2Filter) (int64, error) {
	var lastMessageID int64
	err := pdb.GetContext(ctx,
		&lastMessageID,
		templateGetLastMessageIDQueryV2,
		filter.QueueType,
		filter.QueueName,
		filter.Partition,
	)
	return lastMessageID, err
}
