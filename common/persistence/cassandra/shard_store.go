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

package cassandra

import (
	"context"
	"fmt"
	"strings"

	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
)

const (
	templateCreateShardQuery = `INSERT INTO executions (` +
		`shard_id, type, namespace_id, workflow_id, run_id, visibility_ts, task_id, shard, shard_encoding, range_id)` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`

	templateGetShardQuery = `SELECT shard, shard_encoding ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ?`

	templateUpdateShardQuery = `UPDATE executions ` +
		`SET shard = ?, shard_encoding = ?, range_id = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and namespace_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF range_id = ?`
)

type (
	ShardStore struct {
		ClusterName string
		Session     gocql.Session
		Logger      log.Logger
	}
)

func NewShardStore(
	clusterName string,
	session gocql.Session,
	logger log.Logger,
) *ShardStore {
	return &ShardStore{
		ClusterName: clusterName,
		Session:     session,
		Logger:      logger,
	}
}

func (d *ShardStore) GetOrCreateShard(
	ctx context.Context,
	request *p.InternalGetOrCreateShardRequest,
) (*p.InternalGetOrCreateShardResponse, error) {
	query := d.Session.Query(templateGetShardQuery,
		request.ShardID,
		rowTypeShard,
		rowTypeShardNamespaceID,
		rowTypeShardWorkflowID,
		rowTypeShardRunID,
		defaultVisibilityTimestamp,
		rowTypeShardTaskID,
	).WithContext(ctx)

	var data []byte
	var encoding string
	err := query.Scan(&data, &encoding)
	if err == nil {
		return &p.InternalGetOrCreateShardResponse{
			ShardInfo: p.NewDataBlob(data, encoding),
		}, nil
	} else if !gocql.IsNotFoundError(err) || request.CreateShardInfo == nil {
		return nil, gocql.ConvertError("GetOrCreateShard", err)
	}

	// shard was not found and we should create it
	rangeID, shardInfo, err := request.CreateShardInfo()
	if err != nil {
		return nil, err
	}

	query = d.Session.Query(templateCreateShardQuery,
		request.ShardID,
		rowTypeShard,
		rowTypeShardNamespaceID,
		rowTypeShardWorkflowID,
		rowTypeShardRunID,
		defaultVisibilityTimestamp,
		rowTypeShardTaskID,
		shardInfo.Data,
		shardInfo.EncodingType.String(),
		rangeID,
	).WithContext(ctx)

	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		return nil, gocql.ConvertError("GetOrCreateShard", err)
	}
	if !applied {
		// conflict, try again
		request.CreateShardInfo = nil // prevent loop
		return d.GetOrCreateShard(ctx, request)
	}
	return &p.InternalGetOrCreateShardResponse{
		ShardInfo: shardInfo,
	}, nil
}

func (d *ShardStore) UpdateShard(
	ctx context.Context,
	request *p.InternalUpdateShardRequest,
) error {
	query := d.Session.Query(templateUpdateShardQuery,
		request.ShardInfo.Data,
		request.ShardInfo.EncodingType.String(),
		request.RangeID,
		request.ShardID,
		rowTypeShard,
		rowTypeShardNamespaceID,
		rowTypeShardWorkflowID,
		rowTypeShardRunID,
		defaultVisibilityTimestamp,
		rowTypeShardTaskID,
		request.PreviousRangeID,
	).WithContext(ctx)

	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		return gocql.ConvertError("UpdateShard", err)
	}

	if !applied {
		var columns []string
		for k, v := range previous {
			columns = append(columns, fmt.Sprintf("%s=%v", k, v))
		}

		return &p.ShardOwnershipLostError{
			ShardID: request.ShardID,
			Msg: fmt.Sprintf("Failed to update shard.  previous_range_id: %v, columns: (%v)",
				request.PreviousRangeID, strings.Join(columns, ",")),
		}
	}

	return nil
}

func (d *ShardStore) AssertShardOwnership(
	ctx context.Context,
	request *p.AssertShardOwnershipRequest,
) error {
	return nil
}

func (d *ShardStore) GetName() string {
	return cassandraPersistenceName
}

func (d *ShardStore) GetClusterName() string {
	return d.ClusterName
}

func (d *ShardStore) Close() {
	if d.Session != nil {
		d.Session.Close()
	}
}
