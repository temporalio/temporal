package cassandra

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.temporal.io/api/serviceerror"
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
	resp, err := d.GetShard(ctx, &p.GetShardRequest{ShardID: request.ShardID})
	if err == nil {
		return &p.InternalGetOrCreateShardResponse{
			ShardInfo: resp.ShardInfo,
		}, nil
	} else if _, isNotFound := errors.AsType[*serviceerror.NotFound](err); !isNotFound {
		return nil, err
	}

	// shard was not found and we should create it
	rangeID, shardInfo, err := request.CreateShardInfo()
	if err != nil {
		return nil, err
	}

	query := d.Session.Query(templateCreateShardQuery,
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

	previous := make(map[string]any)
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		return nil, gocql.ConvertError("GetOrCreateShard", err)
	}
	if !applied {
		// conflict, return the shard created concurrently
		resp, err := d.GetShard(ctx, &p.GetShardRequest{ShardID: request.ShardID})
		if err != nil {
			return nil, err
		}
		return &p.InternalGetOrCreateShardResponse{
			ShardInfo: resp.ShardInfo,
		}, nil
	}
	return &p.InternalGetOrCreateShardResponse{
		ShardInfo: shardInfo,
	}, nil
}

func (d *ShardStore) GetShard(
	ctx context.Context,
	request *p.GetShardRequest,
) (*p.InternalGetShardResponse, error) {
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
	if err := query.Scan(&data, &encoding); err != nil {
		return nil, gocql.ConvertError("GetShard", err)
	}
	return &p.InternalGetShardResponse{
		ShardInfo: p.NewDataBlob(data, encoding),
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

	previous := make(map[string]any)
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
	// AssertShardOwnership is not implemented for cassandra shard store
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
