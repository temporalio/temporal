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
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// Not much of a need to make this configurable, we're just reading some strings
	listTaskQueueNamesByBuildIdPageSize = 100

	// Row types for table tasks. Lower bit only: see rowTypeTaskInSubqueue for more details.
	rowTypeTask = iota
	rowTypeTaskQueue

	templateUpdateTaskQueueUserDataQuery = `UPDATE task_queue_user_data SET
		data = ?,
		data_encoding = ?,
		version = ?
		WHERE namespace_id = ?
		AND build_id = ''
		AND task_queue_name = ?
		IF version = ?`
	templateGetTaskQueueUserDataQuery = `SELECT data, data_encoding, version
	    FROM task_queue_user_data
		WHERE namespace_id = ? AND build_id = ''
		AND task_queue_name = ?`
	templateListTaskQueueUserDataQuery       = `SELECT task_queue_name, data, data_encoding, version FROM task_queue_user_data WHERE namespace_id = ? AND build_id = ''`
	templateListTaskQueueNamesByBuildIdQuery = `SELECT task_queue_name FROM task_queue_user_data WHERE namespace_id = ? AND build_id = ?`
	templateInsertTaskQueueUserDataQuery     = `INSERT INTO task_queue_user_data
		(namespace_id, build_id, task_queue_name, data, data_encoding, version) VALUES
		(?           , ''      , ?              , ?   , ?            , 1      ) IF NOT EXISTS`
	templateInsertBuildIdTaskQueueMappingQuery = `INSERT INTO task_queue_user_data
	(namespace_id, build_id, task_queue_name) VALUES
	(?           , ?       , ?)`
	templateDeleteBuildIdTaskQueueMappingQuery = `DELETE FROM task_queue_user_data
	WHERE namespace_id = ? AND build_id = ? AND task_queue_name = ?`
	templateCountTaskQueueByBuildIdQuery = `SELECT COUNT(*) FROM task_queue_user_data WHERE namespace_id = ? AND build_id = ?`
)

type userDataStore struct {
	Session gocql.Session
	Logger  log.Logger
}

func (d *userDataStore) GetTaskQueueUserData(
	ctx context.Context,
	request *p.GetTaskQueueUserDataRequest,
) (*p.InternalGetTaskQueueUserDataResponse, error) {
	query := d.Session.Query(templateGetTaskQueueUserDataQuery,
		request.NamespaceID,
		request.TaskQueue,
	).WithContext(ctx)
	var version int64
	var userDataBytes []byte
	var encoding string
	if err := query.Scan(&userDataBytes, &encoding, &version); err != nil {
		return nil, gocql.ConvertError("GetTaskQueueData", err)
	}

	return &p.InternalGetTaskQueueUserDataResponse{
		Version:  version,
		UserData: p.NewDataBlob(userDataBytes, encoding),
	}, nil
}

func (d *userDataStore) UpdateTaskQueueUserData(
	ctx context.Context,
	request *p.InternalUpdateTaskQueueUserDataRequest,
) error {
	batch := d.Session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)

	for taskQueue, update := range request.Updates {
		if update.Version == 0 {
			batch.Query(templateInsertTaskQueueUserDataQuery,
				request.NamespaceID,
				taskQueue,
				update.UserData.Data,
				update.UserData.EncodingType.String(),
			)
		} else {
			batch.Query(templateUpdateTaskQueueUserDataQuery,
				update.UserData.Data,
				update.UserData.EncodingType.String(),
				update.Version+1,
				request.NamespaceID,
				taskQueue,
				update.Version,
			)
		}
		for _, buildId := range update.BuildIdsAdded {
			batch.Query(templateInsertBuildIdTaskQueueMappingQuery, request.NamespaceID, buildId, taskQueue)
		}
		for _, buildId := range update.BuildIdsRemoved {
			batch.Query(templateDeleteBuildIdTaskQueueMappingQuery, request.NamespaceID, buildId, taskQueue)
		}
	}

	previous := make(map[string]any)
	applied, iter, err := d.Session.MapExecuteBatchCAS(batch, previous)
	for _, update := range request.Updates {
		if update.Applied != nil {
			*update.Applied = applied
		}
	}
	if err != nil {
		return gocql.ConvertError("UpdateTaskQueueUserData", err)
	}
	defer iter.Close()

	if !applied {
		// No error, but not applied. That means we had a conflict.
		// Iterate through results to identify first conflicting row.
		for {
			name, nameErr := getTypedFieldFromRow[string]("task_queue_name", previous)
			previousVersion, verErr := getTypedFieldFromRow[int64]("version", previous)
			update, hasUpdate := request.Updates[name]
			if nameErr == nil && verErr == nil && hasUpdate && update.Version != previousVersion {
				if update.Conflicting != nil {
					*update.Conflicting = true
				}
				return &p.ConditionFailedError{
					Msg: fmt.Sprintf("Failed to update task queues: task queue %q version %d != %d",
						name, update.Version, previousVersion),
				}
			}
			clear(previous)
			if !iter.MapScan(previous) {
				break
			}
		}
		return &p.ConditionFailedError{Msg: "Failed to update task queues: unknown conflict"}
	}

	return nil
}

func (d *userDataStore) ListTaskQueueUserDataEntries(ctx context.Context, request *p.ListTaskQueueUserDataEntriesRequest) (*p.InternalListTaskQueueUserDataEntriesResponse, error) {
	query := d.Session.Query(templateListTaskQueueUserDataQuery, request.NamespaceID).WithContext(ctx)
	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()

	response := &p.InternalListTaskQueueUserDataEntriesResponse{}
	row := make(map[string]interface{})
	for iter.MapScan(row) {
		taskQueue, err := getTypedFieldFromRow[string]("task_queue_name", row)
		if err != nil {
			return nil, err
		}
		data, err := getTypedFieldFromRow[[]byte]("data", row)
		if err != nil {
			return nil, err
		}
		dataEncoding, err := getTypedFieldFromRow[string]("data_encoding", row)
		if err != nil {
			return nil, err
		}
		version, err := getTypedFieldFromRow[int64]("version", row)
		if err != nil {
			return nil, err
		}

		response.Entries = append(response.Entries, p.InternalTaskQueueUserDataEntry{TaskQueue: taskQueue, Data: p.NewDataBlob(data, dataEncoding), Version: version})

		row = make(map[string]interface{}) // Reinitialize map as initialized fails on unmarshalling
	}
	if len(iter.PageState()) > 0 {
		response.NextPageToken = iter.PageState()
	}

	if err := iter.Close(); err != nil {
		return nil, serviceerror.NewUnavailablef("ListTaskQueueUserDataEntries operation failed. Error: %v", err)
	}
	return response, nil
}

func (d *userDataStore) GetTaskQueuesByBuildId(ctx context.Context, request *p.GetTaskQueuesByBuildIdRequest) ([]string, error) {
	query := d.Session.Query(templateListTaskQueueNamesByBuildIdQuery, request.NamespaceID, request.BuildID).WithContext(ctx)
	iter := query.PageSize(listTaskQueueNamesByBuildIdPageSize).Iter()

	var taskQueues []string
	row := make(map[string]interface{})

	for {
		for iter.MapScan(row) {
			taskQueueRaw, ok := row["task_queue_name"]
			if !ok {
				return nil, newFieldNotFoundError("task_queue_name", row)
			}
			taskQueue, ok := taskQueueRaw.(string)
			if !ok {
				var stringType string
				return nil, newPersistedTypeMismatchError("task_queue_name", stringType, taskQueueRaw, row)
			}

			taskQueues = append(taskQueues, taskQueue)

			row = make(map[string]interface{}) // Reinitialize map as initialized fails on unmarshalling
		}
		if len(iter.PageState()) == 0 {
			break
		}
	}

	if err := iter.Close(); err != nil {
		return nil, serviceerror.NewUnavailablef("GetTaskQueuesByBuildId operation failed. Error: %v", err)
	}
	return taskQueues, nil
}

func (d *userDataStore) CountTaskQueuesByBuildId(ctx context.Context, request *p.CountTaskQueuesByBuildIdRequest) (int, error) {
	var count int
	query := d.Session.Query(templateCountTaskQueueByBuildIdQuery, request.NamespaceID, request.BuildID).WithContext(ctx)
	err := query.Scan(&count)
	return count, err
}

// We steal some upper bits of the "row type" field to hold a subqueue index.
// Subqueue 0 must be the same as rowTypeTask (before subqueues were introduced).
// 00000000: task in subqueue 0 (rowTypeTask)
// 00000001: task queue metadata (rowTypeTaskQueue)
// xxxxxx1x: reserved
// 00000100: task in subqueue 1
// nnnnnn00: task in subqueue n, etc.
func rowTypeTaskInSubqueue(subqueue int) int {
	return subqueue<<2 | rowTypeTask // nolint:staticcheck
}

func getTaskTTL(expireTime *timestamppb.Timestamp) int64 {
	var ttl int64
	if expireTime != nil && !expireTime.AsTime().IsZero() {
		expiryTtl := convert.Int64Ceil(time.Until(expireTime.AsTime()).Seconds())

		// 0 means no ttl, we dont want that.
		// Todo: Come back and correctly ignore expired in-memory tasks before persisting
		if expiryTtl < 1 {
			expiryTtl = 1
		}

		ttl = expiryTtl
	}
	return ttl
}
