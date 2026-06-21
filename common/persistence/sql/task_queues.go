package sql

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
)

type taskQueueStore struct {
	SqlStore
	version            sqlplugin.MatchingTaskVersion
	taskScanPartitions uint32
}

func (m *taskQueueStore) CreateTaskQueue(
	ctx context.Context,
	request *persistence.InternalCreateTaskQueueRequest,
) error {
	nidBytes, err := primitives.ParseUUID(request.NamespaceID)
	if err != nil {
		return serviceerror.NewInternal(err.Error())
	}
	tqId, tqHash := taskQueueIdAndHash(nidBytes, request.TaskQueue, request.TaskType, persistence.SubqueueZero)

	row := sqlplugin.TaskQueuesRow{
		RangeHash:    tqHash,
		TaskQueueID:  tqId,
		RangeID:      request.RangeID,
		Data:         request.TaskQueueInfo.Data,
		DataEncoding: request.TaskQueueInfo.EncodingType.String(),
	}
	if _, err := m.DB.InsertIntoTaskQueues(ctx, &row, m.version); err != nil {
		if m.DB.IsDupEntryError(err) {
			return &persistence.ConditionFailedError{Msg: err.Error()}
		}
		return serviceerror.NewUnavailablef("CreateTaskQueue operation failed. Failed to make task queue %v of type %v. Error: %v", request.TaskQueue, request.TaskType, err)
	}

	return nil
}

func (m *taskQueueStore) GetTaskQueue(
	ctx context.Context,
	request *persistence.InternalGetTaskQueueRequest,
) (*persistence.InternalGetTaskQueueResponse, error) {
	nidBytes, err := primitives.ParseUUID(request.NamespaceID)
	if err != nil {
		return nil, serviceerror.NewInternal(err.Error())
	}
	tqId, tqHash := taskQueueIdAndHash(nidBytes, request.TaskQueue, request.TaskType, persistence.SubqueueZero)
	rows, err := m.DB.SelectFromTaskQueues(ctx, sqlplugin.TaskQueuesFilter{
		RangeHash:   tqHash,
		TaskQueueID: tqId,
	}, m.version)

	switch err {
	case nil:
		if len(rows) != 1 {
			return nil, serviceerror.NewUnavailablef(
				"GetTaskQueue operation failed. Expect exactly one result row, but got %d for task queue %v of type %v",
				len(rows), request.TaskQueue, request.TaskType)
		}
		row := rows[0]
		return &persistence.InternalGetTaskQueueResponse{
			RangeID:       row.RangeID,
			TaskQueueInfo: persistence.NewDataBlob(row.Data, row.DataEncoding),
		}, nil
	case sql.ErrNoRows:
		return nil, serviceerror.NewNotFoundf(
			"GetTaskQueue operation failed. TaskQueue: %v, TaskQueueType: %v, Error: %v",
			request.TaskQueue, request.TaskType, err)
	default:
		return nil, serviceerror.NewUnavailablef(
			"GetTaskQueue operation failed. Failed to check if task queue %v of type %v existed. Error: %v",
			request.TaskQueue, request.TaskType, err)
	}
}

func (m *taskQueueStore) UpdateTaskQueue(
	ctx context.Context,
	request *persistence.InternalUpdateTaskQueueRequest,
) (*persistence.UpdateTaskQueueResponse, error) {
	nidBytes, err := primitives.ParseUUID(request.NamespaceID)
	if err != nil {
		return nil, serviceerror.NewInternal(err.Error())
	}

	tqId, tqHash := taskQueueIdAndHash(nidBytes, request.TaskQueue, request.TaskType, persistence.SubqueueZero)
	var resp *persistence.UpdateTaskQueueResponse
	err = m.SqlStore.txExecute(ctx, "UpdateTaskQueue", func(tx sqlplugin.Tx) error {
		if err := lockTaskQueue(ctx,
			tx,
			tqHash,
			tqId,
			request.PrevRangeID,
			m.version,
		); err != nil {
			return err
		}
		result, err := tx.UpdateTaskQueues(ctx, &sqlplugin.TaskQueuesRow{
			RangeHash:    tqHash,
			TaskQueueID:  tqId,
			RangeID:      request.RangeID,
			Data:         request.TaskQueueInfo.Data,
			DataEncoding: request.TaskQueueInfo.EncodingType.String(),
		}, m.version)
		if err != nil {
			return err
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rowsAffected != 1 {
			return fmt.Errorf("%v rows were affected instead of 1", rowsAffected)
		}
		resp = &persistence.UpdateTaskQueueResponse{}
		return nil
	})
	return resp, err
}

func (m *taskQueueStore) ListTaskQueue(
	ctx context.Context,
	request *persistence.ListTaskQueueRequest,
) (*persistence.InternalListTaskQueueResponse, error) {
	pageToken := taskQueuePageToken{MinTaskQueueId: minTaskQueueId}
	if request.PageToken != nil {
		if err := gobDeserialize(request.PageToken, &pageToken); err != nil {
			return nil, serviceerror.NewInternalf("error deserializing page token: %v", err)
		}
	}
	var err error
	var rows []sqlplugin.TaskQueuesRow
	var shardGreaterThan uint32
	var shardLessThan uint32

	i := uint32(0)
	if pageToken.MinRangeHash > 0 {
		// Resume partition position from page token, if exists, before entering loop
		i = getPartitionForRangeHash(pageToken.MinRangeHash, m.taskScanPartitions)
	}

	lastPageFull := !bytes.Equal(pageToken.MinTaskQueueId, minTaskQueueId)
	for ; i < m.taskScanPartitions; i++ {
		// Get start/end boundaries for partition
		shardGreaterThan, shardLessThan = getBoundariesForPartition(i, m.taskScanPartitions)

		// If page token hash is greater than the boundaries for this partition, use the pageToken hash for resume point
		if pageToken.MinRangeHash > shardGreaterThan {
			shardGreaterThan = pageToken.MinRangeHash
		}

		filter := sqlplugin.TaskQueuesFilter{
			RangeHashGreaterThanEqualTo: shardGreaterThan,
			RangeHashLessThanEqualTo:    shardLessThan,
			TaskQueueIDGreaterThan:      minTaskQueueId,
			PageSize:                    &request.PageSize,
		}

		if lastPageFull {
			// Use page token TaskQueueID filter for this query and set this to false
			// in order for the next partition so we don't miss any results.
			filter.TaskQueueIDGreaterThan = pageToken.MinTaskQueueId
			lastPageFull = false
		}

		rows, err = m.DB.SelectFromTaskQueues(ctx, filter, m.version)
		if err != nil {
			return nil, serviceerror.NewUnavailable(err.Error())
		}

		if len(rows) > 0 {
			break
		}
	}

	maxRangeHash := uint32(0)
	resp := &persistence.InternalListTaskQueueResponse{
		Items: make([]*persistence.InternalListTaskQueueItem, len(rows)),
	}

	for i, row := range rows {
		resp.Items[i] = &persistence.InternalListTaskQueueItem{
			RangeID:   row.RangeID,
			TaskQueue: persistence.NewDataBlob(row.Data, row.DataEncoding),
		}

		// Only want to look at up to PageSize number of records to prevent losing data.
		if row.RangeHash > maxRangeHash {
			maxRangeHash = row.RangeHash
		}

		// Enforces PageSize
		if i >= request.PageSize-1 {
			break
		}
	}

	var nextPageToken []byte
	switch {
	case len(rows) >= request.PageSize:
		// Store the details of the lastRow seen up to PageSize.
		// Note we don't increment the rangeHash as we do in the case below.
		// This is so we can exhaust this hash before moving forward.
		lastRow := &rows[request.PageSize-1]
		nextPageToken, err = gobSerialize(&taskQueuePageToken{
			MinRangeHash:   shardGreaterThan,
			MinTaskQueueId: lastRow.TaskQueueID,
		})
	case shardLessThan < math.MaxUint32:
		// Create page token with +1 from the last rangeHash we have seen to prevent duplicating the last row.
		// Since we have not exceeded PageSize, we are confident we won't lose data here and we have exhausted this hash.
		nextPageToken, err = gobSerialize(&taskQueuePageToken{MinRangeHash: shardLessThan + 1, MinTaskQueueId: minTaskQueueId})
	}

	if err != nil {
		return nil, serviceerror.NewUnavailablef("error serializing nextPageToken:%v", err)
	}

	resp.NextPageToken = nextPageToken
	return resp, nil
}

func (m *taskQueueStore) DeleteTaskQueue(
	ctx context.Context,
	request *persistence.DeleteTaskQueueRequest,
) error {
	nidBytes, err := primitives.ParseUUID(request.TaskQueue.NamespaceID)
	if err != nil {
		return serviceerror.NewUnavailable(err.Error())
	}
	tqId, tqHash := taskQueueIdAndHash(nidBytes, request.TaskQueue.TaskQueueName, request.TaskQueue.TaskQueueType, persistence.SubqueueZero)
	result, err := m.DB.DeleteFromTaskQueues(ctx, sqlplugin.TaskQueuesFilter{
		RangeHash:   tqHash,
		TaskQueueID: tqId,
		RangeID:     &request.RangeID,
	}, m.version)
	if err != nil {
		return serviceerror.NewUnavailable(err.Error())
	}
	nRows, err := result.RowsAffected()
	if err != nil {
		return serviceerror.NewUnavailablef("rowsAffected returned error:%v", err)
	}
	if nRows != 1 {
		return &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("delete failed: %v rows affected instead of 1", nRows),
		}
	}
	return nil
}
