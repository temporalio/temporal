package sql

import (
	"context"
	"math"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
)

type (
	sqlTaskManagerV2 struct {
		SqlStore
		userDataStore
		taskQueueStore
	}
)

func newTaskManagerV2(
	db sqlplugin.DB,
	uds userDataStore,
	tqs taskQueueStore,
	logger log.Logger,
) (*sqlTaskManagerV2, error) {
	return &sqlTaskManagerV2{
		SqlStore:       NewSqlStore(db, logger),
		userDataStore:  uds,
		taskQueueStore: tqs,
	}, nil
}

func (m *sqlTaskManagerV2) CreateTasks(
	ctx context.Context,
	request *persistence.InternalCreateTasksRequest,
) (*persistence.CreateTasksResponse, error) {
	nidBytes, err := primitives.ParseUUID(request.NamespaceID)
	if err != nil {
		return nil, serviceerror.NewUnavailable(err.Error())
	}

	// cache by subqueue to minimize calls to taskQueueIdAndHash
	type pair struct {
		id   []byte
		hash uint32
	}
	cache := make(map[int]pair)
	idAndHash := func(subqueue int) ([]byte, uint32) {
		if pair, ok := cache[subqueue]; ok {
			return pair.id, pair.hash
		}
		id, hash := taskQueueIdAndHash(nidBytes, request.TaskQueue, request.TaskType, subqueue)
		cache[subqueue] = pair{id: id, hash: hash}
		return id, hash
	}

	tasksRows := make([]sqlplugin.TasksRowV2, len(request.Tasks))
	for i, v := range request.Tasks {
		tqId, tqHash := idAndHash(v.Subqueue)
		tasksRows[i] = sqlplugin.TasksRowV2{
			RangeHash:   tqHash,
			TaskQueueID: tqId,
			FairLevel: sqlplugin.FairLevel{
				TaskPass: v.TaskPass,
				TaskID:   v.TaskId,
			},
			Data:         v.Task.Data,
			DataEncoding: v.Task.EncodingType.String(),
		}
	}
	var resp *persistence.CreateTasksResponse
	err = m.SqlStore.txExecute(ctx, "CreateTasks", func(tx sqlplugin.Tx) error {
		if _, err1 := tx.InsertIntoTasksV2(ctx, tasksRows); err1 != nil {
			return err1
		}
		// Lock task queue before committing.
		tqId, tqHash := idAndHash(persistence.SubqueueZero)
		if err := lockTaskQueue(ctx,
			tx,
			tqHash,
			tqId,
			request.RangeID,
			sqlplugin.MatchingTaskVersion2,
		); err != nil {
			return err
		}
		resp = &persistence.CreateTasksResponse{UpdatedMetadata: false}
		return nil
	})
	return resp, err
}

func (m *sqlTaskManagerV2) GetTasks(
	ctx context.Context,
	request *persistence.GetTasksRequest,
) (*persistence.InternalGetTasksResponse, error) {
	if request.InclusiveMinPass < 1 {
		return nil, serviceerror.NewInternal("invalid GetTasks request on fair queue: InclusiveMinPass must be >= 1")
	}
	if request.ExclusiveMaxTaskID != math.MaxInt64 {
		// ExclusiveMaxTaskID is not supported in fair queue.
		return nil, serviceerror.NewInternal("invalid GetTasks request on fair queue: ExclusiveMaxTaskID is not supported")
	}
	nidBytes, err := primitives.ParseUUID(request.NamespaceID)
	if err != nil {
		return nil, serviceerror.NewUnavailable(err.Error())
	}

	inclusiveMinLevel := sqlplugin.FairLevel{
		TaskPass: request.InclusiveMinPass,
		TaskID:   request.InclusiveMinTaskID,
	}
	if len(request.NextPageToken) != 0 {
		token, err := deserializePageTokenJson[matchingTaskPageToken](request.NextPageToken)
		if err != nil {
			return nil, err
		} else if token.TaskPass < 1 {
			return nil, serviceerror.NewInternal("invalid token: missing TaskPass")
		}
		inclusiveMinLevel = sqlplugin.FairLevel{
			TaskPass: token.TaskPass,
			TaskID:   token.TaskID,
		}
	}

	tqId, tqHash := taskQueueIdAndHash(nidBytes, request.TaskQueue, request.TaskType, request.Subqueue)
	rows, err := m.DB.SelectFromTasksV2(ctx, sqlplugin.TasksFilterV2{
		RangeHash:         tqHash,
		TaskQueueID:       tqId,
		InclusiveMinLevel: &inclusiveMinLevel,
		PageSize:          &request.PageSize,
	})
	if err != nil {
		return nil, serviceerror.NewUnavailablef("GetTasks operation failed. Failed to get rows. Error: %v", err)
	}

	response := &persistence.InternalGetTasksResponse{
		Tasks: make([]*commonpb.DataBlob, len(rows)),
	}
	for i, v := range rows {
		response.Tasks[i] = persistence.NewDataBlob(v.Data, v.DataEncoding)
	}
	if len(rows) == request.PageSize {
		token, err := serializePageTokenJson(&matchingTaskPageToken{
			TaskPass: rows[len(rows)-1].TaskPass,
			TaskID:   rows[len(rows)-1].TaskID + 1,
		})
		if err != nil {
			return nil, err
		}
		response.NextPageToken = token
	}

	return response, nil
}

func (m *sqlTaskManagerV2) CompleteTasksLessThan(
	ctx context.Context,
	request *persistence.CompleteTasksLessThanRequest,
) (int, error) {
	// Require starting from pass 1.
	if request.ExclusiveMaxPass < 1 {
		return 0, serviceerror.NewInternal("invalid CompleteTasksLessThan request on fair queue")
	}

	nidBytes, err := primitives.ParseUUID(request.NamespaceID)
	if err != nil {
		return 0, serviceerror.NewUnavailable(err.Error())
	}
	tqId, tqHash := taskQueueIdAndHash(nidBytes, request.TaskQueueName, request.TaskType, request.Subqueue)
	exclusiveMaxLevel := sqlplugin.FairLevel{
		TaskPass: request.ExclusiveMaxPass,
		TaskID:   request.ExclusiveMaxTaskID,
	}
	result, err := m.DB.DeleteFromTasksV2(ctx, sqlplugin.TasksFilterV2{
		RangeHash:         tqHash,
		TaskQueueID:       tqId,
		ExclusiveMaxLevel: &exclusiveMaxLevel,
		Limit:             &request.Limit,
	})
	if err != nil {
		return 0, serviceerror.NewUnavailable(err.Error())
	}
	nRows, err := result.RowsAffected()
	if err != nil {
		return 0, serviceerror.NewUnavailablef("rowsAffected returned error: %v", err)
	}
	return int(nRows), nil
}
