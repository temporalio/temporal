package sql

import (
	"context"
	"database/sql"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
)

type (
	sqlTaskManagerV1 struct {
		SqlStore
		userDataStore
		taskQueueStore
	}
)

var (
	// minUUID = primitives.MustParseUUID("00000000-0000-0000-0000-000000000000")
	minTaskQueueId = make([]byte, 0)
)

func newTaskManagerV1(
	db sqlplugin.DB,
	uds userDataStore,
	tqs taskQueueStore,
	logger log.Logger,
) (*sqlTaskManagerV1, error) {
	return &sqlTaskManagerV1{
		SqlStore:       NewSqlStore(db, logger),
		userDataStore:  uds,
		taskQueueStore: tqs,
	}, nil
}

func (m *sqlTaskManagerV1) CreateTasks(
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

	tasksRows := make([]sqlplugin.TasksRow, len(request.Tasks))
	for i, v := range request.Tasks {
		tqId, tqHash := idAndHash(v.Subqueue)
		tasksRows[i] = sqlplugin.TasksRow{
			RangeHash:    tqHash,
			TaskQueueID:  tqId,
			TaskID:       v.TaskId,
			Data:         v.Task.Data,
			DataEncoding: v.Task.EncodingType.String(),
		}
	}
	var resp *persistence.CreateTasksResponse
	err = m.SqlStore.txExecute(ctx, "CreateTasks", func(tx sqlplugin.Tx) error {
		if _, err1 := tx.InsertIntoTasks(ctx, tasksRows); err1 != nil {
			return err1
		}
		// Lock task queue before committing.
		tqId, tqHash := idAndHash(persistence.SubqueueZero)
		if err := lockTaskQueue(ctx,
			tx,
			tqHash,
			tqId,
			request.RangeID,
			sqlplugin.MatchingTaskVersion1,
		); err != nil {
			return err
		}
		resp = &persistence.CreateTasksResponse{UpdatedMetadata: false}
		return nil
	})
	return resp, err
}

func (m *sqlTaskManagerV1) GetTasks(
	ctx context.Context,
	request *persistence.GetTasksRequest,
) (*persistence.InternalGetTasksResponse, error) {
	if request.InclusiveMinPass != 0 {
		return nil, serviceerror.NewInternal("invalid GetTasks request on queue: InclusiveMinPass is not supported")
	}

	nidBytes, err := primitives.ParseUUID(request.NamespaceID)
	if err != nil {
		return nil, serviceerror.NewUnavailable(err.Error())
	}

	inclusiveMinTaskID := request.InclusiveMinTaskID
	exclusiveMaxTaskID := request.ExclusiveMaxTaskID
	if len(request.NextPageToken) != 0 {
		token, err := deserializePageTokenJson[matchingTaskPageToken](request.NextPageToken)
		if err != nil {
			return nil, err
		}
		inclusiveMinTaskID = token.TaskID
	}

	tqId, tqHash := taskQueueIdAndHash(nidBytes, request.TaskQueue, request.TaskType, request.Subqueue)
	rows, err := m.DB.SelectFromTasks(ctx, sqlplugin.TasksFilter{
		RangeHash:          tqHash,
		TaskQueueID:        tqId,
		InclusiveMinTaskID: &inclusiveMinTaskID,
		ExclusiveMaxTaskID: &exclusiveMaxTaskID,
		PageSize:           &request.PageSize,
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
		nextTaskID := rows[len(rows)-1].TaskID + 1
		if nextTaskID < exclusiveMaxTaskID {
			token, err := serializePageTokenJson(&matchingTaskPageToken{
				TaskID: nextTaskID,
			})
			if err != nil {
				return nil, err
			}
			response.NextPageToken = token
		}
	}

	return response, nil
}

func (m *sqlTaskManagerV1) CompleteTasksLessThan(
	ctx context.Context,
	request *persistence.CompleteTasksLessThanRequest,
) (int, error) {
	if request.ExclusiveMaxPass != 0 {
		return 0, serviceerror.NewInternal("invalid CompleteTasksLessThan request on queue")
	}

	nidBytes, err := primitives.ParseUUID(request.NamespaceID)
	if err != nil {
		return 0, serviceerror.NewUnavailable(err.Error())
	}
	tqId, tqHash := taskQueueIdAndHash(nidBytes, request.TaskQueueName, request.TaskType, request.Subqueue)
	result, err := m.DB.DeleteFromTasks(ctx, sqlplugin.TasksFilter{
		RangeHash:          tqHash,
		TaskQueueID:        tqId,
		ExclusiveMaxTaskID: &request.ExclusiveMaxTaskID,
		Limit:              &request.Limit,
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

func lockTaskQueue(
	ctx context.Context,
	tx sqlplugin.Tx,
	tqHash uint32,
	tqId []byte,
	oldRangeID int64,
	v sqlplugin.MatchingTaskVersion,
) error {
	rangeID, err := tx.LockTaskQueues(ctx, sqlplugin.TaskQueuesFilter{
		RangeHash:   tqHash,
		TaskQueueID: tqId,
	}, v)
	switch err {
	case nil:
		if rangeID != oldRangeID {
			return &persistence.ConditionFailedError{
				Msg: fmt.Sprintf("Task queue range ID was %v when it was should have been %v", rangeID, oldRangeID),
			}
		}
		return nil

	case sql.ErrNoRows:
		return &persistence.ConditionFailedError{Msg: "Task queue does not exists"}

	default:
		return serviceerror.NewUnavailablef("Failed to lock task queue. Error: %v", err)
	}
}
