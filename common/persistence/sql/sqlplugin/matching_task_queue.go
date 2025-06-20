package sqlplugin

import (
	"context"
	"database/sql"
)

type (
	// TaskQueuesRow represents a row in task_queues table
	TaskQueuesRow struct {
		RangeHash    uint32
		TaskQueueID  []byte
		RangeID      int64
		Data         []byte
		DataEncoding string
	}

	// TaskQueuesFilter contains the column names within task_queues table that
	// can be used to filter results through a WHERE clause
	TaskQueuesFilter struct {
		RangeHash                   uint32
		RangeHashGreaterThanEqualTo uint32
		RangeHashLessThanEqualTo    uint32
		TaskQueueID                 []byte
		TaskQueueIDGreaterThan      []byte
		RangeID                     *int64
		PageSize                    *int
	}

	GetTaskQueueUserDataRequest struct {
		NamespaceID   []byte
		TaskQueueName string
	}

	UpdateTaskQueueDataRequest struct {
		NamespaceID   []byte
		TaskQueueName string
		Version       int64
		Data          []byte
		DataEncoding  string
	}

	AddToBuildIdToTaskQueueMapping struct {
		NamespaceID   []byte
		TaskQueueName string
		BuildIds      []string
	}

	RemoveFromBuildIdToTaskQueueMapping struct {
		NamespaceID   []byte
		TaskQueueName string
		BuildIds      []string
	}

	GetTaskQueuesByBuildIdRequest struct {
		NamespaceID []byte
		BuildID     string
	}

	CountTaskQueuesByBuildIdRequest struct {
		NamespaceID []byte
		BuildID     string
	}

	ListTaskQueueUserDataEntriesRequest struct {
		NamespaceID       []byte
		LastTaskQueueName string
		Limit             int
	}

	TaskQueueUserDataEntry struct {
		TaskQueueName string
		VersionedBlob
	}

	// MatchingTaskQueue is the SQL persistence interface for matching task queues
	MatchingTaskQueue interface {
		InsertIntoTaskQueues(ctx context.Context, row *TaskQueuesRow) (sql.Result, error)
		UpdateTaskQueues(ctx context.Context, row *TaskQueuesRow) (sql.Result, error)
		// SelectFromTaskQueues returns one or more rows from task_queues table
		// Required Filter params:
		//  to read a single row: {shardID, namespaceID, name, taskType}
		//  to range read multiple rows: {shardID, namespaceIDGreaterThan, nameGreaterThan, taskTypeGreaterThan, pageSize}
		SelectFromTaskQueues(ctx context.Context, filter TaskQueuesFilter) ([]TaskQueuesRow, error)
		DeleteFromTaskQueues(ctx context.Context, filter TaskQueuesFilter) (sql.Result, error)
		LockTaskQueues(ctx context.Context, filter TaskQueuesFilter) (int64, error)
		GetTaskQueueUserData(ctx context.Context, request *GetTaskQueueUserDataRequest) (*VersionedBlob, error)
		UpdateTaskQueueUserData(ctx context.Context, request *UpdateTaskQueueDataRequest) error
		AddToBuildIdToTaskQueueMapping(ctx context.Context, request AddToBuildIdToTaskQueueMapping) error
		RemoveFromBuildIdToTaskQueueMapping(ctx context.Context, request RemoveFromBuildIdToTaskQueueMapping) error
		ListTaskQueueUserDataEntries(ctx context.Context, request *ListTaskQueueUserDataEntriesRequest) ([]TaskQueueUserDataEntry, error)
		GetTaskQueuesByBuildId(ctx context.Context, request *GetTaskQueuesByBuildIdRequest) ([]string, error)
		CountTaskQueuesByBuildId(ctx context.Context, request *CountTaskQueuesByBuildIdRequest) (int, error)
	}
)
