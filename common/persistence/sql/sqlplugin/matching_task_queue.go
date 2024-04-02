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
