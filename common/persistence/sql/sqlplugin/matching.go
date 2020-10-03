// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
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
	"database/sql"
)

type (
	// MatchingTask is the SQL persistence interface for matching tasks
	MatchingTask interface {
		InsertIntoTasks(rows []TasksRow) (sql.Result, error)
		// SelectFromTasks retrieves one or more rows from the tasks table
		// Required filter params - {namespaceID, taskqueueName, taskType, minTaskID, maxTaskID, pageSize}
		SelectFromTasks(filter *TasksFilter) ([]TasksRow, error)
		// DeleteFromTasks deletes a row from tasks table
		// Required filter params:
		//  to delete single row
		//     - {namespaceID, taskqueueName, taskType, taskID}
		//  to delete multiple rows
		//    - {namespaceID, taskqueueName, taskType, taskIDLessThanEquals, limit }
		//    - this will delete upto limit number of tasks less than or equal to the given task id
		DeleteFromTasks(filter *TasksFilter) (sql.Result, error)
	}

	// MatchingTaskQueue is the SQL persistence interface for matching task queues
	MatchingTaskQueue interface {
		InsertIntoTaskQueues(row *TaskQueuesRow) (sql.Result, error)
		ReplaceIntoTaskQueues(row *TaskQueuesRow) (sql.Result, error)
		UpdateTaskQueues(row *TaskQueuesRow) (sql.Result, error)
		// SelectFromTaskQueues returns one or more rows from task_queues table
		// Required Filter params:
		//  to read a single row: {shardID, namespaceID, name, taskType}
		//  to range read multiple rows: {shardID, namespaceIDGreaterThan, nameGreaterThan, taskTypeGreaterThan, pageSize}
		SelectFromTaskQueues(filter *TaskQueuesFilter) ([]TaskQueuesRow, error)
		DeleteFromTaskQueues(filter *TaskQueuesFilter) (sql.Result, error)
		LockTaskQueues(filter *TaskQueuesFilter) (int64, error)
	}
)
