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
	// TasksRow represents a row in tasks table
	TasksRow struct {
		RangeHash    uint32
		TaskQueueID  []byte
		TaskID       int64
		Data         []byte
		DataEncoding string
	}

	// TasksFilter contains the column names within tasks table that
	// can be used to filter results through a WHERE clause
	TasksFilter struct {
		RangeHash          uint32
		TaskQueueID        []byte
		TaskID             *int64
		InclusiveMinTaskID *int64
		ExclusiveMaxTaskID *int64
		Limit              *int
		PageSize           *int
	}

	// MatchingTask is the SQL persistence interface for matching tasks
	MatchingTask interface {
		InsertIntoTasks(ctx context.Context, rows []TasksRow) (sql.Result, error)
		// SelectFromTasks retrieves one or more rows from the tasks table
		// Required filter params - {namespaceID, taskqueueName, taskType, inclusiveMinTaskID, exclusiveMaxTaskID, pageSize}
		SelectFromTasks(ctx context.Context, filter TasksFilter) ([]TasksRow, error)
		// DeleteFromTasks deletes a row from tasks table
		// Required filter params:
		//  to delete single row
		//     - {namespaceID, taskqueueName, taskType, taskID}
		//  to delete multiple rows
		//    - {namespaceID, taskqueueName, taskType, exclusiveMaxTaskID, limit }
		//    - this will delete upto limit number of tasks less than the given max task id
		DeleteFromTasks(ctx context.Context, filter TasksFilter) (sql.Result, error)
	}
)
