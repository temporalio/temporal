// The MIT License
//
// Copyright (c) 2021 Datadog, Inc.
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

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	templateCreateWorkflowExecutionStarted = `INSERT INTO executions_visibility (` +
		`namespace_id, workflow_id, run_id, start_time, execution_time, workflow_type_name, status, memo, encoding, task_queue) ` +
		`VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ` +
		`ON CONFLICT (namespace_id, run_id) DO NOTHING`

	templateCreateWorkflowExecutionClosed = `REPLACE INTO executions_visibility (` +
		`namespace_id, workflow_id, run_id, start_time, execution_time, workflow_type_name, close_time, status, history_length, memo, encoding, task_queue) ` +
		`VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) `

	// RunID condition is needed for correct pagination
	templateConditions = ` AND namespace_id = ?
		 AND start_time >= ?
		 AND start_time <= ?
 		 AND ((run_id > ? and start_time = ?) OR (start_time < ?))
         ORDER BY start_time DESC, run_id
		 LIMIT ?`

	templateConditionsClosedWorkflows = ` AND namespace_id = ?
	AND close_time >= ?
	AND close_time <= ?
	 AND ((run_id > ? and close_time = ?) OR (close_time < ?))
	ORDER BY close_time DESC, run_id
	LIMIT ?`

	templateOpenFieldNames = `workflow_id, run_id, start_time, execution_time, workflow_type_name, status, memo, encoding, task_queue`
	templateOpenSelect     = `SELECT ` + templateOpenFieldNames + ` FROM executions_visibility WHERE status = 1 `

	templateClosedSelect = `SELECT ` + templateOpenFieldNames + `, close_time, history_length
		 FROM executions_visibility WHERE status != 1 `

	templateGetOpenWorkflowExecutions = templateOpenSelect + templateConditions

	templateGetClosedWorkflowExecutions = templateClosedSelect + templateConditionsClosedWorkflows

	templateGetOpenWorkflowExecutionsByType = templateOpenSelect + `AND workflow_type_name = ?` + templateConditions

	templateGetClosedWorkflowExecutionsByType = templateClosedSelect + `AND workflow_type_name = ?` + templateConditionsClosedWorkflows

	templateGetOpenWorkflowExecutionsByID = templateOpenSelect + `AND workflow_id = ?` + templateConditions

	templateGetClosedWorkflowExecutionsByID = templateClosedSelect + `AND workflow_id = ?` + templateConditionsClosedWorkflows

	templateGetClosedWorkflowExecutionsByStatus = templateClosedSelect + `AND status = ?` + templateConditionsClosedWorkflows

	templateGetClosedWorkflowExecution = `SELECT workflow_id, run_id, start_time, execution_time, memo, encoding, close_time, workflow_type_name, status, history_length, task_queue 
		 FROM executions_visibility
		 WHERE namespace_id = ? AND status != 1
		 AND run_id = ?`

	templateDeleteWorkflowExecution = "DELETE FROM executions_visibility WHERE namespace_id = ? AND run_id = ?"
)

var errCloseParams = errors.New("missing one of {closeTime, historyLength} params")

// InsertIntoVisibility inserts a row into visibility table. If an row already exist,
// its left as such and no update will be made
func (mdb *db) InsertIntoVisibility(
	ctx context.Context,
	row *sqlplugin.VisibilityRow,
) (sql.Result, error) {
	row.StartTime = mdb.converter.ToSQLiteDateTime(row.StartTime)
	return mdb.conn.ExecContext(ctx,
		templateCreateWorkflowExecutionStarted,
		row.NamespaceID,
		row.WorkflowID,
		row.RunID,
		row.StartTime,
		row.ExecutionTime,
		row.WorkflowTypeName,
		row.Status,
		row.Memo,
		row.Encoding,
		row.TaskQueue,
	)
}

// ReplaceIntoVisibility replaces an existing row if it exist or creates a new row in visibility table
func (mdb *db) ReplaceIntoVisibility(
	ctx context.Context,
	row *sqlplugin.VisibilityRow,
) (sql.Result, error) {
	switch {
	case row.CloseTime != nil && row.HistoryLength != nil:
		row.StartTime = mdb.converter.ToSQLiteDateTime(row.StartTime)
		closeTime := mdb.converter.ToSQLiteDateTime(*row.CloseTime)
		return mdb.conn.ExecContext(ctx,
			templateCreateWorkflowExecutionClosed,
			row.NamespaceID,
			row.WorkflowID,
			row.RunID,
			row.StartTime,
			row.ExecutionTime,
			row.WorkflowTypeName,
			closeTime,
			row.Status,
			*row.HistoryLength,
			row.Memo,
			row.Encoding,
			row.TaskQueue,
		)
	default:
		return nil, errCloseParams
	}
}

// DeleteFromVisibility deletes a row from visibility table if it exist
func (mdb *db) DeleteFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilityDeleteFilter,
) (sql.Result, error) {
	return mdb.conn.ExecContext(ctx,
		templateDeleteWorkflowExecution,
		filter.NamespaceID,
		filter.RunID,
	)
}

// SelectFromVisibility reads one or more rows from visibility table
func (mdb *db) SelectFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilitySelectFilter,
) ([]sqlplugin.VisibilityRow, error) {
	var err error
	var rows []sqlplugin.VisibilityRow
	if filter.MinTime != nil {
		*filter.MinTime = mdb.converter.ToSQLiteDateTime(*filter.MinTime)
	}
	if filter.MaxTime != nil {
		*filter.MaxTime = mdb.converter.ToSQLiteDateTime(*filter.MaxTime)
	}
	// If filter.Status == 0 (UNSPECIFIED) then only closed workflows will be returned (all excluding 1 (RUNNING)).
	switch {
	case filter.MinTime == nil && filter.RunID != nil && filter.Status != 1:
		var row sqlplugin.VisibilityRow
		err = mdb.conn.GetContext(ctx,
			&row,
			templateGetClosedWorkflowExecution,
			filter.NamespaceID,
			*filter.RunID,
		)
		if err == nil {
			rows = append(rows, row)
		}
	case filter.MinTime != nil && filter.MaxTime != nil &&
		filter.WorkflowID != nil && filter.RunID != nil && filter.PageSize != nil:
		qry := templateGetOpenWorkflowExecutionsByID
		if filter.Status != 1 {
			qry = templateGetClosedWorkflowExecutionsByID
		}
		err = mdb.conn.SelectContext(ctx,
			&rows,
			qry,
			*filter.WorkflowID,
			filter.NamespaceID,
			*filter.MinTime,
			*filter.MaxTime,
			*filter.RunID,
			*filter.MaxTime,
			*filter.MaxTime,
			*filter.PageSize,
		)
	case filter.MinTime != nil && filter.MaxTime != nil &&
		filter.WorkflowTypeName != nil && filter.RunID != nil && filter.PageSize != nil:
		qry := templateGetOpenWorkflowExecutionsByType
		if filter.Status != 1 {
			qry = templateGetClosedWorkflowExecutionsByType
		}
		err = mdb.conn.SelectContext(ctx,
			&rows,
			qry,
			*filter.WorkflowTypeName,
			filter.NamespaceID,
			*filter.MinTime,
			*filter.MaxTime,
			*filter.RunID,
			*filter.MaxTime,
			*filter.MaxTime,
			*filter.PageSize,
		)
	case filter.MinTime != nil && filter.MaxTime != nil &&
		filter.RunID != nil && filter.PageSize != nil &&
		filter.Status != 0 && filter.Status != 1: // 0 is UNSPECIFIED, 1 is RUNNING
		err = mdb.conn.SelectContext(ctx,
			&rows,
			templateGetClosedWorkflowExecutionsByStatus,
			filter.Status,
			filter.NamespaceID,
			*filter.MinTime,
			*filter.MaxTime,
			*filter.RunID,
			*filter.MaxTime,
			*filter.MaxTime,
			*filter.PageSize,
		)
	case filter.MinTime != nil && filter.MaxTime != nil &&
		filter.RunID != nil && filter.PageSize != nil:
		qry := templateGetOpenWorkflowExecutions
		if filter.Status != 1 {
			qry = templateGetClosedWorkflowExecutions
		}
		err = mdb.conn.SelectContext(ctx,
			&rows,
			qry,
			filter.NamespaceID,
			*filter.MinTime,
			*filter.MaxTime,
			*filter.RunID,
			*filter.MaxTime,
			*filter.MaxTime,
			*filter.PageSize,
		)
	default:
		return nil, fmt.Errorf("invalid query filter")
	}
	if err != nil {
		return nil, err
	}
	for i := range rows {
		rows[i].StartTime = mdb.converter.FromSQLiteDateTime(rows[i].StartTime)
		rows[i].ExecutionTime = mdb.converter.FromSQLiteDateTime(rows[i].ExecutionTime)
		if rows[i].CloseTime != nil {
			closeTime := mdb.converter.FromSQLiteDateTime(*rows[i].CloseTime)
			rows[i].CloseTime = &closeTime
		}
	}
	return rows, nil
}
