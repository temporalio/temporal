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

package postgresql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	templateCreateWorkflowExecutionStarted = `INSERT INTO executions_visibility (` +
		`namespace_id, workflow_id, run_id, start_time, execution_time, workflow_type_name, status, memo, encoding, task_queue) ` +
		`VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
         ON CONFLICT (namespace_id, run_id) DO NOTHING`

	templateCreateWorkflowExecutionClosed = `INSERT INTO executions_visibility (` +
		`namespace_id, workflow_id, run_id, start_time, execution_time, workflow_type_name, close_time, status, history_length, memo, encoding, task_queue) ` +
		`VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (namespace_id, run_id) DO UPDATE 
		  SET workflow_id = excluded.workflow_id,
		      start_time = excluded.start_time,
		      execution_time = excluded.execution_time,
              workflow_type_name = excluded.workflow_type_name,
			  close_time = excluded.close_time,
			  status = excluded.status,
			  history_length = excluded.history_length,
			  memo = excluded.memo,
			  encoding = excluded.encoding,
			  task_queue = excluded.task_queue`

	// RunID condition is needed for correct pagination
	templateConditions1 = ` AND namespace_id = $1
		 AND start_time >= $2
		 AND start_time <= $3
		 AND ((run_id > $4 and start_time = $5) OR (start_time < $6))
         ORDER BY start_time DESC, run_id
         LIMIT $7`

	templateConditions2 = ` AND namespace_id = $2
		 AND start_time >= $3
		 AND start_time <= $4
		 AND ((run_id > $5 and start_time = $6) OR (start_time < $7))
         ORDER BY start_time DESC, run_id
		 LIMIT $8`

	templateConditionsClosedWorkflow1 = ` AND namespace_id = $1
		 AND close_time >= $2
		 AND close_time <= $3
		 AND ((run_id > $4 and close_time = $5) OR (close_time < $6))
         ORDER BY close_time DESC, run_id
         LIMIT $7`

	templateConditionsClosedWorkflow2 = ` AND namespace_id = $2
		 AND close_time >= $3
		 AND close_time <= $4
 		 AND ((run_id > $5 and close_time = $6) OR (close_time < $7))
         ORDER BY close_time DESC, run_id
         LIMIT $8`

	templateOpenFieldNames = `workflow_id, run_id, start_time, execution_time, workflow_type_name, status, memo, encoding, task_queue`
	templateOpenSelect     = `SELECT ` + templateOpenFieldNames + ` FROM executions_visibility WHERE status = 1 `

	templateClosedSelect = `SELECT ` + templateOpenFieldNames + `, close_time, history_length
		 FROM executions_visibility WHERE status != 1 `

	templateGetOpenWorkflowExecutions = templateOpenSelect + templateConditions1

	templateGetClosedWorkflowExecutions = templateClosedSelect + templateConditionsClosedWorkflow1

	templateGetOpenWorkflowExecutionsByType = templateOpenSelect + `AND workflow_type_name = $1` + templateConditions2

	templateGetClosedWorkflowExecutionsByType = templateClosedSelect + `AND workflow_type_name = $1` + templateConditionsClosedWorkflow2

	templateGetOpenWorkflowExecutionsByID = templateOpenSelect + `AND workflow_id = $1` + templateConditions2

	templateGetClosedWorkflowExecutionsByID = templateClosedSelect + `AND workflow_id = $1` + templateConditionsClosedWorkflow2

	templateGetClosedWorkflowExecutionsByStatus = templateClosedSelect + `AND status = $1` + templateConditionsClosedWorkflow2

	templateGetClosedWorkflowExecution = `SELECT workflow_id, run_id, start_time, execution_time, memo, encoding, close_time, workflow_type_name, status, history_length, task_queue
		 FROM executions_visibility
		 WHERE namespace_id = $1 AND status != 1
		 AND run_id = $2`

	templateDeleteWorkflowExecution = "DELETE FROM executions_visibility WHERE namespace_id = $1 AND run_id = $2"
)

var errCloseParams = errors.New("missing one of {closeTime, historyLength} params")

// InsertIntoVisibility inserts a row into visibility table. If an row already exist,
// its left as such and no update will be made
func (pdb *db) InsertIntoVisibility(
	ctx context.Context,
	row *sqlplugin.VisibilityRow,
) (sql.Result, error) {
	row.StartTime = pdb.converter.ToPostgreSQLDateTime(row.StartTime)
	return pdb.conn.ExecContext(ctx,
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
func (pdb *db) ReplaceIntoVisibility(
	ctx context.Context,
	row *sqlplugin.VisibilityRow,
) (sql.Result, error) {
	switch {
	case row.CloseTime != nil && row.HistoryLength != nil:
		row.StartTime = pdb.converter.ToPostgreSQLDateTime(row.StartTime)
		closeTime := pdb.converter.ToPostgreSQLDateTime(*row.CloseTime)
		return pdb.conn.ExecContext(ctx,
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
func (pdb *db) DeleteFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilityDeleteFilter,
) (sql.Result, error) {
	return pdb.conn.ExecContext(ctx,
		templateDeleteWorkflowExecution,
		filter.NamespaceID,
		filter.RunID,
	)
}

// SelectFromVisibility reads one or more rows from visibility table
func (pdb *db) SelectFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilitySelectFilter,
) ([]sqlplugin.VisibilityRow, error) {
	var err error
	var rows []sqlplugin.VisibilityRow
	if filter.MinTime != nil {
		*filter.MinTime = pdb.converter.ToPostgreSQLDateTime(*filter.MinTime)
	}
	if filter.MaxTime != nil {
		*filter.MaxTime = pdb.converter.ToPostgreSQLDateTime(*filter.MaxTime)
	}
	// If filter.Status == 0 (UNSPECIFIED) then only closed workflows will be returned (all excluding 1 (RUNNING)).
	switch {
	case filter.MinTime == nil && filter.RunID != nil && filter.Status != 1:
		var row sqlplugin.VisibilityRow
		err = pdb.conn.GetContext(ctx,
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
		err = pdb.conn.SelectContext(ctx,
			&rows,
			qry,
			*filter.WorkflowID,
			filter.NamespaceID,
			*filter.MinTime,
			*filter.MaxTime,
			*filter.RunID,
			*filter.MaxTime,
			*filter.MaxTime,
			*filter.PageSize)
	case filter.MinTime != nil && filter.MaxTime != nil &&
		filter.WorkflowTypeName != nil && filter.RunID != nil && filter.PageSize != nil:
		qry := templateGetOpenWorkflowExecutionsByType
		if filter.Status != 1 {
			qry = templateGetClosedWorkflowExecutionsByType
		}
		err = pdb.conn.SelectContext(ctx,
			&rows,
			qry,
			*filter.WorkflowTypeName,
			filter.NamespaceID,
			*filter.MinTime,
			*filter.MaxTime,
			*filter.RunID,
			*filter.MaxTime,
			*filter.MaxTime,
			*filter.PageSize)
	case filter.MinTime != nil && filter.MaxTime != nil &&
		filter.RunID != nil && filter.PageSize != nil &&
		filter.Status != 0 && filter.Status != 1: // 0 is UNSPECIFIED, 1 is RUNNING
		err = pdb.conn.SelectContext(ctx,
			&rows,
			templateGetClosedWorkflowExecutionsByStatus,
			filter.Status,
			filter.NamespaceID,
			*filter.MinTime,
			*filter.MaxTime,
			*filter.RunID,
			*filter.MaxTime,
			*filter.MaxTime,
			*filter.PageSize)
	case filter.MinTime != nil && filter.MaxTime != nil &&
		filter.RunID != nil && filter.PageSize != nil:
		qry := templateGetOpenWorkflowExecutions
		if filter.Status != 1 {
			qry = templateGetClosedWorkflowExecutions
		}
		err = pdb.conn.SelectContext(ctx,
			&rows,
			qry,
			filter.NamespaceID,
			*filter.MinTime,
			*filter.MaxTime,
			*filter.RunID,
			*filter.MaxTime,
			*filter.MaxTime,
			*filter.PageSize)
	default:
		return nil, fmt.Errorf("invalid query filter")
	}
	if err != nil {
		return nil, err
	}
	for i := range rows {
		rows[i].StartTime = pdb.converter.FromPostgreSQLDateTime(rows[i].StartTime)
		rows[i].ExecutionTime = pdb.converter.FromPostgreSQLDateTime(rows[i].ExecutionTime)
		if rows[i].CloseTime != nil {
			closeTime := pdb.converter.FromPostgreSQLDateTime(*rows[i].CloseTime)
			rows[i].CloseTime = &closeTime
		}
		// need to trim the run ID, or otherwise the returned value will
		//  come with lots of trailing spaces, probably due to the CHAR(64) type
		rows[i].RunID = strings.TrimSpace(rows[i].RunID)
	}
	return rows, nil
}
