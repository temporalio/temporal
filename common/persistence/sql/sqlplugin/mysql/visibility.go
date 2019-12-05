// Copyright (c) 2017 Uber Technologies, Inc.
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

package mysql

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
)

const (
	templateCreateWorkflowExecutionStarted = `INSERT IGNORE INTO executions_visibility (` +
		`domain_id, workflow_id, run_id, start_time, execution_time, workflow_type_name, memo, encoding) ` +
		`VALUES (?, ?, ?, ?, ?, ?, ?, ?)`

	templateCreateWorkflowExecutionClosed = `REPLACE INTO executions_visibility (` +
		`domain_id, workflow_id, run_id, start_time, execution_time, workflow_type_name, close_time, close_status, history_length, memo, encoding) ` +
		`VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	// RunID condition is needed for correct pagination
	templateConditions = ` AND domain_id = ?
		 AND start_time >= ?
		 AND start_time <= ?
 		 AND (run_id > ? OR start_time < ?)
         ORDER BY start_time DESC, run_id
         LIMIT ?`

	templateOpenFieldNames = `workflow_id, run_id, start_time, execution_time, workflow_type_name, memo, encoding`
	templateOpenSelect     = `SELECT ` + templateOpenFieldNames + ` FROM executions_visibility WHERE close_status IS NULL `

	templateClosedSelect = `SELECT ` + templateOpenFieldNames + `, close_time, close_status, history_length
		 FROM executions_visibility WHERE close_status IS NOT NULL `

	templateGetOpenWorkflowExecutions = templateOpenSelect + templateConditions

	templateGetClosedWorkflowExecutions = templateClosedSelect + templateConditions

	templateGetOpenWorkflowExecutionsByType = templateOpenSelect + `AND workflow_type_name = ?` + templateConditions

	templateGetClosedWorkflowExecutionsByType = templateClosedSelect + `AND workflow_type_name = ?` + templateConditions

	templateGetOpenWorkflowExecutionsByID = templateOpenSelect + `AND workflow_id = ?` + templateConditions

	templateGetClosedWorkflowExecutionsByID = templateClosedSelect + `AND workflow_id = ?` + templateConditions

	templateGetClosedWorkflowExecutionsByStatus = templateClosedSelect + `AND close_status = ?` + templateConditions

	templateGetClosedWorkflowExecution = `SELECT workflow_id, run_id, start_time, execution_time, memo, encoding, close_time, workflow_type_name, close_status, history_length 
		 FROM executions_visibility
		 WHERE domain_id = ? AND close_status IS NOT NULL
		 AND run_id = ?`

	templateDeleteWorkflowExecution = "DELETE FROM executions_visibility WHERE domain_id=? AND run_id=?"
)

var errCloseParams = errors.New("missing one of {closeStatus, closeTime, historyLength} params")

// InsertIntoVisibility inserts a row into visibility table. If an row already exist,
// its left as such and no update will be made
func (mdb *db) InsertIntoVisibility(row *sqlplugin.VisibilityRow) (sql.Result, error) {
	row.StartTime = mdb.converter.ToMySQLDateTime(row.StartTime)
	return mdb.conn.Exec(templateCreateWorkflowExecutionStarted,
		row.DomainID,
		row.WorkflowID,
		row.RunID,
		row.StartTime,
		row.ExecutionTime,
		row.WorkflowTypeName,
		row.Memo,
		row.Encoding)
}

// ReplaceIntoVisibility replaces an existing row if it exist or creates a new row in visibility table
func (mdb *db) ReplaceIntoVisibility(row *sqlplugin.VisibilityRow) (sql.Result, error) {
	switch {
	case row.CloseStatus != nil && row.CloseTime != nil && row.HistoryLength != nil:
		row.StartTime = mdb.converter.ToMySQLDateTime(row.StartTime)
		closeTime := mdb.converter.ToMySQLDateTime(*row.CloseTime)
		return mdb.conn.Exec(templateCreateWorkflowExecutionClosed,
			row.DomainID,
			row.WorkflowID,
			row.RunID,
			row.StartTime,
			row.ExecutionTime,
			row.WorkflowTypeName,
			closeTime,
			*row.CloseStatus,
			*row.HistoryLength,
			row.Memo,
			row.Encoding)
	default:
		return nil, errCloseParams
	}
}

// DeleteFromVisibility deletes a row from visibility table if it exist
func (mdb *db) DeleteFromVisibility(filter *sqlplugin.VisibilityFilter) (sql.Result, error) {
	return mdb.conn.Exec(templateDeleteWorkflowExecution, filter.DomainID, filter.RunID)
}

// SelectFromVisibility reads one or more rows from visibility table
func (mdb *db) SelectFromVisibility(filter *sqlplugin.VisibilityFilter) ([]sqlplugin.VisibilityRow, error) {
	var err error
	var rows []sqlplugin.VisibilityRow
	if filter.MinStartTime != nil {
		*filter.MinStartTime = mdb.converter.ToMySQLDateTime(*filter.MinStartTime)
	}
	if filter.MaxStartTime != nil {
		*filter.MaxStartTime = mdb.converter.ToMySQLDateTime(*filter.MaxStartTime)
	}
	switch {
	case filter.MinStartTime == nil && filter.RunID != nil && filter.Closed:
		var row sqlplugin.VisibilityRow
		err = mdb.conn.Get(&row, templateGetClosedWorkflowExecution, filter.DomainID, *filter.RunID)
		if err == nil {
			rows = append(rows, row)
		}
	case filter.MinStartTime != nil && filter.WorkflowID != nil:
		qry := templateGetOpenWorkflowExecutionsByID
		if filter.Closed {
			qry = templateGetClosedWorkflowExecutionsByID
		}
		err = mdb.conn.Select(&rows,
			qry,
			*filter.WorkflowID,
			filter.DomainID,
			mdb.converter.ToMySQLDateTime(*filter.MinStartTime),
			mdb.converter.ToMySQLDateTime(*filter.MaxStartTime),
			*filter.RunID,
			*filter.MinStartTime,
			*filter.PageSize)
	case filter.MinStartTime != nil && filter.WorkflowTypeName != nil:
		qry := templateGetOpenWorkflowExecutionsByType
		if filter.Closed {
			qry = templateGetClosedWorkflowExecutionsByType
		}
		err = mdb.conn.Select(&rows,
			qry,
			*filter.WorkflowTypeName,
			filter.DomainID,
			mdb.converter.ToMySQLDateTime(*filter.MinStartTime),
			mdb.converter.ToMySQLDateTime(*filter.MaxStartTime),
			*filter.RunID,
			*filter.MaxStartTime,
			*filter.PageSize)
	case filter.MinStartTime != nil && filter.CloseStatus != nil:
		err = mdb.conn.Select(&rows,
			templateGetClosedWorkflowExecutionsByStatus,
			*filter.CloseStatus,
			filter.DomainID,
			mdb.converter.ToMySQLDateTime(*filter.MinStartTime),
			mdb.converter.ToMySQLDateTime(*filter.MaxStartTime),
			*filter.RunID,
			mdb.converter.ToMySQLDateTime(*filter.MaxStartTime),
			*filter.PageSize)
	case filter.MinStartTime != nil:
		qry := templateGetOpenWorkflowExecutions
		if filter.Closed {
			qry = templateGetClosedWorkflowExecutions
		}
		err = mdb.conn.Select(&rows,
			qry,
			filter.DomainID,
			mdb.converter.ToMySQLDateTime(*filter.MinStartTime),
			mdb.converter.ToMySQLDateTime(*filter.MaxStartTime),
			*filter.RunID,
			mdb.converter.ToMySQLDateTime(*filter.MaxStartTime),
			*filter.PageSize)
	default:
		return nil, fmt.Errorf("invalid query filter")
	}
	if err != nil {
		return nil, err
	}
	for i := range rows {
		rows[i].StartTime = mdb.converter.FromMySQLDateTime(rows[i].StartTime)
		rows[i].ExecutionTime = mdb.converter.FromMySQLDateTime(rows[i].ExecutionTime)
		if rows[i].CloseTime != nil {
			closeTime := mdb.converter.FromMySQLDateTime(*rows[i].CloseTime)
			rows[i].CloseTime = &closeTime
		}
	}
	return rows, err
}
