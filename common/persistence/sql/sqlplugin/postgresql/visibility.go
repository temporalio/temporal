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
		`NamespaceId, WorkflowId, RunId, StartTime, ExecutionTime, WorkflowType, ExecutionStatus, Memo, MemoEncoding, TaskQueue) ` +
		`VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
         ON CONFLICT (NamespaceId, RunId) DO NOTHING`

	templateCreateWorkflowExecutionClosed = `INSERT INTO executions_visibility (` +
		`NamespaceId, WorkflowId, RunId, StartTime, ExecutionTime, WorkflowType, CloseTime, ExecutionStatus, HistoryLength, Memo, MemoEncoding, TaskQueue) ` +
		`VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (NamespaceId, RunId) DO UPDATE 
		  SET WorkflowId = excluded.WorkflowId,
		      StartTime = excluded.StartTime,
		      ExecutionTime = excluded.ExecutionTime,
              WorkflowType = excluded.WorkflowType,
			  CloseTime = excluded.CloseTime,
			  ExecutionStatus = excluded.ExecutionStatus,
			  HistoryLength = excluded.HistoryLength,
			  Memo = excluded.Memo,
			  MemoEncoding = excluded.MemoEncoding,
			  TaskQueue = excluded.TaskQueue`

	// RunID condition is needed for correct pagination
	templateConditions1 = ` AND NamespaceId = $1
		 AND StartTime >= $2
		 AND StartTime <= $3
		 AND ((RunId > $4 and StartTime = $5) OR (StartTime < $6))
         ORDER BY StartTime DESC, RunId
         LIMIT $7`

	templateConditions2 = ` AND NamespaceId = $2
		 AND StartTime >= $3
		 AND StartTime <= $4
		 AND ((RunId > $5 and StartTime = $6) OR (StartTime < $7))
         ORDER BY StartTime DESC, RunId
		 LIMIT $8`

	templateConditionsClosedWorkflow1 = ` AND NamespaceId = $1
		 AND CloseTime >= $2
		 AND CloseTime <= $3
		 AND ((RunId > $4 and CloseTime = $5) OR (CloseTime < $6))
         ORDER BY CloseTime DESC, RunId
         LIMIT $7`

	templateConditionsClosedWorkflow2 = ` AND NamespaceId = $2
		 AND CloseTime >= $3
		 AND CloseTime <= $4
 		 AND ((RunId > $5 and CloseTime = $6) OR (CloseTime < $7))
         ORDER BY CloseTime DESC, RunId
         LIMIT $8`

	templateOpenFieldNames = `WorkflowId, RunId, StartTime, ExecutionTime, WorkflowType, ExecutionStatus, Memo, MemoEncoding, TaskQueue`
	templateOpenSelect     = `SELECT ` + templateOpenFieldNames + ` FROM executions_visibility WHERE ExecutionStatus = 1 `

	templateClosedSelect = `SELECT ` + templateOpenFieldNames + `, CloseTime, HistoryLength
		 FROM executions_visibility WHERE ExecutionStatus != 1 `

	templateGetOpenWorkflowExecutions = templateOpenSelect + templateConditions1

	templateGetClosedWorkflowExecutions = templateClosedSelect + templateConditionsClosedWorkflow1

	templateGetOpenWorkflowExecutionsByType = templateOpenSelect + `AND WorkflowType = $1` + templateConditions2

	templateGetClosedWorkflowExecutionsByType = templateClosedSelect + `AND WorkflowType = $1` + templateConditionsClosedWorkflow2

	templateGetOpenWorkflowExecutionsByID = templateOpenSelect + `AND WorkflowId = $1` + templateConditions2

	templateGetClosedWorkflowExecutionsByID = templateClosedSelect + `AND WorkflowId = $1` + templateConditionsClosedWorkflow2

	templateGetClosedWorkflowExecutionsByStatus = templateClosedSelect + `AND ExecutionStatus = $1` + templateConditionsClosedWorkflow2

	templateGetClosedWorkflowExecution = `SELECT WorkflowId, RunId, StartTime, ExecutionTime, Memo, MemoEncoding, CloseTime, WorkflowType, ExecutionStatus, HistoryLength, TaskQueue
		 FROM executions_visibility
		 WHERE NamespaceId = $1 AND ExecutionStatus != 1
		 AND RunId = $2`

	templateGetWorkflowExecution = `
		SELECT
			WorkflowId,
			RunId,
			StartTime,
			ExecutionTime,
			Memo,
			MemoEncoding,
			CloseTime,
			WorkflowType,
			ExecutionStatus,
			HistoryLength,
			TaskQueue
		FROM executions_visibility
		WHERE NamespaceId = $1 AND RunId = $2`

	templateDeleteWorkflowExecution = "DELETE FROM executions_visibility WHERE NamespaceId = $1 AND RunId = $2"
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

// GetFromVisibility reads one row from visibility table
func (mdb *db) GetFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilityGetFilter,
) (*sqlplugin.VisibilityRow, error) {
	var row sqlplugin.VisibilityRow
	err := mdb.conn.GetContext(ctx,
		&row,
		templateGetWorkflowExecution,
		filter.NamespaceID,
		filter.RunID,
	)
	if err != nil {
		return nil, err
	}
	return &row, nil
}
