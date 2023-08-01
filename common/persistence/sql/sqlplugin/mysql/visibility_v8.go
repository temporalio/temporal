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

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

var (
	templateInsertWorkflowExecution = fmt.Sprintf(
		`INSERT INTO executions_visibility (%s)
		VALUES (%s)
		ON DUPLICATE KEY UPDATE run_id = VALUES(run_id)`,
		strings.Join(sqlplugin.DbFields, ", "),
		sqlplugin.BuildNamedPlaceholder(sqlplugin.DbFields...),
	)

	templateInsertCustomSearchAttributes = `
		INSERT INTO custom_search_attributes (
			namespace_id, run_id, search_attributes
		) VALUES (:namespace_id, :run_id, :search_attributes)
		ON DUPLICATE KEY UPDATE run_id = VALUES(run_id)`

	templateUpsertWorkflowExecution = fmt.Sprintf(
		`INSERT INTO executions_visibility (%s)
		VALUES (%s)
		%s`,
		strings.Join(sqlplugin.DbFields, ", "),
		sqlplugin.BuildNamedPlaceholder(sqlplugin.DbFields...),
		buildOnDuplicateKeyUpdate(sqlplugin.DbFields...),
	)

	templateUpsertCustomSearchAttributes = `
		INSERT INTO custom_search_attributes (
			namespace_id, run_id, search_attributes
		) VALUES (:namespace_id, :run_id, :search_attributes)
		ON DUPLICATE KEY UPDATE search_attributes = VALUES(search_attributes)`

	templateDeleteWorkflowExecution_v8 = `
		DELETE FROM executions_visibility
		WHERE namespace_id = :namespace_id AND run_id = :run_id`

	templateDeleteCustomSearchAttributes = `
		DELETE FROM custom_search_attributes
		WHERE namespace_id = :namespace_id AND run_id = :run_id`

	templateGetWorkflowExecution_v8 = fmt.Sprintf(
		`SELECT %s FROM executions_visibility
		WHERE namespace_id = :namespace_id AND run_id = :run_id`,
		strings.Join(sqlplugin.DbFields, ", "),
	)
)

func buildOnDuplicateKeyUpdate(fields ...string) string {
	items := make([]string, len(fields))
	for i, field := range fields {
		items[i] = fmt.Sprintf("%s = VALUES(%s)", field, field)
	}
	return fmt.Sprintf("ON DUPLICATE KEY UPDATE %s", strings.Join(items, ", "))
}

// InsertIntoVisibility inserts a row into visibility table. If an row already exist,
// its left as such and no update will be made
func (mdb *dbV8) InsertIntoVisibility(
	ctx context.Context,
	row *sqlplugin.VisibilityRow,
) (result sql.Result, retError error) {
	finalRow := mdb.prepareRowForDB(row)
	tx, err := mdb.db.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := tx.Rollback()
		// If the error is sql.ErrTxDone, it means the transaction already closed, so ignore error.
		if err != nil && !errors.Is(err, sql.ErrTxDone) {
			// Transaction rollback error should never happen, unless db connection was lost.
			retError = fmt.Errorf("transaction rollback failed: %w", retError)
		}
	}()
	result, err = tx.NamedExecContext(ctx, templateInsertWorkflowExecution, finalRow)
	if err != nil {
		return nil, err
	}
	_, err = tx.NamedExecContext(ctx, templateInsertCustomSearchAttributes, finalRow)
	if err != nil {
		return nil, err
	}
	err = tx.Commit()
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ReplaceIntoVisibility replaces an existing row if it exist or creates a new row in visibility table
func (mdb *dbV8) ReplaceIntoVisibility(
	ctx context.Context,
	row *sqlplugin.VisibilityRow,
) (result sql.Result, retError error) {
	finalRow := mdb.prepareRowForDB(row)
	tx, err := mdb.db.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := tx.Rollback()
		// If the error is sql.ErrTxDone, it means the transaction already closed, so ignore error.
		if err != nil && !errors.Is(err, sql.ErrTxDone) {
			// Transaction rollback error should never happen, unless db connection was lost.
			retError = fmt.Errorf("transaction rollback failed: %w", retError)
		}
	}()
	result, err = tx.NamedExecContext(ctx, templateUpsertWorkflowExecution, finalRow)
	if err != nil {
		return nil, err
	}
	_, err = tx.NamedExecContext(ctx, templateUpsertCustomSearchAttributes, finalRow)
	if err != nil {
		return nil, err
	}
	err = tx.Commit()
	if err != nil {
		return nil, err
	}
	return result, nil
}

// DeleteFromVisibility deletes a row from visibility table if it exist
func (mdb *dbV8) DeleteFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilityDeleteFilter,
) (result sql.Result, retError error) {
	tx, err := mdb.db.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := tx.Rollback()
		// If the error is sql.ErrTxDone, it means the transaction already closed, so ignore error.
		if err != nil && !errors.Is(err, sql.ErrTxDone) {
			// Transaction rollback error should never happen, unless db connection was lost.
			retError = fmt.Errorf("transaction rollback failed: %w", retError)
		}
	}()
	_, err = mdb.conn.NamedExecContext(ctx, templateDeleteCustomSearchAttributes, filter)
	if err != nil {
		return nil, err
	}
	result, err = mdb.conn.NamedExecContext(ctx, templateDeleteWorkflowExecution_v8, filter)
	if err != nil {
		return nil, err
	}
	err = tx.Commit()
	if err != nil {
		return nil, err
	}
	return result, nil
}

// SelectFromVisibility reads one or more rows from visibility table
func (mdb *dbV8) SelectFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilitySelectFilter,
) ([]sqlplugin.VisibilityRow, error) {
	if len(filter.Query) == 0 {
		// backward compatibility for existing tests
		err := sqlplugin.GenerateSelectQuery(&filter, mdb.converter.ToMySQLDateTime)
		if err != nil {
			return nil, err
		}
	}

	var rows []sqlplugin.VisibilityRow
	err := mdb.conn.SelectContext(ctx, &rows, filter.Query, filter.QueryArgs...)
	if err != nil {
		return nil, err
	}
	for i := range rows {
		err = mdb.processRowFromDB(&rows[i])
		if err != nil {
			return nil, err
		}
	}
	return rows, nil
}

// GetFromVisibility reads one row from visibility table
func (mdb *dbV8) GetFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilityGetFilter,
) (*sqlplugin.VisibilityRow, error) {
	var row sqlplugin.VisibilityRow
	stmt, err := mdb.conn.PrepareNamedContext(ctx, templateGetWorkflowExecution_v8)
	if err != nil {
		return nil, err
	}
	err = stmt.GetContext(ctx, &row, filter)
	if err != nil {
		return nil, err
	}
	err = mdb.processRowFromDB(&row)
	if err != nil {
		return nil, err
	}
	return &row, nil
}

func (mdb *dbV8) CountFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilitySelectFilter,
) (int64, error) {
	var count int64
	err := mdb.conn.GetContext(ctx, &count, filter.Query, filter.QueryArgs...)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (mdb *dbV8) CountGroupByFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilitySelectFilter,
) ([]sqlplugin.VisibilityCountRow, error) {
	rows, err := mdb.db.db.QueryContext(ctx, filter.Query, filter.QueryArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return sqlplugin.ParseCountGroupByRows(rows, filter.GroupBy)
}

func (mdb *dbV8) prepareRowForDB(row *sqlplugin.VisibilityRow) *sqlplugin.VisibilityRow {
	if row == nil {
		return nil
	}
	finalRow := *row
	finalRow.StartTime = mdb.converter.ToMySQLDateTime(finalRow.StartTime)
	finalRow.ExecutionTime = mdb.converter.ToMySQLDateTime(finalRow.ExecutionTime)
	if finalRow.CloseTime != nil {
		*finalRow.CloseTime = mdb.converter.ToMySQLDateTime(*finalRow.CloseTime)
	}
	return &finalRow
}

func (mdb *dbV8) processRowFromDB(row *sqlplugin.VisibilityRow) error {
	if row == nil {
		return nil
	}
	row.StartTime = mdb.converter.FromMySQLDateTime(row.StartTime)
	row.ExecutionTime = mdb.converter.FromMySQLDateTime(row.ExecutionTime)
	if row.CloseTime != nil {
		closeTime := mdb.converter.FromMySQLDateTime(*row.CloseTime)
		row.CloseTime = &closeTime
	}
	if row.SearchAttributes != nil {
		for saName, saValue := range *row.SearchAttributes {
			switch typedSaValue := saValue.(type) {
			case []interface{}:
				// the only valid type is slice of strings
				strSlice := make([]string, len(typedSaValue))
				for i, item := range typedSaValue {
					switch v := item.(type) {
					case string:
						strSlice[i] = v
					default:
						return fmt.Errorf("Unexpected data type in keyword list: %T (expected string)", v)
					}
				}
				(*row.SearchAttributes)[saName] = strSlice
			default:
				// no-op
			}
		}
	}
	return nil
}
