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
	"fmt"
	"strings"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

var (
	templateInsertWorkflowExecution = fmt.Sprintf(
		`INSERT INTO executions_visibility (%s)
		VALUES (%s)
		ON CONFLICT (namespace_id, run_id) DO NOTHING`,
		strings.Join(sqlplugin.DbFields, ", "),
		sqlplugin.BuildNamedPlaceholder(sqlplugin.DbFields...),
	)

	templateUpsertWorkflowExecution = fmt.Sprintf(
		`INSERT INTO executions_visibility (%s)
		VALUES (%s)
		%s`,
		strings.Join(sqlplugin.DbFields, ", "),
		sqlplugin.BuildNamedPlaceholder(sqlplugin.DbFields...),
		buildOnDuplicateKeyUpdate(sqlplugin.DbFields...),
	)

	templateDeleteWorkflowExecution_v12 = `
		DELETE FROM executions_visibility
		WHERE namespace_id = :namespace_id AND run_id = :run_id`

	templateGetWorkflowExecution_v12 = fmt.Sprintf(
		`SELECT %s FROM executions_visibility
		WHERE namespace_id = :namespace_id AND run_id = :run_id`,
		strings.Join(sqlplugin.DbFields, ", "),
	)
)

func buildOnDuplicateKeyUpdate(fields ...string) string {
	items := make([]string, len(fields))
	for i, field := range fields {
		items[i] = fmt.Sprintf("%s = excluded.%s", field, field)
	}
	return fmt.Sprintf(
		"ON CONFLICT (namespace_id, run_id) DO UPDATE SET %s",
		strings.Join(items, ", "),
	)
}

// InsertIntoVisibility inserts a row into visibility table. If an row already exist,
// its left as such and no update will be made
func (pdb *db) InsertIntoVisibility(
	ctx context.Context,
	row *sqlplugin.VisibilityRow,
) (sql.Result, error) {
	finalRow := pdb.prepareRowForDB(row)
	return pdb.NamedExecContext(ctx, templateInsertWorkflowExecution, finalRow)
}

// ReplaceIntoVisibility replaces an existing row if it exist or creates a new row in visibility table
func (pdb *db) ReplaceIntoVisibility(
	ctx context.Context,
	row *sqlplugin.VisibilityRow,
) (sql.Result, error) {
	finalRow := pdb.prepareRowForDB(row)
	return pdb.NamedExecContext(ctx, templateUpsertWorkflowExecution, finalRow)
}

// DeleteFromVisibility deletes a row from visibility table if it exist
func (pdb *db) DeleteFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilityDeleteFilter,
) (sql.Result, error) {
	return pdb.NamedExecContext(ctx, templateDeleteWorkflowExecution_v12, filter)
}

// SelectFromVisibility reads one or more rows from visibility table
func (pdb *db) SelectFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilitySelectFilter,
) ([]sqlplugin.VisibilityRow, error) {
	if len(filter.Query) == 0 {
		// backward compatibility for existing tests
		err := sqlplugin.GenerateSelectQuery(&filter, pdb.converter.ToPostgreSQLDateTime)
		if err != nil {
			return nil, err
		}
	}

	// Rebind will replace default placeholder `?` with the right placeholder for PostgreSQL.
	db, err := pdb.handle.DB()
	if err != nil {
		return nil, err
	}
	filter.Query = db.Rebind(filter.Query)
	var rows []sqlplugin.VisibilityRow
	err = db.SelectContext(ctx, &rows, filter.Query, filter.QueryArgs...)
	if err != nil {
		return nil, err
	}
	for i := range rows {
		err = pdb.processRowFromDB(&rows[i])
		if err != nil {
			return nil, err
		}
	}
	return rows, nil
}

// GetFromVisibility reads one row from visibility table
func (pdb *db) GetFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilityGetFilter,
) (*sqlplugin.VisibilityRow, error) {
	var row sqlplugin.VisibilityRow
	stmt, err := pdb.PrepareNamedContext(ctx, templateGetWorkflowExecution_v12)
	if err != nil {
		return nil, err
	}
	err = stmt.GetContext(ctx, &row, filter)
	if err != nil {
		return nil, err
	}
	err = pdb.processRowFromDB(&row)
	if err != nil {
		return nil, err
	}
	return &row, nil
}

func (pdb *db) CountFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilitySelectFilter,
) (int64, error) {
	var count int64
	filter.Query = pdb.Rebind(filter.Query)
	err := pdb.GetContext(ctx, &count, filter.Query, filter.QueryArgs...)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (pdb *db) CountGroupByFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilitySelectFilter,
) ([]sqlplugin.VisibilityCountRow, error) {
	filter.Query = pdb.Rebind(filter.Query)
	rows, err := pdb.QueryContext(ctx, filter.Query, filter.QueryArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return sqlplugin.ParseCountGroupByRows(rows, filter.GroupBy)
}

func (pdb *db) prepareRowForDB(row *sqlplugin.VisibilityRow) *sqlplugin.VisibilityRow {
	if row == nil {
		return nil
	}
	finalRow := *row
	finalRow.StartTime = pdb.converter.ToPostgreSQLDateTime(finalRow.StartTime)
	finalRow.ExecutionTime = pdb.converter.ToPostgreSQLDateTime(finalRow.ExecutionTime)
	if finalRow.CloseTime != nil {
		*finalRow.CloseTime = pdb.converter.ToPostgreSQLDateTime(*finalRow.CloseTime)
	}
	return &finalRow
}

func (pdb *db) processRowFromDB(row *sqlplugin.VisibilityRow) error {
	if row == nil {
		return nil
	}
	row.StartTime = pdb.converter.FromPostgreSQLDateTime(row.StartTime)
	row.ExecutionTime = pdb.converter.FromPostgreSQLDateTime(row.ExecutionTime)
	if row.CloseTime != nil {
		closeTime := pdb.converter.FromPostgreSQLDateTime(*row.CloseTime)
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
						return fmt.Errorf("%w: %T (expected string)", sqlplugin.ErrInvalidKeywordListDataType, v)
					}
				}
				(*row.SearchAttributes)[saName] = strSlice
			default:
				// no-op
			}
		}
	}
	// need to trim the run ID, or otherwise the returned value will
	// come with lots of trailing spaces, probably due to the CHAR(64) type
	row.RunID = strings.TrimSpace(row.RunID)
	return nil
}
