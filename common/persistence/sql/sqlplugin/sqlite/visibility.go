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
	"fmt"
	"strings"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

var (
	keywordListSeparator = "♡"

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

	templateDeleteWorkflowExecution = `
		DELETE FROM executions_visibility
		WHERE namespace_id = :namespace_id AND run_id = :run_id`

	templateGetWorkflowExecution = fmt.Sprintf(
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
func (mdb *db) InsertIntoVisibility(
	ctx context.Context,
	row *sqlplugin.VisibilityRow,
) (sql.Result, error) {
	finalRow := mdb.prepareRowForDB(row)
	return mdb.conn.NamedExecContext(ctx, templateInsertWorkflowExecution, finalRow)
}

// ReplaceIntoVisibility replaces an existing row if it exist or creates a new row in visibility table
func (mdb *db) ReplaceIntoVisibility(
	ctx context.Context,
	row *sqlplugin.VisibilityRow,
) (sql.Result, error) {
	finalRow := mdb.prepareRowForDB(row)
	return mdb.conn.NamedExecContext(ctx, templateUpsertWorkflowExecution, finalRow)
}

// DeleteFromVisibility deletes a row from visibility table if it exist
func (mdb *db) DeleteFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilityDeleteFilter,
) (sql.Result, error) {
	return mdb.conn.NamedExecContext(ctx, templateDeleteWorkflowExecution, filter)
}

// SelectFromVisibility reads one or more rows from visibility table
func (mdb *db) SelectFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilitySelectFilter,
) ([]sqlplugin.VisibilityRow, error) {
	if len(filter.Query) == 0 {
		// backward compatibility for existing tests
		err := sqlplugin.GenerateSelectQuery(&filter, mdb.converter.ToSQLiteDateTime)
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
func (mdb *db) GetFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilityGetFilter,
) (*sqlplugin.VisibilityRow, error) {
	var row sqlplugin.VisibilityRow
	stmt, err := mdb.conn.PrepareNamedContext(ctx, templateGetWorkflowExecution)
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

func (mdb *db) CountFromVisibility(
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

func (mdb *db) CountGroupByFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilitySelectFilter,
) ([]sqlplugin.VisibilityCountRow, error) {
	rows, err := mdb.db.QueryContext(ctx, filter.Query, filter.QueryArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return sqlplugin.ParseCountGroupByRows(rows, filter.GroupBy)
}

func (mdb *db) prepareRowForDB(row *sqlplugin.VisibilityRow) *sqlplugin.VisibilityRow {
	if row == nil {
		return nil
	}
	finalRow := *row
	finalRow.StartTime = mdb.converter.ToSQLiteDateTime(finalRow.StartTime)
	finalRow.ExecutionTime = mdb.converter.ToSQLiteDateTime(finalRow.ExecutionTime)
	if finalRow.CloseTime != nil {
		*finalRow.CloseTime = mdb.converter.ToSQLiteDateTime(*finalRow.CloseTime)
	}
	if finalRow.SearchAttributes != nil {
		finalSearchAttributes := sqlplugin.VisibilitySearchAttributes{}
		for name, value := range *finalRow.SearchAttributes {
			switch v := value.(type) {
			case []string:
				finalSearchAttributes[name] = strings.Join(v, keywordListSeparator)
			default:
				finalSearchAttributes[name] = v
			}
		}
		finalRow.SearchAttributes = &finalSearchAttributes
	}
	return &finalRow
}

func (mdb *db) processRowFromDB(row *sqlplugin.VisibilityRow) error {
	if row == nil {
		return nil
	}
	row.StartTime = mdb.converter.FromSQLiteDateTime(row.StartTime)
	row.ExecutionTime = mdb.converter.FromSQLiteDateTime(row.ExecutionTime)
	if row.CloseTime != nil {
		closeTime := mdb.converter.FromSQLiteDateTime(*row.CloseTime)
		row.CloseTime = &closeTime
	}
	if row.SearchAttributes != nil {
		for saName, saValue := range *row.SearchAttributes {
			switch typedSaValue := saValue.(type) {
			case string:
				if strings.Index(typedSaValue, keywordListSeparator) >= 0 {
					// If the string contains the keywordListSeparator, then we need to split it
					// into a list of keywords.
					(*row.SearchAttributes)[saName] = strings.Split(typedSaValue, keywordListSeparator)
				}
			default:
				// no-op
			}
		}
	}
	return nil
}
