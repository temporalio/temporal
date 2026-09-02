package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

var (
	// Insert-if-absent: MERGE with only a WHEN NOT MATCHED branch gives the
	// same semantics as postgresql's ON CONFLICT ... DO NOTHING.
	templateInsertWorkflowExecution = fmt.Sprintf(
		`MERGE executions_visibility WITH (HOLDLOCK) AS target
		USING (SELECT %s) AS source
		ON target.namespace_id = source.namespace_id AND target.run_id = source.run_id
		WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s);`,
		buildMergeSourceSelect(sqlplugin.DbFields...),
		strings.Join(sqlplugin.DbFields, ", "),
		buildMergeInsertValues(sqlplugin.DbFields...),
	)

	templateUpsertWorkflowExecution = fmt.Sprintf(
		`MERGE executions_visibility WITH (HOLDLOCK) AS target
		USING (SELECT %s) AS source
		ON target.namespace_id = source.namespace_id AND target.run_id = source.run_id
		%s
		WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s);`,
		buildMergeSourceSelect(sqlplugin.DbFields...),
		buildOnDuplicateKeyUpdate(sqlplugin.DbFields...),
		strings.Join(sqlplugin.DbFields, ", "),
		buildMergeInsertValues(sqlplugin.DbFields...),
	)

	templateDeleteWorkflowExecution = `
		DELETE FROM executions_visibility
		WHERE namespace_id = :namespace_id AND run_id = :run_id`

	templateGetWorkflowExecution = fmt.Sprintf(
		`SELECT %s FROM executions_visibility
		WHERE namespace_id = ? AND run_id = ?`,
		strings.Join(sqlplugin.DbFields, ", "),
	)
)

// buildMergeSourceSelect returns the projection list of the MERGE source
// row, mapping each named parameter to its column name, e.g.
// ":namespace_id AS namespace_id, :run_id AS run_id, ...".
func buildMergeSourceSelect(fields ...string) string {
	items := make([]string, len(fields))
	for i, field := range fields {
		items[i] = fmt.Sprintf(":%s AS %s", field, field)
	}
	return strings.Join(items, ", ")
}

// buildMergeInsertValues returns the VALUES list of the MERGE insert branch,
// e.g. "source.namespace_id, source.run_id, ...".
func buildMergeInsertValues(fields ...string) string {
	items := make([]string, len(fields))
	for i, field := range fields {
		items[i] = "source." + field
	}
	return strings.Join(items, ", ")
}

func buildOnDuplicateKeyUpdate(fields ...string) string {
	items := make([]string, len(fields))
	for i, field := range fields {
		items[i] = fmt.Sprintf("%s = source.%s", field, field)
	}
	return fmt.Sprintf(
		// The AND condition ensures that no update occurs if the version is behind the saved version.
		"WHEN MATCHED AND target.%s < source.%s THEN UPDATE SET %s",
		sqlplugin.VersionColumnName, sqlplugin.VersionColumnName,
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
	return mdb.NamedExecContext(ctx, templateInsertWorkflowExecution, finalRow)
}

// ReplaceIntoVisibility replaces an existing row if it exist or creates a new row in visibility table
func (mdb *db) ReplaceIntoVisibility(
	ctx context.Context,
	row *sqlplugin.VisibilityRow,
) (sql.Result, error) {
	finalRow := mdb.prepareRowForDB(row)
	return mdb.NamedExecContext(ctx, templateUpsertWorkflowExecution, finalRow)
}

// DeleteFromVisibility deletes a row from visibility table if it exist
func (mdb *db) DeleteFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilityDeleteFilter,
) (sql.Result, error) {
	return mdb.NamedExecContext(ctx, templateDeleteWorkflowExecution, filter)
}

// SelectFromVisibility reads one or more rows from visibility table
func (mdb *db) SelectFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilitySelectFilter,
) ([]sqlplugin.VisibilityRow, error) {
	if len(filter.Query) == 0 {
		// backward compatibility for existing tests
		err := sqlplugin.GenerateSelectQuery(&filter, mdb.converter.ToMSSQLDateTime)
		if err != nil {
			return nil, err
		}
		// GenerateSelectQuery emits a MySQL/PostgreSQL style `LIMIT ?` suffix,
		// which is not valid T-SQL. Rewrite it to the equivalent OFFSET/FETCH
		// clause; the generated query always has an ORDER BY, and the
		// page-size argument stays last so filter.QueryArgs are unchanged.
		filter.Query = strings.Replace(
			filter.Query,
			"LIMIT ?",
			"OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY",
			1,
		)
	}

	// Rebind will replace default placeholder `?` with the right placeholder for SQL Server.
	db, err := mdb.handle.DB()
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
	if err := mdb.GetContext(
		ctx,
		&row,
		templateGetWorkflowExecution,
		filter.NamespaceID,
		filter.RunID,
	); err != nil {
		return nil, err
	}
	if err := mdb.processRowFromDB(&row); err != nil {
		return nil, err
	}
	return &row, nil
}

func (mdb *db) CountFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilitySelectFilter,
) (int64, error) {
	var count int64
	filter.Query = mdb.Rebind(filter.Query)
	err := mdb.GetContext(ctx, &count, filter.Query, filter.QueryArgs...)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (mdb *db) CountGroupByFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilitySelectFilter,
) ([]sqlplugin.VisibilityCountRow, error) {
	filter.Query = mdb.Rebind(filter.Query)
	rows, err := mdb.QueryContext(ctx, filter.Query, filter.QueryArgs...)
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
	finalRow.StartTime = mdb.converter.ToMSSQLDateTime(finalRow.StartTime)
	finalRow.ExecutionTime = mdb.converter.ToMSSQLDateTime(finalRow.ExecutionTime)
	if finalRow.CloseTime != nil {
		*finalRow.CloseTime = mdb.converter.ToMSSQLDateTime(*finalRow.CloseTime)
	}
	if finalRow.SearchAttributes != nil {
		saMap := *finalRow.SearchAttributes
		for name, value := range saMap {
			if dt, ok := value.(time.Time); ok {
				// Truncate to microseconds: the schema's datetime computed
				// columns parse these strings with
				// TRY_CONVERT(datetimeoffset(6), ..., 127), which yields NULL
				// for values with more than 7 fractional digits.
				saMap[name] = dt.UTC().Truncate(time.Microsecond).Format(time.RFC3339Nano)
			}
		}
	}
	return &finalRow
}

func (mdb *db) processRowFromDB(row *sqlplugin.VisibilityRow) error {
	if row == nil {
		return nil
	}
	row.StartTime = mdb.converter.FromMSSQLDateTime(row.StartTime)
	row.ExecutionTime = mdb.converter.FromMSSQLDateTime(row.ExecutionTime)
	if row.CloseTime != nil {
		closeTime := mdb.converter.FromMSSQLDateTime(*row.CloseTime)
		row.CloseTime = &closeTime
	}
	if row.SearchAttributes != nil {
		for saName, saValue := range *row.SearchAttributes {
			switch typedSaValue := saValue.(type) {
			case []any:
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
