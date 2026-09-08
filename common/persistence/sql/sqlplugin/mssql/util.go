package mssql

import (
	"context"
	"database/sql"
	"errors"
)

// batchResult aggregates rows affected across per-row statement executions.
type batchResult struct {
	rowsAffected int64
}

func (r batchResult) LastInsertId() (int64, error) {
	return 0, errors.New("LastInsertId is not supported for batched statements")
}

func (r batchResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// namedExecPerRow executes a named statement once per row and returns the
// summed rows-affected. sqlx expands `INSERT ... VALUES (:a, :b)` into
// multi-row form when given a slice, but it cannot do that for MERGE
// statements, so MERGE-based upserts over row slices go through this helper.
func namedExecPerRow[T any](
	ctx context.Context,
	mdb *db,
	query string,
	rows []T,
) (sql.Result, error) {
	var total int64
	for i := range rows {
		res, err := mdb.NamedExecContext(ctx, query, rows[i])
		if err != nil {
			return nil, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return nil, err
		}
		total += n
	}
	return batchResult{rowsAffected: total}, nil
}
