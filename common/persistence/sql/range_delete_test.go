package sql

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

type dummyResult struct {
	rowsAffected int64
	err          error
}

func (r dummyResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r dummyResult) RowsAffected() (int64, error) {
	return r.rowsAffected, r.err
}

type stubDB struct {
	sqlplugin.DB
	timerDeleteFunc     func(ctx context.Context, filter sqlplugin.TimerTasksRangeFilter) (sql.Result, error)
	scheduledDeleteFunc func(ctx context.Context, filter sqlplugin.HistoryScheduledTasksRangeFilter) (sql.Result, error)
}

func (db *stubDB) RangeDeleteFromTimerTasks(ctx context.Context, filter sqlplugin.TimerTasksRangeFilter) (sql.Result, error) {
	if db.timerDeleteFunc != nil {
		return db.timerDeleteFunc(ctx, filter)
	}
	return dummyResult{rowsAffected: 0}, nil
}

func (db *stubDB) RangeDeleteFromHistoryScheduledTasks(ctx context.Context, filter sqlplugin.HistoryScheduledTasksRangeFilter) (sql.Result, error) {
	if db.scheduledDeleteFunc != nil {
		return db.scheduledDeleteFunc(ctx, filter)
	}
	return dummyResult{rowsAffected: 0}, nil
}

func TestRangeDeleteInBatches_Stub(t *testing.T) {
	ctx := context.Background()

	t.Run("BatchSize 0 executes once with PageSize 0", func(t *testing.T) {
		calls := 0
		sDB := &stubDB{
			timerDeleteFunc: func(ctx context.Context, filter sqlplugin.TimerTasksRangeFilter) (sql.Result, error) {
				calls++
				require.Equal(t, 0, filter.PageSize)
				return dummyResult{rowsAffected: 100}, nil
			},
		}
		store := &sqlExecutionStore{sqlStore: sqlStore{DB: sDB}}
		err := store.rangeCompleteTimerTasks(ctx, &persistence.RangeCompleteHistoryTasksRequest{
			BatchSize: 0,
		})
		require.NoError(t, err)
		require.Equal(t, 1, calls)
	})

	t.Run("Loop stops when RowsAffected < BatchSize", func(t *testing.T) {
		calls := 0
		pageSizes := []int{}
		sDB := &stubDB{
			timerDeleteFunc: func(ctx context.Context, filter sqlplugin.TimerTasksRangeFilter) (sql.Result, error) {
				calls++
				pageSizes = append(pageSizes, filter.PageSize)
				if calls == 1 {
					return dummyResult{rowsAffected: 5}, nil
				}
				return dummyResult{rowsAffected: 2}, nil // < BatchSize (5)
			},
		}
		store := &sqlExecutionStore{sqlStore: sqlStore{DB: sDB}}
		err := store.rangeCompleteTimerTasks(ctx, &persistence.RangeCompleteHistoryTasksRequest{
			BatchSize: 5,
		})
		require.NoError(t, err)
		require.Equal(t, 2, calls)
		require.Equal(t, []int{5, 5}, pageSizes)
	})

	t.Run("Continues while RowsAffected == BatchSize", func(t *testing.T) {
		calls := 0
		sDB := &stubDB{
			timerDeleteFunc: func(ctx context.Context, filter sqlplugin.TimerTasksRangeFilter) (sql.Result, error) {
				calls++
				if calls < 3 {
					return dummyResult{rowsAffected: 10}, nil
				}
				return dummyResult{rowsAffected: 4}, nil // 4 < 10 stops
			},
		}
		store := &sqlExecutionStore{sqlStore: sqlStore{DB: sDB}}
		err := store.rangeCompleteTimerTasks(ctx, &persistence.RangeCompleteHistoryTasksRequest{
			BatchSize: 10,
		})
		require.NoError(t, err)
		require.Equal(t, 3, calls)
	})

	t.Run("Propagates mid-loop error wrapped in serviceerror.Unavailable", func(t *testing.T) {
		calls := 0
		sDB := &stubDB{
			timerDeleteFunc: func(ctx context.Context, filter sqlplugin.TimerTasksRangeFilter) (sql.Result, error) {
				calls++
				if calls == 1 {
					return dummyResult{rowsAffected: 5}, nil
				}
				return nil, errors.New("db connection closed")
			},
		}
		store := &sqlExecutionStore{sqlStore: sqlStore{DB: sDB}}
		err := store.rangeCompleteTimerTasks(ctx, &persistence.RangeCompleteHistoryTasksRequest{
			BatchSize: 5,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "RangeCompleteTimerTask operation failed. Error: db connection closed")
		require.Equal(t, 2, calls)
	})

	t.Run("Stops on cancelled context", func(t *testing.T) {
		cancelCtx, cancel := context.WithCancel(context.Background())
		calls := 0
		sDB := &stubDB{
			timerDeleteFunc: func(ctx context.Context, filter sqlplugin.TimerTasksRangeFilter) (sql.Result, error) {
				calls++
				cancel()
				return dummyResult{rowsAffected: 5}, nil
			},
		}
		store := &sqlExecutionStore{sqlStore: sqlStore{DB: sDB}}
		err := store.rangeCompleteTimerTasks(cancelCtx, &persistence.RangeCompleteHistoryTasksRequest{
			BatchSize: 5,
		})
		require.Error(t, err)
		require.True(t, errors.Is(err, context.Canceled) || errors.Is(errors.Unwrap(err), context.Canceled))
		require.Equal(t, 1, calls)
	})
}
