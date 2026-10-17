package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.uber.org/mock/gomock"
)

func TestConvertSQLError(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		err   error
		cause error
	}{
		{name: "canceled", err: fmt.Errorf("lock: %w", context.Canceled), cause: context.Canceled},
		{name: "deadline exceeded", err: fmt.Errorf("query: %w", context.DeadlineExceeded), cause: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := convertSQLError("lockTaskQueue", "", tc.err)
			require.ErrorIs(t, err, tc.cause)
			require.ErrorContains(t, err, "lockTaskQueue")
			var unavailable *serviceerror.Unavailable
			require.NotErrorAs(t, err, &unavailable)
		})
	}
}

func TestConvertSQLError_WrapsOtherErrorsAsUnavailable(t *testing.T) {
	t.Parallel()

	err := convertSQLError("lockTaskQueue", "failed to lock row", errors.New("connection reset"))
	var unavailable *serviceerror.Unavailable
	require.ErrorAs(t, err, &unavailable)
	require.ErrorContains(t, err, "lockTaskQueue")
	require.ErrorContains(t, err, "failed to lock row")
	require.ErrorContains(t, err, "connection reset")
}

func TestTxExecuteRollback(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		rollbackErr error
		wantLog     bool
	}{
		{name: "transaction already done", rollbackErr: sql.ErrTxDone},
		{name: "rollback fails", rollbackErr: errors.New("connection reset"), wantLog: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			tx := sqlplugin.NewMockTx(ctrl)
			db := sqlplugin.NewMockDB(ctrl)
			logger := log.NewMockLogger(ctrl)
			db.EXPECT().BeginTx(gomock.Any()).Return(tx, nil).Times(2)
			tx.EXPECT().Rollback().Return(tc.rollbackErr).Times(2)
			if tc.wantLog {
				logger.EXPECT().Error("transaction rollback error", tag.Error(tc.rollbackErr)).Times(2)
			}

			store := NewSQLStore(db, logger, nil)
			for _, txExecute := range []func(context.Context, string, func(sqlplugin.Tx) error) error{
				store.txExecute,
				(&userDataStore{SqlStore: store}).txExecute,
			} {
				err := txExecute(context.Background(), "lockTaskQueue", func(sqlplugin.Tx) error { return context.Canceled })
				require.ErrorIs(t, err, context.Canceled)
			}
		})
	}
}
