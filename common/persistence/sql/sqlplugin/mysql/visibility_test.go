package mysql

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCombineRollbackError(t *testing.T) {
	operationErr := errors.New("operation failed")
	rollbackErr := errors.New("rollback failed")

	err := combineRollbackError(operationErr, rollbackErr)

	require.ErrorIs(t, err, operationErr)
	require.ErrorIs(t, err, rollbackErr)
	require.ErrorContains(t, err, "transaction rollback failed")
}

func TestCombineRollbackError_IgnoresClosedTransaction(t *testing.T) {
	operationErr := errors.New("operation failed")

	err := combineRollbackError(operationErr, sql.ErrTxDone)

	require.ErrorIs(t, err, operationErr)
	require.NotErrorIs(t, err, sql.ErrTxDone)
}
