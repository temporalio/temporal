package mssql

import (
	"errors"

	mssql "github.com/microsoft/go-mssqldb"
)

const (
	// Violation of PRIMARY KEY / UNIQUE constraint.
	errCodeDupConstraint = 2627
	// Cannot insert duplicate key row in object with unique index.
	errCodeDupUniqueIndex = 2601
	// Database 'x' already exists.
	errCodeDupDatabase = 1801

	// Failed to update database because the database is read-only
	// (e.g. connection landed on a read-only AG replica after failover).
	errCodeReadOnlyDatabase = 3906
	// Could not run BEGIN TRANSACTION because the database is read-only.
	errCodeReadOnlyTransaction = 3908
	// Cannot open database (offline / in transition).
	errCodeCannotOpenDatabase = 4060
	// Login failed (e.g. expired token from passwordCommand; re-dialing
	// re-runs the password command and picks up a fresh credential).
	errCodeLoginFailed = 18456
	// Azure SQL: service is currently busy (error occurred while processing
	// the request; documented transient fault).
	errCodeAzureServiceBusy = 40197
	// Azure SQL: the service is currently busy, retry later.
	errCodeAzureRetryLater = 40501
	// Azure SQL: database is not currently available.
	errCodeAzureUnavailable = 40613
	// Azure SQL: documented transient faults during reconfiguration.
	errCodeAzureNotEnoughResources    = 49918
	errCodeAzureTooManyCreateRequests = 49919
	errCodeAzureServiceTooBusy        = 49920
)

func mssqlErrorNumber(err error) (int32, bool) {
	var mssqlErr mssql.Error
	if errors.As(err, &mssqlErr) {
		return mssqlErr.SQLErrorNumber(), true
	}
	return 0, false
}

func isDupEntryError(err error) bool {
	number, ok := mssqlErrorNumber(err)
	return ok && (number == errCodeDupConstraint || number == errCodeDupUniqueIndex)
}

func isDupDatabaseError(err error) bool {
	number, ok := mssqlErrorNumber(err)
	return ok && number == errCodeDupDatabase
}

// isConnNeedsRefreshError classifies errors that indicate the pooled
// connections point at a server that can no longer serve writes (failover to
// a read-only replica, database offline, expired credential) so the
// DatabaseHandle re-dials instead of retrying against a broken pool.
func isConnNeedsRefreshError(err error) bool {
	number, ok := mssqlErrorNumber(err)
	if !ok {
		return false
	}
	switch number {
	case errCodeReadOnlyDatabase,
		errCodeReadOnlyTransaction,
		errCodeCannotOpenDatabase,
		errCodeLoginFailed,
		errCodeAzureServiceBusy,
		errCodeAzureRetryLater,
		errCodeAzureUnavailable,
		errCodeAzureNotEnoughResources,
		errCodeAzureTooManyCreateRequests,
		errCodeAzureServiceTooBusy:
		return true
	default:
		return false
	}
}
