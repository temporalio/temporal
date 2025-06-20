package driver

import (
	"github.com/jmoiron/sqlx"
)

const (
	// check http://www.postgresql.org/docs/9.3/static/errcodes-appendix.html
	dupEntryCode            = "23505"
	dupDatabaseCode         = "42P04"
	readOnlyTransactionCode = "25006"
	cannotConnectNowCode    = "57P03"
	featureNotSupportedCode = "0A000"

	// Unsupported "feature" messages to look for
	cannotSetReadWriteModeDuringRecoveryMsg = "cannot set transaction read-write mode during recovery"
)

type Driver interface {
	CreateConnection(dsn string) (*sqlx.DB, error)
	IsDupEntryError(error) bool
	IsDupDatabaseError(error) bool
	IsConnNeedsRefreshError(error) bool
}

func isConnNeedsRefreshError(code, message string) bool {
	if code == readOnlyTransactionCode || code == cannotConnectNowCode {
		return true
	}
	if code == featureNotSupportedCode && message == cannotSetReadWriteModeDuringRecoveryMsg {
		return true
	}
	return false
}
