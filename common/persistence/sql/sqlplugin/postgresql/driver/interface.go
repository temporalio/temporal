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
	// CreateRefreshableConnection creates a connection pool that calls buildDSN
	// before opening each new physical connection, enabling per-connection
	// credential refresh for short-lived tokens (e.g. from passwordCommand).
	CreateRefreshableConnection(buildDSN func() (string, error)) (*sqlx.DB, error)
	IsDupEntryError(error) bool
	IsDupDatabaseError(error) bool
	IsConnNeedsRefreshError(error) bool
	// SupportsGSSAPI reports whether the underlying driver can perform
	// GSSAPI/Kerberos authentication. Only the pgx driver does today;
	// lib/pq does not implement the protocol exchange.
	SupportsGSSAPI() bool
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
