package driver

import (
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib" // register pgx driver for sqlx
	"github.com/jmoiron/sqlx"
)

type PGXDriver struct{}

func (p *PGXDriver) CreateConnection(dsn string) (*sqlx.DB, error) {
	return sqlx.Connect("pgx", dsn)
}

func (p *PGXDriver) IsDupEntryError(err error) bool {
	pgxErr, ok := err.(*pgconn.PgError)
	return ok && pgxErr.Code == dupEntryCode
}

func (p *PGXDriver) IsDupDatabaseError(err error) bool {
	pqErr, ok := err.(*pgconn.PgError)
	return ok && pqErr.Code == dupDatabaseCode
}

func (p *PGXDriver) IsConnNeedsRefreshError(err error) bool {
	pqErr, ok := err.(*pgconn.PgError)
	if !ok {
		return false
	}
	return isConnNeedsRefreshError(pqErr.Code, pqErr.Message)
}
