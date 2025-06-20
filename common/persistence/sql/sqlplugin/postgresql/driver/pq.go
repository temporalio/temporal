package driver

import (
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

type PQDriver struct{}

func (p *PQDriver) CreateConnection(dsn string) (*sqlx.DB, error) {
	return sqlx.Connect("postgres", dsn)
}

func (p *PQDriver) IsDupEntryError(err error) bool {
	pqErr, ok := err.(*pq.Error)
	return ok && pqErr.Code == dupEntryCode
}

func (p *PQDriver) IsDupDatabaseError(err error) bool {
	pqErr, ok := err.(*pq.Error)
	return ok && pqErr.Code == dupDatabaseCode
}

func (p *PQDriver) IsConnNeedsRefreshError(err error) bool {
	pqErr, ok := err.(*pq.Error)
	if !ok {
		return false
	}
	return isConnNeedsRefreshError(string(pqErr.Code), pqErr.Message)
}
