package driver

import (
	"database/sql"
	sqldriver "database/sql/driver"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

type PQDriver struct{}

const pqDriverName = "postgres"

func (p *PQDriver) CreateConnection(dsn string) (*sqlx.DB, error) {
	return sqlx.Connect(pqDriverName, dsn)
}

func (p *PQDriver) CreateRefreshableConnection(buildDSN func() (string, error)) (*sqlx.DB, error) {
	c := sqlplugin.NewRefreshingConnector(
		buildDSN,
		func(dsn string) (sqldriver.Connector, error) {
			return pq.NewConnector(dsn)
		},
		&pq.Driver{},
	)
	db := sqlx.NewDb(sql.OpenDB(c), pqDriverName)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func (p *PQDriver) IsDupEntryError(err error) bool {
	pqErr, ok := err.(*pq.Error)
	return ok && pqErr.Code == dupEntryCode
}

func (p *PQDriver) IsDupDatabaseError(err error) bool {
	pqErr, ok := err.(*pq.Error)
	return ok && pqErr.Code == dupDatabaseCode
}

func (p *PQDriver) SupportsGSSAPI() bool { return false }

func (p *PQDriver) IsConnNeedsRefreshError(err error) bool {
	pqErr, ok := err.(*pq.Error)
	if !ok {
		return false
	}
	return isConnNeedsRefreshError(string(pqErr.Code), pqErr.Message)
}
