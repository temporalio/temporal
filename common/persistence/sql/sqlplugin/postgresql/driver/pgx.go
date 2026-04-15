package driver

import (
	"database/sql"
	sqldriver "database/sql/driver"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

type PGXDriver struct{}

const pgxDriverName = "pgx"

func (p *PGXDriver) CreateConnection(dsn string) (*sqlx.DB, error) {
	return sqlx.Connect(pgxDriverName, dsn)
}

func (p *PGXDriver) CreateRefreshableConnection(buildDSN func() (string, error)) (*sqlx.DB, error) {
	c := sqlplugin.NewRefreshingConnector(
		buildDSN,
		func(dsn string) (sqldriver.Connector, error) {
			cfg, err := pgx.ParseConfig(dsn)
			if err != nil {
				return nil, err
			}
			return stdlib.GetConnector(*cfg), nil
		},
		stdlib.GetDefaultDriver(),
	)
	db := sqlx.NewDb(sql.OpenDB(c), pgxDriverName)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
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
