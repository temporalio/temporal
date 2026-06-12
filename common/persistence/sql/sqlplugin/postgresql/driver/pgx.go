package driver

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

type PGXDriver struct{}

const pgxDriverName = "pgx"

func (p *PGXDriver) CreateConnection(dsn string) (*sqlx.DB, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	connector := stdlib.GetConnector(*cfg, stdlib.OptionAfterConnect(registerCustomTypes))
	db := sqlx.NewDb(sql.OpenDB(connector), pgxDriverName)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func (p *PGXDriver) CreateRefreshableConnection(buildDSN func() (string, error)) (*sqlx.DB, error) {
	c := sqlplugin.NewRefreshingConnector(
		buildDSN,
		func(dsn string) (sqldriver.Connector, error) {
			cfg, err := pgx.ParseConfig(dsn)
			if err != nil {
				return nil, err
			}
			return stdlib.GetConnector(*cfg, stdlib.OptionAfterConnect(registerCustomTypes)), nil
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

// registerCustomTypes overrides pgx's default type mapping.
//
// By default pgx maps time.Duration to the interval type. Under the simple
// query protocol (the path users land on behind PgBouncer in transaction
// pooling) a time.Duration is encoded as an interval literal like "00:00:05".
// Temporal stores all durations as bigint nanoseconds, so that literal is
// rejected. Encoding time.Duration as int8 (bigint in PostgreSQL) instead
// results in the integer nanosecond count
func registerCustomTypes(_ context.Context, conn *pgx.Conn) error {
	// pgx tracks the value type and the pointer type separately, so
	// both must be remapped.
	conn.TypeMap().RegisterDefaultPgType(time.Duration(0), "int8")
	conn.TypeMap().RegisterDefaultPgType((*time.Duration)(nil), "int8")
	return nil
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
