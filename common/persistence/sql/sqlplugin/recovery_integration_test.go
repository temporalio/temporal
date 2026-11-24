//go:build integration

package sqlplugin_test

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	mysqlsession "go.temporal.io/server/common/persistence/sql/sqlplugin/mysql/session"
	pgdriver "go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql/driver"
	pgsession "go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql/session"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/testing/await"
)

func TestSQLPoolRecoveryNativeDrivers(t *testing.T) {
	t.Parallel()
	for _, backend := range []struct {
		name   string
		env    string
		driver pgdriver.Driver
		simple bool
	}{
		{name: "mysql", env: "MYSQL"},
		{name: "postgres_pq", env: "POSTGRES", driver: &pgdriver.PQDriver{}},
		{name: "postgres_pgx", env: "POSTGRES", driver: &pgdriver.PGXDriver{}},
		{name: "postgres_pgx_simple", env: "POSTGRES", driver: &pgdriver.PGXDriver{}, simple: true},
	} {
		t.Run(backend.name, func(t *testing.T) {
			t.Parallel()
			prefix := "TEMPORAL_SQL_RECOVERY_" + backend.env + "_"
			address := os.Getenv(prefix + "ADDR")
			if address == "" {
				t.Skip(prefix + "ADDR is not set")
			}
			configured := func(key string) string {
				if value := os.Getenv(prefix + key); value != "" {
					return value
				}
				return "temporal"
			}
			cfg := &config.SQL{
				User: configured("USER"), Password: configured("PASSWORD"),
				ConnectAddr: address, ConnectProtocol: "tcp", DatabaseName: configured("DATABASE"),
				MaxConns: 1, MaxIdleConns: 1,
			}
			var db *sqlx.DB
			var needsRefresh func(error) bool
			if backend.driver == nil {
				session, err := mysqlsession.NewSession(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
				require.NoError(t, err)
				db = session.DB
				needsRefresh = func(err error) bool {
					mysqlErr, ok := errors.AsType[*mysql.MySQLError](err)
					return ok && (mysqlErr.Number == 1792 || mysqlErr.Number == 1836)
				}
			} else {
				cfg.ConnectAttributes = map[string]string{"sslmode": "disable"}
				if backend.simple {
					cfg.ConnectAttributes["default_query_exec_mode"] = "simple_protocol"
				}
				session, err := pgsession.NewSession(cfg, backend.driver, resolver.NewNoopResolver())
				require.NoError(t, err)
				db = session.DB
				needsRefresh = backend.driver.IsConnNeedsRefreshError
			}
			ts := clock.NewEventTimeSource().Update(time.Now())
			handle := sqlplugin.NewDatabaseHandle(sqlplugin.DbKindMain,
				func() (*sqlx.DB, error) { return db, nil }, needsRefresh,
				log.NewNoopLogger(), metrics.NoopMetricsHandler, ts)
			t.Cleanup(handle.Close)
			table := "pool_recovery_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			_, err := db.ExecContext(t.Context(), "CREATE TABLE "+table+" (id BIGINT PRIMARY KEY, duration_ns BIGINT NOT NULL)")
			require.NoError(t, err)
			t.Cleanup(func() {
				_, err := db.Exec("DROP TABLE IF EXISTS " + table)
				require.NoError(t, err)
			})
			readOnly := "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY"
			if backend.driver == nil {
				readOnly = "SET SESSION TRANSACTION READ ONLY"
			}
			_, err = db.ExecContext(t.Context(), readOnly)
			require.NoError(t, err)
			ts.Advance(2 * time.Second)
			duration := 7*time.Second + 123*time.Nanosecond
			insert := "INSERT INTO " + table + " (id, duration_ns) VALUES (:id, :duration)"
			_, err = db.NamedExecContext(t.Context(), insert, map[string]any{"id": 1, "duration": duration})
			if backend.driver != nil {
				require.Error(t, err)
				state, ok := errors.AsType[interface {
					error
					SQLState() string
				}](err)
				require.True(t, ok)
				require.Equal(t, "25006", state.SQLState())
			}
			if err != nil {
				require.Error(t, handle.ConvertError(err))
				_, err = db.NamedExecContext(t.Context(), insert, map[string]any{"id": 1, "duration": duration})
				require.NoError(t, err)
			}
			if backend.driver == nil {
				require.Error(t, handle.ConvertError(io.EOF))
			}
			current, err := handle.DB()
			require.NoError(t, err)
			require.Same(t, db, current)

			stmt, err := db.PrepareNamedContext(t.Context(), insert)
			require.NoError(t, err)
			_, err = stmt.ExecContext(t.Context(), map[string]any{"id": 2, "duration": duration + time.Nanosecond})
			require.NoError(t, err)
			require.NoError(t, stmt.Close())
			tx, err := db.BeginTxx(t.Context(), nil)
			require.NoError(t, err)
			t.Cleanup(func() {
				if err := tx.Rollback(); !errors.Is(err, sql.ErrTxDone) {
					require.NoError(t, err)
				}
			})
			_, err = tx.NamedExecContext(t.Context(), insert, map[string]any{"id": 3, "duration": duration + 2*time.Nanosecond})
			require.NoError(t, err)
			require.NoError(t, tx.Commit())
			var count, total int64
			require.NoError(t, db.GetContext(t.Context(), &count, "SELECT COUNT(*) FROM "+table))
			require.NoError(t, db.GetContext(t.Context(), &total, "SELECT SUM(duration_ns) FROM "+table))
			require.Equal(t, int64(3), count)
			require.Equal(t, 3*int64(duration)+3, total)
			require.LessOrEqual(t, db.Stats().OpenConnections, 1)

			connectionID := "SELECT pg_backend_pid()"
			if backend.driver == nil {
				connectionID = "SELECT CONNECTION_ID()"
			}
			var before, after int64
			require.NoError(t, db.GetContext(t.Context(), &before, connectionID))
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			_, err = db.BeginTxx(ctx, nil)
			require.NoError(t, err)
			cancel()
			await.RequireTrue(t, func() bool { return db.Stats().InUse == 0 }, time.Second, time.Millisecond)
			require.NoError(t, db.GetContext(t.Context(), &after, connectionID))
			if _, pgx := backend.driver.(*pgdriver.PGXDriver); pgx {
				require.NotEqual(t, before, after)
			} else if backend.driver == nil {
				require.Equal(t, before, after)
			}
		})
	}
}
