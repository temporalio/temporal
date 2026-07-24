package session

import (
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/iancoleman/strcase"
	"github.com/jmoiron/sqlx"
	mssql "github.com/microsoft/go-mssqldb"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
)

// driverName is the name go-mssqldb registers with database/sql and the key
// sqlx uses to pick the bindvar style. "sqlserver" maps to AT (@p1, @p2, ...)
// in sqlx, which is what named-query compilation and Rebind rely on. Do not
// change this to the legacy "mssql" driver name: it uses a different
// parameter convention.
const driverName = "sqlserver"

const (
	encryptAttr           = "encrypt"
	encryptDisable        = "disable"
	encryptTrue           = "true"
	trustServerCertAttr   = "trustservercertificate"
	certificateAttr       = "certificate"
	hostNameInCertificate = "hostnameincertificate"
	databaseAttr          = "database"
)

type Session struct {
	*sqlx.DB
}

func NewSession(
	cfg *config.SQL,
	resolver resolver.ServiceResolver,
) (*Session, error) {
	db, err := createConnection(cfg, resolver)
	if err != nil {
		return nil, err
	}
	return &Session{DB: db}, nil
}

func (s *Session) Close() {
	if s.DB != nil {
		_ = s.DB.Close()
	}
}

func createConnection(
	cfg *config.SQL,
	resolver resolver.ServiceResolver,
) (*sqlx.DB, error) {
	var db *sqlx.DB
	var err error

	if cfg.PasswordCommand != nil {
		// Rebuild the DSN (re-running passwordCommand) for every new physical
		// connection so short-lived credentials stay fresh.
		c := sqlplugin.NewRefreshingConnector(
			func() (string, error) {
				return buildDSN(cfg, resolver)
			},
			func(dsn string) (sqldriver.Connector, error) {
				return mssql.NewConnector(dsn)
			},
			&mssql.Driver{},
		)
		db = sqlx.NewDb(sql.OpenDB(c), driverName)
		if err := db.Ping(); err != nil {
			_ = db.Close()
			return nil, err
		}
	} else {
		var dsn string
		dsn, err = buildDSN(cfg, resolver)
		if err != nil {
			return nil, err
		}
		db, err = sqlx.Connect(driverName, dsn)
		if err != nil {
			return nil, err
		}
	}

	if cfg.MaxConns > 0 {
		db.SetMaxOpenConns(cfg.MaxConns)
	}
	if cfg.MaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.MaxConnLifetime > 0 {
		db.SetConnMaxLifetime(cfg.MaxConnLifetime)
	}

	// Maps struct names in CamelCase to snake without need for db struct tags.
	db.MapperFunc(strcase.ToSnake)
	return db, nil
}

func buildDSN(
	cfg *config.SQL,
	r resolver.ServiceResolver,
) (string, error) {
	password, err := cfg.ResolvePassword()
	if err != nil {
		return "", err
	}
	attrs, err := buildDSNAttrs(cfg)
	if err != nil {
		return "", err
	}
	resolvedAddr := r.Resolve(cfg.ConnectAddr)[0]
	u := url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(cfg.User, password),
		Host:     resolvedAddr,
		RawQuery: attrs.Encode(),
	}
	return u.String(), nil
}

func buildDSNAttrs(cfg *config.SQL) (url.Values, error) {
	parameters := make(url.Values, len(cfg.ConnectAttributes)+2)
	for k, v := range cfg.ConnectAttributes {
		key := strings.TrimSpace(strings.ToLower(k))
		value := strings.TrimSpace(v)
		if parameters.Get(key) != "" {
			return nil, fmt.Errorf("duplicate connection attr: %v:%v, %v:%v",
				key, parameters.Get(key), key, value)
		}
		parameters.Set(key, value)
	}
	parameters.Set(databaseAttr, cfg.DatabaseName)

	if cfg.TLS != nil && cfg.TLS.Enabled {
		// go-mssqldb has no client-certificate authentication over TDS;
		// reject half-configured TLS instead of silently ignoring it.
		if cfg.TLS.CertFile != "" || cfg.TLS.KeyFile != "" {
			return nil, errors.New("failed to build mssql DSN: client certificate auth (certFile/keyFile) is not supported by the sqlserver driver")
		}
		if parameters.Get(encryptAttr) == "" {
			parameters.Set(encryptAttr, encryptTrue)
		}
		if parameters.Get(trustServerCertAttr) == "" && !cfg.TLS.EnableHostVerification {
			parameters.Set(trustServerCertAttr, "true")
		}
		if parameters.Get(certificateAttr) == "" && cfg.TLS.CaFile != "" {
			parameters.Set(certificateAttr, cfg.TLS.CaFile)
		}
		if parameters.Get(hostNameInCertificate) == "" && cfg.TLS.ServerName != "" {
			parameters.Set(hostNameInCertificate, cfg.TLS.ServerName)
		}
	} else if parameters.Get(encryptAttr) == "" {
		parameters.Set(encryptAttr, encryptDisable)
	}

	return parameters, nil
}
