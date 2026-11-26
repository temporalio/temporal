package session

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"maps"
	"os"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/iancoleman/strcase"
	"github.com/jmoiron/sqlx"
	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
)

type Session struct {
	*sqlx.DB
}

const (
	driverName = "mysql"

	isolationLevelAttrName       = "transaction_isolation"
	isolationLevelAttrNameLegacy = "tx_isolation"
	defaultIsolationLevel        = "'READ-COMMITTED'"
	// customTLSName is the name used if a custom tls configuration is created
	customTLSName = "tls-custom"

	interpolateParamsAttr = "interpolateParams"
)

var (
	errVisInterpolateParamsNotSupported = errors.New("interpolateParams is not supported for mysql visibility stores")
	dsnAttrOverrides                    = map[string]string{
		"parseTime":       "true",
		"clientFoundRows": "true",
	}
)

func NewSession(
	dbKind sqlplugin.DbKind,
	cfg *config.SQL,
	resolver resolver.ServiceResolver,
) (*Session, error) {
	db, err := createConnection(dbKind, cfg, resolver)
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
	dbKind sqlplugin.DbKind,
	cfg *config.SQL,
	resolver resolver.ServiceResolver,
) (*sqlx.DB, error) {
	cfgCopy := *cfg
	cfgCopy.ConnectAttributes = maps.Clone(cfg.ConnectAttributes)
	useCustomTLS := cfg.TLS != nil && cfg.TLS.Enabled &&
		(cfg.ConnectAttributes["tls"] == "" || cfg.ConnectAttributes["tls"] == customTLSName)
	if useCustomTLS {
		delete(cfgCopy.ConnectAttributes, "tls")
	}
	c := sqlplugin.NewRefreshingConnector(
		func() (string, error) {
			return buildDSN(dbKind, &cfgCopy, resolver)
		},
		func(dsn string) (driver.Connector, error) {
			mysqlConfig, err := mysql.ParseDSN(dsn)
			if err != nil {
				return nil, err
			}
			tlsConfig, err := buildTLSConfig(cfgCopy.TLS)
			if err != nil {
				return nil, err
			}
			if useCustomTLS {
				mysqlConfig.TLS = tlsConfig
			}
			return mysql.NewConnector(mysqlConfig)
		},
		mysql.MySQLDriver{},
	)
	db := sqlx.NewDb(sql.OpenDB(c), driverName)
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
	dbKind sqlplugin.DbKind,
	cfg *config.SQL,
	r resolver.ServiceResolver,
) (string, error) {
	password, err := cfg.ResolvePassword()
	if err != nil {
		return "", err
	}

	mysqlConfig := mysql.NewConfig()

	mysqlConfig.User = cfg.User
	mysqlConfig.Passwd = password
	mysqlConfig.Addr = r.Resolve(cfg.ConnectAddr)[0]
	mysqlConfig.DBName = cfg.DatabaseName
	mysqlConfig.Net = cfg.ConnectProtocol
	mysqlConfig.Params, err = buildDSNAttrs(dbKind, cfg)
	if err != nil {
		return "", err
	}

	// https://github.com/go-sql-driver/mysql/blob/v1.5.0/dsn.go#L104-L106
	// https://github.com/go-sql-driver/mysql/blob/v1.5.0/dsn.go#L182-L189
	if mysqlConfig.Net == "" {
		mysqlConfig.Net = "tcp"
	}

	// https://github.com/go-sql-driver/mysql#rejectreadonly
	// https://github.com/temporalio/temporal/issues/1703
	mysqlConfig.RejectReadOnly = true

	return mysqlConfig.FormatDSN(), nil
}

func paramInterpolationAllowed(dbKind sqlplugin.DbKind) bool {
	return dbKind != sqlplugin.DbKindVisibility
}

func buildDSNAttrs(dbKind sqlplugin.DbKind, cfg *config.SQL) (map[string]string, error) {
	attrs := make(map[string]string, len(dsnAttrOverrides)+len(cfg.ConnectAttributes)+1)
	// Enable interpolation by default unless this is a mysql8 visibility store
	if paramInterpolationAllowed(dbKind) {
		attrs[interpolateParamsAttr] = "true"
	}
	for k, v := range cfg.ConnectAttributes {
		k1, v1 := sanitizeAttr(k, v)
		// Drop blank isolation entries; keeping them would emit an invalid "SET <key> =".
		if (k1 == isolationLevelAttrName || k1 == isolationLevelAttrNameLegacy) && v1 == "" {
			continue
		}
		attrs[k1] = v1
	}

	// only override isolation level if not specified
	if !hasAttr(attrs, isolationLevelAttrName) &&
		!hasAttr(attrs, isolationLevelAttrNameLegacy) {
		attrs[isolationLevelAttrName] = defaultIsolationLevel
	}

	// these attrs are always overriden
	maps.Copy(attrs, dsnAttrOverrides)

	if !paramInterpolationAllowed(dbKind) {
		if _, ok := attrs[interpolateParamsAttr]; ok {
			return nil, errVisInterpolateParamsNotSupported
		}
	}

	return attrs, nil
}

func hasAttr(attrs map[string]string, key string) bool {
	_, ok := attrs[key]
	return ok
}

func sanitizeAttr(inkey string, invalue string) (string, string) {
	key := strings.ToLower(strings.TrimSpace(inkey))
	value := strings.ToLower(strings.TrimSpace(invalue))
	switch key {
	case isolationLevelAttrName, isolationLevelAttrNameLegacy:
		if len(value) > 0 && value[0] != '\'' { // mysql sys variable values must be enclosed in single quotes
			value = "'" + value + "'"
		}
		return key, value
	default:
		return inkey, invalue
	}
}

func buildTLSConfig(cfg *auth.TLS) (*tls.Config, error) {
	if cfg == nil || !cfg.Enabled {
		return nil, nil
	}

	tlsConfig := auth.NewTLSConfigForServer(cfg.ServerName, cfg.EnableHostVerification)
	if cfg.CaFile != "" {
		rootCertPool := x509.NewCertPool()
		pem, err := os.ReadFile(cfg.CaFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load CA files: %w", err)
		}
		if !rootCertPool.AppendCertsFromPEM(pem) {
			return nil, errors.New("failed to append CA file")
		}
		tlsConfig.RootCAs = rootCertPool
	}
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load tls x509 key pair: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	return tlsConfig, nil
}
