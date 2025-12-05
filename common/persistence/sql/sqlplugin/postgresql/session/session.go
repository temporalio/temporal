package session

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/iancoleman/strcase"
	"github.com/jmoiron/sqlx"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql/driver"
	"go.temporal.io/server/common/resolver"
)

const (
	dsnFmt = "postgres://%v:%v@%v/%v?%v"
)

const (
	sslMode        = "sslmode"
	sslModeNoop    = "disable"
	sslModeRequire = "require"
	sslModeFull    = "verify-full"

	sslCA   = "sslrootcert"
	sslKey  = "sslkey"
	sslCert = "sslcert"
)

type Session struct {
	*sqlx.DB
}

func NewSession(
	cfg *config.SQL,
	d driver.Driver,
	resolver resolver.ServiceResolver,
) (*Session, error) {
	db, err := createConnection(cfg, d, resolver)
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
	d driver.Driver,
	resolver resolver.ServiceResolver,
) (*sqlx.DB, error) {
	dsn, err := buildDSN(cfg, resolver)
	if err != nil {
		return nil, err
	}
	db, err := d.CreateConnection(dsn)
	if err != nil {
		return nil, err
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
	tlsAttrs, err := buildDSNAttr(cfg)
	if err != nil {
		return "", err
	}
	resolvedAddr := r.Resolve(cfg.ConnectAddr)[0]
	return fmt.Sprintf(
		dsnFmt,
		cfg.User,
		url.QueryEscape(cfg.Password),
		resolvedAddr,
		cfg.DatabaseName,
		tlsAttrs.Encode(),
	), nil
}

// nolint: revive
func buildDSNAttr(cfg *config.SQL) (url.Values, error) {
	parameters := make(url.Values, len(cfg.ConnectAttributes))
	for k, v := range cfg.ConnectAttributes {
		key := strings.TrimSpace(k)
		value := strings.TrimSpace(v)
		if parameters.Get(key) != "" {
			panic(fmt.Sprintf("duplicate connection attr: %v:%v, %v:%v",
				key,
				parameters.Get(key),
				key, value,
			))
		}
		parameters.Set(key, value)
	}

	if cfg.TLS != nil && cfg.TLS.Enabled {
		if parameters.Get(sslMode) == "" {
			if cfg.TLS.EnableHostVerification {
				parameters.Set(sslMode, sslModeFull)
			} else {
				parameters.Set(sslMode, sslModeRequire)
			}
		}

		if parameters.Get(sslCA) == "" && cfg.TLS.CaFile != "" {
			parameters.Set(sslCA, cfg.TLS.CaFile)
		}

		if parameters.Get(sslKey) == "" {
			if parameters.Get(sslCert) != "" {
				return nil, errors.New("failed to build postgresql DSN: sslcert connectAttribute is set but sslkey is not set")
			}
			if cfg.TLS.KeyFile != "" {
				if cfg.TLS.CertFile == "" {
					return nil, errors.New("failed to build postgresql DSN: TLS keyFile is set but TLS certFile is not set")
				}
				parameters.Set(sslKey, cfg.TLS.KeyFile)
				parameters.Set(sslCert, cfg.TLS.CertFile)
			} else if cfg.TLS.CertFile != "" {
				return nil, errors.New("failed to build postgresql DSN: TLS certFile is set but TLS keyFile is not set")
			}
		} else if parameters.Get(sslCert) == "" {
			return nil, errors.New("failed to build postgresql DSN: sslkey connectAttribute is set but sslcert is not set")
		}
	} else if parameters.Get(sslMode) == "" {
		parameters.Set(sslMode, sslModeNoop)
	}

	return parameters, nil
}
