package session

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/iancoleman/strcase"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jmoiron/sqlx"
	"go.temporal.io/server/common/auth"
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

	krbSrvName = "krbsrvname"
	krbSPN     = "krbspn"
)

// gssProviderOnce guards one-time registration of the GSSAPI
// provider with pgx. pgx stores the provider in a package-level var,
// so re-registering would leak configuration across SQL instances.
// The first Kerberos-enabled connection wins; subsequent connections
// with different Kerberos configs will return an error from
// registerGSSProvider below.
var (
	gssProviderOnce sync.Once
	gssProviderCfg  *auth.Kerberos
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
	var db *sqlx.DB
	var err error

	if cfg.Kerberos != nil && cfg.Kerberos.Enabled {
		if err := registerGSSProvider(cfg.Kerberos); err != nil {
			return nil, err
		}
	}

	if cfg.PasswordCommand != nil {
		db, err = d.CreateRefreshableConnection(func() (string, error) {
			return buildDSN(cfg, resolver)
		})
	} else {
		var dsn string
		dsn, err = buildDSN(cfg, resolver)
		if err != nil {
			return nil, err
		}
		db, err = d.CreateConnection(dsn)
	}
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
	password, err := cfg.ResolvePassword()
	if err != nil {
		return "", err
	}
	tlsAttrs, err := buildDSNAttr(cfg)
	if err != nil {
		return "", err
	}
	resolvedAddr := r.Resolve(cfg.ConnectAddr)[0]
	return fmt.Sprintf(
		dsnFmt,
		cfg.User,
		url.QueryEscape(password),
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

	if cfg.Kerberos != nil && cfg.Kerberos.Enabled {
		if cfg.PasswordCommand != nil {
			return nil, errors.New("failed to build postgresql DSN: kerberos and passwordCommand are mutually exclusive")
		}
		if cfg.Kerberos.ServiceName != "" {
			if parameters.Get(krbSrvName) != "" && parameters.Get(krbSrvName) != cfg.Kerberos.ServiceName {
				return nil, fmt.Errorf("failed to build postgresql DSN: kerberos serviceName %q conflicts with connectAttribute krbsrvname %q",
					cfg.Kerberos.ServiceName, parameters.Get(krbSrvName))
			}
			parameters.Set(krbSrvName, cfg.Kerberos.ServiceName)
		}
		if cfg.Kerberos.SPN != "" {
			if parameters.Get(krbSPN) != "" && parameters.Get(krbSPN) != cfg.Kerberos.SPN {
				return nil, fmt.Errorf("failed to build postgresql DSN: kerberos SPN %q conflicts with connectAttribute krbspn %q",
					cfg.Kerberos.SPN, parameters.Get(krbSPN))
			}
			parameters.Set(krbSPN, cfg.Kerberos.SPN)
		}
	}

	return parameters, nil
}

// registerGSSProvider registers a pgx GSSAPI provider on first call.
// Subsequent calls succeed only when presented with an identical
// Kerberos configuration — pgx stores the provider in a package
// global, so two SQL stores with divergent Kerberos settings cannot
// both be honoured within one process.
func registerGSSProvider(cfg *auth.Kerberos) error {
	var registerErr error
	gssProviderOnce.Do(func() {
		gssProviderCfg = cfg
		factory := kerberosGSSFactory(cfg)
		pgconn.RegisterGSSProvider(func() (pgconn.GSS, error) {
			return factory()
		})
	})
	if gssProviderCfg != nil && !kerberosConfigEqual(gssProviderCfg, cfg) {
		registerErr = errors.New("kerberos provider already registered with a different configuration; " +
			"all SQL datastores in a single process must share the same kerberos settings")
	}
	return registerErr
}

func kerberosConfigEqual(a, b *auth.Kerberos) bool {
	if a == b {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}
