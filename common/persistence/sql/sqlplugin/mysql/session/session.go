package session

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"maps"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

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
	customTLSName                = "tls-custom"

	interpolateParamsAttr = "interpolateParams"
	srvConnectProtocol    = "tcp+srv"
	checkWritableQuery    = "SELECT @@global.read_only = 0 AND @@global.super_read_only = 0"
)

var (
	errVisInterpolateParamsNotSupported = errors.New("interpolateParams is not supported for mysql visibility stores")
	errReadOnly                         = errors.New("MySQL server is read-only")
	dsnAttrOverrides                    = map[string]string{
		"parseTime":       "true",
		"clientFoundRows": "true",
	}
)

type (
	lookupSRVFunc func(context.Context, string, string, string) (string, []*net.SRV, error)

	multiHostConnector struct {
		buildConfigs      func(context.Context) ([]*mysql.Config, error)
		newConnector      func(*mysql.Config) (driver.Connector, error)
		driver            driver.Driver
		preferredHost     atomic.Uint64
		rememberPreferred bool
	}
)

func (c *multiHostConnector) Connect(ctx context.Context) (driver.Conn, error) {
	configs, err := c.buildConfigs(ctx)
	if err != nil {
		return nil, err
	}
	if len(configs) == 0 {
		return nil, errors.New("no MySQL connection targets")
	}

	connectionErrors := make([]error, 0, len(configs))
	start := 0
	if c.rememberPreferred {
		start = int(c.preferredHost.Load() % uint64(len(configs)))
	}
	for offset := range len(configs) {
		index := (start + offset) % len(configs)
		connector, err := c.newConnector(configs[index])
		if err == nil {
			var connection driver.Conn
			connection, err = connector.Connect(ctx)
			if err == nil && len(configs) > 1 {
				if err = verifyWritable(ctx, connection); err != nil {
					err = errors.Join(err, connection.Close())
				}
			}
			if err == nil {
				if c.rememberPreferred {
					c.preferredHost.Store(uint64(index))
				}
				return connection, nil
			}
		}
		connectionErrors = append(
			connectionErrors,
			fmt.Errorf("MySQL connection attempt %d failed: %w", index+1, err),
		)
	}
	return nil, errors.Join(connectionErrors...)
}

func (c *multiHostConnector) Driver() driver.Driver {
	return c.driver
}

func verifyWritable(ctx context.Context, connection driver.Conn) error {
	queryer, ok := connection.(driver.QueryerContext)
	if !ok {
		return errors.New("MySQL connection does not support writer detection")
	}

	rows, err := queryer.QueryContext(ctx, checkWritableQuery, nil)
	if err != nil {
		return fmt.Errorf("failed to check whether MySQL server is writable: %w", err)
	}

	values := make([]driver.Value, 1)
	nextErr := rows.Next(values)
	closeErr := rows.Close()
	if nextErr != nil {
		return errors.Join(fmt.Errorf("failed to read MySQL writer status: %w", nextErr), closeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("failed to close MySQL writer status rows: %w", closeErr)
	}
	switch writable := values[0].(type) {
	case bool:
		if writable {
			return nil
		}
	case int64:
		if writable == 1 {
			return nil
		}
	case []byte:
		if string(writable) == "1" {
			return nil
		}
	case string:
		if writable == "1" {
			return nil
		}
	default:
		return fmt.Errorf("unexpected MySQL writer status type %T", values[0])
	}
	return errReadOnly
}

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
	tlsConfig, err := buildTLSConfig(cfg)
	if err != nil {
		return nil, err
	}

	mysqlDriver := mysql.MySQLDriver{}
	connector := &multiHostConnector{
		buildConfigs: func(ctx context.Context) ([]*mysql.Config, error) {
			return buildConfigs(ctx, dbKind, cfg, resolver, tlsConfig)
		},
		newConnector:      mysql.NewConnector,
		driver:            mysqlDriver,
		rememberPreferred: cfg.ConnectProtocol != srvConnectProtocol,
	}
	db := sqlx.NewDb(sql.OpenDB(connector), driverName)
	if err := db.Ping(); err != nil {
		_ = db.Close()
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

func buildConfigs(
	ctx context.Context,
	dbKind sqlplugin.DbKind,
	cfg *config.SQL,
	r resolver.ServiceResolver,
	tlsConfig *tls.Config,
) ([]*mysql.Config, error) {
	password, err := cfg.ResolvePassword()
	if err != nil {
		return nil, err
	}

	addresses, err := resolveAddresses(ctx, cfg, r, net.DefaultResolver.LookupSRV)
	if err != nil {
		return nil, err
	}

	params, err := buildDSNAttrs(dbKind, cfg)
	if err != nil {
		return nil, err
	}
	useLocalTLS := tlsConfig != nil &&
		(cfg.ConnectAttributes["tls"] == "" || cfg.ConnectAttributes["tls"] == customTLSName)
	if useLocalTLS && cfg.ConnectAttributes["tls"] == customTLSName {
		params = maps.Clone(params)
		delete(params, "tls")
	}

	network := cfg.ConnectProtocol
	if network == "" || network == srvConnectProtocol {
		network = "tcp"
	}

	configs := make([]*mysql.Config, 0, len(addresses))
	for _, address := range addresses {
		mysqlConfig := mysql.NewConfig()
		mysqlConfig.User = cfg.User
		mysqlConfig.Passwd = password
		mysqlConfig.Addr = address
		mysqlConfig.DBName = cfg.DatabaseName
		mysqlConfig.Net = network
		mysqlConfig.Params = params

		// https://github.com/go-sql-driver/mysql#rejectreadonly
		// https://github.com/temporalio/temporal/issues/1703
		mysqlConfig.RejectReadOnly = true

		mysqlConfig, err = mysql.ParseDSN(mysqlConfig.FormatDSN())
		if err != nil {
			return nil, err
		}
		if useLocalTLS {
			mysqlConfig.TLS = tlsConfig.Clone()
		}
		configs = append(configs, mysqlConfig)
	}
	return configs, nil
}

func resolveAddresses(
	ctx context.Context,
	cfg *config.SQL,
	r resolver.ServiceResolver,
	lookupSRV lookupSRVFunc,
) ([]string, error) {
	if cfg.ConnectProtocol == srvConnectProtocol {
		_, records, err := lookupSRV(ctx, "", "", cfg.ConnectAddr)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve MySQL SRV record %q: %w", cfg.ConnectAddr, err)
		}

		addresses := make([]string, 0, len(records))
		for _, record := range records {
			host := strings.TrimSuffix(record.Target, ".")
			if host == "" {
				return nil, fmt.Errorf("MySQL SRV record %q contains an empty target", cfg.ConnectAddr)
			}
			addresses = append(addresses, net.JoinHostPort(host, strconv.Itoa(int(record.Port))))
		}
		if len(addresses) == 0 {
			return nil, fmt.Errorf("MySQL SRV record %q contains no targets", cfg.ConnectAddr)
		}
		return addresses, nil
	}

	resolvedAddresses := r.Resolve(cfg.ConnectAddr)
	addresses := make([]string, 0, len(resolvedAddresses))
	for _, resolvedAddress := range resolvedAddresses {
		candidates := []string{resolvedAddress}
		if strings.HasPrefix(cfg.ConnectProtocol, "tcp") {
			candidates = strings.Split(resolvedAddress, ",")
		}
		for _, candidate := range candidates {
			address := strings.TrimSpace(candidate)
			if address == "" {
				return nil, errors.New("connectAddr contains an empty MySQL address")
			}
			addresses = append(addresses, address)
		}
	}
	if len(addresses) == 0 {
		return nil, errors.New("connectAddr resolved to no MySQL addresses")
	}
	return addresses, nil
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
		if value[0] != '\'' { // mysql sys variable values must be enclosed in single quotes
			value = "'" + value + "'"
		}
		return key, value
	default:
		return inkey, invalue
	}
}

func buildTLSConfig(cfg *config.SQL) (*tls.Config, error) {
	if cfg.TLS == nil || !cfg.TLS.Enabled {
		return nil, nil
	}

	// TODO: create a way to set MinVersion and CipherSuites via cfg.
	tlsConfig := auth.NewTLSConfigForServer(cfg.TLS.ServerName, cfg.TLS.EnableHostVerification)

	if cfg.TLS.CaFile != "" {
		rootCertPool := x509.NewCertPool()
		pem, err := os.ReadFile(cfg.TLS.CaFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load CA files: %v", err)
		}
		if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
			return nil, errors.New("failed to append CA file")
		}
		tlsConfig.RootCAs = rootCertPool
	}

	if cfg.TLS.CertFile != "" && cfg.TLS.KeyFile != "" {
		clientCert := make([]tls.Certificate, 0, 1)
		certs, err := tls.LoadX509KeyPair(
			cfg.TLS.CertFile,
			cfg.TLS.KeyFile,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to load tls x509 key pair: %v", err)
		}
		clientCert = append(clientCert, certs)
		tlsConfig.Certificates = clientCert
	}

	return tlsConfig, nil
}
