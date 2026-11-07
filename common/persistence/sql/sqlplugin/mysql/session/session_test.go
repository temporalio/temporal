package session

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
	"go.uber.org/mock/gomock"
)

type (
	sessionTestSuite struct {
		suite.Suite
		controller *gomock.Controller
	}
)

func TestSessionTestSuite(t *testing.T) {
	s := new(sessionTestSuite)
	suite.Run(t, s)
}

func (s *sessionTestSuite) SetupSuite() {

}

func (s *sessionTestSuite) TearDownSuite() {

}

func (s *sessionTestSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
}

func (s *sessionTestSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *sessionTestSuite) TestBuildDSN() {
	testCases := []struct {
		name                string
		in                  config.SQL
		outURLPath          string
		outIsolationKey     string
		outIsolationVal     string
		expectInvalidConfig bool
	}{
		{
			name: "no connect attributes",
			in: config.SQL{
				User:            "test",
				Password:        "pass",
				ConnectProtocol: "tcp",
				ConnectAddr:     "192.168.0.1:3306",
				DatabaseName:    "db1",
			},
			outIsolationKey: "transaction_isolation",
			outIsolationVal: "'READ-COMMITTED'",
			outURLPath:      "test:pass@tcp(192.168.0.1:3306)/db1?",
		},
		{
			name: "with connect attributes",
			in: config.SQL{
				User:              "test",
				Password:          "pass",
				ConnectProtocol:   "tcp",
				ConnectAddr:       "192.168.0.1:3306",
				DatabaseName:      "db1",
				ConnectAttributes: map[string]string{"k1": "v1", "k2": "v2"},
			},
			outIsolationKey: "transaction_isolation",
			outIsolationVal: "'READ-COMMITTED'",
			outURLPath:      "test:pass@tcp(192.168.0.1:3306)/db1?",
		},
		{
			name: "override isolation level (quoted, shorthand)",
			in: config.SQL{
				User:              "test",
				Password:          "pass",
				ConnectProtocol:   "tcp",
				ConnectAddr:       "192.168.0.1:3306",
				DatabaseName:      "db1",
				ConnectAttributes: map[string]string{"k1": "v1", "k2": "v2", "tx_isolation": "'REPEATABLE-READ'"},
			},
			outIsolationKey: "tx_isolation",
			outIsolationVal: "'repeatable-read'",
			outURLPath:      "test:pass@tcp(192.168.0.1:3306)/db1?",
		},
		{
			name: "override isolation level (unquoted, shorthand)",
			in: config.SQL{
				User:              "test",
				Password:          "pass",
				ConnectProtocol:   "tcp",
				ConnectAddr:       "192.168.0.1:3306",
				DatabaseName:      "db1",
				ConnectAttributes: map[string]string{"k1": "v1", "k2": "v2", "tx_isolation": "REPEATABLE-READ"},
			},
			outIsolationKey: "tx_isolation",
			outIsolationVal: "'repeatable-read'",
			outURLPath:      "test:pass@tcp(192.168.0.1:3306)/db1?",
		},
		{
			name: "override isolation level (unquoted, full name)",
			in: config.SQL{
				User:              "test",
				Password:          "pass",
				ConnectProtocol:   "tcp",
				ConnectAddr:       "192.168.0.1:3306",
				DatabaseName:      "db1",
				ConnectAttributes: map[string]string{"k1": "v1", "k2": "v2", "transaction_isolation": "REPEATABLE-READ"},
			},
			outIsolationKey: "transaction_isolation",
			outIsolationVal: "'repeatable-read'",
			outURLPath:      "test:pass@tcp(192.168.0.1:3306)/db1?",
		},
	}

	for _, dbKind := range []sqlplugin.DbKind{sqlplugin.DbKindMain, sqlplugin.DbKindVisibility} {
		for _, tc := range testCases {
			s.Run(fmt.Sprintf("%s: %s", dbKind.String(), tc.name), func() {
				r := resolver.NewMockServiceResolver(s.controller)
				r.EXPECT().Resolve(tc.in.ConnectAddr).Return([]string{tc.in.ConnectAddr})

				configs, err := buildConfigs(context.Background(), dbKind, &tc.in, r, nil)
				if tc.expectInvalidConfig {
					s.Error(err, "Expected an invalid configuration error")
				} else {
					s.NoError(err)
				}
				s.Len(configs, 1)
				out := configs[0].FormatDSN()
				s.True(strings.HasPrefix(out, tc.outURLPath), "invalid url path")
				tokens := strings.Split(out, "?")
				s.Len(tokens, 2, "invalid url")
				qry, err := url.Parse("?" + tokens[1])
				s.NoError(err)
				wantAttrs := buildExpectedURLParams(dbKind, tc.in.ConnectAttributes, tc.outIsolationKey, tc.outIsolationVal)
				s.Equal(wantAttrs, qry.Query(), "invalid dsn url params")
			})
		}
	}
}

func (s *sessionTestSuite) Test_Visibility_DoesntSupport_interpolateParams() {
	cfg := config.SQL{
		User:              "test",
		Password:          "pass",
		ConnectProtocol:   "tcp",
		ConnectAddr:       "192.168.0.1:3306",
		DatabaseName:      "db1",
		ConnectAttributes: map[string]string{"interpolateParams": "ignored"},
	}
	r := resolver.NewMockServiceResolver(s.controller)
	r.EXPECT().Resolve(cfg.ConnectAddr).Return([]string{cfg.ConnectAddr})
	_, err := buildConfigs(context.Background(), sqlplugin.DbKindVisibility, &cfg, r, nil)
	s.Error(err, "We should return an error when a MySQL Visibility database is configured with interpolateParams")
}

func TestBuildConfigsFromCommaSeparatedList(t *testing.T) {
	cfg := &config.SQL{
		User:            "test",
		Password:        "pass",
		ConnectAddr:     "mysql-1:3306, mysql-2:3307",
		ConnectProtocol: "tcp",
		DatabaseName:    "db1",
	}
	r := resolver.NewMockServiceResolver(gomock.NewController(t))
	r.EXPECT().Resolve(cfg.ConnectAddr).Return([]string{cfg.ConnectAddr})

	configs, err := buildConfigs(context.Background(), sqlplugin.DbKindMain, cfg, r, nil)

	require.NoError(t, err)
	require.Len(t, configs, 2)
	for index, expectedAddress := range []string{"mysql-1:3306", "mysql-2:3307"} {
		require.Equal(t, expectedAddress, configs[index].Addr)
		require.Equal(t, "tcp", configs[index].Net)
	}
}

func TestResolveAddressesFromSRV(t *testing.T) {
	cfg := &config.SQL{
		ConnectAddr:     "_mysql._tcp.example.com",
		ConnectProtocol: srvConnectProtocol,
	}

	addresses, err := resolveAddresses(
		context.Background(),
		cfg,
		nil,
		func(_ context.Context, service string, proto string, name string) (string, []*net.SRV, error) {
			require.Empty(t, service)
			require.Empty(t, proto)
			require.Equal(t, cfg.ConnectAddr, name)
			return "", []*net.SRV{
				{Target: "mysql-1.example.com.", Port: 3306},
				{Target: "mysql-2.example.com.", Port: 3307},
			}, nil
		},
	)

	require.NoError(t, err)
	require.Equal(t, []string{"mysql-1.example.com:3306", "mysql-2.example.com:3307"}, addresses)
}

func TestResolveAddressesRejectsEmptyAddress(t *testing.T) {
	cfg := &config.SQL{
		ConnectAddr:     "mysql-1:3306, ,mysql-2:3306",
		ConnectProtocol: "tcp",
	}
	r := resolver.NewMockServiceResolver(gomock.NewController(t))
	r.EXPECT().Resolve(cfg.ConnectAddr).Return([]string{cfg.ConnectAddr})

	_, err := resolveAddresses(context.Background(), cfg, r, nil)

	require.ErrorContains(t, err, "empty MySQL address")
}

func TestMultiHostConnectorFallsBackAndRefreshesTargets(t *testing.T) {
	buildCalls := 0
	var attempts []string
	connector := &multiHostConnector{
		rememberPreferred: true,
		buildConfigs: func(context.Context) ([]*mysql.Config, error) {
			buildCalls++
			return []*mysql.Config{{Addr: "first"}, {Addr: "second"}}, nil
		},
		newConnector: func(cfg *mysql.Config) (driver.Connector, error) {
			attempts = append(attempts, cfg.Addr)
			return &fakeConnector{
				connect: func(context.Context) (driver.Conn, error) {
					return &fakeQueryConn{writable: cfg.Addr == "second"}, nil
				},
			}, nil
		},
	}

	for range 2 {
		_, err := connector.Connect(context.Background())
		require.NoError(t, err)
	}
	require.Equal(t, 2, buildCalls)
	require.Equal(t, []string{"first", "second", "second"}, attempts)
}

func TestMultiHostConnectorReturnsAllConnectionErrors(t *testing.T) {
	firstHostErr := errors.New("first host unavailable")
	secondHostErr := errors.New("second host unavailable")
	connector := &multiHostConnector{
		buildConfigs: func(context.Context) ([]*mysql.Config, error) {
			return []*mysql.Config{{Addr: "first"}, {Addr: "second"}}, nil
		},
		newConnector: func(cfg *mysql.Config) (driver.Connector, error) {
			return &fakeConnector{
				connect: func(context.Context) (driver.Conn, error) {
					if cfg.Addr == "first" {
						return nil, firstHostErr
					}
					return nil, secondHostErr
				},
			}, nil
		},
	}

	_, err := connector.Connect(context.Background())

	require.ErrorIs(t, err, firstHostErr)
	require.ErrorIs(t, err, secondHostErr)
}

func TestBuildConfigsKeepTLSPerSession(t *testing.T) {
	build := func(serverName string, explicitLegacyAttribute bool) *mysql.Config {
		t.Helper()
		cfg := &config.SQL{
			User:            "test",
			Password:        "pass",
			ConnectAddr:     "mysql:3306",
			ConnectProtocol: "tcp",
			DatabaseName:    "db1",
			TLS: &auth.TLS{
				Enabled:                true,
				EnableHostVerification: true,
				ServerName:             serverName,
			},
		}
		if explicitLegacyAttribute {
			cfg.ConnectAttributes = map[string]string{"tls": "tls-custom"}
		}
		tlsConfig, err := buildTLSConfig(cfg)
		require.NoError(t, err)
		configs, err := buildConfigs(
			context.Background(),
			sqlplugin.DbKindMain,
			cfg,
			resolver.NewNoopResolver(),
			tlsConfig,
		)
		require.NoError(t, err)
		require.Len(t, configs, 1)
		return configs[0]
	}

	first := build("mysql-1.example.com", false)
	second := build("mysql-2.example.com", true)

	require.Equal(t, "mysql-1.example.com", first.TLS.ServerName)
	require.Equal(t, "mysql-2.example.com", second.TLS.ServerName)
	require.NotSame(t, first.TLS, second.TLS)
}

func TestVerifyWritable(t *testing.T) {
	require.NoError(t, verifyWritable(context.Background(), &fakeQueryConn{writable: true}))
	require.ErrorIs(t, verifyWritable(context.Background(), &fakeQueryConn{writable: false}), errReadOnly)
}

type fakeConnector struct {
	connect func(context.Context) (driver.Conn, error)
}

func (c *fakeConnector) Connect(ctx context.Context) (driver.Conn, error) {
	return c.connect(ctx)
}

func (*fakeConnector) Driver() driver.Driver {
	return nil
}

type fakeConn struct{}

func (fakeConn) Prepare(string) (driver.Stmt, error) {
	return nil, nil
}

func (fakeConn) Close() error {
	return nil
}

func (fakeConn) Begin() (driver.Tx, error) {
	return nil, nil
}

type fakeQueryConn struct {
	fakeConn
	writable bool
}

func (c *fakeQueryConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &fakeRows{writable: c.writable}, nil
}

type fakeRows struct {
	writable bool
	read     bool
}

func (*fakeRows) Columns() []string {
	return []string{"writable"}
}

func (*fakeRows) Close() error {
	return nil
}

func (r *fakeRows) Next(values []driver.Value) error {
	if r.read {
		return io.EOF
	}
	r.read = true
	if r.writable {
		values[0] = []byte("1")
	} else {
		values[0] = []byte("0")
	}
	return nil
}

func buildExpectedURLParams(dbKind sqlplugin.DbKind, attrs map[string]string, isolationKey string, isolationValue string) url.Values {
	result := make(map[string][]string, len(dsnAttrOverrides)+len(attrs)+1)
	for k, v := range attrs {
		result[k] = []string{v}
	}
	result[isolationKey] = []string{isolationValue}
	for k, v := range dsnAttrOverrides {
		result[k] = []string{v}
	}
	result["rejectReadOnly"] = []string{"true"}
	if dbKind != sqlplugin.DbKindVisibility {
		result["interpolateParams"] = []string{"true"}
	}
	return result
}
