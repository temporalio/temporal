package session

import (
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
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

				out, err := buildDSN(dbKind, &tc.in, r)
				if tc.expectInvalidConfig {
					s.Error(err, "Expected an invalid configuration error")
				} else {
					s.NoError(err)
				}
				s.True(strings.HasPrefix(out, tc.outURLPath), "invalid url path")
				tokens := strings.Split(out, "?")
				s.Equal(2, len(tokens), "invalid url")
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
	_, err := buildDSN(sqlplugin.DbKindVisibility, &cfg, r)
	s.Error(err, "We should return an error when a MySQL Visibility database is configured with interpolateParams")
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
