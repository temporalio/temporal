package mysql

import (
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/temporalio/temporal/common/service/config"
)

type StoreTestSuite struct {
	suite.Suite
}

func TestStoreTestSuite(t *testing.T) {
	suite.Run(t, new(StoreTestSuite))
}

func (s *StoreTestSuite) TestBuildDSN() {
	testCases := []struct {
		in              config.SQL
		outURLPath      string
		outIsolationKey string
		outIsolationVal string
	}{
		{
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

	for _, tc := range testCases {
		out := buildDSN(&tc.in)
		s.True(strings.HasPrefix(out, tc.outURLPath), "invalid url path")
		tokens := strings.Split(out, "?")
		s.Equal(2, len(tokens), "invalid url")
		qry, err := url.Parse("?" + tokens[1])
		s.NoError(err)
		wantAttrs := buildExpectedURLParams(tc.in.ConnectAttributes, tc.outIsolationKey, tc.outIsolationVal)
		s.Equal(wantAttrs, qry.Query(), "invalid dsn url params")
	}
}

func buildExpectedURLParams(attrs map[string]string, isolationKey string, isolationValue string) url.Values {
	result := make(map[string][]string, len(dsnAttrOverrides)+len(attrs)+1)
	for k, v := range attrs {
		result[k] = []string{v}
	}
	result[isolationKey] = []string{isolationValue}
	for k, v := range dsnAttrOverrides {
		result[k] = []string{v}
	}
	return result
}
