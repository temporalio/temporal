// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package session

import (
	"net/url"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/resolver"
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
		r := resolver.NewMockServiceResolver(s.controller)
		r.EXPECT().Resolve(tc.in.ConnectAddr).Return([]string{tc.in.ConnectAddr})

		out := buildDSN(&tc.in, r)
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
	result["rejectReadOnly"] = []string{"true"}
	return result
}
