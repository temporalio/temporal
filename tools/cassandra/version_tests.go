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

package cassandra

import (
	"path"
	"runtime"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/cassandra"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/environment"
)

type (
	VersionTestSuite struct {
		*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite
	}
)

func (s *VersionTestSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

func (s *VersionTestSuite) TestVerifyCompatibleVersion() {
	keyspace := "temporal_ver_test_"
	_, filename, _, ok := runtime.Caller(0)
	s.True(ok)
	root := path.Dir(path.Dir(path.Dir(filename)))
	cqlFile := path.Join(root, "schema/cassandra/temporal/schema.cql")

	defer s.createKeyspace(keyspace)()
	s.Nil(RunTool([]string{
		"./tool", "-k", keyspace, "-q", "setup-schema", "-f", cqlFile, "-version", "10.0", "-o",
	}))

	defaultCfg := config.Cassandra{
		Hosts:    environment.GetCassandraAddress(),
		Port:     environment.GetCassandraPort(),
		User:     "",
		Password: "",
		Keyspace: keyspace,
	}
	cfg := config.Persistence{
		DefaultStore: "default",
		DataStores: map[string]config.DataStore{
			"default": {Cassandra: &defaultCfg},
		},
		TransactionSizeLimit: dynamicconfig.GetIntPropertyFn(primitives.DefaultTransactionSizeLimit),
	}
	s.NoError(cassandra.VerifyCompatibleVersion(cfg, resolver.NewNoopResolver()))
}

func (s *VersionTestSuite) createKeyspace(keyspace string) func() {
	cfg := &CQLClientConfig{
		Hosts:       environment.GetCassandraAddress(),
		Port:        environment.GetCassandraPort(),
		Keyspace:    "system",
		Timeout:     defaultTimeout,
		numReplicas: 1,
	}
	client, err := newCQLClient(cfg, log.NewNoopLogger())
	s.NoError(err)

	err = client.createKeyspace(keyspace)
	if err != nil {
		s.Fail("error creating keyspace, err=%v", err)
	}
	return func() {
		s.NoError(client.dropKeyspace(keyspace))
		client.Close()
	}
}
