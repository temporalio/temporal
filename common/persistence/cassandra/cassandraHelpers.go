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
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/gocql/gocql"

	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/service/config"
)

const cassandraPersistenceName = "cassandra"

// CreateCassandraKeyspace creates the keyspace using this session for given replica count
func CreateCassandraKeyspace(s *gocql.Session, keyspace string, replicas int, overwrite bool, logger log.Logger) (err error) {
	// if overwrite flag is set, drop the keyspace and create a new one
	if overwrite {
		err = DropCassandraKeyspace(s, keyspace, logger)
		if err != nil {
			logger.Error("drop keyspace error", tag.Error(err))
			return
		}
	}
	err = s.Query(fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {
		'class' : 'SimpleStrategy', 'replication_factor' : %d}`, keyspace, replicas)).Exec()
	if err != nil {
		logger.Error("create keyspace error", tag.Error(err))
		return
	}
	logger.Debug("created keyspace", tag.Value(keyspace))

	return
}

// DropCassandraKeyspace drops the given keyspace, if it exists
func DropCassandraKeyspace(s *gocql.Session, keyspace string, logger log.Logger) (err error) {
	err = s.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", keyspace)).Exec()
	if err != nil {
		logger.Error("drop keyspace error", tag.Error(err))
		return
	}
	logger.Debug("dropped keyspace", tag.Value(keyspace))
	return
}

// loadCassandraSchema loads the schema from the given .cql files on this keyspace
func loadCassandraSchema(
	dir string,
	fileNames []string,
	hosts []string,
	port int,
	keyspace string,
	tls *auth.TLS,
) (err error) {

	session, err := NewSession(config.Cassandra{
		Hosts:    strings.Join(hosts, ","),
		Port:     port,
		Keyspace: keyspace,
		TLS:      tls,
	}, resolver.NewNoopResolver())
	if err != nil {
		return err
	}

	for _, file := range fileNames {
		// This is only used in tests. Excluding it from security scanners
		// #nosec
		content, err := ioutil.ReadFile(dir + "/" + file)
		if err != nil {
			return fmt.Errorf("error reading contents of file %v:%v", file, err.Error())
		}
		for _, stmt := range strings.Split(string(content), ";") {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				continue
			}
			err = session.Query(stmt).Exec()
			if err != nil {
				return fmt.Errorf("error loading schema from %v: %v", file, err.Error())
			}
		}

	}
	return nil
}
