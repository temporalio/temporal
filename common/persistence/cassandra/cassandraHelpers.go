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
	"os"
	"strings"

	"github.com/gocql/gocql"

	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/tools/cassandra"
	"go.temporal.io/server/tools/common/schema"
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
	override bool,
	tls *auth.TLS,
) (err error) {

	tmpFile, err := ioutil.TempFile("", "_temporal_")
	if err != nil {
		return fmt.Errorf("error creating tmp file:%v", err.Error())
	}
	defer os.Remove(tmpFile.Name())

	for _, file := range fileNames {
		// Flagged for potential file inclusion via variable. No user supplied input is included here - this just reads
		// schema files.
		// #nosec
		content, err := ioutil.ReadFile(dir + "/" + file)
		if err != nil {
			return fmt.Errorf("error reading contents of file %v:%v", file, err.Error())
		}
		_, err = tmpFile.WriteString(string(content) + "\n")
		if err != nil {
			return fmt.Errorf("error writing string to file, err: %v", err.Error())
		}
	}

	tmpFile.Close()

	config := &cassandra.SetupSchemaConfig{
		CQLClientConfig: cassandra.CQLClientConfig{
			Hosts:    strings.Join(hosts, ","),
			Port:     port,
			Timeout:  30,
			Keyspace: keyspace,
			TLS:      tls,
		},
		SetupConfig: schema.SetupConfig{
			SchemaFilePath:    tmpFile.Name(),
			Overwrite:         override,
			DisableVersioning: true,
		},
	}

	err = cassandra.SetupSchema(config)
	if err != nil {
		err = fmt.Errorf("error loading schema:%v", err.Error())
	}
	return
}
