// Copyright (c) 2017 Uber Technologies, Inc.
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
	log "github.com/sirupsen/logrus"
	"github.com/uber/cadence/tools/cassandra"
	"github.com/uber/cadence/tools/common/schema"
)

const cassandraPersistenceName = "cassandra"

// NewCassandraCluster creates a cassandra cluster given comma separated list of clusterHosts
func NewCassandraCluster(clusterHosts string, port int, user, password, dc string) *gocql.ClusterConfig {
	var hosts []string
	for _, h := range strings.Split(clusterHosts, ",") {
		if host := strings.TrimSpace(h); len(host) > 0 {
			hosts = append(hosts, host)
		}
	}

	cluster := gocql.NewCluster(hosts...)
	cluster.ProtoVersion = 4
	if port > 0 {
		cluster.Port = port
	}
	if user != "" && password != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: user,
			Password: password,
		}
	}
	if dc != "" {
		cluster.HostFilter = gocql.DataCentreHostFilter(dc)
	}
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	return cluster
}

// CreateCassandraKeyspace creates the keyspace using this session for given replica count
func CreateCassandraKeyspace(s *gocql.Session, keyspace string, replicas int, overwrite bool) (err error) {
	// if overwrite flag is set, drop the keyspace and create a new one
	if overwrite {
		DropCassandraKeyspace(s, keyspace)
	}
	err = s.Query(fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {
		'class' : 'SimpleStrategy', 'replication_factor' : %d}`, keyspace, replicas)).Exec()
	if err != nil {
		log.Error(`create keyspace error`, err)
		return
	}
	log.WithField(`keyspace`, keyspace).Debug(`created namespace`)

	return
}

// DropCassandraKeyspace drops the given keyspace, if it exists
func DropCassandraKeyspace(s *gocql.Session, keyspace string) (err error) {
	err = s.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", keyspace)).Exec()
	if err != nil {
		log.Error(`drop keyspace error`, err)
		return
	}
	log.WithField(`keyspace`, keyspace).Info(`dropped namespace`)
	return
}

// LoadCassandraSchema loads the schema from the given .cql files on this keyspace
func LoadCassandraSchema(
	dir string, fileNames []string, hosts []string, port int, keyspace string, override bool,
) (err error) {

	tmpFile, err := ioutil.TempFile("", "_cadence_")
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
		tmpFile.WriteString(string(content))
		tmpFile.WriteString("\n")
	}

	tmpFile.Close()

	config := &cassandra.SetupSchemaConfig{
		CQLClientConfig: cassandra.CQLClientConfig{
			Hosts:    strings.Join(hosts, ","),
			Port:     port,
			Keyspace: keyspace,
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
