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
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/uber/cadence/common/auth"

	"github.com/uber/cadence/common/service/config"

	"github.com/gocql/gocql"

	"github.com/uber/cadence/common/cassandra"
	"github.com/uber/cadence/tools/common/schema"
)

type (
	cqlClient struct {
		nReplicas     int
		session       *gocql.Session
		clusterConfig *gocql.ClusterConfig
	}
	// CQLClientConfig contains the configuration for cql client
	CQLClientConfig struct {
		Hosts       string
		Port        int
		User        string
		Password    string
		Keyspace    string
		Timeout     int
		numReplicas int
		TLS         *auth.TLS
	}
)

var errNoHosts = errors.New("Cassandra Hosts list is empty or malformed")
var errGetSchemaVersion = errors.New("Failed to get current schema version from cassandra")

const (
	defaultTimeout       = 30    // Timeout in seconds
	cqlProtoVersion      = 4     // default CQL protocol version
	defaultConsistency   = "ALL" // schema updates must always be ALL
	defaultCassandraPort = 9042
	systemKeyspace       = "system"
)

const (
	readSchemaVersionCQL        = `SELECT curr_version from schema_version where keyspace_name=?`
	listTablesCQL               = `SELECT table_name from system_schema.tables where keyspace_name=?`
	listTypesCQL                = `SELECT type_name from system_schema.types where keyspace_name=?`
	writeSchemaVersionCQL       = `INSERT into schema_version(keyspace_name, creation_time, curr_version, min_compatible_version) VALUES (?,?,?,?)`
	writeSchemaUpdateHistoryCQL = `INSERT into schema_update_history(year, month, update_time, old_version, new_version, manifest_md5, description) VALUES(?,?,?,?,?,?,?)`

	createSchemaVersionTableCQL = `CREATE TABLE schema_version(keyspace_name text PRIMARY KEY, ` +
		`creation_time timestamp, ` +
		`curr_version text, ` +
		`min_compatible_version text);`

	createSchemaUpdateHistoryTableCQL = `CREATE TABLE schema_update_history(` +
		`year int, ` +
		`month int, ` +
		`update_time timestamp, ` +
		`description text, ` +
		`manifest_md5 text, ` +
		`new_version text, ` +
		`old_version text, ` +
		`PRIMARY KEY ((year, month), update_time));`

	createKeyspaceCQL = `CREATE KEYSPACE IF NOT EXISTS %v ` +
		`WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : %v};`
)

var _ schema.DB = (*cqlClient)(nil)

// NewCassandraCluster return gocql clusterConfig
func NewCassandraCluster(cfg *config.Cassandra, timeoutSeconds int) (*gocql.ClusterConfig, error) {
	clusterCfg := cassandra.NewCassandraCluster(*cfg)

	if len(clusterCfg.Hosts) == 0 {
		return nil, errNoHosts
	}

	timeout := time.Duration(timeoutSeconds) * time.Second
	clusterCfg.Timeout = timeout
	clusterCfg.ProtoVersion = cqlProtoVersion
	clusterCfg.Consistency = gocql.ParseConsistency(defaultConsistency)
	return clusterCfg, nil
}

// newCQLClient returns a new instance of CQLClient
func newCQLClient(cfg *CQLClientConfig) (*cqlClient, error) {
	var err error

	cassandraConfig := cfg.toCassandraConfig()
	clusterCfg, err := NewCassandraCluster(cassandraConfig, cfg.Timeout)
	if err != nil {
		return nil, err
	}
	cqlClient := new(cqlClient)
	cqlClient.nReplicas = cfg.numReplicas
	cqlClient.clusterConfig = clusterCfg
	cqlClient.session, err = clusterCfg.CreateSession()
	if err != nil {
		return nil, err
	}
	return cqlClient, nil
}

func (cfg *CQLClientConfig) toCassandraConfig() *config.Cassandra {
	cassandraConfig := config.Cassandra{
		Hosts:    cfg.Hosts,
		Port:     cfg.Port,
		User:     cfg.User,
		Password: cfg.Password,
		Keyspace: cfg.Keyspace,
		TLS:      cfg.TLS,
	}

	return &cassandraConfig
}

func (client *cqlClient) CreateDatabase(name string) error {
	return client.createKeyspace(name)
}

func (client *cqlClient) DropDatabase(name string) error {
	return client.dropKeyspace(name)
}

// createKeyspace creates a cassandra Keyspace if it doesn't exist
func (client *cqlClient) createKeyspace(name string) error {
	return client.Exec(fmt.Sprintf(createKeyspaceCQL, name, client.nReplicas))
}

// dropKeyspace drops a Keyspace
func (client *cqlClient) dropKeyspace(name string) error {
	return client.Exec(fmt.Sprintf("DROP KEYSPACE %v", name))
}

func (client *cqlClient) DropAllTables() error {
	return client.dropAllTablesTypes()
}

// CreateSchemaVersionTables sets up the schema version tables
func (client *cqlClient) CreateSchemaVersionTables() error {
	if err := client.Exec(createSchemaVersionTableCQL); err != nil {
		return err
	}
	return client.Exec(createSchemaUpdateHistoryTableCQL)
}

// ReadSchemaVersion returns the current schema version for the Keyspace
func (client *cqlClient) ReadSchemaVersion() (string, error) {
	query := client.session.Query(readSchemaVersionCQL, client.clusterConfig.Keyspace)
	iter := query.Iter()
	var version string
	if !iter.Scan(&version) {
		iter.Close()
		return "", errGetSchemaVersion
	}
	if err := iter.Close(); err != nil {
		return "", err
	}
	return version, nil
}

// UpdateShemaVersion updates the schema version for the Keyspace
func (client *cqlClient) UpdateSchemaVersion(newVersion string, minCompatibleVersion string) error {
	query := client.session.Query(writeSchemaVersionCQL, client.clusterConfig.Keyspace, time.Now(), newVersion, minCompatibleVersion)
	return query.Exec()
}

// WriteSchemaUpdateLog adds an entry to the schema update history table
func (client *cqlClient) WriteSchemaUpdateLog(oldVersion string, newVersion string, manifestMD5 string, desc string) error {
	now := time.Now().UTC()
	query := client.session.Query(writeSchemaUpdateHistoryCQL)
	query.Bind(now.Year(), int(now.Month()), now, oldVersion, newVersion, manifestMD5, desc)
	return query.Exec()
}

// Exec executes a cql statement
func (client *cqlClient) Exec(stmt string, args ...interface{}) error {
	return client.session.Query(stmt, args...).Exec()
}

// Close closes the cql client
func (client *cqlClient) Close() {
	if client.session != nil {
		client.session.Close()
	}
}

// ListTables lists the table names in a Keyspace
func (client *cqlClient) ListTables() ([]string, error) {
	query := client.session.Query(listTablesCQL, client.clusterConfig.Keyspace)
	iter := query.Iter()
	var names []string
	var name string
	for iter.Scan(&name) {
		names = append(names, name)
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	return names, nil
}

// listTypes lists the User defined types in a Keyspace
func (client *cqlClient) listTypes() ([]string, error) {
	qry := client.session.Query(listTypesCQL, client.clusterConfig.Keyspace)
	iter := qry.Iter()
	var names []string
	var name string
	for iter.Scan(&name) {
		names = append(names, name)
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	return names, nil
}

// dropTable drops a given table from the Keyspace
func (client *cqlClient) dropTable(name string) error {
	return client.Exec(fmt.Sprintf("DROP TABLE %v", name))
}

// dropType drops a given type from the Keyspace
func (client *cqlClient) dropType(name string) error {
	return client.Exec(fmt.Sprintf("DROP TYPE %v", name))
}

// dropAllTablesTypes deletes all tables/types in the
// Keyspace without deleting the Keyspace
func (client *cqlClient) dropAllTablesTypes() error {
	tables, err := client.ListTables()
	if err != nil {
		return err
	}
	log.Printf("Dropping following tables: %v\n", tables)
	for _, table := range tables {
		err1 := client.dropTable(table)
		if err1 != nil {
			log.Printf("Error dropping table %v, err=%v\n", table, err1)
		}
	}

	types, err := client.listTypes()
	if err != nil {
		return err
	}
	log.Printf("Dropping following types: %v\n", types)
	numOfTypes := len(types)
	for i := 0; i < numOfTypes && len(types) > 0; i++ {
		var erroredTypes []string
		for _, t := range types {
			err = client.dropType(t)
			if err != nil {
				log.Printf("Error dropping type %v, err=%v\n", t, err)
				erroredTypes = append(erroredTypes, t)
			}
		}
		types = erroredTypes
	}
	if len(types) > 0 {
		return err
	}
	return nil
}
