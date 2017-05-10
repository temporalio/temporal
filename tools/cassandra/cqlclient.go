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
	"bufio"
	"errors"
	"io"
	"os"
	"strings"
	"time"

	"fmt"
	"github.com/gocql/gocql"
)

type (
	// CQLClient is the interface for implementations
	// that provide a way to talk to cassandra through CQL
	CQLClient interface {
		// Exec executes a cql statement
		Exec(stmt string) error
		// ListTables lists the table names in a keyspace
		ListTables() ([]string, error)
		// ListTypes lists the user defined types in a keyspace
		ListTypes() ([]string, error)
		// CreateKeyspace creates a keyspace, if it doesn't exist
		// it uses SimpleStrategy by default
		CreateKeyspace(name string, replicas int) error
		// DropTable drops the given table
		DropTable(name string) error
		// DropType drops a user defined type from keyspace
		DropType(name string) error
		// CreateSchemaVersionTables sets up the schema version tables
		CreateSchemaVersionTables() error
		// ReadSchemaVersion returns the current schema version for the keyspace
		ReadSchemaVersion() (int64, error)
		// UpdateSchemaVersion updates the schema version for the keyspace
		UpdateSchemaVersion(newVersion int64, minCompatibleVersion int64) error
		// WriteSchemaUpdateLog adds an entry to the schema update history table
		WriteSchemaUpdateLog(oldVersion int64, newVersion int64, manifestMD5 string, desc string) error
	}
	cqlClient struct {
		session       *gocql.Session
		clusterConfig *gocql.ClusterConfig
	}
)

var errNoHosts = errors.New("Cassandra hosts list is empty or malformed")
var errGetSchemaVersion = errors.New("Failed to get current schema version from cassandra")

const (
	newLineDelim       = '\n'
	defaultTimeout     = 30 * time.Second
	cqlProtoVersion    = 4        // default CQL protocol version
	defaultConsistency = "QUORUM" // schema updates must always be QUORUM
)

const (
	readSchemaVersionCQL        = `SELECT curr_version from schema_version where keyspace_name=?`
	listTablesCQL               = `SELECT table_name from system_schema.tables where keyspace_name=?`
	listTypesCQL                = `SELECT type_name from system_schema.types where keyspace_name=?`
	writeSchemaVersionCQL       = `INSERT into schema_version(keyspace_name, creation_time, curr_version, min_compatible_version) VALUES (?,?,?,?)`
	writeSchemaUpdateHistoryCQL = `INSERT into schema_update_history(year, month, update_time, old_version, new_version, manifest_md5, description) VALUES(?,?,?,?,?,?,?)`

	createSchemaVersionTableCQL = `CREATE TABLE schema_version(keyspace_name text PRIMARY KEY, ` +
		`creation_time timestamp, ` +
		`curr_version bigint, ` +
		`min_compatible_version bigint);`

	createSchemaUpdateHistoryTableCQL = `CREATE TABLE schema_update_history(` +
		`year int, ` +
		`month int, ` +
		`update_time timestamp, ` +
		`description text, ` +
		`manifest_md5 text, ` +
		`new_version bigint, ` +
		`old_version bigint, ` +
		`PRIMARY KEY ((year, month), update_time));`

	createKeyspaceCQL = `CREATE KEYSPACE IF NOT EXISTS %v ` +
		`WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : %v};`
)

// newCQLClient returns a new instance of CQLClient
func newCQLClient(hostsCsv string, keyspace string) (CQLClient, error) {
	hosts := parseHosts(hostsCsv)
	if len(hosts) == 0 {
		return nil, errNoHosts
	}
	clusterCfg := gocql.NewCluster(hosts...)
	clusterCfg.Keyspace = keyspace
	clusterCfg.Timeout = defaultTimeout
	clusterCfg.ProtoVersion = cqlProtoVersion
	clusterCfg.Consistency = gocql.ParseConsistency(defaultConsistency)
	cqlClient := new(cqlClient)
	cqlClient.clusterConfig = clusterCfg
	var err error
	cqlClient.session, err = clusterCfg.CreateSession()
	if err != nil {
		return nil, err
	}
	return cqlClient, nil
}

// Createkeyspace creates a keyspace if it doesn't exist
func (client *cqlClient) CreateKeyspace(name string, replicas int) error {
	return client.Exec(fmt.Sprintf(createKeyspaceCQL, name, replicas))
}

// ListTables lists the table names in a keyspace
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

// ListTypes lists the user defined types in a keyspace
func (client *cqlClient) ListTypes() ([]string, error) {
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

// DropTable drops a given table from the keyspace
func (client *cqlClient) DropTable(name string) error {
	return client.Exec(fmt.Sprintf("DROP TABLE %v", name))
}

// DropType drops a given type from the keyspace
func (client *cqlClient) DropType(name string) error {
	return client.Exec(fmt.Sprintf("DROP TYPE %v", name))
}

// CreateSchemaVersionTables sets up the schema version tables
func (client *cqlClient) CreateSchemaVersionTables() error {
	if err := client.Exec(createSchemaVersionTableCQL); err != nil {
		return err
	}
	if err := client.Exec(createSchemaUpdateHistoryTableCQL); err != nil {
		return err
	}
	return nil
}

// ReadSchemaVersion returns the current schema version for the keyspace
func (client *cqlClient) ReadSchemaVersion() (int64, error) {
	query := client.session.Query(readSchemaVersionCQL, client.clusterConfig.Keyspace)
	iter := query.Iter()
	var version int64
	if !iter.Scan(&version) {
		iter.Close()
		return 0, errGetSchemaVersion
	}
	if err := iter.Close(); err != nil {
		return 0, err
	}
	return version, nil
}

// UpdateShemaVersion updates the schema version for the keyspace
func (client *cqlClient) UpdateSchemaVersion(newVersion int64, minCompatibleVersion int64) error {
	query := client.session.Query(writeSchemaVersionCQL, client.clusterConfig.Keyspace, time.Now(), newVersion, minCompatibleVersion)
	return query.Exec()
}

// WriteSchemaUpdateLog adds an entry to the schema update history table
func (client *cqlClient) WriteSchemaUpdateLog(oldVersion int64, newVersion int64, manifestMD5 string, desc string) error {
	now := time.Now().UTC()
	query := client.session.Query(writeSchemaUpdateHistoryCQL)
	query.Bind(now.Year(), int(now.Month()), now, oldVersion, newVersion, manifestMD5, desc)
	return query.Exec()
}

// Exec executes a cql statement
func (client *cqlClient) Exec(stmt string) error {
	return client.session.Query(stmt).Exec()
}

func parseHosts(input string) []string {
	var hosts = make([]string, 0)
	for _, h := range strings.Split(input, ",") {
		if host := strings.TrimSpace(h); len(host) > 0 {
			hosts = append(hosts, host)
		}
	}
	return hosts
}

// ParseCQLFile takes a cql file path as input
// and returns an array of cql statements on
// success.
func ParseCQLFile(filePath string) ([]string, error) {

	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	reader := bufio.NewReader(f)

	var line string
	var currStmt string
	var stmts = make([]string, 0, 4)

	for err == nil {

		line, err = reader.ReadString(newLineDelim)
		line = strings.TrimSpace(line)
		if len(line) < 1 {
			continue
		}

		// Filter out the comment lines, the
		// only recognized comment line format
		// is any line that starts with double dashes
		tokens := strings.Split(line, "--")
		if len(tokens) > 0 && len(tokens[0]) > 0 {
			currStmt += tokens[0]
			// semi-colon is the end of statement delim
			if strings.HasSuffix(currStmt, ";") {
				stmts = append(stmts, currStmt)
				currStmt = ""
			}
		}
	}

	if err == io.EOF {
		return stmts, nil
	}

	return nil, err
}

// dropKeyspace deletes all tables/types in the
// keyspace without deleting the keyspace
func dropKeyspace(client CQLClient) {
	tables, err := client.ListTables()
	if err != nil {
		return
	}
	fmt.Printf("Dropping following tables: %v\n", tables)
	for _, table := range tables {
		err1 := client.DropTable(table)
		if err1 != nil {
			fmt.Printf("Error dropping table %v, err=%v\n", table, err1)
		}
	}
	types, err := client.ListTypes()
	if err != nil {
		return
	}
	fmt.Printf("Dropping following types: %v\n", types)
	for _, t := range types {
		err1 := client.DropType(t)
		if err1 != nil {
			fmt.Printf("Error dropping type %v, err=%v\n", t, err1)
		}
	}
}
