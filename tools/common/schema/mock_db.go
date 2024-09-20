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

package schema

import (
	"fmt"
)

type (
	mockSQLDB struct {
	}
)

// Exec executes a cql statement
func (db *mockSQLDB) Exec(stmt string, args ...interface{}) error {
	return fmt.Errorf("unimplemented")
}

// DropAllTables drops all tables
func (db *mockSQLDB) DropAllTables() error {
	return fmt.Errorf("unimplemented")
}

// CreateSchemaVersionTables sets up the schema version tables
func (db *mockSQLDB) CreateSchemaVersionTables() error {
	return fmt.Errorf("unimplemented")
}

// ReadSchemaVersion returns the current schema version for the keyspace
func (db *mockSQLDB) ReadSchemaVersion() (string, error) {
	return "", fmt.Errorf("unimplemented")
}

// UpdateSchemaVersion updates the schema version for the keyspace
func (db *mockSQLDB) UpdateSchemaVersion(newVersion string, minCompatibleVersion string) error {
	return fmt.Errorf("unimplemented")
}

// WriteSchemaUpdateLog adds an entry to the schema update history table
func (db *mockSQLDB) WriteSchemaUpdateLog(oldVersion string, newVersion string, manifestMD5 string, desc string) error {
	return fmt.Errorf("unimplemented")
}

// Close gracefully closes the client object
func (db *mockSQLDB) Close() {}

// Type gives the type of db (e.g. "cassandra", "sql")
func (db *mockSQLDB) Type() string {
	return "sql"
}
