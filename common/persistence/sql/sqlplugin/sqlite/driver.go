// The MIT License
//
// Copyright (c) 2021 Datadog, Inc.
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

package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"regexp"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

const (
	goSqlDriverName       = "sqlite_temporal"
	sqlConstraintCodes    = sqlite3.SQLITE_CONSTRAINT | sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY | sqlite3.SQLITE_CONSTRAINT_UNIQUE
	sqlTableExistsPattern = "SQL logic error: table .* already exists \\(1\\)"
)

// Driver implements a sql driver that return a custom connection conn. conn implements ResetSession() and IsValid()
// so that the sql library does not close the connection when a transaction context times out.
type Driver struct {
	sqlite.Driver
}

func newDriver() driver.Driver {
	return &Driver{sqlite.Driver{}}
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	c, err := d.Driver.Open(name)
	if err != nil {
		return nil, err
	}
	return &conn{c}, nil
}

type conn struct {
	driver.Conn
}

// ResetSession does nothing.
func (c *conn) ResetSession(ctx context.Context) error {
	return nil
}

// IsValid always returns true. We only have one connection to the sqlite db. It should always be valid. Otherwise, we would have lost the database.
func (c *conn) IsValid() bool {
	return true
}

func init() {
	sql.Register(goSqlDriverName, newDriver())
}

var sqlTableExistsRegex = regexp.MustCompile(sqlTableExistsPattern)

func (*db) IsDupEntryError(err error) bool {
	var sqlErr *sqlite.Error
	if errors.As(err, &sqlErr) {
		return sqlErr.Code()&sqlConstraintCodes != 0
	}

	return false
}

func isTableExistsError(err error) bool {
	var sqlErr *sqlite.Error
	if errors.As(err, &sqlErr) {
		return sqlTableExistsRegex.MatchString(sqlErr.Error())
	}

	return false
}
