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
	"errors"
	"regexp"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

const (
	goSqlDriverName       = "sqlite"
	sqlConstraintCodes    = sqlite3.SQLITE_CONSTRAINT | sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY | sqlite3.SQLITE_CONSTRAINT_UNIQUE
	sqlTableExistsPattern = "SQL logic error: table .* already exists \\(1\\)"
)

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
