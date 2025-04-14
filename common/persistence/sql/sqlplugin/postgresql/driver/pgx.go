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

package driver

import (
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib" // register pgx driver for sqlx
	"github.com/jmoiron/sqlx"
)

type PGXDriver struct{}

func (p *PGXDriver) CreateConnection(dsn string) (*sqlx.DB, error) {
	return sqlx.Connect("pgx", dsn)
}

func (p *PGXDriver) IsDupEntryError(err error) bool {
	pgxErr, ok := err.(*pgconn.PgError)
	return ok && pgxErr.Code == dupEntryCode
}

func (p *PGXDriver) IsDupDatabaseError(err error) bool {
	pqErr, ok := err.(*pgconn.PgError)
	return ok && pqErr.Code == dupDatabaseCode
}

func (p *PGXDriver) IsConnNeedsRefreshError(err error) bool {
	pqErr, ok := err.(*pgconn.PgError)
	if !ok {
		return false
	}
	return isConnNeedsRefreshError(pqErr.Code, pqErr.Message)
}
