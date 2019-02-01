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

package storage

import (
	"fmt"

	"github.com/iancoleman/strcase"
	"github.com/jmoiron/sqlx"
	"github.com/uber/cadence/common/persistence/sql/storage/mysql"
	"github.com/uber/cadence/common/persistence/sql/storage/sqldb"
	"github.com/uber/cadence/common/service/config"
)

const (
	dataSourceName = "%s:%s@%v(%v)/%s?multiStatements=true&tx_isolation=%%27READ-COMMITTED%%27&parseTime=true&clientFoundRows=true"
)

// NewSQLDB creates a returns a reference to a logical connection to the
// underlying SQL database. The returned object is to tied to a single
// SQL database and the object can be used to perform CRUD operations on
// the tables in the database
func NewSQLDB(cfg *config.SQL) (sqldb.Interface, error) {
	db, err := sqlx.Connect(cfg.DriverName,
		fmt.Sprintf(dataSourceName, cfg.User, cfg.Password, cfg.ConnectProtocol, cfg.ConnectAddr, cfg.DatabaseName))
	if err != nil {
		return nil, err
	}
	// Maps struct names in CamelCase to snake without need for db struct tags.
	db.MapperFunc(strcase.ToSnake)
	return mysql.NewDB(db, nil), nil
}
