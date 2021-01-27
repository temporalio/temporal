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

package sql

import (
	"go.temporal.io/server/common/persistence/schema"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/service/config"
)

// VerifyCompatibleVersion ensures that the installed version of temporal and visibility
// is greater than or equal to the expected version.
func VerifyCompatibleVersion(
	cfg config.Persistence,
	r resolver.ServiceResolver,
) error {

	ds, ok := cfg.DataStores[cfg.DefaultStore]
	if ok && ds.SQL != nil {
		db, err := NewSQLAdminDB(ds.SQL, r)
		if err != nil {
			return err
		}
		defer func() { _ = db.Close() }()

		err = schema.VerifyCompatibleVersion(db, ds.SQL.DatabaseName, db.ExpectedVersion())
		if err != nil {
			return err
		}
	}
	ds, ok = cfg.DataStores[cfg.VisibilityStore]
	if ok && ds.SQL != nil {
		db, err := NewSQLAdminDB(ds.SQL, r)
		if err != nil {
			return err
		}
		defer func() { _ = db.Close() }()

		err = schema.VerifyCompatibleVersion(db, ds.SQL.DatabaseName, db.ExpectedVisibilityVersion())
		if err != nil {
			return err
		}
	}
	return nil
}
