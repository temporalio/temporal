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
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
)

// VerifyCompatibleVersion ensures that the installed version of temporal and visibility
// is greater than or equal to the expected version.
func VerifyCompatibleVersion(
	cfg config.Persistence,
	r resolver.ServiceResolver,
) error {

	if err := checkMainDatabase(cfg, r); err != nil {
		return err
	}
	if cfg.StandardVisibilityConfigExist() {
		return checkVisibilityDatabase(cfg, r)
	}
	return nil
}

func checkMainDatabase(
	cfg config.Persistence,
	r resolver.ServiceResolver,
) error {
	ds, ok := cfg.DataStores[cfg.DefaultStore]
	if ok && ds.SQL != nil {
		return checkCompatibleVersion(ds.SQL, r, sqlplugin.DbKindMain)
	}
	return nil
}

func checkVisibilityDatabase(
	cfg config.Persistence,
	r resolver.ServiceResolver,
) error {
	ds, ok := cfg.DataStores[cfg.VisibilityStore]
	if ok && ds.SQL != nil {
		return checkCompatibleVersion(ds.SQL, r, sqlplugin.DbKindVisibility)
	}
	return nil
}

func checkCompatibleVersion(
	cfg *config.SQL,
	r resolver.ServiceResolver,
	dbKind sqlplugin.DbKind,
) error {
	db, err := NewSQLAdminDB(dbKind, cfg, r)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	return db.VerifyVersion()
}
