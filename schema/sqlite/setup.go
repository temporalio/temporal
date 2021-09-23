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

package sqlite

import (
	"bytes"
	_ "embed"
	"fmt"
	"io"

	"go.temporal.io/server/common/config"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
)

var (
	//go:embed v3/temporal/schema.sql
	executionSchema []byte
	//go:embed v3/visibility/schema.sql
	visibilitySchema []byte
)

func SetupSchema(cfg *config.SQL) error {
	db, err := sql.NewSQLAdminDB(sqlplugin.DbKindUnknown, cfg, resolver.NewNoopResolver())
	if err != nil {
		return fmt.Errorf("unable to create SQLite admin DB: %w", err)
	}
	defer func() { _ = db.Close() }()

	statements, err := p.LoadAndSplitQueryFromReaders([]io.Reader{bytes.NewBuffer(executionSchema)})
	if err != nil {
		return fmt.Errorf("error loading execution schema: %w", err)
	}

	for _, stmt := range statements {
		if err = db.Exec(stmt); err != nil {
			return fmt.Errorf("error executing statement %q: %w", stmt, err)
		}
	}

	statements, err = p.LoadAndSplitQueryFromReaders([]io.Reader{bytes.NewBuffer(visibilitySchema)})
	if err != nil {
		return fmt.Errorf("error loading visibility schema: %w", err)
	}

	for _, stmt := range statements {
		if err = db.Exec(stmt); err != nil {
			return fmt.Errorf("error executing statement %q: %w", stmt, err)
		}
	}

	return nil
}
