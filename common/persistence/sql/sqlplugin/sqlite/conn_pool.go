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
	"sync"

	"github.com/jmoiron/sqlx"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/resolver"
)

// This pool properly enabled the support for SQLite in the temporal server.
// Internal Temporal services are highly isolated, each will create at least a single connection to the database violating
// the SQLite concept of safety only within a single thread.
type connPool struct {
	mu   sync.Mutex
	pool map[string]entry
}

type entry struct {
	db       *sqlx.DB
	refCount int
}

func newConnPool() *connPool {
	return &connPool{
		pool: make(map[string]entry),
	}
}

// Allocate allocates the shared database in the pool or returns already exists instance with the same DSN. If instance
// for such DSN already exists, it will be returned instead. Each request counts as reference until Close.
func (cp *connPool) Allocate(
	cfg *config.SQL,
	resolver resolver.ServiceResolver,
	create func(cfg *config.SQL, resolver resolver.ServiceResolver) (*sqlx.DB, error),
) (db *sqlx.DB, err error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	dsn, err := buildDSN(cfg)
	if err != nil {
		return nil, err
	}

	if entry, ok := cp.pool[dsn]; ok {
		entry.refCount++
		return entry.db, nil
	}

	db, err = create(cfg, resolver)
	if err != nil {
		return nil, err
	}

	cp.pool[dsn] = entry{db: db, refCount: 1}

	return db, nil
}

// Close virtual connection to database. Only closes for real once no references left.
func (cp *connPool) Close(cfg *config.SQL) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	dsn, err := buildDSN(cfg)
	if err != nil {
		return
	}

	e, ok := cp.pool[dsn]
	if !ok {
		// no such database
		return
	}

	e.refCount--
	// todo: at the moment pool will persist a single connection to the DB for the whole duration of application
	// temporal will start and stop DB connections multiple times, which will cause the loss of the cache
	// and "db is closed" error
	// if e.refCount == 0 {
	// 	e.db.Close()
	// 	delete(cp.pool, dsn)
	// }
}
