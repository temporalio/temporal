package sqlite

import (
	"sync"

	"github.com/jmoiron/sqlx"
	"go.temporal.io/server/common/config"
)

// connPool shares one *sqlx.DB for each DSN.
type connPool struct {
	mu   sync.Mutex
	pool map[string]*entry
}

type entry struct {
	db       *sqlx.DB
	refCount int
}

func newConnPool() *connPool {
	return &connPool{
		pool: make(map[string]*entry),
	}
}

// acquire returns the shared connection pool for cfg and an idempotent release function.
func (cp *connPool) acquire(
	cfg *config.SQL,
	create func(string) (*sqlx.DB, error),
) (*sqlx.DB, func() error, error) {
	dsn, err := buildDSN(cfg)
	if err != nil {
		return nil, nil, err
	}

	cp.mu.Lock()
	defer cp.mu.Unlock()

	e, ok := cp.pool[dsn]
	if !ok {
		db, err := create(dsn)
		if err != nil {
			return nil, nil, err
		}
		e = &entry{db: db}
		cp.pool[dsn] = e
	}
	e.refCount++
	release := sync.OnceValue(func() error {
		return cp.release(dsn)
	})
	return e.db, release, nil
}

func (cp *connPool) release(dsn string) error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	e := cp.pool[dsn]
	e.refCount--
	if e.refCount != 0 {
		return nil
	}

	delete(cp.pool, dsn)
	return e.db.Close()
}
