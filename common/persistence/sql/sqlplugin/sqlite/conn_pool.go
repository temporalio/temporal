package sqlite

import (
	"sync"

	"github.com/jmoiron/sqlx"
	"go.temporal.io/server/common/config"
)

// This pool properly enabled the support for SQLite in the temporal server.
// Internal Temporal services are highly isolated, each will create at least a single connection to the database violating
// the SQLite concept of safety only within a single thread.
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

// acquire returns the shared database for cfg and an idempotent function that releases this acquisition.
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

// release removes one retention. It only closes the shared database once no references remain.
func (cp *connPool) release(dsn string) error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	e := cp.pool[dsn]
	e.refCount--
	if e.refCount != 0 {
		return nil
	}

	// temporal will start and stop DB connections multiple times. A factory-owned database handle keeps the
	// database alive across that churn and prevents loss of the cache and "db is closed" errors.
	delete(cp.pool, dsn)
	return e.db.Close()
}
