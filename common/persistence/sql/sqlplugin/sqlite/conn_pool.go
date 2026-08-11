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

// Allocate allocates the shared database in the pool or returns already exists instance with the same DSN. If instance
// for such DSN already exists, it will be returned instead. Each request counts as reference until Close.
func (cp *connPool) Allocate(
	cfg *config.SQL,
	create func(string) (*sqlx.DB, error),
) (db *sqlx.DB, release func() error, err error) {
	dsn, err := buildDSN(cfg)
	if err != nil {
		return nil, nil, err
	}

	cp.mu.Lock()
	defer cp.mu.Unlock()

	e, ok := cp.pool[dsn]
	if !ok {
		e = &entry{}
		cp.pool[dsn] = e
	}

	if e.db == nil {
		e.db, err = create(dsn)
		if err != nil {
			if e.refCount == 0 {
				delete(cp.pool, dsn)
			}
			return nil, nil, err
		}
	}
	return e.db, cp.retainLocked(dsn, e), nil
}

func (cp *connPool) retainLocked(dsn string, e *entry) func() error {
	e.refCount++
	return sync.OnceValue(func() error {
		return cp.release(dsn)
	})
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

	// temporal will start and stop DB connections multiple times. An outer database handle keeps the
	// database alive across that churn and prevents loss of the cache and "db is closed" errors.
	delete(cp.pool, dsn)
	if e.db == nil {
		return nil
	}
	return e.db.Close()
}
