package sqlite

import (
	"errors"
	"sync"

	"github.com/jmoiron/sqlx"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

// This pool properly enabled the support for SQLite in the temporal server.
// Internal Temporal services are highly isolated, each will create at least a single connection to the database violating
// the SQLite concept of safety only within a single thread.
type connPool struct {
	mu   sync.Mutex
	pool map[string]*entry
}

type entry struct {
	db         *sqlx.DB
	references int
	leases     int
}

type databaseLease func() error

func (l databaseLease) Close() error {
	return l()
}

func newConnPool() *connPool {
	return &connPool{
		pool: make(map[string]*entry),
	}
}

func (cp *connPool) AcquireLease(cfg *config.SQL) (sqlplugin.DatabaseLease, error) {
	dsn, err := buildDSN(cfg)
	if err != nil {
		return nil, err
	}

	cp.mu.Lock()
	defer cp.mu.Unlock()

	e, ok := cp.pool[dsn]
	if !ok {
		e = &entry{}
		cp.pool[dsn] = e
	}
	e.leases++

	return databaseLease(sync.OnceValue(func() error {
		return cp.releaseLease(dsn)
	})), nil
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
			if e.leases == 0 {
				delete(cp.pool, dsn)
			}
			return nil, nil, err
		}
	}
	e.references++

	return e.db, sync.OnceValue(func() error {
		return cp.releaseReference(dsn)
	}), nil
}

// releaseReference closes a virtual connection to the database. It only closes the shared database once no
// references or leases remain.
func (cp *connPool) releaseReference(dsn string) error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	e, ok := cp.pool[dsn]
	if !ok {
		// no such database
		return errors.New("cannot release SQLite database reference for unknown DSN")
	}
	if e.references == 0 {
		return errors.New("cannot release SQLite database reference with zero references")
	}
	e.references--
	return cp.closeIfUnusedLocked(dsn, e)
}

func (cp *connPool) releaseLease(dsn string) error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	e, ok := cp.pool[dsn]
	if !ok {
		return errors.New("cannot release SQLite database lease for unknown DSN")
	}
	if e.leases == 0 {
		return errors.New("cannot release SQLite database lease with zero leases")
	}
	e.leases--
	return cp.closeIfUnusedLocked(dsn, e)
}

func (cp *connPool) closeIfUnusedLocked(dsn string, e *entry) error {
	if e.references != 0 || e.leases != 0 {
		return nil
	}

	// temporal will start and stop DB connections multiple times. An outer lease keeps the
	// database alive across that churn and prevents loss of the cache and "db is closed" errors.
	delete(cp.pool, dsn)
	if e.db == nil {
		return nil
	}
	return e.db.Close()
}
